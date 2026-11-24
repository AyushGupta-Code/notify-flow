"""End-to-end through a real Temporal test server (time-skipping) and real Postgres."""

import asyncio
import uuid

import pytest
from temporalio.client import WorkflowExecutionStatus
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from notify_flow.config import Settings
from notify_flow.db.models import ProcessedEvent
from notify_flow.events import MemberEvent
from notify_flow.ingest import IngestHandler
from notify_flow.llm.providers import LLMError
from notify_flow.orchestrator.temporal import TemporalOrchestrator
from notify_flow.queue.base import QueueMessage
from notify_flow.worker import build_activities
from notify_flow.workflows.notification import MemberNotificationWorkflow

from .helpers import add_member, count, deliveries, notifications

pytestmark = pytest.mark.integration
TQ = "test-notifications"
WAIT = 3600  # engagement window used in tests (skipped instantly by the test server)


@pytest.fixture(scope="session")
async def env():
    e = await WorkflowEnvironment.start_time_skipping()
    yield e
    await e.shutdown()


def settings(**kw):
    base = dict(
        quiet_hours_enabled=False,
        llm_backoff_base_s=0.0,
        llm_max_attempts=2,
        breaker_failure_threshold=3,
    )
    base.update(kw)
    return Settings(**base)


@pytest.fixture
def make_worker(env, sm):
    def _make(s=None):
        acts = build_activities(s or settings(), sm)
        w = Worker(
            env.client, task_queue=TQ, workflows=[MemberNotificationWorkflow], activities=acts.all()
        )
        return w, acts

    return _make


def msg(event_type="session_missed", event_id=None, member_id="m-001", **payload):
    e = MemberEvent(
        event_id=event_id or f"evt-{uuid.uuid4().hex[:12]}",
        member_id=member_id,
        event_type=event_type,
        payload=payload,
    )
    return QueueMessage(member_id, e.model_dump_json().encode(), {"correlation_id": "cid-test"})


async def run_event(env, sm, m):
    handler = IngestHandler(sm, TemporalOrchestrator(env.client, TQ), "test", WAIT)
    outcome = await handler.handle(m)
    event_id = MemberEvent.model_validate_json(m.value).event_id
    return outcome, await finish(env, f"notify-{event_id}")


async def finish(env, workflow_id):
    """Skip server time until the workflow closes. Auto time-skipping only applies to handles
    returned by start_workflow, and the start happens inside the orchestrator here."""
    handle = env.client.get_workflow_handle(workflow_id)
    for _ in range(500):
        desc = await handle.describe()
        if desc.status != WorkflowExecutionStatus.RUNNING:
            return await handle.result()
        await env.sleep(300)
    raise AssertionError("workflow did not finish")


async def test_send_then_escalate_when_no_engagement(env, sm, make_worker):
    await add_member(sm)
    w, _ = make_worker()
    async with w:
        outcome, result = await run_event(env, sm, msg())
    assert outcome == "started"
    assert result["status"] == "completed" and result["sends"] == ["sent", "sent"]
    rows = await notifications(sm, "m-001")
    assert [r.step for r in rows] == [1, 2]
    assert rows[0].channel == "push" and rows[1].channel != "push"  # escalated
    first = rows[0]
    assert first.copy_source == "llm" and first.prompt_version == "reengage-v2"
    assert first.llm_model == "mock-copywriter-1"
    assert first.channel_model_version == "channel-lr-v1" and first.decision_source == "model"
    assert first.correlation_id == "cid-test"
    assert "Priya" not in first.body_template  # name only rendered at send time
    assert await deliveries(sm, "m-001") == 2


async def test_engagement_signal_stops_escalation(env, sm, make_worker):
    await add_member(sm)
    w, _ = make_worker()
    m = msg()
    event_id = MemberEvent.model_validate_json(m.value).event_id
    orch = TemporalOrchestrator(env.client, TQ)
    async with w:
        await IngestHandler(sm, orch, "test", WAIT).handle(m)
        handle = env.client.get_workflow_handle(f"notify-{event_id}")
        # wait (real time, no skipping) until the first send happened, then engage
        for _ in range(200):
            if await handle.query(MemberNotificationWorkflow.status) == "awaiting_engagement":
                break
            await asyncio.sleep(0.05)
        await orch.signal_engagement(f"notify-{event_id}")
        result = await finish(env, f"notify-{event_id}")
    assert result["status"] == "engaged" and result["sends"] == ["sent"]
    assert await deliveries(sm, "m-001") == 1


async def test_duplicate_events_produce_one_notification(env, sm, make_worker):
    await add_member(sm)
    w, _ = make_worker()
    m = msg(event_id="evt-dup-000001")
    handler = IngestHandler(sm, TemporalOrchestrator(env.client, TQ), "test", WAIT)
    async with w:
        outcomes = [await handler.handle(m) for _ in range(3)]  # redelivery x3
        await finish(env, "notify-evt-dup-000001")
        # even after the workflow closed, a late duplicate starts nothing
        outcomes.append(await handler.handle(m))
        # and a duplicate that bypasses the processed_events table hits the
        # orchestrator's workflow-ID check
        async with sm.begin() as s:
            await s.delete(await s.get(ProcessedEvent, "evt-dup-000001"))
        outcomes.append(await handler.handle(m))
    assert outcomes == ["started", "duplicate", "duplicate", "duplicate", "duplicate"]
    assert await deliveries(sm, "m-001") == 2  # step 1 + escalation, once each
    assert len(await notifications(sm, "m-001")) == 2


async def test_crash_after_vendor_accepts_does_not_double_send(env, sm, make_worker):
    await add_member(sm)
    w, acts = make_worker()
    acts.provider.crash_after_deliver = 1  # first send: vendor delivers, then we crash
    async with w:
        _, result = await run_event(env, sm, msg())
    assert result["sends"] == ["sent", "sent"]
    rows = await notifications(sm, "m-001")
    assert all(r.status == "sent" for r in rows)
    assert await deliveries(sm, "m-001") == 2  # not 3


async def test_llm_outage_degrades_to_templates(env, sm, make_worker):
    await add_member(sm)
    w, acts = make_worker()

    class Down:
        name, model = "down", "down-1"

        async def generate(self, prompt, max_tokens=200):
            raise LLMError("connection refused")

    acts.personalizer.provider = Down()
    async with w:
        _, result = await run_event(env, sm, msg())
        _, result2 = await run_event(env, sm, msg())
    assert result["sends"] == ["sent", "sent"] and result2["sends"] == ["sent", "sent"]
    rows = await notifications(sm, "m-001")
    assert {r.copy_source for r in rows} == {"template"}
    reasons = [r.copy_reason for r in rows]
    assert reasons[0] == "llm_error" and "circuit_open" in reasons
    assert all(r.template_version == "tmpl-reengage-v1" for r in rows)


async def test_pain_checkin_is_template_only_and_not_escalated(env, sm, make_worker):
    await add_member(sm)
    w, _ = make_worker()
    async with w:
        _, result = await run_event(env, sm, msg("pain_score_reported", score=8))
    assert result["sends"] == ["sent"]
    (row,) = await notifications(sm, "m-001")
    assert row.copy_source == "template" and row.copy_reason == "policy"


async def test_quiet_hours_creates_durable_timer(env, sm, make_worker):
    # Quiet window = the current server-clock hour plus the next one, so a wait is certain.
    h = (await env.get_current_time()).hour
    await add_member(sm)
    w, _ = make_worker(
        settings(quiet_hours_enabled=True, quiet_hours_start=h, quiet_hours_end=(h + 2) % 24)
    )
    m = msg()
    event_id = MemberEvent.model_validate_json(m.value).event_id
    async with w:
        await run_event(env, sm, m)
        history = await env.client.get_workflow_handle(f"notify-{event_id}").fetch_history()
    timers = [
        e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()
        for e in history.events
        if e.HasField("timer_started_event_attributes")
    ]
    quiet_waits = [t for t in timers if t.total_seconds() > 60 and t.total_seconds() != WAIT]
    assert quiet_waits and all(t.total_seconds() <= 2 * 3600 for t in quiet_waits), timers
    # first timer is the quiet-hours wait, before any send
    assert timers[0] in quiet_waits


async def test_unknown_member_is_skipped(env, sm, make_worker):
    w, _ = make_worker()
    async with w:
        _, result = await run_event(env, sm, msg(member_id="ghost"))
    assert result == {"status": "skipped", "reason": "unknown_member"}
    assert await count(sm, ProcessedEvent) == 1
