"""AWS Lambda entry points for the executable Step Functions workflow.

The handlers deliberately stay thin. Planning, personalization, guardrails, template
fallback, channel selection, and the idempotent send all use the same implementations as
the Temporal worker. State Functions carries only synthetic member-event state between
tasks, and every handler binds the original correlation ID before logging.
"""

import asyncio
import dataclasses
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from notify_flow.channel.policy import ChannelPolicy
from notify_flow.config import Settings, get_settings
from notify_flow.db.models import Notification
from notify_flow.db.session import make_engine, make_sessionmaker
from notify_flow.llm.personalize import Personalizer
from notify_flow.llm.providers import build_provider
from notify_flow.llm.registry import load_registry
from notify_flow.llm.resilience import CircuitBreaker
from notify_flow.log import configure_logging, correlation_id, get_logger
from notify_flow.notification_service import NotificationService
from notify_flow.orchestrator.stepfunctions import execution_name
from notify_flow.quiet_hours import next_allowed_send
from notify_flow.sender import DbFakeChannelProvider
from notify_flow.types import ComposeInput, CopyResult, NotificationInput, Plan, SendInput

log = get_logger(__name__)
configure_logging(get_settings().log_level, "notify-flow-lambda")


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _service(settings: Settings) -> NotificationService:
    sessionmaker = make_sessionmaker(make_engine(settings.database_url))
    provider = build_provider(
        settings.llm_provider,
        settings.llm_model,
        settings.llm_base_url,
        settings.anthropic_api_key,
    )
    personalizer = Personalizer(
        load_registry(settings.prompt_registry_path),
        provider,
        CircuitBreaker(settings.breaker_failure_threshold, settings.breaker_reset_s),
        timeout_s=settings.llm_timeout_s,
        max_attempts=settings.llm_max_attempts,
        backoff_base_s=settings.llm_backoff_base_s,
    )
    return NotificationService(
        sessionmaker,
        personalizer,
        ChannelPolicy.from_path(settings.channel_model_path),
        DbFakeChannelProvider(sessionmaker),
        settings,
    )


def _notification(state: dict[str, Any]) -> NotificationInput:
    fields = {field.name for field in dataclasses.fields(NotificationInput)}
    return NotificationInput(**{key: value for key, value in state.items() if key in fields})


def _plan(state: dict[str, Any]) -> Plan:
    return Plan(**state["plan"])


def _bind(state: dict[str, Any]) -> None:
    correlation_id.set(state.get("correlation_id"))


def _fail_if_requested(state: dict[str, Any], handler: str) -> None:
    if state.get("force_failure") == handler:
        log.error("lambda.forced_failure", handler=handler, event_id=state.get("event_id"))
        raise RuntimeError(f"forced failure in {handler}")


async def _select_channel(state: dict[str, Any]) -> dict[str, Any]:
    _bind(state)
    _fail_if_requested(state, "select_channel")
    plan = await _service(get_settings()).plan(_notification(state))
    result = dict(state)
    result.update(plan=dataclasses.asdict(plan), step=1, active_channel=plan.channel)
    log.info("lambda.channel_selected", event_id=state["event_id"], channel=plan.channel)
    return result


def select_channel_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return _run(_select_channel(event))


def quiet_hours_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    _bind(event)
    _fail_if_requested(event, "quiet_hours")
    result = dict(event)
    plan = _plan(event)
    wait_seconds = 0
    if plan.quiet_hours is not None:
        now = datetime.now(UTC)
        start, end = plan.quiet_hours
        wait_seconds = max(
            0,
            round((next_allowed_send(now, plan.timezone, start, end) - now).total_seconds()),
        )
    result["quiet_wait_seconds"] = wait_seconds
    log.info("lambda.quiet_hours_checked", event_id=event["event_id"], wait_s=wait_seconds)
    return result


async def _personalize(state: dict[str, Any]) -> dict[str, Any]:
    _bind(state)
    _fail_if_requested(state, "personalize")
    settings = get_settings()
    if state.get("llm_mode") == "down":
        # Exercise the real Ollama HTTP provider against a closed local port. Its LLMError
        # flows through the production retry and template-fallback path.
        settings = Settings(
            **{
                **settings.model_dump(),
                "llm_provider": "ollama",
                "llm_model": "unreachable-test-model",
                "llm_base_url": "http://127.0.0.1:1",
                "llm_timeout_s": 0.2,
                "llm_max_attempts": 2,
                "llm_backoff_base_s": 0.01,
            }
        )
    activities = _service(settings)
    plan = _plan(state)
    copy = await activities.compose(
        ComposeInput(
            state["member_id"],
            state["campaign"],
            state["active_channel"],
            plan.context,
            state.get("correlation_id"),
        )
    )
    result = dict(state)
    result["copy"] = dataclasses.asdict(copy)
    log.info(
        "lambda.personalized",
        event_id=state["event_id"],
        source=copy.source,
        prompt_version=copy.prompt_version,
    )
    return result


def personalize_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return _run(_personalize(event))


async def _send(state: dict[str, Any]) -> dict[str, Any]:
    _bind(state)
    _fail_if_requested(state, "send")
    inp = _notification(state)
    step = int(state.get("step", 1))
    activities = _service(get_settings())
    sent = await activities.send(
        SendInput(inp, _plan(state), CopyResult(**state["copy"]), step, state["active_channel"]),
        workflow_id=execution_name(inp.event_id),
    )
    result = dict(state)
    sends = list(result.get("sends", []))
    sends.append(dataclasses.asdict(sent))
    result.update(sends=sends, last_send=dataclasses.asdict(sent))
    log.info(
        "lambda.send_completed",
        event_id=state["event_id"],
        step=step,
        status=sent.status,
    )
    return result


def send_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return _run(_send(event))


async def _engagement(state: dict[str, Any]) -> dict[str, Any]:
    _bind(state)
    _fail_if_requested(state, "engagement")
    engaged = bool(state.get("force_engaged", False))
    if not engaged:
        activities = _service(get_settings())
        async with activities.sm() as session:
            engaged = (
                await session.execute(
                    select(Notification.engaged_at).where(
                        Notification.event_id == state["event_id"], Notification.step == 1
                    )
                )
            ).scalar_one_or_none() is not None
    result = dict(state)
    result["engaged"] = engaged
    log.info("lambda.engagement_checked", event_id=state["event_id"], engaged=engaged)
    return result


def engagement_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return _run(_engagement(event))


def escalation_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    _bind(event)
    _fail_if_requested(event, "escalation")
    plan = _plan(event)
    result = dict(event)
    result.update(step=2, active_channel=plan.escalation_channel)
    log.info(
        "lambda.escalation_selected",
        event_id=event["event_id"],
        channel=plan.escalation_channel,
    )
    return result
