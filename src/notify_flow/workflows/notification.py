"""Per-event member notification workflow.

plan -> wait for quiet hours -> compose -> send -> wait for engagement (signal or timeout)
-> if no engagement, escalate to the next-best channel -> done.
"""

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from notify_flow.quiet_hours import next_allowed_send
    from notify_flow.types import (
        ComposeInput,
        CopyResult,
        NotificationInput,
        Plan,
        SendInput,
        SendResult,
    )

RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=8,
)
ACT_TIMEOUT = timedelta(seconds=30)
# LLM compose can retry internally; give it more room, and it never fails the workflow
# because the activity falls back to a template.
COMPOSE_TIMEOUT = timedelta(seconds=60)


@workflow.defn(name="MemberNotificationWorkflow")
class MemberNotificationWorkflow:
    def __init__(self) -> None:
        self._engaged = False
        self._status = "started"

    @workflow.signal
    def engaged(self) -> None:
        self._engaged = True

    @workflow.query
    def status(self) -> str:
        return self._status

    async def _wait_for_quiet_hours(self, plan: Plan) -> None:
        # Computed from workflow.now(): deterministic on replay and on the same clock as
        # the durable timer, so a worker restart mid-wait resumes correctly.
        if plan.quiet_hours is None:
            return
        now = workflow.now()
        start, end = plan.quiet_hours
        delay = (next_allowed_send(now, plan.timezone, start, end) - now).total_seconds()
        if delay > 0:
            self._status = "waiting_quiet_hours"
            await workflow.sleep(timedelta(seconds=delay))

    async def _compose_and_send(
        self, inp: NotificationInput, plan: Plan, channel: str, step: int
    ) -> SendResult:
        self._status = f"composing_step_{step}"
        copy = await workflow.execute_activity(
            "compose_message",
            ComposeInput(inp.member_id, inp.campaign, channel, plan.context, inp.correlation_id),
            result_type=CopyResult,
            start_to_close_timeout=COMPOSE_TIMEOUT,
            retry_policy=RETRY,
        )
        self._status = f"sending_step_{step}"
        return await workflow.execute_activity(
            "send_notification",
            SendInput(inp, plan, copy, step, channel),
            result_type=SendResult,
            start_to_close_timeout=ACT_TIMEOUT,
            retry_policy=RETRY,
        )

    @workflow.run
    async def run(self, inp: NotificationInput) -> dict[str, Any]:
        plan = await workflow.execute_activity(
            "plan_notification",
            inp,
            result_type=Plan,
            start_to_close_timeout=ACT_TIMEOUT,
            retry_policy=RETRY,
        )
        if plan.skip:
            self._status = f"skipped:{plan.reason}"
            return {"status": "skipped", "reason": plan.reason}

        await self._wait_for_quiet_hours(plan)
        first = await self._compose_and_send(inp, plan, plan.channel, 1)

        self._status = "awaiting_engagement"
        try:
            await workflow.wait_condition(
                lambda: self._engaged, timeout=timedelta(seconds=inp.engagement_wait_s)
            )
        except TimeoutError:
            pass

        sends = [first.status]
        if not self._engaged and plan.escalation_channel and inp.campaign != "pain_checkin":
            await self._wait_for_quiet_hours(plan)
            second = await self._compose_and_send(inp, plan, plan.escalation_channel, 2)
            sends.append(second.status)

        self._status = "engaged" if self._engaged else "completed"
        return {
            "status": self._status,
            "channel": plan.channel,
            "escalated_to": plan.escalation_channel if len(sends) > 1 else None,
            "sends": sends,
        }
