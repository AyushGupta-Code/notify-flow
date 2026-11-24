"""Temporal activities. Each is safe to retry: reads are pure and the send is keyed."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio import activity

from notify_flow.channel.policy import CHANNELS, ChannelPolicy, MemberFeatures
from notify_flow.config import Settings
from notify_flow.db.models import Member, Notification
from notify_flow.llm.personalize import Personalizer
from notify_flow.log import correlation_id, get_logger
from notify_flow.metrics import (
    END_TO_END_LATENCY,
    NOTIFICATIONS_SENT,
    SEND_DUPLICATES_SUPPRESSED,
    SEND_FAILURES,
)
from notify_flow.quiet_hours import next_allowed_send
from notify_flow.sender import ChannelProvider
from notify_flow.types import (
    ComposeInput,
    CopyResult,
    NotificationInput,
    Plan,
    SendInput,
    SendResult,
)

log = get_logger(__name__)


def _eligible(m: Member) -> tuple[str, ...]:
    flags = {
        "push": m.push_enabled,
        "sms": m.sms_opt_in and bool(m.phone),
        "email": m.email_opt_in and bool(m.email),
    }
    return tuple(c for c in CHANNELS if flags[c])


class NotificationActivities:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        personalizer: Personalizer,
        policy: ChannelPolicy,
        provider: ChannelProvider,
        settings: Settings,
    ) -> None:
        self.sm = sessionmaker
        self.personalizer = personalizer
        self.policy = policy
        self.provider = provider
        self.settings = settings

    def _send_after(self, m: Member) -> float:
        now = datetime.now(UTC)
        if not self.settings.quiet_hours_enabled:
            return now.timestamp()
        return next_allowed_send(
            now, m.timezone, self.settings.quiet_hours_start, self.settings.quiet_hours_end
        ).timestamp()

    @activity.defn(name="plan_notification")
    async def plan_notification(self, inp: NotificationInput) -> Plan:
        correlation_id.set(inp.correlation_id)
        async with self.sm() as s:
            m = await s.get(Member, inp.member_id)
        if m is None:
            log.warning("plan.unknown_member", event_id=inp.event_id)
            return Plan(skip=True, reason="unknown_member")
        eligible = _eligible(m)
        if not eligible:
            return Plan(skip=True, reason="no_eligible_channel")
        local_hour = datetime.now(UTC).astimezone(ZoneInfo(m.timezone)).hour
        features = MemberFeatures(
            m.program_week,
            m.sessions_last_7d,
            m.days_since_last_session,
            m.open_rate_sms,
            m.open_rate_push,
            m.open_rate_email,
            local_hour,
            m.preferred_channel,
            eligible,
        )
        d = self.policy.decide(features)
        context = {
            "program_week": m.program_week,
            "days_since_last_session": m.days_since_last_session,
            "session_count": inp.payload.get("session_count"),
        }
        log.info(
            "plan.decided",
            event_id=inp.event_id,
            channel=d.channel,
            source=d.source,
            model_version=d.model_version,
        )
        return Plan(
            skip=False,
            reason="ok",
            channel=d.channel,
            escalation_channel=d.escalation_channel,
            decision_source=d.source,
            channel_model_version=d.model_version,
            send_after_epoch=self._send_after(m),
            context={k: v for k, v in context.items() if v is not None},
        )

    @activity.defn(name="next_send_time")
    async def next_send_time(self, member_id: str) -> float:
        async with self.sm() as s:
            m = await s.get(Member, member_id)
        return self._send_after(m) if m else datetime.now(UTC).timestamp()

    @activity.defn(name="compose_message")
    async def compose_message(self, inp: ComposeInput) -> CopyResult:
        correlation_id.set(inp.correlation_id)
        async with self.sm() as s:
            m = await s.get(Member, inp.member_id)
        member_values = [v for v in (m.first_name, m.email, m.phone) if v] if m else []
        result = await self.personalizer.compose(
            inp.campaign, inp.channel, inp.context, member_values=member_values
        )
        log.info(
            "compose.done",
            campaign=inp.campaign,
            source=result.source,
            reason=result.reason,
            prompt_version=result.prompt_version,
        )
        return result

    @activity.defn(name="send_notification")
    async def send_notification(self, inp: SendInput) -> SendResult:
        n, plan, copy = inp.notification, inp.plan, inp.copy
        correlation_id.set(n.correlation_id)
        key = f"{n.event_id}:{inp.step}"
        async with self.sm.begin() as s:
            await s.execute(
                insert(Notification)
                .values(
                    idempotency_key=key,
                    member_id=n.member_id,
                    event_id=n.event_id,
                    workflow_id=activity.info().workflow_id,
                    campaign=n.campaign,
                    step=inp.step,
                    channel=inp.channel,
                    status="pending",
                    body_template=copy.body_template,
                    copy_source=copy.source,
                    copy_reason=copy.reason,
                    prompt_version=copy.prompt_version,
                    llm_model=copy.llm_model,
                    template_version=copy.template_version,
                    channel_model_version=plan.channel_model_version,
                    decision_source=plan.decision_source,
                    correlation_id=n.correlation_id,
                )
                .on_conflict_do_nothing(constraint="uq_notifications_idem")
            )
            # Row lock: a concurrent retry of the same key waits here instead of double-sending.
            row = (
                await s.execute(
                    select(Notification)
                    .where(Notification.idempotency_key == key)
                    .with_for_update()
                )
            ).scalar_one()
            if row.status == "sent":
                SEND_DUPLICATES_SUPPRESSED.inc()
                log.info("send.duplicate_suppressed", key=key)
                return SendResult(row.id, "duplicate", row.provider_message_id)
            member = await s.get(Member, n.member_id)
            if member is None:
                raise RuntimeError("member disappeared")
            body = row.body_template.replace("{first_name}", member.first_name)
            try:
                msg_id = await self.provider.send(
                    idempotency_key=key, channel=inp.channel, member_id=n.member_id, body=body
                )
            except Exception:
                SEND_FAILURES.labels(inp.channel).inc()
                raise
            row.status, row.provider_message_id = "sent", msg_id
            row.sent_at = datetime.now(UTC)
        NOTIFICATIONS_SENT.labels(inp.channel, str(inp.step), copy.source).inc()
        if inp.step == 1:
            occurred = datetime.fromisoformat(n.occurred_at)
            END_TO_END_LATENCY.observe(max(0.0, (datetime.now(UTC) - occurred).total_seconds()))
        log.info("send.sent", key=key, channel=inp.channel, copy_source=copy.source)
        return SendResult(row.id, "sent", msg_id)

    def all(self) -> list[Callable[..., Any]]:
        return [
            self.plan_notification,
            self.next_send_time,
            self.compose_message,
            self.send_notification,
        ]
