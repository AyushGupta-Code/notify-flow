"""Shared notification domain operations used by Temporal activities and AWS Lambda."""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notify_flow.channel.policy import CHANNELS, ChannelPolicy, MemberFeatures
from notify_flow.config import Settings
from notify_flow.db.models import Member, Notification, NotificationReservation
from notify_flow.llm.personalize import Personalizer
from notify_flow.log import correlation_id, get_logger
from notify_flow.metrics import (
    END_TO_END_LATENCY,
    NOTIFICATIONS_SENT,
    SEND_DUPLICATES_SUPPRESSED,
    SEND_FAILURES,
    VENDOR_FAILOVERS,
)
from notify_flow.sender import ChannelProvider, ProviderUnavailable
from notify_flow.types import (
    ComposeInput,
    CopyResult,
    NotificationInput,
    Plan,
    SendInput,
    SendResult,
)

log = get_logger(__name__)


def _eligible(member: Member) -> tuple[str, ...]:
    flags = {
        "push": member.push_enabled,
        "sms": member.sms_opt_in and bool(member.phone),
        "email": member.email_opt_in and bool(member.email),
    }
    return tuple(channel for channel in CHANNELS if flags[channel])


class NotificationService:
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

    async def plan(self, inp: NotificationInput) -> Plan:
        correlation_id.set(inp.correlation_id)
        async with self.sm.begin() as session:
            member = (
                await session.execute(
                    select(Member).where(Member.id == inp.member_id).with_for_update()
                )
            ).scalar_one_or_none()
            if member is None:
                log.warning("plan.unknown_member", event_id=inp.event_id)
                return Plan(skip=True, reason="unknown_member")
            eligible = _eligible(member)
            if not eligible:
                return Plan(skip=True, reason="no_eligible_channel")
            reservation = await session.get(NotificationReservation, inp.event_id)
            cap = self.settings.member_frequency_cap_24h
            if cap > 0 and reservation is None:
                cutoff = datetime.now(UTC) - timedelta(hours=24)
                used = (
                    await session.execute(
                        select(func.count())
                        .select_from(NotificationReservation)
                        .where(
                            NotificationReservation.member_id == inp.member_id,
                            NotificationReservation.reserved_at >= cutoff,
                        )
                    )
                ).scalar_one()
                if used >= cap:
                    log.info("plan.frequency_capped", event_id=inp.event_id, cap=cap)
                    return Plan(skip=True, reason="frequency_cap")
                session.add(NotificationReservation(event_id=inp.event_id, member_id=inp.member_id))
        local_hour = datetime.now(UTC).astimezone(ZoneInfo(member.timezone)).hour
        features = MemberFeatures(
            member.program_week,
            member.sessions_last_7d,
            member.days_since_last_session,
            member.open_rate_sms,
            member.open_rate_push,
            member.open_rate_email,
            local_hour,
            member.preferred_channel,
            eligible,
        )
        decision = self.policy.decide(features)
        context = {
            "program_week": member.program_week,
            "days_since_last_session": member.days_since_last_session,
            "session_count": inp.payload.get("session_count"),
        }
        log.info(
            "plan.decided",
            event_id=inp.event_id,
            channel=decision.channel,
            source=decision.source,
            model_version=decision.model_version,
        )
        quiet = (
            (self.settings.quiet_hours_start, self.settings.quiet_hours_end)
            if self.settings.quiet_hours_enabled
            else None
        )
        return Plan(
            skip=False,
            reason="ok",
            channel=decision.channel,
            escalation_channel=decision.escalation_channel,
            decision_source=decision.source,
            channel_model_version=decision.model_version,
            timezone=member.timezone,
            quiet_hours=quiet,
            context={key: value for key, value in context.items() if value is not None},
        )

    async def compose(self, inp: ComposeInput) -> CopyResult:
        correlation_id.set(inp.correlation_id)
        async with self.sm() as session:
            member = await session.get(Member, inp.member_id)
        member_values = (
            [value for value in (member.first_name, member.email, member.phone) if value]
            if member
            else []
        )
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

    async def send(self, inp: SendInput, *, workflow_id: str) -> SendResult:
        notification, plan, copy = inp.notification, inp.plan, inp.copy
        correlation_id.set(notification.correlation_id)
        key = f"{notification.event_id}:{inp.step}"
        async with self.sm.begin() as session:
            await session.execute(
                insert(Notification)
                .values(
                    idempotency_key=key,
                    member_id=notification.member_id,
                    event_id=notification.event_id,
                    workflow_id=workflow_id,
                    campaign=notification.campaign,
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
                    correlation_id=notification.correlation_id,
                )
                .on_conflict_do_nothing(constraint="uq_notifications_idem")
            )
            row = (
                await session.execute(
                    select(Notification)
                    .where(Notification.idempotency_key == key)
                    .with_for_update()
                )
            ).scalar_one()
            if row.status == "sent":
                SEND_DUPLICATES_SUPPRESSED.inc()
                log.info("send.duplicate_suppressed", key=key)
                return SendResult(row.id, "duplicate", row.provider_message_id, row.channel)
            member = await session.get(Member, notification.member_id)
            if member is None:
                raise RuntimeError("member disappeared")
            body = row.body_template.replace("{first_name}", member.first_name)
            delivered_channel = inp.channel
            try:
                message_id = await self.provider.send(
                    idempotency_key=key,
                    channel=delivered_channel,
                    member_id=notification.member_id,
                    body=body,
                )
            except ProviderUnavailable:
                fallback = plan.escalation_channel if inp.step == 1 else None
                if not fallback or fallback == inp.channel:
                    SEND_FAILURES.labels(inp.channel).inc()
                    raise
                VENDOR_FAILOVERS.labels(inp.channel, fallback).inc()
                log.warning(
                    "send.vendor_failover",
                    key=key,
                    from_channel=inp.channel,
                    to_channel=fallback,
                )
                delivered_channel = fallback
                try:
                    message_id = await self.provider.send(
                        idempotency_key=key,
                        channel=delivered_channel,
                        member_id=notification.member_id,
                        body=body,
                    )
                except Exception:
                    SEND_FAILURES.labels(delivered_channel).inc()
                    raise
            except Exception:
                SEND_FAILURES.labels(inp.channel).inc()
                raise
            row.channel = delivered_channel
            row.status = "sent"
            row.provider_message_id = message_id
            row.sent_at = datetime.now(UTC)
        NOTIFICATIONS_SENT.labels(delivered_channel, str(inp.step), copy.source).inc()
        if inp.step == 1:
            occurred = datetime.fromisoformat(notification.occurred_at)
            END_TO_END_LATENCY.observe(max(0.0, (datetime.now(UTC) - occurred).total_seconds()))
        log.info("send.sent", key=key, channel=delivered_channel, copy_source=copy.source)
        return SendResult(row.id, "sent", message_id, delivered_channel)
