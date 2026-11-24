"""Queue -> orchestrator. Idempotent: a redelivered or duplicated event starts nothing new."""

from datetime import UTC, datetime

from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notify_flow.db.models import ProcessedEvent
from notify_flow.events import MemberEvent, campaign_for
from notify_flow.log import correlation_id, get_logger
from notify_flow.metrics import EVENTS_CONSUMED, WORKFLOWS_STARTED
from notify_flow.orchestrator.base import Orchestrator
from notify_flow.queue.base import QueueMessage
from notify_flow.types import NotificationInput

log = get_logger(__name__)


class IngestHandler:
    def __init__(
        self,
        sm: async_sessionmaker[AsyncSession],
        orchestrator: Orchestrator,
        backend: str,
        engagement_wait_s: int,
    ) -> None:
        self.sm = sm
        self.orch = orchestrator
        self.backend = backend
        self.engagement_wait_s = engagement_wait_s

    async def __call__(self, msg: QueueMessage) -> None:
        await self.handle(msg)

    async def handle(self, msg: QueueMessage) -> str:
        correlation_id.set(msg.headers.get("correlation_id"))
        try:
            event = MemberEvent.model_validate_json(msg.value)
        except ValidationError:
            # Poison message: count it and move on rather than block the partition.
            EVENTS_CONSUMED.labels(self.backend, "invalid").inc()
            log.error("ingest.invalid_event")
            return "invalid"

        async with self.sm() as s:
            if await s.get(ProcessedEvent, event.event_id) is not None:
                EVENTS_CONSUMED.labels(self.backend, "duplicate").inc()
                log.info("ingest.duplicate", event_id=event.event_id)
                return "duplicate"

        campaign = campaign_for(event)
        if campaign is None:
            EVENTS_CONSUMED.labels(self.backend, "no_campaign").inc()
            await self._mark(event.event_id, "-")
            return "no_campaign"

        inp = NotificationInput(
            event_id=event.event_id,
            member_id=event.member_id,
            event_type=event.event_type,
            campaign=campaign,
            payload=event.payload,
            occurred_at=event.occurred_at.astimezone(UTC).isoformat(),
            correlation_id=msg.headers.get("correlation_id"),
            engagement_wait_s=self.engagement_wait_s,
        )
        # Start first, record second: if we crash in between, the redelivered message hits
        # the orchestrator's duplicate-ID check instead of being lost.
        res = await self.orch.start_notification(inp)
        WORKFLOWS_STARTED.labels(self.orch.name, "started" if res.started else "duplicate").inc()
        await self._mark(event.event_id, res.workflow_id)
        EVENTS_CONSUMED.labels(self.backend, "started" if res.started else "duplicate").inc()
        log.info(
            "ingest.dispatched",
            event_id=event.event_id,
            workflow_id=res.workflow_id,
            started=res.started,
        )
        return "started" if res.started else "duplicate"

    async def _mark(self, event_id: str, workflow_id: str) -> None:
        async with self.sm.begin() as s:
            await s.execute(
                insert(ProcessedEvent)
                .values(event_id=event_id, workflow_id=workflow_id, processed_at=datetime.now(UTC))
                .on_conflict_do_nothing(index_elements=["event_id"])
            )
