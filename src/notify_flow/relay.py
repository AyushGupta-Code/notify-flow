"""Transactional-outbox relay: publish unpublished outbox rows, then mark them published.
Run: python -m notify_flow.relay"""

import asyncio
import json
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notify_flow.db.models import Outbox
from notify_flow.log import get_logger
from notify_flow.metrics import OUTBOX_PENDING, OUTBOX_PUBLISHED
from notify_flow.queue.base import EventPublisher

log = get_logger(__name__)


class OutboxRelay:
    def __init__(
        self, sm: async_sessionmaker[AsyncSession], publisher: EventPublisher, batch: int = 100
    ) -> None:
        self.sm = sm
        self.pub = publisher
        self.batch = batch

    async def run_once(self) -> int:
        async with self.sm.begin() as s:
            # SKIP LOCKED lets several relay replicas share the table without double-publishing
            # the same row concurrently. A crash after publish but before commit republishes
            # the row; downstream dedup on event_id absorbs that.
            rows = (
                (
                    await s.execute(
                        select(Outbox)
                        .where(Outbox.published_at.is_(None))
                        .order_by(Outbox.id)
                        .limit(self.batch)
                        .with_for_update(skip_locked=True)
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                row.attempts += 1
                await self.pub.publish(
                    row.key,
                    json.dumps(row.payload).encode(),
                    {k: str(v) for k, v in row.headers.items()},
                )
                row.published_at = datetime.now(UTC)
            OUTBOX_PUBLISHED.inc(len(rows))
        async with self.sm() as s:
            pending = (
                await s.execute(
                    select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None))
                )
            ).scalar_one()
            OUTBOX_PENDING.set(pending)
        return len(rows)

    async def run(self, idle_sleep_s: float = 0.2) -> None:
        while True:
            try:
                n = await self.run_once()
            except Exception:
                log.exception("relay.error")
                n = 0
                await asyncio.sleep(2)
            if n == 0:
                await asyncio.sleep(idle_sleep_s)
