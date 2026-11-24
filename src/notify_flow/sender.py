"""Channel providers. Real deployments would wrap Twilio / FCM / SES; locally a DB-backed
fake plays the vendor, honouring the idempotency key the way those APIs do."""

import uuid
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notify_flow.db.models import ProviderDelivery


class ChannelProvider(Protocol):
    async def send(
        self, *, idempotency_key: str, channel: str, member_id: str, body: str
    ) -> str: ...


class DbFakeChannelProvider:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self._sm = sessionmaker
        self.crash_after_deliver = 0  # test hook: raise after delivering N times

    async def send(self, *, idempotency_key: str, channel: str, member_id: str, body: str) -> str:
        async with self._sm.begin() as s:
            await s.execute(
                insert(ProviderDelivery)
                .values(
                    idempotency_key=idempotency_key,
                    provider_message_id=uuid.uuid4().hex,
                    channel=channel,
                    member_id=member_id,
                )
                .on_conflict_do_nothing(index_elements=["idempotency_key"])
            )
            msg_id = (
                await s.execute(
                    select(ProviderDelivery.provider_message_id).where(
                        ProviderDelivery.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one()
        if self.crash_after_deliver > 0:
            self.crash_after_deliver -= 1
            raise ConnectionError("simulated crash after the vendor accepted the message")
        return str(msg_id)
