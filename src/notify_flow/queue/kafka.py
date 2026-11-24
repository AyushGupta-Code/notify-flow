import asyncio

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from notify_flow.log import get_logger
from notify_flow.metrics import CONSUMER_LAG
from notify_flow.queue.base import Handler, QueueMessage

log = get_logger(__name__)


class KafkaPublisher:
    def __init__(self, bootstrap: str, topic: str) -> None:
        self.topic = topic
        # acks=all + idempotent producer: no duplicates from producer retries.
        self._p = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all", enable_idempotence=True)

    async def start(self) -> None:
        await self._p.start()

    async def stop(self) -> None:
        await self._p.stop()

    async def publish(self, key: str, value: bytes, headers: dict[str, str]) -> None:
        # Keyed by member_id so one member's events stay ordered within a partition.
        await self._p.send_and_wait(
            self.topic,
            value=value,
            key=key.encode(),
            headers=[(k, v.encode()) for k, v in headers.items()],
        )


class KafkaEventConsumer:
    def __init__(self, bootstrap: str, topic: str, group: str) -> None:
        self.topic = topic
        self._c = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._stopping = asyncio.Event()

    async def _report_lag(self) -> None:
        while not self._stopping.is_set():
            parts = list(self._c.assignment())
            if parts:
                ends = await self._c.end_offsets(parts)
                for tp in parts:
                    pos = await self._c.position(tp)
                    CONSUMER_LAG.labels(str(tp.partition)).set(max(0, ends[tp] - pos))
            await asyncio.sleep(5)

    async def run(self, handler: Handler) -> None:
        await self._c.start()
        lag_task = asyncio.create_task(self._report_lag())
        try:
            async for msg in self._c:
                headers = {k: v.decode() for k, v in (msg.headers or [])}
                await handler(QueueMessage((msg.key or b"").decode(), msg.value or b"", headers))
                # Commit only after the handler succeeded: at-least-once delivery.
                await self._c.commit({TopicPartition(msg.topic, msg.partition): msg.offset + 1})
                if self._stopping.is_set():
                    break
        finally:
            lag_task.cancel()
            await self._c.stop()

    async def stop(self) -> None:
        self._stopping.set()
