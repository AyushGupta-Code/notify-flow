"""SQS adapter (boto3, run in a thread). Tested against moto; LocalStack wiring is in
docker-compose under the "aws" profile."""

import asyncio
from typing import Any

from notify_flow.log import get_logger
from notify_flow.metrics import CONSUMER_LAG
from notify_flow.queue.base import Handler, QueueMessage

log = get_logger(__name__)


class SqsPublisher:
    def __init__(self, client: Any, queue_url: str) -> None:
        self.sqs = client
        self.url = queue_url

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish(self, key: str, value: bytes, headers: dict[str, str]) -> None:
        attrs = {k: {"DataType": "String", "StringValue": v} for k, v in headers.items()}
        attrs["member_id"] = {"DataType": "String", "StringValue": key}
        await asyncio.to_thread(
            self.sqs.send_message,
            QueueUrl=self.url,
            MessageBody=value.decode(),
            MessageAttributes=attrs,
        )


class SqsEventConsumer:
    def __init__(
        self, client: Any, queue_url: str, wait_s: int = 10, batch: int = 10, pollers: int = 1
    ) -> None:
        self.sqs = client
        self.url = queue_url
        self.wait_s = wait_s
        self.batch = batch
        # Standard SQS queues are unordered, and the handler is idempotent, so messages in a
        # batch and several poll loops can be handled concurrently.
        self.pollers = pollers
        self._stopping = asyncio.Event()

    async def poll_once(self, handler: Handler) -> int:
        resp = await asyncio.to_thread(
            self.sqs.receive_message,
            QueueUrl=self.url,
            MaxNumberOfMessages=self.batch,
            WaitTimeSeconds=self.wait_s,
            MessageAttributeNames=["All"],
        )

        async def one(m: dict[str, Any]) -> bool:
            attrs = {k: v["StringValue"] for k, v in m.get("MessageAttributes", {}).items()}
            try:
                await handler(QueueMessage(attrs.pop("member_id", ""), m["Body"].encode(), attrs))
            except Exception:
                # Leave it: it becomes visible again after the visibility timeout and moves
                # to the DLQ after maxReceiveCount (redrive policy on the queue).
                log.exception("sqs.handler_failed")
                return False
            await asyncio.to_thread(
                self.sqs.delete_message, QueueUrl=self.url, ReceiptHandle=m["ReceiptHandle"]
            )
            return True

        handled = sum(await asyncio.gather(*(one(m) for m in resp.get("Messages", []))))
        attrs = await asyncio.to_thread(
            self.sqs.get_queue_attributes,
            QueueUrl=self.url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        CONSUMER_LAG.labels("sqs").set(int(attrs["Attributes"]["ApproximateNumberOfMessages"]))
        return handled

    async def run(self, handler: Handler) -> None:
        async def loop() -> None:
            while not self._stopping.is_set():
                await self.poll_once(handler)

        await asyncio.gather(*(loop() for _ in range(self.pollers)))

    async def stop(self) -> None:
        self._stopping.set()
