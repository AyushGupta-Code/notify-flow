"""Kafka adapter against a real broker. Skipped unless NF_KAFKA_BOOTSTRAP is set
(CI provides a Kafka service container; this machine has no broker)."""

import asyncio
import json
import os
import uuid

import pytest

from notify_flow.ingest import IngestHandler
from notify_flow.queue.kafka import KafkaEventConsumer, KafkaPublisher

from .test_aws_adapters import RecordingOrchestrator

BOOTSTRAP = os.environ.get("NF_KAFKA_BOOTSTRAP")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    pytest.mark.skipif(not BOOTSTRAP, reason="NF_KAFKA_BOOTSTRAP not set"),
]


async def test_kafka_at_least_once_with_idempotent_handler(sm):
    topic = f"member-events-{uuid.uuid4().hex[:6]}"
    pub = KafkaPublisher(BOOTSTRAP, topic)
    await pub.start()
    body = json.dumps(
        {"event_id": "evt-kafka-0001", "member_id": "m-k", "event_type": "session_missed"}
    ).encode()
    for _ in range(2):  # duplicate publish, as a retrying producer upstream might do
        await pub.publish("m-k", body, {"correlation_id": "cid-k"})
    await pub.publish("m-k", b"not json", {})  # poison message must not block
    await pub.stop()

    orch = RecordingOrchestrator()
    handler = IngestHandler(sm, orch, "kafka", 60)
    consumer = KafkaEventConsumer(BOOTSTRAP, topic, f"g-{uuid.uuid4().hex[:6]}")
    outcomes = []

    async def h(msg):
        outcomes.append(await handler.handle(msg))
        if len(outcomes) == 3:
            await consumer.stop()

    await asyncio.wait_for(consumer.run(h), timeout=60)
    assert outcomes == ["started", "duplicate", "invalid"]
    assert orch.started == {"notify-evt-kafka-0001"}
