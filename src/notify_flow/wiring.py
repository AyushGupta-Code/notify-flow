"""Factories that turn Settings into concrete adapters."""

from typing import Any

from notify_flow.config import Settings
from notify_flow.queue.base import EventConsumer, EventPublisher


def boto_client(service: str, s: Settings) -> Any:
    import boto3

    return boto3.client(service, region_name=s.aws_region, endpoint_url=s.aws_endpoint_url)  # type: ignore[call-overload]


def build_publisher(s: Settings) -> EventPublisher:
    if s.queue_backend == "kafka":
        from notify_flow.queue.kafka import KafkaPublisher

        return KafkaPublisher(s.kafka_bootstrap, s.kafka_topic)
    if s.queue_backend == "sqs":
        from notify_flow.queue.sqs import SqsPublisher

        return SqsPublisher(boto_client("sqs", s), s.sqs_queue_url)
    raise ValueError(s.queue_backend)


def build_consumer(s: Settings) -> EventConsumer:
    if s.queue_backend == "kafka":
        from notify_flow.queue.kafka import KafkaEventConsumer

        return KafkaEventConsumer(s.kafka_bootstrap, s.kafka_topic, s.kafka_group)
    if s.queue_backend == "sqs":
        from notify_flow.queue.sqs import SqsEventConsumer

        return SqsEventConsumer(boto_client("sqs", s), s.sqs_queue_url)
    raise ValueError(s.queue_backend)


async def build_orchestrator(s: Settings) -> Any:
    if s.orchestrator == "temporal":
        from temporalio.client import Client

        from notify_flow.orchestrator.temporal import TemporalOrchestrator

        client = await Client.connect(s.temporal_address, namespace=s.temporal_namespace)
        return TemporalOrchestrator(client, s.temporal_task_queue)
    if s.orchestrator == "stepfunctions":
        from notify_flow.orchestrator.stepfunctions import StepFunctionsOrchestrator

        return StepFunctionsOrchestrator(boto_client("stepfunctions", s), s.sfn_state_machine_arn)
    raise ValueError(s.orchestrator)
