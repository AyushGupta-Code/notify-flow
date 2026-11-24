"""Temporal worker: python -m notify_flow.worker"""

import asyncio
import os

from prometheus_client import start_http_server
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio.client import Client
from temporalio.worker import Worker

from notify_flow.channel.policy import ChannelPolicy
from notify_flow.config import Settings, get_settings
from notify_flow.db.session import make_engine, make_sessionmaker
from notify_flow.llm.personalize import Personalizer
from notify_flow.llm.providers import build_provider
from notify_flow.llm.registry import load_registry
from notify_flow.llm.resilience import CircuitBreaker
from notify_flow.log import configure_logging, get_logger
from notify_flow.sender import DbFakeChannelProvider
from notify_flow.workflows.activities import NotificationActivities
from notify_flow.workflows.notification import MemberNotificationWorkflow


def build_activities(
    s: Settings, sm: async_sessionmaker[AsyncSession] | None = None
) -> NotificationActivities:
    sm = sm or make_sessionmaker(make_engine(s.database_url))
    provider = build_provider(
        s.llm_provider,
        s.llm_model,
        s.llm_base_url,
        s.anthropic_api_key,
        mock_fail_rate=float(os.environ.get("NF_MOCK_LLM_FAIL_RATE", "0")),
    )
    personalizer = Personalizer(
        load_registry(s.prompt_registry_path),
        provider,
        CircuitBreaker(s.breaker_failure_threshold, s.breaker_reset_s),
        timeout_s=s.llm_timeout_s,
        max_attempts=s.llm_max_attempts,
        backoff_base_s=s.llm_backoff_base_s,
    )
    return NotificationActivities(
        sm,
        personalizer,
        ChannelPolicy.from_path(s.channel_model_path),
        DbFakeChannelProvider(sm),
        s,
    )


async def main() -> None:
    s = get_settings()
    configure_logging(s.log_level, "notify-flow-worker")
    start_http_server(s.metrics_port)
    client = await Client.connect(s.temporal_address, namespace=s.temporal_namespace)
    acts = build_activities(s)
    get_logger().info(
        "worker.start",
        task_queue=s.temporal_task_queue,
        llm_provider=s.llm_provider,
        llm_model=s.llm_model,
    )
    await Worker(
        client,
        task_queue=s.temporal_task_queue,
        workflows=[MemberNotificationWorkflow],
        activities=acts.all(),
    ).run()


if __name__ == "__main__":
    asyncio.run(main())
