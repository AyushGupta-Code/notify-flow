"""Ingest consumer: python -m notify_flow.consumer"""

import asyncio

from prometheus_client import start_http_server

from notify_flow.config import get_settings
from notify_flow.db.session import make_engine, make_sessionmaker
from notify_flow.ingest import IngestHandler
from notify_flow.log import configure_logging, get_logger
from notify_flow.wiring import build_consumer, build_orchestrator


async def main() -> None:
    s = get_settings()
    configure_logging(s.log_level, "notify-flow-consumer")
    start_http_server(s.metrics_port)
    sm = make_sessionmaker(make_engine())
    handler = IngestHandler(sm, await build_orchestrator(s), s.queue_backend, s.engagement_wait_s)
    get_logger().info("consumer.start", backend=s.queue_backend, orchestrator=s.orchestrator)
    await build_consumer(s).run(handler)


if __name__ == "__main__":
    asyncio.run(main())
