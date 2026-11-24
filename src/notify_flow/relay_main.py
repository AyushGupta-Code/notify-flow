"""Outbox relay process: python -m notify_flow.relay_main"""

import asyncio

from prometheus_client import start_http_server

from notify_flow.config import get_settings
from notify_flow.db.session import make_engine, make_sessionmaker
from notify_flow.log import configure_logging, get_logger
from notify_flow.relay import OutboxRelay
from notify_flow.wiring import build_publisher


async def main() -> None:
    s = get_settings()
    configure_logging(s.log_level, "notify-flow-relay")
    start_http_server(s.metrics_port)
    pub = build_publisher(s)
    await pub.start()
    get_logger().info("relay.start", backend=s.queue_backend)
    try:
        await OutboxRelay(make_sessionmaker(make_engine()), pub).run()
    finally:
        await pub.stop()


if __name__ == "__main__":
    asyncio.run(main())
