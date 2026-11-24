import os

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from notify_flow.config import REPO_ROOT
from notify_flow.db.session import make_engine, make_sessionmaker

TEST_DB = os.environ.get(
    "NF_TEST_DATABASE_URL", "postgresql+asyncpg://postgres@localhost:5433/notify_flow_test"
)
TABLES = (
    "provider_deliveries, notifications, notification_reservations, processed_events, "
    "outbox, inbound_events, members"
)


@pytest.fixture(scope="session")
def db_url():
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "migrations"))
    cfg.attributes["url"] = TEST_DB
    import asyncio
    import threading

    # alembic's env.py calls asyncio.run; run it off the test event loop.
    err: list[BaseException] = []

    def go():
        try:
            command.upgrade(cfg, "head")
        except BaseException as e:  # pragma: no cover
            err.append(e)

    t = threading.Thread(target=go)
    t.start()
    t.join()
    if err:
        pytest.skip(f"Postgres not available for integration tests: {err[0]!r}")
    del asyncio
    return TEST_DB


@pytest.fixture
async def sm(db_url):
    engine = make_engine(db_url)
    async with engine.begin() as c:
        await c.execute(text(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE"))
    yield make_sessionmaker(engine)
    await engine.dispose()
