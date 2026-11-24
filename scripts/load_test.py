"""Load test: post N events through the API and measure ingest and end-to-end numbers.

    python scripts/load_test.py --api http://localhost:8000 --events 500 --concurrency 20

Reports API ingest throughput and latency, and (by reading Postgres via NF_DATABASE_URL)
how long it took until every step-1 notification was sent. Numbers depend entirely on the
machine and backends it runs against; the README records where each number came from.
"""

import argparse
import asyncio
import json
import statistics
import time
import uuid

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from notify_flow.config import get_settings


def pct(values: list[float], p: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--events", type=int, default=500)
    ap.add_argument("--members", type=int, default=100)
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument(
        "--duplicate-every", type=int, default=10, help="re-post every Nth event to exercise dedup"
    )
    ap.add_argument("--wait-timeout", type=float, default=600)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()

    run = uuid.uuid4().hex[:6]
    async with httpx.AsyncClient(base_url=a.api, timeout=30) as c:
        for i in range(a.members):
            mid = f"load-{run}-{i}"
            r = await c.put(
                f"/v1/admin/members/{mid}",
                json={
                    "id": mid,
                    "first_name": f"Member{i}",
                    "email": f"{mid}@example.test",
                    "phone": "+15550100",
                    "timezone": "UTC",
                    "program_week": 1 + i % 20,
                    "days_since_last_session": i % 30,
                },
            )
            r.raise_for_status()

        events = [
            {
                "event_id": f"evt-{run}-{i:06d}",
                "member_id": f"load-{run}-{i % a.members}",
                "event_type": "session_missed",
                "payload": {},
            }
            for i in range(a.events)
        ]
        posts = events + [events[i] for i in range(0, a.events, a.duplicate_every)]
        latencies: list[float] = []
        statuses: dict[str, int] = {}
        sem = asyncio.Semaphore(a.concurrency)

        async def post(e: dict) -> None:
            async with sem:
                t = time.perf_counter()
                r = await c.post("/v1/events", json=e)
                latencies.append(time.perf_counter() - t)
                key = r.json().get("status", str(r.status_code))
                statuses[key] = statuses.get(key, 0) + 1

        t0 = time.perf_counter()
        await asyncio.gather(*(post(e) for e in posts))
        ingest_s = time.perf_counter() - t0

    engine = create_async_engine(get_settings().database_url)
    t1 = time.perf_counter()
    sent = 0
    while time.perf_counter() - t1 < a.wait_timeout:
        async with engine.connect() as conn:
            sent = (
                await conn.execute(
                    text(
                        "select count(*) from notifications where step=1 and status='sent' "
                        "and event_id like :p"
                    ),
                    {"p": f"evt-{run}-%"},
                )
            ).scalar_one()
        if sent >= a.events:
            break
        await asyncio.sleep(0.5)
    drain_s = time.perf_counter() - t0
    async with engine.connect() as conn:
        e2e = [
            r[0]
            for r in (
                await conn.execute(
                    text(
                        "select extract(epoch from n.sent_at - e.received_at) from notifications n "
                        "join inbound_events e using (event_id) where n.step=1 and n.event_id like :p"
                    ),
                    {"p": f"evt-{run}-%"},
                )
            ).all()
        ]
        dup_sends = (
            await conn.execute(
                text(
                    "select count(*) from (select event_id, step from provider_deliveries d "
                    "join notifications n using (idempotency_key) where n.event_id like :p "
                    "group by event_id, step having count(*) > 1) x"
                ),
                {"p": f"evt-{run}-%"},
            )
        ).scalar_one()
    await engine.dispose()

    result = {
        "events": a.events,
        "posts_including_duplicates": len(posts),
        "concurrency": a.concurrency,
        "api_statuses": statuses,
        "ingest_seconds": round(ingest_s, 2),
        "ingest_posts_per_s": round(len(posts) / ingest_s, 1),
        "api_latency_ms_p50": round(statistics.median(latencies) * 1000, 1),
        "api_latency_ms_p95": round(pct(latencies, 95) * 1000, 1),
        "step1_sent": sent,
        "seconds_until_all_step1_sent": round(drain_s, 2),
        "end_to_end_events_per_s": round(sent / drain_s, 1) if drain_s else None,
        "event_to_send_s_p50": round(statistics.median(e2e), 2) if e2e else None,
        "event_to_send_s_p95": round(pct(e2e, 95), 2) if e2e else None,
        "duplicate_sends": dup_sends,
    }
    print(json.dumps(result, indent=2))
    if a.out:
        with open(a.out, "w") as f:
            json.dump(result, f, indent=2)


if __name__ == "__main__":
    asyncio.run(main())
