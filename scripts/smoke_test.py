"""End-to-end smoke test against a running stack (docker-compose or kind).

    python scripts/smoke_test.py --api http://localhost:8000 --expect-escalation

Creates a synthetic member, posts an event twice (duplicate), and waits for the
notification(s) to be sent. Exits non-zero on failure.
"""

import argparse
import sys
import time
import uuid

import httpx


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--timeout", type=float, default=180)
    ap.add_argument(
        "--expect-escalation",
        action="store_true",
        help="stack runs with a short NF_ENGAGEMENT_WAIT_S, expect step 2",
    )
    a = ap.parse_args()
    c = httpx.Client(base_url=a.api, timeout=10)
    deadline = time.time() + a.timeout
    while True:
        try:
            if c.get("/readyz").status_code == 200:
                break
        except httpx.HTTPError:
            pass
        if time.time() > deadline:
            print("api never became ready")
            return 1
        time.sleep(2)

    mid = f"smoke-{uuid.uuid4().hex[:8]}"
    c.put(
        f"/v1/admin/members/{mid}",
        json={
            "id": mid,
            "first_name": "Smoke",
            "email": f"{mid}@example.test",
            "phone": "+15550100",
            "timezone": "UTC",
        },
    ).raise_for_status()
    event = {
        "event_id": f"evt-{uuid.uuid4().hex}",
        "member_id": mid,
        "event_type": "session_missed",
        "payload": {},
    }
    r1 = c.post("/v1/events", json=event)
    r2 = c.post("/v1/events", json=event)
    print(
        "post #1:",
        r1.status_code,
        r1.json()["status"],
        "| post #2:",
        r2.status_code,
        r2.json()["status"],
    )
    assert r1.json()["status"] == "accepted" and r2.json()["status"] == "duplicate"

    want = 2 if a.expect_escalation else 1
    started = time.time()
    rows: list = []
    while time.time() < deadline:
        rows = [
            n for n in c.get(f"/v1/members/{mid}/notifications").json() if n["status"] == "sent"
        ]
        if len(rows) >= want:
            break
        time.sleep(1)
    for n in rows:
        print(
            f"step {n['step']} via {n['channel']}: source={n['copy_source']} "
            f"prompt={n['prompt_version']} model={n['llm_model']} "
            f"channel_model={n['channel_model_version']}"
        )
    if len(rows) < want:
        print(f"FAIL: expected {want} sent notifications, got {len(rows)}")
        return 1
    if len(rows) > want:
        print(f"FAIL: duplicate sends ({len(rows)})")
        return 1
    print(f"SMOKE OK in {time.time() - started:.1f}s after posting")
    return 0


if __name__ == "__main__":
    sys.exit(main())
