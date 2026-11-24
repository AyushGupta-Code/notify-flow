"""LLM-outage drill against the local stack (scripts/run_local_stack.sh).

Sends steady traffic, kills the fake LLM server, restores it, and samples the worker's
Prometheus metrics every 2 s. Writes drill-output/drill-samples.jsonl and a summary.

    python scripts/drill_llm_outage.py --rate 4 --baseline 60 --outage 180 --recovery 90
"""

import argparse
import asyncio
import json
import os
import signal
import subprocess
import time
import uuid
from pathlib import Path

import httpx
from prometheus_client.parser import text_string_to_metric_families

OUT = Path("drill-output")
WORKER_METRICS = "http://localhost:9101/metrics"


def scrape(text: str) -> dict:
    s = {
        "copy": {},
        "llm": {},
        "sent": 0.0,
        "send_failures": 0.0,
        "circuit": None,
        "llm_latency_sum": 0.0,
        "llm_latency_count": 0.0,
    }
    for fam in text_string_to_metric_families(text):
        for smp in fam.samples:
            n, lab, v = smp.name, smp.labels, smp.value
            if n == "nf_copy_generated_total":
                k = f"{lab['source']}:{lab['reason']}"
                s["copy"][k] = s["copy"].get(k, 0) + v
            elif n == "nf_llm_requests_total":
                s["llm"][lab["outcome"]] = s["llm"].get(lab["outcome"], 0) + v
            elif n == "nf_notifications_sent_total":
                s["sent"] += v
            elif n == "nf_send_failures_total":
                s["send_failures"] += v
            elif n == "nf_llm_circuit_state":
                s["circuit"] = v
            elif n == "nf_llm_latency_seconds_sum":
                s["llm_latency_sum"] += v
            elif n == "nf_llm_latency_seconds_count":
                s["llm_latency_count"] += v
    return s


def start_llm() -> subprocess.Popen:
    return subprocess.Popen(
        [".venv/bin/uvicorn", "--app-dir", "scripts", "fake_ollama:app", "--port", "11435"],
        stdout=open(OUT / "llm.log", "a"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--rate", type=float, default=4.0, help="events per second")
    ap.add_argument("--baseline", type=int, default=60)
    ap.add_argument("--outage", type=int, default=180)
    ap.add_argument("--recovery", type=int, default=90)
    a = ap.parse_args()
    run = uuid.uuid4().hex[:6]
    total = a.baseline + a.outage + a.recovery
    samples_path = OUT / "drill-samples.jsonl"
    samples_path.write_text("")
    phases: dict[str, float] = {}

    async with httpx.AsyncClient(base_url=a.api, timeout=10) as c:
        for i in range(50):
            mid = f"drill-{run}-{i}"
            await c.put(
                f"/v1/admin/members/{mid}",
                json={
                    "id": mid,
                    "first_name": f"Drill{i}",
                    "email": f"{mid}@example.test",
                    "phone": "+15550100",
                    "timezone": "UTC",
                },
            )
        t0 = time.time()
        stop = asyncio.Event()
        posted = {"n": 0, "errors": 0}

        async def traffic() -> None:
            i = 0
            while not stop.is_set():
                e = {
                    "event_id": f"evt-drill-{run}-{i:06d}",
                    "member_id": f"drill-{run}-{i % 50}",
                    "event_type": "session_missed",
                }
                try:
                    r = await c.post("/v1/events", json=e)
                    posted["n"] += r.status_code == 202
                except httpx.HTTPError:
                    posted["errors"] += 1
                i += 1
                await asyncio.sleep(1 / a.rate)

        async def sampler() -> None:
            async with httpx.AsyncClient(timeout=5) as m:
                while not stop.is_set():
                    try:
                        s = scrape((await m.get(WORKER_METRICS)).text)
                        s["t"] = round(time.time() - t0, 1)
                        with samples_path.open("a") as f:
                            f.write(json.dumps(s) + "\n")
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(2)

        tasks = [asyncio.create_task(traffic()), asyncio.create_task(sampler())]
        await asyncio.sleep(a.baseline)
        pid = int((OUT / "llm.pid").read_text())
        os.killpg(os.getpgid(pid), signal.SIGTERM)  # inject: LLM down
        phases["outage_start"] = round(time.time() - t0, 1)
        print(f"[{phases['outage_start']}s] LLM killed", flush=True)
        await asyncio.sleep(a.outage)
        llm = start_llm()
        (OUT / "llm.pid").write_text(str(llm.pid))
        phases["outage_end"] = round(time.time() - t0, 1)
        print(f"[{phases['outage_end']}s] LLM restored", flush=True)
        await asyncio.sleep(a.recovery)
        stop.set()
        await asyncio.gather(*tasks)
        phases["end"] = round(time.time() - t0, 1)
        await asyncio.sleep(30)  # let in-flight step-1 sends finish

    summary = {
        "run": run,
        "rate_per_s": a.rate,
        "phases_s": phases,
        "events_accepted": posted["n"],
        "post_errors": posted["errors"],
        "planned_duration_s": total,
    }
    (OUT / "drill-summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
