"""Summarize an LLM-outage drill from drill-output/drill-samples.jsonl and Postgres.

python scripts/analyze_drill.py
"""

import asyncio
import json
import statistics
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from notify_flow.config import get_settings

OUT = Path("drill-output")


def delta(a: dict, b: dict, key: str) -> dict:
    keys = set(a[key]) | set(b[key])
    return {
        k: round(b[key].get(k, 0) - a[key].get(k, 0))
        for k in sorted(keys)
        if b[key].get(k, 0) - a[key].get(k, 0)
    }


def at(samples: list[dict], t: float) -> dict:
    return min(samples, key=lambda s: abs(s["t"] - t))


async def main() -> None:
    samples = [json.loads(line) for line in (OUT / "drill-samples.jsonl").read_text().splitlines()]
    summ = json.loads((OUT / "drill-summary.json").read_text())
    p = summ["phases_s"]
    windows = {
        "baseline": (0, p["outage_start"]),
        "outage": (p["outage_start"], p["outage_end"]),
        "recovery": (p["outage_end"], p["end"]),
    }
    phases = {}
    for name, (t_a, t_b) in windows.items():
        a, b = at(samples, t_a), at(samples, t_b)
        copy = delta(a, b, "copy")
        llm_eligible = sum(v for k, v in copy.items() if not k.endswith(":policy"))
        fallback = sum(
            v for k, v in copy.items() if k.startswith("template:") and not k.endswith(":policy")
        )
        lat_n = b["llm_latency_count"] - a["llm_latency_count"]
        phases[name] = {
            "seconds": round(t_b - t_a, 1),
            "copy_by_source_reason": copy,
            "llm_outcomes": delta(a, b, "llm"),
            "fallback_ratio": round(fallback / llm_eligible, 3) if llm_eligible else None,
            "sends": round(b["sent"] - a["sent"]),
            "send_failures": round(b["send_failures"] - a["send_failures"]),
            "avg_compose_llm_latency_s": round(
                (b["llm_latency_sum"] - a["llm_latency_sum"]) / lat_n, 3
            )
            if lat_n
            else None,
        }
    opened = [s["t"] for s in samples if s["circuit"] == 2 and s["t"] >= p["outage_start"]]
    closed_after = [s["t"] for s in samples if s["circuit"] == 0 and s["t"] >= p["outage_end"]]
    circuit = {
        "first_seen_open_s_after_kill": round(opened[0] - p["outage_start"], 1) if opened else None,
        "seconds_open_or_half_open": round(sum(2 for s in samples if s["circuit"] in (1, 2))),
        "first_seen_closed_s_after_restore": round(closed_after[0] - p["outage_end"], 1)
        if closed_after
        else None,
    }

    engine = create_async_engine(get_settings().database_url)
    async with engine.connect() as c:
        pat = f"evt-drill-{summ['run']}-%"
        t0 = (
            await c.execute(
                text("select min(received_at) from inbound_events where event_id like :p"),
                {"p": pat},
            )
        ).scalar_one()
        rows = (
            await c.execute(
                text(
                    "select extract(epoch from e.received_at - :t0), n.status, n.copy_source, "
                    "extract(epoch from n.sent_at - e.received_at) from inbound_events e "
                    "left join notifications n on n.event_id = e.event_id and n.step = 1 "
                    "where e.event_id like :p"
                ),
                {"p": pat, "t0": t0},
            )
        ).all()
    await engine.dispose()
    db = {}
    for name, (t_a, t_b) in windows.items():
        sel = [r for r in rows if t_a <= float(r[0]) < t_b]
        lat = sorted(float(r[3]) for r in sel if r[3] is not None)
        db[name] = {
            "events": len(sel),
            "step1_sent": sum(r[1] == "sent" for r in sel),
            "step1_by_copy_source": {k: sum(r[2] == k for r in sel) for k in ("llm", "template")},
            "event_to_send_s_p50": round(statistics.median(lat), 2) if lat else None,
            "event_to_send_s_p95": round(lat[int(0.95 * (len(lat) - 1))], 2) if lat else None,
        }
    report = {"drill": summ, "metrics_by_phase": phases, "circuit": circuit, "db_by_phase": db}
    (OUT / "drill-report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
