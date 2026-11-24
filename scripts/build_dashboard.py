"""Generate deploy/grafana/dashboards/notify-flow.json (python scripts/build_dashboard.py)."""

import json
from pathlib import Path

PANELS = [
    (
        "Events accepted / duplicate (req/s)",
        "sum by (result) (rate(nf_events_ingested_total[1m]))",
        "reqps",
    ),
    ("Outbox pending", "max(nf_outbox_pending)", "short"),
    ("Consumer lag (messages)", "sum by (partition) (nf_consumer_lag_messages)", "short"),
    ("Workflows started", "sum by (result) (rate(nf_workflows_started_total[1m]))", "ops"),
    ("Sends by channel", "sum by (channel, step) (rate(nf_notifications_sent_total[1m]))", "ops"),
    (
        "Copy source (LLM vs template)",
        "sum by (source, reason) (rate(nf_copy_generated_total[1m]))",
        "ops",
    ),
    (
        "LLM fallback ratio",
        'sum(rate(nf_copy_generated_total{source="template",reason=~"llm_error|circuit_open|guardrail"}[5m])) / sum(rate(nf_copy_generated_total{reason!="policy"}[5m]))',
        "percentunit",
    ),
    (
        "LLM circuit state (0 closed, 1 half-open, 2 open)",
        "max by (component) (nf_llm_circuit_state)",
        "short",
    ),
    ("LLM latency p50 / p95", None, "s"),
    ("LLM outcomes", "sum by (outcome) (rate(nf_llm_requests_total[1m]))", "ops"),
    (
        "Channel decisions by source / model version",
        "sum by (source, model_version) (rate(nf_channel_decisions_total[5m]))",
        "ops",
    ),
    ("Duplicate sends suppressed / send failures", None, "ops"),
    (
        "Event to first send p95",
        "histogram_quantile(0.95, sum by (le) (rate(nf_event_to_send_seconds_bucket[5m])))",
        "s",
    ),
    (
        "API p95 latency by route",
        "histogram_quantile(0.95, sum by (le, route) (rate(nf_http_request_seconds_bucket[5m])))",
        "s",
    ),
]
MULTI = {
    "LLM latency p50 / p95": [
        ("p50", "histogram_quantile(0.5, sum by (le) (rate(nf_llm_latency_seconds_bucket[5m])))"),
        ("p95", "histogram_quantile(0.95, sum by (le) (rate(nf_llm_latency_seconds_bucket[5m])))"),
    ],
    "Duplicate sends suppressed / send failures": [
        ("suppressed", "sum(rate(nf_send_duplicates_suppressed_total[5m]))"),
        ("failures", "sum(rate(nf_send_failures_total[5m]))"),
    ],
}


def main() -> None:
    panels = []
    for i, (title, expr, unit) in enumerate(PANELS):
        targets = (
            [
                {"expr": e, "legendFormat": leg, "refId": chr(65 + j)}
                for j, (leg, e) in enumerate(MULTI[title])
            ]
            if expr is None
            else [{"expr": expr, "refId": "A"}]
        )
        panels.append(
            {
                "id": i + 1,
                "title": title,
                "type": "timeseries",
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "gridPos": {"h": 8, "w": 12, "x": (i % 2) * 12, "y": (i // 2) * 8},
                "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
                "targets": targets,
            }
        )
    dash = {
        "uid": "notify-flow",
        "title": "notify-flow",
        "schemaVersion": 39,
        "version": 1,
        "time": {"from": "now-30m", "to": "now"},
        "refresh": "10s",
        "tags": ["notify-flow"],
        "panels": panels,
    }
    out = Path("deploy/grafana/dashboards/notify-flow.json")
    out.write_text(json.dumps(dash, indent=2) + "\n")
    print(f"wrote {out} with {len(panels)} panels")


if __name__ == "__main__":
    main()
