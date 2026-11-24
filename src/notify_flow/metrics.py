"""Prometheus metrics shared by the API, the ingest consumer and the Temporal worker."""

from prometheus_client import Counter, Gauge, Histogram

EVENTS_INGESTED = Counter(
    "nf_events_ingested_total", "Member events accepted by the API", ["event_type", "result"]
)
EVENTS_CONSUMED = Counter(
    "nf_events_consumed_total", "Member events read from the queue", ["backend", "result"]
)
OUTBOX_PUBLISHED = Counter("nf_outbox_published_total", "Outbox rows published to the queue")
OUTBOX_PENDING = Gauge("nf_outbox_pending", "Outbox rows not yet published")
CONSUMER_LAG = Gauge("nf_consumer_lag_messages", "Queue messages not yet consumed", ["partition"])
WORKFLOWS_STARTED = Counter(
    "nf_workflows_started_total", "Notification workflows started", ["orchestrator", "result"]
)
NOTIFICATIONS_SENT = Counter(
    "nf_notifications_sent_total",
    "Notifications handed to a channel provider",
    ["channel", "step", "copy_source"],
)
SEND_DUPLICATES_SUPPRESSED = Counter(
    "nf_send_duplicates_suppressed_total", "Send attempts skipped because the key was already sent"
)
SEND_FAILURES = Counter("nf_send_failures_total", "Channel provider send failures", ["channel"])
VENDOR_FAILOVERS = Counter(
    "nf_vendor_failovers_total",
    "Definitive pre-send provider failures routed to a fallback channel",
    ["from_channel", "to_channel"],
)
CHANNEL_DECISIONS = Counter(
    "nf_channel_decisions_total", "Channel decisions", ["source", "model_version", "channel"]
)
LLM_REQUESTS = Counter(
    "nf_llm_requests_total", "LLM calls by outcome", ["provider", "model", "outcome"]
)
LLM_LATENCY = Histogram(
    "nf_llm_latency_seconds",
    "LLM call latency including retries",
    ["provider"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 16),
)
COPY_GENERATED = Counter("nf_copy_generated_total", "Message copy by source", ["source", "reason"])
BREAKER_STATE = Gauge(
    "nf_llm_circuit_state", "LLM circuit breaker state (0 closed, 1 half-open, 2 open)"
)
END_TO_END_LATENCY = Histogram(
    "nf_event_to_send_seconds",
    "Time from event occurred_at to first send",
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300, 3600, 86400),
)
