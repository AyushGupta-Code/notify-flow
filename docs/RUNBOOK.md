# notify-flow runbook

Each alert in `deploy/prometheus/alerts.yml` links to a section here. Severity `page` means
members may miss or double-receive messages; `warning` means degraded but still delivering.

> This is a portfolio project. The runbook is written as if the service were in production,
> but it has only ever run locally, in CI (docker-compose and kind), and in the drill in
> `docs/incidents/`. Nobody has been paged by it.

## First five minutes (any alert)

1. Open the **notify-flow** Grafana dashboard (uid `notify-flow`). Check, top to bottom:
   events accepted → outbox pending → consumer lag → workflows started → sends by channel.
   The first panel that goes flat or spikes is usually where the problem is.
2. Find a failing example. Every log line is JSON with a `correlation_id`. The API returns it
   in `x-correlation-id`, it travels as a queue header and Temporal memo, and every activity
   logs it. `grep '"correlation_id": "<id>"'` across api/relay/consumer/worker logs gives
   the whole story of one event. Logs are PII-redacted, so share them freely in the incident
   channel.
3. Check Temporal (UI on :8080 locally): `WorkflowId = notify-<event_id>` shows exactly which
   step a member's workflow is on and any activity retry errors.
4. Decide: mitigate first (fallbacks, scale, pause), investigate second.

## llm-fallback-rate-high

**Meaning:** more than 20% of LLM-eligible messages over 5 min used the fallback template
(`reason` = `llm_error`, `circuit_open`, or `guardrail`). Members still receive messages.

- `llm_error` / `circuit_open` dominate → the provider is down or slow. See
  [llm-circuit-open](#llm-circuit-open).
- `guardrail` dominates → the model is producing copy that fails checks (PII, length, tone).
  Likely cause: a new prompt or model version. Roll back: set `active_prompt` for the
  campaign back to the previous version in `prompts/registry.yaml` and redeploy workers.
  Every notification row records `prompt_version` and `llm_model`, so
  `select prompt_version, copy_reason, count(*) from notifications where created_at > now() - interval '1 hour' group by 1,2`
  shows which version is failing.
- Run the offline eval against the suspect version before re-enabling it:
  `python -m evals.run_eval --provider <p> --model <m>`.

## llm-circuit-open

**Meaning:** at least one worker's breaker has been open for 2 min. Copy is coming from
templates; the LLM is not being called except one trial request every `NF_BREAKER_RESET_S`.

1. Confirm provider health (Ollama: `curl $NF_LLM_BASE_URL/api/tags`; Anthropic: status page).
2. Nothing else is required to keep members served: fallback is automatic and templates are
   reviewed copy. Do **not** restart workers to "reset" the breaker; it closes on its own
   after the next successful trial.
3. If the outage will be long, you can stop the trial calls entirely by setting
   `llm_allowed: false` on affected campaigns.
4. After recovery, check the dashboard's "Copy source" panel returns to `llm:ok`.

## consumer-lag-high

**Meaning:** more than 1000 events waiting in Kafka/SQS for 5 min. Messages are delayed, not
lost (offsets are committed / SQS messages deleted only after the workflow start succeeded).

1. Is the consumer running and logging `ingest.dispatched`? If it logs errors on Temporal
   connect, the orchestrator is the problem; see Temporal health.
2. Scale consumers. Kafka: up to the partition count (6 in compose). SQS: raise
   `NF_SQS_POLLERS` or replicas. Handling is idempotent, so over-scaling is safe.
3. A poison message does not block a partition: invalid events are counted as
   `nf_events_consumed_total{result="invalid"}` and skipped. On SQS, a message whose handler
   keeps failing moves to `member-events-dlq` after 5 receives. Replay it with the AWS CLI
   once fixed; the dedup key makes replay safe.

## outbox-backlog

**Meaning:** events were accepted by the API (and stored) but the relay is not publishing.

1. Relay logs: `relay.error` with a broker error → Kafka/SQS is unreachable.
2. The API keeps accepting events while the broker is down; they wait in `outbox`.
   Nothing is lost. When the broker is back the relay drains in order.
3. Multiple relay replicas are safe (`FOR UPDATE SKIP LOCKED`).

## send-failures

**Meaning:** more than 5% of channel-provider calls fail for 10 min.

1. Temporal retries `send_notification` (8 attempts, exponential backoff to 30 s). Each
   attempt uses the same idempotency key (`<event_id>:<step>`), so a retry after a timeout
   cannot double-send.
2. Identify the channel from `nf_send_failures_total{channel}`. If one vendor is down, there
   is no automatic channel failover today (see README "Open gaps").
3. Workflows whose sends exhausted retries show as failed in Temporal. After the vendor
   recovers, reset them to the last good point (`temporal workflow reset`); the send step
   is idempotent.

## api-errors

**Meaning:** more than 2% of API requests return 5xx.

1. `/readyz` fails → Postgres is unreachable; the API cannot accept events (by design: it
   never acknowledges an event it has not stored).
2. Producers should retry with the same `event_id`; duplicates return `200 duplicate`.

## target-down

**Meaning:** Prometheus cannot scrape a component for 2 min. Check the pod/container,
then its last log lines. Worker or consumer down means growing lag; API down means
producers are getting errors.

## Escalation

Primary on-call owns mitigation. Escalate to the platform team for Kafka/Temporal/Postgres
infrastructure, and to clinical operations if template copy itself must change (templates
for `pain_checkin` are clinically reviewed and must not be edited during an incident).
After any `page`, write a postmortem using `docs/incidents/` as the template.
