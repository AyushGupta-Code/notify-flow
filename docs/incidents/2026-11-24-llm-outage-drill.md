# Postmortem: LLM provider outage (injected drill)

| | |
|---|---|
| Date | 2026-11-24 |
| Type | **Drill.** Fault injected on purpose against the docker-free local stack. No real members, no real pager. |
| Duration of fault | 180 s (LLM server killed at t=60 s, restored at t=240 s) |
| Member impact | None missed. 697 of 697 step-1 messages during the outage were sent, all with fallback template copy. |
| Raw data | [`data/2026-11-24-llm-outage-drill-report.json`](data/2026-11-24-llm-outage-drill-report.json), [`samples.jsonl`](data/2026-11-24-llm-outage-drill-samples.jsonl) |

## Setup

- Stack: `scripts/run_local_stack.sh` with Postgres 16, Temporal dev server (in-memory, via the
  Python SDK), moto server as SQS, 4 SQS pollers, 1 Temporal worker, and
  `scripts/fake_ollama.py` as the LLM (Ollama API, ~150 ms per call). `NF_LLM_PROVIDER=ollama`,
  `NF_LLM_TIMEOUT_S=2`, 3 attempts, breaker threshold 5, reset 30 s.
  `NF_ENGAGEMENT_WAIT_S=20`, so every event also produces an escalation message 20 s later.
- Load: `scripts/drill_llm_outage.py --rate 4 --baseline 60 --outage 180 --recovery 90`,
  4 `session_missed` events per second across 50 synthetic members.
- Fault: SIGTERM to the fake LLM server's process group (connection refused), then restart.
- Observation: the drill script scraped the worker's `/metrics` every 2 s (no Prometheus
  server was running on this machine). Per-event outcomes came from Postgres.
  Analysis: `scripts/analyze_drill.py`.

## Timeline (seconds from start of traffic)

| t | Event |
|---|---|
| 0 | Traffic starts at 4 events/s. All copy from the LLM (`llm:ok`), average compose latency 0.153 s. |
| 60.0 | LLM server killed. |
| 62.2 | First sample with `nf_llm_circuit_state = 2` (open), 2.2 s after the kill. |
| 62–240 | Every compose returns template copy, `reason=circuit_open` (1,400 messages). Compose latency drops to 0.003 s because the breaker short-circuits. Sends continue, 0 send failures. |
| ~190 | `NotifyFlowLLMCircuitOpen` would have fired (open > 2 min). Verified by replaying the samples through the real rule with promtool (`deploy/prometheus/alerts_drill_test.yml`, runs in CI). |
| 240.0 | LLM server restarted. |
| 242.9 | First sample with the breaker closed (2.9 s after restore). LLM copy resumes. |
| 331 | Traffic stops. |

## What we measured

| Phase | Events | Step-1 sent | Copy source (step 1) | Event→send p50 / p95 | Send failures |
|---|---|---|---|---|---|
| Baseline (60 s) | 232 | 232 | 232 LLM | 0.39 s / 0.49 s | 0 |
| Outage (180 s) | 697 | 697 | 697 template | 0.26 s / 0.36 s | 0 |
| Recovery (91 s) | 348 | 348 | 346 LLM, 2 template | 0.41 s / 0.51 s | 0 |

Totals: 1,277 events accepted, 0 API errors, 1,277 step-1 notifications sent, plus
escalations (worker counter: 385 + 1,399 + 682 sends across the three phases).

## What went well

- **Graceful degradation worked as designed.** No workflow failed, no message was dropped,
  and nobody had to do anything. The template is reviewed copy, so members got a correct,
  if generic, message.
- **The breaker stopped the retry storm.** Without it, each compose would spend 3 attempts
  with backoff (~0.6 s+) against a dead endpoint. With it, compose took 3 ms and the event→send
  latency actually went *down* during the outage.
- **Recovery was automatic** within one half-open trial (2.9 s after restore).
- **Every message is attributable.** `copy_source`/`copy_reason` on each notification row
  made the per-phase table above a single SQL query.

## What we learned (and changed)

1. **`llm_error` never appeared; everything was `circuit_open`.** Under concurrency, the first
   handful of in-flight compose calls each failed one attempt and together reached the
   threshold of 5 within ~2 s, so their retries hit the open breaker. Someone reading the
   dashboard during a real incident might wonder why there are "no errors". *Action:* the
   runbook's [llm-circuit-open](../RUNBOOK.md#llm-circuit-open) section now says that
   `circuit_open` is the expected signature of a provider outage.
2. **The fallback-rate alert would not have fired.** It uses a 5-minute rate window and
   `for: 5m`, so a 3-minute outage never satisfies it. That is acceptable, because the
   circuit alert caught the outage, but it means the fallback alert is really a slow-burn
   alert (for example, a prompt version that starts failing guardrails). This is now encoded in the
   promtool replay test as an explicit expectation.
3. **The circuit alert could have flapped.** It was `max(nf_llm_circuit_state) == 2`. The
   breaker goes half-open (1) for a moment every 30 s while the provider is still down; a scrape that
   lands in that moment would reset the alert's `for: 2m` timer. The 2 s sampling in this drill
   never caught a half-open value, but nothing guarantees that. *Action:* changed to `>= 1`
   and added a half-open point to the synthetic promtool test.

## Limits of this drill

- One worker process, so one breaker. With N workers, each has its own breaker and each
  sends its own trial calls; the alert uses `max()` across them.
- The fake LLM fails fast (connection refused). A provider that hangs would hit the 2 s
  timeout instead, and the latency picture during the first seconds would differ. Not tested.
- No Prometheus or Alertmanager was running; "would have fired" comes from promtool replay.
- This was a planned drill on a laptop, not a real incident with real stakes, so it says nothing about
  how I handle being paged at 3 a.m.
