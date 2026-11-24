#!/usr/bin/env bash
# Docker-free local stack, used for the load test and the LLM-outage drill on a machine
# without Docker. Expects: Postgres on $PGPORT (default 5433) with database notify_flow, and
# a Temporal dev server on 7233 (python scripts/local_temporal.py). Uses moto server as the
# SQS endpoint and scripts/fake_ollama.py as the LLM. WORKERS=n starts n Temporal workers
# (metrics on 9101, 9111, ...); NF_SQS_POLLERS=n sets consumer concurrency.
#   scripts/run_local_stack.sh start|stop
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=drill-output; mkdir -p "$OUT"
PY=.venv/bin/python
export AWS_ACCESS_KEY_ID=testing AWS_SECRET_ACCESS_KEY=testing AWS_DEFAULT_REGION=us-east-1
export NF_DATABASE_URL="postgresql+asyncpg://postgres@localhost:${PGPORT:-5433}/notify_flow"
export NF_QUEUE_BACKEND=sqs NF_AWS_ENDPOINT_URL=http://localhost:5055
export NF_SQS_QUEUE_URL=http://localhost:5055/123456789012/member-events
export NF_LLM_PROVIDER=ollama NF_LLM_MODEL=fake-ollama NF_LLM_BASE_URL=http://localhost:11435
export NF_LLM_TIMEOUT_S=${NF_LLM_TIMEOUT_S:-2} NF_ENGAGEMENT_WAIT_S=${NF_ENGAGEMENT_WAIT_S:-20}
export NF_QUIET_HOURS_ENABLED=false NF_LOG_LEVEL=INFO

start() {
  setsid nohup .venv/bin/moto_server -p 5055 > $OUT/moto.log 2>&1 & echo $! > $OUT/moto.pid
  sleep 2
  $PY - <<'PY'
import boto3, json
s = boto3.client("sqs", endpoint_url="http://localhost:5055", region_name="us-east-1")
dlq = s.create_queue(QueueName="member-events-dlq")["QueueUrl"]
arn = s.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
s.create_queue(QueueName="member-events", Attributes={"VisibilityTimeout": "30",
    "RedrivePolicy": json.dumps({"deadLetterTargetArn": arn, "maxReceiveCount": "5"})})
print("queues ready")
PY
  .venv/bin/alembic upgrade head
  setsid nohup .venv/bin/uvicorn --app-dir scripts fake_ollama:app --port 11435 > $OUT/llm.log 2>&1 & echo $! > $OUT/llm.pid
  setsid nohup .venv/bin/uvicorn notify_flow.api.app:app --port 8000 --workers 2 > $OUT/api.log 2>&1 & echo $! > $OUT/api.pid
  NF_METRICS_PORT=9102 setsid nohup $PY -m notify_flow.relay_main > $OUT/relay.log 2>&1 & echo $! > $OUT/relay.pid
  NF_METRICS_PORT=9103 setsid nohup $PY -m notify_flow.consumer > $OUT/consumer.log 2>&1 & echo $! > $OUT/consumer.pid
  for i in $(seq 1 "${WORKERS:-1}"); do
    NF_METRICS_PORT=$((9100 + 10 * i - 9)) setsid nohup $PY -m notify_flow.worker > $OUT/worker$i.log 2>&1 & echo $! > $OUT/worker$i.pid
  done
  sleep 4; echo "stack started; logs in $OUT/"
}

stop() {
  for p in worker1 worker2 worker3 worker4 consumer relay api llm moto; do
    [ -f $OUT/$p.pid ] && kill "$(cat $OUT/$p.pid)" 2>/dev/null || true
    rm -f $OUT/$p.pid
  done
  echo stopped
}

"${1:-start}"
