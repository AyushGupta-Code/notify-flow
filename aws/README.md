# AWS orchestration path

`notification.asl.json` is an executable Standard Workflow. Six Lambda entry points in
`notify_flow.aws_lambda` reuse the channel policy, personalization guardrails and fallback,
and idempotent send used by Temporal. The execution name remains `notify-<event_id>`, while
send effects remain keyed by `<event_id>:<step>`.

Deploy to LocalStack with ordinary boto3 endpoint configuration:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
python scripts/deploy_stepfunctions.py \
  --endpoint-url http://localhost:4566 \
  --database-url postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/notify_flow_test
```

Omit `--endpoint-url` to use the configured AWS account. The script reads credentials from
the standard boto3 credential chain. It never writes or supplies credentials. A real AWS
deployment also needs network reachability from Lambda to PostgreSQL and IAM permission to
create or update IAM roles, Lambda functions and the state machine.

The `stepfunctions-localstack` CI job packages the functions, deploys them to the pinned
LocalStack image, starts real state-machine executions, and checks normal delivery,
execution-name deduplication, retry-to-catch behavior, and LLM fallback. Moto remains in the
general integration suite for the SQS adapter and a fast definition-registration check.
