# Changelog

## Unreleased

### Step Functions execution path

- Added six deployable Lambda handlers for channel selection, quiet-hours calculation,
  personalization with guarded fallback, idempotent send, engagement lookup and escalation.
- Replaced the design-only ASL with an executable workflow containing task timeouts,
  jittered retries, catch routes, durable waits and engagement choices.
- Added an endpoint-configured deployment script plus real-execution LocalStack tests and a
  dedicated GitHub Actions job. Verification remains pending until that job runs.
