# Changelog

## Unreleased

### Step Functions execution path

- Added six deployable Lambda handlers for channel selection, quiet-hours calculation,
  personalization with guarded fallback, idempotent send, engagement lookup and escalation.
- Replaced the design-only ASL with an executable workflow containing task timeouts,
  jittered retries, catch routes, durable waits and engagement choices.
- Added an endpoint-configured deployment script plus real-execution LocalStack tests and a
  dedicated GitHub Actions job, now verified in CI.

### Vendor failover

- Added automatic fallback to the plan's next eligible channel when a provider reports a
  definitive pre-send outage. Ambiguous post-send failures continue retrying the original
  idempotency key, preventing failover from creating duplicate deliveries.

### Member frequency caps

- Added atomic, durable rolling-24-hour reservations across campaigns. The default admits
  ten distinct notification events per member; duplicate planning is idempotent and
  concurrent workflows serialize on the member row.
