# Runbook: stopping a consumer

**When to use this.** A consumer is producing bad effects fast enough that stopping intake
is safer than letting the backoff schedule and DLQ absorb them. Examples: a version-guard
regression about to be deployed twice, or a provider outage the breaker has not opened
for yet. Stopping the mapping halts new receives. Messages already in flight still
finish, and nothing on the queue is lost: it waits.

## Stop (the durable way)

Set `enabled = false` on the consumer's module block and apply:

```hcl
module "ledger" {
  source  = "github.com/<org>/factbus//iac/modules/consumer"
  # ...
  enabled = false
}
```

Do it in IaC. A switch thrown only with the CLI is turned back on by the next apply.

## Stop (the fast way, then follow with IaC)

```bash
aws lambda update-event-source-mapping --uuid "$(tofu output -raw ledger_mapping_uuid)" --no-enabled
```

The UUID is the module's `mapping_uuid` output. Re-enable with `--enabled`, or set
`enabled = true` and apply.

## While it is stopped

The queue keeps messages for its retention (14 days by default) and resumes where the
poller left off. Watch the `<name>-age` alarm. Once the oldest message passes
`age_alarm_threshold_seconds` the alarm fires, which is the reminder that a consumer is
still stopped.

## Not covered

No alarm fires on the mapping's disabled state itself. The age alarm is the proxy. A
queue with no traffic while stopped raises nothing.
