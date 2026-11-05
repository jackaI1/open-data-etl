# Watching catalogue runs

I want to know if a scheduled download failed, if a snapshot was rejected, or if
processing slowed down. The pipeline uses Datadog's official Python client to
send aggregate measurements to DogStatsD, the local receiver in the Datadog
Agent. Nothing is sent until `DPD_DATADOG_ENABLED=true` is set.

## Try it without an account

After the [local setup](pipeline.md#local-setup), run:

```sh
make monitor-demo
```

This processes the included 60-product sample, verifies the database, then blanks
the sample's brand names and runs it again. The second run must exit with code 2
and leave no tables or database. Both runs use the actual Datadog client over
UDP, a small network packet format, to an ephemeral port on this computer.
The receiver checks the outcome and quality-check packets, writes them to
`data/reports/monitoring-demo.json`, then closes. The sample copies and both
warehouses are temporary and removed even if an assertion fails. No receiver,
background thread, container or server remains running.

This proves client encoding and delivery to a local socket. It does **not** prove
Datadog cloud ingestion, dashboard rendering or alerts. There is no cloud
account or API key in the demo, tests or CI.

## Send to an existing Datadog Agent

On a computer with the Agent already configured, this takes two commands:

```sh
export DPD_DATADOG_ENABLED=true DD_AGENT_HOST=127.0.0.1 DD_DOGSTATSD_PORT=8125 DD_ENV=local DD_VERSION=dev
make fixture
```

The Agent needs DogStatsD enabled on UDP port 8125. An API key belongs in the
Agent's private configuration, never in this repository or the pipeline.
Use [Datadog's Agent installation instructions](https://docs.datadoghq.com/agent/)
if an Agent is not installed. This requires a Datadog account and may incur
custom-metric charges. I have tested the local receiver path, not a paid account.
For a weekly run on your own persistent machine, use the same variables with
`make pipeline`. The repository's GitHub-hosted weekly runner has no Agent and
keeps monitoring off; setting the flag there alone will not deliver anything.

If the pipeline runs outside the Agent's container, publish only
`127.0.0.1:8125:8125/udp` on that host and enable the Agent's
`DD_DOGSTATSD_NON_LOCAL_TRAFFIC=true` inside its container. Do not expose UDP 8125
to the Internet. For a remote Agent use a trusted private network or tunnel;
DogStatsD UDP is neither authenticated nor encrypted.

After a run, use `datadog-agent status` to inspect DogStatsD packet counts and
parse errors. In Datadog's Metrics Explorer, search `dpd_etl.products` with
`service:open-data-etl` and your environment. Import
[`monitoring/dashboard.json`](../monitoring/dashboard.json) through a dashboard's
**Configure > Import dashboard JSON** menu. It shows completed commands, timing,
product counts and failed quality checks. Select a time range covering the run;
weekly runs produce sparse data, not a continuous heartbeat.

For alerts, create a **Service Check** monitor for `dpd_etl.run`, filtered by
`service:open-data-etl` and your environment, grouped by `command`. Alert on one
CRITICAL result and recover on one OK result. Use `dpd_etl.quality_gate` for
quality-only failures. To catch a weekly job that never starts, configure a
separate absence-of-data alert covering at least eight days. A short no-data
window would page between normal weekly runs. These setup instructions do not
create or send alerts automatically.

## Measurements and boundaries

| Name | Type | Meaning |
| --- | --- | --- |
| `dpd_etl.runs` | Counter | One completion, tagged with `command` and `outcome` |
| `dpd_etl.duration_ms` | Timing | Wall-clock milliseconds for the command, including cleanup |
| `dpd_etl.products` | Gauge | Product count after successful table and database writes |
| `dpd_etl.quality.observed` | Gauge | Observed aggregate value for each allowlisted `check` |
| `dpd_etl.quality.failed` | Gauge | 0 for a passing check, 1 for a failed check |
| `dpd_etl.run` | Service check | 0 (OK) for success, 2 (CRITICAL) for rejection or error |
| `dpd_etl.quality_gate` | Service check | 0 if all quality checks pass, 2 otherwise |

A gauge is the latest measured value. A counter records occurrences. The Agent
aggregates timings into histogram series such as `dpd_etl.duration_ms.avg`.
The dashboard uses that average, not an invented metric name. Extract and
transform are separate commands: `make pipeline` stops if extract fails, so a
failed download produces only an extract failure. Later `make insights` queries
are outside the monitored commands. Direct callers of `run()` can pass a
`Monitor`; the CLI configures and closes it automatically.

Tags are limited to `service:open-data-etl`, `dataset:marketed-dpd`, short `env`
and `version` labels, fixed command/outcome values and known check names.
`DD_ENV` defaults to `local`, and `DD_VERSION` to `dev`; both accept at most 64
lowercase letters, digits, dots, underscores or hyphens, beginning with a letter
or digit. Use a release label, not a unique value per row or run. Dates, product
IDs, file paths, exception text and source records are excluded. The client
replaces inherited `DATADOG_TAGS`, `DD_SERVICE` and entity tags with this allowlist.
If `DD_EXTERNAL_ENV` is set, opt-in monitoring rejects it before processing;
that SDK option would add external environment metadata beyond the allowlist.
Do not put personal information in deployment labels. The Agent can add its own
host tags, which are outside this client's control.

Monitoring runs only in the Spark driver, the process coordinating the work,
so workers do not send duplicate measurements. The client sends synchronously
without buffering or background sender threads and closes its socket in
`finally`. UDP has no delivery acknowledgement: an unavailable Agent can silently
lose packets, and successful local sends do not guarantee cloud delivery.
Telemetry exceptions never change the original pipeline outcome. Invalid opt-in
configuration is rejected before processing. A force-killed process cannot send
its final result. Local JSON reports and nonzero exits remain the reliable failure
record; metrics supplement them.

## Verification

`make lint test monitor-demo` exercises real packets for successful and rejected
snapshots, extract errors, fractions and zero values, safe tags, timing units,
disabled collection, invalid configuration, closed receivers, and an unavailable
Agent. An injected transport exception verifies that monitoring cannot hide the
pipeline's original error. CI runs the same demo and uploads its captured report.

References: [DogStatsD](https://docs.datadoghq.com/extend/dogstatsd/),
[Python client](https://github.com/DataDog/datadogpy),
[service-check status codes](https://docs.datadoghq.com/extend/service_checks/dogstatsd_service_checks_submission/).
