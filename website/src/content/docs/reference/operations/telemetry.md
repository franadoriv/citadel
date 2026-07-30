---
title: Telemetry
description: Capture redacted Citadel incidents locally and optionally deliver them through Sentry or Bugsink.
---

Citadel always records redacted server failures and unexpected process panics
locally. The authenticated dashboard's **Error Journal** groups recurring
incidents by fingerprint and reads them from `citadel-errors.jsonl` beside the
server executable.

Sentry telemetry is optional. When enabled, Citadel uses the Sentry SDK to send
redacted incident metadata to a Sentry-compatible DSN. Local capture does not
depend on the network: an invalid, absent, or unavailable telemetry endpoint
never prevents the server from starting.

## Configure Sentry

Set the DSN and, optionally, an environment label before starting the server:

```bash
export CITADEL_SENTRY_DSN='https://<public-key>@o0.ingest.sentry.io/<project-id>'
export CITADEL_ENVIRONMENT='production'
citadel serve
```

`CITADEL_SENTRY_DSN` is intentionally an environment variable rather than a
`citadel.toml` field, so the DSN stays out of the config browser and normal
configuration files. `CITADEL_ENVIRONMENT` defaults to `production`.

The active Sentry client is kept alive until shutdown so queued events can be
flushed. It never blocks request handling or changes Citadel's local error
handling when delivery fails.

## Use Bugsink instead

[Bugsink](https://www.bugsink.com/) is a lightweight self-hosted error-tracking
service that accepts the Sentry protocol. Create a Bugsink project, copy its
DSN, and use it as the value of `CITADEL_SENTRY_DSN`:

```bash
export CITADEL_SENTRY_DSN='https://<project-key>@bugsink.example/<project-id>'
citadel serve
```

No Sentry server, account, or additional Citadel binary is required when using
Bugsink. The included `CITADEL_BUGSINK_DSN` variable remains supported for
existing deployments, but it is a compatibility alias. When both variables are
set, `CITADEL_SENTRY_DSN` takes precedence.

## Data handling

Citadel deliberately sends and stores only generic incident metadata:

- component and error category tags;
- an error or panic classification;
- generic messages such as `internal failure` or `process panic`; and
- release and environment labels.

Raw panic payloads, internal error details, request bodies, connection strings,
credentials, tokens, and the DSN itself are excluded. Configure Sentry or
Bugsink retention, access controls, and alerting according to your operational
requirements. Citadel disables Sentry's context and backtrace integrations, so
it does not automatically attach host, operating-system, device, or Rust runtime
metadata.

See [Configuration](/reference/operations/configuration/#environment-overrides)
for local journal retention and environment override details, and [Console
API](/reference/admin-api/console/#error-journal) for the dashboard data model.
