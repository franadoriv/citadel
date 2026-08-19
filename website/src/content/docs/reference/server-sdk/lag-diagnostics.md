---
title: Lag diagnostics native API
description: Trusted server lifecycle for bounded client lag captures and secure ingestion.
---

Lag diagnostics is a trusted **native Gateway API**. It is not available to
GameScript, a player RPC, or an arbitrary HTTP handler. The server owns the
capture identity, filter, UTC deadlines, and one-use upload grants; a client
can only accept or decline its locally allowed recording policy.

## Lifecycle

At authenticated admission, issue the server UTC correlation offer with
`Gateway::issue_diagnostics_server_time`. That offer alone does not enable a
client. The client must have opted in in application source and must advertise
its diagnostics capability after authentication.

Use the ingest-aware operations for a real capture:

```rust
let started = gateway.start_lag_capture_with_ingest_at(
    ingest,
    start_capture,
    now,
)?;

// Later, only after the gateway has observed Recording acknowledgements:
let flushed = gateway.flush_lag_capture_with_ingest_at(
    ingest,
    flush_plan,
    now,
)?;
```

`StartCapture` selects the bounded movement metadata kinds and a recording
deadline. `CaptureFlushPlan` binds every expected upload to its capture,
generation, participant, session, tenant, match, attempt, deadline, size cap,
and `analyze` choice. The ingest-aware flush creates a different signed,
one-use grant for each eligible participant and puts it only in that
participant's `FLUSH` frame.

Do not infer that a queued `START` was received, that a requested upload
completed, or that all players contributed. Query
`Gateway::lag_capture_status`, run
`Gateway::expire_lag_capture_deadline_at` from trusted maintenance, and call
`Gateway::complete_lag_capture_if_terminal` when the selected quorum is
settled. Use the returned `requested`, `ineligible`, and `enqueue_failed`
populations for an honest capture outcome.

## Rules that preserve evidence

- Start/flush at the end of a match when that suits the game, but the developer
  may request either at another controlled time.
- Use the actual `Recording` population for `required_uploads`; do not mint a
  shared grant or guess client/session bindings.
- Set `analyze = false` when collection alone is wanted. That path collects
  private raw artifacts but creates no pending analysis or report.
- Derived analysis and report persistence require SQLite, PostgreSQL, or
  CockroachDB. With MongoDB, the node can collect and retain private raw
  artifacts, but `analyze = true` fails closed rather than presenting an
  in-memory or non-durable report. Use `analyze = false` until a MongoDB report
  store is available.
- An upload failure, expiry, wrong participant, or replay is terminal for that
  grant. Issue a new attempt; never replay or extend a previous bearer.
- Treat raw retention independently from the derived report. Removing raw bytes
  blocks regeneration but does not delete an existing report.

## What the resulting report means

The analyzer reports bounded observations of packet arrival spacing, cadence
residual, ordering/id-gap signals, and clock-correlation quality. It includes
sample counts, exclusions, overflows/truncation, percentiles, decoder/analyzer
versions, and UTC-clock uncertainty so a viewer can tell a partial or
insufficient result from a clean one.

It does **not** prove RTT, one-way latency, network asymmetry, packet loss, or
the cause of a player-visible jitter. A packet-id gap is an observed diagnostic
signal, not a loss claim. Preserve this wording in player-facing incident
reports.

See [Lag diagnostics operations](/reference/operations/lag-diagnostics/) for
private storage, retention, and cluster restrictions, and
[Console API](/reference/admin-api/console/#lag-diagnostics) for report and
raw-artifact access.
