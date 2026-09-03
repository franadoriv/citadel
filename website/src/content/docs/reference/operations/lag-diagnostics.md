---
title: Lag diagnostics operations
description: Configure, collect, retain, and interpret bounded private lag captures.
---

Lag diagnostics is disabled by default. It is intended for a controlled debug
window: a game developer starts a capture for opted-in clients, normally flushes
at match end, and optionally asks for a derived report. It is not a continuous
telemetry feed.

## Configure a private ingest root

```toml
[lag_diagnostics]
enabled = true
raw_root = "/var/lib/citadel/lag-raw"
active_key_id = "2026-08"
# Base64url-without-padding HMAC-SHA256 key material, at least 32 bytes after decoding.
upload_hmac_keys = { "2026-08" = "REPLACE_WITH_A_SECRET_32_BYTE_MINIMUM_KEY" }
allowed_origins = ["https://game.example"]
max_compressed_bytes = 4194304
max_decompressed_bytes = 67108864
max_decompression_ratio = 32
max_concurrent_uploads = 4
max_raw_bytes = 4294967296
retention_hours = 168
shared_raw_store = false
```

`raw_root` must be an absolute, private filesystem location; do not configure a
static-file, web-server, backup-export, or user-writable directory. The keyring
signs upload grants and is redacted from diagnostics. Rotate by retaining an
old key long enough to reject/settle outstanding windows and making the new
key id active; never publish a key, a token, or a raw path.

The native API accepts an upload only while its capture is in `Flushing` state.
The route is mounted permanently, but a disabled, expired, replayed, foreign,
wrong-state, wrong-MIME, or oversized request is rejected. Browser CORS is
off when `allowed_origins` is empty; configured origins must be exact HTTPS
origins (or loopback HTTP for local development), never wildcards or paths.

## Storage, quotas, and recovery

Raw artifacts are gzip-compressed `CLAG` files plus private manifests under
`raw_root`. Citadel streams an upload to staging, applies compressed,
decompressed, expansion-ratio, and concurrency limits, validates the `CLAG`
header/rows, fsyncs the manifest, and publishes atomically. It uses pending and
consumed one-use leases so a crash, race, or replay cannot turn a partial file
into a reportable artifact.

Raw bytes, absolute paths, upload tokens, MIME grants, and packet rows do not
go in database report records or Console JSON. Only compact derived report
projections are eligible for the report store. Retention removes raw material
after `retention_hours` or an administrator deletion; the existing derived
report remains visible with raw availability marked unavailable, and
regeneration is then blocked.

Derived report persistence is available with SQLite, PostgreSQL, and
CockroachDB. MongoDB currently supports raw collection and its private
retention lifecycle only: a request with `analyze = true` fails closed instead
of using an ephemeral report cache. For a MongoDB deployment, request
`analyze = false` and retain/download the opaque raw evidence until a durable
MongoDB report adapter is released.

Current ingest is intentionally node-local. Enabling `[cluster]` together with
`lag_diagnostics.enabled = true` fails validation even if
`shared_raw_store = true`: a shared raw root alone cannot make one-use leases
and capture control safe. Keep this feature disabled in a cluster until a
shared raw store **and** a shared capture-control plane are available.

Run `citadel check --config /etc/citadel/citadel.toml` before deployment. If
the root becomes unavailable, leave diagnostics disabled rather than falling
back to a public or temporary directory.

## Interpret reports honestly

Reports summarize observed movement delivery spacing, cadence residual, order
or packet-id-gap signals, and server-clock correlation. They carry the
decoder/analyzer versions, sample count, exclusions, overflow/truncation,
percentiles, bounded windows, and clock uncertainty. A report status can be
`no_analysis`, `pending`, `no_data`, `partial`, `complete`, or `failed`.
Separately, an administrator can see that raw retention is unavailable; the
current API does not disclose whether that resulted from expiry or a manual
deletion. Each outcome means the Console should show the quality fields rather
than turn a missing value into a latency conclusion.

Do not call any metric RTT, one-way latency, asymmetric latency, or packet
loss. A packet-id gap is an observation that can guide further investigation,
not proof that a network dropped a packet. High clock uncertainty, malformed
rows, filter skips, or a truncated client ring make correlation weaker and must
be shown alongside charts.

## Operational troubleshooting

| Symptom | Check | Safe response |
| --- | --- | --- |
| No client is eligible | Client source opt-in and post-auth capability; legacy clients have none. | Start normally; do not force-enable through a URL or server flag. |
| `START` was requested but no uploads are expected | Inspect lifecycle status for `Recording`, disconnect, and enqueue outcomes. | Do not infer receipt from queueing; choose a lower quorum or close as no data. |
| Upload is rejected | Check current UTC deadline, exact origin, MIME, gzip encoding, body cap, and participant binding. | Mint a new per-client `FLUSH` attempt; never reuse a bearer. |
| Report has no data/partial | Read sample count, exclusions, overflow, truncation, and UTC uncertainty. | Describe it as inconclusive, not as good network health. |
| Raw download/regeneration is unavailable | Check retention or an administrator deletion. | Existing reports remain; start a new capture if raw evidence is needed. |
| Cluster config fails validation | Current lease/recovery state is node-local. | Keep diagnostics off in the cluster until distributed ingestion is implemented. |

The client contract is in
[Lag diagnostics (JavaScript)](/reference/client-sdk/lag-diagnostics/); the
trusted capture API is in
[Lag diagnostics native API](/reference/server-sdk/lag-diagnostics/).
