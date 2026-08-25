# Changelog

All notable changes to Citadel are documented here. Version numbers follow
[Semantic Versioning](https://semver.org/).

## Unreleased

## [0.11.0] - 2026-08-25

### Added

- **Scoped machine credentials.** Human administrators can create, rotate, and
  revoke durable, hash-only `ctdl_k1_` API keys with explicit read scopes. Keys
  are header-only, return their secret exactly once, and cannot mutate data or
  manage credentials.
- **Opt-in lag diagnostics.** The JavaScript SDK can record bounded movement
  metadata only when application source enables it and a trusted server issues
  a capture lifecycle. Citadel uses one-use upload grants, private raw retention,
  and redacted derived reports; it does not claim RTT, one-way latency, or packet
  loss. SQLite, PostgreSQL, and CockroachDB persist reports; MongoDB supports
  raw collection only with analysis disabled.
- **Authoritative match lifecycle, input, and telemetry.** Embedded Lua, Python,
  and JavaScript runtimes receive server-owned match lifecycle callbacks and
  protocol-v2 custom match messages through the fenced `citadel.on_input`
  bridge. Trusted scripts can create bounded context-derived telemetry slices;
  report identifiers and payload/identity retention remain server controlled.
- **Receipt-validation foundation.** The console can persist validated purchase
  records without storing raw receipts. Only deterministic custom development
  receipts are enabled today; Apple, Google, and Huawei validators remain
  disabled pending verified provider adapters.
- **Durable logging, telemetry persistence, and match records.** The console
  action trail, authoritative telemetry slice reports, and a new game-script log
  stream are persisted through a bounded write-behind queue instead of living
  only in process-local rings. Four new tables (`matches`, `match_logs`,
  `console_audit_entries`, `telemetry_slice_reports`) ship for SQLite,
  PostgreSQL, and CockroachDB; the in-memory and MongoDB backends keep their
  rings and every read endpoint still answers `200` with `durable: false`.
- **Server-owned match records.** The realtime gateway mints a durable `mt1-`
  match identity at room birth and records open, close, peak participants, join
  total, and termination reason. Game code can neither open nor close a match;
  `citadel.match.set_result(json)` stamps a result on the row the server closes.
- **`citadel.log.write(level, tag, message, payload_json?)`** in Lua, Python,
  and JavaScript. Log lines written inside a match-scoped callback are attributed
  to that match; anything else is stored with a `NULL` match reference.
  `payload_json` is author-supplied and is persisted verbatim — it is visible to
  console operators and the database explorer, so secrets must not be written
  into it.
- **New console pages and routes.** `GET /console/v1/logs`,
  `/console/v1/logs/{log_id}`, `/console/v1/matchlogs`,
  `/console/v1/matchlogs/{match_id}`, and
  `/console/v1/matchlogs/{match_id}/entries`, plus a Logs page and a Match
  Records drill-down in the console. `/console/v1/audit` and
  `/console/v1/telemetry/slices` gain `match_id` and `after` filters and keyset
  paging. A new `logs:read` API-key scope gates the log routes.

### Changed

- Telemetry slice report ids are now salted per node and per boot
  (`ats1-` plus 29 hex, 34 bytes total), so two nodes or two runs can no longer
  mint the same id. Closed slices are reaped on the flush service's own tick
  rather than on an operator's page load, which keeps `closed_at_ms` and
  `duration_ms` from being observer-dependent.
- The console action trail's ring capacity is configurable through
  `logs.audit.capacity` instead of being hardcoded.
- Reads of the audit and log routes are recorded to the ring only, so a machine
  credential polling them can no longer self-amplify into unbounded durable
  writes.

### Known limitations

- `lag_diagnostic_reports.match_id` ships as a nullable column with a working
  read filter, but nothing populates it yet: the write path needs a production
  lag-capture flush caller and per-match capture scoping. The lag-to-match link
  does not work today.

## [0.10.0] - 2026-08-09

### Added

- **GameScript authoritative game logic.** Embedded Lua, Python, and JavaScript
  runtimes can own protected gameplay end-to-end: a supervised worker protocol
  with a real Windows backend and per-match isolation, an immutable
  hash-addressed revision repository (PostgreSQL, CockroachDB, MongoDB, SQLite),
  a per-match execution scheduler, a mandatory readiness gate for match
  admission (`runtime.require_script`), and an authoritative input/command
  bridge (`citadel.on_input`, fenced command batches, lag-compensated fire/hit)
  with Lua/Python/JavaScript parity. GameScript is documented as a headline
  feature.
- **Text-policy content-safety API.** Operator-owned JSON policies loaded during
  script initialization, with identical `load_json`, `scan`, and `sanitize`
  surfaces across Lua, Python, and JavaScript. Rust-owned, cached, and sealed
  before handlers run; fail-closed on an invalid policy.
- Exposed authoritative navigation queries to server runtimes.
- Added a two-node cluster matchmaker probe to the bot stress simulator for
  multi-node load testing.

### Changed

- **Room-scoped replication isolation.** Room membership and replication
  membership are now bound atomically across every lifecycle transition, and the
  object/connection-to-room binding is enforced as an invariant at delivery,
  apply, and bootstrap. Cross-room delivery fails closed, closing a cross-match
  transform/replicated-state leak. Remote authoritative admission fail-closes
  pending the cross-node relay follow-up.
- Extracted `citadel-transform` so the engine SDK C ABI no longer links the
  server crate.
- Party RPC now reuses the shared server runtime.
- Cached Python payload extraction to avoid repeated unpacking.

### Security

- Hardened console operator authentication.
- Terminated TLS on the HTTP surface and served security response headers.

### Fixed

- Kept slash-delimited signed Python payloads valid on native Windows, closing
  READY before the atomic staging-directory activation.
- Tracked match overrun streaks per quantum kind.
- Fenced GameScript submit-dedupe against concurrent revision pruning.
- Made JSON stale-guards independent of build-graph ordering.
- Dropped `COLLATE C` from the CockroachDB settlement-outbox migration.

## [0.9.14] - 2026-08-03

### Added

- Added durable clustered party authority, atomic multi-object storage batches,
  and async outbound HTTP APIs for server runtimes.
- Expanded the runtime extensibility surface for current game-server workflows.

### Fixed

- Kept server release archives free of client SDK contents.
- Repaired Godot Web SDK package validation so fresh copied packages are loaded
  and exercised by the headless release harness.

### Release validation

- Manual native Unreal Engine 5.8/PIE validation was unavailable and explicitly
  waived by the release owner for v0.9.14. This waiver is not a passing Unreal
  validation result.

## [0.9.12] - 2026-07-31

### Added

- Added the durable MongoDB repository backend, including transaction-capable
  deployment checks, operator guidance, and backup/restore coverage.
- Added NetworkPeer replication foundations and authoring surfaces across the
  shipped SDKs, TMX collision import/cooking, and a portable JavaScript browser
  SDK release artifact.
- Added configurable WebSocket liveness probes, retained error incidents with
  optional Sentry reporting, host CPU/RAM/storage telemetry, and opt-in
  deferred durable-storage writes.
- Added the aggressive bot stress simulator and compact log analyzer for
  multiplayer load testing.

### Changed

- Expanded WebDoc database operations into a dedicated Operations > Databases
  navigation group for in-memory, SQLite, PostgreSQL, CockroachDB, and MongoDB.
- Release automation now packages the JavaScript browser SDK alongside the
  existing Windows, portable Linux, and Godot Web artifacts.

### Fixed

- Lua tick and hot-reload integration coverage now consumes unreliable script broadcasts through the transport's latest-wins mailbox, matching production delivery semantics.
- Kept Godot native collection encoding aligned with the version 3 client FFI
  descriptor used by the bundled runtime.
- Resolved the Rustls provider selection and WebSocket liveness implementation
  quality gates required for release validation.

## [0.9.11] - 2026-07-28

### Added

- Linux server releases now include both x86_64 and ARM64 (`aarch64-musl`)
  archives, SHA-256 checksums, CI archive validation, and a hardened systemd
  service template for production deployment.
- Added production PEM TLS configuration for QUIC and WebTransport, with native
  clients verifying public-root certificate chains and hostnames.
- Added authorized, ephemeral chat typing indicators with receiver-side expiry
  for clients connected to the same node.

### Changed

- Restored the capability catalog and corrected the README to accurately
  describe portable Linux releases, TLS transport behavior, and local-node chat
  typing support.

### Fixed

- Restored the original Citadel logo asset.

## [0.9.10] - 2026-07-28

### Added

- Published a portable, statically linked x86_64 Linux server archive with
  every release. Extract the archive and run `./citadel`; Rust, Cargo, and a
  source checkout are not required.
- Added a public server-release installation guide covering Windows, x86_64
  Linux, configuration validation, upgrades, and safe public deployment.

### Changed

- Release automation now builds, validates, and attaches the Linux server ZIP
  alongside the Windows server and client SDK artifacts.

### Fixed

- Production deployment commits now use a GitHub-associated author email so
  Vercel can identify the commit author and deploy the release branch.

## [0.9.9] - 2026-07-27

### Added

- Released the current Citadel server, client SDK, engine-integration, runtime,
  operator-console, storage, social, chat, map, and physics capability baseline
  as versioned Windows packages and a Godot Web SDK artifact.
- Added email/password player authentication with Argon2id verification and
  durable admission limits, plus the read-only operator database explorer.

### Changed

- Release automation now requires a new semantic Cargo version before a push to
  `release` can package artifacts or create a GitHub Release tag.
- The repository metadata now links to the Citadel documentation site.

### Fixed

- Restored clean Rust compilation by correcting incomplete method calls in the
  wire, FFI, realtime, lifecycle, repository, and test paths.
