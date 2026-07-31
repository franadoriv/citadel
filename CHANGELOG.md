# Changelog

All notable changes to Citadel are documented here. Version numbers follow
[Semantic Versioning](https://semver.org/).

## Unreleased

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
