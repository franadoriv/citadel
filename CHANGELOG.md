# Changelog

All notable changes to Citadel are documented here. Version numbers follow
[Semantic Versioning](https://semver.org/).

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

