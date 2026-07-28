# Changelog

All notable changes to Citadel are documented here. Version numbers follow
[Semantic Versioning](https://semver.org/).

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
