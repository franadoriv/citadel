# Contributing to Citadel

Thanks for helping improve Citadel. Contributions may include bug fixes,
documentation, examples, client SDK work, and game-server capabilities.

## Before you start

- Search existing issues and pull requests before opening a new one.
- For a bug or feature idea, open an issue first when the proposed change is
  substantial. This avoids duplicate work and lets maintainers agree on the
  direction.
- Please keep each pull request focused on one concern.

## Development setup

Citadel's source build needs a recent stable Rust toolchain, Python 3, Git, and
Make. On Windows, use the included `make.ps1` wrapper.

```bash
git clone https://github.com/franadoriv/citadel.git
cd citadel
make check
```

`bash scripts/check.sh` is the canonical local verification command. It runs
formatting, Clippy with warnings denied, the workspace tests, and documentation
checks. Run the relevant commands before opening a pull request.

## Pull requests

1. Create a focused branch from `develop`.
2. Add or update tests when behavior changes.
3. Update the public documentation, examples, capability matrix, or changelog
   when applicable.
4. Complete the pull-request template and state exactly what you verified.

Changes to game logic must preserve Citadel's core invariant: clients are
untrusted and authoritative validation belongs on the server.

## Reporting problems and security issues

Use GitHub Issues for reproducible bugs, questions, and feature proposals.
Do not disclose security vulnerabilities in a public issue; see
[SECURITY.md](SECURITY.md) for the responsible reporting path.

## Community standards

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
