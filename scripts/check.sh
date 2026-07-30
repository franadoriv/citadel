#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/python-runtime-env.sh"

cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
# `cargo test` also runs the contract-manifest stale-guard
# (citadel-client-ffi/tests/contract_manifest.rs), which fails if
# crates/citadel-wire/contract.json is out of date vs the canonical Rust consts.
cargo test --workspace --all-targets --all-features -- --test-threads="$RUST_TEST_THREADS"
# The embedded-runtime test binaries need the CPython stdlib path. The later
# source validators use the shell's native Python interpreter, for which that
# Windows-specific path is invalid under WSL.
unset PYTHONHOME
bash scripts/check-docs.sh
bash scripts/check-capability-matrix.sh
bash scripts/check-client-doc-tabs.sh
# Tier-A client-SDK contract parity: fails if any SDK's declared wire/ABI
# constants drift from the canonical contract.json (see check-sdk-parity.sh).
bash scripts/check-sdk-parity.sh
# Cross-engine NetworkPeer structural fixture/binding gate. Editor and browser
# two-client gameplay runs are explicitly an external-environment matrix.
python3 scripts/check-networkpeer-cross-engine.py
# Public player features must declare every released SDK binding and its
# frontend reference before the backend contract can be considered shipped.
bash scripts/check-client-feature-completion.sh
python3 scripts/check-godot-web-sdk.py
# Tier-A/B script-runtime host-API parity for embedded runtimes.
bash scripts/check-runtime-parity.sh
# Local Dockerfile, Compose sample, and container lifecycle invariants that do
# not require a Docker daemon. Release CI deliberately does not run Docker.
bash scripts/check-container-assets.sh
