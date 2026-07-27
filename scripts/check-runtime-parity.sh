#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

source "$repo_root/scripts/python-runtime-env.sh"

echo "check-runtime-parity: running Tier-A host API coverage tests"
cargo test --workspace --all-targets --all-features host_api_surface_matches_manifest

if [ -f tests/lua_runtime_smoke.rs ]; then
  echo "check-runtime-parity: running Lua Tier-B host API smoke"
  cargo test --workspace --test lua_runtime_smoke -- --test-threads=1
fi

if [ -f tests/python_runtime_smoke.rs ]; then
  echo "check-runtime-parity: running Python Tier-B host API smoke"
  cargo test --workspace --all-features --test python_runtime_smoke
fi

if [ -f tests/js_runtime_smoke.rs ]; then
  echo "check-runtime-parity: running JavaScript Tier-B host API smoke"
  cargo test --workspace --features runtime-js --test js_runtime_smoke -- --test-threads=1
fi

echo "check-runtime-parity: OK"
