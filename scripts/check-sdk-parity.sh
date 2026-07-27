#!/usr/bin/env bash
#
# Client-SDK contract parity check.
#
# Tier A (mandatory, all SDKs): diff each SDK's declared wire/ABI constants
# against the canonical contract (crates/citadel-wire/contract.json). SDKs are
# discovered via clients/*/sdk.manifest.json (glob), so adding a Godot/Unreal SDK
# later requires only files under that engine's own clients/<engine>/ directory.
#
# Tier B (optional, per SDK): if an SDK's manifest declares a non-null
# "tier_b_check" (a path, relative to the SDK dir, to an executable check such as
# an Unreal compile-against-header step), it is run after Tier A. Absent/null =>
# skipped. This is the extension point for  (Unreal) so that task does
# not need to edit this script.
#
# The check is cheap (no compilation for the constant tables) and toolchain-free
# beyond a Python interpreter for JSON/regex parsing.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

py=""
for candidate in python3 python; do
  if command -v "$candidate" >/dev/null 2>&1 && "$candidate" --version >/dev/null 2>&1; then
    py="$candidate"
    break
  fi
done
if [[ -z "$py" ]]; then
  echo "check-sdk-parity: python3/python not found; cannot run Tier-A parity" >&2
  exit 1
fi

# Tier A — declared-constant parity for every discovered SDK.
"$py" "$script_dir/check_sdk_parity.py" "$repo_root"

# Tier B — optional per-SDK hooks. Iterate discovered manifests and run any
# declared "tier_b_check" script; a missing/null hook is skipped.
shopt -s nullglob
for manifest in "$repo_root"/clients/*/sdk.manifest.json; do
  sdk_dir="$(dirname "$manifest")"
  engine="$("$py" -c 'import json,sys;print(json.load(open(sys.argv[1])).get("engine",""))' "$manifest")"
  hook="$("$py" -c 'import json,sys;v=json.load(open(sys.argv[1])).get("tier_b_check");print(v if v else "")' "$manifest")"
  if [[ -z "$hook" ]]; then
    continue
  fi
  hook_path="$sdk_dir/$hook"
  if [[ ! -f "$hook_path" ]]; then
    echo "check-sdk-parity: [$engine] tier_b_check '$hook' not found at $hook_path" >&2
    exit 1
  fi
  echo "check-sdk-parity: [$engine] running Tier-B hook: $hook"
  bash "$hook_path"
done
