#!/usr/bin/env bash
# Build a produced Rust static archive, then compile/link/run an actual C ABI
# consumer against it. This catches missing/stale exported symbols that the
# UE-free signature TU cannot detect. Unreal editor/PIE is intentionally not
# implied by this host-native linkage gate.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/citadel-unreal-ffi.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

if [[ -n "${CITADEL_FFI_ARCHIVE:-}" ]]; then
  # This override is deliberately used by the negative gate test: a supplied
  # artifact must be used as-is, so a missing or stale archive cannot be
  # concealed by rebuilding the current Rust library first.
  archive="$CITADEL_FFI_ARCHIVE"
else
  ( cd "$repo_root" && cargo build --release -p citadel-client-ffi )
  archive="$repo_root/target/release/libcitadel_client_ffi.a"
fi
header_dir="$repo_root/crates/citadel-client-ffi/include"
source="$script_dir/tier_b/real_ffi_parity.c"
[[ -f "$archive" ]] || { echo "unreal real FFI parity: archive missing: $archive" >&2; exit 1; }

# cargo's staticlib metadata is authoritative for host-native system deps. The
# Rust archive must appear before these so its unresolved symbols are retained.
cc -std=c11 -Wall -Wextra -Werror -I"$header_dir" "$source" "$archive" \
  -lstdc++ -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc \
  -o "$tmp_dir/real_ffi_parity"
"$tmp_dir/real_ffi_parity"
