#!/usr/bin/env bash
#
# Citadel Unreal SDK — Tier-B compile-against-header parity hook.
#
# Invoked by scripts/check-sdk-parity.sh AFTER Tier-A, via the optional per-SDK
# "tier_b_check" extension point (this task adds files under clients/unreal/ only
# and does NOT edit the shared script). It compiles the Unreal-free translation
# unit tier_b/citadel_parity_tu.cpp, which includes the canonical
# citadel_client.h and binds every exported function to a typed function pointer.
# A C ABI signature change makes the TU fail to compile — exactly the drift
# Tier-B catches.
#
# The TU is compiled OBJECT-ONLY (no link), so the native library need not exist.
#
# Degradation contract (from the task): if NO C/C++ compiler is available on the
# runner, this hook reports a clear SKIP and exits 0 — a missing compiler must
# NOT fail the build. The signature guarantee then holds only where a compiler is
# present (documented in website/src/content/docs/guides/engines.md and the release
# checklist).
set -euo pipefail

hook_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$hook_dir/../.." && pwd)"

tu="$hook_dir/tier_b/citadel_parity_tu.cpp"
sdk_public="$hook_dir/Plugin/Citadel/Source/CitadelClient/Public"
ffi_include="$repo_root/crates/citadel-client-ffi/include"
static_contract="$hook_dir/test_networkpeer_abi_v3.py"
real_ffi_gate="$hook_dir/test_real_ffi_gate.py"
clock_v2_parity="$hook_dir/test_clock_v2_parity.py"

if [[ ! -f "$tu" ]]; then
  echo "unreal Tier-B: translation unit not found at $tu" >&2
  exit 1
fi
if [[ ! -f "$ffi_include/citadel_client.h" ]]; then
  echo "unreal Tier-B: canonical header not found at $ffi_include/citadel_client.h" >&2
  exit 1
fi
if [[ ! -f "$static_contract" ]]; then
  echo "unreal Tier-B: ABI-v3 static contract test not found at $static_contract" >&2
  exit 1
fi
python3 "$static_contract"
if [[ ! -f "$real_ffi_gate" ]]; then
  echo "unreal Tier-B: real-FFI negative gate test not found at $real_ffi_gate" >&2
  exit 1
fi
python3 "$real_ffi_gate"
if [[ ! -f "$clock_v2_parity" ]]; then
  echo "unreal Tier-B: v2 clock parity test not found at $clock_v2_parity" >&2
  exit 1
fi
python3 "$clock_v2_parity"

# Runtime-load guard: a UE module DLL must contain exactly one
# IMPLEMENT_MODULE, or it compiles + links (the gated UE build passes) but the
# editor fails at load with "module could not be initialized successfully after
# it was loaded". The compile-verify builds+links without LOADING the module, so
# this cheap static check guards that regression in the fast `check.sh` path.
module_root="$(cd "$sdk_public/.." && pwd)"
impl_count="$(grep -rE 'IMPLEMENT_(GAME_)?MODULE[[:space:]]*\(' "$module_root" 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$impl_count" -lt 1 ]]; then
  echo "unreal Tier-B: no IMPLEMENT_MODULE(...) found under $module_root — the plugin" >&2
  echo "  would compile but fail to initialize in-editor. Add" >&2
  echo "  IMPLEMENT_MODULE(FDefaultModuleImpl, CitadelClient) in a module .cpp." >&2
  exit 1
fi

# Opt-in gated UE compile. The object-only TU below is the UE-FREE
# signature check that runs in the fast `scripts/check.sh` path. The REAL UE
# compile (plugin against real Unreal headers) is heavy, so it only runs when
# explicitly opted in via CITADEL_UE_BUILD=1. It resolves the UE root itself and
# SKIPs cleanly when none is available. It is deliberately NOT part of the default
# check.sh — see clients/unreal/README.md.
if [[ "${CITADEL_UE_BUILD:-0}" == "1" ]]; then
  echo "unreal Tier-B: CITADEL_UE_BUILD=1 -> running gated UE plugin compile (ue-plugin-build.sh)"
  bash "$hook_dir/ue-plugin-build.sh"
fi

tmp_dir="$(mktemp -d 2>/dev/null || echo "${TMPDIR:-/tmp}/citadel-unreal-tierb.$$")"
mkdir -p "$tmp_dir"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

# Find a C++ compiler. Try GNU/Clang-style front ends first, then MSVC `cl`.
compiler=""
for candidate in c++ g++ clang++; do
  if command -v "$candidate" >/dev/null 2>&1; then
    compiler="$candidate"
    break
  fi
done

if [[ -n "$compiler" ]]; then
  echo "unreal Tier-B: compiling parity TU with '$compiler' (object-only)"
  if "$compiler" -std=c++17 -c \
      -I "$sdk_public" -I "$ffi_include" \
      "$tu" -o "$tmp_dir/citadel_parity_tu.o"; then
    echo "unreal Tier-B: OK — SDK bindings compile against citadel_client.h"
    exit 0
  else
    echo "unreal Tier-B: FAILED — SDK bindings do not compile against citadel_client.h (signature drift)" >&2
    exit 1
  fi
fi

# MSVC: `cl` may be on PATH inside a VS developer environment (and is the
# toolchain the MSVC Rust host uses). Compile-only with /c.
if command -v cl >/dev/null 2>&1; then
  echo "unreal Tier-B: compiling parity TU with MSVC 'cl' (object-only)"
  # cl writes intermediates to the CWD; run it from the temp dir.
  if ( cd "$tmp_dir" && cl //nologo //c //EHsc //std:c++17 \
        //I "$sdk_public" //I "$ffi_include" \
        "$tu" //Fo"citadel_parity_tu.obj" ); then
    echo "unreal Tier-B: OK — SDK bindings compile against citadel_client.h"
    exit 0
  else
    echo "unreal Tier-B: FAILED — SDK bindings do not compile against citadel_client.h (signature drift)" >&2
    exit 1
  fi
fi

echo "unreal Tier-B: SKIP — no C/C++ compiler (c++/g++/clang++/cl) found on this runner."
echo "unreal Tier-B: native-signature parity was NOT verified here; it holds only"
echo "unreal Tier-B: where a compiler is present. See website/src/content/docs/guides/engines.md."
exit 0
