#!/usr/bin/env bash
# bundle-ffi.sh — build the citadel-client-ffi native lib and bundle it (plus the
# canonical C ABI header) INSIDE the Unreal plugin so a dropped-in copy compiles +
# links the real client with no env vars.
#
# CitadelClient.Build.cs auto-detects:
#   <plugin>/Source/CitadelClient/ThirdParty/include/citadel_client.h
#   <plugin>/Source/CitadelClient/ThirdParty/<Platform>/<native archive>
# Both are gitignored (generated). Run this once after cloning, or the release
# package populates them in CI. Then copy Plugin/Citadel/ into your UE project's
# Plugins/ and it builds out of the box.
#
# Usage: bash clients/unreal/bundle-ffi.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MODULE="$SCRIPT_DIR/Plugin/Citadel/Source/CitadelClient"
TP="$MODULE/ThirdParty"

# Host platform → UE platform dir (Win64 now; Mac/Linux as the matrix grows).
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*|Windows_NT) UE_PLAT="Win64"; LIB_NAME="citadel_client_ffi.lib" ;;
  Darwin)                          UE_PLAT="Mac";   LIB_NAME="libcitadel_client_ffi.a" ;;
  Linux)                           UE_PLAT="Linux"; LIB_NAME="libcitadel_client_ffi.a" ;;
  *) echo "bundle-ffi: unsupported host $(uname -s)" >&2; exit 1 ;;
esac

echo "bundle-ffi: building citadel-client-ffi (release)…"
( cd "$REPO_ROOT" && cargo build --release -p citadel-client-ffi )

# Keep the native archive suffix: `.lib` on Windows, `.a` on macOS/Linux.
SRC_LIB="$REPO_ROOT/target/release/$LIB_NAME"
mkdir -p "$TP/$UE_PLAT" "$TP/include"
if [[ "$UE_PLAT" == "Win64" ]]; then
  DEST_LIB="citadel_client_ffi.lib"
else
  DEST_LIB="libcitadel_client_ffi.a"
fi
cp -f "$SRC_LIB" "$TP/$UE_PLAT/$DEST_LIB"
cp -f "$REPO_ROOT/crates/citadel-client-ffi/include/citadel_client.h" "$TP/include/citadel_client.h"

echo "bundle-ffi: OK"
echo "  lib    -> $TP/$UE_PLAT/$DEST_LIB"
echo "  header -> $TP/include/citadel_client.h"
echo "Copy clients/unreal/Plugin/Citadel/ into <YourProject>/Plugins/ and build."
