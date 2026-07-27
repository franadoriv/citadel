#!/usr/bin/env bash
set -euo pipefail

# Submit a signed ZIP containing Citadel's command-line server and/or engine
# native libraries. Apple notarizes this archive directly; `stapler` does not
# apply to a plain ZIP (it is for app bundles, dmg, and pkg artifacts).

archive="${1:?usage: notarize-macos-archive.sh <archive.zip> <keychain-profile>}"
profile="${2:?usage: notarize-macos-archive.sh <archive.zip> <keychain-profile>}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "notarize-macos-archive: must run on macOS" >&2
  exit 2
fi
if [[ ! -f "$archive" ]]; then
  echo "notarize-macos-archive: archive does not exist: $archive" >&2
  exit 2
fi

echo ">> notarizing $archive"
xcrun notarytool submit "$archive" --keychain-profile "$profile" --wait
echo "notarize-macos-archive: accepted $archive"
