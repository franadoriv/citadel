#!/usr/bin/env bash
set -euo pipefail

# Sign the executable code contained in one staged macOS release directory.
# Notarization happens only after the caller creates the archive; see
# notarize-macos-archive.sh. This script is intentionally credential-agnostic so
# local release engineers and GitHub Actions use the same signing step.

stage="${1:?usage: sign-macos-artifacts.sh <stage-dir> <signing-identity>}"
identity="${2:?usage: sign-macos-artifacts.sh <stage-dir> <signing-identity>}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "sign-macos-artifacts: must run on macOS" >&2
  exit 2
fi
if [[ ! -d "$stage" ]]; then
  echo "sign-macos-artifacts: stage directory does not exist: $stage" >&2
  exit 2
fi

sign() {
  local path="$1"
  echo ">> codesign $path"
  codesign --force --options runtime --timestamp --sign "$identity" "$path"
}

# Frameworks contain nested code. Sign each bundle root before individual loose
# dylibs and executables; none of Citadel's current artifacts are app bundles.
while IFS= read -r -d '' framework; do
  sign "$framework"
done < <(find "$stage" -type d -name '*.framework' -print0)

while IFS= read -r -d '' dylib; do
  sign "$dylib"
done < <(find "$stage" -type f -name '*.dylib' -print0)

if [[ -f "$stage/citadel" ]]; then
  sign "$stage/citadel"
fi

while IFS= read -r -d '' executable; do
  # The package-root server and dylibs were already signed above. A dylib has
  # its executable bit set, so exclude it here to avoid replacing its signature.
  [[ "$executable" == "$stage/citadel" || "$executable" == *.dylib ]] || sign "$executable"
done < <(find "$stage" -type f -perm -111 -print0)

echo "sign-macos-artifacts: signed $stage"
