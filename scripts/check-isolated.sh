#!/usr/bin/env bash
# Run the canonical checks without retaining a massive Cargo target cache.
#
# This intentionally trades warm-cache performance for bounded disk usage. All
# build artifacts are isolated in a temporary directory and removed on exit,
# including failures and interrupts.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
minimum_free_gib="${CITADEL_CHECK_MIN_FREE_GIB:-30}"
free_gib="$(python3 -c 'import os, sys; s = os.statvfs(sys.argv[1]); print((s.f_bavail * s.f_frsize) // (1024**3))' "$project_root")"

if (( free_gib < minimum_free_gib )); then
  printf 'Refusing canonical check: only %s GiB free; need at least %s GiB.\n' \
    "$free_gib" "$minimum_free_gib" >&2
  exit 1
fi

target_dir="$(mktemp -d "${TMPDIR:-/tmp}/citadel-target.XXXXXX")"
cleanup() {
  rm -rf "$target_dir"
}
trap cleanup EXIT HUP INT TERM

printf 'Using disposable Cargo target directory: %s\n' "$target_dir"
printf 'Artifacts will be deleted when this command exits.\n'

cd "$project_root"
CARGO_TARGET_DIR="$target_dir" \
CARGO_INCREMENTAL=0 \
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}" \
bash scripts/check.sh "$@"
