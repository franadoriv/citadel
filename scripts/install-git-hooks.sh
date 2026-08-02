#!/usr/bin/env bash
# Configure the repository-owned, fail-closed pre-push checks for this clone.
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
cd "$root"
test -x .githooks/pre-push || {
  echo ".githooks/pre-push must be executable" >&2
  exit 1
}
git config core.hooksPath .githooks
echo "Configured core.hooksPath=.githooks"
