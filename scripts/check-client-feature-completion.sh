#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 "$root/scripts/check_client_feature_completion.py"
python3 "$root/scripts/check_client_feature_completion.py" \
  --manifest tests/fixtures/client-feature-completion/missing-sdk-binding.json --expect-failure
