#!/usr/bin/env bash
set -euo pipefail

if command -v python >/dev/null 2>&1; then
  python_bin=python
elif command -v python3 >/dev/null 2>&1; then
  python_bin=python3
else
  echo "check-capability-matrix: Python 3 is required" >&2
  exit 1
fi

"$python_bin" scripts/generate_readme_capability_matrix.py --check
