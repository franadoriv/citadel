#!/usr/bin/env bash
set -euo pipefail

stage_dir=${1:-bin/server-python}

if [ ! -d "$stage_dir" ]; then
  echo "stage directory does not exist: $stage_dir" >&2
  exit 2
fi

filter_path_without_global_python() {
  local original=${1:-}
  local filtered=""
  local entry
  IFS=':' read -r -a entries <<< "$original"
  for entry in "${entries[@]}"; do
    case "${entry,,}" in
      *miniconda*|*anaconda*|*conda*) continue ;;
    esac
    if [ -z "$filtered" ]; then
      filtered="$entry"
    else
      filtered="$filtered:$entry"
    fi
  done
  printf '%s' "$filtered"
}

(
  cd "$stage_dir"

  exe="./citadel"
  if [ -f "./citadel.exe" ]; then
    exe="./citadel.exe"
  fi
  if [ ! -x "$exe" ] && [ ! -f "$exe" ]; then
    echo "staged citadel binary not found in $stage_dir" >&2
    exit 1
  fi
  if [ ! -f "scripts/main.py" ]; then
    echo "staged scripts/main.py not found in $stage_dir" >&2
    exit 1
  fi
  if [ ! -f "python/Lib/os.py" ]; then
    echo "staged python/Lib/os.py not found in $stage_dir" >&2
    exit 1
  fi

  python_home="$PWD/python"
  if command -v cygpath >/dev/null 2>&1; then
    python_home="$(cygpath -w "$python_home")"
  fi

  echo ">> Smoke: $exe check using bundled CPython"
  env \
    -u PYO3_PYTHON \
    -u PYTHONPATH \
    PYTHONHOME="$python_home" \
    PYTHONNOUSERSITE=1 \
    PATH="$(filter_path_without_global_python "${PATH:-}")" \
    "$exe" check
)
