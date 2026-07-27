#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: scripts/stage-python-bundle.sh <stage-dir>" >&2
  exit 2
fi

stage_dir=$1
python_cmd=${PYTHON:-python}

if [ -z "$stage_dir" ] || [ "$stage_dir" = "/" ]; then
  echo "refusing to stage CPython into an unsafe path: '$stage_dir'" >&2
  exit 2
fi

py_home="$("$python_cmd" - <<'PY'
import pathlib
import sys

prefix = pathlib.Path(getattr(sys, "base_prefix", "") or sys.prefix)
print(prefix.as_posix())
PY
)"

if [ -z "$py_home" ] || [ ! -d "$py_home/Lib" ]; then
  echo "could not locate Python Lib/ from '$python_cmd' (prefix: $py_home)" >&2
  exit 1
fi

mkdir -p "$stage_dir"
bundle_dir="$stage_dir/python"
rm -rf "$bundle_dir"
mkdir -p "$bundle_dir"

echo ">> Copying CPython from $py_home"
cp -R "$py_home/Lib" "$bundle_dir/Lib"
if [ -d "$py_home/DLLs" ]; then
  cp -R "$py_home/DLLs" "$bundle_dir/DLLs"
else
  mkdir -p "$bundle_dir/DLLs"
fi

# Keep the bundle focused on the standard library. Operators can add their own
# wheels/packages deliberately beside their game if they need them.
rm -rf "$bundle_dir/Lib/site-packages"
find "$bundle_dir" -type d -name __pycache__ -prune -exec rm -rf {} +

shopt -s nullglob
python_dlls=("$py_home"/python3*.dll)
if [ "${#python_dlls[@]}" -eq 0 ]; then
  echo "no python3*.dll found in $py_home" >&2
  exit 1
fi

for dll in "$py_home"/*.dll; do
  [ -f "$dll" ] || continue
  cp "$dll" "$stage_dir/"
done

echo ">> Staged bundled CPython under $stage_dir"
