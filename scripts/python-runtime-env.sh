#!/usr/bin/env bash
set -euo pipefail

# PyO3 can usually find the interpreter at build time, but embedded CPython on
# Windows also needs a stdlib prefix at runtime. Use the active `python` because
# this repository intentionally avoids the broken Windows Store `python3` stub.
if command -v python >/dev/null 2>&1; then
  export PYO3_PYTHON="${PYO3_PYTHON:-python}"
  if [ -z "${PYTHONHOME:-}" ]; then
    py_home="$(
      python - <<'PY' | tr -d '\r'
import sys
print(getattr(sys, "base_prefix", "") or sys.prefix)
PY
    )"
    if [ -n "$py_home" ]; then
      export PYTHONHOME="$py_home"
    fi
  fi
fi

# WSL does not automatically forward shell-only variables to Windows child
# processes. Python accepts forward-slash Windows paths, so normalize before
# listing the runtime home in `WSLENV`; this keeps `cargo.exe` test binaries
# pointed at the same stdlib that PyO3 used at build time.
if [ -n "${PYTHONHOME:-}" ]; then
  export PYTHONHOME="${PYTHONHOME//\\//}"
  case ":${WSLENV:-}:" in
    *:PYTHONHOME:* | PYTHONHOME:*) ;;
    *) export WSLENV="${WSLENV:+${WSLENV}:}PYTHONHOME" ;;
  esac
fi

# The Xcode-supplied macOS Python framework is linked as
# `@rpath/Python3.framework/...`; standalone Rust test executables do not get
# Python's rpath automatically. Add the framework prefix as a fallback only for
# test/build processes on macOS, preserving a caller-provided search path.
if [ "$(uname -s)" = "Darwin" ] && command -v python3 >/dev/null 2>&1; then
  python_framework_prefix="$(
    python3 - <<'PY'
import sysconfig
print(sysconfig.get_config_var("PYTHONFRAMEWORKPREFIX") or "")
PY
  )"
  if [ -n "$python_framework_prefix" ]; then
    export DYLD_FALLBACK_LIBRARY_PATH="${python_framework_prefix}${DYLD_FALLBACK_LIBRARY_PATH:+:${DYLD_FALLBACK_LIBRARY_PATH}}"
    case " ${RUSTFLAGS:-} " in
      *"-Wl,-rpath,${python_framework_prefix}"*) ;;
      *) export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-Wl,-rpath,${python_framework_prefix}" ;;
    esac
  fi
fi

# CPython initialization is process-global. Some Rust tests build independent
# embedded runtimes, so parallel execution can race their interpreter setup on
# Windows. Callers that have made their suite serial-safe can opt back in by
# setting RUST_TEST_THREADS explicitly.
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
