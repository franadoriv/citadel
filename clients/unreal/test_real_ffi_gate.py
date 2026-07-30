#!/usr/bin/env python3
"""Negative proof for the Unreal direct-C-ABI artifact gate.

The positive harness builds the current Rust archive by default.  Its explicit
archive override must *not* rebuild when a caller supplies an artifact: this
test proves a missing archive and a valid-but-stale archive without the required
symbols both fail the same compile/link path.
"""
import os
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "clients/unreal/real-ffi-parity.sh"


def expect_failure(archive: Path, expected: str) -> None:
    result = subprocess.run(
        ["bash", str(HARNESS)],
        cwd=ROOT,
        env={**os.environ, "CITADEL_FFI_ARCHIVE": str(archive)},
        text=True,
        capture_output=True,
    )
    output = result.stdout + result.stderr
    if result.returncode == 0:
        raise AssertionError(f"{archive.name}: expected the real-FFI gate to fail")
    if expected not in output:
        raise AssertionError(f"{archive.name}: expected output containing {expected!r}; got {output!r}")


try:
    with tempfile.TemporaryDirectory(prefix="citadel-unreal-ffi-negative-") as temp:
        temp_dir = Path(temp)
        expect_failure(temp_dir / "missing-citadel-client-ffi.a", "archive missing")

        # A syntactically valid archive whose sole object has no Citadel exports
        # represents a stale generated library. Linkage must reject it.
        empty_c = temp_dir / "stale.c"
        empty_o = temp_dir / "stale.o"
        stale_archive = temp_dir / "stale-citadel-client-ffi.a"
        empty_c.write_text("int stale_citadel_ffi_placeholder(void) { return 0; }\n")
        subprocess.run(["cc", "-c", str(empty_c), "-o", str(empty_o)], check=True)
        subprocess.run(["ar", "rcs", str(stale_archive), str(empty_o)], check=True)
        expect_failure(stale_archive, "undefined reference")
except (AssertionError, subprocess.CalledProcessError) as error:
    print(f"unreal real FFI negative gate: FAIL — {error}", file=sys.stderr)
    sys.exit(1)

print("unreal real FFI negative gate: OK — missing and stale archives fail")
