#!/usr/bin/env python3
"""Fail closed if runtime ingress media is tracked or reachable from a ref.

`media/inbound/` is a local hand-off area. It can contain user-provided media,
so it is intentionally ignored and must never become repository or release
content. Checking all local heads and tags also prevents a release from
reintroducing a previously removed ingress object through history.
"""

from __future__ import annotations

import subprocess
import sys

FORBIDDEN_PREFIX = "media/inbound/"


def git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )


def main() -> int:
    tracked = git("ls-files", "-z")
    if tracked.returncode:
        print("Ingress-media guard could not inspect the Git index.", file=sys.stderr)
        return 1
    indexed = [p for p in tracked.stdout.split("\0") if p.startswith(FORBIDDEN_PREFIX)]

    # Use all heads and tags, rather than only HEAD: a release tag must not
    # preserve an ingress path even if the current checkout no longer has it.
    history = git("log", "--branches", "--tags", "--format=%H", "--", FORBIDDEN_PREFIX)
    if history.returncode:
        print("Ingress-media guard could not inspect reachable history.", file=sys.stderr)
        return 1
    commits = [line for line in history.stdout.splitlines() if line]

    if indexed or commits:
        print(
            "Runtime ingress media is tracked or reachable from a local head/tag; "
            "push/release blocked.",
            file=sys.stderr,
        )
        if indexed:
            print(f"Indexed ingress paths: {len(indexed)}", file=sys.stderr)
        if commits:
            print(f"Reachable commits containing ingress media: {len(commits)}", file=sys.stderr)
        return 1

    print("Runtime ingress media guard passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
