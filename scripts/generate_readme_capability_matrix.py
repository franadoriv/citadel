#!/usr/bin/env python
"""Render the compact root README capability snapshot from the catalog."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
CATALOG = ROOT / "manifests" / "capability-matrix.json"
START = "## Capability snapshot\n"
END = "\n## Roadmap\n"
def render(data: dict[str, object]) -> str:
    if not data.get("server_sections") or not data.get("client_sections"):
        raise ValueError("capability matrix is missing server or client sections")

    lines = [
        "## Capability snapshot",
        "",
        "<!-- Generated from manifests/capability-matrix.json; do not edit this section by hand. -->",
        "",
        "Citadel is deliberately honest about its current surface. The full,",
        "machine-readable [capability matrix](manifests/capability-matrix.json) is the",
        "source of truth; this is the useful-at-a-glance version.",
        "",
        "| Area | What ships today |",
        "| --- | --- |",
        "| **Game logic** | Lua by default, with trusted embedded Python and JavaScript builds. All share message, lifecycle, tick, RPC, room, storage, and social-service hooks. |",
        "| **Realtime** | QUIC for native clients, WebTransport for modern browsers, and WebSocket as the broad fallback; rooms, authoritative state, transform sync, actors, maps, and server physics are available. |",
        "| **Game services** | Accounts and sessions, storage, friends, groups, chat, leaderboards, notifications, wallet, purchases, audit records, and an operator dashboard. |",
        "| **Data** | SQLite for the zero-setup default; PostgreSQL, CockroachDB, and transaction-capable MongoDB for durable deployments. Clustered party/matchmaker authority requires PostgreSQL or CockroachDB—SQLite and MongoDB clusters are rejected. |",
        "| **Client paths** | Unity, Unreal, Godot, Rust, and browser/JavaScript SDK surfaces. Their exact engine and OS coverage is in the matrix. |",
        "| **Operations** | Release archives, config validation, health/status endpoints, structured logs, error journal, optional Sentry-compatible telemetry, and TLS/reverse-proxy guidance. |",
        "",
    ]
    return "\n".join(lines) + "\n"


def replace_feature_section(readme: str, section: str) -> str:
    start = readme.find(START)
    if start == -1:
        raise ValueError("README has no Capability snapshot heading")
    end = readme.find(END, start)
    if end == -1:
        raise ValueError("README has no following Roadmap heading")
    return readme[:start] + section + "\n" + readme[end + 1 :]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail when README is stale")
    parser.add_argument("--write", action="store_true", help="write the generated section")
    args = parser.parse_args()
    if args.check == args.write:
        parser.error("choose exactly one of --check or --write")

    data = json.loads(CATALOG.read_text(encoding="utf-8"))
    expected = replace_feature_section(README.read_text(encoding="utf-8"), render(data))
    if args.check:
        if README.read_text(encoding="utf-8") != expected:
            print("capability-matrix: README Feature status is stale; run python scripts/generate_readme_capability_matrix.py --write")
            return 1
        print("capability-matrix: OK")
        return 0

    README.write_text(expected, encoding="utf-8")
    print("capability-matrix: README updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
