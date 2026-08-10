#!/usr/bin/env python
"""Render the human-readable "Engine & platform support" docs page from the catalog.

The canonical source of truth is ``manifests/capability-matrix.json``. This script
renders it into ``website/src/content/docs/support-matrix.md`` so the site always
shows exact per-engine, per-runtime, and per-platform status. Like the README
generator it is idempotent: ``--check`` fails when the page is stale and ``--write``
regenerates it. Wiring the ``--check`` into ``scripts/check.sh`` makes "a feature is
not green until its webdoc is updated" a mechanical gate, not a convention.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "manifests" / "capability-matrix.json"
PAGE = ROOT / "website" / "src" / "content" / "docs" / "support-matrix.md"

# Path to the source of truth on the default branch, for the "generated from" link.
CATALOG_URL = (
    "https://github.com/franadoriv/citadel/blob/develop/manifests/capability-matrix.json"
)

BADGE = {"shipped": "✅", "partial": "🟡", "planned": "⬜", "na": "—"}

COLUMN_LABELS = {
    "common": "Common",
    "lua": "Lua",
    "python": "Python",
    "javascript": "JavaScript",
    "typescript": "TypeScript",
    "rust_game_logic": "Rust",
    "unity": "Unity",
    "unreal": "Unreal",
    "godot": "Godot",
    "web": "Web / JS",
    "rust": "Rust",
    "windows": "Windows",
    "macos": "macOS",
    "linux": "Linux",
}

# Preferred left-to-right column order; unknown keys keep insertion order after these.
COLUMN_ORDER = [
    "common",
    "lua",
    "python",
    "javascript",
    "typescript",
    "rust_game_logic",
    "unity",
    "unreal",
    "godot",
    "web",
    "rust",
    "windows",
    "macos",
    "linux",
]


def escape_cell(text: str) -> str:
    """Escape a Markdown table cell (pipes would otherwise split columns)."""
    return text.replace("|", "\\|")


def badge(value: str) -> str:
    return BADGE.get(value, value)


def columns_of(rows: list[dict[str, str]]) -> list[str]:
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key in ("capability", "detail") or key in keys:
                continue
            keys.append(key)
    return sorted(
        keys,
        key=lambda key: COLUMN_ORDER.index(key) if key in COLUMN_ORDER else len(COLUMN_ORDER),
    )


def render_table(rows: list[dict[str, str]]) -> list[str]:
    cols = columns_of(rows)
    lines = [
        "| Capability | " + " | ".join(COLUMN_LABELS.get(c, c) for c in cols) + " |",
        "| --- |" + "".join(" :---: |" for _ in cols),
    ]
    for row in rows:
        cells = " | ".join(badge(str(row.get(col, "na"))) for col in cols)
        lines.append("| " + escape_cell(str(row["capability"])) + " | " + cells + " |")
    return lines


def render_details(rows: list[dict[str, str]]) -> list[str]:
    detailed = [row for row in rows if row.get("detail")]
    if not detailed:
        return []
    out = ["", "<details>", "<summary>Row-by-row notes &amp; caveats</summary>", ""]
    for row in detailed:
        out.append(f"- **{row['capability']}** — {row['detail']}")
    out += ["", "</details>"]
    return out


def render_section(title: str, rows: list[dict[str, str]]) -> list[str]:
    out = [f"### {title}", ""]
    out += render_table(rows)
    out += render_details(rows)
    out.append("")
    return out


def render(data: dict[str, object]) -> str:
    server_sections = data.get("server_sections")
    client_sections = data.get("client_sections")
    platform_rows = data.get("platform_rows")
    if not server_sections or not client_sections or not platform_rows:
        raise ValueError("capability matrix is missing server, client, or platform data")

    lines = [
        "---",
        "title: Engine & platform support",
        "description: Per-engine, per-runtime, and per-platform status of every Citadel "
        "capability, generated from the canonical capability matrix.",
        "---",
        "",
        "<!-- Generated from manifests/capability-matrix.json by "
        "scripts/generate_docs_capability_matrix.py. Do not edit by hand; run "
        "`python scripts/generate_docs_capability_matrix.py --write`. -->",
        "",
        f"Every row here comes straight from the [canonical capability matrix]({CATALOG_URL}),",
        "the single source of truth for what Citadel ships. A capability is not marked",
        "shipped here until its documentation is updated, so this page and the code stay in",
        "lockstep.",
        "",
        "**Legend:** ✅ Shipped · 🟡 Partial · ⬜ Planned · — Not applicable",
        "",
        "## Client SDKs by engine",
        "",
        "What each engine and browser client SDK can do today. This is the first thing to",
        "check when picking an engine.",
        "",
    ]
    for section in client_sections:
        lines += render_section(str(section["title"]), section["rows"])

    lines += [
        "## Packages by platform",
        "",
        "Which prebuilt download exists per operating system. Where a native package is not",
        "yet published, the SDK still builds from source.",
        "",
    ]
    lines += render_table(platform_rows)
    lines += render_details(platform_rows)
    lines.append("")

    lines += [
        "## Server & game-logic capabilities by runtime",
        "",
        "Server-side features. **Common** means the capability is available server-wide; the",
        "language columns show which embedded game-logic runtimes expose it.",
        "",
    ]
    for section in server_sections:
        lines += render_section(str(section["title"]), section["rows"])

    return "\n".join(lines).rstrip("\n") + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail when the page is stale")
    parser.add_argument("--write", action="store_true", help="write the generated page")
    args = parser.parse_args()
    if args.check == args.write:
        parser.error("choose exactly one of --check or --write")

    data = json.loads(CATALOG.read_text(encoding="utf-8"))
    expected = render(data)
    if args.check:
        current = PAGE.read_text(encoding="utf-8") if PAGE.exists() else ""
        if current != expected:
            print(
                "capability-matrix: support-matrix.md is stale; run "
                "python scripts/generate_docs_capability_matrix.py --write"
            )
            return 1
        print("capability-matrix: support-matrix.md OK")
        return 0

    PAGE.write_text(expected, encoding="utf-8")
    print("capability-matrix: support-matrix.md updated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
