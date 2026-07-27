#!/usr/bin/env python
"""Render the root README capability matrices from their structured catalog."""

from __future__ import annotations

import argparse
from html import escape
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
CATALOG = ROOT / "docs" / "capability-matrix.json"
START = "## Feature status\n"
END = "\n## What we have today\n"
STATUS = {"shipped": "✅", "partial": "🚧", "planned": "📋", "na": "—"}
SCRIPT_COLUMNS = ("lua", "python", "javascript", "rust_game_logic")
PLATFORM_ICONS = {"windows": "🪟", "macos": "🍎", "linux": "🐧"}
CLIENT_PLATFORM_ROWS = {
    "unity": "Unity SDK package",
    "unreal": "Unreal plugin package",
    "godot": "Godot SDK package",
    "web": "Web / JavaScript SDK",
    "rust": "Rust client crate and C ABI source",
}


def cell(value: str) -> str:
    try:
        return STATUS[value]
    except KeyError as error:
        raise ValueError(f"unknown capability status: {value}") from error


def html_row(cells: list[str]) -> str:
    rendered_cells = "".join(f"<td>{escape(value)}</td>" for value in cells)
    return f"    <tr>{rendered_cells}</tr>"


def grouped_html_table(
    headers: list[str], sections: list[dict[str, object]], row_builder: object
) -> list[str]:
    lines = ["<table>", "  <thead>", "    <tr>"]
    lines.extend(f"      <th scope=\"col\">{escape(header)}</th>" for header in headers)
    lines.extend(["    </tr>", "  </thead>"])

    for section in sections:
        rows = section["rows"]
        if not rows:
            continue
        lines.extend(["  <tbody>", f"    <tr><th colspan=\"{len(headers)}\" align=\"left\">{escape(section['title'])}</th></tr>"])
        lines.extend(html_row(row_builder(row)) for row in rows)
        lines.append("  </tbody>")

    lines.append("</table>")
    return lines


def with_server_capabilities(data: dict[str, object]) -> list[dict[str, object]]:
    sections = []
    for section in data["server_sections"]:
        rows = [row for row in section["rows"] if row["common"] != "na"]
        sections.append({"title": section["title"], "rows": rows})
    return sections


def with_script_capabilities(data: dict[str, object]) -> list[dict[str, object]]:
    sections = []
    for section in data["server_sections"]:
        rows = [
            row
            for row in section["rows"]
            if any(row[column] != "na" for column in SCRIPT_COLUMNS)
        ]
        sections.append({"title": section["title"], "rows": rows})
    return sections


def client_delivery(data: dict[str, object]) -> dict[str, dict[str, str]]:
    rows_by_capability = {row["capability"]: row for row in data["platform_rows"]}
    try:
        return {
            target: rows_by_capability[capability]
            for target, capability in CLIENT_PLATFORM_ROWS.items()
        }
    except KeyError as error:
        raise ValueError(f"missing delivery row for {error.args[0]}") from error


def client_os_cell(feature_status: str, delivery: dict[str, str]) -> str:
    if feature_status in {"na", "planned"}:
        return "—"
    if feature_status not in STATUS:
        raise ValueError(f"unknown capability status: {feature_status}")

    icons = " ".join(
        PLATFORM_ICONS[platform]
        for platform in PLATFORM_ICONS
        if delivery[platform] == "shipped"
    )
    if not icons:
        return "—"
    return f"{icons} 🚧" if feature_status == "partial" else icons


def render(data: dict[str, object]) -> str:
    delivery = client_delivery(data)
    lines = [
        "## Feature status",
        "",
        "<!-- Generated from docs/capability-matrix.json; do not edit this section by hand. -->",
        "",
        "Three independent, evidence-backed views of the product surface.",
        "Server and game-script statuses use `✅` shipped, `🚧` partial, `📋` planned, and `—` not applicable.",
        "",
        "### Core server features",
        "",
    ]
    lines.extend(
        grouped_html_table(
            ["Feature", "Brief description", "Status"],
            with_server_capabilities(data),
            lambda row: [row["capability"], row["detail"], cell(row["common"])],
        )
    )

    lines.extend([
        "",
        "### Game-script layer",
        "",
        "Only game-script capabilities appear here; Rust denotes the planned Citadel-as-a-crate and hardened WASM game-logic paths.",
        "",
    ])
    lines.extend(
        grouped_html_table(
            ["Feature", "Lua", "Python", "JavaScript", "Rust game logic", "Brief description"],
            with_script_capabilities(data),
            lambda row: [
                row["capability"],
                cell(row["lua"]),
                cell(row["python"]),
                cell(row["javascript"]),
                cell(row["rust_game_logic"]),
                row["detail"],
            ],
        )
    )

    lines.extend([
        "",
        "### Client SDK readiness by engine and OS",
        "",
        "Each engine cell lists the OSes with a released/tested delivery path: `🪟` Windows, `🍎` macOS, `🐧` Linux. `🚧` after an icon means that engine binding is partial; `—` means no usable feature path yet. Rust is retained as a non-engine client target so its shipped SDK surface is not hidden.",
        "",
    ])
    lines.extend(
        grouped_html_table(
            ["Feature", "Unity", "Unreal", "Godot", "Web / JS", "Rust client", "Brief description"],
            data["client_sections"],
            lambda row: [
                row["capability"],
                client_os_cell(row["unity"], delivery["unity"]),
                client_os_cell(row["unreal"], delivery["unreal"]),
                client_os_cell(row["godot"], delivery["godot"]),
                client_os_cell(row["web"], delivery["web"]),
                client_os_cell(row["rust"], delivery["rust"]),
                row["detail"],
            ],
        )
    )
    return "\n".join(lines) + "\n"


def replace_feature_section(readme: str, section: str) -> str:
    start = readme.find(START)
    if start == -1:
        raise ValueError("README has no Feature status heading")
    end = readme.find(END, start)
    if end == -1:
        raise ValueError("README has no following What we have today heading")
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
