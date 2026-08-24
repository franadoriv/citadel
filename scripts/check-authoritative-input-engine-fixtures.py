#!/usr/bin/env python3
"""Verify engine V1 codec tests consume the shared canonical fixture."""
from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path.cwd()
    fixture_path = root / "clients/authoritative-input-fixtures.json"
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
    required_fixture_values = (
        fixture.get("sequenced_input", {}).get("hex"),
        fixture.get("input_receipt", {}).get("hex"),
    )
    if not all(isinstance(value, str) and value for value in required_fixture_values):
        print("authoritative-input engine fixtures: invalid canonical fixture", file=sys.stderr)
        return 1
    godot_fixture_path = root / "clients/godot/tests/web/authoritative-input-fixtures.json"
    if not godot_fixture_path.is_symlink() or godot_fixture_path.resolve() != fixture_path:
        print(
            "authoritative-input engine fixtures: Godot test fixture must be a "
            "symlink to clients/authoritative-input-fixtures.json",
            file=sys.stderr,
        )
        return 1

    sources = {
        "Godot": root / "clients/godot/tests/web/test_web_client.gd",
        "Unity": root / "clients/unity/Editor/tests/CitadelProtocolAuthoritativeInputTests.cs",
        "Unreal": root / "clients/unreal/test_authoritative_input_codec.py",
    }
    required_consumers = {
        "Godot": (
            'FileAccess.open("res://authoritative-input-fixtures.json", FileAccess.READ)',
            "JSON.new()",
            'fixture.get("sequenced_input", {})',
            'fixture.get("input_receipt", {})',
        ),
        "Unity": (
            "File.ReadAllText(FixturePath)",
            "JsonUtility.FromJson<AuthoritativeInputFixture>",
            "fixture.sequenced_input",
            "fixture.input_receipt",
        ),
        "Unreal": (
            "FIXTURE = json.loads(",
            'ROOT / "clients/authoritative-input-fixtures.json"',
            'FIXTURE["sequenced_input"]',
            'FIXTURE["input_receipt"]',
        ),
    }
    errors: list[str] = []
    for engine, path in sources.items():
        source = path.read_text(encoding="utf-8") if path.is_file() else ""
        missing = [needle for needle in required_consumers[engine] if needle not in source]
        if missing:
            errors.append(f"{engine} does not consume the shared fixture: {', '.join(missing)}")
    if errors:
        print("authoritative-input engine fixtures: " + "; ".join(errors), file=sys.stderr)
        return 1
    print("authoritative-input engine fixtures: Godot, Unity, and Unreal consume the shared fixture")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
