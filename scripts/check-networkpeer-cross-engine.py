#!/usr/bin/env python3
"""Semantic cross-engine NetworkPeer golden-vector gate.

The valid byte strings are pinned canonical Rust `citadel-wire` encoder output.
This gate validates the fixture's semantic shape, then executes the JavaScript
adapter test that decodes those fixed strings and compares every expected field.
It deliberately does not infer parity from adapter source text.
"""
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fixture_path = ROOT / "tests/fixtures/networkpeer-cross-engine-v1.json"
fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
errors = []

if fixture.get("fixture_version") != 1:
    errors.append("fixture_version must be 1")
if fixture.get("runtime_matrix", {}).get("status") != "deferred_external_environment":
    errors.append("runtime matrix must accurately remain deferred")
for name, relative in fixture.get("required_adapters", {}).items():
    if not isinstance(relative, str) or not (ROOT / relative).is_file():
        errors.append(f"{name} adapter missing")

vectors = fixture.get("golden_vectors")
if not isinstance(vectors, list) or len(vectors) != 4:
    errors.append("fixture must contain exactly four golden vectors")
else:
    by_id = {vector.get("id"): vector for vector in vectors if isinstance(vector, dict)}
    required = {
        "canonical_full_all_value_families",
        "canonical_delta_u32_rep_id_boundaries",
        "reject_rep_id_index_above_u32",
        "reject_rep_id_generation_above_u32",
    }
    if set(by_id) != required:
        errors.append("golden vector ids are incomplete or duplicated")
    for vector in vectors:
        encoded = vector.get("encoded_hex")
        if not isinstance(encoded, str) or not encoded or len(encoded) % 2 or not re.fullmatch(r"[0-9a-f]+", encoded):
            errors.append(f"{vector.get('id', '<unknown>')} has invalid encoded_hex")
        if not isinstance(vector.get("canonical_source"), str) or not vector["canonical_source"]:
            errors.append(f"{vector.get('id', '<unknown>')} lacks canonical_source")

    full = by_id.get("canonical_full_all_value_families", {}).get("expected", {})
    expected_full_fields = {"0", "1", "2", "3", "4", "5", "6"}
    if (full.get("object_id"), full.get("is_full"), full.get("result_id"), full.get("base_id")) != (9, True, "3", "0"):
        errors.append("full vector header expected result is not canonical")
    if set(full.get("changes", {})) != expected_full_fields:
        errors.append("full vector must semantically cover every schema field")
    if full.get("changes", {}).get("5", {}).get("bytes_hex") != "6369746164656c":
        errors.append("full vector bytes expected result is invalid")

    boundary = by_id.get("canonical_delta_u32_rep_id_boundaries", {}).get("expected", {})
    removed = boundary.get("changes", {}).get("6", {}).get("removed", [])
    if (boundary.get("object_id"), boundary.get("is_full"), boundary.get("result_id"), boundary.get("base_id")) != (9, False, "5", "3"):
        errors.append("u32 boundary vector header expected result is not canonical")
    if removed != [{"index": 4294967295, "generation": 4294967295}]:
        errors.append("u32 boundary vector must decode both RepId fields at u32::MAX")

    overflow = by_id.get("reject_rep_id_index_above_u32", {})
    if overflow.get("expected_error") != "rep id index must fit u32":
        errors.append("overflow vector must require u32 RepId index rejection")

    generation_overflow = by_id.get("reject_rep_id_generation_above_u32", {})
    if generation_overflow.get("expected_error") != "rep id generation must fit u32":
        errors.append("overflow vector must require u32 RepId generation rejection")

case_ids = set(fixture.get("cases", []))
if not {"canonical_full_all_value_families", "canonical_delta_u32_rep_id_boundaries", "reject_rep_id_index_above_u32", "reject_rep_id_generation_above_u32"}.issubset(case_ids):
    errors.append("fixture cases omit semantic golden coverage")
if errors:
    raise SystemExit("check-networkpeer-cross-engine: " + "; ".join(errors))

# Execute behavior, not source-string presence: JS must decode canonical Rust
# bytes to these expected results and reject the canonical-overflow violation.
subprocess.run(
    ["node", "--test", "--test-name-pattern=canonical Rust semantic golden", "clients/js/test/networkpeer.test.js"],
    cwd=ROOT,
    check=True,
)
print("check-networkpeer-cross-engine: OK (semantic Rust golden vectors; runtime matrix deferred)")
