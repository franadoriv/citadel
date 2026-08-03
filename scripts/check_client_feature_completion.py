#!/usr/bin/env python3
"""Verify released client SDK coverage declared in client-feature-manifest.json."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$")


def slug(value: str) -> str:
    value = value.lower().replace("`", "")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def check_manifest(root: Path, manifest_path: Path) -> list[str]:
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return [f"cannot read manifest {manifest_path}: {error}"]

    errors: list[str] = []
    targets = data.get("released_sdk_targets")
    operations = data.get("operations")
    if data.get("version") != 1:
        errors.append("manifest version must be 1")
    if not isinstance(targets, list) or not targets or not all(isinstance(item, str) for item in targets):
        errors.append("released_sdk_targets must be a non-empty list of target names")
        return errors
    if not isinstance(operations, list) or not operations:
        errors.append("operations must be a non-empty list")
        return errors

    seen_ids: set[str] = set()
    for operation in operations:
        if not isinstance(operation, dict):
            errors.append("operation must be an object")
            continue
        operation_id = operation.get("id")
        if not isinstance(operation_id, str) or not operation_id:
            errors.append("operation has no non-empty id")
            continue
        if operation_id in seen_ids:
            errors.append(f"[{operation_id}] duplicate operation id")
        seen_ids.add(operation_id)

        backend = operation.get("backend", {})
        if not isinstance(backend, dict) or not isinstance(backend.get("method"), str) or not isinstance(backend.get("path"), str):
            errors.append(f"[{operation_id}] backend must declare method and path")

        reference = operation.get("reference", {})
        reference_path = reference.get("path") if isinstance(reference, dict) else None
        anchor = reference.get("anchor") if isinstance(reference, dict) else None
        if not isinstance(reference_path, str) or not isinstance(anchor, str):
            errors.append(f"[{operation_id}] reference must declare path and anchor")
        else:
            doc_path = root / reference_path
            if not doc_path.is_file():
                errors.append(f"[{operation_id}] reference file missing: {reference_path}")
            else:
                headings = {slug(match.group(2)) for line in doc_path.read_text(encoding="utf-8").splitlines() if (match := HEADING.match(line))}
                if anchor not in headings:
                    errors.append(f"[{operation_id}] reference anchor missing: {reference_path}#{anchor}")

        bindings = operation.get("bindings", {})
        exclusions = operation.get("exclusions", {})
        if not isinstance(bindings, dict):
            errors.append(f"[{operation_id}] bindings must be an object")
            continue
        if not isinstance(exclusions, dict):
            errors.append(f"[{operation_id}] exclusions must be an object")
            continue
        for target in targets:
            binding = bindings.get(target)
            exclusion = exclusions.get(target)
            if binding is not None and exclusion is not None:
                errors.append(f"[{operation_id}] [{target}] cannot be both bound and excluded")
                continue
            if exclusion is not None:
                if not isinstance(exclusion, str) or not exclusion.strip():
                    errors.append(f"[{operation_id}] [{target}] exclusion requires a non-empty reason")
                continue
            if not isinstance(binding, dict):
                errors.append(f"[{operation_id}] missing released SDK binding: {target}")
                continue
            source_path = binding.get("path")
            symbol = binding.get("symbol")
            if not isinstance(source_path, str) or not isinstance(symbol, str) or not symbol:
                errors.append(f"[{operation_id}] invalid binding declaration: {target}")
                continue
            source = root / source_path
            if not source.is_file():
                errors.append(f"[{operation_id}] [{target}] binding source missing: {source_path}")
            elif symbol not in source.read_text(encoding="utf-8"):
                errors.append(f"[{operation_id}] [{target}] binding symbol missing: {symbol} in {source_path}")
        for target in bindings:
            if target not in targets:
                errors.append(f"[{operation_id}] binding target is not released: {target}")
        for target in exclusions:
            if target not in targets:
                errors.append(f"[{operation_id}] exclusion target is not released: {target}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", default="manifests/client-feature-manifest.json")
    parser.add_argument("--expect-failure", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    errors = check_manifest(root, root / args.manifest)
    if args.expect_failure:
        if errors:
            print("check-client-feature-completion: negative fixture rejected as expected")
            return 0
        print("check-client-feature-completion: negative fixture unexpectedly passed", file=sys.stderr)
        return 1
    if errors:
        print("check-client-feature-completion: FAIL", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print("check-client-feature-completion: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
