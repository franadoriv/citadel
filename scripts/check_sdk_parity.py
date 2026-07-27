#!/usr/bin/env python3
"""Tier-A declared-constant parity for Citadel client SDKs.

Reads the canonical client contract (``crates/citadel-wire/contract.json``) and,
for every SDK discovered via ``clients/*/sdk.manifest.json``, parses that SDK's
declared constants out of its source files and diffs the values it *claims* to
implement against the contract. A value mismatch, or a claimed canonical key the
SDK omits, is a non-zero exit with a diff naming the SDK, the key, and expected
vs actual. Extra SDK-only constants are allowed but reported.

The check is intentionally toolchain-free: it never compiles the SDKs or runs a
language runtime, it only regex-parses declared constant literals. That covers
constant/layout parity for every engine; marshaling/endianness and native
signature correctness are covered by per-SDK tests and (for Unreal) the Tier-B
hook, per docs/architecture/client-sdk-sync.md.
"""

from __future__ import annotations

import glob
import json
import os
import re
import sys

# Regex parsers per declared-constant format. Each yields {NAME: int} for the
# integer-literal constants declared in a source file. Non-integer or
# expression-valued constants (e.g. `A + 1`) are ignored; SDKs must declare the
# claimed contract values as plain integer literals.
_CSHARP_RE = re.compile(
    r"public\s+const\s+"
    r"(?:byte|sbyte|short|ushort|int|uint|long|ulong)\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
    r"(?P<value>0[xX][0-9A-Fa-f]+|\d+)\s*;"
)
_GDSCRIPT_RE = re.compile(
    r"const\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?::\s*int\s*)?:=\s*"
    r"(?P<value>0[xX][0-9A-Fa-f]+|\d+)\b"
)
_CPP_RE = re.compile(
    r"constexpr\s+\w+\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
    r"(?P<value>0[xX][0-9A-Fa-f]+|\d+)"
)
_JS_RE = re.compile(
    r"export\s+const\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
    r"(?P<value>0[xX][0-9A-Fa-f]+|\d+)\s*;"
)

_PARSERS = {
    "csharp": _CSHARP_RE,
    "gdscript": _GDSCRIPT_RE,
    "cpp": _CPP_RE,
    "js": _JS_RE,
}


def _parse_int(text: str) -> int:
    return int(text, 16) if text.lower().startswith("0x") else int(text)


def parse_constants(path: str, fmt: str) -> dict[str, int]:
    regex = _PARSERS.get(fmt)
    if regex is None:
        raise ValueError(f"unknown constants_format {fmt!r}")
    with open(path, "r", encoding="utf-8") as handle:
        source = handle.read()
    found: dict[str, int] = {}
    for match in regex.finditer(source):
        found[match.group("name")] = _parse_int(match.group("value"))
    return found


def check_sdk(repo_root: str, manifest_path: str, contract: dict) -> tuple[list[str], list[str]]:
    """Return (errors, notes) for a single SDK manifest."""
    errors: list[str] = []
    notes: list[str] = []

    sdk_dir = os.path.dirname(manifest_path)
    rel_sdk = os.path.relpath(sdk_dir, repo_root).replace(os.sep, "/")
    with open(manifest_path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)

    engine = manifest.get("engine", rel_sdk)
    fmt = manifest.get("constants_format")
    files = manifest.get("constants_files", [])
    if not fmt or not files:
        errors.append(f"[{engine}] manifest missing constants_format/constants_files")
        return errors, notes

    # Build one merged {NAME: value} map from all declared-constant files.
    declared: dict[str, int] = {}
    for rel in files:
        abs_path = os.path.join(sdk_dir, rel)
        if not os.path.isfile(abs_path):
            errors.append(f"[{engine}] declared-constant file not found: {rel}")
            continue
        try:
            declared.update(parse_constants(abs_path, fmt))
        except ValueError as exc:
            errors.append(f"[{engine}] {exc}")

    contract_wire = contract.get("wire", {})
    claimed_local_names: set[str] = set()

    # Tier-A: every claimed wire key must exist in the contract and match value.
    for canonical_key, local_name in manifest.get("wire", {}).items():
        claimed_local_names.add(local_name)
        if canonical_key not in contract_wire:
            errors.append(
                f"[{engine}] claims key {canonical_key!r} that is not in the "
                f"canonical contract"
            )
            continue
        expected = contract_wire[canonical_key]
        if local_name not in declared:
            errors.append(
                f"[{engine}] claims {canonical_key} (as {local_name}) but no such "
                f"constant is declared in {files}"
            )
            continue
        actual = declared[local_name]
        if actual != expected:
            errors.append(
                f"[{engine}] {canonical_key} (as {local_name}) drift: "
                f"expected {expected}, SDK declares {actual}"
            )

    # ABI version parity (against contract.json abi_version, itself generated
    # from CITADEL_FFI_ABI_VERSION and identical to the header #define).
    abi_local = manifest.get("abi_version")
    if abi_local:
        claimed_local_names.add(abi_local)
        expected_abi = contract.get("abi_version")
        if abi_local not in declared:
            errors.append(
                f"[{engine}] claims ABI version constant {abi_local} but it is not "
                f"declared in {files}"
            )
        elif declared[abi_local] != expected_abi:
            errors.append(
                f"[{engine}] ABI version drift: expected {expected_abi}, "
                f"SDK declares {declared[abi_local]}"
            )

    # Extra SDK-only declared constants (engine helpers) are allowed but reported.
    extras = sorted(name for name in declared if name not in claimed_local_names)
    if extras:
        notes.append(f"[{engine}] extra SDK-only constants (not compared): {', '.join(extras)}")

    return errors, notes


def main(argv: list[str]) -> int:
    repo_root = argv[1] if len(argv) > 1 else os.getcwd()
    contract_path = os.path.join(repo_root, "crates", "citadel-wire", "contract.json")
    if not os.path.isfile(contract_path):
        print(f"check-sdk-parity: contract not found at {contract_path}", file=sys.stderr)
        return 1
    with open(contract_path, "r", encoding="utf-8") as handle:
        contract = json.load(handle)

    pattern = os.path.join(repo_root, "clients", "*", "sdk.manifest.json")
    manifests = sorted(glob.glob(pattern))
    if not manifests:
        print("check-sdk-parity: no clients/*/sdk.manifest.json found; nothing to check")
        return 0

    all_errors: list[str] = []
    for manifest_path in manifests:
        errors, notes = check_sdk(repo_root, manifest_path, contract)
        all_errors.extend(errors)
        for note in notes:
            print(f"note: {note}")

    if all_errors:
        print("\ncheck-sdk-parity: SDK contract drift detected:", file=sys.stderr)
        for err in all_errors:
            print(f"  - {err}", file=sys.stderr)
        print(
            "\nFix the SDK's declared constants, or if the contract itself "
            "changed, regenerate crates/citadel-wire/contract.json "
            "(CITADEL_REGEN_CONTRACT=1 cargo test -p citadel-client-ffi "
            "--test contract_manifest) and re-sync the SDKs.",
            file=sys.stderr,
        )
        return 1

    checked = ", ".join(os.path.basename(os.path.dirname(m)) for m in manifests)
    print(f"check-sdk-parity: Tier-A parity OK for {len(manifests)} SDK(s): {checked}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
