#!/usr/bin/env python3
"""Validate the public reference contract pinned by TASK-0362.

The manifest is intentionally a finite, reviewable fixture rather than a broad
crawler: it proves the researched 47 cross-bucket links still resolve, derives
the reserved wire-kind range from Rust, and compares selected reference claims
with the capability catalog. It is expected to fail until TASK-0361 replaces the
recorded obsolete reference wording.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / "website/src/content/docs/reference"
MANIFEST = ROOT / "tests/fixtures/reference-contract-manifest.json"
PROTOCOL = ROOT / "crates/citadel-wire/src/protocol.rs"
MATRIX = ROOT / "docs/capability-matrix.json"
LINK = re.compile(r"(?<!!)\[[^]]*]\(([^)\s]+)(?:\s+[^)]*)?\)")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$")
CONST = re.compile(r"pub const (KIND_[A-Z0-9_]+): u16 = (\d+);")


def norm_route(route: str) -> str:
    page, sep, fragment = route.partition("#")
    page = page.rstrip("/") or "/"
    return page + ("#" + fragment if sep else "")


def heading_slug(value: str) -> str:
    value = re.sub(r"`([^`]*)`", r"\1", value)
    # Angle brackets in command names (for example bin-client-<engine>) are
    # literal text, not HTML tags, in the reference headings.
    value = value.replace("<", "").replace(">", "")
    value = unquote(value).lower().strip()
    value = re.sub(r"[^a-z0-9 _-]", "", value)
    return re.sub(r"[ _]+", "-", value).strip("-")


def target_page(route: str) -> Path | None:
    page = route.partition("#")[0].rstrip("/")
    if not page.startswith("/reference/"):
        return None
    relative = page.removeprefix("/reference/")
    candidates = [REFERENCE / f"{relative}.md", REFERENCE / f"{relative}.mdx", REFERENCE / relative / "index.mdx"]
    return next((candidate for candidate in candidates if candidate.is_file()), None)


def all_matrix_rows(matrix: dict):
    for section in matrix.get("server_sections", []) + matrix.get("client_sections", []):
        yield from section.get("rows", [])
    yield from matrix.get("platform_rows", [])


def check_links(manifest: dict, errors: list[str]) -> None:
    entries = manifest["cross_bucket_links"]
    if len(entries) != 47:
        errors.append(f"manifest must contain exactly 47 cross-bucket links, found {len(entries)}")
    for source_anchor, expected in entries:
        file_name, line_number = source_anchor.rsplit(":", 1)
        source = REFERENCE / file_name
        if not source.is_file():
            errors.append(f"{source_anchor}: source page does not exist")
            continue
        lines = source.read_text(encoding="utf-8").splitlines()
        try:
            line = lines[int(line_number) - 1]
        except (IndexError, ValueError):
            errors.append(f"{source_anchor}: source line does not exist")
            continue
        # A source anchor may start a Markdown link whose URL wraps onto the
        # next line. Keep the locator precise while accepting that formatting.
        paragraph = line
        for continuation in lines[int(line_number):]:
            if not continuation.strip():
                break
            paragraph += "\n" + continuation
        links = [norm_route(url) for url in LINK.findall(paragraph)]
        if norm_route(expected) not in links:
            errors.append(f"{source_anchor}: expected link {expected!r} is absent from its paragraph (found {links or 'none'})")
            continue
        destination = target_page(expected)
        if destination is None:
            errors.append(f"{source_anchor}: destination page for {expected!r} does not exist")
            continue
        fragment = expected.partition("#")[2]
        if fragment:
            headings = {heading_slug(match.group(1)) for line in destination.read_text(encoding="utf-8").splitlines() if (match := HEADING.match(line))}
            if fragment not in headings:
                errors.append(f"{source_anchor}: fragment #{fragment} is absent from {destination.relative_to(ROOT)}")


def check_reserved_kinds(errors: list[str]) -> None:
    values = {int(value) for _, value in CONST.findall(PROTOCOL.read_text(encoding="utf-8"))}
    expected = set(range(1, 29))
    missing = sorted(expected - values)
    if missing:
        errors.append(f"protocol reserved kinds must cover 1..28; missing {missing}")
    else:
        print("reference-contract: derived reserved kinds 1..28 from protocol.rs")

    reference_text = "\n".join(path.read_text(encoding="utf-8") for path in REFERENCE.rglob("*.*") if path.suffix in {".md", ".mdx"})
    obsolete = [
        (r"custom(?:\s+(?:game|application))?\s+(?:envelope\s+)?(?:kind|traffic)[^.\n]{0,100}(?:[1-9]\d?|[1-9])\b", "custom-traffic advice below 100"),
        (r">=\s*27", "obsolete >=27 reserved-kind advice"),
        (r"\b1\s*(?:\.\.=?|[-–])\s*25\b", "obsolete 1..25 reserved-kind advice"),
    ]
    for pattern, label in obsolete:
        if re.search(pattern, reference_text, flags=re.IGNORECASE):
            errors.append(f"reference content contains {label}")
    if not re.search(r"custom(?:\s+(?:game|application))?\s+(?:envelope\s+)?(?:kind|traffic)[^.\n]{0,100}>=\s*100", reference_text, flags=re.IGNORECASE):
        errors.append("reference content must explicitly advise custom traffic at >=100")


def check_capability_claims(manifest: dict, errors: list[str]) -> None:
    rows = {row.get("capability"): row for row in all_matrix_rows(json.loads(MATRIX.read_text(encoding="utf-8")))}
    for claim in manifest["capability_claims"]:
        row = rows.get(claim["capability"])
        if row is None:
            errors.append(f"capability matrix is missing {claim['capability']!r}")
        elif row.get(claim["field"]) != claim["expected"]:
            errors.append(f"matrix {claim['capability']!r}.{claim['field']} must be {claim['expected']!r}, found {row.get(claim['field'])!r}")
        page = REFERENCE / claim["page"]
        if not page.is_file():
            errors.append(f"claim page {claim['page']} does not exist")
        elif claim["required_text"].casefold() not in page.read_text(encoding="utf-8").casefold():
            errors.append(f"{claim['page']} must state {claim['required_text']!r} for catalog claim {claim['capability']!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--links-only", action="store_true", help="run only the 47-link resolver")
    parser.add_argument("--reserved-only", action="store_true", help="run only reserved-kind advice validation")
    parser.add_argument("--capability-only", action="store_true", help="run only capability-claim validation")
    args = parser.parse_args()
    selected = sum((args.links_only, args.reserved_only, args.capability_only))
    if selected > 1:
        parser.error("choose at most one focused check")

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    errors: list[str] = []
    if not selected or args.links_only:
        check_links(manifest, errors)
    if not selected or args.reserved_only:
        check_reserved_kinds(errors)
    if not selected or args.capability_only:
        check_capability_claims(manifest, errors)
    if errors:
        print("reference-contract: FAIL")
        print("\n".join(f"- {error}" for error in errors))
        return 1
    scope = "47 links" if args.links_only else "reserved kinds" if args.reserved_only else "capability claims" if args.capability_only else "47 links, reserved kinds 1..28, capability claims"
    print(f"reference-contract: OK ({scope})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
