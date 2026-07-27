#!/usr/bin/env bash
set -euo pipefail

# Documentation gate (hard, pre-develop).
#
# Rules enforced:
#   1. Client-facing code changes (services, http, realtime, runtime, wire,
#      client crates, engine SDKs) REQUIRE website product/API docs updates
#      (website/src/content/docs/**). Journals, plans, and internal docs do
#      NOT satisfy this — the website is the canonical developer-facing
#      documentation.
#   2. Other production code changes require at least internal docs
#      (docs/**, README.md, CHANGELOG.md) or website docs.
#   3. Test-only / tooling-only changes require nothing.
#
# Escape hatch: a commit in the range (or HEAD when diffing the worktree) may
# carry a "Docs-Exempt: <reason>" trailer. The exemption is printed loudly so
# reviewers see it; use it only for changes with genuinely no doc impact.

base_ref="${1:-}"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  exit 0
fi

# Default base: origin/develop, then local develop (the repo's main branch is
# `develop`; the old default of origin/main never existed, which silently
# disabled this gate on committed feature branches).
if [[ -z "$base_ref" ]]; then
  for candidate in origin/develop develop origin/main main; do
    if git rev-parse --verify "$candidate" >/dev/null 2>&1; then
      base_ref="$candidate"
      break
    fi
  done
fi

changed_files=""
range=""

if [[ -n "$base_ref" ]] && git rev-parse --verify "$base_ref" >/dev/null 2>&1; then
  merge_base="$(git merge-base HEAD "$base_ref" || true)"
  if [[ -n "$merge_base" ]]; then
    range="$merge_base..HEAD"
    changed_files="$(git diff --name-only "$merge_base"...HEAD)"
  fi
fi

# Always include uncommitted work so the gate also guards pre-commit runs.
uncommitted="$(git diff --name-only --cached; git diff --name-only)"
changed_files="$(printf '%s\n%s' "$changed_files" "$uncommitted" | sed '/^$/d' | sort -u)"

if [[ -z "$changed_files" ]]; then
  exit 0
fi

client_facing_changed="false"
internal_code_changed="false"
website_docs_changed="false"
internal_docs_changed="false"

while IFS= read -r file; do
  case "$file" in
    src/services/*|src/http/*|src/realtime/*|src/runtime/*|src/session/*|src/identity/*|src/cli.rs|src/config/*|crates/*|clients/*|proto/*|migrations/*)
      client_facing_changed="true"
      ;;
    src/*|Cargo.toml)
      internal_code_changed="true"
      ;;
  esac

  case "$file" in
    website/src/content/docs/*)
      website_docs_changed="true"
      ;;
    docs/*|website/README.md|README.md|CHANGELOG.md)
      internal_docs_changed="true"
      ;;
  esac
done <<< "$changed_files"

# Docs-Exempt trailer: check commits in range and, as a fallback, HEAD.
docs_exempt=""
if [[ -n "$range" ]]; then
  docs_exempt="$(git log --format='%(trailers:key=Docs-Exempt,valueonly)' "$range" 2>/dev/null | sed '/^$/d' | head -1 || true)"
fi
if [[ -z "$docs_exempt" ]]; then
  docs_exempt="$(git log -1 --format='%(trailers:key=Docs-Exempt,valueonly)' 2>/dev/null | sed '/^$/d' | head -1 || true)"
fi

if [[ "$client_facing_changed" == "true" && "$website_docs_changed" != "true" ]]; then
  if [[ -n "$docs_exempt" ]]; then
    echo "check-docs: WARNING — client-facing code changed without website docs."
    echo "check-docs: proceeding only because of Docs-Exempt trailer: $docs_exempt"
  else
    echo "check-docs: FAIL — client-facing code changed without website documentation."
    echo ""
    echo "Client-facing changes (src/services, src/http, src/realtime, src/runtime,"
    echo "crates/*, clients/*, config/CLI, migrations) must be documented in"
    echo "website/src/content/docs/ in the same work session: a reference page with"
    echo "per-method docs and synced multi-engine code tabs. Internal records"
    echo "do not satisfy this gate."
    echo ""
    echo "If this change truly has no client-facing doc impact, add a commit trailer:"
    echo "  Docs-Exempt: <reason>"
    exit 1
  fi
fi

if [[ "$internal_code_changed" == "true" && "$website_docs_changed" != "true" && "$internal_docs_changed" != "true" ]]; then
  if [[ -n "$docs_exempt" ]]; then
    echo "check-docs: WARNING — code changed without docs; Docs-Exempt: $docs_exempt"
  else
    echo "check-docs: FAIL — code changed without documentation updates."
    echo "Update website/src/content/docs/, docs/, README.md, or CHANGELOG.md,"
    echo "or add a 'Docs-Exempt: <reason>' commit trailer."
    exit 1
  fi
fi

echo "check-docs: OK"
