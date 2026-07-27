#!/usr/bin/env bash
set -euo pipefail

# Client documentation parity guard (/).
#
# JavaScript is a shipped WebSocket SDK, but it intentionally does not expose
# the binary room/netcode/map helpers. These RPC/inbox pages are the client
# surfaces it does expose, so every synced engine tab on them must include it.

required=(
  website/src/content/docs/guides/install-client-sdk.mdx
  website/src/content/docs/reference/client-sdk/authentication.md
  website/src/content/docs/reference/client-sdk/friends.mdx
  website/src/content/docs/reference/client-sdk/matchmaker.mdx
  website/src/content/docs/reference/client-sdk/parties.mdx
  website/src/content/docs/reference/client-sdk/notifications.mdx
)

failed=0
for page in "${required[@]}"; do
  tabs="$(grep -c '<Tabs syncKey="engine">' "$page" || true)"
  js="$(grep -c '<TabItem label="JavaScript">' "$page" || true)"
  if [[ "$tabs" -ne "$js" ]]; then
    echo "check-client-doc-tabs: $page has $tabs engine tab sets but $js JavaScript tabs"
    failed=1
  fi
done

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi
echo "check-client-doc-tabs: OK"
