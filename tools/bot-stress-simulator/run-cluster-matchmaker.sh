#!/usr/bin/env bash
# Launch an ephemeral two-node matchmaker cluster and run the cross-node probe.
# Requires PostgreSQL/CockroachDB because Citadel deliberately rejects SQLite
# for cluster leases and fencing.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/citadel-cluster-stress.XXXXXX")"
BIN="${CITADEL_BIN:-$ROOT/target/release/citadel}"
DB_URL="${CITADEL_CLUSTER_DATABASE_URL:-}"

cleanup() {
  local status=$?
  if [[ -n "${NODE_A_PID:-}" ]]; then kill "$NODE_A_PID" 2>/dev/null || true; fi
  if [[ -n "${NODE_B_PID:-}" ]]; then kill "$NODE_B_PID" 2>/dev/null || true; fi
  wait "${NODE_A_PID:-}" "${NODE_B_PID:-}" 2>/dev/null || true
  # The directory contains throwaway private keys. `trash` is recoverable and
  # avoids an irreversible recursive deletion; if it is unavailable we retain
  # the directory and print its location so the operator can remove it safely.
  if command -v trash >/dev/null; then
    trash "$WORK"
  elif command -v gio >/dev/null; then
    gio trash "$WORK"
  else
    echo "Temporary cluster material retained at: $WORK" >&2
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

if [[ -z "$DB_URL" ]]; then
  echo "Set CITADEL_CLUSTER_DATABASE_URL to an ephemeral PostgreSQL or CockroachDB URL." >&2
  exit 2
fi
if ! command -v openssl >/dev/null; then
  echo "openssl is required to generate the throwaway cluster certificates." >&2
  exit 2
fi
if [[ ! -x "$BIN" ]]; then
  (cd "$ROOT" && cargo build --release)
fi

mkdir -p "$WORK/certs" "$WORK/game"
cp "$ROOT/tools/bot-stress-simulator/server/game/"*.lua "$WORK/game/"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=citadel-cluster-stress-ca' \
  -keyout "$WORK/certs/ca-key.pem" -out "$WORK/certs/ca.pem" >/dev/null 2>&1
make_leaf() {
  local node=$1
  openssl req -newkey rsa:2048 -nodes -subj "/CN=${node}.local" \
    -keyout "$WORK/certs/$node-key.pem" -out "$WORK/certs/$node.csr" >/dev/null 2>&1
  printf 'subjectAltName=DNS:%s.local\nextendedKeyUsage=serverAuth,clientAuth\n' "$node" > "$WORK/certs/$node.ext"
  openssl x509 -req -days 1 -CA "$WORK/certs/ca.pem" -CAkey "$WORK/certs/ca-key.pem" \
    -CAcreateserial -in "$WORK/certs/$node.csr" -out "$WORK/certs/$node.pem" \
    -extfile "$WORK/certs/$node.ext" >/dev/null 2>&1
}
make_leaf node-a
make_leaf node-b

write_config() {
  local node=$1 http=$2 ws=$3 control=$4 peer=$5 peer_control=$6
  cat > "$WORK/$node.toml" <<EOF
[server]
node_id = "$node"
public_addr = "127.0.0.1:$http"

[http]
bind = "127.0.0.1:$http"

[database]
url = "$DB_URL"

[authentication.limits]
source = { limit = 10000, window_ms = 60000 }
email = { limit = 10000, window_ms = 900000 }
registration_source = { limit = 10000, window_ms = 3600000 }

[runtime]
enabled = true
language = "lua"
adapter = "embedded"
tier = "trusted"
scripts_dir = "$WORK/game"
tick_hz = 4

[transport.quic]
enabled = false

[transport.websocket]
enabled = true
bind = "127.0.0.1:$ws"

[cluster]
enabled = true
control_bind = "127.0.0.1:$control"
matchmaker_shard = 0
lease_ttl_ms = 5000
handoff_ttl_ms = 30000
command_timeout_ms = 2000

[cluster.tls]
ca_certificate_file = "$WORK/certs/ca.pem"
certificate_file = "$WORK/certs/$node.pem"
private_key_file = "$WORK/certs/$node-key.pem"

[[cluster.peers]]
node_id = "$peer"
control_addr = "127.0.0.1:$peer_control"
server_name = "$peer.local"
certificate_file = "$WORK/certs/$peer.pem"
EOF
}
write_config node-a 7350 7352 7390 node-b 7391
write_config node-b 7354 7356 7391 node-a 7390

"$BIN" check --config "$WORK/node-a.toml"
"$BIN" check --config "$WORK/node-b.toml"
"$BIN" serve --config "$WORK/node-a.toml" >"$WORK/node-a.log" 2>&1 & NODE_A_PID=$!
"$BIN" serve --config "$WORK/node-b.toml" >"$WORK/node-b.log" 2>&1 & NODE_B_PID=$!
for _ in $(seq 1 50); do
  if curl -fsS http://127.0.0.1:7350/health >/dev/null 2>&1 && curl -fsS http://127.0.0.1:7354/health >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -fsS http://127.0.0.1:7350/health >/dev/null
curl -fsS http://127.0.0.1:7354/health >/dev/null

echo "Two nodes ready. The probe will ask for the bot count and endpoints."
(cd "$ROOT/tools/bot-stress-simulator/client" && cargo run --release --bin cluster-matchmaker)
