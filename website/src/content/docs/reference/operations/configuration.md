---
title: Configuration (TOML)
description: Complete Citadel TOML configuration — server, http, logging, and the three transports, with real defaults.
---

Citadel is configured by a TOML file, layered over built-in defaults, then
`CITADEL_` environment variables, then CLI flags. Unknown keys are **rejected**
(`deny_unknown_fields`). Validate any file with `citadel check`.

The config file is selected as follows: an explicit `--config <path>` is always
used as given. With **no** `--config`, Citadel discovers a `citadel.toml` in the
current working directory and loads it if present — the zero-flag "unzip and run"
default — otherwise it falls back to the built-in defaults. The repository (and
the standalone release) ships an editable `citadel.toml` at its root, so
`cargo run` / the packaged `citadel` or `citadel.exe` starts against it with no
flags. The release archive uses `scripts/` for its starter game logic; the
repository configuration may use a different `scripts_dir`.

## Full example with defaults

Every value below is the built-in default. An empty config file (or no file) is
equivalent to this.

```toml
[server]
# Stable identifier for this node within a (future) cluster.
node_id = "dev-1"
# Address other nodes/clients use to reach this node.
public_addr = "127.0.0.1:7350"

[http]
# Address the HTTP / health listener binds to.
bind = "127.0.0.1:7350"

[logging]
# Log level directive, e.g. "info", "debug", "citadel=trace".
level = "info"
# "pretty" (human-readable) or "json" (structured).
format = "pretty"

[errors]
# The journal file is always citadel-errors.jsonl beside the executable.
max_bytes = 8388608
max_entries = 2000

[transport.quic]
# QUIC is the primary realtime transport (datagrams + reliable streams, TLS 1.3).
enabled = false
bind = "127.0.0.1:7351"
outbound_queue_capacity = 1024

[transport.tls]
# Optional CA-issued PEM TLS for public QUIC and WebTransport. Set both paths
# together in production; omit both for the local self-signed development mode.
# certificate_file = "/etc/letsencrypt/live/game.example.com/fullchain.pem"
# private_key_file = "/etc/letsencrypt/live/game.example.com/privkey.pem"

[transport.websocket]
# Reliable-only fallback for browsers without WebTransport and UDP-blocked nets.
enabled = false
bind = "127.0.0.1:7352"
outbound_queue_capacity = 1024
# Native WebSocket Ping/Pong liveness after authentication. Set the interval to
# 0 only when an upstream proxy owns equivalent liveness handling.
heartbeat_interval_ms = 15000
heartbeat_timeout_ms = 45000

[transport.webtransport]
# Browser path: QUIC-grade datagrams + streams over HTTP/3 (own UDP endpoint).
enabled = false
bind = "127.0.0.1:7353"
outbound_queue_capacity = 1024

[transport.network_peer]
# Optional authoritative property replication. This attaches the gateway authority
# only; trusted server lifecycle code still registers classes and spawns objects.
enabled = false
shared_quantized_state = false
interest_cell_size = 100
interest_inner = 100
interest_outer = 125

[runtime]
# Embedded game-logic runtime. With language unset, Citadel autodetects by
# priority in scripts_dir: main.lua (default build), main.py
# (runtime-python builds), then main.js (runtime-js builds). With no entrypoint,
# the built-in relay runs.
enabled = true
# Strict GameScript readiness gate. When true, matches require a validated,
# loaded script with a healthy execution backend: the node refuses to list,
# create, or admit players into matches until one is ready, and a missing
# entrypoint boots the node not-ready instead of silently falling back to the
# relay. The first-run wizard enables this when it scaffolds a scripted project.
require_script = false
# Optional explicit language. Omit for autodetect. Supported values today:
# "lua"; "python" when compiled with --features runtime-python; "js" or
# "javascript" when compiled with --features runtime-js.
# language = "lua"
# Runtime hosting model. Only embedded/trusted is implemented today.
adapter = "embedded"
tier = "trusted"
# Lua is sandboxed even on a trusted node unless this is explicitly changed.
lua_execution_mode = "sandboxed"
scripts_dir = "./game"
maps_dir = "./maps"
# Optional, read-only JSON/CSV gameplay data root. It is separate from scripts;
# Citadel does not create or write it.
# static_data_dir = "./common"
# static_data_max_file_bytes = 1048576
# Per-invocation budget (ms) for message and lifecycle (on_join/on_leave) handlers.
deadline_ms = 100
# Server game-loop rate for citadel.on_tick, in ticks/sec. 0 disables the tick.
tick_hz = 0
# Optional per-tick budget (ms). Omit for auto: min(50, tick period / 2), min 1.
# tick_deadline_ms = 25
# Watch the selected entrypoint and reload it live on change (opt-in; dev convenience).
hot_reload = false
# Change-poll interval (ms) when hot_reload is on.
hot_reload_poll_ms = 500

# Rust-owned outbound HTTP for trusted runtime code. Empty allowed_hosts means
# any public DNS hostname; IP-literal URLs are always rejected.
[runtime.capabilities.outbound_http]
enabled = true
max_concurrent_requests = 16
max_requests_per_minute = 120
allowed_hosts = []
allowed_ports = [80, 443]
# Only enable for an operator-controlled private integration.
allow_private_networks = false

# Secure chat fixed-window policies. Every value must be positive; limits are
# shared by nodes that use the same durable database.
[chat.limits]
join = { limit = 12, window_ms = 60000 }
history = { limit = 60, window_ms = 60000 }
send_user = { limit = 8, window_ms = 10000 }
send_user_channel = { limit = 12, window_ms = 10000 }
send_channel = { limit = 160, window_ms = 10000 }
mutation_user = { limit = 4, window_ms = 60000 }
mutation_user_channel = { limit = 8, window_ms = 60000 }
moderation_operator = { limit = 30, window_ms = 60000 }
moderation_channel = { limit = 60, window_ms = 60000 }

# Public HTTP authentication admission controls. Limits are durable and shared
# by nodes using the same database. Citadel uses the direct TCP peer address;
# forwarded headers are intentionally not trusted.
[authentication.limits]
source = { limit = 30, window_ms = 60000 }
email = { limit = 10, window_ms = 900000 }
registration_source = { limit = 10, window_ms = 3600000 }

# [database] is OPTIONAL and omitted by default (no url => in-memory backend).
# Shown here with its non-url defaults; add a url to enable a durable backend.
[database]
# Connection URL. The backend is chosen by scheme. Omit to run on the in-memory
# backend. Also settable via CITADEL_DATABASE_URL. Carries credentials; never logged.
#   Postgres:    url = "postgres://citadel:citadel@localhost:5432/citadel"
#   CockroachDB: url = "cockroach://root@localhost:26257/citadel?sslmode=disable"
#   SQLite:      url = "sqlite:data.sqlite"   # one embedded file, created on first run
#   MongoDB:     url = "mongodb://user:password@db-1,db-2/citadel?replicaSet=rs0"
max_connections = 10
connect_timeout_ms = 5000
acquire_timeout_ms = 5000
# MongoDB consistency policy; the transactional foundation requires these values.
mongodb_read_preference = "primary"
mongodb_write_concern = "majority"
mongodb_read_concern = "majority"

# Disabled by default. Enabling this starts the durable multi-node matchmaker
# control listener and requires the database and certificate paths below.
[cluster]
enabled = false
control_bind = "127.0.0.1:7390"
matchmaker_shard = 0
lease_ttl_ms = 5000
handoff_ttl_ms = 30000
command_timeout_ms = 2000

[cluster.tls]
# ca_certificate_file = "./certs/cluster-ca.pem"
# certificate_file = "./certs/node.pem"
# private_key_file = "./certs/node-key.pem"

# [[cluster.peers]]
# node_id = "node-b"
# control_addr = "127.0.0.1:7391"
# server_name = "node-b.local"
# certificate_file = "./certs/node-b.pem"

# Storage indexes are optional. Each declaration is operator-controlled and
# creates a matching physical JSON-expression index at server startup.
[[storage.indexes]]
name = "profiles_by_score"
collection = "profiles"
# key = "main" # optional: restrict the index to one object key
fields = ["score", "region"]
```

## Sections

### `[server]`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `node_id` | string | `"dev-1"` | Must not be empty. |
| `public_addr` | socket address | `"127.0.0.1:7350"` | Validated as a socket address. |

### `[http]`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `bind` | socket address | `"127.0.0.1:7350"` | HTTP / health listener. Validated. |

### `[logging]`

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `level` | string | `"info"` | Tracing directive. Must not be empty. |
| `format` | enum | `"pretty"` | `"pretty"` or `"json"`. |

### `[errors]`

The local incident journal is always enabled. It writes redacted failure and
panic summaries to `citadel-errors.jsonl` beside the running executable, so a
standalone deployment keeps its diagnostics with the binary. It never stores
panic payloads, internal error detail, connection strings, tokens, or passwords.
The Error Journal section in `/dashboard` reads the retained summaries through
the authenticated console API.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `max_bytes` | integer | `8388608` | Journal size cap. Must be between `65536` and `1073741824`; oldest entries are pruned when reached. |
| `max_entries` | integer | `2000` | Maximum retained entries. Must be between `1` and `100000`. |

### `[transport.quic]`, `[transport.websocket]`, `[transport.webtransport]`

All three share the same shape:

| Key | Type | Default (quic / ws / wt) | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Whether the listener starts. |
| `bind` | socket address | `7351` / `7352` / `7353` on `127.0.0.1` | Validated **only when enabled**. |
| `outbound_queue_capacity` | integer | `1024` | Per-connection outbound queue in envelopes; must be `>= 1` when enabled. A full or closed queue drops the current outbound attempt rather than blocking realtime routing. |

WebSocket additionally supports `heartbeat_interval_ms` (default `15000`) and
`heartbeat_timeout_ms` (default `45000`). After authentication Citadel sends
native Ping control frames, not game envelopes. A peer that misses the Pong
deadline is closed normally, which runs the usual session cleanup and `on_leave`
hooks. Set `heartbeat_interval_ms = 0` to disable probes; when enabled, the
timeout must be at least `1` ms. The `/status` metrics include aggregate ping,
pong, and liveness-timeout totals.

Notes:

- WebTransport negotiates the HTTP/3 ALPN `h3` and runs on its **own** UDP
  endpoint, separate from native QUIC (`citadel/0`).
- A transport's `bind` and `outbound_queue_capacity` are only validated when that
  transport is `enabled`.
- All enabled transports share one [gateway room](/concepts/gateway/).

### `[transport.network_peer]`

This optional section activates the NetworkPeer authority at the production
gateway. It is **off by default**. Enabling it routes replication delta/ack
frames and sends schema/full-baseline bootstrap at gateway admission, but it does
not let a client register a class or object. Trusted server lifecycle code must
use the gateway registration/spawn/despawn seams. The authority-level shared grid
uses the three distance values below; this is not automatic room or matchmaker
AOI integration.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Attach the NetworkPeer authority to the gateway. |
| `shared_quantized_state` | bool | `false` | Reuse prepared quantized payloads only for equivalent per-receiver bunches. Enable after measuring real fan-out; tokens/baselines remain receiver-specific. |
| `interest_cell_size` | integer | `100` | Uniform shared-grid cell size in world units; must be positive. |
| `interest_inner` | integer | `100` | Enter-relevance distance in world units; must be positive. |
| `interest_outer` | integer | `125` | Exit-relevance distance in world units; must be at least `interest_inner`. |

### `[transport.tls]`

This optional section configures the PEM certificate chain used directly by the
public QUIC and WebTransport UDP listeners. It is intentionally separate from
`[cluster.tls]`, which only secures node-control traffic.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `certificate_file` | path | *(unset)* | PEM leaf certificate followed by intermediates. |
| `private_key_file` | path | *(unset)* | Matching PEM private key (PKCS#8, RSA/PKCS#1, or SEC1). |

Set both paths or neither. With both omitted, Citadel generates local-only
self-signed certificates; do not expose that mode publicly. Set both paths to
a CA-issued chain/key before publishing QUIC or WebTransport. Citadel reads the
files at startup, so renew a certificate then restart the service. This section
does **not** turn the built-in WebSocket listener into `wss://`; use a reverse
proxy for WebSocket and the HTTP dashboard.

### `[chat.limits]`

Secure chat uses repository-owned fixed windows, not a process-local throttle.
Each rule is an inline TOML table with `limit` and `window_ms`; both must be in
the inclusive range `1..=1_000_000` and `1..=86_400_000` respectively. The
server hashes user/channel identities before storing counters. `send_*` applies
user, user/channel, and channel keys together; `mutation_*` applies to author
edits/deletes; moderation uses operator and channel keys. Expired counter and
audit rows are eligible for bounded background cleanup. A persistence failure
fails a chat action closed rather than bypassing its configured policy.

### `[authentication.limits]`

Public device, custom-id, and email/password authentication is admitted before
credential lookup or Argon2 verification. `source` limits all auth requests by
the direct TCP peer address; `email` additionally limits normalized email
attempts, so a distributed password-guessing attack cannot bypass the source
limit; and `registration_source` applies only to `create:true` requests. Each
rule is an inline table with positive `limit` and `window_ms` values in the same
inclusive ranges as `[chat.limits]`.

The durable counter keys are SHA-256 hashes, never raw IP addresses or emails.
SQLite, PostgreSQL, and CockroachDB nodes sharing a database enforce the same
atomic multi-key plan; the in-memory backend is intentionally single-process.
When any allowance is exhausted, Citadel returns `429 rate_limited` and a
conservative whole-second `Retry-After` that covers the full matching plan. It
does not identify the limited email, source, or rule. Do not rely on
`X-Forwarded-For`: Citadel intentionally ignores forwarded-address headers
until trusted-proxy support is configured and authenticated.

### `[database]`

Optional durable persistence. The whole section can be omitted: with no `url`
the node keeps using the in-memory repositories, so a default config runs with no
database. When set, the URL may carry credentials and is **never** echoed in
diagnostics. The **backend is chosen by the URL scheme**:

- `postgres://` / `postgresql://` → the networked **PostgreSQL** backend.
- `cockroach://` / `cockroachdb://` → **CockroachDB**, served through the same
  Postgres backend over CockroachDB's PostgreSQL-wire protocol. See the
  [Running on CockroachDB](/guides/cockroachdb/) guide for the dialect details.
- `sqlite:` URL or a bare file path (e.g. `sqlite:data.sqlite`,
  `sqlite::memory:`, `./data.sqlite`) → the embedded **SQLite** backend: one
  self-contained file, created on first run, with no server to operate.
- `mongodb://` / `mongodb+srv://` → the durable **MongoDB backend**. The URI is
  parsed by MongoDB's official Rust driver, so standard TLS, SCRAM, and X.509
  URI options remain supported. It requires a replica set or sharded cluster;
  standalone `mongod` is rejected because it cannot meet the transaction
  contract. Citadel never falls back to in-memory state when a MongoDB URL is
  configured: an unreachable, non-transactional, or incompatible deployment
  fails startup clearly.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `url` | string | *(unset)* | `postgres://` / `postgresql://` (Postgres), `cockroach://` / `cockroachdb://` (CockroachDB), `mongodb://` / `mongodb+srv://` (MongoDB), or `sqlite:` / a file path (SQLite). Unset runs in-memory. Also settable via `CITADEL_DATABASE_URL`. |
| `max_connections` | integer | `10` | Connection pool size. Must be `>= 1` when a `url` is set. SQLite in-memory databases are forced to a single connection. |
| `connect_timeout_ms` | integer | `5000` | Timeout for the initial connection. Must be `>= 1` when a `url` is set. |
| `acquire_timeout_ms` | integer | `5000` | Timeout for acquiring a pooled connection. Must be `>= 1` when a `url` is set. |
| `mongodb_read_preference` | string | `"primary"` | Must remain `primary` for MongoDB transactional consistency. |
| `mongodb_write_concern` | string | `"majority"` | Must remain `majority` for MongoDB transactional consistency. |
| `mongodb_read_concern` | string | `"majority"` | Must remain `majority` for MongoDB transactional consistency. |

Schema reconciliation is applied on connect. For Postgres, run a
throwaway local database and migrate it with `make db-up` (Windows cmd: `make
db-up`; PowerShell: `.\make db-up`); SQLite needs no setup — the file is
created and migrated automatically.
See the persistence feature docs for the schema and transaction model.

On `citadel serve`, the node **selects** its backend from this section before it
starts serving: it picks Postgres, CockroachDB, SQLite, or MongoDB by URL scheme
(connecting, applying migrations), or runs in-memory with no `url`. If a
configured database is **unreachable** (or a migration fails), startup **fails
fast** with a clear error — the node never starts on a silent in-memory fallback.
The selected backend (`in-memory`, `postgres`, `cockroach`, `sqlite`, or `mongodb`, never
the URL) is reported in the `backend` field of the `/status` endpoint and shown on
the `/dashboard` console.

CockroachDB rides the Postgres backend but is a distinct **flavor**: it uses
CockroachDB-compatible migrations and skips two PostgreSQL-only mechanisms it does
not implement (`COLLATE "C"` and `pg_advisory_xact_lock`). Point a
`cockroach://`/`cockroachdb://` URL at the cluster — a plain `postgres://` URL
aimed at CockroachDB would try to apply the PostgreSQL migrations and fail. See
the [Running on CockroachDB](/guides/cockroachdb/) guide.

The SQLite backend is a full sibling of Postgres behind the same seam: it serves
**storage plus identity and sessions**, so accounts and sessions persist durably
to a single `data.sqlite`. With a `sqlite:` URL the node **self-bootstraps** on
first run — it creates the database file, applies the embedded migrations, and
creates an empty `scripts_dir` (`./game`) — so the standalone flow is unzip and
run, with no migration command or `mkdir`.

### `[cluster]` and `[cluster.tls]`

The optional cluster section enables the live cross-node matchmaker and party
authority. It is a **durable** feature: startup rejects `cluster.enabled = true`
without a `database.url`, and accepts only PostgreSQL or CockroachDB. SQLite and
MongoDB are supported in their appropriate non-clustered deployments but are
rejected for clusters because this owner/fencing path requires portable atomic
multi-object writes. The active shard or party owner is selected by a stored
generation-fenced lease; a non-owner forwards ticket submit/cancel/status,
handoff delivery, admission, and party mutations through a bounded mutual-TLS
connection. The same typed control plane also carries bounded runtime-event
fan-out and fenced cache mutations; Citadel does not proxy realtime sockets or
expose a general inter-node message tunnel.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Starts the cross-node matchmaker path. Requires `[database]`, mTLS paths, and valid peer entries. |
| `control_bind` | socket address | `127.0.0.1:7390` | TCP listener for matchmaker-only mTLS commands. |
| `matchmaker_shard` | integer | `0` | Queue partition resolved by this node. Nodes sharing a queue configure the same value. |
| `lease_ttl_ms` | integer | `5000` | Positive durable ownership lease duration. Renewals keep the same fencing generation. |
| `handoff_ttl_ms` | integer | `30000` | Positive player join-capability lifetime. |
| `command_timeout_ms` | integer | `2000` | Positive deadline for a node-control command. |
| `tls.ca_certificate_file` | path | *(empty)* | PEM private-CA certificate trusted for peer TLS handshakes. Required when enabled. |
| `tls.certificate_file` | path | *(empty)* | This node's PEM certificate chain. Required when enabled; its leaf needs both TLS server-auth and client-auth usages. |
| `tls.private_key_file` | path | *(empty)* | This node's PKCS#8 private-key PEM. Required when enabled; never logged. |

Every `[[cluster.peers]]` entry requires `node_id`, `control_addr`,
`server_name`, and `certificate_file`. The certificate file is the peer's leaf
certificate used to pin that identity after the CA validates TLS. The peer leaf
must carry `server_name` in its DNS SAN. See the complete ordered setup in
[Run a two-node matchmaker](/guides/distributed-matchmaker/).

With `cluster.enabled`, a locally accepted runtime event is also offered to
configured peers through a bounded asynchronous queue. Delivery is
best-effort delivery attempt: peer outages or a full queue can lose a remote
copy, and no retry or replay is promised. Cache writes, CAS writes, and deletes
first commit to the caller's node-local cache, then are offered to a bounded
queue for the current durable global cache-writer lease. The writer propagates
last-writer-wins fenced mutations with an absolute expiry timestamp. Queue
saturation, a peer outage, or a failover can lose that remote attempt; a
successful local call is never a global commit. A writer failover advances the
durable fence, so delayed mutations from an older writer are rejected. Cache
values remain memory-only: they are neither globally linearizable nor durable,
local CAS versions are node-local, and a peer restart does not replay cache
contents.

### `[storage]`

Optional operator-declared JSON storage indexes. Add one
`[[storage.indexes]]` item per index. Citadel validates the definition during
`citadel check`, then creates a matching physical expression index on SQLite,
PostgreSQL, or CockroachDB during server bootstrap. In-memory mode preserves the
same observable query/permission behavior for development.

| Key | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | string | Yes | Unique ASCII identifier (`1..=40` characters, letters/digits/underscores; cannot start with a digit). |
| `collection` | string | Yes | Storage collection this index covers. |
| `key` | string | No | Restricts the index to one object key in the collection. |
| `fields` | string array | Yes | One or more unique top-level JSON identifiers (`1..=64` characters). They are the only fields that may be queried. |

The trusted Lua, Python, and JavaScript runtimes query a declaration with
`storage_index_query(index_name, filters_json, limit)`. `filters_json` is a JSON
object of equality predicates over declared fields; values may be strings,
numbers, or booleans, and `limit` is from `1` through `100`. Results are
identity-ordered and respect the normal storage read permissions for the
accessor. Trusted server scripts use the authoritative accessor, so this is not
a player-facing database endpoint.

At script initialization, game logic may call
`register_storage_index_filter(index_name, callback)`. The callback runs before
that runtime's `storage_write` commits for a matching declaration. It receives
the candidate identity, `value_json`, permissions, expected version, and index
name; return `true` to include the object in the index or `false` to exclude it
without deleting the object. A callback error, deadline, or non-boolean return
rejects the write and preserves the previous object and index membership. The
callback inherits the enclosing runtime-handler deadline and Citadel never
retries it automatically; scripts explicitly choose whether to retry a failed
write, avoiding repeated callback side effects.

Indexes are intentionally **not** player- or script-created DDL. There is no
arbitrary query language, range/full-text filter, or generic client SDK endpoint:
index search is trusted game-logic work and clients consume an application RPC.
A changed definition produces a new, safe physical index on the next startup;
schedule normal database maintenance to remove an obsolete old expression index
if the configuration changes frequently.

### `[runtime]`

The embedded game-logic runtime. With the default `language` unset, Citadel
autodetects the conventional entrypoint in `scripts_dir` by priority:
`main.lua` (Lua, default build), then `main.py` (Python when compiled with
`--features runtime-python`), then `main.js` (JavaScript when compiled with
`--features runtime-js`). If multiple entrypoints are present, Lua wins over
Python and Python wins over JavaScript. An explicit `language` takes precedence
and only looks for that language's entrypoint.

Lua is always available. Python is available only in builds compiled with the
`runtime-python` Cargo feature; selecting Python in a lean/default build fails
with a clear config error instead of silently falling back to Lua. JavaScript is
available only in builds compiled with the `runtime-js` Cargo feature; selecting
it in a lean/default build fails the same way. When disabled, or when the
selected entrypoint is absent, the node uses the built-in position relay.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Consult the script for inbound messages; `false` forces the built-in relay. |
| `require_script` | bool | `false` | Strict GameScript readiness gate. When `true`, no match exists without a validated, loaded script and a healthy execution backend: match listing, match creation, and player admission are all refused with a stable `game script unavailable` error until a script is ready, and every match is born bound to the loaded script revision and generation (admission into a match whose revision is no longer loaded is refused). A missing entrypoint boots the node **not-ready** instead of silently falling back to the relay; the node becomes ready when a valid script loads (for example via `hot_reload` or a later deploy). A present-but-broken script remains a hard startup error. Requires `enabled = true`. The first-run wizard sets this when it scaffolds a scripted project; the default `false` keeps unzip-and-run relay behavior unchanged. |
| `language` | enum | *(unset / autodetect)* | Optional explicit language. Current accepted values: `"lua"`, `"python"`, `"js"` (`"javascript"` is an alias for JS). Lua is always implemented; Python requires a `runtime-python` build; JavaScript requires a `runtime-js` build. |
| `adapter` | enum | `"embedded"` | Runtime hosting adapter. `"embedded"` is implemented. `"external-worker"` and `"wasm"` are reserved for later phases and currently fail validation. |
| `tier` | enum | `"trusted"` | Runtime trust tier. `"trusted"` is implemented. `"hardened"` is reserved for the future WASM/capability-gated tier and currently fails validation. |
| `lua_execution_mode` | enum | `"sandboxed"` | Lua-only capability mode. `"sandboxed"` retains the scoped loader and handler deadlines. Explicit `"trusted"` enables Lua's extended safe standard libraries (`os`, `io`, `package`/unrestricted `require`, and `coroutine`) and disables the Lua deadline hook. Use it only for operator-owned game code; startup logs a warning and `/status` reports the selected mode. |
| `scripts_dir` | string | `"./game"` | Directory holding `main.<ext>` entrypoints, relative to the working dir. Must not be empty when enabled. |
| `maps_dir` | string | `"./maps"` | Directory of cooked `.map` level geometry, scanned once at startup. A room's `map` name resolves to a loaded map here. Absent/empty is fine — the node just has no server-side geometry. |
| `static_data_dir` | string, optional | *(unset)* | Read-only root for Lua, Python, and JavaScript static JSON/CSV gameplay files. It is separate from `scripts_dir`, must already resolve to a directory when the selected game runtime loads, and Citadel never creates or writes it. Unset games retain the existing runtime behavior; `citadel.static_data` then returns an explicit access-denied error. |
| `static_data_max_file_bytes` | integer | `1048576` | Maximum bytes read from one static JSON/CSV file. Must be `>= 1` when `static_data_dir` is set. The check applies before parsing and a bounded read defends against a file growing between stat and read. |
| `deadline_ms` | integer | `100` | Per-invocation budget for message and lifecycle handlers. Must be `>= 1` when enabled. |
| `tick_hz` | integer | `0` | `citadel.on_tick` rate (ticks/sec). `0` disables the game loop; no tick task is spawned. |
| `tick_deadline_ms` | integer | *(auto)* | Optional per-tick budget. Omitted derives `min(50ms, tick period / 2)` (at least 1ms). An explicit `0` is a config error. |
| `hot_reload` | bool | `false` | Watch the selected entrypoint (`main.lua`, `main.py`, or `main.js`) and reload it live on change (dev convenience; opt-in). Reloads are failure-safe: a broken edit is rejected and the previous script keeps serving. In-VM globals reset on each reload. |
| `hot_reload_poll_ms` | integer | `500` | How often (ms) to poll the script for changes when `hot_reload` is on. Must be `>= 1` when `hot_reload` is enabled. Ignored when off. |

Python builds use embedded CPython through PyO3. Local development selects the
interpreter with PyO3's normal discovery; set `PYO3_PYTHON=python` or an
absolute interpreter path if discovery picks the wrong install. On Windows,
embedded CPython also needs its standard-library prefix (`PYTHONHOME`) to point
at the same Python distribution. `scripts/check.sh` sources
`scripts/python-runtime-env.sh`, which sets these defaults from the active
`python` command for local verification.

Python-enabled release artifacts use an explicit bundle layout next to
`citadel.exe`:

```text
citadel.exe
python313.dll
python/
  Lib/
  DLLs/
scripts/main.py
```

At process start, a `runtime-python` build detects that layout and sets
`PYTHONHOME` to `./python` plus `PYTHONPATH` to the bundled `Lib/` and `DLLs/`
before PyO3 initializes CPython. If no bundle is present, behavior is unchanged
and PyO3 uses the local/global interpreter it was built to use. Static
libpython builds are not the default packaging path.

JavaScript builds use embedded QuickJS through `rquickjs`. The capped runtime
loads `main.js` only; it does not transpile TypeScript and does not expose npm,
Node built-ins, or worker threads. Run it with `cargo run --features runtime-js`
or a release artifact that was built with the same feature.

The tick loop starts only when `tick_hz > 0` **and** the script registered an
`on_tick` handler. Each handler invocation is isolated and time-bounded: a slow
or erroring handler can never wedge the node.

The selected runtime is reported in the `/status` JSON under `runtime` and in
the console's `GET /console/v1/runtime` response. The status object includes the
configured language (when set), selected language, selection source
(`explicit`/`autodetected`), entrypoint path, adapter, tier, and scripts
directory.

### `[runtime.capabilities.outbound_http]`

Operator policy for Rust-owned outbound HTTP used by trusted Lua, Python, and
JavaScript runtime code (`citadel.http.fetch`, `start`, `poll`, and `cancel`).
It is enabled by default for compatibility, but operators should set an exact
`allowed_hosts` list for production integrations. This section does **not**
grant client access, raw sockets, or access from realtime interceptors.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Disables every script-visible HTTP operation, including `poll` and `cancel`, when `false`. |
| `max_concurrent_requests` | integer | `16` | Per-runtime maximum in-flight outbound requests; must be `1..=1024`. |
| `max_requests_per_minute` | integer | `120` | Per-runtime rolling 60-second request-acquisition limit; a request is counted when it acquires execution capacity, not when `start` returns. Must be `1..=1000000`. |
| `allowed_hosts` | array of hostname strings | `[]` | Exact DNS hostnames allowed for egress. An empty list permits any **public** DNS hostname; it does not allow IP-literal URLs. At most 128 hostnames. |
| `allowed_ports` | array of integers | `[80, 443]` | Permitted TCP ports; the array must be non-empty and has at most 128 entries. |
| `allow_private_networks` | bool | `false` | Permits resolved private, loopback, link-local, and other non-public addresses only for an explicit operator-controlled private integration. |

Citadel accepts only `http`/`https` URLs with DNS hostnames and rejects URL
credentials, forbidden `Host`/`:authority` overrides, ports outside the list,
and hostnames outside `allowed_hosts`. It resolves and pins the approved address
for the request, preventing DNS-rebinding from changing the connection target.
The Rust client denies ambient proxies and redirects. Independent fixed bounds
also apply: 64 KiB request body, 1 MiB response body, 64 headers / 16 KiB
aggregate header bytes, five-second wall-clock deadline, and 128 retained or
outstanding async handles per runtime. Network/runtime results returned by
`poll` use stable, redacted `error_code` values. Local language argument or
option validation can instead raise a language-visible validation message.

Restart the node after changing this policy. Validate the exact
`citadel.toml` before deployment:

```bash
citadel check --config /etc/citadel/citadel.toml
```

### `[runtime.capabilities.custom_http_endpoints]`

Opt-in policy for script registrations made through `citadel.http.register`.
Every registered route is served under the reserved `/ext` prefix; the script
never receives router access or the session bearer used for authentication.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Enables endpoint registration during runtime startup and reload. Disabled scripts cannot add external routes. |
| `max_request_bytes` | integer | `65536` | Maximum buffered request body per invocation. |
| `max_response_bytes` | integer | `1048576` | Maximum script response body. An oversized response becomes a generic `500`. |
| `max_requests_per_minute` | integer | `120` | Node-local fixed-window limit per endpoint, caller identity (or anonymous), and source IP. |

Only `GET`, `POST`, `PUT`, `PATCH`, and `DELETE` can be registered. A script
chooses `auth = "public"` or `auth = "session"` for each path. Runtime source
reloads preserve this operator-owned policy and publish a complete new registry
only after all registrations succeed.

### `[runtime.capabilities.events]`

Opt-in, process-local event queue for `citadel.events.emit` and
`citadel.events.subscribe`. Version one is best-effort: Citadel does not
persist, retry, replicate, or replay accepted events after restart. Events are
therefore not a substitute for storage, a job queue, or cluster messaging.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Enables the node-local event surface. `emit` returns `false` while disabled or when an event is dropped. |
| `queue_capacity` | integer | `1024` | Fixed maximum number of pending events for this node. |
| `max_event_bytes` | integer | `16384` | Maximum opaque binary payload accepted by `emit`. |
| `max_events_per_minute` | integer | `600` | Node-local fixed-window rate limit per namespace. |

### `[runtime.capabilities.shared_cache]`

Opt-in mutable cache shared by all Lua, Python, and JavaScript runtime VMs on
this process. Without `[cluster]` it is node-local. With `cluster.enabled`,
nodes offer mutations to a bounded queue for a single durable global writer
lease and receive best-effort fenced fan-out. A successful API call is a local
cache update, not a cluster-wide commit. Values themselves stay non-durable and
are not replayed after restart; use durable storage when a value must survive a
restart. Script hot reloads retain the same node-owned cache.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Enables `citadel.cache`; calls fail while disabled. |
| `max_entries` | integer | `1024` | Maximum entries across all namespaces on this node. Inserting a new key at capacity evicts the entry nearest expiry. |
| `max_value_bytes` | integer | `65536` | Maximum opaque binary value accepted by `set` or `cas`. |
| `max_ttl_ms` | integer | `3600000` | Maximum TTL for one value; expiration is lazy on subsequent cache access. |

Namespaces and keys use 1–80 ASCII alphanumeric, `.`, `_`, or `-` characters.
The API exposes `get`, `set`, `delete`, and versioned atomic `cas`; all four
are unavailable in `before_realtime` and `after_realtime` hooks, which remain
observational. Expired entries disappear lazily when that key or the cache is
accessed. Node metrics expose capacity evictions.

`maps_dir` is scanned once at startup: every well-formed `.map` (CMAP level
geometry, produced by the map cook tool) is loaded and indexed by file stem. When
a room is created, its chosen `map` name (from `on_room_create`) is resolved
against this catalog — a match logs the loaded geometry; a name with no matching
`.map` logs a warning so you catch a typo or an uncooked level. A malformed or
unreadable file is skipped, never fatal. The loaded geometry is the input a later
navmesh bake will consume; today loading validates the map and exposes its bounds
and triangle counts.

Hot-reload (opt-in via `hot_reload`) watches the selected entrypoint
(`main.lua` for Lua, `main.py` for Python, `main.js` for JavaScript) by polling its modification time and
size every `hot_reload_poll_ms`. For Lua, it additionally watches each static
JSON/CSV file successfully loaded during top-level initialization. On change it builds a fresh VM and static-data
catalog and swaps them in
under the same lock that serializes dispatch and the tick, so a reload never
interleaves with a running handler. It is failure-safe: a script that fails to
read/parse/register, a data file that fails containment/size/parse/schema
validation, or a script that registers no handlers at all, is rejected and the
previously-loaded script **and parsed data catalog** keep serving. In-VM globals
reset on each successful reload.

For static data, Citadel accepts only `/`-separated relative `.json` and `.csv`
paths. It canonicalizes both the configured root and each requested target,
rejects paths that escape through a symbolic link, and never reveals the host
path to Lua. It also never exposes `io`, `os`, or `package`. Citadel itself does
not write the data tree; use operating-system permissions or a read-only volume
when the operator also needs to prevent other local processes from editing it.
See [Use shared static gameplay data](/guides/static-game-data/) for the ordered
setup and reload workflow.

### `[console]`

Static operator credentials for the [admin console](/reference/admin-api/console/) and
its `/console/v1` API. Passwords are never echoed in diagnostics or the
`/console/v1/config` browser.

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `username` | string | `"admin"` | Operator login username. Must not be empty. |
| `password` | string | `"password"` | Grants the `admin` role (full access). Also settable via `CITADEL_CONSOLE_PASSWORD`. The startup banner warns while the default is unchanged. |
| `viewer_password` | string | *(unset)* | Optional password granting the read-only `viewer` role for the same username. Must not be empty when set. |
| `token_expiry_sec` | integer | `3600` | Console bearer-token lifetime in seconds. Must be `>= 1`. |

## Environment overrides

`CITADEL_`-prefixed variables override file values:

| Variable | Overrides |
| --- | --- |
| `CITADEL_LOG_LEVEL` | `logging.level` |
| `CITADEL_HTTP_BIND` | `http.bind` |
| `CITADEL_NODE_ID` | `server.node_id` |
| `CITADEL_PUBLIC_ADDR` | `server.public_addr` |
| `CITADEL_DATABASE_URL` | `database.url` |
| `CITADEL_CONSOLE_PASSWORD` | `console.password` |

`CITADEL_SENTRY_DSN` is deliberately not a TOML field or config-browser value:
when set, it enables optional Sentry telemetry. The server starts and the local
journal continues normally when the variable is absent or the endpoint is
unavailable. Set `CITADEL_ENVIRONMENT` to label those telemetry events; it
defaults to `production`. `CITADEL_BUGSINK_DSN` remains a lower-priority
compatibility alias; see [Telemetry](/reference/operations/telemetry/) for
Sentry and Bugsink setup.

Unknown `CITADEL_` variables are ignored by configuration loading so future
keys do not break older binaries. CLI flags (see the
[CLI reference](/reference/operations/cli/)) override these.

## Validation

`citadel check` resolves and validates the config, builds the selected runtime
entrypoint if one is present, then prints a non-secret summary. Validation checks
socket-address syntax, non-empty `node_id` and `logging.level`, per-transport
rules for enabled transports, and the `[runtime]` rules (implemented
`adapter`/`tier`, non-empty `scripts_dir`, non-blank `static_data_dir` when
set, a non-zero `static_data_max_file_bytes` when that root is set,
`deadline_ms >= 1`, a non-zero `tick_deadline_ms` when set, and a non-zero `hot_reload_poll_ms` when
`hot_reload` is on) when the runtime is enabled. A broken `main.lua` or
`main.py` is reported during `check`, before listeners start. Diagnostics name
the offending field and never echo secrets.

:::note[Transport TLS]
`[cluster.tls]` protects only node-control traffic. Configure `[transport.tls]`
with a PEM certificate/key pair for public QUIC and WebTransport; native QUIC
clients should use CA and hostname verification (`ClientTls::webpki_roots()`).
WebSocket remains plain `ws://` in Citadel and needs a reverse proxy for WSS.
:::
