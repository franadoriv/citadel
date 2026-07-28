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

[transport.webtransport]
# Browser path: QUIC-grade datagrams + streams over HTTP/3 (own UDP endpoint).
enabled = false
bind = "127.0.0.1:7353"
outbound_queue_capacity = 1024

[runtime]
# Embedded game-logic runtime. With language unset, Citadel autodetects by
# priority in scripts_dir: main.lua (default build), main.py
# (runtime-python builds), then main.js (runtime-js builds). With no entrypoint,
# the built-in relay runs.
enabled = true
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
max_connections = 10
connect_timeout_ms = 5000
acquire_timeout_ms = 5000

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

### `[transport.quic]`, `[transport.websocket]`, `[transport.webtransport]`

All three share the same shape:

| Key | Type | Default (quic / ws / wt) | Notes |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Whether the listener starts. |
| `bind` | socket address | `7351` / `7352` / `7353` on `127.0.0.1` | Validated **only when enabled**. |
| `outbound_queue_capacity` | integer | `1024` | Per-connection outbound queue in envelopes; must be `>= 1` when enabled. A full or closed queue drops the current outbound attempt rather than blocking realtime routing. |

Notes:

- WebTransport negotiates the HTTP/3 ALPN `h3` and runs on its **own** UDP
  endpoint, separate from native QUIC (`citadel/0`).
- A transport's `bind` and `outbound_queue_capacity` are only validated when that
  transport is `enabled`.
- All enabled transports share one [gateway room](/concepts/gateway/).

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

| Key | Type | Default | Notes |
| --- | --- | --- | --- |
| `url` | string | *(unset)* | `postgres://` / `postgresql://` (Postgres), `cockroach://` / `cockroachdb://` (CockroachDB), or `sqlite:` / a file path (SQLite). Unset runs in-memory. Also settable via `CITADEL_DATABASE_URL`. |
| `max_connections` | integer | `10` | Connection pool size. Must be `>= 1` when a `url` is set. SQLite in-memory databases are forced to a single connection. |
| `connect_timeout_ms` | integer | `5000` | Timeout for the initial connection. Must be `>= 1` when a `url` is set. |
| `acquire_timeout_ms` | integer | `5000` | Timeout for acquiring a pooled connection. Must be `>= 1` when a `url` is set. |

Migrations are embedded in the binary and applied on connect. For Postgres, run a
throwaway local database and migrate it with `make db-up` (Windows cmd: `make
db-up`; PowerShell: `.\make db-up`); SQLite needs no setup — the file is
created and migrated automatically.
See the persistence feature docs for the schema and transaction model.

On `citadel serve`, the node **selects** its backend from this section before it
starts serving: it picks Postgres, CockroachDB, or SQLite by URL scheme
(connecting, applying migrations), or runs in-memory with no `url`. If a
configured database is **unreachable** (or a migration fails), startup **fails
fast** with a clear error — the node never starts on a silent in-memory fallback.
The selected backend (`in-memory`, `postgres`, `cockroach`, or `sqlite`, never
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

The optional cluster section enables the live cross-node matchmaker. It is a
**durable** feature: startup rejects `cluster.enabled = true` without a
`database.url`. The active shard owner is selected by a stored generation-fenced
lease; a non-owner forwards ticket submit/cancel/status, handoff delivery, and
admission through a bounded mutual-TLS connection. Citadel does not proxy
realtime sockets or expose a general inter-node message tunnel.

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

Unknown `CITADEL_` variables are ignored so future keys do not break older
binaries. CLI flags (see the [CLI reference](/reference/operations/cli/)) override these.

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
`[cluster.tls]` protects only node-control traffic. QUIC and WebTransport still
use their existing development certificate flows; configuring this section does
not enable public client TLS termination.
:::
