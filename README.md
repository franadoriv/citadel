# Citadel

<p align="center">
  <img src="./assets/branding/citadel-logo.png?raw=true" alt="Citadel" width="240" />
</p>

Citadel is a Rust-first, custom game server inspired by
[Nakama](https://heroiclabs.com/nakama/). It aims for stronger foundations than a
typical clone: **horizontal scalability**, a **language-neutral gamecode
runtime**, **database portability**, and first-class tests and docs.

The goal is a production-grade backend where you run one or more Citadel nodes,
write your game logic in the language you prefer, and connect real-time clients
(browser or native) to it.

> `../nakama` is a local reference checkout used only for research. Citadel is
> not a line-by-line port.

## Requirements

- A recent stable [Rust toolchain](https://rustup.rs) (`cargo`, `rustfmt`,
  `clippy`).
- On Windows, PowerShell 5.1+ (the bundled `make.ps1` mirrors the Makefile).

## Quick start (run the server)

**macOS / Linux:**

```bash
make server        # run the server with all transports enabled
# or the raw command (no subcommand needed — the binary serves by default):
cargo run
```

**Windows (cmd or PowerShell):**

From `cmd.exe`:
```cmd
make setup   # one-time: verify/install the Rust toolchain
make server  # run the server
```

From PowerShell:
```powershell
.\make setup   # one-time: verify/install the Rust toolchain
.\make server  # run the server
```

Running the binary with **no subcommand serves** — `citadel` is equivalent to
`citadel serve`. The other subcommands still work explicitly (`citadel check`
validates config without listening; `citadel --help` lists them).

Once the server is ready it prints a boxed startup banner with the version, node
id, selected database backend, and the links you need:

```text
+------------------------------------------------+
|   ____ ___ _____  _    ____  _____ _            |
|  / ___|_ _|_   _|/ \  |  _ \| ____| |           |
| | |    | |  | | / _ \ | | | |  _| | |           |
| | |___ | |  | |/ ___ \| |_| | |___| |___        |
|  \____|___| |_/_/   \_\____/|_____|_____|        |
|                                                  |
| version 0.9.6   node citadel-1   db sqlite       |
|                                                  |
| Dashboard      http://127.0.0.1:7350/dashboard   |
| Status         http://127.0.0.1:7350/status      |
| Health         http://127.0.0.1:7350/health      |
| QUIC           udp://127.0.0.1:7351              |
| WebSocket      ws://127.0.0.1:7352               |
| WebTransport   https://127.0.0.1:7353            |
+------------------------------------------------+
```

Detailed initialization stays at `debug` so the banner is the prominent, readable
thing on a normal run; raise `logging.level` (or set `CITADEL_LOG_LEVEL=debug`)
to see the full startup trace.

### First-run wizard

On a new interactive install, Citadel can scaffold a game script and choose
SQLite, PostgreSQL, or a transaction-capable MongoDB deployment. CI, `--config`,
`--yes`, and `--non-interactive` skip the wizard; this repository already
includes a working `citadel.toml` and Lua game. See [Choose a database](website/src/content/docs/guides/choose-a-database.mdx)
for the practical trade-offs.

### Drop-and-run standalone server

The repository ships an editable `citadel.toml` at its root that makes Citadel a
self-contained server: it targets a single-file SQLite database, enables the Lua
runtime with hot-reload, and turns on the HTTP surface plus all three realtime
transports. With no `--config` flag the node discovers `./citadel.toml` next to
it, so `cargo run` (or the packaged `citadel(.exe)`) is a zero-setup start:

```text
citadel/
├── citadel(.exe)     # the server
├── citadel.toml      # editable config (loaded automatically; --config overrides)
├── game/             # Lua game logic (auto-created; main.lua hot-reloads)
└── data.sqlite       # auto-created + migrated on first run
```

On first run the node **creates** `data.sqlite`, **applies migrations**, and
**creates an empty `game/`** folder with no manual steps — so accounts and
sessions survive restarts against one local file, no database server required.
Edit `citadel.toml` (or pass `--config`) to point at PostgreSQL, change bind
addresses, or toggle transports. See
[docs/features/persistence.md](docs/features/persistence.md) for the full flow.

### Error journal and optional external reporting

Server failures and unexpected process panics are captured in a bounded,
redacted `citadel-errors.jsonl` beside the binary. Open **Error Journal** in
the authenticated dashboard to review recurring incidents, including their
component, category, first/last-seen time, and count. Raw panic payloads,
internal details, connection strings, passwords, and tokens are excluded.

For a self-hosted external view, set `CITADEL_BUGSINK_DSN` to a Bugsink
Sentry-compatible DSN (and optionally `CITADEL_ENVIRONMENT`). It is disabled
by default; an absent or unreachable endpoint never blocks local capture or
server startup. Retention is configured under `[errors]` in `citadel.toml`.

### Download a release

Milestone downloads and their included quickstarts are published with each
release. The Linux `citadel-linux-x86_64-musl-v<version>.zip` asset is a
ready-to-run, statically linked server for x86_64/AMD64 Linux: unzip it and run
`./citadel`; neither Rust nor the source tree is required. To stage the runnable
local server package use `make bin-server` (or `.\make bin-server` in
PowerShell); use `make package-windows`, `make package-linux`, and `make
package-clients-windows` to build release artifacts locally. On macOS, `make package-macos` and
`make package-clients-macos` build native Apple Silicon or Intel archives for
the host architecture. Local macOS archives are unsigned developer builds. The
public macOS release workflow is intentionally deferred until Apple release
credentials are configured; Windows and Linux releases remain unblocked.

The server exposes an HTTP surface with a health check, a status API, and an
admin console:

- `GET /health` — liveness.
- `GET /status` — JSON node status (uptime, version, live connection/session/
  message gauges).
- `GET /dashboard` — a Nakama-Console-style, navy-themed admin console (live
  Status page + navigable placeholder sections for the not-yet-built features).

## Set up a client–server game

The quickest end-to-end loop is the browser demo. It starts the server, serves
the client, and prints the local URL.

**macOS / Linux:**

```bash
make demo-web
```

**Windows PowerShell:**

```powershell
.\make demo-web
```

For two native clients use `make demo-native2` (or `.\make demo-native2` on
PowerShell). `make benchmark-serve` stages the larger local combat benchmark.
Unity, Godot, Unreal, Rust, and web SDK entry points are listed in the matrix
and documented under `website/` and `clients/`.

### Write your game logic in a script

The server runs your game logic from a `game/` folder next to the binary. With
`runtime.language` unset, Citadel autodetects entrypoints by priority in
`scripts_dir` (`main.lua` in the default build; `main.py` in builds compiled
with `--features runtime-python`; `main.js` in builds compiled with
`--features runtime-js`). Lua is always shipped, Python ships as a feature-gated
embedded CPython trusted-tier adapter, and JavaScript ships as capped embedded
QuickJS mode with no npm, Node APIs, threads, or TypeScript transpilation.
Scripts
handle inbound messages, react to players joining/leaving, run a server game
loop, and log — enough for real server-authoritative logic, not just a relay:

```lua
citadel.on_message(1, function(ctx, body)               -- kind 1 = position
  citadel.broadcast(2, string.pack(">I8", ctx.sender) .. body, true)
end)                                                     -- kind 2 = peer position

citadel.on_join(function(ctx)                            -- a player connected
  citadel.log("player joined: " .. ctx.sender)
  citadel.broadcast(10, string.pack(">I8", ctx.sender), false)
end)

citadel.on_tick(function(dt)                             -- server game loop
  -- authoritative update; runs at runtime.tick_hz (0 = disabled)
end)

citadel.on_rpc("ping", function(ctx, body)              -- request/response RPC
  return "pong"                                          -- reply to the caller only
end)
```

Python (`--features runtime-python`) and JavaScript (`--features runtime-js`)
use the same host surface in `game/main.py` and `game/main.js`. Set
`runtime.hot_reload = true` to reload a valid edit without restarting; invalid
edits retain the previous script. The game-logic guide and per-language server
SDK references under `website/src/content/docs/` describe configuration, limits,
and every host API.

> These minimal instructions are refreshed at every milestone as the client/
> server surface grows.

## Feature status

<!-- Generated from docs/capability-matrix.json; do not edit this section by hand. -->

Three independent, evidence-backed views of the product surface.
Server and game-script statuses use `✅` shipped, `🚧` partial, `📋` planned, and `—` not applicable.

### Core server features

<table>
  <thead>
    <tr>
      <th scope="col">Feature</th>
      <th scope="col">Brief description</th>
      <th scope="col">Status</th>
    </tr>
  </thead>
  <tbody>
    <tr><th colspan="3" align="left">Core, identity, and sessions</th></tr>
    <tr><td>Server bootstrap, CLI, TOML config, and first-run setup</td><td>Run a standalone node with generated config, game directory, and SQLite defaults.</td><td>✅</td></tr>
    <tr><td>Portable server releases and Linux deployment</td><td>Versioned Windows, Linux x86_64 musl, and Linux ARM64 musl archives ship with SHA-256 checksums, CI package validation, and a systemd deployment template.</td><td>✅</td></tr>
    <tr><td>Dockerfile and editable Docker workflow</td><td>Dockerfile and Compose development assets remain available, but release CI/CD no longer builds, tests, attests, or publishes OCI images. Historical GHCR images are not updated by releases.</td><td>🚧</td></tr>
    <tr><td>Health, live status, observability, audit logs</td><td>Health/status endpoints, structured logs, redacted local incident journaling, optional external error reporting, tracing seams, and operator audit records.</td><td>✅</td></tr>
    <tr><td>Device authentication</td><td>Creates or authenticates a device identity and issues a session.</td><td>✅</td></tr>
    <tr><td>Custom-id authentication</td><td>Application-owned identifiers map to accounts and sessions.</td><td>✅</td></tr>
    <tr><td>Email/password authentication</td><td>Transactional email/password registration and sign-in at /v1/auth/email; Argon2id PHC verifiers, durable hashed multi-key admission limits, and existing session tokens ship. Email verification, recovery/change-password, and linking remain pending.</td><td>✅</td></tr>
    <tr><td>Apple sign-in</td><td>Provider adapter planned.</td><td>📋</td></tr>
    <tr><td>Facebook and Facebook Instant sign-in</td><td>Provider adapters planned.</td><td>📋</td></tr>
    <tr><td>Game Center sign-in</td><td>Provider adapter planned.</td><td>📋</td></tr>
    <tr><td>Google sign-in</td><td>Provider adapter planned.</td><td>📋</td></tr>
    <tr><td>Steam sign-in</td><td>Provider adapter planned.</td><td>📋</td></tr>
    <tr><td>Account linking and unlinking</td><td>Identity seams allow future providers but no link/unlink API ships.</td><td>📋</td></tr>
    <tr><td>Player account profile and user discovery</td><td>All released client SDKs expose typed profile read/update and exact known-user lookup; the completion manifest mechanically verifies those bindings and their web reference anchors. There is intentionally no directory, fuzzy search, presence, or recommendations.</td><td>✅</td></tr>
    <tr><td>Session tokens, realtime handshake, revocation</td><td>Opaque bearer tokens, ownership, realtime auth, guest admission, expiry/revocation validation, and durable session seams.</td><td>✅</td></tr>
    <tr><td>Public session refresh and logout API</td><td>All released client SDKs rotate caller-owned opaque token pairs and idempotently revoke one session; the completion manifest mechanically verifies every released-SDK binding and reference anchor.</td><td>✅</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="3" align="left">Storage, databases, economy, and progression</th></tr>
    <tr><td>Versioned JSON storage read/write/delete/list</td><td>Permissions, cursors, create-only, compare-and-swap, and runtime access ship.</td><td>✅</td></tr>
    <tr><td>Storage indexes and query filters</td><td>Operator-declared SQLite/PostgreSQL/CockroachDB indexes provide bounded equality filters plus durable include/exclude callbacks in Lua/Python/JS; index search is trusted game-logic work, not a generic client endpoint.</td><td>✅</td></tr>
    <tr><td>Atomic multi-resource account/storage/wallet updates</td><td>Repository boundaries exist but no public multi-update unit-of-work API.</td><td>📋</td></tr>
    <tr><td>SQLite backend</td><td>Single-file durable default for self-hosted nodes.</td><td>✅</td></tr>
    <tr><td>PostgreSQL backend</td><td>Durable production backend with migrations.</td><td>✅</td></tr>
    <tr><td>CockroachDB backend</td><td>Postgres-wire backend with the shipped domain tables.</td><td>✅</td></tr>
    <tr><td>MongoDB backend</td><td>Durable full-parity backend for transaction-capable replica sets or sharded clusters; standalone MongoDB is rejected. CI validates an authenticated disposable rs0 plus backup/restore integrity.</td><td>✅</td></tr>
    <tr><td>Read-only console database explorer</td><td>Viewer/admin dashboard browsing for the configured SQLite, PostgreSQL, CockroachDB, or MongoDB database: allowlisted metadata, structured bound filters, opaque keyset/row handles, server-side redaction, audit records, deadlines and per-operator node limits. No SQL text, MongoDB commands, mutation, export, or system schemas.</td><td>✅</td></tr>
    <tr><td>Wallet balances and ledger</td><td>Clients read balances/ledger; trusted logic adjusts under invariants.</td><td>✅</td></tr>
    <tr><td>Purchase record persistence and replay rejection</td><td>Durable receipts are hashed; transaction ids cannot be replayed.</td><td>✅</td></tr>
    <tr><td>Production store receipt validation</td><td>Only a deterministic development validator ships; provider integrations are pending.</td><td>📋</td></tr>
    <tr><td>Subscriptions and provider lifecycle</td><td>Admin view derives active/expired state; provider renewal/refund events are pending.</td><td>🚧</td></tr>
    <tr><td>Event and telemetry ingestion</td><td>No player event ingestion or runtime event callback surface.</td><td>📋</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="3" align="left">Social, groups, chat, and notifications</th></tr>
    <tr><td>Friends: invite, accept, block, remove, list</td><td>Durable social graph with game-client RPC and parity host API.</td><td>✅</td></tr>
    <tr><td>Social-provider friend import and friends-of-friends</td><td>Requires provider identity integrations and graph traversal.</td><td>📋</td></tr>
    <tr><td>Groups/clans: CRUD, role-safe membership, and admission workflows</td><td>Open self-join, closed-group requests, invitations, approval/accept/cancel flows, and superadmin ownership transfer are durable and exposed through client RPC and parity host APIs.</td><td>✅</td></tr>
    <tr><td>Group invitations and join requests</td><td>Persisted request and invitation state supports idempotent cancellation, role-safe approval, and invitation acceptance.</td><td>✅</td></tr>
    <tr><td>Authorized durable direct, group, and room chat history</td><td>Send, history, author edit/delete, group-admin moderation, revisions, tombstones, redacted audit records, and multi-key durable limits derive targets server-side and fence friendship, membership, and room access; live delivery is available after chat.join on current authenticated cluster leases.</td><td>🚧</td></tr>
    <tr><td>Chat presence, typing, and live fan-out</td><td>Chat.join/leave, authorized ephemeral typing with receiver-side expiry, presence, committed reliable KIND_CHAT_EVENT fan-out, bounded resync, revocation cleanup, and typed mTLS cross-node durable delivery with leased fenced advertisements ship. Typing is local-node only.</td><td>🚧</td></tr>
    <tr><td>Chat moderation and history administration</td><td>Operator console can inspect and tombstone durable history with an atomic redacted audit record and independent retention.</td><td>✅</td></tr>
    <tr><td>Durable player notification inbox</td><td>List/read APIs, idempotent producers, and persisted inbox records.</td><td>✅</td></tr>
    <tr><td>Local live notification delivery</td><td>Committed notifications attempt reliable KIND_NOTIFICATION delivery on the local node.</td><td>✅</td></tr>
    <tr><td>Cross-node notifications, campaigns, retention, push</td><td>No distributed forwarding, external push, or campaign scheduler.</td><td>📋</td></tr>
    <tr><td>Status follow/unfollow and online presence graph</td><td>Presence is scoped to rooms, not a social follow graph.</td><td>📋</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="3" align="left">Realtime multiplayer, rooms, matchmaking, maps, and physics</th></tr>
    <tr><td>QUIC, WebTransport, and WebSocket</td><td>Native low-latency transport, browser datagram path, and reliable browser/fallback path. QUIC/WebTransport accept production PEM TLS; native clients verify public CA certificates and hostnames, while WebSocket uses WSS through a reverse proxy. Hand-rolled RUDP is deliberately not shipped.</td><td>✅</td></tr>
    <tr><td>Authenticated realtime connection and generic RPC</td><td>Account/guest handshake and correlated request-response messages.</td><td>✅</td></tr>
    <tr><td>Relayed realtime messages</td><td>Game logic can validate, broadcast, or unicast relay messages.</td><td>✅</td></tr>
    <tr><td>Named rooms, membership, labels, and map-ready</td><td>All shipped runtimes expose room creation/admission hooks; the common room boundary scopes membership.</td><td>✅</td></tr>
    <tr><td>Player match listing and query filters</td><td>Operators can inspect matches; no player match-list API.</td><td>📋</td></tr>
    <tr><td>Single-node authoritative matches and presence</td><td>Server rooms, lifecycle, tick, presence, and scoped relay are usable on one node.</td><td>✅</td></tr>
    <tr><td>Multi-node match ownership, migration, and failover</td><td>No end-to-end distributed match runtime.</td><td>📋</td></tr>
    <tr><td>Local ticket matchmaker and reconnect handoff</td><td>Typed mutual queries, TTL, cancellation, atomic cohorts, and account-bound join tokens.</td><td>✅</td></tr>
    <tr><td>Cross-node matchmaker routing and durable leases</td><td>mTLS node-control transport forwards tickets, handoffs, cancellation/status, and admission; durable fenced leases/claims protect SQLite, PostgreSQL, and CockroachDB.</td><td>✅</td></tr>
    <tr><td>Local realtime parties</td><td>Invite/accept/leader/remove and atomic party tickets ship on one node only.</td><td>🚧</td></tr>
    <tr><td>Distributed parties, party data, presence, failover</td><td>No persistence, party data messages, leader failover, or cross-node ownership.</td><td>📋</td></tr>
    <tr><td>Transform sync, prediction, reconciliation, rewind</td><td>Authoritative snapshots and owner modes; browser WebSocket cannot use the unreliable hot path.</td><td>✅</td></tr>
    <tr><td>NetworkPeer property replication</td><td>Authoritative DeltaBunch pipeline ships; engine authoring support differs.</td><td>🚧</td></tr>
    <tr><td>CMAP static collision, server navmesh, and map queries</td><td>Static cooked collision feeds navmesh, map_info, raycasts, overlap, and ground queries.</td><td>✅</td></tr>
    <tr><td>Server-simulated kinematic physics</td><td>Deterministic static-map collision, gravity, impulse, movement intent, and state; no dynamic rigid bodies.</td><td>✅</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="3" align="left">Game logic, automation, and operator tooling</th></tr>
    <tr><td>Before/after API and realtime interception hooks</td><td>Post-handshake before hooks can veto eligible envelopes; after hooks observe the synchronous local delivery outcome without mutation or side effects.</td><td>✅</td></tr>
    <tr><td>Matchmaker callbacks, leaderboard/tournament reset callbacks</td><td>Schedulers and callback contracts are not shipped.</td><td>📋</td></tr>
    <tr><td>Runtime outbound HTTP, custom HTTP endpoints, events, shared cache</td><td>Trusted Lua, Python, and JavaScript expose bounded Rust-owned http.fetch; custom endpoints, events, shared cache, and hardened per-capability grants remain planned.</td><td>🚧</td></tr>
    <tr><td>Dashboard and authenticated operator API</td><td>Accounts, storage, groups, chat, notifications, leaderboards, matches, runtime, config, purchases, audit, and the error journal.</td><td>✅</td></tr>
    <tr><td>Console MFA, user lifecycle, password reset, ACL templates</td><td>Operator authentication roles ship; these advanced controls do not.</td><td>📋</td></tr>
    <tr><td>Cluster discovery, load balancing, generalized node routing</td><td>Ownership and fencing groundwork is not a deployable cluster product.</td><td>📋</td></tr>
  </tbody>
</table>

### Game-script layer

Only game-script capabilities appear here; Rust denotes the planned Citadel-as-a-crate and hardened WASM game-logic paths.

<table>
  <thead>
    <tr>
      <th scope="col">Feature</th>
      <th scope="col">Lua</th>
      <th scope="col">Python</th>
      <th scope="col">JavaScript</th>
      <th scope="col">Rust game logic</th>
      <th scope="col">Brief description</th>
    </tr>
  </thead>
  <tbody>
    <tr><th colspan="6" align="left">Core, identity, and sessions</th></tr>
    <tr><td>Dockerfile and editable Docker workflow</td><td>✅</td><td>📋</td><td>📋</td><td>—</td><td>Dockerfile and Compose development assets remain available, but release CI/CD no longer builds, tests, attests, or publishes OCI images. Historical GHCR images are not updated by releases.</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="6" align="left">Storage, databases, economy, and progression</th></tr>
    <tr><td>Versioned JSON storage read/write/delete/list</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Permissions, cursors, create-only, compare-and-swap, and runtime access ship.</td></tr>
    <tr><td>Storage indexes and query filters</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Operator-declared SQLite/PostgreSQL/CockroachDB indexes provide bounded equality filters plus durable include/exclude callbacks in Lua/Python/JS; index search is trusted game-logic work, not a generic client endpoint.</td></tr>
    <tr><td>Atomic multi-resource account/storage/wallet updates</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>Repository boundaries exist but no public multi-update unit-of-work API.</td></tr>
    <tr><td>Wallet balances and ledger</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Clients read balances/ledger; trusted logic adjusts under invariants.</td></tr>
    <tr><td>Production store receipt validation</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>Only a deterministic development validator ships; provider integrations are pending.</td></tr>
    <tr><td>Subscriptions and provider lifecycle</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>Admin view derives active/expired state; provider renewal/refund events are pending.</td></tr>
    <tr><td>Event and telemetry ingestion</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>No player event ingestion or runtime event callback surface.</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="6" align="left">Social, groups, chat, and notifications</th></tr>
    <tr><td>Friends: invite, accept, block, remove, list</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Durable social graph with game-client RPC and parity host API.</td></tr>
    <tr><td>Social-provider friend import and friends-of-friends</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>Requires provider identity integrations and graph traversal.</td></tr>
    <tr><td>Groups/clans: CRUD, role-safe membership, and admission workflows</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Open self-join, closed-group requests, invitations, approval/accept/cancel flows, and superadmin ownership transfer are durable and exposed through client RPC and parity host APIs.</td></tr>
    <tr><td>Group invitations and join requests</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Persisted request and invitation state supports idempotent cancellation, role-safe approval, and invitation acceptance.</td></tr>
    <tr><td>Authorized durable direct, group, and room chat history</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Send, history, author edit/delete, group-admin moderation, revisions, tombstones, redacted audit records, and multi-key durable limits derive targets server-side and fence friendship, membership, and room access; live delivery is available after chat.join on current authenticated cluster leases.</td></tr>
    <tr><td>Chat presence, typing, and live fan-out</td><td>📋</td><td>📋</td><td>🚧</td><td>📋</td><td>Chat.join/leave, authorized ephemeral typing with receiver-side expiry, presence, committed reliable KIND_CHAT_EVENT fan-out, bounded resync, revocation cleanup, and typed mTLS cross-node durable delivery with leased fenced advertisements ship. Typing is local-node only.</td></tr>
    <tr><td>Durable player notification inbox</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>List/read APIs, idempotent producers, and persisted inbox records.</td></tr>
    <tr><td>Local live notification delivery</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Committed notifications attempt reliable KIND_NOTIFICATION delivery on the local node.</td></tr>
    <tr><td>Cross-node notifications, campaigns, retention, push</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>No distributed forwarding, external push, or campaign scheduler.</td></tr>
    <tr><td>Status follow/unfollow and online presence graph</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>Presence is scoped to rooms, not a social follow graph.</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="6" align="left">Realtime multiplayer, rooms, matchmaking, maps, and physics</th></tr>
    <tr><td>Authenticated realtime connection and generic RPC</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Account/guest handshake and correlated request-response messages.</td></tr>
    <tr><td>Relayed realtime messages</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Game logic can validate, broadcast, or unicast relay messages.</td></tr>
    <tr><td>Named rooms, membership, labels, and map-ready</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>All shipped runtimes expose room creation/admission hooks; the common room boundary scopes membership.</td></tr>
    <tr><td>Player match listing and query filters</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>Operators can inspect matches; no player match-list API.</td></tr>
    <tr><td>Single-node authoritative matches and presence</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Server rooms, lifecycle, tick, presence, and scoped relay are usable on one node.</td></tr>
    <tr><td>Multi-node match ownership, migration, and failover</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>No end-to-end distributed match runtime.</td></tr>
    <tr><td>Distributed parties, party data, presence, failover</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>No persistence, party data messages, leader failover, or cross-node ownership.</td></tr>
    <tr><td>CMAP static collision, server navmesh, and map queries</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Static cooked collision feeds navmesh, map_info, raycasts, overlap, and ground queries.</td></tr>
    <tr><td>Server-simulated kinematic physics</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Deterministic static-map collision, gravity, impulse, movement intent, and state; no dynamic rigid bodies.</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="6" align="left">Game logic, automation, and operator tooling</th></tr>
    <tr><td>Embedded Lua game logic</td><td>✅</td><td>—</td><td>—</td><td>—</td><td>Default runtime with module loading and failure-safe hot reload.</td></tr>
    <tr><td>Embedded Python game logic</td><td>—</td><td>✅</td><td>—</td><td>—</td><td>Feature-gated trusted runtime with parity checks and starter game.</td></tr>
    <tr><td>Embedded JavaScript game logic</td><td>—</td><td>—</td><td>✅</td><td>—</td><td>Feature-gated QuickJS adapter with scoped local ESM modules and dependency-aware hot reload; no Node APIs, npm, workers, native modules, or TypeScript transpilation.</td></tr>
    <tr><td>Read-only static JSON/CSV gameplay data</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Lua, Python, and JavaScript load bounded, parsed gameplay constants from an operator-owned root at initialization, cache them in memory, and atomically replace them with a successful hot reload.</td></tr>
    <tr><td>Rust game logic as a crate</td><td>—</td><td>—</td><td>—</td><td>📋</td><td>Designed builder/scaffold path; native dynamic plugins remain rejected.</td></tr>
    <tr><td>Hardened WASM game logic</td><td>—</td><td>—</td><td>—</td><td>📋</td><td>Capability-gated multi-tenant runtime is designed, not shipped.</td></tr>
    <tr><td>Message/lifecycle/tick/RPC/room hooks</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Manifest-enforced parity for on_message, join/leave, tick, RPC, and room hooks.</td></tr>
    <tr><td>Broadcast/send, actors, maps, physics, storage, log</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Current language-neutral host surface is mechanically checked.</td></tr>
    <tr><td>Friends, groups, leaderboards, chat, wallet, notifications host APIs</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Friends/notifications have direct functions; remaining domain calls use validated bridges.</td></tr>
    <tr><td>Before/after API and realtime interception hooks</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Post-handshake before hooks can veto eligible envelopes; after hooks observe the synchronous local delivery outcome without mutation or side effects.</td></tr>
    <tr><td>Matchmaker callbacks, leaderboard/tournament reset callbacks</td><td>📋</td><td>📋</td><td>📋</td><td>📋</td><td>Schedulers and callback contracts are not shipped.</td></tr>
    <tr><td>Runtime outbound HTTP, custom HTTP endpoints, events, shared cache</td><td>✅</td><td>✅</td><td>✅</td><td>📋</td><td>Trusted Lua, Python, and JavaScript expose bounded Rust-owned http.fetch; custom endpoints, events, shared cache, and hardened per-capability grants remain planned.</td></tr>
  </tbody>
</table>

### Client SDK readiness by engine and OS

Each engine cell lists the OSes with a released/tested delivery path: `🪟` Windows, `🍎` macOS, `🐧` Linux. `🚧` after an icon means that engine binding is partial; `—` means no usable feature path yet. Rust is retained as a non-engine client target so its shipped SDK surface is not hidden.

<table>
  <thead>
    <tr>
      <th scope="col">Feature</th>
      <th scope="col">Unity</th>
      <th scope="col">Unreal</th>
      <th scope="col">Godot</th>
      <th scope="col">Web / JS</th>
      <th scope="col">Rust client</th>
      <th scope="col">Brief description</th>
    </tr>
  </thead>
  <tbody>
    <tr><th colspan="7" align="left">Connection, authentication, and generic API</th></tr>
    <tr><td>Connect and authenticated realtime handshake</td><td>🪟</td><td>🪟</td><td>🪟</td><td>🪟 🍎 🐧</td><td>🪟</td><td>Native engines use QUIC/C ABI paths; Web uses WebSocket handshake.</td></tr>
    <tr><td>Guest realtime handshake</td><td>🪟</td><td>🪟</td><td>🪟</td><td>🪟 🍎 🐧</td><td>🪟</td><td>All clients can connect as a guest where server policy permits.</td></tr>
    <tr><td>Email/password authentication</td><td>🪟</td><td>🪟</td><td>🪟</td><td>🪟 🍎 🐧</td><td>🪟</td><td>First-class HTTP registration/sign-in uses POST /v1/auth/email and returns caller-owned session tokens; durable hashed multi-key admission limits protect the public boundary. Email verification, recovery/change-password, and linking remain pending.</td></tr>
    <tr><td>Player profile, exact lookup, session refresh, and logout</td><td>🪟</td><td>🪟</td><td>🪟</td><td>🪟 🍎 🐧</td><td>🪟</td><td>First-class HTTP lifecycle APIs preserve the sanitized backend error contract; the completion manifest checks their bindings and web anchors across all released SDKs. Refreshed token pairs stay caller-owned for atomic secure storage.</td></tr>
    <tr><td>Correlated generic RPC</td><td>🪟</td><td>🪟</td><td>🪟</td><td>🪟 🍎 🐧</td><td>🪟</td><td>The common route for domain, party, and matchmaker operations.</td></tr>
    <tr><td>Relayed position/message traffic</td><td>🪟</td><td>🪟</td><td>🪟</td><td>🪟 🍎 🐧</td><td>🪟</td><td>All SDKs expose the base framed protocol; helpers vary by SDK.</td></tr>
    <tr><td>Durable notification inbox and local live stream</td><td>🪟</td><td>🪟</td><td>🪟</td><td>🪟 🍎 🐧</td><td>🪟</td><td>Read/ack by RPC and consume KIND_NOTIFICATION with client-side deduplication.</td></tr>
    <tr><td>Durable chat live events</td><td>🪟 🚧</td><td>🪟 🚧</td><td>🪟 🚧</td><td>🪟 🍎 🐧 🚧</td><td>🪟 🚧</td><td>All released clients expose KIND_CHAT_EVENT (28) through their normal inbound envelope path; join/history and exact event semantics are documented. Delivery spans current authenticated cluster leases and remains at-least-once, so clients deduplicate by channel/event id.</td></tr>
    <tr><td>Friends, groups, leaderboards, chat, wallet RPC</td><td>🪟</td><td>🪟</td><td>🪟</td><td>🪟 🍎 🐧</td><td>🪟</td><td>Authenticated generic RPC works across all current client targets.</td></tr>
    <tr><td>Purchases, subscriptions, and external store validation</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>No player-facing purchase surface yet.</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="7" align="left">Rooms, matchmaking, parties, and multiplayer</th></tr>
    <tr><td>Named room component and map-ready event</td><td>🪟</td><td>🪟</td><td>🪟</td><td>🪟 🍎 🐧</td><td>🪟 🚧</td><td>Unity, Unreal, Godot, and JS/Web expose named-room join/create, leave, map-ready, and joined/left lifecycle events; Unity/Godot editor smoke remains manual.</td></tr>
    <tr><td>Local ticket matchmaker RPC workflow</td><td>🪟 🚧</td><td>🪟</td><td>🪟 🚧</td><td>🪟 🍎 🐧 🚧</td><td>🪟 🚧</td><td>All can use generic RPC; dedicated matchmaker event ergonomics differ.</td></tr>
    <tr><td>Local party management and party tickets</td><td>🪟 🚧</td><td>🪟 🚧</td><td>🪟 🚧</td><td>🪟 🍎 🐧 🚧</td><td>🪟 🚧</td><td>All use generic RPC; feature itself remains local-node only.</td></tr>
    <tr><td>Transform sync snapshots and interpolation</td><td>🪟</td><td>🪟</td><td>🪟</td><td>—</td><td>🪟 🚧</td><td>Unity/Unreal/Godot have engine surfaces; WebSocket lacks unreliable snapshot path.</td></tr>
    <tr><td>Owner prediction, reconciliation, and rewind</td><td>🪟 🚧</td><td>🪟</td><td>🪟 🚧</td><td>—</td><td>🪟 🚧</td><td>Unreal is the fully documented owner integration; other surfaces are bounded.</td></tr>
    <tr><td>NetworkPeer property replication authoring</td><td>🪟 🚧</td><td>🪟</td><td>🪟 🚧</td><td>—</td><td>🪟 🚧</td><td>Unreal has declaration API; Unity/Godot share native codec access.</td></tr>
    <tr><td>Networked-actor presence/spawn integration</td><td>🪟 🚧</td><td>🪟</td><td>🪟 🚧</td><td>—</td><td>🪟 🚧</td><td>Unreal is end-to-end; Unity/Godot have transform layers but not full spawn integration.</td></tr>
    <tr><td>Authoritative server physics replication</td><td>🪟 🚧</td><td>🪟</td><td>🪟 🚧</td><td>—</td><td>🪟 🚧</td><td>Replicates through transform/actor layers; no WebSocket binary gameplay helper.</td></tr>
  </tbody>
  <tbody>
    <tr><th colspan="7" align="left">Engine tools and platform-sensitive features</th></tr>
    <tr><td>Unity CMAP map exporter</td><td>🪟</td><td>—</td><td>—</td><td>—</td><td>—</td><td>Static MeshCollider and built-in Terrain extraction with deterministic fixture coverage.</td></tr>
    <tr><td>Godot CMAP map exporter</td><td>—</td><td>—</td><td>🪟</td><td>—</td><td>—</td><td>Static-body mesh extraction plus explicit terrain-provider interface.</td></tr>
    <tr><td>Distributable Godot WebAssembly SDK package</td><td>—</td><td>—</td><td>🪟</td><td>—</td><td>—</td><td>The ZIP installs the public addons/citadel WebSocketPeer client with no GDExtension and includes a matched Godot Web .html/.js/.pck/.wasm verification export; CI opens that real WebAssembly app in Chromium against a running Citadel listener and validates guest auth, relay, receive/poll, close and payload integrity.</td></tr>
    <tr><td>Unreal CMAP map exporter</td><td>—</td><td>🪟 🚧</td><td>—</td><td>—</td><td>—</td><td>Static mesh and Landscape source ship; UE 5.8 editor compile/terrain smoke is pending.</td></tr>
    <tr><td>Browser-native binary netcode helpers</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>Browser SDK remains WebSocket-oriented; no QUIC datagram/NetworkPeer/transform helper.</td></tr>
    <tr><td>Published npm package</td><td>—</td><td>—</td><td>—</td><td>🪟 🍎 🐧 🚧</td><td>—</td><td>Source package exists; registry publication is still tracked work.</td></tr>
  </tbody>
</table>

## What we have today

The generated matrix above is the canonical capability snapshot: it distinguishes
what works end to end, what is partial, and what remains planned across server
runtimes, client SDKs, and released platforms. The public guides and API
references under `website/` contain the operational and per-method detail.

## Roadmap

- Complete the planned player, social, economy, leaderboard, tournament, and
  live-channel rows in the matrix.
- Expand distributed operation: ownership, routing, matchmaking delivery, and
  self-hosted cluster discovery.
- Deliver the remaining runtime tiers and SDK/platform coverage while preserving
  the same host and client-contract parity guarantees.

Implementation sequencing and internal operational records are maintained
privately. The public product/API documentation lives under `website/`.

## Documentation & contributing

- `website/` — the public product/API documentation site.
- `CONTRIBUTING.md` (when added) will describe the public contribution flow.

## Development commands

```bash
make help          # list all targets (from cmd or .\make help from PowerShell on Windows)
make fmt           # cargo fmt
make clippy        # clippy with warnings denied
make test          # workspace test suite
make check         # canonical verification (fmt + clippy + tests + docs)
```

`bash scripts/check.sh` is the canonical local verification command.
