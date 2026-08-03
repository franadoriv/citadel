# Citadel

<p align="center">
  <img src="./assets/branding/citadel-logo.png?raw=true" alt="Citadel" width="240" />
</p>

<p align="center">
  <strong>Build authoritative multiplayer game servers — write gameplay, not backend plumbing.</strong>
</p>

<p align="center">
  <img alt="Rust core" src="https://img.shields.io/badge/core-Rust-dea584?logo=rust&logoColor=white" />
  <img alt="Lua, Python, and JavaScript game logic" src="https://img.shields.io/badge/game%20logic-Lua%20%7C%20Python%20%7C%20JavaScript-6b5b95" />
  <img alt="SQLite, PostgreSQL, CockroachDB, and MongoDB" src="https://img.shields.io/badge/data-SQLite%20%7C%20PostgreSQL%20%7C%20CockroachDB%20%7C%20MongoDB-3b82f6" />
</p>

<p align="center">
  <a href="https://github.com/franadoriv/citadel/releases">Download a release</a> ·
  <a href="website/src/content/docs/quickstart.md">Get started</a> ·
  <a href="website/src/content/docs/tutorials/knights-vs-monsters.mdx">Build a game</a> ·
  <a href="website/src/content/docs/guides/install-client-sdk.mdx">Client SDKs</a>
</p>

Citadel is the authoritative backend for an online game. Your client renders
the world and asks to act; your server-side game logic decides what is valid,
updates shared state, and tells clients what happened.

The core is written in Rust for predictable performance and networking. Your
game code does not need to be: write it in Lua by default, or use trusted
embedded Python or JavaScript builds when those fit your team better.

## Why Citadel

| You focus on | Citadel handles |
| --- | --- |
| Moves, combat, rewards, NPCs, and your rules | Realtime connections, player identity, rooms, sessions, persistence, and server authority |
| The game loop and accepted state | QUIC, WebTransport, and WebSocket transport paths |
| The data your game owns | SQLite locally, plus PostgreSQL, CockroachDB, and transaction-capable MongoDB in durable deployments |
| Your engine and player experience | Unity, Unreal, Godot, Rust, and browser/JavaScript client paths |

## Start a server in minutes

Start with a published release — you do **not** need Git, Rust, Cargo, or a
source checkout to make a game server.

1. Download the matching Windows or Linux server ZIP from
   [GitHub Releases](https://github.com/franadoriv/citadel/releases), then
   extract it.
2. Run Citadel from the extracted folder:

   ```powershell
   # Windows PowerShell
   .\citadel.exe
   ```

   ```bash
   # Linux
   ./citadel
   ```

3. Open <http://127.0.0.1:7350/dashboard>. Your server is running locally.
4. Edit `scripts/main.lua`, save, and watch Citadel reload your valid game
   logic without restarting.

The release includes a working SQLite configuration, starter game script, and
the HTTP/realtime listeners. It creates `data.sqlite` and applies migrations on
first run.

### Your game rules live on the server

This is the loop Citadel is built around: client requests are untrusted; server
rules decide; every player receives accepted state.

```lua
local KIND_ATTACK = 100
local KIND_EVENT = 102

citadel.on_message(KIND_ATTACK, function(ctx, body)
  if body ~= "moss-ogre" then return end       -- reject an invalid target
  -- Check range, cooldown, and server-owned monster health here.
  citadel.broadcast(KIND_EVENT, "attack accepted", true)
end)
```

Save a valid edit and hot reload applies it. A broken edit keeps the previous
working script alive. That makes gameplay iteration fast without making the
client the authority.

## Built for the game you are actually making

- **Game logic:** Lua ships by default. Python and JavaScript are available as
  trusted embedded runtimes; all expose the same core lifecycle and game-service
  model.
- **Realtime multiplayer:** authoritative rooms, presence, messages, RPCs,
  transform sync, networked actors, static maps, and server-simulated physics.
- **Game services:** accounts, sessions, storage, friends, groups, chat,
  leaderboards, notifications, wallet, purchases, audit records, and an admin
  dashboard.
- **Operations:** release archives, config checks, health/status endpoints,
  structured logs, an error journal, Sentry-compatible telemetry, and
  production TLS guidance.

## Choose your next path

| If you want to… | Start here |
| --- | --- |
| Make your first server rule | [Getting started](website/src/content/docs/quickstart.md) |
| Build a small authoritative game | [Knights vs Monsters](website/src/content/docs/tutorials/knights-vs-monsters.mdx) |
| Connect an existing engine project | [Install a client SDK](website/src/content/docs/guides/install-client-sdk.mdx) |
| Deploy a release | [Install a server release](website/src/content/docs/guides/install-server.mdx) |
| See every exact capability and boundary | [Capability matrix](manifests/capability-matrix.json) |

## Build Citadel from source (advanced)

Source builds are for contributing to Citadel, working on its internals, or
running the tracked browser relay demo. They are not required to create a game
server from a release.

```bash
git clone https://github.com/franadoriv/citadel.git
cd citadel
make demo-web
```

On Windows PowerShell, use `./make.ps1 demo-web`. You will need Git, a recent
stable Rust toolchain, Python 3, and Make (or the included PowerShell wrapper).
See [Run the web demo from source](website/src/content/docs/guides/web-demo-from-source.md)
for the complete path.

## Capability snapshot

<!-- Generated from manifests/capability-matrix.json; do not edit this section by hand. -->

Citadel is deliberately honest about its current surface. The full,
machine-readable [capability matrix](manifests/capability-matrix.json) is the
source of truth; this is the useful-at-a-glance version.

| Area | What ships today |
| --- | --- |
| **Game logic** | Lua by default, with trusted embedded Python and JavaScript builds. All share message, lifecycle, tick, RPC, room, storage, and social-service hooks. |
| **Realtime** | QUIC for native clients, WebTransport for modern browsers, and WebSocket as the broad fallback; rooms, authoritative state, transform sync, actors, maps, and server physics are available. |
| **Game services** | Accounts and sessions, storage, friends, groups, chat, leaderboards, notifications, wallet, purchases, audit records, and an operator dashboard. |
| **Data** | SQLite for the zero-setup default; PostgreSQL, CockroachDB, and transaction-capable MongoDB for durable deployments. Clustered party/matchmaker authority requires PostgreSQL or CockroachDB—SQLite and MongoDB clusters are rejected. |
| **Client paths** | Unity, Unreal, Godot, Rust, and browser/JavaScript SDK surfaces. Their exact engine and OS coverage is in the matrix. |
| **Operations** | Release archives, config validation, health/status endpoints, structured logs, error journal, optional Sentry-compatible telemetry, and TLS/reverse-proxy guidance. |


## Roadmap

- Complete the planned player, social, economy, leaderboard, tournament, and
  live-channel capabilities in the matrix.
- Expand distributed operation: ownership, routing, matchmaking delivery, and
  self-hosted cluster discovery.
- Deliver the remaining runtime tiers and SDK/platform coverage while preserving
  the same host and client-contract parity guarantees.

## Documentation and contributing

- `website/` contains the public product and API documentation.
- `CONTRIBUTING.md` will describe the public contribution flow when it is added.

## Development commands

```bash
make help          # list all targets
make fmt           # cargo fmt
make clippy        # clippy with warnings denied
make test          # workspace test suite
make check         # canonical verification (fmt + clippy + tests + docs)
```

`bash scripts/check.sh` is the canonical local verification command.
