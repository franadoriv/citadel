---
title: "Introduction: what Citadel actually does"
description: A plain-English map of Citadel's game clients, authoritative server, embedded game logic, and durable player services.
---

Citadel is the backend half of an online game. Your game engine draws the world
and reads player input. Citadel keeps the shared truth: who connected, which room
they joined, what the server allowed, and what must still exist tomorrow.

## The one-minute mental model

Think of an online arena with three jobs.

1. **The client performs.** Unity, Unreal, Godot, Rust, or the browser reads
   input and renders the result.
2. **Citadel coordinates.** It authenticates the player, keeps the realtime
   connection, places the participant in a room, and routes messages.
3. **Your server game logic judges.** It decides whether a move, attack, reward,
   or room join is valid, changes authoritative state, and tells clients what
   happened.

The client can ask. The server decides. Every client renders the same accepted
result.

## What you can build today

### Shared realtime worlds

- Connect native clients with QUIC, Chromium clients with WebTransport, and
  broad browser clients with WebSocket.
- Authenticate an account or join as a guest when the server allows it.
- Create and join named rooms, signal map readiness, maintain presence, and keep
  realtime traffic inside the correct room.
- Run single-node authoritative matches, transform sync, NetworkPeer property
  replication, networked actors, and opt-in server-simulated physics.

### Server-side game rules

- Run Lua in the default build.
- Enable embedded CPython with `runtime-python` when trusted server code needs
  normal Python modules.
- Enable capped QuickJS with `runtime-js` for lightweight JavaScript without
  Node, npm, worker threads, or TypeScript transpilation.
- Use the shared hook model for player messages, RPCs, joins, leaves, and ticks;
  then broadcast, send, spawn actors, use storage, or call game services.

Lua, Python, and JavaScript share the complete room-lifecycle surface,
including room creation and join-admission hooks. Their reference pages explain
the language-specific argument and return shapes.

### Durable players and operations

- Authenticate device/custom identities and issue session tokens.
- Store versioned JSON with permissions and compare-and-swap updates.
- Use friends, groups, chat, leaderboards, notifications, wallets, and purchase
  verification records.
- Operate the node through health/status endpoints, structured logs, an admin
  dashboard/API, audit records, SQLite, PostgreSQL, CockroachDB, or MongoDB.
  Start with [Choose a database](/guides/choose-a-database/) for the practical
  trade-offs.

### Client choices

Citadel ships SDK surfaces for Unity, Unreal, Godot, Rust, and JavaScript/Web.
Capabilities differ where the platform differs: the web SDK is WebSocket-first,
while native engines expose binary transform and replication helpers.

## What is still in the next season

Citadel is pre-1.0 and does not pretend every arc is finished. Important planned
or partial areas include:

- cross-node ownership and failover for authoritative match state;
- hardened, capability-gated WASM for untrusted game code;
- full-power supervised Node/TypeScript hosting;
- the Rust "Citadel as a crate" game-logic path;
- a published npm package and several platform-specific release/smoke gaps;
- Nakama-parity areas still marked planned in the canonical inventory.

Use the generated
[Feature status matrix](../../../../../README.md#feature-status)
when a production decision depends on exact coverage.

## The journey of one attack

Imagine a player pressing **Attack** near a monster.

1. The client sends an attack request. It does not subtract HP locally.
2. Citadel already knows the connection's participant, account (or guest state),
   and room.
3. Your game logic checks range, cooldown, permissions, and the monster's current
   server-owned HP.
4. If the attack is valid, the server changes HP and records any durable reward.
5. Citadel sends the accepted state to the relevant clients.
6. Every client plays animation and sound from the same result.

That loop — **request → validate → change server state → share the result** — is
the central story behind rooms, RPCs, storage, replication, and the tutorial.

## Pick your next quest

- Want proof in the next few minutes? Run the [Quickstart](/quickstart/).
- Want to learn by building a game? Start [Knights vs Monsters](/tutorials/knights-vs-monsters/).
- Want the server mental model first? Read [Game logic & server authority](/concepts/game-logic/).
- Already have a project? [Install a client SDK](/guides/install-client-sdk/).
