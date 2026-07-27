---
title: Changelog
description: Implemented Citadel capabilities for developers consuming the server and SDKs.
---

This changelog summarizes the developer-facing capabilities that are
**implemented today**. It is grouped by area rather than by release number, since
Citadel is pre-1.0 and its product surface is still moving quickly.

## Realtime transports

- **Transport abstraction** — one wire-agnostic boundary shared by every
  transport, with the typed [envelope](/concepts/envelopes/) codec, per-connection
  bounded outbound queues, and reliable/unreliable delivery policies.
- **QUIC** (`quinn`, TLS 1.3, ALPN `citadel/0`) — unreliable datagrams + reliable
  streams. Primary native transport.
- **WebSocket** (`tokio-tungstenite`) — reliable-only fallback carrying framed
  envelopes as binary messages.
- **WebTransport** (`web-transport-quinn`) — browser path: QUIC-grade datagrams +
  streams over HTTP/3 on its own UDP endpoint, with a dev cert-hash flow.


## Realtime gateway

- **Single-room relay gateway** — relays application messages to *other* sessions
  (no echo to sender), tagging each with the sender's session id. All transports
  share one gateway room and interoperate.
- **Server-side RPC dispatch** — the gateway routes a `KIND_RPC_REQUEST` to a
  server-side `on_rpc` handler and unicasts the correlated `KIND_RPC_RESPONSE`
  back to the caller only (never a broadcast); unknown method, handler error, or a
  blown deadline all yield a well-formed error response without crashing the node.

- **Authenticated handshake** — every connection presents its session token in a
  uniform `KIND_AUTH` first frame; the server validates it and binds the
  connection to the account (`ctx.user_id`), or accepts a guest, or rejects with a
  coarse reason. Guests-allowed by default; an auth-required stance is available.
  See [Envelope reference](/reference/protocol/envelope/#authenticated-handshake) and
  [Gateway](/concepts/gateway/#authenticated-handshake). The `KIND_AUTH` /
  `KIND_AUTH_RESULT` kinds are a client-contract change; SDKs pick up the new
  helpers via the auto-sync fan-out.

## Realtime gameplay

- **Embedded Lua runtime** — `main.lua` game logic with lifecycle hooks
  (`on_join` / `on_leave` / `on_message` / `on_rpc` / `on_tick` /
  `on_room_create` / `on_room_join`), a fixed-rate server tick
  (`runtime.tick_hz`), failure-safe hot-reload, and scoped `require` modules.
  (, , , )
- **Transform sync** — snapshot-based movement replication with area-of-interest,
  client-side interpolation with an adaptive buffer, and an `OwnerPredicted`
  prediction + server-rewind path for authoritative actors. See
  [transform sync](/reference/client-sdk/transform-sync/). (, , )
- **Networked actors (presence + spawn)** — drop-in player replication: a client
  announces its avatar on connect, the server spawns it for every peer, hands a
  newcomer everyone already present, and despawns it on disconnect. Movement rides
  the snapshot path. See [networked actors](/reference/client-sdk/networked-actors/).

- **Server-owned actors / NPCs** — Lua game logic spawns and moves actors nobody is
  connected as (`citadel.spawn_actor` / `move_actor` / `despawn_actor`, `owner = 0`),
  replicated to every client through the same spawn / snapshot / despawn path; late
  joiners receive live NPCs in their presence batch. Requires `runtime.tick_hz > 0`.
  See [server-owned actors](/reference/client-sdk/networked-actors/#server-owned-actors-npcs).

- **Rooms** — server-owned, admission-gated groupings; clients **join-or-create by
  name** as a matchmaking primitive (same name → same room), with the server owning
  the map/mode choice via `on_room_create` and the admission gate via `on_room_join`.
  Networked-actor visibility is room-scoped. See [rooms](/reference/client-sdk/rooms/).
  (–)
- **Maps (level geometry pipeline)** — a level's static collision geometry is
  exported to a versioned `.map` (CMAP) file and loaded server-side: the Unreal
  editor **cook tool** (`Tools → Citadel → Cook Map Data`) writes world-space
  collision triangles; the server scans `runtime.maps_dir` at startup, indexes each
  map by file stem, and resolves a room's `map` name against it on create. This is
  the geometry the server will bake a navmesh from (next phase). See
  [maps](/reference/client-sdk/maps/).

## Shared wire format

- **`citadel-wire`** — the single source of truth for the envelope and its framed
  + datagram encodings, plus the relay protocol constants (`KIND_POSITION`,
  `KIND_PEER_POSITION`) and sender-tagging helpers.
- **RPC request/response wire format** — additive `KIND_RPC_REQUEST` /
  `KIND_RPC_RESPONSE` kinds with typed `encode/decode_rpc_request` and
  `encode/decode_rpc_response` helpers. A client sends a method + payload +
  correlation id; the server replies to that caller only, correlated by
  `request_id`, with an `ok`/`error` status. See the
  [envelope reference](/reference/protocol/envelope/#rpc-requestresponse).

## Client SDKs

- **Rust SDK (`citadel-client`)** — `WsClient` and `QuicClient` with a small
  `connect / send / recv` surface over WebSocket and QUIC.
- **Client RPC call helpers** — `WsClient::call_rpc` and `QuicClient::call_rpc`
  generate a monotonic `request_id`, send a `KIND_RPC_REQUEST` reliably, and await
  the correlated `KIND_RPC_RESPONSE`, returning the reply bytes or a new
  `ClientError::Rpc { request_id, message }` on an error status. They discard
  non-RPC envelopes while awaiting the reply and impose no timeout (wrap in
  `tokio::time::timeout`). The Unity C# sample gains an `RpcClient` MonoBehaviour
  (`CallRpc` + single-poll-owner dispatch by kind, `R` fires sample `add`/`ping`
  calls) over the unchanged C ABI. See the
  [Rust SDK reference](/reference/client-sdk/rust-sdk/#rpc-call-helpers) and the
  [Unity sample](/guides/unity-quic-sample/#calling-an-rpc).
- **C ABI (`citadel-client-ffi`)** — a stable, poll-based, panic-safe C ABI over
  the Rust SDK, with a committed cbindgen header.

## Tooling and demos

- **CLI** — `citadel serve` / `citadel check` with layered TOML/env/flag
  configuration and a non-secret config summary. `serve` is the default command,
  so a bare `citadel` (or `cargo run`) starts the server; `check` validates
  config without listening.
- **Standalone startup UX** — once `serve` is ready it prints a boxed ASCII
  banner (version, node id, selected database backend, and aligned links for the
  dashboard/status/health endpoints and each enabled transport with its bind);
  detailed init logging drops to `debug`. On an interactive terminal with no
  `--config`, a first-run wizard offers to scaffold a starter Lua script and pick
  a database (SQLite default or PostgreSQL), persisting the choice to
  `citadel.toml`. The new `--yes` / `--non-interactive` flag (and any headless or
  `--config` run) skips the wizard and uses silent defaults. See the
  [CLI reference](/reference/operations/cli/#startup-banner).
- **Web demo** (`examples/web-demo/`) — no-build-step Three.js demo over
  WebTransport (with WebSocket fallback).
- **Native demo** (`demo-client`) — macroquad 2D demo over QUIC via the Rust SDK.

- **Unity C# SDK** (`clients/unity/`) — Unity move-and-broadcast demo
  over QUIC through the C ABI, with a `make unity-plugin` target that builds and
  installs the native plugin. Windows x86_64, manual in-editor run.
- **Makefile demo targets** — `make demo-web`, `make demo-native`,
  `make demo-native2`, `make unity-plugin`.

## Persistence

- **PostgreSQL backend** — durable `jsonb`-backed storage plus identity/session
  repositories behind the same async contracts as the in-memory references, with
  embedded `sqlx::migrate!` migrations applied on connect. (, )
- **Backend selection, live in the node** — on `citadel serve` the node selects
  its backend from `[database]` by URL scheme: with a `url` it connects, migrates,
  and runs on the chosen backend (**failing fast** if unreachable); with no `url`
  it runs in-memory. The selected backend (`in-memory` / `postgres` / `sqlite`,
  never the URL) appears in the `/status` `backend` field and on the `/dashboard`
  console. Account creation runs in one unit-of-work transaction.
- **SQLite backend (storage + identity/sessions)** — an embedded, single-file
  `data.sqlite` sibling backend behind the same `Backend`/`UnitOfWork` seam,
  selected with `url = "sqlite:data.sqlite"` (or a bare path). It serves **all
  four** repositories (storage plus users, auth identities, and sessions), so
  accounts and sessions persist durably with the same guarantees as Postgres,
  including atomic account creation through one SQLite transaction. Semantics are
  identical to Postgres/in-memory (optimistic concurrency, permission filtering,
  keyset cursors), proven by the **same** contract tests run **un-gated** on every
  build with no external database. (, )
- **Standalone self-bootstrap** — with a `sqlite:` URL the node bootstraps its own
  on-disk state on first run: it creates `data.sqlite`, applies migrations, and
  creates an empty `game/` scripts folder — no migration command or `mkdir`. With
  no `--config`, it discovers and loads a `citadel.toml` next to the binary, so
  the shipped standalone config is a true unzip-and-run start (SQLite + runtime +
  all transports).

## Authentication

- **HTTP device/custom auth** — `POST /v1/auth/device` and `POST /v1/auth/custom`
  let a client register or log in with a device id or a custom id and receive a
  session token, running through the persistent, transactional
  authentication/session services (account creation is one transaction on the
  selected backend). Uniform `401` on any credential failure (no account
  enumeration), typed `400` on invalid input, and generic `500`s that never leak
  internals. See the [HTTP authentication API](/reference/client-sdk/authentication/).


## Admin console

- **Fully live admin console** — the `/dashboard` single-page console (navy
  Nakama-style shell, no build step, fully self-contained) now has **every
  sidebar section live** against the operator API under `/console/v1`:
  operator login with `admin`/`viewer` roles (`[console]` config section,
  in-process bearer tokens), Accounts (list/search, create, detail with linked
  credentials, ban/unban with session revocation, edit, logical delete,
  export, wallet + friends panels), Storage browser (collections, objects,
  conditional writes/deletes with runtime authority), Groups (roles
  member/admin/superadmin, promote/demote/kick), Chat history moderation,
  Notifications (targeted + broadcast composer), Leaderboards
  (best/set/incr operators, ranked records), live Matches introspection from
  the room registry, Runtime introspection + RPC caller over the embedded Lua
  runtime, a redacted Configuration browser, Purchases & Subscriptions
  (pluggable receipt validation, dev validator today), and a bounded Audit
  Logs trail recording every console mutation. See the
  [console reference](/reference/admin-api/console/).
  (..)

## Not implemented yet (deferred)

- **Signed / refresh-rotating session tokens** — tokens are opaque unsigned
  reference tokens today; signing, rotation, and rate limiting are follow-ups.
- **Fully packaged per-engine bindings** — Unreal (drop-in plugin), Unity (C#
  over the C ABI), and Godot SDKs exist and are kept in contract parity; a
  fully packaged Unity plugin (macOS/Linux, IL2CPP, `.unitypackage`) is still a
  follow-up.
- **Coherent web ↔ native interop** — the web and native demos use different
  position payloads; run two web tabs or two native clients.
- **Production TLS** for QUIC/WebTransport, and `wss://` for WebSocket.
- **RPC ergonomics** — client-side RPC timeouts/retries, a C ABI-level RPC
  convenience, and streaming RPC are follow-ups to the client RPC call helpers.
- **Client-facing APIs for groups, chat, notifications, leaderboards, friends,
  wallet, and purchases** — these product areas now exist server-side with
  operator/console management (see *Admin console* above), but their
  player-facing client/Lua APIs, persistence, and realtime delivery are
  follow-ups; the in-process stores clear on restart (see the technical-debt
  register). Real App Store / Google Play receipt validators are .
- **Matches, parties, tournaments,** and other remaining Nakama product areas.
  (Presence and multi-room grouping now exist — see the networked-actors and
  rooms entries under *Realtime gameplay* above.)
