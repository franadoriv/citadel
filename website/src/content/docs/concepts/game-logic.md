---
title: Game logic & server authority
description: Put shared game rules on the server, choose Lua, Python, or JavaScript, and understand Citadel's runtime hooks, safety limits, and planned tiers.
---

Server authority means the client may **request** an action, but the server has
the final word. A player can ask to swing a sword. Your server script checks the
range, cooldown, permissions, and current monster HP before it changes the
shared world.

That is Citadel's central game-logic model.

![Lua, Python, and JavaScript mages follow one host API spellbook and send their actions into the same Citadel core, while a future hardened tier waits behind a barrier.](../../../assets/docs/runtime-guild-shared-contract.png)

*Different languages, one guild contract. Scoped parity gaps are documented
below; the hardened WASM tier remains a future quest.*

## The rulebook lives beside the server

Put a selected entrypoint in `game/`:

| Language | Entrypoint | Availability | Important limit |
| --- | --- | --- | --- |
| Lua | `main.lua` | Shipped in the default build | Narrow standard library by default |
| Python | `main.py` | Build with `runtime-python` | Trusted CPython; package its runtime and dependencies |
| JavaScript | `main.js` | Build with `runtime-js` | Capped QuickJS; no Node, npm, workers, or TypeScript |

Citadel loads the selected runtime inside the node and calls registered handlers
when something happens. Runtime language selection can be explicit in config or
autodetected Lua-first from the entrypoint.

## Hooks hear what happened

The common runtime model includes these core ideas:

- `on_message` — a client sent a game envelope;
- `on_rpc` — a caller expects one correlated reply;
- `on_join` / `on_leave` — a realtime participant arrived or left;
- `on_tick` — advance server-owned simulation on a schedule.

`before_realtime` / `after_realtime` can veto an eligible post-handshake
envelope before routing, then observe its synchronous delivery outcome.
Interceptors never see handshake credentials or reserved authentication frames.
The before hook can only continue or veto; it cannot rewrite the envelope. The
after hook is observer-only, so any outbound commands it attempts are discarded.
Both hooks may log but cannot use domain, storage, or outbound HTTP APIs.

Lua, Python, and JavaScript all expose the room-lifecycle hooks
`on_room_create` and `on_room_join`. Use the language-specific reference for
their idiomatic argument and return shapes before designing room admission.

The exact name, argument shape, return value, and language caveats live in the
[Lua](/reference/server-sdk/lua-runtime/),
[Python](/reference/server-sdk/python-runtime/), and
[JavaScript](/reference/server-sdk/js-runtime/) reference pages.

## Actions change the world

Handlers can broadcast or send messages, log, spawn/move/despawn replicated
actors, use versioned storage, and reach shipped game services. The general
shape stays the same in every language:

1. receive a trusted context plus untrusted player input;
2. validate the request against server-owned state;
3. change authoritative state or durable data;
4. send the accepted result to the right participant or room.

Citadel mechanically compares declared host-API names across Lua, Python, and
JavaScript. That catches name drift; it does not imply that every scoped runtime
behavior is complete. Packaging, room coverage, standard libraries, value
types, and deadline mechanisms still differ.

## Rooms are authoritative match boundaries

Named rooms provide the current single-node match/presence boundary. Message
contexts include `room_id` when traffic belongs to a room, and room broadcasts
stay inside that audience. All shipped runtimes can keep world state by room
and reject a join before the player enters.

Cross-node authoritative match ownership and failover remain planned. Do not
store the only copy of durable progression in an in-memory room table.

## Server physics is optional

Replicated actors can opt into deterministic kinematic physics against a cooked
room map. Game logic can attach a body, apply an impulse, set movement intent,
and inspect grounded state. The fixed simulation updates server-owned transforms
and the normal snapshot path sends the result to clients.

Worlds without physics bodies do no physics work. Follow
[Server-simulated physics bots](/guides/server-simulated-physics-bots/) for the
complete training arc.

## What happens when a script fails

- **Per-call budgets:** hooks run with a configurable deadline (100 ms by
  default). Lua, Python, and QuickJS use different interruption mechanisms.
- **Caught handler errors:** a normal script error or timeout is logged and that
  invocation is discarded instead of wedging the shared runtime.
- **Failure-safe hot reload:** in development, a valid edit replaces the runtime;
  an invalid edit leaves the previous working script active.
- **Trusted code still means trusted:** native Python extensions or other code
  outside an interpreter's safe interruption path can exceed the normal
  isolation guarantee. Do not describe trusted in-process code as a security
  sandbox.

## Runtime tiers

Citadel separates two deployment goals.

### Trusted tier

For a server operator running code they own. Python is the shipped full-power
trusted runtime. Lua remains narrow by default, and shipped JavaScript is capped
QuickJS rather than the future full Node ecosystem.

### Hardened tier (planned)

For multi-tenant hosts or third-party code. The design uses WASM plus explicit
capabilities, accepting less language power in exchange for a stronger boundary.

Native Rust dynamic libraries are deliberately rejected: Rust has no stable
plugin ABI, and an in-process native plugin would undermine crash isolation.
The sanctioned future Rust paths are "Citadel as a crate" and Rust compiled to
WASM.

## What is planned, not shipped

- hardened WASM loading and capability gating;
- supervised Node for full JavaScript/TypeScript with npm and threads;
- Rust-as-a-crate game logic;
- Rust-to-WASM game logic;
- TypeScript transpilation in capped QuickJS mode.

## Your next quest

- Build a complete rule loop in [Knights vs Monsters](/tutorials/knights-vs-monsters/).
- Compare exact functions in the [server SDK reference](/reference/server-sdk/).
- Add indexed persistence with [Storage indexes](/guides/storage-indexes/).
