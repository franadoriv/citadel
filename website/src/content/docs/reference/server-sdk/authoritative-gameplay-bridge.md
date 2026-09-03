---
title: Authoritative gameplay bridge
description: Route protected client gameplay through your GameScript with citadel.on_input — normalized events in, a fenced command batch out — plus the fire/hit rewind host API.
---

import { Tabs, TabItem } from '@astrojs/starlight/components';

The authoritative gameplay bridge makes your GameScript the sole authority over
protected gameplay. In an **authoritative match** every protected client action —
transform-sync input, relay-mode owner state, replicated-variable writes, and
avatar spawn requests — is decoded and ownership-verified by Rust's *structural*
stage, delivered to your script as a **normalized event**, and only mutates
authoritative state or replicates to peers after your script's fenced,
batch-atomic answer authorizes it. Nothing protected mutates before your script
decides.

The script surface is one handler, `citadel.on_input`, plus the existing
`broadcast`/`send`/actor command APIs and the `citadel.rewind_query` host API.
It is available identically in all three shipped runtimes (Lua, JavaScript,
Python).

## Per-room bridge mode

Every room generation has an immutable server-owned `bridge_mode` selected at
creation. It is included in room snapshots and the operator match listing.
Returning a room spec from `on_room_create` may request one of these values:

```lua
citadel.on_room_create(function(_ctx, _params)
  return {
    map = "Arena",
    bridge_mode = "authoritative", -- or "relay"
  }
end)
```

- `relay` is the compatibility default. Existing state and custom traffic keep
  the legacy relay and `on_message` behavior. `KIND_MATCH_INPUT` is ignored and
  cannot allocate bridge state.
- `authoritative` requires a healthy embedded runtime and captures its current
  revision/generation before the room is inserted. Protected traffic and V1
  match input then use the fenced `on_input` bridge only for that room.

The selected mode cannot be changed by a join, an existing room name, a client
frame, or a script reload. The create callback runs only when a new named room
generation is born; a repeat create joins the immutable existing generation. A
failed authoritative request rejects room creation rather than silently creating
a relay room. `runtime.require_script = true` remains strict and upgrades every
room creation to authoritative mode; with it off, relay and authoritative rooms
may coexist on one node. A reload first fences authoritative admissions, then
retires every authoritative room generation before replacing the embedded VM;
relay rooms remain available.

## Authoritative vs. relay

The bridge activates only for a room whose immutable mode is
**authoritative** and whose captured embedded-script binding remains healthy.
For those rooms:

- Transform input, `KIND_NA_STATE`, `KIND_NA_PRESENCE`, and `KIND_REP_DELTA`
  route through `on_input`; the direct apply paths are unreachable.
- A player-slot grant is refused inside a bound match (the script owns spawns).
- Custom message kinds are **not** relayed inside a bound match. They are
  delivered as protocol-v2 `message` events through the same fenced `on_input`
  batch as protected input. Reserved kinds, including `KIND_POSITION`, stay
  closed on this path. The legacy `on_message` relay path stays closed there, so
  a script cannot reach `move_actor`/`set_transform` or an unscoped cross-room
  send outside the validator.

Relay rooms retain the default unzip-and-run path byte for byte: built-in relay
applies owner state, integrates input, and fans out spawns exactly as before;
`on_input` is never called for them. A node with no enabled/healthy embedded
runtime may still form relay rooms, but it rejects any authoritative-room
request fail closed.

## Native authoritative-match lifecycle

A room bound to an **embedded** authoritative runtime exposes a server-owned
match lifecycle in Lua, JavaScript, and Python. These embedded adapters are the
shipped lifecycle surface; `runtime.adapter = "external-worker"` currently
fails room and matchmaker admission closed because its data-plane protocol does
not yet carry a complete lifecycle frame and server-owned context. Register only
the callbacks your game needs:

```lua
citadel.on_match_created(function(ctx) end)
citadel.on_match_started(function(ctx) end)
citadel.on_match_ended(function(ctx) end)
citadel.on_match_join(function(ctx) end)
citadel.on_match_leave(function(ctx) end)
citadel.on_match_tick(function(ctx) end)
```

Every callback receives context assembled by Citadel: `match_id`, lifecycle
generation, clock epoch, authoritative tick, current scoped participants, and
validated room metadata. End callbacks additionally carry a non-sensitive
termination reason. Scripts cannot select or replace this scope. In JavaScript,
identifier fields are `BigInt`; `match_id_number` is supplied only as a lossy
convenience for older integrations.

Global `on_join`, `on_leave`, and `on_tick` remain compatible for existing
applications. Match lifecycle callbacks are additive and their emitted commands
are applied only to the current authoritative room.

## Normalized events

`citadel.on_input(handler)` registers a per-event handler. The bridge calls it
once for every normalized event in a delivered batch. Each event carries a stable
`kind` tag plus the decoded, ownership-verified intent:

| `kind` | Fields |
|---|---|
| `transform_input` | `object_id`, `ownership_epoch`, `input_seq`, `sim_tick`, `dt`, `move_velocity`, `payload`, `has_fire`, `fire?` |
| `actor_state` | `object_id`, `transform` |
| `replicated_var` | `object_id`, `class_id`, `result_id`, `field_count` |
| `spawn_request` | `archetype_id`, `transform` |
| `message` | `message_kind`, bounded opaque `body`, `reliable`, `sequence?` |

Every event also carries `event_id`, `participant`, `user_id` (absent for
guests), `match_id`, and `tick`. Vectors are `{x, y, z}` tables in Lua and
`[x, y, z]` arrays in JavaScript and Python.

:::note[Generic custom messages]
Bridge protocol version 2 emits `message` for otherwise-unhandled client
envelopes in a bound authoritative match. `message_kind` is the wire kind and
`body` is opaque bytes capped at 64 KiB before the runtime is entered.
`reliable` reports the native ingress path. `sequence` is optional and appears
only when that path exposes a stable application sequence; do not infer a
sequence from `event_id`. The server derives match membership and binding, then
issues the usual generation, clock-epoch, tick, and batch fences before calling
`on_input`. A non-member, stale binding, cross-match, or oversized message is
dropped before runtime execution. `join` and `leave` remain planned.
:::

:::note[Explicit match input V1]
`KIND_MATCH_INPUT` is a reserved reliable client-to-server envelope for one
native authoritative match. Its V1 body is a one-byte version followed by the
client's `u64` input sequence and up to 64 KiB of opaque game bytes. Malformed,
oversized, unreliable, roomless, stale-binding, or non-member frames are
dropped before a script runs; the raw body is never logged by this route.

The bridge delivers it as a `message` event with
`message_kind == KIND_MATCH_INPUT`. `participant_id` and `sequence` are
canonical decimal strings so Lua, JavaScript, and Python preserve the complete
`u64` range. `participant_id` is transport-authenticated and `match_id` remains
server-owned. The sequence is supplied by the client and is delivered exactly;
game logic can retain it per `participant_id` to distinguish fresh, duplicate,
and out-of-order input. Citadel never substitutes a transport frame counter.

Within that callback only, `citadel.match.set_input_ack(participant_id,
sequence)` queues a reliable private acknowledgement for that same currently
validated native room. It accepts canonical decimal strings, never exposes an
ACK to another participant, and is discarded if leave, reconnect, close, reload,
or room succession changes the target scope before materialization. Client-sent
ACK frames are reserved and dropped. Admission is also bounded by the operator's
`[runtime.bridge]` per-match and node-wide pending-batch caps, plus
per-participant message/minute and opaque byte/minute limits; an exhausted limit
drops the new input without invoking the runtime or retaining its body.
:::

### Lua example

```lua
citadel.on_input(function(event)
  if event.kind ~= "message" or event.message_kind ~= 41 then
    return nil
  end

  -- `body` is opaque bytes; these identifiers are exact decimal strings.
  local sequence = event.sequence
  local participant_id = event.participant_id
  -- Apply only game-owned simulation here. Keep per-participant sequence state
  -- if the game needs to reject a duplicate or stale input.
  citadel.match.set_input_ack(participant_id, sequence)
  return nil
end)
```


The handler returns one **decision** per event. The bridge collects all
decisions into a fenced answer for Rust to validate and materialize:

- **Accept** — materialize the client's canonical effect (integrate the input,
  apply the reported state, apply the replicated write, register the spawn).
  Return `nil`/`undefined`/`None`, `true`, `"accept"`, or `{ decision = "accept" }`.
- **Reject** — nothing mutates or replicates. Return `false`, `"reject"`, or
  `{ decision = "reject", reason_code = N, reply = "..." }`.
- **Correct** — materialize *your* value instead of the client's (the
  authoritative state carries your value, never the client's bytes). Return
  `{ decision = "correct", transform = { position, rotation, velocity } }`.

:::caution[`reply` is not delivered yet]
A reject's optional `reply` is accepted and size-checked by the validator, but
the server does not yet send it back to the client: that needs a dedicated
reply wire kind (a planned `citadel-wire` and contract-manifest change). Treat
`reply` as reserved for now — a rejected action simply does not materialize.
:::

<Tabs syncKey="engine">
<TabItem label="Lua">
```lua
citadel.on_input(function(event)
  if event.kind == "transform_input" then
    if event.move_velocity.x > 2000 then
      return { decision = "reject", reason_code = 1 }  -- speed hack
    end
    return nil  -- accept: integrate the movement authoritatively
  elseif event.kind == "actor_state" then
    return {
      decision = "correct",
      transform = {
        position = { x = 0, y = 0, z = 0 },
        rotation = { x = 0, y = 0, z = 0, w = 1 },
        velocity = { x = 0, y = 0, z = 0 },
      },
    }
  end
  return nil
end)
```
</TabItem>
<TabItem label="JavaScript">
```javascript
citadel.on_input((event) => {
  if (event.kind === "transform_input") {
    if (event.move_velocity[0] > 2000) {
      return { decision: "reject", reason_code: 1 };  // speed hack
    }
    return undefined;  // accept
  }
  if (event.kind === "actor_state") {
    return {
      decision: "correct",
      transform: { position: [0, 0, 0], rotation: [0, 0, 0, 1], velocity: [0, 0, 0] },
    };
  }
  return undefined;
});
```
</TabItem>
<TabItem label="Python">
```python
import citadel

def on_input(event):
    if event["kind"] == "transform_input":
        if event["move_velocity"][0] > 2000:
            return {"decision": "reject", "reason_code": 1}  # speed hack
        return None  # accept
    if event["kind"] == "actor_state":
        return {
            "decision": "correct",
            "transform": {"position": [0, 0, 0], "rotation": [0, 0, 0, 1], "velocity": [0, 0, 0]},
        }
    return None

citadel.on_input(on_input)
```
</TabItem>
</Tabs>

## Batch-level commands

Inside `on_input` your script may also emit effects with the existing command
APIs — `citadel.broadcast`, `citadel.send`, `citadel.move_actor`,
`citadel.spawn_actor`, `citadel.despawn_actor`, and the physics commands. They
are collected into the same fenced answer and validated before they materialize:
messages are scope-checked against match membership, object mutations against the
match's world, and physics commands against the match's declared physics
capability.

:::caution[`persist` and `schedule` are validated but not yet executed]
The messaging, actor, and physics commands above materialize today. Persist and
schedule commands are capability-gated and quota-bounded by the validator, but
no executor is wired behind them yet: an authorized `persist` or `schedule`
validates and then no-ops — it does **not** write durable state or enqueue a
task. The durable-effect executor is planned.
:::

## Fire/hit — the rewind host API

Owner decision: **Rust owns the bounded lag-compensated rewind query; your script
decides the consequence.** A `transform_input` event whose `has_fire` is true
carries the client's `fire` intent (origin, direction). Rust does **not**
auto-resolve the hit. Instead, call `citadel.rewind_query` from `on_input` to get
the server-computed candidate hits, then decide damage/death/cooldown yourself
(e.g. with a `send`/`broadcast`). The rewind window is server-clamped from the
shooter's measured lag and the hub's `RewindConfig` — the client's tick is never
trusted.

<Tabs syncKey="engine">
<TabItem label="Lua">
```lua
citadel.on_input(function(event)
  if event.kind == "transform_input" and event.has_fire then
    local result = citadel.rewind_query(
      event.participant, event.fire.origin, event.fire.direction, event.tick)
    for _, hit in ipairs(result.hits) do
      citadel.send(hit.participant, 100, "hit")  -- your consequence
    end
  end
  return nil
end)
```
</TabItem>
<TabItem label="JavaScript">
```javascript
citadel.on_input((event) => {
  if (event.kind === "transform_input" && event.has_fire) {
    const result = citadel.rewind_query(
      event.participant, event.fire.origin, event.fire.direction, event.tick);
    for (const hit of result.hits) {
      citadel.send(hit.participant, 100, "hit");  // your consequence
    }
  }
  return undefined;
});
```
</TabItem>
<TabItem label="Python">
```python
import citadel

def on_input(event):
    if event["kind"] == "transform_input" and event["has_fire"]:
        result = citadel.rewind_query(
            event["participant"], event["fire"]["origin"], event["fire"]["direction"], event["tick"])
        for hit in result["hits"]:
            citadel.send(hit["participant"], 100, "hit")  # your consequence
    return None

citadel.on_input(on_input)
```
</TabItem>
</Tabs>

Each hit is `{ object_id, participant, point, distance }`. `rewind_query` never
mutates state; it is a read-only query.

## Fencing and fail-closed guarantees

Every batch that crosses the script boundary carries six mandatory fencing
fields — `protocol_version`, `generation`, `match_id`, `clock_epoch`, `tick`, and
`batch_id` — and the answer must echo them exactly. The validator is the sole
authority on acceptance and is **batch-atomic**: a single invalid, out-of-scope,
over-quota, or unauthorized command rejects the *entire* batch; nothing in it
materializes. A stale-generation (post-reload), cross-match, cross-epoch,
duplicate, or incomplete answer is rejected whole.

The bridge is fail-closed end to end. If your `on_input` handler errors, times
out, or the batch is never answered (script fault, worker death), **nothing
mutates** — the match is closed match-locally with a requeue hint rather than
applying a default. Per-batch effects are bounded by measure-first
`BridgeQuotas` (command count, body bytes, reply bytes, recipients, persist and
schedule ops).

## Room-scoped snapshot delivery

Authoritative snapshots are filtered to the recipient's room. Server-owned
objects are bound to their authoritative room, while client-owned objects follow
their owner membership. Snapshot delivery rechecks the recipient and every
captured source under the room transaction gate before enqueueing, preventing a
concurrent move from leaking stale state. Concurrent authoritative rooms on one
node therefore keep their transforms isolated.
