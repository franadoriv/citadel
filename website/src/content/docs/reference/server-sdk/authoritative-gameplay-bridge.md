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

## Authoritative vs. relay

The bridge activates only for an **authoritative match**: a node started with
`runtime.require_script = true` (see the readiness gate) whose room is bound to a
loaded script. For those matches:

- Transform input, `KIND_NA_STATE`, `KIND_NA_PRESENCE`, and `KIND_REP_DELTA`
  route through `on_input`; the direct apply paths are unreachable.
- A player-slot grant is refused inside a bound match (the script owns spawns).
- Custom message kinds and `KIND_POSITION` are **not** relayed inside a bound
  match. The legacy `on_message` relay path is closed there, so a script cannot
  reach `move_actor`/`set_transform` or an unscoped cross-room send outside the
  validator. Delivering custom kinds through the fenced batch (the `message`
  event) is planned; until then such frames are dropped fail-closed.

When `require_script = false` (the default unzip-and-run mode) there is **no
bridge**: the built-in relay applies owner state, integrates input, and fans out
spawns exactly as before, byte for byte. `on_input` is simply never called.

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

Every event also carries `event_id`, `participant`, `user_id` (absent for
guests), `match_id`, and `tick`. Vectors are `{x, y, z}` tables in Lua and
`[x, y, z]` arrays in JavaScript and Python.

:::caution[Planned, not yet delivered]
Only `transform_input`, `actor_state`, `replicated_var`, and `spawn_request`
are ever produced today. The `message`, `join`, and `leave` events are defined
in the protocol and marshaled across all three runtimes, but the gateway does
not yet emit them, so `on_input` never receives them. Routing custom message
kinds and participant join/leave through the fenced batch is planned — do not
build logic on `message`/`join`/`leave` yet.
:::

## Decisions

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
