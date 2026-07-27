---
title: Networked actors (presence + spawn)
description: Out-of-the-box player/actor replication — a client announces its avatar on connect, the server spawns it for every peer and hands the newcomer everyone already present, and despawns it on disconnect. Relay movement over the transform-sync snapshot path.
---

Citadel ships **presence + replicated spawn** on top of
[transform sync](./transform-sync): drop-in player/actor replication with no
per-object wiring. A client marks its avatar for replication; on connect the
server announces it to every other client (a **spawn**), hands the newcomer
everyone already present (a **batch spawn**), and destroys it everywhere on
disconnect (a **despawn**). It is the *match presence* model (join/leave) plus a
dynamic spawn system, meant for fast prototyping and later customization.

This layer only manages **discovering / creating / destroying** actors. Movement
reuses the transform-sync snapshot path unchanged — a spawned remote proxy is
bound to a `RemoteInterpolated` object and animated by the normal snapshots.

## Owner movement modes

### Relay (default, client-authoritative)

The local player moves with **native engine input/physics** (e.g. Unreal's
`CharacterMovement`); Citadel relays its transform to the server each tick
(`KIND_NA_STATE`), the server applies it, and the normal per-client snapshots
replicate it to every other client, who see it interpolated. This is the fast,
zero-friction path: it drops onto an existing third-person actor without touching
its movement. It is **not** movement-authoritative (no anti-cheat on position) —
for a server-authoritative owner use the
[`OwnerPredicted` prediction path](./transform-sync#owner-prediction-reconciliation--server-rewind)
instead. The two can coexist: relay for prototyping, prediction for competitive
actors.

The server marks a relay object **owner-predicted server-side** so the sim tick
never integrates it — its transform comes solely from the owner's `NA_STATE`.

### PredictedAuthoritative (opt-in, server-authoritative)

Set `transform_sync.predicted_authoritative_archetypes = [<archetype id>]` on the
server and select the same mode in the engine before announcing presence. The
presence/spawn wire remains unchanged: after the owner's self-spawn, the server
sends `KIND_TSYNC_ROLE`; the local component predicts input, sends the existing
sequenced `KIND_TSYNC_INPUT` frames, and reconciles against snapshots carrying
`last_input_seq`. Raw `KIND_NA_STATE` is rejected for this mode. Remote proxies
remain normal `RemoteInterpolated` actors in both modes.

## The frames

Five envelope kinds on the reserved networked-actor range (16–20):

| Kind | Const | Direction | Delivery | Purpose |
|---|---|---|---|---|
| 16 | `KIND_NA_PRESENCE` | C→S | reliable | Announce this client's avatar `{archetype_id, transform}` |
| 17 | `KIND_NA_SPAWN` | S→C | reliable | Spawn one actor `{object_id, archetype_id, owner, transform}` |
| 18 | `KIND_NA_SPAWN_BATCH` | S→C | reliable | Every actor already present (sent to a newcomer) |
| 19 | `KIND_NA_DESPAWN` | S→C | reliable | Destroy the actor bound to `{object_id}` |
| 20 | `KIND_NA_STATE` | C→S | unreliable | Owner's relay transform report `{object_id, transform}` |

Transforms here are **raw** `f32` (position `[3]`, rotation quaternion `xyzw`
`[4]`, velocity `[3]`) — presence/spawn are rare and `NA_STATE` is one small
packet per owner per tick, while the bandwidth-sensitive observer path is the
already-quantized snapshot.

## Flow

1. On connect the client opts into transform sync (`HELLO`) and sends
   `KIND_NA_PRESENCE` with its archetype id and spawn transform.
2. The server assigns an `object_id`, spawns the object, and:
   - sends the owner its **own** spawn first (so it learns its object id and
     latches its participant id from the `owner` field), then a
     `KIND_NA_SPAWN_BATCH` of everyone already present;
   - sends every other present client a `KIND_NA_SPAWN` for the newcomer.
3. The owner relays its transform each tick via `KIND_NA_STATE`. The server
   applies it (after an ownership check — a client can never move another
   player's object) and the normal snapshots carry it to observers.
4. On disconnect the server despawns the object and broadcasts
   `KIND_NA_DESPAWN` to the remaining clients.

Ownership latching needs **no extra wire field**: because the server sends the
owner its own spawn before anything else (reliable, ordered), the client treats
the first spawn after announcing as itself.

## Multiple archetypes

The `archetype_id` (`u16`) lets one client register several actor classes
(players, NPCs, projectiles). The client maps each id to an engine class; the
server (or game logic) chooses which archetype to spawn.

## Unreal API

`UCitadelNetworkedActorSubsystem` (a `GameInstanceSubsystem`) does the spawning,
binds each proxy to a `UCitadelTransformSync`, and relays the local pawn's
transform:

```cpp
auto* NA = GetGameInstance->GetSubsystem<UCitadelNetworkedActorSubsystem>;
NA->RegisterArchetype(0, BP_ThirdPersonCharacterClass); // archetype id -> class
NA->AnnouncePresence(MyPawn, /*ArchetypeId=*/0);        // relay MyPawn to peers
```

`AnnouncePresence` opts into transform sync, sends `KIND_NA_PRESENCE`, and starts
relaying `MyPawn`'s transform (`StateSendHz`, default 30). Citadel spawns only the
**remote proxies** — the local pawn stays the native one.

Give a spawnable actor the `ICitadelReplicated` interface to react to spawn /
despawn:

```cpp
// BlueprintNativeEvent — override in C++ or Blueprint.
void OnCitadelSpawn(const FCitadelSpawnInfo& Info); // Info.bIsLocalOwner, ObjectId, OwnerId, ArchetypeId
void OnCitadelDespawn;
```

- `Info.bIsLocalOwner == true`: this is your own player — possess it with the
  local `PlayerController` and enable input/camera.
- `Info.bIsLocalOwner == false`: a **remote proxy** — it is interpolated from
  snapshots and must **not** be possessed by the local `PlayerController`.

## Server-owned actors (NPCs)

The fan-out above is driven by a connecting **client**. The server can also own
actors nobody is connected as — **NPCs, projectiles, pickups** — and drive them
from Lua game logic. A server-owned actor is a Networked Actor with **`owner = 0`**
(no participant): the server spawns it, moves it each tick, and it replicates to
every client through the same `NA_SPAWN` / snapshot / `NA_DESPAWN` path. No client
change is needed beyond having a class registered for its archetype, and late
joiners receive live NPCs in their presence batch.

### Lua API

| Function | Purpose |
|---|---|
| `citadel.spawn_actor{ archetype, x, y, z, ai?, map?, waypoints?, speed? } -> id` | Spawn a server-owned actor; `ai = "patrol"` follows waypoints through the cooked map. |
| `citadel.move_actor(id, x, y, z [, vx, vy, vz])` | Set its transform (and optional velocity, for smooth client interpolation). |
| `citadel.despawn_actor(id)` | Destroy it; broadcasts `NA_DESPAWN`. |

Movement can be written from `on_tick`, or declared as `ai = "patrol"` with a
loaded map and waypoints. The server queries a Detour corridor from cooked
collision geometry and emits normal `MoveActor` commands, so clients interpolate
the replicated NPC exactly like a remote player.

:::caution[Requires the game loop]
`spawn_actor` / `move_actor` run from `on_tick`, which only fires when
`runtime.tick_hz > 0`. With the default `tick_hz = 0` the loop is disabled and no
NPC is ever spawned — set a rate (e.g. `30`) in [configuration](./configuration).
:::

Example — one NPC patrolling a square loop, replicated to every client:

```lua
local npcs, spawned = {}, false

citadel.on_tick(function(dt)
    if not spawned then
        spawned = true
        local wps = { {0,0,302}, {500,0,302}, {500,500,302}, {0,500,302} }
        local s = wps[1]
        local id = citadel.spawn_actor{ archetype = 0, x = s[1], y = s[2], z = s[3] }
        npcs[id] = { pos = { s[1], s[2], s[3] }, wps = wps, idx = 2, speed = 250 }
    end
    for id, n in pairs(npcs) do
        local wp = n.wps[n.idx]
        local dx, dy, dz = wp[1] - n.pos[1], wp[2] - n.pos[2], wp[3] - n.pos[3]
        local dist = math.sqrt(dx * dx + dy * dy + dz * dz)
        if dist < 15 then
            n.idx = (n.idx % #n.wps) + 1                 -- next waypoint (loop)
        else
            local step = math.min(n.speed * dt, dist)
            local ux, uy, uz = dx / dist, dy / dist, dz / dist
            n.pos[1], n.pos[2], n.pos[3] =
                n.pos[1] + ux * step, n.pos[2] + uy * step, n.pos[3] + uz * step
            citadel.move_actor(id, n.pos[1], n.pos[2], n.pos[3],
                ux * n.speed, uy * n.speed, uz * n.speed) -- position + velocity
        end
    end
end)
```

`archetype = 0` reuses the player class you already registered
(`RegisterArchetype(0, …)` above), so the NPC renders with **no extra client
code**. Coordinates are world-space cm — set `z` to your character's spawn height
so the NPC stands on the floor rather than sinking through it.

## Limitations

- Relay movement is client-authoritative (no position anti-cheat); opt an
  archetype into `PredictedAuthoritative` for validated owner input.
- Unity/Godot now expose transform-sync engine surfaces over the shared native
  runtime; Networked-Actor spawn/presence integration remains Unreal-first.
- Server-owned NPCs move in straight lines; pathfinding around level geometry
  needs the navmesh bake (a later phase).
- NPCs are currently global (every client sees them), not yet room-scoped.
- In-editor "two clients see each other move / a leaver's proxy disappears / an
  NPC walks its loop" is a manual verification step (not covered by CI).
