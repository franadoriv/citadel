---
title: Transform sync (snapshots)
description: Authoritative per-client transform snapshots over unreliable QUIC datagrams — HELLO negotiation, delta-vs-baseline snapshots, acks, roles, and the Unreal component.
---

Citadel ships **authoritative transform synchronization** out of the box: the
server owns every networked object's transform, advances it on a fixed sim tick,
and streams **per-client delta snapshots** on the unreliable QUIC/WebTransport
datagram path. Clients render remote/server objects interpolated in the past
(roles `RemoteInterpolated` / `ServerSimulated` / `StaticReplicated`). It also
ships **client-side prediction + reconciliation, server rewind (lag
compensation), and adaptive congestion** for the `OwnerPredicted` role — see
[Owner prediction, reconciliation & server rewind](#owner-prediction-reconciliation--server-rewind)
below.

It is built on the shared [netcode codecs & wire foundation](./netcode-codecs)
(bit packing, quantized position/rotation, the ack window, the interest grid), so
every SDK encodes identical bits. The legacy `KIND_POSITION` relay
([envelope format](./envelope)) is unaffected — transform sync is additive.

For drop-in player replication (a client announces its avatar on connect and the
server spawns it on every peer, with no per-object wiring), see
[networked actors](./networked-actors), which layers presence + dynamic spawn on
top of this snapshot path.

## Enabling it

Off by default. In `citadel.toml`:

```toml
[transport.transform_sync]
enabled = true
send_rate_hz = 20   # snapshot packets/sec (the client sizes its buffer from this)
sim_hz = 60         # world simulation ticks/sec
budget = 16         # safe full-baseline budget; 0 deliberately opts out of the MTU cap
demo_movers = 2     # spawn N built-in server-simulated demo avatars (0 = none)
player_slots = 0    # hand each client an owner-predicted player object (0 = off)
# Networked-Actor archetypes that use server validation + client prediction.
# All unlisted archetypes stay byte-identical Relay by default.
predicted_authoritative_archetypes = [2]
```

Two zero-config demo modes (pick one — they share the low object ids, so
`player_slots` takes precedence and suppresses `demo_movers`):

- **`demo_movers = N`** spawns server-simulated avatars (object ids `1..=N`) on
  opposing paths. They move on their own, so a two-client demo shows smooth
  remote interpolation with **no client input and no game script**.
- **`player_slots = N`** is the **client-owned player** demo: the server hands each
  connecting transform-sync client ownership of one object (ids `1..=N`, by join
  order) as `OwnerPredicted`, and frees it on disconnect. Each client **drives its
  own object with input** (client-side prediction + server authority) while every
  other client sees it interpolated — "two clients see each other move." See
  [Owner prediction](#owner-prediction-reconciliation--server-rewind).

## The handshake and frames

Four envelope kinds on the reserved transform range (see
[netcode codecs](./netcode-codecs) for the range reservation):

| Kind | Const | Direction | Delivery | Purpose |
|---|---|---|---|---|
| 7 | `KIND_TSYNC_HELLO` | C↔S | reliable | Negotiate world bounds, precision, quat mode, send/sim rate |
| 8 | `KIND_TSYNC_SNAPSHOT` | S→C | unreliable | Per-client delta snapshot (the hot path) |
| 9 | `KIND_TSYNC_INPUT` | C→S | unreliable | Owner input bundle (redundant frames + optional fire) |
| 10 | `KIND_TSYNC_ACK` | C→S | unreliable | Ack the newest applied snapshot (+ 32-bit history) |
| 11 | `KIND_TSYNC_ROLE` | S→C | reliable | Ownership/role/relevancy transition |
| 12 | `KIND_TSYNC_REWIND` | S→C | reliable | Authoritative lag-compensated hit result |

Flow:

1. The client sends `KIND_TSYNC_HELLO` (empty body) over the reliable path to opt
   in. The server replies with its negotiation — position bounds, velocity bounds,
   quaternion mode (9/10/15-bit smallest-three), and the send/sim rates. Both sides
   build the identical codec.
2. The server streams `KIND_TSYNC_SNAPSHOT` datagrams. Each carries an absolute
   `snapshot_id`, the absolute `base_snapshot_id` it was diffed against (`0` = full
   baseline), the current send rate, a list of removed object ids, and the changed
   objects. Per object: `object_id`, `gen_epoch`, a 3-bit changed mask, then the
   present fields (quantized position, smallest-three rotation, quantized velocity).
   Absent fields are unchanged and filled from the base.
3. The client reconstructs `full[id] = full[base] − removed + updates`, renders
   each object interpolated in the past, and acks with `KIND_TSYNC_ACK`.

## Correctness under loss and reorder

QUIC datagrams are unreliable **and unordered**, so the snapshot protocol is
explicitly loss/reorder-safe:

- Every snapshot names an **absolute** `base_snapshot_id`. The server only ever
  diffs against a baseline the client has **acked** (and the server still holds),
  so a delta can never reference a base the client lacks.
- The client **discards** any snapshot whose base it does not hold, and applies
  only snapshots strictly newer than the last it applied (monotonic guard). A
  discarded snapshot is recovered by the next one whose base the client holds — no
  explicit retransmit.
- Objects entering/leaving a client's area of interest are handled by set
  membership: an exiting object is listed in `removed`; a re-entering object is
  absent from the base and sent as a fresh full baseline. `gen_epoch` guards only
  object-id reuse / respawn.
- **QUIC owns pacing.** The application never runs a second congestion controller;
  under pressure it sends fewer/lower-priority objects per snapshot (`budget`),
  and QUIC decides when bytes go out.

## Client rendering

Clients render remote/server objects **in the past**, interpolating between the
two buffered snapshots that bracket render time: **Hermite** position (when
velocity is replicated) + **slerp** rotation, with a jitter buffer sized from the
send rate and **bounded extrapolation** when the buffer drains. This is the
reusable client runtime shared across engine SDKs.

The jitter buffer is **adaptive**. Its render delay is a multiple of the send
interval that starts at a safe ceiling (`2.5×`) and, per applied snapshot, decays
toward a floor (`1.5×`) on a clean link and grows back toward the ceiling when a
snapshot is lost (detected as a gap in applied snapshot ids). On localhost/LAN
with no loss it converges to the floor automatically — the lowest latency that
still guarantees two samples to interpolate between — while a lossy link keeps the
larger margin. No configuration; the same logic runs in the Rust `RemoteWorldView`
(unit-tested) and its faithful C++ port. Higher `send_rate_hz` shrinks the delay
further because the buffer is sized in *packets*, not seconds (e.g. `2.5×` is
125 ms at 20 Hz but 42 ms at 60 Hz; the floor is 75 ms vs 25 ms).

### Unreal

The `UCitadelTransformSync` component (`clients/unreal/`) binds a replicated actor
to a server object id and applies the interpolated authoritative transform each
frame:

```cpp
GetGameInstance->GetSubsystem<UCitadelTransformSyncSubsystem>->OptIn;

UCitadelTransformSync* Sync = Actor->CreateDefaultSubobject<UCitadelTransformSync>(TEXT("Sync"));
Sync->ObjectId = 1;
Sync->Role = ECitadelSyncRole::ServerSimulated;
Sync->bHermitePosition = true; // needs replicated velocity
```

See `clients/unreal/README.md` for the full setup and the manual two-client demo
(including injecting loss/latency with a network conditioner).

### Unity

`CitadelTransformSync` is a `MonoBehaviour` over the shared native transform
runtime. Route `KIND_TSYNC_HELLO` and `KIND_TSYNC_SNAPSHOT` from your connection's
single poll loop to `HandleEnvelope`; it creates the Rust view, applies snapshots,
and sends `KIND_TSYNC_ACK` itself. Remote objects receive the adaptive
Hermite+slerp sample on their Unity `Transform`.

```csharp
var sync = remoteActor.AddComponent<CitadelTransformSync>;
sync.Client = connection.Client;
sync.ObjectId = objectId;

// In the one envelope dispatcher:
sync.HandleEnvelope(kind, payload, payloadLength);
```

For a local predicted owner, set `IsLocalOwner = true`. Your input controller
still moves immediately; the component consumes the authoritative state plus the
highest contiguous input acknowledgement to make visual corrections. Unity uses
metres by convention, while Citadel's shared runtime uses centimetres, so the
component converts positions at its engine boundary.

### Godot

`CitadelTransformSync` is the matching `Node3D` surface. Bind the scene's shared
`CitadelClient`, then route transform envelopes from its one poll loop. The
GDExtension calls the same `citadel_transform_view_*` C ABI as Unity; Godot does
not maintain a second snapshot decoder.

```gdscript
var sync := CitadelTransformSync.new
sync.object_id = object_id
sync.bind_client(client)
add_child(sync)

# In the connection dispatcher:
sync.handle_envelope(client, kind, payload)
```

The Godot native GDExtension remains a manual in-editor integration step. Its
required behavior is identical to Unity: remote actors interpolate; local owners
apply input immediately and correct from the acknowledged authoritative state.

## P3 hardening defaults

- **MTU budget:** snapshots use a 1,200-byte safe payload ceiling, including the
  two-byte envelope kind. The default snapshot budget is capped at 16 full object
  updates for this envelope size; larger worlds split naturally across snapshot
  ticks rather than relying on IP fragmentation.
- **AOI scale:** the uniform grid stress test covers 4,096 entities. A viewer's
  precise relevance pass sees only the 3×3 neighboring cells (at most nine
  entities in the test distribution), not a global fan-out.
- **Adaptive buffer:** the shared runtime starts at 2.5 send intervals, decays to
  1.5 on a clean link, and regrows on snapshot-id gaps. This retains the existing
  20–60 pps tuning; do not add an engine-local multiplier.
- **Loss-tail harness:** at deterministic 5% loss with a six-tick recovery, the
  model pins delivered datagram p99 at 1 tick versus at least 6 ticks for an
  ordered reliable stream. It demonstrates the head-of-line tradeoff; Quinn/QUIC
  still owns real path pacing and congestion control.

## Owner prediction, reconciliation & server rewind

For the object a client **owns**, interpolating in the past would feel laggy, so
the client predicts and the server lag-compensates hits. The server assigns
ownership on the reliable `KIND_TSYNC_ROLE` frame (`OwnerPredicted` + a monotonic
`ownership_epoch`).

Ownership can be assigned two ways:

- **Server-driven player slots** (config `player_slots > 0`): the gateway assigns
  a player object to each client the moment it opts in (`HELLO`) and sends it the
  `KIND_TSYNC_ROLE`. Since that role frame reaches **only** the owner, the Unreal
  client latches its own participant id from it automatically — no manual id wiring
  is needed for the built-in demo.
- **Game-driven**: game logic calls the server ownership API to hand a specific
  object to a specific participant (e.g. on spawn), for full control over which
  actor a player drives.

### Prediction & reconciliation

- The owner applies its input **immediately** (input-latency-free) and sends
  `KIND_TSYNC_INPUT` — a **redundant bundle** of its last N individually-sequenced
  input frames (`input_seq`, `sim_tick`, `dt`, `object_id`, `ownership_epoch`, a
  kinematic `move_velocity`, opaque game payload, optional fire), plus a
  piggybacked snapshot ack. Redundancy makes a single lost input self-heal.
- The server validates each input (ownership, epoch, rate, bounds), applies inputs
  **strictly in seq order** (buffering out-of-order frames until the gap fills),
  and echoes the **highest *contiguous* applied seq** back to the owner as
  `last_input_seq` (a per-owner field on that client's snapshot).
- On receiving the ack the owner **reconciles**: it snaps its simulation/collision
  state to the authoritative post-input state, drops inputs `<= last_input_seq`,
  and replays only inputs `> last_input_seq` in order. Error smoothing applies to
  the **rendered visual offset only** (0.95 for ≤ 25 cm, 0.85 for ≥ 1 m), never
  the sim state; a teleport-scale error hard-snaps. The result: no visible
  snap-back.

### Networked-Actor owner modes

Networked Actors choose movement policy **on the server per archetype**:

- **Relay (default):** the owner sends the existing `KIND_NA_STATE` raw transform.
  Its wire bytes and behavior are unchanged for existing clients; it is convenient
  for prototypes but client-authoritative.
- **PredictedAuthoritative:** list the archetype in
  `transform_sync.predicted_authoritative_archetypes`. The server sends the normal
  `KIND_TSYNC_ROLE` after the owner's spawn, rejects `KIND_NA_STATE` for that
  object, and reuses `KIND_TSYNC_INPUT` validation, contiguous acknowledgements,
  and reconciliation. A client cannot select this policy itself.

For Unreal, call `SetPredictedAuthoritative(ArchetypeId, true)` before
`AnnouncePresence`. The local native actor is bound to `UCitadelTransformSync` as
soon as its self-spawn arrives, then the ordered role frame activates prediction.

### Server rewind (favor-the-shooter)

- A fire command **rides the input bundle** and is resolved **server-side**
  exactly once, in seq order. The client never resolves the hit.
- The **server computes and clamps the rewind time** from its own per-connection
  state — measured one-way delay (not RTT/2) + the client's interpolation delay +
  a max-unlag clamp. A client-supplied timestamp is only a hint, never trusted.
- It rewinds hit-eligible objects (~1 s of history) to the state the shooter saw,
  runs the hit test, and returns the authoritative result on `KIND_TSYNC_REWIND`.
  Lag compensation **disables above an RTT cutoff** (~220 ms), where the shot
  resolves at present state instead.
- Hit registration is against **server-side kinematic capsules**, not per-bone
  animated hitboxes (a later initiative).

### Adaptive congestion

QUIC owns byte pacing; Citadel never runs a second congestion window. The
application only steps a coarse send rate (good ↔ floor) and the per-snapshot
object budget from **composite** signals (datagram loss / ack age / jitter /
send-queue drops, with RTT as one input — not a bare RTT threshold), with
hysteresis so the rate cannot flap, and ramps interpolation delay slowly so a
rate step never jerks the rewind time.

### Unreal (owner)

```cpp
// Player-slot mode auto-latches the participant id from the assign-ROLE, so this
// is only needed when your game assigns ownership itself:
// Sub->SetLocalParticipantId(MyParticipantId);

// In your pawn each input tick (only the owner write path — e.g. bind your
// movement axis to this). The component drives the owned actor; other clients see
// it interpolated:
Sync->SubmitInput(MoveVelocityCmPerSec, DeltaSeconds);

// Fire: resolved server-side; the result arrives on OnRewindResult.
Sync->RewindHitTest(MuzzleWorldPos, AimWorldDir);
Sync->OnReconciled.AddDynamic(this, &AMyPawn::HandleReconciled);
Sync->OnRewindResult.AddDynamic(this, &AMyPawn::HandleHit);
```

## Limitations

- Kinematic capsule hitreg only (no per-bone animated hitboxes); no server-side
  physics — prediction is kinematic (CMC-style), not full-physics resimulation.
- WebSocket clients (reliable-only) cannot use the unreliable hot path.
- A single global interest grid until matches land; per-match scoping is a soft
  dependency.
- The lag profile (one-way delay / interpolation delay / RTT) is set explicitly;
  wiring it from live QUIC path stats is a follow-up.
