---
title: NetworkPeer property replication
description: Opt-in authoritative property replication with schema validation, per-receiver deltas, and separately verified engine bindings.
---

`NetworkPeer` auto-syncs an actor's replication-flagged properties to the Citadel
server as bit-packed deltas. This page documents the implemented surface: the
property table, the shared `schema_hash` identity, the push/shadow dirty-tracking
model, and the engine bindings that use the shared codec.

When `[transport.network_peer]` is enabled, the production gateway attaches the
authority and routes `KIND_REP_DELTA` / `KIND_REP_ACK`; it also sends a schema
and full-baseline bootstrap at gateway admission. Trusted server lifecycle code
must still register classes and spawn objects—clients cannot author either.
The authority re-encodes accepted `DeltaBunch` frames and uses its shared interest
grid for relevance. A receiver that leaves relevance loses its baseline; on
re-entry it receives a full snapshot. `COND_*` masking remains applied before
the per-receiver delta is built.

This gateway activation is **not** match/room lifecycle integration. Admission
initially uses the gateway's default match (`0`), and trusted code may call the
explicit match join seam; Citadel does not yet bind NetworkPeer AOI to room or
matchmaker ownership automatically. The stable foundation remains the
layout/identity and change-detection contract (including the safety net that
turns a forgotten "mark dirty" into a build failure instead of a silent desync).
This builds directly on the shared
[netcode codecs & wire foundation](/reference/protocol/netcode-codecs/).

## The property table

Each replicated class has an immutable **property table** built **once at
registration** — never per frame. On Unreal it is built by walking the actor's
`CPF_Net` reflected properties; on the server it is built with a small builder.
Every replicated field becomes an ordered `FieldDesc` carrying:

| Member | Meaning |
|---|---|
| `field_id` | Stable handle = the field's **registration-order index**. Mapped both ends; never sent as a name. |
| `type_tag` | Field type (`Bool`, `Int`, `Uint`, `Scalar`, `Vector3`, `Quat`, `Bytes`, `Enum`). |
| `codec_id` | The shared `citadel-wire` codec used to (de)serialize the value. |
| `cond` | Replication condition (`COND_*` analogue: `None`, `OwnerOnly`, `SkipOwner`, `InitialOnly`, `SimulatedOnly`, `AutonomousOnly`, `Custom`, `Never`). |
| `authority` | `ServerOnly` (default) or `ClientOwned`. |
| `bounds` | Server-side validation envelope (numeric range / max length / max cardinality). |
| `push_based` | Whether the field is on the `mark_dirty` fast path or the mandatory shadow net. |

Because `field_id` is assigned by registration order, the layout is shareable
without sending names. **Reordering or inserting a field mid-list renumbers
handles** and changes the schema hash.

## Schema hash

A class layout's identity is the wide canonical **`schema_hash`** — the 128-bit
BLAKE3-derived digest (algorithm `blake3-128/citadel.schema.v1`) over the ordered
`(field_id, type_tag, codec_id, cond, authority, bounds_shape)` tuples, paired
with an explicit `layout_version`. It is deterministic for an identical layout and
changes when any of a field's type, codec, condition, authority, bounds, the
field order, or the `layout_version` changes. Independently built SDKs derive the
same hash for the same class, so a client/server layout mismatch is detectable at
handshake (enforcement lands with the wire delta).

`bounds_shape` is a 64-bit **FNV-1a** fold of a field's bounds together with a
stable per-field **name key**, so the hash also binds each field's identity: two
structurally identical fields (same type/codec/bounds) swapped in registration
order still produce a different hash. The property name is never sent on the wire —
only its fold participates in the hash. This packing is part of the schema
contract and every SDK reproduces it bit-for-bit.

## Authoring surfaces and engine status

The native **C ABI v3** encoder and the Rust `citadel-client` facade both author
schema-bound full snapshots and deltas for bool, bounded int, scalar, bytes,
`Vector3`, quaternion, and keyed collections. They use the canonical wire codec;
invalid or mismatched input fails the whole authored bunch. The C ABI does not
register clients, classes, objects, or matches and does not activate replication
by itself.

Unity has a source-level managed v3 `CitadelNetworkPeerAuthor` wrapper. Unreal
has its property-table/declaration source and Godot has native codec source. None
of those engine bindings has a current in-engine runtime verification: Unity,
Unreal, and Godot were unavailable in this environment. Treat all engine cells as
**partial**, rather than evidence of an end-to-end editor/gameplay integration.

<Tabs syncKey="engine">
  <TabItem label="C++ (Unreal)">

Use `UCitadelNetworkPeer` for declaration, dirty tracking, and frame handling.
The component resolves the replicated layout when the actor is registered.

  </TabItem>
  <TabItem label="Blueprint (Unreal)">

1. Add a **Citadel Network Peer** component to the replicated actor.
2. Mark the supported properties as replicated and configure their authority.
3. Route incoming authoritative frames to the component; it applies the cached layout.

  </TabItem>
  <TabItem label="C# (Unity)">

The managed v3 authoring wrapper is present in source, but it has not been run in
a Unity runtime here. It is a partial binding, not verified engine integration.

```csharp
var body = CitadelNetworkPeerAuthor.Encode(objectId, isFull, resultId, baseId,
    schemaHash, layoutVersion, fields);
```

  </TabItem>
  <TabItem label="GDScript (Godot)">

Godot native codec source remains available, but no Godot runtime was available
for ABI v3 verification. Do not treat this as a verified typed-authoring binding.

  </TabItem>
  <TabItem label="JavaScript">

The browser SDK exposes the schema-bound codec without requiring a browser
runtime for its structural tests. Use it with the reliable `KIND_REP_DELTA` and
`KIND_REP_ACK` envelopes; a missing base returns `needs_full`, sends no ACK,
and does not invent state.

```js
const schema = { hash: new Uint8Array(16), layoutVersion: 3,
  fields: [{ type: "vector3", min: -100, max: 100, valuesPerUnit: 10 }] };
const author = new NetworkPeerAuthor(schema);
const full = author.full(42, 1n, new Map([[0, [1, 2, 3]]]));
const session = new NetworkPeerSession(schema);
const outcome = session.apply(full); // { status: "applied", bunch }
client.send(KIND_REP_ACK, session.ackBody());
```

`NetworkPeerAuthor` / `NetworkPeerSession` do not register an object, apply a
value to a renderer, or add a full-recovery wire request. A real browser
Two-client gameplay run remains deferred external-environment verification.

  </TabItem>
  <TabItem label="Rust">

```rust
let body = author.full(object_id, result_id)
    .unwrap()
    .vector3(position_field, [1.0, 2.0, 3.0])
    .quat(rotation_field, [0.0, 0.0, 0.0, 1.0])
    .finish()?;
```

`NetworkPeerAuthor` binds the canonical `RepSchema`; its typed draft also accepts
bool, int, scalar, bytes, and `CollectionDelta`.

  </TabItem>
</Tabs>

## Change detection: push-model + shadow safety net

A field is assumed **clean** unless a write marks it dirty (UE5 Push Model). The
dirty state is a fixed bitset, one bit per `field_id`; marking is O(1) and a tick
diff is O(registered fields), never O(all properties).

The push model's sharp edge — *forget to mark dirty and the change silently never
replicates* — is contained by three layers:

1. **Structurally-unavoidable marks.** On Unreal a push field's raw value is
   private and the only mutator is the auto-marking `TCitadelReplicated<T>`
   accessor, whose `operator=` marks it dirty. A write cannot bypass the mark.
2. **Mandatory shadow-diff net.** Fields that cannot be wrapped that way
   (strings, collections, nested-struct mutation) are declared non-push and are
   covered by a shadow diff over **only the registered fields**, run once per
   tick, that ORs any difference into the dirty set.
3. **Pre-encode audit (fails closed).** In development/CI, an audit runs over
   **all** fields **before** the tick's delta is built; any field that changed
   without a dirty bit is a **hard failure**, not a warning. A forgotten mark
   fails the build/test rather than shipping stale state.

## Unreal declaration API

Keep writing idiomatic UE and declare the Citadel codec/bounds/authority next to
the property:

```cpp
UPROPERTY(Replicated)
int32 Health;

void AMyPawn::GetLifetimeReplicatedProps(TArray<FLifetimeProperty>& Out) const {
    Super::GetLifetimeReplicatedProps(Out);
    DOREP_CITADEL_CLAMPED(AMyPawn, Health, 0, 100, ECitadelFieldAuthority::ServerOnly);
    DOREP_CITADEL_COND(AMyPawn, Nameplate, ECitadelRepCondition::SimulatedOnly,
                       ECitadelFieldAuthority::ServerOnly);
    DOREP_CITADEL_CLIENTOWNED(AMyPawn, EmoteState); // cosmetic, client may propose
}
```

Add a `UCitadelNetworkPeer` component to the actor; it resolves the cached
`FCitadelRepLayout` once at registration and owns the dirty mask, shadow buffer,
and the dev audit. Mutate push fields through `TCitadelReplicated<T>` so the mark
is automatic.

## Status and limits

- **Partial, opt-in production activation:** the gateway authority is off by
default and needs trusted class/object registration. It validates, applies,
rebroadcasts, bootstraps schema/baselines, and uses authority-level shared-grid
relevance when enabled.
- **Available authoring contracts:** C ABI v3 and Rust support typed scalar,
vector, quaternion, and keyed-collection authoring. They do not automate client
registration, schema/object lifecycle, or transport send.
- **Separate deferred work:** match/room-scoped AOI lifecycle wiring and all
engine-runtime verification are not provided by gateway activation. Engine tools
were unavailable for this documentation pass.
- Schema evolution is an explicit opt-in append-only server contract; strict
single-version registration remains the default.
