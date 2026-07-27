---
title: NetworkPeer property replication
description: Authoritative property replication with schema validation, per-receiver deltas, and interest-filtered cross-engine delivery.
---

`NetworkPeer` auto-syncs an actor's replication-flagged properties to the Citadel
server as bit-packed deltas. This page documents the implemented surface: the
property table, the shared `schema_hash` identity, the push/shadow dirty-tracking
model, and the engine bindings that use the shared codec.

NetworkPeer now validates client-owned input at the server, re-encodes
authoritative `DeltaBunch` frames, and sends each frame only to receivers in the
shared interest grid. A receiver that leaves relevance loses its baseline; on
re-entry it receives a full snapshot. `COND_*` masking remains applied before
the per-receiver delta is built.
The stable foundation remains the layout/identity and the change-detection
contract (including the safety net that turns a forgotten
"mark dirty" into a build failure instead of a silent desync). The `DeltaBunch`
wire body, per-connection baseline/ack, and the server validation pipeline are
implemented. This builds directly on the shared
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

## Unity and Godot codec access

Unity and Godot use the same Rust C-ABI DeltaBunch codec as the server. This keeps
bit packing, schema validation, and malformed-frame handling consistent across
engines. Supply the ordered codec table for the class layout when decoding a
server frame; client-owned scalar fields use the matching encoder before they are
sent to the authority pipeline.

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

```csharp
using var delta = CitadelNetworkPeerDelta.Decode(body, schemaHash, layoutVersion, codecs);
var health = delta.FieldAt(0).int_value;
```

`codecs` is the ordered `CitadelNative.RepCodec[]` for the actor layout. Dispose
the returned delta when the fields have been read.

  </TabItem>
  <TabItem label="GDScript (Godot)">

```gdscript
var delta := client.decode_rep_delta(body, schema_hash, layout_version, codecs)
if not delta.is_empty:
	var health := delta.fields[0].int
```

`codecs` is an ordered array of dictionaries with the C-ABI codec bounds. Use
`encode_rep_delta` for client-owned bool, int, scalar, or byte fields.

  </TabItem>
  <TabItem label="Rust">

```rust
let delta = DeltaBunch::decode(&body, &layout, &mut allocation_budget)?;
```

The Rust client uses the same `citadel-wire` layout and codec definitions.

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

- Implemented: property layout/schema identity, dirty auditing, DeltaBunch
  baseline/ack, authoritative bounds/rate validation, and interest-filtered
  rebroadcast.
- Unity and Godot consume authoritative scalar deltas and encode client-owned
  scalar fields through the same C ABI as Unreal; editor gameplay runs remain the
  manual pre-release verification.
- Schema-evolution compatibility remains the follow-up task.
