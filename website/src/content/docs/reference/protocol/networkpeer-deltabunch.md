---
title: NetworkPeer DeltaBunch (wire)
description: The bit-packed DeltaBunch wire format, FastArray-style keyed collections, per-connection baselines/acks, and the client schema_hash + encoder C ABI that carry Citadel's NetworkPeer replication.
---

Phase 2 of `NetworkPeer` (building on the [property table & dirty
tracking](/reference/client-sdk/networkpeer-replication/)) implements the **wire layer**: the
bit-packed `DeltaBunch`, keyed collections, per-connection baselines, and the
client-side `schema_hash` over the C ABI. It reuses the shared
[netcode codecs & wire foundation](/reference/protocol/netcode-codecs/) and rides three
reserved envelope kinds: `KIND_REP_DELTA=13`, `KIND_REP_ACK=14`,
`KIND_REP_SCHEMA=15`.

This page is the codec + baseline mechanics and the client encode path; the
**server** validate/apply/rebroadcast trust boundary is the
[NetworkPeer Server Authority](/reference/server-sdk/networkpeer-authority/) page.

## The DeltaBunch

One bit-packed packet per actor per tick batches all of that actor's changed
fields. Per-bunch layout (MSB-first bit stream):

```text
object_id      : 32 bits
is_full        : 1 bit
result_id      : bit-varint (nonzero, server-issued monotonic token; acks name it)
if !is_full: base_id : bit-varint (nonzero; the token this delta is diffed against)
if is_full:  schema_hash (128 bits) + layout_version (32 bits)  -- gated vs local schema
changed_mask   : num_fields bits (fixed by the class schema, never the payload)
per set field (ascending order): a scalar value OR a keyed-collection block
```

Key rules:

- **Explicit `is_full`; split `result_id` / `base_id`.** Every bunch establishes a
  **nonzero** `result_id`. `base_id == 0` means a full snapshot (no base). A
  non-full bunch with a zero `base_id`, or any bunch with a zero `result_id`, is
  rejected — no overloaded "`baseline_id == 0`" meaning.
- **Full snapshots carry the schema identity.** The 128-bit `schema_hash` +
  `layout_version` are embedded on `is_full`; a decoder whose local schema differs
  rejects the whole bunch (fail closed).
- **Self-synchronizing decode.** Byte-blob and collection counts are
  length-delimited and checked against the schema cap **and** a hard cap *before*
  any allocation. Varints are canonical (overlong rejected), group-capped, and
  overflow-checked. A short/over read aborts the **whole** bunch — never a partial
  apply. Coalesced bunches are byte-length-framed and decoded in isolation, so a
  hostile length in one bunch cannot corrupt the next.

## Collections (keyed delta)

A collection field carries `removed` / `added` / `changed`:

- Element id `rep_id = { index: u32, generation: u32 }`. The **generation** tag
  makes a reused slot a distinct id, so remove-then-add of the same slot is
  `removed(old gen)` + `added(new gen)`, never an ambiguous in-place change.
- `rep_key` is a `u64` edit counter (effectively never wraps).
- `removed` is a compact id list — survivors are never re-sent.
- The decoder rejects **duplicate `rep_id`s** within or across the three sets and
  caps total ops.

## Baselines and acks

Baselines are **per connection**. The server mints a monotonic **nonzero**
`result_id` per emitted bunch; a receiver acks with `KIND_REP_ACK`
(`[(object_id, result_id, history)…]`, reusing the shared 32-bit ack window). The
baseline advances **only** to an outstanding, strictly-newer token — a
stale/replayed/forged ack never regresses it. Consecutive deltas before an ack all
diff against the last **acked** baseline (cumulative), so a dropped intermediate
delta still carries the change. A newly-relevant receiver, an ack timeout, or a
capped/overflowed baseline falls back to a full snapshot.

### Missing-base recovery status

The currently specified NetworkPeer transport has only `KIND_REP_DELTA`,
`KIND_REP_ACK`, and `KIND_REP_SCHEMA`; it has **no client-to-server
full-recovery/resend request**. On a missing base, a client must reject the
whole bunch atomically, send no ACK for it, and keep no invented baseline. It
then waits for the server's existing full-baseline, relevance re-entry, or ACK
-timeout policy. This is not a prompt resend guarantee. A future prompt
recovery feature must add a named wire kind, its body, gateway handling, and
interoperability tests before SDKs may claim to request a full snapshot.

## Client `schema_hash` + typed authoring (C ABI v3 and Rust)

C ABI **v3** and the Rust client author packets through the one canonical wire
implementation, so their bits match the server codec:

- `citadel_schema_hash(layout_version, fields[], count, out_hash[16])` computes the
  wide 128-bit digest over the ordered field tuples (the `bounds_shape` FNV fold is
  reproduced natively; only the BLAKE3-128 digest crosses the ABI).
- `citadel_rep_encoder_*` is a transactional builder: `new` → optional
  `set_schema` for a full snapshot → typed `add_*` calls → `finish` → `free`.
  ABI v3 adds `add_vector3`, `add_quat`, and `add_collection` alongside bool,
  int, scalar, and bytes. Collection item codecs support those same scalar/vector/
  quaternion kinds; a validation failure makes `finish` emit no partial bunch.
- Rust `NetworkPeerAuthor` binds a `RepSchema` and exposes typed draft methods for
  bool, int, scalar, vector3, quaternion, bytes, and `CollectionDelta`.

These authoring APIs create bytes only. They do not register a client, class, or
object; send an envelope; activate the server authority; or prove an engine
binding works at runtime. See the [C ABI reference](/reference/client-sdk/c-abi/).

## Contract & parity

`contract.json` gains a `netcode.netpeer` block pinning the bunch framing, the
bit-packed varint form, the collection model, and the decoder caps. The three
`KIND_REP_*` constants are declared in the Unreal SDK header and verified by
Tier-A parity; the new C ABI functions are bound in the Tier-B signature check.

## Status and limits

- Implemented: the `DeltaBunch` codec + coalescing, keyed collections,
  per-connection baseline/ack, C ABI v3 typed authoring, and the Rust typed
  authoring facade.
- Gateway authority activation is separately opt-in; it needs trusted lifecycle
  registration and is not automatic match/room AOI integration. See
  [NetworkPeer Server Authority](/reference/server-sdk/networkpeer-authority/).
- Engine runtime verification is deferred because Unity, Unreal, and Godot were
  unavailable in this environment. Schema evolution remains the explicit
  server-side append-only compatibility contract described in the
  [NetworkPeer schema evolution reference](/reference/server-sdk/networkpeer-schema-evolution/).
