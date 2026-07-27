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

## Client `schema_hash` + encoder (C ABI)

Native SDKs compute the class identity and encode a `DeltaBunch` through the
**one** shared implementation, so their bits are byte-identical to the server's:

- `citadel_schema_hash(layout_version, fields[], count, out_hash[16])` computes the
  wide 128-bit digest over the ordered field tuples (the `bounds_shape` FNV fold is
  reproduced natively; only the BLAKE3-128 digest crosses the ABI). This closes the
  earlier gap where the Unreal `SchemaHash` was zeroed.
- `citadel_rep_encoder_*` (a `new` → `add_bool`/`add_int`/`add_scalar`/`add_bytes`
  (+ `set_schema` for a full snapshot) → `finish` → `free` builder) encodes a
  client→server `DeltaBunch` of changed **ClientOwned** scalar fields without
  reimplementing the `BitWriter` or codecs. See the [C ABI reference](/reference/client-sdk/c-abi/).

On Unreal, `FCitadelRepLayout::GetOrBuild` now fills a real `SchemaHash`, and
`UCitadelNetworkPeer::BuildDeltaBunch(is_full, result_id, base_id)` returns the
bytes to send under `KIND_REP_DELTA`.

## Contract & parity

`contract.json` gains a `netcode.netpeer` block pinning the bunch framing, the
bit-packed varint form, the collection model, and the decoder caps. The three
`KIND_REP_*` constants are declared in the Unreal SDK header and verified by
Tier-A parity; the new C ABI functions are bound in the Tier-B signature check.

## Status and limits

- Implemented: the `DeltaBunch` codec + coalescing, keyed collections, the
  per-connection baseline/ack orchestration, and the client `schema_hash` +
  scalar-field encoder over the C ABI.
- The server validate/apply/rebroadcast pipeline (ownership, bounds, rate) and the
  receiver-side apply guard now ship — see
  [NetworkPeer Server Authority](/reference/server-sdk/networkpeer-authority/).
- Not yet shipped: client collection and vector/quat field encode over the C ABI;
  the server retains full support. Schema evolution is available as the explicit
  server-side append-only compatibility contract described in the
  [NetworkPeer schema evolution reference](/reference/server-sdk/networkpeer-schema-evolution/).
