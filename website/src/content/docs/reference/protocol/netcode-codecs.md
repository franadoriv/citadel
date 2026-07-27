---
title: Netcode codecs & wire foundation
description: The shared bit-packing, quantized codecs, schema hash, baseline/ack model, and reserved kinds every Citadel SDK encodes identically.
---

Citadel's advanced netcode (authoritative transform sync and NetworkPeer
property replication) is built on one shared foundation in `citadel-wire`, so
every SDK — Rust, Unreal/C++, Unity/C#, Godot/GDScript — encodes **identical
bits**. This page is the consumer-facing contract; the byte-exact ground truth is
`crates/citadel-wire/tests/wire_vectors.json` and the machine-readable summary is
the `netcode` block of `crates/citadel-wire/contract.json`.

This foundation now powers **authoritative transform sync** (Phase 1) — the
`KIND_TSYNC_HELLO/SNAPSHOT/ACK/ROLE` frames (kinds 7, 8, 10, 11) that use these
codecs are implemented; see [Transform sync](./transform-sync). The NetworkPeer
frames and the transform prediction/rewind frames land in later releases. The
codecs, hash, baseline model, and reserved kinds are stable now.

## Bit order

Bits are packed **most-significant-first within each byte**. Writing an `n`-bit
value emits its most-significant bit first. The final partial byte is
zero-padded to a byte boundary, and a decoder rejects a stream with nonzero pad
bits or an unconsumed trailing byte (the encoding is canonical). Reads are
bound-before-consume: asking for more bits than remain fails without advancing.

## Quantized codecs

Canonical unit is the **centimeter**. Each codec has a stable `codec_id`
(`BOOL=1`, `SCALAR_QUANT=2`, `VECTOR3_QUANT=3`, `QUAT_SMALLEST3_9=4`,
`QUAT_SMALLEST3_10=5`, `QUAT_SMALLEST3_15=6`) that feeds the schema hash.

### Bounded fixed-point scalar

For bounds `[min, max]` at `values_per_unit` codes per cm:

```text
steps  = round((max - min) * values_per_unit)   # round = floor(x + 0.5), in f64
codes  = 0 ..= steps                             # steps + 1 distinct codes (inclusive)
bits   = ceil_log2(steps + 1)
encode: code = clamp(round((clamp(v, min, max) - min) * values_per_unit), 0, steps)
decode: value = min + code / values_per_unit     # code > steps is rejected
```

Both endpoints are exactly representable. Encoding **saturates** out-of-range
values to the bounds (it never wraps) and rejects `NaN` (`±Inf` saturate).
Decoding **rejects** an out-of-range code rather than silently clamping.

### Position vector

Three independent scalar codecs (x, y, z). The default world bounds are
±262144 cm on X/Y and ±32768 cm on Z at 8 codes/cm → 23/23/20 bits (66
bits/position). These defaults are negotiated per connection in later frames.

### Smallest-three quaternion

Modes `Bits9`/`Bits10`/`Bits15` → 29/32/47 bits (2-bit dropped-component index +
three components). Encoding normalizes the quaternion, drops the
largest-magnitude component (ties break to the lowest index, ordered
`0=x,1=y,2=z,3=w`), negates the whole quaternion so the dropped component is
non-negative, and quantizes the three kept components (ascending source index)
over `[-1/√2, +1/√2]`. Decoding clamps `1 - a² - b² - c² ≥ 0` before `sqrt`,
renormalizes, and falls back to identity on any degenerate input — it can never
produce `NaN`. Example: the identity quaternion in `Bits10` encodes to the four
bytes `E0 08 02 00`.

## Schema hash

A class layout is identified by a **128-bit BLAKE3-derived** digest plus an
explicit `layout_version` (algorithm id `blake3-128/citadel.schema.v1`). The
digest is the first 16 bytes of BLAKE3 over `b"citadel.schema.v1"`, the
`layout_version` (u32 LE), the field count (u32 LE), and each field's
`(field_id, type_tag, codec_id, cond, authority, bounds_shape)` in fixed-width
little-endian order. Fields must be in strictly ascending `field_id` order. The
server enforces a minimum accepted `layout_version` (no downgrade to a weaker
layout).

## Baseline & ack model

Deltas are sent against a **server-issued, monotonic, nonzero** baseline token
(`u64`; `0` means "none"/full snapshot). Acknowledgements use a 32-bit sliding
window: a `latest` acked id plus a bitfield where bit `k` (`0..=31`) means
`latest-1-k` was acked. The wire form is `(latest: u64, history: u32)`;
`latest == 0` means nothing acked. A receiver's baseline advances only to a token
the server actually issued and that is strictly newer, so a stale or forged ack
can never regress it.

## Reserved envelope kinds

The two tracks own disjoint kind ranges (bodies defined in later releases):

| Kind | Name | Track |
|---|---|---|
| 7 | `KIND_TSYNC_HELLO` | transform-sync |
| 8 | `KIND_TSYNC_SNAPSHOT` | transform-sync |
| 9 | `KIND_TSYNC_INPUT` | transform-sync |
| 10 | `KIND_TSYNC_ACK` | transform-sync |
| 11 | `KIND_TSYNC_ROLE` | transform-sync |
| 12 | `KIND_TSYNC_REWIND` | transform-sync |
| 13 | `KIND_REP_DELTA` | NetworkPeer |
| 14 | `KIND_REP_ACK` | NetworkPeer |
| 15 | `KIND_REP_SCHEMA` | NetworkPeer |

## C ABI

Native engines call the shared kernel directly, so they cannot drift from the
wire:

- `citadel_quantize_scalar(min, max, values_per_unit, value, out_code)`
- `citadel_dequantize_scalar(min, max, values_per_unit, code, out_value)`
- `citadel_quat_encode_components(quat[4], bits_per_component, out_index, out_codes[3])`
- `citadel_quat_decode_components(index, codes[3], bits_per_component, out_quat[4])`

See the [C ABI reference](/reference/client-sdk/c-abi/) and the committed header
`crates/citadel-client-ffi/include/citadel_client.h`.
