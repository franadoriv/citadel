---
title: NetworkPeer Schema Evolution Contract
description: Version 1 of the opt-in, append-only compatibility contract for NetworkPeer authoritative input.
---

**Contract version: 1.** This is a server-authority registration contract, not a
client negotiation feature and not a wire-format change.

## Default: strict reject

`RepAuthority::register_class(class_id, layout, schema)` remains the default and
accepts exactly one `layout_version`. Its behavior is unchanged: a full snapshot
whose embedded schema hash or layout version differs is rejected, and a delta must
use the layout version bound by a prior accepted full snapshot.

## Opt-in append-only registration

An operator that must support a controlled rolling client upgrade can call:

```rust
authority.register_class_compat(
    class_id,
    current_layout,
    current_schema,
    vec![(older_layout, older_schema)],
    min_accepted_version,
)?;
```

Every accepted older layout is checked at registration. It must have a strictly
lower version than the current layout, its schema must bind exactly to that layout,
and its full `FieldDesc` table must be an exact prefix of the current table. In
other words, fields can only be appended at higher `field_id`s. Renumbering,
inserting, changing, or removing an existing field is rejected.

`min_accepted_version` is a downgrade floor. A full snapshot below it, or for a
version absent from the explicit accepted map, is rejected with the same coarse
schema-binding failure used elsewhere in the authority pipeline.

## Exact decode and authoritative defaults

The server reads the full-snapshot schema identity only to select an accepted
version, then decodes the entire bunch with that version's own exact `RepSchema`.
It never tolerant-decodes an older snapshot with the current schema. Trailing,
unknown, malformed, and noncanonical encodings remain rejected by the normal
`DeltaBunch` decoder.

An authoritative object always holds the current layout's full state, seeded at
`spawn_object`. Older clients simply omit appended fields. Omission leaves the
server's value in place; it never lets an old client choose a critical value by
leaving it out. `ServerOnly` fields remain rejected for every client version.

## Operational notes

Keep compatibility registrations short-lived and explicit during an upgrade. Once
the old layout is no longer deployed, return to the one-version `register_class`
path. The authority exposes `metrics` for object-count and rebroadcast fan-out
sampling; use those measurements—not a guessed actor count—to decide whether the
optional shared-state optimization is worth enabling.

## Opt-in shared quantized state

`NetworkPeer` can reuse the bit-exact quantized field-values payload across
receivers that have the same full/delta mode, baseline id, and changed-field set:

```rust
let authority = RepAuthority::new(RateLimits::default)
    .with_shared_quantized_state(true);
```

The toggle is **off by default**. Enable it only after `authority.metrics` shows
roughly 100–300 concurrently replicated actors in an interest scope with enough
actual fan-out to make per-receiver quantization measurable. It is a server-only
implementation choice: no wire contract or client SDK behavior changes.

Each receiver still gets its own `result_id`, pending/acked baseline, ack-timeout
handling, pending cap, and body-level amplification charge. Shared state reuses
only the already-quantized field payload; each receiver's `DeltaBunch` remains
bit-for-bit equivalent to the default path.
