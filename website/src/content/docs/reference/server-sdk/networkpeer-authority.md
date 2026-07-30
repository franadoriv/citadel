---
title: NetworkPeer Server Authority
description: The untrusted-input trust boundary for NetworkPeer — how the server validates every inbound client delta, applies it to authoritative state, and rebroadcasts its own authoritative delta (never the client's bytes).
---

Phase 3 of `NetworkPeer` (building on the [DeltaBunch wire
layer](/reference/protocol/networkpeer-deltabunch/)) implements the **server authority
pipeline**: the trust boundary that makes Citadel's client-authoritative upstream
safe. Every inbound `KIND_REP_DELTA` is treated as **hostile input**.

The gateway integration is deliberately opt-in: `[transport.network_peer]` builds
and attaches the authority only when `enabled = true`; it is disabled by default.
A gateway admission joins the default replication match and receives a reliable
schema/full-baseline bootstrap. Trusted lifecycle code—not client frames—registers
classes, spawns/despawns objects, and may use `join_rep_match` to bind a receiver
to another trusted match.

The usable slice: a client changes `Health`, the server validates and clamps it,
and a second client sees the **authoritative** value — carried in a bunch the
server re-encoded, never the sender's bytes.

## The trust inversion

Unreal replicates server→client authoritatively. Citadel's `NetworkPeer` upstream
is the inverse: a client proposes changes to fields it owns, so the server can
trust nothing. It validates, applies to authoritative state, then **re-derives and
rebroadcasts its own delta**. Clients only ever apply server-stamped deltas.

## The pipeline (cheap-reject-first, decode-values-last)

```text
inbound KIND_REP_DELTA (conn)
  1. FRAME       hard caps + non-panicking codec.
  2. HEADER+MASK parse object_id, is_full, tokens, changed_mask — NO values yet.
  3. RESOLVE     (conn's match, object_id) -> authoritative object; unknown /
                 cross-match / not-registered = cheap reject before decode.
  4. OWNERSHIP   server-resolved owner == conn AND every masked field ClientOwned;
                 guests may not mutate persistent objects.
  5. RATE+BUDGET per-connection AGGREGATE token buckets + per-bunch hard caps.
  6. DECODE+BOUNDS decode only the owned fields; validate each against the server's
                 compiled bounds (finite floats, post-dequantization range/clamp).
  7. APPLY       re-check owner epoch + object generation under the apply lock
                 (TOCTOU); write validated values; a veto hook may veto.
  8. REBROADCAST server re-encodes ITS OWN delta to peers, honoring COND_*;
                 rebroadcast bytes charged to the originating budget.
```

Field values are decoded only **after** ownership and rate pass, so a client
cannot burn CPU or memory setting a large field it does not own.

## Security model

- **The server never rebroadcasts client bytes.** Rejected fields, objects, or
  values never reach a peer — they are rejected before decode. Accepted values are
  applied to authoritative state and the server emits a fresh, server-stamped
  `DeltaBunch` with its own baseline token.
- **Ownership is server-resolved.** `object_id → owner` comes from the authoritative
  registry cross-checked with the connection's identity; client-claimed ownership is
  ignored. `object_id` is scoped to the connection's match — a cross-match id is a
  cheap reject.
- **Bounds are the server's compiled schema.** A near-boundary value (post-
  dequantization rounding) is clamped into range; a gross out-of-range value, or
  `NaN`/`±Inf`, is rejected. The out-of-range `Health` clamp you see end-to-end is
  the client codec saturating on encode plus the server validating the result.
- **Aggregate rate/budget.** Bunches, bytes, and changed fields per second are
  budgeted **per connection across all objects** (many objects can't multiply the
  budget), plus per-bunch hard caps. Rebroadcast bytes are charged back to the
  originating connection and the fan-out stops once one delta has spent a second's
  budget — a tiny inbound delta can't amplify into unbounded fan-out.
- **Stale / replay guard.** A bunch whose `result_id` is not strictly newer than the
  highest already applied for that `(conn, object)` is dropped — re-checked under the
  apply lock so two in-flight deltas can't regress state.
- **Schema binding.** A delta must diff against an established full-snapshot baseline
  pinned to the bound layout version; a missing or downgraded binding is rejected.
- **TOCTOU.** The owner epoch and an object **generation** are captured at validate
  and re-checked under the apply lock, so an ownership transfer or a re-spawn between
  validate and apply rejects the stale proposal.
- **No content oracle.** Every reject is the same uniform drop with no per-field
  detail and no distinct reply, so a client can't learn *which* check failed. (Reject
  *timing* is only best-effort uniform; a residual object/class existence timing
  side-channel is deferred to the interest pass.)
- **Guests** may own only ephemeral (non-persistent) objects.

## ClientOwned fields, conditions, and the veto hook

- A field is `ServerOnly` by default (client proposals rejected). `ClientOwned`
  fields (cosmetics, emote/input intent) let the owner propose values, still subject
  to bounds, an optional per-field **cooldown**, and rate.
- Rebroadcast honors `COND_*`: an `OwnerOnly` field is never sent to peers, a `None`/
  `SkipOwner`/`SimulatedOnly` field is. (Full per-role interest filtering lands with
  the interest pass.)
- A **veto hook** (the game-logic / Lua reconciliation analogue) may veto a change:
  the authoritative value is left unchanged and the owner is sent a correction, so a
  cheating client sees its illegal change reverted. A panicking hook fails closed.

## Status and limits

- Implemented: the validate → apply → rebroadcast pipeline is wired into the
  opt-in realtime gateway (`KIND_REP_DELTA` / `KIND_REP_ACK`), with trusted
  registration/spawn/despawn and schema/full-baseline bootstrap seams;
  server-stamped rebroadcast, aggregate rate/budget with amplification accounting,
  bounds validation/clamp, stale/schema-binding/TOCTOU guards, `COND_*` masking,
  and the veto hook all apply.
- The authority's shared `InterestGrid` filters relevancy at the replication layer.
  This is **not** automatic AOI-by-room or AOI-by-match: the generic gateway starts
  admissions in match `0`, and a trusted caller must make later match bindings.
  Room and matchmaker lifecycle ownership remain deferred work.
- Inbound client collections and typed C ABI/Rust authoring are supported by the
  wire/authority contracts. Engine bindings remain partial; runtime verification is
  deferred because Unity, Unreal, and Godot were unavailable for this pass.
