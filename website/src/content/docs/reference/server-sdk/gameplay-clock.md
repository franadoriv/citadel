---
title: Authoritative Gameplay Clock
summary: Read the server-owned simulation clock from a TransformHub.
description: "Reference for Citadel’s in-process Rust TransformHub gameplay clock: completed simulation ticks, monotonic simulation time, epochs, and fail-closed availability."
---

`TransformHub::gameplay_clock()` returns the current authoritative simulation
clock for one transform hub:

```rust
pub fn gameplay_clock(&self) -> Option<GameplayClockSnapshot>
```

This is an **in-process Rust server API**. The same three wire-safe fields are
also available only after explicit transform-sync v2 negotiation; see
[Negotiated v2 wire metadata](#negotiated-v2-wire-metadata). It is not a
wall-clock, RTT, or client-supplied time source.

## Read the clock

Call it from trusted server-side code and handle unavailability explicitly:

```rust
let Some(clock) = hub.gameplay_clock() else {
    // The hub clock is unavailable. Stop or surface the operation as unavailable;
    // do not substitute a zero, cached, or newly-created clock.
    return;
};

let completed_steps = clock.tick;
let simulation_rate_hz = clock.tick_hz;
let simulation_elapsed_us = clock.elapsed_us;
let lifetime_epoch = clock.epoch;
```

`GameplayClockSnapshot` is a read-only value with these fields:

- `epoch: u64` — an opaque, nonzero identifier for this hub clock lifetime.
- `tick: u64` — the number of completed authoritative simulation steps in the
  current epoch.
- `tick_hz: u16` — the effective configured simulation rate for this epoch.
- `elapsed_us: u64` — saturating elapsed **simulation** time in microseconds.

Treat `epoch` as an opaque identifier. It is useful for distinguishing a
recreated hub from an earlier lifetime whose tick count may have started at the
same value; it is not a timestamp or a duration.

## Advancement and time semantics

The hub advances the clock only after `sim_tick()` has advanced and latched the
authoritative transform world. Each completed simulation step increments `tick`
once. Building or sending transform snapshots does not increment it.

`elapsed_us` is derived from completed steps and `tick_hz` with exact rational
microsecond accumulation, then saturates at `u64::MAX`. It is monotonic within
one epoch and does not read wall-clock time. Scheduler delay, latency, skipped
interval fires, or snapshot cadence therefore do not create catch-up steps or
make this gameplay time jump.

This clock does **not** measure network latency, round-trip time, arrival time,
or player input time. It does not authorize input, schedule work, or provide a
rewind timestamp.

## Epoch lifecycle

A newly created gameplay clock starts at `tick = 0` and `elapsed_us = 0` with a
fresh nonzero epoch. Recreating a hub creates a new clock lifetime, so consumers
must not treat a tick from a previous epoch as current.

The process-local epoch issuer never wraps or reuses an epoch. After issuing
`u64::MAX`, it is exhausted and new hub clock construction fails rather than
reissuing an old epoch.

## Unavailable clock: fail closed

The return type is `Option<GameplayClockSnapshot>` because a poisoned
`TransformHub` mutex has no trustworthy clock state. In that condition,
`gameplay_clock()` returns `None` directly. It never fabricates an epoch, returns
a fallback snapshot, or exposes a potentially stale snapshot through this API.

When the result is `None`, treat the clock as unavailable: fail the dependent
operation safely, stop the affected server-side work, or propagate an explicit
unavailable result. Do not replace it with zero values, a cached value, or a
clock from another hub.

## Negotiated v2 wire metadata

Transform-sync v1 remains byte-for-byte unchanged. A client that supports clock
metadata sends reliable `KIND_TSYNC_V2_HELLO` (29) with exactly two bytes:
`version = 2`, `capabilities = 0x01`. The server replies on the same kind only
when it accepts that exact manifest. Unknown versions, unknown capability bits,
mixed capabilities, and malformed lengths are rejected; they never select a
guessed layout or downgrade a v2 client.

After acceptance, unreliable `KIND_TSYNC_V2_SNAPSHOT` (30) prepends this
big-endian metadata to an otherwise unchanged v1 snapshot body:

`epoch: u64 | tick: u64 | tick_hz: u16 | v1_snapshot`

`epoch` and `tick_hz` must be nonzero. Clients fence snapshot baselines by
epoch: a different epoch is stale and rejected. On a reconnect/reset, clear
baselines, samples, and acknowledgements before accepting the new nonzero
epoch. `citadel-client-ffi` ABI v3 exposes this additive surface through
`citadel_transform_view_apply_v2_datagram` and
`citadel_transform_view_reset_v2_epoch`.

An owner that opts into v2 sends `KIND_TSYNC_V2_INPUT` (31):
`epoch: u64 | last_observed_tick: u64 | flags: u8 | v1_input_bundle`.
The only valid flags value is zero. These are bounded diagnostics; the server
uses neither hint to authorize input, select simulation work, schedule time, or
rewind. A stale, mixed, zero, or malformed epoch-bearing frame is rejected.

Unity and Godot apply v2 snapshots through the shared runtime after stripping
and fencing the clock wrapper; Godot sends the exact v2 HELLO through its
`CitadelClient` transport after each reliable transform HELLO and after an
explicit epoch reset, then accepts v2 snapshots only after the exact echo.
Its reset clears native baselines plus local acknowledgement and pending-input
prediction state. Unreal decodes the same wrapper before its existing v1
snapshot path, accepts it only after the exact HELLO echo, and sends the v2
input prefix only while that accepted nonzero epoch is current; otherwise it
uses the unchanged v1 input kind. All three clear baseline state on a new
reliable HELLO/reconnect. The JavaScript SDK exposes a wrapper decoder and an
explicit epoch-fence helper for applications that supply their own v1 transform
decoder; it does not claim a complete rendering runtime. Each SDK preserves the
unchanged v1 path when v2 is not negotiated.

## Scope

This page documents the server `GameplayClock`/`TransformHub` surface and its
strictly negotiated v2 transform metadata contract. It does not turn gameplay
clock values into browser time, latency, or a general client scheduling API.
