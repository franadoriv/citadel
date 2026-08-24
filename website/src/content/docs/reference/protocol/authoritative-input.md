---
title: Authoritative input and control codec
description: Version-1 bounded transport-neutral bodies for stream-bound custom input, receipts, and server-issued control.
---

`citadel_wire::authoritative_input` defines byte-exact **V1 body codecs** for
stream-bound authoritative custom input. The codecs are product-neutral: custom
kinds and payloads are opaque, and the module does not select a room,
participant, transport, tick schedule, or product rule.

All integers are big-endian. Opaque custom bodies and corrections are limited to
`65,536` bytes. Decoders validate declared lengths before copying bytes and
reject truncation, over-cap lengths, unknown versions or discriminators, and
trailing bytes.

## V1 authoritative input bodies

```text
// SequencedInput
version:u8                    // 1
stream_token:bytes[16]        // server-issued, opaque, and not all zero
sequence:u64                  // nonzero
original_custom_kind:u16      // opaque to this codec
body_len:u32
body:bytes[body_len]          // 0..=65,536 bytes

// InputReceipt
version:u8                    // 1
match_id:u64                  // server-owned match correlation
stream_id:u64                 // server-owned stream correlation
stream_token:bytes[16]        // required opaque token for that stream
acknowledged_sequence:u64     // highest contiguous processed sequence; 0 means none
decided_sequence:u64          // nonzero input sequence this receipt decides
disposition:u8                // 0 = Accepted, 1 = Rejected
authoritative_tick:u64
correction_present:u8         // 0 or 1
correction_len:u32
correction:bytes[correction_len] // present only when correction_present is 1
```

The receipt's `(match_id, stream_id, stream_token, decided_sequence)` tuple
is its correlation key, so equal sequences cannot cross-correlate between
matches or streams. `InputStreamToken` redacts bearer bytes in `Debug` output.
The canonical non-UTF-8 and `u64::MAX` fixture shared by every engine codec test
is `clients/authoritative-input-fixtures.json`; engines without a native unsigned
64-bit value preserve these fields as eight big-endian bytes.

## Server-issued V1 control plane

`KIND_INPUT_STREAM_CONTROL` (`40`) is a reserved **reliable server-to-client**
envelope. Gateway drops it at ingress: it is never a client command and never
reaches relay, runtime, telemetry, or console paths.

```text
// InputStreamControl::Advertise
version:u8                    // 1
operation:u8                  // 1
match_id:u64                  // server-owned room/match identity
stream_id:u64                 // server-issued stream identity
stream_token:bytes[16]        // opaque, nonzero bearer token

// InputStreamControl::Revoke
version:u8                    // 1
operation:u8                  // 2
match_id:u64
stream_id:u64
```

The server advertises only after authoritative admission revalidates room
membership, script binding, and clock epoch. It sends the matching revoke when
that server-owned capability retires. Revoke omits the token; delivery is
best-effort and never preserves authorization after retirement.

## Capability negotiation and V1 ingress

A standalone post-auth V1 negotiation is the compatibility boundary; it is not
part of transform-sync. After successful authentication the server reliably sends
`KIND_CAPABILITY_OFFER` (`42`) with this fixed non-bearer body:

```text
version:u8 = 1 | capability:u8 = 1 | challenge:bytes[16] // nonzero
```

A supporting client reliably returns `KIND_CAPABILITY_ACCEPTANCE` (`43`) with
the exact canonical byte-for-byte echo. The offer is bound server-side to the
exact authenticated transport generation, is consumed on a successful echo, and
is not a stream bearer. Malformed, forged, replayed, wrong-session, or replaced-
generation echoes fail closed. Only then can the server mark the exact session
capable and issue the bearer-bearing `InputStreamControl::Advertise`. A capability
state clear, connection replacement, or leave retires the stream and its queue.

Older clients may ignore the unknown offer. They retain legacy custom behavior
for kinds `40` and `41` and never receive a bearer token. For a negotiated
participant, `KIND_AUTHORITATIVE_INPUT` carries exactly
`SequencedInput`. Gateway derives match, participant, stream, binding generation,
and clock only from server-owned active lease and room state. It silently drops
malformed, stale, roomless, superseded-generation, clock-stale, revoked, or
reserved-original-kind frames before queueing, scripts, relays, receipts, or
telemetry. Accepted frames remain subject to the bounded fair queue and fixed
tick drain budget. At the fixed tick the gateway revalidates the exact accepted
capability, lease, membership, binding, and clock under the room transaction
before issuing the bridge batch. `KIND_AUTHORITATIVE_INPUT` also carries the
server-to-client `InputReceipt` after that exact fenced outcome materializes;
its optional correction is the bridge's already-bounded opaque response. A
retired, changed, or replaced lease cannot advance the acknowledgement watermark
or receive a receipt.
