---
title: Messages & envelopes
description: Understand Citadel's small kind-plus-body message, reserved protocol kinds, stream framing, datagrams, and sender tags.
---

An **envelope** is a game message with a label on the front. The label says what
the message is; the body contains the bytes for that message.

## The small idea

```rust
struct Envelope {
    kind: u16,   // What type of message is this?
    body: Bytes, // The message data.
}
```

For example, the quickstart sends a position envelope. The tutorial sends
different envelopes for move, attack, and state. Citadel routes by `kind`; your
game owns the meaning and format of its custom `body`.

## Reserved kinds and game kinds

Citadel reserves kinds `1` through `27` for core protocol features such as the
demo relay, RPC, authentication, transform sync, replication, networked actors,
rooms, matchmaker notifications, and player notifications.

Do not reuse a reserved kind for your own message. Citadel examples use custom
game kinds from `100` upward to leave clear room for the protocol catalog:

```text
100 = MOVE
101 = ATTACK
102 = STATE
```

The [envelope reference](/reference/protocol/envelope/) is the source of truth
for every currently reserved value.

## One envelope, two ways to travel

Reliable streams can contain several envelopes, so each message needs a length
prefix:

```text
+-------------------+----------+------------------+
| body length (u32) | kind     | payload          |
| big-endian        | u16 BE   | (body length-2)  |
+-------------------+----------+------------------+
```

Citadel rejects invalid lengths before allocating a body. A partial frame waits
for the rest of its bytes.

A datagram already has a clear packet boundary, so it carries one envelope and
does not need the length prefix:

```text
+----------+------------------+
| kind     | payload          |
| u16 BE   |                  |
+----------+------------------+
```

WebSocket uses framed binary messages. QUIC and WebTransport use framing on
reliable streams and the shorter form on datagrams.

## Who sent this?

When the tracked relay tags a peer message, it prefixes the body with the
temporary realtime sender identity:

```text
+-----------------------------+------------------------+
| sender ParticipantId (u64)  | original message body  |
| 8 bytes, big-endian         |                        |
+-----------------------------+------------------------+
```

Client helpers split that prefix so the game can update the correct remote
player. A `ParticipantId` is not an account id and changes after reconnect.

## Do you need to care about the bytes?

- **Using a Citadel SDK?** Usually no. Use the SDK's methods and message helpers.
- **Designing a game message?** Choose a safe kind and version your body format.
- **Building a new SDK or engine bridge?** Yes. Follow the exact protocol
  reference and shared contract fixtures.

## Go deeper

- [Protocol envelope reference](/reference/protocol/envelope/) — every kind and byte layout.
- [Netcode codecs](/reference/protocol/netcode-codecs/) — compact game-state encoding.
- [Gateway, rooms & relay](/concepts/gateway/) — where an envelope goes.
