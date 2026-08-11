---
title: Envelope format and realtime-kind catalog
description: Exact envelope encodings plus the per-kind realtime contract shared by Citadel server and client SDKs.
---

import { Tabs, TabItem } from '@astrojs/starlight/components';

The realtime byte format lives in `citadel-wire` (`crates/citadel-wire`), the
single source of truth shared by the server and all clients. See
[Envelopes & wire protocol](/concepts/envelopes/) for a conceptual overview.

This page is the protocol catalog: every assigned realtime kind has an anchor
with its direction, delivery expectation, body, lifecycle point, and malformed
or stale-message behavior. A game should use an SDK helper where one exists;
the raw layouts are here for engine bindings, diagnostics, and the surfaces that
intentionally expose generic envelopes.

## Envelope

```rust
pub struct Envelope {
    pub kind: u16,   // message-family discriminant
    pub body: Bytes, // opaque payload
}
```

## Constants

| Constant | Value | Meaning |
| --- | --- | --- |
| `MAX_FRAME_BODY_BYTES` | `8 * 1024 * 1024` (8 MiB) | Max framed body; larger length prefixes are rejected. |
| `LENGTH_PREFIX_BYTES` | `4` | Length-prefix width in the framed encoding. |
| `SENDER_ID_BYTES` | `8` | Sender session-id prefix width in relayed bodies. |
| `KIND_POSITION` / `KIND_PEER_POSITION` | `1` / `2` | Legacy position report and the server-tagged peer relay. |
| `KIND_RPC_REQUEST` / `KIND_RPC_RESPONSE` | `3` / `4` | Correlated server RPC request and caller-only response. |
| `KIND_AUTH` / `KIND_AUTH_RESULT` | `5` / `6` | Required realtime handshake and its result. |
| `KIND_TSYNC_HELLO` … `KIND_TSYNC_REWIND` | `7` … `12` | Transform-sync negotiation, snapshots, input, acknowledgements, roles, and rewind results. |
| `KIND_REP_DELTA` / `KIND_REP_ACK` / `KIND_REP_SCHEMA` | `13` / `14` / `15` | NetworkPeer DeltaBunch, baseline acknowledgement, and schema table. |
| `KIND_NA_PRESENCE` … `KIND_NA_STATE` | `16` … `20` | Networked-actor presence, spawn, despawn, and owner state. |
| `KIND_ROOM_CREATE` … `KIND_ROOM_MAP_READY` | `21` … `25` | Room create/join/load acknowledgement workflow. |
| `KIND_MATCHMAKER_MATCHED` | `26` | Reliable server-to-client matchmaker handoff. |
| `KIND_NOTIFICATION` | `27` | Reliable, at-least-once durable-notification live delivery. |
| `KIND_CHAT_EVENT` | `28` | Reliable, at-least-once local chat presence and committed durable-mutation delivery. |
| `AUTH_STATUS_AUTHENTICATED` / `GUEST` / `REJECTED` | `0` / `1` / `2` | Handshake result states. |
| `AUTH_REASON_AUTH_FAILED` / `AUTH_REQUIRED` / `PROTOCOL` | `0` / `1` / `2` | Coarse rejection classes. |
| `RPC_STATUS_OK` / `RPC_STATUS_ERROR` | `0` / `1` | RPC response outcome. |
| `RPC_REQUEST_ID_BYTES` | `8` | Width of the `request_id` correlation prefix. |

## Framed encoding (stream transports)

For QUIC reliable streams and WebSocket binary messages, where multiple envelopes
may share one byte stream.

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0 | body length | `u32` big-endian | Covers `kind` + payload (i.e. `2 + payload.len`). |
| 4 | kind | `u16` big-endian | |
| 6 | payload | bytes | `body length - 2` bytes. |

Functions: `Envelope::encode_framed -> Bytes` and
`decode_framed(&mut BytesMut) -> Result<Option<Envelope>, WireError>`.
`decode_framed` returns `Ok(None)` when the buffer does not yet hold a complete
frame (keep reading and retry).

Errors (`WireError`):

- `FrameTooLarge` — declared length exceeds `MAX_FRAME_BODY_BYTES`.
- `FrameTooSmall` — declared length below 2 (cannot contain the `kind` header).

## Datagram encoding (datagram transports)

For QUIC / WebTransport unreliable datagrams, where one envelope occupies one
datagram and the datagram boundary provides framing (no length prefix).

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0 | kind | `u16` big-endian | |
| 2 | payload | bytes | Remainder of the datagram. |

Functions: `Envelope::encode_datagram -> Bytes` and
`decode_datagram(&[u8]) -> Result<Envelope, WireError>`.

Error (`WireError`): `DatagramTooSmall` — fewer than 2 bytes.

## Realtime kind catalog

All directions below are relative to the Citadel server. **Reliable** means a
reliable stream (or a WebSocket binary message); **unreliable** means a QUIC or
WebTransport datagram. WebSocket has no unreliable path.

### `KIND_POSITION` (1) — client position report

**Direction/delivery:** client → server; hot-path and normally unreliable.
**Body:** the canonical SDK convention is exactly two little-endian `f32` values,
`x | y` (8 bytes). **When:** send a local player's legacy relay position after a
successful handshake. **Edge behavior:** the built-in relay treats the body as
opaque for application compatibility, but SDK decoders reject a canonical body
that is not 8 bytes. The server never reflects this kind to its sender; it turns
it into kind 2 for peers in the same room.

### `KIND_PEER_POSITION` (2) — server-tagged peer position

**Direction/delivery:** server → client; unreliable. **Body:** `sender_id: u64`
big-endian followed by the original kind-1 bytes. **When:** received by a room
peer when another participant reports a legacy position. **Edge behavior:**
`split_sender` rejects bodies shorter than the 8-byte sender prefix; after that,
the trailing bytes have the same application/canonical-position distinction as
kind 1.

### `KIND_RPC_REQUEST` (3) — server RPC invocation

**Direction/delivery:** client → server; reliable. **Body:**
`request_id: u64 BE | method_len: u16 BE | method: UTF-8 | payload: bytes`.
**When:** send to invoke a registered runtime or built-in RPC after handshake.
**Edge behavior:** a short header, overrun length, or invalid UTF-8 method is
dropped because it cannot be safely correlated; an unknown method or handler
failure instead receives the well-formed kind-4 error response.

### `KIND_RPC_RESPONSE` (4) — caller-only RPC result

**Direction/delivery:** server → requesting client; reliable. **Body:**
`request_id: u64 BE | status: u8 | payload: bytes`, where status `0` is success
and `1` carries a short UTF-8 error message. **When:** exactly once for a parsed
request. **Edge behavior:** clients match the request id and ignore a stale id;
the server never broadcasts RPC responses or exposes runtime stack traces.

### `KIND_AUTH` (5) — realtime handshake request

**Direction/delivery:** client → server; reliable and the first envelope on a
new connection. **Body:** UTF-8 session-token bytes, or empty to explicitly
request a guest session. **When:** immediately after transport connect, before
any gameplay frame. **Edge behavior:** pre-auth datagrams are dropped; a duplicate,
oversized, or wrong-path handshake is rejected with a coarse kind-6 protocol
reason and the connection closes.

### `KIND_AUTH_RESULT` (6) — realtime handshake result

**Direction/delivery:** server → client; reliable. **Body:** `status: u8`, then
the resolved UTF-8 `user_id` for `Authenticated`, no trailer for `Guest`, or one
coarse reason byte for `Rejected`. **When:** exactly once in answer to kind 5.
**Edge behavior:** the client must not route gameplay until an accepted result;
invalid, expired, revoked, and malformed tokens deliberately collapse into the
same rejection class.

### `KIND_TSYNC_HELLO` (7) — transform-sync negotiation

**Direction/delivery:** client ↔ server; reliable. **Body:** two world-bound
records, each `min[3]: f32 BE | max[3]: f32 BE | values_per_unit: u32 BE`, then
`quat_mode: u8 | send_rate_hz: u8 | sim_rate_hz: u8`. **When:** a client sends an
empty opt-in hello and the server replies with the negotiated codec parameters.
**Edge behavior:** bounds or quaternion modes outside the supported contract are
rejected; a client must build the identical codec before accepting kind-8 frames.

### `KIND_TSYNC_SNAPSHOT` (8) — authoritative transform snapshot

**Direction/delivery:** server → client; unreliable. **Body:** an MSB-first
bitstream: `server_tick:32 | snapshot_id:32 | base_snapshot_id:32 |
send_rate_hz:8 | removed_count:16 | update_count:16`, removed object ids, then
quantized per-object deltas. **When:** sent at the negotiated snapshot rate to
interest-relevant clients. **Edge behavior:** the receiver discards an unknown
base or a non-newer snapshot; only zero bit padding may remain. Full layout and
baseline rules are in [transform sync](/reference/client-sdk/transform-sync/).

### `KIND_TSYNC_INPUT` (9) — redundant owner input bundle

**Direction/delivery:** client → server; unreliable. **Body:**
`acked_snapshot_id: u32 BE | last_seen_snapshot_id: u32 BE | frame_count: u8`,
then up to 32 sequenced frames, each `input_seq | sim_tick | dt | object_id |
ownership_epoch | move_velocity[3] | flags | payload_len | payload` (all numeric
fields big-endian); the fire flag appends origin and direction vectors. **When:**
an `OwnerPredicted` client submits movement and optional fire. **Edge behavior:**
the server validates owner/epoch/rate and de-duplicates by input sequence;
truncated or over-counted bundles are rejected.

### `KIND_TSYNC_ACK` (10) — transform baseline acknowledgement

**Direction/delivery:** client → server; unreliable. **Body:**
`acked_snapshot_id: u32 BE | history: u32 BE`. **When:** after applying a
snapshot, unless the acknowledgement is piggybacked on kind 9. **Edge behavior:**
the 32-bit history makes a lost acknowledgement recoverable; a server only bases
deltas on snapshots it knows the receiver applied.

### `KIND_TSYNC_ROLE` (11) — transform role or relevancy transition

**Direction/delivery:** server → client; reliable and idempotent. **Body:**
`object_id: u32 BE | role: u8 | owner: u64 BE | ownership_epoch: u32 BE |
gen_epoch: u16 BE | event: u8`. **When:** assignment, ownership handoff, or
interest enter/exit. **Edge behavior:** clients ignore stale/reordered events
using the ownership and generation epochs; an enter is followed by a usable full
baseline.

### `KIND_TSYNC_REWIND` (12) — authoritative rewind hit result

**Direction/delivery:** server → client; reliable. **Body:**
`input_seq: u32 BE | flags: u8 | object_id: u32 BE | hit_point[3]: f32 BE |
rewind_tick: u32 BE`. **When:** after the server resolves a fire command carried
by kind 9. **Edge behavior:** the server, not the client, computes and clamps the
rewind point; a zero object id and unset hit flag represent a miss.

### `KIND_REP_DELTA` (13) — NetworkPeer DeltaBunch

**Direction/delivery:** client → server proposal or server → client authoritative
state; reliable by default. **Body:** an MSB-first bit-packed DeltaBunch:
`object_id:32 | is_full:1 | result_id: varint`, an optional `base_id`, full-frame
schema identity, a fixed-width changed mask, then ordered field values. **When:**
the server sends a changed replicated object, or a `ClientOwned` field is proposed.
**Edge behavior:** the server treats inbound bytes as untrusted, validates and
re-encodes from authoritative state; malformed, wrong-schema, stale-baseline, or
unauthorised changes fail closed. See the [DeltaBunch reference](/reference/protocol/networkpeer-deltabunch/).

### `KIND_REP_ACK` (14) — NetworkPeer baseline acknowledgement

**Direction/delivery:** client → server; reliable. **Body:** an MSB-first
`entry_count` bit-varint, then each `object_id:32 | acked_result_id: bit-varint |
history:32`; at most 8,192 entries. **When:** after a receiver applies an
authoritative DeltaBunch.
**Edge behavior:** baselines are per connection and advance only to an outstanding,
strictly newer server-issued result id; stale, replayed, or forged acknowledgements
cannot regress them.

### `KIND_REP_SCHEMA` (15) — NetworkPeer schema table

**Direction/delivery:** server → client when an integration elects to send its
optional schema table; reliable. **Body:** an MSB-first `entry_count` bit-varint,
then `class_id:32 | schema_hash:128 | layout_version:32` for each class, capped
at 8,192 entries. **When:** an integration may send it during replication setup;
the current authority path instead gates full kind-13 frames with their embedded
schema identity. **Edge behavior:** a full bunch embeds the schema hash and layout
version as the final gate; a mismatch rejects the entire bunch, never a partial
set of fields.

### `KIND_NA_PRESENCE` (16) — networked-actor presence

**Direction/delivery:** client → server; reliable. **Body:** `archetype_id: u16
BE | transform`, where transform is ten big-endian `f32` values: position `[3]`,
rotation quaternion `[4]` in `xyzw` order, velocity `[3]`. **When:** a client
announces its avatar after transform-sync opt-in. **Edge behavior:** the server
assigns the object id; malformed fixed-size bodies are rejected.

### `KIND_NA_SPAWN` (17) — one networked-actor spawn

**Direction/delivery:** server → client; reliable. **Body:** `object_id: u32 BE |
archetype_id: u16 BE | owner: u64 BE | transform` (the 40-byte transform from
kind 16). **When:** sent to the owner and same-room observers when an actor
appears. **Edge behavior:** owner `0` is server-owned; receivers only instantiate
registered archetypes and must not possess a remote proxy.

### `KIND_NA_SPAWN_BATCH` (18) — existing actor batch

**Direction/delivery:** server → joining client; reliable. **Body:**
`count: u16 BE | spawn[count]`, with each entry exactly the kind-17 body.
**When:** after the new participant receives its own spawn. **Edge behavior:** a
count whose entries overrun the body is rejected; an empty batch is valid.

### `KIND_NA_DESPAWN` (19) — networked-actor removal

**Direction/delivery:** server → client; reliable. **Body:** `object_id: u32 BE`.
**When:** an actor disconnects, is removed, or leaves the receiver's room
visibility. **Edge behavior:** a short body is rejected; removing an already-gone
proxy is safely idempotent at the engine boundary.

### `KIND_NA_STATE` (20) — relay-mode owner state

**Direction/delivery:** client → server; unreliable. **Body:** `object_id: u32
BE | transform` (the 40-byte raw transform). **When:** an owner reports its
native-engine transform in relay mode. **Edge behavior:** the server checks
ownership before applying it and republishing through transform snapshots;
predicted-authoritative archetypes reject this frame and use kind 9 instead.

### `KIND_ROOM_CREATE` (21) — room create or join-by-name

**Direction/delivery:** client → server; reliable. **Body:** `params_len: u16 BE
| params: bytes`; the normal SDK flow uses UTF-8 room-name params as the
join-or-create key. **When:** create a named room or join its existing instance.
**Edge behavior:** a length overrun is rejected; the server runs `on_room_create`
only for a newly created room and replies with kind 23 on admission.

### `KIND_ROOM_JOIN` (22) — join room by id

**Direction/delivery:** client → server; reliable. **Body:** `room_id: u64 BE`.
**When:** join a known room rather than using its name. **Edge behavior:** the Lua
admission hook can deny it; a short body or missing/closed room does not create
membership and produces no client-authored success frame.

### `KIND_ROOM_JOINED` (23) — room admission and map label

**Direction/delivery:** server → admitted client; reliable. **Body:** `room_id:
u64 BE | map_len: u16 BE | map: UTF-8 | mode_len: u16 BE | mode: UTF-8`.
**When:** after a successful create or join, before the client loads the level.
**Edge behavior:** invalid UTF-8 or truncated strings are rejected; the server,
not the client, chooses map and mode from the room label.

### `KIND_ROOM_LEAVE` (24) — leave request or removal notification

**Direction/delivery:** client → server request or server → client notification;
reliable. **Body:** `room_id: u64 BE`. **When:** voluntarily leave, or receive
the result of removal/disconnect handling. **Edge behavior:** it is safe to make
the local cleanup idempotent; membership and room-scoped visibility are removed
server-side immediately.

### `KIND_ROOM_MAP_READY` (25) — level-load acknowledgement

**Direction/delivery:** client → server; reliable. **Body:** `room_id: u64 BE`.
**When:** send only after the map named by kind 23 is open. **Edge behavior:** a
wrong or stale id does not make a participant ready; the server can wait for this
acknowledgement before room-specific fan-out.

### `KIND_MATCHMAKER_MATCHED` (26) — ticket matchmaker handoff

**Direction/delivery:** server → ticket owner; reliable. **Body:** UTF-8 JSON
`{ ticket_id, match_id, join_token, expires_at }`. **When:** a ticket cohort forms.
**Edge behavior:** `join_token` is short-lived and account-bound; the client must
call `matchmaker.accept`, because a raw match id never authorizes entry. Reconnect
recovery uses `matchmaker.status`.

### `KIND_NOTIFICATION` (27) — durable notification live delivery

**Direction/delivery:** server → recipient; reliable. **Body:** UTF-8 JSON of a
committed player notification. **When:** after the notification is persisted and
the recipient has a local live session. **Edge behavior:** live delivery is
best-effort and at-least-once: deduplicate by notification id and reconcile with
`notifications.list` after reconnect or a gap.

### `KIND_CHAT_EVENT` (28) — chat presence, typing, and durable live delivery

**Direction/delivery:** server → client; reliable. **Body:** UTF-8 JSON.
**When:** after a client has successfully called `chat.join` and the server has
committed a durable create, edit, or tombstone; it also carries local
`presence.join` / `presence.leave`, ephemeral `typing`, and `access.revoked`
transitions.

The JSON has a closed version-1 schema with `type` and `channel_id`; unknown or
duplicate fields fail closed. Durable mutation types also include `event_id` and
the complete `message` state. Citadel commits that event and its outbox row in
one transaction, attempts source-node delivery first, then current authenticated
remote leases, and acknowledges the row only after every current destination has
a terminal result. Infrastructure disappearance is retryable. Delivery remains
at-least-once, so SDK state machines deduplicate by `(channel_id, event_id)` and
reconcile gaps from durable history.

`resync_required` carries `watermark_event_id`. Released SDKs keep history-page
application and the correlated acknowledgement behind opaque operations: merely
receiving a page never acknowledges it, and a malformed continuation restarts
from newest. `access.revoked` is terminal until a fresh authorized, correlated
join/rejoin. A `typing` event is ephemeral, has no `event_id`, and has
`{ presence, typing, expires_at }`; receivers clear a true state at that
Unix-millisecond deadline. Typing remains source-node local and dropped typing
does not trigger durable resync.

## Matchmaker handoff notification

When a matchmaker cohort forms, the session-owning node sends a reliable
`KIND_MATCHMAKER_MATCHED` envelope to every ticket owner. Its body is UTF-8 JSON:

```json
{
  "ticket_id": "<opaque ticket id>",
  "match_id": 42,
  "join_token": "<opaque token>",
  "expires_at": 1735689630000
}
```

`join_token` is a short-lived, randomly minted capability bound to the
authenticated account that created the ticket. The client must send it back via
`matchmaker.accept`; a raw `match_id` never authorizes entry. A reconnecting
client can instead call `matchmaker.status` to recover the same unexpired
handoff. See the [matchmaker API](/reference/client-sdk/matchmaker/).

## Relay sender tagging

When the gateway relays a message as `KIND_PEER_POSITION`, the body is:

| Offset | Field | Type |
| --- | --- | --- |
| 0 | sender session id | `u64` big-endian (`SENDER_ID_BYTES` = 8) |
| 8 | original payload | bytes |

Helpers (in `citadel_wire::protocol`):

- `tag_with_sender(sender_id: u64, payload: &[u8]) -> Vec<u8>`
- `split_sender(body: &[u8]) -> Option<(u64, &[u8])>` — returns `None` if the body
  is shorter than 8 bytes.

The `KIND_POSITION` payload convention used by the demos is little-endian `f32`
coordinates, but the codec treats all bodies as opaque.

## Authenticated handshake

Every realtime connection begins with an authentication handshake, uniform across
QUIC, WebSocket, and WebTransport. The client's **first** frame is a `KIND_AUTH`
envelope; the connection is not registered and no other message is processed
until the server replies with a single `KIND_AUTH_RESULT`. On QUIC/WebTransport
the handshake frame must arrive on a reliable stream (pre-auth datagrams are
dropped).

Request body (`KIND_AUTH`, client → server):

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0 | token | bytes | The session access token (utf-8). **Empty** = request a guest connection. |

Response body (`KIND_AUTH_RESULT`, server → client, reliable):

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0 | status | `u8` | `AUTH_STATUS_AUTHENTICATED` (0), `AUTH_STATUS_GUEST` (1), or `AUTH_STATUS_REJECTED` (2). |
| 1 | trailer | bytes | Authenticated: the resolved `user_id` (utf-8). Rejected: a 1-byte reason class. Guest: empty. |

Helpers (in `citadel_wire::protocol`):

- `encode_auth_authenticated(user_id: &str) -> Vec<u8>`
- `encode_auth_guest -> Vec<u8>`
- `encode_auth_rejected(reason_class: u8) -> Vec<u8>`
- `decode_auth_result(body: &[u8]) -> Option<AuthResult>` with
  `is_authenticated` / `is_guest` / `is_rejected`, plus `user_id` and
  `reason_class`.

A rejection is deliberately coarse: an invalid, expired, revoked, or malformed
token all collapse to `AUTH_REASON_AUTH_FAILED`, so the handshake cannot be used
to probe which tokens exist. The token is validated server-side against the
session issued by the [HTTP auth routes](/reference/client-sdk/authentication/) and is never
logged. Whether guests are accepted, and whether a token is required, is a
server-side configuration (`[transport.auth]`). See
[Gateway & rooms](/concepts/gateway/) for behavior.

## RPC request/response

RPC is the request→response counterpart of the fire-and-forget relay, layered on
the same envelope via two additive kinds. A client sends a `KIND_RPC_REQUEST`
naming a `method` (with a payload and a client-chosen `request_id`); the server
runs the matching server-side handler and sends back exactly one
`KIND_RPC_RESPONSE` **to the caller only**, echoing the `request_id` so the client
can correlate the reply with its outstanding call. Both bodies are big-endian.

Request body (`KIND_RPC_REQUEST`, client → server):

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0 | request_id | `u64` big-endian | Client correlation id, echoed in the response. |
| 8 | method_len | `u16` big-endian | Length of the method name in bytes. |
| 10 | method | utf8 | `method_len` bytes; the RPC method name. |
| 10 + method_len | payload | bytes | Opaque request payload. |

Response body (`KIND_RPC_RESPONSE`, server → caller):

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0 | request_id | `u64` big-endian | Echoed from the request. |
| 8 | status | `u8` | `RPC_STATUS_OK` (0) or `RPC_STATUS_ERROR` (1). |
| 9 | payload | bytes | Reply bytes on success, or a short utf8 error message. |

Helpers (in `citadel_wire::protocol`):

- `encode_rpc_request(request_id: u64, method: &str, payload: &[u8]) -> Vec<u8>`
- `decode_rpc_request(body: &[u8]) -> Option<RpcRequest>` — `None` for a truncated
  header, a `method_len` that overruns the buffer, or a non-utf8 method.
- `encode_rpc_response(request_id: u64, status: u8, payload: &[u8]) -> Vec<u8>`
- `decode_rpc_response(body: &[u8]) -> Option<RpcResponse>` — `None` for a body too
  short to hold the header. `RpcResponse::is_ok` reports `status == RPC_STATUS_OK`.

An RPC error (unknown method, handler error, or a blown deadline) is returned as a
well-formed `RPC_STATUS_ERROR` response with a short, generic utf8 message; the
server never leaks internal error detail to the client, and a bad handler never
crashes the node. Responses are sent reliably and only to the caller — an RPC
reply is never broadcast to peers. The server-side handler API
(`citadel.on_rpc(method, fn)`) is covered in the embedded-runtime docs.

Client SDKs provide correlating helpers over this wire format so callers do not
build and match envelopes by hand:

- Rust: [`WsClient::call_rpc` / `QuicClient::call_rpc`](/reference/client-sdk/rust-sdk/#rpc-call-helpers),
  with a usage example in the [Rust SDK guide](/guides/rust-sdk/#calling-an-rpc).
- Unity/C#: the sample's `RpcClient` (`CallRpc` + single-poll-owner dispatch),
  documented in the [Unity QUIC sample](/guides/unity-quic-sample/#calling-an-rpc).

Both generate a monotonic `request_id`, send `KIND_RPC_REQUEST`, and correlate the
`KIND_RPC_RESPONSE` back by that id. The C ABI surface is unchanged — the Unity
correlation lives in the managed layer.

## Client SDK patterns

Use these entry points for client-authored kinds. Own the inbound queue in one
place and dispatch every received envelope by kind; two helpers must not poll the
same connection concurrently.

### Authenticate a realtime connection (kinds 5-6)

<Tabs syncKey="engine">
<TabItem label="C++ (Unreal)">

```cpp
auto* Client = GetGameInstance->GetSubsystem<UCitadelClientSubsystem>;
ECitadelRealtimeAuthStatus Auth;
FString UserId;
uint8 Reason = 0;
if (Client->AuthenticateRealtimeGuest(Auth, UserId, Reason) != ECitadelStatus::Ok ||
    Auth == ECitadelRealtimeAuthStatus::Rejected) return;
```

</TabItem>
<TabItem label="Blueprint (Unreal)">

1. Get **Citadel Client Subsystem** after **Connect Quic** or **Connect Web Socket**.
2. Call **Authenticate Realtime Guest** or **Authenticate Realtime With Session Token**.
3. Route gameplay only for **Authenticated** or **Guest**; a rejected result gives
   only its deliberately coarse reason.

</TabItem>
<TabItem label="C# (Unity)">

```csharp
using var client = CitadelClient.ConnectWebSocket("ws://127.0.0.1:7352/");
var result = client.AuthenticateGuest;
if (!result.IsAccepted) throw new InvalidOperationException($"Realtime auth rejected: {result.Reason}");
```

</TabItem>
<TabItem label="GDScript (Godot)">

```gdscript
var auth := {}
if client.authenticate_guest(auth) != CitadelClient.Status.OK or auth.status == CitadelClient.AuthStatus.REJECTED:
	push_error("Realtime auth rejected: %s" % auth.get("reason", -1))
```

</TabItem>
<TabItem label="Rust">

```rust
use citadel_client::{AuthOutcome, WsClient};

let mut client = WsClient::connect("ws://127.0.0.1:7352/").await?;
let outcome = client.authenticate(None).await?;
assert!(matches!(outcome, AuthOutcome::Authenticated { .. } | AuthOutcome::Guest));
```

</TabItem>
<TabItem label="JavaScript">

```js
const client = await CitadelClient.connect("ws://127.0.0.1:7352/");
await client.handshakeGuest; // KIND_AUTH, then wait for KIND_AUTH_RESULT
```

</TabItem>
</Tabs>

### Send a legacy position (kinds 1-2)

The canonical kind-1 body is exactly two **little-endian** `f32` values. The
server returns kind 2 only to other room peers and adds the big-endian sender id.

<Tabs syncKey="engine">
<TabItem label="C++ (Unreal)">

```cpp
static_assert(PLATFORM_LITTLE_ENDIAN, "KIND_POSITION requires little-endian f32");
float X = 125.0f, Y = 64.0f;
TArray<uint8> Body;
Body.Append(reinterpret_cast<const uint8*>(&X), sizeof(X));
Body.Append(reinterpret_cast<const uint8*>(&Y), sizeof(Y));
Client->Send(CitadelWire::KIND_POSITION, Body, /*bReliable=*/false);
```

</TabItem>
<TabItem label="Blueprint (Unreal)">

The generic `Send(kind: uint16, body)` / `Poll(kind: uint16, body)` interface is
C++-only today because Blueprint cannot represent that wire-kind parameter. Use
the engine components, or add a small C++ adapter; do not invent a Blueprint
integer wrapper for raw envelopes.

</TabItem>
<TabItem label="C# (Unity)">

```csharp
client.Send(CitadelProtocol.KindPosition,
    CitadelProtocol.EncodePosition(125.0f, 64.0f), reliable: false);
```

</TabItem>
<TabItem label="GDScript (Godot)">

```gdscript
client.send(CitadelProtocol.KIND_POSITION,
	CitadelProtocol.encode_position(125.0, 64.0), false)
```

</TabItem>
<TabItem label="Rust">

```rust
let mut body = Vec::with_capacity(8);
body.extend_from_slice(&125.0_f32.to_le_bytes);
body.extend_from_slice(&64.0_f32.to_le_bytes);
client.send(&citadel_wire::Envelope::new(citadel_wire::protocol::KIND_POSITION, body)).await?;
```

</TabItem>
<TabItem label="JavaScript">

```js
const body = new Uint8Array(8);
const view = new DataView(body.buffer);
view.setFloat32(0, 125.0, true); view.setFloat32(4, 64.0, true); // little-endian
client.send(1 /* KIND_POSITION */, body);
```

</TabItem>
</Tabs>

### Invoke an RPC (kinds 3-4)

Raw SDK users must allocate a unique request id and have the one poll owner match
kind-4 responses. Rust and Web provide the correlated helper below.

<Tabs syncKey="engine">
<TabItem label="C++ (Unreal)">

```cpp
const uint64 RequestId = NextRequestId++;
FTCHARToUTF8 MethodUtf8("ping");
const uint16 MethodLen = static_cast<uint16>(MethodUtf8.Length);
TArray<uint8> Body;
for (int Shift = 56; Shift >= 0; Shift -= 8) Body.Add(uint8(RequestId >> Shift));
Body.Add(uint8(MethodLen >> 8)); Body.Add(uint8(MethodLen));
Body.Append(reinterpret_cast<const uint8*>(MethodUtf8.Get), MethodLen);
Client->Send(CitadelWire::KIND_RPC_REQUEST, Body, /*bReliable=*/true);
```

</TabItem>
<TabItem label="Blueprint (Unreal)">

Blueprint has realtime authentication and high-level room nodes, but not raw
`uint16` generic-envelope/RPC transport. Call a C++ adapter that owns correlation
and polling; a bare JSON body is not a valid kind-3 request.

</TabItem>
<TabItem label="C# (Unity)">

```csharp
byte[] body = CitadelProtocol.EncodeRpcRequest(nextRequestId++, "ping", Array.Empty<byte>);
client.Send(CitadelProtocol.KindRpcRequest, body, reliable: true);
```

</TabItem>
<TabItem label="GDScript (Godot)">

```gdscript
var body := CitadelProtocol.encode_rpc_request(next_request_id, "ping", PackedByteArray)
next_request_id += 1
client.send(CitadelProtocol.KIND_RPC_REQUEST, body, true)
```

</TabItem>
<TabItem label="Rust">

```rust
let reply = client.call_rpc("ping", b"").await?;
```

</TabItem>
<TabItem label="JavaScript">

```js
const reply = await client.callRpc("ping");
```

</TabItem>
</Tabs>

### Create or join a room (kinds 21-25)

All room frames are reliable. Unreal owns this flow in its room subsystem. The
other current SDKs can use their generic envelope surface with the exact body;
they do not yet ship dedicated room modules.

<Tabs syncKey="engine">
<TabItem label="C++ (Unreal)">

```cpp
auto* Rooms = GetGameInstance->GetSubsystem<UCitadelRoomSubsystem>;
Rooms->OnRoomJoined.AddDynamic(this, &AMyGameMode::HandleRoomJoined);
Rooms->JoinOrCreateRoom(TEXT("lobby"));

void AMyGameMode::HandleRoomJoined(const FCitadelRoomInfo& Room)
{
    UGameplayStatics::OpenLevel(this, FName(*Room.Map));
    Rooms->SendMapReady(Room.RoomId);
}
```

</TabItem>
<TabItem label="Blueprint (Unreal)">

1. Get **Citadel Room Subsystem** and bind **On Room Joined** first.
2. Call **Join Or Create Room** with `lobby`, or **Join Room** with a known id.
3. Open the event's server-provided `Map`, then call **Send Map Ready** with its
   `Room Id` after the level is loaded.

</TabItem>
<TabItem label="C# (Unity)">

```csharp
byte[] name = Encoding.UTF8.GetBytes("lobby");
byte[] body = new byte[2 + name.Length];
body[0] = (byte)(name.Length >> 8); body[1] = (byte)name.Length;
Array.Copy(name, 0, body, 2, name.Length);
client.Send(21 /* KIND_ROOM_CREATE */, body, reliable: true);
```

</TabItem>
<TabItem label="GDScript (Godot)">

```gdscript
var name := "lobby".to_utf8_buffer
var body := PackedByteArray([name.size >> 8, name.size & 0xff])
body.append_array(name)
client.send(CitadelProtocol.KIND_ROOM_CREATE, body, true)
```

</TabItem>
<TabItem label="Rust">

```rust
let body = citadel_wire::room::RoomCreate { params: b"lobby".to_vec }.encode;
client.send(&citadel_wire::Envelope::new(citadel_wire::protocol::KIND_ROOM_CREATE, body)).await?;
```

</TabItem>
<TabItem label="JavaScript">

```js
const name = new TextEncoder.encode("lobby");
const body = new Uint8Array(2 + name.length);
new DataView(body.buffer).setUint16(0, name.length);
body.set(name, 2);
client.send(21 /* KIND_ROOM_CREATE */, body);
```

</TabItem>
</Tabs>

For the full room lifecycle and admission hooks, see the
[room client reference](/reference/client-sdk/rooms/). Transform, NetworkPeer,
and networked-actor frames have their own higher-level reference pages; their raw
contract remains indexed above so the kind namespace has one canonical catalog.
