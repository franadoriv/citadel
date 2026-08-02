// CitadelWire.h — canonical Citadel wire/ABI constants for the Unreal C++ SDK.
//
// This is the ONE file Tier-A parity parses for this engine. It declares each
// wire/ABI constant as `constexpr <type> NAME = N;` so
// `scripts/check_sdk_parity.py` (the `cpp` format) can diff the literals against
// `crates/citadel-wire/contract.json`. Change a value here and Tier-A fails.
//
// The Unreal SDK is HEADER-DRIVEN (see docs/architecture/client-sdk-sync.md
// section 3): it `#include`s the canonical, cbindgen-generated
// `citadel_client.h` verbatim and NEVER re-declares the C ABI prototypes. These
// constants are the wire-protocol layer (envelope kinds, RPC statuses, body byte
// counts) that lives in Rust (`citadel-wire::protocol`) rather than in the C
// header, plus the ABI version, which we cross-check against the header below.
//
// Endianness (documented, NOT auto-checked — see Tier-B limits): position floats
// are little-endian; the relayed sender id and all RPC integer fields are
// big-endian. Marshaling correctness is covered by per-SDK tests + the manual
// in-editor smoke test, not by this header.
#pragma once

// Canonical C ABI header (cbindgen-generated). Included verbatim so this SDK
// tracks the exact C surface; also provides CITADEL_FFI_ABI_VERSION for the
// compile-time ABI cross-check below.
#include "citadel_client.h"

// Unreal's platform headers define the sized-integer aliases (uint8/uint16/
// uint32) globally. When CitadelWire.h is compiled OUTSIDE Unreal — e.g. by the
// Tier-B parity translation unit on a bare compiler — fall back to <cstdint> so
// the same source compiles standalone. A UE .Build.cs should define
// CITADEL_WITH_UNREAL to use Unreal's own type aliases instead.
#if !defined(CITADEL_WITH_UNREAL)
#include <cstdint>
using uint8 = std::uint8_t;
using uint16 = std::uint16_t;
using uint32 = std::uint32_t;
#endif

namespace CitadelWire
{
    // --- Envelope kinds (u16 on the wire) ---
    // Client -> server: "my position" (body: two little-endian f32 x, y).
    constexpr uint16 KIND_POSITION = 1;
    // Server -> client: a relayed peer position (body: 8-byte big-endian sender
    // id + the two-f32 position payload).
    constexpr uint16 KIND_PEER_POSITION = 2;
    // Client -> server: invoke a server-side RPC (request/response).
    constexpr uint16 KIND_RPC_REQUEST = 3;
    // Server -> client: the correlated reply to a KIND_RPC_REQUEST.
    constexpr uint16 KIND_RPC_RESPONSE = 4;

    // --- Auth handshake (; bodies in citadel_wire::protocol) ---
    // Client -> server: the auth handshake. MUST be the first frame on a new
    // connection. Body: the session token bytes, or empty for an explicit guest.
    constexpr uint16 KIND_AUTH = 5;
    // Server -> client: the reply to a KIND_AUTH handshake. Body: a status byte
    // (AUTH_STATUS_*) plus, on the authenticated path, the resolved user_id (utf8).
    constexpr uint16 KIND_AUTH_RESULT = 6;

    // --- Transform-sync kinds (; bodies in citadel_wire::tsync) ---
    // Client<->server: negotiate world bounds / precision / rates (reliable).
    constexpr uint16 KIND_TSYNC_HELLO = 7;
    // Server -> client: per-client delta snapshot (unreliable datagram hot path).
    constexpr uint16 KIND_TSYNC_SNAPSHOT = 8;
    // Client -> server: owner input bundle (redundant frames + fire, unreliable;
    //  P2). Carries a piggybacked snapshot ack + last-seen-id hint.
    constexpr uint16 KIND_TSYNC_INPUT = 9;
    // Client -> server: snapshot ack (absolute id + 32-bit bitfield, unreliable).
    constexpr uint16 KIND_TSYNC_ACK = 10;
    // Server -> client: ownership/role/relevancy transition (reliable, idempotent).
    constexpr uint16 KIND_TSYNC_ROLE = 11;
    // Server -> client: authoritative lag-compensated hit result for a fire that
    // rode a KIND_TSYNC_INPUT bundle (reliable;  P2). The client never
    // resolves hits itself.
    constexpr uint16 KIND_TSYNC_REWIND = 12;
    // Dedicated negotiated v2 clock metadata kinds; v1 bodies stay unchanged.
    constexpr uint16 KIND_TSYNC_V2_HELLO = 29;
    constexpr uint16 KIND_TSYNC_V2_SNAPSHOT = 30;
    constexpr uint16 KIND_TSYNC_V2_INPUT = 31;

    // --- NetworkPeer replication kinds (; bodies in citadel_wire::netpeer) ---
    // Client<->server: property DeltaBunch (reliable by default). Bit-packed:
    // object_id + is_full + result_id + base_id + schema_hash(full) + changed_mask
    // + length-delimited values + keyed collection add/remove/change.
    constexpr uint16 KIND_REP_DELTA = 13;
    // Client<->server: baseline ack ([(object_id, result_id, history)...]).
    constexpr uint16 KIND_REP_ACK = 14;
    // Server -> client: schema table (class_id -> schema_hash) on join.
    constexpr uint16 KIND_REP_SCHEMA = 15;

    // --- Networked-Actors kinds (; bodies in citadel_wire::na) ---
    // The out-of-the-box presence + replicated-spawn layer above transform-sync.
    // Client -> server: announce this client's avatar {archetype_id, transform}.
    constexpr uint16 KIND_NA_PRESENCE = 16;
    // Server -> client: spawn one networked actor {object_id, archetype_id, owner,
    // transform} (reliable).
    constexpr uint16 KIND_NA_SPAWN = 17;
    // Server -> client: batch spawn — every actor already present, sent to a newly
    // joined client (reliable).
    constexpr uint16 KIND_NA_SPAWN_BATCH = 18;
    // Server -> client: despawn the actor bound to {object_id} (reliable).
    constexpr uint16 KIND_NA_DESPAWN = 19;
    // Client -> server: the owner's relay transform report {object_id, transform}
    // (unreliable hot path).
    constexpr uint16 KIND_NA_STATE = 20;

    // --- Rooms (match/lobby membership + map load), kinds 21-25 (reliable) ---
    // Client -> server: create a room; {params} bytes the game's on_room_create Lua
    // hook interprets (Citadel sends the desired map name).
    constexpr uint16 KIND_ROOM_CREATE = 21;
    // Client -> server: join an existing room {room_id}.
    constexpr uint16 KIND_ROOM_JOIN = 22;
    // Server -> client: you are in the room {room_id, map, mode} — load this map.
    constexpr uint16 KIND_ROOM_JOINED = 23;
    // Client -> server request / server -> client notify: leave/removed {room_id}.
    constexpr uint16 KIND_ROOM_LEAVE = 24;
    // Client -> server: the room's map/level is now open {room_id}.
    constexpr uint16 KIND_ROOM_MAP_READY = 25;
    // Server -> client: JSON matchmaker handoff {ticket_id, match_id, join_token, expires_at}.
    constexpr uint16 KIND_MATCHMAKER_MATCHED = 26;
    // Server -> client: a durable player-notification live delivery. The body is
    // UTF-8 JSON for the persisted notification. Delivery is at-least-once: use
    // the notification id to deduplicate and notifications.list to reconcile.
    constexpr uint16 KIND_NOTIFICATION = 27;
    // Server -> client: authorized chat presence or durable mutation event (UTF-8 JSON).
    constexpr uint16 KIND_CHAT_EVENT = 28;

    // --- RPC response status (u8) ---
    // The handler ran and the payload is its reply.
    constexpr uint8 RPC_STATUS_OK = 0;
    // The call failed; the payload is a utf8 error message.
    constexpr uint8 RPC_STATUS_ERROR = 1;

    // --- Auth result status (u8; the first byte of a KIND_AUTH_RESULT body) ---
    // The token validated; the connection is bound to the user_id that follows.
    constexpr uint8 AUTH_STATUS_AUTHENTICATED = 0;
    // Accepted as an anonymous guest (no account bound). Only when guests allowed.
    constexpr uint8 AUTH_STATUS_GUEST = 1;
    // The handshake was refused; the body carries a coarse AUTH_REASON_* class and
    // the connection closes immediately after.
    constexpr uint8 AUTH_STATUS_REJECTED = 2;

    // --- Auth rejected reason class (u8; coarse by design, never enumeration-aiding) ---
    // Authentication failed (bad/expired/revoked token).
    constexpr uint8 AUTH_REASON_AUTH_FAILED = 0;
    // A token was required but none was presented (guests disallowed here).
    constexpr uint8 AUTH_REASON_AUTH_REQUIRED = 1;
    // The handshake broke protocol (first frame not KIND_AUTH, a duplicate auth,
    // an oversized token, or auth on an unreliable path).
    constexpr uint8 AUTH_REASON_PROTOCOL = 2;

    // --- Body byte counts ---
    // Bytes prefixing a relayed message with the big-endian sender session id.
    constexpr uint32 SENDER_ID_BYTES = 8;
    // Bytes of the two little-endian f32 (x, y) position payload.
    constexpr uint32 POSITION_BYTES = 8;
    // Bytes of the big-endian RPC request id (u64).
    constexpr uint32 RPC_REQUEST_ID_BYTES = 8;
    // Bytes of the big-endian RPC method-length prefix (u16).
    constexpr uint32 RPC_METHOD_LEN_BYTES = 2;

    // --- Native C ABI version ---
    // The ABI version this SDK is written against. Cross-checked against the
    // canonical header's CITADEL_FFI_ABI_VERSION below so a header bump that this
    // SDK has not caught up to is a compile error (belt) and a Tier-A drift
    // against contract.json (braces).
    constexpr uint32 ABI_VERSION = 3;
}

// Compile-time ABI cross-check: fires wherever CitadelWire.h is compiled,
// including the Tier-B TU.
static_assert(CitadelWire::ABI_VERSION == CITADEL_FFI_ABI_VERSION,
              "Citadel Unreal SDK ABI_VERSION drifted from citadel_client.h "
              "CITADEL_FFI_ABI_VERSION; re-sync the SDK.");
