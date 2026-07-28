// Citadel wire protocol constants and (de)serialization for the JS/Web SDK.
//
// These MUST stay in lockstep with `crates/citadel-wire/contract.json`; the
// Tier-A parity check (`scripts/check_sdk_parity.py`, run by
// `scripts/check.sh`) diffs the declared integer constants below against the
// canonical contract and fails the build on any drift. Declare each claimed
// constant as a plain `export const` integer literal so the Tier-A regex
// parser can read it; expressions and non-integer values are ignored by design.

// --- ABI ---------------------------------------------------------------------

/** Client ABI version these bindings target (contract `abi_version`). */
export const EXPECTED_ABI_VERSION = 2;

// --- Envelope kinds ----------------------------------------------------------

/** Client -> server: "my position update" (opaque body). */
export const KIND_POSITION = 1;
/** Server -> client: a relayed peer position, prefixed with the sender id. */
export const KIND_PEER_POSITION = 2;
/** Client -> server: an RPC request (see {@link encodeRpcRequest}). */
export const KIND_RPC_REQUEST = 3;
/** Server -> client: the correlated RPC response (see {@link decodeRpcResponse}). */
export const KIND_RPC_RESPONSE = 4;
/** Client -> server: the connection handshake (empty body = guest). */
export const KIND_AUTH = 5;
/** Server -> client: the handshake result (see {@link decodeAuthResult}). */
export const KIND_AUTH_RESULT = 6;

// Reserved netcode kinds (transform-sync / networked-actors / replication /
// rooms). Application game logic should pick kinds >= 100 to avoid these.
export const KIND_TSYNC_HELLO = 7;
export const KIND_TSYNC_SNAPSHOT = 8;
export const KIND_TSYNC_INPUT = 9;
export const KIND_TSYNC_ACK = 10;
export const KIND_TSYNC_ROLE = 11;
export const KIND_TSYNC_REWIND = 12;
export const KIND_REP_DELTA = 13;
export const KIND_REP_ACK = 14;
export const KIND_REP_SCHEMA = 15;
export const KIND_NA_PRESENCE = 16;
export const KIND_NA_SPAWN = 17;
export const KIND_NA_SPAWN_BATCH = 18;
export const KIND_NA_DESPAWN = 19;
export const KIND_NA_STATE = 20;
export const KIND_ROOM_CREATE = 21;
export const KIND_ROOM_JOIN = 22;
export const KIND_ROOM_JOINED = 23;
export const KIND_ROOM_LEAVE = 24;
export const KIND_ROOM_MAP_READY = 25;
// Server -> client: matchmaker handoff {ticket_id, match_id, join_token, expires_at}.
export const KIND_MATCHMAKER_MATCHED = 26;
/** Server-to-client durable player-notification live delivery (UTF-8 JSON). */
export const KIND_NOTIFICATION = 27;
/** Server-to-client authorized chat presence, ephemeral typing, and live mutation event (UTF-8 JSON). */
export const KIND_CHAT_EVENT = 28;

// Reserved-range bounds (inclusive), for callers that want to test membership.
export const TSYNC_KIND_MIN = 7;
export const TSYNC_KIND_MAX = 12;
export const REP_KIND_MIN = 13;
export const REP_KIND_MAX = 15;
export const NA_KIND_MIN = 16;
export const NA_KIND_MAX = 20;
export const ROOM_KIND_MIN = 21;
export const ROOM_KIND_MAX = 25;
export const MATCHMAKER_KIND_MIN = 26;
export const MATCHMAKER_KIND_MAX = 26;
export const NOTIFICATION_KIND_MIN = 27;
export const NOTIFICATION_KIND_MAX = 27;
export const CHAT_KIND_MIN = 28;
export const CHAT_KIND_MAX = 28;

// --- Auth --------------------------------------------------------------------

/** {@link KIND_AUTH_RESULT} status: token validated; connection bound to a user. */
export const AUTH_STATUS_AUTHENTICATED = 0;
/** {@link KIND_AUTH_RESULT} status: accepted as an anonymous guest. */
export const AUTH_STATUS_GUEST = 1;
/** {@link KIND_AUTH_RESULT} status: handshake refused (connection closes). */
export const AUTH_STATUS_REJECTED = 2;

/** Coarse rejection reason class: auth failed (invalid/expired/revoked token). */
export const AUTH_REASON_AUTH_FAILED = 0;
/** Coarse rejection reason class: authentication was required but absent. */
export const AUTH_REASON_AUTH_REQUIRED = 1;
/** Coarse rejection reason class: a protocol violation. */
export const AUTH_REASON_PROTOCOL = 2;

// --- RPC ---------------------------------------------------------------------

/** RPC response status: handler ran; payload is its reply. */
export const RPC_STATUS_OK = 0;
/** RPC response status: handler failed; payload is a short utf8 message. */
export const RPC_STATUS_ERROR = 1;

/** Bytes of the big-endian `request_id` prefix in an RPC request/response. */
export const RPC_REQUEST_ID_BYTES = 8;
/** Bytes of the big-endian `method_len` field in an RPC request. */
export const RPC_METHOD_LEN_BYTES = 2;

// --- Layouts -----------------------------------------------------------------

/** Bytes of the big-endian sender-id prefix on a relayed peer message. */
export const SENDER_ID_BYTES = 8;
/** Canonical position payload size, in bytes. */
export const POSITION_BYTES = 8;
/** Bytes of a replication schema hash. */
export const SCHEMA_HASH_BYTES = 16;
/** Baseline ack-history window, in bits. */
export const ACK_HISTORY_BITS = 32;

// --- Encoders / decoders -----------------------------------------------------

const _enc = new TextEncoder();
const _dec = new TextDecoder();

/**
 * Encode a `KIND_RPC_REQUEST` body: big-endian `request_id: u64`,
 * `method_len: u16`, `method` (utf8), then the opaque `payload`.
 *
 * @param {bigint | number} requestId Correlation id (monotonic per connection).
 * @param {string} method RPC method name.
 * @param {Uint8Array} [payload] Opaque request payload.
 * @returns {Uint8Array}
 */
export function encodeRpcRequest(requestId, method, payload = EMPTY) {
  const methodBytes = _enc.encode(method);
  const buf = new Uint8Array(
    RPC_REQUEST_ID_BYTES + RPC_METHOD_LEN_BYTES + methodBytes.length + payload.length,
  );
  const dv = new DataView(buf.buffer);
  dv.setBigUint64(0, BigInt(requestId), false);
  dv.setUint16(RPC_REQUEST_ID_BYTES, methodBytes.length, false);
  buf.set(methodBytes, RPC_REQUEST_ID_BYTES + RPC_METHOD_LEN_BYTES);
  buf.set(payload, RPC_REQUEST_ID_BYTES + RPC_METHOD_LEN_BYTES + methodBytes.length);
  return buf;
}

/**
 * Decode a `KIND_RPC_RESPONSE` body: big-endian `request_id: u64`, `status: u8`,
 * then the reply/error `payload`. Returns `null` if too short to hold a header.
 *
 * @param {Uint8Array} body
 * @returns {{ requestId: bigint, status: number, payload: Uint8Array } | null}
 */
export function decodeRpcResponse(body) {
  if (body.length < RPC_REQUEST_ID_BYTES + 1) return null;
  const dv = new DataView(body.buffer, body.byteOffset, body.length);
  return {
    requestId: dv.getBigUint64(0, false),
    status: body[RPC_REQUEST_ID_BYTES],
    payload: body.slice(RPC_REQUEST_ID_BYTES + 1),
  };
}

/**
 * Decode a `KIND_AUTH_RESULT` body: a `status` byte, then (authenticated only)
 * the utf8 `user_id`, or (rejected) a coarse reason-class byte. Returns `null`
 * for an empty body.
 *
 * @param {Uint8Array} body
 * @returns {{ status: number, userId: string, reasonClass: number } | null}
 */
export function decodeAuthResult(body) {
  if (body.length < 1) return null;
  const status = body[0];
  const rest = body.subarray(1);
  if (status === AUTH_STATUS_AUTHENTICATED) {
    return { status, userId: _dec.decode(rest), reasonClass: 0 };
  }
  if (status === AUTH_STATUS_REJECTED) {
    return { status, userId: "", reasonClass: rest.length ? rest[0] : AUTH_REASON_AUTH_FAILED };
  }
  return { status, userId: "", reasonClass: 0 };
}

/**
 * Split a relayed peer body into `[senderId, rest]`, or `null` if too short.
 * The sender id is the big-endian `u64` prefix a server adds when relaying.
 *
 * @param {Uint8Array} body
 * @returns {[bigint, Uint8Array] | null}
 */
export function splitSender(body) {
  if (body.length < SENDER_ID_BYTES) return null;
  const dv = new DataView(body.buffer, body.byteOffset, body.length);
  return [dv.getBigUint64(0, false), body.slice(SENDER_ID_BYTES)];
}

/**
 * Prefix `payload` with a big-endian `u64` sender id (mirror of the server's
 * relay tagging; handy for tests).
 *
 * @param {bigint | number} senderId
 * @param {Uint8Array} payload
 * @returns {Uint8Array}
 */
export function tagWithSender(senderId, payload = EMPTY) {
  const buf = new Uint8Array(SENDER_ID_BYTES + payload.length);
  new DataView(buf.buffer).setBigUint64(0, BigInt(senderId), false);
  buf.set(payload, SENDER_ID_BYTES);
  return buf;
}

/** Encode `KIND_ROOM_CREATE`: u16 BE UTF-8 name length followed by the name. */
export function encodeRoomCreate(name) {
  const nameBytes = _enc.encode(name);
  if (nameBytes.length > 0xffff) throw new RangeError("room name exceeds the u16 wire limit");
  const body = new Uint8Array(2 + nameBytes.length);
  new DataView(body.buffer).setUint16(0, nameBytes.length, false);
  body.set(nameBytes, 2);
  return body;
}

/** Encode the eight-byte big-endian room id used by join, leave, and map-ready. */
export function encodeRoomId(roomId) {
  const body = new Uint8Array(8);
  new DataView(body.buffer).setBigUint64(0, BigInt(roomId), false);
  return body;
}

/**
 * Decode `KIND_ROOM_JOINED`: room id followed by u16-length UTF-8 map and mode.
 * @returns {{ roomId: bigint, map: string, mode: string } | null}
 */
export function decodeRoomJoined(body) {
  if (body.length < 12) return null;
  const dv = new DataView(body.buffer, body.byteOffset, body.length);
  let offset = 8;
  const readString = () => {
    if (offset + 2 > body.length) return null;
    const length = dv.getUint16(offset, false);
    offset += 2;
    if (offset + length > body.length) return null;
    const value = _dec.decode(body.subarray(offset, offset + length));
    offset += length;
    return value;
  };
  const map = readString();
  if (map === null) return null;
  const mode = readString();
  if (mode === null || offset !== body.length) return null;
  return { roomId: dv.getBigUint64(0, false), map, mode };
}

/** Decode an exact eight-byte room id body, or return null if malformed. */
export function decodeRoomId(body) {
  if (body.length !== 8) return null;
  return new DataView(body.buffer, body.byteOffset, body.length).getBigUint64(0, false);
}

/** Shared empty payload. */
export const EMPTY = new Uint8Array(0);
