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
export const EXPECTED_ABI_VERSION = 3;

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
/** Negotiated v2 transform manifest (reliable, C↔S). */
export const KIND_TSYNC_V2_HELLO = 29;
/** Epoch-bearing v2 transform snapshot (unreliable, S→C). */
export const KIND_TSYNC_V2_SNAPSHOT = 30;
/** Epoch-fenced v2 transform input (unreliable, C→S). */
export const KIND_TSYNC_V2_INPUT = 31;
/** Exact version carried by every v2 transform capability manifest. */
export const TSYNC_V2_VERSION = 2;
/** Required capability bit for the v2 gameplay-clock layout. */
export const TSYNC_V2_CLOCK_CAPABILITY = 1;
/** Every v2 capability bit known by this SDK. */
export const TSYNC_V2_KNOWN_CAPABILITIES = 1;
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
/** Server-to-client UTC correlation offer, delivered after `KIND_AUTH_RESULT`. */
export const KIND_DIAG_SERVER_TIME = 34;
/** Client-to-server local opt-in diagnostics capability assertion. */
export const KIND_DIAG_CAPABILITIES = 35;
/** Bounded NTP-style diagnostics clock correlation exchange. */
export const KIND_DIAG_CLOCK_SYNC = 36;
/** Server-to-client constrained lag-capture start control. */
export const KIND_DIAG_START = 37;
/** Server-to-client one-use lag-capture upload grant. */
export const KIND_DIAG_FLUSH = 38;
/** Client-to-server capture lifecycle/status counters. */
export const KIND_DIAG_STATUS = 39;
/** Server-to-client authoritative input-stream lease control (reliable only). */
export const KIND_INPUT_STREAM_CONTROL = 40;
/** Canonical input-stream control body version. */
export const INPUT_STREAM_CONTROL_VERSION = 1;
/** Canonical opcode that installs a server-issued stream lease. */
export const INPUT_STREAM_CONTROL_ADVERTISE = 1;
/** Canonical opcode that retires a server-issued stream lease. */
export const INPUT_STREAM_CONTROL_REVOKE = 2;
/** Exact opaque input-stream token width. */
export const INPUT_STREAM_TOKEN_BYTES = 16;
/** Client-to-server stream-bound authoritative custom input. */
export const KIND_AUTHORITATIVE_INPUT = 41;
/** Canonical stream-bound input body version. */
export const AUTHORITATIVE_INPUT_VERSION = 1;
/** Server-to-client standalone post-auth capability offer (non-bearer). */
export const KIND_CAPABILITY_OFFER = 42;
/** Client-to-server canonical acceptance echo for one capability offer. */
export const KIND_CAPABILITY_ACCEPTANCE = 43;
export const CAPABILITY_NEGOTIATION_VERSION = 1;
export const CAPABILITY_AUTHORITATIVE_INPUT = 1;
export const CAPABILITY_CHALLENGE_BYTES = 16;
/** Maximum opaque body bytes in a stream-bound input. */
export const MAX_SEQUENCED_INPUT_BODY_BYTES = 64 * 1024;

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

/** Bytes of the v2 gameplay-clock prefix on a transform snapshot. */
export const TSYNC_V2_CLOCK_BYTES = 18;

/** Bytes in the exact TSYNC v2 capability manifest. */
export const TSYNC_V2_MANIFEST_BYTES = 2;

/** Encode a supported v2 manifest; the clock capability is mandatory. */
export function encodeTsyncV2Manifest(capabilities = TSYNC_V2_CLOCK_CAPABILITY) {
  if (!Number.isInteger(capabilities)
    || (capabilities & TSYNC_V2_CLOCK_CAPABILITY) === 0
    || (capabilities & ~TSYNC_V2_KNOWN_CAPABILITIES) !== 0) {
    throw new RangeError("unsupported TSYNC v2 capabilities");
  }
  return new Uint8Array([TSYNC_V2_VERSION, capabilities]);
}

/** Decode one exact supported v2 manifest, or null when negotiation fails. */
export function decodeTsyncV2Manifest(body) {
  if (!(body instanceof Uint8Array) || body.length !== TSYNC_V2_MANIFEST_BYTES
    || body[0] !== TSYNC_V2_VERSION
    || (body[1] & TSYNC_V2_CLOCK_CAPABILITY) === 0
    || (body[1] & ~TSYNC_V2_KNOWN_CAPABILITIES) !== 0) return null;
  return { capabilities: body[1] };
}

/** Encode a canonical V1 acceptance echo for a received non-bearer offer. */
export function encodeCapabilityAcceptance(offer) {
  const decoded = decodeCapabilityOffer(offer);
  if (decoded === null) throw new RangeError("invalid capability offer");
  return offer.slice();
}

/** Decode a canonical V1 capability offer or acceptance body. */
export function decodeCapabilityOffer(body) {
  if (!(body instanceof Uint8Array) || body.length !== 2 + CAPABILITY_CHALLENGE_BYTES
    || body[0] !== CAPABILITY_NEGOTIATION_VERSION
    || body[1] !== CAPABILITY_AUTHORITATIVE_INPUT
    || body.slice(2).every((byte) => byte === 0)) return null;
  return { capability: body[1], challenge: body.slice(2) };
}

/**
 * Decode the v2 transform wrapper without interpreting the embedded v1
 * snapshot. The returned `snapshotBody` is byte-for-byte the existing v1
 * `KIND_TSYNC_SNAPSHOT` body, so callers retain their v1 decoder/fallback.
 * `null` rejects a truncated or invalid (zero epoch/rate) wrapper.
 */
export function decodeTsyncV2Snapshot(body) {
  if (body.length < TSYNC_V2_CLOCK_BYTES) return null;
  const view = new DataView(body.buffer, body.byteOffset, body.length);
  const epoch = view.getBigUint64(0, false);
  const tick = view.getBigUint64(8, false);
  const tickHz = view.getUint16(16, false);
  if (epoch === 0n || tickHz === 0) return null;
  return { epoch, tick, tickHz, snapshotBody: body.slice(TSYNC_V2_CLOCK_BYTES) };
}

/**
 * Connection-local v2 epoch fence. It deliberately stores only an epoch and
 * has no input-derived labels or diagnostics. A reconnect must call `reset`
 * before admitting a different epoch; v1 handling is intentionally separate.
 */
export class TsyncV2EpochFence {
  #epoch = null;

  get epoch() { return this.#epoch; }

  apply(body, decodeV1Snapshot) {
    const decoded = decodeTsyncV2Snapshot(body);
    if (decoded === null || (this.#epoch !== null && this.#epoch !== decoded.epoch)) return null;
    const snapshot = decodeV1Snapshot(decoded.snapshotBody);
    if (snapshot === null) return null;
    this.#epoch = decoded.epoch;
    return { clock: { epoch: decoded.epoch, tick: decoded.tick, tickHz: decoded.tickHz }, snapshot };
  }

  reset(epoch) {
    epoch = BigInt(epoch);
    if (epoch === 0n) return false;
    this.#epoch = epoch;
    return true;
  }
}

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

/** Decode an exact server-issued input-stream control body, or `null` if malformed. */
function requireU64(value, name) {
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) throw new RangeError(`${name} must be an unsigned u64`);
    value = BigInt(value);
  } else if (typeof value !== "bigint") {
    throw new RangeError(`${name} must be an unsigned u64`);
  }
  if (value < 0n || value > ((1n << 64n) - 1n)) {
    throw new RangeError(`${name} must be an unsigned u64`);
  }
  return value;
}

/** Decode an exact server-issued input-stream control body, or `null` if malformed. */
export function decodeInputStreamControl(body) {
  if (body.length < 18 || body[0] !== INPUT_STREAM_CONTROL_VERSION) return null;
  const view = new DataView(body.buffer, body.byteOffset, body.length);
  const matchId = view.getBigUint64(2, false);
  const streamId = view.getBigUint64(10, false);
  if (body[1] === INPUT_STREAM_CONTROL_REVOKE) {
    return body.length === 18 ? { opcode: INPUT_STREAM_CONTROL_REVOKE, matchId, streamId } : null;
  }
  if (body[1] !== INPUT_STREAM_CONTROL_ADVERTISE || body.length !== 18 + INPUT_STREAM_TOKEN_BYTES) return null;
  const token = body.slice(18);
  if (token.every((byte) => byte === 0)) return null;
  return { opcode: INPUT_STREAM_CONTROL_ADVERTISE, matchId, streamId, token };
}

/** Encode the canonical `SequencedInput` body, rejecting noncanonical input. */
export function encodeSequencedInput(streamToken, sequence, originalCustomKind, body = EMPTY) {
  if (!(streamToken instanceof Uint8Array) || streamToken.length !== INPUT_STREAM_TOKEN_BYTES || streamToken.every((byte) => byte === 0)) throw new RangeError("stream token must be 16 nonzero bytes");
  if (!(body instanceof Uint8Array) || body.length > MAX_SEQUENCED_INPUT_BODY_BYTES) throw new RangeError("authoritative input body exceeds the wire limit");
  sequence = requireU64(sequence, "sequence");
  if (sequence === 0n) throw new RangeError("authoritative input sequence must be nonzero");
  if (!Number.isInteger(originalCustomKind) || originalCustomKind < 0 || originalCustomKind > 0xffff) throw new RangeError("custom kind must be u16");
  const encoded = new Uint8Array(1 + INPUT_STREAM_TOKEN_BYTES + 8 + 2 + 4 + body.length);
  const view = new DataView(encoded.buffer);
  encoded[0] = AUTHORITATIVE_INPUT_VERSION;
  encoded.set(streamToken, 1);
  view.setBigUint64(17, sequence, false);
  view.setUint16(25, originalCustomKind, false);
  view.setUint32(27, body.length, false);
  encoded.set(body, 31);
  return encoded;
}

/** Decode one exact canonical `SequencedInput` body, or `null` if malformed. */
export function decodeSequencedInput(body) {
  if (!(body instanceof Uint8Array) || body.length < 31 || body[0] !== AUTHORITATIVE_INPUT_VERSION) return null;
  const view = new DataView(body.buffer, body.byteOffset, body.length);
  const token = body.slice(1, 17);
  const sequence = view.getBigUint64(17, false);
  const length = view.getUint32(27, false);
  if (token.every((byte) => byte === 0) || sequence === 0n || length > MAX_SEQUENCED_INPUT_BODY_BYTES || body.length !== 31 + length) return null;
  return { streamToken: token, sequence, originalCustomKind: view.getUint16(25, false), body: body.slice(31) };
}

/** Encode the canonical stream-bound `InputReceipt` body. */
export function encodeInputReceipt({
  matchId,
  streamId,
  streamToken,
  acknowledgedSequence,
  decidedSequence,
  disposition,
  authoritativeTick,
  correction = null,
}) {
  matchId = requireU64(matchId, "matchId");
  streamId = requireU64(streamId, "streamId");
  acknowledgedSequence = requireU64(acknowledgedSequence, "acknowledgedSequence");
  decidedSequence = requireU64(decidedSequence, "decidedSequence");
  authoritativeTick = requireU64(authoritativeTick, "authoritativeTick");
  if (!(streamToken instanceof Uint8Array) || streamToken.length !== INPUT_STREAM_TOKEN_BYTES || streamToken.every((byte) => byte === 0)) throw new RangeError("stream token must be 16 nonzero bytes");
  if (decidedSequence === 0n) throw new RangeError("authoritative receipt decidedSequence must be nonzero");
  if (disposition !== 0 && disposition !== 1) throw new RangeError("receipt disposition must be accepted or rejected");
  if (correction !== null && (!(correction instanceof Uint8Array) || correction.length > MAX_SEQUENCED_INPUT_BODY_BYTES)) throw new RangeError("receipt correction exceeds the wire limit");
  const correctionBytes = correction ?? EMPTY;
  const encoded = new Uint8Array(63 + correctionBytes.length);
  const view = new DataView(encoded.buffer);
  encoded[0] = AUTHORITATIVE_INPUT_VERSION;
  view.setBigUint64(1, matchId, false);
  view.setBigUint64(9, streamId, false);
  encoded.set(streamToken, 17);
  view.setBigUint64(33, acknowledgedSequence, false);
  view.setBigUint64(41, decidedSequence, false);
  encoded[49] = disposition;
  view.setBigUint64(50, authoritativeTick, false);
  encoded[58] = correction === null ? 0 : 1;
  view.setUint32(59, correctionBytes.length, false);
  encoded.set(correctionBytes, 63);
  return encoded;
}

/** Decode one exact canonical `InputReceipt` body, or `null` if malformed. */
export function decodeInputReceipt(body) {
  if (!(body instanceof Uint8Array) || body.length < 63 || body[0] !== AUTHORITATIVE_INPUT_VERSION) return null;
  const view = new DataView(body.buffer, body.byteOffset, body.length);
  const streamToken = body.slice(17, 33);
  const decidedSequence = view.getBigUint64(41, false);
  const disposition = body[49];
  const correctionPresent = body[58];
  const correctionLength = view.getUint32(59, false);
  if (
    streamToken.every((byte) => byte === 0)
    || decidedSequence === 0n
    || disposition > 1
    || correctionPresent > 1
    || correctionLength > MAX_SEQUENCED_INPUT_BODY_BYTES
    || (!correctionPresent && correctionLength !== 0)
    || body.length !== 63 + correctionLength
  ) return null;
  return {
    matchId: view.getBigUint64(1, false),
    streamId: view.getBigUint64(9, false),
    streamToken,
    acknowledgedSequence: view.getBigUint64(33, false),
    decidedSequence,
    disposition,
    authoritativeTick: view.getBigUint64(50, false),
    correction: correctionPresent ? body.slice(63) : null,
  };
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
