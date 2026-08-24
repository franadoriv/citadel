# Citadel wire protocol constants and (de)serialization for Godot (GDScript).
#
# This mirrors `crates/citadel-wire/src/protocol.rs` (the canonical source) and
# the generated `crates/citadel-wire/contract.json`. It is the ONE file the
# Tier-A parity check parses: every `const NAME := N` below is diffed against the
# canonical contract by `scripts/check-sdk-parity.sh`. Keep the values here in
# lockstep with `contract.json`; the check fails the build on any drift.
#
#   KIND_POSITION       = 1  client -> server. Body: two LITTLE-endian f32
#                            (x, y): "my position".
#   KIND_PEER_POSITION  = 2  server -> client. Body: 8-byte BIG-endian sender
#                            session id, followed by the two-f32 position payload.
#   KIND_RPC_REQUEST    = 3  client -> server. Body (all integers BIG-endian):
#                            request_id: u64 | method_len: u16 |
#                            method: utf8 (method_len bytes) | payload.
#   KIND_RPC_RESPONSE   = 4  server -> client (unicast to the caller). Body:
#                            request_id: u64 (echoed) | status: u8
#                            (0 = ok, 1 = error) | payload.
#
# Endianness matters and is NOT auto-checkable (only the constants are): the
# position floats are LITTLE-endian, but the relayed sender id prefix and the RPC
# request_id / method_len are BIG-endian. GDScript's PackedByteArray.encode_*
# helpers are little-endian, so the big-endian fields are written/read byte by
# byte below to stay correct regardless of host.

class_name CitadelProtocol
extends RefCounted

## Transform-sync reliable negotiation and roles, plus unreliable hot-path
## snapshots/input/acks. Values mirror citadel_wire::protocol.
const KIND_TSYNC_HELLO := 7
const KIND_TSYNC_SNAPSHOT := 8
const KIND_TSYNC_INPUT := 9
const KIND_TSYNC_ACK := 10
const KIND_TSYNC_ROLE := 11
const KIND_TSYNC_REWIND := 12
const KIND_TSYNC_V2_HELLO := 29
const KIND_TSYNC_V2_SNAPSHOT := 30
const KIND_TSYNC_V2_INPUT := 31
const TSYNC_V2_CLOCK_BYTES := 18

## Client->server: create/join a named room; server chooses the map.
const KIND_ROOM_CREATE := 21
const KIND_ROOM_JOIN := 22
const KIND_ROOM_JOINED := 23
const KIND_ROOM_LEAVE := 24
const KIND_ROOM_MAP_READY := 25

## Client->server: "my position" (body: two LE f32 x, y).
const KIND_POSITION := 1

## Server->client: a relayed peer position (body: 8-byte BE sender id + the
## two-f32 position payload).
const KIND_PEER_POSITION := 2

## Client->server: invoke a server-side RPC (request/response).
const KIND_RPC_REQUEST := 3

## Server->client: the correlated reply to a KIND_RPC_REQUEST.
const KIND_RPC_RESPONSE := 4

## Client->server: the auth handshake. MUST be the first frame on a new
## connection. Body: the session token bytes, or empty for an explicit guest.
const KIND_AUTH := 5

## Server->client: the reply to a KIND_AUTH handshake. Body: a status byte
## (AUTH_STATUS_*) plus, on the authenticated path, the resolved user_id (utf8).
const KIND_AUTH_RESULT := 6

## Server->client: JSON ticket-matchmaker handoff. Present its opaque token with
## the generic `matchmaker.accept` RPC; a match id alone does not grant entry.
const KIND_MATCHMAKER_MATCHED := 26

## Server->client: durable player-notification live delivery. The body is UTF-8
## JSON for the persisted notification. Delivery is at-least-once, so deduplicate
## by `id` and reconcile the inbox with the `notifications.list` RPC.
const KIND_NOTIFICATION := 27

## Server->client: authorized chat presence, ephemeral typing, and durable
## message event. The body is UTF-8 JSON and is at-least-once; deduplicate
## durable events by (channel_id, event_id), expire typing at expires_at, then
## reconcile with chat.history when requested.
const KIND_CHAT_EVENT := 28

## Server->client reliable authoritative input-stream lease control. This is
## server-only; its opaque token must never be logged or sent by a client.
const KIND_INPUT_STREAM_CONTROL := 40
const INPUT_STREAM_CONTROL_VERSION := 1
const INPUT_STREAM_CONTROL_ADVERTISE := 1
const INPUT_STREAM_CONTROL_REVOKE := 2
const INPUT_STREAM_TOKEN_BYTES := 16
## Client->server stream-bound sequenced custom input; legacy generic input is unchanged.
const KIND_AUTHORITATIVE_INPUT := 41
const AUTHORITATIVE_INPUT_VERSION := 1
const KIND_CAPABILITY_OFFER := 42
const KIND_CAPABILITY_ACCEPTANCE := 43
const CAPABILITY_NEGOTIATION_VERSION := 1
const CAPABILITY_AUTHORITATIVE_INPUT := 1
const CAPABILITY_CHALLENGE_BYTES := 16
const MAX_SEQUENCED_INPUT_BODY_BYTES := 64 * 1024

## Maximum `kind + payload` bytes in one Citadel reliable stream envelope.
## This is shared with the server's framed decoder. The WebSocket transport also
## keeps at most three trailing bytes while it waits for a complete u32 length.
const WEBSOCKET_MAX_FRAME_BODY_BYTES := 16 * 1024 * 1024
const WEBSOCKET_MAX_BUFFERED_BYTES := WEBSOCKET_MAX_FRAME_BODY_BYTES + 7

## Auth result status: the token validated; the connection is bound to the
## user_id that follows in the body.
const AUTH_STATUS_AUTHENTICATED := 0

## Auth result status: accepted as an anonymous guest (no account bound). Only
## possible when the server allows guests.
const AUTH_STATUS_GUEST := 1

## Auth result status: the handshake was refused; the body carries a coarse
## AUTH_REASON_* class and the connection closes immediately after.
const AUTH_STATUS_REJECTED := 2

## Rejected reason class: authentication failed (bad/expired/revoked token). The
## reason is intentionally coarse so it cannot aid account enumeration.
const AUTH_REASON_AUTH_FAILED := 0

## Rejected reason class: a token was required but none was presented (guests
## disallowed on this connection).
const AUTH_REASON_AUTH_REQUIRED := 1

## Rejected reason class: the handshake broke protocol (first frame was not
## KIND_AUTH, a duplicate auth, an oversized token, or auth on an unreliable path).
const AUTH_REASON_PROTOCOL := 2

## RPC response status: the handler ran and payload is its reply.
const RPC_STATUS_OK := 0

## RPC response status: the call failed; payload is a utf8 message.
const RPC_STATUS_ERROR := 1

## Bytes of the big-endian request_id correlation prefix (RPC).
const RPC_REQUEST_ID_BYTES := 8

## Bytes of the big-endian method_len prefix in an RPC request.
const RPC_METHOD_LEN_BYTES := 2

## Bytes used to prefix a relayed message with the sender id (big-endian).
const SENDER_ID_BYTES := 8

## Bytes in a position payload: two little-endian f32.
const POSITION_BYTES := 8

## Minimum RPC response body: request_id (8) + status (1).
const RPC_RESPONSE_MIN_BYTES := RPC_REQUEST_ID_BYTES + 1

## The ABI version these bindings were written against
## (CITADEL_FFI_ABI_VERSION in citadel_client.h / contract.json abi_version).
## The native GDExtension binding must report the same value at startup.
const EXPECTED_ABI_VERSION := 3


## Encode a 2D position as a KIND_POSITION body: two little-endian f32 (x, y).
static func encode_position(x: float, y: float) -> PackedByteArray:
	var buf := PackedByteArray()
	buf.resize(POSITION_BYTES)
	buf.encode_float(0, x)
	buf.encode_float(4, y)
	return buf


## Decode a KIND_POSITION payload (two little-endian f32) from `body` starting at
## `offset`, consuming POSITION_BYTES. Returns an empty array on a malformed body,
## or [x, y] on success.
static func decode_position(body: PackedByteArray, offset: int = 0) -> Array:
	if offset + POSITION_BYTES > body.size():
		return []
	return [body.decode_float(offset), body.decode_float(offset + 4)]


## Split a relayed KIND_PEER_POSITION body into its sender id and position.
## Returns {} on a malformed body, or {"sender_id": int, "x": float, "y": float}.
static func decode_peer_position(body: PackedByteArray) -> Dictionary:
	if body.size() < SENDER_ID_BYTES + POSITION_BYTES:
		return {}
	var pos := decode_position(body, SENDER_ID_BYTES)
	if pos.is_empty():
		return {}
	return {
		"sender_id": _read_be_u64(body, 0),
		"x": pos[0],
		"y": pos[1],
	}


## Encode a KIND_RPC_REQUEST body:
## request_id (u64 BE) | method_len (u16 BE) | method (utf8) | payload.
static func encode_rpc_request(request_id: int, method: String, payload: PackedByteArray) -> PackedByteArray:
	var method_bytes := method.to_utf8_buffer()
	assert(method_bytes.size() <= 0xFFFF, "RPC method exceeds the u16 method_len limit")
	var buf := PackedByteArray()
	buf.resize(RPC_REQUEST_ID_BYTES + RPC_METHOD_LEN_BYTES)
	_write_be_u64(buf, 0, request_id)
	_write_be_u16(buf, RPC_REQUEST_ID_BYTES, method_bytes.size())
	buf.append_array(method_bytes)
	buf.append_array(payload)
	return buf


## Decode a KIND_RPC_RESPONSE body. Returns {} on a body too short to hold the
## request_id (8) + status (1) header, or
## {"request_id": int, "status": int, "payload": PackedByteArray} on success.
static func decode_rpc_response(body: PackedByteArray) -> Dictionary:
	if body.size() < RPC_RESPONSE_MIN_BYTES:
		return {}
	return {
		"request_id": _read_be_u64(body, 0),
		"status": body[RPC_REQUEST_ID_BYTES],
		"payload": body.slice(RPC_RESPONSE_MIN_BYTES),
	}


## Encode a named room create request: a u16 BE UTF-8 length then the room name.
static func encode_room_create(name: String) -> PackedByteArray:
	var name_bytes := name.to_utf8_buffer()
	assert(name_bytes.size() <= 0xFFFF, "Room name exceeds the u16 wire limit")
	var buf := PackedByteArray()
	buf.resize(2)
	_write_be_u16(buf, 0, name_bytes.size())
	buf.append_array(name_bytes)
	return buf


## Encode a room id for KIND_ROOM_JOIN, KIND_ROOM_LEAVE, or KIND_ROOM_MAP_READY.
static func encode_room_id(room_id: int) -> PackedByteArray:
	var buf := PackedByteArray()
	buf.resize(8)
	_write_be_u64(buf, 0, room_id)
	return buf


## Decode KIND_ROOM_JOINED. Returns {} if the body is malformed, otherwise
## {"room_id": int, "map": String, "mode": String}.
static func decode_room_joined(body: PackedByteArray) -> Dictionary:
	if body.size() < 12:
		return {}
	var offset := 8
	var map_result := _read_room_string(body, offset)
	if map_result.is_empty():
		return {}
	offset = map_result["next"]
	var mode_result := _read_room_string(body, offset)
	if mode_result.is_empty() or mode_result["next"] != body.size():
		return {}
	return {"room_id": _read_be_u64(body, 0), "map": map_result["value"], "mode": mode_result["value"]}


## Decode the exact eight-byte room-id body used by leave notifications.
static func decode_room_id(body: PackedByteArray) -> Variant:
	return _read_be_u64(body, 0) if body.size() == 8 else null


## Decode the v2 transform wrapper while preserving the embedded v1 snapshot
## bytes for the shared native runtime. Returns {} for malformed/zero epoch or
## rate; no hint/input values are retained here.
static func decode_tsync_v2_snapshot(body: PackedByteArray) -> Dictionary:
	if body.size() < TSYNC_V2_CLOCK_BYTES:
		return {}
	var epoch := _read_be_u64(body, 0)
	var tick := _read_be_u64(body, 8)
	var tick_hz := _read_be_u16(body, 16)
	if epoch == 0 or tick_hz == 0:
		return {}
	return {
		"epoch": epoch,
		"tick": tick,
		"tick_hz": tick_hz,
		"snapshot": body.slice(TSYNC_V2_CLOCK_BYTES),
	}


## Encode the epoch-bearing authoritative input wrapper around the unchanged v1 bundle.
## `epoch` and `last_observed_tick` are diagnostics only; the authority never
## uses either value to authorize or schedule simulation work.
static func encode_tsync_v2_input(epoch: int, last_observed_tick: int,
		v1_input_bundle: PackedByteArray) -> PackedByteArray:
	if epoch == 0 or last_observed_tick < 0 or v1_input_bundle.is_empty():
		return PackedByteArray()
	var body := PackedByteArray()
	body.resize(17 + v1_input_bundle.size())
	_write_be_u64(body, 0, epoch)
	_write_be_u64(body, 8, last_observed_tick)
	body[16] = 0 # flags; only zero is currently valid.
	for index in v1_input_bundle.size():
		body[17 + index] = v1_input_bundle[index]
	return body

## Encode one stream frame for the WebSocket transport: a big-endian u32 body
## length, followed by the big-endian u16 kind and opaque payload. WebSocket
## messages are a byte stream for Citadel framing purposes, so callers must use
## decode_websocket_frames for inbound packets instead of assuming one packet is
## exactly one envelope.
static func encode_websocket_frame(kind: int, payload: PackedByteArray) -> PackedByteArray:
	assert(kind >= 0 and kind <= 0xFFFF, "Envelope kind must fit in u16")
	var body_len := 2 + payload.size()
	assert(body_len <= WEBSOCKET_MAX_FRAME_BODY_BYTES, "WebSocket frame exceeds Citadel's 16 MiB limit")
	var frame := PackedByteArray()
	frame.resize(4 + body_len)
	_write_be_u32(frame, 0, body_len)
	_write_be_u16(frame, 4, kind)
	for index in payload.size():
		frame[6 + index] = payload[index]
	return frame

## Drain every complete Citadel stream frame from `buffer`. Returns
## {"frames": Array[Dictionary], "remaining": PackedByteArray, "error": String}.
## Incomplete trailing data is retained; malformed lengths return an error and
## no frames, so a browser client can close the invalid connection explicitly.
static func decode_websocket_frames(buffer: PackedByteArray) -> Dictionary:
	var frames: Array[Dictionary] = []
	var offset := 0
	while buffer.size() - offset >= 4:
		var body_len := _read_be_u32(buffer, offset)
		if body_len < 2 or body_len > WEBSOCKET_MAX_FRAME_BODY_BYTES:
			return {"frames": [], "remaining": PackedByteArray(), "error": "invalid Citadel WebSocket frame length"}
		var frame_len := 4 + body_len
		if buffer.size() - offset < frame_len:
			break
		frames.append({
			"kind": _read_be_u16(buffer, offset + 4),
			"payload": buffer.slice(offset + 6, offset + frame_len),
		})
		offset += frame_len
	return {"frames": frames, "remaining": buffer.slice(offset), "error": ""}

## Decode KIND_AUTH_RESULT. A malformed body returns {}. The authenticated form
## carries a UTF-8 user id; guest and rejected forms have no user id. Rejected
## results default their missing reason byte to AUTH_REASON_AUTH_FAILED, exactly
## like the server's decoder.
static func decode_auth_result(body: PackedByteArray) -> Dictionary:
	if body.is_empty():
		return {}
	var status := body[0]
	if status == AUTH_STATUS_AUTHENTICATED:
		var user_id_bytes := body.slice(1)
		# Validate before converting: Godot 4.3 logs malformed UTF-8 and replaces
		# invalid bytes during conversion, while Rust's from_utf8 rejects them.
		if not _is_valid_utf8(user_id_bytes):
			return {}
		var user_id: String = user_id_bytes.get_string_from_utf8()
		return {"status": status, "user_id": user_id, "reason": 0}
	if status == AUTH_STATUS_GUEST:
		return {"status": status, "user_id": "", "reason": 0}
	if status == AUTH_STATUS_REJECTED:
		return {"status": status, "user_id": "", "reason": body[1] if body.size() > 1 else AUTH_REASON_AUTH_FAILED}
	# The stable native FFI maps an unknown status to a safe rejected outcome.
	return {"status": AUTH_STATUS_REJECTED, "user_id": "", "reason": AUTH_REASON_AUTH_FAILED}


## Decode a canonical non-bearer V1 capability offer or acceptance. Returns {} on malformed input.
static func decode_capability_offer(body: PackedByteArray) -> Dictionary:
	if body.size() != 2 + CAPABILITY_CHALLENGE_BYTES or body[0] != CAPABILITY_NEGOTIATION_VERSION or body[1] != CAPABILITY_AUTHORITATIVE_INPUT:
		return {}
	var challenge := body.slice(2)
	var nonzero := false
	for value in challenge:
		nonzero = nonzero or value != 0
	return {"capability": body[1], "challenge": challenge} if nonzero else {}

## Emit the exact canonical acceptance echo for a decoded offer.
static func encode_capability_acceptance(offer: PackedByteArray) -> PackedByteArray:
	return offer.duplicate() if not decode_capability_offer(offer).is_empty() else PackedByteArray()

## Decode server-only KIND_INPUT_STREAM_CONTROL. Returns {} for malformed or
## noncanonical bodies; advertised opaque tokens are never converted to text.
static func decode_input_stream_control(body: PackedByteArray) -> Dictionary:
	if body.size() < 18 or body[0] != INPUT_STREAM_CONTROL_VERSION:
		return {}
	var opcode := body[1]
	var match_id := _read_be_u64(body, 2)
	var stream_id := _read_be_u64(body, 10)
	if opcode == INPUT_STREAM_CONTROL_REVOKE:
		return {"opcode": opcode, "match_id": match_id, "stream_id": stream_id} if body.size() == 18 else {}
	if opcode != INPUT_STREAM_CONTROL_ADVERTISE or body.size() != 18 + INPUT_STREAM_TOKEN_BYTES:
		return {}
	var token := body.slice(18)
	var nonzero := false
	for byte in token:
		nonzero = nonzero or byte != 0
	return {"opcode": opcode, "match_id": match_id, "stream_id": stream_id, "token": token} if nonzero else {}

## Canonical SequencedInput codec. Godot's `int` is signed, so u64
## values may be supplied as a nonnegative `int` or exactly eight big-endian
## bytes. Decoders always return the byte form, preserving the full u64 domain.
static func encode_sequenced_input(token: PackedByteArray, sequence: Variant, original_custom_kind: int, payload: PackedByteArray = PackedByteArray()) -> PackedByteArray:
	var sequence_bytes := _u64_wire_bytes(sequence)
	if token.size() != INPUT_STREAM_TOKEN_BYTES or sequence_bytes.size() != 8 or _u64_is_zero(sequence_bytes) or original_custom_kind < 0 or original_custom_kind > 0xFFFF or payload.size() > MAX_SEQUENCED_INPUT_BODY_BYTES:
		return PackedByteArray()
	if _u64_is_zero(token): return PackedByteArray()
	var result := PackedByteArray(); result.resize(31 + payload.size())
	result[0] = AUTHORITATIVE_INPUT_VERSION
	_copy_bytes(result, 1, token); _copy_bytes(result, 17, sequence_bytes)
	_write_be_u16(result, 25, original_custom_kind); _write_be_u32(result, 27, payload.size()); _copy_bytes(result, 31, payload)
	return result

static func decode_sequenced_input(body: PackedByteArray) -> Dictionary:
	if body.size() < 31 or body[0] != AUTHORITATIVE_INPUT_VERSION: return {}
	var length := _read_be_u32(body, 27); var token := body.slice(1, 17); var sequence := body.slice(17, 25)
	if _u64_is_zero(token) or _u64_is_zero(sequence) or length > MAX_SEQUENCED_INPUT_BODY_BYTES or body.size() != 31 + length: return {}
	return {"stream_token": token, "sequence": sequence, "original_custom_kind": _read_be_u16(body, 25), "body": body.slice(31)}

## Canonical stream-bound InputReceipt codec. All u64 correlation values use
## `_u64_wire_bytes` so u64::MAX round-trips without signed GDScript overflow.
static func encode_input_receipt(match_id: Variant, stream_id: Variant, token: PackedByteArray, acknowledged_sequence: Variant, decided_sequence: Variant, disposition: int, authoritative_tick: Variant, correction: Variant = null) -> PackedByteArray:
	var match_bytes := _u64_wire_bytes(match_id); var stream_bytes := _u64_wire_bytes(stream_id)
	var acknowledged_bytes := _u64_wire_bytes(acknowledged_sequence); var decided_bytes := _u64_wire_bytes(decided_sequence); var tick_bytes := _u64_wire_bytes(authoritative_tick)
	var correction_present := correction != null
	if correction_present and not (correction is PackedByteArray): return PackedByteArray()
	var correction_bytes := PackedByteArray()
	if correction_present: correction_bytes = correction
	if match_bytes.size() != 8 or stream_bytes.size() != 8 or acknowledged_bytes.size() != 8 or decided_bytes.size() != 8 or tick_bytes.size() != 8 or token.size() != INPUT_STREAM_TOKEN_BYTES or _u64_is_zero(token) or _u64_is_zero(decided_bytes) or (disposition != 0 and disposition != 1) or correction_bytes.size() > MAX_SEQUENCED_INPUT_BODY_BYTES:
		return PackedByteArray()
	var result := PackedByteArray(); result.resize(63 + correction_bytes.size())
	result[0] = AUTHORITATIVE_INPUT_VERSION
	_copy_bytes(result, 1, match_bytes); _copy_bytes(result, 9, stream_bytes); _copy_bytes(result, 17, token)
	_copy_bytes(result, 33, acknowledged_bytes); _copy_bytes(result, 41, decided_bytes); result[49] = disposition
	_copy_bytes(result, 50, tick_bytes); result[58] = 1 if correction_present else 0; _write_be_u32(result, 59, correction_bytes.size()); _copy_bytes(result, 63, correction_bytes)
	return result

static func decode_input_receipt(body: PackedByteArray) -> Dictionary:
	if body.size() < 63 or body[0] != AUTHORITATIVE_INPUT_VERSION: return {}
	var correction_len := _read_be_u32(body, 59); var correction_present := body[58]
	if body[49] > 1 or correction_present > 1 or correction_len > MAX_SEQUENCED_INPUT_BODY_BYTES or body.size() != 63 + correction_len or (correction_present == 0 and correction_len != 0): return {}
	var token := body.slice(17, 33); var decided := body.slice(41, 49)
	if _u64_is_zero(token) or _u64_is_zero(decided): return {}
	return {"match_id": body.slice(1, 9), "stream_id": body.slice(9, 17), "stream_token": token, "acknowledged_sequence": body.slice(33, 41), "decided_sequence": decided, "disposition": body[49], "authoritative_tick": body.slice(50, 58), "correction": body.slice(63) if correction_present == 1 else null}

static func _u64_wire_bytes(value: Variant) -> PackedByteArray:
	if value is PackedByteArray:
		return value.duplicate() if value.size() == 8 else PackedByteArray()
	if value is int and value >= 0:
		var result := PackedByteArray(); result.resize(8); _write_be_u64(result, 0, value); return result
	return PackedByteArray()

static func _u64_is_zero(value: PackedByteArray) -> bool:
	if value.is_empty(): return true
	for byte in value:
		if byte != 0: return false
	return true

static func _copy_bytes(destination: PackedByteArray, offset: int, source: PackedByteArray) -> void:
	for index in source.size(): destination[offset + index] = source[index]


static func _read_room_string(body: PackedByteArray, offset: int) -> Dictionary:
	if offset + 2 > body.size():
		return {}
	var length := (body[offset] << 8) | body[offset + 1]
	offset += 2
	if offset + length > body.size():
		return {}
	return {"value": body.slice(offset, offset + length).get_string_from_utf8(), "next": offset + length}


static func _is_valid_utf8(bytes: PackedByteArray) -> bool:
	var index := 0
	while index < bytes.size():
		var first: int = bytes[index]
		if first <= 0x7F:
			index += 1
			continue
		if first >= 0xC2 and first <= 0xDF:
			if index + 1 >= bytes.size() or not _is_utf8_continuation(bytes[index + 1]):
				return false
			index += 2
			continue
		if index + 2 >= bytes.size():
			return false
		var second: int = bytes[index + 1]
		if first == 0xE0:
			if second < 0xA0 or second > 0xBF or not _is_utf8_continuation(bytes[index + 2]):
				return false
			index += 3
			continue
		if (first >= 0xE1 and first <= 0xEC) or (first >= 0xEE and first <= 0xEF):
			if not _is_utf8_continuation(second) or not _is_utf8_continuation(bytes[index + 2]):
				return false
			index += 3
			continue
		if first == 0xED:
			if second < 0x80 or second > 0x9F or not _is_utf8_continuation(bytes[index + 2]):
				return false
			index += 3
			continue
		if index + 3 >= bytes.size():
			return false
		if first == 0xF0:
			if second < 0x90 or second > 0xBF or not _is_utf8_continuation(bytes[index + 2]) or not _is_utf8_continuation(bytes[index + 3]):
				return false
			index += 4
			continue
		if first >= 0xF1 and first <= 0xF3:
			if not _is_utf8_continuation(second) or not _is_utf8_continuation(bytes[index + 2]) or not _is_utf8_continuation(bytes[index + 3]):
				return false
			index += 4
			continue
		if first == 0xF4:
			if second < 0x80 or second > 0x8F or not _is_utf8_continuation(bytes[index + 2]) or not _is_utf8_continuation(bytes[index + 3]):
				return false
			index += 4
			continue
		return false
	return true


static func _is_utf8_continuation(value: int) -> bool:
	return value >= 0x80 and value <= 0xBF


static func _write_be_u64(buf: PackedByteArray, offset: int, value: int) -> void:
	for i in range(8):
		buf[offset + i] = (value >> (8 * (7 - i))) & 0xFF


static func _write_be_u16(buf: PackedByteArray, offset: int, value: int) -> void:
	buf[offset] = (value >> 8) & 0xFF
	buf[offset + 1] = value & 0xFF

static func _write_be_u32(buf: PackedByteArray, offset: int, value: int) -> void:
	for i in range(4):
		buf[offset + i] = (value >> (8 * (3 - i))) & 0xFF

static func _read_be_u16(buf: PackedByteArray, offset: int) -> int:
	return (buf[offset] << 8) | buf[offset + 1]

static func _read_be_u32(buf: PackedByteArray, offset: int) -> int:
	var value := 0
	for i in range(4):
		value = (value << 8) | buf[offset + i]
	return value


static func _read_be_u64(buf: PackedByteArray, offset: int) -> int:
	var value := 0
	for i in range(8):
		value = (value << 8) | buf[offset + i]
	return value
