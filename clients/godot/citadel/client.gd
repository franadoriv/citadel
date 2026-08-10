# Managed GDScript wrapper over the Citadel client C ABI.
#
# This mirrors the Unity `CitadelClient` wrapper and the native `demo-client`:
# connect over QUIC/WebSocket, send envelopes, and drain the non-blocking poll
# queue. It drives the SAME C ABI (`crates/citadel-client-ffi`, ABI v3) that
# Unity and Unreal use, exposed to GDScript through a native GDExtension object
# named `CitadelClientNative` (see README "Where the native library comes from").
# The extension source is shipped in `native/`; when its package artifact is not
# installed, methods fail explicitly through `last_error` rather than crashing.

class_name CitadelClient
extends RefCounted

## Release packages install this addon at `res://addons/citadel/`. Keep the
## binding's resource paths aligned with that public installation contract.
const Protocol = preload("res://addons/citadel/protocol.gd")

## Status codes returned by the Citadel client C ABI. Mirrors `CitadelStatus`
## in `citadel_client.h` (stable, repr(C)). These are ordinary enum values, not
## `const NAME := N`, so the parity check does not treat them as claimed wire
## constants; the ABI contract is guarded by CitadelProtocol.EXPECTED_ABI_VERSION.
enum Status {
	OK = 0,             ## Operation succeeded (and, for poll, an envelope was written).
	AGAIN = 1,          ## Nothing to poll right now; try again later.
	DISCONNECTED = 2,   ## The connection is closed; queue drained.
	INVALID_ARGUMENT = 3, ## A pointer was null or an argument was invalid.
	CONNECT = 4,        ## Connecting or handshaking failed.
	SEND = 5,           ## Sending failed.
	RECEIVE = 6,        ## Receiving/decoding failed.
	INTERNAL = 7,       ## Unexpected internal error (including a caught panic).
}

## Realtime auth handshake status. Mirrors CitadelProtocol.AUTH_STATUS_*.
enum AuthStatus {
	AUTHENTICATED = 0, ## Token validated; result includes user_id.
	GUEST = 1,         ## Connection admitted as an anonymous guest.
	REJECTED = 2,      ## Server refused the handshake; result includes reason.
}

# The native GDExtension handle (a `CitadelClientNative` object).
var _native: Object = null
var last_error: String = ""
var _rep_result_tokens: Dictionary = {}


func _init() -> void:
	if ClassDB.class_exists(&"CitadelClientNative"):
		_native = ClassDB.instantiate(&"CitadelClientNative")
	else:
		last_error = "CitadelClientNative GDExtension is not loaded"


## Verify the loaded native library speaks the ABI these bindings target.
## Returns true when native `citadel_client_abi_version` ==
## CitadelProtocol.EXPECTED_ABI_VERSION. In the skeleton (no native) returns
## false and records `last_error`.
func check_abi_version() -> bool:
	if _native == null:
		last_error = "CitadelClientNative GDExtension is not loaded (skeleton)"
		return false
	var native_abi: int = _native.abi_version()
	if native_abi != Protocol.EXPECTED_ABI_VERSION:
		last_error = "ABI mismatch: native %d, bindings %d" % [
			native_abi, Protocol.EXPECTED_ABI_VERSION
		]
		return false
	return true


## Connect to a Citadel QUIC endpoint (e.g. "127.0.0.1:7351", server name
## "localhost"). `insecure` selects dev TLS that skips certificate verification.
func connect_quic(addr: String, server_name: String, insecure: bool) -> Status:
	if _native == null:
		return _not_wired()
	return _native.connect_quic(addr, server_name, insecure) as Status


## Connect to a Citadel WebSocket endpoint (e.g. "ws://127.0.0.1:7352/").
func connect_websocket(url: String) -> Status:
	if _native == null:
		return _not_wired()
	return _native.connect_websocket(url) as Status


## Perform the realtime auth handshake as an explicit guest. Call immediately
## after connect and before sending gameplay envelopes. On OK, `out` is filled
## with {"status": AuthStatus, "user_id": String, "reason": int}.
func authenticate_guest(out: Dictionary) -> Status:
	return authenticate_token(PackedByteArray(), out)


## Perform the realtime auth handshake with a UTF-8 session token string.
func authenticate_with_token(session_token: String, out: Dictionary) -> Status:
	return authenticate_token(session_token.to_utf8_buffer(), out)


## Perform the realtime auth handshake with raw token bytes. Empty bytes request
## an explicit guest session.
func authenticate_token(token: PackedByteArray, out: Dictionary) -> Status:
	if _native == null:
		return _not_wired()
	var native_result: Dictionary = _native.authenticate(token)
	var status: Status = int(native_result.get("transport_status", Status.INTERNAL)) as Status
	if status == Status.OK:
		out["status"] = native_result.get("status", AuthStatus.REJECTED)
		out["user_id"] = native_result.get("user_id", "")
		out["reason"] = native_result.get("reason", 0)
	else:
		last_error = _native.last_error()
	return status


## Send an envelope. `reliable` chooses a reliable stream vs an unreliable
## datagram on QUIC (WebSocket is always reliable). Use CitadelProtocol to build
## `data` (e.g. CitadelProtocol.encode_position).
func send(kind: int, data: PackedByteArray, reliable: bool) -> Status:
	if _native == null:
		return _not_wired()
	return _native.send(kind, data, reliable) as Status


## Poll for the next inbound envelope (non-blocking). On Status.OK, `out` is
## filled with {"kind": int, "payload": PackedByteArray}. Returns Status.AGAIN
## when nothing is ready, or Status.DISCONNECTED once the queue is drained.
## Exactly one caller should own the poll loop and dispatch by kind.
func poll(out: Dictionary) -> Status:
	if _native == null:
		return _not_wired()
	var native_result: Dictionary = _native.poll()
	var status: Status = int(native_result.get("transport_status", Status.INTERNAL)) as Status
	if status == Status.OK:
		out["truncated"] = bool(native_result.get("truncated", false))
		out["required_len"] = int(native_result.get("required_len", 0))
		out["kind"] = native_result.get("kind", -1)
		out["payload"] = native_result.get("payload", PackedByteArray())
	elif status != Status.AGAIN:
		last_error = _native.last_error()
	return status


## Free the native handle. Safe to call more than once.
func close() -> void:
	if _native != null:
		_native.free_handle()
		_native = null


## Create the shared Rust transform runtime from a KIND_TSYNC_HELLO payload.
## Returns null if the GDExtension is unavailable or rejects the negotiation.
func transform_view_new(hello: PackedByteArray) -> Variant:
	if _native == null:
		_not_wired()
		return null
	return _native.transform_view_new(hello)


## Apply one snapshot datagram. The Rust runtime handles delta baselines,
## reordering/loss, Hermite+slerp, and adaptive buffering.
func transform_view_apply_datagram(view: Variant, snapshot: PackedByteArray) -> bool:
	return _native != null and _native.transform_view_apply_datagram(view, snapshot)


func transform_view_ack(view: Variant) -> PackedByteArray:
	return _native.transform_view_ack(view) if _native != null else PackedByteArray()


func transform_view_sample_now(view: Variant, object_id: int) -> Dictionary:
	return _native.transform_view_sample_now(view, object_id) if _native != null else {}


func transform_view_authoritative(view: Variant, object_id: int) -> Dictionary:
	return _native.transform_view_authoritative(view, object_id) if _native != null else {}


func transform_view_free(view: Variant) -> void:
	if _native != null:
		_native.transform_view_free(view)


## Decode a NetworkPeer authoritative DeltaBunch through the shared Rust codec.
## `codecs` is an ordered Array of dictionaries with ABI codec keys (`kind`,
## `int_min`/`int_max`, `scalar_min`/`scalar_max`/`values_per_unit`, `max_len`).
## On success returns a dictionary with the header and decoded scalar fields.
func decode_rep_delta(body: PackedByteArray, schema_hash: PackedByteArray,
		layout_version: int, codecs: Array) -> Dictionary:
	if _native == null:
		_not_wired()
		return {}
	var decoded: Dictionary = _native.decode_rep(body, schema_hash, layout_version, codecs)
	if int(decoded.get("transport_status", Status.INTERNAL)) != Status.OK:
		last_error = _native.last_error()
		return {}
	return decoded


## Decode and apply authoritative NetworkPeer lifecycle rules. Deltas whose
## base token is not the last accepted token are rejected; a full snapshot
## establishes/replaces that baseline. The returned fields stay engine-native.
func apply_rep_delta(body: PackedByteArray, schema_hash: PackedByteArray,
		layout_version: int, codecs: Array) -> Dictionary:
	var decoded := decode_rep_delta(body, schema_hash, layout_version, codecs)
	if decoded.is_empty():
		return {}
	var object_id := int(decoded.get("object_id", -1))
	if object_id < 0:
		last_error = "NetworkPeer delta omitted object id"
		return {}
	if not bool(decoded.get("is_full", false)) and int(decoded.get("base_id", 0)) != int(_rep_result_tokens.get(object_id, 0)):
		last_error = "Stale NetworkPeer delta baseline"
		return {}
	_rep_result_tokens[object_id] = int(decoded.get("result_id", 0))
	return decoded


## The result token to acknowledge after an authoritative apply. A missing
## object returns zero and must not be sent as an acknowledgement.
func rep_ack_token(object_id: int) -> int:
	return int(_rep_result_tokens.get(object_id, 0))


## Encode client-owned NetworkPeer fields through the shared ABI v3 codec.
## Field dictionaries use `kind` 0=bool, 1=int, 2=scalar, 3=PackedByteArray,
## 4=Vector3, 5=Quaternion, 6=keyed collection. Collection fields provide
## `item_codec`, `max_items`, and `operations`; operations preserve
## `rep_index`, `rep_generation`, and `rep_key`. Values are copied natively.
func encode_rep_delta(object_id: int, is_full: bool, result_id: int, base_id: int,
		field_count: int, schema_hash: PackedByteArray, layout_version: int,
		fields: Array) -> PackedByteArray:
	if _native == null:
		_not_wired()
		return PackedByteArray()
	var encoded: Dictionary = _native.encode_rep(object_id, is_full, result_id, base_id,
		field_count, schema_hash, layout_version, fields)
	if int(encoded.get("transport_status", Status.INTERNAL)) != Status.OK:
		last_error = _native.last_error()
		return PackedByteArray()
	return encoded.get("body", PackedByteArray())


## Encode one sequenced KIND_TSYNC_INPUT frame through the shared Rust wire
## encoder. Engine code retains/replays its own small unacknowledged input ring.
func transform_encode_input(input_seq: int, sim_tick: int, dt: float, object_id: int,
		ownership_epoch: int, velocity: Vector3) -> PackedByteArray:
	if _native == null:
		return PackedByteArray()
	return _native.transform_encode_input(input_seq, sim_tick, dt, object_id,
		ownership_epoch, velocity.x, velocity.y, velocity.z)


func _not_wired() -> Status:
	last_error = "CitadelClientNative GDExtension is not loaded (skeleton)"
	return Status.INTERNAL
