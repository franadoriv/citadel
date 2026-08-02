extends SceneTree

const Protocol = preload("res://addons/citadel/protocol.gd")
const Client = preload("res://addons/citadel/client.gd")
const Rooms = preload("res://addons/citadel/rooms.gd")
const WebClient = preload("res://addons/citadel/web_client.gd")
const TransformSync = preload("res://addons/citadel/transform_sync.gd")

var failures: Array[String] = []

func _init() -> void:
	# A failed preload leaves a non-instantiable GDScript object and Godot can
	# still exit 0 after printing parser errors. Make fresh-package load failures
	# a deterministic test failure instead of a false-green release validation.
	for sdk_script in [Protocol, Client, Rooms, WebClient, TransformSync]:
		_expect(sdk_script.can_instantiate(), "packaged SDK script must load: %s" % sdk_script.resource_path)
	if not failures.is_empty():
		for failure in failures:
			push_error(failure)
		quit(1)
		return
	_test_framing()
	_test_auth_decode()
	_test_transform_v2_wrapper()
	_test_browser_client_contract()
	if failures.is_empty():
		print("Citadel Godot Web SDK tests passed")
		quit(0)
	else:
		for failure in failures:
			push_error(failure)
		quit(1)

func _test_framing() -> void:
	var auth := Protocol.encode_websocket_frame(Protocol.KIND_AUTH, PackedByteArray())
	_expect(auth.hex_encode() == "000000020005", "guest auth frame must match the canonical stream layout")
	var rpc := Protocol.encode_websocket_frame(Protocol.KIND_RPC_REQUEST, PackedByteArray([0x72, 0x70, 0x63]))
	var merged := PackedByteArray()
	merged.append_array(auth)
	merged.append_array(rpc)
	var decoded := Protocol.decode_websocket_frames(merged)
	_expect(decoded["error"].is_empty(), "merged frames must decode")
	_expect(decoded["frames"].size() == 2, "merged frames must yield two envelopes")
	_expect(decoded["frames"][0]["kind"] == Protocol.KIND_AUTH, "first decoded kind must be auth")
	_expect(decoded["frames"][1]["payload"] == PackedByteArray([0x72, 0x70, 0x63]), "second payload must round-trip")
	var partial := rpc.slice(0, rpc.size() - 1)
	var incomplete := Protocol.decode_websocket_frames(partial)
	_expect(incomplete["frames"].is_empty() and incomplete["remaining"] == partial, "partial frame must remain buffered")
	var invalid := Protocol.decode_websocket_frames(PackedByteArray([1, 0, 0, 1]))
	_expect(not invalid["error"].is_empty(), "oversized frame must be rejected")

func _v2_snapshot(epoch: int, tick: int, snapshot: PackedByteArray = PackedByteArray([0xaa])) -> PackedByteArray:
	var body := PackedByteArray()
	body.resize(18 + snapshot.size())
	Protocol._write_be_u64(body, 0, epoch)
	Protocol._write_be_u64(body, 8, tick)
	Protocol._write_be_u16(body, 16, 60)
	for index in snapshot.size():
		body[18 + index] = snapshot[index]
	return body

func _test_transform_v2_wrapper() -> void:
	var client := FakeTransformClient.new()
	var sync := TransformSync.new()
	var hello := PackedByteArray([1, 2, 3])
	sync.handle_envelope(client, Protocol.KIND_TSYNC_HELLO, hello)
	_expect(client.created_hellos == [hello], "wrapper must build its native view from HELLO")
	_expect(client.sent.size() == 1 and client.sent[0].kind == Protocol.KIND_TSYNC_V2_HELLO and client.sent[0].payload == PackedByteArray([2, 1]), "wrapper must initiate v2 through its client transport")
	sync.handle_envelope(client, Protocol.KIND_TSYNC_V2_HELLO, PackedByteArray([2, 1]))
	sync.handle_envelope(client, Protocol.KIND_TSYNC_V2_SNAPSHOT, _v2_snapshot(7, 99))
	_expect(client.applied == [PackedByteArray([0xaa])], "negotiated v2 must apply the embedded v1 snapshot through the native view")
	_expect(client.sent.back().kind == Protocol.KIND_TSYNC_ACK, "successful v2 apply must acknowledge through the transport")
	sync.handle_envelope(client, Protocol.KIND_TSYNC_V2_SNAPSHOT, _v2_snapshot(8, 100))
	_expect(client.applied.size() == 1, "mixed/stale v2 epoch must be rejected before native apply")
	sync._last_ack = 42
	sync._next_input_seq = 9
	sync._pending_inputs.append({"sequence": 8})
	_expect(sync.reset_v2_epoch(8), "explicit reset must admit a new nonzero epoch")
	_expect(sync._last_ack == 0 and sync._next_input_seq == 1 and sync._pending_inputs.is_empty(), "reset must clear acknowledgement and prediction state")
	sync.handle_envelope(client, Protocol.KIND_TSYNC_V2_HELLO, PackedByteArray([2, 1]))
	sync.handle_envelope(client, Protocol.KIND_TSYNC_V2_SNAPSHOT, _v2_snapshot(8, 101))
	_expect(client.applied.size() == 2, "reset epoch must accept its first v2 snapshot")
	sync.handle_envelope(client, Protocol.KIND_TSYNC_SNAPSHOT, PackedByteArray([0xbb]))
	_expect(client.applied.back() == PackedByteArray([0xbb]), "v1 snapshot fallback must remain on the native apply path")

class FakeTransformClient:
	extends RefCounted
	var created_hellos: Array = []
	var applied: Array = []
	var sent: Array = []
	func transform_view_new(hello: PackedByteArray) -> Variant:
		created_hellos.append(hello)
		return created_hellos.size()
	func transform_view_apply_datagram(_view: Variant, snapshot: PackedByteArray) -> bool:
		applied.append(snapshot)
		return true
	func transform_view_ack(_view: Variant) -> PackedByteArray:
		return PackedByteArray([0, 0, 0, 1, 0, 0, 0, 0])
	func transform_view_free(_view: Variant) -> void:
		pass
	func send(kind: int, payload: PackedByteArray, reliable: bool) -> int:
		sent.append({"kind": kind, "payload": payload, "reliable": reliable})
		return 0

func _test_auth_decode() -> void:
	var guest := Protocol.decode_auth_result(PackedByteArray([Protocol.AUTH_STATUS_GUEST]))
	_expect(guest["status"] == Protocol.AUTH_STATUS_GUEST, "guest auth result must decode")
	var rejected := Protocol.decode_auth_result(PackedByteArray([Protocol.AUTH_STATUS_REJECTED]))
	_expect(rejected["reason"] == Protocol.AUTH_REASON_AUTH_FAILED, "missing reject reason defaults to auth failed")
	_expect(Protocol.decode_auth_result(PackedByteArray()).is_empty(), "empty auth result must be rejected")
	_expect(Protocol.decode_auth_result(PackedByteArray([Protocol.AUTH_STATUS_AUTHENTICATED, 0xFF])).is_empty(), "non-UTF8 authenticated user id must be rejected")
	var unknown := Protocol.decode_auth_result(PackedByteArray([0x7F]))
	_expect(unknown["status"] == Protocol.AUTH_STATUS_REJECTED and unknown["reason"] == Protocol.AUTH_REASON_AUTH_FAILED, "unknown auth status must map to the native safe rejection")

func _test_browser_client_contract() -> void:
	var client := WebClient.new()
	_expect(client.check_abi_version(), "browser client must not require a native ABI")
	_expect(Rooms.new(client) != null, "CitadelRooms must accept the browser client contract")
	_expect(client.connect_websocket("https://example.invalid") == Client.Status.INVALID_ARGUMENT, "non-WebSocket URL must fail explicitly")
	_expect(client.connect_quic("127.0.0.1:7351", "localhost", true) == Client.Status.INVALID_ARGUMENT, "browser client must reject QUIC")
	_expect(client.send(Protocol.KIND_AUTH, PackedByteArray(), true) == Client.Status.CONNECT, "send before opening must not enqueue a frame")

func _expect(condition: bool, message: String) -> void:
	if not condition:
		failures.append(message)
