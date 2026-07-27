extends SceneTree

const Protocol = preload("res://addons/citadel/protocol.gd")
const Client = preload("res://addons/citadel/client.gd")
const Rooms = preload("res://addons/citadel/rooms.gd")
const WebClient = preload("res://addons/citadel/web_client.gd")

var failures: Array[String] = []

func _init() -> void:
	_test_framing()
	_test_auth_decode()
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
