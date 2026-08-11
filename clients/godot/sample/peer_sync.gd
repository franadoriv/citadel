# Minimal Citadel move-and-broadcast sample for Godot.
#
# Attach this to a Node in a scene. It connects to the demo server, sends the
# local position each frame, and drains the poll queue, mapping relayed peer
# positions onto child Node2D markers keyed by sender id. It mirrors the Unity
# `Demo/` sample and drives the same C ABI via `CitadelClient`.
#
# The native GDExtension source is in `../native`; install its package artifact
# before running this scene. Without it `connect_quic` returns Status.INTERNAL and
# the sample fails visibly instead of attempting a script-side transport.

extends Node

const ChatLive = preload("res://addons/citadel/chat_live.gd")

@export var addr: String = "127.0.0.1:7351"
@export var server_name: String = "localhost"
@export var insecure: bool = true
@export var speed: float = 4.0

var _client := CitadelClient.new()
var _pos := Vector2.ZERO
var _peers: Dictionary = {}  # sender_id (int) -> Node2D
var _pending_rpc: Dictionary = {}
var _next_rpc_id := 1
var _chat_live: ChatLive


func _ready() -> void:
	_chat_live = ChatLive.new(Callable(self, "_chat_rpc"), 64)
	if not _client.check_abi_version():
		push_warning("Citadel native ABI check failed: %s" % _client.last_error)
	var status := _client.connect_quic(addr, server_name, insecure)
	if status != CitadelClient.Status.OK:
		push_warning("Citadel connect failed (%d): %s" % [status, _client.last_error])
		return
	var auth: Dictionary = {}
	status = _client.authenticate_guest(auth)
	if status != CitadelClient.Status.OK:
		push_warning("Citadel auth failed (%d): %s" % [status, _client.last_error])
		return
	print("Citadel auth status: ", auth.get("status", -1))


func _process(delta: float) -> void:
	var move := Input.get_vector("ui_left", "ui_right", "ui_up", "ui_down")
	_pos += move * speed * delta

	# Fire-and-forget position (unreliable is fine for hot-path state).
	_client.send(CitadelProtocol.KIND_POSITION, CitadelProtocol.encode_position(_pos.x, _pos.y), false)

	# Exactly one poll loop; dispatch by kind.
	var envelope: Dictionary = {}
	var poll_status := _client.poll(envelope)
	while poll_status == CitadelClient.Status.OK:
		if bool(envelope.get("truncated", false)):
			push_warning("Citadel consumed oversized envelope (%d bytes); pending RPCs failed closed" % int(envelope.get("required_len", 0)))
			_fail_pending_rpc()
			envelope.clear()
			poll_status = _client.poll(envelope)
			continue
		var kind := int(envelope.get("kind", -1))
		var payload: PackedByteArray = envelope.get("payload", PackedByteArray())
		if not _chat_live.handle_envelope(kind, payload):
			match kind:
				CitadelProtocol.KIND_PEER_POSITION:
					_on_peer_position(payload)
				CitadelProtocol.KIND_RPC_RESPONSE:
					var reply := CitadelProtocol.decode_rpc_response(payload)
					if reply.is_empty():
						_fail_pending_rpc()
					else:
						var request_id := int(reply.get("request_id", 0))
						if _pending_rpc.has(request_id):
							var callback: Callable = _pending_rpc[request_id]
							_pending_rpc.erase(request_id)
							callback.call(reply.get("payload", PackedByteArray()) if int(reply.get("status", -1)) == CitadelProtocol.RPC_STATUS_OK else PackedByteArray())
		envelope.clear()
		poll_status = _client.poll(envelope)
	if poll_status == CitadelClient.Status.DISCONNECTED:
		_fail_pending_rpc()


func _chat_rpc(method: String, payload: PackedByteArray, callback: Callable) -> bool:
	var request_id := _next_rpc_id
	var body := CitadelProtocol.encode_rpc_request(request_id, method, payload)
	var status := _client.send(CitadelProtocol.KIND_RPC_REQUEST, body, true)
	if status != CitadelClient.Status.OK and status != CitadelClient.Status.AGAIN:
		return false
	_next_rpc_id += 1
	_pending_rpc[request_id] = callback
	return true


func _fail_pending_rpc() -> void:
	var callbacks := _pending_rpc.values()
	_pending_rpc.clear()
	for callback: Callable in callbacks:
		callback.call(PackedByteArray())
	if _chat_live != null:
		_chat_live.on_disconnected()


func _on_peer_position(payload: PackedByteArray) -> void:
	var peer := CitadelProtocol.decode_peer_position(payload)
	if peer.is_empty():
		return
	var marker: Node2D = _peers.get(peer["sender_id"])
	if marker == null:
		marker = Node2D.new()
		add_child(marker)
		_peers[peer["sender_id"]] = marker
	marker.position = Vector2(peer["x"], peer["y"])


func _exit_tree() -> void:
	_client.close()
