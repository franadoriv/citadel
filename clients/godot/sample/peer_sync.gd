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

@export var addr: String = "127.0.0.1:7351"
@export var server_name: String = "localhost"
@export var insecure: bool = true
@export var speed: float = 4.0

var _client := CitadelClient.new()
var _pos := Vector2.ZERO
var _peers: Dictionary = {}  # sender_id (int) -> Node2D


func _ready() -> void:
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
	while _client.poll(envelope) == CitadelClient.Status.OK:
		match int(envelope.get("kind", -1)):
			CitadelProtocol.KIND_PEER_POSITION:
				_on_peer_position(envelope["payload"])
			CitadelProtocol.KIND_RPC_RESPONSE:
				var reply := CitadelProtocol.decode_rpc_response(envelope["payload"])
				print("rpc reply: ", reply)


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
