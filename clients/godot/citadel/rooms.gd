## Named-room operations and lifecycle signals for a CitadelClient.
##
## Keep one application-owned poll loop: forward every inbound envelope to
## handle_envelope, which emits the room signals on that loop's thread.
class_name CitadelRooms
extends RefCounted

const Client = preload("res://addons/citadel/client.gd")
const Protocol = preload("res://addons/citadel/protocol.gd")

signal joined(room: Dictionary)
signal left(room_id: int)

var current_room: Dictionary = {}
var _client: Client


func _init(client: Client) -> void:
	_client = client


## Create or join `name`. The server selects the map and later emits joined.
func join_or_create(name: String) -> Client.Status:
	return _client.send(Protocol.KIND_ROOM_CREATE, Protocol.encode_room_create(name), true)


func join(room_id: int) -> Client.Status:
	return _client.send(Protocol.KIND_ROOM_JOIN, Protocol.encode_room_id(room_id), true)


func leave(room_id: int) -> Client.Status:
	return _client.send(Protocol.KIND_ROOM_LEAVE, Protocol.encode_room_id(room_id), true)


func send_map_ready(room_id: int) -> Client.Status:
	return _client.send(Protocol.KIND_ROOM_MAP_READY, Protocol.encode_room_id(room_id), true)


## Consume one polled envelope. Returns true when it was a room frame.
func handle_envelope(kind: int, payload: PackedByteArray) -> bool:
	if kind == Protocol.KIND_ROOM_JOINED:
		var room := Protocol.decode_room_joined(payload)
		if not room.is_empty():
			current_room = room
			joined.emit(room)
		return true
	if kind == Protocol.KIND_ROOM_LEAVE:
		var room_id: Variant = Protocol.decode_room_id(payload)
		if room_id != null:
			if int(current_room.get("room_id", -1)) == room_id:
				current_room = {}
			left.emit(room_id)
		return true
	return false
