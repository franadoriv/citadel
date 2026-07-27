# Godot engine surface for the shared transform-sync runtime.
#
# The GDExtension exposes the same citadel_transform_view_* C ABI calls as the
# Unity P/Invoke binding. This node never decodes a snapshot itself: it routes
# HELLO/SNAPSHOT envelopes into the native Rust runtime and applies its adaptive
# Hermite+slerp result to the Node3D.

class_name CitadelTransformSync
extends Node3D

@export var object_id: int = 0
@export var local_owner: bool = false
@export var ownership_epoch: int = 0
@export var hard_snap_centimetres: float = 100.0

var _view: Variant = null
var _last_ack: int = 0
var _client: CitadelClient
var _next_input_seq: int = 1
var _pending_inputs: Array[Dictionary] = []


func bind_client(client: CitadelClient) -> void:
	_client = client


## Route this from the connection's one poll dispatcher. `client` is the loaded
## CitadelClient wrapper; its native GDExtension must expose transform_view_*.
func handle_envelope(client: CitadelClient, kind: int, payload: PackedByteArray) -> void:
	bind_client(client)
	if kind == CitadelProtocol.KIND_TSYNC_HELLO:
		free_runtime()
		_view = client.transform_view_new(payload)
		return
	if kind != CitadelProtocol.KIND_TSYNC_SNAPSHOT or _view == null:
		return
	if not client.transform_view_apply_datagram(_view, payload):
		return
	client.send(CitadelProtocol.KIND_TSYNC_ACK, client.transform_view_ack(_view), false)


func _process(_delta: float) -> void:
	if _view == null or object_id == 0:
		return
	if _client == null:
		return
	var sample := _client.transform_view_authoritative(_view, object_id) if local_owner else _client.transform_view_sample_now(_view, object_id)
	if sample.is_empty():
		return
	if local_owner:
		var ack: int = sample.get("input_seq", 0)
		if ack <= _last_ack:
			return
		_last_ack = ack
		_pending_inputs = _pending_inputs.filter(func(input: Dictionary) -> bool: return int(input.sequence) > ack)
	var target := Vector3(sample.position[0], sample.position[1], sample.position[2]) / 100.0
	if local_owner:
		for input in _pending_inputs:
			target += input.velocity * input.dt
		if global_position.distance_to(target) <= hard_snap_centimetres / 100.0:
			global_position = global_position.lerp(target, 0.15)
		else:
			global_position = target
	else:
		global_position = target
	quaternion = Quaternion(sample.rotation[0], sample.rotation[1], sample.rotation[2], sample.rotation[3])


func _exit_tree() -> void:
	free_runtime()


func free_runtime() -> void:
	if _view != null:
		if _client != null:
			_client.transform_view_free(_view)
		_view = null


## Predict and send one local owner input. Call this from the game's input
## controller; remote actors never invoke it.
func submit_input(velocity_metres_per_second: Vector3, dt: float) -> void:
	if not local_owner or _client == null or object_id == 0 or dt < 0.0:
		return
	var seq := _next_input_seq
	_next_input_seq += 1
	_pending_inputs.append({"sequence": seq, "velocity": velocity_metres_per_second, "dt": dt})
	global_position += velocity_metres_per_second * dt
	var body := _client.transform_encode_input(seq, seq, dt, object_id, ownership_epoch,
		velocity_metres_per_second * 100.0)
	if not body.is_empty():
		_client.send(CitadelProtocol.KIND_TSYNC_INPUT, body, false)
