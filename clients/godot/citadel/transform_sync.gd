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
var _client: Variant = null
var _next_input_seq: int = 1
var _pending_inputs: Array[Dictionary] = []
var _clock_epoch: int = 0
var _last_observed_tick: int = 0
var _v2_negotiated: bool = false
var _hello: PackedByteArray = PackedByteArray()


## Bind the real CitadelClient transport abstraction. Keeping this duck-typed
## also permits the deterministic wrapper harness to exercise the exact route.
func bind_client(client: Variant) -> void:
	_client = client


## Route this from the connection's one poll dispatcher. `client` is the loaded
## CitadelClient wrapper; its native GDExtension must expose transform_view_*.
func handle_envelope(client: Variant, kind: int, payload: PackedByteArray) -> void:
	bind_client(client)
	if kind == CitadelProtocol.KIND_TSYNC_HELLO:
		_hello = payload.duplicate()
		_reset_managed_state()
		free_runtime()
		_view = client.transform_view_new(payload)
		# V2 is an explicit opt-in. A peer that never confirms this manifest stays
		# on the existing v1 path rather than receiving guessed v2 frames.
		client.send(CitadelProtocol.KIND_TSYNC_V2_HELLO, PackedByteArray([2, 1]), true)
		return
	if kind == CitadelProtocol.KIND_TSYNC_V2_HELLO:
		# The server echoes only the exact accepted manifest.
		_v2_negotiated = payload == PackedByteArray([2, 1])
		return
	if _view == null:
		return
	var snapshot := payload
	if kind == CitadelProtocol.KIND_TSYNC_V2_SNAPSHOT:
		var decoded := CitadelProtocol.decode_tsync_v2_snapshot(payload)
		if not _v2_negotiated or decoded.is_empty():
			return
		var epoch: int = decoded.epoch
		if _clock_epoch != 0 and _clock_epoch != epoch:
			return # stale/mixed match epoch; explicit reset is required.
		_clock_epoch = epoch
		_last_observed_tick = int(decoded.tick)
		snapshot = decoded.snapshot
	elif kind != CitadelProtocol.KIND_TSYNC_SNAPSHOT:
		return # v1 fallback remains byte-for-byte the existing path.
	if not client.transform_view_apply_datagram(_view, snapshot):
		return
	client.send(CitadelProtocol.KIND_TSYNC_ACK, client.transform_view_ack(_view), false)


## Fence a reconnect/new match before its first v2 snapshot. Rebuild the native
## runtime from the reliable v1 HELLO so all delta baselines are cleared.
func reset_v2_epoch(epoch: int) -> bool:
	if epoch == 0 or _client == null or _hello.is_empty():
		return false
	free_runtime()
	_reset_managed_state()
	_view = _client.transform_view_new(_hello)
	if _view == null:
		return false
	_clock_epoch = epoch
	# A reset is a fresh runtime lifetime. Re-negotiate instead of assuming an
	# earlier acceptance remains valid across reconnect/match boundaries.
	_client.send(CitadelProtocol.KIND_TSYNC_V2_HELLO, PackedByteArray([2, 1]), true)
	return true


func _reset_managed_state() -> void:
	_clock_epoch = 0
	_last_observed_tick = 0
	_last_ack = 0
	_next_input_seq = 1
	_pending_inputs.clear()
	_v2_negotiated = false


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
		if _v2_negotiated and _clock_epoch != 0:
			var v2_body := CitadelProtocol.encode_tsync_v2_input(_clock_epoch, _last_observed_tick, body)
			if not v2_body.is_empty():
				_client.send(CitadelProtocol.KIND_TSYNC_V2_INPUT, v2_body, false)
				return
		_client.send(CitadelProtocol.KIND_TSYNC_INPUT, body, false)
