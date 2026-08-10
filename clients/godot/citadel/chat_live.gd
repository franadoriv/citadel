## Strict durable chat v1 decoder and fail-closed authority owner.
class_name CitadelChatLive
extends RefCounted

const Protocol = preload("res://addons/citadel/protocol.gd")
const VERSION := 1
const MAX_EXACT_INTEGER := 9007199254740991
const MAX_HISTORY_LIMIT := 100

signal raw_envelope(kind: int, payload: PackedByteArray)
signal chat_event(event: ChatEvent)
signal live_event_pending(application: LiveEventApplication)
signal resync_needed(channel_id: String, watermark_event_id: int)

class ChatPresence:
	extends RefCounted
	var presence_id: String
	var user_id: String
	func _init(id: String, user: String) -> void:
		presence_id = id; user_id = user

class ChatMessage:
	extends RefCounted
	var id: int
	var sender: String
	var content: String
	var created_at_unix_ms: int
	var updated_at_unix_ms: int
	var revision: int
	var last_event_id: int
	var deleted: bool
	func _init(value: Dictionary) -> void:
		id = value.id; sender = value.sender; content = value.content
		created_at_unix_ms = value.created_at_unix_ms
		updated_at_unix_ms = value.updated_at_unix_ms
		revision = value.revision; last_event_id = value.last_event_id; deleted = value.deleted

class ChatEvent:
	extends RefCounted
	var version := VERSION
	var type: String
	var channel_id: String
	func _init(event_type: String, channel: String) -> void:
		type = event_type; channel_id = channel

class PresenceJoined:
	extends ChatEvent
	var channel_type: String
	var presence: ChatPresence
	func _init(channel: String, kind: String, value: ChatPresence) -> void:
		super("presence.join", channel); channel_type = kind; presence = value
class PresenceLeft:
	extends ChatEvent
	var presence: ChatPresence
	func _init(channel: String, value: ChatPresence) -> void:
		super("presence.leave", channel); presence = value
class Typing:
	extends ChatEvent
	var presence: ChatPresence
	var typing: bool
	var expires_at: int
	func _init(channel: String, value: ChatPresence, active: bool, expiry: int) -> void:
		super("typing", channel); presence = value; typing = active; expires_at = expiry
class MessageEvent:
	extends ChatEvent
	var event_id: int
	var message: ChatMessage
	func _init(event_type: String, channel: String, id_value: int, value: ChatMessage) -> void:
		super(event_type, channel); event_id = id_value; message = value
class MessageCreated:
	extends MessageEvent
	func _init(channel: String, id_value: int, value: ChatMessage) -> void: super("message.create", channel, id_value, value)
class MessageUpdated:
	extends MessageEvent
	func _init(channel: String, id_value: int, value: ChatMessage) -> void: super("message.update", channel, id_value, value)
class MessageRemoved:
	extends MessageEvent
	func _init(channel: String, id_value: int, value: ChatMessage) -> void: super("message.remove", channel, id_value, value)
class AccessRevoked:
	extends ChatEvent
	var presence: ChatPresence
	func _init(channel: String, value: ChatPresence) -> void: super("access.revoked", channel); presence = value
class ResyncRequired:
	extends ChatEvent
	var watermark_event_id: int
	var scopes: Array[String]
	func _init(channel: String, watermark: int, values: Array[String]) -> void:
		super("resync_required", channel); watermark_event_id = watermark; scopes = values

## One genuine instance is emitted for one not-yet-applied durable event. Fabricated
## instances have no owner completion closure and cannot mutate chat authority.
class LiveEventApplication:
	extends RefCounted
	var event: MessageEvent
	var _completion: Callable
	func _init(value: MessageEvent = null, completion: Callable = Callable()) -> void:
		event = value
		var guard := {"used":false}
		_completion = func(applier: Callable) -> bool:
			if guard.used or event == null or not completion.is_valid() or not applier.is_valid(): return false
			guard.used = true
			var result: Variant = applier.call(event)
			var applied: bool = result if result is bool else false
			completion.call(self, applied)
			return applied
	func apply(applier: Callable) -> bool:
		return _completion.call(applier) if _completion.is_valid() else false

class JoinHandle:
	extends RefCounted
	var _request_id: int
	var _generation: int
	func _init(request_id: int = 0, generation: int = 0) -> void:
		_request_id = request_id; _generation = generation
class JoinResponse:
	extends RefCounted
	var channel_id: String
	var watermark_event_id: int
	var requires_history: bool
	var current: bool
	func _init(channel: String, watermark: int, history_required: bool, is_current: bool) -> void:
		channel_id = channel; watermark_event_id = watermark; requires_history = history_required; current = is_current

class ReconciliationHandle:
	extends RefCounted
	var _request_id: int
	func _init(id: int = 0) -> void: _request_id = id

## Full replacement transaction. Pages are never published independently.
class HistorySnapshotApplication:
	extends RefCounted
	var channel_id: String
	var messages: Array[ChatMessage]
	var snapshot_watermark: int
	var replace := true
	var generation: int
	var snapshot_restarted: bool
	var _completion: Callable
	func _init(channel: String = "", values: Array[ChatMessage] = [], watermark: int = 0, generation_value: int = 0, restarted: bool = false, completion: Callable = Callable()) -> void:
		channel_id = channel; messages = values.duplicate(); snapshot_watermark = watermark
		generation = generation_value; snapshot_restarted = restarted
		var guard := {"used":false}
		_completion = func(applier: Callable) -> bool:
			if guard.used or channel_id.is_empty() or not completion.is_valid() or not applier.is_valid(): return false
			guard.used = true
			var result: Variant = applier.call(self)
			var applied: bool = result if result is bool else false
			completion.call(self, applied)
			return applied
	func apply(applier: Callable) -> bool:
		return _completion.call(applier) if _completion.is_valid() else false

# Every stored callable below is an operation-specific wrapper. Calling one
# directly has exactly the same semantics as calling its public method; none is
# a generic core-command or state-transition seam. Authority itself is captured
# by the closures created in _init and is never stored in an object member.
var _handle_envelope_op: Callable
var _joined_channels_op: Callable
var _disconnect_op: Callable
var _needs_resync_op: Callable
var _is_current_op: Callable
var _active_typing_op: Callable
var _join_op: Callable
var _leave_op: Callable
var _send_op: Callable
var _history_op: Callable
var _reconcile_op: Callable
var _edit_op: Callable
var _delete_op: Callable
var _moderate_op: Callable
var _typing_op: Callable
var _rejoin_op: Callable

func _init(rpc: Callable, max_tracked_channels: int) -> void:
	assert(rpc.is_valid(), "CitadelChatLive requires a valid RPC callable")
	assert(max_tracked_channels > 0, "CitadelChatLive max_tracked_channels must be positive")
	var authority := {
		"channels": {}, "revoked": {}, "typing": {}, "reconciliations": {}, "joins": {},
		"next_reconciliation": 1, "next_join": 1, "join_generation": 1
	}
	var send_rpc := func(method: String, request: Dictionary, callback: Callable) -> bool:
		return rpc.call(method, JSON.stringify(request).to_utf8_buffer(), callback)
	var invalidate_joins := func() -> void:
		authority.join_generation += 1
		authority.joins.clear()
	var has_reconciliation := func(channel_id: String) -> bool:
		for active in authority.reconciliations.values():
			if active.channel_id == channel_id: return true
		return false
	var has_rejoin := func(channel_id: String) -> bool:
		for pending in authority.joins.values():
			if pending.expected_channel == channel_id: return true
		return false
	var clear_channel := func(channel_id: String) -> void:
		authority.channels.erase(channel_id)
		authority.typing.erase(channel_id)
		for request_id in authority.reconciliations.keys():
			if authority.reconciliations[request_id].channel_id == channel_id: authority.reconciliations.erase(request_id)
		for request_id in authority.joins.keys():
			if authority.joins[request_id].expected_channel == channel_id: authority.joins.erase(request_id)
	var complete_join := func(request: Dictionary, bytes: PackedByteArray) -> void:
		if request.generation != authority.join_generation or not authority.joins.has(request.id) or not is_same(authority.joins[request.id], request): return
		authority.joins.erase(request.id)
		var parsed := _join_response(bytes)
		if parsed.is_empty(): return
		var channel_id: String = parsed.channel_id
		var watermark: int = parsed.watermark_event_id
		var requires_history := false
		var current := false
		if request.expected_channel.is_empty():
			if authority.channels.has(channel_id) or authority.channels.size() >= max_tracked_channels: return
			authority.revoked.erase(channel_id)
			authority.channels[channel_id] = {"cursor":watermark, "required":0, "current":true, "target":request.target.duplicate(true), "pending_live":null, "admitted":true, "epoch":1}
			current = true
		else:
			if channel_id != request.expected_channel or not authority.channels.has(channel_id) or authority.revoked.has(channel_id): return
			var state: Dictionary = authority.channels[channel_id]
			state.admitted = true
			state.epoch += 1
			if watermark == request.floor and state.cursor == request.floor and state.required <= state.cursor:
				state.required = 0; state.current = true; current = true
			else:
				state.current = false; state.required = max(state.required, max(state.cursor, watermark)); requires_history = true
				resync_needed.emit(channel_id, watermark)
		if request.callback.is_valid(): request.callback.call(JoinResponse.new(channel_id, watermark, requires_history, current))
	var begin_join := func(target: Dictionary, expected_channel: String, floor_value: int, callback: Callable) -> JoinHandle:
		var id: int = authority.next_join; authority.next_join += 1
		var handle := JoinHandle.new(id, authority.join_generation)
		var request := {"id":id, "handle":handle, "generation":authority.join_generation, "target":target.duplicate(true), "expected_channel":expected_channel, "floor":floor_value, "callback":callback}
		authority.joins[id] = request
		if not send_rpc.call("chat.join", {"target":request.target}, func(bytes: PackedByteArray) -> void: complete_join.call(request, bytes)):
			authority.joins.erase(id)
			return null
		return handle
	var abort_reconciliation := func(request: Dictionary) -> void:
		if authority.reconciliations.has(request.id) and is_same(authority.reconciliations[request.id], request):
			authority.reconciliations.erase(request.id)
	var reconciliation_admitted := func(request: Dictionary) -> bool:
		if authority.revoked.has(request.channel_id) or not authority.channels.has(request.channel_id): return false
		var state: Dictionary = authority.channels[request.channel_id]
		return state.admitted and state.epoch == request.admission_epoch
	var restart_reconciliation := func(request: Dictionary) -> void:
		if not authority.reconciliations.has(request.id) or not is_same(authority.reconciliations[request.id], request) or not reconciliation_admitted.call(request):
			abort_reconciliation.call(request); return
		request.generation += 1; request.page_serial += 1; request.snapshot = -1; request.before = 0
		request.awaiting_page = false; request.awaiting_apply = false; request.awaiting_ack = false; request.restarted = true; request.staged.clear()
		request.request_next.call()
	var request_ack := func(request: Dictionary) -> bool:
		if not authority.reconciliations.has(request.id) or request.awaiting_ack or not reconciliation_admitted.call(request):
			abort_reconciliation.call(request); return false
		if request.snapshot < request.floor:
			restart_reconciliation.call(request); return false
		request.awaiting_ack = true
		var generation: int = request.generation
		var ack := {"channel_id":request.channel_id, "limit":1, "acknowledge_watermark":request.snapshot}
		var sent: bool = send_rpc.call("chat.history", ack, func(bytes: PackedByteArray) -> void:
			if not authority.reconciliations.has(request.id) or request.generation != generation or not request.awaiting_ack or not reconciliation_admitted.call(request): return
			request.awaiting_ack = false
			if bytes.is_empty(): abort_reconciliation.call(request); return
			var response := _history_response(bytes, 1)
			if response.is_empty() or response.watermark_event_id != request.snapshot or response.watermark_event_id < request.floor:
				restart_reconciliation.call(request); return
			if authority.channels.has(request.channel_id) and not authority.revoked.has(request.channel_id):
				var state: Dictionary = authority.channels[request.channel_id]
				if state.admitted and state.required <= request.snapshot and state.cursor <= request.snapshot:
					state.cursor = request.snapshot; state.required = 0; state.current = true
			authority.reconciliations.erase(request.id)
		)
		if not sent:
			request.awaiting_ack = false
			abort_reconciliation.call(request)
		return sent
	var request_page := func(request: Dictionary) -> bool:
		if not authority.reconciliations.has(request.id) or request.awaiting_page or request.awaiting_apply or request.awaiting_ack or not reconciliation_admitted.call(request):
			abort_reconciliation.call(request); return false
		request.page_serial += 1
		var serial: int = request.page_serial
		var wire := history_request(request.channel_id, request.limit, request.before if request.before > 0 else null)
		request.awaiting_page = true
		var sent: bool = send_rpc.call("chat.history", wire, func(bytes: PackedByteArray) -> void:
			if not authority.reconciliations.has(request.id) or request.page_serial != serial or not request.awaiting_page or not reconciliation_admitted.call(request): return
			request.awaiting_page = false
			if bytes.is_empty(): abort_reconciliation.call(request); return
			var response := _history_response(bytes, request.limit)
			if response.is_empty() or (request.snapshot >= 0 and response.watermark_event_id != request.snapshot) or not _newest_first(response.get("items", []), request.before):
				restart_reconciliation.call(request); return
			request.snapshot = response.watermark_event_id
			for item in response.items: request.staged.append(item)
			if response.items.size() == request.limit:
				request.before = response.items.back().id; request.request_next.call(); return
			if request.snapshot < request.floor:
				restart_reconciliation.call(request); return
			request.awaiting_apply = true
			var generation: int = request.generation
			var typed_staged: Array[ChatMessage] = []
			typed_staged.assign(request.staged)
			var application := HistorySnapshotApplication.new(request.channel_id, typed_staged, request.snapshot, generation, request.restarted, func(_candidate: HistorySnapshotApplication, applied: bool) -> void:
				if not authority.reconciliations.has(request.id) or request.generation != generation or not request.awaiting_apply or not reconciliation_admitted.call(request): return
				request.awaiting_apply = false
				if not applied: restart_reconciliation.call(request); return
				request_ack.call(request)
			)
			if request.callback.is_valid(): request.callback.call(application)
			else: application.apply(func(_snapshot: HistorySnapshotApplication) -> bool: return false)
		)
		if not sent:
			request.awaiting_page = false
			abort_reconciliation.call(request)
		return sent
	# Recursion goes through the closure-captured dictionary, never an instance member.
	authority.request_page = request_page

	_handle_envelope_op = func(kind: int, payload: PackedByteArray) -> bool:
		if kind != Protocol.KIND_CHAT_EVENT: return false
		raw_envelope.emit(kind, payload.duplicate())
		var event := decode_event(kind, payload)
		if event == null: return true
		if event is AccessRevoked:
			invalidate_joins.call(); authority.revoked[event.channel_id] = true; clear_channel.call(event.channel_id); chat_event.emit(event); return true
		if authority.revoked.has(event.channel_id): return true
		if event is ResyncRequired:
			if not authority.channels.has(event.channel_id): return true
			var state: Dictionary = authority.channels[event.channel_id]
			state.current = false; state.required = max(state.required, event.watermark_event_id)
			resync_needed.emit(event.channel_id, event.watermark_event_id); chat_event.emit(event); return true
		if event is MessageEvent:
			if not authority.channels.has(event.channel_id): return true
			var state: Dictionary = authority.channels[event.channel_id]
			if not state.admitted or event.event_id <= state.cursor or state.pending_live != null: return true
			if state.cursor > 0 and event.event_id != state.cursor + 1:
				state.current = false; state.required = max(state.required, event.event_id); resync_needed.emit(event.channel_id, event.event_id); return true
			var epoch: int = state.epoch
			var application: LiveEventApplication
			application = LiveEventApplication.new(event, func(candidate: LiveEventApplication, applied: bool) -> void:
				if not authority.channels.has(event.channel_id): return
				var active: Dictionary = authority.channels[event.channel_id]
				if not active.admitted or active.epoch != epoch or not is_same(active.pending_live, candidate): return
				active.pending_live = null
				if not applied or authority.revoked.has(event.channel_id): return
				active.cursor = event.event_id
				if active.required <= active.cursor: active.required = 0
				chat_event.emit(event)
			)
			state.pending_live = application; live_event_pending.emit(application); return true
		if event is Typing and authority.channels.has(event.channel_id) and authority.channels[event.channel_id].admitted:
			if not authority.typing.has(event.channel_id): authority.typing[event.channel_id] = {}
			var key: String = event.presence.presence_id + "\n" + event.presence.user_id
			if event.typing: authority.typing[event.channel_id][key] = {"presence":event.presence, "expires_at":event.expires_at}
			else: authority.typing[event.channel_id].erase(key)
		chat_event.emit(event)
		return true
	_joined_channels_op = func() -> Array[String]:
		var result: Array[String] = []; result.assign(authority.channels.keys()); return result
	_disconnect_op = func() -> void:
		invalidate_joins.call()
		for state in authority.channels.values():
			state.current = false; state.required = max(state.required, state.cursor); state.pending_live = null; state.admitted = false; state.epoch += 1
		authority.typing.clear(); authority.reconciliations.clear()
	_needs_resync_op = func(channel_id: String) -> bool: return authority.channels.has(channel_id) and authority.channels[channel_id].required > 0
	_is_current_op = func(channel_id: String) -> bool: return authority.channels.has(channel_id) and authority.channels[channel_id].current and authority.channels[channel_id].required == 0
	_active_typing_op = func(channel_id: String, now_unix_ms: int) -> Array[ChatPresence]:
		var result: Array[ChatPresence] = []
		if not authority.typing.has(channel_id): return result
		var entries: Dictionary = authority.typing[channel_id]
		for key in entries.keys():
			if entries[key].expires_at <= now_unix_ms: entries.erase(key)
			else: result.append(entries[key].presence)
		return result
	_join_op = func(target: Dictionary, callback: Callable) -> JoinHandle: return begin_join.call(target, "", 0, callback)
	_leave_op = func(channel_id: String, callback: Callable) -> bool:
		if channel_id.is_empty(): return false
		return send_rpc.call("chat.leave", {"channel_id":channel_id}, func(bytes: PackedByteArray) -> void:
			var json := bytes.get_string_from_utf8(); var response: Variant = JSON.parse_string(json)
			if _unique_json_object_keys(json) and response is Dictionary and _exact_keys(response, ["left"]) and response.left is bool and response.left: clear_channel.call(channel_id)
			if callback.is_valid(): callback.call(bytes)
		)
	_send_op = func(channel_id: String, content: String, callback: Callable) -> bool: return send_rpc.call("chat.send", {"channel_id":channel_id, "content":content}, callback) if not channel_id.is_empty() and _valid_content(content) else false
	_history_op = func(request: Dictionary, callback: Callable) -> bool:
		if not _valid_history_request(request): return false
		var saved := request.duplicate(true)
		return send_rpc.call("chat.history", saved, func(bytes: PackedByteArray) -> void:
			var response := _history_response(bytes, saved.limit)
			if not response.is_empty() and callback.is_valid(): callback.call(response.items, response.watermark_event_id)
		)
	_reconcile_op = func(channel_id: String, limit: int, callback: Callable) -> ReconciliationHandle:
		if channel_id.is_empty() or limit < 1 or limit > MAX_HISTORY_LIMIT or not authority.channels.has(channel_id) or authority.revoked.has(channel_id) or has_rejoin.call(channel_id): return null
		var state: Dictionary = authority.channels[channel_id]
		if not state.admitted: return null
		for existing in authority.reconciliations.values():
			if existing.channel_id == channel_id: return existing.handle
		var id: int = authority.next_reconciliation; authority.next_reconciliation += 1
		var handle := ReconciliationHandle.new(id)
		var request := {"id":id, "handle":handle, "channel_id":channel_id, "limit":limit, "generation":1, "page_serial":0, "snapshot":-1, "before":0, "staged":[], "awaiting_page":false, "awaiting_apply":false, "awaiting_ack":false, "restarted":false, "callback":callback, "floor":max(state.cursor, state.required), "admission_epoch":state.epoch}
		request.request_next = func() -> bool: return authority.request_page.call(request)
		authority.reconciliations[id] = request
		if not request.request_next.call(): abort_reconciliation.call(request); return null
		return handle
	_edit_op = func(channel_id: String, message_id: int, content: String, callback: Callable) -> bool: return send_rpc.call("chat.edit", {"channel_id":channel_id, "message_id":message_id, "content":content}, callback) if not channel_id.is_empty() and _strict_int(message_id, 1, MAX_EXACT_INTEGER) and _valid_content(content) else false
	_delete_op = func(channel_id: String, message_id: int, callback: Callable) -> bool: return send_rpc.call("chat.delete", {"channel_id":channel_id, "message_id":message_id}, callback) if not channel_id.is_empty() and _strict_int(message_id, 1, MAX_EXACT_INTEGER) else false
	_moderate_op = func(channel_id: String, message_id: int, callback: Callable) -> bool: return send_rpc.call("chat.moderate", {"channel_id":channel_id, "message_id":message_id}, callback) if not channel_id.is_empty() and _strict_int(message_id, 1, MAX_EXACT_INTEGER) else false
	_typing_op = func(channel_id: String, typing_value: bool, callback: Callable) -> bool: return send_rpc.call("chat.typing", {"channel_id":channel_id, "typing":typing_value}, callback) if not channel_id.is_empty() else false
	_rejoin_op = func(callback: Callable) -> Array[JoinHandle]:
		var handles: Array[JoinHandle] = []
		for channel_id in authority.channels.keys():
			if has_reconciliation.call(channel_id): continue
			var existing: Dictionary = {}
			for pending in authority.joins.values():
				if pending.expected_channel == channel_id: existing = pending; break
			if not existing.is_empty(): handles.append(existing.handle); continue
			var state: Dictionary = authority.channels[channel_id]
			if state.target.is_empty(): continue
			var handle: JoinHandle = begin_join.call(state.target, channel_id, max(state.cursor, state.required), callback)
			if handle != null: handles.append(handle)
		return handles

static func decode_event(kind: int, payload: PackedByteArray) -> ChatEvent:
	if kind != Protocol.KIND_CHAT_EVENT or payload.is_empty(): return null
	var json := payload.get_string_from_utf8()
	if not _unique_json_object_keys(json): return null
	var parsed: Variant = JSON.parse_string(json)
	if not parsed is Dictionary: return null
	var value: Dictionary = parsed
	var integer_counts := {"version":1}
	match value.get("type"):
		"typing": integer_counts["expires_at"] = 1
		"message.create", "message.update", "message.remove":
			for field in ["event_id", "id", "created_at_unix_ms", "updated_at_unix_ms", "revision", "last_event_id"]: integer_counts[field] = 1
		"resync_required": integer_counts["watermark_event_id"] = 1
	if not _integer_json_fields(json, integer_counts): return null
	if not _strict_int(value.get("version"), 1, VERSION) or not _nonempty(value.get("type")) or not _nonempty(value.get("channel_id")): return null
	var event_type: String = value.type
	var channel: String = value.channel_id
	match event_type:
		"presence.join":
			if not _exact_keys(value, ["version", "type", "channel_id", "channel_type", "presence"]): return null
			var presence := _presence(value.get("presence"))
			if presence == null or value.get("channel_type") not in ["direct", "group", "room"]: return null
			return PresenceJoined.new(channel, value.channel_type, presence)
		"presence.leave":
			if not _exact_keys(value, ["version", "type", "channel_id", "presence"]): return null
			var presence := _presence(value.get("presence")); return PresenceLeft.new(channel, presence) if presence != null else null
		"typing":
			if not _exact_keys(value, ["version", "type", "channel_id", "presence", "typing", "expires_at"]): return null
			var presence := _presence(value.get("presence"))
			var active: Variant = value.get("typing")
			var expiry: Variant = value.get("expires_at")
			if presence == null or not active is bool or not _strict_int(expiry, 0, MAX_EXACT_INTEGER): return null
			if (active and expiry <= 0) or (not active and expiry != 0): return null
			return Typing.new(channel, presence, active, expiry)
		"message.create", "message.update", "message.remove":
			return _message_event(value)
		"access.revoked":
			if not _exact_keys(value, ["version", "type", "channel_id", "presence"]): return null
			var presence := _presence(value.get("presence")); return AccessRevoked.new(channel, presence) if presence != null else null
		"resync_required":
			if not _exact_keys(value, ["version", "type", "channel_id", "watermark_event_id", "scopes"]): return null
			var watermark: Variant = value.get("watermark_event_id")
			var raw_scopes: Variant = value.get("scopes")
			if not _strict_int(watermark, 1, MAX_EXACT_INTEGER) or not raw_scopes is Array: return null
			var scopes: Array[String] = []
			for scope in raw_scopes:
				if not scope is String or scope not in ["history", "presence"] or scopes.has(scope): return null
				scopes.append(scope)
			return ResyncRequired.new(channel, watermark, scopes)
	return null

static func _message_event(value: Dictionary) -> ChatEvent:
	if not _exact_keys(value, ["version", "type", "channel_id", "event_id", "message"]): return null
	var event_id: Variant = value.get("event_id")
	var raw: Variant = value.get("message")
	if not _strict_int(event_id, 1, MAX_EXACT_INTEGER) or not _valid_message(raw, event_id): return null
	var event_type: String = value.type
	if event_type == "message.create" and (raw.revision != 1 or raw.deleted or raw.created_at_unix_ms != raw.updated_at_unix_ms): return null
	if event_type != "message.create" and raw.revision <= 1: return null
	if event_type == "message.update" and raw.deleted: return null
	if event_type == "message.remove" and (not raw.deleted or not raw.content.is_empty()): return null
	if event_type != "message.remove" and not _valid_content(raw.content): return null
	var message := ChatMessage.new(raw)
	if event_type == "message.create": return MessageCreated.new(value.channel_id, event_id, message)
	if event_type == "message.update": return MessageUpdated.new(value.channel_id, event_id, message)
	return MessageRemoved.new(value.channel_id, event_id, message)

static func _presence(value: Variant) -> ChatPresence:
	if not value is Dictionary or not _exact_keys(value, ["presence_id", "user_id"]): return null
	if not _nonempty(value.get("presence_id")) or not _nonempty(value.get("user_id")): return null
	return ChatPresence.new(value.presence_id, value.user_id)

static func _valid_message(item: Variant, expected_event: Variant = null) -> bool:
	if not item is Dictionary or not _exact_keys(item, ["id", "sender", "content", "created_at_unix_ms", "updated_at_unix_ms", "revision", "last_event_id", "deleted"]): return false
	if not _strict_int(item.get("id"), 1, MAX_EXACT_INTEGER) or not _nonempty(item.get("sender")) or not item.get("content") is String: return false
	if not _strict_int(item.get("created_at_unix_ms"), 0, MAX_EXACT_INTEGER) or not _strict_int(item.get("updated_at_unix_ms"), 0, MAX_EXACT_INTEGER): return false
	if item.updated_at_unix_ms < item.created_at_unix_ms: return false
	if not _strict_int(item.get("revision"), 1, MAX_EXACT_INTEGER) or not _strict_int(item.get("last_event_id"), 1, MAX_EXACT_INTEGER) or not item.get("deleted") is bool: return false
	if expected_event != null and item.last_event_id != expected_event: return false
	return true

static func _valid_history_message(item: Variant, watermark: int) -> bool:
	if not _valid_message(item) or item.last_event_id > watermark: return false
	if item.revision == 1: return not item.deleted and item.created_at_unix_ms == item.updated_at_unix_ms and _valid_content(item.content)
	if item.deleted: return item.content.is_empty()
	return _valid_content(item.content)

static func _strict_int(value: Variant, minimum: int, maximum: int) -> bool:
	if typeof(value) == TYPE_INT: return value >= minimum and value <= maximum
	# Godot 4.3's JSON decoder represents integer tokens as float Variants. The
	# lexical gate above proves the token had no decimal/exponent before this path.
	return typeof(value) == TYPE_FLOAT and is_finite(value) and floor(value) == value and value >= minimum and value <= maximum
static func _integer_json_fields(json: String, expected_counts: Dictionary) -> bool:
	if not _unique_json_object_keys(json): return false
	for field in expected_counts:
		var any := RegEx.new(); var exact := RegEx.new()
		if any.compile('"%s"\\s*:' % field) != OK: return false
		if exact.compile('"%s"\\s*:\\s*(?:0|[1-9][0-9]*)\\s*(?=,|})' % field) != OK: return false
		if any.search_all(json).size() != expected_counts[field] or exact.search_all(json).size() != expected_counts[field]: return false
	return true

## Bounded pre-parser lexical pass. JSON.parse_string overwrites duplicate object
## members, so every object key is decoded (including escapes) and uniqued first.
static func _unique_json_object_keys(json: String) -> bool:
	if json.to_utf8_buffer().size() > 262144: return false
	var stack: Array[Dictionary] = []
	var index := 0
	while index < json.length():
		var code := json.unicode_at(index)
		if code == 34:
			var start := index
			index += 1
			var escaped := false
			while index < json.length():
				var inner := json.unicode_at(index)
				if escaped: escaped = false
				elif inner == 92: escaped = true
				elif inner == 34: break
				elif inner < 32: return false
				index += 1
			if index >= json.length(): return false
			if not stack.is_empty() and stack.back().kind == "object" and stack.back().expect_key:
				var quoted := json.substr(start, index - start + 1)
				var decoded: Variant = JSON.parse_string(quoted)
				if not decoded is String or stack.back().keys.has(decoded): return false
				stack.back().keys[decoded] = true
				stack.back().expect_key = false
			index += 1
			continue
		match code:
			123:
				stack.append({"kind":"object", "keys":{}, "expect_key":true})
			125:
				if stack.is_empty() or stack.back().kind != "object": return false
				stack.pop_back()
			91:
				stack.append({"kind":"array", "keys":{}, "expect_key":false})
			93:
				if stack.is_empty() or stack.back().kind != "array": return false
				stack.pop_back()
			44:
				if not stack.is_empty() and stack.back().kind == "object": stack.back().expect_key = true
		index += 1
	return stack.is_empty()
static func _nonempty(value: Variant) -> bool:
	return value is String and not value.is_empty()
static func _exact_keys(value: Dictionary, keys: Array) -> bool:
	if value.size() != keys.size(): return false
	for key in keys:
		if not value.has(key): return false
	return true
static func _valid_content(value: String) -> bool:
	if value.is_empty() or value.to_utf8_buffer().size() > 2048: return false
	for index in range(value.length()):
		var codepoint := value.unicode_at(index)
		if codepoint < 32 and codepoint not in [10, 13]: return false
		if codepoint == 127: return false
	return true

func handle_envelope(kind: int, payload: PackedByteArray) -> bool:
	return _handle_envelope_op.call(kind, payload)
func joined_channels() -> Array[String]:
	return _joined_channels_op.call()
func on_disconnected() -> void:
	_disconnect_op.call()
func needs_resync(channel_id: String) -> bool:
	return _needs_resync_op.call(channel_id)
func is_current(channel_id: String) -> bool:
	return _is_current_op.call(channel_id)
func active_typing(channel_id: String, now_unix_ms: int) -> Array[ChatPresence]:
	return _active_typing_op.call(channel_id, now_unix_ms)

static func direct_target(other_user_id: String) -> Dictionary:
	assert(not other_user_id.is_empty()); return {"kind":"direct", "other_user_id":other_user_id}
static func group_target(group_id: int) -> Dictionary:
	assert(_strict_int(group_id, 1, MAX_EXACT_INTEGER)); return {"kind":"group", "group_id":group_id}
static func room_target(room_id: int) -> Dictionary:
	assert(_strict_int(room_id, 1, MAX_EXACT_INTEGER)); return {"kind":"room", "room_id":room_id}
static func history_request(channel_id: String, limit: int, before_message_id: Variant = null) -> Dictionary:
	assert(not channel_id.is_empty() and _strict_int(limit, 1, MAX_HISTORY_LIMIT))
	var request := {"channel_id":channel_id, "limit":limit}
	if before_message_id != null:
		assert(_strict_int(before_message_id, 1, MAX_EXACT_INTEGER)); request.before_message_id = before_message_id
	return request

func join(target: Dictionary, callback: Callable) -> JoinHandle:
	return _join_op.call(target, callback)
func leave(channel_id: String, callback: Callable) -> bool:
	return _leave_op.call(channel_id, callback)
func send_message(channel_id: String, content: String, callback: Callable) -> bool:
	return _send_op.call(channel_id, content, callback)
func history(request: Dictionary, callback: Callable) -> bool:
	return _history_op.call(request, callback)
func begin_reconciliation(channel_id: String, limit: int, callback: Callable) -> ReconciliationHandle:
	return _reconcile_op.call(channel_id, limit, callback)
func edit(channel_id: String, message_id: int, content: String, callback: Callable) -> bool:
	return _edit_op.call(channel_id, message_id, content, callback)
func delete_message(channel_id: String, message_id: int, callback: Callable) -> bool:
	return _delete_op.call(channel_id, message_id, callback)
func moderate(channel_id: String, message_id: int, callback: Callable) -> bool:
	return _moderate_op.call(channel_id, message_id, callback)
func set_typing(channel_id: String, typing: bool, callback: Callable) -> bool:
	return _typing_op.call(channel_id, typing, callback)
func rejoin_tracked_channels(callback: Callable) -> Array[JoinHandle]:
	return _rejoin_op.call(callback)

static func _join_response(bytes: PackedByteArray) -> Dictionary:
	var json := bytes.get_string_from_utf8()
	if not _integer_json_fields(json, {"watermark_event_id":1}): return {}
	var parsed: Variant = JSON.parse_string(json)
	if not parsed is Dictionary or not _exact_keys(parsed, ["channel_id", "watermark_event_id"]): return {}
	if not _nonempty(parsed.get("channel_id")) or not _strict_int(parsed.get("watermark_event_id"), 0, MAX_EXACT_INTEGER): return {}
	return parsed
static func _history_response(bytes: PackedByteArray, requested_limit: int) -> Dictionary:
	var json := bytes.get_string_from_utf8()
	if not _unique_json_object_keys(json): return {}
	var parsed: Variant = JSON.parse_string(json)
	if not parsed is Dictionary or not _exact_keys(parsed, ["items", "watermark_event_id"]): return {}
	if not parsed.items is Array or parsed.items.size() > requested_limit or not _strict_int(parsed.watermark_event_id, 0, MAX_EXACT_INTEGER): return {}
	var count: int = parsed.items.size()
	var integer_counts := {"watermark_event_id":1, "id":count, "created_at_unix_ms":count, "updated_at_unix_ms":count, "revision":count, "last_event_id":count}
	if not _integer_json_fields(json, integer_counts): return {}
	var typed: Array[ChatMessage] = []
	for item in parsed.items:
		if not _valid_history_message(item, parsed.watermark_event_id): return {}
		typed.append(ChatMessage.new(item))
	return {"items":typed, "watermark_event_id":parsed.watermark_event_id}
static func _newest_first(items: Array, before: int) -> bool:
	var previous := before if before > 0 else MAX_EXACT_INTEGER + 1
	for item in items:
		if item.id >= previous: return false
		previous = item.id
	return true
static func _valid_history_request(request: Variant) -> bool:
	if not request is Dictionary or request.size() not in [2, 3] or not _nonempty(request.get("channel_id")) or not _strict_int(request.get("limit"), 1, MAX_HISTORY_LIMIT): return false
	if request.size() == 2: return _exact_keys(request, ["channel_id", "limit"])
	return _exact_keys(request, ["channel_id", "limit", "before_message_id"]) and _strict_int(request.get("before_message_id"), 1, MAX_EXACT_INTEGER)
