extends SceneTree

const Protocol = preload("res://addons/citadel/protocol.gd")
const ChatLive = preload("res://addons/citadel/chat_live.gd")

var failures: Array[String] = []
var emitted: Array[String] = []
var rpc_methods: Array[String] = []
var rpc_payloads: Array[Dictionary] = []
var rpc_callbacks: Array[Callable] = []
var rpc_fail_next := false

func _init() -> void:
	_test_canonical_fixture()
	_test_dispatcher_lifecycle_and_builders()
	_test_join_callbacks_are_correlated_and_revocation_is_final()
	_test_reconciliation_application_and_ack_boundary()
	_test_reconciliation_restarts_moving_snapshot()
	_test_recovery_floor_and_send_failures()
	_test_rpc_errors_cancel_reconciliation_without_stranding()
	_test_bounded_history_response_larger_than_8k_routes_to_snapshot()
	_test_disconnect_fences_every_reconciliation_boundary()
	_test_blocker_regressions()
	for failure in failures:
		push_error(failure)
	quit(0 if failures.is_empty() else 1)

func _fixture_path() -> String:
	return ProjectSettings.globalize_path("res://../../../../tests/fixtures/chat-live-events-v1.json")

func _test_canonical_fixture() -> void:
	var file := FileAccess.open(_fixture_path(), FileAccess.READ)
	_expect(file != null, "canonical chat fixture must be readable")
	if file == null:
		return
	var fixture: Dictionary = JSON.parse_string(file.get_as_text())
	_expect(fixture.get("valid", []).size() == 8, "fixture must contain eight variants")
	for item in fixture.get("valid", []):
		var decoded := ChatLive.decode_event(int(Protocol.KIND_CHAT_EVENT), JSON.stringify(item.event).to_utf8_buffer())
		_expect(decoded != null and decoded.type == item.kind, "valid fixture must decode: %s" % item.name)
	for item in fixture.get("invalid", []):
		var body: String = item.get("payload", JSON.stringify(item.get("event", {})))
		_expect(ChatLive.decode_event(Protocol.KIND_CHAT_EVENT, body.to_utf8_buffer()) == null, "invalid fixture must fail closed: %s" % item.name)

func _test_dispatcher_lifecycle_and_builders() -> void:
	var chat := ChatLive.new(Callable(self, "_rpc"), 4)
	chat.chat_event.connect(func(event: ChatLive.ChatEvent) -> void: emitted.append(event.type))
	chat.live_event_pending.connect(func(application: ChatLive.LiveEventApplication) -> void:
		application.apply(func(_event: ChatLive.MessageEvent) -> bool: return true)
	)
	var join_results: Array[ChatLive.JoinResponse] = []
	var join_handle: ChatLive.JoinHandle = chat.join(ChatLive.direct_target("bob"), func(response: ChatLive.JoinResponse) -> void: join_results.append(response))
	_expect(join_handle != null, "join must return an opaque request handle")
	rpc_callbacks[0].call('{"channel_id":"ch_demo","watermark_event_id":4}'.to_utf8_buffer())
	_expect(join_results.size() == 1 and join_results[0].channel_id == "ch_demo", "initial valid join must return a typed response")
	_expect(chat.is_current("ch_demo"), "initial valid join may establish current state")
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var create := '{"version":1,"type":"message.create","channel_id":"ch_demo","event_id":5,"message":{"id":1,"sender":"alice","content":"hello","created_at_unix_ms":1000,"updated_at_unix_ms":1000,"revision":1,"last_event_id":5,"deleted":false}}'.to_utf8_buffer()
	_expect(chat.handle_envelope(Protocol.KIND_CHAT_EVENT, create), "chat envelope must be consumed")
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, create)
	_expect(emitted.size() == 1, "durable duplicate must be suppressed")
	var gap := '{"version":1,"type":"message.update","channel_id":"ch_demo","event_id":7,"message":{"id":1,"sender":"alice","content":"later","created_at_unix_ms":1000,"updated_at_unix_ms":1100,"revision":2,"last_event_id":7,"deleted":false}}'.to_utf8_buffer()
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, gap)
	_expect(chat.needs_resync("ch_demo"), "durable gap must require resync")
	_expect(not chat.is_current("ch_demo"), "gap remains stale until reconciled history is applied")
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, '{"version":1,"type":"typing","channel_id":"ch_demo","presence":{"presence_id":"p","user_id":"u"},"typing":true,"expires_at":10}'.to_utf8_buffer())
	_expect(chat.active_typing("ch_demo", 9).size() == 1, "typing must remain until expiry")
	_expect(chat.active_typing("ch_demo", 10).is_empty(), "typing must expire without stop")
	chat.on_disconnected()
	_expect(not chat.is_current("ch_demo"), "disconnect must mark channel stale")
	_expect(chat.rejoin_tracked_channels(Callable()).size() == 1, "reconnect must return one opaque rejoin handle")
	_expect(rpc_callbacks.size() == 1, "rejoin must create one correlated join request")
	rpc_callbacks[0].call('{"channel_id":"ch_demo","watermark_event_id":4}'.to_utf8_buffer())
	_expect(not chat.is_current("ch_demo") and chat.needs_resync("ch_demo"), "rejoin cannot bypass outstanding history reconciliation")
	_expect(chat.history(ChatLive.history_request("ch_demo", 50), Callable()), "history builder must issue normal history without ACK")
	_expect(chat.send_message("ch_demo", "hello", Callable()), "send builder must issue RPC")
	_expect(chat.edit("ch_demo", 1, "edited", Callable()), "edit builder must issue RPC")
	_expect(chat.delete_message("ch_demo", 1, Callable()), "delete builder must issue RPC")
	_expect(chat.moderate("ch_demo", 1, Callable()), "moderate builder must issue RPC")
	_expect(chat.set_typing("ch_demo", true, Callable()), "typing builder must issue RPC")
	_expect(chat.leave("ch_demo", Callable()), "leave builder must issue RPC")
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, '{"version":1,"type":"access.revoked","channel_id":"ch_demo","presence":{"presence_id":"p","user_id":"u"}}'.to_utf8_buffer())
	_expect(chat.joined_channels().is_empty(), "revocation must clear private channel state")
	_expect(rpc_methods == ["chat.join", "chat.history", "chat.send", "chat.edit", "chat.delete", "chat.moderate", "chat.typing", "chat.leave"], "helpers must use typed domain RPC methods")

func _test_join_callbacks_are_correlated_and_revocation_is_final() -> void:
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var chat := ChatLive.new(Callable(self, "_rpc"), 4)
	var results: Array[ChatLive.JoinResponse] = []
	var initial: ChatLive.JoinHandle = chat.join(ChatLive.direct_target("bob"), func(response: ChatLive.JoinResponse) -> void: results.append(response))
	_expect(initial != null, "initial join must expose only an opaque handle")
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, '{"version":1,"type":"access.revoked","channel_id":"ch_revoked","presence":{"presence_id":"p","user_id":"u"}}'.to_utf8_buffer())
	rpc_callbacks[0].call('{"channel_id":"ch_revoked","watermark_event_id":4}'.to_utf8_buffer())
	_expect(chat.joined_channels().is_empty() and results.is_empty(), "revocation must invalidate pending join callbacks")

	var fresh: ChatLive.JoinHandle = chat.join(ChatLive.direct_target("bob"), func(response: ChatLive.JoinResponse) -> void: results.append(response))
	_expect(fresh != null, "fresh generation join must be accepted")
	rpc_callbacks[1].call('{"channel_id":"ch_demo","watermark_event_id":4}'.to_utf8_buffer())
	_expect(chat.is_current("ch_demo"), "fresh initial join must establish state")
	chat.on_disconnected()
	chat.rejoin_tracked_channels(Callable())
	var stale_callback := rpc_callbacks[2]
	chat.on_disconnected()
	chat.rejoin_tracked_channels(Callable())
	stale_callback.call('{"channel_id":"ch_demo","watermark_event_id":4}'.to_utf8_buffer())
	_expect(not chat.is_current("ch_demo"), "cross-generation rejoin callback must not restore current")
	rpc_callbacks[3].call('{"channel_id":"ch_other","watermark_event_id":4}'.to_utf8_buffer())
	_expect(not chat.is_current("ch_demo") and not chat.joined_channels().has("ch_other"), "cross-channel rejoin response must not mutate state")
	chat.rejoin_tracked_channels(Callable())
	rpc_callbacks[4].call('{"channel_id":"ch_demo","watermark_event_id":4}'.to_utf8_buffer())
	_expect(chat.is_current("ch_demo"), "same rejoin watermark may restore current state")
	chat.on_disconnected()
	chat.rejoin_tracked_channels(Callable())
	rpc_callbacks[5].call('{"channel_id":"ch_demo","watermark_event_id":8}'.to_utf8_buffer())
	_expect(not chat.is_current("ch_demo") and chat.needs_resync("ch_demo"), "changed rejoin watermark must force history reconciliation")
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var malformed_chat := ChatLive.new(Callable(self, "_rpc"), 2)
	var malformed_results: Array[ChatLive.JoinResponse] = []
	malformed_chat.join(ChatLive.direct_target("mallory"), func(response: ChatLive.JoinResponse) -> void: malformed_results.append(response))
	rpc_callbacks[0].call('{"channel_id":"ch_string","watermark_event_id":"4"}'.to_utf8_buffer())
	_expect(malformed_chat.joined_channels().is_empty() and malformed_results.is_empty(), "join response watermark must retain its typed integer contract")

func _rpc(method: String, payload: PackedByteArray, _callback: Callable) -> bool:
	if rpc_fail_next:
		rpc_fail_next = false
		return false
	rpc_methods.append(method)
	var parsed: Variant = JSON.parse_string(payload.get_string_from_utf8())
	if parsed is Dictionary:
		rpc_payloads.append(parsed)
		rpc_callbacks.append(_callback)
	return parsed is Dictionary

func _test_reconciliation_application_and_ack_boundary() -> void:
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var snapshots: Array[ChatLive.HistorySnapshotApplication] = []
	var chat := ChatLive.new(Callable(self, "_rpc"), 4)
	chat.join(ChatLive.direct_target("bob"), Callable())
	rpc_callbacks[0].call('{"channel_id":"ch_demo","watermark_event_id":4}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	chat.on_disconnected()
	chat.rejoin_tracked_channels(Callable())
	rpc_callbacks[0].call('{"channel_id":"ch_demo","watermark_event_id":9}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var request_handle := chat.begin_reconciliation("ch_demo", 2, func(snapshot: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(snapshot))
	_expect(request_handle != null, "reconciliation must return an opaque request handle")
	_expect(not rpc_payloads[0].has("acknowledge_watermark"), "normal reconciliation pages must not expose ACK")
	rpc_callbacks[0].call(JSON.stringify({"items":[_history_message(9, 9), _history_message(8, 8)],"watermark_event_id":9}).to_utf8_buffer())
	_expect(snapshots.is_empty(), "history pages must remain internal until a complete snapshot exists")
	_expect(rpc_payloads.size() == 2, "full page must continue internally without publishing partial application")
	_expect(int(rpc_payloads[1].before_message_id) == 8, "pagination must continue before the oldest item")
	rpc_callbacks[1].call('{"items":[],"watermark_event_id":9}'.to_utf8_buffer())
	_expect(snapshots.size() == 1 and snapshots[0].replace and snapshots[0].messages.size() == 2, "terminal page must publish one full replacement transaction")
	_expect(not chat.is_current("ch_demo"), "snapshot reception must remain stale")
	_expect(snapshots[0]._completion.call(func(_snapshot: ChatLive.HistorySnapshotApplication) -> bool: return true), "direct completion invocation must be identical to successful snapshot apply")
	_expect(int(rpc_payloads[2].acknowledge_watermark) == 9, "private ACK must use stable snapshot watermark")
	_expect(not chat.is_current("ch_demo"), "ACK request alone must not mark current")
	rpc_callbacks[2].call('{"items":[],"watermark_event_id":9}'.to_utf8_buffer())
	_expect(chat.is_current("ch_demo"), "correlated ACK response may mark channel current")
	_expect(not snapshots[0].apply(func(_snapshot: ChatLive.HistorySnapshotApplication) -> bool: return true), "snapshot application capability must be exactly once")

func _test_reconciliation_restarts_moving_snapshot() -> void:
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var snapshots: Array[ChatLive.HistorySnapshotApplication] = []
	var chat := ChatLive.new(Callable(self, "_rpc"), 2)
	chat.join(ChatLive.direct_target("bob"), Callable())
	rpc_callbacks[0].call('{"channel_id":"ch","watermark_event_id":1}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	chat.on_disconnected()
	chat.rejoin_tracked_channels(Callable())
	rpc_callbacks[0].call('{"channel_id":"ch","watermark_event_id":9}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	chat.begin_reconciliation("ch", 1, func(snapshot: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(snapshot))
	rpc_callbacks[0].call(JSON.stringify({"items":[_history_message(9, 9)],"watermark_event_id":9}).to_utf8_buffer())
	rpc_callbacks[1].call('{"items":[],"watermark_event_id":10}'.to_utf8_buffer())
	_expect(not rpc_payloads[2].has("before_message_id"), "moving snapshot must restart at newest page")
	rpc_callbacks[2].call('{"items":[],"watermark_event_id":10}'.to_utf8_buffer())
	_expect(snapshots.size() == 1 and snapshots[0].snapshot_restarted and snapshots[0].snapshot_watermark == 10, "restarted full snapshot must be explicit to the applier")

func _test_recovery_floor_and_send_failures() -> void:
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var snapshots: Array[ChatLive.HistorySnapshotApplication] = []
	var chat := ChatLive.new(Callable(self, "_rpc"), 2)
	chat.join(ChatLive.direct_target("bob"), Callable())
	rpc_callbacks[0].call('{"channel_id":"floor","watermark_event_id":4}'.to_utf8_buffer())
	chat.on_disconnected(); rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	chat.rejoin_tracked_channels(Callable())
	rpc_callbacks[0].call('{"channel_id":"floor","watermark_event_id":9}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	rpc_fail_next = true
	_expect(chat.begin_reconciliation("floor", 2, Callable()) == null, "page send failure must remove the dead operation")
	var first := chat.begin_reconciliation("floor", 2, func(snapshot: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(snapshot))
	_expect(first != null, "fresh reconciliation must restart after page send failure")
	rpc_callbacks[0].call('{"items":[],"watermark_event_id":8}'.to_utf8_buffer())
	_expect(snapshots.is_empty() and rpc_payloads.size() == 2 and not rpc_payloads[1].has("before_message_id"), "snapshot below captured recovery floor must restart newest and never publish")
	rpc_callbacks[1].call('{"items":[],"watermark_event_id":9}'.to_utf8_buffer())
	_expect(snapshots.size() == 1, "restarted floor-compliant snapshot must publish")
	rpc_fail_next = true
	_expect(snapshots[0].apply(func(_snapshot: ChatLive.HistorySnapshotApplication) -> bool: return true), "local apply remains successful when ACK transport fails")
	_expect(chat.begin_reconciliation("floor", 2, Callable()) != null, "ACK send failure must remove dead operation for deterministic retry")

func _test_disconnect_fences_every_reconciliation_boundary() -> void:
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var snapshots: Array[ChatLive.HistorySnapshotApplication] = []
	var chat := ChatLive.new(Callable(self, "_rpc"), 2)
	chat.join(ChatLive.direct_target("bob"), Callable())
	rpc_callbacks[0].call('{"channel_id":"disconnect","watermark_event_id":4}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	chat.on_disconnected()
	_expect(chat.begin_reconciliation("disconnect", 2, func(value: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(value)) == null, "disconnect must reject reconciliation until a fresh correlated rejoin")
	_expect(rpc_callbacks.is_empty() and snapshots.is_empty(), "rejected post-disconnect reconciliation must send no history RPC and expose no snapshot")
	chat.rejoin_tracked_channels(Callable())
	rpc_callbacks[0].call('{"channel_id":"disconnect","watermark_event_id":9}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()

	_expect(chat.begin_reconciliation("disconnect", 2, func(value: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(value)) != null, "fresh correlated rejoin must restore reconciliation admission")
	var stale_page := rpc_callbacks[0]
	chat.on_disconnected()
	stale_page.call('{"items":[],"watermark_event_id":9}'.to_utf8_buffer())
	_expect(snapshots.is_empty() and rpc_payloads.size() == 1, "disconnect at page boundary must expose no snapshot and send no ACK")

	chat.rejoin_tracked_channels(Callable()); rpc_callbacks.back().call('{"channel_id":"disconnect","watermark_event_id":9}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	chat.begin_reconciliation("disconnect", 2, func(value: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(value))
	rpc_callbacks[0].call('{"items":[],"watermark_event_id":9}'.to_utf8_buffer())
	_expect(snapshots.size() == 1, "terminal page must publish snapshot while admitted")
	chat.on_disconnected()
	var calls_before_apply := rpc_payloads.size()
	snapshots[0].apply(func(_value: ChatLive.HistorySnapshotApplication) -> bool: return true)
	_expect(rpc_payloads.size() == calls_before_apply, "disconnect at application boundary must emit no private ACK")

	chat.rejoin_tracked_channels(Callable()); rpc_callbacks.back().call('{"channel_id":"disconnect","watermark_event_id":9}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear(); snapshots.clear()
	chat.begin_reconciliation("disconnect", 2, func(value: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(value))
	rpc_callbacks[0].call('{"items":[],"watermark_event_id":9}'.to_utf8_buffer())
	snapshots[0].apply(func(_value: ChatLive.HistorySnapshotApplication) -> bool: chat.on_disconnected(); return true)
	_expect(rpc_payloads.size() == 1, "disconnect between local apply and ACK must emit no private ACK")

	chat.rejoin_tracked_channels(Callable()); rpc_callbacks.back().call('{"channel_id":"disconnect","watermark_event_id":9}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear(); snapshots.clear()
	chat.begin_reconciliation("disconnect", 2, func(value: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(value))
	rpc_callbacks[0].call('{"items":[],"watermark_event_id":9}'.to_utf8_buffer()); snapshots[0].apply(func(_value: ChatLive.HistorySnapshotApplication) -> bool: return true)
	var stale_ack := rpc_callbacks[1]
	chat.on_disconnected(); stale_ack.call('{"items":[],"watermark_event_id":9}'.to_utf8_buffer())
	_expect(not chat.is_current("disconnect"), "disconnect at ACK boundary must make the late ACK reply inert")

func _test_rpc_errors_cancel_reconciliation_without_stranding() -> void:
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var snapshots: Array[ChatLive.HistorySnapshotApplication] = []
	var chat := ChatLive.new(Callable(self, "_rpc"), 2)
	chat.join(ChatLive.direct_target("bob"), Callable()); rpc_callbacks[0].call('{"channel_id":"errors","watermark_event_id":4}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var failed_page := chat.begin_reconciliation("errors", 2, Callable())
	rpc_callbacks[0].call(PackedByteArray())
	var after_page_error := chat.begin_reconciliation("errors", 2, func(value: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(value))
	_expect(after_page_error != null and not is_same(failed_page, after_page_error), "asynchronous page RPC error must cancel the consumed operation instead of stranding its handle")
	rpc_callbacks.back().call('{"items":[],"watermark_event_id":4}'.to_utf8_buffer()); snapshots[0].apply(func(_value: ChatLive.HistorySnapshotApplication) -> bool: return true)
	var failed_ack := after_page_error
	rpc_callbacks.back().call(PackedByteArray())
	var after_ack_error := chat.begin_reconciliation("errors", 2, Callable())
	_expect(after_ack_error != null and not is_same(failed_ack, after_ack_error), "asynchronous ACK RPC error must cancel the consumed operation instead of stranding its handle")

func _test_bounded_history_response_larger_than_8k_routes_to_snapshot() -> void:
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var snapshots: Array[ChatLive.HistorySnapshotApplication] = []
	var chat := ChatLive.new(Callable(self, "_rpc"), 2)
	chat.join(ChatLive.direct_target("bob"), Callable()); rpc_callbacks[0].call('{"channel_id":"large","watermark_event_id":20}'.to_utf8_buffer())
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	chat.begin_reconciliation("large", 10, func(value: ChatLive.HistorySnapshotApplication) -> void: snapshots.append(value))
	var items: Array[Dictionary] = []
	for id in range(20, 14, -1):
		var item := _history_message(id, id)
		item.content = "x".repeat(1500)
		items.append(item)
	var response := JSON.stringify({"items":items,"watermark_event_id":20}).to_utf8_buffer()
	_expect(response.size() > 8192, "synthetic bounded history response must exceed the obsolete 8 KiB poll buffer")
	rpc_callbacks[0].call(response)
	_expect(snapshots.size() == 1 and snapshots[0].messages.size() == 6, "bounded history response larger than 8 KiB must route and clean up its page callback")

func _history_message(id: int, event_id: int) -> Dictionary:
	return {"id":id,"sender":"alice","content":"message","created_at_unix_ms":1,"updated_at_unix_ms":1,"revision":1,"last_event_id":event_id,"deleted":false}

func _test_blocker_regressions() -> void:
	_expect(ChatLive.decode_event(Protocol.KIND_CHAT_EVENT, '{"version":1,"type":"presence.leave","type":"presence.leave","channel_id":"dup","presence":{"presence_id":"p","user_id":"u"}}'.to_utf8_buffer()) == null, "duplicate type keys must fail before Godot dictionary coercion")
	# Strict Variant typing: JSON numbers may be int or float, but the wire contract
	# requires exact integer tokens and closed variant fields.
	var valid := {"version":1,"type":"message.create","channel_id":"strict","event_id":5,"message":_history_message(1, 5)}
	for path in ["version", "event_id"]:
		var mutated := valid.duplicate(true)
		mutated[path] = "1" if path == "version" else "5"
		_expect(ChatLive.decode_event(Protocol.KIND_CHAT_EVENT, JSON.stringify(mutated).to_utf8_buffer()) == null, "numeric strings must fail strict event decoding: %s" % path)
	var fractional := valid.duplicate(true); fractional.event_id = 5.5; fractional.message.last_event_id = 5.5
	_expect(ChatLive.decode_event(Protocol.KIND_CHAT_EVENT, JSON.stringify(fractional).to_utf8_buffer()) == null, "fractional event identifiers must fail strict decoding")
	var unknown := valid.duplicate(true); unknown.extra = true
	_expect(ChatLive.decode_event(Protocol.KIND_CHAT_EVENT, JSON.stringify(unknown).to_utf8_buffer()) == null, "unknown variant fields must fail closed")

	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var chat := ChatLive.new(Callable(self, "_rpc"), 4)
	_expect(not chat.has_method("_get_or_create"), "no callable helper may return mutable authority state")
	_expect(chat.has_signal("live_event_pending"), "live durable receipt must expose a typed pending-apply boundary")
	_expect(not chat.has_method("confirm_history_page_applied"), "history confirmation must live on an opaque one-shot application")
	chat.join(ChatLive.direct_target("bob"), Callable())
	rpc_callbacks[0].call('{"channel_id":"revoked","watermark_event_id":4}'.to_utf8_buffer())
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, '{"version":1,"type":"access.revoked","channel_id":"revoked","presence":{"presence_id":"p","user_id":"u"}}'.to_utf8_buffer())
	chat.on_disconnected()
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, '{"version":1,"type":"message.create","channel_id":"revoked","event_id":5,"message":{"id":1,"sender":"a","content":"x","created_at_unix_ms":1,"updated_at_unix_ms":1,"revision":1,"last_event_id":5,"deleted":false}}'.to_utf8_buffer())
	_expect(chat.joined_channels().is_empty(), "revocation tombstone must survive disconnect and later durable events")
	chat.join(ChatLive.direct_target("bob"), Callable())
	rpc_callbacks.back().call('{"channel_id":"revoked","watermark_event_id":4}'.to_utf8_buffer())
	_expect(chat.is_current("revoked"), "only a fresh correlated typed join may clear a revocation tombstone")
	var pending: Array[ChatLive.LiveEventApplication] = []
	var applied_events: Array[String] = []
	chat.live_event_pending.connect(func(application: ChatLive.LiveEventApplication) -> void: pending.append(application))
	chat.chat_event.connect(func(event: ChatLive.ChatEvent) -> void: applied_events.append(event.type))
	var live := '{"version":1,"type":"message.create","channel_id":"revoked","event_id":5,"message":{"id":1,"sender":"a","content":"x","created_at_unix_ms":1,"updated_at_unix_ms":1,"revision":1,"last_event_id":5,"deleted":false}}'.to_utf8_buffer()
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, live)
	_expect(pending.size() == 1 and pending[0].apply(func(_event: ChatLive.MessageEvent) -> bool: return false) == false, "failed live application must not consume the durable event")
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, live)
	_expect(pending.size() == 2 and pending[1]._completion.call(func(_event: ChatLive.MessageEvent) -> bool: return true), "direct completion invocation must be identical to live apply")
	_expect(not pending[1].apply(func(_event: ChatLive.MessageEvent) -> bool: return true), "direct completion invocation must consume exactly-once authority")
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, live)
	_expect(pending.size() == 2 and applied_events.count("message.create") == 1, "successful live apply alone advances dedup state")
	chat.on_disconnected()
	chat.handle_envelope(Protocol.KIND_CHAT_EVENT, '{"version":1,"type":"message.update","channel_id":"revoked","event_id":6,"message":{"id":1,"sender":"a","content":"y","created_at_unix_ms":1,"updated_at_unix_ms":2,"revision":2,"last_event_id":6,"deleted":false}}'.to_utf8_buffer())
	_expect(pending.size() == 2, "disconnect must forbid new live application authority until correlated rejoin")
	_expect(chat.get("_channels") == null, "authority state must not be externally mutable")
	_expect(chat.get("_call") == null and not chat.has_method("_call"), "generic RPC forwarding must not expose ACK construction")

	# Reconciliation and rejoin are singleflight: duplicate starts preserve the
	# existing generation/callback rather than cancelling it.
	rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var sf := ChatLive.new(Callable(self, "_rpc"), 2)
	sf.join(ChatLive.direct_target("bob"), Callable())
	rpc_callbacks[0].call('{"channel_id":"sf","watermark_event_id":1}'.to_utf8_buffer())
	sf.on_disconnected(); rpc_methods.clear(); rpc_payloads.clear(); rpc_callbacks.clear()
	var first_rejoin := sf.rejoin_tracked_channels(Callable())
	var second_rejoin := sf.rejoin_tracked_channels(Callable())
	_expect(first_rejoin.size() == 1 and second_rejoin.size() == 1 and is_same(first_rejoin[0], second_rejoin[0]) and rpc_callbacks.size() == 1, "rejoin must return the existing opaque singleflight handle")
	var blocked_reconcile: Variant = sf.begin_reconciliation("sf", 2, Callable())
	_expect(blocked_reconcile == null and rpc_callbacks.size() == 1, "pending rejoin must exclude reconciliation")
	rpc_callbacks[0].call('{"channel_id":"sf","watermark_event_id":2}'.to_utf8_buffer())
	var first_reconcile: Variant = sf.begin_reconciliation("sf", 2, Callable())
	var second_reconcile: Variant = sf.begin_reconciliation("sf", 2, Callable())
	_expect(is_same(first_reconcile, second_reconcile) and rpc_callbacks.size() == 2, "reconciliation must preserve the existing generation")
	_expect(sf.rejoin_tracked_channels(Callable()).is_empty(), "pending reconciliation must exclude rejoin")

	# A malformed continuation consumes its response authority, clears the staged
	# candidate, and immediately restarts newest. Reusing the old callback is inert.
	rpc_callbacks[1].call(JSON.stringify({"items":[_history_message(9, 9), _history_message(8, 8)],"watermark_event_id":9}).to_utf8_buffer())
	var stale_continuation := rpc_callbacks[2]
	stale_continuation.call(JSON.stringify({"items":[_history_message(8, 8), _history_message(7, 7)],"watermark_event_id":9}).to_utf8_buffer())
	_expect(rpc_payloads.size() == 4 and not rpc_payloads[3].has("before_message_id"), "malformed continuation must transactionally restart from newest")
	var count_after_abort := rpc_payloads.size()
	stale_continuation.call('{"items":[],"watermark_event_id":9}'.to_utf8_buffer())
	_expect(rpc_payloads.size() == count_after_abort, "consumed malformed continuation callback must remain inert")

func _expect(condition: bool, message: String) -> void:
	if not condition:
		failures.append(message)
