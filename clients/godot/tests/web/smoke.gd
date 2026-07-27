extends Node

const Client = preload("res://addons/citadel/client.gd")
const Protocol = preload("res://addons/citadel/protocol.gd")

const E2E_QUERY_PARAMETER := "citadel_ws"
const E2E_TIMEOUT_MS := 12_000

@onready var _status: Label = $Status


# This is the runnable application bundled in the published Web SDK ZIP. With
# no query parameter it remains a harmless visual export smoke. Supplying
# `?citadel_ws=ws://host:port/` turns it into a real browser-to-Citadel proof:
# two browser WebSocket clients guest-authenticate, one sends a position and the
# other must receive Citadel's relayed KIND_PEER_POSITION before this app reports
# success to both the page and the visible label.
func _ready() -> void:
	var client := CitadelWebClient.new()
	assert(client.check_abi_version())
	assert(client.connect_quic("127.0.0.1:7351", "localhost", true) == Client.Status.INVALID_ARGUMENT)
	_set_status("Citadel Godot Web SDK is ready. Add ?citadel_ws=wss://… to verify a live server.")
	_publish_browser_result("ready")
	if not OS.has_feature("web"):
		return
	var endpoint := _endpoint_from_browser()
	if not endpoint.is_empty():
		run_real_citadel_e2e(endpoint)


func run_real_citadel_e2e(endpoint: String) -> void:
	_set_status("Connecting two browser clients to Citadel…")
	_publish_browser_result("running")
	var sender := CitadelWebClient.new()
	var receiver := CitadelWebClient.new()
	if sender.connect_websocket(endpoint) != Client.Status.OK:
		_fail("sender connection request failed: %s" % sender.last_error)
		return
	if receiver.connect_websocket(endpoint) != Client.Status.OK:
		sender.close()
		_fail("receiver connection request failed: %s" % receiver.last_error)
		return
	var sender_open := await _wait_for_open(sender, "sender")
	if not sender_open:
		sender.close()
		receiver.close()
		return
	var receiver_open := await _wait_for_open(receiver, "receiver")
	if not receiver_open:
		sender.close()
		receiver.close()
		return
	var sender_authenticated := await _authenticate_guest(sender, "sender")
	if not sender_authenticated:
		sender.close()
		receiver.close()
		return
	var receiver_authenticated := await _authenticate_guest(receiver, "receiver")
	if not receiver_authenticated:
		sender.close()
		receiver.close()
		return

	var position := Protocol.encode_position(12.5, -3.25)
	if sender.send(Protocol.KIND_POSITION, position, true) != Client.Status.OK:
		sender.close()
		receiver.close()
		_fail("sender could not send a reliable position: %s" % sender.last_error)
		return

	var deadline := Time.get_ticks_msec() + E2E_TIMEOUT_MS
	while Time.get_ticks_msec() < deadline:
		var envelope := {}
		var status := receiver.poll(envelope)
		if status == Client.Status.OK:
			if envelope["kind"] != Protocol.KIND_PEER_POSITION:
				continue
			var peer_position := Protocol.decode_peer_position(envelope["payload"])
			if peer_position.is_empty():
				sender.close()
				receiver.close()
				_fail("Citadel returned a malformed peer position")
				return
			if not is_equal_approx(float(peer_position["x"]), 12.5) or not is_equal_approx(float(peer_position["y"]), -3.25):
				sender.close()
				receiver.close()
				_fail("Citadel relayed unexpected position coordinates")
				return
			sender.close()
			receiver.close()
			_set_status("Citadel browser verification passed: guest auth, reliable relay and disconnect succeeded.")
			_publish_browser_result("pass")
			return
		if status == Client.Status.DISCONNECTED:
			sender.close()
			receiver.close()
			_fail("receiver disconnected before Citadel relayed the position: %s" % receiver.last_error)
			return
		await get_tree().process_frame
	sender.close()
	receiver.close()
	_fail("timed out waiting for a real Citadel relay")


func _wait_for_open(client: CitadelWebClient, label: String) -> bool:
	var deadline := Time.get_ticks_msec() + E2E_TIMEOUT_MS
	while Time.get_ticks_msec() < deadline:
		if client.is_open():
			return true
		if not client.last_error.is_empty():
			_fail("%s did not open: %s" % [label, client.last_error])
			return false
		await get_tree().process_frame
	_fail("%s did not open before the deadline" % label)
	return false


func _authenticate_guest(client: CitadelWebClient, label: String) -> bool:
	var deadline := Time.get_ticks_msec() + E2E_TIMEOUT_MS
	var auth := {}
	while Time.get_ticks_msec() < deadline:
		var status := client.authenticate_guest(auth)
		if status == Client.Status.OK:
			if int(auth.get("status", -1)) != Protocol.AUTH_STATUS_GUEST:
				_fail("%s did not receive Citadel's guest auth result" % label)
				return false
			return true
		if status != Client.Status.AGAIN and status != Client.Status.CONNECT:
			_fail("%s guest authentication failed: %s" % [label, client.last_error])
			return false
		await get_tree().process_frame
	_fail("%s did not receive a Citadel auth response before the deadline" % label)
	return false


func _endpoint_from_browser() -> String:
	var value := JavaScriptBridge.eval("new URLSearchParams(window.location.search).get('%s') || ''" % E2E_QUERY_PARAMETER, true)
	return value if value is String else ""


func _set_status(message: String) -> void:
	_status.text = message
	print(message)


func _publish_browser_result(result: String) -> void:
	# The browser E2E runner reads this DOM marker after loading the actual Godot
	# WebAssembly app. Keep `result` internal/static; it is interpolated into JS.
	var marker := result.to_lower()
	JavaScriptBridge.eval("document.documentElement.setAttribute('data-citadel-e2e', '%s'); document.title = 'CITADEL_WEB_E2E_%s';" % [marker, marker.to_upper()], true)


func _fail(message: String) -> void:
	_set_status("Citadel browser verification failed: %s" % message)
	_publish_browser_result("fail")
	push_error(message)
