extends SceneTree

const Protocol = preload("res://addons/citadel/protocol.gd")
const Client = preload("res://addons/citadel/client.gd")
const WebClient = preload("res://addons/citadel/web_client.gd")
const TIMEOUT_MS := 5_000


func _init() -> void:
	var failure := _run_transport_fixture()
	if failure.is_empty():
		print("Citadel Godot WebSocket integration test passed")
		quit(0)
	else:
		push_error(failure)
		quit(1)


func _run_transport_fixture() -> String:
	var client := WebClient.new()
	var url := _fixture_url()
	if client.connect_websocket(url) != Client.Status.OK:
		return "connect_websocket rejected fixture URL: %s" % client.last_error
	var deadline := Time.get_ticks_msec() + TIMEOUT_MS
	while not client.is_open():
		if Time.get_ticks_msec() >= deadline:
			return "WebSocket did not open: %s" % client.last_error
		OS.delay_msec(10)

	var auth := {}
	while true:
		var auth_status := client.authenticate_guest(auth)
		if auth_status == Client.Status.OK:
			break
		if auth_status != Client.Status.AGAIN:
			return "guest authentication failed with %d: %s" % [auth_status, client.last_error]
		if Time.get_ticks_msec() >= deadline:
			return "guest authentication timed out: %s" % client.last_error
		OS.delay_msec(10)
	if auth.get("status") != Protocol.AUTH_STATUS_GUEST:
		return "fixture expected a guest auth result, got %s" % auth

	# The browser transport has no unreliable datagrams, but accepts the native
	# compatibility flag and sends this envelope reliably over WebSocket.
	var position := Protocol.encode_position(1.25, -2.5)
	if client.send(Protocol.KIND_POSITION, position, false) != Client.Status.OK:
		return "reliable WebSocket fallback send failed: %s" % client.last_error

	var saw_peer_position := false
	var saw_notification := false
	while Time.get_ticks_msec() < deadline:
		var envelope := {}
		var poll_status := client.poll(envelope)
		if poll_status == Client.Status.OK:
			if envelope["kind"] == Protocol.KIND_PEER_POSITION:
				var peer := Protocol.decode_peer_position(envelope["payload"])
				if peer.get("sender_id") != 42 or not is_equal_approx(peer.get("x", 0.0), 1.25) or not is_equal_approx(peer.get("y", 0.0), -2.5):
					return "relayed peer position did not preserve Citadel's wire values"
				saw_peer_position = true
			elif envelope["kind"] == Protocol.KIND_NOTIFICATION:
				if envelope["payload"] != PackedByteArray([0x6E, 0x6F, 0x74, 0x69, 0x63, 0x65]):
					return "coalesced notification payload did not round-trip"
				saw_notification = true
			if saw_peer_position and saw_notification:
				client.close()
				return ""
		elif poll_status == Client.Status.DISCONNECTED:
			return "server disconnected before both relayed envelopes arrived: %s" % client.last_error
		OS.delay_msec(10)
	return "timed out waiting for the relayed envelopes: %s" % client.last_error


func _fixture_url() -> String:
	for argument in OS.get_cmdline_user_args():
		if argument.begins_with("--url="):
			return argument.trim_prefix("--url=")
	return "ws://127.0.0.1:7352/"
