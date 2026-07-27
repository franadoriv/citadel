# Browser-safe Citadel client for Godot 4 Web exports.
#
# Unlike CitadelClient, this class does not use CitadelClientNative or a native
# GDExtension. It preserves the client and room-helper surface over Godot's
# WebSocketPeer, which is available in browser exports. WebSocket is reliable
# only: QUIC/datagrams, transform snapshots, and native replication codecs are
# intentionally unavailable here.
class_name CitadelWebClient
extends "res://addons/citadel/client.gd"

const WebProtocol = preload("res://addons/citadel/protocol.gd")

var _peer: WebSocketPeer
var _frames: Array[Dictionary] = []
var _buffer := PackedByteArray()
var _auth_result: Dictionary = {}
var _auth_sent := false
var _auth_completed := false
var _auth_accepted := false
var _sending_auth_handshake := false
var _opened := false

func _init() -> void:
	# Do not initialize the desktop GDExtension path from CitadelClient.
	_native = null
	last_error = ""

## The browser transport has no native ABI dependency; its protocol constants
## are checked through CitadelProtocol's normal SDK parity manifest.
func check_abi_version() -> bool:
	return true

## Start a non-blocking WebSocket connection. Return OK when Godot accepted the
## request; call pump each frame and wait for is_open before authenticating.
func connect_websocket(url: String) -> Status:
	close()
	if not (url.begins_with("ws://") or url.begins_with("wss://")):
		last_error = "Citadel Web endpoint must use ws:// or wss://"
		return Status.INVALID_ARGUMENT
	last_error = ""
	_peer = WebSocketPeer.new()
	# Citadel accepts up to 16 MiB (kind plus payload) in a framed stream
	# envelope. Godot's default 64 KiB socket buffers would otherwise reject a
	# valid server frame before decode_websocket_frames can enforce that limit.
	_peer.inbound_buffer_size = WebProtocol.WEBSOCKET_MAX_BUFFERED_BYTES
	_peer.outbound_buffer_size = WebProtocol.WEBSOCKET_MAX_BUFFERED_BYTES
	var result := _peer.connect_to_url(url)
	if result != OK:
		last_error = "WebSocketPeer.connect_to_url failed: %d" % result
		_peer = null
		return Status.CONNECT
	return Status.OK

## Browser exports cannot use QUIC or the GDExtension transport.
func connect_quic(_addr: String, _server_name: String, _insecure: bool) -> Status:
	last_error = "Godot Web supports Citadel over WebSocket only"
	return Status.INVALID_ARGUMENT

## Drive Godot's non-blocking WebSocket and decode all received Citadel frames.
## Call once from _process; poll also invokes it for callers without a central
## pump loop.
func pump() -> void:
	if _peer == null:
		return
	# WebSocketPeer.poll returns void in Godot 4. Its ready state and packet
	# queue expose connection and receive errors after every non-blocking tick.
	_peer.poll()
	var state := _peer.get_ready_state()
	if state == WebSocketPeer.STATE_OPEN:
		_opened = true
	while _peer.get_available_packet_count() > 0:
		var packet := _peer.get_packet()
		if _peer.was_string_packet():
			# Match the native WebSocket client: non-binary messages do not contain
			# Citadel envelopes and are ignored rather than corrupting the stream.
			continue
		if _buffer.size() + packet.size() > WebProtocol.WEBSOCKET_MAX_BUFFERED_BYTES:
			_fail_protocol("Citadel WebSocket receive buffer exceeded the 16 MiB frame limit")
			return
		_buffer.append_array(packet)
		var decoded := WebProtocol.decode_websocket_frames(_buffer)
		var error: String = decoded["error"]
		if not error.is_empty():
			_fail_protocol(error)
			return
		_buffer = decoded["remaining"]
		for frame in decoded["frames"]:
			if frame["kind"] == WebProtocol.KIND_AUTH_RESULT:
				if not _auth_sent or _auth_completed:
					_fail_protocol("received unexpected or duplicate KIND_AUTH_RESULT")
					return
				_auth_result = WebProtocol.decode_auth_result(frame["payload"])
				if _auth_result.is_empty():
					_fail_protocol("server sent malformed KIND_AUTH_RESULT")
					return
				_auth_completed = true
				_auth_accepted = _auth_result["status"] != WebProtocol.AUTH_STATUS_REJECTED
			else:
				if _auth_sent and not _auth_completed:
					_fail_protocol("expected KIND_AUTH_RESULT as the first server auth reply, got kind %d" % frame["kind"])
					return
				_frames.append(frame)
	if state == WebSocketPeer.STATE_CLOSED and last_error.is_empty():
		var close_code := _peer.get_close_code()
		if not _opened:
			last_error = "WebSocket connection failed before opening (code %d): %s" % [close_code, _peer.get_close_reason()]
		elif close_code == -1:
			last_error = "WebSocket connection closed (code %d): %s" % [close_code, _peer.get_close_reason()]

func is_open() -> bool:
	pump()
	return _peer != null and _peer.get_ready_state() == WebSocketPeer.STATE_OPEN

func authenticate_guest(out: Dictionary) -> Status:
	return authenticate_token(PackedByteArray(), out)

func authenticate_with_token(session_token: String, out: Dictionary) -> Status:
	return authenticate_token(session_token.to_utf8_buffer(), out)

## Start the auth request once, then return AGAIN until pump receives the
## auth result. This is deliberately non-blocking for the browser main thread.
func authenticate_token(token: PackedByteArray, out: Dictionary) -> Status:
	pump()
	if not _auth_result.is_empty():
		out.merge(_auth_result, true)
		if _auth_result["status"] == WebProtocol.AUTH_STATUS_REJECTED:
			last_error = "Citadel authentication was rejected (reason %d)" % _auth_result["reason"]
		_auth_result = {}
		return Status.OK
	if _auth_completed:
		last_error = "Citadel authentication already completed for this connection"
		return Status.INVALID_ARGUMENT
	if not is_open():
		return Status.CONNECT
	if not _auth_sent:
		_sending_auth_handshake = true
		var status := send(WebProtocol.KIND_AUTH, token, true)
		_sending_auth_handshake = false
		if status != Status.OK:
			return status
		_auth_sent = true
	return Status.AGAIN

func send(kind: int, data: PackedByteArray, _reliable: bool) -> Status:
	pump()
	if kind < 0 or kind > 0xFFFF:
		last_error = "Citadel envelope kind must fit in u16"
		return Status.INVALID_ARGUMENT
	if 2 + data.size() > WebProtocol.WEBSOCKET_MAX_FRAME_BODY_BYTES:
		last_error = "Citadel WebSocket frame exceeds the 16 MiB envelope limit"
		return Status.INVALID_ARGUMENT
	if not is_open():
		return Status.CONNECT
	if (not _auth_completed or not _auth_accepted) and not _sending_auth_handshake:
		last_error = "complete the Citadel authentication handshake before sending gameplay frames"
		return Status.CONNECT
	var result := _peer.send(WebProtocol.encode_websocket_frame(kind, data), WebSocketPeer.WRITE_MODE_BINARY)
	if result != OK:
		last_error = "WebSocketPeer.send failed: %d" % result
		return Status.SEND
	return Status.OK

func poll(out: Dictionary) -> Status:
	pump()
	if not _frames.is_empty():
		var frame: Dictionary = _frames.pop_front()
		out["kind"] = frame["kind"]
		out["payload"] = frame["payload"]
		return Status.OK
	if _peer != null and _peer.get_ready_state() == WebSocketPeer.STATE_CLOSED:
		return Status.DISCONNECTED
	return Status.AGAIN

func close() -> void:
	if _peer != null:
		_peer.close()
	_peer = null
	_frames.clear()
	_buffer = PackedByteArray()
	_auth_result = {}
	_auth_sent = false
	_auth_completed = false
	_auth_accepted = false
	_sending_auth_handshake = false
	_opened = false

func _fail_protocol(message: String) -> void:
	last_error = message
	if _peer != null:
		_peer.close(1002, message)
