# Typed player account/session HTTP client for Godot 4.
#
# Add one CitadelHttpClient node to the scene tree, set base_url, and connect to
# `completed`. This node deliberately does not retain bearer or refresh secrets:
# games pass them to each call and atomically persist a replacement pair after a
# successful refresh.
class_name CitadelHttpClient
extends HTTPRequest

signal completed(ok: bool, status: int, code: String, message: String, payload: Dictionary)

@export var base_url := ""
var _active := false

func _ready() -> void:
	request_completed.connect(_on_request_completed)

## GET /v1/account. Result payload is a sanitized public profile.
func get_account(access_token: String) -> Error:
	return _start("GET", "/v1/account", access_token)

## PATCH /v1/account. `patch` may contain username and/or display_name; assign
## `null` to display_name to clear it.
func update_account(access_token: String, patch: Dictionary) -> Error:
	return _start("PATCH", "/v1/account", access_token, patch)

## POST /v1/users/lookup. `query` uses exact user_ids and/or usernames only;
## this method is not a public player-directory search.
func lookup_users(access_token: String, query: Dictionary) -> Error:
	return _start("POST", "/v1/users/lookup", access_token, query)

## POST /v1/auth/email. Set `create` for registration; never log `password`.
## The successful payload contains the caller-owned session token pair.
func authenticate_email(email: String, password: String, create := false, username := "") -> Error:
	var body := {"email": email, "password": password, "create": create}
	if not username.is_empty():
		body["username"] = username
	return _start("POST", "/v1/auth/email", "", body)

## POST /v1/session/refresh. No bearer header is ever sent for refresh.
func refresh_session(refresh_token: String) -> Error:
	return _start("POST", "/v1/session/refresh", "", {"refresh_token": refresh_token})

## POST /v1/session/logout. Supply either secret, or both for the same session.
## A successful retry is idempotent and emits a 204 success with an empty payload.
func logout_session(access_token := "", refresh_token := "") -> Error:
	var body: Variant = null if refresh_token.is_empty() else {"refresh_token": refresh_token}
	return _start("POST", "/v1/session/logout", access_token, body)

func _start(method: String, path: String, access_token: String, body: Variant = null) -> Error:
	if _active:
		return ERR_BUSY
	if base_url.strip_edges().is_empty():
		return ERR_INVALID_PARAMETER
	var headers := PackedStringArray(["Accept: application/json"])
	if not access_token.is_empty():
		headers.append("Authorization: Bearer " + access_token)
	var content := ""
	if body != null:
		headers.append("Content-Type: application/json")
		content = JSON.stringify(body)
	_active = true
	var error := request(base_url.trim_suffix("/") + path, headers, HTTPClient.METHOD_GET if method == "GET" else _method(method), content)
	if error != OK:
		_active = false
	return error

func _method(method: String):
	match method:
		"POST": return HTTPClient.METHOD_POST
		"PATCH": return HTTPClient.METHOD_PATCH
		_: return HTTPClient.METHOD_GET

func _on_request_completed(result: int, response_code: int, _headers: PackedStringArray, body: PackedByteArray) -> void:
	_active = false
	if result != HTTPRequest.RESULT_SUCCESS:
		completed.emit(false, 0, "transport_error", "request failed", {})
		return
	if response_code == 204:
		completed.emit(true, response_code, "", "", {})
		return
	var decoded: Variant = JSON.parse_string(body.get_string_from_utf8())
	if response_code < 200 or response_code >= 300:
		var error_body: Dictionary = decoded if decoded is Dictionary else {}
		completed.emit(false, response_code, str(error_body.get("code", "http_error")), str(error_body.get("message", "request failed")), {})
		return
	if not decoded is Dictionary:
		completed.emit(false, response_code, "invalid_response", "server returned an invalid response", {})
		return
	completed.emit(true, response_code, "", "", decoded)
