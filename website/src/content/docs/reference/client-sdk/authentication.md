---
title: HTTP authentication API
description: Device, custom-id, and email/password authentication endpoints — register or log in over HTTP and receive a session token.
---

import { Tabs, TabItem } from '@astrojs/starlight/components';

Citadel exposes device authentication (a device id), custom-id, and email/password sign-in. Every
successful request receives the same opaque session-token pair; the password is
only accepted by the HTTP login boundary and is never a realtime credential.

Both endpoints run through the server's persistent, transactional authentication
and session services, so account creation is a single all-or-nothing operation
on whichever backend the node runs (in-memory by default, or PostgreSQL when
configured — see [Configuration](/reference/operations/configuration/)).

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/auth/device` | Authenticate (or register) with a device id |
| `POST` | `/v1/auth/custom` | Authenticate (or register) with a custom id |
| `POST` | `/v1/auth/email` | Authenticate (or register) with an email and password |
| `GET` / `PATCH` | `/v1/account` | Read or change the caller's basic profile |
| `POST` | `/v1/users/lookup` | Exact, authenticated lookup of known players |
| `POST` | `/v1/session/refresh` | Rotate a refresh token into a new token pair |
| `POST` | `/v1/session/logout` | Revoke one session safely and idempotently |

Both are served on the node's HTTP listener (the same address as `/health` and
`/status`, default `127.0.0.1:7350`).

## Lifecycle SDK availability

Every route above has a first-class binding in Unreal C++ and Blueprint, Unity
C#, Godot GDScript, Rust, and JavaScript/Web. Each binding keeps tokens
caller-owned so games can use their platform's secure storage and atomically
replace both values after a refresh.

## Request

`Content-Type: application/json`. The device/custom endpoints take this body shape:

```json
{
  "id": "device-or-custom-id",
  "create": false,
  "username": "optional-on-create",
  "display_name": "optional",
  "metadata": { "optional": "json object" }
}
```

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `id` | string | yes | The device or custom id. 1–128 bytes, no control characters. |
| `create` | bool | no (default `false`) | Create an account if the id is unknown. Must be `true` to register. |
| `username` | string | on create | Required when `create` is `true`. 1–128 bytes, no control characters. |
| `display_name` | string | no | Human-facing name. |
| `metadata` | object | no | Arbitrary JSON **object** attached to a new account. |

Notes:

- `create` defaults to **`false`**. Registration is an explicit opt-in; a client
  that only wants to log in can omit it.
- Unknown fields are **rejected** (`400`), and the body is size-limited, so a
  typo'd field name fails fast rather than being silently ignored.

### `POST /v1/auth/email`

Email/password registration and login uses a separate strict body shape:

```json
{
  "email": "player@example.com",
  "password": "correct horse battery staple",
  "create": false,
  "username": "optional-on-create"
}
```

`email` is trimmed and ASCII-case-normalized before lookup. `password` is
8–1024 bytes, is not normalized, and rejects control characters. With
`create:true`, Citadel stores an Argon2id PHC verifier with a fresh random salt,
never the plaintext password. Email verification, recovery/change-password,
and linking are not shipped yet.

## Success response

`201 Created` when a new account was registered, `200 OK` for a returning
account:

```json
{
  "token": "access-token",
  "refresh_token": "refresh-token",
  "user_id": "user-...",
  "username": "player",
  "created": true
}
```

| Field | Notes |
| --- | --- |
| `token` | The access token. Send it as the bearer credential on future requests. |
| `refresh_token` | Present only when the session is refreshable. |
| `user_id` | Stable account id. |
| `username` | The account's username. |
| `created` | `true` if this request registered a new account. |

Repeating a request with the **same id** returns the **same `user_id`** — the
call is idempotent for an existing account.

## Use the token on a realtime connection

HTTP authentication issues the account session token; it does not by itself
admit a QUIC/WebSocket connection to the realtime gateway. After connecting,
present the token as the first realtime envelope with the SDK helper for your
engine:

- C ABI / Unity / Unreal / Godot wrappers: call the realtime authenticate helper
  with the token bytes, or call the guest helper for an empty guest handshake.
- Rust `citadel-client`: call `connect_with_token` / `authenticate(Some(token))`
  for WebSocket, or send `KIND_AUTH` as the first QUIC reliable frame.
- JavaScript: call `handshakeToken(token)` or `handshakeGuest`.

The server answers with `KIND_AUTH_RESULT`. Only after that reply does the
gateway register the session, fire `on_join`, and route gameplay traffic.

## Realtime authentication by SDK

<Tabs syncKey="engine">
<TabItem label="Unreal C++">

```cpp
// AuthenticateRealtimeWithSessionToken presents the HTTP access token first.
Client->AuthenticateRealtimeWithSessionToken(Token);
```
</TabItem>
<TabItem label="Unreal Blueprint">

1. Keep the HTTP response `token` string.
2. On **Citadel Client Subsystem**, call **Authenticate Realtime With Session Token**.
3. Wait for the authentication-complete event before sending gameplay frames.
</TabItem>
<TabItem label="Unity C#">

```csharp
client.AuthenticateWithToken(token);
```
</TabItem>
<TabItem label="Godot GDScript">

```gdscript
var auth := {}
var status := client.authenticate_with_token(token, auth)
```
</TabItem>
<TabItem label="Rust (citadel-client)">

```rust
client.authenticate(Some(&token)).await?;
```
</TabItem>
<TabItem label="JavaScript">

```js
const client = await CitadelClient.connect("ws://127.0.0.1:7352/");
// Dedicated helpers frame KIND_AUTH and await KIND_AUTH_RESULT for you.
await client.handshakeToken(token); // or client.handshakeGuest() for a guest session
```
</TabItem>
</Tabs>

:::note[Token format]
Session tokens are currently opaque, unsigned reference tokens. Treat both
access and refresh tokens as opaque strings: store them securely, never parse
them, and never log them.
:::

## Errors

Failures return a sanitized JSON body and an appropriate status:

```json
{ "code": "authentication_failed", "message": "authentication failed" }
```

| Status | `code` | When |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed/oversized body, unknown field, or an invalid `id`/`username`/`metadata`. |
| `401` | `authentication_failed` | Unknown id with `create:false`, or an account that cannot authenticate. |
| `409` | `conflict` | A uniqueness conflict (for example a username already taken). |
| `429` | `rate_limited` | Authentication admission limit reached. The response carries `Retry-After` in whole seconds; it never identifies which source or email limiter matched. |
| `500` | `internal_error` | Unexpected server error. The body is always generic. |

Every credential failure returns the **same** `401 authentication_failed`: the
API never reveals whether an id/email exists or whether its password was wrong.
Error bodies never include a token, `user_id`, password, or server internals.

### Authentication abuse controls

Citadel applies durable fixed-window admission limits before credential work.
The defaults are 30 requests per source address per minute, 10 email attempts
per normalized email per 15 minutes, and 10 registrations per source address
per hour. Counter keys are hashes rather than raw email or address values and
are shared by nodes using the same SQLite, PostgreSQL, or CockroachDB backend.

Citadel uses the direct TCP peer address and intentionally ignores
`X-Forwarded-For`; do not place it behind a proxy that rewrites source identity
until a trusted-proxy configuration is available. Operators may tune the values
under [`[authentication.limits]`](/reference/operations/configuration/#authenticationlimits).

## Examples

Register a new device account:

```bash
curl -sS -X POST http://127.0.0.1:7350/v1/auth/device \
  -H 'Content-Type: application/json' \
  -d '{"id":"device-abc","create":true,"username":"player1"}'
# 201 Created
# {"token":"...","refresh_token":"...","user_id":"user-...","username":"player1","created":true}
```

Log in again with the same id (idempotent — same `user_id`, `created:false`):

```bash
curl -sS -X POST http://127.0.0.1:7350/v1/auth/device \
  -H 'Content-Type: application/json' \
  -d '{"id":"device-abc"}'
# 200 OK
```

Custom-id authentication is identical, at `/v1/auth/custom`.

Register an email/password account:

```bash
curl -sS -X POST http://127.0.0.1:7350/v1/auth/email \
  -H 'Content-Type: application/json' \
  -d '{"email":"player@example.com","password":"correct horse battery staple","create":true,"username":"player1"}'
# 201 Created
```

### Email/password SDK method

All SDK methods below send `POST /v1/auth/email` without a bearer header.
`create` is `false` by default; set it only for registration. Each returns (or
emits) the same session-token payload shown above. Never log the request
object, especially its password.

<Tabs syncKey="engine">
<TabItem label="Unreal C++">

```cpp
// Bind OnAuthenticated before calling. bCreate registers; false signs in.
Subsystem->AuthenticateEmail(
    "http://127.0.0.1:7350", "player@example.com",
    "correct horse battery staple", true, "player1");
```
</TabItem>
<TabItem label="Unreal Blueprint">

1. On **Citadel Client Subsystem**, call **Authenticate Email**.
2. Set **Base Url**, **Email**, **Password**, **Create** (`true` to register),
   and **Username** when registering.
3. Bind **On Authenticated** and securely persist the returned token pair.
</TabItem>
<TabItem label="Unity C#">

```csharp
var http = new CitadelHttpClient("http://127.0.0.1:7350");
var session = await http.AuthenticateEmailAsync(new EmailAuthenticationRequest {
    Email = "player@example.com", Password = "correct horse battery staple",
    Create = true, Username = "player1",
});
```
</TabItem>
<TabItem label="Godot GDScript">

```gdscript
# Connect `completed` once; its payload is the session-token pair on success.
http.completed.connect(func(ok, _status, _code, _message, payload):
	if ok: secure_store_tokens(payload)
)
http.authenticate_email("player@example.com", "correct horse battery staple", true, "player1")
```
</TabItem>
<TabItem label="Rust (citadel-client)">

```rust
use citadel_client::{CitadelHttpClient, EmailAuthenticationRequest};

let http = CitadelHttpClient::new("http://127.0.0.1:7350")?;
let session = http.authenticate_email(&EmailAuthenticationRequest {
    email: "player@example.com".into, password: "correct horse battery staple".into,
    create: true, username: Some("player1".into),
}).await?;
```
</TabItem>
<TabItem label="JavaScript">

```js
const http = new CitadelHttpClient("http://127.0.0.1:7350");
const session = await http.authenticateEmail({
  email: "player@example.com", password: "correct horse battery staple",
  create: true, username: "player1",
});
```
</TabItem>
</Tabs>

## Player account and session lifecycle

These operations are ordinary authenticated HTTP endpoints. Every released
Citadel SDK exposes first-class lifecycle methods below; they are deliberately
separate from the realtime transport client so an HTTP refresh never mutates a
live socket behind the game's back.
The access token goes in `Authorization: Bearer <token>` except when refreshing,
which accepts only the refresh secret in the JSON body.

### `GET /v1/account`

Returns the authenticated caller's public profile.

**Parameters:** no body; a valid access bearer token is required.

**Returns:** `200 OK` with `{ "user_id", "username", "display_name?" }`.
It never includes metadata, linked credentials, account state, or timestamps.

**Errors:** `401 authentication_failed` for a missing, expired, revoked, or
invalid access token; `500 internal_error` for an unexpected server failure.

### `PATCH /v1/account`

Changes the caller's `username` and/or `display_name`; both are validated like
their registration counterparts. `display_name: null` clears the display name.
The account id, creation time, credentials, metadata, and lifecycle state are
not mutable through this endpoint.

**Parameters:** a valid access bearer plus a JSON body containing at least one
of `username` (string) or `display_name` (string or `null`). Unknown fields are
rejected.

**Returns:** `200 OK` with the sanitized profile.

**Errors:** `400 invalid_request` for an empty/invalid patch, `401
authentication_failed` for an invalid bearer, `409 conflict` if another account
owns the requested username, and `500 internal_error` for an unexpected failure.

### `POST /v1/users/lookup`

Looks up explicitly known players. This is **not search**: clients provide up to
100 exact `user_ids` and/or exact `usernames`. The endpoint requires a valid
access bearer. Unknown, disabled, and tombstoned accounts are all omitted from
the `users` array, so the response does not reveal account state.

**Parameters:** `{ "user_ids": ["..."], "usernames": ["..."] }`; at least
one value is required. Unknown fields and more than 100 total keys are rejected.

**Returns:** `200 OK` with `{ "users": [{ "user_id", "username",
"display_name?" }] }`.

**Errors:** `400 invalid_request` for malformed keys/body, `401
authentication_failed` for an invalid bearer, and `500 internal_error` for an
unexpected failure.

### `POST /v1/session/refresh`

Rotates a live refresh credential. It returns a replacement access/refresh pair
for the same account. A successfully rotated refresh token stops working
immediately; a replay, expired token, revoked session, or malformed token all
produce the same sanitized authentication failure.

**Parameters:** `{ "refresh_token": "opaque-secret" }`. Do not send an
`Authorization` header and do not log this body.

**Returns:** `200 OK` with the same token-pair shape as authentication and
`created: false`.

**Errors:** `400 invalid_request` for a malformed body, `401
authentication_failed` for every unusable refresh credential, and `500
internal_error` for an unexpected failure.

### `POST /v1/session/logout`

Revokes exactly one session and returns `204 No Content`. Supply its current
access token in the bearer header, its refresh token as
`{ "refresh_token": "opaque-secret" }`, or both. If both are supplied they
must name the same session; a mismatch is a safe no-op. A retry, an already
revoked credential, or an expired credential also returns `204`, so logout is
safe to retry and cannot reveal token/session state.

**Errors:** `400 invalid_request` for an invalid JSON body, `401
authentication_failed` for a malformed bearer/refresh secret, and `500
internal_error` for an unexpected failure.

### Refresh and securely replace the token pair

The following calls are the same operation in each released SDK. Persist the
returned pair atomically; never keep the old refresh token after success.

<Tabs syncKey="engine">
<TabItem label="Unreal C++">

```cpp
// Bind OnSessionRefreshed before calling RefreshSession. The delegate supplies
// FCitadelSessionTokenPair; persist Token and RefreshToken atomically.
Citadel->RefreshSession(BaseUrl, RefreshToken);
```
</TabItem>
<TabItem label="Unreal Blueprint">

1. Get the `CitadelClientSubsystem` and bind **On Session Refreshed**.
2. Call **Refresh Session** with the HTTP origin and stored refresh token.
3. In the delegate, atomically save **Token** and **Refresh Token**; on
   **On Player Request Failed** with `authentication_failed`, sign in again.
</TabItem>
<TabItem label="Unity C#">

```csharp
var http = new CitadelHttpClient(baseUrl);
var tokens = await http.RefreshSessionAsync(refreshToken);
// Atomically persist tokens.token and tokens.refresh_token here.
```
</TabItem>
<TabItem label="Godot GDScript">

```gdscript
var http := CitadelHttpClient.new
http.base_url = base_url
add_child(http)
http.completed.connect(func(ok, _status, code, _message, payload):
    if ok: save_tokens_atomically(payload.token, payload.refresh_token)
    elif code == "authentication_failed": show_sign_in)
http.refresh_session(refresh_token)
```
</TabItem>
<TabItem label="Rust (citadel-client)">

```rust
use citadel_client::CitadelHttpClient;

let http = CitadelHttpClient::new("https://game.example")?;
let tokens = http.refresh_session(&refresh_token).await?;
// Persist both replacement values atomically before using `tokens.token` for
// `WsClient::connect_with_token` or `authenticate`.
access_token = tokens.token;
refresh_token = tokens.refresh_token.ok_or("server did not issue a refresh token")?;
```
</TabItem>
<TabItem label="JavaScript">

```js
import { CitadelHttpClient, HttpApiError } from '@citadel/client';

const http = new CitadelHttpClient(baseUrl);
let tokens;
try {
  tokens = await http.refreshSession(refreshToken);
} catch (error) {
  if (error instanceof HttpApiError && error.code === 'authentication_failed') {
    // The old refresh token is unusable: return the player to sign-in.
  }
  throw error;
}
accessToken = tokens.token;
refreshToken = tokens.refresh_token;
```
</TabItem>
</Tabs>

### JavaScript/Web SDK lifecycle methods

`CitadelHttpClient` accepts the node's HTTP origin and an optional injected
`fetch` implementation. It does not retain credentials: pass tokens in and
atomically persist the `SessionTokenPair` returned by refresh.

| Method | Parameters | Result | Errors |
| --- | --- | --- | --- |
| `getAccount(accessToken)` | Access token | `PublicProfile` | `HttpApiError` with the sanitized HTTP status/code/message. |
| `updateAccount(accessToken, patch)` | Access token; `username?`, `display_name?` | Updated `PublicProfile` | Same as `PATCH /v1/account`. |
| `lookupUsers(accessToken, query)` | Access token; exact `user_ids?`, `usernames?` | `{ users: PublicProfile[] }` | Same as `POST /v1/users/lookup`. |
| `refreshSession(refreshToken)` | Refresh secret | `SessionTokenPair` | Same as `POST /v1/session/refresh`; no bearer header is sent. |
| `logoutSession({ accessToken?, refreshToken? })` | One or both credentials for the same session | `void` on idempotent `204` | Same as `POST /v1/session/logout`. |

### Rust SDK lifecycle methods

`citadel_client::CitadelHttpClient::new(base_url)` uses reqwest with rustls TLS
and does not retain credentials. The caller owns secure storage and must replace
the old pair atomically after `refresh_session` succeeds.

| Method | Parameters | Result | Errors |
| --- | --- | --- | --- |
| `get_account(access_token)` | `&str` access token | `PublicProfile` | `ClientError::Http { status, code, message }` with the sanitized HTTP error. |
| `update_account(access_token, &UpdateAccountRequest)` | Access token; optional `username`, optional `display_name` (`Some(None)` clears it) | Updated `PublicProfile` | Same as `PATCH /v1/account`. |
| `lookup_users(access_token, &LookupUsersRequest)` | Access token; exact `user_ids`/`usernames` vectors | `LookupUsersResponse` | Same as `POST /v1/users/lookup`. |
| `refresh_session(refresh_token)` | `&str` refresh secret | `SessionTokenPair` | Same as `POST /v1/session/refresh`; no bearer header is sent. |
| `logout_session(access_token, refresh_token)` | `Option<&str>` for one or both credentials | `` on idempotent `204` | Same as `POST /v1/session/logout`. |

### Unreal C++ and Blueprint lifecycle methods

`UCitadelClientSubsystem` exposes asynchronous Blueprint-callable methods. Bind
the matching success delegate and `OnPlayerRequestFailed`; errors preserve the
sanitized status/code/message returned by the backend.

| Method | Parameters | Result | Errors |
| --- | --- | --- | --- |
| `GetAccount(BaseUrl, AccessToken)` | HTTP origin, access token | `OnAccountReceived(FCitadelPublicProfile)` | `OnPlayerRequestFailed`. |
| `UpdateAccount(BaseUrl, AccessToken, Username, DisplayName, bClearDisplayName)` | Profile patch; clear flag serializes JSON `null` | `OnAccountUpdated(FCitadelPublicProfile)` | Same as `PATCH /v1/account`. |
| `LookupUsers(BaseUrl, AccessToken, UserIds, Usernames)` | Exact lookup lists | `OnUsersLookupReceived(TArray<FCitadelPublicProfile>)` | Same as `POST /v1/users/lookup`. |
| `RefreshSession(BaseUrl, RefreshToken)` | Refresh secret only | `OnSessionRefreshed(FCitadelSessionTokenPair)` | Same as `POST /v1/session/refresh`; sends no bearer. |
| `LogoutSession(BaseUrl, AccessToken, RefreshToken)` | One or both same-session credentials | Completion is the idempotent `204`; failures use `OnPlayerRequestFailed` | Same as `POST /v1/session/logout`. |

### Unity C# lifecycle methods

`new CitadelHttpClient(baseUrl)` returns `Task`-based operations using
`UnityWebRequest`. `CitadelHttpException` contains the sanitized HTTP status
and code; it never contains credentials or server internals.

| Method | Parameters | Result | Errors |
| --- | --- | --- | --- |
| `GetAccountAsync(accessToken)` | Access token | `Task<PublicProfile>` | `CitadelHttpException`. |
| `UpdateAccountAsync(accessToken, patch)` | `UpdateAccountRequest`; use `ClearDisplayName` for null | `Task<PublicProfile>` | Same as `PATCH /v1/account`. |
| `LookupUsersAsync(accessToken, query)` | `LookupUsersRequest` exact keys | `Task<LookupUsersResponse>` | Same as `POST /v1/users/lookup`. |
| `RefreshSessionAsync(refreshToken)` | Refresh secret | `Task<SessionTokenPair>` | Same as refresh; sends no bearer. |
| `LogoutSessionAsync(accessToken, refreshToken)` | One or both credentials | `Task` on idempotent `204` | Same as logout. |

### Godot GDScript lifecycle methods

Add `CitadelHttpClient` as a node and connect its `completed(ok, status, code,
message, payload)` signal. The result payload is a profile, `{ users }`, or a
token pair as appropriate; failures use the documented sanitized code/message.

| Method | Parameters | Result | Errors |
| --- | --- | --- | --- |
| `get_account(access_token)` | Access token | Signal payload `PublicProfile` dictionary | `completed(false, ...)`. |
| `update_account(access_token, patch)` | Dictionary with `username`/`display_name`; `null` clears | Updated profile payload | Same as PATCH. |
| `lookup_users(access_token, query)` | Exact `user_ids`/`usernames` dictionary | `{ "users": [...] }` | Same as lookup. |
| `refresh_session(refresh_token)` | Refresh secret | Token-pair payload; no bearer | Same as refresh. |
| `logout_session(access_token, refresh_token)` | One or both credentials | Empty payload on idempotent `204` | Same as logout. |

## Related

- [Sessions](/concepts/sessions/) — how sessions relate to realtime connections.
- [Manage a player session](/guides/manage-player-session/) — the ordered refresh/logout workflow.
- [Configuration](/reference/operations/configuration/) — selecting the PostgreSQL backend
  so accounts persist across restarts.
