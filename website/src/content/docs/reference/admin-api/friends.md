---
title: Friends (console API)
description: Console-operator reference for pairwise friend relationships — invite/accept, block, remove, and the FriendState machine.
---

:::note[Console-admin-only surface]
Friends management exists **only** as part of the operator [console
API](/reference/admin-api/console) today (`/console/v1/accounts/{id}/friends*`). There
is no game-client SDK surface yet — no Unity, Godot, Unreal (C++ or
Blueprint), or Rust client method to invite, accept, block, or list friends
from inside a game. Engine tabs will be added to this page once that client
surface lands.
:::

Citadel models friends as a pairwise relationship store (one entry per directed
`(user, other)` pair) attached to each account. It mirrors Nakama's four-state
model: an invite creates `invited_sent` / `invited_received` on the two sides, a
matching invite from the other side (or an explicit accept) upgrades both sides
to `friend`, and `blocked` is a one-sided state that stops the blocked side from
re-inviting. Source: `src/repository/friends.rs` (state machine + repository
contract), `src/services/friends.rs` (validate-then-delegate service), HTTP
handlers in `src/http/console_api/accounts.rs`.

**Friend relations are durable**. They are persisted behind the
standard repository seam as two directed edges per relationship in a
`friend_edges (owner_id, other_id, state, updated_unix_ms)` table, so on the
Postgres and SQLite backends relations, invites, and blocks **survive a node
restart**. The default in-memory backend remains non-durable by design (it holds
the same edges in process memory and clears them on restart), which is the
appropriate behavior for tests and ephemeral local runs. The invite→mutual /
blocked-pair state machine lives in one pure, unit-tested place
(`plan_add` in `src/repository/friends.rs`) and is exercised against all three
backends by `tests/friends_repository_contract.rs`.

## Authentication

Every route below requires a console bearer token from `POST
/console/v1/login` (see [Login and roles](/reference/admin-api/console/#login-and-roles)):

```
Authorization: Bearer <token>
```

| Role | Access |
| --- | --- |
| `admin` | Read and mutate (invite/accept, remove/unblock). |
| `viewer` | Read-only. A mutation attempt returns `403 forbidden`. |

## The `FriendState` enum

```rust
pub enum FriendState {
    InvitedSent,     // "invited_sent"     — this user sent a pending invite
    InvitedReceived,  // "invited_received" — this user received a pending invite
    Friend,           // "friend"           — mutual
    Blocked,          // "blocked"          — this user blocked the other (one-sided)
}
```

Serialized as the lowercase snake_case tokens shown above (`#[serde(rename_all
= "snake_case")]`).

**State transitions** (`FriendsService::add`, `remove`, `block` in
`src/services/friends.rs`):

- Inviting a user with no existing relation sets `invited_sent` on the
  inviter's side and `invited_received` on the invitee's side.
- Inviting a user who already sent (or already is) `invited_sent`/`friend`
  from the other side upgrades **both** sides to `friend` (a matching invite
  is how the console "accepts" on the account's behalf).
- Re-inviting an existing `friend` is a no-op success (state stays `friend`).
- `remove` deletes the relation in both directions — this is also how a block
  is lifted (removing clears the blocker's one-sided `blocked` entry).
- A relation where either side is `blocked` rejects a new invite with `409
  conflict`.
- A user cannot friend or block themselves (`user == other`) — rejected with
  `400 invalid_request`.

Blocking itself has no dedicated console route today; only `add` (invite/
accept), `list`, and `remove` are exposed over HTTP. The service method
`FriendsService::block` exists and is unit-tested, but is not yet wired to a
console endpoint — see [Gaps](#known-limitations-and-gaps).

## `GET /console/v1/accounts/{id}/friends`

Returns the addressed account's relations, ordered by the other user's id.

**Auth:** bearer token, any role (`admin` or `viewer`).

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | string | yes | The account whose friend list to read. |

**Response `200 OK`** — a JSON array of rows:

```json
[
  {
    "user_id": "u-2",
    "state": "friend",
    "updated_unix_ms": 1751792000000
  }
]
```

| Field | Type | Meaning |
| --- | --- | --- |
| `user_id` | string | The other account in the relation. |
| `state` | string | One of `invited_sent`, `invited_received`, `friend`, `blocked`. |
| `updated_unix_ms` | integer | When this relation last changed (Unix milliseconds). |

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `404` | `not_found` | `id` does not name an existing account. |

**Example**

```bash
curl -s http://127.0.0.1:7350/console/v1/accounts/u-1/friends \
  -H "Authorization: Bearer $TOKEN"
```

## `POST /console/v1/accounts/{id}/friends`

Invite another account, or complete a mutual friendship if the other side
already invited `id`. Acts **for** the addressed account (`id`), so a matching
call from the other account's id completes the friendship — this is how an
operator can "accept" an invite on a player's behalf.

**Auth:** bearer token, `admin` only. A `viewer` gets `403 forbidden`.

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | string | yes | The account acting (inviting or accepting). |

**Request body**

```json
{ "user_id": "u-2" }
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `user_id` | string | yes | The other account id. Unknown fields are rejected. |

**Response `200 OK`** — the acting account's full, updated friend list (same
shape as the `GET` response above).

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed body, unknown field, or `id == user_id`. |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller is a `viewer`. |
| `404` | `not_found` | `id` or `user_id` does not name an existing account. |
| `409` | `conflict` | Either side has blocked the other. |

Audited as `accounts.friends.add`.

**Example**

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/accounts/u-1/friends \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"u-2"}'
```

## `DELETE /console/v1/accounts/{id}/friends/{other}`

Removes the relation in both directions. This is also how an operator lifts a
block (a blocker calling this against the blocked user's id clears the
one-sided `blocked` entry).

**Auth:** bearer token, `admin` only. A `viewer` gets `403 forbidden`.

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | string | yes | One side of the relation. |
| `other` | string | yes | The other side of the relation. |

**Response:** `204 No Content` (no body). Idempotent — removing an
already-absent relation still returns `204`.

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `400` | `invalid_request` | `id == other`, or a malformed path segment. |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller is a `viewer`. |

Audited as `accounts.friends.remove`. Note this route does not check that
`id`/`other` name existing accounts before removing — it operates directly on
the relationship store.

**Example**

```bash
curl -s -X DELETE http://127.0.0.1:7350/console/v1/accounts/u-1/friends/u-2 \
  -H "Authorization: Bearer $TOKEN"
```

## Errors (shared shape)

Every error uses the console API's shared JSON error body:

```json
{ "code": "conflict", "message": "relationship is blocked" }
```

See the [console API's error table](/reference/admin-api/console/#errors) for the
full status/code list.

## Test coverage

The friends panel's invite/accept/upgrade-to-mutual, remove-both-sides, and
404-for-unknown-account behavior are covered end-to-end in
`tests/console_accounts.rs` (`wallet_and_friends_panels`). The state machine
itself — mutual upgrade, idempotent remove, one-sided block stopping
re-invites, unblock, and other-id ordering — runs against all three backends
(in-memory, SQLite always, Postgres opt-in via `DATABASE_URL`) in the shared
contract test `tests/friends_repository_contract.rs`. The pure `plan_add`
state machine and the reference in-memory repository are unit-tested in
`src/repository/friends.rs`; the self-friendship rejection is unit-tested in
`src/services/friends.rs`.

## Known limitations and gaps

- **In-memory backend is non-durable (by design).** On the default in-memory
  backend, relations live in process memory and a node restart clears them. Run
  with a `[database]` URL (Postgres or SQLite) for durable friends — the same
  relations, invites, and blocks then survive a restart.
- **No dedicated block endpoint.** `FriendsService::block` exists and is
  unit-tested, but no console HTTP route calls it today; blocking is only
  reachable programmatically, not through `/console/v1`.
- **No game-client surface.** Players cannot invite, accept, block, or list
  friends from a running game client — only a console operator can, on a
  player's behalf.
