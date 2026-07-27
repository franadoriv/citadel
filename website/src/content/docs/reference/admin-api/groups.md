---
title: Groups (console API)
description: Console-operator reference for player groups/clans — CRUD, membership, the three-tier role ladder, and the superadmin invariant.
---

:::note[Console-admin-only surface]
Groups exist **only** as part of the operator [console API](/reference/admin-api/console)
today (`/console/v1/groups*`). There is no game-client SDK surface yet — no
Unity, Godot, Unreal (C++ or Blueprint), or Rust client method to create,
join, or administer a group from inside a game. Engine tabs will be added to
this page once that client surface lands.
:::

Citadel models player groups (clans/guilds) as a small domain service: a group
has a unique name, a free-form description, an open/closed flag, and an optional
member cap (`max_size == 0` means unlimited). Membership is a three-tier role
ladder — `member` → `admin` → `superadmin` — with the invariant that **a group
always keeps at least one `superadmin`**: the last superadmin can neither be
demoted nor kicked. Source: `src/repository/groups.rs` (role/pagination state
machine + repository contract), `src/services/groups.rs` (validate-then-delegate
service), HTTP handlers in `src/http/console_api/groups.rs`.

**Groups are durable**. They are persisted behind the standard
repository seam as a `groups` row (a database-assigned id, unique `name`,
metadata, and the founding `creator_id`) plus `group_memberships` rows
(`PRIMARY KEY (group_id, user_id)`, `ON DELETE CASCADE`), so on the Postgres and
SQLite backends groups, membership, and roles **survive a node restart**. The
default in-memory backend remains non-durable by design (it holds the same
groups in process memory and clears them on restart), which is the appropriate
behavior for tests and ephemeral local runs. The role ladder, the
last-superadmin invariant, the unique-name/member-cap rules, and list pagination
live in one pure, unit-tested place (`src/repository/groups.rs`) and are
exercised against all three backends by
`tests/groups_repository_contract.rs`. Group ids are assigned durably by the
database identity column (an in-process counter on the in-memory backend).

## Authentication

Every route below requires a console bearer token from `POST
/console/v1/login` (see [Login and roles](/reference/admin-api/console/#login-and-roles)):

```
Authorization: Bearer <token>
```

| Role | Access |
| --- | --- |
| `admin` | Read and mutate (create/update/delete, member add/promote/demote/kick). |
| `viewer` | Read-only (`GET` routes). A mutation attempt returns `403 forbidden`. |

## The `GroupRole` ladder

```rust
pub enum GroupRole {
    Member,      // "member"     — ordinary participant
    Admin,       // "admin"      — administers members (add/kick/promote/demote)
    Superadmin,  // "superadmin" — full ownership; every group keeps at least one
}
```

Serialized as the lowercase tokens `member` / `admin` / `superadmin`.

- **Promote** walks one tier up: `member` → `admin` → `superadmin`. Promoting
  an already-`superadmin` member returns `409 conflict`.
- **Demote** walks one tier down: `superadmin` → `admin` → `member`. Demoting
  an already-`member` returns `409 conflict`.
- **Superadmin invariant.** If a member is the group's **only** `superadmin`,
  demoting or kicking that member is rejected with `409 conflict`
  (`"cannot demote/kick the group's last superadmin"`). A second superadmin
  may always be freely demoted or kicked.
- **`open`/`max_size` are advisory today.** `open` is stored metadata only —
  there is no self-service join-request flow yet; every membership change
  goes through an admin-console `add_member` call regardless of the `open`
  flag. `max_size` (`0` = unlimited) **is** enforced on `add_member`.

## `GET /console/v1/groups`

Paged group summaries (no member roll — see the detail route for that).

**Auth:** bearer token, any role.

**Query parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `filter` | string | no | Case-sensitive substring match over the group name. |
| `limit` | integer | no | Page size. Default `50`, capped at `200`. |
| `offset` | integer | no | Number of matching groups to skip. Default `0`. |

**Response `200 OK`**

```json
{
  "items": [
    {
      "id": 1,
      "name": "raiders",
      "description": "a test group",
      "open": true,
      "max_size": 0,
      "member_count": 1,
      "created_at_unix_ms": 1751792000000
    }
  ],
  "total": 1
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `items[].id` | integer | Server-assigned group id. |
| `items[].name` | string | Unique group name. |
| `items[].description` | string | Free-form description. |
| `items[].open` | boolean | Advisory open/closed flag (see above). |
| `items[].max_size` | integer | Member cap; `0` = unlimited. |
| `items[].member_count` | integer | Current member count. |
| `items[].created_at_unix_ms` | integer | Creation time (Unix milliseconds). |
| `total` | integer | Total groups matching `filter`, before paging. |

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |

**Example**

```bash
curl -s "http://127.0.0.1:7350/console/v1/groups?filter=raid&limit=50" \
  -H "Authorization: Bearer $TOKEN"
```

## `POST /console/v1/groups`

Create a group. The creator becomes its founding `superadmin`.

**Auth:** bearer token, `admin` only. A `viewer` gets `403 forbidden`.

**Request body**

```json
{
  "name": "raiders",
  "description": "PvE guild",
  "open": true,
  "max_size": 50,
  "creator_user_id": "u-1"
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `name` | string | yes | Unique, non-blank group name. `409` on a duplicate. |
| `description` | string | no | Free-form description. Default `""`. |
| `open` | boolean | no | Advisory open/closed flag. Default `true`. |
| `max_size` | integer | no | Member cap; `0` = unlimited. Default `0`. |
| `creator_user_id` | string | no | The founding superadmin's user id. Default: the operator's own username. |

Unknown fields are rejected with `400 invalid_request`.

**Response `201 Created`** — the group detail (summary + member roll,
see [`GET /console/v1/groups/{id}`](#get-consolev1groupsid) for field shapes):

```json
{
  "id": 1,
  "name": "raiders",
  "description": "PvE guild",
  "open": true,
  "max_size": 50,
  "member_count": 1,
  "created_at_unix_ms": 1751792000000,
  "members": [
    { "user_id": "u-1", "role": "superadmin", "joined_at_unix_ms": 1751792000000 }
  ]
}
```

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `400` | `invalid_request` | Blank `name`/`creator_user_id`, malformed body, or unknown field. |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller is a `viewer`. |
| `409` | `conflict` | A group with that `name` already exists. |

Audited as `groups.create`.

**Example**

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/groups \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"raiders","description":"PvE guild","max_size":50}'
```

## `GET /console/v1/groups/{id}`

One group plus its full member roll, in join order.

**Auth:** bearer token, any role.

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | integer | yes | The group id. |

**Response `200 OK`**

```json
{
  "id": 1,
  "name": "raiders",
  "description": "PvE guild",
  "open": true,
  "max_size": 50,
  "member_count": 2,
  "created_at_unix_ms": 1751792000000,
  "members": [
    { "user_id": "u-1", "role": "superadmin", "joined_at_unix_ms": 1751792000000 },
    { "user_id": "u-2", "role": "member", "joined_at_unix_ms": 1751792005000 }
  ]
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `id`, `name`, `description`, `open`, `max_size`, `member_count`, `created_at_unix_ms` | — | Same as the listing row. |
| `members[].user_id` | string | Member's account id. |
| `members[].role` | string | `member`, `admin`, or `superadmin`. |
| `members[].joined_at_unix_ms` | integer | When the member joined/was added (Unix milliseconds). |

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `404` | `not_found` | No group with that id. |

**Example**

```bash
curl -s http://127.0.0.1:7350/console/v1/groups/1 \
  -H "Authorization: Bearer $TOKEN"
```

## `PUT /console/v1/groups/{id}`

Patch `description`/`open`/`max_size`. Each field is an optional partial
update; an absent field leaves the current value unchanged. `name` cannot be
changed through this route.

**Auth:** bearer token, `admin` only. A `viewer` gets `403 forbidden`.

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | integer | yes | The group id. |

**Request body**

```json
{ "description": "new description", "open": false }
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `description` | string | no | Replacement description. Omit to leave unchanged. |
| `open` | boolean | no | Replacement open/closed flag. Omit to leave unchanged. |
| `max_size` | integer | no | Replacement member cap. Omit to leave unchanged. |

**Response `200 OK`** — the updated group detail (same shape as
[`GET /console/v1/groups/{id}`](#get-consolev1groupsid)).

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed body or unknown field. |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller is a `viewer`. |
| `404` | `not_found` | No group with that id. |

Audited as `groups.update`.

**Example**

```bash
curl -s -X PUT http://127.0.0.1:7350/console/v1/groups/1 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"description":"new description","open":false}'
```

## `DELETE /console/v1/groups/{id}`

Delete the group outright, including its membership.

**Auth:** bearer token, `admin` only. A `viewer` gets `403 forbidden`.

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | integer | yes | The group id. |

**Response:** `204 No Content` (no body).

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller is a `viewer`. |
| `404` | `not_found` | No group with that id. |

Audited as `groups.delete`.

**Example**

```bash
curl -s -X DELETE http://127.0.0.1:7350/console/v1/groups/1 \
  -H "Authorization: Bearer $TOKEN"
```

## `POST /console/v1/groups/{id}/members`

Add a user as a `member`.

**Auth:** bearer token, `admin` only. A `viewer` gets `403 forbidden`.

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | integer | yes | The group id. |

**Request body**

```json
{ "user_id": "u-2" }
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `user_id` | string | yes | The account to add as a `member`. |

**Response `200 OK`** — the updated group detail (same shape as
[`GET /console/v1/groups/{id}`](#get-consolev1groupsid)).

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed body or unknown field. |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller is a `viewer`. |
| `404` | `not_found` | No group with that id. |
| `409` | `conflict` | The user is already a member, or the group is at `max_size`. |

Audited as `groups.member.add`.

**Example**

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/groups/1/members \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"u-2"}'
```

## `POST /console/v1/groups/{id}/members/{user_id}/promote`

Promote a member one tier: `member` → `admin` → `superadmin`.

**Auth:** bearer token, `admin` only. A `viewer` gets `403 forbidden`.

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | integer | yes | The group id. |
| `user_id` | string | yes | The member to promote. |

**Request body:** none.

**Response `200 OK`** — the updated group detail.

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller is a `viewer`. |
| `404` | `not_found` | No such group, or `user_id` is not a member. |
| `409` | `conflict` | Member already holds the highest role (`superadmin`). |

Audited as `groups.member.promote`.

**Example**

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/groups/1/members/u-2/promote \
  -H "Authorization: Bearer $TOKEN"
```

## `POST /console/v1/groups/{id}/members/{user_id}/demote`

Demote a member one tier: `superadmin` → `admin` → `member`.

**Auth:** bearer token, `admin` only. A `viewer` gets `403 forbidden`.

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | integer | yes | The group id. |
| `user_id` | string | yes | The member to demote. |

**Request body:** none.

**Response `200 OK`** — the updated group detail.

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller is a `viewer`. |
| `404` | `not_found` | No such group, or `user_id` is not a member. |
| `409` | `conflict` | Member already holds the lowest role (`member`), **or** the member is the group's last `superadmin`. |

Audited as `groups.member.demote`.

**Example**

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/groups/1/members/u-2/demote \
  -H "Authorization: Bearer $TOKEN"
```

## `POST /console/v1/groups/{id}/members/{user_id}/kick`

Remove a member outright.

**Auth:** bearer token, `admin` only. A `viewer` gets `403 forbidden`.

**Path parameters**

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | integer | yes | The group id. |
| `user_id` | string | yes | The member to remove. |

**Request body:** none.

**Response `200 OK`** — the updated group detail.

**Errors**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller is a `viewer`. |
| `404` | `not_found` | No such group, or `user_id` is not a member. |
| `409` | `conflict` | The member is the group's last `superadmin`. |

Audited as `groups.member.kick`.

**Example**

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/groups/1/members/u-2/kick \
  -H "Authorization: Bearer $TOKEN"
```

## Errors (shared shape)

Every error uses the console API's shared JSON error body:

```json
{ "code": "conflict", "message": "cannot kick the group's last superadmin" }
```

See the [console API's error table](/reference/admin-api/console/#errors) for the
full status/code list.

## Test coverage

The full console membership lifecycle — create, list/filter, detail,
add/promote/demote/kick, the last-superadmin guard, update, delete, the
viewer-`403` boundary, and the audit trail — is covered end-to-end by
`tests/console_groups.rs`. Role-ladder mechanics and store invariants
(uniqueness, max-size, promote/demote bounds, last-superadmin protection) are
unit-tested in `src/repository/groups.rs`, and the durable persistence contract
(all of the above, plus round-trip durability) runs against the in-memory,
SQLite, and Postgres backends in `tests/groups_repository_contract.rs`.

## Known limitations and gaps

- **In-memory backend is non-durable (by design).** On the default in-memory
  backend, groups and membership live in process memory and a node restart
  clears them. Run with a `[database]` URL (Postgres or SQLite) for durable
  groups — the same groups, membership, and roles then survive a restart.
- **`open` has no join-request flow yet.** The flag round-trips through
  create/list/detail responses, but there is no self-service join path — every
  membership change is an admin-console `add_member` call.
- **No game-client surface.** Players cannot create, join, or administer a
  group from a running game client — only a console operator can.
