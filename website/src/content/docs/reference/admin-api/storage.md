---
title: Storage reference
description: The collection/object storage data model and the console storage browser API (/console/v1/storage*).
---

Citadel's storage engine is a typed collection/key/owner object store — the
same domain model backing device saves, unlocks, and any other structured
per-player or system data — persisted through the node's real backend
(in-memory, SQLite, or Postgres). It is the same store the
[storage browser](/reference/admin-api/console/#storage-browser) exposes to operators.

:::note[Console-admin-only surface today]
The routes on this page are the **operator** surface under `/console/v1`. There
is no game-client SDK method for storage yet — no Unreal C++, Blueprint, Unity
C#, Godot GDScript, or Rust (`citadel-client`) accessor exists. Do not treat the
shapes below as a client contract; a client-facing storage API is future work.
:::

## Data model

| Concept | Type | Notes |
| --- | --- | --- |
| Owner | `Owner::System` \| `Owner::User(UserId)` | `System` is server/runtime-owned data with no end user; `User` belongs to one account. |
| Collection | `Collection` (validated string) | A namespace within an owner. Non-empty, ≤128 bytes, no control characters. |
| Key | `Key` (validated string) | Unique within `owner` + `collection`. Same validation as `Collection`. |
| Object identity | `ObjectId { owner, collection, key }` | The full address of one stored object. |
| Value | `StorageValue` (JSON object) | Must be a top-level JSON **object** — arrays, strings, and bare numbers are rejected. |
| Version | `Version` (opaque string) | Content-addressed: two writes with identical JSON produce the same version, so a previously read version works as an optimistic-concurrency (`If-Match`-style) precondition. |
| Permissions | `Permissions { read, write }` | See below. |

**Read permission codes** (mirrors Nakama's numbering):

| Code | Name | Meaning |
| --- | --- | --- |
| `0` | `NoRead` | Only the runtime-authoritative path may read. |
| `1` | `OwnerRead` | The runtime and the owning user may read. |
| `2` | `PublicRead` | Anyone, including unauthenticated callers, may read. |

**Write permission codes:**

| Code | Name | Meaning |
| --- | --- | --- |
| `0` | `NoWrite` | Only the runtime-authoritative path may write. |
| `1` | `OwnerWrite` | The runtime and the owning user may write. |

Console operations run as the **runtime accessor** (`Accessor::Runtime`):
object permissions never block a console read/write — the bearer token and
console role (`admin`/`viewer`) are the only gate. Ownership on every route is
selected with an optional `user_id` query parameter; when it is absent, the
route addresses the **system** owner.

Values are JSON objects only — there is no opaque/binary value type in the
console API today. A JSON body up to 512 KiB is accepted per object through
these routes (bigger than the router-wide console body limit, since storage
values can be larger than typical admin payloads).

## `GET /console/v1/storage`

List every collection with its total object count.

- **Auth:** bearer token, any role (`admin` or `viewer`).
- **Params:** none.
- **Response `200`:**

```json
{
  "collections": [
    { "collection": "saves", "objects": 42 },
    { "collection": "unlocks", "objects": 7 }
  ]
}
```

- **Errors:** `401 authentication_failed` (missing/invalid/expired token).

```bash
curl -s http://127.0.0.1:7350/console/v1/storage \
  -H "Authorization: Bearer $TOKEN"
```

## `GET /console/v1/storage/{collection}`

Paged object **summaries** for one collection — key, owner, version, and
permission codes, but not the value itself.

- **Auth:** bearer token, any role.
- **Path param:** `collection` — the collection name.
- **Query params:**

| Param | Type | Default | Meaning |
| --- | --- | --- | --- |
| `user_id` | string | absent = system owner | Restrict to one owning user; absent lists across all owners. |
| `limit` | integer | `50` | Page size, capped at `200`. |
| `cursor` | string | none | Opaque resume token from a previous page's `next`. |

- **Response `200`:**

```json
{
  "collection": "saves",
  "items": [
    { "user_id": "u-1", "key": "slot-1", "version": "0a1b2c3d4e5f6789",
      "read_permission": 1, "write_permission": 1 }
  ],
  "next": "opaque-cursor-token"
}
```

`next` is omitted once there are no more pages.

- **Errors:** `400 invalid_request` (malformed `collection`/`user_id`/query),
  `401 authentication_failed`.

```bash
curl -s "http://127.0.0.1:7350/console/v1/storage/saves?user_id=u-1&limit=50" \
  -H "Authorization: Bearer $TOKEN"
```

## `GET /console/v1/storage/{collection}/{key}`

Read one full object: value, version, and permission codes.

- **Auth:** bearer token, any role.
- **Path params:** `collection`, `key`.
- **Query params:** `user_id` (optional; absent = system owner).
- **Response `200`:**

```json
{
  "collection": "saves",
  "user_id": "u-1",
  "key": "slot-1",
  "value": { "hp": 87, "level": 4 },
  "version": "0a1b2c3d4e5f6789",
  "read_permission": 1,
  "write_permission": 1
}
```

- **Errors:** `400 invalid_request` (malformed identity), `401
  authentication_failed`, `404 not_found` (no object at that address).

```bash
curl -s "http://127.0.0.1:7350/console/v1/storage/saves/slot-1?user_id=u-1" \
  -H "Authorization: Bearer $TOKEN"
```

## `PUT /console/v1/storage/{collection}/{key}`

Create or overwrite an object. **Admin only.**

- **Auth:** bearer token, `admin` role (`viewer` gets `403`).
- **Path params:** `collection`, `key`.
- **Query params:** `user_id` (optional; absent = system owner).
- **Body:**

```json
{
  "value": { "hp": 100 },
  "read_permission": 1,
  "write_permission": 1,
  "version": "0a1b2c3d4e5f6789"
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `value` | JSON object | yes | The value to store. Must be a JSON object (not an array, string, or number). |
| `read_permission` | integer `0`\|`1`\|`2` | no (default `1`, owner-private) | See the read permission table above. |
| `write_permission` | integer `0`\|`1` | no (default `1`, owner-private) | See the write permission table above. |
| `version` | string | no | Optimistic-concurrency precondition: the write only succeeds if the object's current version equals this token. Omit for an unconditional upsert. |

Unknown body fields are rejected (`deny_unknown_fields`).

- **Response `200 OK`:** the full object, same shape as the `GET` above, with
  the freshly computed `version`.
- **Errors:** `400 invalid_request` (malformed body, bad permission code, value
  not a JSON object), `401 authentication_failed`, `403 forbidden` (viewer),
  `409 conflict` (the `version` precondition did not match the object's current
  version).
- **Audit:** recorded as `storage.write`, target `{collection}/{key} (user
  {user_id})` or `(system)`, detail `wrote version {version}`.

```bash
curl -s -X PUT "http://127.0.0.1:7350/console/v1/storage/saves/slot-1?user_id=u-1" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"value": {"hp": 100}, "read_permission": 1, "write_permission": 1}'
```

## `DELETE /console/v1/storage/{collection}/{key}`

Delete an object. **Admin only.** Idempotent.

- **Auth:** bearer token, `admin` role (`viewer` gets `403`).
- **Path params:** `collection`, `key`.
- **Query params:** `user_id` (optional; absent = system owner), `version`
  (optional precondition — the delete only succeeds if it matches the
  object's current version).
- **Response:** `204 No Content`.
- **Errors:** `400 invalid_request` (malformed identity), `401
  authentication_failed`, `403 forbidden` (viewer), `409 conflict` (`version`
  precondition mismatch).
- **Audit:** recorded as `storage.delete`, same target format as the write,
  detail `deleted object`.

```bash
curl -s -X DELETE "http://127.0.0.1:7350/console/v1/storage/saves/slot-1?user_id=u-1" \
  -H "Authorization: Bearer $TOKEN"
```

## Lua runtime storage access

The embedded Lua runtime (see [Lua runtime API](/reference/server-sdk/lua-runtime/)) does
**not** expose storage functions today. The `citadel` global registers
messaging, RPC, logging, and actor-spawning hooks (`on_message`, `on_rpc`,
`log`, `broadcast`, `send`, `spawn_actor`, `move_actor`, `despawn_actor`), but
no `citadel.storage_*` accessor exists in `src/runtime/lua.rs`. Game scripts
cannot read or write storage objects from Lua yet — this is a gap, not an
intentionally hidden feature; track it as future runtime-surface work rather
than assuming it is reachable.

## Errors

Storage routes use the console API's shared JSON error shape (see
[Admin console & console API — Errors](/reference/admin-api/console/#errors)):

```json
{ "code": "invalid_request", "message": "storage value must be a JSON object" }
```

| Status | Code | When |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed collection/key/user id, bad permission code, non-object value. |
| `401` | `authentication_failed` | Missing, invalid, or expired bearer token. |
| `403` | `forbidden` | A `viewer` attempted `PUT`/`DELETE`. |
| `404` | `not_found` | `GET` for an object that does not exist. |
| `409` | `conflict` | `version` precondition did not match the current object version. |

## Known limitations

- **Console-admin-only.** No client-facing storage API exists yet; see the
  status callout above.
- **No Lua storage access.** See [Lua runtime storage access](#lua-runtime-storage-access)
  above.
- **Values are JSON objects only.** There is no opaque/binary value type or
  content-type metadata in the current contract.
- For the full section walkthrough (roles, login, and how Storage sits among
  the other console sections), see [Admin console & console API](/reference/admin-api/console/#storage-browser).
