---
title: Leaderboards API reference
description: Per-method reference for the console-operator /console/v1/leaderboards API — boards, ranked records, sort orders, and operators.
---

Citadel's leaderboards are administered today through the **console API**
(`/console/v1/leaderboards*`) — the same operator surface documented in
[Admin console & console API](/reference/admin-api/console/). This page is the
per-method contract for that surface: HTTP verb, auth, request shape,
response shape, and errors for every route. For the console UI walkthrough
(login flow, roles, SPA usage) see the console reference; this page does not
duplicate that.

:::caution[Console-operator surface only]
Leaderboards have **no player-facing/game-client API yet**. There is no
Unity, Godot, Unreal, or Rust (`citadel-client`) SDK method for reading or
submitting scores — only an authenticated console operator can create
boards and submit records, via the routes below. Do not call these routes
from a game client; they require a console bearer token, not a player
session. When a client-facing leaderboards API ships, it will get its own
reference page and client SDK coverage.
:::

## Auth

Every route below requires a console bearer token from
`POST /console/v1/login` (see
[Login and roles](/reference/admin-api/console/#login-and-roles)):

```
Authorization: Bearer <token>
```

Two roles exist:

| Role | Access |
| --- | --- |
| `admin` | Read and mutate (create/delete boards, submit/delete records). |
| `viewer` | Read-only. Any mutating route below returns `403 forbidden`. |

Missing/invalid/expired tokens all return the uniform
`401 authentication_failed` — the boundary does not distinguish failure
reasons. See [Errors](#errors) for the shared error shape.

## Domain model

A **leaderboard** (board) has:

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | string | Unique, operator-chosen identifier. Non-empty, ≤128 bytes, no control characters. |
| `sort` | `"asc"` \| `"desc"` | Which score value ranks best (see [Sort order](#sort-order-sort)). |
| `operator` | `"best"` \| `"set"` \| `"incr"` | How a new submission combines with a user's existing record (see [Operators](#operators-operator)). |
| `reset_schedule` | string \| `null` | Free-form schedule string. **Stored and returned verbatim; never parsed or executed** — see [Known limitations](#known-limitations). |

Each user has **at most one record per board**:

| Field | Type | Meaning |
| --- | --- | --- |
| `user_id` | string | The submitting user's id. Non-empty, ≤256 bytes, no control characters. |
| `score` | i64 | The primary score. |
| `subscore` | i64 | The secondary score — breaks ties in ranking and in the `best` operator. |
| `metadata` | object \| `null` | Optional caller-supplied JSON object. |
| `updated_at_unix_ms` | integer | When the record last changed (operator-applied or not). |
| `submissions` | integer | How many times this user has submitted to this board (counts every submit, whether or not it changed the stored score). |

### Sort order (`sort`)

- `"asc"` — lower scores rank better (e.g. race times).
- `"desc"` — higher scores rank better (e.g. points). This is the default
  when `sort` is omitted on create.

**Ranking rule:** records are ordered by `(score, subscore)` in the direction
`sort` prefers, then by `user_id` ascending as a final, deterministic
tie-break. Rank `1` is always the best record. Two users who submit an
identical `(score, subscore)` still get a stable, reproducible rank order.

### Operators (`operator`)

Decides how a new submission combines with a user's existing record on that
board. Default is `"best"` when `operator` is omitted on create.

- **`set`** — unconditionally overwrites `score`, `subscore`, and `metadata`
  with the submitted values.
- **`incr`** — adds the submitted `score` and `subscore` to the existing
  totals (initializing at zero on a user's first submission), and replaces
  `metadata` with the submitted value.
- **`best`** — keeps whichever of the existing and submitted `(score,
  subscore)` pair is better for the board's `sort`:
  - `asc`: lower is better, so a **tied score prefers the lower subscore**.
  - `desc`: higher is better, so a **tied score prefers the higher
    subscore**.

  This is the same direction that governs ranking, so "the record that would
  rank highest wins" either way. `metadata` is only replaced when the
  submitted pair wins — a losing submission still increments `submissions`
  but leaves the stored score and metadata untouched.

`submissions` always increments on every submit call, regardless of operator
or outcome.

## Endpoints

### List leaderboards

```
GET /console/v1/leaderboards
```

**Auth:** bearer token, any role.

**Request:** no parameters.

**Response `200 OK`:**

```json
{
  "items": [
    {
      "id": "points",
      "sort": "desc",
      "operator": "best",
      "reset_schedule": null,
      "records": 3
    }
  ],
  "total": 1
}
```

`items` is id-ordered. `records` is the board's current record count.
`total` equals `items.length` — there is no pagination over the board list
itself.

**Errors:** `401` (missing/invalid token).

```bash
curl -s http://127.0.0.1:7350/console/v1/leaderboards \
  -H "Authorization: Bearer $TOKEN"
```

### Create a leaderboard

```
POST /console/v1/leaderboards
```

**Auth:** bearer token, `admin` only.

**Request body:**

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | string | yes | Unique board id. Non-empty, ≤128 bytes, no control characters. |
| `sort` | `"asc"` \| `"desc"` | no | Defaults to `"desc"`. |
| `operator` | `"best"` \| `"set"` \| `"incr"` | no | Defaults to `"best"`. |
| `reset_schedule` | string | no | Stored verbatim; not parsed or executed. |

Unknown fields in the body are rejected with `400`.

**Response `201 Created`:**

```json
{
  "id": "points",
  "sort": "desc",
  "operator": "best",
  "reset_schedule": null,
  "records": 0
}
```

**Errors:**

| Status | Code | Cause |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed JSON body, unknown field, empty/too-long/control-character `id`. |
| `401` | `authentication_failed` | Missing/invalid/expired token. |
| `403` | `forbidden` | Caller is `viewer`. |
| `409` | `conflict` | A board with the same `id` already exists. |

Audited as `leaderboards.create`.

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/leaderboards \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"id":"points","sort":"desc","operator":"best"}'
```

### Delete a leaderboard

```
DELETE /console/v1/leaderboards/{id}
```

**Auth:** bearer token, `admin` only.

**Request:** `id` (path) — the board to delete. Deletes the board and every
one of its records.

**Response:** `204 No Content`.

**Errors:**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired token. |
| `403` | `forbidden` | Caller is `viewer`. |
| `404` | `not_found` | No board with `id`. |

Audited as `leaderboards.delete`.

```bash
curl -s -X DELETE http://127.0.0.1:7350/console/v1/leaderboards/points \
  -H "Authorization: Bearer $TOKEN"
```

### Submit a record

```
POST /console/v1/leaderboards/{id}/records
```

This is the record submission flow: the console is the record producer
today (there is no game-client submission path). A submission applies the
board's `operator` per the [semantics above](#operators-operator).

**Auth:** bearer token, `admin` only.

**Request:** `id` (path) — the target board.

**Request body:**

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `user_id` | string | yes | Submitting user's id. Non-empty, ≤256 bytes, no control characters. |
| `score` | integer (i64) | yes | The primary score. |
| `subscore` | integer (i64) | no | Defaults to `0`. Tie-breaker for ranking and the `best` operator. |
| `metadata` | object | no | Must be a JSON object when present (a scalar/array is rejected). |

Unknown fields in the body are rejected with `400`.

**Response `200 OK`:**

```json
{
  "user_id": "u-1",
  "score": 90,
  "subscore": 0,
  "metadata": null,
  "updated_at_unix_ms": 1751792000000,
  "submissions": 1
}
```

This is the record's state **after** applying the operator, not necessarily
the submitted values (e.g. a losing `best` submission still returns the
previously-stored score). The response does not include `rank` — computing
it would require re-ranking the whole board on every write. Read the rank
back with [the ranked records route](#list-ranked-records).

**Errors:**

| Status | Code | Cause |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed body, unknown field, invalid `user_id`, or `metadata` present but not a JSON object. |
| `401` | `authentication_failed` | Missing/invalid/expired token. |
| `403` | `forbidden` | Caller is `viewer`. |
| `404` | `not_found` | No board with `id`. |

Audited as `leaderboards.record.submit`.

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/leaderboards/points/records \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"u-1","score":90}'
```

### List ranked records

```
GET /console/v1/leaderboards/{id}/records
```

**Auth:** bearer token, any role.

**Request:** `id` (path) — the target board.

**Query parameters:**

| Parameter | Type | Required | Meaning |
| --- | --- | --- | --- |
| `limit` | integer | no | Page size. Defaults to `50`, clamped to `[1, 500]`. |
| `offset` | integer | no | Rank offset — `0` starts at rank `1`. Defaults to `0`. |

**Response `200 OK`:**

```json
{
  "board": "points",
  "total": 3,
  "items": [
    {
      "rank": 1,
      "user_id": "u-3",
      "score": 100,
      "subscore": 0,
      "metadata": null,
      "updated_at_unix_ms": 1751792000000,
      "submissions": 1
    }
  ]
}
```

`total` is the board's full record count, unaffected by `limit`/`offset`.
`items` are ranked best-first per the board's [sort order](#sort-order-sort),
starting at `offset` and bounded by `limit`; `rank` is `1`-based and reflects
the record's position in the whole board, not the page.

**Errors:**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired token. |
| `404` | `not_found` | No board with `id`. |

```bash
curl -s "http://127.0.0.1:7350/console/v1/leaderboards/points/records?limit=10&offset=0" \
  -H "Authorization: Bearer $TOKEN"
```

### Delete a record

```
DELETE /console/v1/leaderboards/{id}/records/{user_id}
```

**Auth:** bearer token, `admin` only.

**Request:** `id` (path) — the target board; `user_id` (path) — the record
to remove.

**Response:** `204 No Content`.

**Errors:**

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired token. |
| `403` | `forbidden` | Caller is `viewer`. |
| `404` | `not_found` | No board with `id`, or no record for `user_id` on that board. |

Audited as `leaderboards.record.delete`.

```bash
curl -s -X DELETE http://127.0.0.1:7350/console/v1/leaderboards/points/records/u-1 \
  -H "Authorization: Bearer $TOKEN"
```

## Ties and subscore

Ties are resolved deterministically at two points, both driven by `sort` and
`subscore`:

1. **Ranking** (`GET .../records`): equal `(score, subscore)` pairs are
   ordered by `user_id` ascending, so identical scores always produce the
   same rank order across reads.
2. **The `best` operator**: a submission with an equal `score` to the stored
   record is decided by `subscore` in the direction that would rank
   higher — lower subscore wins under `asc`, higher subscore wins under
   `desc`. A losing tie still counts toward `submissions` but does not
   change the stored `score`, `subscore`, or `metadata`.

`set` and `incr` do not use `subscore` for tie-breaking on submission — they
always overwrite or accumulate respectively; `subscore` only affects them as
one of the two values being overwritten or accumulated.

## Errors

All console errors share the JSON shape:

```json
{ "code": "invalid_request", "message": "leaderboard id must not be empty" }
```

| Status | Code | Meaning |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed body, unknown field, or invalid `id`/`user_id`/`metadata`. |
| `401` | `authentication_failed` | Missing/invalid/expired console token (uniform — see [Login and roles](/reference/admin-api/console/#login-and-roles)). |
| `403` | `forbidden` | A `viewer` attempted a mutating route. |
| `404` | `not_found` | Unknown board id, or unknown record. |
| `409` | `conflict` | `POST /console/v1/leaderboards` with a duplicate `id`. |

## Known limitations

- **Console-operator only, today.** No player-facing/game-client leaderboards
  API exists yet, so there is no Unity, Godot, Unreal, or Rust
  (`citadel-client`) client SDK surface to document. The console is both the
  administration surface and the only record producer.
- **Persisted.** Boards and records are stored behind the
  repository seam, so they survive a node restart on the Postgres and SQLite
  backends. The default in-memory backend stays non-durable by design (a
  restart clears it). The authoritative records are persisted; a record's rank
  is derived on read (ordered by `(score, subscore)` in the board's sort
  direction, then `user_id` as a stable tie-break). A durable rank cache is not
  implemented yet (tracked as a follow-up in
  `docs/architecture/technical-debt.md`).
- **`reset_schedule` is stored, not executed.** The string round-trips
  through create/list responses, but Citadel does not parse or run it — no
  board resets automatically on a schedule today.

## Test coverage

Operator semantics (`best`/`set`/`incr` under both sort orders), ranking with
ties, and pagination are pure, unit-tested helpers in
`src/repository/leaderboards.rs`, shared by every backend. The persistence
contract — create/get/list, the operator + rank behavior, metadata durability,
board-delete cascade, and the not-found/conflict error paths — is verified
against the in-memory, SQLite (un-gated), and Postgres (opt-in via
`DATABASE_URL`) backends by `tests/leaderboards_repository_contract.rs`. The
thin service validation lives in `src/services/leaderboards.rs`. The full
console lifecycle — create, submit, rank, delete, the `viewer` `403` boundary,
and the audit trail — is covered end-to-end by `tests/console_leaderboards.rs`.
Request/response wire shapes (defaults, unknown-field rejection, route
registration) are unit-tested in `src/http/console_api/leaderboards.rs`.

## See also

- [Admin console & console API](/reference/admin-api/console/) — login, roles, the
  full section route map, and the shared error/audit model.
