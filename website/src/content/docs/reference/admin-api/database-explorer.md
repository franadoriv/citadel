---
title: Database Explorer
description: Read-only, metadata-validated diagnostic browsing for the configured Citadel database.
---

The Dashboard **Database Explorer** is an operator diagnostic surface for the
single database configured on this Citadel node. It is deliberately not a SQL
console: it has no SQL-text, mutation, DDL, import, or export operation.

Both `admin` and `viewer` console roles may use every route below. Returned
cells are redacted by the server for credential-like columns (password hashes,
tokens, keys, and secrets); role does not bypass redaction.

## Availability and limits

The explorer is available for durable SQLite, PostgreSQL, and CockroachDB
backends. It lists only application objects: SQLite `main` objects excluding
`sqlite_` internals, and the PostgreSQL/CockroachDB `public` schema. In-memory
nodes return `400` because they have no SQL metadata to inspect.

Each query is bounded to 100 rows and a 1 MiB serialized response, uses a
five-second database deadline, and accepts at most eight scalar filters. A
node also admits at most 60 explorer requests per authenticated operator per
minute; excess requests return `429 rate_limited` with `Retry-After`. This
small per-node guard is not a distributed quota.

Tables without a primary key remain visible as metadata but cannot be browsed.
PostgreSQL/CockroachDB index and relation metadata are intentionally omitted
until their separate adapters have live compatibility coverage.

## Routes

| Route | Method | Purpose |
| --- | --- | --- |
| `/console/v1/database` | GET | List allowed tables and views. |
| `/console/v1/database/{schema}/{table}` | GET | Describe columns, primary key, capabilities, indexes, and relations. |
| `/console/v1/database/rows` | POST | Return one redacted, keyset-paginated row page. |
| `/console/v1/database/row` | POST | Return one redacted row through a previously issued opaque reference. |

All routes require `Authorization: Bearer <console-token>`. Every successful
read adds an in-memory console audit record with the operator, action, and
logical table, never SQL, filter values, cursors, row keys, or returned cells.

## List tables

```http
GET /console/v1/database
Authorization: Bearer <token>
```

```json
{
  "tables": [
    { "table": { "schema": "main", "table": "users" }, "kind": "table" }
  ]
}
```

## Describe table

```http
GET /console/v1/database/main/users
Authorization: Bearer <token>
```

The response identifies sensitive columns, the primary key, and the
`stable_keyset_pagination` capability. Treat metadata as informational: the
server re-resolves it for every row request.

## Browse rows

```http
POST /console/v1/database/rows
Authorization: Bearer <token>
Content-Type: application/json

{
  "table": { "schema": "main", "table": "users" },
  "filters": [{ "column": "username", "operator": "contains", "value": "ada" }],
  "sort": { "column": "id", "direction": "asc" },
  "limit": 50
}
```

The only supported operators are `eq`, `neq`, `lt`, `lte`, `gt`, `gte`,
`contains`, and `is_null`. The column must be a freshly resolved,
non-sensitive column. Values are JSON scalars; identifiers never become SQL
syntax and values are always bound parameters.

Responses have typed cells (`null`, `boolean`, `integer`, `decimal`, `text`,
`binary_base64`, `json`, `timestamp`, or `redacted`) and an opaque `row_ref`.
When `next` is present, send it unchanged as `cursor` with the same table and
sort specification. Cursors and row references are server-held 256-bit handles
that expire after five minutes and are invalidated by a node restart.

## Inspect a row

```json
POST /console/v1/database/row
{ "table": { "schema": "main", "table": "users" }, "row_ref": "<opaque>" }
```

The reference is scoped to its issuing table. A changed/deleted row yields
`404`; an expired, malformed, or cross-table reference yields `400`.

## Operating safely

Use a database credential with read-only permissions where deployment allows
it. Citadel also enforces the API boundary itself, which matters for SQLite and
single-URL deployments. The explorer is intended for bounded diagnosis, not
bulk extraction; use a purpose-built, access-controlled operational workflow
for exports.
