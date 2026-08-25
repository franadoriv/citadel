---
title: Audit log reference
description: The console audit trail data model and the GET /console/v1/audit endpoint.
---

Every console mutation — and every login attempt — is recorded as an
`AuditEntry`: who acted, with which role, what they did, on what, and when. It
is the accountability record for the
[admin console](/reference/admin-api/console/); section handlers record entries
explicitly at each call site, so every entry is greppable back to the exact
mutation that produced it.

Each entry lands in two places at once: a bounded in-process ring, and — on a
backend that has the durable log tables (SQLite, PostgreSQL, CockroachDB) — the
`console_audit_entries` table, which survives a restart. The ring is never
switched off, so a read is answered on every backend; the `durable` field on the
response says which of the two answered it.

:::note[Console-admin-only surface]
The audit trail is an **operator** surface under `/console/v1`. There is no
game-client SDK method for it — no Unreal C++, Blueprint, Unity C#, Godot
GDScript, or Rust (`citadel-client`) accessor exists, and none is planned; it
exists purely so operators can review what other operators did.
:::

## `AuditEntry` fields

| Field | Type | Meaning |
| --- | --- | --- |
| `time_unix_ms` | integer | When the action happened, Unix milliseconds. |
| `actor` | string | The operator username that acted, or the presented (possibly wrong) username for a failed login attempt. |
| `role` | string | The operator's role at the time: `admin`, `viewer`, or `-` when unauthenticated (e.g. a failed login has no resolved role). |
| `action` | string | A dotted verb identifying the action, e.g. `console.login`, `storage.write`, `accounts.ban`. |
| `target` | string | The acted-on resource (an id, path, or composite label), or `-` when not applicable. |
| `details` | string | A sanitized, human-readable summary. Never carries passwords, tokens, or raw request payloads — this is enforced at every call site, not filtered after the fact. |
| `match_id` | string | **Optional, and absent from almost every entry.** An operator action is not match-scoped and is deliberately never forced into a match; the field appears only on entries a match-scoped subsystem recorded. An entry without one serializes exactly as it did before the field existed. |

```json
{
  "time_unix_ms": 1751791000000,
  "actor": "admin",
  "role": "admin",
  "action": "console.login",
  "target": "console",
  "details": "login succeeded (admin)"
}
```

## `GET /console/v1/audit`

Read the trail newest-first, with optional filters.

- **Auth:** bearer token, any role (`admin` or `viewer`) — reading the trail
  is how a viewer verifies what admins did, so a human read of it is not itself
  audited, and an API-key read of it is recorded in the ring only (see
  [Which console actions are audited](#which-console-actions-are-audited)).
- **Query params:**

| Param | Type | Default | Meaning |
| --- | --- | --- | --- |
| `limit` | integer | `100` | Page size, newest-first. Capped at `500`. |
| `actor` | string | none | Exact actor (username) match. |
| `action` | string | none | Action **prefix** match — `storage` matches both `storage.write` and `storage.delete`. A literal `%` or `_` matches only itself; it is not a wildcard. |
| `match_id` | string | none | Exact match reference. Omitting it matches **every** entry, including the operator actions that belong to no match at all. |
| `after` | string | none | Keyset cursor: pass the previous page's `next_after` to read the page after it. |

Filters are conjunctive (all supplied filters must match). Unknown query
parameters are rejected with `400`.

`match_id` and `after` need the durable trail. On a backend without one they are
answered honestly rather than silently ignored: a ring entry records no match and
has no durable key, so either filter returns an empty page.

- **Response `200`:**

```json
{
  "entries": [
    { "time_unix_ms": 1751791000000, "actor": "admin", "role": "admin",
      "action": "storage.write", "target": "saves/slot-1 (user u-1)",
      "details": "wrote version 0a1b2c3d4e5f6789" },
    { "time_unix_ms": 1751790000000, "actor": "admin", "role": "admin",
      "action": "console.login", "target": "console",
      "details": "login succeeded (admin)" }
  ],
  "retained": 2,
  "capacity": 1024,
  "durable": true,
  "dropped_total": 0
}
```

| Field | Meaning |
| --- | --- |
| `entries` | The matching page, newest first. |
| `retained` | How much history exists right now: the ring's **unfiltered** depth, or — when `durable` is `true` — the number of stored rows matching the supplied filters. |
| `capacity` | The configured retention bound, `logs.audit.capacity` (1024 by default). It sizes the in-process ring on every backend. |
| `next_after` | Cursor for the next page. **Absent on the last page, and absent from every page a ring answered** — the endpoint never hands out a cursor it could not honour. |
| `durable` | Whether this page came from the `console_audit_entries` table. `false` means the in-process ring answered, so this is a process-local cache and not durable history. |
| `dropped_total` | Records the write-behind queues have dropped since boot. `0` means nothing was lost; a non-zero value distinguishes a quiet trail from a lossy one. |

- **Errors:** `400 invalid_request` (unknown query parameter, or a malformed
  `after` cursor), `401 authentication_failed` (missing/invalid/expired token).

```bash
curl -s "http://127.0.0.1:7350/console/v1/audit?limit=100&actor=admin&action=storage" \
  -H "Authorization: Bearer $TOKEN"
```

## Which console actions are audited

Audit entries are recorded explicitly at each mutating handler. As of this
writing, the following dotted actions are recorded (derived from every
`audit_log.record(...)` call site under `src/http/console_api/`):

| Action | Recorded by |
| --- | --- |
| `console.login` / `console.login_failed` | Every login attempt (success and failure). |
| `storage.write` / `storage.delete` | Storage object `PUT`/`DELETE`. |
| `accounts.create` / `accounts.update` / `accounts.ban` / `accounts.unban` / `accounts.delete` | Account administration mutations. |
| `accounts.wallet.adjust` | Wallet credit/debit. |
| `accounts.friends.add` / `accounts.friends.remove` | Friends panel mutations. |
| `groups.create` / `groups.update` / `groups.delete` | Group lifecycle mutations. |
| `groups.member.add` / `groups.member.promote` / `groups.member.demote` / `groups.member.kick` | Group membership mutations. |
| `chat.message.append` / `chat.message.delete` | Chat moderation actions. |
| `notifications.send` / `notifications.delete` | Notification composer actions. |
| `leaderboards.create` / `leaderboards.delete` | Leaderboard lifecycle mutations. |
| `leaderboards.record.submit` / `leaderboards.record.delete` | Leaderboard record mutations. |
| `purchases.validate` | Purchase receipt validation. |
| `runtime.rpc` | The console's Lua RPC caller. |

Human reads (every `GET` section route) and the audit trail's own `GET` are
**never** audited. Only mutations and login attempts are recorded — this is
enforced call-site by call-site, not by middleware, so it is greppable and
testable but does mean a newly added mutation must add its own `record` call.

An API-key read is different: it is recorded centrally as `console.read` after
authentication and route authorization. Those entries stay in the ring only when
the route read is `/console/v1/audit`, `/console/v1/logs`, or
`/console/v1/matchlogs`. A credential polling one of those would otherwise write
one durable row per poll, forever, and the only reader of those rows would be the
poller that produced them.

## Errors

The audit route uses the console API's shared JSON error shape (see
[Admin console & console API — Errors](/reference/admin-api/console/#errors)):

```json
{ "code": "authentication_failed", "message": "authentication failed" }
```

| Status | Code | When |
| --- | --- | --- |
| `400` | `invalid_request` | An unrecognized query parameter, or an `after` value that is not a well-formed trail id. |
| `401` | `authentication_failed` | Missing, invalid, or expired bearer token. |

## Retention and durability

The in-process ring holds at most `capacity` entries (`logs.audit.capacity`,
1024 by default) and evicts the oldest for every new one. It exists on every
backend and is what `durable: false` means.

On SQLite, PostgreSQL, and CockroachDB the same entry is also written to
`console_audit_entries` and retained for `logs.audit.retention_days` (365 by
default), after which a bounded periodic prune removes it. Eviction from the
ring is not a loss from the table: the two bounds are independent, and the ring
being 1024 entries deep says nothing about how much stored history exists.

Recording is synchronous, but the database write is not. An entry enters a
bounded write-behind queue and is flushed in batches every
`logs.flush_interval_ms` (250 ms by default), so an action can take up to that
long to appear on a `durable: true` page. If the queue is ever full the oldest
queued record is dropped rather than blocking the console, and `dropped_total`
counts it.

:::caution[The in-memory and MongoDB backends have no durable trail]
Neither backend has the durable log tables, so the ring is the entire trail and
a **node restart clears it**. Reads still answer `200` — they are simply marked
`durable: false`, `match_id` and `after` return an empty page, and `next_after`
is never present. Do not rely on those two backends for long-term compliance or
forensic retention.
:::

For the full console walkthrough (roles, login flow, and how Audit sits among
the other console sections), see
[Admin console & console API](/reference/admin-api/console/#audit-logs).
