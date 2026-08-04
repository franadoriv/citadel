---
title: Audit log reference
description: The console audit trail data model and the GET /console/v1/audit endpoint.
---

Every console mutation — and every login attempt — is recorded as an
`AuditEntry` in an in-process audit trail: who acted, with which role, what
they did, on what, and when. It is the accountability record for the
[admin console](/reference/admin-api/console/); section handlers record entries
explicitly at each call site, so every entry is greppable back to the exact
mutation that produced it.

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
  is how a viewer verifies what admins did, so it is not itself audited (that
  would flood the ring on every read).
- **Query params:**

| Param | Type | Default | Meaning |
| --- | --- | --- | --- |
| `limit` | integer | `100` | Page size, newest-first. Capped at `500`. |
| `actor` | string | none | Exact actor (username) match. |
| `action` | string | none | Action **prefix** match — `storage` matches both `storage.write` and `storage.delete`. |

Filters are conjunctive (all supplied filters must match). Unknown query
parameters are rejected with `400`.

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
  "capacity": 1024
}
```

| Field | Meaning |
| --- | --- |
| `entries` | The matching page, newest first. |
| `retained` | Total entries currently held in the ring, **unfiltered** — how much history actually exists right now. |
| `capacity` | The ring's retention bound (see the caution below). |

- **Errors:** `400 invalid_request` (unknown query parameter), `401
  authentication_failed` (missing/invalid/expired token).

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

Reads (every `GET` section route) and the audit trail's own `GET` are
**never** audited. Only mutations and login attempts are recorded — this is
enforced call-site by call-site, not by middleware, so it is greppable and
testable but does mean a newly added mutation must add its own `record` call.

## Errors

The audit route uses the console API's shared JSON error shape (see
[Admin console & console API — Errors](/reference/admin-api/console/#errors)):

```json
{ "code": "authentication_failed", "message": "authentication failed" }
```

| Status | Code | When |
| --- | --- | --- |
| `400` | `invalid_request` | An unrecognized query parameter was supplied. |
| `401` | `authentication_failed` | Missing, invalid, or expired bearer token. |

:::caution[Retention is a bounded in-memory ring — not durable]
The audit trail is a **bounded, in-process ring** holding at most
`capacity` entries (1024 by default). Once the ring is full, the **oldest
entry is evicted** for every new one recorded — there is no overflow to disk
or a database. A **node restart clears the trail entirely**. Durable audit
persistence has not shipped yet; it is tracked as known technical debt. Do
not rely on this trail for
long-term compliance or forensic retention until that lands.
:::

For the full console walkthrough (roles, login flow, and how Audit sits among
the other console sections), see
[Admin console & console API](/reference/admin-api/console/#audit-logs).
