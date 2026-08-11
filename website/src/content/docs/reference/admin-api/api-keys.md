---
title: API keys and service accounts
description: Create, scope, rotate, revoke, and use Citadel V1 machine credentials safely.
---

Citadel has two deliberately separate bearer credential types:

- **Human console sessions** are short-lived opaque tokens returned by
  `POST /console/v1/login`. They identify an operator with the `admin` or
  `viewer` role, live only in node memory, and are **not JWT** credentials.
- **API keys** are durable machine identities. They have a public ID, a name,
  an explicit set of read scopes, a generation, and optional expiration. They
  never become a human username or role.

Both use the HTTP authorization header:

```http
Authorization: Bearer <CITADEL_API_KEY>
```

Never send either credential in a query parameter, URL, cookie, request body,
or log. URLs are routinely retained by proxies, browsers, and telemetry. Use
TLS whenever the client is not connecting over trusted loopback.

## Key format and storage

A V1 API key starts with the unambiguous `ctdl_k1_` prefix and has this shape:

```text
ctdl_k1_<32-lowercase-hex-public-id>_<43-character-base64url-secret>
```

The full value is returned exactly once after creation or rotation. Citadel
stores the public ID and non-secret metadata plus a one-way SHA-256 hash of the
32-byte random secret: this is hash-only storage, never plaintext-secret
storage. Later list and detail responses contain no `secret` field. Citadel
also redacts credentials from errors, debug output, audit records, metrics, and
logs.

Treat the one-time response like a password: copy it directly into the target
process's secret manager or protected environment injection. Do not put it in
source control, shell history, screenshots, tickets, or application logs. If
it is lost, rotate the key; it cannot be retrieved.

## V1 scopes

V1 API keys are read-only. The exact supported scopes are:

| Scope | Read capability |
| --- | --- |
| `telemetry:read` | Node telemetry. |
| `config:read` | Redacted effective configuration. |
| `audit:read` | Audit entries. |
| `errors:read` | Redacted error journal. |
| `accounts:read` | Account metadata and detail. |
| `groups:read` | Group metadata and membership. |
| `runtime:read` | Runtime status and introspection. |
| `matches:read` | Live match metadata and detail. |
| `storage:read` | Storage metadata and objects. |
| `database:read` | Read-only database explorer. |
| `chat:read` | Chat channels and history. |
| `notifications:read` | Notification records. |
| `leaderboards:read` | Leaderboards and records. |
| `tournaments:read` | Tournaments and records. |
| `purchases:read` | Purchase records. |
| `subscriptions:read` | Subscription records. |

Grant only the scopes the process needs. Scope checks happen at each HTTP
endpoint, not only in the dashboard. An absent scope, malformed or unknown
key, stale generation, expiration, or revocation is rejected fail closed.
Unknown authentication failures remain uniform and do not reveal whether a
public ID exists.

API keys cannot call write endpoints, runtime RPCs, or API-key management
routes, even if a scope name appears related. V1 defines no write or wildcard
scope. The database explorer is the only method-level exception: its bounded,
typed row reads use `POST /console/v1/database/rows` and
`POST /console/v1/database/row` because their filters are request bodies; those
two exact routes are authorized as semantic reads only with `database:read`.
Every other API-key `POST`, `PUT`, `PATCH`, or `DELETE` remains denied.

## Human-admin-only management API

Every management endpoint requires an authenticated human `admin` session.
A human `viewer` and every API-key principal receive `403 forbidden`; API keys
cannot create, inspect, rotate, or revoke credentials.

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/console/v1/api-keys` | Create a key from `name`, `scopes`, and optional `expires_at`; returns the one-time secret. |
| `GET` | `/console/v1/api-keys` | List metadata and derived `active`, `expired`, or `revoked` status; never returns secrets. |
| `GET` | `/console/v1/api-keys/{id}` | Read one metadata record; never returns a secret. |
| `POST` | `/console/v1/api-keys/{id}/rotate` | Replace the verifier for the expected `generation`, invalidate the previous secret, and return a new secret exactly once. |
| `POST` | `/console/v1/api-keys/{id}/revoke` | Revoke the expected `generation` immediately. |

`expires_at` is either `null` or Unix time in milliseconds and must be in the
future at creation. Expiration and revocation take effect immediately at the
authentication boundary. Rotation preserves the key's public ID, name, scopes,
and expiration, increments `generation`, and makes the old bearer unusable.
Use the generation returned by the latest metadata read so concurrent or stale
rotation/revocation attempts fail rather than overwriting a newer lifecycle
change.

### Create and use a key

The examples use placeholders intentionally. Keep the human session token and
the one-time API key out of command history in production.

```bash
CITADEL_URL="https://citadel.example.com"
HUMAN_ADMIN_TOKEN="<OPAQUE_HUMAN_ADMIN_SESSION>"

curl --fail-with-body -X POST "$CITADEL_URL/console/v1/api-keys" \
  -H "Authorization: Bearer $HUMAN_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  --data '{
    "name": "operations-observer",
    "scopes": ["telemetry:read", "errors:read", "audit:read"],
    "expires_at": 1798761600000
  }'
```

The `201 Created` body has `{ "key": { ...metadata... }, "secret": "..." }`.
Move `secret` immediately to protected process configuration and discard the
response. A later metadata list has only public fields:

```bash
curl --fail-with-body "$CITADEL_URL/console/v1/api-keys" \
  -H "Authorization: Bearer $HUMAN_ADMIN_TOKEN"
```

Use the machine credential on a scoped read endpoint:

```bash
CITADEL_API_KEY="<CITADEL_API_KEY>"
curl --fail-with-body "$CITADEL_URL/console/v1/telemetry" \
  -H "Authorization: Bearer $CITADEL_API_KEY"
```

### Rotate or revoke

Read current metadata first and substitute its public ID and generation:

```bash
KEY_ID="<PUBLIC_API_KEY_ID>"
GENERATION="<CURRENT_GENERATION>"

curl --fail-with-body -X POST \
  "$CITADEL_URL/console/v1/api-keys/$KEY_ID/rotate" \
  -H "Authorization: Bearer $HUMAN_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  --data "{\"generation\":$GENERATION}"
```

Deploy the newly revealed bearer, verify the consumer, and discard the
response. The previous bearer stops authenticating as soon as rotation
commits. To permanently disable the current generation:

```bash
curl --fail-with-body -X POST \
  "$CITADEL_URL/console/v1/api-keys/$KEY_ID/revoke" \
  -H "Authorization: Bearer $HUMAN_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  --data "{\"generation\":$GENERATION}"
```

Revocation is terminal for that credential. Create a replacement if access is
needed again.

## Status, audit, and last use

Metadata exposes `created_at`, optional `expires_at`, optional `revoked_at`,
`generation`, scopes, and optional `last_used_at`. The dashboard derives and
shows these states:

- `active`: neither expired nor revoked;
- `expired`: its expiration has passed;
- `revoked`: explicitly revoked (takes precedence over expiration).

Successful API-key authentication advances `last_used_at`. Updates are
coalesced per key generation and flushed by management metadata reads, before
rotation/revocation, and after HTTP/HTTPS has drained in-flight requests during
a graceful shutdown. Repository writes are conditional on the same active
generation, so a delayed observation cannot update a rotated, revoked, or
expired credential. This avoids a durable write on every request. As with other
coalesced telemetry, an abrupt process or host failure can lose only the
observations still pending in memory; credential validity and lifecycle state
remain fully durable.

Creation, rotation, and revocation produce audit actions
`api_keys.create`, `api_keys.rotate`, and `api_keys.revoke`. Every API-key
request that passes credential and scope authorization also produces a
centralized `console.read` audit action containing the HTTP method and route
path; handler-specific surfaces such as Database Explorer may add a more
specific read action and logical target. Audit principals distinguish `human`
from `api_key`; machine records use the public credential ID/name and scopes,
never the bearer, verifier, Authorization header, query filters, or returned
data.

## V1 limits

- API keys are read-only; there are no write, admin, or wildcard scopes.
- Management stays human-admin-only and cannot be delegated to a machine key.
- Authorization is header-only; query-string credentials are unsupported.
- Invalid, unknown, expired, revoked, under-scoped, or stale credentials fail
  closed without credential-oracle detail.
- A secret is available exactly once. Recovery means rotation, not retrieval.
