---
title: Wallet
description: Per-account virtual-currency wallets — balances, credit/debit adjustments, and the change ledger — over the console API.
---

:::caution[Console-admin-only]
Wallet is an **operator surface only**. There is no game-client SDK method for
reading a balance or spending currency — no Unreal, Unity, Godot, or Rust
client call exists today. Everything on this page is driven through the
`/console/v1` console API (bearer token, `admin`/`viewer` roles); see
[Admin console & console API](/reference/admin-api/console/) for login and the general
console conventions this page assumes. A client-facing wallet API is not
implemented and is not documented here.
:::

Each account carries a virtual-currency wallet: a map of `currency -> integer
balance`, plus an append-only ledger of every change. Balances and the ledger
are **persisted behind the storage backend**: on the Postgres and
SQLite backends they survive a node restart; the in-memory backend stays
non-durable by design.

Invariants enforced by the wallet:

- Balances are **non-negative**. An adjustment that would overdraw is rejected
  and changes nothing.
- Every successful adjustment appends exactly one ledger entry carrying the
  post-adjustment balance **atomically** with the balance update, in one
  transaction — the stored balance and the ledger can never tear apart, and
  concurrent adjustments to the same account+currency are serialized (no lost or
  doubled credits).
- A zero `delta` or a malformed currency code (empty, over 64 bytes, or
  containing control characters/newlines) is a validation error.

Money is stored as **integers** end to end (no floating point). The balance is a
stored read model (authoritative); the ledger is a bounded audit trail — the
oldest entries beyond a 10,000-entry retention bound are evicted, so the ledger
is a trail, not the source of truth.

## Get wallet

```
GET /console/v1/accounts/:id/wallet
```

**Auth:** bearer token, any role (`admin` or `viewer`).

### Path parameters

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | string | yes | The account id. |

### Response `200 OK`

```json
{
  "balances": { "coins": 70, "gems": 2 },
  "ledger": [
    {
      "seq": 2,
      "user_id": "u-1",
      "currency": "coins",
      "delta": -30,
      "balance_after": 70,
      "reason": "spend",
      "time_unix_ms": 1751791000123
    },
    {
      "seq": 1,
      "user_id": "u-1",
      "currency": "coins",
      "delta": 100,
      "balance_after": 100,
      "reason": "grant",
      "time_unix_ms": 1751791000000
    }
  ]
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `balances` | object | Currency-code-ordered map of `currency -> balance`. Empty object for an account with no wallet activity. |
| `ledger` | array | This account's entries, **newest first**, capped at the most recent 100. |
| `ledger[].seq` | integer | Monotonic ledger id (a global sequence, starts at 1). |
| `ledger[].user_id` | string | The wallet owner (matches `id`). |
| `ledger[].currency` | string | Currency code the entry affected. |
| `ledger[].delta` | integer | Signed change applied (negative = debit). |
| `ledger[].balance_after` | integer | Balance immediately after this entry. |
| `ledger[].reason` | string | Operator- or game-supplied note; defaults to `"console adjustment"` when omitted on write. |
| `ledger[].time_unix_ms` | integer | When the change happened (Unix milliseconds). |

### Errors

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `404` | `not_found` | No account with that id exists. |

### Example

```bash
curl -s http://127.0.0.1:7350/console/v1/accounts/u-1/wallet \
  -H "Authorization: Bearer $TOKEN"
```

## Adjust wallet (credit / debit)

```
POST /console/v1/accounts/:id/wallet
```

**Auth:** bearer token, **`admin` only** — a `viewer` token gets `403`.
Audited as `accounts.wallet.adjust`.

### Path parameters

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `id` | string | yes | The account id. |

### Body parameters

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `currency` | string | yes | Currency code (e.g. `coins`, `gems`). Non-empty, at most 64 bytes, no control characters. |
| `delta` | integer | yes | Signed change to apply. Positive credits, negative debits. Must not be `0`. |
| `reason` | string | no | Operator note recorded in the ledger. Defaults to `"console adjustment"` when omitted. |

Unknown fields in the body are rejected (`400`).

### Response `200 OK`

Same shape as [Get wallet](#get-wallet) — the full, post-adjustment balances
and ledger (most recent 100 entries) for the account.

### Errors

| Status | Code | Cause |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed body, unknown field, empty/oversized currency code, or `delta == 0`. |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller has the `viewer` role. |
| `404` | `not_found` | No account with that id exists. |
| `409` | `conflict` | The debit would overdraw the balance below zero, or the balance would overflow. Nothing is changed. |

### Example

```bash
# Credit 100 coins
curl -s -X POST http://127.0.0.1:7350/console/v1/accounts/u-1/wallet \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"currency": "coins", "delta": 100, "reason": "grant"}'

# Debit 30 coins
curl -s -X POST http://127.0.0.1:7350/console/v1/accounts/u-1/wallet \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"currency": "coins", "delta": -30, "reason": "spend"}'
```

## Known limitations

- **No game-client SDK surface.** Wallet is console-admin-only today. A
  player-facing wallet read/spend API (Unreal/Unity/Godot/Rust clients) is not
  implemented.
- **Ledger is capped, not paged.** `GET` always returns the newest 100 entries
  for the account; the durable ledger itself retains at most the newest 10,000
  entries globally (the oldest are evicted). There is no cursor to page further
  back.
- **Cross-account ledger-id contention.** Changes to the same account+currency
  are fully serialized. Two changes to *different* accounts racing on the shared
  global ledger id can rarely collide and one is rejected with `409 conflict`
  (retryable) — the same accepted tradeoff as the notifications/chat producers.

## Source

`src/repository/wallet.rs` (money invariants + atomic ledger/balance, backend
contract, unit-tested), `src/repository/pg/wallet.rs` +
`src/repository/sqlite/wallet.rs` (durable backends),
`src/services/wallet.rs` (validate-then-delegate service),
`src/http/console_api/accounts.rs` (`wallet_handler`,
`wallet_adjust_handler`). Cross-reference
[Admin console & console API](/reference/admin-api/console/) for login, roles, and the
audit trail these routes participate in.
