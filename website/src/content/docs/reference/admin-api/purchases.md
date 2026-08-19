---
title: Purchases & subscriptions
description: Validated in-app purchase records and derived subscription status over the console API, behind a pluggable receipt-validator seam.
---

:::caution[Console-admin-only]
Purchases and subscriptions are an **operator surface only**. There is no
game-client SDK method to submit a receipt or read purchase history — no
Unreal, Unity, Godot, or Rust client call exists today. Every route on this
page is driven through the `/console/v1` console API (bearer token,
`admin`/`viewer` roles); see
[Admin console & console API](/reference/admin-api/console/) for login and general
console conventions. A client-facing purchase-submission API is not
implemented.
:::

:::caution[Development receipts only — real store validators pending]
Citadel now has an asynchronous, server-owned receipt-validation foundation:
provider egress is bounded, credential values are never accepted from TOML, and
Apple/Google/Huawei adapters are disabled until their dedicated verified
implementations ship. The only enabled validator is the deterministic **custom**
development validator, which parses a JSON receipt and makes no network call.
Submitting an `apple`, `google`, or `huawei` receipt to the current node returns
a sanitized provider-disabled error; it never claims to validate a real store.
Production App Store / Google Play validation remains unimplemented.
:::

Validated purchases are **persisted behind the storage backend**:
on the Postgres and SQLite backends they survive a node restart; the in-memory
backend stays non-durable by design. The raw receipt is **never stored** — only
its SHA-256 hex digest — so the store cannot leak resubmittable receipt
material. A `transaction_id` is recorded at most once (the durable primary key);
replaying the same receipt is a conflict. Subscriptions are a read-derived view
over purchases that carry an expiry — there is no separate subscription store.

## The `store` enum

Every purchase/subscription row carries a `store`, one of:

| Value | Meaning |
| --- | --- |
| `apple` | Apple App Store. |
| `google` | Google Play. |
| `huawei` | Huawei AppGallery. |
| `custom` | A game-defined custom store — the dev validator's natural home. |

The `store` value is recorded as given; the **dev validator does not verify it
against the actual store** — it only shapes the response and audit line.

## Dev validator receipt shape

The dev validator (the only validator that ships) accepts a receipt as a JSON
string with exactly these fields — unknown fields are rejected:

```json
{
  "transaction_id": "tx-1",
  "product_id": "gold-pack",
  "subscription_expiry_unix_ms": 1751999999999
}
```

| Field | Type | Required | Meaning |
| --- | --- | --- | --- |
| `transaction_id` | string | yes | Store-unique transaction id. Must be non-empty. |
| `product_id` | string | yes | The purchased product id. Must be non-empty. |
| `subscription_expiry_unix_ms` | integer | no | Present only for subscription products. When set, the purchase also shows up in the [subscriptions listing](#list-subscriptions) with a status derived from this expiry. |

A non-subscription (consumable) purchase simply omits
`subscription_expiry_unix_ms`.

## List purchases

```
GET /console/v1/purchases?user_id&limit
```

**Auth:** bearer token, any role.

### Query parameters

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `user_id` | string | no | Restrict to one buying account. Omit to list every account's purchases. |
| `limit` | integer | no | Page size, newest-first. Default `50`, capped at `200`. |

### Response `200 OK`

```json
{
  "items": [
    {
      "transaction_id": "tx-1",
      "user_id": "u-1",
      "product_id": "gold-pack",
      "store": "custom",
      "receipt_sha256": "3f786850e387550fdab836ed7e6dc881de23001b",
      "validated_at_unix_ms": 1751791000000
    }
  ]
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `items` | array | Newest-first validated purchases (by validation time, then transaction id), optionally filtered to `user_id`. |
| `items[].transaction_id` | string | Store-unique transaction id. |
| `items[].user_id` | string | The buying account. |
| `items[].product_id` | string | The purchased product. |
| `items[].store` | string | One of `apple`/`google`/`huawei`/`custom`. |
| `items[].receipt_sha256` | string | SHA-256 hex digest of the raw receipt. The receipt itself is never stored. |
| `items[].validated_at_unix_ms` | integer | When the purchase was validated (Unix milliseconds). |
| `items[].subscription_expiry_unix_ms` | integer, optional | Present only when the product is a subscription. |

### Errors

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |

### Example

```bash
curl -s "http://127.0.0.1:7350/console/v1/purchases?user_id=u-1&limit=20" \
  -H "Authorization: Bearer $TOKEN"
```

## Get one purchase

```
GET /console/v1/purchases/:transaction_id
```

**Auth:** bearer token, any role.

### Path parameters

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `transaction_id` | string | yes | The transaction id to look up. |

### Response `200 OK`

One purchase object, same shape as an `items[]` entry above.

### Errors

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `404` | `not_found` | No purchase with that transaction id. |

### Example

```bash
curl -s http://127.0.0.1:7350/console/v1/purchases/tx-1 \
  -H "Authorization: Bearer $TOKEN"
```

## Validate + record a purchase

```
POST /console/v1/purchases
```

**Auth:** bearer token, **`admin` only** — a `viewer` token gets `403`.
Audited as `purchases.validate`.

### Body parameters

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `user_id` | string | yes | The buying account. Not validated against the account store — any string is accepted. |
| `store` | string | yes | One of `apple`, `google`, `huawei`, `custom`. Any other value is a `400`. |
| `receipt` | string | yes | The raw receipt document. For the shipped dev validator this must be the [dev validator receipt shape](#dev-validator-receipt-shape) as a JSON string. |

Unknown fields in the body are rejected (`400`).

### Response `201 Created`

```json
{
  "transaction_id": "tx-1",
  "user_id": "u-1",
  "product_id": "gold-pack",
  "store": "custom",
  "receipt_sha256": "3f786850e387550fdab836ed7e6dc881de23001b",
  "validated_at_unix_ms": 1751791000000
}
```

Same shape as a purchase listing row (see [List purchases](#list-purchases)).

### Errors

| Status | Code | Cause |
| --- | --- | --- |
| `400` | `invalid_request` | Malformed body, unknown field/store value, or a receipt that fails validation (not JSON, missing/empty `transaction_id`/`product_id`, or unknown fields in the receipt document). |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |
| `403` | `forbidden` | Caller has the `viewer` role. |
| `409` | `conflict` | The `transaction_id` was already recorded (replayed receipt). Nothing changes. |

### Example

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/purchases \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "user_id": "u-1",
        "store": "custom",
        "receipt": "{\"transaction_id\":\"tx-1\",\"product_id\":\"gold-pack\"}"
      }'
```

Subscription example (sets `subscription_expiry_unix_ms` inside the receipt
document):

```bash
curl -s -X POST http://127.0.0.1:7350/console/v1/purchases \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "user_id": "u-1",
        "store": "apple",
        "receipt": "{\"transaction_id\":\"tx-vip\",\"product_id\":\"vip\",\"subscription_expiry_unix_ms\":1751999999999}"
      }'
```

## List subscriptions

```
GET /console/v1/subscriptions?user_id&limit
```

**Auth:** bearer token, any role.

Only purchases whose receipt carried `subscription_expiry_unix_ms` appear
here; consumable (non-subscription) purchases never do.

### Query parameters

| Name | Type | Required | Meaning |
| --- | --- | --- | --- |
| `user_id` | string | no | Restrict to one account's subscriptions. Omit to list every account's. |
| `limit` | integer | no | Page size, newest-first. Default `50`, capped at `200`. |

### Response `200 OK`

```json
{
  "items": [
    {
      "transaction_id": "tx-vip",
      "user_id": "u-1",
      "product_id": "vip",
      "store": "apple",
      "expiry_unix_ms": 1751999999999,
      "status": "active"
    }
  ]
}
```

| Field | Type | Meaning |
| --- | --- | --- |
| `items` | array | Newest-first subscription rows, optionally filtered to `user_id`. |
| `items[].transaction_id` | string | The owning transaction. |
| `items[].user_id` | string | The subscribing account. |
| `items[].product_id` | string | The subscription product. |
| `items[].store` | string | One of `apple`/`google`/`huawei`/`custom`. |
| `items[].expiry_unix_ms` | integer | Subscription expiry (Unix milliseconds). |
| `items[].status` | string | `"active"` or `"expired"`, derived by comparing `expiry_unix_ms` against the **read-time clock** — not stored, recomputed on every request. |

### Errors

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `authentication_failed` | Missing/invalid/expired bearer token. |

### Example

```bash
curl -s "http://127.0.0.1:7350/console/v1/subscriptions?user_id=u-1" \
  -H "Authorization: Bearer $TOKEN"
```

## Known limitations

- **Dev validator only.** No network call is made to any real store; `store`
  is recorded as given but not verified against it. Real App Store / Google
  Play validators are pending.
- **No game-client SDK surface.** Receipt submission and purchase/subscription
  reads are console-admin-only today. A player-facing purchase API
  (Unreal/Unity/Godot/Rust clients) is not implemented.
- **Raw receipts are never retained**, only their SHA-256 digest — by design,
  not a gap, but it means a lost/forgotten receipt cannot be re-inspected from
  the store.

## Source

`src/repository/purchases.rs` (`PurchaseStore` enum, `Purchase`/`SubscriptionRow`
value types, replay/paging/subscription-derivation contract, unit-tested),
`src/repository/pg/purchases.rs` + `src/repository/sqlite/purchases.rs` (durable
backends), `src/services/purchases.rs` (`ReceiptValidator` trait,
`DevReceiptValidator`, `PurchaseService` validate-then-delegate, unit-tested
including replay and malformed-receipt rejection),
`src/http/console_api/purchases.rs` (`list_handler`, `validate_handler`,
`detail_handler`, `subscriptions_handler`). Cross-reference
[Admin console & console API](/reference/admin-api/console/) for login, roles, and the
audit trail these routes participate in.
