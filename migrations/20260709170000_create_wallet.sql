-- : per-user wallets (balances + change ledger) and validated purchases.
--
-- Backs `repository::pg::wallet` (`PgWalletRepository`) and
-- `repository::pg::purchases` (`PgPurchasesRepository`). This is money, so the
-- balance read model and the append-only ledger are updated together in one
-- transaction by the repository; the schema only persists the authoritative
-- rows. The checked, non-negative balance arithmetic, the ledger capacity bound,
-- and the purchase paging / subscription derivation live in the repository's pure
-- helpers (`src/repository/wallet.rs`, `src/repository/purchases.rs`), shared
-- across all three backends.
--
-- Notes on deliberate choices:
--
-- * `wallet_balances` is the authoritative stored balance, one row per
--   `(user_id, currency)`; `wallet_ledger` is the append-only audit trail.
--   Balances are NOT re-derived by summing the ledger because the ledger is
--   capacity-bounded (the oldest entries are evicted), so it is a trail, not the
--   source of truth. Every change updates the balance and appends exactly one
--   ledger row carrying `balance_after`, atomically.
-- * `wallet_ledger.id` is a single global monotonic sequence the repository
--   computes as `MAX(id) + 1` inside the change transaction (NOT a database
--   serial / `GENERATED ALWAYS AS IDENTITY`), so the CockroachDB flavor is
--   DDL-identical apart from `COLLATE "C"` and there are no identity-column
--   quirks. Eviction removes only the oldest rows, so the id never rewinds.
-- * `balance`/`delta`/`balance_after` are integer money (`bigint`, domain `i64`);
--   NO floating point. `balance` is constrained non-negative.
-- * `purchases.transaction_id` is the store-unique primary key, so a replayed
--   receipt collides (mapped to `Conflict`). Only the SHA-256 digest of the raw
--   receipt is stored, never the receipt itself. A subscription is a purchase row
--   carrying `subscription_expiry_unix_ms`; there is no separate subscriptions
--   table (subscriptions are a read-derived view).
-- * `*_unix_ms` timestamps are domain Unix-epoch millis stored as `bigint` for an
--   exact round-trip.
-- * `text COLLATE "C"` gives deterministic, locale-independent equality (matching
--   `users`/`sessions`/`groups`/`leaderboards`/`chat_messages`/`notifications`).

CREATE TABLE IF NOT EXISTS wallet_balances (
    user_id            text COLLATE "C" NOT NULL,
    currency           text COLLATE "C" NOT NULL,
    balance            bigint NOT NULL,
    updated_at_unix_ms bigint NOT NULL,

    PRIMARY KEY (user_id, currency),

    CONSTRAINT wallet_balances_nonneg_ck CHECK (balance >= 0)
);

CREATE TABLE IF NOT EXISTS wallet_ledger (
    id                 bigint NOT NULL,
    user_id            text COLLATE "C" NOT NULL,
    currency           text COLLATE "C" NOT NULL,
    delta              bigint NOT NULL,
    balance_after      bigint NOT NULL,
    reason             text NOT NULL,
    created_at_unix_ms bigint NOT NULL,

    PRIMARY KEY (id),

    CONSTRAINT wallet_ledger_id_ck CHECK (id > 0)
);

-- Supports the newest-first, per-user ledger reads.
CREATE INDEX IF NOT EXISTS wallet_ledger_user_id_idx
    ON wallet_ledger (user_id, id);

CREATE TABLE IF NOT EXISTS purchases (
    transaction_id              text COLLATE "C" PRIMARY KEY,
    user_id                     text COLLATE "C" NOT NULL,
    product_id                  text NOT NULL,
    store                       text NOT NULL,
    receipt_sha256              text NOT NULL,
    validated_at_unix_ms        bigint NOT NULL,
    subscription_expiry_unix_ms bigint,

    CONSTRAINT purchases_store_ck CHECK (store IN ('apple', 'google', 'huawei', 'custom'))
);

-- Supports the user-filtered, newest-first purchase / subscription reads.
CREATE INDEX IF NOT EXISTS purchases_user_time_idx
    ON purchases (user_id, validated_at_unix_ms);
