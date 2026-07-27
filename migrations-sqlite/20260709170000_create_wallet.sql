-- : SQLite per-user wallets (balances + change ledger) and validated
-- purchases.
--
-- Sibling to the Postgres migration in `../migrations`, backing
-- `repository::sqlite::wallet` (`SqliteWalletRepository`) and
-- `repository::sqlite::purchases` (`SqlitePurchasesRepository`). The schema
-- mirrors that Postgres migration with SQLite-native types so the SAME wallet /
-- purchases contract tests pass against both backends.
--
-- Dialect mapping vs the Postgres schema:
--   * `text COLLATE "C"` -> `TEXT`; SQLite's default BINARY collation is byte-wise,
--     matching Postgres `COLLATE "C"`.
--   * `bigint` (ids / money / millis) -> `INTEGER`; SQLite has one integer class
--     and the u64/i64 round-trip is exact.
--   * `subscription_expiry_unix_ms` stays nullable (a non-subscription purchase).
--
-- Money invariants: `wallet_balances.balance` is the authoritative stored balance
-- (non-negative), one row per `(user_id, currency)`; `wallet_ledger` is the
-- append-only audit trail whose `id` is a single global monotonic value the
-- repository computes as `MAX(id) + 1` inside the change transaction
-- (`BEGIN IMMEDIATE` serializes it), so no AUTOINCREMENT is needed. Every change
-- appends one ledger row and updates the balance atomically. NO floating point.

CREATE TABLE IF NOT EXISTS wallet_balances (
    user_id            TEXT NOT NULL,
    currency           TEXT NOT NULL,
    balance            INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,

    PRIMARY KEY (user_id, currency),

    CHECK (balance >= 0)
);

CREATE TABLE IF NOT EXISTS wallet_ledger (
    id                 INTEGER NOT NULL,
    user_id            TEXT NOT NULL,
    currency           TEXT NOT NULL,
    delta              INTEGER NOT NULL,
    balance_after      INTEGER NOT NULL,
    reason             TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,

    PRIMARY KEY (id),

    CHECK (id > 0)
);

-- Supports the newest-first, per-user ledger reads.
CREATE INDEX IF NOT EXISTS wallet_ledger_user_id_idx
    ON wallet_ledger (user_id, id);

CREATE TABLE IF NOT EXISTS purchases (
    transaction_id              TEXT PRIMARY KEY,
    user_id                     TEXT NOT NULL,
    product_id                  TEXT NOT NULL,
    store                       TEXT NOT NULL,
    receipt_sha256              TEXT NOT NULL,
    validated_at_unix_ms        INTEGER NOT NULL,
    subscription_expiry_unix_ms INTEGER,

    CHECK (store IN ('apple', 'google', 'huawei', 'custom'))
);

-- Supports the user-filtered, newest-first purchase / subscription reads.
CREATE INDEX IF NOT EXISTS purchases_user_time_idx
    ON purchases (user_id, validated_at_unix_ms);
