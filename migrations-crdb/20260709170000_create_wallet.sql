-- : CockroachDB per-user wallets (balances + change ledger) and
-- validated purchases (CRDB flavor of the Postgres migration in `../migrations`).
--
-- CockroachDB speaks the PostgreSQL wire protocol, so Citadel reuses its Postgres
-- repositories (`repository::pg::PgWalletRepository`,
-- `repository::pg::PgPurchasesRepository`) unchanged and only forks the DDL where
-- CRDB's dialect differs from PostgreSQL. The single difference vs the Postgres
-- schema is the removal of `COLLATE "C"`:
--
--   * PostgreSQL uses `COLLATE "C"` to force deterministic, byte-wise ordering
--     (independent of the server locale).
--   * CockroachDB rejects `COLLATE "C"` (`invalid locale C: language tag is not
--     well-formed`) — it only accepts ICU/language-tag collations. CRDB's default
--     `STRING`/`text` collation is ALREADY byte-wise/deterministic, so dropping the
--     clause yields the same ordering.
--
-- `wallet_ledger.id` is a single global monotonic value the repository computes as
-- `MAX(id) + 1` inside the change transaction (not a database serial), so this
-- schema needs no `GENERATED ALWAYS AS IDENTITY` — sidestepping CRDB's
-- identity-column quirks. Money stays integer (`bigint`); the non-negative balance
-- CHECK, the store CHECK, and the composite keys are all supported by CockroachDB
-- and kept identical to the Postgres migration so the SAME contract tests pass.

CREATE TABLE IF NOT EXISTS wallet_balances (
    user_id            text NOT NULL,
    currency           text NOT NULL,
    balance            bigint NOT NULL,
    updated_at_unix_ms bigint NOT NULL,

    PRIMARY KEY (user_id, currency),

    CONSTRAINT wallet_balances_nonneg_ck CHECK (balance >= 0)
);

CREATE TABLE IF NOT EXISTS wallet_ledger (
    id                 bigint NOT NULL,
    user_id            text NOT NULL,
    currency           text NOT NULL,
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
    transaction_id              text PRIMARY KEY,
    user_id                     text NOT NULL,
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
