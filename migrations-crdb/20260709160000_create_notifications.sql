-- : CockroachDB console notification store (CRDB flavor of the Postgres
-- migration in `../migrations`).
--
-- CockroachDB speaks the PostgreSQL wire protocol, so Citadel reuses its Postgres
-- repository (`repository::pg::PgNotificationsRepository`) unchanged and only forks
-- the DDL where CRDB's dialect differs from PostgreSQL. The single difference vs
-- the Postgres schema is the removal of `COLLATE "C"`:
--
--   * PostgreSQL uses `COLLATE "C"` to force deterministic, byte-wise ordering
--     (independent of the server locale).
--   * CockroachDB rejects `COLLATE "C"` (`invalid locale C: language tag is not
--     well-formed`) — it only accepts ICU/language-tag collations. CRDB's default
--     `STRING`/`text` collation is ALREADY byte-wise/deterministic, so dropping the
--     clause yields the same ordering.
--
-- The `id` is a single global monotonic value the repository computes as
-- `MAX(id) + 1` inside the enqueue transaction (not a database serial), so this
-- schema needs no `GENERATED ALWAYS AS IDENTITY` — sidestepping CRDB's
-- identity-column quirks. Every other construct (nullable recipient/read columns,
-- the jsonb object CHECK) is supported by CockroachDB and kept identical to the
-- Postgres migration so the SAME notifications contract tests pass against both
-- backends.

CREATE TABLE IF NOT EXISTS notifications (
    id                 bigint NOT NULL,
    recipient_id       text,
    subject            text NOT NULL,
    content            jsonb NOT NULL,
    -- CockroachDB's `integer` alias is INT8, unlike PostgreSQL's INT4. The
    -- public notification contract is an i32, so pin the physical type to INT4.
    code               int4 NOT NULL,
    created_at_unix_ms bigint NOT NULL,
    read_at_unix_ms    bigint,

    PRIMARY KEY (id),

    CONSTRAINT notifications_id_ck CHECK (id > 0),
    CONSTRAINT notifications_content_object_ck CHECK (jsonb_typeof(content) = 'object')
);

-- Supports the visibility-filtered, newest-first reads.
CREATE INDEX IF NOT EXISTS notifications_recipient_time_idx
    ON notifications (recipient_id, created_at_unix_ms);
