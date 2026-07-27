-- : CockroachDB leaderboards and their per-user records (CRDB flavor of
-- the Postgres migration in `../migrations`).
--
-- CockroachDB speaks the PostgreSQL wire protocol, so Citadel reuses its Postgres
-- repository (`repository::pg::PgLeaderboardsRepository`) unchanged and only forks
-- the DDL where CRDB's dialect differs from PostgreSQL. The single difference vs
-- the Postgres schema is the removal of `COLLATE "C"`:
--
--   * PostgreSQL uses `COLLATE "C"` to force deterministic, byte-wise ordering
--     (independent of the server locale).
--   * CockroachDB rejects `COLLATE "C"` (`invalid locale C: language tag is not
--     well-formed`) — it only accepts ICU/language-tag collations. CRDB's default
--     `STRING`/`text` collation is ALREADY byte-wise/deterministic, so dropping
--     the clause yields the same ordering.
--
-- Board `id` is the caller-supplied opaque identifier (not a database serial), so
-- this schema needs no `GENERATED ALWAYS AS IDENTITY` — sidestepping CRDB's
-- identity-column quirks. Every other construct (jsonb, composite key, CHECK
-- constraints with `btrim`/`jsonb_typeof`, ON DELETE CASCADE) is supported by
-- CockroachDB and kept identical to the Postgres migration so the SAME
-- leaderboards contract tests pass against both backends.

CREATE TABLE IF NOT EXISTS leaderboards (
    id                 text PRIMARY KEY,
    sort_order         text NOT NULL,
    operator           text NOT NULL,
    reset_schedule     text,
    created_at_unix_ms bigint NOT NULL,

    CONSTRAINT leaderboards_id_ck CHECK (btrim(id) <> ''),
    CONSTRAINT leaderboards_sort_order_ck CHECK (sort_order IN ('asc', 'desc')),
    CONSTRAINT leaderboards_operator_ck CHECK (operator IN ('best', 'set', 'incr'))
);

CREATE TABLE IF NOT EXISTS leaderboard_records (
    leaderboard_id     text NOT NULL REFERENCES leaderboards(id) ON DELETE CASCADE,
    owner_id           text NOT NULL,
    score              bigint NOT NULL,
    subscore           bigint NOT NULL DEFAULT 0,
    metadata           jsonb,
    submissions        bigint NOT NULL DEFAULT 1,
    updated_at_unix_ms bigint NOT NULL,

    PRIMARY KEY (leaderboard_id, owner_id),

    CONSTRAINT leaderboard_records_owner_ck CHECK (btrim(owner_id) <> ''),
    CONSTRAINT leaderboard_records_submissions_ck CHECK (submissions >= 0),
    CONSTRAINT leaderboard_records_metadata_object_ck
        CHECK (metadata IS NULL OR jsonb_typeof(metadata) = 'object')
);

-- Supports loading (and counting) a board's records for rank derivation.
CREATE INDEX IF NOT EXISTS leaderboard_records_board_idx
    ON leaderboard_records (leaderboard_id);
