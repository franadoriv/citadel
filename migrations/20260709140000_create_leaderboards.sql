-- : leaderboards and their per-user records.
--
-- Backs `repository::pg::leaderboards` (`PgLeaderboardsRepository`). A board is
-- one `leaderboards` row (its fixed shape: sort order + score-write operator);
-- each user's score is one `leaderboard_records` row keyed by
-- `(leaderboard_id, owner_id)`. The score-write operator semantics (best/set/
-- incr) and the ranking are enforced in the repository's pure helpers
-- (`src/repository/leaderboards.rs`), shared across all three backends; the
-- schema only persists the authoritative records. Rank is derived on read; a
-- durable rank cache is intentionally out of scope (see technical-debt.md).
--
-- Notes on deliberate choices:
--
-- * `id` is the caller-supplied, already-validated board identifier (opaque
--   `text`, not a database serial), so no `GENERATED ALWAYS AS IDENTITY` is
--   needed and the CockroachDB flavor is DDL-identical apart from `COLLATE "C"`.
--   `text COLLATE "C"` gives deterministic, locale-independent equality (matching
--   `users`/`sessions`/`groups`).
-- * `sort_order` and `operator` store the same stable lowercase tokens the
--   `SortOrder`/`Operator` enums emit (`as_str`), parsed back with `from_token`;
--   CHECK constraints keep the columns self-describing.
-- * `reset_schedule` is stored verbatim and never executed (kept for a future
--   scheduled-reset task).
-- * `owner_id` is the record's user id; `score`/`subscore` are `bigint` (domain
--   `i64`); `submissions` is a `bigint` submission counter (domain `u32`);
--   `metadata` is nullable `jsonb`; `*_unix_ms` timestamps are domain
--   Unix-epoch millis stored as `bigint` for an exact round-trip.
-- * `leaderboard_records` references `leaderboards(id) ON DELETE CASCADE`, so
--   deleting a board removes its records in one statement.

CREATE TABLE IF NOT EXISTS leaderboards (
    id                 text COLLATE "C" PRIMARY KEY,
    sort_order         text NOT NULL,
    operator           text NOT NULL,
    reset_schedule     text,
    created_at_unix_ms bigint NOT NULL,

    CONSTRAINT leaderboards_id_ck CHECK (btrim(id) <> ''),
    CONSTRAINT leaderboards_sort_order_ck CHECK (sort_order IN ('asc', 'desc')),
    CONSTRAINT leaderboards_operator_ck CHECK (operator IN ('best', 'set', 'incr'))
);

CREATE TABLE IF NOT EXISTS leaderboard_records (
    leaderboard_id     text COLLATE "C" NOT NULL
                           REFERENCES leaderboards(id) ON DELETE CASCADE,
    owner_id           text COLLATE "C" NOT NULL,
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
