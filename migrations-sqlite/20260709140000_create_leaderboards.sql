-- : SQLite leaderboards and their per-user records.
--
-- Sibling to the Postgres migration in `../migrations`, backing
-- `repository::sqlite::leaderboards` (`SqliteLeaderboardsRepository`). The schema
-- mirrors that Postgres migration with SQLite-native types so the SAME
-- leaderboards contract tests pass against both backends. Every SQLite-specific
-- choice stays behind the repository impl.
--
-- Dialect mapping vs the Postgres schema:
--   * `text COLLATE "C"` -> `TEXT`; SQLite's default BINARY collation is byte-wise,
--     matching Postgres `COLLATE "C"`.
--   * `bigint` (score/subscore/submissions/millis) -> `INTEGER`; SQLite has one
--     integer class and the i64/u32/u64 round-trip is exact.
--   * `jsonb` metadata -> `TEXT` (the repository serializes JSON at the boundary,
--     matching the storage repository's JSON-as-TEXT choice).
--
-- `leaderboard_records` cascades on board delete; SQLite enforces this because the
-- connection is opened with `PRAGMA foreign_keys = ON`. Board `id` is the
-- caller-supplied opaque identifier (a `TEXT PRIMARY KEY`), so no AUTOINCREMENT is
-- needed.

CREATE TABLE IF NOT EXISTS leaderboards (
    id                 TEXT PRIMARY KEY,
    sort_order         TEXT NOT NULL,
    operator           TEXT NOT NULL,
    reset_schedule     TEXT,
    created_at_unix_ms INTEGER NOT NULL,

    CHECK (trim(id) <> ''),
    CHECK (sort_order IN ('asc', 'desc')),
    CHECK (operator IN ('best', 'set', 'incr'))
);

CREATE TABLE IF NOT EXISTS leaderboard_records (
    leaderboard_id     TEXT NOT NULL REFERENCES leaderboards(id) ON DELETE CASCADE,
    owner_id           TEXT NOT NULL,
    score              INTEGER NOT NULL,
    subscore           INTEGER NOT NULL DEFAULT 0,
    metadata           TEXT,
    submissions        INTEGER NOT NULL DEFAULT 1,
    updated_at_unix_ms INTEGER NOT NULL,

    PRIMARY KEY (leaderboard_id, owner_id),

    CHECK (trim(owner_id) <> ''),
    CHECK (submissions >= 0)
);

-- Supports loading (and counting) a board's records for rank derivation.
CREATE INDEX IF NOT EXISTS leaderboard_records_board_idx
    ON leaderboard_records (leaderboard_id);
