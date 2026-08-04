-- SQLite cannot alter a CHECK constraint in place. Rebuild the parent and child
-- tables transactionally, preserving every leaderboard record and its cascade.
CREATE TABLE leaderboards_decr (
    id                 TEXT PRIMARY KEY,
    sort_order         TEXT NOT NULL,
    operator           TEXT NOT NULL,
    reset_schedule     TEXT,
    created_at_unix_ms INTEGER NOT NULL,
    CHECK (trim(id) <> ''),
    CHECK (sort_order IN ('asc', 'desc')),
    CHECK (operator IN ('best', 'set', 'incr', 'decr'))
);
INSERT INTO leaderboards_decr
    (id, sort_order, operator, reset_schedule, created_at_unix_ms)
SELECT id, sort_order, operator, reset_schedule, created_at_unix_ms
FROM leaderboards;

CREATE TABLE leaderboard_records_decr (
    leaderboard_id     TEXT NOT NULL REFERENCES leaderboards_decr(id) ON DELETE CASCADE,
    owner_id           TEXT NOT NULL,
    score              INTEGER NOT NULL,
    subscore           INTEGER NOT NULL DEFAULT 0,
    metadata           TEXT,
    submissions        INTEGER NOT NULL DEFAULT 1,
    updated_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (leaderboard_id, owner_id),
    CHECK (trim(owner_id) <> ''),
    CHECK (submissions >= 0),
    CHECK (metadata IS NULL OR (json_valid(metadata) AND json_type(metadata) = 'object'))
);
INSERT INTO leaderboard_records_decr
    (leaderboard_id, owner_id, score, subscore, metadata, submissions, updated_at_unix_ms)
SELECT leaderboard_id, owner_id, score, subscore, metadata, submissions, updated_at_unix_ms
FROM leaderboard_records;

DROP TABLE leaderboard_records;
DROP TABLE leaderboards;
ALTER TABLE leaderboards_decr RENAME TO leaderboards;
ALTER TABLE leaderboard_records_decr RENAME TO leaderboard_records;
CREATE INDEX leaderboard_records_board_idx ON leaderboard_records (leaderboard_id);
