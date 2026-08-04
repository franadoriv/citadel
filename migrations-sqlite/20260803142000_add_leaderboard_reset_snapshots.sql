-- Immutable pre-reset records, keyed by the reset epoch that archived them.
CREATE TABLE leaderboard_reset_snapshot_records (
    leaderboard_id TEXT NOT NULL,
    due_at_unix_ms INTEGER NOT NULL,
    owner_id TEXT NOT NULL,
    score INTEGER NOT NULL,
    subscore INTEGER NOT NULL,
    metadata TEXT,
    submissions INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (leaderboard_id, due_at_unix_ms, owner_id),
    FOREIGN KEY (leaderboard_id, due_at_unix_ms)
        REFERENCES leaderboard_reset_epochs (leaderboard_id, due_at_unix_ms) ON DELETE CASCADE
);
