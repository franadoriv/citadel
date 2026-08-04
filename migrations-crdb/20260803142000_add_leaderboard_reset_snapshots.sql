-- Immutable pre-reset records, keyed by the reset epoch that archived them.
CREATE TABLE IF NOT EXISTS leaderboard_reset_snapshot_records (
    leaderboard_id STRING NOT NULL,
    due_at_unix_ms INT8 NOT NULL,
    owner_id STRING NOT NULL,
    score INT8 NOT NULL,
    subscore INT8 NOT NULL,
    metadata JSONB,
    submissions INT8 NOT NULL,
    updated_at_unix_ms INT8 NOT NULL,
    PRIMARY KEY (leaderboard_id, due_at_unix_ms, owner_id),
    FOREIGN KEY (leaderboard_id, due_at_unix_ms)
        REFERENCES leaderboard_reset_epochs (leaderboard_id, due_at_unix_ms) ON DELETE CASCADE
);
