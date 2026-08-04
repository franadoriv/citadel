-- Immutable pre-reset records, keyed by the reset epoch that archived them.
CREATE TABLE IF NOT EXISTS leaderboard_reset_snapshot_records (
    leaderboard_id text COLLATE "C" NOT NULL,
    due_at_unix_ms bigint NOT NULL,
    owner_id text COLLATE "C" NOT NULL,
    score bigint NOT NULL,
    subscore bigint NOT NULL,
    metadata jsonb,
    submissions bigint NOT NULL,
    updated_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (leaderboard_id, due_at_unix_ms, owner_id),
    FOREIGN KEY (leaderboard_id, due_at_unix_ms)
        REFERENCES leaderboard_reset_epochs (leaderboard_id, due_at_unix_ms) ON DELETE CASCADE
);
