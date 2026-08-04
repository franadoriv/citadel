CREATE TABLE leaderboard_reset_scheduler_lease (
    lease_key TEXT PRIMARY KEY CHECK (lease_key = 'leaderboards'),
    node_id TEXT NOT NULL,
    fencing_token INTEGER NOT NULL CHECK (fencing_token > 0),
    expires_at_unix_ms INTEGER NOT NULL
);
CREATE TABLE leaderboard_reset_epochs (
    leaderboard_id TEXT NOT NULL REFERENCES leaderboards(id) ON DELETE CASCADE,
    due_at_unix_ms INTEGER NOT NULL,
    fencing_token INTEGER NOT NULL,
    claimed_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (leaderboard_id, due_at_unix_ms)
);
CREATE TABLE leaderboard_reset_outbox (
    leaderboard_id TEXT NOT NULL,
    due_at_unix_ms INTEGER NOT NULL,
    fencing_token INTEGER NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (leaderboard_id, due_at_unix_ms),
    FOREIGN KEY (leaderboard_id, due_at_unix_ms)
        REFERENCES leaderboard_reset_epochs (leaderboard_id, due_at_unix_ms) ON DELETE CASCADE
);
