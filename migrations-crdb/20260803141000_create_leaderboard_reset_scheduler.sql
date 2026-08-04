CREATE TABLE IF NOT EXISTS leaderboard_reset_scheduler_lease (
    lease_key STRING PRIMARY KEY CHECK (lease_key = 'leaderboards'),
    node_id STRING NOT NULL,
    fencing_token INT8 NOT NULL CHECK (fencing_token > 0),
    expires_at_unix_ms INT8 NOT NULL
);
CREATE TABLE IF NOT EXISTS leaderboard_reset_epochs (
    leaderboard_id STRING NOT NULL REFERENCES leaderboards(id) ON DELETE CASCADE,
    due_at_unix_ms INT8 NOT NULL,
    fencing_token INT8 NOT NULL,
    claimed_at_unix_ms INT8 NOT NULL,
    PRIMARY KEY (leaderboard_id, due_at_unix_ms)
);
CREATE TABLE IF NOT EXISTS leaderboard_reset_outbox (
    leaderboard_id STRING NOT NULL,
    due_at_unix_ms INT8 NOT NULL,
    fencing_token INT8 NOT NULL,
    created_at_unix_ms INT8 NOT NULL,
    PRIMARY KEY (leaderboard_id, due_at_unix_ms),
    FOREIGN KEY (leaderboard_id, due_at_unix_ms)
        REFERENCES leaderboard_reset_epochs (leaderboard_id, due_at_unix_ms) ON DELETE CASCADE
);
