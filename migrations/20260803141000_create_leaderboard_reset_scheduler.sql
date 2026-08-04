-- Durable singleton lease, deduplicated reset epochs, and callback retry outbox.
CREATE TABLE IF NOT EXISTS leaderboard_reset_scheduler_lease (
    lease_key text PRIMARY KEY CHECK (lease_key = 'leaderboards'),
    node_id text NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    expires_at_unix_ms bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS leaderboard_reset_epochs (
    leaderboard_id text NOT NULL REFERENCES leaderboards(id) ON DELETE CASCADE,
    due_at_unix_ms bigint NOT NULL,
    fencing_token bigint NOT NULL,
    claimed_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (leaderboard_id, due_at_unix_ms)
);
CREATE TABLE IF NOT EXISTS leaderboard_reset_outbox (
    leaderboard_id text NOT NULL,
    due_at_unix_ms bigint NOT NULL,
    fencing_token bigint NOT NULL,
    created_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (leaderboard_id, due_at_unix_ms),
    FOREIGN KEY (leaderboard_id, due_at_unix_ms)
        REFERENCES leaderboard_reset_epochs (leaderboard_id, due_at_unix_ms) ON DELETE CASCADE
);
