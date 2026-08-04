-- Settlement and the reward/callback request share one SQLite transaction.
-- Delivery is at-least-once; consumers deduplicate with tournament_id + due_at.
CREATE TABLE tournament_settlement_outbox (
    tournament_id TEXT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    leaderboard_id TEXT NOT NULL,
    due_at_unix_ms INTEGER NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (tournament_id, due_at_unix_ms)
);
CREATE INDEX tournament_settlement_outbox_pending_idx
    ON tournament_settlement_outbox (created_at_unix_ms, tournament_id, due_at_unix_ms);
