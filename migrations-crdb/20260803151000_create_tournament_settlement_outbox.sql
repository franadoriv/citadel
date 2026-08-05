-- Settlement and the reward/callback request share one database transaction.
-- Delivery is at-least-once; consumers deduplicate with tournament_id + due_at.
-- PostgreSQL-only `COLLATE "C"` clauses are deliberately omitted: CockroachDB
-- rejects them (`invalid locale C`) and its ordering is byte-wise already.
CREATE TABLE IF NOT EXISTS tournament_settlement_outbox (
    tournament_id text NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    leaderboard_id text NOT NULL,
    due_at_unix_ms bigint NOT NULL,
    created_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (tournament_id, due_at_unix_ms)
);
CREATE INDEX IF NOT EXISTS tournament_settlement_outbox_pending_idx
    ON tournament_settlement_outbox (created_at_unix_ms, tournament_id, due_at_unix_ms);
