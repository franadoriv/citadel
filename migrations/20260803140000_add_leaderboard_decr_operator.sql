-- Permit the durable leaderboard decrement operator without rewriting the
-- historical create-table migration (which may already have been checksummed).
ALTER TABLE leaderboards DROP CONSTRAINT leaderboards_operator_ck;
ALTER TABLE leaderboards
    ADD CONSTRAINT leaderboards_operator_ck
    CHECK (operator IN ('best', 'set', 'incr', 'decr'));
