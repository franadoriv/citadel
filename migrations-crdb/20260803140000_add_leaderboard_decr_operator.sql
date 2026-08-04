-- CockroachDB flavor of the durable leaderboard decrement operator migration.
ALTER TABLE leaderboards DROP CONSTRAINT leaderboards_operator_ck;
ALTER TABLE leaderboards
    ADD CONSTRAINT leaderboards_operator_ck
    CHECK (operator IN ('best', 'set', 'incr', 'decr'));
