CREATE TABLE IF NOT EXISTS tournaments (
    id STRING PRIMARY KEY,
    leaderboard_id STRING NOT NULL REFERENCES leaderboards(id) ON DELETE RESTRICT,
    state STRING NOT NULL,
    registration_opens_at_unix_ms INT8 NOT NULL,
    registration_closes_at_unix_ms INT8 NOT NULL,
    starts_at_unix_ms INT8 NOT NULL,
    ends_at_unix_ms INT8 NOT NULL,
    settled_due_at_unix_ms INT8,
    created_at_unix_ms INT8 NOT NULL,
    updated_at_unix_ms INT8 NOT NULL,
    CHECK (registration_opens_at_unix_ms <= registration_closes_at_unix_ms),
    CHECK (registration_closes_at_unix_ms <= starts_at_unix_ms),
    CHECK (starts_at_unix_ms <= ends_at_unix_ms)
);
CREATE TABLE IF NOT EXISTS tournament_entries (
    tournament_id STRING NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    user_id STRING NOT NULL,
    registered_at_unix_ms INT8 NOT NULL,
    PRIMARY KEY (tournament_id, user_id)
);
CREATE TABLE IF NOT EXISTS tournament_results (
    tournament_id STRING NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    user_id STRING NOT NULL,
    rank INT8 NOT NULL,
    score INT8 NOT NULL,
    subscore INT8 NOT NULL,
    PRIMARY KEY (tournament_id, user_id)
);
CREATE INDEX IF NOT EXISTS tournaments_lifecycle_idx ON tournaments (state, ends_at_unix_ms);
CREATE INDEX IF NOT EXISTS tournament_entries_tournament_idx ON tournament_entries (tournament_id, registered_at_unix_ms);
CREATE UNIQUE INDEX IF NOT EXISTS tournament_results_rank_uq ON tournament_results (tournament_id, rank);
