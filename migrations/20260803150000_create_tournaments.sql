CREATE TABLE IF NOT EXISTS tournaments (
    id text COLLATE "C" PRIMARY KEY,
    leaderboard_id text COLLATE "C" NOT NULL REFERENCES leaderboards(id) ON DELETE RESTRICT,
    state text NOT NULL,
    registration_opens_at_unix_ms bigint NOT NULL,
    registration_closes_at_unix_ms bigint NOT NULL,
    starts_at_unix_ms bigint NOT NULL,
    ends_at_unix_ms bigint NOT NULL,
    settled_due_at_unix_ms bigint,
    created_at_unix_ms bigint NOT NULL,
    updated_at_unix_ms bigint NOT NULL,
    CHECK (registration_opens_at_unix_ms <= registration_closes_at_unix_ms),
    CHECK (registration_closes_at_unix_ms <= starts_at_unix_ms),
    CHECK (starts_at_unix_ms <= ends_at_unix_ms)
);
CREATE TABLE IF NOT EXISTS tournament_entries (
    tournament_id text COLLATE "C" NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    user_id text COLLATE "C" NOT NULL,
    registered_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (tournament_id, user_id)
);
CREATE TABLE IF NOT EXISTS tournament_results (
    tournament_id text COLLATE "C" NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    user_id text COLLATE "C" NOT NULL,
    rank bigint NOT NULL,
    score bigint NOT NULL,
    subscore bigint NOT NULL,
    PRIMARY KEY (tournament_id, user_id)
);
CREATE INDEX IF NOT EXISTS tournaments_lifecycle_idx ON tournaments (state, ends_at_unix_ms);
CREATE INDEX IF NOT EXISTS tournament_entries_tournament_idx ON tournament_entries (tournament_id, registered_at_unix_ms);
CREATE UNIQUE INDEX IF NOT EXISTS tournament_results_rank_uq ON tournament_results (tournament_id, rank);
