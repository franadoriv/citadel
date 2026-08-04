CREATE TABLE tournaments (
    id TEXT PRIMARY KEY,
    leaderboard_id TEXT NOT NULL REFERENCES leaderboards(id) ON DELETE RESTRICT,
    state TEXT NOT NULL,
    registration_opens_at_unix_ms INTEGER NOT NULL,
    registration_closes_at_unix_ms INTEGER NOT NULL,
    starts_at_unix_ms INTEGER NOT NULL,
    ends_at_unix_ms INTEGER NOT NULL,
    settled_due_at_unix_ms INTEGER,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    CHECK (registration_opens_at_unix_ms <= registration_closes_at_unix_ms),
    CHECK (registration_closes_at_unix_ms <= starts_at_unix_ms),
    CHECK (starts_at_unix_ms <= ends_at_unix_ms)
);
CREATE TABLE tournament_entries (
    tournament_id TEXT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    registered_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (tournament_id, user_id)
);
CREATE TABLE tournament_results (
    tournament_id TEXT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    rank INTEGER NOT NULL,
    score INTEGER NOT NULL,
    subscore INTEGER NOT NULL,
    PRIMARY KEY (tournament_id, user_id)
);
CREATE INDEX tournaments_lifecycle_idx ON tournaments (state, ends_at_unix_ms);
CREATE INDEX tournament_entries_tournament_idx ON tournament_entries (tournament_id, registered_at_unix_ms);
CREATE UNIQUE INDEX tournament_results_rank_uq ON tournament_results (tournament_id, rank);
