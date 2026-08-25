-- Durable match lifecycle record. Stores no participant identities, no account
-- ids, no session or transport identifiers, and no script output: live
-- membership stays in the room registry and per-match detail lives in
-- match_logs. `result_json` is author-supplied by the game script.
--
-- Deliberately no foreign key: this pool runs with `foreign_keys(true)` and the
-- write path is write-behind, so a child row can reach the database before its
-- parent. Referential integrity comes from flush ordering (matches open first)
-- and retention ordering (matches outlive the rows that reference them).
CREATE TABLE IF NOT EXISTS matches (
    match_id           TEXT PRIMARY KEY,
    node_id            TEXT NOT NULL,
    boot_id            TEXT NOT NULL,
    room_id            INTEGER NOT NULL,
    name               TEXT,
    map                TEXT NOT NULL,
    mode               TEXT NOT NULL,
    max_players        INTEGER NOT NULL,
    script_revision_id TEXT,
    script_generation  INTEGER,
    clock_epoch        INTEGER NOT NULL,
    opened_at_ms       INTEGER NOT NULL,
    closed_at_ms       INTEGER,
    termination_reason TEXT
        CHECK (termination_reason IS NULL OR termination_reason IN
              ('final_departure','server_closed','formation_abandoned')),
    peak_participants  INTEGER NOT NULL DEFAULT 0,
    join_total         INTEGER NOT NULL DEFAULT 0,
    result_json        TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    -- `room_id` is a per-process counter restarting at 1, so it identifies a
    -- match only together with the node and the boot that minted it.
    UNIQUE (node_id, boot_id, room_id)
);

CREATE INDEX IF NOT EXISTS matches_opened_idx ON matches (opened_at_ms, match_id);
CREATE INDEX IF NOT EXISTS matches_open_idx   ON matches (closed_at_ms, match_id);
