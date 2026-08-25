-- Durable match lifecycle record. Stores no participant identities, no account
-- ids, no session or transport identifiers, and no script output: live
-- membership stays in the room registry and per-match detail lives in
-- match_logs. `result_json` is author-supplied by the game script.
--
-- No foreign key references this table. The write path is write-behind, so a
-- child row can reach the database before its parent; referential integrity is
-- guaranteed by flush ordering (matches open first) and retention ordering
-- (matches are retained longer than the rows that reference them).
CREATE TABLE IF NOT EXISTS matches (
    match_id           TEXT COLLATE "C" PRIMARY KEY,
    node_id            TEXT COLLATE "C" NOT NULL,
    boot_id            TEXT COLLATE "C" NOT NULL,
    room_id            BIGINT NOT NULL,
    name               TEXT,
    map                TEXT COLLATE "C" NOT NULL,
    mode               TEXT COLLATE "C" NOT NULL,
    max_players        INTEGER NOT NULL,
    script_revision_id TEXT COLLATE "C",
    script_generation  BIGINT,
    clock_epoch        BIGINT NOT NULL,
    opened_at_ms       BIGINT NOT NULL,
    closed_at_ms       BIGINT,
    termination_reason TEXT COLLATE "C"
        CHECK (termination_reason IS NULL OR termination_reason IN
              ('final_departure','server_closed','formation_abandoned')),
    peak_participants  INTEGER NOT NULL DEFAULT 0,
    join_total         INTEGER NOT NULL DEFAULT 0,
    result_json        JSONB,
    -- `room_id` is a per-process counter restarting at 1, so it identifies a
    -- match only together with the node and the boot that minted it.
    UNIQUE (node_id, boot_id, room_id)
);

CREATE INDEX IF NOT EXISTS matches_opened_idx ON matches (opened_at_ms, match_id);
CREATE INDEX IF NOT EXISTS matches_open_idx   ON matches (closed_at_ms, match_id);
