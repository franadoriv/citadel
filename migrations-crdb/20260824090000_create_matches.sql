-- Cockroach-compatible durable match lifecycle record. Stores no participant
-- identities, no account ids, no session or transport identifiers, and no
-- script output: live membership stays in the room registry and per-match
-- detail lives in match_logs. `result_json` is author-supplied by the game
-- script.
--
-- No foreign key references this table. The write path is write-behind, so a
-- child row can reach the database before its parent; referential integrity is
-- guaranteed by flush ordering (matches open first) and retention ordering
-- (matches are retained longer than the rows that reference them).
CREATE TABLE IF NOT EXISTS matches (
    match_id           STRING PRIMARY KEY,
    node_id            STRING NOT NULL,
    boot_id            STRING NOT NULL,
    room_id            INT8 NOT NULL,
    name               STRING,
    map                STRING NOT NULL,
    mode               STRING NOT NULL,
    max_players        INT4 NOT NULL,
    script_revision_id STRING,
    script_generation  INT8,
    clock_epoch        INT8 NOT NULL,
    opened_at_ms       INT8 NOT NULL,
    closed_at_ms       INT8,
    termination_reason STRING
        CHECK (termination_reason IS NULL OR termination_reason IN
              ('final_departure','server_closed','formation_abandoned')),
    peak_participants  INT4 NOT NULL DEFAULT 0,
    join_total         INT4 NOT NULL DEFAULT 0,
    result_json        JSONB,
    -- `room_id` is a per-process counter restarting at 1, so it identifies a
    -- match only together with the node and the boot that minted it.
    UNIQUE (node_id, boot_id, room_id)
);

CREATE INDEX IF NOT EXISTS matches_opened_idx ON matches (opened_at_ms, match_id);
CREATE INDEX IF NOT EXISTS matches_open_idx   ON matches (closed_at_ms, match_id);
