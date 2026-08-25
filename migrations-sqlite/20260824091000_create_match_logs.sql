-- Free-form game-script log stream. `payload_json` is author-supplied and is
-- deliberately NOT redacted: operators see exactly what their own game script
-- wrote. The server adds no credentials, bearer tokens, session ids, or
-- transport identifiers to any column here. `match_id` is optional: logs
-- written outside a match-scoped callback carry NULL, and no foreign key ties
-- this table to `matches` (see that migration for why).
CREATE TABLE IF NOT EXISTS match_logs (
    log_id        TEXT PRIMARY KEY,
    match_id      TEXT,
    node_id       TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    level         TEXT NOT NULL
        CHECK (level IN ('trace','debug','info','warn','error')),
    tag           TEXT NOT NULL
        CHECK (length(CAST(tag AS BLOB)) BETWEEN 1 AND 64),
    message       TEXT NOT NULL
        CHECK (length(CAST(message AS BLOB)) BETWEEN 1 AND 1024),
    payload_json  TEXT CHECK (payload_json IS NULL OR json_valid(payload_json))
);

-- `log_id` is already time-ordered, so keyset paging rides the primary key.
-- The retention index exists solely for the bounded prune predicate.
CREATE INDEX IF NOT EXISTS match_logs_match_idx     ON match_logs (match_id, log_id);
CREATE INDEX IF NOT EXISTS match_logs_level_idx     ON match_logs (level, log_id);
CREATE INDEX IF NOT EXISTS match_logs_retention_idx ON match_logs (created_at_ms);
