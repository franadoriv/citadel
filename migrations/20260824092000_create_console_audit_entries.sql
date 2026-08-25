-- Console action trail. Never stores passwords, bearer tokens, API-key secrets,
-- console session tokens, or raw request/response payloads: every call site
-- sanitizes `details` by construction. `match_id` is OPTIONAL and usually NULL:
-- operator actions are not match-scoped and are deliberately never forced into
-- a match.
--
-- `audit_id` is time-ordered, so `ORDER BY audit_id DESC` is both the
-- newest-first order and its own deterministic tiebreak. That matters: a login
-- failure and the login that follows it share one millisecond timestamp.
CREATE TABLE IF NOT EXISTS console_audit_entries (
    audit_id      TEXT COLLATE "C" PRIMARY KEY,
    node_id       TEXT COLLATE "C" NOT NULL,
    time_unix_ms  BIGINT NOT NULL,
    actor_type    TEXT COLLATE "C" NOT NULL,
    actor         TEXT COLLATE "C" NOT NULL,
    credential_id TEXT COLLATE "C",
    key_name      TEXT,
    scopes_json   TEXT,
    role          TEXT COLLATE "C" NOT NULL,
    action        TEXT COLLATE "C" NOT NULL,
    target        TEXT NOT NULL,
    details       TEXT NOT NULL,
    match_id      TEXT COLLATE "C"
);

CREATE INDEX IF NOT EXISTS console_audit_entries_actor_idx     ON console_audit_entries (actor, audit_id);
CREATE INDEX IF NOT EXISTS console_audit_entries_action_idx    ON console_audit_entries (action, audit_id);
CREATE INDEX IF NOT EXISTS console_audit_entries_match_idx     ON console_audit_entries (match_id, audit_id);
CREATE INDEX IF NOT EXISTS console_audit_entries_retention_idx ON console_audit_entries (time_unix_ms);
