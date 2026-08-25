-- Cockroach-compatible console action trail. Never stores passwords, bearer
-- tokens, API-key secrets, console session tokens, or raw request/response
-- payloads: every call site sanitizes `details` by construction. `match_id` is
-- OPTIONAL and usually NULL: operator actions are not match-scoped and are
-- deliberately never forced into a match.
--
-- `audit_id` is time-ordered, so `ORDER BY audit_id DESC` is both the
-- newest-first order and its own deterministic tiebreak. That matters: a login
-- failure and the login that follows it share one millisecond timestamp.
CREATE TABLE IF NOT EXISTS console_audit_entries (
    audit_id      STRING PRIMARY KEY,
    node_id       STRING NOT NULL,
    time_unix_ms  INT8 NOT NULL,
    actor_type    STRING NOT NULL,
    actor         STRING NOT NULL,
    credential_id STRING,
    key_name      STRING,
    scopes_json   STRING,
    role          STRING NOT NULL,
    action        STRING NOT NULL,
    target        STRING NOT NULL,
    details       STRING NOT NULL,
    match_id      STRING
);

CREATE INDEX IF NOT EXISTS console_audit_entries_actor_idx     ON console_audit_entries (actor, audit_id);
CREATE INDEX IF NOT EXISTS console_audit_entries_action_idx    ON console_audit_entries (action, audit_id);
CREATE INDEX IF NOT EXISTS console_audit_entries_match_idx     ON console_audit_entries (match_id, audit_id);
CREATE INDEX IF NOT EXISTS console_audit_entries_retention_idx ON console_audit_entries (time_unix_ms);
