-- : every durable message state carries a revision and channel event.
ALTER TABLE chat_messages ADD COLUMN updated_at_unix_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE chat_messages ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE chat_messages ADD COLUMN last_event_id INTEGER NOT NULL DEFAULT 0;
UPDATE chat_messages
SET updated_at_unix_ms = created_at_unix_ms,
    last_event_id = id
WHERE updated_at_unix_ms = 0 OR last_event_id = 0;

CREATE TABLE chat_events (
    channel_id TEXT NOT NULL,
    event_id INTEGER NOT NULL,
    event_kind TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    occurred_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (channel_id, event_id),
    CHECK (event_kind IN ('created', 'updated', 'deleted'))
);
INSERT OR IGNORE INTO chat_events (channel_id, event_id, event_kind, message_id, revision, occurred_at_unix_ms)
SELECT channel_id, id, 'created', id, revision, created_at_unix_ms
FROM chat_messages;

CREATE TABLE chat_moderation_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at_unix_ms INTEGER NOT NULL,
    actor_kind TEXT NOT NULL,
    actor_id_hash TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action = 'tombstone'),
    reason_code TEXT NOT NULL,
    channel_id_hash TEXT NOT NULL,
    message_id INTEGER NOT NULL,
    author_id_hash TEXT NOT NULL,
    authority_epoch INTEGER NOT NULL,
    correlation_id TEXT NOT NULL,
    node_id TEXT NOT NULL
);
CREATE INDEX chat_moderation_audit_expiry_idx
ON chat_moderation_audit (occurred_at_unix_ms, audit_id);

CREATE TABLE chat_rate_limits (
    rate_key TEXT NOT NULL,
    window_started_at_unix_ms INTEGER NOT NULL,
    used INTEGER NOT NULL CHECK (used >= 0),
    PRIMARY KEY (rate_key, window_started_at_unix_ms)
);
CREATE INDEX chat_rate_limits_expiry_idx
ON chat_rate_limits (window_started_at_unix_ms);
