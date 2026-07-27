-- : every durable message state carries a revision and channel event.
ALTER TABLE chat_messages ADD COLUMN updated_at_unix_ms bigint NOT NULL DEFAULT 0;
ALTER TABLE chat_messages ADD COLUMN revision bigint NOT NULL DEFAULT 1;
ALTER TABLE chat_messages ADD COLUMN last_event_id bigint NOT NULL DEFAULT 0;
UPDATE chat_messages
SET updated_at_unix_ms = created_at_unix_ms,
    last_event_id = id
WHERE updated_at_unix_ms = 0 OR last_event_id = 0;

CREATE TABLE IF NOT EXISTS chat_events (
    channel_id text COLLATE "C" NOT NULL,
    event_id bigint NOT NULL,
    event_kind text COLLATE "C" NOT NULL,
    message_id bigint NOT NULL,
    revision bigint NOT NULL,
    occurred_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (channel_id, event_id),
    CONSTRAINT chat_events_kind_ck CHECK (event_kind IN ('created', 'updated', 'deleted'))
);
INSERT INTO chat_events (channel_id, event_id, event_kind, message_id, revision, occurred_at_unix_ms)
SELECT channel_id, id, 'created', id, revision, created_at_unix_ms
FROM chat_messages
ON CONFLICT (channel_id, event_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS chat_moderation_audit (
    audit_id bigserial PRIMARY KEY,
    occurred_at_unix_ms bigint NOT NULL,
    actor_kind text COLLATE "C" NOT NULL,
    actor_id_hash text COLLATE "C" NOT NULL,
    action text COLLATE "C" NOT NULL,
    reason_code text COLLATE "C" NOT NULL,
    channel_id_hash text COLLATE "C" NOT NULL,
    message_id bigint NOT NULL,
    author_id_hash text COLLATE "C" NOT NULL,
    authority_epoch bigint NOT NULL,
    correlation_id text COLLATE "C" NOT NULL,
    node_id text COLLATE "C" NOT NULL,
    CONSTRAINT chat_moderation_audit_action_ck CHECK (action = 'tombstone')
);
CREATE INDEX IF NOT EXISTS chat_moderation_audit_expiry_idx
ON chat_moderation_audit (occurred_at_unix_ms, audit_id);

CREATE TABLE IF NOT EXISTS chat_rate_limits (
    rate_key text COLLATE "C" NOT NULL,
    window_started_at_unix_ms bigint NOT NULL,
    used bigint NOT NULL,
    PRIMARY KEY (rate_key, window_started_at_unix_ms),
    CONSTRAINT chat_rate_limits_used_ck CHECK (used >= 0)
);
CREATE INDEX IF NOT EXISTS chat_rate_limits_expiry_idx
ON chat_rate_limits (window_started_at_unix_ms);
