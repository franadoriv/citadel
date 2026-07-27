-- : every durable message state carries a revision and channel event.
-- Cockroach may return before an asynchronous schema backfill has completed.
-- `IF NOT EXISTS` makes a safe migration-runner retry resume after that job
-- instead of failing on an already-created first column.
ALTER TABLE chat_messages
    ADD COLUMN IF NOT EXISTS updated_at_unix_ms INT8 NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS revision INT8 NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_event_id INT8 NOT NULL DEFAULT 0;
-- Cockroach applies the preceding schema change asynchronously. New rows get
-- the safe defaults above; do not read the just-added columns in this same
-- migration, because that races the backfill job on current CockroachDB.

CREATE TABLE IF NOT EXISTS chat_events (
    channel_id STRING NOT NULL,
    event_id INT8 NOT NULL,
    event_kind STRING NOT NULL,
    message_id INT8 NOT NULL,
    revision INT8 NOT NULL,
    occurred_at_unix_ms INT8 NOT NULL,
    PRIMARY KEY (channel_id, event_id),
    CONSTRAINT chat_events_kind_ck CHECK (event_kind IN ('created', 'updated', 'deleted'))
);
INSERT INTO chat_events (channel_id, event_id, event_kind, message_id, revision, occurred_at_unix_ms)
SELECT channel_id, id, 'created', id, revision, created_at_unix_ms
FROM chat_messages
ON CONFLICT (channel_id, event_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS chat_moderation_audit (
    audit_id INT8 PRIMARY KEY DEFAULT unique_rowid(),
    occurred_at_unix_ms INT8 NOT NULL,
    actor_kind STRING NOT NULL,
    actor_id_hash STRING NOT NULL,
    action STRING NOT NULL CHECK (action = 'tombstone'),
    reason_code STRING NOT NULL,
    channel_id_hash STRING NOT NULL,
    message_id INT8 NOT NULL,
    author_id_hash STRING NOT NULL,
    authority_epoch INT8 NOT NULL,
    correlation_id STRING NOT NULL,
    node_id STRING NOT NULL
);
CREATE INDEX IF NOT EXISTS chat_moderation_audit_expiry_idx
ON chat_moderation_audit (occurred_at_unix_ms, audit_id);

CREATE TABLE IF NOT EXISTS chat_rate_limits (
    rate_key STRING NOT NULL,
    window_started_at_unix_ms INT8 NOT NULL,
    used INT8 NOT NULL CHECK (used >= 0),
    PRIMARY KEY (rate_key, window_started_at_unix_ms)
);
CREATE INDEX IF NOT EXISTS chat_rate_limits_expiry_idx
ON chat_rate_limits (window_started_at_unix_ms);
