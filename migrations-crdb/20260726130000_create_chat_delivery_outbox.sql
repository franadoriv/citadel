-- : durable source records for bounded cross-node chat delivery.
-- Destination nodes are resolved from current leased advertisements at dispatch
-- time, so this table never stores a socket or participant capability.
CREATE TABLE IF NOT EXISTS chat_delivery_outbox (
    outbox_id INT8 PRIMARY KEY DEFAULT unique_rowid(),
    channel_id STRING NOT NULL,
    event_id INT8 NOT NULL,
    authority_epoch INT8 NOT NULL,
    payload STRING NOT NULL,
    created_at_unix_ms INT8 NOT NULL,
    expires_at_unix_ms INT8 NOT NULL,
    CONSTRAINT chat_delivery_outbox_event_uq UNIQUE (channel_id, event_id),
    CONSTRAINT chat_delivery_outbox_expiry_ck CHECK (expires_at_unix_ms > created_at_unix_ms)
);
CREATE INDEX IF NOT EXISTS chat_delivery_outbox_expiry_idx
ON chat_delivery_outbox (expires_at_unix_ms, outbox_id);
