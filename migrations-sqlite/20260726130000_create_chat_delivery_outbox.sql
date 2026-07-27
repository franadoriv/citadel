-- : durable source records for bounded cross-node chat delivery.
-- Destination nodes are resolved from current leased advertisements at dispatch
-- time, so this table never stores a socket or participant capability.
CREATE TABLE chat_delivery_outbox (
    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id TEXT NOT NULL,
    event_id INTEGER NOT NULL,
    authority_epoch INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    expires_at_unix_ms INTEGER NOT NULL CHECK (expires_at_unix_ms > created_at_unix_ms),
    UNIQUE (channel_id, event_id)
);
CREATE INDEX chat_delivery_outbox_expiry_idx
ON chat_delivery_outbox (expires_at_unix_ms, outbox_id);
