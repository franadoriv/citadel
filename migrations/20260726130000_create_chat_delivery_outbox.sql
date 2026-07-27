-- : durable source records for bounded cross-node chat delivery.
-- Destination nodes are resolved from current leased advertisements at dispatch
-- time, so this table never stores a socket or participant capability.
CREATE TABLE IF NOT EXISTS chat_delivery_outbox (
    outbox_id bigserial PRIMARY KEY,
    channel_id text COLLATE "C" NOT NULL,
    event_id bigint NOT NULL,
    authority_epoch bigint NOT NULL,
    payload text NOT NULL,
    created_at_unix_ms bigint NOT NULL,
    expires_at_unix_ms bigint NOT NULL,
    CONSTRAINT chat_delivery_outbox_event_uq UNIQUE (channel_id, event_id),
    CONSTRAINT chat_delivery_outbox_expiry_ck CHECK (expires_at_unix_ms > created_at_unix_ms)
);
CREATE INDEX IF NOT EXISTS chat_delivery_outbox_expiry_idx
ON chat_delivery_outbox (expires_at_unix_ms, outbox_id);
