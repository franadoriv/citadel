-- Own live-delivery work by the node that committed it. Legacy rows cannot be
-- attributed safely, so they are discarded and clients recover from history.
-- The temporary empty default also makes rolling old-writer races fail closed:
-- exact-origin dispatch queries can never claim those rows.
ALTER TABLE chat_delivery_outbox
ADD COLUMN origin_node_id STRING NOT NULL DEFAULT '';

DELETE FROM chat_delivery_outbox WHERE origin_node_id = '';

ALTER TABLE chat_delivery_outbox
ALTER COLUMN origin_node_id DROP DEFAULT;

CREATE INDEX chat_delivery_outbox_origin_active_idx
ON chat_delivery_outbox (origin_node_id, expires_at_unix_ms, outbox_id);
