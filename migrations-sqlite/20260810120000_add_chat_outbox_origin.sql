-- Own live-delivery work by the node that committed it. Legacy rows cannot be
-- attributed safely, so they are discarded and clients recover from history.
-- Keeping an empty default makes concurrent rolling old writers fail closed:
-- exact-origin dispatch queries never claim their unattributed rows.
ALTER TABLE chat_delivery_outbox
ADD COLUMN origin_node_id TEXT NOT NULL DEFAULT '';

DELETE FROM chat_delivery_outbox WHERE origin_node_id = '';

CREATE INDEX chat_delivery_outbox_origin_active_idx
ON chat_delivery_outbox (origin_node_id, expires_at_unix_ms, outbox_id);
