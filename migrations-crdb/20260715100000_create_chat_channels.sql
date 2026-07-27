-- : CockroachDB-compatible durable opaque chat descriptors.

CREATE TABLE IF NOT EXISTS chat_channels (
    channel_id       text PRIMARY KEY NOT NULL,
    channel_type     text NOT NULL,
    canonical_key    text NOT NULL UNIQUE,
    created_at_unix_ms bigint NOT NULL,

    CONSTRAINT chat_channels_type_ck CHECK (channel_type IN ('room', 'group', 'direct'))
);

CREATE INDEX IF NOT EXISTS chat_channels_type_idx ON chat_channels (channel_type);
