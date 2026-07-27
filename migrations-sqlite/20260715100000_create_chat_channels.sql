-- : durable, opaque descriptors for player-visible chat channels.

CREATE TABLE IF NOT EXISTS chat_channels (
    channel_id       TEXT PRIMARY KEY NOT NULL,
    channel_type     TEXT NOT NULL,
    canonical_key    TEXT NOT NULL UNIQUE,
    created_at_unix_ms INTEGER NOT NULL,

    CHECK (channel_type IN ('room', 'group', 'direct'))
);

CREATE INDEX IF NOT EXISTS chat_channels_type_idx ON chat_channels (channel_type);
