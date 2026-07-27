-- : durable, opaque descriptors for player-visible chat channels.
--
-- Legacy chat_messages channel ids remain operator-visible history only. New
-- player requests resolve a server-derived canonical subject through this table
-- and receive its random channel_id only after authorization succeeds.

CREATE TABLE IF NOT EXISTS chat_channels (
    channel_id       text COLLATE "C" PRIMARY KEY,
    channel_type     text COLLATE "C" NOT NULL,
    canonical_key    text COLLATE "C" NOT NULL UNIQUE,
    created_at_unix_ms bigint NOT NULL,

    CONSTRAINT chat_channels_type_ck CHECK (channel_type IN ('room', 'group', 'direct'))
);

CREATE INDEX IF NOT EXISTS chat_channels_type_idx ON chat_channels (channel_type);
