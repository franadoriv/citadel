-- : chat channels and their per-channel bounded message history.
--
-- Backs `repository::pg::chat` (`PgChatRepository`). There is deliberately no
-- separate `channels` table: a channel exists iff a row bears its id, its fixed
-- type is denormalized onto every message row (constant per channel), and its
-- activity summary (`messages` = the monotonic append counter, `last_activity`)
-- is derived from the retained rows. Because eviction removes only the *oldest*
-- rows, the newest row is always retained, so `MAX(id)` is the channel's
-- total-ever-appended counter even after eviction. The retention/eviction bound,
-- the newest-first paging, and the channel listing sort live in the repository's
-- pure helpers (`src/repository/chat.rs`), shared across all three backends.
--
-- Notes on deliberate choices:
--
-- * `(channel_id, id)` is the composite primary key. `id` is a per-channel,
--   monotonic sequence computed by the repository as `MAX(id) + 1` inside the
--   append transaction (NOT a database serial / `GENERATED ALWAYS AS IDENTITY`),
--   so the CockroachDB flavor is DDL-identical apart from `COLLATE "C"` and there
--   are no cross-backend identity quirks.
-- * `channel_type` stores the same stable lowercase tokens the `ChannelType` enum
--   emits (`as_str`), parsed back with `from_token`; a CHECK keeps it
--   self-describing. It is denormalized onto every row and is constant per channel.
-- * `deleted` is the tombstone flag: a moderated message keeps its row (with
--   `content` blanked) so ids and paging stay contiguous.
-- * `*_unix_ms` timestamps are domain Unix-epoch millis stored as `bigint` for an
--   exact round-trip.
-- * `text COLLATE "C"` gives deterministic, locale-independent equality (matching
--   `users`/`sessions`/`groups`/`leaderboards`).

CREATE TABLE IF NOT EXISTS chat_messages (
    channel_id         text COLLATE "C" NOT NULL,
    id                 bigint NOT NULL,
    channel_type       text NOT NULL,
    sender_id          text NOT NULL,
    content            text NOT NULL,
    deleted            boolean NOT NULL DEFAULT false,
    created_at_unix_ms bigint NOT NULL,

    PRIMARY KEY (channel_id, id),

    CONSTRAINT chat_messages_id_ck CHECK (id > 0),
    CONSTRAINT chat_messages_channel_type_ck CHECK (channel_type IN ('room', 'group', 'direct'))
);

-- Supports the per-channel history reads and the activity aggregation.
CREATE INDEX IF NOT EXISTS chat_messages_channel_time_idx
    ON chat_messages (channel_id, created_at_unix_ms);
