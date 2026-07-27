-- : SQLite chat channels and message history.
--
-- Sibling to the Postgres migration in `../migrations`, backing
-- `repository::sqlite::chat` (`SqliteChatRepository`). The schema mirrors that
-- Postgres migration with SQLite-native types so the SAME chat contract tests
-- pass against both backends. Every SQLite-specific choice stays behind the
-- repository impl.
--
-- Dialect mapping vs the Postgres schema:
--   * `text COLLATE "C"` -> `TEXT`; SQLite's default BINARY collation is byte-wise,
--     matching Postgres `COLLATE "C"`.
--   * `bigint` (id / millis) -> `INTEGER`; SQLite has one integer class and the
--     u64/i64 round-trip is exact.
--   * `boolean deleted` -> `INTEGER` (0/1); the repository binds/decodes it as a
--     bool at the boundary.
--
-- The message `id` is a per-channel monotonic value the repository computes as
-- `MAX(id) + 1` inside the append transaction (`BEGIN IMMEDIATE` serializes it),
-- so no AUTOINCREMENT is needed.

CREATE TABLE IF NOT EXISTS chat_messages (
    channel_id         TEXT NOT NULL,
    id                 INTEGER NOT NULL,
    channel_type       TEXT NOT NULL,
    sender_id          TEXT NOT NULL,
    content            TEXT NOT NULL,
    deleted            INTEGER NOT NULL DEFAULT 0,
    created_at_unix_ms INTEGER NOT NULL,

    PRIMARY KEY (channel_id, id),

    CHECK (id > 0),
    CHECK (channel_type IN ('room', 'group', 'direct'))
);

-- Supports the per-channel history reads and the activity aggregation.
CREATE INDEX IF NOT EXISTS chat_messages_channel_time_idx
    ON chat_messages (channel_id, created_at_unix_ms);
