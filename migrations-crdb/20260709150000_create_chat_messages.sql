-- : CockroachDB chat channels and message history (CRDB flavor of the
-- Postgres migration in `../migrations`).
--
-- CockroachDB speaks the PostgreSQL wire protocol, so Citadel reuses its Postgres
-- repository (`repository::pg::PgChatRepository`) unchanged and only forks the DDL
-- where CRDB's dialect differs from PostgreSQL. The single difference vs the
-- Postgres schema is the removal of `COLLATE "C"`:
--
--   * PostgreSQL uses `COLLATE "C"` to force deterministic, byte-wise ordering
--     (independent of the server locale).
--   * CockroachDB rejects `COLLATE "C"` (`invalid locale C: language tag is not
--     well-formed`) — it only accepts ICU/language-tag collations. CRDB's default
--     `STRING`/`text` collation is ALREADY byte-wise/deterministic, so dropping
--     the clause yields the same ordering.
--
-- The message `id` is a per-channel, monotonic value the repository computes as
-- `MAX(id) + 1` inside the append transaction (not a database serial), so this
-- schema needs no `GENERATED ALWAYS AS IDENTITY` — sidestepping CRDB's
-- identity-column quirks. Every other construct (composite key, boolean flag,
-- CHECK constraints) is supported by CockroachDB and kept identical to the
-- Postgres migration so the SAME chat contract tests pass against both backends.

CREATE TABLE IF NOT EXISTS chat_messages (
    channel_id         text NOT NULL,
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
