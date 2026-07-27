-- : SQLite console notification store.
--
-- Sibling to the Postgres migration in `../migrations`, backing
-- `repository::sqlite::notifications` (`SqliteNotificationsRepository`). The schema
-- mirrors that Postgres migration with SQLite-native types so the SAME
-- notifications contract tests pass against both backends. Every SQLite-specific
-- choice stays behind the repository impl.
--
-- Dialect mapping vs the Postgres schema:
--   * `text COLLATE "C"` -> `TEXT`; SQLite's default BINARY collation is byte-wise,
--     matching Postgres `COLLATE "C"`.
--   * `bigint` (id / millis) -> `INTEGER`; SQLite has one integer class and the
--     u64/i64 round-trip is exact.
--   * `jsonb content` -> `TEXT`; the repository serializes/deserializes the JSON
--     object at the boundary (matching `storage_objects.value`).
--   * `recipient_id` and `read_at_unix_ms` stay nullable (broadcast / unread).
--
-- The `id` is a single global monotonic value the repository computes as
-- `MAX(id) + 1` inside the enqueue transaction (`BEGIN IMMEDIATE` serializes it),
-- so no AUTOINCREMENT is needed.

CREATE TABLE IF NOT EXISTS notifications (
    id                 INTEGER NOT NULL,
    recipient_id       TEXT,
    subject            TEXT NOT NULL,
    content            TEXT NOT NULL,
    code               INTEGER NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    read_at_unix_ms    INTEGER,

    PRIMARY KEY (id),

    CHECK (id > 0)
);

-- Supports the visibility-filtered, newest-first reads.
CREATE INDEX IF NOT EXISTS notifications_recipient_time_idx
    ON notifications (recipient_id, created_at_unix_ms);
