-- : the console notification store.
--
-- Backs `repository::pg::notifications` (`PgNotificationsRepository`). All state
-- lives in one `notifications` table: each row is one notification, addressed to a
-- single account (`recipient_id`) or to everyone (a broadcast, `recipient_id IS
-- NULL`). A reader sees their own targeted notifications plus every broadcast; the
-- unfiltered operator view sees everything. The capacity/eviction bound and the
-- visibility-filtered newest-first paging live in the repository's pure helpers
-- (`src/repository/notifications.rs`), shared across all three backends.
--
-- Notes on deliberate choices:
--
-- * `id` is a single global monotonic sequence computed by the repository as
--   `MAX(id) + 1` inside the enqueue transaction (NOT a database serial /
--   `GENERATED ALWAYS AS IDENTITY`), so the CockroachDB flavor is DDL-identical
--   apart from `COLLATE "C"` and there are no cross-backend identity quirks.
-- * `recipient_id` is nullable: `NULL` is a broadcast; a non-null value targets one
--   account by user id. The bound is a single global ring (the oldest rows are
--   evicted beyond the retention capacity), matching the original in-process store.
-- * `content` is `jsonb` and constrained to a JSON object, mirroring
--   `storage_objects.value`.
-- * `read_at_unix_ms` is nullable: `NULL` is unread, a value records when the
--   recipient marked it read (the `Notification.read` bool is derived from it).
-- * `*_unix_ms` timestamps are domain Unix-epoch millis stored as `bigint` for an
--   exact round-trip.
-- * `text COLLATE "C"` gives deterministic, locale-independent equality (matching
--   `users`/`sessions`/`groups`/`leaderboards`/`chat_messages`).

CREATE TABLE IF NOT EXISTS notifications (
    id                 bigint NOT NULL,
    recipient_id       text COLLATE "C",
    subject            text NOT NULL,
    content            jsonb NOT NULL,
    code               integer NOT NULL,
    created_at_unix_ms bigint NOT NULL,
    read_at_unix_ms    bigint,

    PRIMARY KEY (id),

    CONSTRAINT notifications_id_ck CHECK (id > 0),
    CONSTRAINT notifications_content_object_ck CHECK (jsonb_typeof(content) = 'object')
);

-- Supports the visibility-filtered, newest-first reads.
CREATE INDEX IF NOT EXISTS notifications_recipient_time_idx
    ON notifications (recipient_id, created_at_unix_ms);
