-- : SQLite groups (clans) and their membership.
--
-- Sibling to the Postgres migration in `../migrations`, backing
-- `repository::sqlite::groups` (`SqliteGroupsRepository`). The schema mirrors
-- that Postgres migration with SQLite-native types so the SAME groups contract
-- tests pass against both backends. Every SQLite-specific choice stays behind the
-- repository impl.
--
-- Dialect mapping vs the Postgres schema:
--   * `bigint GENERATED ALWAYS AS IDENTITY` -> `INTEGER PRIMARY KEY AUTOINCREMENT`;
--     both assign the id durably (AUTOINCREMENT never reuses a rowid). The
--     repository reads the new id with `last_insert_rowid`.
--   * `text COLLATE "C"` -> `TEXT`; SQLite's default BINARY collation is byte-wise,
--     matching Postgres `COLLATE "C"`.
--   * `boolean` -> `INTEGER` (0/1); sqlx encodes/decodes `bool` transparently.
--   * `bigint` (domain millis / max_size) -> `INTEGER`; SQLite has one integer
--     class and the u64/u32 round-trip is exact.
--
-- `updated_at_unix_ms` is set to `created_at_unix_ms` on insert and reserved for a
-- future update-with-clock path (the console `update` carries no timestamp).
-- `group_memberships` cascades on group delete; SQLite enforces this because the
-- connection is opened with `PRAGMA foreign_keys = ON`.

CREATE TABLE IF NOT EXISTS groups (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    name               TEXT NOT NULL UNIQUE,
    description        TEXT NOT NULL DEFAULT '',
    open               INTEGER NOT NULL DEFAULT 1,
    max_size           INTEGER NOT NULL DEFAULT 0,
    creator_id         TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,

    CHECK (trim(name) <> ''),
    CHECK (trim(creator_id) <> ''),
    CHECK (max_size >= 0)
);

CREATE TABLE IF NOT EXISTS group_memberships (
    group_id          INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    user_id           TEXT NOT NULL,
    role              TEXT NOT NULL,
    joined_at_unix_ms INTEGER NOT NULL,

    PRIMARY KEY (group_id, user_id),

    CHECK (trim(user_id) <> ''),
    CHECK (role IN ('member', 'admin', 'superadmin'))
);

-- Supports loading a group's member roll in join order.
CREATE INDEX IF NOT EXISTS group_memberships_group_idx
    ON group_memberships (group_id, joined_at_unix_ms, user_id);
