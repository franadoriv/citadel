-- : first SQLite migration — the storage objects table.
--
-- Sibling to the Postgres migration in `../migrations`, backing
-- `repository::sqlite::SqliteStorageRepository`. The schema mirrors the portable
-- domain types in `src/storage/mod.rs` and reproduces the Postgres storage table
-- with SQLite-native types so the SAME storage contract tests pass against both
-- backends. Every SQLite-specific choice stays behind the repository impl.
--
-- Dialect mapping vs the Postgres schema:
--   * `smallint`     -> `INTEGER` (SQLite has one integer class).
--   * `text`         -> `TEXT`; SQLite's default BINARY collation is byte-wise,
--                       matching Postgres `COLLATE "C"`, so keyset pagination
--                       orders identically without a locale dependency.
--   * `jsonb`        -> `TEXT` holding a JSON document. There is no `jsonb`; the
--                       repository serializes/deserializes JSON at the boundary.
--   * `timestamptz`  -> `INTEGER` unix seconds via `strftime` defaults. These
--                       audit columns are not read back by the repository (the
--                       domain carries no created/updated time), so the coarser
--                       default is intentional.
--
-- Owner encoding avoids NULLs in the primary key exactly like Postgres:
-- `Owner::System` is `(owner_kind = 0, owner_id = '')` and `Owner::User(id)` is
-- `(owner_kind = 1, owner_id = <id>)`.
--
-- JSON-shape and length/control-character checks that Postgres enforces with
-- `jsonb_typeof`/`octet_length`/regex are enforced by the domain layer
-- (`StorageValue`/`Collection`/`Key`) rather than restated here, because SQLite
-- has no portable regex operator and JSON1 availability is not guaranteed on
-- every build. The remaining invariants are cheap and portable, so they are kept
-- as table CHECKs (defense in depth identical to Postgres).

CREATE TABLE IF NOT EXISTS storage_objects (
    owner_kind       INTEGER NOT NULL,
    owner_id         TEXT NOT NULL,
    collection       TEXT NOT NULL,
    object_key       TEXT NOT NULL,

    value            TEXT NOT NULL,
    version          TEXT NOT NULL,
    read_permission  INTEGER NOT NULL,
    write_permission INTEGER NOT NULL,

    created_at       INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at       INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),

    PRIMARY KEY (owner_kind, owner_id, collection, object_key),

    CHECK ((owner_kind = 0 AND owner_id = '') OR (owner_kind = 1 AND owner_id <> '')),
    CHECK (collection <> ''),
    CHECK (object_key <> ''),
    CHECK (read_permission BETWEEN 0 AND 2),
    CHECK (write_permission BETWEEN 0 AND 1),
    -- Mirror Postgres `jsonb_typeof(value) = 'object'`: reject non-object / invalid
    -- JSON at the DB boundary too (defense in depth; the domain `StorageValue`
    -- already enforces this). sqlx bundles libsqlite3 with JSON1, so `json_valid`
    -- and `json_type` are available.
    CHECK (json_valid(value) AND json_type(value) = 'object')
);

-- Supports collection-scoped listing and keyset pagination in the same
-- (owner_kind, owner_id, object_key) order the repository uses for cursors.
CREATE INDEX IF NOT EXISTS storage_objects_collection_cursor_idx
    ON storage_objects (collection, owner_kind, owner_id, object_key);
