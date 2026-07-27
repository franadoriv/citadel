-- : CockroachDB storage objects table (CRDB flavor of the Postgres
-- migration in `../migrations`).
--
-- CockroachDB speaks the PostgreSQL wire protocol, so Citadel reuses its Postgres
-- repository (`repository::pg::PgStorageRepository`) unchanged and only forks the
-- DDL where CRDB's dialect differs from PostgreSQL. The single difference vs the
-- Postgres schema is the removal of `COLLATE "C"`:
--
--   * PostgreSQL uses `COLLATE "C"` to force deterministic, byte-wise ordering
--     (independent of the server locale) for keyset pagination.
--   * CockroachDB rejects `COLLATE "C"` (`invalid locale C: language tag is not
--     well-formed`) — it only accepts ICU/language-tag collations. CRDB's default
--     `STRING`/`text` collation is ALREADY byte-wise/deterministic, so dropping
--     the clause yields the exact same ordering the repository's cursors rely on.
--
-- Every other construct here (jsonb, composite key, CHECK constraints with
-- `octet_length`/`jsonb_typeof`/POSIX regex, partial/covering indexes) is
-- supported by CockroachDB and is kept identical to the Postgres migration so the
-- SAME storage contract tests pass against both backends.

CREATE TABLE IF NOT EXISTS storage_objects (
    owner_kind       smallint NOT NULL,
    owner_id         text NOT NULL,
    collection       text NOT NULL,
    object_key       text NOT NULL,

    value            jsonb NOT NULL,
    version          text NOT NULL,
    read_permission  smallint NOT NULL,
    write_permission smallint NOT NULL,

    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (owner_kind, owner_id, collection, object_key),

    CONSTRAINT storage_objects_owner_kind_ck
        CHECK ((owner_kind = 0 AND owner_id = '') OR (owner_kind = 1 AND owner_id <> '')),
    CONSTRAINT storage_objects_collection_ck
        CHECK (collection <> '' AND octet_length(collection) <= 128 AND collection !~ '[[:cntrl:]]'),
    CONSTRAINT storage_objects_object_key_ck
        CHECK (object_key <> '' AND octet_length(object_key) <= 128 AND object_key !~ '[[:cntrl:]]'),
    CONSTRAINT storage_objects_value_object_ck
        CHECK (jsonb_typeof(value) = 'object'),
    CONSTRAINT storage_objects_read_perm_ck
        CHECK (read_permission BETWEEN 0 AND 2),
    CONSTRAINT storage_objects_write_perm_ck
        CHECK (write_permission BETWEEN 0 AND 1)
);

-- Supports collection-scoped listing and keyset pagination in the same
-- (owner_kind, owner_id, object_key) order the repository uses for cursors.
CREATE INDEX IF NOT EXISTS storage_objects_collection_cursor_idx
    ON storage_objects (collection, owner_kind, owner_id, object_key);
