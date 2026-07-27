-- : first Postgres migration — the storage objects table.
--
-- Backs `repository::pg::PgStorageRepository` (the Postgres implementation of the
-- `StorageRepository` contract). The schema deliberately mirrors the portable
-- domain types in `src/storage/mod.rs` and keeps all Postgres-specific choices
-- (jsonb, composite key, `COLLATE "C"`) behind the repository implementation.
--
-- Owner encoding avoids NULLs in the primary key (Postgres treats NULLs as
-- distinct, which would break upsert identity): `Owner::System` is
-- `(owner_kind = 0, owner_id = '')` and `Owner::User(id)` is
-- `(owner_kind = 1, owner_id = <id>)`. `COLLATE "C"` gives deterministic,
-- byte-wise ordering for keyset pagination that does not depend on the
-- database's locale.

CREATE TABLE IF NOT EXISTS storage_objects (
    owner_kind       smallint NOT NULL,
    owner_id         text COLLATE "C" NOT NULL,
    collection       text COLLATE "C" NOT NULL,
    object_key       text COLLATE "C" NOT NULL,

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
