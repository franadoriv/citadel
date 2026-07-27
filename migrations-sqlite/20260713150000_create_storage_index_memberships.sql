-- : SQLite counterpart of the durable storage-index projection.

CREATE TABLE IF NOT EXISTS storage_index_definitions (
    index_name  TEXT PRIMARY KEY,
    collection  TEXT NOT NULL,
    object_key  TEXT NULL
);

CREATE TABLE IF NOT EXISTS storage_index_memberships (
    index_name  TEXT NOT NULL,
    owner_kind  INTEGER NOT NULL,
    owner_id    TEXT NOT NULL,
    collection  TEXT NOT NULL,
    object_key  TEXT NOT NULL,

    PRIMARY KEY (index_name, owner_kind, owner_id, collection, object_key),
    FOREIGN KEY (index_name) REFERENCES storage_index_definitions(index_name)
        ON DELETE CASCADE,
    FOREIGN KEY (owner_kind, owner_id, collection, object_key)
        REFERENCES storage_objects(owner_kind, owner_id, collection, object_key)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS storage_index_memberships_lookup_idx
    ON storage_index_memberships (index_name, owner_kind, owner_id, collection, object_key);
