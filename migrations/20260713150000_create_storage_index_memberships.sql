-- : durable projection membership for operator-declared storage
-- indexes. The storage object remains authoritative; these rows only decide
-- whether it participates in a configured index query.

CREATE TABLE IF NOT EXISTS storage_index_definitions (
    index_name  text COLLATE "C" PRIMARY KEY,
    collection  text COLLATE "C" NOT NULL,
    object_key  text COLLATE "C" NULL
);

CREATE TABLE IF NOT EXISTS storage_index_memberships (
    index_name  text COLLATE "C" NOT NULL,
    owner_kind  smallint NOT NULL,
    owner_id    text COLLATE "C" NOT NULL,
    collection  text COLLATE "C" NOT NULL,
    object_key  text COLLATE "C" NOT NULL,

    PRIMARY KEY (index_name, owner_kind, owner_id, collection, object_key),
    FOREIGN KEY (index_name) REFERENCES storage_index_definitions(index_name)
        ON DELETE CASCADE,
    FOREIGN KEY (owner_kind, owner_id, collection, object_key)
        REFERENCES storage_objects(owner_kind, owner_id, collection, object_key)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS storage_index_memberships_lookup_idx
    ON storage_index_memberships (index_name, owner_kind, owner_id, collection, object_key);
