-- : CockroachDB durable projection membership for operator-declared storage
-- indexes. This is the CockroachDB flavor of the Postgres migration of the
-- same version. CockroachDB's default text ordering is deterministic, so the
-- PostgreSQL-only `COLLATE "C"` clauses are intentionally omitted.

CREATE TABLE IF NOT EXISTS storage_index_definitions (
    index_name  text PRIMARY KEY,
    collection  text NOT NULL,
    object_key  text NULL
);

CREATE TABLE IF NOT EXISTS storage_index_memberships (
    index_name  text NOT NULL,
    owner_kind  smallint NOT NULL,
    owner_id    text NOT NULL,
    collection  text NOT NULL,
    object_key  text NOT NULL,

    PRIMARY KEY (index_name, owner_kind, owner_id, collection, object_key),
    FOREIGN KEY (index_name) REFERENCES storage_index_definitions(index_name)
        ON DELETE CASCADE,
    FOREIGN KEY (owner_kind, owner_id, collection, object_key)
        REFERENCES storage_objects(owner_kind, owner_id, collection, object_key)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS storage_index_memberships_lookup_idx
    ON storage_index_memberships (index_name, owner_kind, owner_id, collection, object_key);
