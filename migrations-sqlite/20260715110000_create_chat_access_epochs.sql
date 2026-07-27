-- : versioned authority subjects used to fence chat grants.
CREATE TABLE IF NOT EXISTS chat_access_epochs (
    access_key TEXT PRIMARY KEY NOT NULL,
    epoch INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,

    CHECK (epoch >= 0)
);
