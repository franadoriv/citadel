-- : versioned authority subjects used to fence chat grants.
CREATE TABLE IF NOT EXISTS chat_access_epochs (
    access_key text COLLATE "C" PRIMARY KEY,
    epoch bigint NOT NULL,
    updated_at_unix_ms bigint NOT NULL,

    CONSTRAINT chat_access_epochs_epoch_ck CHECK (epoch >= 0)
);
