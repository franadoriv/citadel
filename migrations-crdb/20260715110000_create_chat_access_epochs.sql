-- : versioned authority subjects used to fence chat grants.
CREATE TABLE IF NOT EXISTS chat_access_epochs (
    access_key STRING PRIMARY KEY,
    epoch INT8 NOT NULL,
    updated_at_unix_ms INT8 NOT NULL,

    CONSTRAINT chat_access_epochs_epoch_ck CHECK (epoch >= 0)
);
