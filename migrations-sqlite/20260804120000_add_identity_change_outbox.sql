-- Durable, redacted audit/outbox for successful current-account credential unlink.
CREATE TABLE IF NOT EXISTS identity_change_outbox (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id               TEXT NOT NULL,
    event_type            TEXT NOT NULL,
    provider              TEXT NOT NULL,
    external_id_redacted  TEXT NOT NULL,
    password_verifier     TEXT,
    created_at            INTEGER NOT NULL,
    CHECK (event_type = 'credential_unlinked'),
    CHECK (provider IN ('device', 'custom', 'email')),
    CHECK (external_id_redacted = '[redacted]'),
    CHECK (password_verifier IS NULL)
);
CREATE INDEX IF NOT EXISTS identity_change_outbox_user_created_idx
    ON identity_change_outbox (user_id, created_at, id);
