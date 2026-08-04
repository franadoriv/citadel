-- Durable, redacted audit/outbox for successful current-account credential unlink.
CREATE TABLE IF NOT EXISTS identity_change_outbox (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id               text NOT NULL,
    event_type            text NOT NULL,
    provider              text NOT NULL,
    external_id_redacted  text NOT NULL,
    password_verifier     text,
    created_at            bigint NOT NULL,
    CONSTRAINT identity_change_outbox_event_ck CHECK (event_type = 'credential_unlinked'),
    CONSTRAINT identity_change_outbox_provider_ck CHECK (provider IN ('device', 'custom', 'email')),
    CONSTRAINT identity_change_outbox_redaction_ck CHECK (external_id_redacted = '[redacted]'),
    CONSTRAINT identity_change_outbox_no_verifier_ck CHECK (password_verifier IS NULL)
);
CREATE INDEX IF NOT EXISTS identity_change_outbox_user_created_idx
    ON identity_change_outbox (user_id, created_at, id);
