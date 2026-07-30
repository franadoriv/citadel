-- : CockroachDB email/password identity verifier. This migration is dialect
-- neutral; it mirrors the PostgreSQL migration at the same version.
ALTER TABLE auth_identities
    ADD COLUMN IF NOT EXISTS password_verifier text;

ALTER TABLE auth_identities
    DROP CONSTRAINT IF EXISTS auth_identities_provider_ck;
ALTER TABLE auth_identities
    ADD CONSTRAINT auth_identities_provider_ck
        CHECK (provider IN ('device', 'custom', 'email'));

ALTER TABLE auth_identities
    ADD CONSTRAINT auth_identities_email_verifier_ck CHECK (
        (provider = 'email' AND password_verifier IS NOT NULL)
        OR (provider <> 'email' AND password_verifier IS NULL)
    );
