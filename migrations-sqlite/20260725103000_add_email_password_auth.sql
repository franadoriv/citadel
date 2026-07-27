-- : SQLite cannot alter the original provider CHECK constraint, so
-- rebuild this small identity table while preserving every existing link.
ALTER TABLE auth_identities RENAME TO auth_identities_old;

CREATE TABLE auth_identities (
    provider          TEXT NOT NULL,
    external_id       TEXT NOT NULL,
    user_id           TEXT NOT NULL,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    password_verifier TEXT,

    PRIMARY KEY (provider, external_id),
    CHECK (provider IN ('device', 'custom', 'email')),
    CHECK (external_id <> ''),
    CHECK (trim(user_id) <> ''),
    CHECK (updated_at >= created_at),
    CHECK ((provider = 'email' AND password_verifier IS NOT NULL)
        OR (provider <> 'email' AND password_verifier IS NULL))
);

INSERT INTO auth_identities (provider, external_id, user_id, created_at, updated_at)
SELECT provider, external_id, user_id, created_at, updated_at
FROM auth_identities_old;

DROP TABLE auth_identities_old;
CREATE INDEX auth_identities_user_id_idx ON auth_identities (user_id);
