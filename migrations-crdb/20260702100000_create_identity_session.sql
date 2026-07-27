-- : CockroachDB identity and session tables (CRDB flavor of the
-- Postgres migration in `../migrations`).
--
-- Sibling of the Postgres `20260702100000_create_identity_session.sql`, backing
-- the same `repository::pg` implementations (`PgUserRepository`,
-- `PgAuthIdentityRepository`, `PgSessionRepository`) unchanged. The only dialect
-- fork vs the Postgres schema is dropping `COLLATE "C"` from the `text` id/label
-- columns: CockroachDB rejects the `C` locale and its default string collation is
-- already byte-wise/deterministic, so equality and ordering match the Postgres
-- `COLLATE "C"` columns without a locale dependency. Every other construct
-- (jsonb, composite keys, CHECK constraints, unique/partial indexes) is CRDB-
-- compatible and kept identical so the shared contract tests pass on both.

-- Accounts. `username` is globally unique (case- and byte-exact, matching the
-- in-memory `UserRepository`). `metadata`, when present, is a JSON object.
CREATE TABLE IF NOT EXISTS users (
    id            text PRIMARY KEY,
    username      text NOT NULL,
    display_name  text,
    metadata      jsonb,
    state         text NOT NULL,
    created_at    bigint NOT NULL,
    updated_at    bigint NOT NULL,

    CONSTRAINT users_id_ck CHECK (btrim(id) <> ''),
    CONSTRAINT users_username_ck
        CHECK (username <> '' AND octet_length(username) <= 128 AND username !~ '[[:cntrl:]]'),
    CONSTRAINT users_display_name_ck
        CHECK (display_name IS NULL
               OR (btrim(display_name) <> '' AND octet_length(display_name) <= 255
                   AND display_name !~ '[[:cntrl:]]')),
    CONSTRAINT users_metadata_object_ck
        CHECK (metadata IS NULL OR jsonb_typeof(metadata) = 'object'),
    CONSTRAINT users_state_ck
        CHECK (state IN ('active', 'disabled', 'tombstoned')),
    CONSTRAINT users_updated_after_created_ck
        CHECK (updated_at >= created_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS users_username_key ON users (username);

-- Credential-to-account links. The composite primary key `(provider,
-- external_id)` enforces the one-credential-to-one-account invariant, so a
-- duplicate link surfaces as a unique violation (mapped to a typed conflict) and
-- never as a credential-existence oracle.
CREATE TABLE IF NOT EXISTS auth_identities (
    provider     text NOT NULL,
    external_id  text NOT NULL,
    user_id      text NOT NULL,
    created_at   bigint NOT NULL,
    updated_at   bigint NOT NULL,

    PRIMARY KEY (provider, external_id),

    CONSTRAINT auth_identities_provider_ck
        CHECK (provider IN ('device', 'custom')),
    CONSTRAINT auth_identities_external_id_ck
        CHECK (external_id <> '' AND octet_length(external_id) <= 128
               AND external_id !~ '[[:cntrl:]]'),
    CONSTRAINT auth_identities_user_id_ck CHECK (btrim(user_id) <> ''),
    CONSTRAINT auth_identities_updated_after_created_ck
        CHECK (updated_at >= created_at)
);

-- Listing every identity linked to an account.
CREATE INDEX IF NOT EXISTS auth_identities_user_id_idx ON auth_identities (user_id);

-- Sessions. The authoritative record is the full session serialized into `data`
-- (the private lifecycle state is only reachable via Deserialize). Flat columns
-- are projected out for lookups and the bulk-revoke scan. `token_ref` is the
-- lookup index; a bulk revoke clears it to NULL while `data` retains the
-- reference, mirroring the in-memory by-id / by-token split.
CREATE TABLE IF NOT EXISTS sessions (
    id          text PRIMARY KEY,
    user_id     text NOT NULL,
    token_ref   text,
    state_kind  text NOT NULL,
    data        jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT sessions_id_ck CHECK (btrim(id) <> ''),
    CONSTRAINT sessions_user_id_ck CHECK (btrim(user_id) <> ''),
    CONSTRAINT sessions_state_kind_ck
        CHECK (state_kind IN ('active', 'expired', 'revoked')),
    CONSTRAINT sessions_data_object_ck CHECK (jsonb_typeof(data) = 'object')
);

-- Scans the active sessions of a user for the atomic bulk revoke.
CREATE INDEX IF NOT EXISTS sessions_user_id_state_idx ON sessions (user_id, state_kind);

-- Resolves a session by its non-secret token reference (live rows only).
CREATE INDEX IF NOT EXISTS sessions_token_ref_idx
    ON sessions (token_ref) WHERE token_ref IS NOT NULL;
