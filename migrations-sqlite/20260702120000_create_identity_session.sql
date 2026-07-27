-- : SQLite identity and session tables.
--
-- Sibling to the Postgres migration in `../migrations`, backing
-- `repository::sqlite::identity` (`SqliteUserRepository`,
-- `SqliteAuthIdentityRepository`) and `repository::sqlite::session`
-- (`SqliteSessionRepository`). The schema mirrors that Postgres migration with
-- SQLite-native types so the SAME identity/session contract tests pass against
-- both backends. Every SQLite-specific choice stays behind the repository impls.
--
-- Dialect mapping vs the Postgres schema:
--   * `text COLLATE "C"` -> `TEXT`; SQLite's default BINARY collation is byte-wise,
--                           matching Postgres `COLLATE "C"` for deterministic,
--                           locale-independent equality/ordering.
--   * `bigint` (domain millis) -> `INTEGER`; SQLite has one integer class and the
--                           u64 epoch-millis round-trip is exact.
--   * `jsonb`            -> `TEXT` holding a JSON document; the repository
--                           serializes/deserializes JSON at the boundary.
--   * `timestamptz` audit -> `INTEGER` unix seconds via `strftime` defaults. These
--                           audit columns are not read back by the repository (the
--                           user/identity domain millis live in their own columns;
--                           the session's lifecycle timestamps live inside `data`),
--                           so the coarser default is intentional.
--
-- Length / control-character / regex checks that Postgres enforces with
-- `octet_length`/`!~ '[[:cntrl:]]'`/`btrim` are enforced by the domain layer
-- (`UserId`/`Username`/`DisplayName`/`DeviceId`/`CustomId`/`SessionId`) rather
-- than restated here, because SQLite has no portable regex operator — exactly the
-- precedent set by the storage migration. The remaining invariants are cheap and
-- portable, so they are kept as table CHECKs (defense in depth identical to
-- Postgres). sqlx bundles libsqlite3 with JSON1, so `json_valid`/`json_type` are
-- available for the object-shape checks.

-- Accounts. `username` is globally unique (byte-exact, matching the in-memory
-- `UserRepository`). `metadata`, when present, is a JSON object. `created_at`
-- and `updated_at` are the domain epoch-millis timestamps read back by the
-- repository (not audit columns).
-- `id ... NOT NULL PRIMARY KEY`: unlike Postgres, a SQLite non-INTEGER PRIMARY
-- KEY column does NOT imply NOT NULL (a preserved historical quirk), so it is
-- declared explicitly to match the Postgres `NOT NULL` primary key.
CREATE TABLE IF NOT EXISTS users (
    id            TEXT NOT NULL PRIMARY KEY,
    username      TEXT NOT NULL,
    display_name  TEXT,
    metadata      TEXT,
    state         TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,

    CHECK (trim(id) <> ''),
    CHECK (username <> ''),
    CHECK (display_name IS NULL OR trim(display_name) <> ''),
    CHECK (metadata IS NULL OR (json_valid(metadata) AND json_type(metadata) = 'object')),
    CHECK (state IN ('active', 'disabled', 'tombstoned')),
    CHECK (updated_at >= created_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS users_username_key ON users (username);

-- Credential-to-account links. The composite primary key `(provider,
-- external_id)` enforces the one-credential-to-one-account invariant, so a
-- duplicate link surfaces as a unique violation (mapped to a typed conflict via
-- `super::db_err`) and never as a credential-existence oracle.
CREATE TABLE IF NOT EXISTS auth_identities (
    provider     TEXT NOT NULL,
    external_id  TEXT NOT NULL,
    user_id      TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,

    PRIMARY KEY (provider, external_id),

    CHECK (provider IN ('device', 'custom', 'email')),
    CHECK (external_id <> ''),
    CHECK (trim(user_id) <> ''),
    CHECK (updated_at >= created_at)
);

-- Listing every identity linked to an account.
CREATE INDEX IF NOT EXISTS auth_identities_user_id_idx ON auth_identities (user_id);

-- Sessions. The authoritative record is the full session serialized into `data`
-- (the private lifecycle state is only reachable via Deserialize). Flat columns
-- are projected out for lookups and the bulk-revoke scan. `token_ref` is the
-- lookup index; a bulk revoke clears it to NULL while `data` retains the
-- reference, mirroring the in-memory by-id / by-token split. `created_at` /
-- `updated_at` are coarse audit columns and are not read back.
CREATE TABLE IF NOT EXISTS sessions (
    id          TEXT NOT NULL PRIMARY KEY,
    user_id     TEXT NOT NULL,
    token_ref   TEXT,
    state_kind  TEXT NOT NULL,
    data        TEXT NOT NULL,
    created_at  INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
    updated_at  INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),

    CHECK (trim(id) <> ''),
    CHECK (trim(user_id) <> ''),
    CHECK (state_kind IN ('active', 'expired', 'revoked')),
    CHECK (json_valid(data) AND json_type(data) = 'object')
);

-- Scans the active sessions of a user for the atomic bulk revoke.
CREATE INDEX IF NOT EXISTS sessions_user_id_state_idx ON sessions (user_id, state_kind);

-- Resolves a session by its non-secret token reference (live rows only).
CREATE INDEX IF NOT EXISTS sessions_token_ref_idx
    ON sessions (token_ref) WHERE token_ref IS NOT NULL;
