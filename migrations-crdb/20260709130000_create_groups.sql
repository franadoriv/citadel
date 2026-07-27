-- : CockroachDB groups and memberships (CRDB flavor of the Postgres
-- migration in `../migrations`).
--
-- PostgreSQL's `text COLLATE "C"` is omitted because CockroachDB rejects that
-- locale and already orders its default `text` values byte-wise. PostgreSQL's
-- `GENERATED ALWAYS AS IDENTITY` is replaced with `unique_rowid`: it produces
-- durable, cluster-unique INT8 identifiers and is the CockroachDB-native primary
-- key default. Citadel treats group ids as opaque assigned u64 values, so this
-- preserves the repository contract without coupling it to a serial sequence.

CREATE TABLE IF NOT EXISTS groups (
    id                 bigint NOT NULL DEFAULT unique_rowid() PRIMARY KEY,
    name               text NOT NULL UNIQUE,
    description        text NOT NULL DEFAULT '',
    open               boolean NOT NULL DEFAULT true,
    max_size           bigint NOT NULL DEFAULT 0,
    creator_id         text NOT NULL,
    created_at_unix_ms bigint NOT NULL,
    updated_at_unix_ms bigint NOT NULL,

    CONSTRAINT groups_name_ck CHECK (btrim(name) <> ''),
    CONSTRAINT groups_creator_ck CHECK (btrim(creator_id) <> ''),
    CONSTRAINT groups_max_size_ck CHECK (max_size >= 0)
);

CREATE TABLE IF NOT EXISTS group_memberships (
    group_id          bigint NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    user_id           text NOT NULL,
    role              text NOT NULL,
    joined_at_unix_ms bigint NOT NULL,

    PRIMARY KEY (group_id, user_id),

    CONSTRAINT group_memberships_user_ck CHECK (btrim(user_id) <> ''),
    CONSTRAINT group_memberships_role_ck
        CHECK (role IN ('member', 'admin', 'superadmin'))
);

-- Supports loading a group's member roll in join order.
CREATE INDEX IF NOT EXISTS group_memberships_group_idx
    ON group_memberships (group_id, joined_at_unix_ms, user_id);
