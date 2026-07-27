-- : groups (clans) and their membership.
--
-- Backs `repository::pg::groups` (`PgGroupsRepository`). Persists the player-group
-- domain (Nakama's superadmin/admin/member, open/closed model). One group is one
-- `groups` row plus N `group_memberships` rows; the role ladder and the
-- last-superadmin invariant are enforced in the repository's pure helpers
-- (`src/repository/groups.rs`). Every Postgres-specific choice stays behind the
-- repository implementation; the schema mirrors the portable value types.
--
-- Notes on deliberate choices:
--
-- * `id` is a database identity (`bigint GENERATED ALWAYS AS IDENTITY`), so group
--   ids are assigned durably by the database and never collide across restarts
--   (unlike an in-process counter). The domain type is a `u64`.
-- * `name` is `text COLLATE "C"` and `UNIQUE`: deterministic, locale-independent
--   equality (matching `users`/`sessions`/`friend_edges`), and the unique index
--   is the durable backstop the repository relies on for the name-conflict rule.
-- * `open` is a real `boolean`; `max_size` is a `bigint` (`0` = unlimited), wide
--   enough for the domain `u32`.
-- * `creator_id` records the founding superadmin for provenance.
-- * `*_unix_ms` timestamps are domain Unix-epoch millis (`u64`) stored as
--   `bigint` for an exact round-trip with no datetime/locale conversion —
--   matching the identity/session/friends tables. `updated_at_unix_ms` is set to
--   `created_at_unix_ms` on insert and is reserved for a future
--   update-with-clock path (the current console `update` carries no timestamp).
-- * `group_memberships` references `groups(id) ON DELETE CASCADE`, so deleting a
--   group removes its member roll in one statement.

CREATE TABLE IF NOT EXISTS groups (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name               text COLLATE "C" NOT NULL UNIQUE,
    description        text NOT NULL DEFAULT '',
    open               boolean NOT NULL DEFAULT true,
    max_size           bigint NOT NULL DEFAULT 0,
    creator_id         text COLLATE "C" NOT NULL,
    created_at_unix_ms bigint NOT NULL,
    updated_at_unix_ms bigint NOT NULL,

    CONSTRAINT groups_name_ck CHECK (btrim(name) <> ''),
    CONSTRAINT groups_creator_ck CHECK (btrim(creator_id) <> ''),
    CONSTRAINT groups_max_size_ck CHECK (max_size >= 0)
);

CREATE TABLE IF NOT EXISTS group_memberships (
    group_id          bigint NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    user_id           text COLLATE "C" NOT NULL,
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
