-- : friend-relationship edges.
--
-- Backs `repository::pg::friends` (`PgFriendsRepository`). Persists the pairwise,
-- directed friend graph designed in `website/src/content/docs/reference/client-sdk/friends.mdx`
--: one relationship is two directed edges, `(owner_id, other_id)` and
-- `(other_id, owner_id)`, each carrying one of Nakama's four states. Every
-- Postgres-specific choice stays behind the repository implementation; the schema
-- mirrors the portable value types in `src/repository/friends.rs`.
--
-- Notes on deliberate choices:
--
-- * Ids are opaque, already-validated domain strings (the service validates the
--   `user`/`other` labels and rejects `user == other`), not Postgres `uuid`s.
--   Columns are `text COLLATE "C"` for deterministic, locale-independent
--   equality/ordering, matching `users`/`sessions`.
-- * The relationship state is stored as the same stable lowercase token the
--   `FriendState` enum emits (`as_str`), so the schema is self-describing and
--   the repository parses it back with `FriendState::from_token`.
-- * `updated_unix_ms` is the domain Unix-epoch-millis timestamp (a `u64`) stored
--   as `bigint`, so the round-trip is exact and no datetime/locale conversion is
--   needed — matching the identity/session tables.

CREATE TABLE IF NOT EXISTS friend_edges (
    owner_id         text COLLATE "C" NOT NULL,
    other_id         text COLLATE "C" NOT NULL,
    state            text NOT NULL,
    updated_unix_ms  bigint NOT NULL,

    PRIMARY KEY (owner_id, other_id),

    CONSTRAINT friend_edges_owner_id_ck CHECK (btrim(owner_id) <> ''),
    CONSTRAINT friend_edges_other_id_ck CHECK (btrim(other_id) <> ''),
    CONSTRAINT friend_edges_not_self_ck CHECK (owner_id <> other_id),
    CONSTRAINT friend_edges_state_ck
        CHECK (state IN ('invited_sent', 'invited_received', 'friend', 'blocked'))
);
