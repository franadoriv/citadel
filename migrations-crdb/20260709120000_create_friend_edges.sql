-- : CockroachDB friend-relationship edges (CRDB flavor of the
-- Postgres migration in `../migrations`).
--
-- CockroachDB's default `text` ordering is deterministic and byte-wise, so the
-- PostgreSQL-only `COLLATE "C"` clauses are deliberately omitted. The relation
-- shape, primary key, and checks otherwise match PostgreSQL exactly; the shared
-- PgFriendsRepository therefore provides the same two-directed-edge contract on
-- both database flavors.

CREATE TABLE IF NOT EXISTS friend_edges (
    owner_id         text NOT NULL,
    other_id         text NOT NULL,
    state            text NOT NULL,
    updated_unix_ms  bigint NOT NULL,

    PRIMARY KEY (owner_id, other_id),

    CONSTRAINT friend_edges_owner_id_ck CHECK (btrim(owner_id) <> ''),
    CONSTRAINT friend_edges_other_id_ck CHECK (btrim(other_id) <> ''),
    CONSTRAINT friend_edges_not_self_ck CHECK (owner_id <> other_id),
    CONSTRAINT friend_edges_state_ck
        CHECK (state IN ('invited_sent', 'invited_received', 'friend', 'blocked'))
);
