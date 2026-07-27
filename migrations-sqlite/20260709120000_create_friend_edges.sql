-- : SQLite friend-relationship edges.
--
-- Sibling to the Postgres migration in `../migrations`, backing
-- `repository::sqlite::friends` (`SqliteFriendsRepository`). The schema mirrors
-- that Postgres migration with SQLite-native types so the SAME friends contract
-- tests pass against both backends. Every SQLite-specific choice stays behind the
-- repository impl.
--
-- Dialect mapping vs the Postgres schema:
--   * `text COLLATE "C"` -> `TEXT`; SQLite's default BINARY collation is byte-wise,
--                           matching Postgres `COLLATE "C"`.
--   * `bigint` (domain millis) -> `INTEGER`; SQLite has one integer class and the
--                           u64 epoch-millis round-trip is exact.
--
-- Length / control-character checks that Postgres enforces with
-- `btrim`/`octet_length` are enforced by the service/domain layer, matching the
-- precedent set by the identity/session migrations. The remaining invariants are
-- cheap and portable, so they are kept as table CHECKs (defense in depth
-- identical to Postgres).

CREATE TABLE IF NOT EXISTS friend_edges (
    owner_id         TEXT NOT NULL,
    other_id         TEXT NOT NULL,
    state            TEXT NOT NULL,
    updated_unix_ms  INTEGER NOT NULL,

    PRIMARY KEY (owner_id, other_id),

    CHECK (trim(owner_id) <> ''),
    CHECK (trim(other_id) <> ''),
    CHECK (owner_id <> other_id),
    CHECK (state IN ('invited_sent', 'invited_received', 'friend', 'blocked'))
);
