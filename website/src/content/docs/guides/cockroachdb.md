---
title: Running Citadel on CockroachDB
description: Point Citadel at a CockroachDB cluster over the PostgreSQL wire protocol — configuration, bringing up a node, and the PostgreSQL-vs-CockroachDB behavioral differences Citadel handles for you.
---

CockroachDB speaks the **PostgreSQL wire protocol**, so Citadel runs on it through
the same Postgres backend that serves PostgreSQL — no separate driver, no separate
repository code. CockroachDB is a distinct **flavor** of that backend: Citadel
applies CockroachDB-compatible migrations and skips two PostgreSQL-only mechanisms
CockroachDB does not implement. This gives you a horizontally scalable,
distributed SQL store (the same database Nakama ran on) with the identical
storage, identity, and session behavior you get on PostgreSQL and SQLite.

CockroachDB support is a Citadel **bonus** capability; it does not change the
client-facing API in any way. A game client needs no changes for the server to run
on CockroachDB.

## Configure the connection

Set `database.url` to a `cockroach://` (or `cockroachdb://`) URL. The scheme is how
Citadel selects the CockroachDB flavor:

```toml
[database]
url = "cockroach://root@localhost:26257/citadel?sslmode=disable"
max_connections = 10
connect_timeout_ms = 5000
acquire_timeout_ms = 5000
```

Or via the environment:

```bash
export CITADEL_DATABASE_URL="cockroach://root@localhost:26257/citadel?sslmode=disable"
```

Everything after the scheme is a standard PostgreSQL connection string
(credentials, host, port, database, query parameters such as `sslmode`), so a
secure cluster uses the usual `sslmode=verify-full` plus certificate parameters.
The URL may carry credentials and is **never** echoed in logs, diagnostics, or the
`/status` endpoint.

:::caution
Use the `cockroach://` / `cockroachdb://` scheme, **not** a plain `postgres://`
URL. A `postgres://` URL is treated as standard PostgreSQL and would try to apply
the PostgreSQL migrations (which use `COLLATE "C"`), and CockroachDB rejects that.
The scheme is the explicit opt-in to the CockroachDB dialect.
:::

On `citadel serve`, the node connects, applies the embedded CockroachDB migrations,
and reports `backend = "cockroach"` on the `/status` endpoint and the `/dashboard`
console. If the cluster is unreachable or a migration fails, startup **fails fast**
— the node never falls back silently.

## Bring up a local CockroachDB

The repository ships a throwaway single-node fixture, `docker-compose.crdb.yml`:

```bash
# Start a single insecure node (SQL on 26257, Admin UI on 8080).
docker compose -f docker-compose.crdb.yml up -d

# Create the database once the node is live.
docker compose -f docker-compose.crdb.yml exec crdb \
  cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS citadel;"

# Run Citadel against it.
CITADEL_DATABASE_URL="cockroach://root@localhost:26257/citadel?sslmode=disable" \
  cargo run -- serve

# Tear it down when finished (-v also removes the data volume).
docker compose -f docker-compose.crdb.yml down -v
```

For a production cluster, provision CockroachDB the usual way (self-hosted or
CockroachDB Cloud), create a `citadel` database and a role, and use a secure
`cockroach://` URL with TLS parameters.

## PostgreSQL vs CockroachDB: what Citadel handles

CockroachDB is highly PostgreSQL-compatible, but not identical. Citadel absorbs the
differences that matter for its schema and queries so the same storage, identity,
session, friends, groups, leaderboards, chat, notifications, wallet, and purchase
contracts pass on both backends:

| Difference | On PostgreSQL | On CockroachDB | How Citadel handles it |
| --- | --- | --- | --- |
| String collation | Columns use `COLLATE "C"` for deterministic, byte-wise ordering (keyset pagination). | Rejects the `C` locale; the default string collation is **already** byte-wise/deterministic. | Uses a CockroachDB-specific migration set (`migrations-crdb/`) that drops `COLLATE "C"`; ordering is identical. |
| Per-object write lock | The storage repository takes `pg_advisory_xact_lock` to close the absent-row race between two concurrent creators under the default `READ COMMITTED` isolation. | Does not implement `pg_advisory_xact_lock`; its **default `SERIALIZABLE` isolation** (strictly stronger) plus the primary-key constraint already reject the racing insert. | Skips the advisory lock on the CockroachDB flavor; the create-only precondition and optimistic-version conflicts still hold. |
| Migration serialization | SQLx serializes concurrent migrators with a PostgreSQL advisory lock. | No advisory locks. | Disables SQLx migration locking for CockroachDB (a node applies migrations once at startup). |
| Isolation level | `READ COMMITTED` by default. | `SERIALIZABLE` by default. | Citadel's transactional workflows are correct under the stronger level. Pooled wallet changes retry CockroachDB serialization restarts with bounded exponential backoff so concurrent credits are not spuriously lost. |
| Integer aliases | `integer` is a 32-bit `INT4`, matching the notification `code` API. | `integer` is the 64-bit `INT8` alias. | The CRDB notification migration explicitly uses `INT4` for `code`, preserving the public `i32` contract. |
| Group identifiers | `GENERATED ALWAYS AS IDENTITY` assigns each `groups.id`. | The PostgreSQL identity form is not portable here. | The CRDB migration uses `unique_rowid` as the durable cluster-unique `INT8` default; Citadel continues to treat the returned group id as an opaque `u64`. |

Everything else — `jsonb` values, composite primary keys, `CHECK` constraints
(`octet_length`, `jsonb_typeof`, POSIX-class regex), unique and partial indexes,
`SELECT … FOR UPDATE`, `ON CONFLICT … DO NOTHING`, and the optimistic-version
semantics — is used unchanged on CockroachDB.

## Verifying compatibility

The gated compatibility matrix in `tests/cockroachdb_compatibility.rs` runs the
storage, identity, session, and atomic-unit-of-work contracts against a live
CockroachDB instance. The full domain matrix uses the existing repository contract
suites against the same `cockroach://` URL. They skip cleanly when their database
environment variables are unset, so the default check suite stays green without a
database:

```bash
docker compose -f docker-compose.crdb.yml up -d
docker compose -f docker-compose.crdb.yml exec crdb \
  cockroach sql --insecure -e "CREATE DATABASE IF NOT EXISTS citadel;"

CITADEL_TEST_COCKROACH_URL="postgres://root@localhost:26257/citadel?sslmode=disable" \
  cargo test --test cockroachdb_compatibility

for test in friends_repository_contract groups_repository_contract \
  leaderboards_repository_contract chat_repository_contract \
  notifications_repository_contract wallet_repository_contract; do
  DATABASE_URL="cockroach://root@localhost:26257/citadel?sslmode=disable" \
    cargo test --test "$test"
done

docker compose -f docker-compose.crdb.yml down -v
```

(`cockroachdb_compatibility` accepts a `postgres://` URL for convenience and
re-flags it as the CockroachDB flavor internally. The domain tests must receive a
`cockroach://`/`cockroachdb://` URL so migration selection is explicit.)

## Known limitations

- Single-node fixture only in-tree: `docker-compose.crdb.yml` is for local
  validation, not a production topology.
- The test-only storage reset uses `DELETE FROM` rather than `TRUNCATE`, because
  CockroachDB implements `TRUNCATE` as an asynchronous schema change that races
  rapid repeated resets. This affects tests only, not runtime behavior.
