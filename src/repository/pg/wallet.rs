//! Postgres wallet repository.
//!
//! [`PgWalletRepository`] is the durable backend for
//! [`WalletRepository`](crate::repository::WalletRepository). Balances live in a
//! `wallet_balances` read-model table (`PRIMARY KEY (user_id, currency)`); every
//! change is appended to a `wallet_ledger` table (a single global monotonic `id`,
//! `balance_after` carrying the post-change balance). The checked, non-negative
//! balance arithmetic and the ledger capacity bound are reused from the shared
//! pure helpers in [`crate::repository::wallet`], so this backend cannot drift
//! from the in-memory reference or the SQLite sibling.
//!
//! A change is a read-modify-write and runs in one transaction: it materializes
//! the `(user, currency)` balance row (at 0) if absent, takes a
//! `SELECT … FOR UPDATE` lock on that fixed primary-key row (so a waiter re-reads
//! the latest committed balance — no lost update), computes the new balance with
//! [`apply_delta`], appends the ledger row with a global `MAX(id) + 1`, updates
//! the balance, and evicts the oldest ledger rows beyond `capacity` —
//! all-or-nothing, so a balance can never update without its ledger entry (or the
//! reverse). Because only the oldest ledger rows are evicted, the newest is
//! always retained and the id sequence never rewinds.
//!
//! Concurrency: locking the fixed balance row serializes all changes to the same
//! `(user, currency)`, so no credit is ever lost or doubled there. Two changes to
//! *different* `(user, currency)` pairs racing on the same global `MAX(id) + 1`
//! can collide on the ledger primary key (mapped to `Conflict`); this is the same
//! accepted tradeoff as the chat/notifications global-id producers and is fine for
//! the console-scale writer today.

use async_trait::async_trait;
use sqlx::postgres::{PgConnection, PgRow};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::config::PgFlavor;
use crate::error::{AppError, AppResult};
use crate::repository::WalletRepository;
use crate::repository::wallet::{LedgerEntry, apply_delta, ledger_overflow};
use crate::time::TimestampMillis;

use super::{PgExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

// --- SQL --------------------------------------------------------------------

// Materialize the balance row (at 0) if it does not yet exist, so the following
// `FOR UPDATE` always locks a real row. Concurrent materializations of the same
// new `(user, currency)` serialize on the primary key and the loser no-ops.
const MATERIALIZE_BALANCE_SQL: &str = "\
INSERT INTO wallet_balances (user_id, currency, balance, updated_at_unix_ms) \
VALUES ($1, $2, 0, $3) ON CONFLICT (user_id, currency) DO NOTHING";

// Lock THIS user+currency balance row for the read-modify-write. Because a fixed
// primary-key row is locked (not `ORDER BY … LIMIT 1`), a waiter re-reads the
// latest committed balance once the lock is granted — so concurrent changes to
// the same balance serialize and never lose an update.
const LOCK_BALANCE_SQL: &str =
    "SELECT balance FROM wallet_balances WHERE user_id = $1 AND currency = $2 FOR UPDATE";

// Global monotonic ledger id. Read under the balance-row lock, so same-balance
// changes compute distinct ids.
const MAX_LEDGER_ID_SQL: &str = "SELECT COALESCE(MAX(id), 0) AS m FROM wallet_ledger";

const INSERT_LEDGER_SQL: &str = "\
INSERT INTO wallet_ledger \
(id, user_id, currency, delta, balance_after, reason, created_at_unix_ms) \
VALUES ($1, $2, $3, $4, $5, $6, $7)";

const UPDATE_BALANCE_SQL: &str = "\
UPDATE wallet_balances SET balance = $3, updated_at_unix_ms = $4 \
WHERE user_id = $1 AND currency = $2";

const COUNT_LEDGER_SQL: &str = "SELECT count(*) AS n FROM wallet_ledger";

const EVICT_LEDGER_SQL: &str =
    "DELETE FROM wallet_ledger WHERE id IN (SELECT id FROM wallet_ledger ORDER BY id ASC LIMIT $1)";

const SELECT_BALANCES_SQL: &str =
    "SELECT currency, balance FROM wallet_balances WHERE user_id = $1 ORDER BY currency";

const SELECT_LEDGER_SQL: &str = "\
SELECT id, user_id, currency, delta, balance_after, reason, created_at_unix_ms \
FROM wallet_ledger WHERE user_id = $1 ORDER BY id DESC LIMIT $2";

// --- mapping helpers --------------------------------------------------------

fn parse_ledger(row: &PgRow) -> AppResult<LedgerEntry> {
    let id: i64 = get(row, "id")?;
    let user_id: String = get(row, "user_id")?;
    let currency: String = get(row, "currency")?;
    let delta: i64 = get(row, "delta")?;
    let balance_after: i64 = get(row, "balance_after")?;
    let reason: String = get(row, "reason")?;
    let created: i64 = get(row, "created_at_unix_ms")?;
    Ok(LedgerEntry {
        seq: to_u64(id, "ledger id")?,
        user_id,
        currency,
        delta,
        balance_after,
        reason,
        time_unix_ms: millis_to_ts(created)?.unix_millis(),
    })
}

fn to_u64(value: i64, what: &str) -> AppResult<u64> {
    u64::try_from(value).map_err(|_| AppError::internal(format!("{what} out of range")))
}

fn to_i64(value: u64, what: &str) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| AppError::internal(format!("{what} out of range")))
}

// --- repository -------------------------------------------------------------

/// Postgres [`WalletRepository`].
pub struct PgWalletRepository {
    executor: PgExecutor,
    flavor: PgFlavor,
}

impl PgWalletRepository {
    /// Bind a wallet repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: PgExecutor, flavor: PgFlavor) -> Self {
        Self { executor, flavor }
    }
}

macro_rules! with_tx {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
                let result = {
                    let $conn = &mut *tx;
                    $body
                };
                match result {
                    Ok(value) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(value)
                    }
                    Err(error) => {
                        let _ = tx.rollback().await;
                        Err(error)
                    }
                }
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

macro_rules! with_conn {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                let $conn = &mut *conn;
                $body
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

#[async_trait]
impl WalletRepository for PgWalletRepository {
    async fn apply_change(
        &self,
        user_id: &str,
        currency: &str,
        delta: i64,
        reason: &str,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<LedgerEntry> {
        if self.flavor == PgFlavor::Cockroach
            && let PgExecutor::Pool(pool) = &self.executor
        {
            return apply_change_with_cockroach_retries(
                pool, user_id, currency, delta, reason, capacity, now,
            )
            .await;
        }
        with_tx!(self, conn =>
            apply_change_conn(conn, user_id, currency, delta, reason, capacity, now).await)
    }

    async fn balances(&self, user_id: &str) -> AppResult<BTreeMap<String, i64>> {
        with_conn!(self, conn => balances_conn(conn, user_id).await)
    }

    async fn ledger(&self, user_id: &str, limit: usize) -> AppResult<Vec<LedgerEntry>> {
        with_conn!(self, conn => ledger_conn(conn, user_id, limit).await)
    }
}

/// CockroachDB uses serializable transactions and asks the client to retry a
/// transaction that loses a write race. A wallet mutation is a single
/// idempotent-in-effect repository transaction, so a bounded retry is safe for
/// pooled/autocommit calls. Explicit unit-of-work callers retain ownership of
/// their transaction and receive the retryable database error instead.
#[allow(clippy::too_many_arguments)]
async fn apply_change_with_cockroach_retries(
    pool: &sqlx::postgres::PgPool,
    user_id: &str,
    currency: &str,
    delta: i64,
    reason: &str,
    capacity: usize,
    now: TimestampMillis,
) -> AppResult<LedgerEntry> {
    const MAX_ATTEMPTS: usize = 8;

    for attempt in 0..MAX_ATTEMPTS {
        let mut tx = pool.begin().await.map_err(db_err)?;
        let result =
            apply_change_conn(&mut tx, user_id, currency, delta, reason, capacity, now).await;
        match result {
            Ok(entry) => match tx.commit().await.map_err(db_err) {
                Ok(()) => return Ok(entry),
                Err(error) if cockroach_retryable(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    cockroach_retry_backoff(attempt).await;
                }
                Err(error) => return Err(error),
            },
            Err(error) => {
                let _ = tx.rollback().await;
                if cockroach_retryable(&error) && attempt + 1 < MAX_ATTEMPTS {
                    cockroach_retry_backoff(attempt).await;
                } else {
                    return Err(error);
                }
            }
        }
    }

    unreachable!("the bounded CockroachDB retry loop always returns")
}

fn cockroach_retryable(error: &AppError) -> bool {
    error.log_detail().is_some_and(|detail| {
        detail.contains("restart transaction")
            || detail.contains("TransactionRetryWithProtoRefreshError")
    })
}

async fn cockroach_retry_backoff(attempt: usize) {
    let delay_ms = 1_u64 << attempt.min(6);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
}

#[allow(clippy::too_many_arguments)]
async fn apply_change_conn(
    conn: &mut PgConnection,
    user_id: &str,
    currency: &str,
    delta: i64,
    reason: &str,
    capacity: usize,
    now: TimestampMillis,
) -> AppResult<LedgerEntry> {
    let created = ts_to_millis(now)?;

    // Serialize concurrent changes to this balance: materialize the row, then lock
    // it. The waiter re-reads the latest committed balance, so no update is lost.
    sqlx::query(MATERIALIZE_BALANCE_SQL)
        .bind(user_id)
        .bind(currency)
        .bind(created)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    let locked = sqlx::query(LOCK_BALANCE_SQL)
        .bind(user_id)
        .bind(currency)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let current: i64 = get(&locked, "balance")?;
    // Validate BEFORE writing so an overflow/overdraw rolls back untouched.
    let next = apply_delta(current, delta)?;

    let max_row = sqlx::query(MAX_LEDGER_ID_SQL)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let new_id = to_u64(get::<i64>(&max_row, "m")?, "ledger id")? + 1;

    sqlx::query(INSERT_LEDGER_SQL)
        .bind(to_i64(new_id, "ledger id")?)
        .bind(user_id)
        .bind(currency)
        .bind(delta)
        .bind(next)
        .bind(reason)
        .bind(created)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

    sqlx::query(UPDATE_BALANCE_SQL)
        .bind(user_id)
        .bind(currency)
        .bind(next)
        .bind(created)
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;

    let total = count_ledger(conn).await?;
    let evict = ledger_overflow(total, capacity);
    if evict > 0 {
        sqlx::query(EVICT_LEDGER_SQL)
            .bind(to_i64(evict as u64, "ledger eviction count")?)
            .execute(&mut *conn)
            .await
            .map_err(db_err)?;
    }

    Ok(LedgerEntry {
        seq: new_id,
        user_id: user_id.to_string(),
        currency: currency.to_string(),
        delta,
        balance_after: next,
        reason: reason.to_string(),
        time_unix_ms: now.unix_millis(),
    })
}

async fn count_ledger(conn: &mut PgConnection) -> AppResult<usize> {
    let row = sqlx::query(COUNT_LEDGER_SQL)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let n: i64 = get(&row, "n")?;
    usize::try_from(n).map_err(|_| AppError::internal("wallet ledger count out of range"))
}

async fn balances_conn(conn: &mut PgConnection, user_id: &str) -> AppResult<BTreeMap<String, i64>> {
    let rows = sqlx::query(SELECT_BALANCES_SQL)
        .bind(user_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    let mut balances = BTreeMap::new();
    for row in &rows {
        let currency: String = get(row, "currency")?;
        let balance: i64 = get(row, "balance")?;
        balances.insert(currency, balance);
    }
    Ok(balances)
}

async fn ledger_conn(
    conn: &mut PgConnection,
    user_id: &str,
    limit: usize,
) -> AppResult<Vec<LedgerEntry>> {
    let rows = sqlx::query(SELECT_LEDGER_SQL)
        .bind(user_id)
        .bind(to_i64(limit as u64, "ledger limit")?)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter().map(parse_ledger).collect()
}
