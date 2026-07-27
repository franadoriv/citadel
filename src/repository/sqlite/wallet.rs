//! SQLite wallet repository.
//!
//! [`SqliteWalletRepository`] is the durable single-file backend for
//! [`WalletRepository`](crate::repository::WalletRepository) and the sibling of
//! the Postgres impl in `../pg/wallet.rs`. Balances live in a `wallet_balances`
//! read-model table (`PRIMARY KEY (user_id, currency)`); every change is appended
//! to a `wallet_ledger` table (a single global monotonic `id`, `balance_after`
//! carrying the post-change balance). The checked, non-negative balance
//! arithmetic and the ledger capacity bound are reused from the shared pure
//! helpers in [`crate::repository::wallet`], so the two backends cannot drift.
//!
//! SQLite has no `SELECT … FOR UPDATE`, so a change runs under `BEGIN IMMEDIATE`,
//! which takes the writer slot up front and serializes the read-modify-write the
//! way the Postgres row lock does. The whole change — ledger append + balance
//! upsert + eviction — is one transaction, so a balance can never update without
//! its ledger entry (or the reverse).

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnection, SqliteRow};
use std::collections::BTreeMap;

use crate::error::{AppError, AppResult};
use crate::repository::WalletRepository;
use crate::repository::wallet::{LedgerEntry, apply_delta, ledger_overflow};
use crate::time::TimestampMillis;

use super::{SqliteExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

// --- SQL --------------------------------------------------------------------

const SELECT_HEAD_SQL: &str = "SELECT id FROM wallet_ledger ORDER BY id DESC LIMIT 1";

const SELECT_BALANCE_SQL: &str =
    "SELECT balance FROM wallet_balances WHERE user_id = ? AND currency = ?";

const INSERT_LEDGER_SQL: &str = "\
INSERT INTO wallet_ledger \
(id, user_id, currency, delta, balance_after, reason, created_at_unix_ms) \
VALUES (?, ?, ?, ?, ?, ?, ?)";

const UPSERT_BALANCE_SQL: &str = "\
INSERT INTO wallet_balances (user_id, currency, balance, updated_at_unix_ms) \
VALUES (?, ?, ?, ?) \
ON CONFLICT (user_id, currency) DO UPDATE SET \
balance = excluded.balance, updated_at_unix_ms = excluded.updated_at_unix_ms";

const COUNT_LEDGER_SQL: &str = "SELECT count(*) AS n FROM wallet_ledger";

const EVICT_LEDGER_SQL: &str =
    "DELETE FROM wallet_ledger WHERE id IN (SELECT id FROM wallet_ledger ORDER BY id ASC LIMIT ?)";

const SELECT_BALANCES_SQL: &str =
    "SELECT currency, balance FROM wallet_balances WHERE user_id = ? ORDER BY currency";

const SELECT_LEDGER_SQL: &str = "\
SELECT id, user_id, currency, delta, balance_after, reason, created_at_unix_ms \
FROM wallet_ledger WHERE user_id = ? ORDER BY id DESC LIMIT ?";

// --- mapping helpers --------------------------------------------------------

fn parse_ledger(row: &SqliteRow) -> AppResult<LedgerEntry> {
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

/// SQLite [`WalletRepository`].
pub struct SqliteWalletRepository {
    executor: SqliteExecutor,
}

impl SqliteWalletRepository {
    /// Bind a wallet repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }
}

macro_rules! with_tx {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
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
            SqliteExecutor::Tx(cell) => {
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
            SqliteExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                let $conn = &mut *conn;
                $body
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

#[async_trait]
impl WalletRepository for SqliteWalletRepository {
    async fn apply_change(
        &self,
        user_id: &str,
        currency: &str,
        delta: i64,
        reason: &str,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<LedgerEntry> {
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

#[allow(clippy::too_many_arguments)]
async fn apply_change_conn(
    conn: &mut SqliteConnection,
    user_id: &str,
    currency: &str,
    delta: i64,
    reason: &str,
    capacity: usize,
    now: TimestampMillis,
) -> AppResult<LedgerEntry> {
    let head = sqlx::query(SELECT_HEAD_SQL)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    let max_id = match head {
        Some(row) => {
            let id: i64 = get(&row, "id")?;
            to_u64(id, "ledger id")?
        }
        None => 0,
    };
    let new_id = max_id + 1;

    let current = load_balance(conn, user_id, currency).await?;
    // Validate BEFORE writing so an overflow/overdraw rolls back untouched.
    let next = apply_delta(current, delta)?;
    let created = ts_to_millis(now)?;

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

    sqlx::query(UPSERT_BALANCE_SQL)
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

async fn load_balance(
    conn: &mut SqliteConnection,
    user_id: &str,
    currency: &str,
) -> AppResult<i64> {
    let row = sqlx::query(SELECT_BALANCE_SQL)
        .bind(user_id)
        .bind(currency)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    match row {
        Some(row) => get(&row, "balance"),
        None => Ok(0),
    }
}

async fn count_ledger(conn: &mut SqliteConnection) -> AppResult<usize> {
    let row = sqlx::query(COUNT_LEDGER_SQL)
        .fetch_one(&mut *conn)
        .await
        .map_err(db_err)?;
    let n: i64 = get(&row, "n")?;
    usize::try_from(n).map_err(|_| AppError::internal("wallet ledger count out of range"))
}

async fn balances_conn(
    conn: &mut SqliteConnection,
    user_id: &str,
) -> AppResult<BTreeMap<String, i64>> {
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
    conn: &mut SqliteConnection,
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
