//! SQLite purchases repository.
//!
//! [`SqlitePurchasesRepository`] is the durable single-file backend for
//! [`PurchasesRepository`](crate::repository::PurchasesRepository) and the
//! sibling of the Postgres impl in `../pg/purchases.rs`. Every validated purchase
//! is one `purchases` row keyed by its store-unique `transaction_id` (the primary
//! key, so a replayed receipt is a `Conflict`); a subscription is a purchase
//! carrying `subscription_expiry_unix_ms` (no separate table). The user-filtered
//! newest-first paging and the subscription `active`/`expired` derivation are
//! reused from the shared pure helpers in [`crate::repository::purchases`], so the
//! two backends cannot drift.

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnection, SqliteRow};

use crate::error::{AppError, AppResult};
use crate::repository::PurchasesRepository;
use crate::repository::purchases::{
    Purchase, PurchaseStore, SubscriptionRow, page_purchases, subscription_rows,
};
use crate::time::TimestampMillis;

use super::{SqliteExecutor, db_err, get, millis_to_ts, tx_closed};

// --- SQL --------------------------------------------------------------------

const INSERT_SQL: &str = "\
INSERT INTO purchases \
(transaction_id, user_id, product_id, store, receipt_sha256, validated_at_unix_ms, \
 subscription_expiry_unix_ms) \
VALUES (?, ?, ?, ?, ?, ?, ?)";

const SELECT_ALL_SQL: &str = "\
SELECT transaction_id, user_id, product_id, store, receipt_sha256, validated_at_unix_ms, \
 subscription_expiry_unix_ms FROM purchases";

const SELECT_ONE_SQL: &str = "\
SELECT transaction_id, user_id, product_id, store, receipt_sha256, validated_at_unix_ms, \
 subscription_expiry_unix_ms FROM purchases WHERE transaction_id = ?";

// --- mapping helpers --------------------------------------------------------

fn parse_purchase(row: &SqliteRow) -> AppResult<Purchase> {
    let transaction_id: String = get(row, "transaction_id")?;
    let user_id: String = get(row, "user_id")?;
    let product_id: String = get(row, "product_id")?;
    let store: String = get(row, "store")?;
    let receipt_sha256: String = get(row, "receipt_sha256")?;
    let validated: i64 = get(row, "validated_at_unix_ms")?;
    let expiry: Option<i64> = get(row, "subscription_expiry_unix_ms")?;
    Ok(Purchase {
        transaction_id,
        user_id,
        product_id,
        store: PurchaseStore::from_token(&store)?,
        receipt_sha256,
        validated_at_unix_ms: millis_to_ts(validated)?.unix_millis(),
        subscription_expiry_unix_ms: expiry
            .map(|e| millis_to_ts(e).map(|ts| ts.unix_millis()))
            .transpose()?,
    })
}

fn to_i64(value: u64, what: &str) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| AppError::internal(format!("{what} out of range")))
}

// --- repository -------------------------------------------------------------

/// SQLite [`PurchasesRepository`].
pub struct SqlitePurchasesRepository {
    executor: SqliteExecutor,
}

impl SqlitePurchasesRepository {
    /// Bind a purchases repository to an execution handle (pool or transaction).
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }
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
impl PurchasesRepository for SqlitePurchasesRepository {
    async fn record(&self, purchase: Purchase) -> AppResult<Purchase> {
        with_conn!(self, conn => record_conn(conn, purchase).await)
    }

    async fn list(&self, user_id: Option<&str>, limit: usize) -> AppResult<Vec<Purchase>> {
        with_conn!(self, conn => list_conn(conn, user_id, limit).await)
    }

    async fn get(&self, transaction_id: &str) -> AppResult<Option<Purchase>> {
        with_conn!(self, conn => get_conn(conn, transaction_id).await)
    }

    async fn subscriptions(
        &self,
        user_id: Option<&str>,
        limit: usize,
        now: TimestampMillis,
    ) -> AppResult<Vec<SubscriptionRow>> {
        with_conn!(self, conn => subscriptions_conn(conn, user_id, limit, now).await)
    }
}

async fn record_conn(conn: &mut SqliteConnection, purchase: Purchase) -> AppResult<Purchase> {
    // A duplicate transaction id collides on the primary key; `db_err` maps the
    // unique violation to `Conflict`.
    sqlx::query(INSERT_SQL)
        .bind(&purchase.transaction_id)
        .bind(&purchase.user_id)
        .bind(&purchase.product_id)
        .bind(purchase.store.as_str())
        .bind(&purchase.receipt_sha256)
        .bind(to_i64(purchase.validated_at_unix_ms, "validated_at")?)
        .bind(
            purchase
                .subscription_expiry_unix_ms
                .map(|e| to_i64(e, "subscription_expiry"))
                .transpose()?,
        )
        .execute(&mut *conn)
        .await
        .map_err(db_err)?;
    Ok(purchase)
}

async fn load_all(conn: &mut SqliteConnection) -> AppResult<Vec<Purchase>> {
    let rows = sqlx::query(SELECT_ALL_SQL)
        .fetch_all(&mut *conn)
        .await
        .map_err(db_err)?;
    rows.iter().map(parse_purchase).collect()
}

async fn list_conn(
    conn: &mut SqliteConnection,
    user_id: Option<&str>,
    limit: usize,
) -> AppResult<Vec<Purchase>> {
    Ok(page_purchases(load_all(conn).await?, user_id, limit))
}

async fn get_conn(
    conn: &mut SqliteConnection,
    transaction_id: &str,
) -> AppResult<Option<Purchase>> {
    let row = sqlx::query(SELECT_ONE_SQL)
        .bind(transaction_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_err)?;
    row.as_ref().map(parse_purchase).transpose()
}

async fn subscriptions_conn(
    conn: &mut SqliteConnection,
    user_id: Option<&str>,
    limit: usize,
    now: TimestampMillis,
) -> AppResult<Vec<SubscriptionRow>> {
    Ok(subscription_rows(
        load_all(conn).await?,
        user_id,
        limit,
        now,
    ))
}
