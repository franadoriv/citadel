//! Purchases repository contract.
//!
//! Persists **validated** in-app purchase and subscription records behind the
//! same repository seam as identity/session/storage and the other domain
//! features, so the purchases an operator (or a game) validates survive a node
//! restart on the durable backends. Only the *result* of receipt validation is
//! stored — never the raw receipt, only its SHA-256 digest — so the store cannot
//! leak resubmittable receipt material. Receipt validation itself
//! ([`ReceiptValidator`](crate::services::ReceiptValidator) /
//! [`DevReceiptValidator`](crate::services::DevReceiptValidator)) stays in the
//! service layer; this module persists the [`Purchase`] it produces.
//!
//! Following the friends/groups/leaderboards/chat/notifications template, the
//! non-trivial read logic — the user-filtered, newest-first paging and the
//! subscription `active`/`expired` derivation — lives in exactly one place: the
//! pure [`page_purchases`] / [`subscription_status`] helpers, unit-tested
//! directly here. Every backend ([`InMemoryPurchasesRepository`], the Postgres
//! `PgPurchasesRepository`, the SQLite `SqlitePurchasesRepository`) only does
//! read → apply the pure decision → write, so the three implementations cannot
//! drift.
//!
//! A `transaction_id` is recorded at most once (the table's primary key); a
//! replayed receipt is a [`Conflict`](crate::error::ErrorCategory::Conflict).
//! Newest-first is ordered by `(validated_at_unix_ms, transaction_id)` descending
//! — a total, deterministic order every backend reproduces, with no database
//! serial to fork across the Postgres/CockroachDB/SQLite flavors. Subscriptions
//! are a read-derived view over purchases that carry an expiry, not a separate
//! table.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::time::TimestampMillis;

/// The store a receipt came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PurchaseStore {
    /// Apple App Store.
    Apple,
    /// Google Play.
    Google,
    /// Huawei AppGallery.
    Huawei,
    /// A game-defined custom store (the dev validator's natural home).
    Custom,
}

impl PurchaseStore {
    /// Stable lowercase token for responses, audit entries, and the durable
    /// `store` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Apple => "apple",
            Self::Google => "google",
            Self::Huawei => "huawei",
            Self::Custom => "custom",
        }
    }

    /// Parse a stored `store` token back into a [`PurchaseStore`].
    ///
    /// # Errors
    /// Returns an `Internal` error if the token is not one of the known values —
    /// a corrupt/foreign row rather than a client-visible condition.
    pub fn from_token(token: &str) -> AppResult<Self> {
        match token {
            "apple" => Ok(Self::Apple),
            "google" => Ok(Self::Google),
            "huawei" => Ok(Self::Huawei),
            "custom" => Ok(Self::Custom),
            other => Err(AppError::internal(format!(
                "unknown purchase store token `{other}`"
            ))),
        }
    }
}

/// One recorded, validated purchase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Purchase {
    /// Store-unique transaction id.
    pub transaction_id: String,
    /// The buying account.
    pub user_id: String,
    /// The purchased product.
    pub product_id: String,
    /// Originating store.
    pub store: PurchaseStore,
    /// SHA-256 hex digest of the raw receipt (the receipt itself is never
    /// stored).
    pub receipt_sha256: String,
    /// When the purchase was validated (Unix millis).
    pub validated_at_unix_ms: u64,
    /// Subscription expiry when the product is a subscription.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_expiry_unix_ms: Option<u64>,
}

/// One subscription row, with liveness derived at read time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SubscriptionRow {
    /// The owning transaction.
    pub transaction_id: String,
    /// The subscribing account.
    pub user_id: String,
    /// The subscription product.
    pub product_id: String,
    /// Originating store.
    pub store: PurchaseStore,
    /// Expiry (Unix millis).
    pub expiry_unix_ms: u64,
    /// `active` or `expired`, relative to the read-time clock.
    pub status: &'static str,
}

// --- Pure decision helpers (the unit-tested read logic) ----------------------

/// Whether a subscription with `expiry` is `active` or `expired` at `now`.
#[must_use]
pub fn subscription_status(expiry_unix_ms: u64, now: TimestampMillis) -> &'static str {
    if expiry_unix_ms > now.unix_millis() {
        "active"
    } else {
        "expired"
    }
}

/// Order `all` newest-first and return the requested page (up to `limit`),
/// optionally filtered to one user. Newest-first is `(validated_at, transaction
/// id)` descending — a total, deterministic order. The single place the paging
/// semantics live, so every backend returns identical pages.
#[must_use]
pub fn page_purchases(
    mut all: Vec<Purchase>,
    user_id: Option<&str>,
    limit: usize,
) -> Vec<Purchase> {
    all.retain(|purchase| user_id.is_none_or(|user| purchase.user_id == user));
    all.sort_by(|a, b| {
        (b.validated_at_unix_ms, &b.transaction_id)
            .cmp(&(a.validated_at_unix_ms, &a.transaction_id))
    });
    all.truncate(limit);
    all
}

/// Derive the newest-first subscription rows (purchases carrying an expiry) for
/// the requested filter, up to `limit`, with `active`/`expired` derived against
/// `now`. The single place the subscription-view semantics live.
#[must_use]
pub fn subscription_rows(
    all: Vec<Purchase>,
    user_id: Option<&str>,
    limit: usize,
    now: TimestampMillis,
) -> Vec<SubscriptionRow> {
    page_purchases(all, user_id, usize::MAX)
        .into_iter()
        .filter_map(|purchase| {
            purchase
                .subscription_expiry_unix_ms
                .map(|expiry| SubscriptionRow {
                    transaction_id: purchase.transaction_id,
                    user_id: purchase.user_id,
                    product_id: purchase.product_id,
                    store: purchase.store,
                    expiry_unix_ms: expiry,
                    status: subscription_status(expiry, now),
                })
        })
        .take(limit)
        .collect()
}

// --- Repository contract -----------------------------------------------------

/// Persistence boundary for validated purchase / subscription records.
///
/// The service layer runs receipt validation and builds the [`Purchase`]
/// (computing its receipt digest and validation time) before delegating, so
/// implementations only persist and query. Transaction-id uniqueness (replay
/// rejection) is enforced here so every backend agrees.
#[async_trait]
pub trait PurchasesRepository: Send + Sync {
    /// Record a validated purchase.
    ///
    /// # Errors
    /// - [`Conflict`](crate::error::ErrorCategory::Conflict) if the
    ///   `transaction_id` was already recorded (a replayed receipt).
    /// - A backend error on failure.
    async fn record(&self, purchase: Purchase) -> AppResult<Purchase>;

    /// Purchases newest-first, optionally filtered to one user, up to `limit`.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn list(&self, user_id: Option<&str>, limit: usize) -> AppResult<Vec<Purchase>>;

    /// One purchase by transaction id, or `None` if unknown.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn get(&self, transaction_id: &str) -> AppResult<Option<Purchase>>;

    /// Subscription rows newest-first with `active`/`expired` derived against
    /// `now`, optionally filtered to one user, up to `limit`.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn subscriptions(
        &self,
        user_id: Option<&str>,
        limit: usize,
        now: TimestampMillis,
    ) -> AppResult<Vec<SubscriptionRow>>;
}

/// The stable "duplicate transaction" error, shared by every backend.
pub(crate) fn duplicate_transaction() -> AppError {
    AppError::conflict("transaction already recorded (duplicate receipt)")
}

// --- In-memory reference implementation --------------------------------------

/// The purchase store: `transaction_id -> Purchase`.
type PurchaseStoreMap = HashMap<String, Purchase>;

/// A contract-faithful, in-memory [`PurchasesRepository`] (the reference impl).
///
/// Single-process and not durable, but it enforces the full replay-rejection +
/// paging + subscription-derivation contract through the shared pure helpers, so
/// the contract tests in `tests/wallet_repository_contract.rs` can be reused
/// against the durable backends.
#[derive(Debug, Default)]
pub struct InMemoryPurchasesRepository {
    by_transaction: Mutex<PurchaseStoreMap>,
}

impl InMemoryPurchasesRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, PurchaseStoreMap>> {
        self.by_transaction
            .lock()
            .map_err(|_| AppError::internal("purchases repository mutex poisoned"))
    }
}

#[async_trait]
impl PurchasesRepository for InMemoryPurchasesRepository {
    async fn record(&self, purchase: Purchase) -> AppResult<Purchase> {
        let mut store = self.guard()?;
        if store.contains_key(&purchase.transaction_id) {
            return Err(duplicate_transaction());
        }
        store.insert(purchase.transaction_id.clone(), purchase.clone());
        Ok(purchase)
    }

    async fn list(&self, user_id: Option<&str>, limit: usize) -> AppResult<Vec<Purchase>> {
        let all = self.guard()?.values().cloned().collect();
        Ok(page_purchases(all, user_id, limit))
    }

    async fn get(&self, transaction_id: &str) -> AppResult<Option<Purchase>> {
        Ok(self.guard()?.get(transaction_id).cloned())
    }

    async fn subscriptions(
        &self,
        user_id: Option<&str>,
        limit: usize,
        now: TimestampMillis,
    ) -> AppResult<Vec<SubscriptionRow>> {
        let all = self.guard()?.values().cloned().collect();
        Ok(subscription_rows(all, user_id, limit, now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    fn purchase(tx: &str, user: &str, at: u64, expiry: Option<u64>) -> Purchase {
        Purchase {
            transaction_id: tx.to_string(),
            user_id: user.to_string(),
            product_id: "p".to_string(),
            store: PurchaseStore::Custom,
            receipt_sha256: "digest".to_string(),
            validated_at_unix_ms: at,
            subscription_expiry_unix_ms: expiry,
        }
    }

    // --- pure helpers -------------------------------------------------------

    #[test]
    fn store_tokens_round_trip() {
        for store in [
            PurchaseStore::Apple,
            PurchaseStore::Google,
            PurchaseStore::Huawei,
            PurchaseStore::Custom,
        ] {
            assert_eq!(
                PurchaseStore::from_token(store.as_str()).expect("parse"),
                store
            );
        }
        assert!(PurchaseStore::from_token("steam").is_err());
    }

    #[test]
    fn subscription_status_derives_from_expiry() {
        assert_eq!(subscription_status(10_000, ts(5_000)), "active");
        assert_eq!(subscription_status(2_000, ts(5_000)), "expired");
        assert_eq!(
            subscription_status(5_000, ts(5_000)),
            "expired",
            "not > now"
        );
    }

    #[test]
    fn page_purchases_is_newest_first_and_user_filtered() {
        let all = vec![
            purchase("tx-1", "u-1", 1, None),
            purchase("tx-2", "u-2", 2, None),
            purchase("tx-3", "u-1", 3, None),
        ];
        let mine = page_purchases(all.clone(), Some("u-1"), 10);
        let ids: Vec<&str> = mine.iter().map(|p| p.transaction_id.as_str()).collect();
        assert_eq!(ids, vec!["tx-3", "tx-1"], "newest first, user-filtered");

        let limited = page_purchases(all, None, 2);
        let ids: Vec<&str> = limited.iter().map(|p| p.transaction_id.as_str()).collect();
        assert_eq!(ids, vec!["tx-3", "tx-2"], "limit applied after ordering");
    }

    #[test]
    fn subscription_rows_excludes_consumables() {
        let all = vec![
            purchase("tx-live", "u-1", 1, Some(10_000)),
            purchase("tx-dead", "u-1", 2, Some(2_000)),
            purchase("tx-consumable", "u-1", 3, None),
        ];
        let subs = subscription_rows(all, Some("u-1"), 10, ts(5_000));
        assert_eq!(subs.len(), 2, "consumables are not subscriptions");
        let live = subs
            .iter()
            .find(|s| s.transaction_id == "tx-live")
            .expect("live");
        assert_eq!(live.status, "active");
        let dead = subs
            .iter()
            .find(|s| s.transaction_id == "tx-dead")
            .expect("dead");
        assert_eq!(dead.status, "expired");
    }

    // --- InMemoryPurchasesRepository (reference impl) ----------------------

    #[tokio::test]
    async fn record_rejects_duplicate_transaction() {
        let repo = InMemoryPurchasesRepository::new();
        repo.record(purchase("tx-1", "u-1", 1, None))
            .await
            .expect("first");
        assert_eq!(
            repo.record(purchase("tx-1", "u-2", 2, None))
                .await
                .expect_err("replay")
                .category(),
            crate::error::ErrorCategory::Conflict
        );
        assert_eq!(repo.list(None, 10).await.expect("list").len(), 1);
    }

    #[tokio::test]
    async fn get_returns_recorded_purchase() {
        let repo = InMemoryPurchasesRepository::new();
        repo.record(purchase("tx-1", "u-1", 1, None))
            .await
            .expect("record");
        assert_eq!(
            repo.get("tx-1")
                .await
                .expect("get")
                .expect("present")
                .user_id,
            "u-1"
        );
        assert!(repo.get("nope").await.expect("get").is_none());
    }
}
