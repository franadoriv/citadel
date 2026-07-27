//! In-app purchase and subscription records (, persisted in ).
//!
//! `PurchaseService` is a thin validate-then-delegate layer over a
//! [`PurchasesRepository`](crate::repository::PurchasesRepository), behind a
//! pluggable [`ReceiptValidator`] seam. It runs a raw receipt through the
//! validator, builds the [`Purchase`] to persist (computing its receipt digest
//! and validation time), and forwards it to the selected persistence backend, so
//! validated purchases and subscription state now survive a node restart on the
//! Postgres and SQLite backends (the in-memory backend stays non-durable by
//! design).
//!
//! Only the deterministic [`DevReceiptValidator`] ships today: it parses a JSON
//! "receipt" (`{"transaction_id", "product_id", "subscription_expiry_unix_ms"?}`)
//! with no network calls, which keeps validation honest for prototyping and
//! tests. Real App Store / Google Play validators are explicit follow-up work
//! (they require outbound HTTPS and store credentials) — see the follow-up task
//! filed by  and the technical-debt register.
//!
//! Invariants: a `transaction_id` is recorded at most once (a replayed receipt is
//! a `Conflict`, enforced by the repository's primary key), and the raw receipt
//! is never stored — only its SHA-256 hex digest, so the store cannot leak
//! resubmittable receipt material.
//!
//! The value types [`Purchase`], [`PurchaseStore`], and [`SubscriptionRow`], plus
//! the paging / subscription-derivation rules, live in the repository layer
//! (`src/repository/purchases.rs`); they are re-exported here so existing
//! console/HTTP consumers keep their `crate::services::…` paths.

use std::sync::Arc;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};
use crate::repository::PurchasesRepository;
use crate::time::TimestampMillis;

// Persistence value types live in the repository module; re-exported so
// `crate::services::Purchase` / `PurchaseStore` / `SubscriptionRow` keep
// resolving for console/HTTP consumers.
pub use crate::repository::purchases::{Purchase, PurchaseStore, SubscriptionRow};

/// What a validator extracted from a receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedReceipt {
    /// Store-unique transaction id.
    pub transaction_id: String,
    /// The purchased product id.
    pub product_id: String,
    /// Subscription expiry, when the product is a subscription.
    pub subscription_expiry_unix_ms: Option<u64>,
}

/// Pluggable receipt validation seam.
///
/// Implementations must be deterministic w.r.t. their inputs and must not
/// panic on hostile receipts — validation failure is a `Validation` error.
pub trait ReceiptValidator: Send + Sync {
    /// Validate `receipt` for `store`, extracting the purchase facts.
    ///
    /// # Errors
    /// `Validation` when the receipt is malformed or fails verification.
    fn validate(&self, store: PurchaseStore, receipt: &str) -> AppResult<ValidatedReceipt>;
}

/// The deterministic development validator: the "receipt" is a JSON document.
///
/// Accepts `{"transaction_id": "...", "product_id": "...",
/// "subscription_expiry_unix_ms": 123?}` and performs no network I/O. Real
/// store validators replace this behind the same trait.
#[derive(Debug, Default)]
pub struct DevReceiptValidator;

/// The dev receipt document shape.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DevReceipt {
    transaction_id: String,
    product_id: String,
    #[serde(default)]
    subscription_expiry_unix_ms: Option<u64>,
}

impl ReceiptValidator for DevReceiptValidator {
    fn validate(&self, _store: PurchaseStore, receipt: &str) -> AppResult<ValidatedReceipt> {
        let parsed: DevReceipt = serde_json::from_str(receipt).map_err(|e| {
            AppError::validation("receipt failed validation").with_detail(e.to_string())
        })?;
        if parsed.transaction_id.trim().is_empty() || parsed.product_id.trim().is_empty() {
            return Err(AppError::validation(
                "receipt failed validation: transaction_id and product_id are required",
            ));
        }
        Ok(ValidatedReceipt {
            transaction_id: parsed.transaction_id,
            product_id: parsed.product_id,
            subscription_expiry_unix_ms: parsed.subscription_expiry_unix_ms,
        })
    }
}

/// Purchase/subscription record store backed by a persistence repository, behind
/// a validator seam.
#[derive(Clone)]
pub struct PurchaseService {
    validator: Arc<dyn ReceiptValidator>,
    repo: Arc<dyn PurchasesRepository>,
}

impl std::fmt::Debug for PurchaseService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PurchaseService").finish_non_exhaustive()
    }
}

impl PurchaseService {
    /// Create a store over a purchases repository (from the selected backend)
    /// using the default [`DevReceiptValidator`].
    #[must_use]
    pub fn new(repo: Arc<dyn PurchasesRepository>) -> Self {
        Self::with_validator(repo, Arc::new(DevReceiptValidator))
    }

    /// Create a store over an explicit validator (tests may inject one).
    #[must_use]
    pub fn with_validator(
        repo: Arc<dyn PurchasesRepository>,
        validator: Arc<dyn ReceiptValidator>,
    ) -> Self {
        Self { validator, repo }
    }

    /// Validate `receipt` and record the purchase for `user_id`.
    ///
    /// # Errors
    /// - `Validation` when the receipt fails validation.
    /// - `Conflict` when the transaction id was already recorded (replay).
    /// - A backend error on failure.
    pub async fn validate_and_record(
        &self,
        user_id: &str,
        store: PurchaseStore,
        receipt: &str,
        now: TimestampMillis,
    ) -> AppResult<Purchase> {
        let validated = self.validator.validate(store, receipt)?;
        let purchase = Purchase {
            transaction_id: validated.transaction_id,
            user_id: user_id.to_string(),
            product_id: validated.product_id,
            store,
            receipt_sha256: sha256_hex(receipt),
            validated_at_unix_ms: now.unix_millis(),
            subscription_expiry_unix_ms: validated.subscription_expiry_unix_ms,
        };
        self.repo.record(purchase).await
    }

    /// Purchases newest-first, optionally filtered to one user.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn purchases(&self, user_id: Option<&str>, limit: usize) -> AppResult<Vec<Purchase>> {
        self.repo.list(user_id, limit).await
    }

    /// One purchase by transaction id.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn get(&self, transaction_id: &str) -> AppResult<Option<Purchase>> {
        self.repo.get(transaction_id).await
    }

    /// Subscription rows newest-first, with `active`/`expired` derived against
    /// `now`, optionally filtered to one user.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn subscriptions(
        &self,
        user_id: Option<&str>,
        limit: usize,
        now: TimestampMillis,
    ) -> AppResult<Vec<SubscriptionRow>> {
        self.repo.subscriptions(user_id, limit, now).await
    }
}

/// SHA-256 hex digest of a receipt (fingerprint; the receipt is never stored).
fn sha256_hex(receipt: &str) -> String {
    let digest = Sha256::digest(receipt.as_bytes());
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryPurchasesRepository;

    fn service() -> PurchaseService {
        PurchaseService::new(Arc::new(InMemoryPurchasesRepository::new()))
    }

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    fn receipt(tx: &str, product: &str, expiry: Option<u64>) -> String {
        match expiry {
            Some(expiry) => format!(
                r#"{{"transaction_id":"{tx}","product_id":"{product}","subscription_expiry_unix_ms":{expiry}}}"#
            ),
            None => format!(r#"{{"transaction_id":"{tx}","product_id":"{product}"}}"#),
        }
    }

    #[tokio::test]
    async fn validated_purchase_is_recorded_with_receipt_fingerprint() {
        let service = service();
        let raw = receipt("tx-1", "gold-pack", None);
        let purchase = service
            .validate_and_record("u-1", PurchaseStore::Custom, &raw, ts(1_000))
            .await
            .expect("validate");
        assert_eq!(purchase.transaction_id, "tx-1");
        assert_eq!(purchase.product_id, "gold-pack");
        assert_eq!(purchase.receipt_sha256.len(), 64);
        // The raw receipt (with resubmittable material) is never retained.
        let rendered = format!("{:?}", service.get("tx-1").await.expect("get"));
        assert!(
            !rendered.contains("gold-pack\"}"),
            "receipt body not stored: {rendered}"
        );
        assert_eq!(service.purchases(None, 10).await.expect("list").len(), 1);
    }

    #[tokio::test]
    async fn duplicate_transactions_are_rejected() {
        let service = service();
        let raw = receipt("tx-dup", "gold", None);
        service
            .validate_and_record("u-1", PurchaseStore::Custom, &raw, ts(1))
            .await
            .expect("first");
        let err = service
            .validate_and_record("u-2", PurchaseStore::Custom, &raw, ts(2))
            .await
            .expect_err("replay rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Conflict);
        assert_eq!(service.purchases(None, 10).await.expect("list").len(), 1);
    }

    #[tokio::test]
    async fn malformed_receipts_fail_validation() {
        let service = service();
        for bad in [
            "not json",
            "{}",
            r#"{"transaction_id":"","product_id":"x"}"#,
        ] {
            let err = service
                .validate_and_record("u-1", PurchaseStore::Custom, bad, ts(1))
                .await
                .expect_err("rejected");
            assert_eq!(err.category(), crate::error::ErrorCategory::Validation);
        }
    }

    #[tokio::test]
    async fn listing_filters_by_user_newest_first() {
        let service = service();
        for (n, user) in [(1, "u-1"), (2, "u-2"), (3, "u-1")] {
            service
                .validate_and_record(
                    user,
                    PurchaseStore::Custom,
                    &receipt(&format!("tx-{n}"), "p", None),
                    ts(n),
                )
                .await
                .expect("record");
        }
        let mine = service.purchases(Some("u-1"), 10).await.expect("list");
        let ids: Vec<&str> = mine.iter().map(|p| p.transaction_id.as_str()).collect();
        assert_eq!(ids, vec!["tx-3", "tx-1"], "newest first, user-filtered");
    }

    #[tokio::test]
    async fn subscription_status_derives_from_expiry() {
        let service = service();
        service
            .validate_and_record(
                "u-1",
                PurchaseStore::Apple,
                &receipt("tx-live", "vip", Some(10_000)),
                ts(1),
            )
            .await
            .expect("live sub");
        service
            .validate_and_record(
                "u-1",
                PurchaseStore::Google,
                &receipt("tx-dead", "vip", Some(2_000)),
                ts(2),
            )
            .await
            .expect("dead sub");
        service
            .validate_and_record(
                "u-1",
                PurchaseStore::Custom,
                &receipt("tx-consumable", "gold", None),
                ts(3),
            )
            .await
            .expect("non-subscription");
        let subs = service
            .subscriptions(Some("u-1"), 10, ts(5_000))
            .await
            .expect("subscriptions");
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
}
