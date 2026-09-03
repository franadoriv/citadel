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
//! The assembled node uses [`CompositeReceiptValidator`]: only the deterministic
//! custom development validator is enabled today, while Apple, Google, and Huawei
//! adapters fail closed until their dedicated verified tasks ship. The custom
//! validator parses a JSON receipt with no network calls, keeping local
//! prototyping/tests honest without claiming real-store validation.
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
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::config::PurchaseValidationConfig;
use crate::error::{AppError, AppResult};
use crate::observability::NodeMetrics;
use crate::repository::PurchasesRepository;
use crate::runtime::outbound_http::{
    OutboundHttpPolicy, OutboundHttpRequest, OutboundHttpResponse, TrustedHttpClient,
};
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
#[async_trait]
pub trait ReceiptValidator: Send + Sync {
    /// Validate `receipt` for `store`, extracting the purchase facts.
    ///
    /// # Errors
    /// Returns a sanitized typed error when the receipt is malformed, its
    /// provider is disabled, or remote validation cannot complete.
    async fn validate(&self, store: PurchaseStore, receipt: &str) -> AppResult<ValidatedReceipt>;
}

/// Stable, sanitized receipt-validation outcomes for logs and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptValidationErrorCode {
    PayloadTooLarge,
    HttpsRequired,
    ProviderDisabled,
    ProviderUnavailable,
    ProviderTimedOut,
}

impl ReceiptValidationErrorCode {
    /// Stable lowercase label; never contains provider payloads or credentials.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PayloadTooLarge => "payload_too_large",
            Self::HttpsRequired => "https_required",
            Self::ProviderDisabled => "provider_disabled",
            Self::ProviderUnavailable => "provider_unavailable",
            Self::ProviderTimedOut => "provider_timed_out",
        }
    }

    fn app_error(self) -> AppError {
        match self {
            Self::PayloadTooLarge => {
                AppError::validation("receipt validation payload is too large")
            }
            Self::HttpsRequired => AppError::validation("receipt validation requires HTTPS"),
            Self::ProviderDisabled => {
                AppError::permission("receipt validation provider is disabled")
            }
            Self::ProviderUnavailable => AppError::new(
                crate::error::ErrorCategory::Transport,
                "receipt validation provider is unavailable",
            ),
            Self::ProviderTimedOut => AppError::new(
                crate::error::ErrorCategory::Deadline,
                "receipt validation provider timed out",
            ),
        }
    }
}

/// Server-owned HTTP boundary reserved for future real receipt providers.
///
/// It reuses Citadel's bounded Rust-owned HTTPS client: no runtime receives its
/// credentials or a socket, redirects and proxies stay disabled, DNS rebinding
/// is fenced, and both concurrency and request rate are limited.
#[derive(Clone, Debug)]
pub struct ReceiptValidationHttpClient {
    client: TrustedHttpClient,
    timeout: Duration,
    // Provider adapters may retry only explicitly idempotent operations within
    // this small policy budget; the foundation does not retry arbitrary POSTs.
    _max_retries: u8,
    metrics: Arc<NodeMetrics>,
}

impl ReceiptValidationHttpClient {
    /// Build from the validated non-secret purchase policy.
    #[must_use]
    pub fn from_config(config: &PurchaseValidationConfig, metrics: Arc<NodeMetrics>) -> Self {
        let policy = OutboundHttpPolicy {
            enabled: true,
            max_concurrent_requests: config.max_concurrent_requests,
            max_requests_per_minute: config.max_requests_per_minute,
            allowed_hosts: config.allowed_hosts.clone(),
            allowed_ports: vec![443],
            allow_private_networks: false,
        };
        let client = TrustedHttpClient::new_with_policy(policy)
            .expect("validated purchase policy builds a bounded HTTP client");
        Self {
            client,
            timeout: Duration::from_millis(config.timeout_ms),
            _max_retries: config.max_retries,
            metrics,
        }
    }

    /// Execute one provider request under the shared bounded egress policy.
    ///
    /// Provider adapters call this method rather than constructing a networking
    /// client themselves. Transport internals are deliberately collapsed into a
    /// small redacted error taxonomy.
    pub async fn execute(&self, request: OutboundHttpRequest) -> AppResult<OutboundHttpResponse> {
        self.metrics.record_purchase_validation_request();
        if !request.url.starts_with("https://") {
            self.metrics.record_purchase_validation_failure();
            return Err(ReceiptValidationErrorCode::HttpsRequired.app_error());
        }
        match tokio::time::timeout(self.timeout, self.client.execute(request)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_error)) => {
                self.metrics.record_purchase_validation_failure();
                Err(ReceiptValidationErrorCode::ProviderUnavailable.app_error())
            }
            Err(_) => {
                self.metrics.record_purchase_validation_failure();
                Err(ReceiptValidationErrorCode::ProviderTimedOut.app_error())
            }
        }
    }
}

/// Composite provider boundary. Real providers remain absent until their own
/// verified implementation tasks install adapters; the custom dev path remains
/// available for local prototypes and tests.
#[derive(Clone)]
pub struct CompositeReceiptValidator {
    max_receipt_bytes: usize,
    custom: Arc<dyn ReceiptValidator>,
    apple: Option<Arc<dyn ReceiptValidator>>,
    google: Option<Arc<dyn ReceiptValidator>>,
    huawei: Option<Arc<dyn ReceiptValidator>>,
    _http: ReceiptValidationHttpClient,
}

impl std::fmt::Debug for CompositeReceiptValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeReceiptValidator")
            .finish_non_exhaustive()
    }
}

impl CompositeReceiptValidator {
    /// Assemble the default-safe provider set from configuration.
    #[must_use]
    pub fn from_config(config: &PurchaseValidationConfig, metrics: Arc<NodeMetrics>) -> Self {
        Self {
            max_receipt_bytes: config.max_receipt_bytes,
            custom: Arc::new(DevReceiptValidator),
            apple: None,
            google: None,
            huawei: None,
            _http: ReceiptValidationHttpClient::from_config(config, metrics),
        }
    }

    fn provider(&self, store: PurchaseStore) -> Option<&Arc<dyn ReceiptValidator>> {
        match store {
            PurchaseStore::Custom => Some(&self.custom),
            PurchaseStore::Apple => self.apple.as_ref(),
            PurchaseStore::Google => self.google.as_ref(),
            PurchaseStore::Huawei => self.huawei.as_ref(),
        }
    }
}

#[async_trait]
impl ReceiptValidator for CompositeReceiptValidator {
    async fn validate(&self, store: PurchaseStore, receipt: &str) -> AppResult<ValidatedReceipt> {
        if receipt.len() > self.max_receipt_bytes {
            return Err(ReceiptValidationErrorCode::PayloadTooLarge.app_error());
        }
        let Some(provider) = self.provider(store) else {
            return Err(ReceiptValidationErrorCode::ProviderDisabled.app_error());
        };
        provider.validate(store, receipt).await
    }
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

#[async_trait]
impl ReceiptValidator for DevReceiptValidator {
    async fn validate(&self, _store: PurchaseStore, receipt: &str) -> AppResult<ValidatedReceipt> {
        let parsed: DevReceipt = serde_json::from_str(receipt)
            .map_err(|_| AppError::validation("receipt failed validation"))?;
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
    /// Create a store over a purchases repository with the fail-closed default
    /// provider set. Only custom development receipts are accepted until a real
    /// store adapter is installed by its dedicated implementation task.
    #[must_use]
    pub fn new(repo: Arc<dyn PurchasesRepository>) -> Self {
        Self::with_validator(
            repo,
            Arc::new(CompositeReceiptValidator::from_config(
                &PurchaseValidationConfig::default(),
                Arc::new(NodeMetrics::new()),
            )),
        )
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
        let validated = self.validator.validate(store, receipt).await?;
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
    use std::collections::BTreeMap;

    use crate::repository::InMemoryPurchasesRepository;

    fn service() -> PurchaseService {
        PurchaseService::new(Arc::new(InMemoryPurchasesRepository::new()))
    }

    fn dev_service() -> PurchaseService {
        PurchaseService::with_validator(
            Arc::new(InMemoryPurchasesRepository::new()),
            Arc::new(DevReceiptValidator),
        )
    }

    fn composite() -> CompositeReceiptValidator {
        CompositeReceiptValidator::from_config(
            &PurchaseValidationConfig::default(),
            Arc::new(NodeMetrics::new()),
        )
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
    async fn malformed_dev_receipts_do_not_retain_attacker_controlled_parse_detail() {
        let error = DevReceiptValidator
            .validate(
                PurchaseStore::Custom,
                r#"{"transaction_id":"tx","product_id":"gold","receipt_canary":"secret"}"#,
            )
            .await
            .expect_err("unknown fields must fail validation");
        assert_eq!(error.category(), crate::error::ErrorCategory::Validation);
        assert_eq!(
            error.log_detail(),
            None,
            "receipt parse detail must stay redacted"
        );
    }

    #[tokio::test]
    async fn purchase_service_default_rejects_real_store_labeled_dev_json() {
        let error = service()
            .validate_and_record(
                "u-1",
                PurchaseStore::Google,
                &receipt("tx-google", "gold", None),
                ts(1),
            )
            .await
            .expect_err("the default constructor must not claim Google validation");
        assert_eq!(error.category(), crate::error::ErrorCategory::Permission);
        assert_eq!(error.message(), "receipt validation provider is disabled");
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
    async fn real_store_providers_are_disabled_by_default_but_custom_dev_receipts_work() {
        let validator = composite();

        let error = validator
            .validate(
                PurchaseStore::Apple,
                r#"{"transaction_id":"tx-1","product_id":"gold"}"#,
            )
            .await
            .expect_err("Apple validation must stay disabled until its provider task ships");
        assert_eq!(error.category(), crate::error::ErrorCategory::Permission);
        assert_eq!(error.message(), "receipt validation provider is disabled");
        assert_eq!(
            error.log_detail(),
            None,
            "receipt material is never retained in errors"
        );

        let receipt = validator
            .validate(
                PurchaseStore::Custom,
                r#"{"transaction_id":"dev-1","product_id":"gold"}"#,
            )
            .await
            .expect("the deterministic custom development validator remains available");
        assert_eq!(receipt.transaction_id, "dev-1");
    }

    #[tokio::test]
    async fn receipt_validation_http_client_refuses_non_https_before_network_io() {
        let metrics = Arc::new(NodeMetrics::new());
        let client = ReceiptValidationHttpClient::from_config(
            &PurchaseValidationConfig::default(),
            Arc::clone(&metrics),
        );
        let error = client
            .execute(OutboundHttpRequest {
                method: "GET".to_string(),
                url: "http://api.storekit.itunes.apple.com/".to_string(),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .await
            .expect_err("receipt validation egress must require HTTPS");
        assert_eq!(error.category(), crate::error::ErrorCategory::Validation);
        assert_eq!(error.message(), "receipt validation requires HTTPS");
        assert_eq!(metrics.snapshot().purchase_validation_requests_total, 1);
        assert_eq!(metrics.snapshot().purchase_validation_failures_total, 1);
    }

    #[tokio::test]
    async fn composite_validator_rejects_oversized_receipts_before_provider_dispatch() {
        let config = PurchaseValidationConfig {
            max_receipt_bytes: 3,
            ..PurchaseValidationConfig::default()
        };
        let validator =
            CompositeReceiptValidator::from_config(&config, Arc::new(NodeMetrics::new()));

        let error = validator
            .validate(PurchaseStore::Custom, "four")
            .await
            .expect_err("payload limit must reject before parsing or egress");
        assert_eq!(error.category(), crate::error::ErrorCategory::Validation);
        assert_eq!(error.message(), "receipt validation payload is too large");
    }

    #[tokio::test]
    async fn subscription_status_derives_from_expiry() {
        let service = dev_service();
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
