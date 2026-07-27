//! Per-user virtual-currency wallets (, persisted in ).
//!
//! `WalletService` is a thin validate-then-delegate layer over a
//! [`WalletRepository`](crate::repository::WalletRepository): it validates the
//! currency label and rejects a zero delta, then forwards every operation to the
//! selected persistence backend, so per-user balances and their change ledger now
//! survive a node restart on the Postgres and SQLite backends (the in-memory
//! backend stays non-durable by design).
//!
//! Invariants (enforced in the repository's pure helpers so all three backends
//! agree):
//!
//! - Balances are non-negative: an adjustment that would overdraw is rejected
//!   with a `Conflict` error and changes nothing.
//! - Every successful adjustment appends exactly one ledger entry carrying the
//!   post-adjustment balance **atomically** with the balance update, so the
//!   stored balance and the ledger never tear apart.
//!
//! The value type [`LedgerEntry`] and the money rules live in the repository
//! layer (`src/repository/wallet.rs`) as pure, unit-tested helpers; the type is
//! re-exported here so existing console/HTTP consumers keep their
//! `crate::services::…` paths.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{AppError, AppResult};
use crate::repository::WalletRepository;
use crate::time::TimestampMillis;
use crate::validate;

// Persistence value types live in the repository module; re-exported so
// `crate::services::LedgerEntry` keeps resolving for console/HTTP consumers.
pub use crate::repository::wallet::{DEFAULT_LEDGER_CAPACITY, LedgerEntry};

/// Maximum byte length of a currency code.
const MAX_CURRENCY_LEN: usize = 64;

/// Per-user wallet store backed by a persistence repository.
///
/// Holds an `Arc<dyn WalletRepository>` from the selected backend plus the ledger
/// retention `capacity`. All operations are `async` and delegate to the
/// repository; `adjust` validates its input first.
#[derive(Clone)]
pub struct WalletService {
    repo: Arc<dyn WalletRepository>,
    capacity: usize,
}

impl WalletService {
    /// Create a service over a wallet repository (from the selected backend) using
    /// the default ledger retention bound ([`DEFAULT_LEDGER_CAPACITY`]).
    #[must_use]
    pub fn new(repo: Arc<dyn WalletRepository>) -> Self {
        Self::with_capacity(repo, DEFAULT_LEDGER_CAPACITY)
    }

    /// Create a service retaining at most `capacity` ledger entries (minimum 1).
    #[must_use]
    pub fn with_capacity(repo: Arc<dyn WalletRepository>, capacity: usize) -> Self {
        Self {
            repo,
            capacity: capacity.max(1),
        }
    }

    /// The ledger retention bound.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Apply a signed adjustment to `user_id`'s `currency` balance, appending a
    /// ledger entry. Returns the new balance.
    ///
    /// # Errors
    /// - `Validation` for a malformed currency code or a zero delta.
    /// - `Conflict` if the adjustment would overdraw or overflow the balance.
    /// - A backend error on failure.
    pub async fn adjust(
        &self,
        user_id: &str,
        currency: &str,
        delta: i64,
        reason: &str,
        now: TimestampMillis,
    ) -> AppResult<i64> {
        validate::label("currency", currency, MAX_CURRENCY_LEN)?;
        if delta == 0 {
            return Err(AppError::validation("wallet delta must not be zero"));
        }
        let entry = self
            .repo
            .apply_change(user_id, currency, delta, reason, self.capacity, now)
            .await?;
        Ok(entry.balance_after)
    }

    /// The user's balances, currency-ordered. Empty map for an unknown user.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn balances(&self, user_id: &str) -> AppResult<BTreeMap<String, i64>> {
        self.repo.balances(user_id).await
    }

    /// The user's ledger entries, newest-first, up to `limit`.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn ledger(&self, user_id: &str, limit: usize) -> AppResult<Vec<LedgerEntry>> {
        self.repo.ledger(user_id, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryWalletRepository;

    fn service() -> WalletService {
        WalletService::new(Arc::new(InMemoryWalletRepository::new()))
    }

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    #[tokio::test]
    async fn credit_debit_round_trip_updates_balance_and_ledger() {
        let wallet = service();
        assert_eq!(
            wallet
                .adjust("u-1", "coins", 100, "grant", ts(1))
                .await
                .expect("credit"),
            100
        );
        assert_eq!(
            wallet
                .adjust("u-1", "coins", -30, "spend", ts(2))
                .await
                .expect("debit"),
            70
        );
        assert_eq!(
            wallet.balances("u-1").await.expect("balances").get("coins"),
            Some(&70)
        );

        let ledger = wallet.ledger("u-1", 10).await.expect("ledger");
        assert_eq!(ledger.len(), 2);
        assert_eq!(ledger[0].delta, -30, "newest first");
        assert_eq!(ledger[0].balance_after, 70);
        assert_eq!(ledger[1].balance_after, 100);
    }

    #[tokio::test]
    async fn overdraft_is_rejected_and_changes_nothing() {
        let wallet = service();
        wallet
            .adjust("u-1", "coins", 10, "grant", ts(1))
            .await
            .expect("credit");
        let err = wallet
            .adjust("u-1", "coins", -11, "spend", ts(2))
            .await
            .expect_err("overdraft rejected");
        assert_eq!(err.category(), crate::error::ErrorCategory::Conflict);
        assert_eq!(
            wallet.balances("u-1").await.expect("balances").get("coins"),
            Some(&10)
        );
        assert_eq!(
            wallet.ledger("u-1", 10).await.expect("ledger").len(),
            1,
            "no ledger entry appended"
        );
    }

    #[tokio::test]
    async fn zero_delta_and_bad_currency_are_validation_errors() {
        let wallet = service();
        assert!(
            wallet
                .adjust("u-1", "coins", 0, "noop", ts(1))
                .await
                .is_err()
        );
        assert!(wallet.adjust("u-1", "", 1, "grant", ts(1)).await.is_err());
        assert!(
            wallet
                .adjust("u-1", "with\nnewline", 1, "x", ts(1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn currencies_are_independent_and_users_isolated() {
        let wallet = service();
        wallet
            .adjust("u-1", "coins", 5, "grant", ts(1))
            .await
            .expect("coins");
        wallet
            .adjust("u-1", "gems", 2, "grant", ts(2))
            .await
            .expect("gems");
        wallet
            .adjust("u-2", "coins", 9, "grant", ts(3))
            .await
            .expect("other user");
        let balances = wallet.balances("u-1").await.expect("balances");
        assert_eq!(balances.len(), 2);
        assert_eq!(balances["gems"], 2);
        assert_eq!(wallet.balances("u-2").await.expect("balances")["coins"], 9);
        assert!(wallet.balances("u-3").await.expect("balances").is_empty());
        assert_eq!(
            wallet.ledger("u-2", 10).await.expect("ledger").len(),
            1,
            "ledger filtered per user"
        );
    }
}
