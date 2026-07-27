//! Wallet repository contract.
//!
//! Persists per-user virtual-currency **balances** plus an append-only,
//! bounded **ledger** of every change behind the same repository seam as
//! identity/session/storage/friends/groups/leaderboards/chat/notifications, so a
//! player's balances and the ledger they replay from survive a node restart on
//! the durable backends. This is money, so correctness is the whole point: every
//! balance-changing operation appends exactly one ledger entry carrying the
//! post-change balance **and** updates the stored balance **atomically in one
//! transaction** — never two separate autocommit writes that could tear under a
//! crash or a concurrent adjustment.
//!
//! Following the friends/groups/leaderboards/chat/notifications template, the
//! money decision — the checked, non-negative balance arithmetic and the ledger
//! capacity bound — lives in exactly one place: the pure [`apply_delta`] /
//! [`ledger_overflow`] helpers, unit-tested directly here. Every backend
//! ([`InMemoryWalletRepository`], the Postgres `PgWalletRepository`, the SQLite
//! `SqliteWalletRepository`) only does (lock/transaction) read → apply the pure
//! decision → write, so the three implementations cannot drift on the money
//! rules.
//!
//! Balances are a **stored read model** (one row per `(user_id, currency)`)
//! updated in lockstep with the ledger append, not re-derived by summing the
//! ledger on every read: the ledger is capacity-bounded (the oldest entries are
//! evicted) so it is an audit trail, not the source of truth, and a stored
//! balance behind a row/writer lock is the safe, contention-free concurrency
//! model. Ledger ids are a single global monotonic sequence computed as
//! `MAX(id) + 1` inside the change transaction (never a database serial), so the
//! CockroachDB flavor has no identity-column quirks and eviction (oldest-only)
//! never rewinds the sequence.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;

use async_trait::async_trait;
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::time::TimestampMillis;

/// Default bound on retained ledger entries (global, newest kept).
///
/// The ledger is an append-only audit trail; balances are the authoritative
/// stored read model, so bounding the ledger loses history but never a balance.
pub const DEFAULT_LEDGER_CAPACITY: usize = 10_000;

/// One recorded wallet change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LedgerEntry {
    /// Monotonic sequence number (a single global ledger id).
    pub seq: u64,
    /// The wallet owner.
    pub user_id: String,
    /// Currency code (e.g. `coins`, `gems`).
    pub currency: String,
    /// Signed change applied.
    pub delta: i64,
    /// Balance after applying the change.
    pub balance_after: i64,
    /// Sanitized, operator- or game-supplied reason.
    pub reason: String,
    /// When the change happened (Unix millis).
    pub time_unix_ms: u64,
}

// --- Pure decision helpers (the unit-tested money logic) ---------------------

/// Apply a signed `delta` to `current`, enforcing the two money invariants:
/// balances never overflow and never go negative. The single place the balance
/// arithmetic lives, so all three backends compute a new balance identically.
///
/// # Errors
/// - [`Conflict`](crate::error::ErrorCategory::Conflict) if the change would
///   overflow `i64`.
/// - [`Conflict`](crate::error::ErrorCategory::Conflict) if the change would
///   overdraw the balance (result < 0).
pub fn apply_delta(current: i64, delta: i64) -> AppResult<i64> {
    let Some(next) = current.checked_add(delta) else {
        return Err(AppError::conflict("wallet balance would overflow"));
    };
    if next < 0 {
        return Err(AppError::conflict(
            "insufficient funds: wallet balances cannot go negative",
        ));
    }
    Ok(next)
}

/// How many of the oldest ledger entries to evict so that at most `capacity`
/// remain, given `retained` rows are present after an append. The single place
/// the durable backends compute eviction, mirroring the in-memory ring.
#[must_use]
pub fn ledger_overflow(retained: usize, capacity: usize) -> usize {
    retained.saturating_sub(capacity.max(1))
}

// --- Repository contract -----------------------------------------------------

/// Persistence boundary for per-user wallets: balances plus the change ledger.
///
/// The service layer validates the currency label and rejects a zero delta
/// before delegating, so implementations may assume those are well-formed. The
/// money invariants (overflow/overdraw) and the ledger bound are enforced here
/// (via the pure helpers above) so every backend agrees.
#[async_trait]
pub trait WalletRepository: Send + Sync {
    /// Atomically apply a signed adjustment to `user_id`'s `currency` balance:
    /// append one ledger entry carrying the post-change balance and update the
    /// stored balance in one transaction, evicting the oldest ledger entries
    /// beyond `capacity`. Returns the recorded [`LedgerEntry`].
    ///
    /// # Errors
    /// - [`Conflict`](crate::error::ErrorCategory::Conflict) if the change would
    ///   overflow or overdraw the balance (nothing is written).
    /// - A backend error on failure.
    async fn apply_change(
        &self,
        user_id: &str,
        currency: &str,
        delta: i64,
        reason: &str,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<LedgerEntry>;

    /// The user's balances, currency-ordered. Empty map for an unknown user.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn balances(&self, user_id: &str) -> AppResult<BTreeMap<String, i64>>;

    /// The user's ledger entries, newest-first, up to `limit`.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn ledger(&self, user_id: &str, limit: usize) -> AppResult<Vec<LedgerEntry>>;
}

// --- In-memory reference implementation --------------------------------------

/// Mutable state behind the lock: per-`(user, currency)` balances plus the
/// newest-last global ledger ring.
#[derive(Debug, Default)]
struct WalletState {
    /// `user -> currency -> balance` (currencies name-ordered for stable output).
    balances: HashMap<String, BTreeMap<String, i64>>,
    /// Global, bounded, newest-last ledger. The next id is derived as
    /// `max(existing ids) + 1`, matching the durable `MAX(id) + 1` rule.
    ledger: VecDeque<LedgerEntry>,
}

/// A contract-faithful, in-memory [`WalletRepository`] (the reference impl).
///
/// Single-process and not durable, but it enforces the full money contract
/// (checked arithmetic, atomic ledger-append + balance-update under one lock,
/// capacity eviction) through the shared pure helpers, so the contract tests in
/// `tests/wallet_repository_contract.rs` can be reused against the durable
/// backends.
#[derive(Debug, Default)]
pub struct InMemoryWalletRepository {
    state: Mutex<WalletState>,
}

impl InMemoryWalletRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, WalletState>> {
        self.state
            .lock()
            .map_err(|_| AppError::internal("wallet repository mutex poisoned"))
    }
}

#[async_trait]
impl WalletRepository for InMemoryWalletRepository {
    async fn apply_change(
        &self,
        user_id: &str,
        currency: &str,
        delta: i64,
        reason: &str,
        capacity: usize,
        now: TimestampMillis,
    ) -> AppResult<LedgerEntry> {
        let mut state = self.guard()?;
        let current = state
            .balances
            .get(user_id)
            .and_then(|by_currency| by_currency.get(currency))
            .copied()
            .unwrap_or(0);
        // Compute (and validate) BEFORE mutating anything, so an overflow/overdraw
        // leaves balances and the ledger untouched.
        let next = apply_delta(current, delta)?;
        let seq = state.ledger.iter().map(|e| e.seq).max().unwrap_or(0) + 1;
        state
            .balances
            .entry(user_id.to_string())
            .or_default()
            .insert(currency.to_string(), next);
        let entry = LedgerEntry {
            seq,
            user_id: user_id.to_string(),
            currency: currency.to_string(),
            delta,
            balance_after: next,
            reason: reason.to_string(),
            time_unix_ms: now.unix_millis(),
        };
        state.ledger.push_back(entry.clone());
        for _ in 0..ledger_overflow(state.ledger.len(), capacity) {
            state.ledger.pop_front();
        }
        Ok(entry)
    }

    async fn balances(&self, user_id: &str) -> AppResult<BTreeMap<String, i64>> {
        Ok(self
            .guard()?
            .balances
            .get(user_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn ledger(&self, user_id: &str, limit: usize) -> AppResult<Vec<LedgerEntry>> {
        Ok(self
            .guard()?
            .ledger
            .iter()
            .rev()
            .filter(|entry| entry.user_id == user_id)
            .take(limit)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    // --- pure helpers -------------------------------------------------------

    #[test]
    fn apply_delta_credits_and_debits() {
        assert_eq!(apply_delta(0, 100).expect("credit"), 100);
        assert_eq!(apply_delta(100, -30).expect("debit"), 70);
        assert_eq!(apply_delta(5, -5).expect("to zero"), 0);
    }

    #[test]
    fn apply_delta_rejects_overdraw_and_overflow() {
        assert_eq!(
            apply_delta(10, -11).expect_err("overdraw").category(),
            crate::error::ErrorCategory::Conflict
        );
        assert_eq!(
            apply_delta(i64::MAX, 1).expect_err("overflow").category(),
            crate::error::ErrorCategory::Conflict
        );
    }

    #[test]
    fn ledger_overflow_keeps_capacity_newest() {
        assert_eq!(ledger_overflow(5, 3), 2);
        assert_eq!(ledger_overflow(3, 3), 0);
        assert_eq!(ledger_overflow(1, 10_000), 0);
        // Zero capacity clamps to one.
        assert_eq!(ledger_overflow(2, 0), 1);
    }

    // --- InMemoryWalletRepository (reference impl) --------------------------

    #[tokio::test]
    async fn credit_then_debit_updates_balance_and_ledger() {
        let repo = InMemoryWalletRepository::new();
        let cap = DEFAULT_LEDGER_CAPACITY;
        let credit = repo
            .apply_change("u-1", "coins", 100, "grant", cap, ts(1))
            .await
            .expect("credit");
        assert_eq!(credit.seq, 1);
        assert_eq!(credit.balance_after, 100);
        let debit = repo
            .apply_change("u-1", "coins", -30, "spend", cap, ts(2))
            .await
            .expect("debit");
        assert_eq!(debit.seq, 2);
        assert_eq!(debit.balance_after, 70);

        assert_eq!(
            repo.balances("u-1").await.expect("balances").get("coins"),
            Some(&70)
        );
        let ledger = repo.ledger("u-1", 10).await.expect("ledger");
        assert_eq!(ledger.len(), 2);
        assert_eq!(ledger[0].delta, -30, "newest first");
        assert_eq!(ledger[1].balance_after, 100);
    }

    #[tokio::test]
    async fn overdraft_is_rejected_and_changes_nothing() {
        let repo = InMemoryWalletRepository::new();
        let cap = DEFAULT_LEDGER_CAPACITY;
        repo.apply_change("u-1", "coins", 10, "grant", cap, ts(1))
            .await
            .expect("credit");
        assert_eq!(
            repo.apply_change("u-1", "coins", -11, "spend", cap, ts(2))
                .await
                .expect_err("overdraft")
                .category(),
            crate::error::ErrorCategory::Conflict
        );
        assert_eq!(
            repo.balances("u-1").await.expect("balances").get("coins"),
            Some(&10)
        );
        assert_eq!(
            repo.ledger("u-1", 10).await.expect("ledger").len(),
            1,
            "no ledger entry appended on a rejected debit"
        );
    }

    #[tokio::test]
    async fn currencies_are_independent_and_users_isolated() {
        let repo = InMemoryWalletRepository::new();
        let cap = DEFAULT_LEDGER_CAPACITY;
        repo.apply_change("u-1", "coins", 5, "grant", cap, ts(1))
            .await
            .expect("coins");
        repo.apply_change("u-1", "gems", 2, "grant", cap, ts(2))
            .await
            .expect("gems");
        repo.apply_change("u-2", "coins", 9, "grant", cap, ts(3))
            .await
            .expect("other user");
        let balances = repo.balances("u-1").await.expect("balances");
        assert_eq!(balances.len(), 2);
        assert_eq!(balances["gems"], 2);
        assert_eq!(repo.balances("u-2").await.expect("balances")["coins"], 9);
        assert!(repo.balances("u-3").await.expect("balances").is_empty());
        assert_eq!(
            repo.ledger("u-2", 10).await.expect("ledger").len(),
            1,
            "ledger filtered per user"
        );
    }

    #[tokio::test]
    async fn ledger_ring_evicts_oldest_beyond_capacity() {
        let repo = InMemoryWalletRepository::new();
        for i in 1..=5u64 {
            repo.apply_change("u-1", "coins", 1, "credit", 3, ts(i))
                .await
                .expect("credit");
        }
        // Balance still reflects every change (it is the stored read model, not
        // re-derived from the bounded ledger).
        assert_eq!(
            repo.balances("u-1").await.expect("balances").get("coins"),
            Some(&5)
        );
        let ledger = repo.ledger("u-1", 10).await.expect("ledger");
        assert_eq!(ledger.len(), 3, "only the newest 3 entries retained");
        assert_eq!(ledger[0].seq, 5, "eviction never rewinds the sequence");
        assert_eq!(ledger[0].balance_after, 5);
    }
}
