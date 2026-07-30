//! Contract tests for the wallet and purchases repositories.
//!
//! These assert the money semantics that *any* [`WalletRepository`] /
//! [`PurchasesRepository`] implementation must honor — the atomic ledger-append +
//! balance-update, the non-negative/overflow guards, the ledger capacity
//! eviction, replay rejection, and the newest-first / subscription-derivation
//! reads — plus concurrency scenarios proving that read-modify-write is
//! serialized (never two separate autocommit writes) and a duplicated purchase
//! transaction is recorded exactly once. Each scenario runs against every backend:
//!
//! - always against the in-memory reference impls,
//! - always against a real embedded SQLite backend (un-gated; no server), and
//! - against a real Postgres backend when `DATABASE_URL` (or
//!   `CITADEL_TEST_DATABASE_URL`) is set, proving all three behave identically.
//!   The Postgres run is skipped when neither variable is set, so
//!   `bash scripts/check.sh` stays green without a database.
//!
//! Run the Postgres side locally with:
//!
//! ```text
//! DATABASE_URL=postgres://citadel:citadel@localhost:5432/citadel \
//!   cargo test --test wallet_repository_contract
//! ```

use std::sync::Arc;

use citadel::error::ErrorCategory;
use citadel::repository::{
    InMemoryPurchasesRepository, InMemoryWalletRepository, Purchase, PurchaseStore,
    PurchasesRepository, WalletRepository,
};
use citadel::time::TimestampMillis;

/// A roomy ledger capacity for scenarios not exercising eviction.
const CAP: usize = 10_000;

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

// --- Wallet scenarios (backend-agnostic) ------------------------------------

async fn scenario_credit_then_debit_persists(repo: &dyn WalletRepository) {
    let credit = repo
        .apply_change("u-1", "coins", 100, "grant", CAP, ts(1))
        .await
        .expect("credit");
    assert_eq!(credit.balance_after, 100);
    let debit = repo
        .apply_change("u-1", "coins", -30, "spend", CAP, ts(2))
        .await
        .expect("debit");
    assert_eq!(debit.balance_after, 70);

    // Durability: fresh reads reflect the stored balance and newest-first ledger.
    assert_eq!(
        repo.balances("u-1").await.expect("balances").get("coins"),
        Some(&70)
    );
    let ledger = repo.ledger("u-1", 10).await.expect("ledger");
    assert_eq!(ledger.len(), 2);
    assert_eq!(ledger[0].delta, -30, "newest first");
    assert_eq!(ledger[0].balance_after, 70);
    assert_eq!(ledger[1].balance_after, 100);
    assert!(ledger[0].seq > ledger[1].seq, "monotonic ledger ids");
}

async fn scenario_overdraw_and_overflow_change_nothing(repo: &dyn WalletRepository) {
    repo.apply_change("u-1", "coins", 10, "grant", CAP, ts(1))
        .await
        .expect("credit");
    assert_eq!(
        repo.apply_change("u-1", "coins", -11, "spend", CAP, ts(2))
            .await
            .expect_err("overdraw")
            .category(),
        ErrorCategory::Conflict
    );
    assert_eq!(
        repo.apply_change("u-1", "coins", i64::MAX, "overflow", CAP, ts(3))
            .await
            .expect_err("overflow")
            .category(),
        ErrorCategory::Conflict
    );
    // Neither rejected change touched the balance or appended a ledger entry.
    assert_eq!(
        repo.balances("u-1").await.expect("balances").get("coins"),
        Some(&10)
    );
    assert_eq!(repo.ledger("u-1", 10).await.expect("ledger").len(), 1);
}

async fn scenario_currencies_and_users_isolated(repo: &dyn WalletRepository) {
    repo.apply_change("u-1", "coins", 5, "grant", CAP, ts(1))
        .await
        .expect("coins");
    repo.apply_change("u-1", "gems", 2, "grant", CAP, ts(2))
        .await
        .expect("gems");
    repo.apply_change("u-2", "coins", 9, "grant", CAP, ts(3))
        .await
        .expect("other");
    let balances = repo.balances("u-1").await.expect("balances");
    assert_eq!(balances.len(), 2);
    assert_eq!(balances["coins"], 5);
    assert_eq!(balances["gems"], 2);
    assert_eq!(repo.balances("u-2").await.expect("balances")["coins"], 9);
    assert!(repo.balances("u-3").await.expect("balances").is_empty());
    assert_eq!(
        repo.ledger("u-2", 10).await.expect("ledger").len(),
        1,
        "ledger filtered per user"
    );
}

async fn scenario_ledger_capacity_evicts_oldest(repo: &dyn WalletRepository) {
    for i in 1..=5u64 {
        repo.apply_change("u-1", "coins", 1, "credit", 3, ts(i))
            .await
            .expect("credit");
    }
    // Balance still reflects every change (it is the stored read model).
    assert_eq!(
        repo.balances("u-1").await.expect("balances").get("coins"),
        Some(&5)
    );
    let ledger = repo.ledger("u-1", 10).await.expect("ledger");
    assert_eq!(ledger.len(), 3, "only the newest 3 retained");
    assert_eq!(ledger[0].seq, 5, "eviction never rewinds the sequence");
}

async fn scenario_empty_wallet_reads_are_empty(repo: &dyn WalletRepository) {
    assert!(repo.balances("ghost").await.expect("balances").is_empty());
    assert!(repo.ledger("ghost", 10).await.expect("ledger").is_empty());
}

/// No-lost-credit: fire many concurrent same-user credits and prove the final
/// balance equals the sum (the read-modify-write is atomic/serialized, not two
/// racing autocommit writes) and one ledger entry landed per credit.
async fn scenario_concurrent_credits_never_lose(repo: Arc<dyn WalletRepository>) {
    const N: i64 = 25;
    // Prime one credit first so the durable ledger table is non-empty (closing the
    // brand-new-table head-lock race) before the concurrent burst.
    repo.apply_change("u-c", "coins", 100, "prime", CAP, ts(1))
        .await
        .expect("prime");

    let mut handles = Vec::new();
    for _ in 0..N {
        let repo = Arc::clone(&repo);
        handles.push(tokio::spawn(async move {
            repo.apply_change("u-c", "coins", 1, "credit", CAP, ts(2))
                .await
                .expect("concurrent credit");
        }));
    }
    for handle in handles {
        handle.await.expect("join");
    }

    assert_eq!(
        repo.balances("u-c").await.expect("balances").get("coins"),
        Some(&(100 + N)),
        "every concurrent credit is reflected — none lost or doubled"
    );
    let ledger = repo.ledger("u-c", 1000).await.expect("ledger");
    assert_eq!(
        ledger.len(),
        (N + 1) as usize,
        "one ledger entry per successful change"
    );
    // Every ledger id is distinct and the newest reflects the final balance.
    let mut seqs: Vec<u64> = ledger.iter().map(|e| e.seq).collect();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs.len(), (N + 1) as usize, "ledger ids are unique");
    assert_eq!(
        ledger[0].balance_after,
        100 + N,
        "final balance is consistent"
    );
}

/// Concurrent replay delivery: exactly one submission may create the purchase;
/// every competing delivery must receive a conflict and no duplicate record.
async fn scenario_concurrent_duplicate_purchase_is_deduplicated(
    repo: Arc<dyn PurchasesRepository>,
) {
    const N: usize = 16;
    let mut handles = Vec::new();
    for _ in 0..N {
        let repo = Arc::clone(&repo);
        handles.push(tokio::spawn(async move {
            repo.record(purchase("tx-concurrent-dup", "u-1", 1, None))
                .await
        }));
    }

    let mut successes = 0;
    let mut conflicts = 0;
    for handle in handles {
        match handle.await.expect("join") {
            Ok(_) => successes += 1,
            Err(error) => {
                assert_eq!(
                    error.category(),
                    ErrorCategory::Conflict,
                    "replay is a conflict"
                );
                conflicts += 1;
            }
        }
    }
    assert_eq!(successes, 1, "only one concurrent purchase delivery wins");
    assert_eq!(conflicts, N - 1, "every replay is rejected");
    assert_eq!(
        repo.list(None, 10).await.expect("list").len(),
        1,
        "the transaction id is durable exactly once"
    );
}

// --- Purchases scenarios (backend-agnostic) ---------------------------------

async fn scenario_record_and_get_purchase(repo: &dyn PurchasesRepository) {
    repo.record(purchase("tx-1", "u-1", 1, None))
        .await
        .expect("record");
    let got = repo.get("tx-1").await.expect("get").expect("present");
    assert_eq!(got.user_id, "u-1");
    assert_eq!(got.store, PurchaseStore::Custom);
    assert!(repo.get("nope").await.expect("get").is_none());
}

async fn scenario_duplicate_transaction_rejected(repo: &dyn PurchasesRepository) {
    repo.record(purchase("tx-dup", "u-1", 1, None))
        .await
        .expect("first");
    assert_eq!(
        repo.record(purchase("tx-dup", "u-2", 2, None))
            .await
            .expect_err("replay")
            .category(),
        ErrorCategory::Conflict
    );
    assert_eq!(repo.list(None, 10).await.expect("list").len(), 1);
}

async fn scenario_list_newest_first_user_filtered(repo: &dyn PurchasesRepository) {
    for (n, user) in [(1u64, "u-1"), (2, "u-2"), (3, "u-1")] {
        repo.record(purchase(&format!("tx-{n}"), user, n, None))
            .await
            .expect("record");
    }
    let mine = repo.list(Some("u-1"), 10).await.expect("list");
    let ids: Vec<&str> = mine.iter().map(|p| p.transaction_id.as_str()).collect();
    assert_eq!(ids, vec!["tx-3", "tx-1"], "newest first, user-filtered");
    assert_eq!(repo.list(None, 10).await.expect("list").len(), 3);
    assert_eq!(repo.list(None, 2).await.expect("list").len(), 2, "limit");
}

async fn scenario_subscriptions_derived(repo: &dyn PurchasesRepository) {
    repo.record(purchase("tx-live", "u-1", 1, Some(10_000)))
        .await
        .expect("live");
    repo.record(purchase("tx-dead", "u-1", 2, Some(2_000)))
        .await
        .expect("dead");
    repo.record(purchase("tx-consumable", "u-1", 3, None))
        .await
        .expect("consumable");
    let subs = repo
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

// --- Scenario tables --------------------------------------------------------

type ScenarioFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>;
type WalletScenario = (
    &'static str,
    fn(&dyn WalletRepository) -> ScenarioFuture<'_>,
);
type PurchasesScenario = (
    &'static str,
    fn(&dyn PurchasesRepository) -> ScenarioFuture<'_>,
);

macro_rules! wallet_scenarios {
    ($($name:ident),* $(,)?) => {
        vec![$((
            stringify!($name),
            (|repo| -> ScenarioFuture<'_> { Box::pin($name(repo)) })
                as fn(&dyn WalletRepository) -> ScenarioFuture<'_>,
        )),*]
    };
}

macro_rules! purchases_scenarios {
    ($($name:ident),* $(,)?) => {
        vec![$((
            stringify!($name),
            (|repo| -> ScenarioFuture<'_> { Box::pin($name(repo)) })
                as fn(&dyn PurchasesRepository) -> ScenarioFuture<'_>,
        )),*]
    };
}

fn all_wallet_scenarios() -> Vec<WalletScenario> {
    wallet_scenarios![
        scenario_credit_then_debit_persists,
        scenario_overdraw_and_overflow_change_nothing,
        scenario_currencies_and_users_isolated,
        scenario_ledger_capacity_evicts_oldest,
        scenario_empty_wallet_reads_are_empty,
    ]
}

fn all_purchases_scenarios() -> Vec<PurchasesScenario> {
    purchases_scenarios![
        scenario_record_and_get_purchase,
        scenario_duplicate_transaction_rejected,
        scenario_list_newest_first_user_filtered,
        scenario_subscriptions_derived,
    ]
}

// --- In-memory runs (always) ------------------------------------------------

#[tokio::test]
async fn in_memory_backend_satisfies_the_contract() {
    for (_name, run) in all_wallet_scenarios() {
        run(&InMemoryWalletRepository::new()).await;
    }
    for (_name, run) in all_purchases_scenarios() {
        run(&InMemoryPurchasesRepository::new()).await;
    }
    scenario_concurrent_credits_never_lose(Arc::new(InMemoryWalletRepository::new())).await;
    scenario_concurrent_duplicate_purchase_is_deduplicated(Arc::new(
        InMemoryPurchasesRepository::new(),
    ))
    .await;
}

// --- SQLite run (always; embedded, no server) -------------------------------

mod sqlite {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::SqliteDatabase;

    async fn connect() -> SqliteDatabase {
        let config = DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        };
        SqliteDatabase::connect(&config)
            .await
            .expect("connect + migrate against an in-memory SQLite database")
    }

    #[tokio::test]
    async fn sqlite_backend_satisfies_the_contract() {
        let db = connect().await;
        for (name, run) in all_wallet_scenarios() {
            db.reset_storage_for_tests().await.expect("reset");
            eprintln!("sqlite wallet scenario: {name}");
            run(db.wallet_repository().as_ref()).await;
        }
        for (name, run) in all_purchases_scenarios() {
            db.reset_storage_for_tests().await.expect("reset");
            eprintln!("sqlite purchases scenario: {name}");
            run(db.purchases_repository().as_ref()).await;
        }
        db.reset_storage_for_tests().await.expect("reset");
        scenario_concurrent_credits_never_lose(db.wallet_repository()).await;
        db.reset_storage_for_tests().await.expect("reset");
        scenario_concurrent_duplicate_purchase_is_deduplicated(db.purchases_repository()).await;
    }
}

// --- Postgres run (opt-in via DATABASE_URL) ---------------------------------

mod postgres {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::PgDatabase;

    fn test_database_url() -> Option<String> {
        std::env::var("DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
            .filter(|url| !url.trim().is_empty())
    }

    #[tokio::test]
    async fn postgres_backend_satisfies_the_contract() {
        let Some(url) = test_database_url() else {
            eprintln!(
                "skipping Postgres wallet contract: set DATABASE_URL or \
                 CITADEL_TEST_DATABASE_URL to run it"
            );
            return;
        };

        let config = DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        };
        let db = PgDatabase::connect(&config)
            .await
            .expect("connect + migrate against the test Postgres");

        for (name, run) in all_wallet_scenarios() {
            db.reset_storage_for_tests().await.expect("reset");
            eprintln!("postgres wallet scenario: {name}");
            run(db.wallet_repository().as_ref()).await;
        }
        for (name, run) in all_purchases_scenarios() {
            db.reset_storage_for_tests().await.expect("reset");
            eprintln!("postgres purchases scenario: {name}");
            run(db.purchases_repository().as_ref()).await;
        }
        db.reset_storage_for_tests().await.expect("reset");
        scenario_concurrent_credits_never_lose(db.wallet_repository()).await;
        db.reset_storage_for_tests().await.expect("reset");
        scenario_concurrent_duplicate_purchase_is_deduplicated(db.purchases_repository()).await;
    }
}

// --- MongoDB run (opt-in; real rs0 only) -----------------------------------

mod mongodb {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::{Backend, MongoDatabase};

    async fn connect() -> Option<MongoDatabase> {
        let url = std::env::var("CITADEL_TEST_MONGODB_URL").ok()?;
        MongoDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .ok()
    }

    #[tokio::test]
    async fn mongodb_replica_set_satisfies_the_contract() {
        let Some(db) = connect().await else {
            eprintln!("skipping MongoDB wallet contract: CITADEL_TEST_MONGODB_URL is unset");
            return;
        };
        for (name, run) in all_wallet_scenarios() {
            db.clear_wallet_purchases_data_for_tests()
                .await
                .expect("reset");
            eprintln!("mongodb wallet scenario: {name}");
            run(db.wallet_repository().as_ref()).await;
        }
        for (name, run) in all_purchases_scenarios() {
            db.clear_wallet_purchases_data_for_tests()
                .await
                .expect("reset");
            eprintln!("mongodb purchases scenario: {name}");
            run(db.purchases_repository().as_ref()).await;
        }
        db.clear_wallet_purchases_data_for_tests()
            .await
            .expect("reset");
        scenario_concurrent_credits_never_lose(db.wallet_repository()).await;
        db.clear_wallet_purchases_data_for_tests()
            .await
            .expect("reset");
        scenario_concurrent_duplicate_purchase_is_deduplicated(db.purchases_repository()).await;
    }
}
