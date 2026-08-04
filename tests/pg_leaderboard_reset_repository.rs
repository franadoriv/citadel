//! PostgreSQL-wire durable scheduler repository contract.
//!
//! The same adapter runs against PostgreSQL and CockroachDB; each integration
//! test is opt-in so ordinary checks do not require either service.

use citadel::config::DatabaseConfig;
use citadel::leaderboard_scheduler::{ResetEpoch, SchedulerFencingToken};
use citadel::repository::{CreateLeaderboardRequest, Operator, PgDatabase, SortOrder};
use citadel::time::{DurationMillis, TimestampMillis};

fn ts(value: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(value)
}

fn postgres_url() -> Option<String> {
    std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
        .filter(|url| !url.trim().is_empty())
}

fn cockroach_url() -> Option<String> {
    std::env::var("CITADEL_TEST_COCKROACH_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
        .map(|url| {
            url.strip_prefix("postgresql://")
                .map(|rest| format!("cockroach://{rest}"))
                .or_else(|| {
                    url.strip_prefix("postgres://")
                        .map(|rest| format!("cockroach://{rest}"))
                })
                .unwrap_or(url)
        })
}

async fn assert_durable_scheduler_contract(url: String) {
    let db = PgDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and migrate PostgreSQL-wire backend");
    db.reset_storage_for_tests().await.expect("reset backend");
    db.leaderboards_repository()
        .create(
            CreateLeaderboardRequest {
                id: "daily".to_owned(),
                sort: SortOrder::Desc,
                operator: Operator::Set,
                reset_schedule: None,
            },
            ts(0),
        )
        .await
        .expect("create scheduler test leaderboard");
    let leaderboards = db.leaderboards_repository();
    leaderboards
        .submit(
            "daily",
            "alice",
            10,
            1,
            Some(serde_json::json!({"tier": "gold"})),
            ts(2),
        )
        .await
        .expect("seed alice");
    leaderboards
        .submit("daily", "bob", 20, 2, None, ts(3))
        .await
        .expect("seed bob");

    let repository = db.leaderboard_reset_repository();
    let epoch = ResetEpoch::new("daily".to_owned(), ts(100));

    let first = repository
        .acquire_lease("node-a", ts(0), DurationMillis::from_millis(10))
        .await
        .expect("initial lease")
        .expect("first node holds lease");
    assert_eq!(first.fencing_token, SchedulerFencingToken::new(1));
    assert!(
        repository
            .acquire_lease("node-b", ts(9), DurationMillis::from_millis(10))
            .await
            .expect("live peer lease")
            .is_none()
    );
    assert_eq!(
        repository
            .claim_epoch(epoch.clone(), SchedulerFencingToken::new(2), ts(1))
            .await
            .expect_err("stale token is rejected")
            .category(),
        citadel::error::ErrorCategory::Conflict
    );
    assert!(
        repository
            .claim_epoch(epoch.clone(), first.fencing_token, ts(1))
            .await
            .expect("atomically roll over under the current fence")
    );
    assert_eq!(
        leaderboards
            .records("daily", 10, 0)
            .await
            .expect("live records are cleared")
            .total,
        0
    );
    let snapshot = repository
        .snapshot(&epoch)
        .await
        .expect("immutable snapshot lookup")
        .expect("snapshot exists for committed epoch");
    assert_eq!(snapshot.epoch, epoch);
    assert_eq!(snapshot.records.len(), 2);
    assert_eq!(snapshot.records[0].user_id, "alice");
    assert_eq!(
        snapshot.records[0].metadata,
        Some(serde_json::json!({"tier": "gold"}))
    );
    assert_eq!(snapshot.records[1].user_id, "bob");
    assert_eq!(
        repository
            .pending_outbox(10)
            .await
            .expect("outbox coupled to committed epoch"),
        vec![citadel::leaderboard_scheduler::ResetOutboxRecord {
            epoch: epoch.clone(),
            fencing_token: first.fencing_token,
        }]
    );

    assert!(
        !repository
            .claim_epoch(epoch.clone(), first.fencing_token, ts(1))
            .await
            .expect("duplicate epoch is not another reset")
    );
    assert_eq!(
        repository.snapshot(&epoch).await.expect("same snapshot"),
        Some(snapshot)
    );

    let failover = repository
        .acquire_lease("node-b", ts(10), DurationMillis::from_millis(10))
        .await
        .expect("expired lease failover")
        .expect("node b holds expired lease");
    assert_eq!(failover.fencing_token, SchedulerFencingToken::new(2));
}

#[tokio::test]
async fn postgres_scheduler_repository_is_durable_and_fenced() {
    let Some(url) = postgres_url() else {
        eprintln!(
            "skipping PostgreSQL scheduler repository: set DATABASE_URL or CITADEL_TEST_DATABASE_URL"
        );
        return;
    };
    assert_durable_scheduler_contract(url).await;
}

#[tokio::test]
async fn cockroach_scheduler_repository_is_durable_and_fenced() {
    let Some(url) = cockroach_url() else {
        eprintln!("skipping CockroachDB scheduler repository: set CITADEL_TEST_COCKROACH_URL");
        return;
    };
    assert_durable_scheduler_contract(url).await;
}
