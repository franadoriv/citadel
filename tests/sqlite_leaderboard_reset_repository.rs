//! SQLite durability tests for the leaderboard reset scheduler repository.

use citadel::config::DatabaseConfig;
use citadel::leaderboard_scheduler::{ResetEpoch, SchedulerFencingToken};
use citadel::repository::{CreateLeaderboardRequest, Operator, SortOrder, SqliteDatabase};
use citadel::time::TimestampMillis;

fn ts(value: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(value)
}

async fn connect() -> SqliteDatabase {
    SqliteDatabase::connect(&DatabaseConfig {
        url: Some("sqlite::memory:".to_owned()),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and migrate SQLite")
}

async fn create_board(db: &SqliteDatabase, id: &str) {
    db.leaderboards_repository()
        .create(
            CreateLeaderboardRequest {
                id: id.to_owned(),
                sort: SortOrder::Desc,
                operator: Operator::Set,
                reset_schedule: None,
            },
            ts(0),
        )
        .await
        .expect("create scheduler test leaderboard");
}

#[tokio::test]
async fn lease_is_exclusive_renewable_and_fenced_after_expiry() {
    let db = connect().await;
    let repository = db.leaderboard_reset_repository();

    let first = repository
        .acquire_lease(
            "node-a",
            ts(0),
            citadel::time::DurationMillis::from_millis(10),
        )
        .await
        .expect("initial lease")
        .expect("node a acquires empty lease");
    assert_eq!(first.fencing_token, SchedulerFencingToken::new(1));
    assert_eq!(first.expires_at, ts(10));

    assert!(
        repository
            .acquire_lease(
                "node-b",
                ts(9),
                citadel::time::DurationMillis::from_millis(10)
            )
            .await
            .expect("live peer lease is a normal no-op")
            .is_none()
    );

    let renewal = repository
        .acquire_lease(
            "node-a",
            ts(9),
            citadel::time::DurationMillis::from_millis(10),
        )
        .await
        .expect("owner renewal")
        .expect("owner renews lease");
    assert_eq!(renewal.fencing_token, SchedulerFencingToken::new(1));
    assert_eq!(renewal.expires_at, ts(19));

    let failover = repository
        .acquire_lease(
            "node-b",
            ts(19),
            citadel::time::DurationMillis::from_millis(10),
        )
        .await
        .expect("expired lease failover")
        .expect("node b acquires expired lease");
    assert_eq!(failover.fencing_token, SchedulerFencingToken::new(2));
    assert_eq!(failover.node_id, "node-b");
}

#[tokio::test]
async fn claim_requires_current_token_and_deduplicates_epoch_with_outbox() {
    let db = connect().await;
    create_board(&db, "daily").await;
    let repository = db.leaderboard_reset_repository();
    let epoch = ResetEpoch::new("daily".to_owned(), ts(100));
    let lease = repository
        .acquire_lease(
            "node-a",
            ts(0),
            citadel::time::DurationMillis::from_millis(10),
        )
        .await
        .expect("lease")
        .expect("acquired");

    assert_eq!(
        repository
            .claim_epoch(epoch.clone(), SchedulerFencingToken::new(2), ts(1))
            .await
            .expect_err("wrong fencing token is rejected")
            .category(),
        citadel::error::ErrorCategory::Conflict
    );
    assert!(
        repository
            .claim_epoch(epoch.clone(), lease.fencing_token, ts(1))
            .await
            .expect("claim under current lease")
    );
    assert!(
        !repository
            .claim_epoch(epoch, lease.fencing_token, ts(1))
            .await
            .expect("duplicate is not a reset")
    );
}

#[tokio::test]
async fn claim_atomically_snapshots_and_clears_records_under_the_current_fence() {
    let db = connect().await;
    create_board(&db, "daily").await;
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
    let token = repository
        .acquire_lease(
            "node-a",
            ts(0),
            citadel::time::DurationMillis::from_millis(10),
        )
        .await
        .expect("lease")
        .expect("acquired")
        .fencing_token;
    let epoch = ResetEpoch::new("daily".to_owned(), ts(100));

    assert!(
        repository
            .claim_epoch(epoch.clone(), token, ts(1))
            .await
            .expect("roll over")
    );
    assert_eq!(
        leaderboards
            .records("daily", 10, 0)
            .await
            .expect("cleared records")
            .total,
        0
    );
    let snapshot = repository
        .snapshot(&epoch)
        .await
        .expect("immutable snapshot lookup")
        .expect("immutable snapshot exists");
    assert_eq!(snapshot.epoch, epoch);
    assert_eq!(snapshot.records.len(), 2);
    assert_eq!(snapshot.records[0].user_id, "alice");
    assert_eq!(
        snapshot.records[0].metadata,
        Some(serde_json::json!({"tier": "gold"}))
    );
    assert_eq!(snapshot.records[1].user_id, "bob");

    assert!(
        !repository
            .claim_epoch(epoch.clone(), token, ts(1))
            .await
            .expect("idempotent")
    );
    assert_eq!(
        repository.snapshot(&epoch).await.expect("same snapshot"),
        Some(snapshot)
    );
}

#[tokio::test]
async fn pending_outbox_is_ordered_limited_and_acknowledged_idempotently() {
    let db = connect().await;
    create_board(&db, "daily").await;
    create_board(&db, "weekly").await;
    let repository = db.leaderboard_reset_repository();
    let token = repository
        .acquire_lease(
            "node-a",
            ts(0),
            citadel::time::DurationMillis::from_millis(10),
        )
        .await
        .expect("lease")
        .expect("acquired")
        .fencing_token;
    let daily = ResetEpoch::new("daily".to_owned(), ts(100));
    let weekly = ResetEpoch::new("weekly".to_owned(), ts(200));
    assert!(
        repository
            .claim_epoch(daily.clone(), token, ts(1))
            .await
            .expect("claim daily")
    );
    assert!(
        repository
            .claim_epoch(weekly.clone(), token, ts(2))
            .await
            .expect("claim weekly")
    );

    assert_eq!(
        repository.pending_outbox(1).await.expect("bounded read"),
        vec![citadel::leaderboard_scheduler::ResetOutboxRecord {
            epoch: daily.clone(),
            fencing_token: token,
        }]
    );
    repository
        .acknowledge_outbox(&daily)
        .await
        .expect("acknowledge delivered callback");
    repository
        .acknowledge_outbox(&daily)
        .await
        .expect("duplicate acknowledgement is idempotent");
    assert_eq!(
        repository
            .pending_outbox(10)
            .await
            .expect("remaining callback"),
        vec![citadel::leaderboard_scheduler::ResetOutboxRecord {
            epoch: weekly,
            fencing_token: token,
        }]
    );
}
