//! MongoDB durability contract for the leaderboard reset scheduler.
//!
//! Requires a transaction-capable replica set or sharded MongoDB deployment:
//! `CITADEL_TEST_MONGODB_URL='mongodb://localhost:27017/citadel_test?replicaSet=rs0' \
//! cargo test --test mongodb_leaderboard_reset_repository -- --nocapture`

use citadel::config::DatabaseConfig;
use citadel::leaderboard_scheduler::{ResetEpoch, SchedulerFencingToken};
use citadel::repository::{Backend, CreateLeaderboardRequest, MongoDatabase, Operator, SortOrder};
use citadel::time::{DurationMillis, TimestampMillis};
use mongodb::bson::{Document, doc};

fn ts(value: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(value)
}

async fn connect() -> Option<MongoDatabase> {
    let url = std::env::var("CITADEL_TEST_MONGODB_URL").ok()?;
    Some(
        MongoDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect transaction-capable MongoDB"),
    )
}

async fn create_board(db: &MongoDatabase, id: &str) {
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
async fn mongo_scheduler_repository_atomically_snapshots_clears_and_stages_outbox() {
    let Some(db) = connect().await else {
        eprintln!("skipping MongoDB scheduler repository: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    db.clear_leaderboard_reset_data_for_tests()
        .await
        .expect("clear isolated scheduler fixture");
    db.clear_leaderboards_data_for_tests()
        .await
        .expect("clear isolated leaderboard fixture");

    let database = db.database_for_tests();
    for (collection, expected_index) in [
        (
            "leaderboard_reset_scheduler_lease",
            "scheduler_lease_key_uq",
        ),
        ("leaderboard_reset_epochs", "scheduler_epoch_uq"),
        ("leaderboard_reset_outbox", "scheduler_outbox_epoch_uq"),
        (
            "leaderboard_reset_snapshot_records",
            "scheduler_snapshot_record_uq",
        ),
    ] {
        let indexes = database
            .run_command(doc! {"listIndexes": collection})
            .await
            .expect("scheduler collection and indexes are reconciled");
        assert!(
            indexes
                .get_document("cursor")
                .expect("index cursor")
                .get_array("firstBatch")
                .expect("index batch")
                .iter()
                .filter_map(mongodb::bson::Bson::as_document)
                .any(|index| index.get_str("name").ok() == Some(expected_index))
        );
    }

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
    let first = repository
        .acquire_lease("node-a", ts(0), DurationMillis::from_millis(10))
        .await
        .expect("initial lease")
        .expect("initial owner");
    assert_eq!(first.fencing_token, SchedulerFencingToken::new(1));
    assert!(
        repository
            .acquire_lease("node-b", ts(9), DurationMillis::from_millis(10))
            .await
            .expect("live peer lease")
            .is_none()
    );

    let epoch = ResetEpoch::new("daily".to_owned(), ts(100));
    assert_eq!(
        repository
            .claim_epoch(epoch.clone(), SchedulerFencingToken::new(2), ts(1))
            .await
            .expect_err("stale fencing token")
            .category(),
        citadel::ErrorCategory::Conflict
    );
    assert!(
        repository
            .claim_epoch(epoch.clone(), first.fencing_token, ts(1))
            .await
            .expect("atomic rollover under current fence")
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
    assert_eq!(
        repository.pending_outbox(10).await.expect("outbox"),
        vec![citadel::leaderboard_scheduler::ResetOutboxRecord {
            epoch: epoch.clone(),
            fencing_token: first.fencing_token,
        }]
    );
    assert!(
        !repository
            .claim_epoch(epoch.clone(), first.fencing_token, ts(1))
            .await
            .expect("duplicate epoch preserves existing snapshot")
    );
    assert_eq!(
        repository.snapshot(&epoch).await.expect("same snapshot"),
        Some(snapshot)
    );

    let failover = repository
        .acquire_lease("node-b", ts(10), DurationMillis::from_millis(10))
        .await
        .expect("expired lease failover")
        .expect("replacement owner");
    assert_eq!(failover.fencing_token, SchedulerFencingToken::new(2));

    // The stale worker cannot append another epoch/outbox after failover.
    let epochs = database.collection::<Document>("leaderboard_reset_epochs");
    let outbox = database.collection::<Document>("leaderboard_reset_outbox");
    assert_eq!(
        epochs
            .count_documents(doc! {"leaderboard_id": "daily"})
            .await
            .expect("epochs"),
        1
    );
    assert_eq!(
        outbox
            .count_documents(doc! {"leaderboard_id": "daily"})
            .await
            .expect("outbox"),
        1
    );
}
