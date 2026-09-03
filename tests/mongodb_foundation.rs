//! MongoDB durable-backend integration check.
//!
//! Run against a transaction-capable replica set, never standalone mongod:
//! `CITADEL_TEST_MONGODB_URL='mongodb://localhost:27017/citadel_test?replicaSet=rs0' \
//! cargo test --test mongodb_foundation -- --nocapture`

use citadel::config::DatabaseConfig;
use citadel::database_explorer::{
    DatabaseExplorer, FilterOperator, ListRowsRequest, RowFilter, SortDirection, SortSpec, TableRef,
};
use citadel::repository::chat::ChatDeliveryRequest;
use citadel::repository::{
    Backend, BackendKind, ChannelType, ChatDeliveryOutboxRecord, ChatModerationAudit,
    ChatRateLimit, ChatRepository, CreateGroupRequest, CreateLeaderboardRequest, FriendState,
    GroupFilter, MongoDatabase, Operator, Recipient, SortOrder, UnitOfWork, select_backend,
};
use citadel::storage::{
    Accessor, Collection, Key, ObjectId, Owner, Permissions, StorageIndexDefinition,
    StorageIndexField, StorageIndexName, StorageIndexQuery, StorageValue, UserId, WriteRequest,
};
use citadel::time::TimestampMillis;
use mongodb::bson::{Bson, JavaScriptCodeWithScope, doc, oid::ObjectId as BsonObjectId};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn replica_set_connects_reconciles_schema_idempotently_and_starts_a_transaction() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL").ok() else {
        eprintln!(
            "skipping MongoDB durable-backend integration test: CITADEL_TEST_MONGODB_URL is unset"
        );
        return;
    };
    let config = DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    };

    let first = select_backend(&config)
        .await
        .expect("replica set selects a durable MongoDB backend");
    assert_eq!(first.kind(), BackendKind::MongoDb);
    first
        .begin()
        .await
        .expect("transaction begins on replica set")
        .rollback()
        .await
        .expect("transaction aborts cleanly");

    select_backend(&config)
        .await
        .expect("second reconciliation is idempotent");
}

#[tokio::test]
async fn mongodb_backend_chat_repository_is_durable_across_fresh_connections() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL").ok() else {
        eprintln!("skipping MongoDB chat backend routing test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let config = DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    };
    let verifier = MongoDatabase::connect(&config)
        .await
        .expect("connect Mongo verifier");
    for collection in ["chat_channels", "chat_messages", "chat_events"] {
        verifier
            .database_for_tests()
            .collection::<mongodb::bson::Document>(collection)
            .delete_many(doc! {})
            .await
            .expect("clear Mongo chat routing fixture");
    }

    let backend = select_backend(&config).await.expect("select Mongo backend");
    assert_eq!(backend.kind(), BackendKind::MongoDb);
    backend
        .chat_repository()
        .post_message(
            "mongo-backend-routing",
            ChannelType::Room,
            "alice",
            "durable",
            10,
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("post through Backend::chat_repository");

    let fresh = MongoDatabase::connect(&config)
        .await
        .expect("reconnect Mongo verifier");
    let history = fresh
        .mongo_chat_repository()
        .channel_history("mongo-backend-routing", 10, None)
        .await
        .expect("read durable Mongo message");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].content, "durable");
}

#[tokio::test]
async fn chat_foundation_reconciles_all_collections_and_session_helpers_rollback_together() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB chat foundation test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };

    let database = db.database_for_tests();
    for collection in [
        "chat_channels",
        "chat_access_epochs",
        "chat_messages",
        "chat_events",
        "chat_moderation_audit",
        "chat_rate_limits",
        "chat_delivery_outbox",
    ] {
        let indexes = database
            .run_command(doc! { "listIndexes": collection })
            .await
            .expect("chat collection and indexes are reconciled");
        let names = indexes
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("index batch")
            .iter()
            .filter_map(|index| index.as_document())
            .filter_map(|index| index.get_str("name").ok())
            .collect::<Vec<_>>();
        assert!(names.len() > 1, "{collection} has its contract index");
    }
    for (collection, index) in [
        ("chat_channels", "chat_channel_type"),
        ("chat_messages", "chat_message_time_order"),
        ("chat_rate_limits", "chat_rate_limit_expiry"),
        ("chat_delivery_outbox", "chat_outbox_event_uq"),
    ] {
        let indexes = database
            .run_command(doc! { "listIndexes": collection })
            .await
            .expect("indexes");
        assert!(
            indexes
                .get_document("cursor")
                .expect("cursor")
                .get_array("firstBatch")
                .expect("index batch")
                .iter()
                .filter_map(|entry| entry.as_document())
                .any(|entry| entry.get_str("name").ok() == Some(index)),
            "{collection}.{index} exists"
        );
    }

    let left = database.collection::<mongodb::bson::Document>("chat_foundation_left");
    let right = database.collection::<mongodb::bson::Document>("chat_foundation_right");
    left.delete_many(doc! {}).await.expect("clear left fixture");
    right
        .delete_many(doc! {})
        .await
        .expect("clear right fixture");

    let uow = db.begin().await.expect("begin enclosing UoW");
    uow.mongo_chat_repository()
        .with_transaction(|database, session| {
            Box::pin(async move {
                database
                    .collection::<mongodb::bson::Document>("chat_foundation_left")
                    .insert_one(doc! { "_id": "left" })
                    .session(&mut *session)
                    .await?;
                database
                    .collection::<mongodb::bson::Document>("chat_foundation_right")
                    .insert_one(doc! { "_id": "right" })
                    .session(&mut *session)
                    .await?;
                Ok(())
            })
        })
        .await
        .expect("session-bound chat helper writes through the UoW");
    uow.rollback().await.expect("rollback enclosing UoW");

    assert_eq!(left.count_documents(doc! {}).await.expect("count left"), 0);
    assert_eq!(
        right.count_documents(doc! {}).await.expect("count right"),
        0
    );
}

#[tokio::test]
async fn mongodb_chat_conversation_message_contract_uses_transactions_and_keyset_history() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB chat contract test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let database = db.database_for_tests();
    for name in ["chat_channels", "chat_access_epochs", "chat_messages"] {
        database
            .collection::<mongodb::bson::Document>(name)
            .delete_many(doc! {})
            .await
            .expect("clear chat fixture");
    }
    let repo = db.mongo_chat_repository();
    let first = repo
        .resolve_canonical_channel(
            "room:contract",
            ChannelType::Room,
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("resolve");
    assert_eq!(
        first,
        repo.resolve_canonical_channel(
            "room:contract",
            ChannelType::Room,
            TimestampMillis::from_unix_millis(2)
        )
        .await
        .expect("idempotent resolve")
    );
    for n in 1..=4 {
        assert_eq!(
            repo.post_message(
                &first.id,
                ChannelType::Room,
                "alice",
                "message",
                3,
                TimestampMillis::from_unix_millis(n)
            )
            .await
            .expect("post"),
            n
        );
    }
    assert_eq!(
        repo.channel_history(&first.id, 2, None)
            .await
            .expect("first page")
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![4, 3]
    );
    assert_eq!(
        repo.channel_history(&first.id, 2, Some(3))
            .await
            .expect("keyset page")
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>(),
        vec![2]
    );
    let epoch = repo
        .current_access_epoch("room:contract")
        .await
        .expect("epoch");
    assert_eq!(
        repo.post_message_authorized(
            &first.id,
            ChannelType::Room,
            "alice",
            "authorized",
            3,
            "room:contract",
            epoch,
            TimestampMillis::from_unix_millis(5)
        )
        .await
        .expect("authorized post"),
        5
    );
    repo.advance_access_epoch("room:contract", TimestampMillis::from_unix_millis(6))
        .await
        .expect("revoke");
    assert!(
        repo.post_message_authorized(
            &first.id,
            ChannelType::Room,
            "alice",
            "stale",
            3,
            "room:contract",
            epoch,
            TimestampMillis::from_unix_millis(7)
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn mongodb_chat_visible_state_matches_0391_contract_edges() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB chat contract edges: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let database = db.database_for_tests();
    for name in ["chat_channels", "chat_access_epochs", "chat_messages"] {
        database
            .collection::<mongodb::bson::Document>(name)
            .delete_many(doc! {})
            .await
            .expect("clear chat fixture");
    }

    let repo = db.mongo_chat_repository();
    let descriptor = repo
        .resolve_canonical_channel(
            "direct:alice:bob",
            ChannelType::Direct,
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("resolve canonical descriptor");
    assert!(descriptor.id.starts_with("ch_"));
    assert!(
        repo.resolve_canonical_channel(
            "direct:alice:bob",
            ChannelType::Group,
            TimestampMillis::from_unix_millis(2),
        )
        .await
        .is_err()
    );

    for id in 1..=3 {
        repo.post_message(
            "lobby-eu",
            ChannelType::Room,
            "alice",
            "message",
            2,
            TimestampMillis::from_unix_millis(id),
        )
        .await
        .expect("post bounded message");
    }
    repo.post_message(
        "lobby-na",
        ChannelType::Room,
        "bob",
        "message",
        2,
        TimestampMillis::from_unix_millis(3),
    )
    .await
    .expect("post tied channel");
    assert_eq!(
        repo.channel_history("lobby-eu", 0, None)
            .await
            .expect("history")
            .iter()
            .map(|message| message.id)
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
    assert_eq!(
        repo.list_channels(Some("lobby"), 0)
            .await
            .expect("case-sensitive filter")
            .into_iter()
            .map(|channel| channel.channel)
            .collect::<Vec<_>>(),
        vec!["lobby-eu", "lobby-na"],
        "activity ties use ascending channel id"
    );
    assert!(
        repo.list_channels(Some("LOBBY"), 0)
            .await
            .expect("case-sensitive filter")
            .is_empty()
    );

    let edited = repo
        .edit_message(
            "lobby-eu",
            3,
            "edited",
            TimestampMillis::from_unix_millis(4),
        )
        .await
        .expect("edit");
    assert_eq!((edited.revision, edited.last_event_id), (2, 4));
    assert!(
        repo.delete_message("lobby-eu", 2, TimestampMillis::from_unix_millis(5))
            .await
            .expect("tombstone")
    );
    assert!(
        !repo
            .delete_message("lobby-eu", 2, TimestampMillis::from_unix_millis(6))
            .await
            .expect("repeat tombstone")
    );

    let epoch = repo
        .current_access_epoch("room:lobby-eu")
        .await
        .expect("epoch");
    repo.advance_access_epoch("room:lobby-eu", TimestampMillis::from_unix_millis(7))
        .await
        .expect("revoke");
    assert!(
        repo.channel_history_authorized("lobby-eu", 10, None, "room:lobby-eu", epoch)
            .await
            .is_err()
    );
    assert!(
        repo.post_message_authorized(
            "lobby-eu",
            ChannelType::Room,
            "alice",
            "stale",
            2,
            "room:lobby-eu",
            epoch,
            TimestampMillis::from_unix_millis(8),
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn mongodb_chat_rejects_post_type_conflicts_and_recovers_canonical_create_races() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB chat remediation test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let database = db.database_for_tests();
    for name in ["chat_channels", "chat_messages"] {
        database
            .collection::<mongodb::bson::Document>(name)
            .delete_many(doc! {})
            .await
            .expect("clear chat remediation fixture");
    }

    let repo = db.mongo_chat_repository();
    repo.post_message(
        "immutable-post-type",
        ChannelType::Room,
        "alice",
        "first",
        10,
        TimestampMillis::from_unix_millis(1),
    )
    .await
    .expect("create room channel and first message");
    let channels = database.collection::<mongodb::bson::Document>("chat_channels");
    let before = channels
        .find_one(doc! { "channel_id": "immutable-post-type" })
        .await
        .expect("read channel before conflicting post")
        .expect("created channel");

    assert!(
        repo.post_message(
            "immutable-post-type",
            ChannelType::Group,
            "alice",
            "must not append",
            10,
            TimestampMillis::from_unix_millis(2),
        )
        .await
        .is_err()
    );
    let after = channels
        .find_one(doc! { "channel_id": "immutable-post-type" })
        .await
        .expect("read channel after conflicting post")
        .expect("channel remains");
    assert_eq!(after.get_str("channel_type").expect("stored type"), "room");
    assert_eq!(after.get_i64("next_id").expect("message sequence"), 1);
    assert_eq!(after.get_i64("next_event_id").expect("event sequence"), 1);
    assert_eq!(
        after, before,
        "conflicting type must not mutate the channel"
    );
    assert_eq!(
        repo.channel_history("immutable-post-type", 10, None)
            .await
            .expect("history after rejected post")
            .len(),
        1
    );

    // Start both resolutions together against the real rs0 unique index. One
    // creator may win the insert, but both callers must receive that canonical
    // descriptor instead of surfacing duplicate-key E11000.
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let left_barrier = Arc::clone(&barrier);
    let right_barrier = Arc::clone(&barrier);
    let left = db.mongo_chat_repository();
    let right = db.mongo_chat_repository();
    let (left, right) = tokio::join!(
        async move {
            left_barrier.wait().await;
            left.resolve_canonical_channel(
                "room:concurrent-canonical-create",
                ChannelType::Room,
                TimestampMillis::from_unix_millis(3),
            )
            .await
        },
        async move {
            right_barrier.wait().await;
            right
                .resolve_canonical_channel(
                    "room:concurrent-canonical-create",
                    ChannelType::Room,
                    TimestampMillis::from_unix_millis(3),
                )
                .await
        }
    );
    let left = left.expect("left concurrent resolve");
    let right = right.expect("right concurrent resolve");
    assert_eq!(
        left, right,
        "concurrent creators share one canonical channel"
    );
    assert_eq!(
        channels
            .count_documents(doc! { "canonical_key": "room:concurrent-canonical-create" })
            .await
            .expect("count canonical channel"),
        1
    );
}

#[tokio::test]
async fn mongodb_chat_0392_delivery_audit_and_rate_limits_use_real_rs0_transactions() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB TASK-0392 integration test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let database = db.database_for_tests();
    for name in [
        "chat_channels",
        "chat_access_epochs",
        "chat_messages",
        "chat_events",
        "chat_moderation_audit",
        "chat_rate_limits",
        "chat_delivery_outbox",
    ] {
        database
            .collection::<mongodb::bson::Document>(name)
            .delete_many(doc! {})
            .await
            .expect("clear TASK-0392 fixture");
    }
    let repo = db.mongo_chat_repository();
    let delivery = ChatDeliveryRequest {
        origin_node_id: "node-a".to_owned(),
        authority_epoch: 0,
        expires_at: TimestampMillis::from_unix_millis(100),
        event_type: "message.create",
    };
    let posted = repo
        .post_message_authorized_with_delivery(
            "delivery-rs0",
            ChannelType::Room,
            "alice",
            "one",
            10,
            "delivery-rs0",
            0,
            &delivery,
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("authorized delivery post");
    assert_eq!(posted.last_event_id, 1);
    assert!(
        repo.active_delivery_outbox("node-b", TimestampMillis::from_unix_millis(2), 10)
            .await
            .expect("foreign worker cannot see delivery")
            .is_empty()
    );
    assert!(
        !repo
            .acknowledge_delivery_outbox("node-b", "delivery-rs0", posted.last_event_id)
            .await
            .expect("foreign worker cannot acknowledge delivery")
    );
    let pending = repo
        .active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(2), 10)
        .await
        .expect("first worker sees delivery");
    assert_eq!(pending.len(), 1);
    assert_eq!(
        repo.active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(3), 10)
            .await
            .expect("failed worker can retry before the exclusive deadline"),
        pending,
    );
    assert!(
        repo.acknowledge_delivery_outbox("node-a", "delivery-rs0", posted.last_event_id)
            .await
            .expect("ack delivery")
    );
    assert!(
        repo.active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(3), 10)
            .await
            .expect("ack removes pending delivery")
            .is_empty()
    );

    let duplicate = ChatDeliveryOutboxRecord {
        origin_node_id: "node-a".to_owned(),
        channel_id: "concurrent-outbox".to_owned(),
        event_id: 7,
        authority_epoch: 0,
        payload: "opaque".to_owned(),
        created_at: TimestampMillis::from_unix_millis(1),
        expires_at: TimestampMillis::from_unix_millis(100),
    };
    let (left, right) = tokio::join!(
        repo.stage_delivery_outbox(duplicate.clone()),
        repo.stage_delivery_outbox(duplicate.clone()),
    );
    assert_eq!(
        usize::from(left.expect("left stage")) + usize::from(right.expect("right stage")),
        1
    );
    assert!(
        !repo
            .stage_delivery_outbox(duplicate)
            .await
            .expect("idempotent outbox stage")
    );

    repo.advance_access_epoch("delivery-rs0", TimestampMillis::from_unix_millis(4))
        .await
        .expect("revoke authorization");
    assert!(
        repo.post_message_authorized_with_delivery(
            "delivery-rs0",
            ChannelType::Room,
            "alice",
            "rejected",
            10,
            "delivery-rs0",
            0,
            &delivery,
            TimestampMillis::from_unix_millis(5),
        )
        .await
        .is_err()
    );
    assert_eq!(
        repo.active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(5), 10)
            .await
            .expect("authorization failure creates no delivery")
            .len(),
        1,
    );

    let uow = db.begin().await.expect("begin delivery rollback UoW");
    uow.mongo_chat_repository()
        .post_message_authorized_with_delivery(
            "rollback-delivery",
            ChannelType::Room,
            "alice",
            "discard",
            10,
            "rollback-delivery",
            0,
            &delivery,
            TimestampMillis::from_unix_millis(6),
        )
        .await
        .expect("stage delivery in UoW");
    uow.rollback().await.expect("rollback delivery UoW");
    assert!(
        repo.channel_history("rollback-delivery", 10, None)
            .await
            .expect("history after rollback")
            .is_empty()
    );
    assert_eq!(
        database
            .collection::<mongodb::bson::Document>("chat_delivery_outbox")
            .count_documents(doc! {"channel_id": "rollback-delivery"})
            .await
            .expect("outbox count after rollback"),
        0
    );

    let message_id = repo
        .post_message(
            "moderation-rs0",
            ChannelType::Room,
            "alice",
            "secret",
            10,
            TimestampMillis::from_unix_millis(7),
        )
        .await
        .expect("post moderation fixture");
    let audit = ChatModerationAudit::tombstone(
        "operator",
        "admin@example.test",
        "operator_remove",
        "moderation-rs0",
        message_id,
        "alice",
        0,
        "correlation-rs0",
        "node-rs0",
        TimestampMillis::from_unix_millis(8),
    );
    let uow = db.begin().await.expect("begin moderation rollback UoW");
    assert!(
        uow.mongo_chat_repository()
            .moderate_delete_message(
                "moderation-rs0",
                message_id,
                &audit,
                TimestampMillis::from_unix_millis(8)
            )
            .await
            .expect("moderate in UoW")
    );
    uow.rollback().await.expect("rollback moderation UoW");
    assert!(
        !repo
            .channel_history("moderation-rs0", 1, None)
            .await
            .expect("message remains after rollback")[0]
            .deleted
    );
    assert_eq!(repo.moderation_audit_count().await.expect("audit count"), 0);

    let exhausted = ChatRateLimit {
        key: "rate-exhausted".to_owned(),
        limit: 1,
        window_ms: 10,
    };
    let fresh = ChatRateLimit {
        key: "rate-fresh".to_owned(),
        limit: 1,
        window_ms: 10,
    };
    repo.consume_rate_limits(
        std::slice::from_ref(&exhausted),
        TimestampMillis::from_unix_millis(1),
    )
    .await
    .expect("consume exhausted key once");
    assert!(
        repo.consume_rate_limits(
            &[fresh.clone(), exhausted.clone()],
            TimestampMillis::from_unix_millis(2)
        )
        .await
        .is_err()
    );
    repo.consume_rate_limits(
        std::slice::from_ref(&fresh),
        TimestampMillis::from_unix_millis(2),
    )
    .await
    .expect("rejected plan did not consume fresh key");
    repo.consume_rate_limits(
        std::slice::from_ref(&exhausted),
        TimestampMillis::from_unix_millis(10),
    )
    .await
    .expect("fixed window expires at boundary");
    assert!(
        repo.cleanup_rate_limits(TimestampMillis::from_unix_millis(11), 10)
            .await
            .expect("cleanup expired rate limits")
            > 0
    );
}

#[tokio::test]
async fn mongodb_chat_session_bound_lifecycle_methods_read_their_writes_and_rollback() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB lifecycle transaction test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let database = db.database_for_tests();
    for name in [
        "chat_channels",
        "chat_messages",
        "chat_events",
        "chat_moderation_audit",
        "chat_delivery_outbox",
    ] {
        database
            .collection::<mongodb::bson::Document>(name)
            .delete_many(doc! {})
            .await
            .expect("clear lifecycle fixture");
    }
    let repo = db.mongo_chat_repository();
    let live = |channel: &str, event_id| ChatDeliveryOutboxRecord {
        origin_node_id: "node-a".to_owned(),
        channel_id: channel.to_owned(),
        event_id,
        authority_epoch: 0,
        payload: format!("payload-{channel}-{event_id}"),
        created_at: TimestampMillis::from_unix_millis(1),
        expires_at: TimestampMillis::from_unix_millis(100),
    };

    let uow = db.begin().await.expect("begin staging UoW");
    let tx_repo = uow.mongo_chat_repository();
    assert!(
        tx_repo
            .stage_delivery_outbox(live("tx-stage", 1))
            .await
            .expect("stage in enclosing session")
    );
    assert_eq!(
        tx_repo
            .active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(2), 10)
            .await
            .expect("session reads staged outbox"),
        vec![live("tx-stage", 1)]
    );
    uow.rollback().await.expect("abort staging UoW");
    assert!(
        repo.active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(2), 10)
            .await
            .expect("aborted stage is not visible")
            .is_empty()
    );

    assert!(
        repo.stage_delivery_outbox(live("tx-ack", 2))
            .await
            .expect("stage acknowledgement fixture")
    );
    let uow = db.begin().await.expect("begin acknowledgement UoW");
    let tx_repo = uow.mongo_chat_repository();
    assert!(
        tx_repo
            .acknowledge_delivery_outbox("node-a", "tx-ack", 2)
            .await
            .expect("acknowledge in enclosing session")
    );
    assert!(
        tx_repo
            .active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(2), 10)
            .await
            .expect("session reads acknowledged outbox")
            .is_empty()
    );
    uow.rollback().await.expect("abort acknowledgement UoW");
    assert_eq!(
        repo.active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(2), 10)
            .await
            .expect("aborted acknowledgement does not leak"),
        vec![live("tx-ack", 2)]
    );

    let expired = ChatDeliveryOutboxRecord {
        expires_at: TimestampMillis::from_unix_millis(2),
        ..live("tx-cleanup", 3)
    };
    assert!(
        repo.stage_delivery_outbox(expired)
            .await
            .expect("stage cleanup fixture")
    );
    let uow = db.begin().await.expect("begin cleanup UoW");
    assert_eq!(
        uow.mongo_chat_repository()
            .cleanup_delivery_outbox(TimestampMillis::from_unix_millis(2), 10)
            .await
            .expect("cleanup in enclosing session"),
        1
    );
    uow.rollback().await.expect("abort cleanup UoW");
    assert_eq!(
        repo.cleanup_delivery_outbox(TimestampMillis::from_unix_millis(2), 10)
            .await
            .expect("aborted cleanup does not leak"),
        1
    );

    let message_id = repo
        .post_message(
            "tx-audit",
            ChannelType::Room,
            "alice",
            "secret",
            10,
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("post audit fixture");
    let audit = ChatModerationAudit::tombstone(
        "operator",
        "admin@example.test",
        "operator_remove",
        "tx-audit",
        message_id,
        "alice",
        0,
        "correlation-tx-audit",
        "node-tx-audit",
        TimestampMillis::from_unix_millis(1),
    );
    assert!(
        repo.moderate_delete_message(
            "tx-audit",
            message_id,
            &audit,
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("create audit fixture")
    );
    let uow = db.begin().await.expect("begin audit cleanup UoW");
    let tx_repo = uow.mongo_chat_repository();
    assert_eq!(
        tx_repo
            .moderation_audit_count()
            .await
            .expect("session count sees audit"),
        1
    );
    assert_eq!(
        tx_repo
            .cleanup_moderation_audit(TimestampMillis::from_unix_millis(2), 10)
            .await
            .expect("audit cleanup in enclosing session"),
        1
    );
    assert_eq!(
        tx_repo
            .moderation_audit_count()
            .await
            .expect("session count reads cleanup"),
        0
    );
    uow.rollback().await.expect("abort audit cleanup UoW");
    assert_eq!(
        repo.moderation_audit_count()
            .await
            .expect("aborted audit cleanup does not leak"),
        1
    );
}

#[tokio::test]
async fn mongodb_chat_session_bound_lifecycle_sequence_commits_atomically() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB lifecycle commit test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let outbox = db
        .database_for_tests()
        .collection::<mongodb::bson::Document>("chat_delivery_outbox");
    outbox
        .delete_many(doc! {})
        .await
        .expect("clear lifecycle commit fixture");
    let repo = db.mongo_chat_repository();
    let retained = ChatDeliveryOutboxRecord {
        origin_node_id: "node-a".to_owned(),
        channel_id: "tx-sequence".to_owned(),
        event_id: 1,
        authority_epoch: 0,
        payload: "retained".to_owned(),
        created_at: TimestampMillis::from_unix_millis(1),
        expires_at: TimestampMillis::from_unix_millis(100),
    };
    let expired = ChatDeliveryOutboxRecord {
        origin_node_id: "node-a".to_owned(),
        channel_id: "tx-sequence".to_owned(),
        event_id: 2,
        authority_epoch: 0,
        payload: "expired".to_owned(),
        created_at: TimestampMillis::from_unix_millis(1),
        expires_at: TimestampMillis::from_unix_millis(2),
    };
    assert!(
        repo.stage_delivery_outbox(retained.clone())
            .await
            .expect("stage retained fixture")
    );
    assert!(
        repo.stage_delivery_outbox(expired)
            .await
            .expect("stage expired fixture")
    );

    let uow = db.begin().await.expect("begin lifecycle sequence UoW");
    let tx_repo = uow.mongo_chat_repository();
    assert!(
        tx_repo
            .acknowledge_delivery_outbox("node-a", "tx-sequence", 1)
            .await
            .expect("ack retained outbox")
    );
    assert_eq!(
        tx_repo
            .cleanup_delivery_outbox(TimestampMillis::from_unix_millis(2), 10)
            .await
            .expect("clean expired outbox"),
        1
    );
    assert!(
        tx_repo
            .active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(3), 10)
            .await
            .expect("sequence reads its own writes")
            .is_empty()
    );
    uow.commit()
        .await
        .expect("commit lifecycle sequence atomically");
    assert!(
        repo.active_delivery_outbox("node-a", TimestampMillis::from_unix_millis(3), 10)
            .await
            .expect("committed sequence persists")
            .is_empty()
    );
    assert_eq!(
        outbox
            .count_documents(doc! {})
            .await
            .expect("all lifecycle rows removed by one commit"),
        0
    );
}

#[tokio::test]
async fn mongodb_explorer_reads_metadata_and_pages_without_accepting_query_documents() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB explorer integration test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let collection = db
        .database_for_tests()
        .collection::<mongodb::bson::Document>("explorer_contract");
    collection
        .delete_many(doc! {})
        .await
        .expect("clear explorer fixture");
    collection
        .insert_many([
            doc! {
                "_id": BsonObjectId::new(),
                "rank": 1_i64,
                "name": "first",
                "token_value": "never expose",
                "profile": {
                    "display_name": "First",
                    "Api_Key": "nested secret",
                    "devices": [
                        { "name": "phone", "TOKEN": "array secret" },
                        { "name": "laptop", "preferences": { "theme": "dark" } },
                    ],
                },
                "script_context": Bson::JavaScriptCodeWithScope(JavaScriptCodeWithScope {
                    code: "return safe_value;".to_owned(),
                    scope: doc! {
                        "apiKey": "camel-case-secret",
                        "APIKEY": "upper-case-secret",
                        "nested": { "api-key": "separator-secret" },
                    },
                }),
            },
            doc! { "_id": "two", "rank": 2_i64, "name": "second", "token_value": "never expose" },
            doc! { "_id": "three", "rank": 3_i64, "name": "third", "token_value": "never expose" },
        ])
        .await
        .expect("seed explorer fixture");
    collection
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "rank": 1_i32 })
                .build(),
        )
        .await
        .expect("create explorer index");

    let explorer = db.database_explorer();
    let table = TableRef::new("mongodb", "explorer_contract").expect("safe table");
    assert!(
        explorer
            .list_tables()
            .await
            .expect("collections")
            .iter()
            .any(|item| item.table == table)
    );
    let description = explorer.describe_table(&table).await.expect("metadata");
    assert!(description.capabilities.indexes);
    assert!(!description.capabilities.foreign_keys);
    assert!(
        description
            .columns
            .iter()
            .any(|column| column.name == "rank")
    );
    assert!(
        description
            .indexes
            .iter()
            .any(|index| index.columns == ["rank"])
    );

    let request = ListRowsRequest {
        table: table.clone(),
        filters: Vec::new(),
        sort: SortSpec {
            column: "rank".to_owned(),
            direction: SortDirection::Asc,
        },
        cursor: None,
        limit: Some(2),
    };
    let first = explorer.list_rows(&request).await.expect("first page");
    assert_eq!(first.rows.len(), 2);
    assert!(matches!(
        first.rows[0].values.get("token_value"),
        Some(citadel::database_explorer::DatabaseValue::Redacted)
    ));
    assert_eq!(
        first.rows[0].values.get("profile"),
        Some(&citadel::database_explorer::DatabaseValue::Json(json!({
            "display_name": "First",
            "<redacted_field>": "<redacted>",
            "devices": [
                { "name": "phone", "<redacted_field>": "<redacted>" },
                { "name": "laptop", "preferences": { "theme": "dark" } },
            ],
        })))
    );
    let script_context = first.rows[0]
        .values
        .get("script_context")
        .expect("script context is projected");
    let serialized_script_context =
        serde_json::to_string(script_context).expect("extended JSON serialization succeeds");
    for leaked in [
        "apiKey",
        "APIKEY",
        "api-key",
        "camel-case-secret",
        "upper-case-secret",
        "separator-secret",
    ] {
        assert!(
            !serialized_script_context.contains(leaked),
            "explorer response leaked protected JavaScript scope key or value: {leaked}"
        );
    }
    let second = explorer
        .list_rows(&ListRowsRequest {
            cursor: first.next.clone(),
            ..request.clone()
        })
        .await
        .expect("second page");
    assert_eq!(second.rows.len(), 1);
    assert!(
        explorer
            .get_row(&citadel::database_explorer::RowDetailRequest {
                table: table.clone(),
                row_ref: first.rows[0].row_ref.clone()
            })
            .await
            .is_ok()
    );

    let contains = ListRowsRequest {
        filters: vec![RowFilter {
            column: "name".to_owned(),
            operator: FilterOperator::Contains,
            value: Some(json!("irs")),
        }],
        limit: Some(1),
        ..request.clone()
    };
    let contains_page = explorer
        .list_rows(&contains)
        .await
        .expect("literal contains");
    assert_eq!(contains_page.rows.len(), 1);

    let injection = ListRowsRequest {
        table: table.clone(),
        filters: vec![RowFilter {
            column: "rank".to_owned(),
            operator: FilterOperator::Eq,
            value: Some(json!({"$gt": 0})),
        }],
        sort: SortSpec {
            column: "rank".to_owned(),
            direction: SortDirection::Asc,
        },
        cursor: None,
        limit: Some(1),
    };
    assert!(
        explorer.list_rows(&injection).await.is_err(),
        "query documents are never accepted as filters"
    );
    assert!(
        explorer
            .describe_table(&TableRef::new("mongodb", "$cmd").expect("portable reference"))
            .await
            .is_err()
    );
}

async fn connect_for_retry_test() -> Option<MongoDatabase> {
    let url = std::env::var("CITADEL_TEST_MONGODB_URL").ok()?;
    MongoDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .ok()
}

async fn fail_next(db: &MongoDatabase, command: &str, label: &str) {
    db.admin_database_for_tests()
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": { "times": 1 },
            "data": {
                "failCommands": [command],
                "errorCode": 91_i32,
                "errorLabels": [label],
                "failInternalCommands": true,
            },
        })
        .await
        .expect("enable one-shot retry failpoint");
}

#[tokio::test]
async fn wallet_transaction_rolls_back_on_a_real_intermediate_write_failure() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!(
            "skipping MongoDB wallet rollback integration test: CITADEL_TEST_MONGODB_URL is unset"
        );
        return;
    };
    db.clear_wallet_purchases_data_for_tests()
        .await
        .expect("clear economy data");
    db.admin_database_for_tests()
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": { "times": 1 },
            "data": { "failCommands": ["insert"], "errorCode": 2_i32, "failInternalCommands": true },
        })
        .await
        .expect("enable one-shot non-retryable insert failure");

    let error = db
        .wallet_repository()
        .apply_change(
            "rollback-user",
            "coins",
            10,
            "fault-injection",
            100,
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect_err("ledger insert failure aborts the economy transaction");
    assert_ne!(error.category(), citadel::error::ErrorCategory::Conflict);
    assert!(
        db.wallet_repository()
            .balances("rollback-user")
            .await
            .expect("read balance after abort")
            .is_empty()
    );
    assert!(
        db.wallet_repository()
            .ledger("rollback-user", 10)
            .await
            .expect("read ledger after abort")
            .is_empty()
    );
}

#[tokio::test]
async fn wallet_mutation_retries_transient_work_and_unknown_commit_without_duplicate_ledger() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!(
            "skipping MongoDB wallet retry integration test: CITADEL_TEST_MONGODB_URL is unset"
        );
        return;
    };
    db.clear_wallet_purchases_data_for_tests()
        .await
        .expect("clear economy data");
    let wallet = db.wallet_repository();

    // This fails the ledger insert inside the real wallet mutation. The retry
    // must replay balance materialization, sequence allocation, and ledger
    // insert as one transaction rather than merely retrying a generic helper.
    fail_next(&db, "insert", "TransientTransactionError").await;
    wallet
        .apply_change(
            "retry-user",
            "coins",
            7,
            "transient",
            100,
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("wallet retries its transient write conflict");
    assert_eq!(
        wallet
            .balances("retry-user")
            .await
            .expect("balance after transient retry")
            .get("coins"),
        Some(&7)
    );
    let ledger = wallet
        .ledger("retry-user", 10)
        .await
        .expect("ledger after transient retry");
    assert_eq!(
        ledger.len(),
        1,
        "replayed work did not duplicate the ledger"
    );
    assert_eq!(ledger[0].delta, 7);

    // A result-unknown commit retries only commitTransaction. Replaying the
    // mutation here would duplicate its ledger entry, so this proves the real
    // wallet path retains MongoDB's distinct commit-retry semantics.
    fail_next(&db, "commitTransaction", "UnknownTransactionCommitResult").await;
    wallet
        .apply_change(
            "retry-user",
            "coins",
            3,
            "unknown-commit",
            100,
            TimestampMillis::from_unix_millis(2),
        )
        .await
        .expect("wallet retries its unknown commit result");
    assert_eq!(
        wallet
            .balances("retry-user")
            .await
            .expect("balance after commit retry")
            .get("coins"),
        Some(&10)
    );
    let ledger = wallet
        .ledger("retry-user", 10)
        .await
        .expect("ledger after commit retry");
    assert_eq!(ledger.len(), 2, "commit retry did not duplicate the ledger");
    assert_eq!(ledger.iter().map(|entry| entry.delta).sum::<i64>(), 10);
}

#[tokio::test]
async fn replica_set_retries_whole_transaction_and_unknown_commit_result() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB retry integration test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let collection = db
        .database_for_tests()
        .collection::<mongodb::bson::Document>("transaction_retry_contract");
    collection.delete_many(doc! {}).await.expect("clear");

    fail_next(&db, "insert", "TransientTransactionError").await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&attempts);
    db.with_transaction(move |database, session| {
        let attempts = Arc::clone(&observed);
        Box::pin(async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            database
                .collection::<mongodb::bson::Document>("transaction_retry_contract")
                .insert_one(doc! { "_id": "transient" })
                .session(session)
                .await?;
            Ok(())
        })
    })
    .await
    .expect("whole transaction retries after TransientTransactionError");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    fail_next(&db, "commitTransaction", "UnknownTransactionCommitResult").await;
    let commits = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&commits);
    db.with_transaction(move |database, session| {
        let commits = Arc::clone(&observed);
        Box::pin(async move {
            commits.fetch_add(1, Ordering::SeqCst);
            database
                .collection::<mongodb::bson::Document>("transaction_retry_contract")
                .insert_one(doc! { "_id": "unknown-commit" })
                .session(session)
                .await?;
            Ok(())
        })
    })
    .await
    .expect("commit retries after UnknownTransactionCommitResult");
    assert_eq!(commits.load(Ordering::SeqCst), 1, "only commit is retried");
    assert_eq!(collection.count_documents(doc! {}).await.expect("count"), 2);
}

#[tokio::test]
async fn unit_of_work_storage_writes_and_memberships_abort_with_the_enclosing_session() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!(
            "skipping MongoDB UoW storage rollback integration test: CITADEL_TEST_MONGODB_URL is unset"
        );
        return;
    };
    db.clear_storage_data_for_tests()
        .await
        .expect("clear storage");

    let index = StorageIndexDefinition::new(
        StorageIndexName::new("uow_rollback_index").expect("index name"),
        Collection::new("uow_rollback_profiles").expect("collection"),
        None,
        vec![StorageIndexField::new("score").expect("field")],
    )
    .expect("index definition");
    db.storage_repository()
        .install_index(&index)
        .await
        .expect("install index outside the UoW");

    let alice = UserId::new("uow-rollback-alice").expect("user id");
    let id = ObjectId::new(
        Owner::user(alice.clone()),
        Collection::new("uow_rollback_profiles").expect("collection"),
        Key::new("profile").expect("key"),
    );
    let uow = db.begin().await.expect("begin UoW");
    uow.storage_repository()
        .write(
            &Accessor::User(alice.clone()),
            WriteRequest::upsert(
                id.clone(),
                StorageValue::new(json!({ "score": 42 })).expect("storage value"),
                Permissions::owner_private(),
            ),
        )
        .await
        .expect("write through the UoW session");
    uow.rollback().await.expect("abort enclosing UoW");

    assert!(
        db.storage_repository()
            .read(&Accessor::User(alice.clone()), &id)
            .await
            .expect("read after abort")
            .is_none(),
        "the object must not survive the UoW abort"
    );
    assert!(
        db.storage_repository()
            .query_index(
                &Accessor::User(alice),
                &StorageIndexQuery::from_json_filters(index, &Default::default(), 10)
                    .expect("index query"),
            )
            .await
            .expect("query memberships after abort")
            .is_empty(),
        "index memberships must not survive the UoW abort"
    );
}

#[tokio::test]
async fn friends_reciprocal_writes_are_atomic_unique_and_serialize_concurrent_invites() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!(
            "skipping MongoDB friends concurrency integration test: CITADEL_TEST_MONGODB_URL is unset"
        );
        return;
    };
    db.clear_friends_data_for_tests()
        .await
        .expect("clear friend edges");
    let repo = db.friends_repository();

    let (left, right) = tokio::join!(
        repo.add(
            "concurrent-a",
            "concurrent-b",
            TimestampMillis::from_unix_millis(1)
        ),
        repo.add(
            "concurrent-b",
            "concurrent-a",
            TimestampMillis::from_unix_millis(2)
        ),
    );
    left.expect("first concurrent invite commits");
    right.expect("second concurrent invite commits after retry if necessary");
    assert_eq!(
        repo.list("concurrent-a").await.expect("list a")[0].state,
        FriendState::Friend
    );
    assert_eq!(
        repo.list("concurrent-b").await.expect("list b")[0].state,
        FriendState::Friend
    );

    let edges = db
        .database_for_tests()
        .collection::<mongodb::bson::Document>("friend_edges");
    assert_eq!(
        edges.count_documents(doc! {}).await.expect("count edges"),
        2,
        "the reciprocal relationship consists of exactly two unique directed edges"
    );
    let duplicate = edges
        .insert_one(doc! {
            "owner_id": "concurrent-a",
            "other_id": "concurrent-b",
            "state": "friend",
            "updated_unix_ms": 3_i64,
        })
        .await
        .expect_err("compound unique index rejects a duplicate directed edge");
    assert!(duplicate.to_string().contains("11000"));
}

#[tokio::test]
async fn unit_of_work_friend_writes_abort_with_the_enclosing_session() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!(
            "skipping MongoDB friends rollback integration test: CITADEL_TEST_MONGODB_URL is unset"
        );
        return;
    };
    db.clear_friends_data_for_tests()
        .await
        .expect("clear friend edges");

    let uow = db.begin().await.expect("begin UoW");
    uow.friends_repository()
        .add(
            "rollback-a",
            "rollback-b",
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("write reciprocal edges through the UoW session");
    uow.rollback().await.expect("abort enclosing UoW");

    let repo = db.friends_repository();
    assert!(
        repo.list("rollback-a")
            .await
            .expect("list after abort")
            .is_empty(),
        "the initiating edge must not survive abort"
    );
    assert!(
        repo.list("rollback-b")
            .await
            .expect("list after abort")
            .is_empty(),
        "the reciprocal edge must not survive abort"
    );
}

#[tokio::test]
async fn groups_writes_abort_with_uow_and_concurrent_joins_preserve_capacity() {
    use citadel::repository::CreateGroupRequest;
    let Some(db) = connect_for_retry_test().await else {
        eprintln!(
            "skipping MongoDB groups rollback/concurrency test: CITADEL_TEST_MONGODB_URL is unset"
        );
        return;
    };
    db.clear_groups_data_for_tests()
        .await
        .expect("clear groups");
    let uow = db.begin().await.expect("begin UoW");
    let created = uow
        .groups_repository()
        .create(CreateGroupRequest {
            name: "uow-groups".into(),
            description: "rollback".into(),
            open: true,
            max_size: 2,
            creator_user_id: "owner".into(),
            now: TimestampMillis::from_unix_millis(1),
        })
        .await
        .expect("create through session");
    uow.groups_repository()
        .add_member(created.id, "member", TimestampMillis::from_unix_millis(2))
        .await
        .expect("membership through session");
    uow.rollback().await.expect("abort UoW");
    assert!(
        db.groups_repository()
            .get(created.id)
            .await
            .expect("read")
            .is_none(),
        "group and membership roll back together"
    );

    let repo = db.groups_repository();
    let group = repo
        .create(CreateGroupRequest {
            name: "capacity-group".into(),
            description: "concurrency".into(),
            open: true,
            max_size: 2,
            creator_user_id: "owner".into(),
            now: TimestampMillis::from_unix_millis(3),
        })
        .await
        .expect("create");
    let (a, b) = tokio::join!(
        repo.join(group.id, "a", TimestampMillis::from_unix_millis(4)),
        repo.join(group.id, "b", TimestampMillis::from_unix_millis(5)),
    );
    assert!(a.is_ok() || b.is_ok(), "one join obtains the final seat");
    assert!(
        a.is_err() || b.is_err(),
        "the other concurrent join is rejected at the cap"
    );
    assert_eq!(
        repo.get(group.id)
            .await
            .expect("get")
            .expect("group")
            .member_count(),
        2
    );
}

#[tokio::test]
async fn friend_edge_validator_rejects_invalid_rows_and_is_idempotent() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB friend validator test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    db.clear_friends_data_for_tests()
        .await
        .expect("clear friend edges");
    let edges = db
        .database_for_tests()
        .collection::<mongodb::bson::Document>("friend_edges");
    for invalid in [
        doc! {"owner_id":"same", "other_id":"same", "state":"friend", "updated_unix_ms":1_i64},
        doc! {"owner_id":"", "other_id":"other", "state":"friend", "updated_unix_ms":1_i64},
        doc! {"owner_id":"owner", "other_id":"other", "state":"invalid", "updated_unix_ms":1_i64},
    ] {
        assert!(
            edges.insert_one(invalid).await.is_err(),
            "validator rejects invalid edge"
        );
    }
    edges.insert_one(doc! {"owner_id":"owner", "other_id":"other", "state":"friend", "updated_unix_ms":1_i64}).await.expect("valid row accepted");
    // A fresh connect runs collMod with the same strict/error validator again.
    let url = std::env::var("CITADEL_TEST_MONGODB_URL").expect("test URL");
    MongoDatabase::connect(&DatabaseConfig {
        url: Some(url),
        ..DatabaseConfig::default()
    })
    .await
    .expect("validator reconciliation remains idempotent");
}

#[tokio::test]
async fn api_key_validator_rejects_non_32_byte_verifiers() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB API-key validator test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let keys = db
        .database_for_tests()
        .collection::<mongodb::bson::Document>("api_keys");
    keys.delete_many(doc! { "id": { "$in": [
        "11111111111111111111111111111111",
        "22222222222222222222222222222222",
    ]}})
    .await
    .expect("clear validator fixtures");

    for (id, length) in [
        ("11111111111111111111111111111111", 31_usize),
        ("22222222222222222222222222222222", 33_usize),
    ] {
        let invalid = doc! {
            "id": id,
            "name": "invalid verifier fixture",
            "scopes": ["telemetry:read"],
            "secret_verifier": mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: vec![0_u8; length],
            },
            "generation": 1_i64,
            "created_at_ms": 1_i64,
            "expires_at_ms": mongodb::bson::Bson::Null,
            "revoked_at_ms": mongodb::bson::Bson::Null,
            "last_used_at_ms": mongodb::bson::Bson::Null,
        };
        assert!(
            keys.insert_one(invalid).await.is_err(),
            "validator must reject a {length}-byte verifier"
        );
    }
}

/// Exercises the four social projections together through the public Mongo
/// repositories.  The individual contract suites own the exhaustive domain
/// matrices; this test guards their integration boundary: collections do not
/// leak into each other, public ordering/paging remains stable, and reads see
/// only their intended social projection.
#[tokio::test]
async fn social_projections_are_isolated_and_keep_stable_ordered_pages() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB social integration test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    db.clear_friends_data_for_tests()
        .await
        .expect("clear friends");
    db.clear_groups_data_for_tests()
        .await
        .expect("clear groups");
    db.clear_leaderboards_data_for_tests()
        .await
        .expect("clear leaderboards");
    db.clear_notifications_data_for_tests()
        .await
        .expect("clear notifications");

    let friends = db.friends_repository();
    friends
        .add("alice", "zoe", TimestampMillis::from_unix_millis(1))
        .await
        .expect("invite zoe");
    friends
        .add("alice", "bob", TimestampMillis::from_unix_millis(2))
        .await
        .expect("invite bob");
    let groups = db.groups_repository();
    for name in ["first", "second", "third"] {
        groups
            .create(CreateGroupRequest {
                name: name.to_owned(),
                description: name.to_owned(),
                open: true,
                max_size: 0,
                creator_user_id: "alice".to_owned(),
                now: TimestampMillis::from_unix_millis(3),
            })
            .await
            .expect("create group");
    }
    let leaderboards = db.leaderboards_repository();
    leaderboards
        .create(
            CreateLeaderboardRequest {
                id: "social".to_owned(),
                sort: SortOrder::Desc,
                operator: Operator::Set,
                reset_schedule: None,
            },
            TimestampMillis::from_unix_millis(4),
        )
        .await
        .expect("create board");
    for (user, score) in [("alice", 30), ("bob", 20), ("zoe", 10)] {
        leaderboards
            .submit(
                "social",
                user,
                score,
                0,
                None,
                TimestampMillis::from_unix_millis(5),
            )
            .await
            .expect("submit score");
    }
    let notifications = db.notifications_repository();
    let payload = json!({"domain": "social"});
    notifications
        .enqueue(
            Recipient::User("alice".to_owned()),
            "private",
            &payload,
            0,
            20,
            TimestampMillis::from_unix_millis(6),
        )
        .await
        .expect("private notification");
    let broadcast = notifications
        .enqueue(
            Recipient::Broadcast,
            "broadcast",
            &payload,
            0,
            20,
            TimestampMillis::from_unix_millis(7),
        )
        .await
        .expect("broadcast notification");
    notifications
        .enqueue(
            Recipient::User("bob".to_owned()),
            "other",
            &payload,
            0,
            20,
            TimestampMillis::from_unix_millis(8),
        )
        .await
        .expect("other notification");
    // Reading every projection through a fresh MongoDatabase proves the public
    // social adapters persist durable MongoDB state across connections.
    let verifier = connect_for_retry_test()
        .await
        .expect("reconnect to the same MongoDB database");
    assert_eq!(
        verifier
            .friends_repository()
            .list("alice")
            .await
            .expect("ordered relations")
            .into_iter()
            .map(|row| row.user_id)
            .collect::<Vec<_>>(),
        vec!["bob", "zoe"],
        "relations are ordered independently of other social collections"
    );
    let groups_page = verifier
        .groups_repository()
        .list(&GroupFilter {
            limit: 1,
            offset: 1,
            ..GroupFilter::default()
        })
        .await
        .expect("group page from fresh MongoDatabase");
    assert_eq!(groups_page.total, 3);
    assert_eq!(groups_page.items[0].name, "second");
    let scores = verifier
        .leaderboards_repository()
        .records("social", 1, 1)
        .await
        .expect("rank page from fresh MongoDatabase");
    assert_eq!(scores.total, 3);
    assert_eq!(scores.items[0].user_id, "bob");
    assert_eq!(scores.items[0].rank, 2);
    let page = verifier
        .notifications_repository()
        .list(Some("alice"), 1, None)
        .await
        .expect("visible page");
    assert_eq!(
        page.total, 2,
        "targeted notifications stay isolated by recipient"
    );
    assert_eq!(
        page.items[0].id, broadcast,
        "newest visible notification is first"
    );
    let resumed = verifier
        .notifications_repository()
        .list(Some("alice"), 1, Some(broadcast))
        .await
        .expect("cursor page");
    assert_eq!(resumed.items[0].subject, "private");
}

#[tokio::test]
async fn social_concurrent_writes_keep_each_projection_consistent() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB social concurrency test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    db.clear_friends_data_for_tests()
        .await
        .expect("clear friends");
    db.clear_groups_data_for_tests()
        .await
        .expect("clear groups");
    db.clear_leaderboards_data_for_tests()
        .await
        .expect("clear leaderboards");
    db.clear_notifications_data_for_tests()
        .await
        .expect("clear notifications");
    let groups = db.groups_repository();
    let group = groups
        .create(CreateGroupRequest {
            name: "concurrent-social".to_owned(),
            description: "integration".to_owned(),
            open: true,
            max_size: 3,
            creator_user_id: "owner".to_owned(),
            now: TimestampMillis::from_unix_millis(1),
        })
        .await
        .expect("create group");
    let leaderboards = db.leaderboards_repository();
    leaderboards
        .create(
            CreateLeaderboardRequest {
                id: "concurrent-social".to_owned(),
                sort: SortOrder::Desc,
                operator: Operator::Incr,
                reset_schedule: None,
            },
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("create board");
    let payload = json!({"test": "concurrent-social"});
    let friends = db.friends_repository();
    let notifications = db.notifications_repository();
    let (friend, join, score, notification) = tokio::join!(
        friends.add("alice", "bob", TimestampMillis::from_unix_millis(2)),
        groups.join(group.id, "alice", TimestampMillis::from_unix_millis(2)),
        leaderboards.submit(
            "concurrent-social",
            "alice",
            7,
            0,
            None,
            TimestampMillis::from_unix_millis(2)
        ),
        notifications.enqueue(
            Recipient::User("alice".to_owned()),
            "concurrent",
            &payload,
            0,
            20,
            TimestampMillis::from_unix_millis(2)
        ),
    );
    friend.expect("friend write");
    join.expect("group join");
    score.expect("score write");
    notification.expect("notification write");
    assert_eq!(
        db.friends_repository()
            .list("alice")
            .await
            .expect("friends")
            .len(),
        1
    );
    assert_eq!(
        groups
            .get(group.id)
            .await
            .expect("group")
            .expect("present")
            .member_count(),
        2
    );
    assert_eq!(
        leaderboards
            .records("concurrent-social", 10, 0)
            .await
            .expect("scores")
            .items[0]
            .score,
        7
    );
    assert_eq!(
        db.notifications_repository()
            .list(Some("alice"), 10, None)
            .await
            .expect("notifications")
            .total,
        1
    );
}

#[tokio::test]
async fn social_uow_rollback_aborts_relationship_and_group_writes_together() {
    let Some(db) = connect_for_retry_test().await else {
        eprintln!("skipping MongoDB social rollback test: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    db.clear_friends_data_for_tests()
        .await
        .expect("clear friends");
    db.clear_groups_data_for_tests()
        .await
        .expect("clear groups");
    let uow = db.begin().await.expect("begin UoW");
    uow.friends_repository()
        .add(
            "rollback-owner",
            "rollback-friend",
            TimestampMillis::from_unix_millis(1),
        )
        .await
        .expect("add friend");
    let group = uow
        .groups_repository()
        .create(CreateGroupRequest {
            name: "rollback-social".to_owned(),
            description: "integration".to_owned(),
            open: true,
            max_size: 0,
            creator_user_id: "rollback-owner".to_owned(),
            now: TimestampMillis::from_unix_millis(1),
        })
        .await
        .expect("create group");
    uow.rollback().await.expect("abort social UoW");
    assert!(
        db.friends_repository()
            .list("rollback-owner")
            .await
            .expect("friends after abort")
            .is_empty()
    );
    assert!(
        db.groups_repository()
            .get(group.id)
            .await
            .expect("group after abort")
            .is_none()
    );
}
