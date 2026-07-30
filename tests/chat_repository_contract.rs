//! Contract tests for the chat repository.
//!
//! These assert the channel-creation / bounded-history / newest-first-paging /
//! tombstone / listing semantics that *any* [`ChatRepository`] implementation must
//! honor. Each scenario is written against `&dyn ChatRepository` and is run against
//! every backend:
//!
//! - always against [`InMemoryChatRepository`] (the reference impl),
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
//!   cargo test --test chat_repository_contract
//! ```

use citadel::error::ErrorCategory;
use citadel::repository::{
    ChannelType, ChatModerationAudit, ChatRateLimit, ChatRepository, InMemoryChatRepository,
};
use citadel::time::TimestampMillis;

/// A roomy capacity for scenarios that are not exercising eviction.
const CAP: usize = 1000;

fn ts(v: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(v)
}

async fn post(
    repo: &dyn ChatRepository,
    channel: &str,
    channel_type: ChannelType,
    sender: &str,
    content: &str,
    capacity: usize,
    now: u64,
) -> u64 {
    repo.post_message(channel, channel_type, sender, content, capacity, ts(now))
        .await
        .expect("post message")
}

// --- Scenarios (backend-agnostic) -------------------------------------------

async fn scenario_append_creates_channel_and_summarizes(repo: &dyn ChatRepository) {
    let first = post(repo, "lobby", ChannelType::Room, "alice", "hi", CAP, 1).await;
    let second = post(repo, "lobby", ChannelType::Room, "bob", "hey", CAP, 2).await;
    assert_eq!((first, second), (1, 2), "per-channel sequential ids");

    // Durability: a fresh listing derives the channel + activity summary.
    let channels = repo.list_channels(None, 0).await.expect("list");
    assert_eq!(channels.len(), 1);
    assert_eq!(channels[0].channel, "lobby");
    assert_eq!(channels[0].channel_type, "room");
    assert_eq!(channels[0].messages, 2, "total-ever-appended counter");
    assert_eq!(channels[0].last_activity_unix_ms, 2);
    assert_eq!(repo.channel_count().await.expect("count"), 1);

    // Durability: the messages come back on a fresh read.
    let page = repo
        .channel_history("lobby", 0, None)
        .await
        .expect("history");
    assert_eq!(page.iter().map(|m| m.id).collect::<Vec<_>>(), vec![2, 1]);
    assert_eq!(page[0].sender, "bob");
    assert_eq!(page[1].content, "hi");
    assert_eq!(page[0].revision, 1);
    assert_eq!(page[0].updated_at_unix_ms, 2);
    assert_eq!(page[0].last_event_id, 2);
}

async fn scenario_channel_type_fixed_at_creation(repo: &dyn ChatRepository) {
    post(repo, "g-1", ChannelType::Group, "a", "hi", CAP, 1).await;
    // A later append with a different type does not change the channel's type.
    post(repo, "g-1", ChannelType::Room, "b", "hey", CAP, 2).await;
    let channels = repo.list_channels(None, 0).await.expect("list");
    assert_eq!(channels[0].channel_type, "group");
}

async fn scenario_history_newest_first_with_before_cursor(repo: &dyn ChatRepository) {
    for seq in 1..=5u64 {
        post(
            repo,
            "lobby",
            ChannelType::Room,
            "a",
            &format!("m{seq}"),
            CAP,
            seq,
        )
        .await;
    }
    let first = repo.channel_history("lobby", 2, None).await.expect("page");
    assert_eq!(first.iter().map(|m| m.id).collect::<Vec<_>>(), vec![5, 4]);
    let next = repo
        .channel_history("lobby", 2, Some(4))
        .await
        .expect("page");
    assert_eq!(next.iter().map(|m| m.id).collect::<Vec<_>>(), vec![3, 2]);
    // Unbounded (limit 0) returns everything retained, newest first.
    let all = repo.channel_history("lobby", 0, None).await.expect("page");
    assert_eq!(
        all.iter().map(|m| m.id).collect::<Vec<_>>(),
        vec![5, 4, 3, 2, 1]
    );
}

async fn scenario_bounded_history_evicts_oldest_but_ids_keep_incrementing(
    repo: &dyn ChatRepository,
) {
    for seq in 1..=5u64 {
        post(
            repo,
            "lobby",
            ChannelType::Room,
            "a",
            &format!("m{seq}"),
            3,
            seq,
        )
        .await;
    }
    let ids: Vec<u64> = repo
        .channel_history("lobby", 0, None)
        .await
        .expect("history")
        .iter()
        .map(|m| m.id)
        .collect();
    assert_eq!(ids, vec![5, 4, 3], "only the last 3 retained");
    // The next append still assigns id 6, proving eviction never rewinds ids.
    let next = post(repo, "lobby", ChannelType::Room, "a", "m6", 3, 6).await;
    assert_eq!(next, 6);
    // The activity counter counts every append, not the retained rows.
    let channels = repo.list_channels(None, 0).await.expect("list");
    assert_eq!(channels[0].messages, 6);
    // Paging back into evicted territory yields nothing below the retained window.
    let paged = repo
        .channel_history("lobby", 10, Some(4))
        .await
        .expect("page");
    assert!(paged.is_empty(), "ids 1..3 were evicted");
}

async fn scenario_history_for_unknown_channel_is_empty(repo: &dyn ChatRepository) {
    assert!(
        repo.channel_history("nope", 10, None)
            .await
            .expect("history")
            .is_empty()
    );
    assert_eq!(repo.channel_count().await.expect("count"), 0);
}

async fn scenario_delete_tombstones_durably_and_is_idempotent(repo: &dyn ChatRepository) {
    post(repo, "lobby", ChannelType::Room, "alice", "one", CAP, 1).await;
    let target = post(repo, "lobby", ChannelType::Room, "bob", "secret", CAP, 2).await;
    assert!(
        repo.delete_message("lobby", target, TimestampMillis::from_unix_millis(3))
            .await
            .expect("delete")
    );

    // Durability: a fresh read shows the tombstone (blanked content, still present).
    let page = repo
        .channel_history("lobby", 0, None)
        .await
        .expect("history");
    let tomb = page.iter().find(|m| m.id == target).expect("still present");
    assert!(tomb.deleted);
    assert_eq!(tomb.content, "");
    assert_eq!(tomb.revision, 2);
    assert_eq!(tomb.updated_at_unix_ms, 3);
    assert_eq!(tomb.last_event_id, 3);
    // Tombstoning does not reduce the activity counter.
    let channels = repo.list_channels(None, 0).await.expect("list");
    assert_eq!(channels[0].messages, 2);

    // Idempotent: a second delete is a no-op success.
    assert!(
        !repo
            .delete_message("lobby", target, TimestampMillis::from_unix_millis(4))
            .await
            .expect("second")
    );
}

async fn scenario_delete_unknown_channel_or_id_is_not_found(repo: &dyn ChatRepository) {
    assert_eq!(
        repo.delete_message("ghost", 1, TimestampMillis::from_unix_millis(1))
            .await
            .expect_err("unknown channel")
            .category(),
        ErrorCategory::NotFound
    );
    let id = post(repo, "lobby", ChannelType::Room, "a", "hi", CAP, 1).await;
    assert_eq!(
        repo.delete_message("lobby", id + 1, TimestampMillis::from_unix_millis(2))
            .await
            .expect_err("unknown id")
            .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_edit_and_tombstone_advance_per_channel_event_order(repo: &dyn ChatRepository) {
    let id = post(repo, "lobby", ChannelType::Room, "alice", "first", CAP, 1).await;
    let edited = repo
        .edit_message("lobby", id, "edited", ts(2))
        .await
        .expect("edit");
    assert_eq!(edited.revision, 2);
    assert_eq!(edited.last_event_id, 2);
    assert_eq!(edited.updated_at_unix_ms, 2);
    assert_eq!(edited.content, "edited");

    assert!(
        repo.delete_message("lobby", id, ts(3))
            .await
            .expect("tombstone")
    );
    let message = repo
        .channel_history("lobby", 1, None)
        .await
        .expect("history")
        .remove(0);
    assert!(message.deleted);
    assert_eq!(message.content, "");
    assert_eq!(message.revision, 3);
    assert_eq!(message.last_event_id, 3);
    assert_eq!(message.updated_at_unix_ms, 3);
    assert!(repo.edit_message("lobby", id, "no", ts(4)).await.is_err());
}

async fn scenario_rate_limit_plan_is_all_or_nothing_and_expires(repo: &dyn ChatRepository) {
    let exhausted = ChatRateLimit {
        key: "opaque-exhausted".to_string(),
        limit: 1,
        window_ms: 10,
    };
    repo.consume_rate_limits(std::slice::from_ref(&exhausted), ts(1))
        .await
        .expect("initial allowance");
    let fresh = ChatRateLimit {
        key: "opaque-fresh".to_string(),
        limit: 1,
        window_ms: 10,
    };
    let error = repo
        .consume_rate_limits(&[fresh.clone(), exhausted.clone()], ts(2))
        .await
        .expect_err("exhausted key rejects entire plan");
    assert_eq!(error.category(), ErrorCategory::Permission);
    repo.consume_rate_limits(&[fresh], ts(2))
        .await
        .expect("fresh key was not consumed by rejected plan");
    repo.consume_rate_limits(&[exhausted], ts(10))
        .await
        .expect("new fixed window resets allowance");
    assert!(repo.cleanup_rate_limits(ts(11), 10).await.expect("cleanup") > 0);
}

async fn scenario_moderation_tombstone_writes_one_redacted_audit_and_expires(
    repo: &dyn ChatRepository,
) {
    let id = post(repo, "lobby", ChannelType::Room, "alice", "secret", CAP, 1).await;
    let audit = ChatModerationAudit::tombstone(
        "operator",
        "admin@example.test",
        "operator_remove",
        "lobby",
        id,
        "alice",
        0,
        "correlation-1",
        "node-a",
        ts(2),
    );
    assert!(!audit.actor_id_hash.contains("admin@example.test"));
    assert!(!audit.author_id_hash.contains("alice"));
    assert!(
        repo.moderate_delete_message("lobby", id, &audit, ts(2))
            .await
            .expect("moderate")
    );
    assert_eq!(repo.moderation_audit_count().await.expect("audit count"), 1);
    assert!(
        !repo
            .moderate_delete_message("lobby", id, &audit, ts(3))
            .await
            .expect("repeat moderation")
    );
    assert_eq!(repo.moderation_audit_count().await.expect("audit count"), 1);
    assert_eq!(
        repo.cleanup_moderation_audit(ts(3), 10)
            .await
            .expect("audit cleanup"),
        1
    );
    assert_eq!(repo.moderation_audit_count().await.expect("audit count"), 0);
}

async fn scenario_channels_filter_sort_and_limit(repo: &dyn ChatRepository) {
    post(repo, "lobby-eu", ChannelType::Room, "a", "hi", CAP, 1).await;
    post(repo, "lobby-na", ChannelType::Room, "a", "hi", CAP, 2).await;
    post(repo, "raid-1", ChannelType::Group, "a", "hi", CAP, 3).await;

    // Case-sensitive substring filter.
    let filtered = repo
        .list_channels(Some("lobby"), 0)
        .await
        .expect("filtered");
    assert_eq!(filtered.len(), 2);

    // Most-recently-active first; limit bounds the rows.
    let limited = repo.list_channels(None, 1).await.expect("limited");
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].channel, "raid-1");

    // Full order across the set.
    let names: Vec<String> = repo
        .list_channels(None, 0)
        .await
        .expect("all")
        .into_iter()
        .map(|c| c.channel)
        .collect();
    assert_eq!(names, vec!["raid-1", "lobby-na", "lobby-eu"]);
    assert_eq!(repo.channel_count().await.expect("count"), 3);
}

async fn scenario_canonical_descriptors_and_access_epochs_are_durable(repo: &dyn ChatRepository) {
    let first = repo
        .resolve_canonical_channel("direct:alice:bob", ChannelType::Direct, ts(1))
        .await
        .expect("allocate direct descriptor");
    let replay = repo
        .resolve_canonical_channel("direct:alice:bob", ChannelType::Direct, ts(2))
        .await
        .expect("resolve same descriptor");
    assert_eq!(
        first.id, replay.id,
        "canonical key has one opaque descriptor"
    );
    assert!(first.id.starts_with("ch_"));
    assert_eq!(first.channel_type, ChannelType::Direct);
    assert!(
        repo.resolve_canonical_channel("direct:alice:bob", ChannelType::Group, ts(3))
            .await
            .is_err(),
        "a durable descriptor type is immutable"
    );

    let access_key = "direct:alice:bob";
    assert_eq!(
        repo.current_access_epoch(access_key).await.expect("epoch"),
        0
    );
    assert_eq!(
        repo.advance_access_epoch(access_key, ts(4))
            .await
            .expect("revoke"),
        1
    );
    assert_eq!(
        repo.current_access_epoch(access_key).await.expect("epoch"),
        1
    );
    assert_eq!(
        repo.post_message_authorized(
            &first.id,
            ChannelType::Direct,
            "alice",
            "stale",
            CAP,
            access_key,
            0,
            ts(5),
        )
        .await
        .expect_err("revoked grant must not write")
        .category(),
        ErrorCategory::Permission
    );
    assert_eq!(
        repo.post_message_authorized(
            &first.id,
            ChannelType::Direct,
            "alice",
            "current",
            CAP,
            access_key,
            1,
            ts(6),
        )
        .await
        .expect("current epoch writes"),
        1
    );
    assert_eq!(
        repo.channel_history_authorized(&first.id, 10, None, access_key, 0)
            .await
            .expect_err("revoked grant must not read")
            .category(),
        ErrorCategory::Permission
    );
}

// --- Scenario table ---------------------------------------------------------

type ScenarioFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>;
type Scenario = (&'static str, fn(&dyn ChatRepository) -> ScenarioFuture<'_>);

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        vec![$((
            stringify!($name),
            (|repo| -> ScenarioFuture<'_> { Box::pin($name(repo)) })
                as fn(&dyn ChatRepository) -> ScenarioFuture<'_>,
        )),*]
    };
}

fn all_scenarios() -> Vec<Scenario> {
    scenarios![
        scenario_append_creates_channel_and_summarizes,
        scenario_channel_type_fixed_at_creation,
        scenario_history_newest_first_with_before_cursor,
        scenario_bounded_history_evicts_oldest_but_ids_keep_incrementing,
        scenario_history_for_unknown_channel_is_empty,
        scenario_delete_tombstones_durably_and_is_idempotent,
        scenario_delete_unknown_channel_or_id_is_not_found,
        scenario_edit_and_tombstone_advance_per_channel_event_order,
        scenario_rate_limit_plan_is_all_or_nothing_and_expires,
        scenario_moderation_tombstone_writes_one_redacted_audit_and_expires,
        scenario_channels_filter_sort_and_limit,
        scenario_canonical_descriptors_and_access_epochs_are_durable,
    ]
}

// --- In-memory runs (always) ------------------------------------------------

#[tokio::test]
async fn in_memory_backend_satisfies_the_contract() {
    for (name, run) in all_scenarios() {
        let repo = InMemoryChatRepository::new();
        run(&repo).await;
        let _ = name;
    }
}

// --- SQLite run (always; embedded, no server) -------------------------------

mod sqlite {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::SqliteDatabase;

    #[tokio::test]
    async fn sqlite_backend_satisfies_the_contract() {
        let config = DatabaseConfig {
            url: Some("sqlite::memory:".to_string()),
            ..DatabaseConfig::default()
        };
        let db = SqliteDatabase::connect(&config)
            .await
            .expect("connect + migrate against an in-memory SQLite database");
        let repo = db.chat_repository();

        for (name, run) in all_scenarios() {
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("sqlite scenario: {name}");
            run(repo.as_ref()).await;
        }
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
                "skipping Postgres chat contract: set DATABASE_URL or \
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
        let repo = db.chat_repository();

        for (name, run) in all_scenarios() {
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("postgres scenario: {name}");
            run(repo.as_ref()).await;
        }
    }
}

// MongoDB conversation/message parity is intentionally a subset here: audit,
// rate-limit, and outbox scenarios belong to the follow-on delivery task.
mod mongodb {
    use super::*;
    use ::mongodb::bson::{Document, doc};
    use citadel::config::DatabaseConfig;
    use citadel::repository::MongoDatabase;

    #[tokio::test]
    async fn mongodb_conversations_and_messages_satisfy_the_contract() {
        let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL").ok() else {
            eprintln!("skipping MongoDB chat contract: set CITADEL_TEST_MONGODB_URL");
            return;
        };
        let db = MongoDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect + reconcile MongoDB replica set");
        let scenarios = scenarios![
            scenario_append_creates_channel_and_summarizes,
            // MongoDB rejects a conflicting append type (covered by the real
            // rs0 remediation test); legacy reference backends preserve their
            // original type while accepting that append.
            scenario_history_newest_first_with_before_cursor,
            scenario_bounded_history_evicts_oldest_but_ids_keep_incrementing,
            scenario_history_for_unknown_channel_is_empty,
            scenario_delete_tombstones_durably_and_is_idempotent,
            scenario_delete_unknown_channel_or_id_is_not_found,
            scenario_edit_and_tombstone_advance_per_channel_event_order,
            scenario_channels_filter_sort_and_limit,
            scenario_canonical_descriptors_and_access_epochs_are_durable,
        ];
        for (name, run) in scenarios {
            for collection in [
                "chat_channels",
                "chat_access_epochs",
                "chat_messages",
                "chat_events",
                "chat_moderation_audit",
                "chat_rate_limits",
                "chat_delivery_outbox",
            ] {
                db.database_for_tests()
                    .collection::<Document>(collection)
                    .delete_many(doc! {})
                    .await
                    .expect("clear MongoDB chat fixture");
            }
            eprintln!("mongodb chat scenario: {name}");
            run(&db.mongo_chat_repository()).await;
        }
    }
}
