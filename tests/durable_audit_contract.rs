//! The console trail's durability seam.
//!
//! `tests/console_audit_repository_contract.rs` pins the SQL. This pins the
//! layer above it: that recording stays synchronous, that the ring and the
//! durable trail are two destinations for one record rather than alternatives,
//! and that `App` reads whichever of them the selected backend actually has.
//!
//! The embedded SQLite half always runs; the in-memory half is the degradation
//! every backend without durable log tables falls back to.

use std::sync::{Arc, Mutex, PoisonError};

use citadel::App;
use citadel::config::{Config, DatabaseConfig};
use citadel::ids::{NodeIdentity, SHORT_PREFIX_ID_LEN, valid_id};
use citadel::repository::{Backend, DurableAuditFilter, DurableAuditRow, SqliteDatabase};
use citadel::services::{AuditEntry, AuditFilter, AuditLog, AuditSink};
use citadel::time::TimestampMillis;

fn entry(at_ms: u64, actor: &str, action: &str) -> AuditEntry {
    AuditEntry::new(
        TimestampMillis::from_unix_millis(at_ms),
        actor,
        "admin",
        action,
        "-",
        "ok",
    )
}

/// Stands in for the write-behind writer: it records what it was handed and
/// never blocks, which is the entire contract [`AuditSink`] states.
#[derive(Debug, Default)]
struct CollectingSink {
    published: Mutex<Vec<(AuditEntry, Option<String>)>>,
}

impl CollectingSink {
    fn published(&self) -> Vec<(AuditEntry, Option<String>)> {
        self.published
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn actions(&self) -> Vec<String> {
        self.published()
            .into_iter()
            .map(|(entry, _)| entry.action)
            .collect()
    }
}

impl AuditSink for CollectingSink {
    fn publish(&self, entry: &AuditEntry, match_id: Option<&str>) {
        self.published
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((entry.clone(), match_id.map(str::to_string)));
    }
}

#[test]
fn the_sink_keeps_what_the_ring_evicts() {
    let sink = Arc::new(CollectingSink::default());
    let log = AuditLog::new(2).with_sink(Arc::clone(&sink) as Arc<dyn AuditSink>);
    for seq in 1..=6 {
        log.record(entry(seq, "ops", format!("storage.write.{seq}").as_str()));
    }
    assert_eq!(log.len(), 2, "the ring keeps its bound");
    assert_eq!(
        sink.actions(),
        vec![
            "storage.write.1",
            "storage.write.2",
            "storage.write.3",
            "storage.write.4",
            "storage.write.5",
            "storage.write.6",
        ],
        "eviction from the ring is not a loss from the durable trail"
    );
}

#[test]
fn a_volatile_record_never_reaches_the_sink() {
    let sink = Arc::new(CollectingSink::default());
    let log = AuditLog::new(1_024).with_sink(Arc::clone(&sink) as Arc<dyn AuditSink>);
    for seq in 1..=200 {
        log.record_volatile(entry(seq, "ci-poller", "console.read"));
    }
    log.record(entry(201, "ops", "accounts.ban"));
    assert_eq!(log.len(), 201, "the ring still holds every read");
    assert_eq!(
        sink.actions(),
        vec!["accounts.ban"],
        "a credential polling the trail must not write one durable row per poll"
    );
}

#[test]
fn a_match_scoped_record_agrees_with_itself_and_the_ring_can_filter_on_it() {
    let sink = Arc::new(CollectingSink::default());
    let log = AuditLog::new(16).with_sink(Arc::clone(&sink) as Arc<dyn AuditSink>);
    log.record(entry(1, "ops", "accounts.ban"));
    log.record_for_match(entry(2, "ops", "matchlog.detail"), Some("mt1-abc"));
    log.record_for_match(
        entry(3, "ops", "matchlog.entries").with_match_id("mt1-def"),
        None,
    );

    let published = sink.published();
    assert_eq!(
        published
            .iter()
            .map(|(entry, match_id)| (entry.match_id.as_deref(), match_id.as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (None, None),
            (Some("mt1-abc"), Some("mt1-abc")),
            (Some("mt1-def"), Some("mt1-def")),
        ],
        "the entry and the sink argument never disagree"
    );

    let scoped = log.list(&AuditFilter {
        match_id: Some("mt1-abc".to_string()),
        ..AuditFilter::default()
    });
    assert_eq!(
        scoped
            .iter()
            .map(|entry| entry.action.as_str())
            .collect::<Vec<_>>(),
        vec!["matchlog.detail"]
    );
    assert_eq!(
        log.list(&AuditFilter::default()).len(),
        3,
        "an absent match filter matches the operator actions that belong to no match"
    );
}

#[test]
fn an_entry_outside_a_match_serializes_exactly_as_it_did_before() {
    let plain = serde_json::to_value(entry(7, "ops", "console.login")).expect("serialize");
    assert!(
        plain.get("match_id").is_none(),
        "the optional reference must not appear on the 44 call sites that have none"
    );
    let scoped = serde_json::to_value(entry(7, "ops", "matchlog.detail").with_match_id("mt1-abc"))
        .expect("serialize");
    assert_eq!(scoped["match_id"], "mt1-abc");
}

#[tokio::test]
async fn a_backend_without_durable_log_tables_reads_the_ring_and_offers_no_cursor() {
    let app = App::new(Config::default());
    assert!(!app.audit_is_durable());
    app.audit_log().record(entry(1, "ops", "storage.write"));
    app.audit_log().record(entry(2, "ops", "accounts.ban"));

    // `0` is the frozen ring contract: it means "the retention bound", never
    // "no rows".
    let rows = app
        .list_audit(&DurableAuditFilter::default())
        .await
        .expect("ring page");
    assert_eq!(
        rows.iter()
            .map(|row| row.entry.action.as_str())
            .collect::<Vec<_>>(),
        vec!["accounts.ban", "storage.write"],
        "newest first, exactly as the ring has always answered"
    );
    assert!(
        rows.iter().all(|row| row.audit_id.is_empty()),
        "a ring entry has no durable key, so it can never be handed out as a cursor"
    );
    assert_eq!(
        app.count_audit(&DurableAuditFilter::default())
            .await
            .expect("ring count"),
        2
    );

    // The ring records no match and stores no cursor, so either filter is
    // honestly empty rather than silently ignored.
    for filter in [
        DurableAuditFilter {
            match_id: Some("mt1-abc".to_string()),
            ..DurableAuditFilter::default()
        },
        DurableAuditFilter {
            after_audit_id: Some(format!("au1-{:029x}", 1_u64)),
            ..DurableAuditFilter::default()
        },
    ] {
        assert!(
            app.list_audit(&filter).await.expect("ring page").is_empty(),
            "a ring page never pretends to answer a durable-only filter"
        );
    }
}

#[tokio::test]
async fn a_sqlite_backend_reads_the_durable_trail() {
    let database = SqliteDatabase::connect(&DatabaseConfig {
        url: Some("sqlite::memory:".to_owned()),
        ..DatabaseConfig::default()
    })
    .await
    .expect("connect and migrate SQLite");
    database
        .reset_storage_for_tests()
        .await
        .expect("clear SQLite fixtures");
    let database: Arc<dyn Backend> = Arc::new(database);
    let repository = database
        .audit_repository()
        .expect("SQLite audit repository");
    let app = App::with_backend(Config::default(), Arc::clone(&database));
    assert!(app.audit_is_durable());

    let identity = NodeIdentity::new("durable-audit-node");
    // Minted in ascending time, so the primary key is already the sort order
    // the trail is read in.
    let rows = vec![
        DurableAuditRow {
            audit_id: identity.mint("au1-", 1_000),
            node_id: identity.node_id().to_string(),
            match_id: None,
            entry: entry(1_000, "ops", "storage.write"),
        },
        DurableAuditRow {
            audit_id: identity.mint("au1-", 2_000),
            node_id: identity.node_id().to_string(),
            match_id: None,
            entry: entry(2_000, "other", "accounts.ban"),
        },
        DurableAuditRow {
            audit_id: identity.mint("au1-", 3_000),
            node_id: identity.node_id().to_string(),
            match_id: Some("mt1-abc".to_string()),
            entry: entry(3_000, "ops", "matchlog.detail"),
        },
    ];
    repository.append_batch(&rows).await.expect("append");

    let page = app
        .list_audit(&DurableAuditFilter {
            limit: 10,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("durable page");
    assert_eq!(
        page.iter()
            .map(|row| row.entry.action.as_str())
            .collect::<Vec<_>>(),
        vec!["matchlog.detail", "accounts.ban", "storage.write"],
        "the durable trail is newest-first too"
    );
    assert!(
        page.iter()
            .all(|row| valid_id(&row.audit_id, "au1-", SHORT_PREFIX_ID_LEN)),
        "every durable row hands the console a usable cursor"
    );

    // The ring reads `0` as its capacity; a durable `LIMIT 0` would silently
    // return nothing, so `App` translates it before it reaches SQL.
    let translated = app
        .list_audit(&DurableAuditFilter::default())
        .await
        .expect("durable page with the ring's zero");
    assert_eq!(translated.len(), 1);
    assert_eq!(translated[0].entry.action, "matchlog.detail");

    // Keyset paging over the durable trail never repeats or skips a row.
    let after_first = app
        .list_audit(&DurableAuditFilter {
            after_audit_id: Some(page[0].audit_id.clone()),
            limit: 10,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("second durable page");
    assert_eq!(
        after_first
            .iter()
            .map(|row| row.entry.action.as_str())
            .collect::<Vec<_>>(),
        vec!["accounts.ban", "storage.write"]
    );

    // Counts ignore the cursor and the page size: they say how much history
    // matches, not how much of it one page showed.
    assert_eq!(
        app.count_audit(&DurableAuditFilter {
            after_audit_id: Some(page[0].audit_id.clone()),
            limit: 1,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("durable count"),
        3
    );
    assert_eq!(
        app.count_audit(&DurableAuditFilter {
            actor: Some("ops".to_string()),
            ..DurableAuditFilter::default()
        })
        .await
        .expect("filtered durable count"),
        2
    );

    // An operator action is never forced into a match, and an absent filter
    // still matches the ones that carry none.
    let scoped = app
        .list_audit(&DurableAuditFilter {
            match_id: Some("mt1-abc".to_string()),
            limit: 10,
            ..DurableAuditFilter::default()
        })
        .await
        .expect("match-scoped page");
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].entry.action, "matchlog.detail");
}
