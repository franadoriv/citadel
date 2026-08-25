//! Console audit trail.
//!
//! Every console mutation and login attempt leaves an [`AuditEntry`]: who acted,
//! with which role, what they did, on what, and when. Machine-credential reads
//! are recorded centrally after authentication and route authorization, before
//! handler execution; mutation and human-read entries remain explicit in their
//! owning handlers.
//!
//! Every entry lands in a bounded in-process ring — the newest
//! [`AuditLog::capacity`] entries, oldest evicted — and, on a node whose
//! backend stores one, in a durable trail behind an [`AuditSink`]. The ring is
//! never switched off: it answers a read on every backend, and it is the whole
//! trail on the in-memory and MongoDB ones, where a restart clears it.
//!
//! [`AuditLog::record`] and [`AuditLog::list`] stay synchronous because their
//! callers cannot await — one of them is a `map_err` closure. Publishing to the
//! sink is therefore an enqueue onto a bounded queue, never a database write,
//! and an acknowledged record means only that it entered that queue.
//!
//! Entries must never carry secrets: passwords, tokens, and raw payloads stay
//! out of `details` by construction at every call site.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::services::ConsolePrincipal;
use crate::time::TimestampMillis;

/// Default bound on retained entries.
pub const DEFAULT_AUDIT_CAPACITY: usize = 1024;

/// One recorded console action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditEntry {
    /// When the action happened (unix milliseconds).
    pub time_unix_ms: u64,
    /// Actor kind (`human`, `api_key`, or `unauthenticated`).
    pub actor_type: String,
    /// The operator username or public API-key id.
    pub actor: String,
    /// Public machine credential id, when the actor is an API key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
    /// Human-readable API-key name, when applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_name: Option<String>,
    /// Explicit machine scopes, absent for human actors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Vec<String>>,
    /// The operator's role at the time (`admin`, `viewer`, `api_key`, or `-`).
    pub role: String,
    /// Dotted action verb, e.g. `console.login`, `storage.write`,
    /// `accounts.ban`.
    pub action: String,
    /// The acted-on resource (an id or path), or `-` when not applicable.
    pub target: String,
    /// Sanitized, human-readable summary. Never carries secrets.
    pub details: String,
    /// Optional durable match reference.
    ///
    /// Operator actions are deliberately exempt from being forced into a match,
    /// so this is `None` at every console call site; it exists for the entries a
    /// match-scoped subsystem records. Declared last and skipped when absent, so
    /// an entry without one serializes exactly as it did before this field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_id: Option<String>,
}

impl AuditEntry {
    /// Build an entry stamped at `time`.
    #[must_use]
    pub fn new(
        time: TimestampMillis,
        actor: impl Into<String>,
        role: impl Into<String>,
        action: impl Into<String>,
        target: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        let role = role.into();
        Self {
            time_unix_ms: time.unix_millis(),
            actor_type: if role == "-" {
                "unauthenticated".to_string()
            } else {
                "human".to_string()
            },
            actor: actor.into(),
            credential_id: None,
            key_name: None,
            scopes: None,
            role,
            action: action.into(),
            target: target.into(),
            details: details.into(),
            match_id: None,
        }
    }

    /// Build an entry carrying explicit human or machine actor metadata.
    #[must_use]
    pub fn for_principal(
        time: TimestampMillis,
        principal: &ConsolePrincipal,
        action: impl Into<String>,
        target: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        let (credential_id, key_name, scopes) = match principal {
            ConsolePrincipal::Human(_) => (None, None, None),
            ConsolePrincipal::ApiKey(key) => (
                Some(key.id.as_str().to_string()),
                Some(key.name.clone()),
                Some(
                    key.scopes
                        .iter()
                        .map(|scope| scope.as_str().to_string())
                        .collect(),
                ),
            ),
        };
        Self {
            time_unix_ms: time.unix_millis(),
            actor_type: principal.actor_type().to_string(),
            actor: principal.actor_id(),
            credential_id,
            key_name,
            scopes,
            role: principal.role_label().to_string(),
            action: action.into(),
            target: target.into(),
            details: details.into(),
            match_id: None,
        }
    }

    /// Attach a durable match reference to an entry that has one.
    #[must_use]
    pub fn with_match_id(mut self, match_id: impl Into<String>) -> Self {
        self.match_id = Some(match_id.into());
        self
    }
}

/// Filter for reading the log. Fields are conjunctive; `None` matches all.
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    /// Exact actor match.
    pub actor: Option<String>,
    /// Action prefix match (`storage` matches `storage.write`).
    pub action: Option<String>,
    /// Exact durable match reference. `None` matches every entry, including the
    /// ones recorded outside a match — an operator action is never forced into
    /// one.
    pub match_id: Option<String>,
    /// Maximum entries returned (newest first). `0` means no explicit limit;
    /// reads are always bounded by the ring capacity.
    pub limit: usize,
}

/// Durable destination for the console trail.
///
/// Declared here so [`AuditLog::record`] keeps its synchronous signature: the
/// implementation is a bounded write-behind queue, so `publish` enqueues and
/// returns rather than awaiting a database round trip.
pub trait AuditSink: Send + Sync + std::fmt::Debug {
    /// Hand one recorded entry to the durable trail.
    ///
    /// `match_id` repeats `entry.match_id` so a sink that stores the reference
    /// in its own column does not have to reach into the entry for it. This
    /// must never block, await, or fail the caller.
    fn publish(&self, entry: &AuditEntry, match_id: Option<&str>);
}

/// A bounded, in-process, newest-first audit ring with an optional durable
/// sink. The ring answers every read; the sink is what survives a restart.
#[derive(Debug)]
pub struct AuditLog {
    capacity: usize,
    entries: Mutex<VecDeque<AuditEntry>>,
    sink: Option<Arc<dyn AuditSink>>,
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new(DEFAULT_AUDIT_CAPACITY)
    }
}

impl AuditLog {
    /// Create a log retaining at most `capacity` entries (minimum 1).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: Mutex::new(VecDeque::new()),
            sink: None,
        }
    }

    /// Publish every recorded entry to `sink` as well as to the ring.
    #[must_use]
    pub fn with_sink(mut self, sink: Arc<dyn AuditSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Whether a durable trail is attached. The console reports it so an
    /// operator is never shown a process-local ring as durable history.
    #[must_use]
    pub fn has_sink(&self) -> bool {
        self.sink.is_some()
    }

    /// The retention bound.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Record one entry, evicting the oldest beyond [`Self::capacity`].
    pub fn record(&self, entry: AuditEntry) {
        self.record_for_match(entry, None);
    }

    /// Record one entry against a durable match reference.
    ///
    /// An explicit `match_id` overrides whatever the entry carried; passing
    /// `None` keeps the entry's own, so a caller that built the reference into
    /// the entry does not have to repeat it here.
    pub fn record_for_match(&self, entry: AuditEntry, match_id: Option<&str>) {
        let mut entry = entry;
        if let Some(match_id) = match_id {
            entry.match_id = Some(match_id.to_string());
        }
        if let Some(sink) = &self.sink {
            sink.publish(&entry, entry.match_id.as_deref());
        }
        self.push(entry);
    }

    /// Record one entry in the ring only, never in the durable trail.
    ///
    /// This is for the central `console.read` a machine credential leaves when
    /// it reads the trail itself: in a ring that entry evicts, but a durable
    /// row per poll would be an unbounded, self-feeding write whose only reader
    /// is the poller that produced it.
    pub fn record_volatile(&self, entry: AuditEntry) {
        self.push(entry);
    }

    /// Read entries newest-first, applying `filter`.
    #[must_use]
    pub fn list(&self, filter: &AuditFilter) -> Vec<AuditEntry> {
        let entries = self.lock();
        let limit = if filter.limit == 0 {
            self.capacity
        } else {
            filter.limit
        };
        entries
            .iter()
            .rev()
            .filter(|entry| {
                filter
                    .actor
                    .as_ref()
                    .is_none_or(|actor| &entry.actor == actor)
            })
            .filter(|entry| {
                filter
                    .action
                    .as_ref()
                    .is_none_or(|action| entry.action.starts_with(action.as_str()))
            })
            .filter(|entry| {
                filter
                    .match_id
                    .as_ref()
                    .is_none_or(|match_id| entry.match_id.as_deref() == Some(match_id.as_str()))
            })
            .take(limit)
            .cloned()
            .collect()
    }

    /// Number of retained entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether nothing has been recorded (or everything was evicted).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Append to the ring, evicting the oldest beyond [`Self::capacity`].
    fn push(&self, entry: AuditEntry) {
        let mut entries = self.lock();
        if entries.len() == self.capacity {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Lock the ring, recovering from a poisoned lock: the deque holds no
    /// cross-entry invariants a panicking writer could break halfway.
    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<AuditEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(seq: u64, actor: &str, action: &str) -> AuditEntry {
        AuditEntry::new(
            TimestampMillis::from_unix_millis(seq),
            actor,
            "admin",
            action,
            "-",
            format!("entry {seq}"),
        )
    }

    #[test]
    fn list_returns_newest_first() {
        let log = AuditLog::new(10);
        log.record(entry(1, "ops", "console.login"));
        log.record(entry(2, "ops", "storage.write"));
        log.record(entry(3, "ops", "storage.delete"));
        let all = log.list(&AuditFilter::default());
        let times: Vec<u64> = all.iter().map(|e| e.time_unix_ms).collect();
        assert_eq!(times, vec![3, 2, 1]);
    }

    #[test]
    fn ring_never_exceeds_capacity_and_drops_oldest() {
        let log = AuditLog::new(3);
        for seq in 1..=5 {
            log.record(entry(seq, "ops", "console.login"));
        }
        assert_eq!(log.len(), 3);
        let times: Vec<u64> = log
            .list(&AuditFilter::default())
            .iter()
            .map(|e| e.time_unix_ms)
            .collect();
        assert_eq!(times, vec![5, 4, 3], "oldest entries evicted");
    }

    #[test]
    fn filters_are_conjunctive_and_action_is_prefix() {
        let log = AuditLog::new(10);
        log.record(entry(1, "ops", "storage.write"));
        log.record(entry(2, "viewer-bot", "storage.write"));
        log.record(entry(3, "ops", "accounts.ban"));
        let filter = AuditFilter {
            actor: Some("ops".to_string()),
            action: Some("storage".to_string()),
            limit: 0,
            ..AuditFilter::default()
        };
        let hits = log.list(&filter);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].time_unix_ms, 1);
    }

    #[test]
    fn limit_bounds_the_page() {
        let log = AuditLog::new(10);
        for seq in 1..=6 {
            log.record(entry(seq, "ops", "console.login"));
        }
        let page = log.list(&AuditFilter {
            limit: 2,
            ..AuditFilter::default()
        });
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].time_unix_ms, 6);
    }

    #[test]
    fn zero_capacity_is_clamped_to_one() {
        let log = AuditLog::new(0);
        log.record(entry(1, "ops", "a"));
        log.record(entry(2, "ops", "b"));
        assert_eq!(log.len(), 1);
        assert_eq!(log.capacity(), 1);
    }

    #[test]
    fn entry_serializes_with_stable_field_names() {
        let value = serde_json::to_value(entry(7, "ops", "console.login")).expect("serializes");
        assert_eq!(value["time_unix_ms"], 7);
        assert_eq!(value["actor"], "ops");
        assert_eq!(value["role"], "admin");
        assert_eq!(value["action"], "console.login");
        assert!(
            value.get("match_id").is_none(),
            "an entry outside a match serializes exactly as it did before the field existed"
        );
    }

    /// Records what a durable sink was handed, in order.
    #[derive(Debug, Default)]
    struct RecordingSink {
        published: Mutex<Vec<(AuditEntry, Option<String>)>>,
    }

    impl RecordingSink {
        fn published(&self) -> Vec<(AuditEntry, Option<String>)> {
            self.published
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl AuditSink for RecordingSink {
        fn publish(&self, entry: &AuditEntry, match_id: Option<&str>) {
            self.published
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((entry.clone(), match_id.map(str::to_string)));
        }
    }

    #[test]
    fn a_sink_sees_every_recorded_entry_and_the_ring_still_answers() {
        let sink = Arc::new(RecordingSink::default());
        let log = AuditLog::new(10).with_sink(Arc::clone(&sink) as Arc<dyn AuditSink>);
        assert!(log.has_sink());
        log.record(entry(1, "ops", "storage.write"));
        log.record(entry(2, "ops", "accounts.ban"));
        assert_eq!(log.len(), 2, "durability never replaces the ring");
        let published = sink.published();
        assert_eq!(
            published
                .iter()
                .map(|(entry, _)| entry.action.as_str())
                .collect::<Vec<_>>(),
            vec!["storage.write", "accounts.ban"],
            "the sink sees entries oldest-first, as recorded"
        );
        assert!(published.iter().all(|(_, match_id)| match_id.is_none()));
    }

    #[test]
    fn a_match_scoped_record_stamps_the_entry_and_the_sink_argument() {
        let sink = Arc::new(RecordingSink::default());
        let log = AuditLog::new(10).with_sink(Arc::clone(&sink) as Arc<dyn AuditSink>);
        log.record_for_match(entry(1, "ops", "matchlog.detail"), Some("mt1-abc"));
        // An entry that already carries a reference keeps it when none is passed.
        log.record_for_match(
            entry(2, "ops", "matchlog.entries").with_match_id("mt1-def"),
            None,
        );
        let published = sink.published();
        assert_eq!(
            published
                .iter()
                .map(|(_, match_id)| match_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("mt1-abc"), Some("mt1-def")]
        );
        assert_eq!(
            published
                .iter()
                .map(|(entry, _)| entry.match_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("mt1-abc"), Some("mt1-def")],
            "the entry and the sink argument never disagree"
        );
        let scoped = log.list(&AuditFilter {
            match_id: Some("mt1-abc".to_string()),
            ..AuditFilter::default()
        });
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].action, "matchlog.detail");
        assert_eq!(
            log.list(&AuditFilter::default()).len(),
            2,
            "an absent match filter matches entries with no match at all"
        );
    }

    #[test]
    fn a_volatile_record_stays_out_of_the_durable_trail() {
        let sink = Arc::new(RecordingSink::default());
        let log = AuditLog::new(1_024).with_sink(Arc::clone(&sink) as Arc<dyn AuditSink>);
        for seq in 1..=100 {
            log.record_volatile(entry(seq, "poller", "console.read"));
        }
        assert_eq!(log.len(), 100, "the ring still holds the read trail");
        assert!(
            sink.published().is_empty(),
            "a credential polling the trail must not write one durable row per poll"
        );
    }
}
