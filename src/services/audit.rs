//! Console audit trail.
//!
//! Every console mutation (and login attempt) leaves an [`AuditEntry`]: who
//! acted, with which role, what they did, on what, and when. Section handlers
//! record entries explicitly — an explicit call is greppable, testable, and
//! never guesses the target the way blanket middleware would.
//!
//! The log is a bounded in-process ring: the newest [`AuditLog::capacity`]
//! entries are kept, older ones are dropped, and a node restart clears it.
//! Durable audit persistence is recorded technical debt.
//!
//! Entries must never carry secrets: passwords, tokens, and raw payloads stay
//! out of `details` by construction at every call site.

use std::collections::VecDeque;
use std::sync::Mutex;

use serde::Serialize;

use crate::time::TimestampMillis;

/// Default bound on retained entries.
pub const DEFAULT_AUDIT_CAPACITY: usize = 1024;

/// One recorded console action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditEntry {
    /// When the action happened (unix milliseconds).
    pub time_unix_ms: u64,
    /// The operator username that acted (or the presented username for a
    /// failed login).
    pub actor: String,
    /// The operator's role at the time (`admin`, `viewer`, or `-` when
    /// unauthenticated, e.g. a failed login).
    pub role: String,
    /// Dotted action verb, e.g. `console.login`, `storage.write`,
    /// `accounts.ban`.
    pub action: String,
    /// The acted-on resource (an id or path), or `-` when not applicable.
    pub target: String,
    /// Sanitized, human-readable summary. Never carries secrets.
    pub details: String,
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
        Self {
            time_unix_ms: time.unix_millis(),
            actor: actor.into(),
            role: role.into(),
            action: action.into(),
            target: target.into(),
            details: details.into(),
        }
    }
}

/// Filter for reading the log. Fields are conjunctive; `None` matches all.
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    /// Exact actor match.
    pub actor: Option<String>,
    /// Action prefix match (`storage` matches `storage.write`).
    pub action: Option<String>,
    /// Maximum entries returned (newest first). `0` means no explicit limit;
    /// reads are always bounded by the ring capacity.
    pub limit: usize,
}

/// A bounded, in-process, newest-first audit ring.
#[derive(Debug)]
pub struct AuditLog {
    capacity: usize,
    entries: Mutex<VecDeque<AuditEntry>>,
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
        }
    }

    /// The retention bound.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Record one entry, evicting the oldest beyond [`Self::capacity`].
    pub fn record(&self, entry: AuditEntry) {
        let mut entries = self.lock();
        if entries.len() == self.capacity {
            entries.pop_front();
        }
        entries.push_back(entry);
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
    }
}
