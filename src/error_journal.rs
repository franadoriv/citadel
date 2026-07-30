//! Bounded, local incident storage for the operator dashboard.
//!
//! The journal is deliberately a small JSONL file instead of a database. It is
//! available before the primary datastore is healthy, survives a process crash,
//! and can live beside the server executable. Entries contain only the stable
//! error category and a caller-supplied component label. In particular, error
//! messages and details, panic payloads, request data, and stack traces are
//! never persisted here.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::AppError;

/// File name used by [`ErrorJournal::from_executable_path`].
pub const DEFAULT_JOURNAL_FILE_NAME: &str = "citadel-errors.jsonl";

/// Maximum size of a default journal file.
pub const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Maximum number of raw incident occurrences retained by default.
pub const DEFAULT_MAX_ENTRIES: usize = 2_000;

/// Largest page returned from [`ErrorJournal::read_page`].
pub const MAX_PAGE_SIZE: usize = 100;

/// The class of incident recorded in the local journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentKind {
    /// A typed application error reached an operator-visible boundary.
    Error,
    /// An unhandled process panic was observed by the global panic hook.
    Panic,
}

/// A safe incident input accepted by [`ErrorJournal::append`].
///
/// Construct this type through [`Self::from_app_error`] or [`Self::panic`].
/// Those constructors deliberately exclude all `AppError` text, panic payloads,
/// request data, and backtraces, which may contain secrets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalIncident {
    kind: IncidentKind,
    category: String,
    component: String,
    message: String,
}

impl JournalIncident {
    /// Capture only a category-derived marker for a typed application error.
    ///
    /// An [`AppError`] message is often sanitized for a client response, but it
    /// is still caller-provided text and is not a storage-safe security
    /// boundary. The journal therefore does not retain any of its text.
    #[must_use]
    pub fn from_app_error(component: impl AsRef<str>, error: &AppError) -> Self {
        let category = error.category().code().to_string();
        Self {
            kind: IncidentKind::Error,
            message: generic_message(IncidentKind::Error, &category),
            category,
            component: sanitize_component(component.as_ref()),
        }
    }

    /// Record a generic panic marker without inspecting the panic payload.
    ///
    /// Panic payloads, source locations, and backtraces can include request or
    /// configuration data. They belong in the configured external error
    /// reporter, not in the dashboard-visible local journal.
    #[must_use]
    pub fn panic(component: impl AsRef<str>) -> Self {
        Self {
            kind: IncidentKind::Panic,
            category: "internal".to_string(),
            component: sanitize_component(component.as_ref()),
            message: generic_message(IncidentKind::Panic, "internal"),
        }
    }

    fn fingerprint(&self) -> String {
        fingerprint(self.kind, &self.category, &self.component, &self.message)
    }
}

/// Result of an [`ErrorJournal::append`] attempt.
///
/// Recording must never turn a recoverable application error into a server
/// failure, so I/O, serialization, and contention failures are intentionally
/// collapsed to [`Self::Skipped`]. Callers can emit a separate tracing event
/// when desired, but must not propagate this result as their original error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalAppendOutcome {
    /// The incident was durably written to the local journal file.
    Written,
    /// The journal was unavailable, contended, or too small for this incident.
    Skipped,
}

impl JournalAppendOutcome {
    /// Whether the incident was written.
    #[must_use]
    pub const fn is_written(self) -> bool {
        matches!(self, Self::Written)
    }
}

/// One dashboard-visible incident, grouped by its stable fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JournalEntry {
    /// Stable SHA-256 fingerprint of the safe incident fields.
    pub fingerprint: String,
    /// Incident class.
    pub kind: IncidentKind,
    /// Stable [`crate::ErrorCategory`] code.
    pub category: String,
    /// Sanitized component label supplied by the recorder.
    pub component: String,
    /// Generic safe message; it never includes `AppError` or panic text.
    pub message: String,
    /// First observed time as Unix milliseconds.
    pub first_seen_ms: u64,
    /// Most recent observed time as Unix milliseconds.
    pub last_seen_ms: u64,
    /// Number of retained occurrences with this fingerprint.
    pub count: u64,
}

/// A bounded, offset-paginated dashboard result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JournalPage {
    /// Incidents ordered by most recent occurrence first.
    pub entries: Vec<JournalEntry>,
    /// Number of distinct incident fingerprints available in this read.
    pub total: usize,
    /// Offset for the following page, if there is one.
    pub next_offset: Option<usize>,
}

/// A local, best-effort JSONL incident journal.
///
/// The journal has a process-local non-blocking lock. If another writer is
/// compacting the file, the new incident is skipped rather than delaying the
/// server's error or panic path. The file is kept bounded by retaining the most
/// recent complete JSONL records.
#[derive(Debug, Clone)]
pub struct ErrorJournal {
    path: PathBuf,
    max_bytes: usize,
    max_entries: usize,
    write_lock: Arc<Mutex<()>>,
}

impl ErrorJournal {
    /// Create a default journal beside `executable_path`.
    ///
    /// An executable path supplied by [`std::env::current_exe`] is normally
    /// absolute, so this places `citadel-errors.jsonl` in the same directory as
    /// the server binary. A path without a parent falls back to the current
    /// working directory.
    #[must_use]
    pub fn from_executable_path(executable_path: impl AsRef<Path>) -> Self {
        Self::new(journal_path_for_executable(executable_path.as_ref()))
    }

    /// Create a journal at an explicit file path with default retention limits.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self::with_limits(path, DEFAULT_MAX_BYTES, DEFAULT_MAX_ENTRIES)
    }

    /// Create a journal at an explicit path and retention limits.
    ///
    /// Values smaller than one are normalized to one. An individual serialized
    /// incident larger than `max_bytes` is safely skipped.
    #[must_use]
    pub fn with_limits(path: impl Into<PathBuf>, max_bytes: usize, max_entries: usize) -> Self {
        Self {
            path: path.into(),
            max_bytes: max_bytes.max(1),
            max_entries: max_entries.max(1),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Return the journal file path for diagnostics and dashboard wiring.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Best-effort append of one safe incident occurrence.
    ///
    /// The method intentionally has no error return. Failure to journal an
    /// incident must not mask or replace the triggering server error. Repeated
    /// incidents retain one raw JSONL row each; [`Self::read_page`] groups them
    /// into `fingerprint`/`count` dashboard entries.
    #[must_use]
    pub fn append(&self, incident: JournalIncident) -> JournalAppendOutcome {
        self.append_at(incident, unix_time_ms())
    }

    /// Read a page of grouped, dashboard-safe incidents.
    ///
    /// Corrupt or unknown JSONL rows are ignored so a partial write or a manual
    /// edit cannot make the dashboard unavailable. I/O errors similarly yield
    /// an empty page; the HTTP layer may expose journal availability separately
    /// without leaking filesystem paths or error details.
    #[must_use]
    pub fn read_page(&self, offset: usize, limit: usize) -> JournalPage {
        let Ok(_guard) = self.write_lock.try_lock() else {
            return JournalPage::empty();
        };

        // A prior process may have been interrupted after moving the old file
        // aside but before installing the compacted replacement. Restore that
        // durable backup before reading whenever possible.
        let _ = self.recover_interrupted_replace();
        let records = self.read_records();
        let mut grouped: HashMap<String, JournalEntry> = HashMap::new();
        for record in records {
            let entry = grouped
                .entry(record.fingerprint.clone())
                .or_insert_with(|| JournalEntry {
                    fingerprint: record.fingerprint,
                    kind: record.kind,
                    category: record.category,
                    component: record.component,
                    message: record.message,
                    first_seen_ms: record.timestamp_ms,
                    last_seen_ms: record.timestamp_ms,
                    count: 0,
                });
            entry.first_seen_ms = entry.first_seen_ms.min(record.timestamp_ms);
            entry.last_seen_ms = entry.last_seen_ms.max(record.timestamp_ms);
            entry.count = entry.count.saturating_add(record.count.max(1));
        }

        let mut entries: Vec<_> = grouped.into_values().collect();
        entries.sort_unstable_by(|left, right| {
            right
                .last_seen_ms
                .cmp(&left.last_seen_ms)
                .then_with(|| left.fingerprint.cmp(&right.fingerprint))
        });

        let total = entries.len();
        if limit == 0 || offset >= total {
            return JournalPage {
                entries: Vec::new(),
                total,
                next_offset: None,
            };
        }
        let end = offset.saturating_add(limit.min(MAX_PAGE_SIZE)).min(total);
        let next_offset = (end < total).then_some(end);
        JournalPage {
            entries: entries.drain(offset..end).collect(),
            total,
            next_offset,
        }
    }

    fn append_at(&self, incident: JournalIncident, timestamp_ms: u64) -> JournalAppendOutcome {
        let Ok(_guard) = self.write_lock.try_lock() else {
            return JournalAppendOutcome::Skipped;
        };

        let fingerprint = incident.fingerprint();
        let record = StoredIncident {
            version: 1,
            timestamp_ms,
            kind: incident.kind,
            category: incident.category,
            component: incident.component,
            message: incident.message,
            fingerprint,
            count: 1,
        };
        let Ok(mut line) = serde_json::to_vec(&record) else {
            return JournalAppendOutcome::Skipped;
        };
        line.push(b'\n');
        if line.len() > self.max_bytes || self.ensure_parent().is_err() {
            return JournalAppendOutcome::Skipped;
        }
        if self.recover_interrupted_replace().is_err() && !self.path.exists() {
            return JournalAppendOutcome::Skipped;
        }

        let existing_len = fs::metadata(&self.path)
            .map(|metadata| metadata.len() as usize)
            .unwrap_or(0);
        let must_compact = existing_len.saturating_add(line.len()) > self.max_bytes
            || existing_len > self.max_bytes
            || self.retained_line_count() >= self.max_entries;
        if !must_compact {
            return append_line(&self.path, &line);
        }

        let byte_budget = self.max_bytes.saturating_sub(line.len());
        let mut retained = self.complete_tail_lines(byte_budget);
        let keep = self.max_entries.saturating_sub(1);
        if retained.len() > keep {
            retained.drain(..retained.len() - keep);
        }
        retained.push(line);
        self.replace_lines(&retained)
    }

    fn ensure_parent(&self) -> std::io::Result<()> {
        let Some(parent) = self.path.parent() else {
            return Ok(());
        };
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        fs::create_dir_all(parent)
    }

    /// Replace a compacted journal without truncating the current file in
    /// place. Windows does not permit a direct rename over an existing file, so
    /// keep a same-directory backup until the synced temporary replacement is
    /// installed. If a process is killed in the small handoff gap, the next
    /// read/append restores the `.bak` file before proceeding.
    fn replace_lines(&self, lines: &[Vec<u8>]) -> JournalAppendOutcome {
        let temporary = self.temporary_path();
        let backup = self.backup_path();
        if temporary.exists() && fs::remove_file(&temporary).is_err() {
            return JournalAppendOutcome::Skipped;
        }
        if self.path.exists() && backup.exists() && fs::remove_file(&backup).is_err() {
            return JournalAppendOutcome::Skipped;
        }

        let Ok(mut file) = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
        else {
            return JournalAppendOutcome::Skipped;
        };
        for line in lines {
            if file.write_all(line).is_err() {
                let _ = fs::remove_file(&temporary);
                return JournalAppendOutcome::Skipped;
            }
        }
        if file.flush().is_err() || file.sync_data().is_err() {
            let _ = fs::remove_file(&temporary);
            return JournalAppendOutcome::Skipped;
        }
        drop(file);

        if self.path.exists() && fs::rename(&self.path, &backup).is_err() {
            let _ = fs::remove_file(&temporary);
            return JournalAppendOutcome::Skipped;
        }
        if fs::rename(&temporary, &self.path).is_err() {
            if backup.exists() && !self.path.exists() {
                let _ = fs::rename(&backup, &self.path);
            }
            let _ = fs::remove_file(&temporary);
            return JournalAppendOutcome::Skipped;
        }
        let _ = fs::remove_file(backup);
        JournalAppendOutcome::Written
    }

    fn recover_interrupted_replace(&self) -> std::io::Result<()> {
        let backup = self.backup_path();
        if !backup.exists() {
            return Ok(());
        }
        if self.path.exists() {
            fs::remove_file(backup)
        } else {
            fs::rename(backup, &self.path)
        }
    }

    fn backup_path(&self) -> PathBuf {
        sidecar_path(&self.path, ".bak")
    }

    fn temporary_path(&self) -> PathBuf {
        sidecar_path(&self.path, &format!(".{}.tmp", std::process::id()))
    }

    fn retained_line_count(&self) -> usize {
        self.complete_tail_lines(self.max_bytes)
            .len()
            .min(self.max_entries)
    }

    fn read_records(&self) -> Vec<StoredIncident> {
        self.complete_tail_lines(self.max_bytes)
            .into_iter()
            .filter_map(|line| serde_json::from_slice(&line).ok())
            .filter(StoredIncident::is_safe)
            .collect()
    }

    fn complete_tail_lines(&self, byte_budget: usize) -> Vec<Vec<u8>> {
        if byte_budget == 0 {
            return Vec::new();
        }
        let Ok(mut file) = File::open(&self.path) else {
            return Vec::new();
        };
        let Ok(length) = file.metadata().map(|metadata| metadata.len()) else {
            return Vec::new();
        };
        let start = length.saturating_sub(byte_budget as u64);
        if file.seek(SeekFrom::Start(start)).is_err() {
            return Vec::new();
        }

        let mut bytes = Vec::new();
        if file.read_to_end(&mut bytes).is_err() {
            return Vec::new();
        }
        if start > 0 {
            let Some(first_newline) = bytes.iter().position(|byte| *byte == b'\n') else {
                return Vec::new();
            };
            bytes.drain(..=first_newline);
        }

        bytes
            .split_inclusive(|byte| *byte == b'\n')
            .filter(|line| line.last() == Some(&b'\n'))
            .map(Vec::from)
            .collect()
    }
}

impl JournalPage {
    fn empty() -> Self {
        Self {
            entries: Vec::new(),
            total: 0,
            next_offset: None,
        }
    }
}

/// Derive the default journal location from an executable path.
#[must_use]
pub fn journal_path_for_executable(executable_path: &Path) -> PathBuf {
    executable_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(
            || PathBuf::from(DEFAULT_JOURNAL_FILE_NAME),
            |parent| parent.join(DEFAULT_JOURNAL_FILE_NAME),
        )
}

fn append_line(path: &Path, line: &[u8]) -> JournalAppendOutcome {
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) else {
        return JournalAppendOutcome::Skipped;
    };
    if file.write_all(line).is_err() || file.flush().is_err() || file.sync_data().is_err() {
        return JournalAppendOutcome::Skipped;
    }
    JournalAppendOutcome::Written
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn generic_message(kind: IncidentKind, category: &str) -> String {
    match kind {
        IncidentKind::Error => format!("{category} failure"),
        IncidentKind::Panic => "unexpected server panic".to_string(),
    }
}

fn fingerprint(kind: IncidentKind, category: &str, component: &str, message: &str) -> String {
    let mut digest = Sha256::new();
    for part in [
        match kind {
            IncidentKind::Error => "error",
            IncidentKind::Panic => "panic",
        },
        category,
        component,
        message,
    ] {
        digest.update(part.as_bytes());
        // A separator prevents ambiguous concatenations such as `ab` + `c`
        // and `a` + `bc` from producing the same fingerprint input.
        digest.update([0]);
    }
    format!("{:x}", digest.finalize())
}

fn sanitize_component(component: &str) -> String {
    let sanitized: String = component
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '.' => character,
            _ => '_',
        })
        .take(96)
        .collect();
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredIncident {
    version: u8,
    timestamp_ms: u64,
    kind: IncidentKind,
    category: String,
    component: String,
    message: String,
    fingerprint: String,
    count: u64,
}

impl StoredIncident {
    fn is_safe(&self) -> bool {
        if self.version != 1 || !is_known_category(&self.category) || self.count == 0 {
            return false;
        }
        if self.kind == IncidentKind::Panic && self.category != "internal" {
            return false;
        }
        if self.component != sanitize_component(&self.component) {
            return false;
        }
        let message = generic_message(self.kind, &self.category);
        self.message == message
            && self.fingerprint
                == fingerprint(self.kind, &self.category, &self.component, &self.message)
    }
}

fn is_known_category(category: &str) -> bool {
    matches!(
        category,
        "config"
            | "auth"
            | "permission"
            | "validation"
            | "not_found"
            | "conflict"
            | "deadline"
            | "cancelled"
            | "database"
            | "runtime"
            | "transport"
            | "internal"
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(0);

    fn test_path(label: &str) -> PathBuf {
        let nonce = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "citadel-error-journal-{label}-{}-{nonce}.jsonl",
            std::process::id()
        ))
    }

    #[test]
    fn default_path_uses_the_executable_directory() {
        let executable = Path::new("C:/citadel/bin/citadel.exe");
        let journal = ErrorJournal::from_executable_path(executable);
        let expected = PathBuf::from("C:/citadel/bin").join(DEFAULT_JOURNAL_FILE_NAME);
        assert_eq!(journal_path_for_executable(executable), expected);
        assert_eq!(journal.path(), expected);
    }

    #[test]
    fn app_error_entries_exclude_internal_detail() {
        let path = test_path("redaction");
        let journal = ErrorJournal::new(&path);
        let error = AppError::database("postgres://user:very-secret@db.example/citadel")
            .with_detail("postgres://user:very-secret@db.example/citadel");

        assert!(
            journal
                .append(JournalIncident::from_app_error("repository.pg", &error))
                .is_written()
        );

        let raw = fs::read_to_string(&path).expect("journal should be readable");
        assert!(!raw.contains("very-secret"));
        assert!(!raw.contains("postgres://"));
        let page = journal.read_page(0, 10);
        assert_eq!(page.total, 1);
        assert_eq!(page.entries[0].category, "database");
        assert_eq!(page.entries[0].message, "database failure");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn panic_entries_never_persist_the_payload() {
        let path = test_path("panic");
        let journal = ErrorJournal::new(&path);

        assert!(
            journal
                .append_at(JournalIncident::panic("http"), 42)
                .is_written()
        );

        let raw = fs::read_to_string(&path).expect("journal should be readable");
        assert!(raw.contains("unexpected server panic"));
        assert!(!raw.contains("payload"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn dashboard_ignores_tampered_rows_with_untrusted_text() {
        let path = test_path("tampered");
        fs::write(
            &path,
            r#"{"version":1,"timestamp_ms":1,"kind":"error","category":"internal","component":"http","message":"api-key=very-secret","fingerprint":"untrusted","count":1}
"#,
        )
        .expect("tampered fixture should be written");
        let journal = ErrorJournal::new(&path);

        assert_eq!(journal.read_page(0, 10), JournalPage::empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn read_page_groups_repeated_fingerprints_and_paginates() {
        let path = test_path("pagination");
        let journal = ErrorJournal::new(&path);
        let database = AppError::database("database write failed");
        let runtime = AppError::internal("runtime stopped unexpectedly");

        assert!(
            journal
                .append_at(
                    JournalIncident::from_app_error("repository.pg", &database),
                    10
                )
                .is_written()
        );
        assert!(
            journal
                .append_at(
                    JournalIncident::from_app_error("repository.pg", &database),
                    20
                )
                .is_written()
        );
        assert!(
            journal
                .append_at(JournalIncident::from_app_error("runtime", &runtime), 30)
                .is_written()
        );

        let first = journal.read_page(0, 1);
        assert_eq!(first.total, 2);
        assert_eq!(first.entries.len(), 1);
        assert_eq!(first.entries[0].component, "runtime");
        assert_eq!(first.entries[0].count, 1);
        assert_eq!(first.next_offset, Some(1));

        let second = journal.read_page(first.next_offset.expect("second page"), 1);
        assert_eq!(second.entries.len(), 1);
        assert_eq!(second.entries[0].component, "repository.pg");
        assert_eq!(second.entries[0].count, 2);
        assert_eq!(second.entries[0].first_seen_ms, 10);
        assert_eq!(second.entries[0].last_seen_ms, 20);
        assert_eq!(second.next_offset, None);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn page_size_is_hard_capped_even_for_an_unbounded_request() {
        let path = test_path("page-cap");
        let journal = ErrorJournal::with_limits(&path, 64 * 1024, MAX_PAGE_SIZE + 2);
        for index in 0..=MAX_PAGE_SIZE {
            assert!(
                journal
                    .append_at(
                        JournalIncident::from_app_error(
                            format!("runtime.{index}"),
                            &AppError::internal("failed"),
                        ),
                        index as u64,
                    )
                    .is_written()
            );
        }

        let page = journal.read_page(0, usize::MAX);
        assert_eq!(page.entries.len(), MAX_PAGE_SIZE);
        assert_eq!(page.next_offset, Some(MAX_PAGE_SIZE));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn journal_retains_only_the_newest_bounded_occurrences() {
        let path = test_path("bounded");
        let journal = ErrorJournal::with_limits(&path, 1_024, 2);

        for (timestamp, component) in [
            (1, "runtime.first"),
            (2, "runtime.second"),
            (3, "runtime.third"),
        ] {
            assert!(
                journal
                    .append_at(
                        JournalIncident::from_app_error(component, &AppError::internal("failed")),
                        timestamp,
                    )
                    .is_written()
            );
        }

        let page = journal.read_page(0, 10);
        assert_eq!(page.total, 2);
        assert_eq!(page.entries[0].component, "runtime.third");
        assert_eq!(page.entries[1].component, "runtime.second");
        assert!(fs::metadata(&path).expect("journal metadata").len() <= 1_024);
        assert!(
            !journal.backup_path().exists(),
            "a completed compaction must not leave a stale backup"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn interrupted_compaction_recovers_the_preserved_journal_before_reading() {
        let path = test_path("recovery");
        let journal = ErrorJournal::new(&path);
        assert!(
            journal
                .append_at(
                    JournalIncident::from_app_error("runtime", &AppError::internal("failed")),
                    42,
                )
                .is_written()
        );

        // Simulate a process kill after the old file moved to its backup but
        // before the synced compacted replacement was installed.
        let backup = journal.backup_path();
        fs::rename(&path, &backup).expect("move journal to recovery backup");
        assert!(!path.exists());
        assert!(backup.exists());

        let page = journal.read_page(0, 10);
        assert_eq!(page.total, 1);
        assert_eq!(page.entries[0].component, "runtime");
        assert!(path.exists(), "read restores the preserved journal");
        assert!(!backup.exists(), "successful recovery consumes the backup");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn unavailable_journal_is_skipped_without_propagating_an_error() {
        let directory = std::env::temp_dir().join(format!(
            "citadel-error-journal-directory-{}",
            NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&directory).expect("test directory should be created");
        let journal = ErrorJournal::new(&directory);

        assert_eq!(
            journal.append(JournalIncident::panic("startup")),
            JournalAppendOutcome::Skipped
        );
        let _ = fs::remove_dir(directory);
    }
}
