//! Process-wide incident reporting.
//!
//! The local journal is always available and deliberately independent from
//! network reporting. When `CITADEL_BUGSINK_DSN` is configured, the same
//! redacted incident summaries are also sent through the Sentry-compatible
//! protocol. Missing or unreachable external reporting never blocks startup or
//! changes the server's error handling.

use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::{Arc, Once, OnceLock, RwLock};

use sentry::{ClientInitGuard, ClientOptions, Level};

use crate::VERSION;
use crate::config::ErrorJournalConfig;
use crate::error::AppError;
use crate::error_journal::{
    DEFAULT_JOURNAL_FILE_NAME, ErrorJournal, JournalIncident, journal_path_for_executable,
};

static JOURNAL: OnceLock<RwLock<Arc<ErrorJournal>>> = OnceLock::new();
static PANIC_HOOK: Once = Once::new();

/// Keeps the optional external-reporting client alive until shutdown.
///
/// Dropping the guard flushes any queued external events. The local journal is
/// process-wide and remains available even when this guard carries no client.
pub struct ReportingGuard {
    _external: Option<ClientInitGuard>,
}

/// Initialize process-wide incident reporting after configuration and logging
/// are available.
///
/// This operation is intentionally infallible: a journal write failure is
/// never allowed to prevent the game server from starting. The default journal
/// path is `citadel-errors.jsonl` in the executable's directory.
#[must_use]
pub fn initialize(config: &ErrorJournalConfig) -> ReportingGuard {
    let journal = Arc::new(new_local_journal(config));
    replace_journal(Arc::clone(&journal));
    install_panic_hook();

    let external = bugsink_dsn().map(|dsn| {
        let environment = std::env::var("CITADEL_ENVIRONMENT")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "production".to_string());
        tracing::info!(
            journal = %journal.path().display(),
            "incident journal initialized with external reporting enabled"
        );
        sentry::init(ClientOptions {
            dsn: Some(dsn),
            release: Some(Cow::Borrowed(VERSION)),
            environment: Some(Cow::Owned(environment)),
            send_default_pii: false,
            ..ClientOptions::default()
        })
    });

    if external.is_none() {
        tracing::info!(
            journal = %journal.path().display(),
            "incident journal initialized; external reporting disabled"
        );
    }

    ReportingGuard {
        _external: external,
    }
}

/// Install local panic capture before configuration resolution.
///
/// The initial journal uses built-in retention defaults and is replaced with
/// the resolved `[errors]` configuration in [`initialize`]. This means a
/// panic while loading configuration, initializing logging, or running the
/// first-run wizard is still recorded beside the executable.
pub fn install_early_panic_capture() {
    let _ = active_journal();
    install_panic_hook();
}

/// Return the active process journal, if startup reporting has been initialized.
#[must_use]
pub fn journal() -> Arc<ErrorJournal> {
    active_journal()
}

/// Construct a journal for an [`App`](crate::App) assembled outside the normal
/// process startup path (primarily tests). Production startup installs the
/// process-wide instance before assembling the app.
#[must_use]
pub fn journal_for_config(config: &ErrorJournalConfig) -> Arc<ErrorJournal> {
    if JOURNAL.get().is_none() {
        let journal = Arc::new(new_local_journal(config));
        replace_journal(Arc::clone(&journal));
        journal
    } else {
        active_journal()
    }
}

/// Record an application failure in the local journal and, when configured,
/// forward a redacted summary to the external service.
pub fn report_app_error(component: &str, error: &AppError) {
    let component = safe_component(component);
    let _ = active_journal().append(JournalIncident::from_app_error(&component, error));
    sentry::with_scope(
        |scope| {
            scope.set_tag("component", component);
            scope.set_tag("category", error.category().code());
        },
        || {
            sentry::capture_message(&safe_error_message(error.category().code()), Level::Error);
        },
    );
}

/// Record an unexpected process panic before delegating to the previous panic
/// hook. The panic payload is deliberately ignored because it can include
/// request data or credentials.
pub fn report_panic(component: &str) {
    let component = safe_component(component);
    let _ = active_journal().append(JournalIncident::panic(&component));
    sentry::with_scope(
        |scope| {
            scope.set_tag("component", component);
            scope.set_tag("incident_kind", "panic");
        },
        || {
            sentry::capture_message("process panic", Level::Fatal);
        },
    );
}

fn new_local_journal(config: &ErrorJournalConfig) -> ErrorJournal {
    let executable =
        std::env::current_exe().unwrap_or_else(|_| PathBuf::from(DEFAULT_JOURNAL_FILE_NAME));
    let path = journal_path_for_executable(&executable);
    let max_bytes = usize::try_from(config.max_bytes).unwrap_or(usize::MAX);
    ErrorJournal::with_limits(path, max_bytes, config.max_entries)
}

fn active_journal() -> Arc<ErrorJournal> {
    let slot = JOURNAL.get_or_init(|| {
        let config = ErrorJournalConfig::default();
        RwLock::new(Arc::new(new_local_journal(&config)))
    });
    match slot.read() {
        Ok(journal) => Arc::clone(&journal),
        Err(poisoned) => Arc::clone(&poisoned.into_inner()),
    }
}

fn replace_journal(journal: Arc<ErrorJournal>) {
    let slot = JOURNAL.get_or_init(|| RwLock::new(Arc::clone(&journal)));
    match slot.write() {
        Ok(mut active) => *active = journal,
        Err(poisoned) => *poisoned.into_inner() = journal,
    }
}

fn install_panic_hook() {
    PANIC_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            report_panic("process");
            previous(info);
        }));
    });
}

fn bugsink_dsn() -> Option<sentry::types::Dsn> {
    let value = std::env::var("CITADEL_BUGSINK_DSN")
        .ok()
        .filter(|value| !value.trim().is_empty())?;
    match value.parse() {
        Ok(dsn) => Some(dsn),
        Err(_) => {
            tracing::warn!("external incident reporting disabled: invalid configured DSN");
            None
        }
    }
}

fn safe_error_message(category: &str) -> String {
    format!("{category} failure")
}

fn safe_component(component: &str) -> String {
    let value: String = component
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '.' => character,
            _ => '_',
        })
        .take(96)
        .collect();
    if value.is_empty() {
        "unknown".to_string()
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_event_messages_do_not_include_error_details() {
        let error =
            AppError::internal("request payload leaked").with_detail("token=very-secret-value");
        assert_eq!(
            safe_error_message(error.category().code()),
            "internal failure"
        );
    }

    #[test]
    fn external_component_tags_are_bounded_and_sanitized() {
        assert_eq!(
            safe_component("http/request?token=secret"),
            "http_request_token_secret"
        );
        assert_eq!(safe_component(""), "unknown");
    }
}
