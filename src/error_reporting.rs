//! Process-wide Sentry telemetry and local incident reporting.
//!
//! The local journal is always available and deliberately independent from
//! network telemetry. When `CITADEL_SENTRY_DSN` is configured, redacted
//! incident metadata is sent to Sentry. Bugsink is supported as a
//! Sentry-compatible self-hosted backend. Missing or unreachable telemetry
//! never blocks startup or changes the server's error handling.

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

/// Keeps the optional Sentry telemetry client alive until shutdown.
///
/// Dropping the guard flushes any queued telemetry events. The local journal is
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

    let external = sentry_dsn().map(|dsn| {
        let environment = std::env::var("CITADEL_ENVIRONMENT")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "production".to_string());
        tracing::info!(
            journal = %journal.path().display(),
            "Sentry telemetry initialized with local incident journal"
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
            "local incident journal initialized; Sentry telemetry disabled"
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
/// forward a redacted summary to Sentry when telemetry is configured.
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

fn sentry_dsn() -> Option<sentry::types::Dsn> {
    let sentry = std::env::var("CITADEL_SENTRY_DSN").ok();
    let legacy_bugsink = std::env::var("CITADEL_BUGSINK_DSN").ok();

    sentry_dsn_from_values(sentry, legacy_bugsink)
}

fn sentry_dsn_from_values(
    sentry: Option<String>,
    legacy_bugsink: Option<String>,
) -> Option<sentry::types::Dsn> {
    if !has_non_empty_value(sentry.as_ref()) && has_non_empty_value(legacy_bugsink.as_ref()) {
        tracing::warn!(
            "CITADEL_BUGSINK_DSN is a compatibility alias; configure CITADEL_SENTRY_DSN instead"
        );
    }

    let value = configured_sentry_dsn(sentry, legacy_bugsink)?;
    match value.parse() {
        Ok(dsn) => Some(dsn),
        Err(_) => {
            tracing::warn!("Sentry telemetry disabled: invalid configured DSN");
            None
        }
    }
}

fn configured_sentry_dsn(sentry: Option<String>, legacy_bugsink: Option<String>) -> Option<String> {
    sentry
        .filter(|value| !value.trim().is_empty())
        .or_else(|| legacy_bugsink.filter(|value| !value.trim().is_empty()))
}

fn has_non_empty_value(value: Option<&String>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
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

    const TEST_DSN: &str = "https://public@example.invalid/1";

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

    #[test]
    fn primary_sentry_dsn_takes_precedence_over_legacy_bugsink_alias() {
        assert_eq!(
            configured_sentry_dsn(Some("sentry".to_string()), Some("bugsink".to_string())),
            Some("sentry".to_string())
        );
    }

    #[test]
    fn primary_sentry_dsn_is_selected_when_no_legacy_alias_is_set() {
        assert_eq!(
            configured_sentry_dsn(Some("sentry".to_string()), None),
            Some("sentry".to_string())
        );
    }

    #[test]
    fn blank_primary_dsn_falls_back_to_legacy_bugsink_alias() {
        assert_eq!(
            configured_sentry_dsn(Some("  ".to_string()), Some("bugsink".to_string())),
            Some("bugsink".to_string())
        );
    }

    #[test]
    fn blank_or_absent_dsn_settings_disable_telemetry() {
        assert_eq!(configured_sentry_dsn(None, None), None);
        assert_eq!(
            configured_sentry_dsn(Some(" ".to_string()), Some("\t".to_string())),
            None
        );
    }

    #[test]
    fn invalid_primary_dsn_does_not_fall_back_to_legacy_alias() {
        assert_eq!(
            configured_sentry_dsn(
                Some("not-a-valid-dsn".to_string()),
                Some("https://bugsink.example/1".to_string())
            ),
            Some("not-a-valid-dsn".to_string())
        );
    }

    #[test]
    fn sentry_dsn_parser_accepts_the_primary_value() {
        assert!(sentry_dsn_from_values(Some(TEST_DSN.to_string()), None).is_some());
    }

    #[test]
    fn sentry_dsn_parser_accepts_the_legacy_bugsink_value() {
        assert!(sentry_dsn_from_values(None, Some(TEST_DSN.to_string())).is_some());
    }

    #[test]
    fn sentry_dsn_parser_does_not_fall_back_when_the_primary_value_is_invalid() {
        let dsn = sentry_dsn_from_values(Some("invalid".to_string()), Some(TEST_DSN.to_string()));

        assert!(dsn.is_none());
    }
}
