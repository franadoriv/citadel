//! The Citadel admin console ( shell,  live integration).
//!
//! A self-contained, single-page admin console served at
//! [`DASHBOARD_PATH`](super::DASHBOARD_PATH): a Nakama-Console-style shell
//! (top bar + left sidebar navigation + content area) themed to a navy-marine
//! palette, with **every sidebar section live** against the operator API.
//!
//! Design constraints (see the task and the console feature doc):
//!
//! - No build step, framework, or CDN. The page is plain HTML + CSS + vanilla
//!   JS embedded at compile time via [`include_str!`], so the binary stays
//!   fully self-contained and the console works with zero external fetches.
//! - The **Status** section polls public [`STATUS_PATH`] (no bearer token) for
//!   node facts and transports. Its host CPU, memory, and storage cards use an
//!   authenticated console API request, so capacity information is never
//!   exposed through the public status contract.
//! - Every other section (Accounts, Groups, Chat, Notifications, Storage,
//!   Database Explorer, Leaderboards, Matches, Purchases/Subscriptions, Configuration, API
//!   Explorer/Runtime, Audit Logs) drives its live `/console/v1/*` backend:
//!   bearer login via `POST /console/v1/login` (token kept in
//!   `sessionStorage`), role-aware mutation controls (`viewer` sees them
//!   disabled), tables with filter/paging, and create/edit/delete flows with
//!   inline confirmation (native dialogs are never used).
//!
//! [`STATUS_PATH`]: super::STATUS_PATH

use axum::extract::State;
use axum::response::Html;

use crate::app::App;

/// The complete console single-page app, embedded at compile time.
///
/// Kept as a `&'static str` so serving it is allocation-free and the asset is
/// baked into the binary rather than read from disk at runtime.
pub const CONSOLE_HTML: &str = include_str!("assets/console.html");

/// Section navigation labels rendered in the console sidebar.
///
/// Exposed so integration tests can assert the shell ships the expected
/// information architecture without duplicating the label strings.
pub const NAV_LABELS: &[&str] = &[
    "Status",
    "Accounts",
    "Groups",
    "Chat",
    "Notifications",
    "Storage",
    "Database Explorer",
    "Leaderboards",
    "Matches",
    "Purchases & Subscriptions",
    "Configuration",
    "API Explorer / Runtime",
    "Audit Logs",
    "Error Journal",
];

/// HTML console handler: serves the embedded single-page app.
///
/// The only side effect is bumping the `http_requests_total` counter so the
/// console's own traffic is reflected on its Status page.
pub(super) async fn console_handler(State(app): State<App>) -> Html<&'static str> {
    app.metrics().record_http_request();
    Html(CONSOLE_HTML)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_html_is_a_complete_document() {
        assert!(CONSOLE_HTML.starts_with("<!DOCTYPE html>"));
        assert!(CONSOLE_HTML.contains("Citadel <span>Console</span>"));
        assert!(CONSOLE_HTML.trim_end().ends_with("</html>"));
    }

    #[test]
    fn console_html_contains_every_nav_label() {
        for label in NAV_LABELS {
            assert!(
                CONSOLE_HTML.contains(label),
                "console is missing navigation label: {label}"
            );
        }
    }

    #[test]
    fn console_is_self_contained_no_external_assets() {
        // No external stylesheets/scripts/CDN fetches: the page must work
        // fully offline from a single served document.
        assert!(!CONSOLE_HTML.contains("http://"));
        assert!(!CONSOLE_HTML.contains("https://"));
        assert!(!CONSOLE_HTML.contains("cdn."));
    }

    #[test]
    fn status_section_reads_the_status_endpoint() {
        // Node facts stay on the public /status endpoint (no bearer).
        assert!(CONSOLE_HTML.contains("fetch('/status'"));
        // Host capacity telemetry is only requested with the console bearer.
        assert!(CONSOLE_HTML.contains("/console/v1/telemetry"));
        // Optional deferred-storage metrics returned by /status are rendered,
        // rather than only being visible in raw JSON.
        assert!(CONSOLE_HTML.contains("Deferred storage"));
        assert!(CONSOLE_HTML.contains("deferred.queued_items"));
        assert!(CONSOLE_HTML.contains("deferred.shutdown_abandoned_bytes"));
        // The rest of the SPA authenticates against the console API...
        assert!(CONSOLE_HTML.contains("/console/v1/login"));
        // ...and no placeholder affordance survives anywhere.
        assert!(!CONSOLE_HTML.contains("Not yet implemented"));
        assert!(!CONSOLE_HTML.contains("badge-nyi"));
    }
}
