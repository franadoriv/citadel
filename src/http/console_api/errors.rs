//! Console incident-journal section (`GET /console/v1/errors`).
//!
//! The response exposes only the redacted fields retained by the local JSONL
//! journal. Both console roles can read it; incident review is operationally
//! useful to viewers and does not mutate server state.

use axum::Json;
use axum::extract::{Query, State};
use serde::Deserialize;

use crate::app::App;
use crate::error_journal::JournalPage;
use crate::services::ConsolePrincipal;

/// The Error Journal section route.
pub const ERRORS_PATH: &str = "/console/v1/errors";

/// Accepted query parameters for [`ERRORS_PATH`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorsQuery {
    /// Zero-based incident offset, newest-first.
    pub offset: Option<usize>,
    /// Maximum entries to return; the journal applies its own hard cap.
    pub limit: Option<usize>,
}

/// `GET /console/v1/errors`: read retained incidents newest-first.
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Query(query): Query<ErrorsQuery>,
) -> Json<JournalPage> {
    app.metrics().record_http_request();
    Json(
        app.error_journal()
            .read_page(query.offset.unwrap_or(0), query.limit.unwrap_or(100)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_path_is_a_registered_console_section() {
        assert!(super::super::SECTION_PATHS.contains(&ERRORS_PATH));
    }
}
