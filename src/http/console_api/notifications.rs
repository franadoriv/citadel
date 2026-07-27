//! Console Notifications section.
//!
//! A targeting-or-broadcast composer over the in-process
//! [`NotificationService`](crate::services::NotificationService):
//!
//! - `GET /console/v1/notifications` — newest-first page of notifications
//!   (any role). An optional `user_id` narrows to that user's own targeted
//!   notifications plus every broadcast; absent lists everything (the
//!   operator-wide view).
//! - `POST /console/v1/notifications` — send a notification (admin, audited).
//!   Omitting `user_id` sends a broadcast.
//! - `DELETE /console/v1/notifications/{id}` — delete one (admin, audited).
//!
//! The store is now persisted behind the repository seam, so
//! notifications survive a node restart on the Postgres and SQLite backends (the
//! in-memory backend stays non-durable by design). Realtime push delivery
//! (deliver-if-online over the session/routing seam) remains out of scope.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::services::{AuditEntry, ConsoleIdentity, Notification, Recipient};
use crate::time::{Clock, SystemClock};

/// The Notifications section route (list + send).
pub const NOTIFICATIONS_PATH: &str = "/console/v1/notifications";

/// Single-notification route pattern (delete).
pub const NOTIFICATION_ID_PATH: &str = "/console/v1/notifications/:id";

/// Default page size when `limit` is absent.
const DEFAULT_LIMIT: usize = 50;
/// Hard ceiling on one page, independent of the requested `limit`.
const MAX_LIMIT: usize = 200;

/// Accepted query parameters for [`NOTIFICATIONS_PATH`] `GET`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListParams {
    /// Restrict to one recipient's own targeted notifications plus every
    /// broadcast; absent lists everything.
    pub user_id: Option<String>,
    /// Page size (default 50, capped at 200).
    pub limit: Option<usize>,
    /// Resume cursor: only notifications strictly older than this id.
    pub before: Option<u64>,
}

/// One notification row, as returned by list/send.
#[derive(Debug, Clone, Serialize)]
pub struct NotificationRow {
    /// Server-assigned id.
    pub id: u64,
    /// Targeted recipient's user id, or `null` for a broadcast.
    pub user_id: Option<String>,
    /// Subject line.
    pub subject: String,
    /// Arbitrary JSON payload (always a JSON object).
    pub content: serde_json::Value,
    /// Application-defined status/kind code.
    pub code: i32,
    /// When it was sent (unix milliseconds).
    pub created_at_unix_ms: u64,
    /// Whether the recipient has marked it read.
    pub read: bool,
}

impl NotificationRow {
    fn from_notification(n: Notification) -> Self {
        Self {
            id: n.id,
            user_id: n.recipient.user_id().map(str::to_string),
            subject: n.subject,
            content: n.content,
            code: n.code,
            created_at_unix_ms: n.created_at.unix_millis(),
            read: n.read,
        }
    }
}

/// The JSON response for the list route.
#[derive(Debug, Clone, Serialize)]
pub struct NotificationsPage {
    /// Page items, newest first.
    pub items: Vec<NotificationRow>,
    /// Total notifications visible to the requested filter (ignores paging).
    pub total: usize,
}

/// The JSON body accepted by the send route.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendBody {
    /// Target user id; absent sends a broadcast to every account.
    pub user_id: Option<String>,
    /// Subject line (must be non-empty).
    pub subject: String,
    /// Arbitrary JSON object payload (default `{}`).
    #[serde(default = "default_content")]
    pub content: serde_json::Value,
    /// Application-defined status/kind code (default `0`).
    #[serde(default)]
    pub code: i32,
}

fn default_content() -> serde_json::Value {
    serde_json::json!({})
}

/// Render a recipient for audit targets.
fn recipient_target(user_id: Option<&str>) -> String {
    match user_id {
        Some(id) => format!("user {id}"),
        None => "broadcast".to_string(),
    }
}

/// `GET /console/v1/notifications`: newest-first page (any role).
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
    Query(params): Query<ListParams>,
) -> Result<Json<NotificationsPage>, ApiError> {
    app.metrics().record_http_request();
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let page = app
        .notifications()
        .list(params.user_id.as_deref(), limit, params.before)
        .await?;
    Ok(Json(NotificationsPage {
        items: page
            .items
            .into_iter()
            .map(NotificationRow::from_notification)
            .collect(),
        total: page.total,
    }))
}

/// `POST /console/v1/notifications`: send targeted or broadcast (admin).
pub(super) async fn send_handler(
    State(app): State<App>,
    operator: ConsoleIdentity,
    body: Result<Json<SendBody>, JsonRejection>,
) -> Result<(StatusCode, Json<NotificationRow>), ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let body = match body {
        Ok(Json(body)) => body,
        Err(rejection) => {
            return Err(AppError::validation("invalid request body")
                .with_detail(rejection.body_text())
                .into());
        }
    };
    let recipient = match body.user_id.clone() {
        Some(id) => Recipient::User(id),
        None => Recipient::Broadcast,
    };
    let now = SystemClock.now();
    let id = app
        .notifications()
        .send(
            recipient,
            body.subject.clone(),
            body.content.clone(),
            body.code,
            now,
        )
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.username,
        operator.role.as_str(),
        "notifications.send",
        recipient_target(body.user_id.as_deref()),
        format!("sent notification {id} ({})", body.subject),
    ));
    Ok((
        StatusCode::CREATED,
        Json(NotificationRow {
            id,
            user_id: body.user_id,
            subject: body.subject,
            content: body.content,
            code: body.code,
            created_at_unix_ms: now.unix_millis(),
            read: false,
        }),
    ))
}

/// `DELETE /console/v1/notifications/{id}`: delete (admin).
pub(super) async fn delete_handler(
    State(app): State<App>,
    operator: ConsoleIdentity,
    Path(id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    app.notifications().delete(id).await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.username,
        operator.role.as_str(),
        "notifications.delete",
        id.to_string(),
        "deleted notification",
    ));
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recipient_target_names_user_or_broadcast() {
        assert_eq!(recipient_target(Some("u-1")), "user u-1");
        assert_eq!(recipient_target(None), "broadcast");
    }

    #[test]
    fn send_body_defaults_content_and_code_and_rejects_unknown_fields() {
        let body: SendBody = serde_json::from_str(r#"{"subject":"hi"}"#).expect("parse");
        assert_eq!(body.content, serde_json::json!({}));
        assert_eq!(body.code, 0);
        assert!(body.user_id.is_none());
        assert!(
            serde_json::from_str::<SendBody>(r#"{"subject":"hi","extra":1}"#).is_err(),
            "unknown fields rejected"
        );
    }

    #[test]
    fn notifications_paths_are_registered_sections() {
        assert!(super::super::SECTION_PATHS.contains(&NOTIFICATIONS_PATH));
        assert!(NOTIFICATION_ID_PATH.starts_with(NOTIFICATIONS_PATH));
    }
}
