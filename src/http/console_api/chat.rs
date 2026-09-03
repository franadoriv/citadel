//! Console Chat section.
//!
//! Operator-scope access to the in-process
//! [`ChatService`](crate::services::ChatService): channel listing, paged
//! message history, a console-side message producer, and message moderation
//! (tombstone delete). This is a **history and moderation surface only** —
//! realtime chat delivery over the socket has not landed yet, so
//! `POST .../messages` is the console standing in as the producer until wire
//! delivery exists. History is in-process and a node restart clears it.
//!
//! - `GET /console/v1/chat` — every channel with its message count and last
//!   activity, most-recently-active first.
//! - `POST /console/v1/chat/{channel}/messages` — append a message (admin,
//!   audited `chat.message.append`); creates the channel on first use.
//! - `GET /console/v1/chat/{channel}/messages` — paged history, newest first.
//! - `DELETE /console/v1/chat/{channel}/messages/{id}` — tombstone a message
//!   (admin, audited `chat.message.delete`).

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::services::{AuditEntry, ChannelType, ChatMessage, ConsolePrincipal};
use crate::time::{Clock, SystemClock};

/// The Chat section route (channel listing).
pub const CHAT_PATH: &str = "/console/v1/chat";

/// Per-channel message collection route pattern (`GET`/`POST`).
pub const CHAT_MESSAGES_PATH: &str = "/console/v1/chat/:channel/messages";

/// Single-message route pattern (`DELETE`).
pub const CHAT_MESSAGE_PATH: &str = "/console/v1/chat/:channel/messages/:id";

/// Default channel-listing page size.
const DEFAULT_CHANNELS_LIMIT: usize = 100;
/// Hard ceiling on one channel-listing page.
const MAX_CHANNELS_LIMIT: usize = 500;

/// Default message-history page size.
const DEFAULT_MESSAGES_LIMIT: usize = 50;
/// Hard ceiling on one message-history page.
const MAX_MESSAGES_LIMIT: usize = 200;

/// Query parameters for [`CHAT_PATH`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelsQuery {
    /// Case-sensitive substring filter over the channel id.
    pub filter: Option<String>,
    /// Maximum channels returned (default 100, capped at 500).
    pub limit: Option<usize>,
}

/// One channel row in the listing.
#[derive(Debug, Clone, Serialize)]
pub struct ChannelRow {
    /// The channel id.
    pub channel: String,
    /// The channel's type (`room`, `group`, or `direct`).
    pub channel_type: &'static str,
    /// Total messages ever appended (not reduced by eviction/tombstoning).
    pub messages: u64,
    /// The most recent append's time (unix millis).
    pub last_activity_unix_ms: u64,
}

impl From<crate::services::ChannelSummary> for ChannelRow {
    fn from(summary: crate::services::ChannelSummary) -> Self {
        Self {
            channel: summary.channel,
            channel_type: summary.channel_type,
            messages: summary.messages,
            last_activity_unix_ms: summary.last_activity_unix_ms,
        }
    }
}

/// The JSON response for [`CHAT_PATH`].
#[derive(Debug, Clone, Serialize)]
pub struct ChannelsResponse {
    /// Matching channels, most-recently-active first.
    pub items: Vec<ChannelRow>,
    /// Total channel count before filtering/limiting.
    pub total: usize,
}

/// Query parameters for the message-history `GET`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessagesQuery {
    /// Maximum messages returned (default 50, capped at 200).
    pub limit: Option<usize>,
    /// Resume a page: only messages with `id < before` are returned.
    pub before: Option<u64>,
}

/// The JSON body accepted by the message `POST` (the console producer).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendBody {
    /// The sending identity, as presented by the operator.
    pub sender: String,
    /// The message body.
    pub content: String,
    /// The channel type to create with, if the channel does not exist yet.
    /// Ignored (and not required) when the channel already exists. Defaults
    /// to `room`.
    #[serde(default)]
    pub channel_type: Option<String>,
}

/// One message row, shared by the append response and history pages.
#[derive(Debug, Clone, Serialize)]
pub struct MessageRow {
    /// Per-channel sequential id.
    pub id: u64,
    /// The sending identity.
    pub sender: String,
    /// The message body; empty once tombstoned.
    pub content: String,
    /// When the message was appended (unix millis).
    pub created_at_unix_ms: u64,
    /// When the message state was last changed (unix millis).
    pub updated_at_unix_ms: u64,
    /// Monotonic state revision; creation starts at one.
    pub revision: u64,
    /// Per-channel event watermark for this message state.
    pub last_event_id: u64,
    /// Whether the message has been moderated away.
    pub deleted: bool,
}

impl From<ChatMessage> for MessageRow {
    fn from(message: ChatMessage) -> Self {
        Self {
            id: message.id,
            sender: message.sender,
            content: message.content,
            created_at_unix_ms: message.created_at_unix_ms,
            updated_at_unix_ms: message.updated_at_unix_ms,
            revision: message.revision,
            last_event_id: message.last_event_id,
            deleted: message.deleted,
        }
    }
}

/// The JSON response for the message-history `GET`.
#[derive(Debug, Clone, Serialize)]
pub struct MessagesPage {
    /// The channel this page belongs to.
    pub channel: String,
    /// Messages, newest first.
    pub items: Vec<MessageRow>,
    /// Cursor for the next page (pass as `before`), present when the page was
    /// full and more (older) messages may remain.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<u64>,
}

/// `GET /console/v1/chat`: every channel with its message count and last
/// activity, most-recently-active first. Answers `200` with no query params.
pub(super) async fn channels_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Query(query): Query<ChannelsQuery>,
) -> Result<Json<ChannelsResponse>, ApiError> {
    app.metrics().record_http_request();
    let chat = app.chat();
    let limit = query
        .limit
        .unwrap_or(DEFAULT_CHANNELS_LIMIT)
        .clamp(1, MAX_CHANNELS_LIMIT);
    let items = chat
        .channels(query.filter.as_deref(), limit)
        .await?
        .into_iter()
        .map(ChannelRow::from)
        .collect();
    Ok(Json(ChannelsResponse {
        items,
        total: chat.channel_count().await?,
    }))
}

/// `POST /console/v1/chat/{channel}/messages`: append a message (admin,
/// audited). Creates the channel on first use.
pub(super) async fn append_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path(channel): Path<String>,
    body: Result<Json<AppendBody>, JsonRejection>,
) -> Result<(StatusCode, Json<MessageRow>), ApiError> {
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
    let channel_type = match body.channel_type.as_deref() {
        Some(token) => ChannelType::parse(token)?,
        None => ChannelType::Room,
    };
    let now = SystemClock.now();
    let id = app
        .chat()
        .append(
            &channel,
            channel_type,
            body.sender.clone(),
            body.content.clone(),
            now,
        )
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "chat.message.append",
        format!("{channel}#{id}"),
        format!("appended message from {}", body.sender),
    ));
    let message = app
        .chat()
        .messages(&channel, 0, None)
        .await?
        .into_iter()
        .find(|message| message.id == id)
        .ok_or_else(|| AppError::internal("appended chat message was not retained"))?;
    Ok((StatusCode::OK, Json(MessageRow::from(message))))
}

/// `GET /console/v1/chat/{channel}/messages`: paged history, newest first.
pub(super) async fn messages_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Path(channel): Path<String>,
    Query(query): Query<MessagesQuery>,
) -> Result<Json<MessagesPage>, ApiError> {
    app.metrics().record_http_request();
    let limit = query
        .limit
        .unwrap_or(DEFAULT_MESSAGES_LIMIT)
        .clamp(1, MAX_MESSAGES_LIMIT);
    let items: Vec<MessageRow> = app
        .chat()
        .messages(&channel, limit, query.before)
        .await?
        .into_iter()
        .map(MessageRow::from)
        .collect();
    let next = (items.len() == limit)
        .then(|| items.last().map(|row| row.id))
        .flatten();
    Ok(Json(MessagesPage {
        channel,
        items,
        next,
    }))
}

/// `DELETE /console/v1/chat/{channel}/messages/{id}`: tombstone a message
/// (admin, audited). Idempotent; unknown channel or id is `404`.
pub(super) async fn delete_message_handler(
    State(app): State<App>,
    operator: ConsolePrincipal,
    Path((channel, id)): Path<(String, u64)>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let now = SystemClock.now();
    app.chat()
        .consume_rate_limits(
            &app.chat_rate_limits()
                .moderation(&operator.actor_id(), &channel),
            now,
        )
        .await?;
    app.chat()
        .moderate_delete_message(
            &channel,
            id,
            "operator",
            &operator.actor_id(),
            "operator_remove",
            0,
            "",
            &app.config().server.node_id,
            now,
        )
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.actor_id(),
        operator.role_label(),
        "chat.message.delete",
        format!("{channel}#{id}"),
        "deleted (tombstoned) message",
    ));
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_paths_are_registered_and_nested_correctly() {
        assert!(super::super::SECTION_PATHS.contains(&CHAT_PATH));
        assert!(CHAT_MESSAGES_PATH.starts_with(CHAT_PATH));
        assert!(CHAT_MESSAGE_PATH.starts_with(CHAT_MESSAGES_PATH.trim_end_matches("messages")));
    }

    #[test]
    fn append_body_defaults_channel_type_to_none_and_rejects_unknown_fields() {
        let body: AppendBody =
            serde_json::from_str(r#"{"sender":"alice","content":"hi"}"#).expect("parse");
        assert_eq!(body.sender, "alice");
        assert!(body.channel_type.is_none());
        assert!(
            serde_json::from_str::<AppendBody>(r#"{"sender":"a","content":"b","extra":1}"#)
                .is_err()
        );
    }

    #[test]
    fn channel_row_maps_from_service_summary() {
        let summary = crate::services::ChannelSummary {
            channel: "lobby".to_string(),
            channel_type: "room",
            messages: 3,
            last_activity_unix_ms: 42,
        };
        let row = ChannelRow::from(summary);
        assert_eq!(row.channel, "lobby");
        assert_eq!(row.channel_type, "room");
        assert_eq!(row.messages, 3);
        assert_eq!(row.last_activity_unix_ms, 42);
    }
}
