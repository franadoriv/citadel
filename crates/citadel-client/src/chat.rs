//! Typed client-side contract for `KIND_CHAT_EVENT` (28).
//!
//! Citadel deliberately delivers durable chat events at least once. A
//! [`ChatEventCursor`] belongs to one joined channel and provides bounded,
//! caller-owned duplicate/gap detection without hiding network polling or
//! retaining a global channel map.

use std::sync::atomic::{AtomicU64, Ordering};

use citadel_wire::{Envelope, protocol::KIND_CHAT_EVENT};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::chat_rpc::{ChatHistoryOptions, ChatRequestError, ChatRpcRequest, ChatTarget};

static NEXT_CHAT_CURSOR_ID: AtomicU64 = AtomicU64::new(1);

fn next_chat_cursor_id() -> Result<u64, ChatEventError> {
    NEXT_CHAT_CURSOR_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| ChatEventError::CursorIdentityExhausted)
}

const fn expected_channel_type(target: &ChatTarget) -> &'static str {
    match target {
        ChatTarget::CurrentRoom => "room",
        ChatTarget::Group { .. } => "group",
        ChatTarget::Direct { .. } => "direct",
    }
}

/// Closed set of version-1 chat event variants emitted by Citadel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatEventKind {
    PresenceJoin,
    PresenceLeave,
    Typing,
    MessageCreate,
    MessageUpdate,
    MessageRemove,
    AccessRevoked,
    ResyncRequired,
}

/// Identity attached to presence, typing, and access-revocation events.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatPresence {
    pub presence_id: String,
    pub user_id: String,
}

/// Exact durable message state serialized by Citadel's chat repository.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatMessage {
    pub id: u64,
    pub sender: String,
    pub content: String,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    pub revision: u64,
    pub last_event_id: u64,
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatJoinResult {
    pub channel_id: String,
    pub channel_type: String,
    pub presence: Vec<ChatPresence>,
    pub watermark_event_id: u64,
    pub subscription: String,
}

impl ChatJoinResult {
    pub fn decode(body: &[u8]) -> Result<Self, ChatEventError> {
        let value = decode_json_object(body)?;
        let channel_id = non_empty_string(&value, "channel_id")?.to_owned();
        let channel_type = non_empty_string(&value, "channel_type")?.to_owned();
        if !matches!(channel_type.as_str(), "direct" | "group" | "room") {
            return Err(ChatEventError::InvalidField("channel_type"));
        }
        let presence = value
            .get("presence")
            .and_then(Value::as_array)
            .ok_or(ChatEventError::MissingField("presence"))?
            .iter()
            .map(decode_presence_value)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            channel_id,
            channel_type,
            presence,
            watermark_event_id: value
                .get("watermark_event_id")
                .and_then(Value::as_u64)
                .ok_or(ChatEventError::MissingField("watermark_event_id"))?,
            subscription: non_empty_string(&value, "subscription")?.to_owned(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatHistoryResult {
    pub items: Vec<ChatMessage>,
    pub watermark_event_id: u64,
}

impl ChatHistoryResult {
    pub fn decode(body: &[u8]) -> Result<Self, ChatEventError> {
        let (items, watermark_event_id) = decode_history_response(body)?;
        Ok(Self {
            items,
            watermark_event_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatMutationResult {
    pub message: ChatMessage,
    pub event_id: u64,
}

impl ChatMutationResult {
    pub fn decode(body: &[u8]) -> Result<Self, ChatEventError> {
        let value = decode_json_object(body)?;
        let event_id = positive_u64(&value, "event_id")?;
        let message = value
            .get("message")
            .ok_or(ChatEventError::MissingField("message"))
            .and_then(decode_message_value)?;
        if message.last_event_id != event_id {
            return Err(ChatEventError::InvalidField("message"));
        }
        Ok(Self { message, event_id })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatRemoveResult {
    pub message_id: u64,
    pub deleted: bool,
    pub event_id: Option<u64>,
}

impl ChatRemoveResult {
    pub fn decode(body: &[u8]) -> Result<Self, ChatEventError> {
        let value = decode_json_object(body)?;
        let message_id = positive_u64(&value, "message_id")?;
        let deleted = value
            .get("deleted")
            .and_then(Value::as_bool)
            .ok_or(ChatEventError::MissingField("deleted"))?;
        let event_id = match value.get("event_id") {
            None | Some(Value::Null) => None,
            Some(value) => Some(
                value
                    .as_u64()
                    .filter(|event_id| *event_id > 0)
                    .ok_or(ChatEventError::InvalidField("event_id"))?,
            ),
        };
        if deleted != event_id.is_some() {
            return Err(ChatEventError::InvalidField("event_id"));
        }
        Ok(Self {
            message_id,
            deleted,
            event_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChatTypingResult {
    pub typing: bool,
    pub expires_at: u64,
}

impl ChatTypingResult {
    pub fn decode(body: &[u8]) -> Result<Self, ChatEventError> {
        let value = decode_json_object(body)?;
        Ok(Self {
            typing: value
                .get("typing")
                .and_then(Value::as_bool)
                .ok_or(ChatEventError::MissingField("typing"))?,
            expires_at: value
                .get("expires_at")
                .and_then(Value::as_u64)
                .ok_or(ChatEventError::MissingField("expires_at"))?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChatLeaveResult {
    pub left: bool,
}

impl ChatLeaveResult {
    pub fn decode(body: &[u8]) -> Result<Self, ChatEventError> {
        let value = decode_json_object(body)?;
        Ok(Self {
            left: value
                .get("left")
                .and_then(Value::as_bool)
                .ok_or(ChatEventError::MissingField("left"))?,
        })
    }
}

impl ChatEventKind {
    /// Canonical JSON value of the event's `type` field.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PresenceJoin => "presence.join",
            Self::PresenceLeave => "presence.leave",
            Self::Typing => "typing",
            Self::MessageCreate => "message.create",
            Self::MessageUpdate => "message.update",
            Self::MessageRemove => "message.remove",
            Self::AccessRevoked => "access.revoked",
            Self::ResyncRequired => "resync_required",
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum ChatEventWire {
    #[serde(rename = "presence.join")]
    PresenceJoin {
        version: u64,
        channel_id: String,
        channel_type: String,
        presence: ChatPresence,
    },
    #[serde(rename = "presence.leave")]
    PresenceLeave {
        version: u64,
        channel_id: String,
        presence: ChatPresence,
    },
    #[serde(rename = "typing")]
    Typing {
        version: u64,
        channel_id: String,
        presence: ChatPresence,
        typing: bool,
        expires_at: u64,
    },
    #[serde(rename = "message.create")]
    MessageCreate {
        version: u64,
        channel_id: String,
        event_id: u64,
        message: ChatMessage,
    },
    #[serde(rename = "message.update")]
    MessageUpdate {
        version: u64,
        channel_id: String,
        event_id: u64,
        message: ChatMessage,
    },
    #[serde(rename = "message.remove")]
    MessageRemove {
        version: u64,
        channel_id: String,
        event_id: u64,
        message: ChatMessage,
    },
    #[serde(rename = "access.revoked")]
    AccessRevoked {
        version: u64,
        channel_id: String,
        presence: ChatPresence,
    },
    #[serde(rename = "resync_required")]
    ResyncRequired {
        version: u64,
        channel_id: String,
        watermark_event_id: u64,
        #[serde(default)]
        scopes: Option<Vec<String>>,
    },
}

/// A validated version-1 chat event. Every variant has a closed schema;
/// unknown or duplicate fields fail closed before typed authority is created.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatEvent {
    kind: ChatEventKind,
    channel_id: String,
    event_id: Option<u64>,
    watermark_event_id: Option<u64>,
    expires_at: Option<u64>,
    typing: Option<bool>,
    presence: Option<ChatPresence>,
    message: Option<ChatMessage>,
    raw: Value,
}

impl ChatEvent {
    /// Decode and validate a UTF-8 JSON body from a kind-28 envelope.
    pub fn decode(body: &[u8]) -> Result<Self, ChatEventError> {
        let wire: ChatEventWire =
            serde_json::from_slice(body).map_err(|_| ChatEventError::InvalidJson)?;
        let raw: Value = serde_json::from_slice(body).map_err(|_| ChatEventError::InvalidJson)?;
        let (
            kind,
            version,
            channel_id,
            event_id,
            watermark_event_id,
            expires_at,
            typing,
            presence,
            message,
        ) = match wire {
            ChatEventWire::PresenceJoin {
                version,
                channel_id,
                channel_type,
                presence,
            } => {
                if !matches!(channel_type.as_str(), "direct" | "group" | "room") {
                    return Err(ChatEventError::InvalidField("channel_type"));
                }
                validate_presence(&presence)?;
                (
                    ChatEventKind::PresenceJoin,
                    version,
                    channel_id,
                    None,
                    None,
                    None,
                    None,
                    Some(presence),
                    None,
                )
            }
            ChatEventWire::PresenceLeave {
                version,
                channel_id,
                presence,
            } => {
                validate_presence(&presence)?;
                (
                    ChatEventKind::PresenceLeave,
                    version,
                    channel_id,
                    None,
                    None,
                    None,
                    None,
                    Some(presence),
                    None,
                )
            }
            ChatEventWire::Typing {
                version,
                channel_id,
                presence,
                typing,
                expires_at,
            } => {
                validate_presence(&presence)?;
                if !typing && expires_at != 0 {
                    return Err(ChatEventError::InvalidField("expires_at"));
                }
                (
                    ChatEventKind::Typing,
                    version,
                    channel_id,
                    None,
                    None,
                    Some(expires_at),
                    Some(typing),
                    Some(presence),
                    None,
                )
            }
            ChatEventWire::MessageCreate {
                version,
                channel_id,
                event_id,
                message,
            } => {
                validate_wire_message(&message, ChatEventKind::MessageCreate, event_id)?;
                (
                    ChatEventKind::MessageCreate,
                    version,
                    channel_id,
                    Some(event_id),
                    None,
                    None,
                    None,
                    None,
                    Some(message),
                )
            }
            ChatEventWire::MessageUpdate {
                version,
                channel_id,
                event_id,
                message,
            } => {
                validate_wire_message(&message, ChatEventKind::MessageUpdate, event_id)?;
                (
                    ChatEventKind::MessageUpdate,
                    version,
                    channel_id,
                    Some(event_id),
                    None,
                    None,
                    None,
                    None,
                    Some(message),
                )
            }
            ChatEventWire::MessageRemove {
                version,
                channel_id,
                event_id,
                message,
            } => {
                validate_wire_message(&message, ChatEventKind::MessageRemove, event_id)?;
                (
                    ChatEventKind::MessageRemove,
                    version,
                    channel_id,
                    Some(event_id),
                    None,
                    None,
                    None,
                    None,
                    Some(message),
                )
            }
            ChatEventWire::AccessRevoked {
                version,
                channel_id,
                presence,
            } => {
                validate_presence(&presence)?;
                (
                    ChatEventKind::AccessRevoked,
                    version,
                    channel_id,
                    None,
                    None,
                    None,
                    None,
                    Some(presence),
                    None,
                )
            }
            ChatEventWire::ResyncRequired {
                version,
                channel_id,
                watermark_event_id,
                scopes,
            } => {
                if let Some(scopes) = scopes {
                    let mut unique = std::collections::HashSet::with_capacity(scopes.len());
                    if scopes
                        .iter()
                        .any(|scope| scope.trim().is_empty() || !unique.insert(scope))
                    {
                        return Err(ChatEventError::InvalidField("scopes"));
                    }
                }
                (
                    ChatEventKind::ResyncRequired,
                    version,
                    channel_id,
                    None,
                    Some(watermark_event_id),
                    None,
                    None,
                    None,
                    None,
                )
            }
        };

        if version != 1 {
            return Err(ChatEventError::UnsupportedVersion(version));
        }
        if channel_id.trim().is_empty() {
            return Err(ChatEventError::InvalidField("channel_id"));
        }

        Ok(Self {
            kind,
            channel_id,
            event_id,
            watermark_event_id,
            expires_at,
            typing,
            presence,
            message,
            raw,
        })
    }

    /// Decode only a canonical kind-28 envelope.
    pub fn from_envelope(envelope: &Envelope) -> Result<Self, ChatEventError> {
        if envelope.kind != KIND_CHAT_EVENT {
            return Err(ChatEventError::WrongEnvelopeKind(envelope.kind));
        }
        Self::decode(&envelope.body)
    }

    pub const fn version(&self) -> u8 {
        1
    }

    pub const fn kind(&self) -> ChatEventKind {
        self.kind
    }

    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    pub const fn event_id(&self) -> Option<u64> {
        self.event_id
    }

    pub const fn watermark_event_id(&self) -> Option<u64> {
        self.watermark_event_id
    }

    pub const fn expires_at(&self) -> Option<u64> {
        self.expires_at
    }

    pub const fn typing(&self) -> Option<bool> {
        self.typing
    }

    /// Whether this event currently represents an active typing indication.
    pub const fn typing_active_at(&self, now_unix_ms: u64) -> bool {
        matches!((self.typing, self.expires_at), (Some(true), Some(expiry)) if now_unix_ms < expiry)
    }

    pub const fn presence(&self) -> Option<&ChatPresence> {
        self.presence.as_ref()
    }

    pub const fn message(&self) -> Option<&ChatMessage> {
        self.message.as_ref()
    }

    pub const fn raw(&self) -> &Value {
        &self.raw
    }
}

fn non_empty_string<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, ChatEventError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(ChatEventError::MissingField(field))
}

fn positive_u64(object: &Map<String, Value>, field: &'static str) -> Result<u64, ChatEventError> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or(ChatEventError::MissingField(field))
}

fn decode_presence_value(value: &Value) -> Result<ChatPresence, ChatEventError> {
    let presence: ChatPresence = serde_json::from_value(value.clone())
        .map_err(|_| ChatEventError::InvalidField("presence"))?;
    validate_presence(&presence)?;
    Ok(presence)
}

fn validate_presence(presence: &ChatPresence) -> Result<(), ChatEventError> {
    if presence.presence_id.trim().is_empty() || presence.user_id.trim().is_empty() {
        return Err(ChatEventError::InvalidField("presence"));
    }
    Ok(())
}

fn decode_json_object(body: &[u8]) -> Result<Map<String, Value>, ChatEventError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| ChatEventError::InvalidJson)?;
    value
        .as_object()
        .cloned()
        .ok_or(ChatEventError::ExpectedObject)
}

fn validate_wire_message(
    message: &ChatMessage,
    kind: ChatEventKind,
    event_id: u64,
) -> Result<(), ChatEventError> {
    const MAX_CHAT_CONTENT_BYTES: usize = 2_048;

    if event_id == 0
        || message.id == 0
        || message.sender.trim().is_empty()
        || message.updated_at_unix_ms < message.created_at_unix_ms
    {
        return Err(ChatEventError::InvalidField("message"));
    }
    if message.last_event_id != event_id {
        return Err(ChatEventError::InvalidField("message"));
    }
    let valid_content = !message.content.trim().is_empty()
        && message.content.len() <= MAX_CHAT_CONTENT_BYTES
        && !message
            .content
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r'));
    let state_matches_kind = match kind {
        ChatEventKind::MessageCreate => {
            valid_content
                && !message.deleted
                && message.revision == 1
                && message.created_at_unix_ms == message.updated_at_unix_ms
        }
        ChatEventKind::MessageUpdate => valid_content && !message.deleted && message.revision > 1,
        ChatEventKind::MessageRemove => {
            message.deleted && message.content.is_empty() && message.revision > 1
        }
        _ => false,
    };
    if !state_matches_kind {
        return Err(ChatEventError::InvalidField("message"));
    }
    Ok(())
}

fn decode_message_value(value: &Value) -> Result<ChatMessage, ChatEventError> {
    let message: ChatMessage = serde_json::from_value(value.clone())
        .map_err(|_| ChatEventError::InvalidField("message"))?;
    let valid_state = if message.revision == 1 {
        !message.deleted && message.created_at_unix_ms == message.updated_at_unix_ms
    } else {
        message.revision > 1 && (!message.deleted || message.content.is_empty())
    };
    if message.id == 0
        || message.sender.trim().is_empty()
        || message.last_event_id == 0
        || message.updated_at_unix_ms < message.created_at_unix_ms
        || !valid_state
    {
        return Err(ChatEventError::InvalidField("message"));
    }
    Ok(message)
}

/// Fail-closed errors for the typed chat-event boundary.
#[derive(Debug, thiserror::Error)]
pub enum ChatEventError {
    #[error("chat event body is not valid JSON")]
    InvalidJson,
    #[error("chat event body must be a JSON object")]
    ExpectedObject,
    #[error("chat event is missing or has an invalid `{0}` field")]
    MissingField(&'static str),
    #[error("chat event has an invalid `{0}` field")]
    InvalidField(&'static str),
    #[error("unsupported chat event version {0}")]
    UnsupportedVersion(u64),
    #[error("unknown chat event type `{0}`")]
    UnknownType(String),
    #[error("expected KIND_CHAT_EVENT (28), got envelope kind {0}")]
    WrongEnvelopeKind(u16),
    #[error(
        "chat event channel `{event_channel}` does not match cursor channel `{cursor_channel}`"
    )]
    ChannelMismatch {
        cursor_channel: String,
        event_channel: String,
    },
    #[error("chat cursor channel id must not be empty")]
    EmptyCursorChannel,
    #[error("chat cursor cannot `{0}` in its current lifecycle state")]
    InvalidCursorState(&'static str),
    #[error("chat reconciliation watermark is stale or incomplete")]
    InvalidReconciliationWatermark,
    #[error("chat cursor identity or request sequence space is exhausted")]
    CursorIdentityExhausted,
    #[error(transparent)]
    ChatRequest(#[from] ChatRequestError),
}

/// Lifecycle of one joined channel's durable cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatCursorState {
    Current,
    Disconnected,
    AwaitingJoin,
    Reconciling { required_watermark: u64 },
    ReadyToAcknowledge { watermark: u64 },
    AwaitingAcknowledgement { watermark: u64 },
    Revoked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChatRequestToken {
    cursor_id: u64,
    sequence: u64,
}

/// Opaque, single-use authorization attempt for an initial `chat.join`.
///
/// The request can be borrowed for transport I/O, but the attempt itself must
/// be consumed with strict response bytes to construct a current cursor. It
/// intentionally does not implement `Clone`.
#[derive(Debug)]
pub struct ChatJoinAttempt {
    request: ChatRpcRequest,
    cursor_id: u64,
    expected_channel_type: &'static str,
}

impl ChatJoinAttempt {
    pub fn new(target: ChatTarget) -> Result<Self, ChatEventError> {
        let expected_channel_type = expected_channel_type(&target);
        Ok(Self {
            request: ChatRpcRequest::join(target)?,
            cursor_id: next_chat_cursor_id()?,
            expected_channel_type,
        })
    }

    pub const fn request(&self) -> &ChatRpcRequest {
        &self.request
    }

    pub const fn method(&self) -> &'static str {
        self.request.method()
    }

    pub const fn json(&self) -> &Value {
        self.request.json()
    }

    pub fn body(&self) -> &[u8] {
        self.request.body()
    }
}

/// Opaque request handle that correlates a typed `chat.join` response.
#[derive(Debug)]
pub struct ChatJoinRequest {
    request: ChatRpcRequest,
    token: ChatRequestToken,
    expected_channel_type: &'static str,
    expected_recovery_floor: u64,
}

impl ChatJoinRequest {
    pub const fn method(&self) -> &'static str {
        self.request.method()
    }

    pub const fn json(&self) -> &Value {
        self.request.json()
    }

    pub fn body(&self) -> &[u8] {
        self.request.body()
    }
}

/// Opaque request handle that must accompany its correlated history response.
#[derive(Debug)]
pub struct ChatHistoryRequest {
    request: ChatRpcRequest,
    token: ChatRequestToken,
}

impl ChatHistoryRequest {
    pub const fn method(&self) -> &'static str {
        self.request.method()
    }

    pub const fn json(&self) -> &Value {
        self.request.json()
    }

    pub fn body(&self) -> &[u8] {
        self.request.body()
    }
}

/// Validated history page awaiting explicit application by the caller.
///
/// This handle is intentionally not `Clone`: exactly one cursor may complete
/// or abort the correlated application operation.
#[derive(Debug)]
pub struct ChatHistoryApplication {
    token: ChatRequestToken,
    messages: Vec<ChatMessage>,
    response_watermark: u64,
    limit: u16,
}

impl ChatHistoryApplication {
    pub fn messages(&self) -> &[ChatMessage] {
        &self.messages
    }

    pub const fn watermark_event_id(&self) -> u64 {
        self.response_watermark
    }
}

/// Opaque ACK handle that must accompany its correlated RPC response.
#[derive(Debug)]
pub struct ChatAcknowledgementRequest {
    request: ChatRpcRequest,
    token: ChatRequestToken,
}

impl ChatAcknowledgementRequest {
    pub const fn method(&self) -> &'static str {
        self.request.method()
    }

    pub const fn json(&self) -> &Value {
        self.request.json()
    }

    pub fn body(&self) -> &[u8] {
        self.request.body()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryProgress {
    next_before_message_id: Option<u64>,
    snapshot_watermark: Option<u64>,
    pending: Option<(ChatRequestToken, u16, Option<u64>)>,
    application: Option<ChatRequestToken>,
}

/// Result of comparing one validated event with a channel's durable watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatEventDisposition {
    Apply {
        event_id: u64,
    },
    Duplicate {
        event_id: u64,
    },
    ReconcileGap {
        current_watermark: u64,
        observed_event_id: u64,
    },
    ResyncRequired {
        watermark_event_id: u64,
    },
    AccessRevoked,
    Ephemeral,
}

/// Caller-owned durable cursor for one joined channel.
///
/// A current cursor cannot be fabricated from a channel and watermark. It is
/// created only by consuming a correlated [`ChatJoinAttempt`] and strict join
/// response bytes:
///
/// ```compile_fail
/// use citadel_client::ChatEventCursor;
/// let _bypass = ChatEventCursor::new("channel", 7);
/// ```
///
/// Join attempts are non-clone and single use, so replay does not compile:
///
/// ```compile_fail
/// use citadel_client::{ChatEventCursor, ChatJoinAttempt, ChatTarget};
/// let attempt = ChatJoinAttempt::new(ChatTarget::CurrentRoom).unwrap();
/// let _first = ChatEventCursor::from_join_response(attempt, b"{}");
/// let _replayed = ChatEventCursor::from_join_response(attempt, b"{}");
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct ChatEventCursor {
    channel_id: String,
    watermark: u64,
    state: ChatCursorState,
    history: Option<HistoryProgress>,
    cursor_id: u64,
    next_request_sequence: u64,
    pending_ack: Option<ChatRequestToken>,
    pending_join: Option<ChatRequestToken>,
}

impl ChatEventCursor {
    /// Consume an initial join attempt and strict wire response to authorize a
    /// new current cursor. A decoded [`ChatJoinResult`] alone is insufficient.
    pub fn from_join_response(
        attempt: ChatJoinAttempt,
        body: &[u8],
    ) -> Result<Self, ChatEventError> {
        let result = ChatJoinResult::decode(body)?;
        if result.channel_type != attempt.expected_channel_type {
            return Err(ChatEventError::InvalidField("channel_type"));
        }
        Ok(Self {
            channel_id: result.channel_id,
            watermark: result.watermark_event_id,
            state: ChatCursorState::Current,
            history: None,
            cursor_id: attempt.cursor_id,
            next_request_sequence: 1,
            pending_ack: None,
            pending_join: None,
        })
    }

    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    pub const fn watermark(&self) -> u64 {
        self.watermark
    }

    pub const fn state(&self) -> ChatCursorState {
        self.state
    }

    pub fn disconnect(&mut self) {
        if self.state != ChatCursorState::Revoked {
            self.state = ChatCursorState::Disconnected;
        }
        self.history = None;
        self.pending_ack = None;
        self.pending_join = None;
    }

    /// Start a reconnect/join operation when no recovery generation is active.
    pub fn rejoin_request(
        &mut self,
        target: ChatTarget,
    ) -> Result<ChatJoinRequest, ChatEventError> {
        if !matches!(
            self.state,
            ChatCursorState::Current | ChatCursorState::Disconnected
        ) || self.pending_join.is_some()
        {
            return Err(ChatEventError::InvalidCursorState("rejoin busy cursor"));
        }
        let token = self.issue_request_token()?;
        let expected_channel_type = expected_channel_type(&target);
        let expected_recovery_floor = self.watermark;
        let request = ChatRpcRequest::join(target)?;
        self.state = ChatCursorState::AwaitingJoin;
        self.history = None;
        self.pending_ack = None;
        self.pending_join = Some(token);
        Ok(ChatJoinRequest {
            request,
            token,
            expected_channel_type,
            expected_recovery_floor,
        })
    }

    /// Accept only the typed result correlated to the active join operation.
    pub fn accept_rejoin_response(
        &mut self,
        request: ChatJoinRequest,
        result: ChatJoinResult,
    ) -> Result<bool, ChatEventError> {
        if self.state != ChatCursorState::AwaitingJoin || self.pending_join != Some(request.token) {
            return Err(ChatEventError::InvalidCursorState(
                "accept mismatched join response",
            ));
        }
        if result.channel_id != self.channel_id {
            return Err(ChatEventError::ChannelMismatch {
                cursor_channel: self.channel_id.clone(),
                event_channel: result.channel_id,
            });
        }
        if result.channel_type != request.expected_channel_type {
            return Err(ChatEventError::InvalidField("channel_type"));
        }
        self.pending_join = None;
        if result.watermark_event_id == request.expected_recovery_floor
            && self.watermark == request.expected_recovery_floor
        {
            self.state = ChatCursorState::Current;
            self.history = None;
            self.pending_ack = None;
            Ok(false)
        } else {
            self.start_reconciliation(
                result
                    .watermark_event_id
                    .max(request.expected_recovery_floor),
            );
            Ok(true)
        }
    }

    /// Build a history page without acknowledgement while reconciliation is active.
    pub fn reconciliation_history_request(
        &mut self,
        mut options: ChatHistoryOptions,
    ) -> Result<ChatHistoryRequest, ChatEventError> {
        if !matches!(self.state, ChatCursorState::Reconciling { .. }) {
            return Err(ChatEventError::InvalidCursorState("request history"));
        }
        let token = self.issue_request_token()?;
        let progress = self
            .history
            .as_mut()
            .ok_or(ChatEventError::InvalidCursorState("request history"))?;
        if progress.pending.is_some() {
            return Err(ChatEventError::InvalidCursorState(
                "request concurrent history",
            ));
        }
        if progress.application.is_some() {
            return Err(ChatEventError::InvalidCursorState(
                "request history before application",
            ));
        }
        if options.before_message_id.is_some()
            && options.before_message_id != progress.next_before_message_id
        {
            return Err(ChatEventError::InvalidCursorState("skip history page"));
        }
        options.before_message_id = progress.next_before_message_id;
        let before_message_id = options.before_message_id;
        let limit = options.limit.unwrap_or(50);
        let request = ChatRpcRequest::history(&self.channel_id, options)?;
        progress.pending = Some((token, limit, before_message_id));
        Ok(ChatHistoryRequest { request, token })
    }

    /// Validate one ordered newest-first response without advancing pagination.
    pub fn accept_history_response(
        &mut self,
        request: ChatHistoryRequest,
        body: &[u8],
    ) -> Result<ChatHistoryApplication, ChatEventError> {
        let ChatCursorState::Reconciling { required_watermark } = self.state else {
            return Err(ChatEventError::InvalidCursorState("accept history"));
        };
        let progress = self
            .history
            .as_ref()
            .ok_or(ChatEventError::InvalidCursorState("accept history"))?;
        let (pending_token, limit, before_message_id) = progress.pending.ok_or(
            ChatEventError::InvalidCursorState("accept unsolicited history"),
        )?;
        let pending_snapshot_watermark = progress.snapshot_watermark;
        if request.token != pending_token {
            return Err(ChatEventError::InvalidCursorState(
                "accept mismatched history",
            ));
        }

        let (messages, response_watermark) = match decode_history_response(body) {
            Ok(response) => response,
            Err(error) => {
                self.start_reconciliation(required_watermark);
                return Err(error);
            }
        };
        let snapshot = pending_snapshot_watermark.unwrap_or(response_watermark);
        if messages.len() > usize::from(limit)
            || !messages.windows(2).all(|pair| pair[0].id > pair[1].id)
            || before_message_id
                .is_some_and(|bound| messages.iter().any(|message| message.id >= bound))
        {
            self.start_reconciliation(required_watermark);
            return Err(ChatEventError::InvalidField("history.items"));
        }
        if response_watermark < required_watermark
            || pending_snapshot_watermark.is_some_and(|watermark| watermark != response_watermark)
        {
            self.start_reconciliation(response_watermark.max(snapshot).max(required_watermark));
            return Err(ChatEventError::InvalidReconciliationWatermark);
        }
        if messages
            .iter()
            .any(|message| message.last_event_id > snapshot)
        {
            self.start_reconciliation(required_watermark);
            return Err(ChatEventError::InvalidReconciliationWatermark);
        }
        let progress = self
            .history
            .as_mut()
            .ok_or(ChatEventError::InvalidCursorState("accept history"))?;
        progress.pending = None;
        progress.application = Some(request.token);
        Ok(ChatHistoryApplication {
            token: request.token,
            messages,
            response_watermark,
            limit,
        })
    }

    /// Confirm that the caller incorporated the validated page.
    pub fn complete_history_application(
        &mut self,
        application: ChatHistoryApplication,
    ) -> Result<(), ChatEventError> {
        let progress = self
            .history
            .as_mut()
            .ok_or(ChatEventError::InvalidCursorState(
                "complete history application",
            ))?;
        if progress.application != Some(application.token) {
            return Err(ChatEventError::InvalidCursorState(
                "complete mismatched history application",
            ));
        }
        progress.application = None;
        let snapshot = *progress
            .snapshot_watermark
            .get_or_insert(application.response_watermark);
        if application.messages.len() < usize::from(application.limit) {
            self.state = ChatCursorState::ReadyToAcknowledge {
                watermark: snapshot,
            };
            self.history = None;
            return Ok(());
        }
        let next_before = application
            .messages
            .last()
            .map(|message| message.id)
            .ok_or(ChatEventError::InvalidField("history.items"))?;
        if progress
            .next_before_message_id
            .is_some_and(|previous| next_before >= previous)
        {
            return Err(ChatEventError::InvalidField("history.items"));
        }
        progress.next_before_message_id = Some(next_before);
        Ok(())
    }

    /// Abort page application and invalidate all partial reconciliation state.
    pub fn abort_history_application(
        &mut self,
        application: ChatHistoryApplication,
    ) -> Result<(), ChatEventError> {
        let required_watermark = match self.state {
            ChatCursorState::Reconciling { required_watermark } => required_watermark,
            _ => {
                return Err(ChatEventError::InvalidCursorState(
                    "abort history application",
                ));
            }
        };
        let progress = self
            .history
            .as_ref()
            .ok_or(ChatEventError::InvalidCursorState(
                "abort history application",
            ))?;
        if progress.application != Some(application.token) {
            return Err(ChatEventError::InvalidCursorState(
                "abort mismatched history application",
            ));
        }
        self.start_reconciliation(required_watermark.max(application.response_watermark));
        Ok(())
    }

    /// Build the final acknowledgement only after every history page was applied.
    pub fn acknowledge_reconciliation(
        &mut self,
    ) -> Result<ChatAcknowledgementRequest, ChatEventError> {
        let ChatCursorState::ReadyToAcknowledge { watermark } = self.state else {
            return Err(ChatEventError::InvalidCursorState("acknowledge history"));
        };
        let token = self.issue_request_token()?;
        let request = ChatRpcRequest::history_acknowledgement(&self.channel_id, watermark)?;
        self.state = ChatCursorState::AwaitingAcknowledgement { watermark };
        self.pending_ack = Some(token);
        Ok(ChatAcknowledgementRequest { request, token })
    }

    /// Mark the view current only after the acknowledgement response arrives.
    pub fn complete_reconciliation(
        &mut self,
        request: ChatAcknowledgementRequest,
        body: &[u8],
    ) -> Result<(), ChatEventError> {
        let ChatCursorState::AwaitingAcknowledgement { watermark } = self.state else {
            return Err(ChatEventError::InvalidCursorState(
                "complete reconciliation",
            ));
        };
        if self.pending_ack != Some(request.token) {
            return Err(ChatEventError::InvalidCursorState(
                "complete mismatched acknowledgement",
            ));
        }
        self.pending_ack = None;
        let (_, response_watermark) = match decode_history_response(body) {
            Ok(response) => response,
            Err(error) => {
                self.start_reconciliation(watermark);
                return Err(error);
            }
        };
        if response_watermark != watermark {
            self.start_reconciliation(response_watermark.max(watermark));
            return Err(ChatEventError::InvalidReconciliationWatermark);
        }
        self.watermark = response_watermark;
        self.state = ChatCursorState::Current;
        self.pending_ack = None;
        Ok(())
    }

    /// Classify one validated event. Gaps do not advance the watermark.
    pub fn observe(&mut self, event: &ChatEvent) -> Result<ChatEventDisposition, ChatEventError> {
        if event.channel_id != self.channel_id {
            return Err(ChatEventError::ChannelMismatch {
                cursor_channel: self.channel_id.clone(),
                event_channel: event.channel_id.clone(),
            });
        }
        if event.kind == ChatEventKind::AccessRevoked {
            self.watermark = 0;
            self.state = ChatCursorState::Revoked;
            self.history = None;
            self.pending_ack = None;
            self.pending_join = None;
            return Ok(ChatEventDisposition::AccessRevoked);
        }
        if self.state != ChatCursorState::Current {
            return Err(ChatEventError::InvalidCursorState("observe live event"));
        }
        if let Some(watermark_event_id) = event.watermark_event_id {
            self.start_reconciliation(watermark_event_id);
            return Ok(ChatEventDisposition::ResyncRequired { watermark_event_id });
        }
        let Some(event_id) = event.event_id else {
            return Ok(ChatEventDisposition::Ephemeral);
        };
        if event_id <= self.watermark {
            return Ok(ChatEventDisposition::Duplicate { event_id });
        }
        if self.watermark.checked_add(1) == Some(event_id) {
            self.watermark = event_id;
            return Ok(ChatEventDisposition::Apply { event_id });
        }
        self.start_reconciliation(event_id);
        Ok(ChatEventDisposition::ReconcileGap {
            current_watermark: self.watermark,
            observed_event_id: event_id,
        })
    }

    fn start_reconciliation(&mut self, required_watermark: u64) {
        self.state = ChatCursorState::Reconciling {
            required_watermark: required_watermark.max(self.watermark),
        };
        self.history = Some(HistoryProgress {
            next_before_message_id: None,
            snapshot_watermark: None,
            pending: None,
            application: None,
        });
        self.pending_ack = None;
        self.pending_join = None;
    }

    fn issue_request_token(&mut self) -> Result<ChatRequestToken, ChatEventError> {
        let sequence = self.next_request_sequence;
        self.next_request_sequence = sequence
            .checked_add(1)
            .ok_or(ChatEventError::CursorIdentityExhausted)?;
        Ok(ChatRequestToken {
            cursor_id: self.cursor_id,
            sequence,
        })
    }
}

fn decode_history_response(body: &[u8]) -> Result<(Vec<ChatMessage>, u64), ChatEventError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| ChatEventError::InvalidJson)?;
    let object = value.as_object().ok_or(ChatEventError::ExpectedObject)?;
    let watermark = object
        .get("watermark_event_id")
        .and_then(Value::as_u64)
        .ok_or(ChatEventError::MissingField("watermark_event_id"))?;
    let items = object
        .get("items")
        .and_then(Value::as_array)
        .ok_or(ChatEventError::MissingField("items"))?;
    let messages = items
        .iter()
        .map(decode_message_value)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((messages, watermark))
}
