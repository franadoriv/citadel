//! Typed client-side contract for `KIND_CHAT_EVENT` (28).
//!
//! Citadel deliberately delivers durable chat events at least once. A
//! [`ChatEventCursor`] belongs to one joined channel and provides bounded,
//! caller-owned duplicate/gap detection without hiding network polling or
//! retaining a global channel map.

use citadel_wire::{Envelope, protocol::KIND_CHAT_EVENT};
use serde::Deserialize;
use serde_json::{Map, Value};

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
pub struct ChatPresence {
    pub presence_id: String,
    pub user_id: String,
}

/// Exact durable message state serialized by Citadel's chat repository.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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

    fn parse(value: &str) -> Option<Self> {
        match value {
            "presence.join" => Some(Self::PresenceJoin),
            "presence.leave" => Some(Self::PresenceLeave),
            "typing" => Some(Self::Typing),
            "message.create" => Some(Self::MessageCreate),
            "message.update" => Some(Self::MessageUpdate),
            "message.remove" => Some(Self::MessageRemove),
            "access.revoked" => Some(Self::AccessRevoked),
            "resync_required" => Some(Self::ResyncRequired),
            _ => None,
        }
    }

    const fn is_durable(self) -> bool {
        matches!(
            self,
            Self::MessageCreate | Self::MessageUpdate | Self::MessageRemove
        )
    }

    const fn requires_presence(self) -> bool {
        matches!(
            self,
            Self::PresenceJoin | Self::PresenceLeave | Self::Typing | Self::AccessRevoked
        )
    }
}

/// A validated version-1 chat event. Unknown fields are retained in [`Self::raw`]
/// for additive server evolution, while unknown versions/types fail closed.
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
        let raw: Value = serde_json::from_slice(body).map_err(|_| ChatEventError::InvalidJson)?;
        let object = raw.as_object().ok_or(ChatEventError::ExpectedObject)?;

        let version = object
            .get("version")
            .and_then(Value::as_u64)
            .ok_or(ChatEventError::MissingField("version"))?;
        if version != 1 {
            return Err(ChatEventError::UnsupportedVersion(version));
        }
        let type_name = non_empty_string(object, "type")?;
        let kind = ChatEventKind::parse(type_name)
            .ok_or_else(|| ChatEventError::UnknownType(type_name.to_owned()))?;
        let channel_id = non_empty_string(object, "channel_id")?.to_owned();

        let presence = kind
            .requires_presence()
            .then(|| decode_presence(object))
            .transpose()?;

        let (event_id, message) = if kind.is_durable() {
            let id = positive_u64(object, "event_id")?;
            let message = decode_message(object, kind, id)?;
            (Some(id), Some(message))
        } else {
            (None, None)
        };

        let (typing, expires_at) = if kind == ChatEventKind::Typing {
            let state = object
                .get("typing")
                .and_then(Value::as_bool)
                .ok_or(ChatEventError::MissingField("typing"))?;
            let expiry = object
                .get("expires_at")
                .and_then(Value::as_u64)
                .ok_or(ChatEventError::MissingField("expires_at"))?;
            (Some(state), Some(expiry))
        } else {
            (None, None)
        };

        let watermark_event_id = if kind == ChatEventKind::ResyncRequired {
            Some(
                object
                    .get("watermark_event_id")
                    .and_then(Value::as_u64)
                    .ok_or(ChatEventError::MissingField("watermark_event_id"))?,
            )
        } else {
            None
        };

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

fn decode_presence(object: &Map<String, Value>) -> Result<ChatPresence, ChatEventError> {
    let value = object
        .get("presence")
        .ok_or(ChatEventError::MissingField("presence"))?;
    let presence: ChatPresence = serde_json::from_value(value.clone())
        .map_err(|_| ChatEventError::InvalidField("presence"))?;
    if presence.presence_id.trim().is_empty() || presence.user_id.trim().is_empty() {
        return Err(ChatEventError::InvalidField("presence"));
    }
    Ok(presence)
}

fn decode_message(
    object: &Map<String, Value>,
    kind: ChatEventKind,
    event_id: u64,
) -> Result<ChatMessage, ChatEventError> {
    let value = object
        .get("message")
        .ok_or(ChatEventError::MissingField("message"))?;
    let message: ChatMessage = serde_json::from_value(value.clone())
        .map_err(|_| ChatEventError::InvalidField("message"))?;
    if message.id == 0
        || message.sender.trim().is_empty()
        || message.revision == 0
        || message.last_event_id == 0
        || message.last_event_id != event_id
        || message.updated_at_unix_ms < message.created_at_unix_ms
    {
        return Err(ChatEventError::InvalidField("message"));
    }
    let state_matches_kind = match kind {
        ChatEventKind::MessageCreate => {
            !message.deleted
                && message.revision == 1
                && message.created_at_unix_ms == message.updated_at_unix_ms
        }
        ChatEventKind::MessageUpdate => !message.deleted && message.revision > 1,
        ChatEventKind::MessageRemove => {
            message.deleted && message.content.is_empty() && message.revision > 1
        }
        _ => false,
    };
    if !state_matches_kind {
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
    Ephemeral,
}

/// Caller-owned durable cursor for one joined channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatEventCursor {
    channel_id: String,
    watermark: u64,
}

impl ChatEventCursor {
    /// Initialize from the `watermark_event_id` returned by `chat.join` or
    /// `chat.history`.
    pub fn new(channel_id: impl Into<String>, watermark: u64) -> Result<Self, ChatEventError> {
        let channel_id = channel_id.into();
        if channel_id.trim().is_empty() {
            return Err(ChatEventError::EmptyCursorChannel);
        }
        Ok(Self {
            channel_id,
            watermark,
        })
    }

    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    pub const fn watermark(&self) -> u64 {
        self.watermark
    }

    /// Replace the cursor after a successful `chat.history` reconciliation.
    pub const fn reset(&mut self, watermark: u64) {
        self.watermark = watermark;
    }

    /// Classify one validated event. Gaps do not advance the watermark.
    pub fn observe(&mut self, event: &ChatEvent) -> Result<ChatEventDisposition, ChatEventError> {
        if event.channel_id != self.channel_id {
            return Err(ChatEventError::ChannelMismatch {
                cursor_channel: self.channel_id.clone(),
                event_channel: event.channel_id.clone(),
            });
        }
        if let Some(watermark_event_id) = event.watermark_event_id {
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
        Ok(ChatEventDisposition::ReconcileGap {
            current_watermark: self.watermark,
            observed_event_id: event_id,
        })
    }
}
