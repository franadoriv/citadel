//! Validated high-level requests for Citadel's chat RPC surface.

use serde_json::Value;

/// Authorized target shape accepted by `chat.join`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChatTarget {
    CurrentRoom,
    Group { group_id: u64 },
    Direct { other_user_id: String },
}

/// Optional pagination fields for a normal `chat.history` request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChatHistoryOptions {
    pub limit: Option<u16>,
    pub before_message_id: Option<u64>,
}

/// Fail-closed errors produced before a chat RPC reaches the network.
#[derive(Debug, thiserror::Error)]
pub enum ChatRequestError {
    #[error("invalid chat request field `{0}`")]
    InvalidField(&'static str),
    #[error("could not encode chat request JSON")]
    Encoding,
}

/// A validated domain RPC request ready for the generic RPC transport.
#[derive(Debug, Clone, PartialEq)]
pub struct ChatRpcRequest {
    method: &'static str,
    json: Value,
    body: Vec<u8>,
}

impl ChatRpcRequest {
    pub fn join(target: ChatTarget) -> Result<Self, ChatRequestError> {
        let target = match target {
            ChatTarget::CurrentRoom => serde_json::json!({"kind": "room"}),
            ChatTarget::Group { group_id } if group_id > 0 => {
                serde_json::json!({"kind": "group", "group_id": group_id})
            }
            ChatTarget::Group { .. } => {
                return Err(ChatRequestError::InvalidField("target.group_id"));
            }
            ChatTarget::Direct { other_user_id } if !other_user_id.trim().is_empty() => {
                serde_json::json!({"kind": "direct", "other_user_id": other_user_id})
            }
            ChatTarget::Direct { .. } => {
                return Err(ChatRequestError::InvalidField("target.other_user_id"));
            }
        };
        Self::build("chat.join", serde_json::json!({"target": target}))
    }

    pub fn leave(channel_id: &str) -> Result<Self, ChatRequestError> {
        Self::channel_request("chat.leave", channel_id, serde_json::Map::new())
    }

    pub fn send(channel_id: &str, content: &str) -> Result<Self, ChatRequestError> {
        validate_content(content)?;
        let mut fields = serde_json::Map::new();
        fields.insert("content".to_owned(), Value::String(content.to_owned()));
        Self::channel_request("chat.send", channel_id, fields)
    }

    pub fn edit(
        channel_id: &str,
        message_id: u64,
        content: &str,
    ) -> Result<Self, ChatRequestError> {
        validate_content(content)?;
        let mut fields = message_fields(message_id)?;
        fields.insert("content".to_owned(), Value::String(content.to_owned()));
        Self::channel_request("chat.edit", channel_id, fields)
    }

    pub fn delete(channel_id: &str, message_id: u64) -> Result<Self, ChatRequestError> {
        Self::channel_request("chat.delete", channel_id, message_fields(message_id)?)
    }

    pub fn moderate(channel_id: &str, message_id: u64) -> Result<Self, ChatRequestError> {
        Self::channel_request("chat.moderate", channel_id, message_fields(message_id)?)
    }

    pub fn typing(channel_id: &str, typing: bool) -> Result<Self, ChatRequestError> {
        let mut fields = serde_json::Map::new();
        fields.insert("typing".to_owned(), Value::Bool(typing));
        Self::channel_request("chat.typing", channel_id, fields)
    }

    pub fn history(
        channel_id: &str,
        options: ChatHistoryOptions,
    ) -> Result<Self, ChatRequestError> {
        if options.limit.is_some_and(|limit| limit == 0 || limit > 200) {
            return Err(ChatRequestError::InvalidField("limit"));
        }
        if options.before_message_id == Some(0) {
            return Err(ChatRequestError::InvalidField("before_message_id"));
        }
        let mut fields = serde_json::Map::new();
        if let Some(limit) = options.limit {
            fields.insert("limit".to_owned(), Value::from(limit));
        }
        if let Some(before_message_id) = options.before_message_id {
            fields.insert(
                "before_message_id".to_owned(),
                Value::from(before_message_id),
            );
        }
        Self::channel_request("chat.history", channel_id, fields)
    }

    pub(crate) fn history_acknowledgement(
        channel_id: &str,
        watermark: u64,
    ) -> Result<Self, ChatRequestError> {
        let mut fields = serde_json::Map::new();
        fields.insert("limit".to_owned(), Value::from(1));
        fields.insert("acknowledge_watermark".to_owned(), Value::from(watermark));
        Self::channel_request("chat.history", channel_id, fields)
    }

    pub const fn method(&self) -> &'static str {
        self.method
    }

    pub const fn json(&self) -> &Value {
        &self.json
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    fn channel_request(
        method: &'static str,
        channel_id: &str,
        mut fields: serde_json::Map<String, Value>,
    ) -> Result<Self, ChatRequestError> {
        if channel_id.trim().is_empty() {
            return Err(ChatRequestError::InvalidField("channel_id"));
        }
        fields.insert(
            "channel_id".to_owned(),
            Value::String(channel_id.to_owned()),
        );
        Self::build(method, Value::Object(fields))
    }

    fn build(method: &'static str, json: Value) -> Result<Self, ChatRequestError> {
        let body = serde_json::to_vec(&json).map_err(|_| ChatRequestError::Encoding)?;
        Ok(Self { method, json, body })
    }
}

fn message_fields(message_id: u64) -> Result<serde_json::Map<String, Value>, ChatRequestError> {
    if message_id == 0 {
        return Err(ChatRequestError::InvalidField("message_id"));
    }
    let mut fields = serde_json::Map::new();
    fields.insert("message_id".to_owned(), Value::from(message_id));
    Ok(fields)
}

fn validate_content(content: &str) -> Result<(), ChatRequestError> {
    const MAX_CHAT_CONTENT_BYTES: usize = 2_048;
    if content.trim().is_empty()
        || content.len() > MAX_CHAT_CONTENT_BYTES
        || content
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r'))
    {
        return Err(ChatRequestError::InvalidField("content"));
    }
    Ok(())
}
