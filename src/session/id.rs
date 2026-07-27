//! Session and node identity types.
//!
//! [`SessionId`] is deliberately opaque: unlike Nakama, which derives a node
//! hash from the session UUID to decide routing, Citadel never encodes or parses
//! node ownership out of the session id. Ownership is resolved explicitly through
//! the session directory (see [`crate::session::ownership`]).

use serde::{Deserialize, Serialize};

use crate::error::AppResult;
use crate::validate;

/// Maximum byte length for a session or node id.
const MAX_ID_LEN: usize = 128;

/// An opaque, validated session identity.
///
/// The value carries no routing information; use the session directory to map a
/// [`SessionId`] to its owning [`NodeId`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SessionId(String);

impl SessionId {
    /// Construct a session id, validating shape.
    ///
    /// # Errors
    /// Returns a validation error if empty/whitespace-only, longer than 128
    /// bytes, or containing control characters.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        validate::label("session id", &value, MAX_ID_LEN)?;
        Ok(Self(value))
    }

    /// The raw session id string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A stable identity for one running Citadel node/process.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
    /// Construct a node id, validating shape.
    ///
    /// # Errors
    /// Returns a validation error if empty/whitespace-only, longer than 128
    /// bytes, or containing control characters.
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let value = value.into();
        validate::label("node id", &value, MAX_ID_LEN)?;
        Ok(Self(value))
    }

    /// The raw node id string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_and_node_ids_validate() {
        assert!(SessionId::new("").is_err());
        assert!(SessionId::new("with\nnewline").is_err());
        assert!(SessionId::new("sess-abc").is_ok());
        assert!(NodeId::new("   ").is_err());
        assert!(NodeId::new("node-a").is_ok());
    }

    #[test]
    fn ids_display_as_raw_value() {
        let session = SessionId::new("sess-abc").expect("session");
        let node = NodeId::new("node-a").expect("node");
        assert_eq!(session.to_string(), "sess-abc");
        assert_eq!(node.to_string(), "node-a");
    }
}
