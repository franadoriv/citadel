//! Versioned wire bodies for explicit, game-defined native-match input.
//!
//! The envelope kind selects this route; the server derives participant and room
//! scope from authenticated transport state. Neither is present in these bytes.

/// Current match-input body version.
pub const MATCH_INPUT_VERSION: u8 = 1;

/// One explicit browser-to-server input frame.
///
/// `sequence` is game-defined monotonic ordering information. Core preserves it
/// exactly and leaves duplicate/stale policy to the authoritative match script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchInput {
    /// Client-authored ordering value; zero is valid for protocols that start
    /// their counter at zero.
    pub sequence: u64,
    /// Opaque game-defined bytes, bounded before any runtime allocation.
    pub body: Vec<u8>,
}

/// One private server-to-client acknowledgement of processed input sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchInputAck {
    /// Last sequence the authoritative match script reports as processed.
    pub last_processed_sequence: u64,
}

/// Stable validation failures at the explicit match-input boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MatchInputError {
    /// The version byte was not understood.
    #[error("unsupported match input version")]
    UnsupportedVersion,
    /// The fixed version/sequence prefix was incomplete.
    #[error("truncated match input")]
    Truncated,
    /// The opaque body exceeded the server-owned bound.
    #[error("match input body exceeds maximum")]
    BodyTooLarge,
}

impl MatchInput {
    /// Version plus big-endian sequence prefix.
    pub const HEADER_BYTES: usize = 1 + 8;
    /// Maximum opaque input body accepted by core before dispatch.
    pub const MAX_BODY_BYTES: usize = 64 * 1024;

    /// Encode an exact V1 body.
    pub fn encode(&self) -> Result<Vec<u8>, MatchInputError> {
        if self.body.len() > Self::MAX_BODY_BYTES {
            return Err(MatchInputError::BodyTooLarge);
        }
        let mut out = Vec::with_capacity(Self::HEADER_BYTES + self.body.len());
        out.push(MATCH_INPUT_VERSION);
        out.extend_from_slice(&self.sequence.to_be_bytes());
        out.extend_from_slice(&self.body);
        Ok(out)
    }

    /// Decode an exact V1 input body without interpreting game bytes.
    pub fn decode(body: &[u8]) -> Result<Self, MatchInputError> {
        if body.len() < Self::HEADER_BYTES {
            return Err(MatchInputError::Truncated);
        }
        if body[0] != MATCH_INPUT_VERSION {
            return Err(MatchInputError::UnsupportedVersion);
        }
        let payload = &body[Self::HEADER_BYTES..];
        if payload.len() > Self::MAX_BODY_BYTES {
            return Err(MatchInputError::BodyTooLarge);
        }
        let mut sequence = [0_u8; 8];
        sequence.copy_from_slice(&body[1..Self::HEADER_BYTES]);
        Ok(Self {
            sequence: u64::from_be_bytes(sequence),
            body: payload.to_vec(),
        })
    }
}

impl MatchInputAck {
    /// Fixed V1 acknowledgement body size.
    pub const BYTES: usize = 1 + 8;

    /// Encode an exact V1 acknowledgement.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::BYTES);
        out.push(MATCH_INPUT_VERSION);
        out.extend_from_slice(&self.last_processed_sequence.to_be_bytes());
        out
    }

    /// Decode an exact V1 acknowledgement.
    pub fn decode(body: &[u8]) -> Result<Self, MatchInputError> {
        if body.len() < Self::BYTES {
            return Err(MatchInputError::Truncated);
        }
        if body[0] != MATCH_INPUT_VERSION {
            return Err(MatchInputError::UnsupportedVersion);
        }
        if body.len() != Self::BYTES {
            return Err(MatchInputError::BodyTooLarge);
        }
        let mut sequence = [0_u8; 8];
        sequence.copy_from_slice(&body[1..]);
        Ok(Self {
            last_processed_sequence: u64::from_be_bytes(sequence),
        })
    }
}
