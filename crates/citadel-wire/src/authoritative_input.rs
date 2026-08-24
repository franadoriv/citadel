//! Version-1 bodies for stream-bound authoritative custom input and control.
//!
//! This module only defines transport-neutral byte contracts. It does not route
//! envelopes, select a room or participant, interpret a custom kind/body, or
//! decide authoritative outcomes.

/// Current stream-bound authoritative-custom-input body version.
pub const AUTHORITATIVE_INPUT_VERSION: u8 = 1;
/// Exact byte width of an opaque server-issued input stream token.
pub const INPUT_STREAM_TOKEN_BYTES: usize = 16;
/// Version of the standalone post-auth capability negotiation body.
pub const CAPABILITY_NEGOTIATION_VERSION: u8 = 1;
/// The only capability offered by this negotiation family.
pub const CAPABILITY_AUTHORITATIVE_INPUT: u8 = 1;
/// Exact byte width of a server-issued non-bearer capability challenge.
pub const CAPABILITY_CHALLENGE_BYTES: usize = 16;
/// Maximum opaque custom body accepted in one sequenced input.
pub const MAX_SEQUENCED_INPUT_BODY_BYTES: usize = 64 * 1024;

/// Structural validation errors for standalone post-auth capability messages.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CapabilityNegotiationError {
    /// The body did not contain its fixed-width fields.
    #[error("capability negotiation body is truncated: expected {needed} bytes, got {got}")]
    Truncated { needed: usize, got: usize },
    /// A body version is not understood and must not be guessed.
    #[error("unsupported capability negotiation version {0}")]
    UnsupportedVersion(u8),
    /// The capability discriminator is not defined by this V1 contract.
    #[error("unsupported capability {0}")]
    UnsupportedCapability(u8),
    /// A fixed body had bytes after its declared fields.
    #[error("capability negotiation body has {0} trailing bytes")]
    TrailingBytes(usize),
    /// The challenge has no entropy and must not be accepted.
    #[error("capability challenge must not be all zero")]
    AllZeroChallenge,
}

/// A server-issued post-auth capability offer. The challenge is deliberately
/// non-bearer material: it can only be echoed once by the exact live transport
/// generation recorded by the server.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CapabilityOffer {
    capability: u8,
    challenge: [u8; CAPABILITY_CHALLENGE_BYTES],
}

impl core::fmt::Debug for CapabilityOffer {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("CapabilityOffer")
            .field("capability", &self.capability)
            .field("challenge", &"[REDACTED]")
            .finish()
    }
}

impl CapabilityOffer {
    /// Construct the exact known V1 offer.
    pub fn new(
        capability: u8,
        challenge: [u8; CAPABILITY_CHALLENGE_BYTES],
    ) -> Result<Self, CapabilityNegotiationError> {
        if capability != CAPABILITY_AUTHORITATIVE_INPUT {
            return Err(CapabilityNegotiationError::UnsupportedCapability(
                capability,
            ));
        }
        if challenge.iter().all(|byte| *byte == 0) {
            return Err(CapabilityNegotiationError::AllZeroChallenge);
        }
        Ok(Self {
            capability,
            challenge,
        })
    }

    /// The offered feature discriminator.
    #[must_use]
    pub const fn capability(self) -> u8 {
        self.capability
    }

    /// Exact non-bearer challenge bytes for server-side equality validation.
    #[must_use]
    pub const fn challenge(self) -> [u8; CAPABILITY_CHALLENGE_BYTES] {
        self.challenge
    }

    /// Encode the canonical fixed-width offer body.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        let mut body = Vec::with_capacity(2 + CAPABILITY_CHALLENGE_BYTES);
        body.push(CAPABILITY_NEGOTIATION_VERSION);
        body.push(self.capability);
        body.extend_from_slice(&self.challenge);
        body
    }

    /// Decode exactly one canonical offer body.
    pub fn decode(body: &[u8]) -> Result<Self, CapabilityNegotiationError> {
        let expected = 2 + CAPABILITY_CHALLENGE_BYTES;
        if body.len() < expected {
            return Err(CapabilityNegotiationError::Truncated {
                needed: expected,
                got: body.len(),
            });
        }
        if body.len() > expected {
            return Err(CapabilityNegotiationError::TrailingBytes(
                body.len() - expected,
            ));
        }
        if body[0] != CAPABILITY_NEGOTIATION_VERSION {
            return Err(CapabilityNegotiationError::UnsupportedVersion(body[0]));
        }
        let mut challenge = [0; CAPABILITY_CHALLENGE_BYTES];
        challenge.copy_from_slice(&body[2..]);
        Self::new(body[1], challenge)
    }
}

/// A client acceptance must be a byte-for-byte canonical echo of a live offer.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CapabilityAcceptance(CapabilityOffer);

impl core::fmt::Debug for CapabilityAcceptance {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("CapabilityAcceptance([REDACTED])")
    }
}

impl CapabilityAcceptance {
    /// Build the canonical client echo for an accepted offer.
    #[must_use]
    pub const fn from_offer(offer: CapabilityOffer) -> Self {
        Self(offer)
    }

    /// Decode an acceptance body using the same fixed canonical layout as an offer.
    pub fn decode(body: &[u8]) -> Result<Self, CapabilityNegotiationError> {
        CapabilityOffer::decode(body).map(Self)
    }

    /// Encode the canonical acceptance echo.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        self.0.encode()
    }

    /// Whether this acceptance exactly echoes one offer.
    #[must_use]
    pub fn matches_offer(self, offer: CapabilityOffer) -> bool {
        self.0.capability == offer.capability && self.0.challenge == offer.challenge
    }
}

/// Structural validation errors for authoritative custom-input bodies.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthoritativeInputError {
    /// A body ended before its declared fixed or variable-width field.
    #[error("authoritative input body is truncated: expected at least {needed} bytes, got {got}")]
    Truncated { needed: usize, got: usize },
    /// A body version is not understood and must not be guessed.
    #[error("unsupported authoritative input version {0}")]
    UnsupportedVersion(u8),
    /// A bounded opaque body exceeded the layout limit.
    #[error("authoritative input field {field} exceeds maximum {max}: {actual}")]
    TooLarge {
        /// The bounded field name.
        field: &'static str,
        /// Maximum accepted byte count.
        max: usize,
        /// Actual byte count.
        actual: usize,
    },
    /// Sequence zero is reserved as the absence of input.
    #[error("authoritative input sequence must be nonzero")]
    ZeroSequence,
    /// A fixed-layout body had bytes after its declared payload.
    #[error("authoritative input body has {0} trailing bytes")]
    TrailingBytes(usize),
    /// A discriminator or field combination was not canonical.
    #[error("invalid authoritative input {0}")]
    Invalid(&'static str),
    /// Stream tokens must contain at least one nonzero byte.
    #[error("authoritative input stream token must not be all zero")]
    AllZeroStreamToken,
}

/// Opaque fixed-width server-issued stream token.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct InputStreamToken([u8; INPUT_STREAM_TOKEN_BYTES]);

impl core::fmt::Debug for InputStreamToken {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("InputStreamToken([REDACTED])")
    }
}

impl InputStreamToken {
    /// Construct a token from opaque fixed-width bytes.
    pub fn new(bytes: [u8; INPUT_STREAM_TOKEN_BYTES]) -> Result<Self, AuthoritativeInputError> {
        require_nonzero_stream_token(&bytes)?;
        Ok(Self(bytes))
    }

    /// Borrow the exact opaque bytes carried on the wire.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; INPUT_STREAM_TOKEN_BYTES] {
        &self.0
    }

    fn decode(reader: &mut Reader<'_>) -> Result<Self, AuthoritativeInputError> {
        let mut bytes = [0; INPUT_STREAM_TOKEN_BYTES];
        bytes.copy_from_slice(reader.bytes(INPUT_STREAM_TOKEN_BYTES)?);
        Self::new(bytes)
    }
}

/// Current server-issued input-stream control body version.
pub const INPUT_STREAM_CONTROL_VERSION: u8 = 1;
/// Canonical server-to-client control opcode that establishes a stream lease.
pub const INPUT_STREAM_CONTROL_ADVERTISE: u8 = 1;
/// Canonical server-to-client control opcode that retires a stream lease.
pub const INPUT_STREAM_CONTROL_REVOKE: u8 = 2;

/// Server-only control-plane body carried by `KIND_INPUT_STREAM_CONTROL`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum InputStreamControl {
    /// Establish one server-owned stream for the recipient's current match.
    Advertise {
        /// Server-owned room/match identity.
        match_id: u64,
        /// Server-issued stream incarnation identity.
        stream_id: u64,
        /// Opaque bearer token, never rendered through `Debug`.
        token: InputStreamToken,
    },
    /// Retire one previously advertised stream.
    Revoke {
        /// Server-owned room/match identity.
        match_id: u64,
        /// Server-issued stream incarnation identity.
        stream_id: u64,
    },
}

impl core::fmt::Debug for InputStreamControl {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Advertise {
                match_id,
                stream_id,
                ..
            } => formatter
                .debug_struct("InputStreamControl::Advertise")
                .field("match_id", match_id)
                .field("stream_id", stream_id)
                .field("token", &"[REDACTED]")
                .finish(),
            Self::Revoke {
                match_id,
                stream_id,
            } => formatter
                .debug_struct("InputStreamControl::Revoke")
                .field("match_id", match_id)
                .field("stream_id", stream_id)
                .finish(),
        }
    }
}

/// Structural validation error for an input-stream control body.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InputStreamControlError {
    /// A body ended before its fixed-width fields completed.
    #[error("input stream control body is truncated: expected at least {needed} bytes, got {got}")]
    Truncated { needed: usize, got: usize },
    /// A control body version is not understood and must not be guessed.
    #[error("unsupported input stream control version {0}")]
    UnsupportedVersion(u8),
    /// The operation discriminator is not a server-issued control operation.
    #[error("invalid input stream control operation {0}")]
    InvalidOperation(u8),
    /// A fixed-layout control body had unexpected bytes after its fields.
    #[error("input stream control body has {0} trailing bytes")]
    TrailingBytes(usize),
    /// An advertised bearer token was structurally invalid.
    #[error(transparent)]
    InvalidToken(#[from] AuthoritativeInputError),
}

impl InputStreamControl {
    /// Encode one canonical server-to-client control body.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        match self {
            Self::Advertise {
                match_id,
                stream_id,
                token,
            } => {
                let mut body = Vec::with_capacity(2 + 8 + 8 + INPUT_STREAM_TOKEN_BYTES);
                body.push(INPUT_STREAM_CONTROL_VERSION);
                body.push(INPUT_STREAM_CONTROL_ADVERTISE);
                body.extend_from_slice(&match_id.to_be_bytes());
                body.extend_from_slice(&stream_id.to_be_bytes());
                body.extend_from_slice(token.as_bytes());
                body
            }
            Self::Revoke {
                match_id,
                stream_id,
            } => {
                let mut body = Vec::with_capacity(2 + 8 + 8);
                body.push(INPUT_STREAM_CONTROL_VERSION);
                body.push(INPUT_STREAM_CONTROL_REVOKE);
                body.extend_from_slice(&match_id.to_be_bytes());
                body.extend_from_slice(&stream_id.to_be_bytes());
                body
            }
        }
    }

    /// Decode exactly one canonical input-stream control body.
    pub fn decode(body: &[u8]) -> Result<Self, InputStreamControlError> {
        const PREFIX_BYTES: usize = 2 + 8 + 8;
        if body.len() < 2 {
            return Err(InputStreamControlError::Truncated {
                needed: 2,
                got: body.len(),
            });
        }
        if body[0] != INPUT_STREAM_CONTROL_VERSION {
            return Err(InputStreamControlError::UnsupportedVersion(body[0]));
        }
        if body.len() < PREFIX_BYTES {
            return Err(InputStreamControlError::Truncated {
                needed: PREFIX_BYTES,
                got: body.len(),
            });
        }
        let match_id = u64::from_be_bytes(body[2..10].try_into().expect("fixed control match id"));
        let stream_id =
            u64::from_be_bytes(body[10..18].try_into().expect("fixed control stream id"));
        match body[1] {
            INPUT_STREAM_CONTROL_ADVERTISE => {
                let expected = PREFIX_BYTES + INPUT_STREAM_TOKEN_BYTES;
                if body.len() < expected {
                    return Err(InputStreamControlError::Truncated {
                        needed: expected,
                        got: body.len(),
                    });
                }
                if body.len() > expected {
                    return Err(InputStreamControlError::TrailingBytes(
                        body.len() - expected,
                    ));
                }
                let mut raw_token = [0; INPUT_STREAM_TOKEN_BYTES];
                raw_token.copy_from_slice(&body[PREFIX_BYTES..expected]);
                Ok(Self::Advertise {
                    match_id,
                    stream_id,
                    token: InputStreamToken::new(raw_token)?,
                })
            }
            INPUT_STREAM_CONTROL_REVOKE => {
                if body.len() > PREFIX_BYTES {
                    return Err(InputStreamControlError::TrailingBytes(
                        body.len() - PREFIX_BYTES,
                    ));
                }
                Ok(Self::Revoke {
                    match_id,
                    stream_id,
                })
            }
            operation => Err(InputStreamControlError::InvalidOperation(operation)),
        }
    }
}

/// Version-1 stream-bound sequenced custom input: `version:u8 |
/// stream_token:bytes[16] | sequence:u64 BE | original_custom_kind:u16 BE |
/// body_len:u32 BE | body`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedInput {
    /// Opaque server-issued token required to correlate this input to a stream.
    pub stream_token: InputStreamToken,
    /// Client-selected nonzero stream sequence.
    pub sequence: u64,
    /// The original custom envelope kind, preserved without interpretation.
    pub original_custom_kind: u16,
    /// Bounded opaque custom body.
    pub body: Vec<u8>,
}

impl SequencedInput {
    /// Fixed prefix before the opaque body.
    pub const PREFIX_BYTES: usize = 1 + INPUT_STREAM_TOKEN_BYTES + 8 + 2 + 4;

    /// Encode one canonical stream-bound sequenced custom input.
    pub fn encode(&self) -> Result<Vec<u8>, AuthoritativeInputError> {
        require_nonzero_sequence(self.sequence)?;
        require_body_len("body", self.body.len(), MAX_SEQUENCED_INPUT_BODY_BYTES)?;
        let mut out = Vec::with_capacity(Self::PREFIX_BYTES + self.body.len());
        out.push(AUTHORITATIVE_INPUT_VERSION);
        out.extend_from_slice(self.stream_token.as_bytes());
        out.extend_from_slice(&self.sequence.to_be_bytes());
        out.extend_from_slice(&self.original_custom_kind.to_be_bytes());
        out.extend_from_slice(&(self.body.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.body);
        Ok(out)
    }

    /// Decode exactly one canonical stream-bound sequenced custom input.
    pub fn decode(body: &[u8]) -> Result<Self, AuthoritativeInputError> {
        let mut reader = Reader::new(body);
        reader.version()?;
        let stream_token = InputStreamToken::decode(&mut reader)?;
        let sequence = reader.u64()?;
        require_nonzero_sequence(sequence)?;
        let original_custom_kind = reader.u16()?;
        let body_len = reader.u32()? as usize;
        require_body_len("body", body_len, MAX_SEQUENCED_INPUT_BODY_BYTES)?;
        let body = reader.bytes(body_len)?.to_vec();
        reader.finish()?;
        Ok(Self {
            stream_token,
            sequence,
            original_custom_kind,
            body,
        })
    }
}

/// A generic, server-authoritative decision about input processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AuthoritativeInputDisposition {
    /// The authoritative server accepted the input.
    Accepted = 0,
    /// The authoritative server rejected the input.
    Rejected = 1,
}

impl AuthoritativeInputDisposition {
    fn encode(self) -> u8 {
        self as u8
    }

    fn decode(value: u8) -> Result<Self, AuthoritativeInputError> {
        match value {
            0 => Ok(Self::Accepted),
            1 => Ok(Self::Rejected),
            _ => Err(AuthoritativeInputError::Invalid("receipt disposition")),
        }
    }
}

/// Version-1 stream-bound authoritative input receipt: `version:u8 | match_id:u64
/// BE | stream_id:u64 BE | stream_token:bytes[16] | acknowledged_sequence:u64 BE |
/// decided_sequence:u64 BE | disposition:u8 | authoritative_tick:u64 BE |
/// correction_present:u8 | correction_len:u32 BE | correction`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputReceipt {
    /// Server-owned correlation for the match scope.
    pub match_id: u64,
    /// Server-owned correlation for the input stream.
    pub stream_id: u64,
    /// Opaque server-issued token for the correlated input stream.
    pub stream_token: InputStreamToken,
    /// Highest contiguous processed sequence; zero means no contiguous input.
    pub acknowledged_sequence: u64,
    /// Nonzero sequence of the input decided by this receipt.
    pub decided_sequence: u64,
    /// Coarse authoritative result, intentionally without product-specific detail.
    pub disposition: AuthoritativeInputDisposition,
    /// Server-owned simulation tick at the authoritative decision.
    pub authoritative_tick: u64,
    /// Optional bounded opaque correction bytes.
    pub correction: Option<Vec<u8>>,
}

impl InputReceipt {
    /// Fixed prefix before an optional opaque correction.
    pub const PREFIX_BYTES: usize = 1 + 8 + 8 + INPUT_STREAM_TOKEN_BYTES + 8 + 8 + 1 + 8 + 1 + 4;

    /// Encode one canonical stream-bound authoritative input receipt.
    pub fn encode(&self) -> Result<Vec<u8>, AuthoritativeInputError> {
        require_nonzero_sequence(self.decided_sequence)?;
        let correction = self.correction.as_deref().unwrap_or_default();
        require_body_len(
            "correction",
            correction.len(),
            MAX_SEQUENCED_INPUT_BODY_BYTES,
        )?;
        let mut out = Vec::with_capacity(Self::PREFIX_BYTES + correction.len());
        out.push(AUTHORITATIVE_INPUT_VERSION);
        out.extend_from_slice(&self.match_id.to_be_bytes());
        out.extend_from_slice(&self.stream_id.to_be_bytes());
        out.extend_from_slice(self.stream_token.as_bytes());
        out.extend_from_slice(&self.acknowledged_sequence.to_be_bytes());
        out.extend_from_slice(&self.decided_sequence.to_be_bytes());
        out.push(self.disposition.encode());
        out.extend_from_slice(&self.authoritative_tick.to_be_bytes());
        out.push(u8::from(self.correction.is_some()));
        out.extend_from_slice(&(correction.len() as u32).to_be_bytes());
        out.extend_from_slice(correction);
        Ok(out)
    }

    /// Decode exactly one canonical stream-bound authoritative input receipt.
    pub fn decode(body: &[u8]) -> Result<Self, AuthoritativeInputError> {
        let mut reader = Reader::new(body);
        reader.version()?;
        let match_id = reader.u64()?;
        let stream_id = reader.u64()?;
        let stream_token = InputStreamToken::decode(&mut reader)?;
        let acknowledged_sequence = reader.u64()?;
        let decided_sequence = reader.u64()?;
        require_nonzero_sequence(decided_sequence)?;
        let disposition = AuthoritativeInputDisposition::decode(reader.u8()?)?;
        let authoritative_tick = reader.u64()?;
        let correction_present = match reader.u8()? {
            0 => false,
            1 => true,
            _ => {
                return Err(AuthoritativeInputError::Invalid(
                    "receipt correction presence",
                ));
            }
        };
        let correction_len = reader.u32()? as usize;
        require_body_len("correction", correction_len, MAX_SEQUENCED_INPUT_BODY_BYTES)?;
        let correction_bytes = reader.bytes(correction_len)?;
        if !correction_present && correction_len != 0 {
            return Err(AuthoritativeInputError::Invalid(
                "receipt absent correction length",
            ));
        }
        let correction = correction_present.then(|| correction_bytes.to_vec());
        reader.finish()?;
        Ok(Self {
            match_id,
            stream_id,
            stream_token,
            acknowledged_sequence,
            decided_sequence,
            disposition,
            authoritative_tick,
            correction,
        })
    }
}

fn require_nonzero_sequence(sequence: u64) -> Result<(), AuthoritativeInputError> {
    if sequence == 0 {
        Err(AuthoritativeInputError::ZeroSequence)
    } else {
        Ok(())
    }
}

fn require_nonzero_stream_token(
    token: &[u8; INPUT_STREAM_TOKEN_BYTES],
) -> Result<(), AuthoritativeInputError> {
    if token.iter().all(|byte| *byte == 0) {
        Err(AuthoritativeInputError::AllZeroStreamToken)
    } else {
        Ok(())
    }
}

fn require_body_len(
    field: &'static str,
    actual: usize,
    max: usize,
) -> Result<(), AuthoritativeInputError> {
    if actual > max {
        Err(AuthoritativeInputError::TooLarge { field, max, actual })
    } else {
        Ok(())
    }
}

struct Reader<'a> {
    body: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(body: &'a [u8]) -> Self {
        Self { body, offset: 0 }
    }

    fn version(&mut self) -> Result<(), AuthoritativeInputError> {
        let version = self.u8()?;
        if version != AUTHORITATIVE_INPUT_VERSION {
            return Err(AuthoritativeInputError::UnsupportedVersion(version));
        }
        Ok(())
    }

    fn u8(&mut self) -> Result<u8, AuthoritativeInputError> {
        Ok(self.bytes(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, AuthoritativeInputError> {
        let bytes = self.bytes(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32, AuthoritativeInputError> {
        let bytes = self.bytes(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, AuthoritativeInputError> {
        let bytes = self.bytes(8)?;
        Ok(u64::from_be_bytes(
            bytes.try_into().expect("exact u64 bytes"),
        ))
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8], AuthoritativeInputError> {
        let end = self.offset.saturating_add(len);
        if end > self.body.len() {
            return Err(AuthoritativeInputError::Truncated {
                needed: end,
                got: self.body.len(),
            });
        }
        let bytes = &self.body[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn finish(self) -> Result<(), AuthoritativeInputError> {
        let trailing = self.body.len() - self.offset;
        if trailing == 0 {
            Ok(())
        } else {
            Err(AuthoritativeInputError::TrailingBytes(trailing))
        }
    }
}
