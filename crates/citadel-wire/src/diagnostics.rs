//! Versioned realtime controls for opt-in lag diagnostics.
//!
//! These controls deliberately keep three time domains separate:
//!
//! - `client_*_mono_us` values are a client's monotonic elapsed time;
//! - `server_*_utc_*` values are UTC correlation metadata only; and
//! - gameplay epoch/tick metadata remains in [`crate::tsync`].
//!
//! In particular, the exchange in this module is suitable for estimating a
//! server-UTC offset and its uncertainty. It is not evidence of one-way
//! latency and must not be labelled as such.

use std::fmt;

/// Current diagnostics-control body version.
pub const DIAGNOSTICS_VERSION: u8 = 1;
/// Maximum number of requested packet filters in a capture.
pub const MAX_CAPTURE_FILTERS: usize = 16;
/// Largest server-requested recorder budget accepted by v1, in bytes.
pub const MAX_CAPTURE_BYTES: u32 = 64 * 1024 * 1024;
/// The only same-origin HTTP route that accepts a diagnostics artifact. Capture
/// identifiers and bearer capabilities deliberately never appear in its URL.
pub const DIAGNOSTICS_UPLOAD_PATH: &str = "/v1/diagnostics/captures/upload";
/// Bounded byte length for the fixed same-origin upload path in `FLUSH`.
pub const MAX_UPLOAD_PATH_BYTES: usize = 128;
/// Bounded byte length for a compact signed upload capability in `FLUSH`.
pub const MAX_UPLOAD_TOKEN_BYTES: usize = 2_048;

/// Capability bit asserted only by an SDK whose immutable local configuration
/// has enabled diagnostics recording.
pub const CAPABILITY_RECORDING: u16 = 0x0001;
/// Capability bits understood by this version.
pub const KNOWN_CAPABILITIES: u16 = CAPABILITY_RECORDING;

/// Stable decode/validation errors for diagnostics controls.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DiagnosticsError {
    /// A body did not contain its required prefix.
    #[error("diagnostics body is truncated: expected at least {needed} bytes, got {got}")]
    Truncated { needed: usize, got: usize },
    /// The version byte is unknown and must never be guessed at.
    #[error("unsupported diagnostics version {0}")]
    UnsupportedVersion(u8),
    /// A fixed-layout body contained unrecognised trailing bytes.
    #[error("diagnostics body has {0} trailing bytes")]
    TrailingBytes(usize),
    /// A nonzero protocol identifier was encoded as zero.
    #[error("diagnostics field {0} must be nonzero")]
    Zero(&'static str),
    /// A bounded count or size fell outside its contract.
    #[error("diagnostics field {field} is out of range: {value}")]
    OutOfRange { field: &'static str, value: u64 },
    /// A bitset includes capability bits this version does not understand.
    #[error("diagnostics capabilities contain unknown bits {0:#x}")]
    UnknownCapabilities(u16),
    /// A direction or discriminator is not part of the v1 domain.
    #[error("invalid diagnostics {0}")]
    Invalid(&'static str),
}

/// A nonzero, server-minted 128-bit capture identity.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CaptureId([u8; 16]);

impl CaptureId {
    /// Construct a capture id, rejecting the all-zero sentinel.
    pub fn new(value: [u8; 16]) -> Result<Self, DiagnosticsError> {
        if value.iter().all(|byte| *byte == 0) {
            return Err(DiagnosticsError::Zero("capture_id"));
        }
        Ok(Self(value))
    }

    /// Return the exact wire bytes.
    #[must_use]
    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for CaptureId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CaptureId(..)")
    }
}

/// `KIND_DIAG_SERVER_TIME` (S->C, reliable), sent after `AUTH_RESULT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerTime {
    /// Opaque nonzero offer to be echoed by `Capabilities`.
    pub offer_id: u64,
    /// Server-local UTC Unix epoch milliseconds for correlation only.
    pub server_utc_ms: u64,
}

impl ServerTime {
    /// Fixed v1 body size.
    pub const BYTES: usize = 1 + 8 + 8;

    /// Encode the fixed v1 body.
    pub fn encode(self) -> Result<Vec<u8>, DiagnosticsError> {
        require_nonzero(self.offer_id, "offer_id")?;
        require_nonzero(self.server_utc_ms, "server_utc_ms")?;
        let mut out = Vec::with_capacity(Self::BYTES);
        out.push(DIAGNOSTICS_VERSION);
        put_u64(&mut out, self.offer_id);
        put_u64(&mut out, self.server_utc_ms);
        Ok(out)
    }

    /// Decode a v1 body and reject both unknown versions and trailing bytes.
    pub fn decode(body: &[u8]) -> Result<Self, DiagnosticsError> {
        let mut reader = Reader::new(body);
        reader.version()?;
        let value = Self {
            offer_id: reader.u64()?,
            server_utc_ms: reader.u64()?,
        };
        require_nonzero(value.offer_id, "offer_id")?;
        require_nonzero(value.server_utc_ms, "server_utc_ms")?;
        reader.finish()?;
        Ok(value)
    }
}

/// `KIND_DIAG_CAPABILITIES` (C->S, reliable).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// The exact `ServerTime.offer_id` this SDK observed after authentication.
    pub offer_id: u64,
    /// Feature bits enabled by immutable SDK code configuration.
    pub features: u16,
}

impl Capabilities {
    /// Fixed v1 body size.
    pub const BYTES: usize = 1 + 8 + 2;

    /// Whether the SDK locally enabled recording diagnostics.
    #[must_use]
    pub const fn recording_enabled(self) -> bool {
        self.features & CAPABILITY_RECORDING != 0
    }

    /// Encode a valid v1 capabilities assertion.
    pub fn encode(self) -> Result<Vec<u8>, DiagnosticsError> {
        require_nonzero(self.offer_id, "offer_id")?;
        validate_capabilities(self.features)?;
        let mut out = Vec::with_capacity(Self::BYTES);
        out.push(DIAGNOSTICS_VERSION);
        put_u64(&mut out, self.offer_id);
        put_u16(&mut out, self.features);
        Ok(out)
    }

    /// Decode a valid v1 capabilities assertion.
    pub fn decode(body: &[u8]) -> Result<Self, DiagnosticsError> {
        let mut reader = Reader::new(body);
        reader.version()?;
        let value = Self {
            offer_id: reader.u64()?,
            features: reader.u16()?,
        };
        require_nonzero(value.offer_id, "offer_id")?;
        validate_capabilities(value.features)?;
        reader.finish()?;
        Ok(value)
    }
}

/// A bounded NTP-style time correlation exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockSync {
    /// Client's request with its own monotonic send time.
    Request {
        /// Client-chosen probe correlation sequence (zero is valid).
        sequence: u32,
        /// Client monotonic elapsed microseconds at send (`t0`).
        client_sent_mono_us: u64,
    },
    /// Server response containing receive and send UTC timestamps (`t1`, `t2`).
    Response {
        /// Echoed request sequence.
        sequence: u32,
        /// Echoed client monotonic send time.
        client_sent_mono_us: u64,
        /// Server UTC microseconds immediately after receipt.
        server_received_utc_us: u64,
        /// Server UTC microseconds immediately before response enqueue.
        server_sent_utc_us: u64,
    },
}

impl ClockSync {
    /// Request body length.
    pub const REQUEST_BYTES: usize = 1 + 1 + 4 + 8;
    /// Response body length.
    pub const RESPONSE_BYTES: usize = Self::REQUEST_BYTES + 8 + 8;
    const REQUEST_TAG: u8 = 0;
    const RESPONSE_TAG: u8 = 1;

    /// Encode the exact v1 request or response body.
    pub fn encode(self) -> Result<Vec<u8>, DiagnosticsError> {
        let mut out = Vec::with_capacity(match self {
            Self::Request { .. } => Self::REQUEST_BYTES,
            Self::Response { .. } => Self::RESPONSE_BYTES,
        });
        out.push(DIAGNOSTICS_VERSION);
        match self {
            Self::Request {
                sequence,
                client_sent_mono_us,
            } => {
                out.push(Self::REQUEST_TAG);
                put_u32(&mut out, sequence);
                put_u64(&mut out, client_sent_mono_us);
            }
            Self::Response {
                sequence,
                client_sent_mono_us,
                server_received_utc_us,
                server_sent_utc_us,
            } => {
                require_nonzero(server_received_utc_us, "server_received_utc_us")?;
                require_nonzero(server_sent_utc_us, "server_sent_utc_us")?;
                if server_sent_utc_us < server_received_utc_us {
                    return Err(DiagnosticsError::Invalid(
                        "clock_sync server timestamp order",
                    ));
                }
                out.push(Self::RESPONSE_TAG);
                put_u32(&mut out, sequence);
                put_u64(&mut out, client_sent_mono_us);
                put_u64(&mut out, server_received_utc_us);
                put_u64(&mut out, server_sent_utc_us);
            }
        }
        Ok(out)
    }

    /// Decode one exact v1 exchange body.
    pub fn decode(body: &[u8]) -> Result<Self, DiagnosticsError> {
        let mut reader = Reader::new(body);
        reader.version()?;
        let tag = reader.u8()?;
        let sequence = reader.u32()?;
        let client_sent_mono_us = reader.u64()?;
        let result = match tag {
            Self::REQUEST_TAG => Self::Request {
                sequence,
                client_sent_mono_us,
            },
            Self::RESPONSE_TAG => {
                let server_received_utc_us = reader.u64()?;
                let server_sent_utc_us = reader.u64()?;
                require_nonzero(server_received_utc_us, "server_received_utc_us")?;
                require_nonzero(server_sent_utc_us, "server_sent_utc_us")?;
                if server_sent_utc_us < server_received_utc_us {
                    return Err(DiagnosticsError::Invalid(
                        "clock_sync server timestamp order",
                    ));
                }
                Self::Response {
                    sequence,
                    client_sent_mono_us,
                    server_received_utc_us,
                    server_sent_utc_us,
                }
            }
            _ => return Err(DiagnosticsError::Invalid("clock_sync direction")),
        };
        reader.finish()?;
        Ok(result)
    }
}

/// Correlation result derived on a client after it receives a `ClockSync` response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockCorrelation {
    /// Estimated `server_utc_us - client_monotonic_us`; graph alignment only.
    pub server_utc_offset_us: i64,
    /// NTP round-trip delay after removing server processing time.
    pub round_trip_delay_us: u64,
    /// Conservative uncertainty bound: half the observed round trip.
    pub uncertainty_us: u64,
}

impl ClockCorrelation {
    /// Calculate correlation using NTP's four timestamps. `client_received_mono_us`
    /// is captured locally when the response arrives. Returns `None` for an
    /// impossible ordering rather than manufacturing a sample.
    #[must_use]
    pub fn from_response(response: ClockSync, client_received_mono_us: u64) -> Option<Self> {
        let ClockSync::Response {
            client_sent_mono_us,
            server_received_utc_us,
            server_sent_utc_us,
            ..
        } = response
        else {
            return None;
        };
        if client_received_mono_us < client_sent_mono_us
            || server_sent_utc_us < server_received_utc_us
        {
            return None;
        }
        let client_elapsed = client_received_mono_us - client_sent_mono_us;
        let server_elapsed = server_sent_utc_us - server_received_utc_us;
        let round_trip_delay_us = client_elapsed.checked_sub(server_elapsed)?;
        let first = i128::from(server_received_utc_us) - i128::from(client_sent_mono_us);
        let second = i128::from(server_sent_utc_us) - i128::from(client_received_mono_us);
        let offset = (first + second) / 2;
        Some(Self {
            server_utc_offset_us: offset.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
            round_trip_delay_us,
            uncertainty_us: round_trip_delay_us / 2,
        })
    }
}

/// Packet direction used in server-requested recording filters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketDirection {
    /// Packet received by this SDK from the server.
    Inbound = 0,
    /// Packet sent by this SDK to the server.
    Outbound = 1,
}

impl PacketDirection {
    fn decode(value: u8) -> Result<Self, DiagnosticsError> {
        match value {
            0 => Ok(Self::Inbound),
            1 => Ok(Self::Outbound),
            _ => Err(DiagnosticsError::Invalid("packet filter direction")),
        }
    }
}

/// A constrained recording filter. `entity_id: None` means every entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketFilter {
    /// Realtime envelope kind to record.
    pub kind: u16,
    /// Direction relative to the SDK.
    pub direction: PacketDirection,
    /// Optional nonzero entity/session selector.
    pub entity_id: Option<u64>,
}

impl PacketFilter {
    const BYTES: usize = 2 + 1 + 1 + 8;

    fn encode_into(self, out: &mut Vec<u8>) -> Result<(), DiagnosticsError> {
        if self.kind == 0 {
            return Err(DiagnosticsError::Zero("filter.kind"));
        }
        put_u16(out, self.kind);
        out.push(self.direction as u8);
        match self.entity_id {
            Some(id) => {
                require_nonzero(id, "filter.entity_id")?;
                out.push(1);
                put_u64(out, id);
            }
            None => {
                out.push(0);
                put_u64(out, 0);
            }
        }
        Ok(())
    }

    fn decode(reader: &mut Reader<'_>) -> Result<Self, DiagnosticsError> {
        let kind = reader.u16()?;
        require_nonzero(u64::from(kind), "filter.kind")?;
        let direction = PacketDirection::decode(reader.u8()?)?;
        let entity_tag = reader.u8()?;
        let raw_entity = reader.u64()?;
        let entity_id = match entity_tag {
            0 if raw_entity == 0 => None,
            1 => {
                require_nonzero(raw_entity, "filter.entity_id")?;
                Some(raw_entity)
            }
            _ => return Err(DiagnosticsError::Invalid("packet filter entity selector")),
        };
        Ok(Self {
            kind,
            direction,
            entity_id,
        })
    }
}

/// `KIND_DIAG_START` (S->C, reliable).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartCapture {
    /// Server-minted capture identity.
    pub capture_id: CaptureId,
    /// Immutable nonzero generation; never reuse it for a retry attempt.
    pub generation: u64,
    /// Server UTC deadline for recording, in Unix milliseconds.
    pub deadline_server_utc_ms: u64,
    /// Hard local recorder byte ceiling requested by the server.
    pub max_record_bytes: u32,
    /// Bounded server-requested packet metadata filters.
    pub filters: Vec<PacketFilter>,
}

impl StartCapture {
    /// Encode a bounded v1 START body.
    pub fn encode(&self) -> Result<Vec<u8>, DiagnosticsError> {
        validate_start(self)?;
        let mut out =
            Vec::with_capacity(1 + 16 + 8 + 8 + 4 + 1 + self.filters.len() * PacketFilter::BYTES);
        out.push(DIAGNOSTICS_VERSION);
        out.extend_from_slice(&self.capture_id.bytes());
        put_u64(&mut out, self.generation);
        put_u64(&mut out, self.deadline_server_utc_ms);
        put_u32(&mut out, self.max_record_bytes);
        out.push(self.filters.len() as u8);
        for filter in &self.filters {
            filter.encode_into(&mut out)?;
        }
        Ok(out)
    }

    /// Decode a bounded v1 START body.
    pub fn decode(body: &[u8]) -> Result<Self, DiagnosticsError> {
        let mut reader = Reader::new(body);
        reader.version()?;
        let capture_id = CaptureId::new(reader.array16()?)?;
        let generation = reader.u64()?;
        let deadline_server_utc_ms = reader.u64()?;
        let max_record_bytes = reader.u32()?;
        let count = usize::from(reader.u8()?);
        if count > MAX_CAPTURE_FILTERS {
            return Err(DiagnosticsError::OutOfRange {
                field: "filters",
                value: count as u64,
            });
        }
        let mut filters = Vec::with_capacity(count);
        for _ in 0..count {
            filters.push(PacketFilter::decode(&mut reader)?);
        }
        reader.finish()?;
        let value = Self {
            capture_id,
            generation,
            deadline_server_utc_ms,
            max_record_bytes,
            filters,
        };
        validate_start(&value)?;
        Ok(value)
    }
}

/// MIME type accepted by the diagnostics ingest route. The binary CLAG
/// validator remains authoritative; this is only a cheap HTTP precheck.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UploadContentType {
    /// `application/vnd.citadel.lag-capture`.
    CitadelLagCapture = 1,
}

impl UploadContentType {
    /// HTTP content type string used by the upload client and server precheck.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CitadelLagCapture => "application/vnd.citadel.lag-capture",
        }
    }

    fn decode(value: u8) -> Result<Self, DiagnosticsError> {
        match value {
            1 => Ok(Self::CitadelLagCapture),
            _ => Err(DiagnosticsError::Invalid("upload content type")),
        }
    }
}

/// Content encoding accepted by the diagnostics ingest route. Captures are
/// intentionally compressed before upload so a prolonged recording cannot
/// turn into an unbounded network transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UploadContentEncoding {
    /// RFC 1952 gzip framing around the CLAG payload.
    Gzip = 1,
}

impl UploadContentEncoding {
    /// HTTP content encoding string used by the upload client and server precheck.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
        }
    }

    fn decode(value: u8) -> Result<Self, DiagnosticsError> {
        match value {
            1 => Ok(Self::Gzip),
            _ => Err(DiagnosticsError::Invalid("upload content encoding")),
        }
    }
}

/// `KIND_DIAG_FLUSH` (S->C, reliable). It carries one opaque, signed,
/// short-lived capability for this exact participant and attempt. The token is
/// never echoed in `STATUS`, logged through `Debug`, or placed in a URL.
#[derive(Clone, PartialEq, Eq)]
pub struct FlushCapture {
    /// Target capture.
    pub capture_id: CaptureId,
    /// Must equal the generation from START.
    pub generation: u64,
    /// Fresh per-FLUSH attempt identifier; distinct from capture generation.
    pub attempt_id: u64,
    /// Server UTC upload deadline, Unix milliseconds.
    pub upload_deadline_server_utc_ms: u64,
    /// Maximum compressed HTTP body bytes for this exact one-use upload.
    pub max_compressed_bytes: u32,
    /// MIME precheck required by the permanent state-gated ingest route.
    pub content_type: UploadContentType,
    /// Compression precheck required by the permanent state-gated ingest route.
    pub content_encoding: UploadContentEncoding,
    /// Exact same-origin relative upload route. It contains no capture identity
    /// and no secret and therefore can be safely carried on the realtime wire.
    pub upload_path: String,
    /// One-use opaque signed capability. This must be sent only as an HTTP
    /// `Authorization: Bearer` value and is intentionally redacted in Debug.
    pub upload_token: String,
}

impl fmt::Debug for FlushCapture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlushCapture")
            .field("capture_id", &self.capture_id)
            .field("generation", &self.generation)
            .field("attempt_id", &self.attempt_id)
            .field(
                "upload_deadline_server_utc_ms",
                &self.upload_deadline_server_utc_ms,
            )
            .field("max_compressed_bytes", &self.max_compressed_bytes)
            .field("content_type", &self.content_type)
            .field("content_encoding", &self.content_encoding)
            .field("upload_path", &self.upload_path)
            .field("upload_token", &"[redacted]")
            .finish()
    }
}

impl FlushCapture {
    /// Minimum v1 body size, excluding the bounded path and token bytes.
    pub const MIN_BYTES: usize = 1 + 16 + 8 + 8 + 8 + 4 + 1 + 1 + 2 + 2;

    /// Encode a v1 FLUSH body.
    pub fn encode(&self) -> Result<Vec<u8>, DiagnosticsError> {
        validate_flush(self)?;
        let path = self.upload_path.as_bytes();
        let token = self.upload_token.as_bytes();
        let mut out = Vec::with_capacity(Self::MIN_BYTES + path.len() + token.len());
        out.push(DIAGNOSTICS_VERSION);
        out.extend_from_slice(&self.capture_id.bytes());
        put_u64(&mut out, self.generation);
        put_u64(&mut out, self.attempt_id);
        put_u64(&mut out, self.upload_deadline_server_utc_ms);
        put_u32(&mut out, self.max_compressed_bytes);
        out.push(self.content_type as u8);
        out.push(self.content_encoding as u8);
        put_u16(&mut out, path.len() as u16);
        put_u16(&mut out, token.len() as u16);
        out.extend_from_slice(path);
        out.extend_from_slice(token);
        Ok(out)
    }

    /// Decode a v1 FLUSH body.
    pub fn decode(body: &[u8]) -> Result<Self, DiagnosticsError> {
        let mut reader = Reader::new(body);
        reader.version()?;
        let capture_id = CaptureId::new(reader.array16()?)?;
        let generation = reader.u64()?;
        let attempt_id = reader.u64()?;
        let upload_deadline_server_utc_ms = reader.u64()?;
        let max_compressed_bytes = reader.u32()?;
        let content_type = UploadContentType::decode(reader.u8()?)?;
        let content_encoding = UploadContentEncoding::decode(reader.u8()?)?;
        let upload_path_len = usize::from(reader.u16()?);
        let upload_token_len = usize::from(reader.u16()?);
        if upload_path_len > MAX_UPLOAD_PATH_BYTES {
            return Err(DiagnosticsError::OutOfRange {
                field: "upload_path",
                value: upload_path_len as u64,
            });
        }
        if upload_token_len > MAX_UPLOAD_TOKEN_BYTES {
            return Err(DiagnosticsError::OutOfRange {
                field: "upload_token",
                value: upload_token_len as u64,
            });
        }
        let upload_path = std::str::from_utf8(reader.bytes(upload_path_len)?)
            .map_err(|_| DiagnosticsError::Invalid("upload path encoding"))?
            .to_owned();
        let upload_token = std::str::from_utf8(reader.bytes(upload_token_len)?)
            .map_err(|_| DiagnosticsError::Invalid("upload token encoding"))?
            .to_owned();
        let value = Self {
            capture_id,
            generation,
            attempt_id,
            upload_deadline_server_utc_ms,
            max_compressed_bytes,
            content_type,
            content_encoding,
            upload_path,
            upload_token,
        };
        reader.finish()?;
        validate_flush(&value)?;
        Ok(value)
    }
}

/// Client-reported capture state. A server records this as an assertion from the
/// authenticated session; a queue success is deliberately not this state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CaptureStatusCode {
    /// The client accepted START and is recording. This is the first expected uploader state.
    Recording = 1,
    /// The client began a FLUSH attempt.
    UploadStarted = 2,
    /// The client observed a completed upload response.
    Uploaded = 3,
    /// The client declined/failed the requested work; counters may still aid diagnosis.
    Failed = 4,
}

impl CaptureStatusCode {
    fn decode(value: u8) -> Result<Self, DiagnosticsError> {
        match value {
            1 => Ok(Self::Recording),
            2 => Ok(Self::UploadStarted),
            3 => Ok(Self::Uploaded),
            4 => Ok(Self::Failed),
            _ => Err(DiagnosticsError::Invalid("capture status code")),
        }
    }
}

/// `KIND_DIAG_STATUS` (C->S, reliable), never carrying a URL or upload token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaptureStatus {
    /// Target capture.
    pub capture_id: CaptureId,
    /// START generation fence.
    pub generation: u64,
    /// Client progress/terminal state.
    pub code: CaptureStatusCode,
    /// FLUSH attempt fence. It is zero while reporting `Recording`; upload
    /// progress must echo the nonzero attempt id from FLUSH.
    pub attempt_id: u64,
    /// Number of events retained in the recording snapshot.
    pub recorded_packets: u32,
    /// Deterministic local ring-buffer drops/overwrites.
    pub dropped_packets: u32,
    /// Serialized/compressed-independent raw recording bytes.
    pub recorded_bytes: u32,
}

impl CaptureStatus {
    /// Fixed v1 body size.
    pub const BYTES: usize = 1 + 16 + 8 + 1 + 8 + 4 + 4 + 4;

    /// Encode the fixed v1 status body.
    pub fn encode(self) -> Result<Vec<u8>, DiagnosticsError> {
        require_nonzero(self.generation, "generation")?;
        let mut out = Vec::with_capacity(Self::BYTES);
        out.push(DIAGNOSTICS_VERSION);
        out.extend_from_slice(&self.capture_id.bytes());
        put_u64(&mut out, self.generation);
        out.push(self.code as u8);
        validate_status_attempt(self.code, self.attempt_id)?;
        put_u64(&mut out, self.attempt_id);
        put_u32(&mut out, self.recorded_packets);
        put_u32(&mut out, self.dropped_packets);
        put_u32(&mut out, self.recorded_bytes);
        Ok(out)
    }

    /// Decode a fixed v1 status body.
    pub fn decode(body: &[u8]) -> Result<Self, DiagnosticsError> {
        let mut reader = Reader::new(body);
        reader.version()?;
        let value = Self {
            capture_id: CaptureId::new(reader.array16()?)?,
            generation: reader.u64()?,
            code: CaptureStatusCode::decode(reader.u8()?)?,
            attempt_id: reader.u64()?,
            recorded_packets: reader.u32()?,
            dropped_packets: reader.u32()?,
            recorded_bytes: reader.u32()?,
        };
        require_nonzero(value.generation, "generation")?;
        validate_status_attempt(value.code, value.attempt_id)?;
        reader.finish()?;
        Ok(value)
    }
}

fn validate_status_attempt(
    code: CaptureStatusCode,
    attempt_id: u64,
) -> Result<(), DiagnosticsError> {
    match code {
        CaptureStatusCode::Recording if attempt_id != 0 => {
            Err(DiagnosticsError::Invalid("recording status attempt_id"))
        }
        CaptureStatusCode::UploadStarted | CaptureStatusCode::Uploaded => {
            require_nonzero(attempt_id, "attempt_id")
        }
        CaptureStatusCode::Recording | CaptureStatusCode::Failed => Ok(()),
    }
}

fn validate_capabilities(features: u16) -> Result<(), DiagnosticsError> {
    if features == 0 {
        return Err(DiagnosticsError::Zero("capabilities"));
    }
    let unknown = features & !KNOWN_CAPABILITIES;
    if unknown != 0 {
        return Err(DiagnosticsError::UnknownCapabilities(unknown));
    }
    Ok(())
}

fn validate_start(value: &StartCapture) -> Result<(), DiagnosticsError> {
    require_nonzero(value.generation, "generation")?;
    require_nonzero(value.deadline_server_utc_ms, "deadline_server_utc_ms")?;
    if value.max_record_bytes == 0 || value.max_record_bytes > MAX_CAPTURE_BYTES {
        return Err(DiagnosticsError::OutOfRange {
            field: "max_record_bytes",
            value: u64::from(value.max_record_bytes),
        });
    }
    if value.filters.len() > MAX_CAPTURE_FILTERS {
        return Err(DiagnosticsError::OutOfRange {
            field: "filters",
            value: value.filters.len() as u64,
        });
    }
    if value.filters.is_empty() {
        return Err(DiagnosticsError::Zero("filters"));
    }
    for filter in &value.filters {
        // Validate through the exact serialiser so encode/decode share one domain.
        let mut ignored = Vec::with_capacity(PacketFilter::BYTES);
        filter.encode_into(&mut ignored)?;
    }
    Ok(())
}

fn validate_flush(value: &FlushCapture) -> Result<(), DiagnosticsError> {
    require_nonzero(value.generation, "generation")?;
    require_nonzero(value.attempt_id, "attempt_id")?;
    require_nonzero(
        value.upload_deadline_server_utc_ms,
        "upload_deadline_server_utc_ms",
    )?;
    if value.max_compressed_bytes == 0 || value.max_compressed_bytes > MAX_CAPTURE_BYTES {
        return Err(DiagnosticsError::OutOfRange {
            field: "max_compressed_bytes",
            value: u64::from(value.max_compressed_bytes),
        });
    }
    let path = value.upload_path.as_bytes();
    if path.len() > MAX_UPLOAD_PATH_BYTES {
        return Err(DiagnosticsError::OutOfRange {
            field: "upload_path",
            value: path.len() as u64,
        });
    }
    if value.upload_path != DIAGNOSTICS_UPLOAD_PATH {
        return Err(DiagnosticsError::Invalid("upload path"));
    }
    let token = value.upload_token.as_bytes();
    if token.is_empty() || token.len() > MAX_UPLOAD_TOKEN_BYTES {
        return Err(DiagnosticsError::OutOfRange {
            field: "upload_token",
            value: token.len() as u64,
        });
    }
    if !token
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.'))
    {
        return Err(DiagnosticsError::Invalid("upload token"));
    }
    Ok(())
}

fn require_nonzero(value: u64, field: &'static str) -> Result<(), DiagnosticsError> {
    if value == 0 {
        Err(DiagnosticsError::Zero(field))
    } else {
        Ok(())
    }
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

struct Reader<'a> {
    body: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(body: &'a [u8]) -> Self {
        Self { body, offset: 0 }
    }

    fn version(&mut self) -> Result<(), DiagnosticsError> {
        let version = self.u8()?;
        if version != DIAGNOSTICS_VERSION {
            return Err(DiagnosticsError::UnsupportedVersion(version));
        }
        Ok(())
    }

    fn u8(&mut self) -> Result<u8, DiagnosticsError> {
        self.need(1)?;
        let value = self.body[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16, DiagnosticsError> {
        self.need(2)?;
        let value = u16::from_be_bytes(
            self.body[self.offset..self.offset + 2]
                .try_into()
                .expect("exact"),
        );
        self.offset += 2;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32, DiagnosticsError> {
        self.need(4)?;
        let value = u32::from_be_bytes(
            self.body[self.offset..self.offset + 4]
                .try_into()
                .expect("exact"),
        );
        self.offset += 4;
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64, DiagnosticsError> {
        self.need(8)?;
        let value = u64::from_be_bytes(
            self.body[self.offset..self.offset + 8]
                .try_into()
                .expect("exact"),
        );
        self.offset += 8;
        Ok(value)
    }

    fn array16(&mut self) -> Result<[u8; 16], DiagnosticsError> {
        self.need(16)?;
        let value = self.body[self.offset..self.offset + 16]
            .try_into()
            .expect("exact");
        self.offset += 16;
        Ok(value)
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], DiagnosticsError> {
        self.need(length)?;
        let value = &self.body[self.offset..self.offset + length];
        self.offset += length;
        Ok(value)
    }

    fn need(&self, length: usize) -> Result<(), DiagnosticsError> {
        let needed = self.offset.saturating_add(length);
        if self.body.len() < needed {
            return Err(DiagnosticsError::Truncated {
                needed,
                got: self.body.len(),
            });
        }
        Ok(())
    }

    fn finish(&self) -> Result<(), DiagnosticsError> {
        let trailing = self.body.len().saturating_sub(self.offset);
        if trailing == 0 {
            Ok(())
        } else {
            Err(DiagnosticsError::TrailingBytes(trailing))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capture_id() -> CaptureId {
        CaptureId::new([7; 16]).expect("nonzero id")
    }

    fn start() -> StartCapture {
        StartCapture {
            capture_id: capture_id(),
            generation: 2,
            deadline_server_utc_ms: 1_700_000_000_000,
            max_record_bytes: 4096,
            filters: vec![PacketFilter {
                kind: 9,
                direction: PacketDirection::Inbound,
                entity_id: Some(42),
            }],
        }
    }

    #[test]
    fn every_v1_control_round_trips_exactly() {
        let server_time = ServerTime {
            offer_id: 1,
            server_utc_ms: 2,
        };
        assert_eq!(
            ServerTime::decode(&server_time.encode().expect("encode")).expect("decode"),
            server_time
        );

        let capabilities = Capabilities {
            offer_id: 1,
            features: CAPABILITY_RECORDING,
        };
        assert_eq!(
            Capabilities::decode(&capabilities.encode().expect("encode")).expect("decode"),
            capabilities
        );

        let request = ClockSync::Request {
            sequence: 3,
            client_sent_mono_us: 4,
        };
        assert_eq!(
            ClockSync::decode(&request.encode().expect("encode")).expect("decode"),
            request
        );

        let response = ClockSync::Response {
            sequence: 3,
            client_sent_mono_us: 4,
            server_received_utc_us: 10,
            server_sent_utc_us: 11,
        };
        assert_eq!(
            ClockSync::decode(&response.encode().expect("encode")).expect("decode"),
            response
        );

        let start = start();
        assert_eq!(
            StartCapture::decode(&start.encode().expect("encode")).expect("decode"),
            start
        );

        let flush = FlushCapture {
            capture_id: capture_id(),
            generation: 2,
            attempt_id: 3,
            upload_deadline_server_utc_ms: 4,
            max_compressed_bytes: 4_096,
            content_type: UploadContentType::CitadelLagCapture,
            content_encoding: UploadContentEncoding::Gzip,
            upload_path: DIAGNOSTICS_UPLOAD_PATH.to_string(),
            upload_token: "fixture-token.01".to_string(),
        };
        assert_eq!(
            FlushCapture::decode(&flush.encode().expect("encode")).expect("decode"),
            flush
        );

        let status = CaptureStatus {
            capture_id: capture_id(),
            generation: 2,
            code: CaptureStatusCode::Recording,
            attempt_id: 0,
            recorded_packets: 3,
            dropped_packets: 4,
            recorded_bytes: 5,
        };
        assert_eq!(
            CaptureStatus::decode(&status.encode().expect("encode")).expect("decode"),
            status
        );
    }

    #[test]
    fn golden_start_fixture_is_stable() {
        let encoded = start().encode().expect("encode");
        assert_eq!(
            encoded,
            vec![
                1, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 1,
                139, 207, 229, 104, 0, 0, 0, 16, 0, 1, 0, 9, 0, 1, 0, 0, 0, 0, 0, 0, 0, 42,
            ]
        );
    }

    #[test]
    fn rejects_unknown_versions_truncation_and_trailing_bytes() {
        assert!(matches!(
            ServerTime::decode(&[]),
            Err(DiagnosticsError::Truncated { .. })
        ));
        assert!(matches!(
            ServerTime::decode(&[9; ServerTime::BYTES]),
            Err(DiagnosticsError::UnsupportedVersion(9))
        ));
        let mut body = ServerTime {
            offer_id: 1,
            server_utc_ms: 2,
        }
        .encode()
        .expect("encode");
        body.push(0);
        assert!(matches!(
            ServerTime::decode(&body),
            Err(DiagnosticsError::TrailingBytes(1))
        ));
    }

    #[test]
    fn rejects_zero_ids_unknown_capabilities_and_invalid_filter() {
        assert!(CaptureId::new([0; 16]).is_err());
        assert!(matches!(
            Capabilities {
                offer_id: 1,
                features: 0x8000
            }
            .encode(),
            Err(DiagnosticsError::UnknownCapabilities(0x8000))
        ));
        let mut invalid = start();
        invalid.filters[0].kind = 0;
        assert!(invalid.encode().is_err());
        assert!(
            CaptureStatus {
                capture_id: capture_id(),
                generation: 1,
                code: CaptureStatusCode::Uploaded,
                attempt_id: 0,
                recorded_packets: 0,
                dropped_packets: 0,
                recorded_bytes: 0,
            }
            .encode()
            .is_err()
        );
    }

    #[test]
    fn correlation_is_bounded_and_not_one_way_latency() {
        let sample = ClockSync::Response {
            sequence: 1,
            client_sent_mono_us: 1_000,
            server_received_utc_us: 10_120,
            server_sent_utc_us: 10_130,
        };
        let correlation = ClockCorrelation::from_response(sample, 1_080).expect("valid sample");
        assert_eq!(correlation.server_utc_offset_us, 9_085);
        assert_eq!(correlation.round_trip_delay_us, 70);
        assert_eq!(correlation.uncertainty_us, 35);
        assert!(ClockCorrelation::from_response(sample, 999).is_none());
    }
}
