//! Citadel realtime wire format: the [`Envelope`] and its codec.
//!
//! This crate is the single source of truth for Citadel's realtime byte format,
//! shared by the server and every client (the Rust SDK and demos) so they cannot
//! drift apart. It is transport-agnostic: the same envelope is carried over QUIC
//! datagrams/streams and WebSocket binary messages.
//!
//! Two complementary representations are provided:
//!
//! - [`Envelope::encode_framed`] / [`decode_framed`] produce a length-delimited
//!   binary frame for stream transports (QUIC reliable streams, WebSocket
//!   binary), where multiple envelopes may share one byte stream.
//! - [`Envelope::encode_datagram`] / [`decode_datagram`] produce a bare body for
//!   datagram transports (QUIC unreliable datagrams), where exactly one envelope
//!   occupies one datagram and framing is provided by the datagram boundary.
//!
//! The body is an opaque byte payload; higher layers attach typed message
//! semantics. The server maps [`WireError`] to its own error category at
//! transport boundaries.

use bytes::{Buf, BufMut, Bytes, BytesMut};

pub mod authoritative_input;
pub mod baseline;
pub mod bits;
pub mod codec;
pub mod diagnostics;
pub mod interest;
pub mod na;
pub mod netpeer;
pub mod protocol;
pub mod room;
pub mod schema;
pub mod tsync;

/// Maximum envelope body size accepted by the framed codec, in bytes.
///
/// Bounds memory per frame and rejects hostile length prefixes before
/// allocation. Datagram transports impose their own, smaller MTU-bound limit.
pub const MAX_FRAME_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Number of bytes used for the length prefix in the framed encoding.
pub const LENGTH_PREFIX_BYTES: usize = 4;

/// An error decoding a Citadel wire frame or datagram.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    /// A framed length prefix exceeded [`MAX_FRAME_BODY_BYTES`].
    #[error("frame body length {len} exceeds maximum {max}")]
    FrameTooLarge {
        /// Declared body length.
        len: usize,
        /// Configured maximum.
        max: usize,
    },
    /// A declared body length was too small to contain the envelope header.
    #[error("frame body length {len} too small to contain a header")]
    FrameTooSmall {
        /// Declared body length.
        len: usize,
    },
    /// A datagram was too short to contain the envelope header.
    #[error("datagram length {len} too small to contain a header")]
    DatagramTooSmall {
        /// Datagram length.
        len: usize,
    },
}

/// A wire-agnostic realtime envelope.
///
/// `kind` is a small numeric discriminant for the message family; `body` is the
/// opaque payload. Concrete message types and their (de)serialization layer
/// above this codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    /// Numeric message-family discriminant.
    pub kind: u16,
    /// Opaque payload bytes.
    pub body: Bytes,
}

impl Envelope {
    /// Construct an envelope from a kind and an owned byte payload.
    #[must_use]
    pub fn new(kind: u16, body: impl Into<Bytes>) -> Self {
        Self {
            kind,
            body: body.into(),
        }
    }

    /// Total encoded size of the framed form (prefix + header + body).
    #[must_use]
    pub fn framed_len(&self) -> usize {
        LENGTH_PREFIX_BYTES + self.datagram_len()
    }

    /// Encoded size of the datagram form (header + body), without a length
    /// prefix.
    #[must_use]
    pub fn datagram_len(&self) -> usize {
        // 2 bytes for `kind`, then the body.
        2 + self.body.len()
    }

    /// Encode the envelope as a length-delimited frame for stream transports.
    ///
    /// Layout: `u32` big-endian body length (kind + payload), then `u16`
    /// big-endian `kind`, then the payload bytes.
    #[must_use]
    pub fn encode_framed(&self) -> Bytes {
        let body_len = self.datagram_len();
        let mut buf = BytesMut::with_capacity(LENGTH_PREFIX_BYTES + body_len);
        // body_len fits in u32 for any payload below MAX_FRAME_BODY_BYTES.
        buf.put_u32(body_len as u32);
        buf.put_u16(self.kind);
        buf.put_slice(&self.body);
        buf.freeze()
    }

    /// Encode the envelope as a bare datagram body (no length prefix).
    ///
    /// Layout: `u16` big-endian `kind`, then the payload bytes. The datagram
    /// boundary provides framing.
    #[must_use]
    pub fn encode_datagram(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.datagram_len());
        buf.put_u16(self.kind);
        buf.put_slice(&self.body);
        buf.freeze()
    }
}

/// Decode a single length-delimited frame from `buf`.
///
/// On success the consumed bytes are advanced out of `buf` and the decoded
/// [`Envelope`] is returned. Returns `Ok(None)` when `buf` does not yet contain
/// a complete frame (the caller should read more bytes and retry). Returns a
/// [`WireError`] for a frame whose declared length is invalid.
pub fn decode_framed(buf: &mut BytesMut) -> Result<Option<Envelope>, WireError> {
    if buf.len() < LENGTH_PREFIX_BYTES {
        return Ok(None);
    }
    // Peek the length without consuming, so partial frames stay buffered.
    let body_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if body_len > MAX_FRAME_BODY_BYTES {
        return Err(WireError::FrameTooLarge {
            len: body_len,
            max: MAX_FRAME_BODY_BYTES,
        });
    }
    if body_len < 2 {
        return Err(WireError::FrameTooSmall { len: body_len });
    }
    if buf.len() < LENGTH_PREFIX_BYTES + body_len {
        return Ok(None);
    }
    buf.advance(LENGTH_PREFIX_BYTES);
    let kind = buf.get_u16();
    let payload_len = body_len - 2;
    let body = buf.split_to(payload_len).freeze();
    Ok(Some(Envelope { kind, body }))
}

/// Decode a single datagram body into an [`Envelope`].
///
/// The whole `data` slice is treated as one envelope. Returns a [`WireError`]
/// when the datagram is too short to contain a header.
pub fn decode_datagram(data: &[u8]) -> Result<Envelope, WireError> {
    if data.len() < 2 {
        return Err(WireError::DatagramTooSmall { len: data.len() });
    }
    let kind = u16::from_be_bytes([data[0], data[1]]);
    let body = Bytes::copy_from_slice(&data[2..]);
    Ok(Envelope { kind, body })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framed_round_trip_single_envelope() {
        let env = Envelope::new(7, &b"hello world"[..]);
        let frame = env.encode_framed();
        let mut buf = BytesMut::from(&frame[..]);
        let decoded = decode_framed(&mut buf)
            .expect("decode ok")
            .expect("complete frame");
        assert_eq!(decoded, env);
        assert!(buf.is_empty(), "all bytes consumed");
    }

    #[test]
    fn framed_round_trip_multiple_envelopes_in_one_buffer() {
        let a = Envelope::new(1, &b"aaa"[..]);
        let b = Envelope::new(2, &b"bbbb"[..]);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&a.encode_framed());
        buf.extend_from_slice(&b.encode_framed());

        let first = decode_framed(&mut buf).expect("ok").expect("first frame");
        assert_eq!(first, a);
        let second = decode_framed(&mut buf).expect("ok").expect("second frame");
        assert_eq!(second, b);
        assert!(decode_framed(&mut buf).expect("ok").is_none());
    }

    #[test]
    fn framed_returns_none_on_partial_prefix() {
        let mut buf = BytesMut::from(&[0u8, 0u8][..]);
        assert!(decode_framed(&mut buf).expect("ok").is_none());
        assert_eq!(buf.len(), 2, "partial bytes retained");
    }

    #[test]
    fn framed_returns_none_on_partial_body() {
        let env = Envelope::new(9, &b"abcdef"[..]);
        let frame = env.encode_framed();
        let mut buf = BytesMut::from(&frame[..frame.len() - 3]);
        assert!(decode_framed(&mut buf).expect("ok").is_none());
    }

    #[test]
    fn framed_rejects_oversized_length_prefix() {
        let mut buf = BytesMut::new();
        buf.put_u32((MAX_FRAME_BODY_BYTES + 1) as u32);
        let err = decode_framed(&mut buf).expect_err("oversized must error");
        assert!(matches!(err, WireError::FrameTooLarge { .. }));
    }

    #[test]
    fn framed_rejects_undersized_body_length() {
        let mut buf = BytesMut::new();
        buf.put_u32(1);
        let err = decode_framed(&mut buf).expect_err("undersized must error");
        assert!(matches!(err, WireError::FrameTooSmall { .. }));
    }

    #[test]
    fn datagram_round_trip() {
        let env = Envelope::new(42, &b"datagram-body"[..]);
        let bytes = env.encode_datagram();
        let decoded = decode_datagram(&bytes).expect("decode ok");
        assert_eq!(decoded, env);
    }

    #[test]
    fn datagram_round_trip_empty_body() {
        let env = Envelope::new(3, Bytes::new());
        let bytes = env.encode_datagram();
        let decoded = decode_datagram(&bytes).expect("decode ok");
        assert_eq!(decoded, env);
        assert!(decoded.body.is_empty());
    }

    #[test]
    fn datagram_rejects_short_input() {
        let err = decode_datagram(&[0u8]).expect_err("too short must error");
        assert!(matches!(err, WireError::DatagramTooSmall { .. }));
    }

    #[test]
    fn reported_lengths_match_encoded_lengths() {
        let env = Envelope::new(5, &b"sized"[..]);
        assert_eq!(env.encode_framed().len(), env.framed_len());
        assert_eq!(env.encode_datagram().len(), env.datagram_len());
    }
}
