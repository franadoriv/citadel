//! Realtime envelope codec for the server, re-exported from `citadel-wire`.
//!
//! The byte format lives in the shared [`citadel_wire`] crate so the server and
//! every client (the Rust SDK and demos) cannot drift apart. This module
//! re-exports the shared types and provides server-side decode wrappers that map
//! [`citadel_wire::WireError`] to the server's [`AppError`] at transport
//! boundaries.

use bytes::BytesMut;

pub use citadel_wire::{Envelope, LENGTH_PREFIX_BYTES, MAX_FRAME_BODY_BYTES, WireError};

use crate::error::{AppError, ErrorCategory};

/// Map a [`WireError`] to a transport-category [`AppError`].
fn to_app_error(e: WireError) -> AppError {
    AppError::new(ErrorCategory::Transport, e.to_string())
}

/// Decode a single length-delimited frame, mapping decode failures to
/// [`AppError`]. See [`citadel_wire::decode_framed`].
pub fn decode_framed(buf: &mut BytesMut) -> Result<Option<Envelope>, AppError> {
    citadel_wire::decode_framed(buf).map_err(to_app_error)
}

/// Decode a single datagram body, mapping decode failures to [`AppError`].
/// See [`citadel_wire::decode_datagram`].
pub fn decode_datagram(data: &[u8]) -> Result<Envelope, AppError> {
    citadel_wire::decode_datagram(data).map_err(to_app_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_decode_framed_maps_error_to_transport_category() {
        // An oversized length prefix must surface as a Transport error.
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&u32::to_be_bytes((MAX_FRAME_BODY_BYTES + 1) as u32));
        let err = decode_framed(&mut buf).expect_err("oversized must error");
        assert_eq!(err.category(), ErrorCategory::Transport);
    }

    #[test]
    fn server_decode_datagram_maps_error_to_transport_category() {
        let err = decode_datagram(&[0u8]).expect_err("too short must error");
        assert_eq!(err.category(), ErrorCategory::Transport);
    }

    #[test]
    fn server_round_trip_uses_shared_format() {
        let env = Envelope::new(7, &b"hello"[..]);
        let frame = env.encode_framed();
        let mut buf = BytesMut::from(&frame[..]);
        let decoded = decode_framed(&mut buf)
            .expect("ok")
            .expect("complete frame");
        assert_eq!(decoded, env);
    }
}
