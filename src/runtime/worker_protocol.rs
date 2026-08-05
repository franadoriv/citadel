//! Versioned control-plane frames for the supervised GameScript worker.

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_CONTROL_FRAME_BYTES: usize = 64 * 1024;
#[cfg(unix)]
pub const PRIVATE_SOCKET_MODE: u32 = 0o600;
type HmacSha256 = Hmac<Sha256>;

pub fn challenge_proof(secret: &[u8; 32], nonce: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(secret).expect("fixed-size HMAC key");
    mac.update(nonce);
    mac.finalize().into_bytes().into()
}

pub fn verify_challenge_proof(secret: &[u8; 32], nonce: &[u8], proof: &[u8]) -> bool {
    let mut mac = HmacSha256::new_from_slice(secret).expect("fixed-size HMAC key");
    mac.update(nonce);
    mac.verify_slice(proof).is_ok()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlFrame {
    ParentHello {
        protocol_version: u16,
        nonce: Vec<u8>,
    },
    ParentShutdown {
        protocol_version: u16,
    },
    WorkerStopped {
        protocol_version: u16,
    },
    WorkerHealth {
        protocol_version: u16,
    },
    WorkerReady {
        protocol_version: u16,
    },
    WorkerHello {
        protocol_version: u16,
        proof: Vec<u8>,
    },
}

pub fn is_valid_worker_health(frame: &ControlFrame) -> bool {
    matches!(frame, ControlFrame::WorkerHealth { protocol_version } if *protocol_version == PROTOCOL_VERSION)
}

pub fn verify_worker_hello(secret: &[u8; 32], nonce: &[u8], frame: &ControlFrame) -> bool {
    match frame {
        ControlFrame::WorkerHello {
            protocol_version,
            proof,
        } if *protocol_version == PROTOCOL_VERSION => {
            let Ok(proof) = <[u8; 32]>::try_from(proof.as_slice()) else {
                return false;
            };
            verify_challenge_proof(secret, nonce, &proof)
        }
        _ => false,
    }
}

pub fn encode_frame(frame: &ControlFrame) -> Result<Vec<u8>, ProtocolError> {
    serde_json::to_vec(frame).map_err(|_| ProtocolError::MalformedFrame)
}

use std::io::{Read, Write};

pub fn write_control_frame(
    stream: &mut impl Write,
    frame: &ControlFrame,
) -> Result<(), ProtocolError> {
    let payload = encode_frame(frame)?;
    // Symmetric fail-closed limit: outbound frames obey the same cap the
    // reader enforces, and nothing is written for an oversized frame so the
    // stream never desynchronizes.
    if payload.len() > MAX_CONTROL_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .map_err(|_| ProtocolError::MalformedFrame)?;
    stream
        .write_all(&payload)
        .map_err(|_| ProtocolError::MalformedFrame)
}

pub fn read_control_frame(stream: &mut impl Read) -> Result<ControlFrame, ProtocolError> {
    let mut prefix = [0; 4];
    stream
        .read_exact(&mut prefix)
        .map_err(|_| ProtocolError::MalformedFrame)?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_CONTROL_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    let mut payload = vec![0; length];
    stream
        .read_exact(&mut payload)
        .map_err(|_| ProtocolError::MalformedFrame)?;
    decode_frame(&payload)
}

pub fn decode_frame(bytes: &[u8]) -> Result<ControlFrame, ProtocolError> {
    if bytes.len() > MAX_CONTROL_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge);
    }
    serde_json::from_slice(bytes).map_err(|_| ProtocolError::MalformedFrame)
}

/// Bootstrap frame sent only over the parent-created private transport.
#[derive(Clone, PartialEq, Eq)]
pub struct BootstrapRequest {
    protocol_version: u16,
    secret: Vec<u8>,
}

impl std::fmt::Debug for BootstrapRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BootstrapRequest")
            .field("protocol_version", &self.protocol_version)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    EmptyBootstrapSecret,
    InvalidBootstrapSecretLength(usize),
    UnsupportedVersion(u16),
    MalformedFrame,
    FrameTooLarge,
}

impl BootstrapRequest {
    pub fn new(protocol_version: u16, secret: Vec<u8>) -> Result<Self, ProtocolError> {
        if protocol_version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(protocol_version));
        }
        if secret.is_empty() {
            return Err(ProtocolError::EmptyBootstrapSecret);
        }
        if secret.len() != 32 {
            return Err(ProtocolError::InvalidBootstrapSecretLength(secret.len()));
        }
        Ok(Self {
            protocol_version,
            secret,
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn bootstrap_request_rejects_an_empty_secret() {
        let error = super::BootstrapRequest::new(1, Vec::new())
            .expect_err("an empty bootstrap secret would allow unauthenticated workers");
        assert_eq!(error, super::ProtocolError::EmptyBootstrapSecret);
    }

    #[test]
    fn bootstrap_request_preserves_protocol_version_and_secret() {
        let request =
            super::BootstrapRequest::new(1, vec![7; 32]).expect("valid bootstrap request");
        assert_eq!(request.protocol_version, 1);
        assert_eq!(request.secret.len(), 32);
    }

    #[test]
    fn bootstrap_request_rejects_unknown_protocol_versions() {
        let error = super::BootstrapRequest::new(2, vec![7; 32])
            .expect_err("unknown version must fail closed");
        assert_eq!(error, super::ProtocolError::UnsupportedVersion(2));
    }

    #[test]
    fn challenge_proof_is_bound_to_secret_and_nonce() {
        let secret = [7; 32];
        let proof = super::challenge_proof(&secret, b"parent-nonce");
        assert_ne!(proof, super::challenge_proof(&secret, b"other-nonce"));
        assert!(super::verify_challenge_proof(
            &secret,
            b"parent-nonce",
            &proof
        ));
        assert!(!super::verify_challenge_proof(
            &[8; 32],
            b"parent-nonce",
            &proof
        ));
    }

    #[test]
    fn length_prefixed_frame_round_trips() {
        let frame = super::ControlFrame::ParentHello {
            protocol_version: super::PROTOCOL_VERSION,
            nonce: vec![2; 32],
        };
        let mut wire = Vec::new();
        super::write_control_frame(&mut wire, &frame).expect("write");
        assert_eq!(
            super::read_control_frame(&mut wire.as_slice()).expect("read"),
            frame
        );
    }

    #[test]
    fn worker_hello_requires_matching_version_and_proof() {
        let secret = [7; 32];
        let frame = super::ControlFrame::WorkerHello {
            protocol_version: super::PROTOCOL_VERSION,
            proof: super::challenge_proof(&secret, b"nonce").to_vec(),
        };
        assert!(super::verify_worker_hello(&secret, b"nonce", &frame));
        assert!(!super::verify_worker_hello(&secret, b"other", &frame));
    }

    #[test]
    fn control_frame_round_trips_worker_stopped() {
        let frame = super::ControlFrame::WorkerStopped {
            protocol_version: super::PROTOCOL_VERSION,
        };
        let encoded = super::encode_frame(&frame).expect("encode");
        assert_eq!(super::decode_frame(&encoded).expect("decode"), frame);
    }

    #[test]
    fn control_frame_round_trips_parent_shutdown() {
        let frame = super::ControlFrame::ParentShutdown {
            protocol_version: super::PROTOCOL_VERSION,
        };
        let encoded = super::encode_frame(&frame).expect("encode");
        assert_eq!(super::decode_frame(&encoded).expect("decode"), frame);
    }

    #[test]
    fn health_check_accepts_a_versioned_worker_health_frame() {
        let worker = super::ControlFrame::WorkerHealth {
            protocol_version: super::PROTOCOL_VERSION,
        };
        assert!(super::is_valid_worker_health(&worker));
    }

    #[test]
    fn control_frame_round_trips_worker_health() {
        let frame = super::ControlFrame::WorkerHealth {
            protocol_version: super::PROTOCOL_VERSION,
        };
        let encoded = super::encode_frame(&frame).expect("encode");
        assert_eq!(super::decode_frame(&encoded).expect("decode"), frame);
    }

    #[test]
    fn control_frame_round_trips_worker_ready() {
        let frame = super::ControlFrame::WorkerReady {
            protocol_version: super::PROTOCOL_VERSION,
        };
        let encoded = super::encode_frame(&frame).expect("encode");
        assert_eq!(super::decode_frame(&encoded).expect("decode"), frame);
    }

    #[test]
    fn control_frame_round_trips_a_parent_hello() {
        let frame = super::ControlFrame::ParentHello {
            protocol_version: super::PROTOCOL_VERSION,
            nonce: vec![1; 32],
        };
        let encoded = super::encode_frame(&frame).expect("frame encodes");
        assert_eq!(super::decode_frame(&encoded).expect("frame decodes"), frame);
    }

    #[test]
    fn writer_rejects_oversized_frames_fail_closed() {
        let frame = super::ControlFrame::ParentHello {
            protocol_version: super::PROTOCOL_VERSION,
            nonce: vec![7; super::MAX_CONTROL_FRAME_BYTES + 1],
        };
        let mut wire = Vec::new();
        assert_eq!(
            super::write_control_frame(&mut wire, &frame),
            Err(super::ProtocolError::FrameTooLarge)
        );
        assert!(
            wire.is_empty(),
            "no bytes may reach the transport for an oversized frame"
        );
    }

    #[test]
    fn decoder_rejects_oversized_frames_before_deserialization() {
        let bytes = vec![b' '; super::MAX_CONTROL_FRAME_BYTES + 1];
        assert_eq!(
            super::decode_frame(&bytes),
            Err(super::ProtocolError::FrameTooLarge)
        );
    }

    #[test]
    fn bootstrap_request_requires_a_256_bit_secret() {
        let error = super::BootstrapRequest::new(1, vec![7; 31])
            .expect_err("short bootstrap secret must be rejected");
        assert_eq!(
            error,
            super::ProtocolError::InvalidBootstrapSecretLength(31)
        );
    }
}
