//! Published byte-level vectors for the versioned lag-diagnostics controls.
//!
//! `diagnostics_vectors.json` is the SDK-facing v1 wire lock: every integer is
//! big-endian, every body starts with version 1, and no field is implied by JSON
//! or a transport-specific representation.
#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use citadel_wire::diagnostics::{
    CAPABILITY_RECORDING, Capabilities, CaptureId, CaptureStatus, CaptureStatusCode, ClockSync,
    DIAGNOSTICS_UPLOAD_PATH, FlushCapture, PacketDirection, PacketFilter, ServerTime, StartCapture,
    UploadContentEncoding, UploadContentType,
};
use serde_json::Value;

fn hex(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(char::from_digit(u32::from(byte >> 4), 16).unwrap());
        result.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap());
    }
    result
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("diagnostics_vectors.json")
}

fn id() -> CaptureId {
    CaptureId::new([7; 16]).unwrap()
}

fn rendered_controls() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        (
            "server_time",
            ServerTime {
                offer_id: 1,
                server_utc_ms: 2,
            }
            .encode()
            .unwrap(),
        ),
        (
            "capabilities",
            Capabilities {
                offer_id: 1,
                features: CAPABILITY_RECORDING,
            }
            .encode()
            .unwrap(),
        ),
        (
            "clock_sync_request",
            ClockSync::Request {
                sequence: 3,
                client_sent_mono_us: 4,
            }
            .encode()
            .unwrap(),
        ),
        (
            "clock_sync_response",
            ClockSync::Response {
                sequence: 3,
                client_sent_mono_us: 4,
                server_received_utc_us: 10,
                server_sent_utc_us: 11,
            }
            .encode()
            .unwrap(),
        ),
        (
            "start",
            StartCapture {
                capture_id: id(),
                generation: 2,
                deadline_server_utc_ms: 1_700_000_000_000,
                max_record_bytes: 4096,
                filters: vec![PacketFilter {
                    kind: 9,
                    direction: PacketDirection::Inbound,
                    entity_id: Some(42),
                }],
            }
            .encode()
            .unwrap(),
        ),
        (
            "flush",
            FlushCapture {
                capture_id: id(),
                generation: 2,
                attempt_id: 3,
                upload_deadline_server_utc_ms: 4,
                max_compressed_bytes: 4_096,
                content_type: UploadContentType::CitadelLagCapture,
                content_encoding: UploadContentEncoding::Gzip,
                upload_path: DIAGNOSTICS_UPLOAD_PATH.to_string(),
                upload_token: "fixture-token.01".to_string(),
            }
            .encode()
            .unwrap(),
        ),
        (
            "status_recording",
            CaptureStatus {
                capture_id: id(),
                generation: 2,
                code: CaptureStatusCode::Recording,
                attempt_id: 0,
                recorded_packets: 3,
                dropped_packets: 4,
                recorded_bytes: 5,
            }
            .encode()
            .unwrap(),
        ),
    ]
}

#[test]
fn diagnostics_vectors_json_is_in_sync() {
    let fixture: Value = serde_json::from_str(
        &std::fs::read_to_string(fixture_path()).expect("read diagnostics vector fixture"),
    )
    .expect("valid diagnostics vector fixture");
    assert_eq!(fixture["version"], 1);
    assert_eq!(fixture["endianness"], "big-endian");

    let expected = fixture["controls"].as_array().expect("controls array");
    let rendered = rendered_controls();
    assert_eq!(expected.len(), rendered.len());
    for (fixture, (name, body)) in expected.iter().zip(rendered) {
        assert_eq!(fixture["name"], name);
        assert_eq!(fixture["bytes_hex"], hex(&body), "{name} fixture drifted");
    }
}
