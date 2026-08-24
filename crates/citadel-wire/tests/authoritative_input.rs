//! Byte-exact tests for the version-1 stream-bound authoritative-input codec.
#![allow(clippy::unwrap_used)]

use citadel_wire::authoritative_input::{
    AUTHORITATIVE_INPUT_VERSION, AuthoritativeInputDisposition, AuthoritativeInputError,
    InputReceipt, InputStreamToken, MAX_SEQUENCED_INPUT_BODY_BYTES, SequencedInput,
};

fn token() -> InputStreamToken {
    InputStreamToken::new([
        0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0, 0xF0,
        0x01,
    ])
    .expect("nonzero fixed-width token")
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16).expect("hex")
        })
        .collect()
}

#[test]
fn shared_cross_sdk_fixture_is_v1_and_round_trips() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../clients/authoritative-input-fixtures.json"
    ))
    .expect("checked-in SDK fixture is valid JSON");
    assert_eq!(fixture["version"], 1);
    let input = decode_hex(
        fixture["sequenced_input"]["hex"]
            .as_str()
            .expect("input fixture hex"),
    );
    let receipt = decode_hex(
        fixture["input_receipt"]["hex"]
            .as_str()
            .expect("receipt fixture hex"),
    );
    assert_eq!(input[0], AUTHORITATIVE_INPUT_VERSION);
    assert_eq!(receipt[0], AUTHORITATIVE_INPUT_VERSION);
    assert_eq!(
        SequencedInput::decode(&input)
            .expect("fixture input decodes")
            .encode()
            .expect("re-encodes"),
        input
    );
    assert_eq!(
        InputReceipt::decode(&receipt)
            .expect("fixture receipt decodes")
            .encode()
            .expect("re-encodes"),
        receipt
    );
}

#[test]
fn sequenced_input_carries_required_stream_token_in_v1() {
    let input = SequencedInput {
        stream_token: token(),
        sequence: 0x0102_0304_0506_0708,
        original_custom_kind: 0xBEEF,
        body: vec![0xAA, 0xBB, 0xCC],
    };
    assert_eq!(
        input.encode().expect("encodes"),
        vec![
            1, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0,
            0xF0, 0x01, 1, 2, 3, 4, 5, 6, 7, 8, 0xBE, 0xEF, 0, 0, 0, 3, 0xAA, 0xBB, 0xCC,
        ]
    );
    assert_eq!(
        SequencedInput::decode(&input.encode().expect("encodes")),
        Ok(input)
    );
}

#[test]
fn receipt_binds_equal_sequences_to_match_stream_and_token_in_v1() {
    let first = InputReceipt {
        match_id: 7,
        stream_id: 11,
        stream_token: token(),
        acknowledged_sequence: 4,
        decided_sequence: 9,
        disposition: AuthoritativeInputDisposition::Accepted,
        authoritative_tick: 12,
        correction: None,
    };
    let different_match = InputReceipt {
        match_id: 8,
        ..first.clone()
    };
    let different_stream = InputReceipt {
        stream_id: 13,
        ..first.clone()
    };
    assert_ne!(
        first.encode().expect("encodes"),
        different_match.encode().expect("encodes")
    );
    assert_ne!(
        first.encode().expect("encodes"),
        different_stream.encode().expect("encodes")
    );
    assert_eq!(
        InputReceipt::decode(&first.encode().expect("encodes")),
        Ok(first)
    );
}

#[test]
fn decoders_reject_wrong_version_noncanonical_token_and_inexact_bodies() {
    let input = SequencedInput {
        stream_token: token(),
        sequence: 9,
        original_custom_kind: 0xBEEF,
        body: vec![1, 2],
    }
    .encode()
    .expect("input encodes");
    let mut wrong_version = input.clone();
    wrong_version[0] = 2;
    assert_eq!(
        SequencedInput::decode(&wrong_version),
        Err(AuthoritativeInputError::UnsupportedVersion(2))
    );
    let mut zero_token = input.clone();
    zero_token[1..17].fill(0);
    assert_eq!(
        SequencedInput::decode(&zero_token),
        Err(AuthoritativeInputError::AllZeroStreamToken)
    );
    assert!(matches!(
        SequencedInput::decode(&input[..30]),
        Err(AuthoritativeInputError::Truncated { .. })
    ));
    let mut trailing = input;
    trailing.push(0xA5);
    assert_eq!(
        SequencedInput::decode(&trailing),
        Err(AuthoritativeInputError::TrailingBytes(1))
    );
}

#[test]
fn receipt_rejects_invalid_disposition_correction_and_boundaries() {
    let receipt = InputReceipt {
        match_id: 1,
        stream_id: 2,
        stream_token: token(),
        acknowledged_sequence: 0,
        decided_sequence: 1,
        disposition: AuthoritativeInputDisposition::Rejected,
        authoritative_tick: 3,
        correction: Some(vec![0x00, 0xff]),
    };
    let encoded = receipt.encode().expect("receipt encodes");
    assert_eq!(InputReceipt::decode(&encoded), Ok(receipt));

    let mut bad_disposition = encoded.clone();
    bad_disposition[49] = 2;
    assert_eq!(
        InputReceipt::decode(&bad_disposition),
        Err(AuthoritativeInputError::Invalid("receipt disposition"))
    );
    let mut absent_correction = encoded.clone();
    absent_correction[58] = 0;
    assert_eq!(
        InputReceipt::decode(&absent_correction),
        Err(AuthoritativeInputError::Invalid(
            "receipt absent correction length"
        ))
    );
    assert!(matches!(
        InputReceipt::decode(&encoded[..62]),
        Err(AuthoritativeInputError::Truncated { .. })
    ));

    let oversized = SequencedInput {
        stream_token: token(),
        sequence: 1,
        original_custom_kind: 1,
        body: vec![0; MAX_SEQUENCED_INPUT_BODY_BYTES + 1],
    };
    assert!(matches!(
        oversized.encode(),
        Err(AuthoritativeInputError::TooLarge { field: "body", .. })
    ));
}
