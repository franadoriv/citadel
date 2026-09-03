//! V1 vectors for explicit, game-defined match input and private acknowledgements.

use citadel_wire::match_input::{MatchInput, MatchInputAck, MatchInputError};
use citadel_wire::protocol::{KIND_MATCH_INPUT, KIND_MATCH_INPUT_ACK};

#[test]
fn match_input_kinds_are_distinct_server_owned_controls() {
    assert_ne!(KIND_MATCH_INPUT, KIND_MATCH_INPUT_ACK);
}

#[test]
fn opaque_match_input_round_trips_sequence_and_non_utf8_body() {
    let input = MatchInput {
        sequence: 41,
        body: vec![0, 0xff, 0x80, 7],
    };
    let encoded = input.encode().expect("bounded input encodes");
    assert_eq!(MatchInput::decode(&encoded).expect("input decodes"), input);

    let ack = MatchInputAck {
        last_processed_sequence: 41,
    };
    let encoded_ack = ack.encode();
    assert_eq!(
        MatchInputAck::decode(&encoded_ack).expect("ack decodes"),
        ack
    );
}

#[test]
fn match_input_rejects_oversized_or_truncated_bodies_before_runtime_delivery() {
    let oversized = MatchInput {
        sequence: 1,
        body: vec![0; MatchInput::MAX_BODY_BYTES + 1],
    };
    assert_eq!(oversized.encode(), Err(MatchInputError::BodyTooLarge));
    assert_eq!(
        MatchInput::decode(&[1, 0, 0]),
        Err(MatchInputError::Truncated)
    );
}
