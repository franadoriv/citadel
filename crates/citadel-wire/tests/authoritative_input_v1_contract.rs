use citadel_wire::authoritative_input::{
    AUTHORITATIVE_INPUT_VERSION, AuthoritativeInputDisposition, CAPABILITY_AUTHORITATIVE_INPUT,
    CAPABILITY_CHALLENGE_BYTES, CAPABILITY_NEGOTIATION_VERSION, CapabilityAcceptance,
    CapabilityOffer, INPUT_STREAM_TOKEN_BYTES, InputReceipt, InputStreamToken, SequencedInput,
};
use citadel_wire::protocol::{
    KIND_AUTHORITATIVE_INPUT, KIND_CAPABILITY_ACCEPTANCE, KIND_CAPABILITY_OFFER,
};

fn token() -> InputStreamToken {
    InputStreamToken::new([0x21; INPUT_STREAM_TOKEN_BYTES]).expect("nonzero token")
}

#[test]
fn stream_bound_authoritative_input_is_the_only_v1_layout() {
    assert_eq!(KIND_AUTHORITATIVE_INPUT, 41);
    assert_eq!(AUTHORITATIVE_INPUT_VERSION, 1);

    let input = SequencedInput {
        stream_token: token(),
        sequence: 0xffff_ffff_ffff_ffff,
        original_custom_kind: 900,
        body: vec![0x00, 0xff, 0x80],
    };
    assert_eq!(
        input.encode().expect("canonical input"),
        vec![
            1, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21,
            0x21, 0x21, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x03, 0x84, 0, 0, 0, 3, 0,
            0xff, 0x80,
        ]
    );

    let receipt = InputReceipt {
        match_id: u64::MAX,
        stream_id: 0x1112_1314_1516_1718,
        stream_token: token(),
        acknowledged_sequence: 0,
        decided_sequence: u64::MAX,
        disposition: AuthoritativeInputDisposition::Rejected,
        authoritative_tick: 0x5152_5354_5556_5758,
        correction: Some(vec![0, 0xff, 0x80, 0x41]),
    };
    assert_eq!(
        receipt.encode().expect("canonical receipt"),
        vec![
            1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
            0x17, 0x18, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21,
            0x21, 0x21, 0x21, 0x21, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 1, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 1, 0, 0, 0, 4, 0, 0xff,
            0x80, 0x41,
        ]
    );
}

#[test]
fn capability_offer_and_acceptance_are_canonical_non_bearer_v1_echoes() {
    assert_eq!(KIND_CAPABILITY_OFFER, 42);
    assert_eq!(KIND_CAPABILITY_ACCEPTANCE, 43);
    assert_eq!(CAPABILITY_NEGOTIATION_VERSION, 1);
    assert_eq!(CAPABILITY_AUTHORITATIVE_INPUT, 1);
    assert_eq!(CAPABILITY_CHALLENGE_BYTES, 16);

    let offer = CapabilityOffer::new(
        CAPABILITY_AUTHORITATIVE_INPUT,
        [
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
            0x1e, 0x1f,
        ],
    )
    .expect("known capability and nonzero challenge");
    let expected = vec![
        1, 1, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ];
    assert_eq!(offer.encode(), expected);
    assert_eq!(CapabilityOffer::decode(&expected), Ok(offer));

    let acceptance = CapabilityAcceptance::from_offer(offer);
    assert_eq!(acceptance.encode(), expected);
    assert_eq!(CapabilityAcceptance::decode(&expected), Ok(acceptance));
}
