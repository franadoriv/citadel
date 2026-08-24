use citadel_wire::authoritative_input::{
    INPUT_STREAM_CONTROL_ADVERTISE, INPUT_STREAM_CONTROL_REVOKE, INPUT_STREAM_CONTROL_VERSION,
    INPUT_STREAM_TOKEN_BYTES, InputStreamControl, InputStreamControlError, InputStreamToken,
};

fn token() -> InputStreamToken {
    InputStreamToken::new([0xA5; INPUT_STREAM_TOKEN_BYTES]).expect("nonzero token")
}

#[test]
fn canonical_control_constants_and_bodies_round_trip() {
    assert_eq!(INPUT_STREAM_CONTROL_VERSION, 1);
    assert_eq!(INPUT_STREAM_CONTROL_ADVERTISE, 1);
    assert_eq!(INPUT_STREAM_CONTROL_REVOKE, 2);

    let advertise = InputStreamControl::Advertise {
        match_id: 0x0102_0304_0506_0708,
        stream_id: 0x1112_1314_1516_1718,
        token: token(),
    };
    assert_eq!(
        advertise.encode(),
        [
            1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0xA5,
            0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5, 0xA5,
            0xA5,
        ]
    );
    assert_eq!(
        InputStreamControl::decode(&advertise.encode()),
        Ok(advertise)
    );

    let revoke = InputStreamControl::Revoke {
        match_id: 0x0102_0304_0506_0708,
        stream_id: 0x1112_1314_1516_1718,
    };
    assert_eq!(
        revoke.encode(),
        [
            1, 2, 1, 2, 3, 4, 5, 6, 7, 8, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        ]
    );
    assert_eq!(InputStreamControl::decode(&revoke.encode()), Ok(revoke));
}

#[test]
fn control_decoder_exhaustively_rejects_noncanonical_bodies() {
    for len in 0..2 {
        assert_eq!(
            InputStreamControl::decode(&vec![0; len]),
            Err(InputStreamControlError::Truncated {
                needed: 2,
                got: len
            })
        );
    }
    assert_eq!(
        InputStreamControl::decode(&[2, 1]),
        Err(InputStreamControlError::UnsupportedVersion(2))
    );
    for len in 2..18 {
        let mut body = vec![1, 1];
        body.resize(len, 0);
        assert_eq!(
            InputStreamControl::decode(&body),
            Err(InputStreamControlError::Truncated {
                needed: 18,
                got: len
            })
        );
    }
    assert_eq!(
        InputStreamControl::decode(&[1, 99, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]),
        Err(InputStreamControlError::InvalidOperation(99))
    );

    let mut advertise = InputStreamControl::Advertise {
        match_id: 1,
        stream_id: 2,
        token: token(),
    }
    .encode();
    for len in 18..34 {
        assert_eq!(
            InputStreamControl::decode(&advertise[..len]),
            Err(InputStreamControlError::Truncated {
                needed: 34,
                got: len
            })
        );
    }
    advertise.push(0);
    assert_eq!(
        InputStreamControl::decode(&advertise),
        Err(InputStreamControlError::TrailingBytes(1))
    );

    let mut zero_token = InputStreamControl::Revoke {
        match_id: 1,
        stream_id: 2,
    }
    .encode();
    zero_token[1] = INPUT_STREAM_CONTROL_ADVERTISE;
    zero_token.resize(34, 0);
    assert!(matches!(
        InputStreamControl::decode(&zero_token),
        Err(InputStreamControlError::InvalidToken(_))
    ));

    let mut revoke = InputStreamControl::Revoke {
        match_id: 1,
        stream_id: 2,
    }
    .encode();
    revoke.push(0);
    assert_eq!(
        InputStreamControl::decode(&revoke),
        Err(InputStreamControlError::TrailingBytes(1))
    );
}

#[test]
fn control_debug_is_redacted() {
    let control = InputStreamControl::Advertise {
        match_id: 1,
        stream_id: 2,
        token: InputStreamToken::new([0x5A; INPUT_STREAM_TOKEN_BYTES]).expect("token"),
    };
    let debug = format!("{control:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("5A"));
    assert!(!debug.contains("90"));
}
