#[test]
fn public_startup_message_is_stable() {
    assert_eq!(
        citadel::startup_message(),
        "citadel: Rust-native game server foundation"
    );
}
