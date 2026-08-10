use citadel_client::{ChatEvent, ChatEventCursor, ChatEventDisposition, ChatEventKind};
use citadel_wire::{Envelope, protocol::KIND_CHAT_EVENT};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct Fixture {
    version: u8,
    valid: Vec<ValidCase>,
    invalid: Vec<InvalidCase>,
}

#[derive(Deserialize)]
struct ValidCase {
    name: String,
    kind: String,
    event: Value,
}

#[derive(Deserialize)]
struct InvalidCase {
    name: String,
    payload: Option<String>,
    event: Option<Value>,
}

fn fixture() -> Fixture {
    serde_json::from_str(include_str!(
        "../../../tests/fixtures/chat-live-events-v1.json"
    ))
    .expect("canonical chat fixture must be valid JSON")
}

fn decode(value: Value) -> ChatEvent {
    ChatEvent::decode(value.to_string().as_bytes()).expect("valid fixture event")
}

#[test]
fn canonical_fixture_decodes_all_closed_v1_variants() {
    let fixture = fixture();
    assert_eq!(fixture.version, 1);
    assert_eq!(fixture.valid.len(), 8);

    for case in fixture.valid {
        let event = decode(case.event);
        assert_eq!(event.kind().as_str(), case.kind, "{}", case.name);
        assert_eq!(event.channel_id(), "ch_demo", "{}", case.name);
    }
}

#[test]
fn canonical_fixture_rejects_malformed_or_incomplete_events() {
    for case in fixture().invalid {
        let bytes = case.payload.map(String::into_bytes).unwrap_or_else(|| {
            case.event
                .expect("event or payload")
                .to_string()
                .into_bytes()
        });
        assert!(
            ChatEvent::decode(&bytes).is_err(),
            "{} unexpectedly decoded",
            case.name
        );
    }
}

#[test]
fn envelope_entrypoint_is_kind_scoped() {
    let body = fixture().valid[0].event.to_string().into_bytes();
    let event = ChatEvent::from_envelope(&Envelope::new(KIND_CHAT_EVENT, body.clone()))
        .expect("kind 28 decodes");
    assert_eq!(event.kind(), ChatEventKind::PresenceJoin);
    assert!(ChatEvent::from_envelope(&Envelope::new(1, body)).is_err());
}

#[test]
fn per_channel_cursor_classifies_delivery_without_unbounded_global_state() {
    let valid = fixture().valid;
    let create = decode(valid[3].event.clone());
    let update = decode(valid[4].event.clone());
    let remove = decode(valid[5].event.clone());
    let typing = decode(valid[2].event.clone());
    let resync = decode(valid[7].event.clone());

    let mut cursor = ChatEventCursor::new("ch_demo", 4).expect("cursor");
    assert_eq!(
        cursor.observe(&create).expect("create"),
        ChatEventDisposition::Apply { event_id: 5 }
    );
    assert_eq!(
        cursor.observe(&create).expect("duplicate"),
        ChatEventDisposition::Duplicate { event_id: 5 }
    );
    assert_eq!(
        cursor.observe(&remove).expect("gap"),
        ChatEventDisposition::ReconcileGap {
            current_watermark: 5,
            observed_event_id: 7
        }
    );
    assert_eq!(
        cursor.observe(&typing).expect("typing"),
        ChatEventDisposition::Ephemeral
    );
    assert_eq!(
        cursor.observe(&resync).expect("resync"),
        ChatEventDisposition::ResyncRequired {
            watermark_event_id: 9
        }
    );
    cursor.reset(5);
    assert_eq!(
        cursor.observe(&update).expect("update"),
        ChatEventDisposition::Apply { event_id: 6 }
    );
    assert_eq!(cursor.watermark(), 6);
}

#[test]
fn cursor_rejects_cross_channel_events() {
    let event = decode(fixture().valid[3].event.clone());
    let mut cursor = ChatEventCursor::new("another", 0).expect("cursor");
    assert!(cursor.observe(&event).is_err());
    assert_eq!(cursor.watermark(), 0);
}

#[test]
fn current_server_serializer_round_trips_through_the_client_boundary() {
    use citadel::repository::chat::{
        ChannelType, ChatMessage as ServerChatMessage, serialize_delivery_event,
    };

    let server_message = ServerChatMessage {
        id: 11,
        sender: "alice".to_owned(),
        content: "server payload".to_owned(),
        created_at_unix_ms: 1_000,
        updated_at_unix_ms: 1_100,
        revision: 2,
        last_event_id: 17,
        deleted: false,
    };
    let payload = serialize_delivery_event(
        "ch_server",
        ChannelType::Direct,
        "message.update",
        &server_message,
    )
    .expect("server serializer");

    let event = ChatEvent::decode(payload.as_bytes()).expect("client decoder");
    assert_eq!(event.kind(), ChatEventKind::MessageUpdate);
    assert_eq!(event.event_id(), Some(17));
    assert_eq!(event.message().expect("typed message").last_event_id, 17);
}
