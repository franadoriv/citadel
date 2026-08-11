use citadel_client::{
    ChatCursorState, ChatEvent, ChatEventCursor, ChatEventDisposition, ChatEventKind,
    ChatHistoryOptions, ChatHistoryResult, ChatJoinAttempt, ChatJoinResult, ChatLeaveResult,
    ChatMutationResult, ChatRemoveResult, ChatRpcRequest, ChatTarget, ChatTypingResult,
};
use citadel_wire::{Envelope, protocol::KIND_CHAT_EVENT};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct Fixture {
    version: u8,
    valid: Vec<ValidCase>,
    content_validation: Vec<ContentValidationCase>,
    invalid: Vec<InvalidCase>,
}

#[derive(Deserialize)]
struct ContentValidationCase {
    name: String,
    event: String,
    content: Option<String>,
    content_repeat: Option<ContentRepeat>,
    accepted: bool,
}

#[derive(Deserialize)]
struct ContentRepeat {
    value: String,
    count: usize,
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

fn valid_event(name: &str) -> Value {
    let fixture: Fixture = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/chat-live-events-v1.json"
    )))
    .expect("fixture");
    fixture
        .valid
        .into_iter()
        .find(|case| case.name == name)
        .expect("named valid chat fixture")
        .event
}

fn history_message(id: u64, last_event_id: u64) -> Value {
    serde_json::json!({
        "id": id,
        "sender": "alice",
        "content": format!("message-{id}"),
        "created_at_unix_ms": 1_700_000_000_000_u64 + id,
        "updated_at_unix_ms": 1_700_000_000_000_u64 + id,
        "revision": 1,
        "last_event_id": last_event_id,
        "deleted": false
    })
}

fn rejoin(cursor: &mut ChatEventCursor, watermark_event_id: u64) -> bool {
    let request = cursor
        .rejoin_request(ChatTarget::CurrentRoom)
        .expect("rejoin request");
    let body = serde_json::to_vec(&serde_json::json!({
        "channel_id": cursor.channel_id(),
        "channel_type": "room",
        "presence": [],
        "watermark_event_id": watermark_event_id,
        "subscription": "sub"
    }))
    .expect("join JSON");
    let result = ChatJoinResult::decode(&body).expect("typed join result");
    cursor
        .accept_rejoin_response(request, result)
        .expect("correlated join result")
}

fn joined_cursor(channel_id: &str, watermark_event_id: u64) -> ChatEventCursor {
    let attempt = ChatJoinAttempt::new(ChatTarget::CurrentRoom).expect("join attempt");
    assert_eq!(attempt.method(), "chat.join");
    let body = serde_json::to_vec(&serde_json::json!({
        "channel_id": channel_id,
        "channel_type": "room",
        "presence": [],
        "watermark_event_id": watermark_event_id,
        "subscription": "sub"
    }))
    .expect("join JSON");
    ChatEventCursor::from_join_response(attempt, &body).expect("correlated typed join")
}

#[test]
fn only_a_consumed_typed_join_attempt_constructs_a_current_cursor() {
    let cursor = joined_cursor("ch_demo", 7);
    assert_eq!(cursor.channel_id(), "ch_demo");
    assert_eq!(cursor.watermark(), 7);
    assert_eq!(cursor.state(), ChatCursorState::Current);

    let malformed = ChatJoinAttempt::new(ChatTarget::CurrentRoom).expect("join attempt");
    assert!(
        ChatEventCursor::from_join_response(malformed, br#"{"channel_id":"ch_demo"}"#).is_err()
    );

    let direct = ChatJoinAttempt::new(ChatTarget::Direct {
        other_user_id: "alice".to_owned(),
    })
    .expect("direct join attempt");
    let room_response = br#"{"channel_id":"ch_demo","channel_type":"room","presence":[],"watermark_event_id":7,"subscription":"sub"}"#;
    assert!(
        ChatEventCursor::from_join_response(direct, room_response).is_err(),
        "a response for a different typed join target must fail closed"
    );
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
fn canonical_fixture_matches_the_utf8_chat_content_boundary() {
    let fixture = fixture();
    let exact_multibyte = fixture
        .content_validation
        .iter()
        .find(|case| case.name == "update_multibyte_exactly_2048_utf8_bytes")
        .expect("shared fixture must lock the exact multibyte boundary");
    let repeat = exact_multibyte
        .content_repeat
        .as_ref()
        .expect("exact multibyte case uses repeated content");
    assert_eq!(repeat.value.repeat(repeat.count).len(), 2048);
    assert!(exact_multibyte.accepted);
    for case in fixture.content_validation {
        let mut event = fixture
            .valid
            .iter()
            .find(|valid| valid.name == case.event)
            .expect("content case base event")
            .event
            .clone();
        let content = case.content.unwrap_or_else(|| {
            let repeat = case.content_repeat.expect("literal or repeated content");
            repeat.value.repeat(repeat.count)
        });
        event["message"]["content"] = Value::String(content);
        assert_eq!(
            ChatEvent::decode(event.to_string().as_bytes()).is_ok(),
            case.accepted,
            "{}",
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

    let mut cursor = joined_cursor("ch_demo", 4);
    assert_eq!(
        cursor.observe(&create).expect("create"),
        ChatEventDisposition::Apply { event_id: 5 }
    );
    assert_eq!(
        cursor.observe(&create).expect("duplicate"),
        ChatEventDisposition::Duplicate { event_id: 5 }
    );
    assert_eq!(
        cursor.observe(&typing).expect("typing"),
        ChatEventDisposition::Ephemeral
    );
    assert_eq!(
        cursor.observe(&remove).expect("gap"),
        ChatEventDisposition::ReconcileGap {
            current_watermark: 5,
            observed_event_id: 7
        }
    );
    assert!(
        cursor.observe(&update).is_err(),
        "live events pause during reconciliation"
    );
    let page = serde_json::json!({
        "items": [history_message(1, 7)],
        "watermark_event_id": 7
    });
    let request = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(2),
            ..Default::default()
        })
        .expect("history request");
    let application = cursor
        .accept_history_response(request, &serde_json::to_vec(&page).expect("history json"))
        .expect("terminal page");
    cursor
        .complete_history_application(application)
        .expect("page applied");
    let ack = cursor
        .acknowledge_reconciliation()
        .expect("ack after terminal history page");
    cursor
        .complete_reconciliation(ack, &serde_json::to_vec(&page).expect("ack json"))
        .expect("ack response");
    assert_eq!(
        cursor.observe(&remove).expect("replayed remove"),
        ChatEventDisposition::Duplicate { event_id: 7 }
    );
    assert_eq!(cursor.watermark(), 7);

    let mut resync_cursor = joined_cursor("ch_demo", 5);
    assert_eq!(
        resync_cursor.observe(&resync).expect("resync"),
        ChatEventDisposition::ResyncRequired {
            watermark_event_id: 9
        }
    );
    assert_eq!(
        resync_cursor.state(),
        ChatCursorState::Reconciling {
            required_watermark: 9
        }
    );
}

#[test]
fn cursor_rejects_cross_channel_events() {
    let event = decode(fixture().valid[3].event.clone());
    let mut cursor = joined_cursor("another", 0);
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

#[test]
fn domain_request_builders_match_the_gateway_contract() {
    let join = ChatRpcRequest::join(ChatTarget::Direct {
        other_user_id: "bob".to_owned(),
    })
    .expect("join request");
    assert_eq!(join.method(), "chat.join");
    assert_eq!(join.json()["target"]["kind"], "direct");
    assert_eq!(join.json()["target"]["other_user_id"], "bob");

    let group = ChatRpcRequest::join(ChatTarget::Group { group_id: 7 }).expect("group join");
    assert_eq!(group.json()["target"]["group_id"], 7);
    let room = ChatRpcRequest::join(ChatTarget::CurrentRoom).expect("room join");
    assert_eq!(room.json()["target"]["kind"], "room");

    assert_eq!(
        ChatRpcRequest::leave("ch").expect("leave").method(),
        "chat.leave"
    );
    assert_eq!(
        ChatRpcRequest::send("ch", "hello").expect("send").method(),
        "chat.send"
    );
    assert_eq!(
        ChatRpcRequest::edit("ch", 1, "edited")
            .expect("edit")
            .method(),
        "chat.edit"
    );
    assert_eq!(
        ChatRpcRequest::delete("ch", 1).expect("delete").method(),
        "chat.delete"
    );
    assert_eq!(
        ChatRpcRequest::moderate("ch", 1)
            .expect("moderate")
            .method(),
        "chat.moderate"
    );
    assert_eq!(
        ChatRpcRequest::typing("ch", true).expect("typing").method(),
        "chat.typing"
    );

    let history = ChatRpcRequest::history(
        "ch",
        ChatHistoryOptions {
            limit: Some(50),
            before_message_id: Some(123),
        },
    )
    .expect("history");
    assert_eq!(history.json()["limit"], 50);
    assert_eq!(history.json()["before_message_id"], 123);
    assert!(history.json().get("acknowledge_watermark").is_none());
    assert_eq!(
        serde_json::from_slice::<Value>(history.body()).expect("request body"),
        *history.json()
    );
}

#[test]
fn domain_request_builders_fail_closed_before_network_io() {
    assert!(
        ChatRpcRequest::join(ChatTarget::Direct {
            other_user_id: " ".to_owned()
        })
        .is_err()
    );
    assert!(ChatRpcRequest::join(ChatTarget::Group { group_id: 0 }).is_err());
    assert!(ChatRpcRequest::leave("").is_err());
    assert!(ChatRpcRequest::send("ch", "\t").is_err());
    assert!(ChatRpcRequest::send("ch", &"x".repeat(2_049)).is_err());
    assert!(ChatRpcRequest::send("ch", "bad\u{0000}").is_err());
    assert!(ChatRpcRequest::edit("ch", 0, "x").is_err());
    assert!(
        ChatRpcRequest::history(
            "ch",
            ChatHistoryOptions {
                limit: Some(201),
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[test]
fn domain_responses_are_typed_and_fail_closed() {
    let join = ChatJoinResult::decode(
        br#"{"channel_id":"ch","channel_type":"direct","presence":[{"presence_id":"p","user_id":"alice"}],"watermark_event_id":7,"subscription":"sub"}"#,
    )
    .expect("join");
    assert_eq!(join.channel_id, "ch");
    assert_eq!(join.watermark_event_id, 7);

    let message = history_message(1, 7);
    let history_body = serde_json::to_vec(&serde_json::json!({
        "items": [message.clone()],
        "watermark_event_id": 7
    }))
    .expect("history json");
    let history = ChatHistoryResult::decode(&history_body).expect("history");
    assert_eq!(history.items.len(), 1);
    let mutation_body = serde_json::to_vec(&serde_json::json!({
        "message": message,
        "event_id": 7
    }))
    .expect("mutation json");
    assert_eq!(
        ChatMutationResult::decode(&mutation_body)
            .expect("mutation")
            .event_id,
        7
    );
    assert!(ChatMutationResult::decode(br#"{"message":{},"event_id":7}"#).is_err());

    assert!(ChatRemoveResult::decode(br#"{"message_id":1,"deleted":true,"event_id":7}"#).is_ok());
    assert!(ChatRemoveResult::decode(br#"{"message_id":1,"deleted":true}"#).is_err());
    assert!(ChatTypingResult::decode(br#"{"typing":true,"expires_at":123}"#).is_ok());
    assert!(ChatTypingResult::decode(br#"{"typing":true}"#).is_err());
    assert!(ChatLeaveResult::decode(br#"{"left":true}"#).is_ok());
    assert!(ChatLeaveResult::decode(br#"{"left":1}"#).is_err());
    assert!(
        ChatJoinResult::decode(
            br#"{"channel_id":"ch","channel_type":"future","presence":[],"watermark_event_id":0,"subscription":"sub"}"#
        )
        .is_err()
    );
}

#[test]
fn remove_result_distinguishes_an_absent_event_id_from_an_invalid_one() {
    for body in [
        br#"{"message_id":1,"deleted":false}"#.as_slice(),
        br#"{"message_id":1,"deleted":false,"event_id":null}"#.as_slice(),
    ] {
        let result = ChatRemoveResult::decode(body).expect("optional event id");
        assert!(!result.deleted);
        assert_eq!(result.event_id, None);
    }

    for body in [
        br#"{"message_id":1,"deleted":false,"event_id":"7"}"#.as_slice(),
        br#"{"message_id":1,"deleted":false,"event_id":-1}"#.as_slice(),
        br#"{"message_id":1,"deleted":false,"event_id":1.5}"#.as_slice(),
        br#"{"message_id":1,"deleted":false,"event_id":18446744073709551616}"#.as_slice(),
        br#"{"message_id":1,"deleted":true,"event_id":null}"#.as_slice(),
        br#"{"message_id":1,"deleted":true}"#.as_slice(),
    ] {
        assert!(
            ChatRemoveResult::decode(body).is_err(),
            "invalid optional event_id unexpectedly decoded: {}",
            String::from_utf8_lossy(body)
        );
    }
}

#[test]
fn reconnect_and_gap_reconciliation_require_ack_before_current() {
    let create = valid_event("message_create");
    let create = ChatEvent::decode(&serde_json::to_vec(&create).expect("json")).expect("event");
    let mut cursor = joined_cursor("ch_demo", 4);
    cursor.disconnect();
    assert_eq!(cursor.state(), ChatCursorState::Disconnected);
    assert!(rejoin(&mut cursor, 7));
    assert_eq!(
        cursor.state(),
        ChatCursorState::Reconciling {
            required_watermark: 7
        }
    );
    assert!(
        cursor.observe(&create).is_err(),
        "live events cannot apply while reconciling"
    );

    let first_request = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(2),
            ..Default::default()
        })
        .expect("first page");
    assert_eq!(first_request.method(), "chat.history");
    assert!(first_request.json().get("acknowledge_watermark").is_none());
    assert!(
        cursor.acknowledge_reconciliation().is_err(),
        "ack requires a terminal page"
    );

    let first_page = serde_json::json!({
        "items": [history_message(3, 7), history_message(2, 6)],
        "watermark_event_id": 7
    });
    let application = cursor
        .accept_history_response(
            first_request,
            &serde_json::to_vec(&first_page).expect("page json"),
        )
        .expect("first page response");
    assert_eq!(application.messages().len(), 2);
    cursor
        .complete_history_application(application)
        .expect("first page applied");
    assert_eq!(
        cursor.state(),
        ChatCursorState::Reconciling {
            required_watermark: 7
        }
    );
    let second_request = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(2),
            ..Default::default()
        })
        .expect("second page");
    assert_eq!(second_request.json()["before_message_id"], 2);

    let terminal_page = serde_json::json!({
        "items": [history_message(1, 5)],
        "watermark_event_id": 7
    });
    let application = cursor
        .accept_history_response(
            second_request,
            &serde_json::to_vec(&terminal_page).expect("page json"),
        )
        .expect("terminal page response");
    cursor
        .complete_history_application(application)
        .expect("terminal page applied");
    assert_eq!(
        cursor.state(),
        ChatCursorState::ReadyToAcknowledge { watermark: 7 }
    );
    let ack = cursor.acknowledge_reconciliation().expect("ack request");
    assert_eq!(ack.json()["acknowledge_watermark"], 7);
    assert_eq!(
        cursor.state(),
        ChatCursorState::AwaitingAcknowledgement { watermark: 7 }
    );
    let ack_response = serde_json::json!({"items": [], "watermark_event_id": 7});
    cursor
        .complete_reconciliation(ack, &serde_json::to_vec(&ack_response).expect("json"))
        .expect("acknowledged");
    assert_eq!(cursor.state(), ChatCursorState::Current);
    assert_eq!(cursor.watermark(), 7);
}

#[test]
fn revocation_clears_cursor_and_typing_expires_at_the_server_deadline() {
    let revoked = valid_event("access_revoked");
    let revoked = ChatEvent::decode(&serde_json::to_vec(&revoked).expect("json")).expect("revoked");
    let typing = valid_event("typing");
    let typing = ChatEvent::decode(&serde_json::to_vec(&typing).expect("json")).expect("typing");
    let mut cursor = joined_cursor("ch_demo", 7);
    assert_eq!(
        cursor.observe(&revoked).expect("observe"),
        ChatEventDisposition::AccessRevoked
    );
    assert_eq!(cursor.state(), ChatCursorState::Revoked);
    assert_eq!(cursor.watermark(), 0);
    assert!(cursor.observe(&typing).is_err());
    let expiry = typing.expires_at().expect("typing expiry");
    assert!(typing.typing_active_at(expiry - 1));
    assert!(!typing.typing_active_at(expiry));
}

#[test]
fn revocation_is_terminal_for_that_cursor_across_disconnect_and_rejoin() {
    let revoked = valid_event("access_revoked");
    let revoked = ChatEvent::decode(&serde_json::to_vec(&revoked).expect("json")).expect("event");
    let mut revoked_cursor = joined_cursor("ch_demo", 7);

    revoked_cursor.observe(&revoked).expect("revocation");
    revoked_cursor.disconnect();
    assert_eq!(revoked_cursor.state(), ChatCursorState::Revoked);
    assert!(
        revoked_cursor
            .rejoin_request(ChatTarget::CurrentRoom)
            .is_err(),
        "a revoked cursor cannot be reused for a newly authorized join"
    );
    assert_eq!(revoked_cursor.state(), ChatCursorState::Revoked);
    assert_eq!(revoked_cursor.watermark(), 0);

    let newly_authorized = joined_cursor("ch_demo", 0);
    assert_eq!(newly_authorized.state(), ChatCursorState::Current);
}

#[test]
fn reconciliation_restarts_instead_of_acking_a_moving_snapshot() {
    let mut cursor = joined_cursor("ch_demo", 4);
    assert!(rejoin(&mut cursor, 7));
    let first_request = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("first request");
    let first = serde_json::json!({
        "items": [history_message(2, 7)],
        "watermark_event_id": 7
    });
    let application = cursor
        .accept_history_response(first_request, &serde_json::to_vec(&first).expect("json"))
        .expect("first page");
    cursor
        .complete_history_application(application)
        .expect("first page applied");
    let second_request = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("second request");
    let moved = serde_json::json!({
        "items": [history_message(1, 5)],
        "watermark_event_id": 8
    });
    assert!(
        cursor
            .accept_history_response(second_request, &serde_json::to_vec(&moved).expect("json"),)
            .is_err()
    );
    assert_eq!(
        cursor.state(),
        ChatCursorState::Reconciling {
            required_watermark: 8
        }
    );
    assert!(cursor.acknowledge_reconciliation().is_err());
    let restarted = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("restart request");
    assert!(restarted.json().get("before_message_id").is_none());
}

#[test]
fn history_responses_are_bound_to_the_exact_cursor_request() {
    let mut first = joined_cursor("first", 1);
    let mut second = joined_cursor("second", 1);
    assert!(rejoin(&mut first, 2));
    assert!(rejoin(&mut second, 2));
    let first_request = first
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("first request");
    second
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("second request");
    let response = serde_json::json!({"items": [], "watermark_event_id": 2});
    assert!(
        second
            .accept_history_response(first_request, &serde_json::to_vec(&response).expect("json"),)
            .is_err()
    );
    assert!(second.acknowledge_reconciliation().is_err());
}

#[test]
fn received_history_page_requires_correlated_application_before_progress() {
    let mut cursor = joined_cursor("ch_demo", 4);
    let join = cursor
        .rejoin_request(ChatTarget::CurrentRoom)
        .expect("rejoin request");
    let joined = ChatJoinResult::decode(
        br#"{"channel_id":"ch_demo","channel_type":"room","presence":[],"watermark_event_id":7,"subscription":"sub"}"#,
    )
    .expect("typed join");
    assert!(cursor.accept_rejoin_response(join, joined).expect("rejoin"));

    let request = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(2),
            ..Default::default()
        })
        .expect("history request");
    let terminal = serde_json::json!({
        "items": [history_message(1, 7)],
        "watermark_event_id": 7
    });
    let application = cursor
        .accept_history_response(request, &serde_json::to_vec(&terminal).expect("json"))
        .expect("validated response");
    assert_eq!(application.messages().len(), 1);
    assert!(
        cursor
            .reconciliation_history_request(ChatHistoryOptions::default())
            .is_err(),
        "the next page cannot be requested before application confirmation"
    );
    assert!(
        cursor.acknowledge_reconciliation().is_err(),
        "a terminal response cannot enable ACK before application confirmation"
    );

    cursor
        .complete_history_application(application)
        .expect("application confirmation");
    assert_eq!(
        cursor.state(),
        ChatCursorState::ReadyToAcknowledge { watermark: 7 }
    );
}

#[test]
fn aborted_history_application_restarts_from_newest() {
    let mut cursor = joined_cursor("ch_demo", 4);
    let join = cursor
        .rejoin_request(ChatTarget::CurrentRoom)
        .expect("rejoin request");
    let joined = ChatJoinResult::decode(
        br#"{"channel_id":"ch_demo","channel_type":"room","presence":[],"watermark_event_id":7,"subscription":"sub"}"#,
    )
    .expect("typed join");
    cursor
        .accept_rejoin_response(join, joined)
        .expect("rejoin response");
    let request = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("history request");
    let page = serde_json::json!({
        "items": [history_message(2, 7)],
        "watermark_event_id": 7
    });
    let application = cursor
        .accept_history_response(request, &serde_json::to_vec(&page).expect("json"))
        .expect("validated response");
    cursor
        .abort_history_application(application)
        .expect("abort application");
    let restarted = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("restarted request");
    assert!(restarted.json().get("before_message_id").is_none());
}

#[test]
fn continuation_rejects_every_message_outside_its_strict_request_boundary() {
    let mut cursor = joined_cursor("ch_demo", 4);
    assert!(rejoin(&mut cursor, 7));
    let first_request = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(2),
            ..Default::default()
        })
        .expect("first request");
    let first = serde_json::json!({
        "items": [history_message(3, 7), history_message(2, 6)],
        "watermark_event_id": 7
    });
    let application = cursor
        .accept_history_response(first_request, &serde_json::to_vec(&first).expect("json"))
        .expect("first page");
    cursor
        .complete_history_application(application)
        .expect("first page applied");
    let continuation = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(2),
            ..Default::default()
        })
        .expect("continuation");
    assert_eq!(continuation.json()["before_message_id"], 2);
    let out_of_bounds = serde_json::json!({
        "items": [history_message(3, 7), history_message(1, 5)],
        "watermark_event_id": 7
    });

    assert!(
        cursor
            .accept_history_response(
                continuation,
                &serde_json::to_vec(&out_of_bounds).expect("json"),
            )
            .is_err()
    );
    assert_eq!(
        cursor.state(),
        ChatCursorState::Reconciling {
            required_watermark: 7
        }
    );
    assert!(cursor.acknowledge_reconciliation().is_err());
    let restarted = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(2),
            ..Default::default()
        })
        .expect("restart from newest");
    assert!(restarted.json().get("before_message_id").is_none());
}

#[test]
fn malformed_continuation_aborts_the_entire_reconciliation_generation() {
    let mut cursor = joined_cursor("ch_demo", 4);
    assert!(rejoin(&mut cursor, 7));
    let first_request = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("first request");
    let first = serde_json::json!({
        "items": [history_message(2, 7)],
        "watermark_event_id": 7
    });
    let application = cursor
        .accept_history_response(first_request, &serde_json::to_vec(&first).expect("json"))
        .expect("first page");
    cursor
        .complete_history_application(application)
        .expect("first page applied");
    let continuation = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("continuation");

    assert!(cursor.accept_history_response(continuation, b"{}").is_err());
    assert_eq!(
        cursor.state(),
        ChatCursorState::Reconciling {
            required_watermark: 7
        }
    );
    assert!(cursor.acknowledge_reconciliation().is_err());
    let restarted = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("restart from newest");
    assert!(restarted.json().get("before_message_id").is_none());
}

#[test]
fn acknowledgement_handles_reject_cross_cursor_and_post_revocation_responses() {
    let ready_to_ack = || {
        let mut cursor = joined_cursor("ch_demo", 1);
        assert!(rejoin(&mut cursor, 2));
        let request = cursor
            .reconciliation_history_request(ChatHistoryOptions::default())
            .expect("history request");
        let response = serde_json::to_vec(&serde_json::json!({
            "items": [],
            "watermark_event_id": 2
        }))
        .expect("history response");
        let application = cursor
            .accept_history_response(request, &response)
            .expect("history response accepted");
        cursor
            .complete_history_application(application)
            .expect("history applied");
        let acknowledgement = cursor
            .acknowledge_reconciliation()
            .expect("acknowledgement request");
        (cursor, acknowledgement, response)
    };

    let (first, first_ack, response) = ready_to_ack();
    let (mut second, second_ack, _) = ready_to_ack();
    assert!(
        second
            .complete_reconciliation(first_ack, &response)
            .is_err(),
        "an ACK handle from another cursor must not complete reconciliation"
    );
    assert_eq!(
        second.state(),
        ChatCursorState::AwaitingAcknowledgement { watermark: 2 }
    );
    second
        .complete_reconciliation(second_ack, &response)
        .expect("matching ACK response");
    assert_eq!(second.state(), ChatCursorState::Current);
    assert_eq!(
        first.state(),
        ChatCursorState::AwaitingAcknowledgement { watermark: 2 }
    );

    let (mut revoked_cursor, late_ack, response) = ready_to_ack();
    let revoked = valid_event("access_revoked");
    let revoked = ChatEvent::decode(&serde_json::to_vec(&revoked).expect("json")).expect("event");
    revoked_cursor.observe(&revoked).expect("revocation");
    assert!(
        revoked_cursor
            .complete_reconciliation(late_ack, &response)
            .is_err(),
        "a late ACK response must not revive a revoked cursor"
    );
    assert_eq!(revoked_cursor.state(), ChatCursorState::Revoked);
    assert_eq!(revoked_cursor.watermark(), 0);
}

#[test]
fn rejoin_cannot_downgrade_or_discard_active_reconciliation() {
    let mut cursor = joined_cursor("ch_demo", 4);
    let mut gap = valid_event("message_update");
    gap["event_id"] = serde_json::json!(9);
    gap["message"]["last_event_id"] = serde_json::json!(9);
    let gap = ChatEvent::decode(&serde_json::to_vec(&gap).expect("gap JSON")).expect("gap event");
    assert_eq!(
        cursor.observe(&gap).expect("observe gap"),
        ChatEventDisposition::ReconcileGap {
            current_watermark: 4,
            observed_event_id: 9
        }
    );

    let history = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("history request");
    assert!(
        cursor.rejoin_request(ChatTarget::CurrentRoom).is_err(),
        "rejoin must be busy while a stronger reconciliation generation is active"
    );
    assert_eq!(
        cursor.state(),
        ChatCursorState::Reconciling {
            required_watermark: 9
        }
    );
    assert_eq!(cursor.watermark(), 4);

    let first_page = serde_json::json!({
        "items": [history_message(2, 9)],
        "watermark_event_id": 9
    });
    let first_response = serde_json::to_vec(&first_page).expect("history JSON");
    let application = cursor
        .accept_history_response(history, &first_response)
        .expect("the original recovery request remains valid");
    assert!(
        cursor.rejoin_request(ChatTarget::CurrentRoom).is_err(),
        "rejoin must not discard an application awaiting confirmation"
    );
    cursor
        .complete_history_application(application)
        .expect("history application");
    assert!(
        cursor.rejoin_request(ChatTarget::CurrentRoom).is_err(),
        "rejoin must not discard accumulated pagination state"
    );

    let continuation = cursor
        .reconciliation_history_request(ChatHistoryOptions {
            limit: Some(1),
            ..Default::default()
        })
        .expect("continuation request");
    assert_eq!(continuation.json()["before_message_id"], 2);
    let terminal = serde_json::json!({
        "items": [],
        "watermark_event_id": 9
    });
    let response = serde_json::to_vec(&terminal).expect("terminal history JSON");
    let application = cursor
        .accept_history_response(continuation, &response)
        .expect("terminal history response");
    cursor
        .complete_history_application(application)
        .expect("terminal history application");
    assert!(
        cursor.rejoin_request(ChatTarget::CurrentRoom).is_err(),
        "rejoin must not bypass the required ACK"
    );
    let acknowledgement = cursor
        .acknowledge_reconciliation()
        .expect("rejected rejoin did not prevent ACK");
    assert!(
        cursor.rejoin_request(ChatTarget::CurrentRoom).is_err(),
        "rejoin must not replace an in-flight ACK"
    );
    cursor
        .complete_reconciliation(acknowledgement, &response)
        .expect("complete original reconciliation");
    assert_eq!(cursor.state(), ChatCursorState::Current);
    assert_eq!(cursor.watermark(), 9);
}

#[test]
fn rejoin_response_below_the_captured_floor_never_marks_current() {
    let mut cursor = joined_cursor("ch_demo", 4);
    cursor.disconnect();
    let request = cursor
        .rejoin_request(ChatTarget::CurrentRoom)
        .expect("rejoin request");
    let stale = ChatJoinResult::decode(
        br#"{"channel_id":"ch_demo","channel_type":"room","presence":[],"watermark_event_id":3,"subscription":"sub"}"#,
    )
    .expect("typed stale join");

    assert!(
        cursor
            .accept_rejoin_response(request, stale)
            .expect("correlated rejoin response")
    );
    assert_eq!(cursor.watermark(), 4);
    assert_eq!(
        cursor.state(),
        ChatCursorState::Reconciling {
            required_watermark: 4
        }
    );
}

#[test]
fn repeated_rejoin_is_busy_and_does_not_replace_the_first_request() {
    let mut cursor = joined_cursor("ch_demo", 4);
    cursor.disconnect();
    let first = cursor
        .rejoin_request(ChatTarget::CurrentRoom)
        .expect("first rejoin");
    assert!(
        cursor.rejoin_request(ChatTarget::CurrentRoom).is_err(),
        "only one recovery request may be active"
    );
    assert_eq!(cursor.state(), ChatCursorState::AwaitingJoin);

    let joined = ChatJoinResult::decode(
        br#"{"channel_id":"ch_demo","channel_type":"room","presence":[],"watermark_event_id":4,"subscription":"sub"}"#,
    )
    .expect("typed join");
    assert!(
        !cursor
            .accept_rejoin_response(first, joined)
            .expect("the first request remains authoritative")
    );
    assert_eq!(cursor.state(), ChatCursorState::Current);
    assert_eq!(cursor.watermark(), 4);
}

#[test]
fn rejoin_responses_are_typed_correlated_and_invalidated_by_revocation() {
    let mut first = joined_cursor("ch_demo", 4);
    let mut second = joined_cursor("ch_demo", 4);
    first.disconnect();
    second.disconnect();
    let first_request = first
        .rejoin_request(ChatTarget::CurrentRoom)
        .expect("first rejoin");
    second
        .rejoin_request(ChatTarget::CurrentRoom)
        .expect("second rejoin");
    let joined = ChatJoinResult::decode(
        br#"{"channel_id":"ch_demo","channel_type":"room","presence":[],"watermark_event_id":7,"subscription":"sub"}"#,
    )
    .expect("typed join");
    assert!(
        second
            .accept_rejoin_response(first_request, joined)
            .is_err(),
        "a cross-cursor response cannot revive another cursor"
    );
    assert_eq!(second.state(), ChatCursorState::AwaitingJoin);

    first.disconnect();
    let late_request = first
        .rejoin_request(ChatTarget::CurrentRoom)
        .expect("rejoin after disconnect invalidated the lost request handle");
    let revoked = valid_event("access_revoked");
    let revoked = ChatEvent::decode(&serde_json::to_vec(&revoked).expect("json")).expect("event");
    first.observe(&revoked).expect("revocation");
    let joined = ChatJoinResult::decode(
        br#"{"channel_id":"ch_demo","channel_type":"room","presence":[],"watermark_event_id":7,"subscription":"sub"}"#,
    )
    .expect("typed join");
    assert!(first.accept_rejoin_response(late_request, joined).is_err());
    assert_eq!(first.state(), ChatCursorState::Revoked);
    assert_eq!(first.watermark(), 0);
}

#[test]
fn rejoin_response_must_match_the_private_typed_target() {
    let mut cursor = joined_cursor("ch_demo", 4);
    cursor.disconnect();
    let request = cursor
        .rejoin_request(ChatTarget::Direct {
            other_user_id: "alice".to_owned(),
        })
        .expect("direct rejoin");
    let room_result = ChatJoinResult::decode(
        br#"{"channel_id":"ch_demo","channel_type":"room","presence":[],"watermark_event_id":4,"subscription":"sub"}"#,
    )
    .expect("typed room result");

    assert!(cursor.accept_rejoin_response(request, room_result).is_err());
    assert_eq!(cursor.state(), ChatCursorState::AwaitingJoin);
}
