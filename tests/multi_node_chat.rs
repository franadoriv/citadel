//! Deterministic two-node coverage for the deliberately narrow chat cluster path.
//!
//! The nodes share durable repositories but keep independent gateways, session
//! registries, and local chat-presence registries. This exercises the same
//! destination-fenced durable command boundary production binds to mTLS without
//! requiring sockets, certificates, or timing-sensitive background workers.

use std::sync::Arc;

use citadel::chat_cluster::{
    ChatDeliveryDispatcher, ChatLeaseUpdate, ChatPresenceDirectory, ChatPresenceLease,
};
use citadel::realtime::registry::{ParticipantIdentity, SessionHandle};
use citadel::realtime::{
    ChatPresenceRegistry, DomainRpcServices, Gateway, Outbound, ParticipantId,
};
use citadel::repository::{
    ChatRepository, InMemoryBackend, InMemoryChatRepository, InMemoryFriendsRepository,
    InMemoryGroupsRepository, InMemoryLeaderboardsRepository, InMemoryWalletRepository,
};
use citadel::services::{
    ChatChannelAuthorizer, ChatRateLimitPolicy, ChatService, FriendsService, GroupsService,
    LeaderboardService, PlayerNotificationService, WalletService,
};
use citadel::session::{NodeId, OwnershipGeneration, SessionId};
use citadel::storage::UserId;
use citadel::time::{Clock, SystemClock, TimestampMillis};
use citadel::transport::codec::Envelope;
use citadel::transport::{Delivery, TransportKind};
use citadel_wire::protocol::{
    self, KIND_CHAT_EVENT, KIND_RPC_REQUEST, KIND_RPC_RESPONSE, decode_rpc_response,
    encode_rpc_request,
};
use tokio::sync::mpsc;

fn node(value: &str) -> NodeId {
    NodeId::new(value.to_owned()).expect("valid test node")
}

fn gateway(
    node_id: &str,
    friends: Arc<FriendsService>,
    groups: Arc<GroupsService>,
    chat: Arc<ChatService>,
) -> Gateway {
    Gateway::new().with_domain_services(DomainRpcServices {
        chat_authorizer: Arc::new(ChatChannelAuthorizer::new(
            Arc::clone(&friends),
            Arc::clone(&groups),
        )),
        chat_rate_limits: ChatRateLimitPolicy::default(),
        chat_presence: Arc::new(ChatPresenceRegistry::new()),
        chat_cluster_presence: None,
        node_id: node_id.to_owned(),
        friends,
        player_notifications: Arc::new(PlayerNotificationService::new(Arc::new(
            InMemoryBackend::new(),
        ))),
        groups,
        leaderboards: Arc::new(LeaderboardService::new(Arc::new(
            InMemoryLeaderboardsRepository::new(),
        ))),
        chat,
        wallet: Arc::new(WalletService::new(
            Arc::new(InMemoryWalletRepository::new()),
        )),
    })
}

fn register(gateway: &Gateway, user: &str) -> (ParticipantId, mpsc::Receiver<Outbound>) {
    let id = gateway.next_participant_id();
    let (outbound, receiver) = mpsc::channel(8);
    gateway.register_session(SessionHandle {
        id,
        kind: TransportKind::WebSocket,
        outbound,
        identity: Some(ParticipantIdentity {
            user_id: UserId::new(user).expect("valid test user"),
            session_id: SessionId::new(format!("session-{user}")).expect("valid test session"),
            expires_at: TimestampMillis::from_unix_millis(9_999_999_999),
        }),
    });
    (id, receiver)
}

fn rpc(request_id: u64, method: &str, body: serde_json::Value) -> Envelope {
    Envelope::new(
        KIND_RPC_REQUEST,
        encode_rpc_request(request_id, method, body.to_string().as_bytes()),
    )
}

async fn rpc_response(receiver: &mut mpsc::Receiver<Outbound>) -> (u64, u8, Vec<u8>) {
    let outbound = receiver.recv().await.expect("RPC response delivered");
    assert_eq!(outbound.envelope.kind, KIND_RPC_RESPONSE);
    let response = decode_rpc_response(&outbound.envelope.body).expect("valid RPC response");
    (
        response.request_id,
        response.status,
        response.payload.to_vec(),
    )
}

async fn two_nodes_deliver_durable_chat_but_never_route_typing(
    chat_repository: Arc<dyn ChatRepository>,
) {
    let friends = Arc::new(FriendsService::new(Arc::new(
        InMemoryFriendsRepository::new(),
    )));
    let groups = Arc::new(GroupsService::new(
        Arc::new(InMemoryGroupsRepository::new()),
    ));
    let chat = Arc::new(ChatService::new(Arc::clone(&chat_repository)));
    let node_a = Arc::new(gateway(
        "node-a",
        friends.clone(),
        groups.clone(),
        chat.clone(),
    ));
    let node_b = Arc::new(gateway("node-b", friends, groups, chat));
    let (alice, mut alice_rx) = register(&node_a, "alice");
    let (bob, mut bob_rx) = register(&node_b, "bob");

    // Establish one authorized direct channel while the participants remain on
    // different local gateways.
    node_a.handle_inbound(
        alice,
        &rpc(1, "friends.add", serde_json::json!({"other": "bob"})),
    );
    assert_eq!(rpc_response(&mut alice_rx).await.1, protocol::RPC_STATUS_OK);
    node_b.handle_inbound(
        bob,
        &rpc(2, "friends.add", serde_json::json!({"other": "alice"})),
    );
    assert_eq!(rpc_response(&mut bob_rx).await.1, protocol::RPC_STATUS_OK);

    node_a.handle_inbound(
        alice,
        &rpc(
            3,
            "chat.join",
            serde_json::json!({
                "target": {"kind": "direct", "other_user_id": "bob"}
            }),
        ),
    );
    let (_, status, body) = rpc_response(&mut alice_rx).await;
    assert_eq!(status, protocol::RPC_STATUS_OK);
    let channel_id =
        serde_json::from_slice::<serde_json::Value>(&body).expect("chat join JSON")["channel_id"]
            .as_str()
            .expect("channel id")
            .to_owned();

    node_b.handle_inbound(
        bob,
        &rpc(
            4,
            "chat.join",
            serde_json::json!({
                "target": {"kind": "direct", "other_user_id": "alice"}
            }),
        ),
    );
    assert_eq!(rpc_response(&mut bob_rx).await.1, protocol::RPC_STATUS_OK);

    // Each node has its own leased directory. Node A learns node B's
    // channel-level lease; node B independently validates that same fence.
    let now = SystemClock.now();
    let lease = ChatPresenceLease {
        channel_id: channel_id.clone(),
        node_id: node("node-b"),
        generation: OwnershipGeneration::new(1),
        expires_at: TimestampMillis::from_unix_millis(now.unix_millis().saturating_add(60_000)),
    };
    let source_directory = Arc::new(ChatPresenceDirectory::default());
    let destination_directory = Arc::new(ChatPresenceDirectory::default());
    assert_eq!(
        source_directory.advertise(lease.clone(), now),
        ChatLeaseUpdate::Applied
    );
    assert_eq!(
        destination_directory.advertise(lease, now),
        ChatLeaseUpdate::Applied
    );

    node_a.handle_inbound(
        alice,
        &rpc(
            5,
            "chat.send",
            serde_json::json!({
                "channel_id": channel_id,
                "content": "durable across nodes"
            }),
        ),
    );
    assert_eq!(rpc_response(&mut alice_rx).await.1, protocol::RPC_STATUS_OK);

    let local = Arc::clone(&node_a);
    let local_delivery = Arc::clone(&local);
    let local_node = node("node-a");
    let destination = Arc::clone(&node_b);
    let destination_directory_for_router = Arc::clone(&destination_directory);
    let dispatcher = ChatDeliveryDispatcher::new_with_local_delivery(
        local_node.clone(),
        chat_repository,
        source_directory,
        Arc::new(move |delivery| Ok(local_delivery.deliver_local_chat(&local_node, delivery))),
        Arc::new(move |destination_node, delivery| {
            assert_eq!(destination_node.as_str(), "node-b");
            Ok(destination.deliver_remote_chat(
                destination_node,
                &destination_directory_for_router,
                delivery,
            ))
        }),
    );
    let stats = dispatcher
        .dispatch_once(now, 8)
        .await
        .expect("dispatch succeeds");
    assert_eq!(stats.attempted, 1);
    assert_eq!(stats.acknowledged, 1);
    let local_event = alice_rx
        .recv()
        .await
        .expect("dispatcher delivers source-local durable event");
    assert_eq!(local_event.envelope.kind, KIND_CHAT_EVENT);
    let remote_event = bob_rx.recv().await.expect("remote durable event");
    assert_eq!(remote_event.delivery, Delivery::Reliable);
    assert_eq!(remote_event.envelope.kind, KIND_CHAT_EVENT);
    let remote_json: serde_json::Value =
        serde_json::from_slice(&remote_event.envelope.body).expect("chat event JSON");
    assert_eq!(remote_json["type"], "message.create");
    assert_eq!(remote_json["message"]["content"], "durable across nodes");

    // Typing is intentionally ephemeral and process-local: it never enters
    // the durable dispatcher and must not appear on node B's socket queue.
    node_a.handle_inbound(
        alice,
        &rpc(
            6,
            "chat.typing",
            serde_json::json!({
                "channel_id": remote_json["channel_id"],
                "typing": true
            }),
        ),
    );
    assert_eq!(rpc_response(&mut alice_rx).await.1, protocol::RPC_STATUS_OK);
    assert!(bob_rx.try_recv().is_err(), "typing must not cross nodes");
    assert_eq!(
        dispatcher
            .dispatch_once(now, 8)
            .await
            .expect("typing creates no durable work")
            .loaded,
        0
    );
}

async fn two_nodes_deliver_moderation_tombstone_locally_and_remotely(
    chat_repository: Arc<dyn ChatRepository>,
) {
    let friends = Arc::new(FriendsService::new(Arc::new(
        InMemoryFriendsRepository::new(),
    )));
    let groups = Arc::new(GroupsService::new(
        Arc::new(InMemoryGroupsRepository::new()),
    ));
    let chat = Arc::new(ChatService::new(Arc::clone(&chat_repository)));
    let node_a = Arc::new(gateway(
        "node-a",
        friends.clone(),
        groups.clone(),
        chat.clone(),
    ));
    let node_b = Arc::new(gateway("node-b", friends, groups, chat));
    let (alice, mut alice_rx) = register(&node_a, "alice");
    let (bob, mut bob_rx) = register(&node_b, "bob");

    node_a.handle_inbound(
        alice,
        &rpc(20, "groups.create", serde_json::json!({"name": "Raiders"})),
    );
    let (_, status, body) = rpc_response(&mut alice_rx).await;
    assert_eq!(status, protocol::RPC_STATUS_OK);
    let group_id = serde_json::from_slice::<serde_json::Value>(&body).expect("group JSON")["id"]
        .as_u64()
        .expect("group id");
    node_a.handle_inbound(
        alice,
        &rpc(
            21,
            "groups.add_member",
            serde_json::json!({"group_id": group_id, "user_id": "bob"}),
        ),
    );
    assert_eq!(rpc_response(&mut alice_rx).await.1, protocol::RPC_STATUS_OK);

    for (gateway, participant, receiver, request_id) in [
        (&*node_a, alice, &mut alice_rx, 22_u64),
        (&*node_b, bob, &mut bob_rx, 23_u64),
    ] {
        gateway.handle_inbound(
            participant,
            &rpc(
                request_id,
                "chat.join",
                serde_json::json!({"target": {"kind": "group", "group_id": group_id}}),
            ),
        );
        assert_eq!(rpc_response(receiver).await.1, protocol::RPC_STATUS_OK);
    }
    let channel = chat_repository
        .resolve_canonical_channel(
            &format!("group:{group_id}"),
            citadel::repository::ChannelType::Group,
            SystemClock.now(),
        )
        .await
        .expect("group channel");

    let now = SystemClock.now();
    let expires_at = TimestampMillis::from_unix_millis(now.unix_millis().saturating_add(60_000));
    let directory_a = Arc::new(ChatPresenceDirectory::default());
    let directory_b = Arc::new(ChatPresenceDirectory::default());
    for directory in [&directory_a, &directory_b] {
        assert_eq!(
            directory.advertise(
                ChatPresenceLease {
                    channel_id: channel.id.clone(),
                    node_id: node("node-a"),
                    generation: OwnershipGeneration::new(1),
                    expires_at,
                },
                now,
            ),
            ChatLeaseUpdate::Applied
        );
        assert_eq!(
            directory.advertise(
                ChatPresenceLease {
                    channel_id: channel.id.clone(),
                    node_id: node("node-b"),
                    generation: OwnershipGeneration::new(1),
                    expires_at,
                },
                now,
            ),
            ChatLeaseUpdate::Applied
        );
    }

    node_b.handle_inbound(
        bob,
        &rpc(
            24,
            "chat.send",
            serde_json::json!({"channel_id": channel.id, "content": "remove me"}),
        ),
    );
    assert_eq!(rpc_response(&mut bob_rx).await.1, protocol::RPC_STATUS_OK);

    let source_b = Arc::clone(&node_b);
    let source_node_b = node("node-b");
    let destination_a = Arc::clone(&node_a);
    let destination_directory_a = Arc::clone(&directory_a);
    ChatDeliveryDispatcher::new_with_local_delivery(
        source_node_b.clone(),
        Arc::clone(&chat_repository),
        Arc::clone(&directory_b),
        Arc::new(move |delivery| Ok(source_b.deliver_local_chat(&source_node_b, delivery))),
        Arc::new(move |destination, delivery| {
            Ok(destination_a.deliver_remote_chat(destination, &destination_directory_a, delivery))
        }),
    )
    .dispatch_once(now, 8)
    .await
    .expect("deliver create");
    let bob_create = bob_rx
        .recv()
        .await
        .expect("dispatcher delivers sender create");
    assert_eq!(bob_create.envelope.kind, KIND_CHAT_EVENT);
    let alice_create = alice_rx.recv().await.expect("recipient receives create");
    assert_eq!(alice_create.envelope.kind, KIND_CHAT_EVENT);

    node_a.handle_inbound(
        alice,
        &rpc(
            25,
            "chat.moderate",
            serde_json::json!({"channel_id": channel.id, "message_id": 1}),
        ),
    );
    let (_, status, body) = rpc_response(&mut alice_rx).await;
    assert_eq!(status, protocol::RPC_STATUS_OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).expect("moderation RPC JSON")["deleted"],
        true
    );

    let source_a = Arc::clone(&node_a);
    let source_node_a = node("node-a");
    let destination_b = Arc::clone(&node_b);
    let destination_directory_b = Arc::clone(&directory_b);
    let stats = ChatDeliveryDispatcher::new_with_local_delivery(
        source_node_a.clone(),
        chat_repository,
        directory_a,
        Arc::new(move |delivery| Ok(source_a.deliver_local_chat(&source_node_a, delivery))),
        Arc::new(move |destination, delivery| {
            Ok(destination_b.deliver_remote_chat(destination, &destination_directory_b, delivery))
        }),
    )
    .dispatch_once(now, 8)
    .await
    .expect("deliver remove");
    assert_eq!(stats.attempted, 1);
    assert_eq!(stats.acknowledged, 1);
    let alice_remove = alice_rx
        .recv()
        .await
        .expect("dispatcher delivers moderator remove");
    assert_eq!(alice_remove.envelope.kind, KIND_CHAT_EVENT);
    let remove_json: serde_json::Value =
        serde_json::from_slice(&alice_remove.envelope.body).expect("remove JSON");
    assert_eq!(remove_json["type"], "message.remove");
    assert_eq!(remove_json["message"]["deleted"], true);
    let bob_remove = bob_rx.recv().await.expect("author receives remote remove");
    assert_eq!(bob_remove.envelope.kind, 28);
    let remote_json: serde_json::Value =
        serde_json::from_slice(&bob_remove.envelope.body).expect("remote remove JSON");
    assert_eq!(remote_json["type"], "message.remove");
    assert_eq!(remote_json["message"]["deleted"], true);
}

#[tokio::test]
async fn two_nodes_deliver_moderation_tombstone_in_memory_reference() {
    two_nodes_deliver_moderation_tombstone_locally_and_remotely(Arc::new(
        InMemoryChatRepository::new(),
    ))
    .await;
}

#[tokio::test]
async fn two_nodes_deliver_durable_chat_but_never_route_typing_in_memory_reference() {
    two_nodes_deliver_durable_chat_but_never_route_typing(Arc::new(InMemoryChatRepository::new()))
        .await;
}

#[tokio::test]
async fn two_nodes_deliver_durable_chat_but_never_route_typing_over_mongodb_rs0() {
    let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL").ok() else {
        eprintln!("skipping MongoDB multi-node chat: CITADEL_TEST_MONGODB_URL is unset");
        return;
    };
    let db = citadel::repository::MongoDatabase::connect(&citadel::config::DatabaseConfig {
        url: Some(url),
        ..citadel::config::DatabaseConfig::default()
    })
    .await
    .expect("connect + reconcile MongoDB replica set");
    for collection in [
        "chat_channels",
        "chat_access_epochs",
        "chat_messages",
        "chat_events",
        "chat_moderation_audit",
        "chat_rate_limits",
        "chat_delivery_outbox",
    ] {
        db.database_for_tests()
            .collection::<mongodb::bson::Document>(collection)
            .delete_many(mongodb::bson::doc! {})
            .await
            .expect("clear MongoDB multi-node chat fixture");
    }
    two_nodes_deliver_durable_chat_but_never_route_typing(Arc::new(db.mongo_chat_repository()))
        .await;
}
