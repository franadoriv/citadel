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
    InMemoryBackend, InMemoryChatRepository, InMemoryFriendsRepository, InMemoryGroupsRepository,
    InMemoryLeaderboardsRepository, InMemoryWalletRepository,
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

#[tokio::test]
async fn two_nodes_deliver_durable_chat_but_never_route_typing() {
    let friends = Arc::new(FriendsService::new(Arc::new(
        InMemoryFriendsRepository::new(),
    )));
    let groups = Arc::new(GroupsService::new(
        Arc::new(InMemoryGroupsRepository::new()),
    ));
    let chat_repository = Arc::new(InMemoryChatRepository::new());
    let chat = Arc::new(ChatService::new(chat_repository.clone()));
    let node_a = gateway("node-a", friends.clone(), groups.clone(), chat.clone());
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
    let local_event = alice_rx.recv().await.expect("local durable event");
    assert_eq!(local_event.envelope.kind, KIND_CHAT_EVENT);
    assert_eq!(rpc_response(&mut alice_rx).await.1, protocol::RPC_STATUS_OK);

    let destination = Arc::clone(&node_b);
    let destination_directory_for_router = Arc::clone(&destination_directory);
    let dispatcher = ChatDeliveryDispatcher::new(
        node("node-a"),
        chat_repository,
        source_directory,
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
