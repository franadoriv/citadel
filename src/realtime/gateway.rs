//! Realtime gateway: routes inbound application messages between sessions.
//!
//! For Step 1 the gateway implements a single global "room": a position update
//! from one session is relayed to every OTHER session (no echo to the sender),
//! tagged with the sender's [`ParticipantId`] so receivers can render each other.
//! The gateway depends only on the [`SessionRegistry`]'s abstract outbound sink,
//! never on a concrete transport.
//!
//! Wire kinds (shared with the demos):
//!
//! - [`KIND_POSITION`] (client -> server): "my position update". Body is opaque
//!   (the demos use little-endian f32 coordinates).
//! - [`KIND_PEER_POSITION`] (server -> client): a relayed position. Body is
//!   `u64` big-endian sender session id followed by the original payload.
//!
//! Unknown kinds are dropped and logged. A typed message taxonomy is future
//! work; this is the minimal relay needed to prove multi-client interaction.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use citadel_wire::diagnostics::{
    Capabilities, CaptureId, CaptureStatus, ClockSync, FlushCapture, ServerTime, StartCapture,
};
use citadel_wire::protocol;
use serde::Serialize;

use crate::authoritative_decision_telemetry::{
    AuthoritativeDecisionCorrelation, AuthoritativeDecisionOutcome, AuthoritativeDecisionReason,
    AuthoritativeDecisionRecorder,
};
use crate::chat_cluster::{
    ChatDeliveryDisposition, ChatPresenceDirectory, LocalChatPresenceAnnouncer, RemoteChatDelivery,
};
use crate::lag_diagnostics::{CaptureFlushGrant, CaptureFlushPlan, LagDiagnosticsService};
use crate::lifecycle::CancellationToken;
use crate::maps::MapCatalog;
use crate::match_recorder::{MatchRecorder, MatchTerminationReason};
use crate::matchmaker::{Matchmaker, MatchmakerStats, TicketId, TicketRequest, TicketState};
use crate::matchmaker_cluster::{
    InMemoryMatchmakerCluster, InMemoryMatchmakerHandoffRouter, MatchmakerRouterError,
    MatchmakerShardLease, PartyAdmissionFence, RemoteMatchmakerAdmission, RemoteMatchmakerHandoff,
    RemoteMatchmakerTicketOwner,
};
use crate::matchmaker_live::LiveMatchmakerNode;
use crate::matchmaker_transport::{
    PartyControlCommand, PartyControlOperation, PartyControlReply, PartyQueueAdmission,
    TlsMatchmakerHandoffRouter,
};
use crate::observability::{NodeMetrics, ScriptGateSurface};
use crate::party::{PartyId, PartyRegistry, PartySnapshot};
use crate::party_presence::{
    LocalPartyPresence, PartyPresenceCommand, PartyPresenceDelivery,
    PartyPresenceDeliveryDisposition, PartyPresenceDirectory, PartyPresenceLease,
    PartyPresenceSnapshot, PartyPresenceWithdrawal, RemotePartyPresenceDelivery,
};
use crate::realtime::auth::{AuthOutcome, Authenticator, PresentedCredential};
use crate::realtime::chat_presence::{ChatPresenceRegistry, ChatSubscription};
use crate::realtime::diagnostics::{
    LagCaptureError, LagCaptureFlush, LagCaptureManager, LagCaptureStart, LagCaptureStatus,
};
use crate::realtime::identity::ResumeSecret;
use crate::realtime::netpeer::layout::RepLayout;
use crate::realtime::netpeer::{RepAuthority, RepReject, RepSnapshot, Validated};
use crate::realtime::registry::{
    CloseDisposition, LatestOutboundReceiver, Outbound, ParticipantId, ParticipantIdGen,
    ReplacedTransportCleanup, SessionHandle, SessionRegistry,
};
use crate::realtime::rooms::{
    BridgeMode, JoinError, RemoteRoomMember, RoomId, RoomLabel, RoomRegistry, RoomSnapshot,
};
use crate::realtime::transform::TransformHub;
use crate::runtime::{
    BridgeCommandSink, BridgeMatchContext, BridgeQuotas, BridgeRepField, BridgeRepValue,
    BridgeTransform, Capability, Correction, Decision, EventDraft, FireIntent, GameScriptReadiness,
    LifecycleHook, MAX_MATCH_MESSAGE_BODY_BYTES, MAX_RESERVED_KIND,
    NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE, NativeMatchContext, NativeMatchLifecycleHook,
    NativeMatchLifecycleUnavailable, NormalizedEventBatch, NormalizedPayload, OutboundCommand,
    PendingBatchLedger, RealtimeAfterOutcome, RealtimeInterception, RoomBridgeMode, RpcOutcome,
    Runtime, SCRIPT_UNAVAILABLE_MESSAGE, ScriptBinding, ScriptCommand, ScriptCommandBatch,
    ValidatedBatch, ValidatedOutcome,
};
use crate::services::party_directory::{
    PartyOwnerResolution, PartyQueueFreeze, StoragePartyDirectory,
};
use crate::services::{
    ChatChannelAuthorizer, ChatRateLimitPolicy, ChatService, ChatTarget, CreateGroupRequest,
    FriendsService, Group, GroupFilter, GroupsService, LeaderboardService, PlayerNotification,
    PlayerNotificationDelivery, PlayerNotificationService, UpdateGroupRequest, WalletService,
    validate_chat_content,
};
use crate::session::SessionTokenSecret;
use crate::session::{NodeId, OwnershipGeneration};
use crate::time::{Clock, DurationMillis, SystemClock, TimestampMillis};
use crate::transport::{Delivery, Envelope};
use citadel_wire::match_input::{MatchInput, MatchInputAck};
use citadel_wire::netpeer::{FieldDelta, RepSchema};

pub use citadel_wire::protocol::{
    KIND_AUTH, KIND_AUTH_RESULT, KIND_CHAT_EVENT, KIND_DIAG_CAPABILITIES, KIND_DIAG_CLOCK_SYNC,
    KIND_DIAG_FLUSH, KIND_DIAG_SERVER_TIME, KIND_DIAG_START, KIND_DIAG_STATUS, KIND_MATCH_INPUT,
    KIND_MATCH_INPUT_ACK, KIND_MATCHMAKER_MATCHED, KIND_NA_DESPAWN, KIND_NA_PRESENCE,
    KIND_NA_SPAWN, KIND_NA_SPAWN_BATCH, KIND_NA_STATE, KIND_NOTIFICATION, KIND_PEER_POSITION,
    KIND_POSITION, KIND_REP_ACK, KIND_REP_DELTA, KIND_ROOM_CREATE, KIND_ROOM_JOIN,
    KIND_ROOM_JOINED, KIND_ROOM_LEAVE, KIND_ROOM_MAP_READY, KIND_RPC_REQUEST, KIND_RPC_RESPONSE,
    KIND_TSYNC_ACK, KIND_TSYNC_HELLO, KIND_TSYNC_INPUT, KIND_TSYNC_REWIND, KIND_TSYNC_ROLE,
    KIND_TSYNC_SNAPSHOT, KIND_TSYNC_V2_HELLO, KIND_TSYNC_V2_INPUT, ROOM_KIND_MAX, ROOM_KIND_MIN,
    RPC_STATUS_OK,
};

const MATCHMAKER_HANDOFF_TTL_MS: u64 = 30_000;
pub(crate) const REMOTE_AUTHORITATIVE_ADMISSION_UNAVAILABLE_MESSAGE: &str =
    "remote authoritative match admission is unavailable";
/// Receiver-side expiration for a chat typing indication. Typing is intentionally
/// ephemeral: it has no durable event id and never participates in resync.
const CHAT_TYPING_TTL_MS: u64 = 5_000;
const PARTY_OWNER_LEASE_MS: u64 = 15_000;
const PARTY_PRESENCE_LEASE_MS: u64 = 15_000;

/// Native ingress metadata supplied to the authoritative bridge for a generic
/// custom client message. Sequence is optional because framing transports do not
/// all expose a stable application sequence; callers must never fabricate one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundMessageMetadata {
    /// Whether this envelope arrived on a reliable path.
    pub reliable: bool,
    /// Native ingress sequence when the transport provides one.
    pub sequence: Option<u64>,
}

impl InboundMessageMetadata {
    /// Metadata for a reliable, ordered ingress path without an exposed
    /// application sequence.
    #[must_use]
    pub const fn reliable() -> Self {
        Self {
            reliable: true,
            sequence: None,
        }
    }

    /// Metadata for an unreliable ingress path, which exposes no sequence.
    #[must_use]
    pub const fn unreliable() -> Self {
        Self {
            reliable: false,
            sequence: None,
        }
    }
}

impl Default for InboundMessageMetadata {
    fn default() -> Self {
        Self::reliable()
    }
}

/// Native result for a secure FLUSH. Unlike the legacy base lifecycle result,
/// this carries a distinct redacted grant for every realtime participant so a
/// bearer cannot be replayed by another client in the same capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LagCaptureUploadFlush {
    /// Per-participant FLUSH bodies to enqueue on the matching session only.
    pub grants: Vec<CaptureFlushGrant>,
    /// Sessions whose bounded queues accepted their own FLUSH body.
    pub requested: Vec<ParticipantId>,
    /// Sessions whose queue could not accept a body; their grants were
    /// durably consumed and removed from the expected-upload denominator.
    pub enqueue_failed: Vec<ParticipantId>,
}

/// Session-node presence state. It is intentionally separate from the durable
/// party directory: it contains only local sockets and is discarded on crash.
#[derive(Debug)]
struct PartyPresenceGateway {
    directory: Arc<PartyPresenceDirectory>,
    local: LocalPartyPresence,
    remote: Mutex<HashMap<String, HashMap<NodeId, PartyPresenceSnapshot>>>,
    source_sequences: Mutex<HashMap<String, u64>>,
    generations: Mutex<HashMap<String, u64>>,
    active: Mutex<HashMap<String, OwnershipGeneration>>,
}

impl PartyPresenceGateway {
    fn new(directory: Arc<PartyPresenceDirectory>) -> Self {
        Self {
            directory,
            local: LocalPartyPresence::default(),
            remote: Mutex::new(HashMap::new()),
            source_sequences: Mutex::new(HashMap::new()),
            generations: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
        }
    }

    fn renew(
        &self,
        party_id: &str,
        node_id: NodeId,
        revision: u64,
        now: TimestampMillis,
    ) -> Option<PartyPresenceLease> {
        let mut active = self.active.lock().ok()?;
        let generation = match active.get(party_id) {
            Some(generation) => *generation,
            None => {
                let mut generations = self.generations.lock().ok()?;
                let next = generations.entry(party_id.to_owned()).or_insert(0);
                *next = next.saturating_add(1);
                let generation = OwnershipGeneration::new(*next);
                active.insert(party_id.to_owned(), generation);
                generation
            }
        };
        let expires_at = now
            .checked_add(DurationMillis::from_millis(PARTY_PRESENCE_LEASE_MS))
            .ok()?;
        Some(PartyPresenceLease {
            party_id: party_id.to_owned(),
            node_id,
            generation,
            expires_at,
            party_revision: revision,
        })
    }

    fn withdraw(&self, party_id: &str, node_id: NodeId) -> Option<PartyPresenceWithdrawal> {
        self.active
            .lock()
            .ok()?
            .remove(party_id)
            .map(|generation| PartyPresenceWithdrawal {
                party_id: party_id.to_owned(),
                node_id,
                generation,
            })
    }

    fn active_generation(&self, party_id: &str) -> Option<OwnershipGeneration> {
        self.active.lock().ok()?.get(party_id).copied()
    }

    /// Replace one remote node's most recent local-member snapshot. A source
    /// sequence is monotonic only within that source node, which is exactly the
    /// scope used here; the receiving node creates the client-visible sequence.
    fn replace_remote(
        &self,
        party_id: &str,
        source: NodeId,
        snapshot: PartyPresenceSnapshot,
    ) -> bool {
        let Ok(mut remote) = self.remote.lock() else {
            return false;
        };
        let snapshots = remote.entry(party_id.to_owned()).or_default();
        if snapshots.get(&source).is_some_and(|current| {
            (snapshot.party_revision, snapshot.sequence)
                <= (current.party_revision, current.sequence)
        }) {
            return false;
        }
        snapshots.insert(source, snapshot);
        true
    }

    fn clear_remote(&self, party_id: &str, source: &NodeId) {
        let Ok(mut remote) = self.remote.lock() else {
            return;
        };
        if let Some(snapshots) = remote.get_mut(party_id) {
            snapshots.remove(source);
            if snapshots.is_empty() {
                remote.remove(party_id);
            }
        }
    }

    fn merged_online_members(&self, party_id: &str) -> Vec<String> {
        let mut members = self.local.online_members(party_id);
        if let Ok(remote) = self.remote.lock()
            && let Some(snapshots) = remote.get(party_id)
        {
            members.extend(
                snapshots
                    .values()
                    .flat_map(|snapshot| snapshot.online_members.iter().cloned()),
            );
        }
        members.sort();
        members.dedup();
        members
    }

    fn source_snapshot(&self, party_id: &str, party_revision: u64) -> PartyPresenceSnapshot {
        let sequence = self
            .source_sequences
            .lock()
            .ok()
            .map(|mut sequences| {
                let sequence = sequences.entry(party_id.to_owned()).or_insert(0);
                *sequence = sequence.saturating_add(1);
                *sequence
            })
            .unwrap_or(0);
        PartyPresenceSnapshot {
            party_id: party_id.to_owned(),
            party_revision,
            sequence,
            online_members: self.local.online_members(party_id),
        }
    }
}

#[derive(Clone)]
struct DurablePartyGateway {
    directory: Arc<StoragePartyDirectory>,
    node_id: NodeId,
    router: Arc<TlsMatchmakerHandoffRouter>,
}

/// Drive a durable party-directory future to completion from the synchronous
/// inbound RPC path.
///
/// The frame dispatch in [`Gateway::handle_inbound`] is synchronous while the
/// party directory is async, so the two have to be bridged. When the server's
/// multi-threaded runtime is available this reuses it via `block_in_place`,
/// exactly as [`ServiceDomainHost::block`](crate::runtime::host_services) does
/// for host calls: the worker hands its queued tasks to a sibling thread rather
/// than stalling the scheduler.
///
/// Building a fresh runtime per call — the previous behaviour — cost an OS
/// thread spawn plus reactor and timer setup on every party RPC, and discarded
/// the connection pool and timer state immediately afterwards.
///
/// The dedicated-thread path survives as a fallback for callers with no runtime
/// or a current-thread one (unit tests constructing a gateway directly). It is
/// never taken on the server.
fn party_block_on<T: Send + 'static>(
    future: impl Future<Output = crate::error::AppResult<T>> + Send + 'static,
) -> crate::error::AppResult<T> {
    use tokio::runtime::{Handle, RuntimeFlavor};

    if let Ok(handle) = Handle::try_current()
        && handle.runtime_flavor() == RuntimeFlavor::MultiThread
    {
        // `block_in_place` is a no-op off a worker thread, so this is also
        // correct when the tick loop calls in from the blocking pool.
        return tokio::task::block_in_place(|| handle.block_on(future));
    }

    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| crate::error::AppError::internal(error.to_string()))?
            .block_on(future)
    })
    .join()
    .map_err(|_| crate::error::AppError::internal("party directory worker panicked"))?
}

#[derive(Clone)]
struct JoinToken(String);

impl JoinToken {
    fn generate() -> Result<Self, getrandom::Error> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes)?;
        Ok(Self(
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes),
        ))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for JoinToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JoinToken([redacted])")
    }
}

#[derive(Debug, Clone)]
struct PendingMatchHandoff {
    user_id: String,
    room_id: RoomId,
    token: JoinToken,
    expires_at: crate::time::TimestampMillis,
}

#[derive(Debug, Clone)]
struct QueuedTicketOwner {
    user_id: String,
    participant: ParticipantId,
}

#[derive(Debug, Default)]
struct MatchmakerHandoffs {
    queued_owners: HashMap<TicketId, Vec<QueuedTicketOwner>>,
    pending: HashMap<TicketId, Vec<PendingMatchHandoff>>,
}

#[derive(Clone)]
struct ClusterMatchmakerGateway {
    node_id: NodeId,
    lease: MatchmakerShardLease,
    authority: Arc<InMemoryMatchmakerCluster>,
    router: Arc<InMemoryMatchmakerHandoffRouter>,
}

/// Domain-feature services reachable from built-in client RPC methods.
///
/// A game client calls a persisted domain feature by sending a `KIND_RPC_REQUEST`
/// with a reserved dotted method name (`friends.add`, …) and a JSON payload; the
/// gateway answers from these services instead of the script runtime. The struct
/// grows one field per feature as its client RPC lands; the first is friends.
#[derive(Clone)]
pub struct DomainRpcServices {
    /// Friend relationships ( persistence,  client RPC).
    pub friends: Arc<FriendsService>,
    /// Player inbox reads and read acknowledgements.
    pub player_notifications: Arc<PlayerNotificationService>,
    /// Player-authorized groups/clans operations.
    pub groups: Arc<GroupsService>,
    /// Read/submit game leaderboard operations.
    pub leaderboards: Arc<LeaderboardService>,
    /// Durable chat history operations.
    pub chat: Arc<ChatService>,
    /// Canonical descriptor and social/group/room access policy.
    pub chat_authorizer: Arc<ChatChannelAuthorizer>,
    /// Cross-node, repository-owned chat abuse policy.
    pub chat_rate_limits: ChatRateLimitPolicy,
    /// Single-node presence and bounded reconciliation state.
    pub chat_presence: Arc<ChatPresenceRegistry>,
    /// Optional cluster lease publisher. It contains only channel/node leases,
    /// never participant or socket identities.
    pub chat_cluster_presence: Option<Arc<LocalChatPresenceAnnouncer>>,
    /// Stable node attribution retained with redacted chat moderation audits.
    pub node_id: String,
    /// Player-owned wallet reads.
    pub wallet: Arc<WalletService>,
}

#[derive(Serialize)]
struct ChatCreateResponse<'a> {
    message: &'a crate::repository::ChatMessage,
    event_id: u64,
}

impl<'a> From<&'a crate::repository::ChatMessage> for ChatCreateResponse<'a> {
    fn from(message: &'a crate::repository::ChatMessage) -> Self {
        Self {
            message,
            event_id: message.last_event_id,
        }
    }
}

#[derive(Serialize)]
struct ChatEditResponse<'a> {
    message: &'a crate::repository::ChatMessage,
    event_id: u64,
}

impl<'a> From<&'a crate::repository::ChatMessage> for ChatEditResponse<'a> {
    fn from(message: &'a crate::repository::ChatMessage) -> Self {
        Self {
            message,
            event_id: message.last_event_id,
        }
    }
}

#[derive(Serialize)]
struct ChatDeleteResponse {
    deleted: bool,
    message_id: u64,
    event_id: Option<u64>,
}

impl ChatDeleteResponse {
    const fn deleted(message_id: u64, event_id: u64) -> Self {
        Self {
            deleted: true,
            message_id,
            event_id: Some(event_id),
        }
    }

    const fn not_deleted(message_id: u64) -> Self {
        Self {
            deleted: false,
            message_id,
            event_id: None,
        }
    }
}

#[derive(Serialize)]
struct ChatModerateResponse {
    deleted: bool,
    message_id: u64,
    event_id: Option<u64>,
}

impl ChatModerateResponse {
    const fn deleted(message_id: u64, event_id: u64) -> Self {
        Self {
            deleted: true,
            message_id,
            event_id: Some(event_id),
        }
    }

    const fn not_deleted(message_id: u64) -> Self {
        Self {
            deleted: false,
            message_id,
            event_id: None,
        }
    }
}

/// A domain-RPC method name reserved for the server's built-in handlers.
///
/// A method with one of these prefixes is answered by [`DomainRpcServices`], not
/// the script runtime, so a game cannot shadow `friends.add` with an `on_rpc`
/// handler of the same name.
#[must_use]
fn is_domain_rpc_method(method: &str) -> bool {
    method.starts_with("friends.")
        || method.starts_with("notifications.")
        || method.starts_with("groups.")
        || method.starts_with("leaderboards.")
        || method.starts_with("chat.")
        || method.starts_with("wallet.")
}

fn ticket_id_arg(payload: &[u8]) -> Result<TicketId, String> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| "invalid JSON body".to_owned())?;
    let raw = value
        .get("ticket_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "missing string field: ticket_id".to_owned())?;
    TicketId::parse(raw).map_err(|error| error.to_string())
}

fn join_token_arg(payload: &[u8]) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| "invalid JSON body".to_owned())?;
    let raw = value
        .get("join_token")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "missing string field: join_token".to_owned())?;
    if raw.is_empty()
        || raw.len() > 128
        || !raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err("invalid join_token".to_owned());
    }
    Ok(raw.to_owned())
}

fn party_id_arg(payload: &[u8]) -> Result<PartyId, String> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| "invalid JSON body".to_owned())?;
    let raw = value
        .get("party_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "missing string field: party_id".to_owned())?;
    PartyId::parse(raw).map_err(|error| error.to_string())
}

fn target_user_id_arg(payload: &[u8]) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| "invalid JSON body".to_owned())?;
    value
        .get("target_user_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| "missing string field: target_user_id".to_owned())
}

fn expected_party_revision_arg(payload: &[u8]) -> Result<Option<u64>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| "invalid party RPC JSON payload".to_owned())?;
    Ok(value.get("revision").and_then(serde_json::Value::as_u64))
}

const fn matchmaker_state_name(state: TicketState) -> &'static str {
    match state {
        TicketState::Queued => "queued",
        TicketState::Matched => "matched",
        TicketState::Removed => "removed",
    }
}

fn party_json(party: PartySnapshot) -> serde_json::Value {
    serde_json::json!({
        "party_id": party.party_id.as_str(),
        "leader_user_id": party.leader_user_id,
        "members": party.members,
        "invitations": party.invitations,
        "revision": party.revision,
    })
}

impl DomainRpcServices {
    /// Answer one built-in domain RPC, returning `(status, json_body)` for a
    /// `KIND_RPC_RESPONSE`. A missing `user_id` (guest caller) or any service
    /// error yields [`protocol::RPC_STATUS_ERROR`] with a short UTF-8 message;
    /// success yields [`protocol::RPC_STATUS_OK`] with a JSON object body.
    async fn dispatch(
        &self,
        sender: ParticipantId,
        registry: &SessionRegistry,
        method: &str,
        user_id: Option<&str>,
        room_id: Option<RoomId>,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let Some(user) = user_id else {
            return Self::err("authentication required");
        };
        let response = match method {
            "friends.add" => self.friends_add(user, payload).await,
            "friends.remove" => self.friends_remove(user, payload).await,
            "friends.block" => self.friends_block(user, payload).await,
            "friends.list" => self.friends_list(user).await,
            "notifications.list" => self.notifications_list(user, payload).await,
            "notifications.mark_read" => self.notifications_mark_read(user, payload).await,
            "groups.create" => self.groups_create(user, payload).await,
            "groups.list" => self.groups_list(payload).await,
            "groups.get" => self.groups_get(payload).await,
            "groups.update" => self.groups_update(user, payload).await,
            "groups.delete" => self.groups_delete(user, payload).await,
            "groups.add_member" => self.groups_add_member(user, payload).await,
            "groups.leave" => self.groups_leave(user, payload).await,
            "groups.kick" => self.groups_kick(user, payload).await,
            "groups.promote" => self.groups_promote(user, payload).await,
            "groups.demote" => self.groups_demote(user, payload).await,
            "leaderboards.list" => self.leaderboards_list().await,
            "leaderboards.records" => self.leaderboards_records(payload).await,
            "leaderboards.submit" => self.leaderboards_submit(user, payload).await,
            "chat.join" => {
                self.chat_join(registry, sender, user, room_id, payload)
                    .await
            }
            "chat.leave" => self.chat_leave(registry, sender, user, payload).await,
            "chat.typing" => self.chat_typing(registry, sender, user, payload).await,
            "chat.send" => self.chat_send(registry, sender, user, payload).await,
            "chat.history" => self.chat_history(sender, user, payload).await,
            "chat.edit" => self.chat_edit(registry, sender, user, payload).await,
            "chat.delete" => self.chat_delete(registry, sender, user, payload).await,
            "chat.moderate" => self.chat_moderate(registry, sender, user, payload).await,
            "wallet.balances" => self.wallet_balances(user).await,
            "wallet.ledger" => self.wallet_ledger(user, payload).await,
            other => Self::err(&format!("unknown domain method: {other}")),
        };
        if response.0 == RPC_STATUS_OK
            && matches!(
                method,
                "friends.remove"
                    | "friends.block"
                    | "groups.delete"
                    | "groups.leave"
                    | "groups.kick"
            )
        {
            self.cleanup_revoked_chat_subscriptions(registry).await;
        }
        response
    }

    async fn groups_create(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let Some(name) = value.get("name").and_then(serde_json::Value::as_str) else {
            return Self::err("missing string field: name");
        };
        let description = value
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let open = value
            .get("open")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let max_size = match value.get("max_size") {
            Some(value) => match value.as_u64().and_then(|value| u32::try_from(value).ok()) {
                Some(value) => value,
                None => return Self::err("max_size must be an unsigned 32-bit integer"),
            },
            None => 0,
        };
        match self
            .groups
            .create_for_player(
                user,
                CreateGroupRequest {
                    name: name.to_owned(),
                    description: description.to_owned(),
                    open,
                    max_size,
                    creator_user_id: String::new(),
                    now: SystemClock.now(),
                },
            )
            .await
        {
            Ok(group) => Self::ok(group_json(&group)),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn groups_list(&self, payload: &[u8]) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let limit = match Self::page_usize(&value, "limit", 50, 200) {
            Ok(value) => value,
            Err(message) => return Self::err(&message),
        };
        let offset = match Self::page_usize(&value, "offset", 0, usize::MAX) {
            Ok(value) => value,
            Err(message) => return Self::err(&message),
        };
        let name_contains = match value.get("name_contains") {
            Some(value) => match value.as_str() {
                Some(value) => Some(value.to_owned()),
                None => return Self::err("name_contains must be a string"),
            },
            None => None,
        };
        match self
            .groups
            .list(&GroupFilter {
                name_contains,
                limit,
                offset,
            })
            .await
        {
            Ok(page) => Self::ok(
                serde_json::json!({"items": page.items.iter().map(group_summary_json).collect::<Vec<_>>(), "total": page.total}),
            ),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn groups_get(&self, payload: &[u8]) -> (u8, Vec<u8>) {
        let id = match Self::group_id_arg(payload) {
            Ok(id) => id,
            Err(response) => return response,
        };
        match self.groups.get(id).await {
            Ok(group) => Self::ok(group_json(&group)),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn groups_update(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let (id, request) = match Self::group_update_arg(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        match self.groups.update_as_player(user, id, request).await {
            Ok(group) => Self::ok(group_json(&group)),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn groups_delete(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let id = match Self::group_id_arg(payload) {
            Ok(id) => id,
            Err(response) => return response,
        };
        match self.groups.delete_as_player(user, id).await {
            Ok(()) => Self::ok(serde_json::json!({})),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn groups_add_member(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let (id, target) = match Self::group_target_arg(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        match self
            .groups
            .add_member_as_player(user, id, target, SystemClock.now())
            .await
        {
            Ok(group) => Self::ok(group_json(&group)),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn groups_leave(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let id = match Self::group_id_arg(payload) {
            Ok(id) => id,
            Err(response) => return response,
        };
        match self.groups.leave_as_player(user, id).await {
            Ok(group) => Self::ok(group_json(&group)),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn groups_kick(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let (id, target) = match Self::group_target_arg(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        match self.groups.kick_member_as_player(user, id, &target).await {
            Ok(group) => Self::ok(group_json(&group)),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn groups_promote(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let (id, target) = match Self::group_target_arg(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        match self.groups.promote_as_player(user, id, &target).await {
            Ok(group) => Self::ok(group_json(&group)),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn groups_demote(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let (id, target) = match Self::group_target_arg(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        match self.groups.demote_as_player(user, id, &target).await {
            Ok(group) => Self::ok(group_json(&group)),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn leaderboards_list(&self) -> (u8, Vec<u8>) {
        match self.leaderboards.list().await {
            Ok(items) => Self::ok(serde_json::json!({"items": items})),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn leaderboards_records(&self, payload: &[u8]) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let Some(board) = value.get("board_id").and_then(serde_json::Value::as_str) else {
            return Self::err("missing string field: board_id");
        };
        let limit = match Self::page_usize(&value, "limit", 50, 200) {
            Ok(value) => value,
            Err(message) => return Self::err(&message),
        };
        let offset = match Self::page_usize(&value, "offset", 0, usize::MAX) {
            Ok(value) => value,
            Err(message) => return Self::err(&message),
        };
        match self.leaderboards.records(board, limit, offset).await {
            Ok(page) => Self::ok(serde_json::json!({"items": page.items, "total": page.total})),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn leaderboards_submit(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let Some(board) = value.get("board_id").and_then(serde_json::Value::as_str) else {
            return Self::err("missing string field: board_id");
        };
        let Some(score) = value.get("score").and_then(serde_json::Value::as_i64) else {
            return Self::err("missing signed integer field: score");
        };
        let subscore = value
            .get("subscore")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let metadata = value.get("metadata").cloned();
        match self
            .leaderboards
            .submit(board, user, score, subscore, metadata, SystemClock.now())
            .await
        {
            Ok(record) => Self::ok(serde_json::json!({"record": record})),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn chat_join(
        &self,
        registry: &SessionRegistry,
        sender: ParticipantId,
        user: &str,
        room_id: Option<RoomId>,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let target = match Self::chat_target(&value, room_id) {
            Ok(target) => target,
            Err(message) => return Self::err(&message),
        };
        if let Err(error) = self
            .chat
            .consume_rate_limits(&self.chat_rate_limits.join(user), SystemClock.now())
            .await
        {
            return Self::err(&error.to_string());
        }
        let lease = match self
            .chat_authorizer
            .authorize_fenced(user, target.clone())
            .await
        {
            Ok(lease) => lease,
            Err(error) => return Self::err(&error.to_string()),
        };
        let channel = match self
            .chat
            .resolve_canonical_channel(
                &lease.channel.canonical_key,
                lease.channel.channel_type,
                SystemClock.now(),
            )
            .await
        {
            Ok(channel) => channel,
            Err(error) => return Self::err(&error.to_string()),
        };
        let watermark_event_id = match self.chat_watermark(&channel, &lease).await {
            Ok(watermark) => watermark,
            Err(error) => return Self::err(&error),
        };
        let join = self
            .chat_presence
            .join(&channel.id, sender, user, target, lease.access_epoch);
        if join.inserted {
            if join.existing.is_empty()
                && let Some(announcer) = &self.chat_cluster_presence
            {
                announcer.advertise(&channel.id, SystemClock.now());
            }
            let event = serde_json::json!({
                "version": 1,
                "type": "presence.join",
                "channel_id": channel.id,
                "channel_type": channel.channel_type.as_str(),
                "presence": {"presence_id": join.subscription.presence_id, "user_id": user},
            });
            self.send_chat_event(
                registry,
                &channel.id,
                &join.existing,
                event,
                watermark_event_id,
            );
        }
        Self::ok(serde_json::json!({
            "channel_id": channel.id,
            "channel_type": channel.channel_type.as_str(),
            "presence": join.existing.iter().map(|entry| serde_json::json!({
                "presence_id": entry.presence_id,
                "user_id": entry.user_id,
            })).collect::<Vec<_>>(),
            "watermark_event_id": watermark_event_id,
            "subscription": join.subscription.id,
        }))
    }

    async fn chat_leave(
        &self,
        registry: &SessionRegistry,
        sender: ParticipantId,
        user: &str,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let channel_id = match Self::chat_channel_id(&value) {
            Ok(channel_id) => channel_id,
            Err(message) => return Self::err(&message),
        };
        let Some(subscription) = self.chat_presence.subscription(&channel_id, sender) else {
            return Self::ok(serde_json::json!({"left": false}));
        };
        if subscription.user_id != user {
            return Self::err("CHAT_NOT_SUBSCRIBED");
        }
        self.announce_leave(registry, &channel_id, sender, 0);
        Self::ok(serde_json::json!({"left": true}))
    }

    async fn chat_send(
        &self,
        _registry: &SessionRegistry,
        sender: ParticipantId,
        user: &str,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let channel_id = match Self::chat_channel_id(&value) {
            Ok(channel_id) => channel_id,
            Err(message) => return Self::err(&message),
        };
        let Some(content) = value.get("content").and_then(serde_json::Value::as_str) else {
            return Self::err("missing string field: content");
        };
        if let Err(error) = validate_chat_content(content) {
            return Self::err(&error.to_string());
        }
        let (channel, lease) = match self.authorize_subscription(sender, user, &channel_id).await {
            Ok(value) => value,
            Err(error) => return Self::err(&error),
        };
        let now = SystemClock.now();
        if let Err(error) = self
            .chat
            .consume_rate_limits(&self.chat_rate_limits.send(user, &channel.id), now)
            .await
        {
            return Self::err(&error.to_string());
        }
        let delivery = match self.chat_delivery_request(lease.access_epoch, "message.create", now) {
            Ok(delivery) => delivery,
            Err(error) => return Self::err(&error),
        };
        let message = match self
            .chat
            .append_authorized_with_delivery(
                &channel.id,
                channel.channel_type,
                user,
                content,
                &lease.channel.access_key,
                lease.access_epoch,
                &delivery,
                now,
            )
            .await
        {
            Ok(message) => message,
            Err(error) => return Self::chat_mutation_failure(&error),
        };
        Self::ok(ChatCreateResponse::from(&message))
    }

    /// Broadcast a server-authorized, non-durable typing indication to the
    /// other local subscribers in the channel. The receiver must clear a true
    /// indication at `expires_at`; a false indication expires immediately.
    async fn chat_typing(
        &self,
        registry: &SessionRegistry,
        sender: ParticipantId,
        user: &str,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let channel_id = match Self::chat_channel_id(&value) {
            Ok(channel_id) => channel_id,
            Err(message) => return Self::err(&message),
        };
        let Some(typing) = value.get("typing").and_then(serde_json::Value::as_bool) else {
            return Self::err("missing boolean field: typing");
        };
        let (channel, lease) = match self.authorize_subscription(sender, user, &channel_id).await {
            Ok(value) => value,
            Err(error) => return Self::err(&error),
        };
        let now = SystemClock.now();
        if let Err(error) = self
            .chat
            .consume_rate_limits(&self.chat_rate_limits.typing(user, &channel.id), now)
            .await
        {
            return Self::err(&error.to_string());
        }
        let expires_at = if typing {
            now.unix_millis().saturating_add(CHAT_TYPING_TTL_MS)
        } else {
            now.unix_millis()
        };
        let Some(subscription) = self.chat_presence.subscription(&channel.id, sender) else {
            return Self::err("CHAT_NOT_SUBSCRIBED");
        };
        let event = serde_json::json!({
            "version": 1,
            "type": "typing",
            "channel_id": channel.id,
            "channel_type": channel.channel_type.as_str(),
            "presence": {"presence_id": subscription.presence_id, "user_id": user},
            "typing": typing,
            "expires_at": expires_at,
        });
        let Ok(recipients) = self
            .chat_presence
            .subscribers_at_authority_epoch(&channel.id, lease.access_epoch)
        else {
            return Self::err("CHAT_UNAVAILABLE");
        };
        let recipients = recipients
            .into_iter()
            .filter(|entry| entry.participant != sender)
            .collect::<Vec<_>>();
        self.send_ephemeral_chat_event(registry, &recipients, event);
        Self::ok(serde_json::json!({"typing": typing, "expires_at": expires_at}))
    }

    async fn chat_history(
        &self,
        sender: ParticipantId,
        user: &str,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let channel_id = match Self::chat_channel_id(&value) {
            Ok(channel_id) => channel_id,
            Err(message) => return Self::err(&message),
        };
        let (channel, lease) = match self.authorize_subscription(sender, user, &channel_id).await {
            Ok(value) => value,
            Err(error) => return Self::err(&error),
        };
        let limit = match Self::page_usize(&value, "limit", 50, 200) {
            Ok(limit) => limit,
            Err(message) => return Self::err(&message),
        };
        let before_id = match value.get("before_message_id") {
            Some(value) => match value.as_u64() {
                Some(value) => Some(value),
                None => return Self::err("before_message_id must be an unsigned integer"),
            },
            None => None,
        };
        if let Err(error) = self
            .chat
            .consume_rate_limits(&self.chat_rate_limits.history(user), SystemClock.now())
            .await
        {
            return Self::err(&error.to_string());
        }
        let watermark_event_id = match self.chat_watermark(&channel, &lease).await {
            Ok(watermark) => watermark,
            Err(error) => return Self::err(&error),
        };
        match self
            .chat
            .authorized_messages(
                &channel.id,
                limit,
                before_id,
                &lease.channel.access_key,
                lease.access_epoch,
            )
            .await
        {
            Ok(items) => {
                if value
                    .get("acknowledge_watermark")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|ack| ack >= watermark_event_id)
                {
                    self.chat_presence.clear_needs_resync(&channel.id, sender);
                }
                Self::ok(serde_json::json!({
                    "items": items,
                    "watermark_event_id": watermark_event_id,
                }))
            }
            Err(error) => Self::err(&error.to_string()),
        }
    }

    async fn chat_edit(
        &self,
        _registry: &SessionRegistry,
        sender: ParticipantId,
        user: &str,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let channel_id = match Self::chat_channel_id(&value) {
            Ok(channel_id) => channel_id,
            Err(message) => return Self::err(&message),
        };
        let Some(id) = value.get("message_id").and_then(serde_json::Value::as_u64) else {
            return Self::err("missing unsigned integer field: message_id");
        };
        let Some(content) = value.get("content").and_then(serde_json::Value::as_str) else {
            return Self::err("missing string field: content");
        };
        let (channel, lease) = match self.authorize_subscription(sender, user, &channel_id).await {
            Ok(value) => value,
            Err(error) => return Self::err(&error),
        };
        if let Err(error) = self
            .chat
            .consume_rate_limits(
                &self.chat_rate_limits.mutation(user, &channel.id),
                SystemClock.now(),
            )
            .await
        {
            return Self::err(&error.to_string());
        }
        let now = SystemClock.now();
        let delivery = match self.chat_delivery_request(lease.access_epoch, "message.update", now) {
            Ok(delivery) => delivery,
            Err(error) => return Self::err(&error),
        };
        match self
            .chat
            .edit_as_author_with_delivery(
                &channel.id,
                channel.channel_type,
                id,
                user,
                content,
                &lease.channel.access_key,
                lease.access_epoch,
                crate::services::DEFAULT_AUTHOR_EDIT_WINDOW_MS,
                &delivery,
                now,
            )
            .await
        {
            Ok(message) => Self::ok(ChatEditResponse::from(&message)),
            Err(error) => Self::chat_mutation_failure(&error),
        }
    }

    async fn chat_delete(
        &self,
        _registry: &SessionRegistry,
        sender: ParticipantId,
        user: &str,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let channel_id = match Self::chat_channel_id(&value) {
            Ok(channel_id) => channel_id,
            Err(message) => return Self::err(&message),
        };
        let Some(id) = value.get("message_id").and_then(serde_json::Value::as_u64) else {
            return Self::err("missing unsigned integer field: message_id");
        };
        let (channel, lease) = match self.authorize_subscription(sender, user, &channel_id).await {
            Ok(value) => value,
            Err(error) => return Self::err(&error),
        };
        if let Err(error) = self
            .chat
            .consume_rate_limits(
                &self.chat_rate_limits.mutation(user, &channel.id),
                SystemClock.now(),
            )
            .await
        {
            return Self::err(&error.to_string());
        }
        let now = SystemClock.now();
        let delivery = match self.chat_delivery_request(lease.access_epoch, "message.remove", now) {
            Ok(delivery) => delivery,
            Err(error) => return Self::err(&error),
        };
        match self
            .chat
            .delete_as_author_with_delivery(
                &channel.id,
                channel.channel_type,
                id,
                user,
                &lease.channel.access_key,
                lease.access_epoch,
                crate::services::DEFAULT_AUTHOR_DELETE_WINDOW_MS,
                &delivery,
                now,
            )
            .await
        {
            Ok(message) => {
                let Some(message) = message else {
                    return Self::ok(ChatDeleteResponse::not_deleted(id));
                };
                Self::ok(ChatDeleteResponse::deleted(id, message.last_event_id))
            }
            Err(error) => Self::chat_mutation_failure(&error),
        }
    }

    async fn chat_moderate(
        &self,
        _registry: &SessionRegistry,
        sender: ParticipantId,
        user: &str,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let channel_id = match Self::chat_channel_id(&value) {
            Ok(channel_id) => channel_id,
            Err(message) => return Self::err(&message),
        };
        let Some(id) = value.get("message_id").and_then(serde_json::Value::as_u64) else {
            return Self::err("missing unsigned integer field: message_id");
        };
        let Some(subscription) = self.chat_presence.subscription(&channel_id, sender) else {
            return Self::err("CHAT_NOT_SUBSCRIBED");
        };
        let ChatTarget::Group { group_id } = subscription.target else {
            return Self::err("CHAT_UNAVAILABLE");
        };
        let (channel, lease) = match self.authorize_subscription(sender, user, &channel_id).await {
            Ok(value) => value,
            Err(error) => return Self::err(&error),
        };
        let message = match self
            .chat
            .authorized_messages(
                &channel.id,
                0,
                None,
                &lease.channel.access_key,
                lease.access_epoch,
            )
            .await
            .ok()
            .and_then(|items| items.into_iter().find(|message| message.id == id))
        {
            Some(message) => message,
            None => return Self::err("CHAT_UNAVAILABLE"),
        };
        if let Err(error) = self
            .groups
            .authorize_chat_moderation(user, group_id, &message.sender)
            .await
        {
            return Self::err(&error.to_string());
        }
        let now = SystemClock.now();
        let delivery = match self.chat_delivery_request(lease.access_epoch, "message.remove", now) {
            Ok(delivery) => delivery,
            Err(error) => return Self::err(&error),
        };
        match self
            .chat
            .moderate_delete_message_authorized_with_delivery(
                &channel.id,
                channel.channel_type,
                id,
                "group_admin",
                user,
                "group_moderation",
                &lease.channel.access_key,
                lease.access_epoch,
                "",
                &self.node_id,
                &delivery,
                now,
            )
            .await
        {
            Ok(Some(message)) => Self::ok(ChatModerateResponse::deleted(id, message.last_event_id)),
            Ok(None) => Self::ok(ChatModerateResponse::not_deleted(id)),
            Err(error) => Self::chat_mutation_failure(&error),
        }
    }

    async fn authorize_subscription(
        &self,
        sender: ParticipantId,
        user: &str,
        channel_id: &str,
    ) -> Result<
        (
            crate::repository::ChatChannel,
            crate::services::AuthorizedChatLease,
        ),
        String,
    > {
        let subscription = self
            .chat_presence
            .subscription(channel_id, sender)
            .filter(|subscription| subscription.user_id == user)
            .ok_or_else(|| "CHAT_NOT_SUBSCRIBED".to_owned())?;
        let lease = self
            .chat_authorizer
            .authorize_fenced(user, subscription.target)
            .await
            .map_err(|_| "CHAT_UNAVAILABLE".to_owned())?;
        let channel = self
            .chat
            .resolve_canonical_channel(
                &lease.channel.canonical_key,
                lease.channel.channel_type,
                SystemClock.now(),
            )
            .await
            .map_err(|_| "CHAT_UNAVAILABLE".to_owned())?;
        if channel.id != channel_id {
            return Err("CHAT_UNAVAILABLE".to_owned());
        }
        Ok((channel, lease))
    }

    /// Remove local delivery state immediately after an authority-changing RPC.
    /// The durable services remain the source of truth; this registry sweep is
    /// deliberately local and is repeated by every action that changes direct,
    /// group, or room access.  will add the equivalent cross-node
    /// revocation router.
    async fn cleanup_revoked_chat_subscriptions(&self, registry: &SessionRegistry) {
        for subscription in self.chat_presence.all_subscriptions() {
            if self
                .chat_authorizer
                .authorize_fenced(&subscription.user_id, subscription.target.clone())
                .await
                .is_ok()
            {
                continue;
            }
            let Some(leave) = self
                .chat_presence
                .leave(&subscription.channel_id, subscription.participant)
            else {
                continue;
            };
            let revoked = serde_json::json!({
                "version": 1,
                "type": "access.revoked",
                "channel_id": &leave.channel_id,
                "presence": {
                    "presence_id": &leave.subscription.presence_id,
                    "user_id": &leave.subscription.user_id,
                },
            });
            let outbound = Outbound::reliable(Envelope::new(KIND_CHAT_EVENT, revoked.to_string()));
            let _ = registry.send_to(leave.subscription.participant, &outbound);
            let presence_leave = serde_json::json!({
                "version": 1,
                "type": "presence.leave",
                "channel_id": &leave.channel_id,
                "presence": {
                    "presence_id": &leave.subscription.presence_id,
                    "user_id": &leave.subscription.user_id,
                },
            });
            self.send_chat_event(
                registry,
                &leave.channel_id,
                &leave.remaining,
                presence_leave,
                0,
            );
        }
    }

    fn chat_channel_id(value: &serde_json::Value) -> Result<String, String> {
        if value.get("target").is_some()
            || value.get("channel").is_some()
            || value.get("channel_type").is_some()
        {
            return Err("CHAT_PROTOCOL_UPGRADE_REQUIRED".to_owned());
        }
        value
            .get("channel_id")
            .and_then(serde_json::Value::as_str)
            .filter(|channel_id| !channel_id.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| "missing string field: channel_id".to_owned())
    }

    /// Reads the channel-wide durable event watermark under the caller's
    /// authorization fence. It intentionally does not derive the watermark
    /// from a history page: an edit of an older message can have a newer event
    /// id than the first (newest-by-message-id) history entry.
    async fn chat_watermark(
        &self,
        channel: &crate::repository::ChatChannel,
        lease: &crate::services::AuthorizedChatLease,
    ) -> Result<u64, String> {
        self.chat
            .authorized_messages(
                &channel.id,
                0,
                None,
                &lease.channel.access_key,
                lease.access_epoch,
            )
            .await
            .map(|items| {
                items
                    .iter()
                    .map(|message| message.last_event_id)
                    .max()
                    .unwrap_or(0)
            })
            .map_err(|_| "CHAT_UNAVAILABLE".to_owned())
    }

    /// Bound durable remote retries independently of a socket's lifetime.
    fn chat_delivery_request(
        &self,
        authority_epoch: u64,
        event_type: &'static str,
        now: TimestampMillis,
    ) -> Result<crate::services::ChatDeliveryRequest, String> {
        const RETRY_WINDOW_MS: u64 = 30_000;
        let expires_at = now
            .checked_add(DurationMillis::from_millis(RETRY_WINDOW_MS))
            .map_err(|_| "CHAT_UNAVAILABLE".to_owned())?;
        Ok(crate::services::ChatDeliveryRequest {
            origin_node_id: self.node_id.clone(),
            authority_epoch,
            expires_at,
            event_type,
        })
    }

    fn send_chat_event(
        &self,
        registry: &SessionRegistry,
        channel_id: &str,
        subscriptions: &[ChatSubscription],
        event: serde_json::Value,
        watermark_event_id: u64,
    ) {
        for subscription in subscriptions {
            let body = if subscription.needs_resync {
                serde_json::json!({
                    "version": 1,
                    "type": "resync_required",
                    "channel_id": channel_id,
                    "watermark_event_id": watermark_event_id,
                    "scopes": ["history", "presence"],
                })
            } else {
                event.clone()
            };
            let outbound = Outbound::reliable(Envelope::new(KIND_CHAT_EVENT, body.to_string()));
            if !registry.send_to(subscription.participant, &outbound) {
                self.chat_presence
                    .mark_needs_resync(channel_id, subscription.participant);
            }
        }
    }

    /// Send an ephemeral chat event. A full outbound queue may drop it without
    /// setting `needs_resync`: typing is neither persisted nor recoverable.
    fn send_ephemeral_chat_event(
        &self,
        registry: &SessionRegistry,
        subscriptions: &[ChatSubscription],
        event: serde_json::Value,
    ) {
        let outbound = Outbound::reliable(Envelope::new(KIND_CHAT_EVENT, event.to_string()));
        for subscription in subscriptions {
            let _ = registry.send_to(subscription.participant, &outbound);
        }
    }

    fn announce_leave(
        &self,
        registry: &SessionRegistry,
        channel_id: &str,
        participant: ParticipantId,
        watermark_event_id: u64,
    ) {
        let Some(leave) = self.chat_presence.leave(channel_id, participant) else {
            return;
        };
        if leave.remaining.is_empty()
            && let Some(announcer) = &self.chat_cluster_presence
        {
            announcer.withdraw(channel_id);
        }
        let event = serde_json::json!({
            "version": 1,
            "type": "presence.leave",
            "channel_id": channel_id,
            "presence": {"presence_id": leave.subscription.presence_id, "user_id": leave.subscription.user_id},
        });
        self.send_chat_event(
            registry,
            channel_id,
            &leave.remaining,
            event,
            watermark_event_id,
        );
    }

    #[allow(dead_code)]
    async fn legacy_chat_send(
        &self,
        user: &str,
        room_id: Option<RoomId>,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let Some(content) = value.get("content").and_then(serde_json::Value::as_str) else {
            return Self::err("missing string field: content");
        };
        if let Err(error) = validate_chat_content(content) {
            return Self::err(&error.to_string());
        }
        let target = match Self::chat_target(&value, room_id) {
            Ok(target) => target,
            Err(message) => return Self::err(&message),
        };
        if let Err(err) = self
            .chat
            .consume_rate_limits(&self.chat_rate_limits.join(user), SystemClock.now())
            .await
        {
            return Self::err(&err.to_string());
        }
        let lease = match self.chat_authorizer.authorize_fenced(user, target).await {
            Ok(lease) => lease,
            Err(err) => return Self::err(&err.to_string()),
        };
        let channel = match self
            .chat
            .resolve_canonical_channel(
                &lease.channel.canonical_key,
                lease.channel.channel_type,
                SystemClock.now(),
            )
            .await
        {
            Ok(channel) => channel,
            Err(err) => return Self::err(&err.to_string()),
        };
        let now = SystemClock.now();
        if let Err(err) = self
            .chat
            .consume_rate_limits(&self.chat_rate_limits.send(user, &channel.id), now)
            .await
        {
            return Self::err(&err.to_string());
        }
        match self
            .chat
            .append_authorized(
                &channel.id,
                channel.channel_type,
                user,
                content,
                &lease.channel.access_key,
                lease.access_epoch,
                now,
            )
            .await
        {
            Ok(id) => Self::ok(serde_json::json!({"channel_id": channel.id, "id": id})),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    #[allow(dead_code)]
    async fn legacy_chat_history(
        &self,
        user: &str,
        room_id: Option<RoomId>,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let target = match Self::chat_target(&value, room_id) {
            Ok(target) => target,
            Err(message) => return Self::err(&message),
        };
        if let Err(err) = self
            .chat
            .consume_rate_limits(&self.chat_rate_limits.join(user), SystemClock.now())
            .await
        {
            return Self::err(&err.to_string());
        }
        let lease = match self.chat_authorizer.authorize_fenced(user, target).await {
            Ok(lease) => lease,
            Err(err) => return Self::err(&err.to_string()),
        };
        let channel = match self
            .chat
            .resolve_canonical_channel(
                &lease.channel.canonical_key,
                lease.channel.channel_type,
                SystemClock.now(),
            )
            .await
        {
            Ok(channel) => channel,
            Err(err) => return Self::err(&err.to_string()),
        };
        let limit = match Self::page_usize(&value, "limit", 50, 200) {
            Ok(value) => value,
            Err(message) => return Self::err(&message),
        };
        let before_id = match value.get("before_id") {
            Some(value) => match value.as_u64() {
                Some(value) => Some(value),
                None => return Self::err("before_id must be an unsigned integer"),
            },
            None => None,
        };
        if let Err(err) = self
            .chat
            .consume_rate_limits(&self.chat_rate_limits.history(user), SystemClock.now())
            .await
        {
            return Self::err(&err.to_string());
        }
        match self
            .chat
            .authorized_messages(
                &channel.id,
                limit,
                before_id,
                &lease.channel.access_key,
                lease.access_epoch,
            )
            .await
        {
            Ok(items) => Self::ok(serde_json::json!({"channel_id": channel.id, "items": items})),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    #[allow(dead_code)]
    async fn legacy_chat_edit(
        &self,
        user: &str,
        room_id: Option<RoomId>,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let Some(id) = value.get("id").and_then(serde_json::Value::as_u64) else {
            return Self::err("missing unsigned integer field: id");
        };
        let Some(content) = value.get("content").and_then(serde_json::Value::as_str) else {
            return Self::err("missing string field: content");
        };
        if let Err(error) = validate_chat_content(content) {
            return Self::err(&error.to_string());
        }
        let (channel, lease) = match self
            .legacy_chat_channel_for_mutation(user, room_id, &value)
            .await
        {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(error) = self
            .chat
            .consume_rate_limits(
                &self.chat_rate_limits.mutation(user, &channel.id),
                SystemClock.now(),
            )
            .await
        {
            return Self::err(&error.to_string());
        }
        match self
            .chat
            .edit_as_author(
                &channel.id,
                id,
                user,
                content,
                &lease.channel.access_key,
                lease.access_epoch,
                crate::services::DEFAULT_AUTHOR_EDIT_WINDOW_MS,
                SystemClock.now(),
            )
            .await
        {
            Ok(message) => {
                Self::ok(serde_json::json!({"channel_id": channel.id, "message": message}))
            }
            Err(error) => Self::err(&error.to_string()),
        }
    }

    #[allow(dead_code)]
    async fn legacy_chat_delete(
        &self,
        user: &str,
        room_id: Option<RoomId>,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let Some(id) = value.get("id").and_then(serde_json::Value::as_u64) else {
            return Self::err("missing unsigned integer field: id");
        };
        let (channel, lease) = match self
            .legacy_chat_channel_for_mutation(user, room_id, &value)
            .await
        {
            Ok(value) => value,
            Err(response) => return response,
        };
        if let Err(error) = self
            .chat
            .consume_rate_limits(
                &self.chat_rate_limits.mutation(user, &channel.id),
                SystemClock.now(),
            )
            .await
        {
            return Self::err(&error.to_string());
        }
        match self
            .chat
            .delete_as_author(
                &channel.id,
                id,
                user,
                &lease.channel.access_key,
                lease.access_epoch,
                crate::services::DEFAULT_AUTHOR_DELETE_WINDOW_MS,
                SystemClock.now(),
            )
            .await
        {
            Ok(deleted) => {
                Self::ok(serde_json::json!({"channel_id": channel.id, "deleted": deleted}))
            }
            Err(error) => Self::err(&error.to_string()),
        }
    }

    #[allow(dead_code)]
    async fn legacy_chat_moderate(
        &self,
        user: &str,
        room_id: Option<RoomId>,
        payload: &[u8],
    ) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let Some(id) = value.get("id").and_then(serde_json::Value::as_u64) else {
            return Self::err("missing unsigned integer field: id");
        };
        let group_id = match Self::chat_target(&value, room_id) {
            Ok(ChatTarget::Group { group_id }) => group_id,
            Ok(ChatTarget::Direct { .. } | ChatTarget::CurrentRoom { .. }) => {
                return Self::err("CHAT_UNAVAILABLE");
            }
            Err(error) => return Self::err(&error),
        };
        if let Err(error) = self
            .chat
            .consume_rate_limits(&self.chat_rate_limits.join(user), SystemClock.now())
            .await
        {
            return Self::err(&error.to_string());
        }
        let lease = match self
            .chat_authorizer
            .authorize_fenced(user, ChatTarget::Group { group_id })
            .await
        {
            Ok(lease) => lease,
            Err(error) => return Self::err(&error.to_string()),
        };
        let channel = match self
            .chat
            .resolve_canonical_channel(
                &lease.channel.canonical_key,
                lease.channel.channel_type,
                SystemClock.now(),
            )
            .await
        {
            Ok(channel) => channel,
            Err(error) => return Self::err(&error.to_string()),
        };
        let message = match self
            .chat
            .authorized_messages(
                &channel.id,
                0,
                None,
                &lease.channel.access_key,
                lease.access_epoch,
            )
            .await
        {
            Ok(messages) => messages.into_iter().find(|message| message.id == id),
            Err(error) => return Self::err(&error.to_string()),
        };
        let Some(message) = message else {
            return Self::err("CHAT_UNAVAILABLE");
        };
        if let Err(error) = self
            .groups
            .authorize_chat_moderation(user, group_id, &message.sender)
            .await
        {
            return Self::err(&error.to_string());
        }
        if let Err(error) = self
            .chat
            .consume_rate_limits(
                &self.chat_rate_limits.moderation(user, &channel.id),
                SystemClock.now(),
            )
            .await
        {
            return Self::err(&error.to_string());
        }
        match self
            .chat
            .moderate_delete_message_authorized(
                &channel.id,
                id,
                "group_admin",
                user,
                "group_moderation",
                &lease.channel.access_key,
                lease.access_epoch,
                "",
                &self.node_id,
                SystemClock.now(),
            )
            .await
        {
            Ok(deleted) => {
                Self::ok(serde_json::json!({"channel_id": channel.id, "deleted": deleted}))
            }
            Err(error) => Self::err(&error.to_string()),
        }
    }

    #[allow(dead_code)]
    async fn legacy_chat_channel_for_mutation(
        &self,
        user: &str,
        room_id: Option<RoomId>,
        value: &serde_json::Value,
    ) -> Result<
        (
            crate::repository::ChatChannel,
            crate::services::AuthorizedChatLease,
        ),
        (u8, Vec<u8>),
    > {
        self.chat
            .consume_rate_limits(&self.chat_rate_limits.join(user), SystemClock.now())
            .await
            .map_err(|error| Self::err(&error.to_string()))?;
        let target = Self::chat_target(value, room_id).map_err(|message| Self::err(&message))?;
        let lease = self
            .chat_authorizer
            .authorize_fenced(user, target)
            .await
            .map_err(|error| Self::err(&error.to_string()))?;
        let channel = self
            .chat
            .resolve_canonical_channel(
                &lease.channel.canonical_key,
                lease.channel.channel_type,
                SystemClock.now(),
            )
            .await
            .map_err(|error| Self::err(&error.to_string()))?;
        Ok((channel, lease))
    }

    fn chat_target(
        value: &serde_json::Value,
        current_room_id: Option<RoomId>,
    ) -> Result<ChatTarget, String> {
        if value.get("channel").is_some() || value.get("channel_type").is_some() {
            return Err("CHAT_PROTOCOL_UPGRADE_REQUIRED".to_owned());
        }
        let target = value
            .get("target")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| "missing object field: target".to_owned())?;
        let kind = target
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "missing string field: target.kind".to_owned())?;
        match kind {
            "direct" => target
                .get("other_user_id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .map(|other_user_id| ChatTarget::Direct {
                    other_user_id: other_user_id.to_owned(),
                })
                .ok_or_else(|| "missing string field: target.other_user_id".to_owned()),
            "group" => target
                .get("group_id")
                .and_then(serde_json::Value::as_u64)
                .map(|group_id| ChatTarget::Group { group_id })
                .ok_or_else(|| "missing unsigned integer field: target.group_id".to_owned()),
            "room" => current_room_id
                .map(|room_id| ChatTarget::CurrentRoom { room_id })
                .ok_or_else(|| "CHAT_UNAVAILABLE".to_owned()),
            _ => Err("unknown target.kind".to_owned()),
        }
    }

    async fn wallet_balances(&self, user: &str) -> (u8, Vec<u8>) {
        match self.wallet.balances(user).await {
            Ok(balances) => Self::ok(serde_json::json!({"balances": balances})),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn wallet_ledger(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let value = match Self::json_object(payload) {
            Ok(value) => value,
            Err(response) => return response,
        };
        let limit = match Self::page_usize(&value, "limit", 50, 200) {
            Ok(value) => value,
            Err(message) => return Self::err(&message),
        };
        match self.wallet.ledger(user, limit).await {
            Ok(items) => Self::ok(serde_json::json!({"items": items})),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn friends_add(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let other = match Self::other_arg(payload) {
            Ok(other) => other,
            Err(response) => return response,
        };
        match self.friends.add(user, &other, SystemClock.now()).await {
            Ok(state) => Self::ok(serde_json::json!({ "state": state })),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn friends_remove(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let other = match Self::other_arg(payload) {
            Ok(other) => other,
            Err(response) => return response,
        };
        match self.friends.remove(user, &other).await {
            Ok(removed) => Self::ok(serde_json::json!({ "removed": removed })),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn friends_block(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let other = match Self::other_arg(payload) {
            Ok(other) => other,
            Err(response) => return response,
        };
        match self.friends.block(user, &other, SystemClock.now()).await {
            Ok(()) => Self::ok(serde_json::json!({})),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn friends_list(&self, user: &str) -> (u8, Vec<u8>) {
        match self.friends.list(user).await {
            Ok(rows) => Self::ok(serde_json::json!({ "friends": rows })),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn notifications_list(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let value: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(value) => value,
            Err(_) => return Self::err("invalid JSON body"),
        };
        let limit = value
            .get("limit")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(50) as usize;
        let cursor = value.get("cursor").and_then(serde_json::Value::as_str);
        match self.player_notifications.list(user, limit, cursor).await {
            Ok(page) => Self::ok(
                serde_json::json!({ "notifications": page.items, "cursor": page.next_cursor }),
            ),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    async fn notifications_mark_read(&self, user: &str, payload: &[u8]) -> (u8, Vec<u8>) {
        let value: serde_json::Value = match serde_json::from_slice(payload) {
            Ok(value) => value,
            Err(_) => return Self::err("invalid JSON body"),
        };
        let Some(ids) = value.get("ids").and_then(serde_json::Value::as_array) else {
            return Self::err("missing array field: ids");
        };
        let ids = ids
            .iter()
            .map(serde_json::Value::as_str)
            .collect::<Option<Vec<_>>>();
        let Some(ids) = ids else {
            return Self::err("ids must contain strings");
        };
        let ids = ids.into_iter().map(ToOwned::to_owned).collect::<Vec<_>>();
        match self
            .player_notifications
            .mark_read(user, &ids, SystemClock.now())
            .await
        {
            Ok(changed) => Self::ok(serde_json::json!({ "read_ids": changed })),
            Err(err) => Self::err(&err.to_string()),
        }
    }

    /// Extract the required `{"other":"<id>"}` argument, or an error response.
    fn other_arg(payload: &[u8]) -> Result<String, (u8, Vec<u8>)> {
        let value: serde_json::Value =
            serde_json::from_slice(payload).map_err(|_| Self::err("invalid JSON body"))?;
        match value.get("other").and_then(serde_json::Value::as_str) {
            Some(other) if !other.is_empty() => Ok(other.to_string()),
            _ => Err(Self::err("missing string field: other")),
        }
    }

    fn json_object(payload: &[u8]) -> Result<serde_json::Value, (u8, Vec<u8>)> {
        match serde_json::from_slice(payload) {
            Ok(value @ serde_json::Value::Object(_)) => Ok(value),
            Ok(_) => Err(Self::err("JSON body must be an object")),
            Err(_) => Err(Self::err("invalid JSON body")),
        }
    }

    fn group_id_arg(payload: &[u8]) -> Result<u64, (u8, Vec<u8>)> {
        let value = Self::json_object(payload)?;
        value
            .get("group_id")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| Self::err("missing unsigned integer field: group_id"))
    }

    fn group_target_arg(payload: &[u8]) -> Result<(u64, String), (u8, Vec<u8>)> {
        let value = Self::json_object(payload)?;
        let id = value
            .get("group_id")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| Self::err("missing unsigned integer field: group_id"))?;
        let target = value
            .get("user_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| Self::err("missing string field: user_id"))?;
        Ok((id, target))
    }

    fn group_update_arg(payload: &[u8]) -> Result<(u64, UpdateGroupRequest), (u8, Vec<u8>)> {
        let value = Self::json_object(payload)?;
        let id = value
            .get("group_id")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| Self::err("missing unsigned integer field: group_id"))?;
        let description = match value.get("description") {
            Some(value) => Some(
                value
                    .as_str()
                    .ok_or_else(|| Self::err("description must be a string"))?
                    .to_owned(),
            ),
            None => None,
        };
        let open = match value.get("open") {
            Some(value) => Some(
                value
                    .as_bool()
                    .ok_or_else(|| Self::err("open must be a boolean"))?,
            ),
            None => None,
        };
        let max_size = match value.get("max_size") {
            Some(value) => Some(
                value
                    .as_u64()
                    .and_then(|value| u32::try_from(value).ok())
                    .ok_or_else(|| Self::err("max_size must be an unsigned 32-bit integer"))?,
            ),
            None => None,
        };
        if description.is_none() && open.is_none() && max_size.is_none() {
            return Err(Self::err("at least one group field must be provided"));
        }
        Ok((
            id,
            UpdateGroupRequest {
                description,
                open,
                max_size,
            },
        ))
    }

    fn page_usize(
        value: &serde_json::Value,
        field: &str,
        default: usize,
        maximum: usize,
    ) -> Result<usize, String> {
        match value.get(field) {
            Some(value) => value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value <= maximum)
                .ok_or_else(|| format!("{field} must be an unsigned integer at most {maximum}")),
            None => Ok(default),
        }
    }

    /// A successful response: `RPC_STATUS_OK` + the JSON body bytes.
    fn ok(body: impl Serialize) -> (u8, Vec<u8>) {
        match serde_json::to_vec(&body) {
            Ok(body) => (protocol::RPC_STATUS_OK, body),
            Err(_) => Self::err("CHAT_UNAVAILABLE"),
        }
    }

    fn chat_mutation_failure(_error: &crate::error::AppError) -> (u8, Vec<u8>) {
        Self::err("CHAT_UNAVAILABLE")
    }

    /// An error response: `RPC_STATUS_ERROR` + a short UTF-8 message.
    fn err(message: &str) -> (u8, Vec<u8>) {
        (protocol::RPC_STATUS_ERROR, message.as_bytes().to_vec())
    }
}

fn group_summary_json(group: &Group) -> serde_json::Value {
    serde_json::json!({
        "id": group.id,
        "name": group.name,
        "description": group.description,
        "open": group.open,
        "max_size": group.max_size,
        "member_count": group.member_count(),
        "created_at_unix_ms": group.created_at.unix_millis(),
    })
}

fn group_json(group: &Group) -> serde_json::Value {
    let mut value = group_summary_json(group);
    value["members"] = serde_json::Value::Array(
        group
            .members()
            .iter()
            .map(|member| {
                serde_json::json!({
                    "user_id": member.user_id,
                    "role": member.role.as_str(),
                    "joined_at_unix_ms": member.joined_at.unix_millis(),
                })
            })
            .collect(),
    );
    value
}

/// The resolved handshake for a connecting client, plus whether the triggering
/// first frame should be replayed as normal inbound traffic (the implicit-guest
/// legacy path). See [`Gateway::resolve_handshake`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    /// The authentication outcome.
    pub outcome: AuthOutcome,
    /// Whether the (non-handshake) first frame that triggered an implicit-guest
    /// acceptance should be dispatched to the gateway after registration, so a
    /// pre-handshake client's first real message is not lost.
    pub replay_first: bool,
}

/// The realtime gateway: shared, transport-agnostic routing state.
///
/// Held as `Arc<Gateway>` by each transport listener. Transports mint sessions,
/// register outbound channels, and forward inbound envelopes here.
///
/// The gateway carries the process-wide [`NodeMetrics`] registry so that the
/// realtime lifecycle it already mediates — connections opening/closing,
/// sessions registering/unregistering, and messages relayed — moves the node
/// dashboard's live gauges. Standalone construction via [`new`]
/// installs a private, throwaway registry; production wires the shared one via
/// [`with_metrics`].
///
/// [`new`]: Gateway::new
/// [`with_metrics`]: Gateway::with_metrics
#[derive(Debug, Clone, Copy)]
struct NpcEntry {
    /// Globally unique transform-world id delivered to room members.
    object_id: u32,
    archetype_id: u16,
    room_id: Option<RoomId>,
}

type NpcKey = (Option<RoomId>, u32);

/// The trusted room identity paired with every room-scoped replication match,
/// object, and connection. Match ids remain an internal RepAuthority index;
/// this table prevents them from becoming a second, drifting room identity.
#[derive(Debug, Default)]
struct RepRoomBindings {
    room_matches: HashMap<RoomId, u64>,
    match_rooms: HashMap<u64, RoomId>,
    objects: HashMap<u32, RoomId>,
    connections: HashMap<ParticipantId, RoomId>,
}

struct RepObjectSpawn {
    object_id: u32,
    match_id: u64,
    room_id: Option<RoomId>,
    class_id: u32,
    owner: Option<ParticipantId>,
    persistent: bool,
    initial: RepSnapshot,
}

/// Result owned by a concrete transport after registration. The cancellation
/// token fires only when a newer connection replaces this exact authenticated
/// session; WebSocket deliberately retains its existing fenced-loop behavior.
#[derive(Debug)]
pub struct TransportRegistration {
    /// Latest-wins outbound mailbox for the registered transport writer.
    pub unreliable: LatestOutboundReceiver,
    /// Signal for QUIC-family transports to close a superseded receive loop.
    pub superseded: CancellationToken,
    /// Shared with datagram routing to make same-session cancellation a decode
    /// and metrics linearization boundary.
    pub supersession_gate: Arc<Mutex<()>>,
    /// Serializes concrete application writes with supersession/close.
    pub transport_write_gate: Arc<tokio::sync::Mutex<()>>,
    /// Closes write admission while an already-admitted frame flushes.
    pub superseding: Arc<std::sync::atomic::AtomicBool>,
    /// Deferred old-generation cleanup, released after its inbound handoff gate
    /// drains. Concrete transports own the task that invokes Gateway cleanup.
    pub replaced_cleanup: Option<ReplacedTransportCleanup>,
    /// Fires after this generation's own inbound supersession gate is released.
    pub inbound_supersession_drained: CancellationToken,
}

pub struct Gateway {
    registry: SessionRegistry,
    ids: ParticipantIdGen,
    metrics: Arc<NodeMetrics>,
    /// Optional bounded recorder for already-validated bridge outcomes.
    /// It retains only generic classifications and opaque numeric correlations.
    authoritative_decision_recorder: Option<Arc<AuthoritativeDecisionRecorder>>,
    /// Optional embedded script runtime. When present, inbound messages are
    /// dispatched to script handlers; when `None`, the built-in relay runs.
    runtime: Option<Arc<dyn Runtime>>,
    /// Resolves the realtime auth handshake (binds token -> account, or guest).
    authenticator: Authenticator,
    /// Optional authoritative transform-sync hub. When present, the
    /// gateway handles `KIND_TSYNC_HELLO`/`KIND_TSYNC_ACK` and fans out per-client
    /// snapshots via [`Gateway::transform_tick`].
    transform: Option<Arc<TransformHub>>,
    /// Optional `NetworkPeer` server authority. When present, the
    /// gateway routes `KIND_REP_DELTA`/`KIND_REP_ACK` through the untrusted-input
    /// validate -> apply -> rebroadcast pipeline.
    rep: Option<Arc<RepAuthority>>,
    /// Trusted binding between replication match ids and RoomRegistry identity.
    /// All room lifecycle paths update this together with the RepAuthority
    /// connection membership before a client can receive room-scoped state.
    rep_rooms: Mutex<RepRoomBindings>,
    /// Serializes every room membership/binding transition with every
    /// room-scoped client enqueue. `RoomRegistry` and `RepRoomBindings` have
    /// independent internal locks, so this is the transaction boundary that
    /// prevents either one from becoming observable without the other.
    room_scope: Mutex<()>,
    /// Serializes protocol `ROOM_CREATE` callbacks with their named room birth.
    /// It is distinct from `room_scope` so a script callback cannot deadlock the
    /// membership/replication transaction while still running exactly once per
    /// named room generation.
    room_creation_gate: Mutex<()>,
    /// Fences runtime dispatch against a reload generation swap. Ingress and
    /// lifecycle hold a shared guard through the runtime callback; reload holds
    /// the exclusive guard through retirement and VM replacement.
    generation_gate: RwLock<()>,
    /// Suppresses script lifecycle dispatch while the exclusive generation
    /// writer retires rooms. Those callbacks must not enter either generation.
    reload_retiring: AtomicBool,
    /// Server-owned room membership (, Phase A). Always present: the
    /// gateway routes `KIND_ROOM_*` through it (create/join/leave/map-ready).
    rooms: RoomRegistry,
    /// Server-owned networked actors (NPCs, ): `(room, script id)` ->
    /// globally unique transform-world identity and metadata.
    /// Populated by the Lua `spawn_actor` command; used to spawn them for a client
    /// that announces presence (so late joiners see existing NPCs).
    npcs: Mutex<HashMap<NpcKey, NpcEntry>>,
    /// Monotonic allocation cursor for room-scoped actor identities. The high
    /// range avoids ordinary player-slot ids while retaining global object-id
    /// uniqueness in the shared transform world.
    next_scoped_actor_id: Mutex<u32>,
    /// Loaded `.map` level geometry. A room's `map` name resolves
    /// against this catalog on create; empty when no maps are cooked/loaded.
    maps: Arc<MapCatalog>,
    /// Persisted domain-feature services reachable from built-in client RPC
    /// methods. When present, reserved `friends.*` (etc.) RPC methods
    /// are answered here; when `None`, they fall through to the script runtime.
    domain: Option<DomainRpcServices>,
    /// Local ticket queue. Formed cohorts receive a short-lived handoff before
    /// trusted admission to a server-owned room.
    matchmaker: Matchmaker,
    /// Authenticated, local party membership used to resolve indivisible party
    /// tickets before they enter the matchmaker.
    parties: PartyRegistry,
    /// Durable, fenced party authority. `None` preserves standalone/local test
    /// compatibility while production startup attaches it before listeners bind.
    durable_parties: Option<DurablePartyGateway>,
    party_presence: Option<PartyPresenceGateway>,
    cluster_matchmaker: Option<ClusterMatchmakerGateway>,
    /// Durable, live multi-node matchmaker path. When configured it owns the
    /// queue through a bounded worker rather than the local in-process index.
    live_matchmaker: Option<Arc<LiveMatchmakerNode>>,
    /// Owner binding and short-lived handoffs for formed tickets. Tokens are
    /// redacted from `Debug` through [`JoinToken`].
    handoffs: Mutex<MatchmakerHandoffs>,
    /// The GameScript readiness gate. Present only when
    /// `runtime.require_script` is enabled; every match surface then fails
    /// closed unless one atomic snapshot is `Ready`, and rooms are born bound
    /// to that snapshot's `(revision, generation)`. `None` preserves ungated
    /// behavior byte for byte.
    script_readiness: Option<Arc<GameScriptReadiness>>,
    /// Whether every room must be authoritative (legacy `require_script`).
    strict_script_rooms: bool,
    /// The authoritative-gameplay bridge. Present only when the deployment runs
    /// authoritative matches (attached alongside the readiness gate). While
    /// present, a match participant's protected gameplay frames route through
    /// the per-match pending-batch ledger + validator instead of the direct
    /// executors. `None` preserves the non-authoritative relay path byte for
    /// byte, so every existing (bridge-less) deployment and test is unchanged.
    bridge: Option<Arc<GatewayBridge>>,
    /// Trusted native lag-diagnostics lifecycle and post-auth capability state.
    /// It is intentionally not exposed to the embedded GameScript runtime.
    diagnostics: LagCaptureManager,
    /// Optional durable match-record emitter. Attached only when the node has a
    /// durable log store; `None` keeps every lifecycle path byte for byte and
    /// leaves the room registry as the only match history.
    ///
    /// The gateway is the sole writer: a match row is opened when the server
    /// creates the room and closed with the server's own end timestamp and
    /// termination reason. No script-facing surface reaches it.
    matches: Option<Arc<MatchRecorder>>,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("registry", &self.registry)
            .field("ids", &self.ids)
            .field("metrics", &self.metrics)
            .field(
                "authoritative_decision_recorder",
                &self.authoritative_decision_recorder.is_some(),
            )
            .field("runtime_attached", &self.runtime.is_some())
            .field("authenticator", &self.authenticator)
            .field("transform", &self.transform)
            .field("rep", &self.rep)
            .field("rep_rooms", &self.rep_rooms)
            .field("room_scope", &"locked transaction gate")
            .field("rooms", &self.rooms)
            .field("npcs", &self.npcs)
            .field("maps", &self.maps)
            .field("domain_attached", &self.domain.is_some())
            .field("matchmaker", &self.matchmaker)
            .field("parties", &"[redacted]")
            .field("durable_parties", &self.durable_parties.is_some())
            .field("live_matchmaker", &self.live_matchmaker.is_some())
            .field("handoffs", &"[redacted]")
            .field("script_readiness", &self.script_readiness.is_some())
            .field("bridge", &self.bridge.is_some())
            .field("diagnostics", &self.diagnostics)
            .field("match_recorder", &self.matches.is_some())
            .finish()
    }
}

impl Gateway {
    /// Create an empty gateway with a single implicit global room.
    ///
    /// The gauge registry is private to this gateway; use [`with_metrics`] to
    /// share the node's [`NodeMetrics`] so the dashboard reflects live traffic.
    ///
    /// [`with_metrics`]: Gateway::with_metrics
    #[must_use]
    pub fn new() -> Self {
        Self::with_metrics(Arc::new(NodeMetrics::new()))
    }

    /// Create a gateway that reports its lifecycle to the shared `metrics`.
    ///
    /// No script runtime is attached; inbound messages use the built-in relay.
    #[must_use]
    pub fn with_metrics(metrics: Arc<NodeMetrics>) -> Self {
        Self::with_metrics_and_runtime(metrics, None)
    }

    /// Create a gateway with shared `metrics` and an optional script `runtime`.
    ///
    /// When `runtime` is `Some`, inbound envelopes are dispatched to the script's
    /// registered handlers and the resulting [`OutboundCommand`]s are applied to
    /// the session registry. When `None`, the built-in position relay runs.
    #[must_use]
    pub fn with_metrics_and_runtime(
        metrics: Arc<NodeMetrics>,
        runtime: Option<Arc<dyn Runtime>>,
    ) -> Self {
        Self::with_metrics_runtime_auth(metrics, runtime, Authenticator::guest_only())
    }

    /// Configure the bounded in-process cluster seams used by the distributed
    /// matchmaker. Production node transport implements the same router contract;
    /// leaving this unset preserves single-node behavior.
    #[must_use]
    pub fn with_matchmaker_cluster(
        mut self,
        node_id: NodeId,
        lease: MatchmakerShardLease,
        authority: Arc<InMemoryMatchmakerCluster>,
        router: Arc<InMemoryMatchmakerHandoffRouter>,
    ) -> Self {
        self.cluster_matchmaker = Some(ClusterMatchmakerGateway {
            node_id,
            lease,
            authority,
            router,
        });
        self
    }

    /// Register this shared gateway as the match-owner admission endpoint.
    /// Call after wrapping the configured gateway in [`Arc`].
    pub fn register_matchmaker_cluster_endpoint(self: &Arc<Self>) {
        let Some(cluster) = &self.cluster_matchmaker else {
            return;
        };
        let node_id = cluster.node_id.clone();
        let weak = Arc::downgrade(self);
        cluster.router.register_admission_handler(
            node_id,
            Arc::new(move |request| {
                let gateway = weak.upgrade().ok_or_else(|| {
                    MatchmakerRouterError::UnknownDestination(request.requester_node.clone())
                })?;
                gateway.accept_remote_matchmaker_admission(request)
            }),
        );
    }

    /// Attach the durable multi-node matchmaker worker. Call
    /// [`Self::register_live_matchmaker_endpoint`] after wrapping the gateway
    /// in an [`Arc`] so control-plane callbacks can notify local sessions.
    #[must_use]
    pub fn with_live_matchmaker(mut self, matchmaker: Arc<LiveMatchmakerNode>) -> Self {
        self.live_matchmaker = Some(matchmaker);
        self
    }

    /// Bind the shared gateway to its live matchmaker worker. This preserves the
    /// strict split between session-node socket ownership and worker/control
    /// plane coordination.
    pub fn register_live_matchmaker_endpoint(self: &Arc<Self>) {
        if let Some(matchmaker) = &self.live_matchmaker {
            matchmaker.attach_gateway(Arc::downgrade(self));
        }
    }

    /// Send one asynchronous live-matchmaker RPC result to a local socket.
    /// The worker never owns that socket; it only asks this session node to
    /// write the already-correlated response.
    pub(crate) fn live_matchmaker_reply(
        &self,
        sender: ParticipantId,
        request_id: u64,
        ok: bool,
        body: String,
    ) {
        let status = if ok {
            protocol::RPC_STATUS_OK
        } else {
            protocol::RPC_STATUS_ERROR
        };
        let _ = self.reply_rpc(sender, request_id, status, body.as_bytes());
    }

    /// Allocate an empty, closed room for a durably formed live cohort.
    ///
    /// Fails closed (no room is born) when the GameScript readiness gate is
    /// attached and not `Ready`; a created room is bound to the gating
    /// snapshot's `(revision, generation)`.
    pub(crate) fn live_matchmaker_create_room(&self, participants: usize) -> Result<RoomId, ()> {
        if self.require_native_match_lifecycle().is_err() {
            tracing::error!(
                reason = NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE,
                "refused live matchmaker room creation before native lifecycle match existed"
            );
            return Err(());
        }
        let room_id = {
            let _scope = self.lock_room_scope();
            let binding = self.script_gate(ScriptGateSurface::LiveForm)?;
            self.rooms.create_bound(
                RoomLabel {
                    map: "default".to_owned(),
                    mode: "matchmaker".to_owned(),
                    max_players: u16::try_from(participants).unwrap_or(u16::MAX),
                    open: false,
                },
                binding,
            )
        };
        self.dispatch_match_created(room_id);
        Ok(room_id)
    }

    /// Persisted live handoffs are notified only through the recipient's local
    /// session node. A disconnected user keeps the handoff in the worker and
    /// can recover it through `matchmaker.status` after reconnecting.
    pub(crate) fn live_matchmaker_notify(&self, handoff: &RemoteMatchmakerHandoff) {
        let Some(participant) = self.registry.participant_for_user(&handoff.user_id) else {
            return;
        };
        let body = serde_json::json!({
            "ticket_id": handoff.ticket_id.as_str(),
            "match_id": handoff.match_id,
            "join_token": handoff.join_token,
            "expires_at": handoff.expires_at.unix_millis(),
        })
        .to_string()
        .into_bytes();
        let _ = self.send_reliable(participant, KIND_MATCHMAKER_MATCHED, body);
    }

    /// Complete trusted admission into a match owned by this same node.
    ///
    /// Fails closed under the readiness gate: a non-Ready snapshot (or a room
    /// bound to a superseded load) refuses the admission and replies the one
    /// stable client-safe message. The live worker's generic failure reply
    /// for the same request id is then redundant and ignored by correlation.
    pub(crate) fn live_matchmaker_finish_local_accept(
        &self,
        sender: ParticipantId,
        request_id: u64,
        room_id: RoomId,
    ) -> Result<(), ()> {
        if self.require_native_match_lifecycle().is_err() {
            let _ = self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE.as_bytes(),
            );
            return Err(());
        }
        let Ok(binding) = self.script_gate(ScriptGateSurface::LiveAcceptLocal) else {
            let _ = self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                SCRIPT_UNAVAILABLE_MESSAGE.as_bytes(),
            );
            return Err(());
        };
        let (admission, previous) = {
            let _scope = self.lock_room_scope();
            let previous = self
                .rooms
                .room_of(sender)
                .and_then(|id| self.room_snapshot_for_lifecycle(id));
            let admission = self
                .rooms
                .admit_match_bound(sender, room_id, binding.as_ref());
            if admission.is_ok() {
                self.bind_rep_connection_to_room_under_scope(sender, room_id);
            }
            (admission, previous)
        };
        let label = match admission {
            Ok(label) => label,
            Err(JoinError::StaleScript) => {
                let _ = self.reply_rpc(
                    sender,
                    request_id,
                    protocol::RPC_STATUS_ERROR,
                    SCRIPT_UNAVAILABLE_MESSAGE.as_bytes(),
                );
                return Err(());
            }
            Err(_) => return Err(()),
        };
        self.dispatch_local_match_admission(sender, previous, room_id, false);
        let body = serde_json::json!({ "accepted": true, "match_id": room_id }).to_string();
        let _ = self.reply_rpc(sender, request_id, protocol::RPC_STATUS_OK, body.as_bytes());
        let _ = self.reply_joined_with_rep_bootstrap(sender, room_id, label);
        Ok(())
    }

    /// Complete a trusted admission that was validated on a remote match node.
    ///
    /// Script-bound remote matches remain fail-closed until an authenticated
    /// owner-to-session state/intent data plane exists. Relay matches keep the
    /// established distributed-matchmaker admission behavior.
    pub(crate) fn live_matchmaker_finish_remote_accept(
        &self,
        sender: ParticipantId,
        request_id: u64,
        room_id: RoomId,
    ) -> Result<(), ()> {
        if self.require_native_match_lifecycle().is_err() {
            let _ = self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE.as_bytes(),
            );
            return Err(());
        }
        let binding = match self.script_gate(ScriptGateSurface::LiveAcceptRemote) {
            Ok(binding) => binding,
            Err(()) => {
                let _ = self.reply_rpc(
                    sender,
                    request_id,
                    protocol::RPC_STATUS_ERROR,
                    SCRIPT_UNAVAILABLE_MESSAGE.as_bytes(),
                );
                return Err(());
            }
        };
        if binding.is_some() {
            let _ = self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                REMOTE_AUTHORITATIVE_ADMISSION_UNAVAILABLE_MESSAGE.as_bytes(),
            );
            return Err(());
        }
        let label = RoomLabel {
            map: "default".to_owned(),
            mode: "matchmaker".to_owned(),
            max_players: 0,
            open: false,
        };
        {
            let _scope = self.lock_room_scope();
            self.bind_rep_connection_to_room_under_scope(sender, room_id);
        }
        let body = serde_json::json!({ "accepted": true, "match_id": room_id }).to_string();
        let _ = self.reply_rpc(sender, request_id, protocol::RPC_STATUS_OK, body.as_bytes());
        let _ = self.reply_joined_with_rep_bootstrap(sender, room_id, label);
        Ok(())
    }

    /// Admit a member whose socket belongs to another session node after the
    /// durable formation/admission claim has succeeded.
    ///
    /// Script-bound remote matches fail closed on this owner node until their
    /// authenticated state/intent relay exists. Relay matches retain their
    /// prior remote room-membership admission.
    pub(crate) fn live_matchmaker_admit_remote(
        &self,
        requester_node: NodeId,
        user_id: String,
        room_id: RoomId,
    ) -> Result<(), ()> {
        self.require_native_match_lifecycle().map_err(|_| ())?;
        let binding = self.script_gate(ScriptGateSurface::LiveAdmitRemote)?;
        if self.remote_match_requires_state_relay(room_id) {
            return Err(());
        }
        let member = RemoteRoomMember {
            node_id: requester_node,
            user_id,
        };
        let (admission, previous) = {
            let _scope = self.lock_room_scope();
            let previous = self
                .rooms
                .remote_room_of(&member)
                .and_then(|id| self.room_snapshot_for_lifecycle(id));
            let admission =
                self.rooms
                    .admit_remote_match_bound(member.clone(), room_id, binding.as_ref());
            (admission, previous)
        };
        admission.map_err(|_| ())?;
        self.dispatch_remote_match_admission(&member, previous, room_id);
        Ok(())
    }

    /// Shard-owner-side validation after a party ticket crossed the
    /// asynchronous worker/control route. A revision change causes the worker
    /// to cancel before it publishes or forms the queued ticket.
    pub(crate) fn live_matchmaker_revalidate_party_admission(
        &self,
        admission: &PartyAdmissionFence,
    ) -> bool {
        let Some(parties) = &self.durable_parties else {
            return false;
        };
        let Ok(party_id) = PartyId::parse(&admission.party_id) else {
            return false;
        };
        let directory = Arc::clone(&parties.directory);
        let leader = admission.leader_user_id.clone();
        matches!(
            party_block_on(async move { directory.snapshot_for(&leader, &party_id).await }),
            Ok(snapshot) if snapshot.revision == admission.revision
                && snapshot.leader_user_id == admission.leader_user_id
        )
    }

    /// Release an admission freeze after the authoritative shard rejects it.
    /// This path is deliberately storage-backed; a remote shard must not leave
    /// a party permanently frozen after it cancels a stale ticket.
    pub(crate) fn live_matchmaker_release_party_admission(&self, admission: &PartyAdmissionFence) {
        let _ = self.release_durable_party_admission(admission);
    }

    /// Create a gateway with shared `metrics`, an optional script `runtime`, and
    /// an explicit `authenticator` that governs the realtime auth handshake.
    ///
    /// Production wires an authenticator backed by the node's session service and
    /// the configured stance (see `transport::start_enabled`); standalone/test
    /// gateways use [`Authenticator::guest_only`].
    #[must_use]
    pub fn with_metrics_runtime_auth(
        metrics: Arc<NodeMetrics>,
        runtime: Option<Arc<dyn Runtime>>,
        authenticator: Authenticator,
    ) -> Self {
        Self {
            registry: SessionRegistry::new(),
            ids: ParticipantIdGen::new(),
            metrics,
            authoritative_decision_recorder: None,
            runtime,
            authenticator,
            transform: None,
            rep: None,
            rep_rooms: Mutex::new(RepRoomBindings::default()),
            room_scope: Mutex::new(()),
            room_creation_gate: Mutex::new(()),
            generation_gate: RwLock::new(()),
            reload_retiring: AtomicBool::new(false),
            rooms: RoomRegistry::new(),
            npcs: Mutex::new(HashMap::new()),
            next_scoped_actor_id: Mutex::new(0x8000_0000),
            maps: Arc::new(MapCatalog::empty()),
            domain: None,
            matchmaker: Matchmaker::new(),
            parties: PartyRegistry::new(),
            durable_parties: None,
            party_presence: None,
            cluster_matchmaker: None,
            live_matchmaker: None,
            handoffs: Mutex::new(MatchmakerHandoffs::default()),
            script_readiness: None,
            strict_script_rooms: false,
            bridge: None,
            diagnostics: LagCaptureManager::default(),
            matches: None,
        }
    }

    /// Attach the GameScript readiness gate (builder style, before the
    /// gateway is shared). Wired only when `runtime.require_script` is
    /// enabled; see the field docs for the fail-closed contract.
    #[must_use]
    pub fn with_script_readiness(mut self, readiness: Arc<GameScriptReadiness>) -> Self {
        self.script_readiness = Some(readiness);
        self.strict_script_rooms = true;
        self
    }

    /// Attach readiness for opt-in per-room authoritative creation while keeping
    /// relay rooms available. The gate is consulted only when a room requests
    /// authoritative mode.
    #[must_use]
    pub fn with_optional_script_readiness(mut self, readiness: Arc<GameScriptReadiness>) -> Self {
        self.script_readiness = Some(readiness);
        self
    }

    /// The attached readiness gate, when this node requires a script.
    #[must_use]
    pub fn script_readiness(&self) -> Option<&Arc<GameScriptReadiness>> {
        self.script_readiness.as_ref()
    }

    /// Enable the authoritative-gameplay bridge (builder style, before the
    /// gateway is shared as `Arc<Gateway>`). `quotas` bounds every match's
    /// per-batch effects. Once enabled, a match participant's protected
    /// gameplay frames route through the per-match validator; leaving it unset
    /// preserves the non-authoritative relay path byte for byte.
    #[must_use]
    pub fn with_bridge(
        mut self,
        quotas: BridgeQuotas,
        capabilities: std::collections::HashSet<Capability>,
    ) -> Self {
        self.bridge = Some(Arc::new(GatewayBridge::new(quotas, capabilities)));
        self
    }

    /// Attach the app-composed bounded recorder before the gateway is shared.
    ///
    /// The recorder is observed only after bridge validation succeeds; it has no
    /// client, console, or runtime-host API surface.
    #[must_use]
    pub fn with_authoritative_decision_recorder(
        mut self,
        recorder: Arc<AuthoritativeDecisionRecorder>,
    ) -> Self {
        self.authoritative_decision_recorder = Some(recorder);
        self
    }

    /// Attach the durable match-record emitter before the gateway is shared.
    ///
    /// Also hands the room registry the node's own identity, so that a room's
    /// minted `match_id` and the `node_id` its record is written under agree.
    /// Rooms cannot exist yet at this point in the builder, so no live match
    /// ever sees its key change.
    #[must_use]
    pub fn with_match_recorder(mut self, recorder: Arc<MatchRecorder>) -> Self {
        self.rooms
            .adopt_identity(Arc::clone(recorder.writer().identity()));
        self.matches = Some(recorder);
        self
    }

    /// Whether the authoritative bridge is enabled on this node.
    #[must_use]
    pub fn bridge_enabled(&self) -> bool {
        self.bridge.is_some()
    }

    /// Wire this gateway as its runtime's bridge command sink (weakly — the
    /// gateway owns the runtime, so a strong reference would leak the cycle).
    /// A no-op when no runtime is attached; safe even when the bridge is
    /// disabled, since the sink then rejects every answer early.
    pub fn attach_bridge_sink(self: &Arc<Self>) {
        if let Some(runtime) = &self.runtime {
            runtime
                .attach_bridge_sink(Arc::downgrade(self) as std::sync::Weak<dyn BridgeCommandSink>);
        }
    }

    /// Attach the persisted domain-feature services, consuming and returning
    /// `self` (builder style, used before the gateway is shared as
    /// `Arc<Gateway>`). Enables built-in client RPC methods (`friends.*`, …);
    /// .
    #[must_use]
    pub fn with_domain_services(mut self, services: DomainRpcServices) -> Self {
        self.domain = Some(services);
        self
    }

    /// Attach the storage-backed party directory and its authenticated control
    /// router before the gateway is shared with transport listeners.
    #[must_use]
    pub fn with_storage_party_directory(
        mut self,
        directory: Arc<StoragePartyDirectory>,
        node_id: NodeId,
        router: Arc<TlsMatchmakerHandoffRouter>,
    ) -> Self {
        self.durable_parties = Some(DurablePartyGateway {
            directory,
            node_id,
            router,
        });
        self.party_presence = Some(PartyPresenceGateway::new(Arc::new(
            PartyPresenceDirectory::default(),
        )));
        self
    }

    /// Register the local mTLS party-owner callback after the shared gateway is
    /// available. The command still carries the durable fence and is rejected
    /// as stale if ownership changed between routing and application.
    pub fn register_party_directory_endpoint(self: &Arc<Self>) {
        let Some(parties) = &self.durable_parties else {
            return;
        };
        let router = Arc::clone(&parties.router);
        let gateway = Arc::downgrade(self);
        router.register_party_handler(Arc::new(move |_source, command| {
            gateway
                .upgrade()
                .map_or(PartyControlReply::Rejected, |gateway| {
                    gateway.apply_remote_party_command(command)
                })
        }));
        self.register_party_presence_endpoint();
    }

    /// Register the narrow party-presence control boundary on this gateway's
    /// authenticated router. The remote command carries only a party/node lease;
    /// member-level visibility is authorized and emitted locally.
    pub fn register_party_presence_endpoint(self: &Arc<Self>) {
        let (Some(parties), Some(presence)) = (&self.durable_parties, &self.party_presence) else {
            return;
        };
        let directory = Arc::clone(&presence.directory);
        let gateway_for_leases = Arc::downgrade(self);
        parties
            .router
            .register_party_presence_handler(Arc::new(move |_source, command| match command {
                PartyPresenceCommand::Advertise(lease) => {
                    let source = lease.node_id.clone();
                    let party_id = lease.party_id.clone();
                    let update = directory.advertise(lease, SystemClock.now());
                    if update == crate::party_presence::PartyPresenceUpdate::Applied
                        && let Some(gateway) = gateway_for_leases.upgrade()
                        && let Some(presence) = &gateway.party_presence
                    {
                        // A rejoin receives a newer fence; an old source
                        // snapshot must not remain visible while awaiting its
                        // first snapshot under that fence.
                        presence.clear_remote(&party_id, &source);
                    }
                    update
                }
                PartyPresenceCommand::Withdraw(withdrawal) => {
                    let source = withdrawal.node_id.clone();
                    let party_id = withdrawal.party_id.clone();
                    let update = directory.withdraw(withdrawal);
                    if update == crate::party_presence::PartyPresenceUpdate::Applied
                        && let Some(gateway) = gateway_for_leases.upgrade()
                        && let Some(presence) = &gateway.party_presence
                    {
                        presence.clear_remote(&party_id, &source);
                    }
                    update
                }
            }));
        let gateway = Arc::downgrade(self);
        parties
            .router
            .register_party_presence_delivery_handler(Arc::new(move |source, delivery| {
                gateway
                    .upgrade()
                    .map_or(PartyPresenceDeliveryDisposition::Rejected, |gateway| {
                        gateway.deliver_remote_party_presence(source, delivery)
                    })
            }));
    }

    /// Apply one mTLS-authenticated source-node snapshot. The transport has
    /// already checked framing/deadlines; this method fences both node leases,
    /// then reloads durable membership before touching a local socket queue.
    fn deliver_remote_party_presence(
        &self,
        source: NodeId,
        delivery: RemotePartyPresenceDelivery,
    ) -> PartyPresenceDeliveryDisposition {
        let (Some(parties), Some(presence)) = (&self.durable_parties, &self.party_presence) else {
            return PartyPresenceDeliveryDisposition::Rejected;
        };
        let now = SystemClock.now();
        if delivery.origin_node != source
            || delivery.deadline <= now
            || !presence.directory.matches_destination(
                &delivery.party_id,
                &parties.node_id,
                delivery.destination_generation,
                now,
            )
            || !presence.directory.matches_destination(
                &delivery.party_id,
                &source,
                delivery.origin_generation,
                now,
            )
        {
            return PartyPresenceDeliveryDisposition::Stale;
        }
        let Ok(party_id) = PartyId::parse(&delivery.party_id) else {
            return PartyPresenceDeliveryDisposition::Rejected;
        };
        let directory = Arc::clone(&parties.directory);
        let Ok(snapshot) = party_block_on(async move { directory.snapshot(&party_id).await })
        else {
            return PartyPresenceDeliveryDisposition::Unauthorized;
        };
        if snapshot.revision != delivery.snapshot.party_revision
            || delivery
                .snapshot
                .online_members
                .iter()
                .any(|member| !snapshot.members.contains(member))
        {
            return PartyPresenceDeliveryDisposition::Stale;
        }
        if !presence.replace_remote(&delivery.party_id, source, delivery.snapshot) {
            return PartyPresenceDeliveryDisposition::Stale;
        }
        let update = presence.local.snapshot_for_online_members(
            snapshot.party_id.as_str(),
            snapshot.revision,
            presence.merged_online_members(snapshot.party_id.as_str()),
        );
        self.emit_party_presence(&snapshot.members, update);
        PartyPresenceDeliveryDisposition::Delivered
    }

    /// Resolve accepted membership after a socket connects. Invitation claims
    /// deliberately return no presence state, preserving the party privacy
    /// boundary even during reconnect.
    fn sync_party_presence_for_session(&self, user_id: &str, participant: ParticipantId) {
        let (Some(parties), Some(_)) = (&self.durable_parties, &self.party_presence) else {
            return;
        };
        let directory = Arc::clone(&parties.directory);
        let user_id = user_id.to_owned();
        if let Ok(Some(snapshot)) =
            party_block_on(async move { directory.member_snapshot_for(&user_id).await })
        {
            self.reconcile_party_presence(snapshot);
        }
        let _ = participant; // participant is included by the registry snapshot above.
    }

    /// Reconcile local socket state from one committed durable party snapshot,
    /// then fan out only to its accepted local members through SessionRegistry.
    fn reconcile_party_presence(&self, snapshot: PartySnapshot) {
        let (Some(parties), Some(presence)) = (&self.durable_parties, &self.party_presence) else {
            return;
        };
        let members = snapshot.members.clone();
        let sessions: Vec<_> = members
            .iter()
            .flat_map(|member| {
                self.registry
                    .participants_for_user(member)
                    .into_iter()
                    .map(move |id| (member.clone(), id.get()))
            })
            .collect();
        let changed = presence
            .local
            .reconcile(snapshot.party_id.as_str(), &sessions);
        if !changed {
            return;
        }
        let now = SystemClock.now();
        let active_lease = if presence
            .local
            .has_online_members(snapshot.party_id.as_str())
        {
            let lease = presence.renew(
                snapshot.party_id.as_str(),
                parties.node_id.clone(),
                snapshot.revision,
                now,
            );
            if let Some(lease) = &lease {
                for peer in parties.router.peer_nodes() {
                    let _ = parties
                        .router
                        .advertise_party_presence(&peer, lease.clone());
                }
            }
            lease
        } else {
            None
        };
        let source_update = presence.source_snapshot(snapshot.party_id.as_str(), snapshot.revision);
        let update = presence.local.snapshot_for_online_members(
            snapshot.party_id.as_str(),
            snapshot.revision,
            presence.merged_online_members(snapshot.party_id.as_str()),
        );
        // Fan out exactly one typed source snapshot per live destination node.
        // On a final local leave this happens before withdrawal, so the empty
        // snapshot is still fenced by the source's current advertisement.
        if let Some(origin_generation) = active_lease
            .as_ref()
            .map(|lease| lease.generation)
            .or_else(|| presence.active_generation(snapshot.party_id.as_str()))
        {
            for destination in presence
                .directory
                .destinations(snapshot.party_id.as_str(), now)
            {
                if destination.node_id == parties.node_id {
                    continue;
                }
                let _ = parties.router.deliver_party_presence(
                    &destination.node_id,
                    RemotePartyPresenceDelivery {
                        party_id: snapshot.party_id.as_str().to_owned(),
                        origin_node: parties.node_id.clone(),
                        origin_generation,
                        destination_generation: destination.generation,
                        snapshot: source_update.clone(),
                        deadline: now
                            .checked_add(DurationMillis::from_millis(PARTY_PRESENCE_LEASE_MS))
                            .unwrap_or(now),
                    },
                );
            }
        }
        if active_lease.is_none()
            && let Some(withdrawal) =
                presence.withdraw(snapshot.party_id.as_str(), parties.node_id.clone())
        {
            for peer in parties.router.peer_nodes() {
                let _ = parties
                    .router
                    .withdraw_party_presence(&peer, withdrawal.clone());
            }
        }
        self.emit_party_presence(&members, update);
    }

    fn emit_party_presence(&self, members: &[String], update: PartyPresenceSnapshot) {
        let Some(presence) = &self.party_presence else {
            return;
        };
        for member in members {
            for recipient in self.registry.participants_for_user(member) {
                let recipient_key = recipient.to_string();
                let delivery = presence.local.delivery_for(&recipient_key, update.clone());
                let send = |kind: &str, snapshot: &PartyPresenceSnapshot, delivery: Delivery| {
                    let body = serde_json::json!({
                        "type": kind,
                        "party_id": snapshot.party_id,
                        "party_revision": snapshot.party_revision,
                        "presence_sequence": snapshot.sequence,
                        "online_members": snapshot.online_members,
                    })
                    .to_string()
                    .into_bytes();
                    // Presence is latest-state traffic. Keeping it on the
                    // registry's bounded replacement queue prevents a slow
                    // party client from delaying its RPC reply; a replacement
                    // failure is converted to snapshot+resync below.
                    self.registry.send_to(
                        recipient,
                        &Outbound::new(delivery, Envelope::new(KIND_NOTIFICATION, body)),
                    )
                };
                let delivered = match delivery {
                    PartyPresenceDelivery::Delta(snapshot) => {
                        send("party.presence.delta", &snapshot, Delivery::Unreliable)
                    }
                    PartyPresenceDelivery::Snapshot(snapshot) => {
                        send("party.presence.snapshot", &snapshot, Delivery::Unreliable)
                    }
                    PartyPresenceDelivery::ResyncRequired {
                        party_id,
                        party_revision,
                        sequence,
                    } => {
                        let resync = PartyPresenceSnapshot {
                            party_id,
                            party_revision,
                            sequence,
                            online_members: Vec::new(),
                        };
                        // The resync barrier is reliable so it cannot be
                        // replaced by the immediately following latest-wins
                        // snapshot in the same notification mailbox.
                        send("party.presence.resync", &resync, Delivery::Reliable)
                            && send("party.presence.snapshot", &update, Delivery::Unreliable)
                    }
                };
                if !delivered {
                    presence
                        .local
                        .mark_queue_drop(update.party_id.as_str(), &recipient_key);
                }
            }
        }
    }

    /// Attach the optional fenced cluster-presence lifecycle before sharing the
    /// gateway. Single-node gateways intentionally retain `None`.
    #[must_use]
    pub fn with_chat_cluster_presence(
        mut self,
        announcer: Arc<LocalChatPresenceAnnouncer>,
    ) -> Self {
        if let Some(domain) = &mut self.domain {
            domain.chat_cluster_presence = Some(announcer);
        }
        self
    }

    /// Attach the loaded map catalog, consuming and returning `self` (builder
    /// style). When a room is created, its chosen `map` name is resolved against
    /// this catalog. Defaults to an empty catalog (no cooked maps).
    #[must_use]
    pub fn with_maps(mut self, maps: Arc<MapCatalog>) -> Self {
        self.maps = maps;
        self
    }

    /// Attach an authoritative transform-sync hub, consuming and returning `self`
    /// (builder style, used before the gateway is shared as `Arc<Gateway>`).
    #[must_use]
    pub fn with_transform_hub(mut self, hub: Arc<TransformHub>) -> Self {
        self.transform = Some(hub);
        self
    }

    /// The attached transform-sync hub, if any.
    #[must_use]
    pub fn transform_hub(&self) -> Option<&Arc<TransformHub>> {
        self.transform.as_ref()
    }

    /// Attach a `NetworkPeer` server authority, consuming and returning `self`
    /// (builder style, used before the gateway is shared as `Arc<Gateway>`).
    #[must_use]
    pub fn with_rep_authority(mut self, rep: Arc<RepAuthority>) -> Self {
        self.rep = Some(rep);
        self
    }

    /// The attached `NetworkPeer` server authority, if any.
    #[must_use]
    pub fn rep_authority(&self) -> Option<&Arc<RepAuthority>> {
        self.rep.as_ref()
    }

    /// Register one approved replicated class from trusted server lifecycle
    /// code. Network clients have no protocol route to this method.
    pub fn register_rep_class(
        &self,
        class_id: u32,
        layout: &'static RepLayout,
        schema: RepSchema,
    ) -> Result<(), RepReject> {
        self.rep
            .as_ref()
            .ok_or(RepReject::UnknownObject)?
            .register_class(class_id, layout, schema)
    }

    /// Spawn a replicated object from trusted server lifecycle code. Clients can
    /// propose values only through the authority's validated delta path.
    pub fn spawn_rep_object(
        &self,
        object_id: u32,
        match_id: u64,
        class_id: u32,
        owner: Option<ParticipantId>,
        persistent: bool,
        initial: RepSnapshot,
    ) -> Result<(), RepReject> {
        let _scope = self.lock_room_scope();
        let room_id = owner.and_then(|participant| self.rooms.room_of(participant));
        self.spawn_rep_object_scoped_under_scope(RepObjectSpawn {
            object_id,
            match_id,
            room_id,
            class_id,
            owner,
            persistent,
            initial,
        })
    }

    /// Spawn a server-owned replicated object bound to one explicit room. This
    /// is the trusted lifecycle path for objects without a participant owner;
    /// `match_id` is an internal RepAuthority index and never a room identity.
    pub fn spawn_rep_object_in_room(
        &self,
        object_id: u32,
        match_id: u64,
        room_id: RoomId,
        class_id: u32,
        persistent: bool,
        initial: RepSnapshot,
    ) -> Result<(), RepReject> {
        let _scope = self.lock_room_scope();
        self.spawn_rep_object_scoped_under_scope(RepObjectSpawn {
            object_id,
            match_id,
            room_id: Some(room_id),
            class_id,
            owner: None,
            persistent,
            initial,
        })
    }

    /// Spawn only while [`Self::room_scope`] is held. In particular, an owner
    /// cannot change rooms between deriving its room and recording the object's
    /// trusted room binding.
    fn spawn_rep_object_scoped_under_scope(&self, spawn: RepObjectSpawn) -> Result<(), RepReject> {
        if spawn.owner.is_none() && spawn.room_id.is_none() && self.rooms.room_count() != 0 {
            // Server-owned gameplay state in a room must name that room. The
            // legacy roomless path remains available before any room exists.
            return Err(RepReject::NoMatch);
        }
        if let Some(room_id) = spawn.room_id {
            self.bind_rep_object_to_room_under_scope(spawn.object_id, spawn.match_id, room_id)?;
        }
        let result = self
            .rep
            .as_ref()
            .ok_or(RepReject::UnknownObject)?
            .spawn_object(
                spawn.object_id,
                spawn.match_id,
                spawn.class_id,
                spawn.owner.map(ParticipantId::get),
                spawn.persistent,
                spawn.initial,
            );
        if result.is_err()
            && spawn.room_id.is_some()
            && let Ok(mut bindings) = self.rep_rooms.lock()
        {
            bindings.objects.remove(&spawn.object_id);
        }
        result
    }

    /// Despawn a replicated object from trusted server lifecycle code.
    pub fn despawn_rep_object(&self, object_id: u32) -> bool {
        let _scope = self.lock_room_scope();
        let despawned = self
            .rep
            .as_ref()
            .is_some_and(|rep| rep.despawn_object(object_id));
        if despawned && let Ok(mut bindings) = self.rep_rooms.lock() {
            bindings.objects.remove(&object_id);
        }
        despawned
    }

    /// Bind a roomless receiver to a trusted legacy replication match and send
    /// its schema table followed by full baselines. A participant in a room is
    /// always rebound from the room's trusted binding instead; the supplied id
    /// cannot make replication membership drift from RoomRegistry membership.
    pub fn join_rep_match(&self, id: ParticipantId, match_id: u64, is_guest: bool) {
        let Some(rep) = &self.rep else {
            return;
        };
        let _scope = self.lock_room_scope();
        let room_bound = {
            if let Some(room_id) = self.rooms.room_of(id) {
                self.bind_rep_connection_to_room_under_scope(id, room_id);
                true
            } else {
                false
            }
        };
        if room_bound {
            drop(_scope);
            let _ = self.send_rep_bootstrap(id);
            return;
        }
        rep.join_match(id.get(), match_id, is_guest);
        drop(_scope);
        let _ = self.send_rep_bootstrap(id);
    }

    /// Associate an object with its room and make that room's RepAuthority
    /// index unambiguous before it can be replicated. Existing objects prevent
    /// a trusted lifecycle caller from silently changing a live room's match.
    /// Bind one replicated object while [`Self::room_scope`] is held. The
    /// membership snapshot and every receiver binding are committed as one
    /// transaction, so a scoped object cannot become visible to the authority
    /// before its room identity is enforceable at delivery.
    fn bind_rep_object_to_room_under_scope(
        &self,
        object_id: u32,
        match_id: u64,
        room_id: RoomId,
    ) -> Result<(), RepReject> {
        let members = self.rooms.members(room_id);
        {
            let mut bindings = self.rep_rooms.lock().map_err(|_| RepReject::Frame)?;
            if bindings
                .match_rooms
                .get(&match_id)
                .is_some_and(|bound_room| *bound_room != room_id)
            {
                return Err(RepReject::NoMatch);
            }
            if let Some(previous_match) = bindings.room_matches.get(&room_id).copied()
                && previous_match != match_id
                && bindings
                    .objects
                    .values()
                    .any(|bound_room| *bound_room == room_id)
            {
                return Err(RepReject::NoMatch);
            }
            if let Some(previous_match) = bindings.room_matches.insert(room_id, match_id)
                && bindings.match_rooms.get(&previous_match) == Some(&room_id)
            {
                bindings.match_rooms.remove(&previous_match);
            }
            bindings.match_rooms.insert(match_id, room_id);
            bindings.objects.insert(object_id, room_id);
            for member in &members {
                bindings.connections.insert(*member, room_id);
            }
        }
        if let Some(rep) = &self.rep {
            for member in members {
                let is_guest = self.registry.user_id_of(member).is_none();
                rep.join_match(member.get(), match_id, is_guest);
            }
        }
        Ok(())
    }

    /// Bind a connection to `room_id` while [`Self::room_scope`] is held.
    /// Callers that also mutate [`RoomRegistry`] must use this variant in the
    /// same critical section as that mutation.
    fn bind_rep_connection_to_room_under_scope(&self, id: ParticipantId, room_id: RoomId) {
        let match_id = {
            let Ok(mut bindings) = self.rep_rooms.lock() else {
                return;
            };
            let match_id = *bindings.room_matches.entry(room_id).or_insert(room_id);
            bindings.connections.insert(id, room_id);
            match_id
        };
        let Some(rep) = &self.rep else {
            return;
        };
        rep.join_match(id.get(), match_id, self.registry.user_id_of(id).is_none());
        // Preserve the existing room-join wire ordering while still allocating
        // the per-object sender baselines needed for the next room-scoped delta.
        // The explicit trusted bootstrap path remains responsible for delivery.
        let _ = rep.bootstrap(id.get());
    }

    /// Remove a connection binding while [`Self::room_scope`] is held.
    fn unbind_rep_connection_under_scope(&self, id: ParticipantId) {
        if let Some(rep) = &self.rep {
            rep.leave(id.get());
        }
        if let Ok(mut bindings) = self.rep_rooms.lock() {
            bindings.connections.remove(&id);
        }
    }

    /// Obtain the gateway-wide transaction gate for room membership, trusted
    /// replication bindings, and their client-facing deliveries.
    fn lock_room_scope(&self) -> std::sync::MutexGuard<'_, ()> {
        self.room_scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[must_use]
    fn rep_object_room(&self, object_id: u32) -> Option<RoomId> {
        self.rep_rooms
            .lock()
            .ok()
            .and_then(|bindings| bindings.objects.get(&object_id).copied())
    }

    fn send_bound_rep_object_under_scope(
        &self,
        source: Option<ParticipantId>,
        target: ParticipantId,
        object_id: u32,
        room_id: RoomId,
        outbound: &Outbound,
    ) -> bool {
        let Ok(bindings) = self.rep_rooms.lock() else {
            return false;
        };
        if bindings.objects.get(&object_id) != Some(&room_id)
            || bindings.connections.get(&target) != Some(&room_id)
            || source.is_some_and(|sender| bindings.connections.get(&sender) != Some(&room_id))
        {
            return false;
        }
        drop(bindings);
        let owners: Vec<u64> = source.into_iter().map(ParticipantId::get).collect();
        self.rooms
            .while_member_and_owners_in(target, Some(room_id), &owners, || {
                self.registry.send_to(target, outbound)
            })
            .unwrap_or(false)
    }

    /// Whether an embedded script runtime is driving message dispatch.
    #[must_use]
    pub fn has_runtime(&self) -> bool {
        self.runtime.is_some()
    }

    /// The embedded script runtime, when one is attached (console
    /// introspection / RPC-caller seam; ).
    #[must_use]
    pub fn runtime(&self) -> Option<&Arc<dyn Runtime>> {
        self.runtime.as_ref()
    }

    /// Allocate a fresh session id for a new connection.
    pub fn next_participant_id(&self) -> ParticipantId {
        self.ids.next_id()
    }

    /// Whether the configured stance requires a valid token to connect.
    #[must_use]
    pub fn require_auth(&self) -> bool {
        self.authenticator.require_auth()
    }

    /// Resolve the realtime auth handshake from a connection's FIRST inbound
    /// envelope.
    ///
    /// Classifies the envelope into a [`PresentedCredential`] — a `KIND_AUTH`
    /// frame carries a token (non-empty body) or an explicit guest request (empty
    /// body); any other first frame is a pre-handshake/legacy client — then
    /// resolves it against the node's session service and stance. Transports call
    /// this once, before registering the participant, and act on the returned
    /// [`Handshake`]:
    ///
    /// - [`AuthOutcome::Authenticated`]/[`AuthOutcome::Guest`]: register a
    ///   [`SessionHandle`] (with the bound identity for the authenticated path),
    ///   send a `KIND_AUTH_RESULT`, and — when `replay_first` — dispatch the
    ///   triggering frame so a legacy client's first message is not dropped.
    /// - [`AuthOutcome::Rejected`]: send a `KIND_AUTH_RESULT` rejection and close
    ///   the connection *without* registering anything (no partial state).
    ///
    /// The token secret never leaves this path: it is validated and dropped, and
    /// only the resolved `user_id`/`session_id` is retained.
    pub async fn resolve_handshake(&self, first: &Envelope) -> Handshake {
        let (presented, replay_candidate) = classify_handshake(first);
        let outcome = self.authenticator.resolve(presented).await;
        // Only replay the first frame when it was a non-handshake frame we
        // accepted as an implicit guest.
        let replay_first = replay_candidate && matches!(outcome, AuthOutcome::Guest);
        Handshake {
            outcome,
            replay_first,
        }
    }

    /// Access the session registry (read-only fan-out state).
    ///
    /// Prefer [`register_session`]/[`unregister_session`] for the connect and
    /// disconnect lifecycle so the `sessions_active` gauge stays in sync.
    ///
    /// [`register_session`]: Gateway::register_session
    /// [`unregister_session`]: Gateway::unregister_session
    #[must_use]
    pub fn registry(&self) -> &SessionRegistry {
        &self.registry
    }

    /// Mint the post-auth `SERVER_TIME` offer for a transport session. The
    /// transport must enqueue the resulting envelope immediately after the
    /// unchanged `AUTH_RESULT`; the offer is bound to `participant` and cannot
    /// enable diagnostics by itself.
    pub fn issue_diagnostics_server_time(
        &self,
        participant: ParticipantId,
        now: TimestampMillis,
    ) -> Result<ServerTime, LagCaptureError> {
        self.diagnostics.issue_server_time(participant, now)
    }

    /// Drop an offer made for a transport registration that lost its close or
    /// revocation race before becoming a live session.
    pub fn abandon_diagnostics_session(&self, participant: ParticipantId) {
        self.diagnostics.abandon_session(participant);
    }

    /// Start an opt-in diagnostics capture using the production UTC correlation
    /// clock. This is a trusted native Gateway API, never a GameScript command.
    pub fn start_lag_capture(
        &self,
        request: StartCapture,
    ) -> Result<LagCaptureStart, LagCaptureError> {
        self.start_lag_capture_at(request, SystemClock.now())
    }

    /// Start a capture at an explicit UTC instant for deterministic callers and
    /// tests. A local outbound queue accepting START is recorded as `Requested`,
    /// never as a client receipt or expected upload.
    pub fn start_lag_capture_at(
        &self,
        request: StartCapture,
        now: TimestampMillis,
    ) -> Result<LagCaptureStart, LagCaptureError> {
        let body = request
            .encode()
            .map_err(|_| LagCaptureError::InvalidRequest)?;
        let result_request = request.clone();
        let connected = self.registry.participants();
        let (candidates, ineligible) = self.diagnostics.begin(request, &connected, now)?;
        let mut requested = Vec::new();
        let mut enqueue_failed = Vec::new();
        for participant in candidates {
            let queued = self.send_reliable(participant, KIND_DIAG_START, body.clone());
            self.diagnostics.mark_start_enqueue(participant, queued);
            if queued {
                requested.push(participant);
            } else {
                enqueue_failed.push(participant);
            }
        }
        let _ = self.diagnostics.finish_if_terminal();
        Ok(LagCaptureStart {
            request: result_request,
            requested,
            ineligible,
            enqueue_failed,
        })
    }

    /// Start a capture together with the private raw-ingest lifecycle. The
    /// ingest reservation is native-only and is rolled back if the realtime
    /// START is rejected before any client can observe it.
    pub fn start_lag_capture_with_ingest_at(
        &self,
        ingest: &LagDiagnosticsService,
        request: StartCapture,
        now: TimestampMillis,
    ) -> Result<LagCaptureStart, LagCaptureError> {
        if !ingest.is_enabled() {
            return Err(LagCaptureError::IngestUnavailable);
        }
        ingest
            .register_recording(
                request.capture_id,
                request.generation,
                request.deadline_server_utc_ms,
            )
            .map_err(|_| LagCaptureError::IngestUnavailable)?;
        match self.start_lag_capture_at(request.clone(), now) {
            Ok(start) => Ok(start),
            Err(error) => {
                let _ = ingest.discard_recording(request.capture_id, request.generation);
                Err(error)
            }
        }
    }

    /// Request a flush from only clients that authenticated `Recording`. Upload
    /// URL/token material belongs to the capture-ingest layer and is deliberately
    /// absent from `CaptureStatus` and this base lifecycle API.
    pub fn flush_lag_capture(
        &self,
        request: FlushCapture,
    ) -> Result<LagCaptureFlush, LagCaptureError> {
        self.flush_lag_capture_at(request, SystemClock.now())
    }

    /// Deterministic-time form of [`Self::flush_lag_capture`].
    pub fn flush_lag_capture_at(
        &self,
        request: FlushCapture,
        now: TimestampMillis,
    ) -> Result<LagCaptureFlush, LagCaptureError> {
        let status = self
            .diagnostics
            .status(request.capture_id)
            .ok_or(LagCaptureError::UnknownCapture)?;
        if status
            .participants
            .iter()
            .filter(|participant| {
                participant.state == crate::realtime::LagCaptureParticipantState::Recording
            })
            .count()
            != 1
        {
            return Err(LagCaptureError::PerParticipantGrantRequired);
        }
        let body = request
            .encode()
            .map_err(|_| LagCaptureError::InvalidRequest)?;
        let targets = self.diagnostics.prepare_flush(&request, now)?;
        let mut requested = Vec::new();
        let mut enqueue_failed = Vec::new();
        for participant in targets {
            let queued = self.send_reliable(participant, KIND_DIAG_FLUSH, body.clone());
            self.diagnostics.mark_flush_enqueue(participant, queued);
            if queued {
                requested.push(participant);
            } else {
                enqueue_failed.push(participant);
            }
        }
        let _ = self.diagnostics.finish_if_terminal();
        Ok(LagCaptureFlush {
            request,
            requested,
            enqueue_failed,
        })
    }

    /// Secure native FLUSH path. It obtains the exact server-observed
    /// `Recording` population, mints one one-use signed grant per participant,
    /// and sends a different FLUSH body to every bounded transport queue.
    ///
    /// The supplied plan's identity bindings are trusted native data from the
    /// match/session owner. Unselected bindings are ignored; a missing binding
    /// aborts before any grant is issued.
    pub fn flush_lag_capture_with_ingest_at(
        &self,
        ingest: &LagDiagnosticsService,
        mut plan: CaptureFlushPlan,
        now: TimestampMillis,
    ) -> Result<LagCaptureUploadFlush, LagCaptureError> {
        if !ingest.is_enabled() {
            return Err(LagCaptureError::IngestUnavailable);
        }
        let capture_id = plan.capture_id;
        let generation = plan.generation;
        let attempt_id = plan.attempt_id;
        let upload_deadline = plan.upload_deadline_server_utc_ms;
        let targets = self.diagnostics.prepare_flush_identity(
            capture_id,
            generation,
            attempt_id,
            upload_deadline,
            now,
        )?;
        if targets.is_empty() {
            return Ok(LagCaptureUploadFlush {
                grants: Vec::new(),
                requested: Vec::new(),
                enqueue_failed: Vec::new(),
            });
        }
        let mut bindings = plan
            .participants
            .drain(..)
            .map(|binding| (binding.participant_id, binding))
            .collect::<HashMap<_, _>>();
        let mut selected = Vec::with_capacity(targets.len());
        for participant in &targets {
            let Some(binding) = bindings.remove(&participant.get()) else {
                self.diagnostics
                    .rollback_flush_identity(capture_id, generation, attempt_id);
                return Err(LagCaptureError::InvalidFlush);
            };
            selected.push(binding);
        }
        plan.participants = selected;
        let grants = match ingest.open_flush(plan, now) {
            Ok(grants) => grants,
            Err(_) => {
                self.diagnostics
                    .rollback_flush_identity(capture_id, generation, attempt_id);
                return Err(LagCaptureError::IngestUnavailable);
            }
        };
        let mut requested = Vec::with_capacity(grants.len());
        let mut enqueue_failed = Vec::new();
        let mut delivered = Vec::with_capacity(grants.len());
        for grant in grants {
            let participant = ParticipantId::from_raw(grant.participant_id);
            let queued = grant
                .flush
                .encode()
                .map(|body| self.send_reliable(participant, KIND_DIAG_FLUSH, body))
                .unwrap_or(false);
            self.diagnostics.mark_flush_enqueue(participant, queued);
            if queued {
                requested.push(participant);
                delivered.push(grant);
            } else {
                enqueue_failed.push(participant);
            }
        }
        let _ = self.diagnostics.finish_if_terminal();
        Ok(LagCaptureUploadFlush {
            grants: delivered,
            requested,
            enqueue_failed,
        })
    }

    /// Return the active capture's native lifecycle snapshot. It distinguishes
    /// queueing, client start acknowledgement, upload start, completion, and
    /// disconnect rather than inferring any of them from transport success.
    #[must_use]
    pub fn lag_capture_status(&self, capture_id: CaptureId) -> Option<LagCaptureStatus> {
        self.diagnostics.status(capture_id)
    }

    /// Advance the active capture's server-UTC deadline state. This must be
    /// called by trusted match/capture maintenance, not by client input.
    pub fn expire_lag_capture_deadline(&self) -> usize {
        self.expire_lag_capture_deadline_at(SystemClock.now())
    }

    /// Deterministic-time deadline transition for maintenance and tests.
    pub fn expire_lag_capture_deadline_at(&self, now: TimestampMillis) -> usize {
        self.diagnostics.expire_deadline(now)
    }

    /// Complete an all-settled capture early (for example after the ingest
    /// layer records every expected upload). The terminal snapshot remains
    /// queryable by id while the next match may start a fresh capture.
    pub fn complete_lag_capture_if_terminal(&self) -> bool {
        self.diagnostics.finish_if_terminal()
    }

    pub fn deliver_local_chat(
        &self,
        origin_node: &NodeId,
        delivery: RemoteChatDelivery,
    ) -> ChatDeliveryDisposition {
        if delivery.deadline <= SystemClock.now() {
            return ChatDeliveryDisposition::Rejected;
        }
        let Some(event) = Self::validated_durable_chat_event(&delivery) else {
            return ChatDeliveryDisposition::Rejected;
        };
        let Some(domain) = &self.domain else {
            return ChatDeliveryDisposition::Unavailable;
        };
        if origin_node.as_str() != domain.node_id {
            return ChatDeliveryDisposition::Rejected;
        }
        let Ok(subscriptions) = domain
            .chat_presence
            .subscribers_at_authority_epoch(&delivery.channel_id, delivery.authority_epoch)
        else {
            return ChatDeliveryDisposition::Unavailable;
        };
        if subscriptions.is_empty() {
            return ChatDeliveryDisposition::Unknown;
        }
        domain.send_chat_event(
            &self.registry,
            &delivery.channel_id,
            &subscriptions,
            event,
            delivery.event_id,
        );
        ChatDeliveryDisposition::Delivered
    }

    fn validated_durable_chat_event(delivery: &RemoteChatDelivery) -> Option<serde_json::Value> {
        let event: crate::repository::chat::ChatDeliveryEvent =
            serde_json::from_str(&delivery.payload).ok()?;
        if event.channel_id != delivery.channel_id
            || event.event_id != delivery.event_id
            || crate::repository::chat::validate_delivery_event_state(&event).is_err()
        {
            return None;
        }
        serde_json::to_value(event).ok()
    }

    /// Apply one already-authenticated typed remote chat command to current
    /// local subscriptions. The caller owns mTLS peer validation; this method
    /// enforces the destination lease and authority fences before it can touch
    /// a session queue.
    pub fn deliver_remote_chat(
        &self,
        local_node: &NodeId,
        directory: &ChatPresenceDirectory,
        delivery: RemoteChatDelivery,
    ) -> ChatDeliveryDisposition {
        if delivery.deadline <= SystemClock.now() {
            return ChatDeliveryDisposition::Rejected;
        }
        let Some(event) = Self::validated_durable_chat_event(&delivery) else {
            return ChatDeliveryDisposition::Rejected;
        };
        let Some(domain) = &self.domain else {
            return ChatDeliveryDisposition::Unavailable;
        };
        let disposition = directory.validate_local_delivery(
            local_node,
            &delivery,
            &domain.chat_presence,
            SystemClock.now(),
        );
        if disposition != ChatDeliveryDisposition::Delivered {
            return disposition;
        }
        let Ok(subscriptions) = domain
            .chat_presence
            .subscribers_at_authority_epoch(&delivery.channel_id, delivery.authority_epoch)
        else {
            return ChatDeliveryDisposition::Unavailable;
        };
        domain.send_chat_event(
            &self.registry,
            &delivery.channel_id,
            &subscriptions,
            event,
            delivery.event_id,
        );
        ChatDeliveryDisposition::Delivered
    }

    /// Renew all current local channel leases. The supervised cluster worker
    /// calls this independently of client traffic so an idle but connected
    /// subscriber remains discoverable until it leaves or disconnects.
    pub fn renew_chat_cluster_presence(&self) {
        if let Some(domain) = &self.domain
            && let Some(announcer) = &domain.chat_cluster_presence
        {
            announcer.renew(&domain.chat_presence, SystemClock.now());
        }
    }

    /// The shared node metrics registry surfaced by the dashboard.
    #[must_use]
    pub fn node_metrics(&self) -> &Arc<NodeMetrics> {
        &self.metrics
    }

    /// Non-sensitive telemetry from the local ticket index. This is separate
    /// from transport counters because it deliberately reports no player or
    /// query data.
    #[must_use]
    pub fn matchmaker_stats(&self) -> MatchmakerStats {
        self.matchmaker.stats()
    }

    /// Drive one scheduled local-matchmaker evaluation. The supervised service
    /// calls this at 250 ms even when no game-script tick is configured.
    pub fn matchmaker_tick(&self) -> usize {
        let now = SystemClock.now();
        // The 250 ms cadence doubles as the degraded-hold clock: a backend
        // that stays unhealthy past the (PROVISIONAL, injectable) hold window
        // escalates from Degraded to Unavailable. Both states gate new
        // matches; the escalation is an operator signal, not a teardown.
        if let Some(readiness) = &self.script_readiness
            && readiness.expire_degraded_hold(now)
        {
            tracing::warn!(
                "script readiness degraded hold expired without recovery; now unavailable"
            );
        }
        self.activate_formed_matches(now)
    }

    /// Record that a transport connection was accepted (dashboard gauge +1).
    ///
    /// Transports call this once per accepted connection, paired with
    /// [`connection_closed`] on teardown.
    ///
    /// [`connection_closed`]: Gateway::connection_closed
    pub fn connection_opened(&self) {
        self.metrics.connection_opened();
    }

    /// Record that a transport connection was torn down (dashboard gauge -1).
    pub fn connection_closed(&self) {
        self.metrics.connection_closed();
    }

    /// Register a session handle and reflect it in the `sessions_active` gauge.
    ///
    /// After the session is in the registry, the script's `citadel.on_join`
    /// handler (if any) runs with `ctx.sender` set to the new participant; any
    /// commands it emits are broadcast to the other participants (the joiner is
    /// excluded, matching a spawn-notification to existing peers). The join hook
    /// runs *after* the registry insert, so a concurrent tick may briefly observe
    /// the new participant before `on_join` fires — acceptable for the MVP.
    pub fn register_session(&self, handle: SessionHandle) -> LatestOutboundReceiver {
        self.register_session_with_initial(handle, None)
    }

    /// Register a session with an optional protocol envelope that must lead its
    /// reliable queue. The registry assigns the close fence before publishing
    /// the session, so handshake acknowledgements cannot bypass revocation.
    pub fn register_session_with_initial(
        &self,
        handle: SessionHandle,
        initial: Option<Outbound>,
    ) -> LatestOutboundReceiver {
        self.register_session_with_initials(handle, initial.into_iter().collect())
    }

    /// Register a session with an ordered reliable protocol prefix. This is
    /// used by transports to preserve `AUTH_RESULT` then `SERVER_TIME` without
    /// changing the legacy auth-result body or allowing lifecycle messages to
    /// overtake either envelope.
    pub fn register_session_with_initials(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
    ) -> LatestOutboundReceiver {
        let id = handle.id;
        let authenticated = handle.is_authenticated();
        let authenticated_user = handle
            .identity
            .as_ref()
            .map(|identity| identity.user_id.as_str().to_owned());
        let unreliable = self.registry.register_with_initials(handle, initials);
        // A durable revocation that completed after token validation but before
        // publication leaves a registry tombstone. Do not run lifecycle work or
        // alter gauges for that rejected registration.
        if !self.registry.accepts_work(id) {
            return unreliable;
        }
        self.complete_gateway_registration(id, authenticated, authenticated_user.as_deref(), true);
        unreliable
    }

    /// Run side effects for a registration only while the registry confirms that
    /// this exact generation still owns the active session mapping. Replacement
    /// and cleanup share that ownership gate, so an obsolete registration cannot
    /// emit Join, publish presence, or move gauges after it loses ownership.
    fn complete_gateway_registration(
        &self,
        id: ParticipantId,
        authenticated: bool,
        authenticated_user: Option<&str>,
        initialize_replication: bool,
    ) -> bool {
        self.registry.run_gateway_registration(id, || {
            self.metrics.participant_opened();
            if authenticated {
                self.metrics.session_opened();
                if let Some(user_id) = authenticated_user {
                    self.sync_party_presence_for_session(user_id, id);
                }
            }
            if initialize_replication && let Some(rep) = &self.rep {
                let room_bound = {
                    let _scope = self.lock_room_scope();
                    let assigned_room = self
                        .rep_rooms
                        .lock()
                        .ok()
                        .and_then(|bindings| bindings.connections.get(&id).copied());
                    if let Some(room_id) = assigned_room {
                        self.bind_rep_connection_to_room_under_scope(id, room_id);
                        true
                    } else {
                        false
                    }
                };
                if room_bound {
                    let _ = self.send_rep_bootstrap(id);
                } else if self.bridge.is_some() && !rep.is_joined(id.get()) {
                    let _ = self.send_rep_schema(id);
                } else if self.bridge.is_none() {
                    rep.join_match(id.get(), 0, self.registry.user_id_of(id).is_none());
                    let _ = self.send_rep_bootstrap(id);
                }
            }
            self.dispatch_lifecycle(LifecycleHook::Join, id);
        })
    }

    /// Time-checked authenticated registration used by realtime transports.
    /// Unlike the legacy compatibility entry points, this binds the exact
    /// `SessionId` to one active participant, fences an incumbent, and rejects
    /// an identity at its expiry boundary.
    pub fn register_session_at(
        &self,
        handle: SessionHandle,
        now: TimestampMillis,
    ) -> TransportRegistration {
        self.register_session_with_initials_at(handle, Vec::new(), now)
    }

    /// Time-checked form of [`Self::register_session_with_initials`].
    pub fn register_session_with_initials_at(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
        now: TimestampMillis,
    ) -> TransportRegistration {
        self.register_session_with_initials_at_after_publish(handle, initials, now, || {})
    }

    /// Internal registration seam that keeps the post-publication ownership
    /// check explicit for deterministic interleaving tests.
    fn register_session_with_initials_at_after_publish<F>(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
        now: TimestampMillis,
        after_publish: F,
    ) -> TransportRegistration
    where
        F: FnOnce(),
    {
        let id = handle.id;
        let authenticated = handle.is_authenticated();
        let authenticated_user = handle
            .identity
            .as_ref()
            .map(|identity| identity.user_id.as_str().to_owned());
        let registration = self.registry.register_session_at(handle, initials, now);
        let unreliable = registration.unreliable;
        let superseded = registration.superseded;
        let supersession_gate = registration.supersession_gate;
        let transport_write_gate = registration.transport_write_gate;
        let superseding = registration.superseding;
        let mut replaced_cleanup = registration.replaced_cleanup;
        let inbound_supersession_drained = registration.inbound_supersession_drained;
        if !registration.accepted {
            return TransportRegistration {
                unreliable,
                superseded,
                supersession_gate,
                transport_write_gate,
                superseding,
                replaced_cleanup,
                inbound_supersession_drained,
            };
        }
        after_publish();
        if let Some(replaced) = registration.replaced {
            // The registry synchronously fences receive admission before it can
            // defer cancellation behind an outbound flush. Do not release room
            // state until its inbound handoff gate has drained; the concrete
            // transport owns the deferred cleanup task. The uncontended fast
            // path is already drained and preserves direct callers' cleanup.
            if replaced_cleanup
                .as_ref()
                .is_some_and(ReplacedTransportCleanup::is_ready)
            {
                self.unregister_session(replaced.participant_id());
                replaced_cleanup = None;
            }
        }
        self.complete_gateway_registration(id, authenticated, authenticated_user.as_deref(), true);
        TransportRegistration {
            unreliable,
            superseded,
            supersession_gate,
            transport_write_gate,
            superseding,
            replaced_cleanup,
            inbound_supersession_drained,
        }
    }

    /// Start a bounded reconnect grace window for an exact active participant.
    /// The caller supplies server-minted opaque material and tears the transport
    /// down through this method; callers never choose another session id.
    pub fn begin_reconnect_grace(
        &self,
        id: ParticipantId,
        secret: ResumeSecret,
        requested_until: TimestampMillis,
    ) -> bool {
        self.registry
            .begin_reconnect_grace(id, secret, requested_until)
            .then(|| self.unregister_session(id))
            .is_some()
    }

    /// Deterministic bounded reconnect-grace transition. The registry caps the
    /// requested expiry from `now`, so tests and production callers use the same
    /// resource bound without relying on wall-clock timing.
    pub fn begin_reconnect_grace_at(
        &self,
        id: ParticipantId,
        secret: ResumeSecret,
        now: TimestampMillis,
        requested_until: TimestampMillis,
    ) -> bool {
        if !self
            .registry
            .begin_reconnect_grace_at(id, secret, now, requested_until)
        {
            return false;
        }
        self.unregister_session(id);
        true
    }

    /// Redeem an exact-session, one-use grace ticket after current
    /// authentication. This is server-internal lifecycle plumbing: no production
    /// transport handshake supplies a resume secret or advertises resume to
    /// version-1 clients. Do not expose it to clients without a versioned,
    /// cross-SDK handshake contract.
    pub fn resume_session_at(
        &self,
        handle: SessionHandle,
        secret: ResumeSecret,
        now: TimestampMillis,
    ) -> LatestOutboundReceiver {
        let id = handle.id;
        let authenticated = handle.is_authenticated();
        let authenticated_user = handle
            .identity
            .as_ref()
            .map(|identity| identity.user_id.as_str().to_owned());
        let registration = self.registry.resume_session_at(handle, secret, now);
        let unreliable = registration.unreliable;
        if !registration.accepted {
            return unreliable;
        }
        self.complete_gateway_registration(id, authenticated, authenticated_user.as_deref(), false);
        unreliable
    }

    /// Sweep grace windows at a supplied instant. Grace transitions do not leave
    /// a transport behind (it was cleaned during `begin_reconnect_grace`), so
    /// this returns the exact number of terminal session records reclaimed.
    pub fn expire_reconnect_grace_at(&self, now: TimestampMillis) -> usize {
        self.registry.expire_reconnect_grace_at(now).len()
    }

    /// Reclaim durable session-revocation tombstones after their authoritative
    /// access expiry. Production maintenance invokes this independently of a
    /// game runtime or socket activity.
    pub fn expire_revocation_tombstones_at(&self, now: TimestampMillis) -> usize {
        self.registry.expire_revocation_tombstones_at(now)
    }

    #[cfg(test)]
    pub(crate) fn reconnect_grace_count(&self) -> usize {
        self.registry.reconnect_grace_count()
    }

    #[cfg(test)]
    pub(crate) fn revocation_tombstone_count(&self) -> usize {
        self.registry.revocation_tombstone_count()
    }

    /// Whether a just-registered participant survived the durable-revocation
    /// publication barrier. Transports use this to tear down an accepted auth
    /// handshake that lost the registration race without emitting an ack.
    #[must_use]
    pub fn accepts_work(&self, id: ParticipantId) -> bool {
        self.registry.accepts_work(id)
    }

    fn send_rep_bootstrap(&self, id: ParticipantId) -> usize {
        let Some(rep) = &self.rep else {
            return 0;
        };
        // Keep the object binding alive from `bootstrap()` through the bounded
        // enqueue. A concurrent despawn can therefore produce either no frame
        // or a still-bound frame, never a stale frame that falls through to an
        // unscoped delivery path.
        let _scope = self.lock_room_scope();
        let mut sent = 0;
        for out in rep.bootstrap(id.get()) {
            let bytes = out.body.len() as u64;
            let outbound = Outbound::reliable(Envelope::new(out.kind, out.body));
            let delivered = match out.object_id {
                Some(object_id) => {
                    let binding = self.rep_rooms.lock().ok().map(|bindings| {
                        (
                            bindings.objects.get(&object_id).copied(),
                            bindings.connections.get(&id).copied(),
                        )
                    });
                    match binding {
                        Some((Some(room_id), Some(connection_room)))
                            if connection_room == room_id =>
                        {
                            self.rooms
                                .while_member_in(id, Some(room_id), || {
                                    self.registry.send_to(id, &outbound)
                                })
                                .unwrap_or(false)
                        }
                        // A room-bound receiver never accepts an object that
                        // lacks a trusted object binding. This fails closed for
                        // stale/despawned state and forbids match-id inference.
                        Some((None, None)) if self.rooms.room_of(id).is_none() => {
                            self.registry.send_to(id, &outbound)
                        }
                        _ => false,
                    }
                }
                None => self.registry.send_to(id, &outbound),
            };
            if delivered {
                self.metrics.record_message_out(bytes);
                sent += 1;
            }
        }
        sent
    }

    /// Send schema metadata without joining a replication match. This is the
    /// pre-admission registration path; it deliberately carries no state.
    fn send_rep_schema(&self, id: ParticipantId) -> usize {
        let Some(rep) = &self.rep else {
            return 0;
        };
        let Some(out) = rep.schema_bootstrap(id.get()) else {
            return 0;
        };
        let bytes = out.body.len() as u64;
        let outbound = Outbound::reliable(Envelope::new(out.kind, out.body));
        if self.registry.send_to(id, &outbound) {
            self.metrics.record_message_out(bytes);
            1
        } else {
            0
        }
    }

    /// Unregister a session on disconnect and drop the `sessions_active` gauge.
    ///
    /// The script's `citadel.on_leave` handler (if any) runs *before* the
    /// registry removal so its emitted commands still reach the remaining peers
    /// (the leaver is excluded), then the session is removed and the gauge drops.
    pub fn unregister_session(&self, id: ParticipantId) {
        if !self.registry.claim_cleanup(id) {
            return;
        }
        // A replacement/close can win after registry publication but before its
        // Gateway side effects begin. Only a generation that completed those
        // effects may emit Leave or decrement their paired gauges.
        if !self.registry.retire_gateway_registration(id) {
            let _ = self.registry.unregister(id);
            return;
        }
        // Freeze the diagnostics participant state before lifecycle hooks or
        // registry removal can make a disconnect look like a missing upload.
        self.diagnostics.disconnect(id);
        // Run the leave hook while the participant (and its identity) is still
        // registered, so `ctx.user_id` is available to the handler.
        self.dispatch_lifecycle(LifecycleHook::Leave, id);
        // Drop any transform-sync snapshot state for this participant, and free
        // any player object it owned (player-slot mode) so the id can be reused.
        if let Some(hub) = &self.transform {
            hub.release_player_slot(id.get());
            // Free any networked-actor presence and tell the remaining peers to
            // despawn its proxy.
            let peers = hub.presence_peers(id.get());
            if let Some(object_id) = hub.release_presence(id.get()) {
                let body = citadel_wire::na::NaDespawn { object_id }.encode();
                for peer in peers {
                    let peer_id = ParticipantId::from_raw(peer);
                    // Only same-room peers ever spawned this actor, so only they
                    // need the despawn (room dimension, ). Runs before
                    // `leave_room` below, so the leaver's room is still known.
                    let _ =
                        self.send_reliable_same_room(id, peer_id, KIND_NA_DESPAWN, body.clone());
                }
            }
            hub.unregister_client(id.get());
        }
        if self.matchmaker.cancel_owner(id, SystemClock.now()) {
            self.forget_ticket_owner_for_participant(id);
        }
        // Remove it from its room and notify the members that remain.
        self.leave_room(id);
        // Chat presence is local socket state, not durable history. Remove every
        // subscription before the transport sink disappears and announce leaves
        // only to the surviving authorized local subscribers.
        if let Some(domain) = &self.domain {
            for leave in domain.chat_presence.remove_participant(id) {
                if leave.remaining.is_empty()
                    && let Some(announcer) = &domain.chat_cluster_presence
                {
                    announcer.withdraw(&leave.channel_id);
                }
                let event = serde_json::json!({
                    "version": 1,
                    "type": "presence.leave",
                    "channel_id": &leave.channel_id,
                    "presence": {
                        "presence_id": leave.subscription.presence_id,
                        "user_id": leave.subscription.user_id,
                    },
                });
                domain.send_chat_event(
                    &self.registry,
                    &leave.channel_id,
                    &leave.remaining,
                    event,
                    0,
                );
            }
        }
        let affected_parties = self
            .party_presence
            .as_ref()
            .map(|presence| presence.local.parties_for_session(id.get()))
            .unwrap_or_default();
        let removed = self.registry.unregister(id);
        // Rebuild presence only after unregistering, so this socket cannot be
        // reintroduced by the registry snapshot used for fan-out.
        if let Some(parties) = &self.durable_parties {
            for party_id in affected_parties {
                let Ok(id) = PartyId::parse(&party_id) else {
                    continue;
                };
                let directory = Arc::clone(&parties.directory);
                if let Ok(snapshot) = party_block_on(async move { directory.snapshot(&id).await }) {
                    self.reconcile_party_presence(snapshot);
                }
            }
        }
        if let Some(removed) = removed {
            self.metrics.participant_closed();
            // Decrement the authenticated-session gauge exactly when it was
            // incremented (an account-bound participant left).
            if removed.is_authenticated() {
                self.metrics.session_closed();
            }
        }
    }

    /// Fence and clean up every local connection for one exact session.
    /// A routed caller supplies `expected_generation`, so an old owner cannot
    /// close a replacement connection after reconnect or migration.
    pub async fn disconnect_session(
        &self,
        session_id: &crate::session::SessionId,
        command_id: &str,
        expected_generation: Option<u64>,
        expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> usize {
        let closed = self
            .registry
            .close_session_at(session_id, command_id, expected_generation, expires_at, now)
            .await;
        let mut cleaned = 0;
        for (connection, disposition) in closed {
            if disposition == CloseDisposition::Closing {
                self.unregister_session(connection.participant_id());
                cleaned += 1;
            }
        }
        cleaned
    }

    /// Dispatch a lifecycle hook to the script runtime and apply its commands.
    ///
    /// A no-op when no runtime is attached. Emitted commands are applied with the
    /// participant excluded from broadcasts (lifecycle notifications target the
    /// *other* participants). Blocks briefly on the serialized Lua lock, bounded
    /// by the handler deadline, exactly like [`handle_inbound`](Gateway::handle_inbound).
    fn dispatch_lifecycle(&self, hook: LifecycleHook, id: ParticipantId) {
        if let Some(runtime) = &self.runtime {
            let room_id = {
                let _scope = self.lock_room_scope();
                self.rooms.room_of(id)
            };
            let user_id = self.registry.user_id_of(id);
            let commands = runtime.dispatch_lifecycle(hook, id.get(), user_id.as_deref());
            self.apply_commands_scoped(Some(id), room_id, commands);
        }
    }

    /// Dispatch one server-owned native match lifecycle callback. The room
    /// snapshot is copied before entering script code, so a handler cannot select
    /// or mutate its own match identity, membership, clock, or close reason.
    ///
    /// This is also the single funnel the durable match record is written from:
    /// every one of the gateway's firing sites reaches the recorder here, so a
    /// match cannot be created or closed without its row being queued. The
    /// observation is deliberately split around the script dispatch — the
    /// handler runs *inside* it, so the room must already be in the recorder's
    /// directory before `on_match_created` can write a match-scoped log line,
    /// and must still be there when `on_match_ended` returns.
    fn dispatch_match_lifecycle(
        &self,
        hook: NativeMatchLifecycleHook,
        room: RoomSnapshot,
        termination_reason: Option<MatchTerminationReason>,
        budget: std::time::Duration,
    ) {
        if self.reload_retiring.load(Ordering::Acquire) {
            return;
        }
        let Some(runtime) = &self.runtime else {
            return;
        };
        let (clock_epoch, tick) = self
            .transform
            .as_ref()
            .and_then(|hub| hub.gameplay_clock())
            .map(|clock| (clock.epoch, clock.tick))
            .unwrap_or((0, 0));
        let room_id = room.id;
        // Before the context is built: building it moves `room.label` out from
        // under the snapshot the recorder still needs whole.
        if let Some(recorder) = &self.matches {
            recorder.observe_before(
                hook,
                &room,
                termination_reason,
                clock_epoch,
                SystemClock.now().unix_millis(),
            );
        }
        let context = NativeMatchContext {
            match_id: room.id,
            lifecycle_generation: room
                .script_binding
                .as_ref()
                .map_or(0, |binding| binding.generation),
            clock_epoch,
            tick,
            participants: room.members.iter().map(|member| member.get()).collect(),
            map: room.label.map,
            mode: room.label.mode,
            max_players: room.label.max_players,
            open: termination_reason.is_none() && room.label.open,
            termination_reason: termination_reason.map(|reason| reason.as_str().to_owned()),
        };
        let _generation = self
            .generation_gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if room.bridge_mode == BridgeMode::Authoritative
            && (self.rooms.bridge_mode(room.id) != Some(BridgeMode::Authoritative)
                || self.rooms.binding(room.id) != room.script_binding)
        {
            return;
        }
        let commands = runtime.dispatch_match_lifecycle(hook, context, budget);
        self.apply_commands_scoped(None, Some(room_id), commands);
        if let Some(recorder) = &self.matches {
            recorder.observe_after(hook, room_id);
        }
    }

    /// Require the selected adapter to carry every server-owned native lifecycle
    /// callback before any room creation or admission mutates state. A missing
    /// runtime preserves relay-compatible rooms; any attached adapter must
    /// explicitly support the lifecycle surface.
    fn require_native_match_lifecycle(&self) -> Result<(), NativeMatchLifecycleUnavailable> {
        if !self.strict_script_rooms {
            return Ok(());
        }
        self.require_authoritative_match_lifecycle()
    }

    /// Require lifecycle support for a room that is explicitly authoritative,
    /// regardless of whether the node also permits relay rooms.
    fn require_authoritative_match_lifecycle(&self) -> Result<(), NativeMatchLifecycleUnavailable> {
        self.runtime.as_ref().map_or(Ok(()), |runtime| {
            runtime
                .supports_native_match_lifecycle()
                .then_some(())
                .ok_or(NativeMatchLifecycleUnavailable)
        })
    }

    fn native_match_budget(&self) -> std::time::Duration {
        self.runtime
            .as_ref()
            .map_or_else(|| std::time::Duration::ZERO, |runtime| runtime.budget())
    }

    fn room_snapshot_for_lifecycle(&self, room_id: RoomId) -> Option<RoomSnapshot> {
        self.rooms
            .snapshot()
            .into_iter()
            .find(|room| room.id == room_id)
    }

    fn dispatch_match_created(&self, room_id: RoomId) {
        if let Some(room) = self.room_snapshot_for_lifecycle(room_id) {
            self.dispatch_match_lifecycle(
                NativeMatchLifecycleHook::Created,
                room,
                None,
                self.native_match_budget(),
            );
        }
    }

    /// Dispatch the exact native transitions for one successful local admission.
    /// The room registry owns the membership mutation; this observes its before
    /// and after snapshots so protocol and trusted paths share idempotence and
    /// move semantics without exposing a second mutation API.
    fn dispatch_local_match_admission(
        &self,
        participant: ParticipantId,
        previous: Option<RoomSnapshot>,
        room_id: RoomId,
        created: bool,
    ) {
        if previous.as_ref().is_some_and(|room| room.id == room_id) {
            return;
        }
        let Some(current) = self.room_snapshot_for_lifecycle(room_id) else {
            return;
        };
        let budget = self.native_match_budget();
        if let Some(previous) = previous {
            let ended = self.room_snapshot_for_lifecycle(previous.id).is_none();
            self.forget_match_input_admission(previous.id, participant);
            if ended {
                self.drop_bridge_match(previous.id);
            }
            let mut leaving = self
                .room_snapshot_for_lifecycle(previous.id)
                .unwrap_or(previous);
            leaving.members.retain(|member| *member != participant);
            self.dispatch_match_lifecycle(
                NativeMatchLifecycleHook::Leave,
                leaving.clone(),
                None,
                budget,
            );
            if ended {
                self.dispatch_match_lifecycle(
                    NativeMatchLifecycleHook::Ended,
                    leaving,
                    Some(MatchTerminationReason::FinalDeparture),
                    budget,
                );
            }
        }
        if created {
            self.dispatch_match_lifecycle(
                NativeMatchLifecycleHook::Created,
                current.clone(),
                None,
                budget,
            );
        }
        if current.members.len() + current.remote_member_count == 1 {
            self.dispatch_match_lifecycle(
                NativeMatchLifecycleHook::Started,
                current.clone(),
                None,
                budget,
            );
        }
        self.dispatch_match_lifecycle(NativeMatchLifecycleHook::Join, current, None, budget);
    }

    /// The owner node observes trusted remote admission just as it observes a
    /// local admission. Remote member identities remain control-plane data, so
    /// the server-owned script context exposes only local participant ids.
    fn dispatch_remote_match_admission(
        &self,
        member: &RemoteRoomMember,
        previous: Option<RoomSnapshot>,
        room_id: RoomId,
    ) {
        if previous.as_ref().is_some_and(|room| room.id == room_id) {
            return;
        }
        let Some(current) = self.room_snapshot_for_lifecycle(room_id) else {
            return;
        };
        let budget = self.native_match_budget();
        if let Some(previous) = previous {
            let ended = self.room_snapshot_for_lifecycle(previous.id).is_none();
            let mut leaving = self
                .room_snapshot_for_lifecycle(previous.id)
                .unwrap_or(previous);
            leaving.remote_member_count = leaving.remote_member_count.saturating_sub(1);
            self.dispatch_match_lifecycle(
                NativeMatchLifecycleHook::Leave,
                leaving.clone(),
                None,
                budget,
            );
            if ended {
                self.dispatch_match_lifecycle(
                    NativeMatchLifecycleHook::Ended,
                    leaving,
                    Some(MatchTerminationReason::FinalDeparture),
                    budget,
                );
            }
        }
        if current.members.len() + current.remote_member_count == 1 {
            self.dispatch_match_lifecycle(
                NativeMatchLifecycleHook::Started,
                current.clone(),
                None,
                budget,
            );
        }
        self.dispatch_match_lifecycle(NativeMatchLifecycleHook::Join, current, None, budget);
        let _ = member;
    }

    /// Run one server game-loop tick and deliver its commands to all sessions.
    ///
    /// Invoked by the periodic tick task. `dt` is the nominal step and `budget`
    /// the per-tick time budget; both come from `runtime.tick_hz` /
    /// `runtime.tick_deadline_ms`. Commands broadcast to every session (a tick
    /// has no originating sender). Returns the number of sessions delivered to.
    /// A no-op returning 0 when no runtime is attached.
    pub fn tick(&self, dt: std::time::Duration, budget: std::time::Duration) -> usize {
        self.expire_revocation_tombstones_at(SystemClock.now());
        let Some(runtime) = &self.runtime else {
            return 0;
        };
        let _generation = self
            .generation_gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Preserve the global tick for server-wide work, then advance each live
        // match independently. A match command fan-outs only to that match's
        // current presence snapshot.
        let rooms = self.rooms.snapshot();
        // A process-global tick has no match identity. Once multiple matches
        // coexist, applying its arbitrary game commands would create an
        // unscoped client-facing path; per-room ticks below remain available.
        let mut delivered = if rooms.len() <= 1 {
            self.apply_commands(None, runtime.tick(dt, budget))
        } else {
            let _ = runtime.tick(dt, budget);
            0
        };
        for room in rooms {
            let room_id = room.id;
            self.dispatch_match_lifecycle(NativeMatchLifecycleHook::Tick, room, None, budget);
            let commands = runtime.tick_in_room(room_id, dt, budget);
            delivered += self.apply_commands_scoped(None, Some(room_id), commands);
        }
        delivered
    }

    /// Handle an inbound envelope from `sender`, routing it per its kind.
    ///
    /// Returns the number of peer sessions the message was relayed to (0 for
    /// dropped/unknown kinds), which is useful for tests and metrics.
    ///
    /// Every accepted envelope counts as one inbound message on the dashboard;
    /// each relayed copy counts as one outbound message so the `messages_*`
    /// gauges track real relay traffic.
    pub fn handle_inbound(&self, sender: ParticipantId, env: &Envelope) -> usize {
        self.handle_inbound_with_metadata(sender, env, InboundMessageMetadata::default())
    }

    /// Handle an inbound envelope with native transport metadata. The metadata
    /// is used only for the versioned custom-message bridge event; all legacy
    /// routes keep their existing envelope-only behavior.
    pub fn handle_inbound_with_metadata(
        &self,
        sender: ParticipantId,
        env: &Envelope,
        metadata: InboundMessageMetadata,
    ) -> usize {
        // The controller is the first application boundary after transport
        // framing. Once close linearizes, no late buffered frame may reach
        // runtime/domain handling or update inbound metrics.
        if !self.registry.accepts_work(sender) {
            tracing::debug!(%sender, kind = env.kind, "gateway dropped inbound after connection close");
            return 0;
        }
        self.metrics.record_message_in(env.body.len() as u64);
        // `KIND_AUTH`/`KIND_AUTH_RESULT` are handshake-only: a connection
        // authenticates exactly once, before registration. A
        // post-handshake auth frame — a stray/late frame, one batched behind a
        // legacy first frame, or a re-auth attempt — is reserved and dropped here,
        // never dispatched. This guarantees a token can never reach game logic
        // (e.g. a script that registered `on_message(KIND_AUTH, ...)`) and that a
        // live connection cannot silently rebind to another account.
        if env.kind == KIND_AUTH || env.kind == KIND_AUTH_RESULT {
            tracing::debug!(%sender, kind = env.kind, "gateway dropped a reserved post-handshake auth frame");
            return 0;
        }

        // Diagnostics controls are a trusted native protocol surface. They are
        // handled before runtime interception/dispatch and never fall through
        // to relay or GameScript handlers, including malformed and stale input.
        if self.handle_diagnostics_control(sender, env) {
            return 0;
        }

        // Realtime interception starts only after a transport has completed the
        // handshake and registered a participant. The reserved auth guard above
        // deliberately keeps credentials and re-auth attempts out of script code.
        let user_id = self.registry.user_id_of(sender);
        let room_id = self.rooms.room_of(sender);
        let runtime = self.runtime.as_deref();
        if env.kind == KIND_MATCH_INPUT_ACK {
            tracing::debug!(%sender, "gateway dropped client-sent match input acknowledgement");
            return 0;
        }
        // Explicit V1 match input is a reserved core route: it never reaches a
        // global interceptor, legacy on_message, or relay path. The envelope
        // body names only a sequence and opaque game bytes; membership, identity,
        // match and binding come from server registries.
        if env.kind == KIND_MATCH_INPUT {
            if !self
                .runtime
                .as_ref()
                .is_some_and(|runtime| runtime.supports_match_input_v1())
            {
                tracing::debug!(%sender, "gateway dropped match input for an adapter without V1 input parity");
                return 0;
            }
            let Some((authoritative_room, binding)) = self.authoritative_match(sender) else {
                return 0;
            };
            return self.route_bridge_match_input(
                sender,
                env,
                authoritative_room,
                &binding,
                metadata,
            );
        }
        // Generic custom traffic in a bound match must not traverse a global
        // runtime interceptor before membership/binding validation. The native
        // bridge is its first script boundary; protected/reserved kinds retain
        // their dedicated native routes below.
        if let Some((authoritative_room, binding)) = self.authoritative_match(sender)
            && env.kind > MAX_RESERVED_KIND
        {
            return self.route_bridge_match_message(
                sender,
                env,
                authoritative_room,
                &binding,
                metadata,
            );
        }
        if let Some(runtime) = runtime
            && runtime.before_realtime(
                sender.get(),
                user_id.as_deref(),
                room_id,
                env.kind,
                &env.body,
            ) == RealtimeInterception::Drop
        {
            runtime.after_realtime(
                sender.get(),
                user_id.as_deref(),
                room_id,
                env.kind,
                &env.body,
                RealtimeAfterOutcome {
                    dropped: true,
                    delivered: 0,
                },
            );
            return 0;
        }

        let delivered = if env.kind == KIND_RPC_REQUEST {
            self.handle_rpc_request(sender, &env.body)
        } else if env.kind == KIND_TSYNC_HELLO
            || env.kind == KIND_TSYNC_ACK
            || env.kind == KIND_TSYNC_INPUT
            || env.kind == KIND_TSYNC_V2_HELLO
            || env.kind == KIND_TSYNC_V2_INPUT
        {
            self.handle_transform_control(sender, env)
        } else if env.kind == KIND_NA_PRESENCE || env.kind == KIND_NA_STATE {
            self.handle_networked_actor(sender, env)
        } else if env.kind >= ROOM_KIND_MIN && env.kind <= ROOM_KIND_MAX {
            self.handle_room(sender, env)
        } else if env.kind == KIND_REP_DELTA || env.kind == KIND_REP_ACK {
            self.handle_rep_frame(sender, env)
        } else if let Some((room_id, binding)) = self.authoritative_match(sender) {
            // Custom envelopes inside a bound match never reach legacy
            // on_message. The server-derived room/binding and the ledger's
            // clock/tick fences are installed before on_input runs; a stale,
            // moved, non-member, oversized, or otherwise foreign frame is
            // dropped before runtime invocation.
            if env.kind <= MAX_RESERVED_KIND {
                tracing::debug!(
                    %sender,
                    kind = env.kind,
                    "gateway dropped reserved frame on the custom-message bridge path"
                );
                0
            } else {
                self.route_bridge_match_message(sender, env, room_id, &binding, metadata)
            }
        } else if let Some(runtime) = runtime {
            let user_id = self.registry.user_id_of(sender);
            let commands = match room_id {
                Some(room_id) => runtime.dispatch_in_room(
                    sender.get(),
                    user_id.as_deref(),
                    room_id,
                    env.kind,
                    &env.body,
                ),
                None => runtime.dispatch(sender.get(), user_id.as_deref(), env.kind, &env.body),
            };
            self.apply_commands_scoped(Some(sender), room_id, commands)
        } else {
            self.relay_builtin(sender, env)
        };

        if let Some(runtime) = runtime {
            runtime.after_realtime(
                sender.get(),
                user_id.as_deref(),
                room_id,
                env.kind,
                &env.body,
                RealtimeAfterOutcome {
                    dropped: false,
                    delivered,
                },
            );
        }
        delivered
    }

    /// Handle all diagnostics kinds before any script/runtime path. The return
    /// value means the kind was reserved (even if the particular body failed
    /// validation), so no untrusted diagnostics bytes become gameplay input.
    fn handle_diagnostics_control(&self, sender: ParticipantId, env: &Envelope) -> bool {
        match env.kind {
            KIND_DIAG_CAPABILITIES => {
                let accepted = self.registry.is_authenticated(sender)
                    && Capabilities::decode(&env.body)
                        .ok()
                        .is_some_and(|capabilities| {
                            self.diagnostics.accept_capabilities(sender, capabilities)
                        });
                tracing::debug!(%sender, accepted, "processed diagnostics capability assertion");
                true
            }
            KIND_DIAG_CLOCK_SYNC => {
                let Ok(request) = ClockSync::decode(&env.body) else {
                    tracing::debug!(%sender, "dropped malformed diagnostics clock probe");
                    return true;
                };
                if !self.registry.is_authenticated(sender)
                    || !self.diagnostics.accepts_clock_sync(sender)
                {
                    tracing::debug!(%sender, "dropped unauthorised diagnostics clock probe");
                    return true;
                }
                let received_utc_us = SystemClock::now_utc_micros();
                let sent_utc_us = SystemClock::now_utc_micros();
                if let Some(response) =
                    LagCaptureManager::reply_clock_sync(request, received_utc_us, sent_utc_us)
                    && let Ok(body) = response.encode()
                {
                    let _ = self.send_reliable(sender, KIND_DIAG_CLOCK_SYNC, body);
                }
                true
            }
            KIND_DIAG_STATUS => {
                let result = if self.registry.is_authenticated(sender) {
                    CaptureStatus::decode(&env.body)
                        .map_err(|_| LagCaptureError::InvalidRequest)
                        .and_then(|status| self.diagnostics.apply_status(sender, status))
                } else {
                    Err(LagCaptureError::NotCapable)
                };
                if result.is_ok() {
                    let _ = self.diagnostics.finish_if_terminal();
                }
                tracing::debug!(%sender, accepted = result.is_ok(), "processed diagnostics capture status");
                true
            }
            // These are server-to-client-only kinds. A client echo, replay, or
            // forged control is reserved and dropped before runtime dispatch.
            KIND_DIAG_SERVER_TIME | KIND_DIAG_START | KIND_DIAG_FLUSH => true,
            _ => false,
        }
    }

    /// Handle a `KIND_RPC_REQUEST`: run the runtime RPC handler and reply to the
    /// caller only, correlated by `request_id`.
    ///
    /// The request/response path is distinct from the fire-and-forget relay: it
    /// decodes the request, invokes [`Runtime::call_rpc`], and sends exactly
    /// one `KIND_RPC_RESPONSE` back to the originating session via
    /// [`SessionRegistry::send_to`] — never a broadcast, so a reply can only ever
    /// reach its caller. A malformed request (undecodable header) is dropped and
    /// logged: without a trustworthy `request_id` there is nothing to correlate.
    /// An unknown method, a handler error, or a blown deadline all produce a
    /// well-formed `status != 0` response. Returns 1 if a response was queued to
    /// the caller, 0 otherwise (malformed request, or the caller already
    /// disconnected). RPC responses are sent reliably.
    fn handle_rpc_request(&self, sender: ParticipantId, body: &[u8]) -> usize {
        let Some(request) = protocol::decode_rpc_request(body) else {
            tracing::debug!(%sender, "gateway dropped a malformed RPC request");
            return 0;
        };
        if request.method.starts_with("matchmaker.") {
            return self.handle_matchmaker_rpc(
                sender,
                request.request_id,
                request.method,
                request.payload,
            );
        }
        if request.method.starts_with("party.") {
            return self.handle_party_rpc(
                sender,
                request.request_id,
                request.method,
                request.payload,
            );
        }
        // Reserved domain-feature methods (`friends.*`, …) are answered by the
        // server's persisted services asynchronously, off this synchronous relay
        // path. The reply is unicast and correlated by request_id, so
        // out-of-order completion is harmless. Anything else falls through to the
        // script runtime below.
        if let Some(domain) = &self.domain
            && is_domain_rpc_method(request.method)
        {
            return self.spawn_domain_rpc(sender, domain.clone(), &request);
        }
        let (status, payload) = match &self.runtime {
            Some(runtime) => {
                let user_id = self.registry.user_id_of(sender);
                match runtime.call_rpc(
                    sender.get(),
                    user_id.as_deref(),
                    request.method,
                    request.payload,
                ) {
                    RpcOutcome::Ok(reply) => (protocol::RPC_STATUS_OK, reply),
                    RpcOutcome::Err(message) => (protocol::RPC_STATUS_ERROR, message.into_bytes()),
                }
            }
            // No script runtime is attached: RPC is unavailable, but still reply
            // (correlated) so the caller is not left waiting.
            None => (
                protocol::RPC_STATUS_ERROR,
                b"RPC runtime not available".to_vec(),
            ),
        };
        let response = Envelope::new(
            KIND_RPC_RESPONSE,
            protocol::encode_rpc_response(request.request_id, status, &payload),
        );
        let out_bytes = response.body.len() as u64;
        // Request/response is reliable; the reply goes to the caller only.
        let outbound = Outbound::reliable(response);
        if self.registry.send_to(sender, &outbound) {
            self.metrics.record_message_out(out_bytes);
            1
        } else {
            tracing::debug!(%sender, "RPC caller disconnected before its response was delivered");
            0
        }
    }

    /// Handle the authenticated, process-local party RPC surface. Party state is
    /// deliberately account-owned; matchmaker queueing resolves its members to
    /// active participants only when the leader submits a ticket.
    fn handle_party_rpc(
        &self,
        sender: ParticipantId,
        request_id: u64,
        method: &str,
        payload: &[u8],
    ) -> usize {
        let Some(user_id) = self.registry.user_id_of(sender) else {
            return self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                b"authentication required",
            );
        };
        if self.durable_parties.is_some() {
            return self.handle_durable_party_rpc(sender, request_id, method, payload, user_id);
        }
        let result: Result<serde_json::Value, String> = match method {
            "party.create" => self
                .parties
                .create(&user_id)
                .map(party_json)
                .map_err(|error| error.to_string()),
            "party.invite" => {
                let id = party_id_arg(payload);
                let target = target_user_id_arg(payload);
                match (id, target) {
                    (Ok(id), Ok(target)) => {
                        self.ensure_party_mutable(&user_id, &id).and_then(|()| {
                            self.parties
                                .invite(&user_id, &id, &target)
                                .map(party_json)
                                .map_err(|error| error.to_string())
                        })
                    }
                    (Err(error), _) | (_, Err(error)) => Err(error),
                }
            }
            "party.accept" => match party_id_arg(payload) {
                Ok(id) => self.ensure_party_mutable(&user_id, &id).and_then(|()| {
                    self.parties
                        .accept(&user_id, &id)
                        .map(party_json)
                        .map_err(|error| error.to_string())
                }),
                Err(error) => Err(error),
            },
            "party.leave" => match party_id_arg(payload) {
                Ok(id) => self.ensure_party_mutable(&user_id, &id).and_then(|()| {
                    self.parties
                        .leave(&user_id, &id)
                        .map(|()| serde_json::json!({ "left": true }))
                        .map_err(|error| error.to_string())
                }),
                Err(error) => Err(error),
            },
            "party.promote" => {
                let id = party_id_arg(payload);
                let target = target_user_id_arg(payload);
                match (id, target) {
                    (Ok(id), Ok(target)) => {
                        self.ensure_party_mutable(&user_id, &id).and_then(|()| {
                            self.parties
                                .promote(&user_id, &id, &target)
                                .map(party_json)
                                .map_err(|error| error.to_string())
                        })
                    }
                    (Err(error), _) | (_, Err(error)) => Err(error),
                }
            }
            "party.remove" => {
                let id = party_id_arg(payload);
                let target = target_user_id_arg(payload);
                match (id, target) {
                    (Ok(id), Ok(target)) => {
                        self.ensure_party_mutable(&user_id, &id).and_then(|()| {
                            self.parties
                                .remove(&user_id, &id, &target)
                                .map(party_json)
                                .map_err(|error| error.to_string())
                        })
                    }
                    (Err(error), _) | (_, Err(error)) => Err(error),
                }
            }
            "party.close" => match party_id_arg(payload) {
                Ok(id) => self.ensure_party_mutable(&user_id, &id).and_then(|()| {
                    self.parties
                        .close(&user_id, &id)
                        .map(|()| serde_json::json!({ "closed": true }))
                        .map_err(|error| error.to_string())
                }),
                Err(error) => Err(error),
            },
            "party.status" => match party_id_arg(payload) {
                Ok(id) => self
                    .parties
                    .snapshot_for(&user_id, &id)
                    .map(party_json)
                    .map_err(|error| error.to_string()),
                Err(error) => Err(error),
            },
            other => Err(format!("unknown party method: {other}")),
        };
        match result {
            Ok(body) => self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_OK,
                body.to_string().as_bytes(),
            ),
            Err(error) => self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                error.as_bytes(),
            ),
        }
    }

    fn handle_durable_party_rpc(
        &self,
        sender: ParticipantId,
        request_id: u64,
        method: &str,
        payload: &[u8],
        user_id: String,
    ) -> usize {
        let Some(parties) = &self.durable_parties else {
            unreachable!()
        };
        let now = SystemClock.now();
        let result: Result<serde_json::Value, String> = if method == "party.create" {
            let create_request_id = request_id.to_string();
            let id = PartyId::generate().map_err(|error| error.to_string());
            id.and_then(|id| {
                let expires = now
                    .checked_add(DurationMillis::from_millis(PARTY_OWNER_LEASE_MS))
                    .map_err(|error| error.to_string())?;
                let directory = Arc::clone(&parties.directory);
                let node = parties.node_id.clone();
                party_block_on(async move {
                    directory
                        .create_with_request(
                            id,
                            &user_id,
                            Some(&create_request_id),
                            node,
                            expires,
                            now,
                        )
                        .await
                })
                .map(|(snapshot, _)| {
                    self.metrics.record_party_owner_lease_acquire();
                    self.reconcile_party_presence(snapshot.clone());
                    party_json(snapshot)
                })
                .map_err(|error| error.to_string())
            })
        } else if method == "party.status" {
            party_id_arg(payload).and_then(|id| {
                let directory = Arc::clone(&parties.directory);
                party_block_on(async move { directory.snapshot_for(&user_id, &id).await })
                    .map(party_json)
                    .map_err(|error| error.to_string())
            })
        } else {
            self.durable_party_mutation(parties, method, payload, &user_id, request_id, now)
                .map(|snapshot| match method {
                    "party.leave" => {
                        serde_json::json!({ "left": true, "revision": snapshot.revision })
                    }
                    "party.close" => {
                        serde_json::json!({ "closed": true, "revision": snapshot.revision })
                    }
                    _ => party_json(snapshot),
                })
        };
        match result {
            Ok(body) => self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_OK,
                body.to_string().as_bytes(),
            ),
            Err(error) => self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                error.as_bytes(),
            ),
        }
    }

    fn durable_party_mutation(
        &self,
        parties: &DurablePartyGateway,
        method: &str,
        payload: &[u8],
        actor: &str,
        request_id: u64,
        now: TimestampMillis,
    ) -> Result<PartySnapshot, String> {
        let party_id = party_id_arg(payload)?;
        // Ticket safety is enforced by the aggregate's durable queue freeze.
        // Do not consult this Gateway's local matchmaker cache: the ticket
        // shard may be remote and such a check would race an admitted ticket.
        let operation = match method {
            "party.invite" => PartyControlOperation::Invite {
                target: target_user_id_arg(payload)?,
            },
            "party.accept" => PartyControlOperation::Accept,
            "party.leave" => PartyControlOperation::Leave,
            "party.promote" => PartyControlOperation::Promote {
                target: target_user_id_arg(payload)?,
            },
            "party.remove" => PartyControlOperation::Remove {
                target: target_user_id_arg(payload)?,
            },
            "party.close" => PartyControlOperation::Close,
            other => return Err(format!("unknown party method: {other}")),
        };
        let expires = now
            .checked_add(DurationMillis::from_millis(PARTY_OWNER_LEASE_MS))
            .map_err(|e| e.to_string())?;
        // Older clients do not send a revision. Preserve that RPC contract by
        // taking a fresh durable snapshot before applying the fenced command;
        // newer callers can supply a revision for explicit optimistic CAS.
        let expected_revision = match expected_party_revision_arg(payload)? {
            Some(revision) => revision,
            None => {
                let directory = Arc::clone(&parties.directory);
                let snapshot_id = party_id.clone();
                party_block_on(async move { directory.snapshot(&snapshot_id).await })
                    .map_err(|error| error.to_string())?
                    .revision
            }
        };
        let directory = Arc::clone(&parties.directory);
        let node = parties.node_id.clone();
        let resolving_party_id = party_id.clone();
        let resolution = party_block_on(async move {
            directory
                .acquire_or_resolve(&resolving_party_id, node, expires, now)
                .await
        })
        .map_err(|error| error.to_string())?;
        let lease = match resolution {
            PartyOwnerResolution::Local(lease) => lease,
            PartyOwnerResolution::Remote(lease) => {
                self.metrics.record_party_owner_forward();
                let command = PartyControlCommand {
                    party_id,
                    lease: lease.clone(),
                    actor: actor.to_owned(),
                    request_id: request_id.to_string(),
                    expected_revision,
                    operation,
                };
                let result = match parties.router.party_command(&lease.owner_node, command) {
                    Ok(PartyControlReply::Snapshot(snapshot, reply_lease))
                        if reply_lease == lease =>
                    {
                        Ok(snapshot)
                    }
                    Ok(PartyControlReply::Snapshot(_, _)) => Err(
                        "party owner reply fence mismatched; retry with a fresh snapshot"
                            .to_owned(),
                    ),
                    Ok(PartyControlReply::StaleOwnerFence) => {
                        self.metrics.record_party_owner_stale_reject();
                        Err("party owner fence is stale; retry with a fresh snapshot".to_owned())
                    }
                    Ok(PartyControlReply::QueueAdmission(_))
                    | Ok(PartyControlReply::Rejected)
                    | Err(_) => Err("party owner is unavailable".to_owned()),
                };
                if let Ok(snapshot) = &result
                    && method != "party.close"
                {
                    self.reconcile_party_presence(snapshot.clone());
                }
                return result;
            }
        };
        if !self.recover_durable_party_owner(parties, &lease, now)? {
            // Generation did not advance, so acquire_or_resolve renewed the
            // existing local owner fence rather than recovering a takeover.
            self.metrics.record_party_owner_lease_renew();
        }
        let command = PartyControlCommand {
            party_id: party_id.clone(),
            lease,
            actor: actor.to_owned(),
            request_id: request_id.to_string(),
            expected_revision,
            operation,
        };
        let expected_reply_lease = command.lease.clone();
        let result = match self.apply_remote_party_command(command) {
            PartyControlReply::Snapshot(snapshot, reply_lease)
                if reply_lease == expected_reply_lease =>
            {
                Ok(snapshot)
            }
            PartyControlReply::StaleOwnerFence => {
                self.metrics.record_party_owner_stale_reject();
                Err("party owner fence is stale; retry with a fresh snapshot".to_owned())
            }
            PartyControlReply::Snapshot(_, _)
            | PartyControlReply::QueueAdmission(_)
            | PartyControlReply::Rejected => Err("party command rejected".to_owned()),
        };
        if let Ok(snapshot) = &result
            && method != "party.close"
        {
            self.reconcile_party_presence(snapshot.clone());
        }
        result
    }

    /// Finish a successful higher-generation owner takeover before allowing a
    /// mutation. The durable directory is the source of both the snapshot and
    /// the one-per-generation resync claim; node-local presence is rebuilt only
    /// from that committed membership state.
    fn recover_durable_party_owner(
        &self,
        parties: &DurablePartyGateway,
        lease: &crate::services::party_directory::PartyOwnerLease,
        now: TimestampMillis,
    ) -> Result<bool, String> {
        let directory = Arc::clone(&parties.directory);
        let recovery_lease = lease.clone();
        let recovered =
            party_block_on(
                async move { directory.claim_failover_resync(&recovery_lease, now).await },
            )
            .map_err(|error| error.to_string())?;
        let Some(snapshot) = recovered else {
            return Ok(false);
        };
        let party_revision = snapshot.revision;

        // The control route is installed before listeners bind; replaying this
        // path never changes routing. Reconcile the committed snapshot before
        // publishing the recoverable client barrier and opening mutations.
        self.reconcile_party_presence(snapshot.clone());
        self.emit_party_failover_resync(snapshot, lease);
        self.metrics.record_party_owner_lease_acquire();
        self.metrics.record_party_owner_failover();
        self.metrics.record_party_resync();
        tracing::info!(
            owner_generation = lease.generation.get(),
            party_revision,
            "party owner failover recovery completed"
        );
        Ok(true)
    }

    /// Notify only current party members that their next authoritative view is
    /// a recovery snapshot. The payload deliberately contains no node address,
    /// token, request body, or raw presence/user-list audit data; the snapshot
    /// is sent only through each member's authenticated local session.
    fn emit_party_failover_resync(
        &self,
        snapshot: PartySnapshot,
        lease: &crate::services::party_directory::PartyOwnerLease,
    ) {
        let resync = serde_json::json!({
            "type": "party.resync_required",
            "party_id": snapshot.party_id.as_str(),
            "party_revision": snapshot.revision,
            "generation": lease.generation.get(),
            "reason": "owner_failover",
        })
        .to_string()
        .into_bytes();
        let state = serde_json::json!({
            "type": "party.snapshot",
            "party_id": snapshot.party_id.as_str(),
            "leader_user_id": snapshot.leader_user_id,
            "members": snapshot.members,
            "invitations": snapshot.invitations,
            "revision": snapshot.revision,
            "generation": lease.generation.get(),
        })
        .to_string()
        .into_bytes();
        for member in &snapshot.members {
            for recipient in self.registry.participants_for_user(member) {
                let _ = self.send_reliable(recipient, KIND_NOTIFICATION, resync.clone());
                let _ = self.send_reliable(recipient, KIND_NOTIFICATION, state.clone());
            }
        }
    }

    fn apply_remote_party_command(&self, command: PartyControlCommand) -> PartyControlReply {
        let Some(parties) = &self.durable_parties else {
            return PartyControlReply::Rejected;
        };
        // The transport authenticates the sending node, while this check binds
        // the delivered command to the destination owner's local identity.
        // Never allow a node to apply another node's lease merely because it
        // can read the shared storage.
        if parties.node_id != command.lease.owner_node || command.party_id != command.lease.party_id
        {
            self.metrics.record_party_owner_stale_reject();
            return PartyControlReply::StaleOwnerFence;
        }
        if let Err(error) =
            self.recover_durable_party_owner(parties, &command.lease, SystemClock.now())
        {
            if error.contains("fence") {
                self.metrics.record_party_owner_stale_reject();
                return PartyControlReply::StaleOwnerFence;
            }
            return PartyControlReply::Rejected;
        }
        let directory = Arc::clone(&parties.directory);
        let lease = command.lease.clone();
        let actor = command.actor.clone();
        let request_id = command.request_id.clone();
        if let PartyControlOperation::ReleaseQueueAdmission { admission } = &command.operation {
            let freeze = PartyQueueFreeze {
                revision: admission.revision,
                owner_generation: admission.owner_generation,
                admission_generation: admission.admission_generation,
                admission_token: admission.admission_token,
            };
            let release_actor = actor.clone();
            return match party_block_on(async move {
                directory
                    .release_queue_freeze(&lease, &release_actor, &freeze, SystemClock.now())
                    .await
            }) {
                Ok(()) => PartyControlReply::Snapshot(
                    // Release has no public state result; a fence-bound snapshot
                    // merely acknowledges that this exact cleanup was applied.
                    PartySnapshot {
                        party_id: command.party_id,
                        leader_user_id: actor,
                        members: vec![],
                        invitations: vec![],
                        revision: command.expected_revision,
                    },
                    command.lease,
                ),
                Err(error) if error.to_string().contains("fence") => {
                    PartyControlReply::StaleOwnerFence
                }
                Err(_) => PartyControlReply::Rejected,
            };
        }
        if let PartyControlOperation::RenewQueueAdmission {
            admission,
            ticket_expires_at,
        } = &command.operation
        {
            let freeze = PartyQueueFreeze {
                revision: admission.revision,
                owner_generation: admission.owner_generation,
                admission_generation: admission.admission_generation,
                admission_token: admission.admission_token,
            };
            let renewal_actor = actor.clone();
            let ticket_expires_at = *ticket_expires_at;
            return match party_block_on(async move {
                directory
                    .renew_queue_freeze(
                        &lease,
                        &renewal_actor,
                        &freeze,
                        ticket_expires_at,
                        SystemClock.now(),
                    )
                    .await
            }) {
                Ok(()) => PartyControlReply::Snapshot(
                    PartySnapshot {
                        party_id: command.party_id,
                        leader_user_id: actor,
                        members: vec![],
                        invitations: vec![],
                        revision: command.expected_revision,
                    },
                    command.lease,
                ),
                Err(error) if error.to_string().contains("fence") => {
                    PartyControlReply::StaleOwnerFence
                }
                Err(_) => PartyControlReply::Rejected,
            };
        }
        if let PartyControlOperation::QueueAdmission { ticket_expires_at } = &command.operation {
            let ticket_expires_at = *ticket_expires_at;
            return match party_block_on(async move {
                directory
                    .queue_snapshot(
                        &lease,
                        &actor,
                        command.expected_revision,
                        ticket_expires_at,
                        SystemClock.now(),
                    )
                    .await
            }) {
                Ok((members, freeze)) => PartyControlReply::QueueAdmission(PartyQueueAdmission {
                    members,
                    revision: freeze.revision,
                    lease: command.lease,
                    admission_generation: freeze.admission_generation,
                    admission_token: freeze.admission_token,
                }),
                Err(error) if error.to_string().contains("fence") => {
                    PartyControlReply::StaleOwnerFence
                }
                Err(_) => PartyControlReply::Rejected,
            };
        }
        let result = party_block_on(async move {
            match command.operation {
                PartyControlOperation::Invite { target } => {
                    directory
                        .invite(
                            &lease,
                            &actor,
                            &request_id,
                            &target,
                            command.expected_revision,
                            SystemClock.now(),
                        )
                        .await
                }
                PartyControlOperation::Accept => {
                    directory
                        .accept(
                            &lease,
                            &actor,
                            &request_id,
                            command.expected_revision,
                            SystemClock.now(),
                        )
                        .await
                }
                PartyControlOperation::Leave => {
                    directory
                        .leave(
                            &lease,
                            &actor,
                            &request_id,
                            command.expected_revision,
                            SystemClock.now(),
                        )
                        .await
                }
                PartyControlOperation::Promote { target } => {
                    directory
                        .promote(
                            &lease,
                            &actor,
                            &request_id,
                            &target,
                            command.expected_revision,
                            SystemClock.now(),
                        )
                        .await
                }
                PartyControlOperation::Remove { target } => {
                    directory
                        .remove(
                            &lease,
                            &actor,
                            &request_id,
                            &target,
                            command.expected_revision,
                            SystemClock.now(),
                        )
                        .await
                }
                PartyControlOperation::Close => {
                    directory
                        .close(
                            &lease,
                            &actor,
                            &request_id,
                            command.expected_revision,
                            SystemClock.now(),
                        )
                        .await
                }
                PartyControlOperation::QueueAdmission { .. }
                | PartyControlOperation::RenewQueueAdmission { .. }
                | PartyControlOperation::ReleaseQueueAdmission { .. } => {
                    unreachable!("handled above")
                }
            }
        });
        match result {
            Ok(snapshot) => PartyControlReply::Snapshot(snapshot, command.lease),
            Err(error) if error.to_string().contains("fence") => PartyControlReply::StaleOwnerFence,
            Err(_) => PartyControlReply::Rejected,
        }
    }

    /// Take the member list from the durable aggregate under its current owner
    /// fence.  Unlike `PartyRegistry::queue_members`, this path is shared by
    /// every gateway and validates one committed revision immediately before a
    /// ticket is admitted.
    fn durable_party_queue_snapshot(
        &self,
        parties: &DurablePartyGateway,
        party_id: PartyId,
        actor: &str,
        ticket_expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> Result<(Vec<String>, PartyAdmissionFence), String> {
        let expires = now
            .checked_add(DurationMillis::from_millis(PARTY_OWNER_LEASE_MS))
            .map_err(|error| error.to_string())?;
        let directory = Arc::clone(&parties.directory);
        let node = parties.node_id.clone();
        let resolving_id = party_id.clone();
        let resolution = party_block_on(async move {
            directory
                .acquire_or_resolve(&resolving_id, node, expires, now)
                .await
        })
        .map_err(|error| error.to_string())?;
        let lease = match resolution {
            PartyOwnerResolution::Local(lease) | PartyOwnerResolution::Remote(lease) => lease,
        };
        let snapshot_directory = Arc::clone(&parties.directory);
        let id = party_id.clone();
        let revision = party_block_on(async move { snapshot_directory.snapshot(&id).await })
            .map_err(|error| error.to_string())?
            .revision;
        let command = PartyControlCommand {
            party_id: party_id.clone(),
            lease: lease.clone(),
            actor: actor.to_owned(),
            request_id: format!("queue-admission-{revision}"),
            expected_revision: revision,
            operation: PartyControlOperation::QueueAdmission { ticket_expires_at },
        };
        let reply = if lease.owner_node == parties.node_id {
            self.apply_remote_party_command(command)
        } else {
            parties
                .router
                .party_command(&lease.owner_node, command)
                .map_err(|_| "party owner is unavailable".to_owned())?
        };
        match reply {
            PartyControlReply::QueueAdmission(admission)
                if admission.lease == lease && admission.lease.owner_node == lease.owner_node =>
            {
                Ok((
                    admission.members,
                    PartyAdmissionFence {
                        party_id: party_id.as_str().to_owned(),
                        leader_user_id: actor.to_owned(),
                        revision: admission.revision,
                        owner_generation: lease.generation.get(),
                        admission_generation: admission.admission_generation,
                        admission_token: admission.admission_token,
                    },
                ))
            }
            PartyControlReply::StaleOwnerFence => {
                Err("party owner fence is stale; retry with a fresh snapshot".to_owned())
            }
            PartyControlReply::QueueAdmission(_)
            | PartyControlReply::Snapshot(_, _)
            | PartyControlReply::Rejected => Err("party owner rejected queue admission".to_owned()),
        }
    }

    /// Route cleanup through the *currently* resolved party owner.  The
    /// admission's original owner generation/token are payload fences, while
    /// the command lease proves which owner is allowed to serialize cleanup.
    fn release_durable_party_admission(
        &self,
        admission: &PartyAdmissionFence,
    ) -> Result<(), String> {
        let parties = self
            .durable_parties
            .as_ref()
            .ok_or_else(|| "durable party state unavailable".to_owned())?;
        let party_id = PartyId::parse(&admission.party_id)
            .map_err(|_| "invalid party admission".to_owned())?;
        let now = SystemClock.now();
        let expires = now
            .checked_add(DurationMillis::from_millis(PARTY_OWNER_LEASE_MS))
            .map_err(|e| e.to_string())?;
        let directory = Arc::clone(&parties.directory);
        let node = parties.node_id.clone();
        let resolving = party_id.clone();
        let resolution = party_block_on(async move {
            directory
                .acquire_or_resolve(&resolving, node, expires, now)
                .await
        })
        .map_err(|e| e.to_string())?;
        let lease = match resolution {
            PartyOwnerResolution::Local(lease) | PartyOwnerResolution::Remote(lease) => lease,
        };
        let command = PartyControlCommand {
            party_id,
            lease: lease.clone(),
            actor: admission.leader_user_id.clone(),
            request_id: format!(
                "queue-release-{}-{}",
                admission.admission_generation, admission.admission_token
            ),
            expected_revision: admission.revision,
            operation: PartyControlOperation::ReleaseQueueAdmission {
                admission: admission.clone(),
            },
        };
        let reply = if lease.owner_node == parties.node_id {
            self.apply_remote_party_command(command)
        } else {
            parties
                .router
                .party_command(&lease.owner_node, command)
                .map_err(|_| "party owner is unavailable".to_owned())?
        };
        match reply {
            PartyControlReply::Snapshot(_, reply_lease) if reply_lease == lease => Ok(()),
            PartyControlReply::StaleOwnerFence => {
                Err("party owner fence is stale; retry".to_owned())
            }
            PartyControlReply::Snapshot(_, _)
            | PartyControlReply::QueueAdmission(_)
            | PartyControlReply::Rejected => {
                Err("party owner rejected admission cleanup".to_owned())
            }
        }
    }

    /// Route exact-fenced admission renewal to the current party owner. This
    /// is called by the authoritative matchmaker shard immediately before it
    /// creates the live ticket, so persisted expiry and ticket expiry share
    /// one timestamp.
    pub(crate) fn live_matchmaker_renew_party_admission(
        &self,
        admission: &PartyAdmissionFence,
        ticket_expires_at: TimestampMillis,
    ) -> Result<(), String> {
        let parties = self
            .durable_parties
            .as_ref()
            .ok_or_else(|| "durable party state unavailable".to_owned())?;
        let party_id = PartyId::parse(&admission.party_id)
            .map_err(|_| "invalid party admission".to_owned())?;
        let now = SystemClock.now();
        let expires = now
            .checked_add(DurationMillis::from_millis(PARTY_OWNER_LEASE_MS))
            .map_err(|e| e.to_string())?;
        let directory = Arc::clone(&parties.directory);
        let node = parties.node_id.clone();
        let resolving = party_id.clone();
        let resolution = party_block_on(async move {
            directory
                .acquire_or_resolve(&resolving, node, expires, now)
                .await
        })
        .map_err(|e| e.to_string())?;
        let lease = match resolution {
            PartyOwnerResolution::Local(lease) | PartyOwnerResolution::Remote(lease) => lease,
        };
        let command = PartyControlCommand {
            party_id,
            lease: lease.clone(),
            actor: admission.leader_user_id.clone(),
            request_id: format!(
                "queue-renew-{}-{}",
                admission.admission_generation, admission.admission_token
            ),
            expected_revision: admission.revision,
            operation: PartyControlOperation::RenewQueueAdmission {
                admission: admission.clone(),
                ticket_expires_at,
            },
        };
        let reply = if lease.owner_node == parties.node_id {
            self.apply_remote_party_command(command)
        } else {
            parties
                .router
                .party_command(&lease.owner_node, command)
                .map_err(|_| "party owner is unavailable".to_owned())?
        };
        match reply {
            PartyControlReply::Snapshot(_, reply_lease) if reply_lease == lease => Ok(()),
            PartyControlReply::StaleOwnerFence => {
                Err("party owner fence is stale; retry".to_owned())
            }
            PartyControlReply::Snapshot(_, _)
            | PartyControlReply::QueueAdmission(_)
            | PartyControlReply::Rejected => {
                Err("party owner rejected admission renewal".to_owned())
            }
        }
    }

    fn ensure_party_mutable(&self, requester_user_id: &str, id: &PartyId) -> Result<(), String> {
        let party = self
            .parties
            .snapshot_for(requester_user_id, id)
            .map_err(|error| error.to_string())?;
        let Some(leader) = self.registry.participant_for_user(&party.leader_user_id) else {
            return Ok(());
        };
        let live_queued = self
            .live_matchmaker
            .as_ref()
            .is_some_and(|matchmaker| matchmaker.has_queued_ticket_for_user(&party.leader_user_id));
        if live_queued || self.matchmaker.has_queued_ticket(leader, SystemClock.now()) {
            return Err("party is queued; cancel its matchmaker ticket first".to_owned());
        }
        Ok(())
    }

    /// Handle the local ticket-matchmaker RPC surface. These methods require an
    /// authenticated realtime participant even though the index itself keys on
    /// the local participant id: a guest must not create a durable-looking queue
    /// entry that cannot be recovered after reconnecting.
    fn handle_matchmaker_rpc(
        &self,
        sender: ParticipantId,
        request_id: u64,
        method: &str,
        payload: &[u8],
    ) -> usize {
        let Some(user_id) = self.registry.user_id_of(sender) else {
            return self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                b"authentication required",
            );
        };
        let now = SystemClock.now();
        match method {
            "matchmaker.add" => {
                if self.require_native_match_lifecycle().is_err() {
                    tracing::error!(
                        participant = sender.get(),
                        reason = NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE,
                        "refused matchmaker admission before native lifecycle match creation"
                    );
                    return self.reply_rpc(
                        sender,
                        request_id,
                        protocol::RPC_STATUS_ERROR,
                        NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE.as_bytes(),
                    );
                }
                // Readiness gate: a ticket that can only ever form a match a
                // ready script must exist for is refused up front rather than
                // parked in a queue that cannot activate.
                if self
                    .script_gate(ScriptGateSurface::MatchmakerQueue)
                    .is_err()
                {
                    return self.reply_rpc(
                        sender,
                        request_id,
                        protocol::RPC_STATUS_ERROR,
                        SCRIPT_UNAVAILABLE_MESSAGE.as_bytes(),
                    );
                }
                let mut request: TicketRequest = match serde_json::from_slice(payload) {
                    Ok(request) => request,
                    Err(_) => {
                        return self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            b"invalid JSON body",
                        );
                    }
                };
                // Preserve the committed party revision through admission. If
                // a mutation wins after this snapshot, the post-admission
                // check below cancels the just-created ticket rather than
                // leaving a ticket with stale membership.
                let mut durable_party_admission = None;
                let ticket_expires_at =
                    match now.checked_add(DurationMillis::from_millis(request.ttl_ms)) {
                        Ok(expires_at) if expires_at > now => expires_at,
                        Ok(_) => {
                            return self.reply_rpc(
                                sender,
                                request_id,
                                protocol::RPC_STATUS_ERROR,
                                b"ticket TTL must be positive",
                            );
                        }
                        Err(error) => {
                            return self.reply_rpc(
                                sender,
                                request_id,
                                protocol::RPC_STATUS_ERROR,
                                error.to_string().as_bytes(),
                            );
                        }
                    };
                let owners = match request.party_id.take() {
                    Some(raw_party_id) => {
                        let party_id = match PartyId::parse(raw_party_id) {
                            Ok(id) => id,
                            Err(error) => {
                                return self.reply_rpc(
                                    sender,
                                    request_id,
                                    protocol::RPC_STATUS_ERROR,
                                    error.to_string().as_bytes(),
                                );
                            }
                        };
                        let (members, admission) = match &self.durable_parties {
                            Some(parties) => match self.durable_party_queue_snapshot(
                                parties,
                                party_id.clone(),
                                &user_id,
                                ticket_expires_at,
                                now,
                            ) {
                                Ok((members, admission)) => (members, Some(admission)),
                                Err(error) => {
                                    return self.reply_rpc(
                                        sender,
                                        request_id,
                                        protocol::RPC_STATUS_ERROR,
                                        error.as_bytes(),
                                    );
                                }
                            },
                            None => match self.parties.queue_members(&user_id, &party_id) {
                                Ok(members) => (members, None),
                                Err(error) => {
                                    return self.reply_rpc(
                                        sender,
                                        request_id,
                                        protocol::RPC_STATUS_ERROR,
                                        error.to_string().as_bytes(),
                                    );
                                }
                            },
                        };
                        durable_party_admission = admission;
                        let mut owners = Vec::with_capacity(members.len());
                        for member_user_id in members {
                            let participant = if member_user_id == user_id {
                                sender
                            } else if let Some(participant) =
                                self.registry.participant_for_user(&member_user_id)
                            {
                                participant
                            } else {
                                return self.reply_rpc(
                                    sender,
                                    request_id,
                                    protocol::RPC_STATUS_ERROR,
                                    b"all party members must be connected",
                                );
                            };
                            owners.push(QueuedTicketOwner {
                                user_id: member_user_id,
                                participant,
                            });
                        }
                        owners
                    }
                    None => vec![QueuedTicketOwner {
                        user_id: user_id.clone(),
                        participant: sender,
                    }],
                };
                if let Some(live) = &self.live_matchmaker {
                    let remote_owners = owners
                        .into_iter()
                        .map(|owner| RemoteMatchmakerTicketOwner {
                            user_id: owner.user_id,
                            session_node: live.node_id().clone(),
                        })
                        .collect();
                    let party_admission = durable_party_admission.clone();
                    if !live.submit_from_session(
                        sender,
                        request_id,
                        remote_owners,
                        request,
                        party_admission,
                    ) {
                        // The durable snapshot was frozen before enqueueing.
                        // A saturated session-worker queue has no authoritative
                        // ticket which could later release it.
                        if let Some(admission) = durable_party_admission {
                            let _ = self.release_durable_party_admission(&admission);
                        }
                        return self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            b"matchmaker is temporarily unavailable",
                        );
                    }
                    return 0;
                }
                let participants = owners.iter().map(|owner| owner.participant).collect();
                match self
                    .matchmaker
                    .add_party(sender, participants, request, now)
                {
                    Ok(id) => {
                        if let Some(admission) = durable_party_admission {
                            let party_id = PartyId::parse(&admission.party_id)
                                .expect("admission party id was validated");
                            let directory = Arc::clone(
                                &self
                                    .durable_parties
                                    .as_ref()
                                    .expect("durable party state")
                                    .directory,
                            );
                            let requester = user_id.clone();
                            let current = party_block_on(async move {
                                directory.snapshot_for(&requester, &party_id).await
                            });
                            if !matches!(current, Ok(ref snapshot) if snapshot.revision == admission.revision)
                            {
                                // A concurrent membership mutation won the
                                // race. Cancellation is owner-bound and is the
                                // same operation exposed by matchmaker.cancel.
                                let _ = self.matchmaker.cancel(sender, &id, SystemClock.now());
                                let _ = self.release_durable_party_admission(&admission);
                                return self.reply_rpc(
                                    sender,
                                    request_id,
                                    protocol::RPC_STATUS_ERROR,
                                    b"party changed during queue admission; retry",
                                );
                            }
                        }
                        self.remember_ticket_owners(id.clone(), owners);
                        let body = serde_json::json!({ "ticket_id": id.as_str() }).to_string();
                        let mut sent = self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_OK,
                            body.as_bytes(),
                        );
                        sent += self.activate_formed_matches(now);
                        sent
                    }
                    Err(error) => {
                        // `add_party` failed before it created a ticket, so
                        // there is no later cancellation/formation lifecycle
                        // to unfreeze this durable admission.
                        if let Some(admission) = durable_party_admission {
                            let _ = self.release_durable_party_admission(&admission);
                        }
                        self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            error.to_string().as_bytes(),
                        )
                    }
                }
            }
            "matchmaker.cancel" => {
                let id = match ticket_id_arg(payload) {
                    Ok(id) => id,
                    Err(message) => {
                        return self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            message.as_bytes(),
                        );
                    }
                };
                if let Some(live) = &self.live_matchmaker {
                    if !live.cancel_from_session(sender, request_id, user_id, id) {
                        return self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            b"matchmaker is temporarily unavailable",
                        );
                    }
                    return 0;
                }
                let cancelled = self.matchmaker.cancel(sender, &id, now);
                if cancelled {
                    self.forget_ticket_owner(&id);
                }
                let body = serde_json::json!({ "cancelled": cancelled }).to_string();
                self.reply_rpc(sender, request_id, protocol::RPC_STATUS_OK, body.as_bytes())
            }
            "matchmaker.status" => {
                let id = match ticket_id_arg(payload) {
                    Ok(id) => id,
                    Err(message) => {
                        return self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            message.as_bytes(),
                        );
                    }
                };
                if let Some(live) = &self.live_matchmaker {
                    if !live.status_from_session(sender, request_id, user_id, id) {
                        return self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            b"matchmaker is temporarily unavailable",
                        );
                    }
                    return 0;
                }
                let Some(state) = self.matchmaker.state(sender, &id, now) else {
                    return self.reply_rpc(
                        sender,
                        request_id,
                        protocol::RPC_STATUS_ERROR,
                        b"ticket not found",
                    );
                };
                let handoff = self.handoff_for(&id, &user_id, now);
                let body = match handoff {
                    Some(handoff) => serde_json::json!({
                        "state": matchmaker_state_name(state),
                        "match": {
                            "match_id": handoff.room_id,
                            "join_token": handoff.token.as_str(),
                            "expires_at": handoff.expires_at.unix_millis(),
                        }
                    })
                    .to_string(),
                    None => {
                        serde_json::json!({ "state": matchmaker_state_name(state) }).to_string()
                    }
                };
                self.reply_rpc(sender, request_id, protocol::RPC_STATUS_OK, body.as_bytes())
            }
            "matchmaker.accept" => {
                // Readiness gate at the session boundary: this covers the
                // live path too, so a closed gate never consumes a durable
                // admission claim the player could not redeem. The trusted
                // admission paths re-check the gate (and the room's binding)
                // for defense in depth.
                if self
                    .script_gate(ScriptGateSurface::MatchmakerAccept)
                    .is_err()
                {
                    return self.reply_rpc(
                        sender,
                        request_id,
                        protocol::RPC_STATUS_ERROR,
                        SCRIPT_UNAVAILABLE_MESSAGE.as_bytes(),
                    );
                }
                let id = match ticket_id_arg(payload) {
                    Ok(id) => id,
                    Err(message) => {
                        return self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            message.as_bytes(),
                        );
                    }
                };
                let token = match join_token_arg(payload) {
                    Ok(token) => token,
                    Err(message) => {
                        return self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            message.as_bytes(),
                        );
                    }
                };
                if let Some(live) = &self.live_matchmaker {
                    if !live.accept_from_session(sender, request_id, user_id, id, token) {
                        return self.reply_rpc(
                            sender,
                            request_id,
                            protocol::RPC_STATUS_ERROR,
                            b"matchmaker is temporarily unavailable",
                        );
                    }
                    return 0;
                }
                self.accept_matchmaker_handoff(sender, request_id, &id, &user_id, &token, now)
            }
            other => self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                format!("unknown matchmaker method: {other}").as_bytes(),
            ),
        }
    }

    /// Evaluate the queue and allocate one closed room per formed cohort. Every
    /// member receives a short-lived, owner-bound `KIND_MATCHMAKER_MATCHED`
    /// handoff; only `matchmaker.accept` performs trusted admission.
    fn activate_formed_matches(&self, now: crate::time::TimestampMillis) -> usize {
        if self.require_native_match_lifecycle().is_err() {
            return 0;
        }
        self.prune_expired_handoffs(now);
        // Readiness gate: while not Ready, queued tickets are held unevaluated
        // (never consumed) and no match room is born on this tick.
        let binding = match self.script_gate(ScriptGateSurface::MatchmakerActivate) {
            Ok(binding) => binding,
            Err(()) => return 0,
        };
        let expires_at = now
            .checked_add(crate::time::DurationMillis::from_millis(
                MATCHMAKER_HANDOFF_TTL_MS,
            ))
            .unwrap_or(now);
        let mut sent = 0;
        for formed in self.matchmaker.evaluate(now) {
            if let Some(cluster) = &self.cluster_matchmaker
                && cluster
                    .authority
                    .claim_formations(&formed.tickets, &cluster.lease, now)
                    .is_err()
            {
                tracing::warn!(
                    node = %cluster.node_id,
                    "matchmaker discarded a cohort whose fenced formation claim failed"
                );
                continue;
            }
            let cap = u16::try_from(formed.participants.len()).unwrap_or(u16::MAX);
            let room_id = {
                let _scope = self.lock_room_scope();
                if let Some(expected) = binding.as_ref() {
                    match self.authoritative_script_gate(ScriptGateSurface::MatchmakerActivate) {
                        Ok(current) if current == *expected => {}
                        Ok(_) | Err(()) => continue,
                    }
                }
                self.rooms.create_bound(
                    RoomLabel {
                        map: "default".to_owned(),
                        mode: "matchmaker".to_owned(),
                        max_players: cap,
                        open: false,
                    },
                    binding.clone(),
                )
            };
            self.dispatch_match_created(room_id);
            let mut deliveries = Vec::new();
            let mut complete = true;
            for (ticket, members) in formed.tickets.iter().zip(&formed.ticket_members) {
                let Some(owners) = self.take_ticket_owners(ticket) else {
                    tracing::error!(
                        ticket_id = ticket.as_str(),
                        room_id,
                        "formed ticket had no owner binding"
                    );
                    complete = false;
                    break;
                };
                if owners.len() != members.len() {
                    tracing::error!(
                        ticket_id = ticket.as_str(),
                        room_id,
                        expected_members = members.len(),
                        bound_owners = owners.len(),
                        "formed ticket ownership did not cover every party member"
                    );
                    complete = false;
                    break;
                }
                for owner in owners {
                    let token = match JoinToken::generate() {
                        Ok(token) => token,
                        Err(error) => {
                            tracing::error!(
                                ticket_id = ticket.as_str(),
                                room_id,
                                %error,
                                "could not mint a secure matchmaker handoff token"
                            );
                            complete = false;
                            break;
                        }
                    };
                    let handoff = PendingMatchHandoff {
                        user_id: owner.user_id,
                        room_id,
                        token,
                        expires_at,
                    };
                    deliveries.push((owner.participant, ticket, handoff));
                }
                if !complete {
                    break;
                }
            }
            if !complete || deliveries.len() != formed.participants.len() {
                let _ = self.discard_empty_room(room_id);
                continue;
            }
            for (_, ticket, handoff) in &deliveries {
                self.insert_handoff((*ticket).clone(), handoff.clone());
            }
            for (participant, ticket, handoff) in deliveries {
                let body = serde_json::json!({
                    "ticket_id": ticket.as_str(),
                    "match_id": handoff.room_id,
                    "join_token": handoff.token.as_str(),
                    "expires_at": handoff.expires_at.unix_millis(),
                })
                .to_string()
                .into_bytes();
                if self.send_reliable(participant, KIND_MATCHMAKER_MATCHED, body) {
                    sent += 1;
                }
            }
        }
        sent
    }

    fn remember_ticket_owners(&self, ticket: TicketId, owners: Vec<QueuedTicketOwner>) {
        if let Ok(mut handoffs) = self.handoffs.lock() {
            handoffs.queued_owners.insert(ticket, owners);
        }
    }

    fn forget_ticket_owner(&self, ticket: &TicketId) {
        if let Ok(mut handoffs) = self.handoffs.lock() {
            handoffs.queued_owners.remove(ticket);
        }
    }

    fn forget_ticket_owner_for_participant(&self, participant: ParticipantId) {
        if let Ok(mut handoffs) = self.handoffs.lock() {
            handoffs
                .queued_owners
                .retain(|_, owners| !owners.iter().any(|owner| owner.participant == participant));
        }
    }

    fn take_ticket_owners(&self, ticket: &TicketId) -> Option<Vec<QueuedTicketOwner>> {
        self.handoffs
            .lock()
            .ok()
            .and_then(|mut handoffs| handoffs.queued_owners.remove(ticket))
    }

    fn insert_handoff(&self, ticket: TicketId, handoff: PendingMatchHandoff) {
        if let Ok(mut handoffs) = self.handoffs.lock() {
            handoffs.pending.entry(ticket).or_default().push(handoff);
        }
    }

    fn handoff_for(
        &self,
        ticket: &TicketId,
        user_id: &str,
        now: crate::time::TimestampMillis,
    ) -> Option<PendingMatchHandoff> {
        self.handoffs
            .lock()
            .ok()?
            .pending
            .get(ticket)
            .and_then(|handoffs| {
                handoffs
                    .iter()
                    .find(|handoff| handoff.user_id == user_id && handoff.expires_at > now)
                    .cloned()
            })
    }

    fn prune_expired_handoffs(&self, now: crate::time::TimestampMillis) {
        let expired_rooms = {
            let Ok(mut handoffs) = self.handoffs.lock() else {
                return;
            };
            let mut rooms = HashSet::new();
            handoffs.pending.retain(|_, pending| {
                pending.retain(|handoff| {
                    let expired = handoff.expires_at <= now;
                    if expired {
                        rooms.insert(handoff.room_id);
                    }
                    !expired
                });
                !pending.is_empty()
            });
            rooms
        };
        for room_id in expired_rooms {
            let still_pending = self
                .handoffs
                .lock()
                .map(|handoffs| {
                    handoffs
                        .pending
                        .values()
                        .flatten()
                        .any(|handoff| handoff.room_id == room_id)
                })
                .unwrap_or(true);
            if !still_pending {
                let _ = self.discard_empty_room(room_id);
            }
        }
    }

    fn accept_matchmaker_handoff(
        &self,
        sender: ParticipantId,
        request_id: u64,
        ticket: &TicketId,
        user_id: &str,
        token: &str,
        now: crate::time::TimestampMillis,
    ) -> usize {
        if self.require_native_match_lifecycle().is_err() {
            return self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE.as_bytes(),
            );
        }
        self.prune_expired_handoffs(now);
        // Readiness gate: trusted admission is refused (stable client-safe
        // message) while not Ready and for rooms bound to a superseded load.
        let binding = match self.script_gate(ScriptGateSurface::MatchmakerAccept) {
            Ok(binding) => binding,
            Err(()) => {
                return self.reply_rpc(
                    sender,
                    request_id,
                    protocol::RPC_STATUS_ERROR,
                    SCRIPT_UNAVAILABLE_MESSAGE.as_bytes(),
                );
            }
        };
        let Some(handoff) = self.handoff_for(ticket, user_id, now) else {
            return self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                b"match handoff not found or expired",
            );
        };
        if handoff.token.as_str() != token {
            return self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                b"invalid match join token",
            );
        }
        let (admission, previous) = {
            let _scope = self.lock_room_scope();
            let previous = self
                .rooms
                .room_of(sender)
                .and_then(|id| self.room_snapshot_for_lifecycle(id));
            let admission = self
                .rooms
                .admit_match_bound(sender, handoff.room_id, binding.as_ref());
            if admission.is_ok() {
                self.bind_rep_connection_to_room_under_scope(sender, handoff.room_id);
            }
            (admission, previous)
        };
        match admission {
            Ok(label) => {
                self.dispatch_local_match_admission(sender, previous, handoff.room_id, false);
                if let Ok(mut handoffs) = self.handoffs.lock() {
                    let remove_ticket = if let Some(pending) = handoffs.pending.get_mut(ticket) {
                        pending.retain(|candidate| {
                            candidate.user_id != user_id || candidate.token.as_str() != token
                        });
                        pending.is_empty()
                    } else {
                        false
                    };
                    if remove_ticket {
                        handoffs.pending.remove(ticket);
                    }
                }
                let body = serde_json::json!({ "accepted": true, "match_id": handoff.room_id })
                    .to_string();
                self.reply_rpc(sender, request_id, protocol::RPC_STATUS_OK, body.as_bytes())
                    + self.reply_joined_with_rep_bootstrap(sender, handoff.room_id, label)
            }
            Err(JoinError::StaleScript) => self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                SCRIPT_UNAVAILABLE_MESSAGE.as_bytes(),
            ),
            Err(error) => self.reply_rpc(
                sender,
                request_id,
                protocol::RPC_STATUS_ERROR,
                format!("match admission failed: {error:?}").as_bytes(),
            ),
        }
    }

    fn accept_remote_matchmaker_admission(
        &self,
        request: RemoteMatchmakerAdmission,
    ) -> Result<u64, MatchmakerRouterError> {
        let Some(cluster) = &self.cluster_matchmaker else {
            return Err(MatchmakerRouterError::UnknownDestination(
                request.requester_node,
            ));
        };
        self.require_native_match_lifecycle()
            .map_err(|_| MatchmakerRouterError::Rejected(cluster.node_id.clone()))?;
        let binding = self
            .script_gate(ScriptGateSurface::ClusterAdmitRemote)
            .map_err(|()| MatchmakerRouterError::Rejected(cluster.node_id.clone()))?;
        let now = SystemClock.now();
        let handoff = self
            .handoff_for(&request.ticket_id, &request.user_id, now)
            .ok_or_else(|| MatchmakerRouterError::UnknownDestination(cluster.node_id.clone()))?;
        if handoff.token.as_str() != request.join_token || request.formation_lease != cluster.lease
        {
            return Err(MatchmakerRouterError::UnknownDestination(
                cluster.node_id.clone(),
            ));
        }
        if self.remote_match_requires_state_relay(handoff.room_id) {
            return Err(MatchmakerRouterError::AuthoritativeAdmissionUnavailable(
                cluster.node_id.clone(),
            ));
        }
        cluster
            .authority
            .claim_admission(&request.ticket_id, &request.user_id, &cluster.lease, now)
            .map_err(|_| MatchmakerRouterError::UnknownDestination(cluster.node_id.clone()))?;
        let member = RemoteRoomMember {
            node_id: request.requester_node,
            user_id: request.user_id,
        };
        let (admission, previous) = {
            let _scope = self.lock_room_scope();
            let previous = self
                .rooms
                .remote_room_of(&member)
                .and_then(|id| self.room_snapshot_for_lifecycle(id));
            let admission = self.rooms.admit_remote_match_bound(
                member.clone(),
                handoff.room_id,
                binding.as_ref(),
            );
            (admission, previous)
        };
        admission
            .map_err(|_| MatchmakerRouterError::UnknownDestination(cluster.node_id.clone()))?;
        self.dispatch_remote_match_admission(&member, previous, handoff.room_id);
        Ok(handoff.room_id)
    }

    /// Send one reliable, correlated RPC response to its caller.
    fn reply_rpc(
        &self,
        sender: ParticipantId,
        request_id: u64,
        status: u8,
        payload: &[u8],
    ) -> usize {
        let response = Envelope::new(
            KIND_RPC_RESPONSE,
            protocol::encode_rpc_response(request_id, status, payload),
        );
        let out_bytes = response.body.len() as u64;
        let outbound = Outbound::reliable(response);
        if self.registry.send_to(sender, &outbound) {
            self.metrics.record_message_out(out_bytes);
            1
        } else {
            tracing::debug!(%sender, "RPC caller disconnected before its response was delivered");
            0
        }
    }

    /// Answer a reserved domain-feature RPC (`friends.*`, …) on a spawned task.
    ///
    /// The caller's `user_id` and outbound sink are resolved synchronously (so a
    /// caller that has already disconnected short-circuits without spawning), then
    /// the async domain service call runs off the relay path and unicasts a
    /// correlated `KIND_RPC_RESPONSE` on completion. Returns 0: nothing is sent
    /// synchronously, the spawned task delivers the reply. See .
    fn spawn_domain_rpc(
        &self,
        sender: ParticipantId,
        domain: DomainRpcServices,
        request: &protocol::RpcRequest<'_>,
    ) -> usize {
        if !self.registry.accepts_work(sender) {
            tracing::debug!(%sender, "domain RPC caller disconnected before dispatch");
            return 0;
        }
        let user_id = self.registry.user_id_of(sender);
        let room_id = self.rooms.room_of(sender);
        let request_id = request.request_id;
        let method = request.method.to_string();
        let payload = request.payload.to_vec();
        let metrics = Arc::clone(&self.metrics);
        let registry = self.registry.clone();
        tokio::spawn(async move {
            let (status, body) = domain
                .dispatch(
                    sender,
                    &registry,
                    &method,
                    user_id.as_deref(),
                    room_id,
                    &payload,
                )
                .await;
            let response = Envelope::new(
                KIND_RPC_RESPONSE,
                protocol::encode_rpc_response(request_id, status, &body),
            );
            let out_bytes = response.body.len() as u64;
            if registry.send_to(sender, &Outbound::reliable(response)) {
                metrics.record_message_out(out_bytes);
            } else {
                tracing::debug!(%sender, "domain RPC caller disconnected before its response");
            }
        });
        0
    }

    /// Handle a transform-sync control frame (`KIND_TSYNC_HELLO` /
    /// `KIND_TSYNC_ACK` / `KIND_TSYNC_INPUT`, /0175).
    ///
    /// A `HELLO` registers the sender for transform sync and replies with the
    /// server's negotiation body reliably (so the client builds the identical
    /// codec). An `ACK` advances the sender's confirmed baseline. An `INPUT`
    /// bundle applies owner input (validated, in seq order, deduped) and delivers
    /// any authoritative rewind result reliably to the shooter. Returns the number
    /// of frames sent back (1 for a `HELLO` reply, N rewind replies for an
    /// `INPUT`, 0 for an `ACK`). A no-op returning 0 when no hub is attached.
    fn handle_transform_control(&self, sender: ParticipantId, env: &Envelope) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        match env.kind {
            KIND_TSYNC_HELLO => {
                let reply = hub.handle_hello(sender.get());
                tracing::info!(
                    participant = sender.get(),
                    "transform-sync: participant opted in (HELLO received)"
                );
                let outbound = Outbound::reliable(Envelope::new(reply.kind, reply.body));
                let out_bytes = outbound.envelope.body.len() as u64;
                let mut sent = if self.registry.send_to(sender, &outbound) {
                    self.metrics.record_message_out(out_bytes);
                    1
                } else {
                    0
                };
                // Player-slot mode: hand this participant a client-owned player
                // object and tell it (reliably) so its client flips that object to
                // owner-predicted. A no-op when `player_slots == 0`.
                //
                // B8 disposition: a player-slot grant spawns a transform object,
                // so it is not allowed to run inside an authoritative match. HELLO
                // is session setup that precedes room membership, so the normal
                // grant happens outside any match; this guard additionally refuses
                // a re-HELLO from a participant already in a bound match, where the
                // script owns spawns through the SpawnRequest path (B5). Making the
                // pre-match grant itself script-confirmed (spec §3.2 hybrid) is a
                // follow-up. Once in a match, the object cannot move or replicate
                // without script authorization (B1/B4/B6).
                if self.authoritative_match(sender).is_some() {
                    return sent;
                }
                if let Some((object_id, role)) = hub.assign_player_slot(sender.get()) {
                    let role_out =
                        Outbound::reliable(Envelope::new(KIND_TSYNC_ROLE, role.encode()));
                    let role_bytes = role_out.envelope.body.len() as u64;
                    if self.registry.send_to(sender, &role_out) {
                        self.metrics.record_message_out(role_bytes);
                        tracing::info!(
                            participant = sender.get(),
                            object_id,
                            "transform-sync: assigned player object (owner-predicted)"
                        );
                        sent += 1;
                    }
                }
                sent
            }
            KIND_TSYNC_V2_HELLO => {
                let Some(reply) = hub.handle_v2_hello(sender.get(), &env.body) else {
                    return 0;
                };
                let outbound = Outbound::reliable(Envelope::new(reply.kind, reply.body));
                let bytes = outbound.envelope.body.len() as u64;
                if self.registry.send_to(sender, &outbound) {
                    self.metrics.record_message_out(bytes);
                    1
                } else {
                    0
                }
            }
            KIND_TSYNC_ACK => {
                hub.handle_ack(sender.get(), &env.body);
                0
            }
            KIND_TSYNC_INPUT => {
                // Authoritative match: owner input becomes TransformInput events
                // routed through the validator; the direct apply_owner_input
                // (bypass B1) is unreachable here. Non-authoritative deployments
                // keep the direct owner-input path unchanged.
                if let Some((room_id, binding)) = self.authoritative_match(sender) {
                    let Ok(bundle) = citadel_wire::tsync::InputBundle::decode(&env.body) else {
                        return 0;
                    };
                    return self.route_bridge_input(sender, &bundle.frames, room_id, &binding);
                }
                // Owner input rides the unreliable path; a carried fire command
                // yields an authoritative rewind result delivered reliably to the
                // shooter only (design §5.2). The client never resolves hits.
                let mut sent = 0;
                for reply in hub.handle_input(sender.get(), &env.body) {
                    let outbound = Outbound::reliable(Envelope::new(reply.kind, reply.body));
                    let out_bytes = outbound.envelope.body.len() as u64;
                    if self.registry.send_to(sender, &outbound) {
                        self.metrics.record_message_out(out_bytes);
                        sent += 1;
                    }
                }
                sent
            }
            KIND_TSYNC_V2_INPUT => {
                // Authoritative match: epoch-validate the v2 wrapper, then route
                // the inner bundle through the validator (bypass B3 closed).
                if let Some((room_id, binding)) = self.authoritative_match(sender) {
                    let Some(bundle) = hub.decode_v2_input(sender.get(), &env.body) else {
                        return 0;
                    };
                    return self.route_bridge_input(sender, &bundle.frames, room_id, &binding);
                }
                let mut sent = 0;
                for reply in hub.handle_v2_input(sender.get(), &env.body) {
                    let outbound = Outbound::reliable(Envelope::new(reply.kind, reply.body));
                    let bytes = outbound.envelope.body.len() as u64;
                    if self.registry.send_to(sender, &outbound) {
                        self.metrics.record_message_out(bytes);
                        sent += 1;
                    }
                }
                sent
            }
            _ => 0,
        }
    }

    /// Handle a Networked-Actors frame (`KIND_NA_PRESENCE` / `KIND_NA_STATE`,
    /// ) — the out-of-the-box presence + replicated-spawn layer.
    ///
    /// A `PRESENCE` registers the sender's avatar and drives the spawn fan-out:
    /// the owner is sent its own spawn (reliably, **first**, so it learns its
    /// object id and latches its participant id), then a batch of everyone already
    /// present, and every other present participant is sent the newcomer's spawn.
    /// A `STATE` is the owner's relay transform report (unreliable hot path):
    /// applied to the owned object after an ownership check; the normal snapshots
    /// replicate it to observers. Returns the number of frames delivered. A no-op
    /// returning 0 when no hub is attached.
    fn handle_networked_actor(&self, sender: ParticipantId, env: &Envelope) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        match env.kind {
            KIND_NA_PRESENCE => {
                // Authoritative match: the presence/spawn request becomes a
                // normalized event; registration + spawn fan-out (bypass B5)
                // happen only on the script's accept/correct. Non-authoritative
                // deployments register + fan out directly, unchanged.
                if let Some((room_id, binding)) = self.authoritative_match(sender) {
                    return self.route_bridge_presence(sender, env, room_id, &binding);
                }
                let Ok(presence) = citadel_wire::na::NaPresence::decode(&env.body) else {
                    tracing::debug!(%sender, "gateway dropped a malformed NA_PRESENCE");
                    return 0;
                };
                self.do_register_presence(sender, presence.archetype_id, presence.transform)
            }
            KIND_NA_STATE => {
                // Authoritative match: the owner report becomes a normalized
                // event routed through the validator; the direct verbatim write
                // (bypass B4 — the worst, since Relay is the default movement
                // mode) is unreachable here. Non-authoritative (relay)
                // deployments keep the direct owner-state write unchanged.
                if let Some((room_id, binding)) = self.authoritative_match(sender) {
                    return self.route_bridge_na_state(sender, env, room_id, &binding);
                }
                let Ok(state) = citadel_wire::na::NaState::decode(&env.body) else {
                    return 0;
                };
                hub.apply_owner_state(sender.get(), state.object_id, state.transform);
                0
            }
            _ => 0,
        }
    }

    /// Register a networked-actor presence for `sender` and drive the spawn
    /// fan-out (self spawn first, optional owner role, same-room present batch +
    /// server NPCs, then the newcomer's spawn to same-room peers). Returns the
    /// number of frames delivered. This is the presence executor shared by the
    /// direct (non-authoritative) path and the bridge's accepted `SpawnRequest`.
    fn do_register_presence(
        &self,
        sender: ParticipantId,
        archetype_id: u16,
        transform: citadel_wire::na::NaTransform,
    ) -> usize {
        let _scope = self.lock_room_scope();
        let room_id = self.rooms.room_of(sender);
        self.do_register_presence_under_scope(sender, room_id, archetype_id, transform)
    }

    /// Register presence while the caller holds the room transaction gate.
    /// `room_id` is captured before the hub changes, so every self/batch/peer
    /// enqueue shares a linearization point with the sender's membership.
    fn do_register_presence_under_scope(
        &self,
        sender: ParticipantId,
        sender_room: Option<RoomId>,
        archetype_id: u16,
        transform: citadel_wire::na::NaTransform,
    ) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        if self.rooms.room_of(sender) != sender_room {
            return 0;
        }
        let Some(reg) = hub.register_presence(sender.get(), archetype_id, transform) else {
            return 0;
        };
        if self.bridge.is_some()
            && sender_room.is_some_and(|room_id| self.rooms.binding(room_id).is_some())
        {
            // A bridge-admitted presence is a server-owned match fact. Bind its
            // object before a synchronous script answer can target or correct it.
            // Direct/legacy presence keeps its existing node-global behavior.
            hub.set_object_room(reg.object_id, sender_room);
        }
        tracing::info!(
            participant = sender.get(),
            object_id = reg.object_id,
            archetype = archetype_id,
            predicted_authoritative = reg.owner_role.is_some(),
            "networked-actors: presence registered"
        );
        let mut sent = 0;
        // 1) The owner's own spawn FIRST (tells it its object id).
        if self.send_reliable_in_scope_under_scope(
            sender,
            sender_room,
            KIND_NA_SPAWN,
            reg.self_spawn.encode(),
        ) {
            sent += 1;
        }
        // 2) A predicted owner receives its role only after its own spawn: the
        // engine can now bind the native pawn to object_id before the existing
        // input/prediction component sees the assignment.
        if let Some(role) = reg.owner_role
            && self.send_reliable_in_scope_under_scope(
                sender,
                sender_room,
                KIND_TSYNC_ROLE,
                role.encode(),
            )
        {
            sent += 1;
        }
        // 3) Everyone already present IN THE SAME ROOM, so the newcomer sees its
        //    room's world (not players in other rooms sharing the world).
        let mut batch_spawns: Vec<citadel_wire::na::NaSpawn> = reg
            .batch
            .spawns
            .into_iter()
            .filter(|s| self.rooms.room_of(ParticipantId::from_raw(s.owner)) == sender_room)
            .collect();
        let mut batch_owners: Vec<u64> = batch_spawns
            .iter()
            .filter_map(|spawn| (spawn.owner != 0).then_some(spawn.owner))
            .collect();
        // Add visible server-owned NPCs so a late joiner instantiates its room's
        // actors without receiving another match's state.
        if let Ok(npcs) = self.npcs.lock() {
            for npc in npcs.values() {
                if npc
                    .room_id
                    .is_some_and(|room_id| sender_room != Some(room_id))
                {
                    continue;
                }
                let transform = self
                    .transform
                    .as_ref()
                    .and_then(|h| h.get_transform(npc.object_id))
                    .map(|s| citadel_wire::na::NaTransform {
                        position: s.position,
                        rotation: s.rotation,
                        velocity: s.velocity,
                    })
                    .unwrap_or_else(citadel_wire::na::NaTransform::identity);
                batch_spawns.push(citadel_wire::na::NaSpawn {
                    object_id: npc.object_id,
                    archetype_id: npc.archetype_id,
                    owner: 0,
                    transform,
                });
            }
        }
        let batch = citadel_wire::na::NaSpawnBatch {
            spawns: batch_spawns,
        };
        batch_owners.sort_unstable();
        batch_owners.dedup();
        if self.send_reliable_in_scope_with_owners_under_scope(
            sender,
            sender_room,
            &batch_owners,
            KIND_NA_SPAWN_BATCH,
            batch.encode(),
        ) {
            sent += 1;
        }
        // 4) Tell every same-room participant to spawn the newcomer.
        let peer_body = reg.peer_spawn.encode();
        for peer in reg.peers {
            let peer_id = ParticipantId::from_raw(peer);
            if self.send_reliable_same_room_under_scope(
                sender,
                peer_id,
                KIND_NA_SPAWN,
                peer_body.clone(),
            ) {
                sent += 1;
            }
        }
        sent
    }

    /// Send one reliable envelope to a single participant, recording the outbound
    /// metric. Returns whether it was delivered (the target was live).
    fn send_reliable(&self, target: ParticipantId, kind: u16, body: Vec<u8>) -> bool {
        let outbound = Outbound::reliable(Envelope::new(kind, body));
        let out_bytes = outbound.envelope.body.len() as u64;
        if self.registry.send_to(target, &outbound) {
            self.metrics.record_message_out(out_bytes);
            true
        } else {
            false
        }
    }

    /// Send a room-scoped frame only after rechecking both the recipient and
    /// every captured source owner under the room transaction gate.
    #[cfg(test)]
    fn send_reliable_in_scope_with_owners(
        &self,
        target: ParticipantId,
        room_id: Option<RoomId>,
        owners: &[u64],
        kind: u16,
        body: Vec<u8>,
    ) -> bool {
        let _scope = self.lock_room_scope();
        self.send_reliable_in_scope_with_owners_under_scope(target, room_id, owners, kind, body)
    }

    fn send_reliable_in_scope_under_scope(
        &self,
        target: ParticipantId,
        room_id: Option<RoomId>,
        kind: u16,
        body: Vec<u8>,
    ) -> bool {
        self.send_reliable_in_scope_with_owners_under_scope(target, room_id, &[], kind, body)
    }

    fn send_reliable_in_scope_with_owners_under_scope(
        &self,
        target: ParticipantId,
        room_id: Option<RoomId>,
        owners: &[u64],
        kind: u16,
        body: Vec<u8>,
    ) -> bool {
        let outbound = Outbound::reliable(Envelope::new(kind, body));
        let bytes = outbound.envelope.body.len() as u64;
        let delivered = self
            .rooms
            .while_member_and_owners_in(target, room_id, owners, || {
                self.registry.send_to(target, &outbound)
            })
            .unwrap_or(false);
        if delivered {
            self.metrics.record_message_out(bytes);
        }
        delivered
    }

    fn send_reliable_same_room(
        &self,
        source: ParticipantId,
        target: ParticipantId,
        kind: u16,
        body: Vec<u8>,
    ) -> bool {
        let _scope = self.lock_room_scope();
        self.send_reliable_same_room_under_scope(source, target, kind, body)
    }

    fn send_reliable_same_room_under_scope(
        &self,
        source: ParticipantId,
        target: ParticipantId,
        kind: u16,
        body: Vec<u8>,
    ) -> bool {
        let outbound = Outbound::reliable(Envelope::new(kind, body));
        let bytes = outbound.envelope.body.len() as u64;
        let delivered = self
            .rooms
            .while_same_room(source, target, || self.registry.send_to(target, &outbound))
            .unwrap_or(false);
        if delivered {
            self.metrics.record_message_out(bytes);
        }
        delivered
    }

    /// Fan out one already-built frame while holding the room membership lock.
    /// `SessionRegistry::send_to` is nonblocking, so this closes the move-versus-
    /// enqueue race without waiting on transport I/O under the room lock.
    fn send_to_room_members(
        &self,
        room_id: RoomId,
        source: Option<ParticipantId>,
        exclude: Option<ParticipantId>,
        outbound: &Outbound,
    ) -> usize {
        let _scope = self.lock_room_scope();
        self.send_to_room_members_under_scope(room_id, source, exclude, outbound)
    }

    fn send_to_room_members_under_scope(
        &self,
        room_id: RoomId,
        source: Option<ParticipantId>,
        exclude: Option<ParticipantId>,
        outbound: &Outbound,
    ) -> usize {
        self.rooms
            .while_members_in(room_id, |members| {
                if source.is_some_and(|participant| !members.contains(&participant)) {
                    return 0;
                }
                members
                    .iter()
                    .filter(|&&member| Some(member) != exclude)
                    .filter(|&&member| self.registry.send_to(member, outbound))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Snapshot local match metadata for diagnostics and console queries.
    #[must_use]
    pub fn room_snapshot(&self) -> Vec<crate::realtime::rooms::RoomSnapshot> {
        self.rooms.snapshot()
    }

    /// Return a match's immutable script binding, if it is still live.
    #[must_use]
    pub fn room_binding(&self, room_id: RoomId) -> Option<ScriptBinding> {
        self.rooms.binding(room_id)
    }

    /// Create an empty trusted room. Membership admission remains a separate
    /// gateway operation so it can atomically install its replication binding.
    /// Returns a typed refusal without mutating room state when the selected
    /// runtime cannot carry the complete native lifecycle.
    pub fn create_room(&self, label: RoomLabel) -> Result<RoomId, NativeMatchLifecycleUnavailable> {
        let (bridge_mode, binding) = if self.strict_script_rooms {
            self.require_authoritative_match_lifecycle()?;
            let binding = self
                .authoritative_script_gate(ScriptGateSurface::RoomCreate)
                .map_err(|_| NativeMatchLifecycleUnavailable)?;
            (BridgeMode::Authoritative, Some(binding))
        } else {
            (BridgeMode::Relay, None)
        };
        let room_id = {
            let _scope = self.lock_room_scope();
            if let Some(expected) = binding.as_ref() {
                let current = self
                    .authoritative_script_gate(ScriptGateSurface::RoomCreate)
                    .map_err(|_| NativeMatchLifecycleUnavailable)?;
                if current != *expected {
                    return Err(NativeMatchLifecycleUnavailable);
                }
            }
            self.rooms.create_with_mode(label, bridge_mode, binding)
        };
        self.dispatch_match_created(room_id);
        Ok(room_id)
    }

    /// Atomically admit a trusted local participant and bind its replication
    /// connection to the same room. Server-side integrations use this instead
    /// of mutating `RoomRegistry` directly.
    pub fn join_room(
        &self,
        participant: ParticipantId,
        room_id: RoomId,
    ) -> Result<RoomLabel, JoinError> {
        self.require_native_match_lifecycle()
            .map_err(JoinError::from)?;
        let (label, previous) = {
            let _scope = self.lock_room_scope();
            let previous = self
                .rooms
                .room_of(participant)
                .and_then(|id| self.room_snapshot_for_lifecycle(id));
            let expected = match self.rooms.bridge_mode(room_id) {
                Some(BridgeMode::Authoritative) => Some(
                    self.authoritative_script_gate(ScriptGateSurface::RoomJoin)
                        .map_err(|_| JoinError::StaleScript)?,
                ),
                Some(BridgeMode::Relay) if !self.strict_script_rooms => None,
                Some(BridgeMode::Relay) | None => return Err(JoinError::StaleScript),
            };
            let label = self
                .rooms
                .join_bound(participant, room_id, expected.as_ref())?;
            self.bind_rep_connection_to_room_under_scope(participant, room_id);
            (label, previous)
        };
        self.dispatch_local_match_admission(participant, previous, room_id, false);
        Ok(label)
    }

    /// Atomically join a named trusted room or create it and bind the caller.
    pub fn join_or_create_room(
        &self,
        participant: ParticipantId,
        name: &str,
        make_label: impl FnOnce() -> RoomLabel,
    ) -> Result<(RoomId, RoomLabel), JoinError> {
        let binding = if self.strict_script_rooms {
            Some(
                self.authoritative_script_gate(ScriptGateSurface::RoomCreate)
                    .map_err(|_| JoinError::StaleScript)?,
            )
        } else {
            None
        };
        self.join_or_create_room_bound(participant, name, binding, make_label)
    }

    /// Trusted named-room admission with an explicit script binding. The
    /// binding is checked by `RoomRegistry` and the replication connection is
    /// committed in the same room transaction.
    pub fn join_or_create_room_bound(
        &self,
        participant: ParticipantId,
        name: &str,
        binding: Option<ScriptBinding>,
        make_label: impl FnOnce() -> RoomLabel,
    ) -> Result<(RoomId, RoomLabel), JoinError> {
        let binding = match (self.strict_script_rooms, binding) {
            (true, _) => Some(
                self.authoritative_script_gate(ScriptGateSurface::RoomCreate)
                    .map_err(|_| JoinError::StaleScript)?,
            ),
            (false, Some(requested)) => {
                self.require_authoritative_match_lifecycle()
                    .map_err(JoinError::from)?;
                let current = self
                    .authoritative_script_gate(ScriptGateSurface::RoomCreate)
                    .map_err(|_| JoinError::StaleScript)?;
                if requested != current {
                    return Err(JoinError::StaleScript);
                }
                Some(current)
            }
            (false, None) => None,
        };
        self.require_native_match_lifecycle()
            .map_err(JoinError::from)?;
        let (room_id, label, created, previous) = {
            let _scope = self.lock_room_scope();
            if let Some(expected) = binding.as_ref() {
                let current = self
                    .authoritative_script_gate(ScriptGateSurface::RoomCreate)
                    .map_err(|_| JoinError::StaleScript)?;
                if current != *expected {
                    return Err(JoinError::StaleScript);
                }
            }
            let existing_rooms: HashSet<RoomId> = self
                .rooms
                .snapshot()
                .into_iter()
                .map(|room| room.id)
                .collect();
            let previous = self
                .rooms
                .room_of(participant)
                .and_then(|id| self.room_snapshot_for_lifecycle(id));
            let (room_id, label) =
                self.rooms
                    .join_or_create_bound(participant, name, binding, make_label)?;
            self.bind_rep_connection_to_room_under_scope(participant, room_id);
            (room_id, label, !existing_rooms.contains(&room_id), previous)
        };
        self.dispatch_local_match_admission(participant, previous, room_id, created);
        Ok((room_id, label))
    }

    /// Drop a room that never admitted a member. Matchmaker failure cleanup is
    /// serialized with admissions so an expired handoff cannot race a bind.
    pub(crate) fn discard_empty_room(&self, room_id: RoomId) -> bool {
        let (discarded, room) = {
            let _scope = self.lock_room_scope();
            let room = self.room_snapshot_for_lifecycle(room_id);
            let discarded = self.rooms.discard_empty(room_id);
            (discarded, room)
        };
        if discarded {
            if let Some(room) = room {
                self.dispatch_match_lifecycle(
                    NativeMatchLifecycleHook::Ended,
                    room,
                    Some(MatchTerminationReason::FormationAbandoned),
                    self.native_match_budget(),
                );
            }
            if let Some(runtime) = &self.runtime {
                runtime.on_match_closed(room_id);
            }
        }
        discarded
    }

    /// Unit tests use the concrete registry to construct deliberate stale
    /// snapshots. Production code cannot obtain this mutable transition bypass.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn rooms(&self) -> &RoomRegistry {
        &self.rooms
    }

    /// Whether a remote match needs the unavailable authoritative state relay.
    /// A relay node has no script gate and no bound match, so it retains the
    /// established cross-node matchmaker path.
    pub(crate) fn remote_match_requires_state_relay(&self, room_id: RoomId) -> bool {
        self.rooms.bridge_mode(room_id) == Some(BridgeMode::Authoritative)
            && self.rooms.binding(room_id).is_some()
    }

    /// Consult the GameScript readiness gate for one enforcement `surface`.
    ///
    /// `Ok(None)` when no gate is attached (`require_script` off — ungated
    /// behavior, unchanged). `Ok(Some(binding))` when ONE atomic Ready
    /// snapshot admitted the caller; a match born from this call must carry
    /// exactly that binding. `Err(())` fails closed after counting the
    /// rejection for `surface`; the caller owes the client the stable
    /// [`SCRIPT_UNAVAILABLE_MESSAGE`] wherever a reply channel exists.
    fn script_gate(&self, surface: ScriptGateSurface) -> Result<Option<ScriptBinding>, ()> {
        if !self.strict_script_rooms {
            return Ok(None);
        }
        self.authoritative_script_gate(surface).map(Some)
    }

    /// Consult readiness for an explicitly authoritative room. Unlike
    /// [`Self::script_gate`], this gate remains active on optional-runtime nodes.
    fn authoritative_script_gate(&self, surface: ScriptGateSurface) -> Result<ScriptBinding, ()> {
        let Some(readiness) = &self.script_readiness else {
            return Err(());
        };
        match readiness.gate() {
            Ok(binding) => Ok(binding),
            Err(_) => {
                self.metrics.record_script_gate_rejection(surface);
                tracing::debug!(
                    surface = surface.code(),
                    "script readiness gate refused an authoritative room surface"
                );
                Err(())
            }
        }
    }

    /// Send the stable, client-safe policy rejection for a refused room frame
    /// (`KIND_ROOM_REJECT`). Returns frames delivered.
    fn reply_room_reject(&self, sender: ParticipantId, request_kind: u16) -> usize {
        self.reply_room_reject_with_reason(sender, request_kind, SCRIPT_UNAVAILABLE_MESSAGE)
    }

    /// Send a client-safe refusal for a room operation before it can create or
    /// admit a match. The reason is a stable operator-facing policy message,
    /// never a runtime error or script detail.
    fn reply_room_reject_with_reason(
        &self,
        sender: ParticipantId,
        request_kind: u16,
        reason: &str,
    ) -> usize {
        let body = citadel_wire::room::RoomReject {
            request_kind,
            reason: reason.to_owned(),
        }
        .encode();
        usize::from(self.send_reliable(sender, citadel_wire::protocol::KIND_ROOM_REJECT, body))
    }

    /// Acquire the exclusive runtime-generation fence for one reload swap.
    /// Only the reload service may hold this across retirement and replacement.
    pub(crate) fn lock_reload_generation(&self) -> std::sync::RwLockWriteGuard<'_, ()> {
        self.generation_gate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Mark the short interval where reload retirement closes rooms. Their
    /// lifecycle commands are deliberately suppressed rather than dispatched
    /// into an ambiguous old/new runtime generation.
    pub(crate) fn set_reload_retiring(&self, retiring: bool) {
        self.reload_retiring.store(retiring, Ordering::Release);
    }

    /// Retire every authoritative room after a successful runtime reload. A room
    /// captures one revision/generation at birth; allowing it to continue under
    /// a replacement VM would cross that generation fence. Relay rooms remain.
    pub fn retire_authoritative_rooms_for_reload(&self) {
        // Serialize the retirement snapshot with authoritative room birth. The
        // reload source has already closed readiness before calling us; a birth
        // that linearizes before this gate is included below, while one after it
        // revalidates readiness and is refused.
        let room_ids: Vec<RoomId> = {
            let _scope = self.lock_room_scope();
            self.rooms
                .snapshot()
                .into_iter()
                .filter(|room| room.bridge_mode == BridgeMode::Authoritative)
                .map(|room| room.id)
                .collect()
        };
        for room_id in room_ids {
            let _ = self.close_match(room_id);
        }
    }

    /// Close one authoritative match server-side.
    ///
    /// Every member receives a reliable `KIND_MATCH_CLOSED` — a server-error
    /// close carrying a requeue hint — and the room is pruned from the
    /// registry. The requeue model is client-prompted: the hinted client
    /// re-submits its own matchmaker ticket; the server retains no ticket on
    /// the member's behalf, so nothing can resume the dead match. Returns the
    /// number of notifications delivered (0 when no such room exists).
    pub fn close_match(&self, room_id: RoomId) -> usize {
        let (members, room) = {
            let _scope = self.lock_room_scope();
            let room = self
                .rooms
                .snapshot()
                .into_iter()
                .find(|snapshot| snapshot.id == room_id);
            let Some(members) = self.rooms.close(room_id) else {
                return 0;
            };
            for member in &members {
                self.unbind_rep_connection_under_scope(*member);
            }
            // A late validator answer must not observe the room as closed but
            // still find a live ledger. Remove both identities in the same
            // transaction as the membership/binding transition.
            self.drop_bridge_match(room_id);
            (members, room)
        };
        if let Some(room) = room {
            let budget = self.native_match_budget();
            let mut leaving = room.clone();
            for member in &members {
                leaving.members.retain(|candidate| candidate != member);
                self.dispatch_match_lifecycle(
                    NativeMatchLifecycleHook::Leave,
                    leaving.clone(),
                    None,
                    budget,
                );
            }
            for _ in 0..leaving.remote_member_count {
                leaving.remote_member_count -= 1;
                self.dispatch_match_lifecycle(
                    NativeMatchLifecycleHook::Leave,
                    leaving.clone(),
                    None,
                    budget,
                );
            }
            self.dispatch_match_lifecycle(
                NativeMatchLifecycleHook::Ended,
                leaving,
                Some(MatchTerminationReason::ServerClosed),
                budget,
            );
        }
        // Let a process-hosting runtime adapter release the match's execution
        // context (a no-op for embedded adapters, and for a worker-initiated
        // close whose context is already gone).
        if let Some(runtime) = &self.runtime {
            runtime.on_match_closed(room_id);
        }
        tracing::warn!(
            room_id,
            members = members.len(),
            "match closed server-side; members returned to matchmaking"
        );
        let body = citadel_wire::room::MatchClosed {
            room_id,
            reason: citadel_wire::room::MATCH_CLOSE_REASON_SERVER_ERROR,
            requeue_hint: true,
        }
        .encode();
        let mut sent = 0;
        for member in members {
            if self.send_reliable(
                member,
                citadel_wire::protocol::KIND_MATCH_CLOSED,
                body.clone(),
            ) {
                sent += 1;
            }
        }
        sent
    }

    /// Close every live match via [`Self::close_match`].
    ///
    /// The worker-death flow: when the supervised GameScript worker crashes,
    /// every dependent match is closed the same match-local way — members
    /// informed and returned to matchmaking, rooms pruned — before the
    /// replacement worker (which never resumes old matches) starts serving.
    pub fn close_all_matches(&self) -> usize {
        self.rooms
            .snapshot()
            .into_iter()
            .map(|room| self.close_match(room.id))
            .sum()
    }

    /// Close only rooms whose immutable mode depends on the current script
    /// generation. Optional-mode relay rooms survive external-worker failures.
    pub fn close_all_authoritative_matches(&self) -> usize {
        self.rooms
            .snapshot()
            .into_iter()
            .filter(|room| room.bridge_mode == BridgeMode::Authoritative)
            .map(|room| self.close_match(room.id))
            .sum()
    }

    /// Apply one external match's command batch, room-scoped exactly like an
    /// embedded dispatch result (no excluded sender: the worker's fan-out
    /// semantics are its own).
    pub fn apply_external_match_commands(
        &self,
        room_id: RoomId,
        commands: Vec<OutboundCommand>,
    ) -> usize {
        self.apply_commands_scoped(None, Some(room_id), commands)
    }

    /// Route a room frame (kinds 21-25).
    ///
    /// Phase A1: `ROOM_CREATE` interprets its params as the desired map name (Phase
    /// A2 will route this through the Lua `on_room_create` hook returning a typed
    /// label); `ROOM_JOIN`/`ROOM_LEAVE` track membership and fan out
    /// `ROOM_JOINED`/`ROOM_LEAVE`; `ROOM_MAP_READY` is recorded (used by A4 fan-out).
    fn handle_room(&self, sender: ParticipantId, env: &Envelope) -> usize {
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomLeave, RoomMapReady};
        match env.kind {
            KIND_ROOM_CREATE => {
                let Ok(create) = RoomCreate::decode(&env.body) else {
                    tracing::debug!(%sender, "gateway dropped a malformed ROOM_CREATE");
                    return 0;
                };
                // The params are the room's matchmaking NAME: everyone asking for the
                // same name lands in the same room. The loaded script may request
                // the immutable bridge mode only while the room is born.
                let name = {
                    let s = String::from_utf8_lossy(&create.params);
                    if s.is_empty() {
                        "default".to_owned()
                    } else {
                        s.into_owned()
                    }
                };
                let _creation_gate = self
                    .room_creation_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(existing) = self.rooms.named_room(&name) {
                    let binding = match existing.bridge_mode {
                        BridgeMode::Relay if !self.strict_script_rooms => None,
                        BridgeMode::Relay => {
                            return self.reply_room_reject(sender, KIND_ROOM_CREATE);
                        }
                        BridgeMode::Authoritative => {
                            match self.authoritative_script_gate(ScriptGateSurface::RoomCreate) {
                                Ok(binding) => Some(binding),
                                Err(()) => return self.reply_room_reject(sender, KIND_ROOM_CREATE),
                            }
                        }
                    };
                    return self.join_and_reply(sender, existing.id, binding, KIND_ROOM_CREATE);
                }
                let (label, requested_mode) = self.room_spec_for_create(sender, &create.params);
                let (bridge_mode, binding) = match requested_mode {
                    RoomBridgeMode::Relay if !self.strict_script_rooms => (BridgeMode::Relay, None),
                    RoomBridgeMode::Relay => {
                        // `require_script` remains deployment-wide strict: it
                        // upgrades the compatibility default to authoritative.
                        if self.require_authoritative_match_lifecycle().is_err() {
                            return self.reply_room_reject_with_reason(
                                sender,
                                KIND_ROOM_CREATE,
                                NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE,
                            );
                        }
                        match self.script_gate(ScriptGateSurface::RoomCreate) {
                            Ok(Some(binding)) => (BridgeMode::Authoritative, Some(binding)),
                            Ok(None) | Err(()) => {
                                return self.reply_room_reject(sender, KIND_ROOM_CREATE);
                            }
                        }
                    }
                    RoomBridgeMode::Authoritative => {
                        if self.require_authoritative_match_lifecycle().is_err() {
                            return self.reply_room_reject_with_reason(
                                sender,
                                KIND_ROOM_CREATE,
                                NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE,
                            );
                        }
                        match self.authoritative_script_gate(ScriptGateSurface::RoomCreate) {
                            Ok(binding) => (BridgeMode::Authoritative, Some(binding)),
                            Err(()) => {
                                return self.reply_room_reject(sender, KIND_ROOM_CREATE);
                            }
                        }
                    }
                };
                let (admission, previous, existing_rooms) = {
                    let _scope = self.lock_room_scope();
                    let previous = self
                        .rooms
                        .room_of(sender)
                        .and_then(|id| self.room_snapshot_for_lifecycle(id));
                    let existing_rooms: HashSet<RoomId> = self
                        .rooms
                        .snapshot()
                        .into_iter()
                        .map(|room| room.id)
                        .collect();
                    if bridge_mode == BridgeMode::Authoritative {
                        match self.authoritative_script_gate(ScriptGateSurface::RoomCreate) {
                            Ok(current) if binding.as_ref() == Some(&current) => {}
                            Ok(_) | Err(()) => {
                                return self.reply_room_reject(sender, KIND_ROOM_CREATE);
                            }
                        }
                    }
                    let admission = self.rooms.join_or_create_with_mode(
                        sender,
                        &name,
                        bridge_mode,
                        binding,
                        || label,
                    );
                    if let Ok((room_id, _)) = &admission {
                        self.bind_rep_connection_to_room_under_scope(sender, *room_id);
                    }
                    (admission, previous, existing_rooms)
                };
                match admission {
                    Ok((room_id, label)) => {
                        tracing::info!(
                            participant = sender.get(),
                            room_id,
                            name = %name,
                            map = %label.map,
                            "room: join-or-create"
                        );
                        self.dispatch_local_match_admission(
                            sender,
                            previous,
                            room_id,
                            !existing_rooms.contains(&room_id),
                        );
                        self.reply_joined(sender, room_id, label)
                    }
                    Err(JoinError::StaleScript) => {
                        // Policy refusal (superseded named room): visible.
                        self.reply_room_reject(sender, KIND_ROOM_CREATE)
                    }
                    Err(reason) => {
                        tracing::debug!(
                            participant = sender.get(),
                            name = %name,
                            ?reason,
                            "room: join-or-create rejected"
                        );
                        0
                    }
                }
            }
            KIND_ROOM_JOIN => {
                let Ok(join) = RoomJoin::decode(&env.body) else {
                    return 0;
                };
                let binding = match self.rooms.bridge_mode(join.room_id) {
                    Some(BridgeMode::Relay) => None,
                    Some(BridgeMode::Authoritative) => {
                        if self.require_authoritative_match_lifecycle().is_err() {
                            return self.reply_room_reject_with_reason(
                                sender,
                                KIND_ROOM_JOIN,
                                NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE,
                            );
                        }
                        match self.authoritative_script_gate(ScriptGateSurface::RoomJoin) {
                            Ok(binding) => Some(binding),
                            Err(()) => {
                                return self.reply_room_reject(sender, KIND_ROOM_JOIN);
                            }
                        }
                    }
                    None => return 0,
                };
                // Lua admission gate (`on_room_join`); admits by default.
                if let Some(runtime) = &self.runtime {
                    let user_id = self.registry.user_id_of(sender);
                    if !runtime.call_room_join(sender.get(), user_id.as_deref(), join.room_id) {
                        tracing::debug!(
                            participant = sender.get(),
                            room_id = join.room_id,
                            "room: join rejected by on_room_join"
                        );
                        return 0;
                    }
                }
                self.join_and_reply(sender, join.room_id, binding, KIND_ROOM_JOIN)
            }
            KIND_ROOM_LEAVE => {
                if RoomLeave::decode(&env.body).is_err() {
                    return 0;
                }
                self.leave_room(sender)
            }
            KIND_ROOM_MAP_READY => {
                let Ok(ready) = RoomMapReady::decode(&env.body) else {
                    return 0;
                };
                tracing::debug!(
                    participant = sender.get(),
                    room_id = ready.room_id,
                    "room: client reported map ready"
                );
                0
            }
            _ => 0,
        }
    }

    /// Join `sender` to `room_id` and reply with `ROOM_JOINED` (carrying the label's
    /// map/mode) on success. `binding` is the gating snapshot's identity on a
    /// gated node (`None` when ungated): a room bound to a superseded load
    /// refuses the join with a visible policy reject. Returns frames sent
    /// (0 on a silent legacy rejection).
    fn join_and_reply(
        &self,
        sender: ParticipantId,
        room_id: RoomId,
        binding: Option<ScriptBinding>,
        request_kind: u16,
    ) -> usize {
        if self.rooms.bridge_mode(room_id) == Some(BridgeMode::Authoritative)
            && self.require_authoritative_match_lifecycle().is_err()
        {
            return self.reply_room_reject_with_reason(
                sender,
                request_kind,
                NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE,
            );
        }
        let (admission, previous) = {
            let _scope = self.lock_room_scope();
            let previous = self
                .rooms
                .room_of(sender)
                .and_then(|id| self.room_snapshot_for_lifecycle(id));
            let admission = self.rooms.join_bound(sender, room_id, binding.as_ref());
            if admission.is_ok() {
                self.bind_rep_connection_to_room_under_scope(sender, room_id);
            }
            (admission, previous)
        };
        match admission {
            Ok(label) => {
                self.dispatch_local_match_admission(sender, previous, room_id, false);
                self.reply_joined(sender, room_id, label)
            }
            Err(JoinError::StaleScript) => self.reply_room_reject(sender, request_kind),
            Err(reason) => {
                tracing::debug!(
                    participant = sender.get(),
                    room_id,
                    ?reason,
                    "room: join rejected"
                );
                0
            }
        }
    }

    /// Send `KIND_ROOM_JOINED` (room id + map + mode) to a participant that just
    /// entered `room_id`. Returns 1 if delivered.
    fn reply_joined(&self, sender: ParticipantId, room_id: RoomId, label: RoomLabel) -> usize {
        let body = citadel_wire::room::RoomJoined {
            room_id,
            map: label.map,
            mode: label.mode,
        }
        .encode();
        usize::from(self.send_reliable(sender, KIND_ROOM_JOINED, body))
    }

    /// Complete a previously committed room admission. The `ROOM_JOINED` frame
    /// remains first for compatibility, followed by the trusted room baseline.
    fn reply_joined_with_rep_bootstrap(
        &self,
        sender: ParticipantId,
        room_id: RoomId,
        label: RoomLabel,
    ) -> usize {
        self.reply_joined(sender, room_id, label) + self.send_rep_bootstrap(sender)
    }

    /// Remove `sender` from its room and notify the members that remain. Returns the
    /// number of `ROOM_LEAVE` notifications delivered.
    fn leave_room(&self, sender: ParticipantId) -> usize {
        let (leave, before) = {
            let _scope = self.lock_room_scope();
            let before = self.rooms.room_of(sender).and_then(|room_id| {
                self.rooms
                    .snapshot()
                    .into_iter()
                    .find(|room| room.id == room_id)
            });
            let leave = self.rooms.leave(sender);
            self.unbind_rep_connection_under_scope(sender);
            (leave, before)
        };
        let Some((room_id, remaining)) = leave else {
            return 0;
        };
        let ended = self.room_snapshot_for_lifecycle(room_id).is_none();
        self.forget_match_input_admission(room_id, sender);
        if let Some(mut room) = self
            .rooms
            .snapshot()
            .into_iter()
            .find(|room| room.id == room_id)
            .or(before)
        {
            room.members.retain(|member| *member != sender);
            let budget = self.native_match_budget();
            self.dispatch_match_lifecycle(
                NativeMatchLifecycleHook::Leave,
                room.clone(),
                None,
                budget,
            );
            if ended {
                self.dispatch_match_lifecycle(
                    NativeMatchLifecycleHook::Ended,
                    room,
                    Some(MatchTerminationReason::FinalDeparture),
                    budget,
                );
            }
        }
        // A leave that empties the room prunes it from the registry. Let a
        // process-hosting runtime adapter release the match's execution
        // context, exactly like [`Self::close_match`] does (embedded adapters
        // no-op); without this, an exodus-abandoned match ticks until worker
        // restart. Room ids are never reused, so a pruned id observed here
        // cannot belong to a later room.
        if remaining.is_empty() && self.rooms.label(room_id).is_none() {
            self.drop_bridge_match(room_id);
            if let Some(runtime) = &self.runtime {
                runtime.on_match_closed(room_id);
            }
        }
        let body = citadel_wire::room::RoomLeave { room_id }.encode();
        let mut sent = 0;
        for peer in remaining {
            if self.send_reliable(peer, KIND_ROOM_LEAVE, body.clone()) {
                sent += 1;
            }
        }
        sent
    }

    /// Resolve a room-create callback into a label plus its requested immutable
    /// bridge mode. The gateway—not the script—will validate the requested mode
    /// against readiness and persist the final room policy.
    fn room_spec_for_create(
        &self,
        sender: ParticipantId,
        params: &[u8],
    ) -> (RoomLabel, RoomBridgeMode) {
        let params_map = {
            let s = String::from_utf8_lossy(params);
            if s.is_empty() {
                "default".to_owned()
            } else {
                s.into_owned()
            }
        };
        let (label, bridge_mode) = if let Some(runtime) = &self.runtime {
            let user_id = self.registry.user_id_of(sender);
            if let Some(spec) = runtime.call_room_create(sender.get(), user_id.as_deref(), params) {
                let bridge_mode = spec.bridge_mode;
                (
                    RoomLabel {
                        map: if spec.map.is_empty() {
                            params_map
                        } else {
                            spec.map
                        },
                        mode: spec.mode,
                        max_players: spec.max_players,
                        open: spec.open,
                    },
                    bridge_mode,
                )
            } else {
                (RoomLabel::with_map(params_map), RoomBridgeMode::Relay)
            }
        } else {
            (RoomLabel::with_map(params_map), RoomBridgeMode::Relay)
        };
        self.log_map_resolution(&label.map);
        (label, bridge_mode)
    }

    /// Resolve a room's chosen `map` name against the loaded map catalog, logging
    /// whether cooked geometry is available.
    ///
    /// A missing map is only a warning: the room still forms and relay movement
    /// works without server-side geometry today. The geometry matters once the
    /// navmesh bake / pathfinding (Phase C) lands. The warning is what surfaces a
    /// typo in `on_room_create`'s map name or a map that was never cooked.
    fn log_map_resolution(&self, map_name: &str) {
        match self.maps.get(map_name) {
            Some(m) => tracing::info!(
                map = %map_name,
                verts = m.vertex_count(),
                tris = m.triangle_count(),
                "room map resolved to loaded geometry"
            ),
            None if self.maps.is_empty() => tracing::debug!(
                map = %map_name,
                "room map has no cooked .map (no maps loaded)"
            ),
            None => tracing::warn!(
                map = %map_name,
                available = ?self.maps.names().collect::<Vec<_>>(),
                "room map name has no matching .map in maps_dir"
            ),
        }
    }

    /// Handle a `NetworkPeer` replication frame (`KIND_REP_DELTA` /
    /// `KIND_REP_ACK`, ).
    ///
    /// A `DELTA` is **untrusted input**: it runs the whole
    /// validate -> apply -> rebroadcast pipeline; the server re-encodes its own
    /// authoritative delta to the relevant peers (never the client's bytes) and
    /// delivers each reliably. A reject produces no output — the coarse, uniform,
    /// no-oracle outcome (the client cannot tell which check failed). An `ACK`
    /// advances the server's rebroadcast baselines. Returns the number of frames
    /// delivered. A no-op returning 0 when no authority is attached.
    fn handle_rep_frame(&self, sender: ParticipantId, env: &Envelope) -> usize {
        let Some(rep) = &self.rep else {
            return 0;
        };
        if env.kind == KIND_REP_ACK {
            rep.handle_ack(sender.get(), &env.body);
            return 0;
        }
        // Authoritative match: a delta runs the structural validate stage and
        // becomes a ReplicatedVarWrite event; apply + rebroadcast (bypass B6)
        // happen only on the script's accept. Non-authoritative deployments keep
        // the direct validate -> apply -> rebroadcast pipeline unchanged.
        if let Some((room_id, binding)) = self.authoritative_match(sender) {
            return self.route_bridge_rep(sender, env, room_id, &binding);
        }
        let _scope = self.lock_room_scope();
        let proposed_object = citadel_wire::netpeer::DeltaBunch::peek_object_id(&env.body);
        if let Some(object_id) = proposed_object
            && let Some(room_id) = self.rep_object_room(object_id)
        {
            let source_is_bound = self
                .rep_rooms
                .lock()
                .ok()
                .is_some_and(|bindings| bindings.connections.get(&sender) == Some(&room_id));
            if !source_is_bound || self.rooms.room_of(sender) != Some(room_id) {
                return 0;
            }
        } else if self.rooms.room_of(sender).is_some() {
            // A room member may only mutate a room-bound object. This preserves
            // legacy roomless replication while denying any unscoped fallback
            // inside a match.
            return 0;
        }
        let now_ms = SystemClock.now().unix_millis();
        let mut delivered = 0;
        for out in rep.handle_delta(sender.get(), &env.body, now_ms) {
            // RepAuthority's match index is a server-side replication seam and
            // legacy sessions may still share its global match. The gateway is
            // the client-facing boundary: a delta proposed by a room member may
            // only cross that member's current RoomRegistry scope. Roomless
            // sessions intentionally remain one relay-compatible scope.
            let outbound = Outbound::new(
                if out.reliable {
                    Delivery::Reliable
                } else {
                    Delivery::Unreliable
                },
                Envelope::new(out.kind, out.body),
            );
            let out_bytes = outbound.envelope.body.len() as u64;
            let target = ParticipantId::from_raw(out.participant);
            let is_delta = out.kind == KIND_REP_DELTA;
            let accepted = if is_delta {
                match out.object_id.and_then(|object_id| {
                    self.rep_object_room(object_id)
                        .map(|room_id| (object_id, room_id))
                }) {
                    Some((object_id, room_id)) => self.send_bound_rep_object_under_scope(
                        Some(sender),
                        target,
                        object_id,
                        room_id,
                        &outbound,
                    ),
                    None if self.rooms.room_of(sender).is_none() => self
                        .rooms
                        .while_same_room(sender, target, || {
                            self.registry.send_to(target, &outbound)
                        })
                        .unwrap_or(false),
                    None => false,
                }
            } else {
                self.registry.send_to(target, &outbound)
            };
            if accepted {
                self.metrics.record_message_out(out_bytes);
                delivered += 1;
            }
        }
        delivered
    }

    /// Run one transform-sync sim + snapshot tick and fan out per-client delta
    /// snapshots on the unreliable path.
    ///
    /// Advances the authoritative world one sim step, latches the frame, builds
    /// one snapshot per registered transform-sync client from that latched frame,
    /// and delivers each via [`SessionRegistry::send_to`] unreliably. Returns the
    /// number of snapshots delivered. A no-op returning 0 when no hub is attached.
    pub fn transform_tick(&self) -> usize {
        self.transform_sim_step();
        self.transform_snapshot_step()
    }

    /// Advance the authoritative transform world **one sim step** (no snapshot).
    ///
    /// Called every sim step by [`TransformTickService`] so `server_tick` and the
    /// physics advance at `sim_hz` — the rate the client is told in the `HELLO` and
    /// uses to size its interpolation buffer. Snapshots are emitted separately by
    /// [`transform_snapshot_step`](Self::transform_snapshot_step) at a lower rate.
    pub fn transform_sim_step(&self) {
        if let Some(hub) = &self.transform {
            hub.sim_tick();
        }
    }

    /// Build one delta snapshot per registered transform-sync client from the
    /// latched frame and fan them out on the unreliable path. Does **not** advance
    /// the world. Returns the number of snapshots delivered (0 when no hub).
    pub fn transform_snapshot_step(&self) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        let mut delivered = 0;
        for out in hub.snapshot_tick_scoped(|participant| {
            self.rooms.room_of(ParticipantId::from_raw(participant))
        }) {
            let out_bytes = out.body.len() as u64;
            if self.deliver_transform_snapshot(out) {
                self.metrics.record_message_out(out_bytes);
                delivered += 1;
            }
        }
        delivered
    }

    /// Deliver a snapshot only if its recipient and every source object still
    /// hold the room scope used to build it. The transform and membership locks
    /// stay held through the enqueue, so neither an object/owner move nor a
    /// recipient move can slip between validation and delivery.
    fn deliver_transform_snapshot(&self, out: crate::realtime::transform::HubOutbound) -> bool {
        let outbound = Outbound::new(
            if out.unreliable {
                Delivery::Unreliable
            } else {
                Delivery::Reliable
            },
            Envelope::new(out.kind, out.body.clone()),
        );
        let participant = ParticipantId::from_raw(out.participant);
        let Some(hub) = &self.transform else {
            return false;
        };
        let _scope = self.lock_room_scope();
        hub.while_snapshot_sources_current(&out, || {
            self.rooms.while_member_and_owners_in(
                participant,
                out.room_scope,
                &out.source_owners,
                || self.registry.send_to(participant, &outbound),
            )
        })
        .flatten()
        .unwrap_or(false)
    }

    /// The built-in position relay used when no script runtime is attached.
    fn relay_builtin(&self, sender: ParticipantId, env: &Envelope) -> usize {
        match env.kind {
            KIND_POSITION => {
                let body = protocol::tag_with_sender(sender.get(), &env.body);
                let relayed = Envelope::new(KIND_PEER_POSITION, body);
                // Positions are hot-path state: relay best-effort/unreliable.
                let outbound = Outbound::unreliable(relayed);
                let out_bytes = outbound.envelope.body.len() as u64;
                let _scope = self.lock_room_scope();
                let delivered = match self.rooms.room_of(sender) {
                    Some(room_id) => self
                        .rooms
                        .while_members_in(room_id, |members| {
                            if !members.contains(&sender) {
                                return 0;
                            }
                            members
                                .iter()
                                .filter(|&&member| member != sender)
                                .filter(|&&member| self.registry.send_to(member, &outbound))
                                .count()
                        })
                        .unwrap_or(0),
                    None => self.registry.broadcast_except(sender, &outbound),
                };
                for _ in 0..delivered {
                    self.metrics.record_message_out(out_bytes);
                }
                delivered
            }
            other => {
                tracing::debug!(%sender, kind = other, "gateway dropped unknown message kind");
                0
            }
        }
    }

    /// Apply script-produced [`OutboundCommand`]s to the session registry.
    ///
    /// `exclude` is the participant left out of broadcasts: `Some(sender)` for
    /// message dispatch and lifecycle hooks (never echo to the originator),
    /// `None` for the server tick (which broadcasts to everyone). Returns the
    /// total number of sessions the commands were delivered to, and records one
    /// outbound message per delivered copy so the dashboard gauges track
    /// script-driven traffic exactly like the built-in relay.
    fn apply_commands(
        &self,
        exclude: Option<ParticipantId>,
        commands: Vec<OutboundCommand>,
    ) -> usize {
        self.apply_commands_scoped(exclude, None, commands)
    }

    /// Apply commands whose broadcast side effects are restricted to `room_id`
    /// when it is present. Direct sends deliberately retain their explicit target
    /// semantics; a future multi-node router validates that target's ownership.
    fn apply_commands_scoped(
        &self,
        exclude: Option<ParticipantId>,
        room_id: Option<RoomId>,
        commands: Vec<OutboundCommand>,
    ) -> usize {
        // A command stream without a room identity is legacy global work. Once
        // more than one room is live, arbitrary script commands must not become
        // a client-facing state broadcast; match-local handlers use `Some` and
        // retain the normal scoped path below. The one-room/roomless behavior is
        // intentionally unchanged.
        if room_id.is_none() && self.rooms.room_count() > 1 {
            return 0;
        }
        let mut delivered_total = 0;
        for command in commands {
            match command {
                OutboundCommand::Broadcast {
                    kind,
                    body,
                    unreliable,
                } => {
                    let outbound =
                        Outbound::new(delivery_for(unreliable), Envelope::new(kind, body));
                    let out_bytes = outbound.envelope.body.len() as u64;
                    let delivered = match room_id {
                        Some(room_id) => {
                            self.send_to_room_members(room_id, exclude, exclude, &outbound)
                        }
                        None => match exclude {
                            Some(sender) => self.registry.broadcast_except(sender, &outbound),
                            None => self.registry.broadcast_all(&outbound),
                        },
                    };
                    for _ in 0..delivered {
                        self.metrics.record_message_out(out_bytes);
                    }
                    delivered_total += delivered;
                }
                OutboundCommand::Send {
                    session,
                    kind,
                    body,
                    unreliable,
                } => {
                    let outbound =
                        Outbound::new(delivery_for(unreliable), Envelope::new(kind, body));
                    let out_bytes = outbound.envelope.body.len() as u64;
                    let target = ParticipantId::from_raw(session);
                    let delivered = match room_id {
                        Some(room_id) => {
                            let _scope = self.lock_room_scope();
                            let owners: Vec<u64> = exclude
                                .into_iter()
                                .map(|participant| participant.get())
                                .collect();
                            self.rooms
                                .while_member_and_owners_in(target, Some(room_id), &owners, || {
                                    self.registry.send_to(target, &outbound)
                                })
                                .unwrap_or(false)
                        }
                        None => self.registry.send_to(target, &outbound),
                    };
                    if delivered {
                        self.metrics.record_message_out(out_bytes);
                        delivered_total += 1;
                    }
                }
                OutboundCommand::SpawnActor {
                    object_id,
                    archetype,
                    position,
                } => {
                    delivered_total +=
                        self.spawn_server_actor(object_id, archetype, position, room_id);
                }
                OutboundCommand::MoveActor {
                    object_id,
                    position,
                    rotation,
                    velocity,
                } => {
                    if let (Some(object_id), Some(hub)) =
                        (self.command_object_id(room_id, object_id), &self.transform)
                    {
                        hub.set_transform(
                            object_id,
                            crate::realtime::transform::TransformState {
                                position,
                                rotation,
                                velocity,
                            },
                        );
                    }
                }
                OutboundCommand::SetPhysics { object_id, opts } => {
                    if let (Some(object_id), Some(hub)) =
                        (self.command_object_id(room_id, object_id), &self.transform)
                    {
                        if opts.is_some_and(|opts| opts.enabled) {
                            self.select_physics_map(hub, room_id);
                        }
                        hub.set_physics(object_id, opts);
                    }
                }
                OutboundCommand::ApplyImpulse { object_id, impulse } => {
                    if let (Some(object_id), Some(hub)) =
                        (self.command_object_id(room_id, object_id), &self.transform)
                    {
                        hub.apply_impulse(object_id, impulse);
                    }
                }
                OutboundCommand::SetMoveIntent { object_id, intent } => {
                    if let (Some(object_id), Some(hub)) =
                        (self.command_object_id(room_id, object_id), &self.transform)
                    {
                        hub.set_move_intent(object_id, intent);
                    }
                }
                OutboundCommand::SetInputAck { .. } => {
                    tracing::debug!(
                        "dropped internal input acknowledgement outside validated bridge materialization"
                    );
                }
                OutboundCommand::DespawnActor { object_id } => {
                    delivered_total += self.despawn_server_actor(object_id, room_id);
                }
            }
        }
        delivered_total
    }

    /// Resolve the command's active room map and hand its collision mesh to the
    /// transform hub. The hub caches the resulting BVH by map name, so this is a
    /// command/map-selection path only and never simulation-tick work.
    fn select_physics_map(&self, hub: &TransformHub, room_id: Option<RoomId>) {
        let Some(map_name) = room_id
            .and_then(|room_id| self.rooms.label(room_id))
            .map(|label| label.map)
        else {
            hub.set_physics_map(None);
            return;
        };
        let Some(map) = self.maps.get(&map_name) else {
            hub.set_physics_map(None);
            return;
        };
        hub.set_physics_map(Some((&map_name, &map.file.collision)));
    }

    /// Spawn a server-owned NPC: place it in the transform world, remember its
    /// room scope, and fan out an `NA_SPAWN` (owner `0` = server) to clients in
    /// that scope. Movement then flows through the snapshot path.
    fn spawn_server_actor(
        &self,
        object_id: u32,
        archetype: u16,
        position: [f32; 3],
        room_id: Option<RoomId>,
    ) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        let key = (room_id, object_id);
        let actual_object_id = {
            let Ok(mut npcs) = self.npcs.lock() else {
                return 0;
            };
            if let Some(existing) = npcs.get_mut(&key) {
                existing.archetype_id = archetype;
                existing.object_id
            } else {
                let identity_conflicts = npcs.values().any(|npc| npc.object_id == object_id);
                let actual_object_id = if room_id.is_some() || identity_conflicts {
                    let Some(id) = self.allocate_scoped_actor_id(&npcs, hub) else {
                        return 0;
                    };
                    id
                } else {
                    object_id
                };
                npcs.insert(
                    key,
                    NpcEntry {
                        object_id: actual_object_id,
                        archetype_id: archetype,
                        room_id,
                    },
                );
                actual_object_id
            }
        };
        hub.set_object_room(actual_object_id, room_id);
        hub.set_transform(
            actual_object_id,
            crate::realtime::transform::TransformState {
                position,
                rotation: [0.0, 0.0, 0.0, 1.0],
                velocity: [0.0; 3],
            },
        );
        let spawn = citadel_wire::na::NaSpawn {
            object_id: actual_object_id,
            archetype_id: archetype,
            owner: 0,
            transform: citadel_wire::na::NaTransform {
                position,
                rotation: [0.0, 0.0, 0.0, 1.0],
                velocity: [0.0; 3],
            },
        };
        let outbound = Outbound::reliable(Envelope::new(KIND_NA_SPAWN, spawn.encode()));
        match room_id {
            Some(room_id) => self.send_to_room_members(room_id, None, None, &outbound),
            None => self.registry.broadcast_all(&outbound),
        }
    }

    /// Despawn a server-owned NPC: drop it from the world + registry and fan out an
    /// `NA_DESPAWN` to every client.
    fn despawn_server_actor(&self, script_object_id: u32, room_id: Option<RoomId>) -> usize {
        let room_id = self
            .npcs
            .lock()
            .ok()
            .and_then(|mut npcs| npcs.remove(&(room_id, script_object_id)));
        let Some(npc) = room_id else {
            return 0;
        };
        if let Some(hub) = &self.transform {
            hub.despawn(npc.object_id);
        }
        let body = citadel_wire::na::NaDespawn {
            object_id: npc.object_id,
        }
        .encode();
        let outbound = Outbound::reliable(Envelope::new(KIND_NA_DESPAWN, body));
        match npc.room_id {
            Some(room_id) => self.send_to_room_members(room_id, None, None, &outbound),
            None => self.registry.broadcast_all(&outbound),
        }
    }

    /// Resolve one script-visible actor id inside the command's room scope.
    /// Commands without a matching scoped actor are ignored rather than allowed
    /// to mutate a same-numbered actor belonging to another room.
    fn actor_object_id(&self, room_id: Option<RoomId>, script_object_id: u32) -> Option<u32> {
        self.npcs.lock().ok().and_then(|npcs| {
            npcs.get(&(room_id, script_object_id))
                .map(|npc| npc.object_id)
        })
    }

    /// Resolve a command target without changing the legacy unscoped path for
    /// transform objects that are not script-owned actors. A room-scoped command
    /// deliberately has no raw-id fallback: that would reintroduce cross-room
    /// mutation of a same-numbered actor.
    fn command_object_id(&self, room_id: Option<RoomId>, script_object_id: u32) -> Option<u32> {
        self.actor_object_id(room_id, script_object_id)
            .or_else(|| room_id.is_none().then_some(script_object_id))
    }

    fn allocate_scoped_actor_id(
        &self,
        npcs: &HashMap<NpcKey, NpcEntry>,
        hub: &TransformHub,
    ) -> Option<u32> {
        let mut next = self.next_scoped_actor_id.lock().ok()?;
        let first = *next;
        loop {
            let candidate = *next;
            *next = if candidate == u32::MAX {
                0x8000_0000
            } else {
                candidate + 1
            };
            if !npcs.values().any(|npc| npc.object_id == candidate)
                && !hub.contains_object(candidate)
            {
                return Some(candidate);
            }
            if *next == first {
                return None;
            }
        }
    }
}

/// Map a script's `unreliable` flag to a transport delivery mode.
fn delivery_for(unreliable: bool) -> Delivery {
    if unreliable {
        Delivery::Unreliable
    } else {
        Delivery::Reliable
    }
}

/// Classify a connection's first inbound envelope into a presented credential,
/// and whether that frame is a legacy first frame that should be replayed after
/// an implicit-guest acceptance.
///
/// - `KIND_AUTH` with an empty body: an explicit guest request.
/// - `KIND_AUTH` with a body: a session token (utf-8). A non-utf8 or oversized
///   body cannot be a valid token, so it is a malformed-token auth failure.
/// - any other kind: a pre-handshake/legacy client (replay the frame if the
///   stance accepts it as a guest).
fn classify_handshake(first: &Envelope) -> (PresentedCredential, bool) {
    if first.kind != KIND_AUTH {
        return (PresentedCredential::NoHandshake, true);
    }
    if first.body.is_empty() {
        return (PresentedCredential::Guest, false);
    }
    match std::str::from_utf8(&first.body) {
        Ok(token) => match SessionTokenSecret::new(token) {
            Ok(secret) => (PresentedCredential::Token(secret), false),
            Err(_) => (PresentedCredential::MalformedToken, false),
        },
        Err(_) => (PresentedCredential::MalformedToken, false),
    }
}

impl Default for Gateway {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience: build a shared gateway handle.
#[must_use]
pub fn shared() -> Arc<Gateway> {
    Arc::new(Gateway::new())
}

/// One participant's bounded fixed-window explicit-input admission counters.
struct MatchInputRateWindow {
    started_at: Instant,
    messages: usize,
    bytes: usize,
}

impl MatchInputRateWindow {
    fn new(now: Instant) -> Self {
        Self {
            started_at: now,
            messages: 0,
            bytes: 0,
        }
    }
}

/// Per-node authoritative-bridge state: one [`PendingBatchLedger`] per live
/// authoritative match plus the per-batch quotas. Held behind the gateway's
/// optional `bridge` field; absent on non-authoritative deployments.
struct GatewayBridge {
    /// One ledger per authoritative match (keyed by `RoomId`). Created lazily on
    /// the match's first protected frame, dropped when the match closes.
    ledgers: Mutex<HashMap<RoomId, PendingBatchLedger>>,
    /// Per-active-participant ingress windows for explicit V1 input. Entries are
    /// removed on leave/close; each window is fixed at one minute.
    input_rate_windows: Mutex<HashMap<(RoomId, ParticipantId), MatchInputRateWindow>>,
    /// Per-batch quotas the validator enforces (from `[runtime.bridge]`;
    /// PROVISIONAL measure-first defaults).
    quotas: BridgeQuotas,
    /// Capabilities the deployment grants scripts (from `[runtime.bridge]`).
    /// A capability-gated command family (Persist/Schedule/Physics) is rejected
    /// unless its capability is present. Deployment-wide until the revision store
    /// declares capabilities per revision.
    capabilities: std::collections::HashSet<Capability>,
    /// Replicated-delta writes awaiting a script answer, keyed by `(RoomId,
    /// event_id)`. A `KIND_REP_DELTA` validates to a non-serializable
    /// [`Validated`] proposal that only [`RepAuthority::apply_and_rebroadcast`]
    /// can materialize, so the proposal is stashed here (on the gateway, never
    /// crossing the worker boundary) and applied on the matching `Accept`. Only
    /// the decoded field values ride the normalized event. Dropped on any other
    /// outcome and when the match closes.
    pending_rep: Mutex<HashMap<(RoomId, u64), Validated>>,
}

impl GatewayBridge {
    fn new(quotas: BridgeQuotas, capabilities: std::collections::HashSet<Capability>) -> Self {
        Self {
            ledgers: Mutex::new(HashMap::new()),
            input_rate_windows: Mutex::new(HashMap::new()),
            quotas,
            capabilities,
            pending_rep: Mutex::new(HashMap::new()),
        }
    }
}

/// One match's view of the facts the validator queries, backed by the live
/// `RoomRegistry`/`TransformHub`. Constructed per validation; borrows the
/// gateway read-only.
struct GatewayBridgeContext<'a> {
    gateway: &'a Gateway,
    room_id: RoomId,
}

impl BridgeMatchContext for GatewayBridgeContext<'_> {
    fn is_member(&self, participant: u64) -> bool {
        self.gateway
            .rooms
            .members(self.room_id)
            .contains(&ParticipantId::from_raw(participant))
    }

    fn object_in_match(&self, object_id: u32) -> bool {
        self.gateway
            .transform
            .as_ref()
            .and_then(|hub| hub.object_room(object_id))
            == Some(self.room_id)
    }

    fn rep_value_in_bounds(&self, object_id: u32, field_id: u16, value: &BridgeRepValue) -> bool {
        // Query the object's real RepLayout bounds. A script value is exact —
        // out of bounds is rejected, never clamped. No authority attached ⇒ no
        // replicated object exists ⇒ fail closed.
        match &self.gateway.rep {
            Some(rep) => rep.value_in_bounds(object_id, field_id, &value.clone().into()),
            None => false,
        }
    }

    fn has_capability(&self, capability: Capability) -> bool {
        // Granted deployment-wide via `[runtime.bridge]` (opt-in); per-revision
        // capability declaration is a later step.
        self.gateway
            .bridge
            .as_ref()
            .is_some_and(|bridge| bridge.capabilities.contains(&capability))
    }
}

impl Gateway {
    /// The authoritative match `sender` participates in, with its script
    /// binding, or `None` when the bridge is disabled or the room is not bound
    /// to a script (a non-authoritative deployment or roomless participant).
    fn authoritative_match(&self, sender: ParticipantId) -> Option<(RoomId, ScriptBinding)> {
        self.bridge.as_ref()?;
        let room_id = self.rooms.room_of(sender)?;
        if self.rooms.bridge_mode(room_id) != Some(BridgeMode::Authoritative) {
            return None;
        }
        let binding = self.rooms.binding(room_id)?;
        // A room binding is a generation fence, not merely metadata. On nodes
        // with readiness authority, reject any stale/degraded/reloading binding
        // before a protected frame can enter the replacement runtime.
        if let Some(readiness) = &self.script_readiness
            && readiness.gate().ok().as_ref() != Some(&binding)
        {
            return None;
        }
        Some((room_id, binding))
    }

    /// Issue a protected event only while its local originator is still bound
    /// to the match. The lock is released before the runtime callback, because
    /// an embedded validator may answer inline; materialization repeats the
    /// same test under the gate before it mutates state.
    fn deliver_bridge_batch_for_member(
        &self,
        sender: ParticipantId,
        room_id: RoomId,
        binding: &ScriptBinding,
        drafts: Vec<EventDraft>,
    ) {
        let _generation = self
            .generation_gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let scope = self.lock_room_scope();
        if self.rooms.room_of(sender) != Some(room_id)
            || self.rooms.binding(room_id).as_ref() != Some(binding)
        {
            return;
        }
        let batch = self.issue_bridge_batch(room_id, binding, drafts);
        drop(scope);
        if let (Some(batch), Some(runtime)) = (batch, &self.runtime) {
            runtime.deliver_event_batch(batch);
        }
    }

    /// Bind bridge-ingress objects and issue their normalized event batch at one
    /// room-scope linearization point. The server-owned membership and current
    /// room script binding are rechecked before any `object_rooms` write, so a
    /// frame captured before a move or reload cannot leave a stale room binding
    /// behind even though its runtime delivery is asynchronous.
    fn bind_bridge_objects_and_deliver_for_member(
        &self,
        sender: ParticipantId,
        room_id: RoomId,
        binding: &ScriptBinding,
        object_ids: &[u32],
        drafts: Vec<EventDraft>,
    ) {
        let _generation = self
            .generation_gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let batch = {
            let _scope = self.lock_room_scope();
            if self.rooms.room_of(sender) != Some(room_id)
                || self.rooms.binding(room_id).as_ref() != Some(binding)
            {
                return;
            }
            let Some(batch) = self.issue_bridge_batch(room_id, binding, drafts) else {
                return;
            };
            if let Some(hub) = &self.transform {
                for &object_id in object_ids {
                    hub.set_object_room(object_id, Some(room_id));
                }
            }
            batch
        };
        if let Some(runtime) = &self.runtime {
            runtime.deliver_event_batch(batch);
        }
    }

    /// Issue `drafts` to `room_id`'s ledger (bound to `binding`) and return the
    /// fenced batch, without delivering it. Callers that must correlate extra
    /// per-event state to the assigned event ids (the replicated-delta stash)
    /// issue here, record their state, then deliver. The ledger lock is released
    /// before this returns.
    fn issue_bridge_batch(
        &self,
        room_id: RoomId,
        binding: &ScriptBinding,
        drafts: Vec<EventDraft>,
    ) -> Option<NormalizedEventBatch> {
        let bridge = self.bridge.as_ref()?;
        let (clock_epoch, tick) = self
            .transform
            .as_ref()
            .and_then(|hub| hub.gameplay_clock())
            .map(|clock| (clock.epoch, clock.tick))
            .unwrap_or((0, 0));
        let mut ledgers = bridge.ledgers.lock().unwrap_or_else(|e| e.into_inner());
        let total_pending: usize = ledgers.values().map(PendingBatchLedger::pending_len).sum();
        let ledger = ledgers
            .entry(room_id)
            .or_insert_with(|| PendingBatchLedger::new(room_id, binding.generation, clock_epoch));
        // A reload (new generation) or a clock reset clears any batch the
        // superseded turn was still waiting on, so a stale answer can never
        // resurrect it.
        if ledger.generation() != binding.generation {
            ledger.advance_generation(binding.generation);
        }
        if ledger.clock_epoch() != clock_epoch {
            ledger.set_clock_epoch(clock_epoch);
        }
        if ledger.pending_len() >= bridge.quotas.max_pending_batches
            || total_pending >= bridge.quotas.max_pending_batches_total
        {
            tracing::debug!(
                room_id = %room_id,
                pending = ledger.pending_len(),
                total_pending,
                per_match_cap = bridge.quotas.max_pending_batches,
                total_cap = bridge.quotas.max_pending_batches_total,
                "bridge dropped ingress because pending batch capacity is exhausted"
            );
            return None;
        }
        Some(ledger.issue(drafts, tick))
    }

    /// Structural stage for a generic custom envelope in an authoritative
    /// match. The room and script binding came only from server registries;
    /// membership and the current binding are rechecked under the room gate
    /// before a bounded opaque payload can reach on_input. Ledger issue then
    /// supplies the version/generation/clock/tick/batch fences.
    fn route_bridge_match_message(
        &self,
        sender: ParticipantId,
        env: &Envelope,
        room_id: RoomId,
        binding: &ScriptBinding,
        metadata: InboundMessageMetadata,
    ) -> usize {
        if env.body.len() > MAX_MATCH_MESSAGE_BODY_BYTES {
            tracing::debug!(
                %sender,
                kind = env.kind,
                bytes = env.body.len(),
                max_bytes = MAX_MATCH_MESSAGE_BODY_BYTES,
                "bridge dropped oversized custom client message before runtime"
            );
            return 0;
        }
        let draft = EventDraft {
            participant: sender.get(),
            user_id: self.registry.user_id_of(sender),
            payload: NormalizedPayload::MatchMessage {
                kind: env.kind,
                body: env.body.to_vec(),
                reliable: metadata.reliable,
                sequence: metadata.sequence,
            },
        };
        self.deliver_bridge_batch_for_member(sender, room_id, binding, vec![draft]);
        0
    }

    /// Atomically consume one bounded explicit-input admission slot for the
    /// current `(room, participant)` pair. The caller has already authenticated
    /// the sender and decoded a bounded V1 body. Zero limits intentionally deny
    /// all input; an operator cannot accidentally turn an exhausted limiter into
    /// unlimited admission.
    fn admit_match_input(&self, room_id: RoomId, sender: ParticipantId, body_bytes: usize) -> bool {
        let _scope = self.lock_room_scope();
        if self.rooms.room_of(sender) != Some(room_id) {
            return false;
        }
        let Some(bridge) = &self.bridge else {
            return false;
        };
        let now = Instant::now();
        let mut windows = bridge
            .input_rate_windows
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let window = windows
            .entry((room_id, sender))
            .or_insert_with(|| MatchInputRateWindow::new(now));
        if now.duration_since(window.started_at) >= Duration::from_secs(60) {
            *window = MatchInputRateWindow::new(now);
        }
        let next_messages = window.messages.saturating_add(1);
        let next_bytes = window.bytes.saturating_add(body_bytes);
        if next_messages > bridge.quotas.max_match_input_messages_per_minute
            || next_bytes > bridge.quotas.max_match_input_bytes_per_minute
        {
            tracing::debug!(
                %sender,
                room_id = %room_id,
                "bridge dropped explicit match input at configured ingress rate limit"
            );
            return false;
        }
        window.messages = next_messages;
        window.bytes = next_bytes;
        true
    }

    /// Structural stage for an explicit V1 match-input envelope. Decoding is
    /// bounded before body allocation; the gateway installs authenticated sender
    /// identity and authoritative room/binding before the opaque game bytes cross
    /// the bridge. A transport sequence is deliberately not substituted here:
    /// the V1 body carries the exact game input sequence.
    fn route_bridge_match_input(
        &self,
        sender: ParticipantId,
        env: &Envelope,
        room_id: RoomId,
        binding: &ScriptBinding,
        metadata: InboundMessageMetadata,
    ) -> usize {
        if !metadata.reliable {
            tracing::debug!(%sender, "bridge dropped unreliable explicit match input");
            return 0;
        }
        let Ok(input) = MatchInput::decode(&env.body) else {
            tracing::debug!(%sender, "bridge dropped malformed explicit match input");
            return 0;
        };
        if !self.admit_match_input(room_id, sender, input.body.len()) {
            return 0;
        }
        let draft = EventDraft {
            participant: sender.get(),
            user_id: self.registry.user_id_of(sender),
            payload: NormalizedPayload::MatchMessage {
                kind: KIND_MATCH_INPUT,
                body: input.body,
                reliable: metadata.reliable,
                sequence: Some(input.sequence),
            },
        };
        self.deliver_bridge_batch_for_member(sender, room_id, binding, vec![draft]);
        0
    }

    /// Structural stage for an authoritative `KIND_REP_DELTA`: run the rep
    /// authority's validate stage (schema, ownership, bounds, rate — no state
    /// mutation), issue a `ReplicatedVarWrite` normalized event carrying the
    /// decoded scalar field values, and stash the non-serializable validated
    /// proposal by event id. Apply + rebroadcast happen only on the script's
    /// accept. A structural reject produces no event and no output (the coarse,
    /// no-oracle outcome). Returns 0.
    fn route_bridge_rep(
        &self,
        sender: ParticipantId,
        env: &Envelope,
        room_id: RoomId,
        binding: &ScriptBinding,
    ) -> usize {
        let _generation = self
            .generation_gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(rep) = &self.rep else {
            return 0;
        };
        let Some(bridge) = &self.bridge else {
            return 0;
        };
        let scope = self.lock_room_scope();
        let now_ms = SystemClock.now().unix_millis();
        let Ok(validated) = rep.validate(sender.get(), &env.body, now_ms) else {
            return 0;
        };
        let source_is_bound = self.rep_rooms.lock().ok().is_some_and(|bindings| {
            bindings.objects.get(&validated.object_id()) == Some(&room_id)
                && bindings.connections.get(&sender) == Some(&room_id)
        });
        if !source_is_bound || self.rooms.room_of(sender) != Some(room_id) {
            return 0;
        }
        let fields: Vec<BridgeRepField> = validated
            .fields()
            .iter()
            .filter_map(|(field_id, delta)| match delta {
                FieldDelta::Value(value) => Some(BridgeRepField {
                    field_id: *field_id,
                    value: value.clone().into(),
                }),
                // Collection deltas are opaque to the script in v1; they still
                // apply via the stashed proposal on accept.
                FieldDelta::Collection(_) => None,
            })
            .collect();
        let draft = EventDraft {
            participant: sender.get(),
            user_id: self.registry.user_id_of(sender),
            payload: NormalizedPayload::ReplicatedVarWrite {
                object_id: validated.object_id(),
                class_id: validated.class_id(),
                // PROVISIONAL: the exact 128-bit schema hash is not yet surfaced
                // from Validated; the script keys on class_id/object_id in v1.
                schema_hash: [0u8; 16],
                result_id: validated.result_id(),
                fields,
            },
        };
        let Some(batch) = self.issue_bridge_batch(room_id, binding, vec![draft]) else {
            return 0;
        };
        if let Some(event) = batch.events.first() {
            bridge
                .pending_rep
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert((room_id, event.event_id), validated);
        }
        drop(scope);
        if let Some(runtime) = &self.runtime {
            runtime.deliver_event_batch(batch);
        }
        0
    }

    /// Structural stage for an authoritative `KIND_NA_STATE` report: decode,
    /// verify the sender owns the object in relay mode, sanitize the transform,
    /// then issue an `ActorStateReport` normalized event. Nothing mutates here —
    /// the write happens only if the script's validated answer accepts/corrects
    /// it. Returns 0 (no synchronous reply).
    fn route_bridge_na_state(
        &self,
        sender: ParticipantId,
        env: &Envelope,
        room_id: RoomId,
        binding: &ScriptBinding,
    ) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        let Ok(state) = citadel_wire::na::NaState::decode(&env.body) else {
            tracing::debug!(%sender, "bridge dropped a malformed NA_STATE");
            return 0;
        };
        // Structural ownership: only the owner's own object becomes an event.
        if hub.relay_owned_object(sender.get()) != Some(state.object_id) {
            return 0;
        }
        // Structural sanitize: a non-finite/degenerate report never reaches the
        // script (F30).
        let Some(transform) = hub.sanitize_report(state.transform) else {
            return 0;
        };
        let draft = EventDraft {
            participant: sender.get(),
            user_id: self.registry.user_id_of(sender),
            payload: NormalizedPayload::ActorStateReport {
                object_id: state.object_id,
                transform: transform.into(),
            },
        };
        self.bind_bridge_objects_and_deliver_for_member(
            sender,
            room_id,
            binding,
            &[state.object_id],
            vec![draft],
        );
        0
    }

    /// Structural stage for an authoritative `KIND_NA_PRESENCE`: decode +
    /// sanitize the requested transform, then issue a `SpawnRequest` normalized
    /// event. No object is registered and nothing is fanned out here — that
    /// happens only on the script's accept/correct. Returns 0.
    fn route_bridge_presence(
        &self,
        sender: ParticipantId,
        env: &Envelope,
        room_id: RoomId,
        binding: &ScriptBinding,
    ) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        let Ok(presence) = citadel_wire::na::NaPresence::decode(&env.body) else {
            tracing::debug!(%sender, "bridge dropped a malformed NA_PRESENCE");
            return 0;
        };
        let Some(transform) = hub.sanitize_report(presence.transform) else {
            return 0;
        };
        let draft = EventDraft {
            participant: sender.get(),
            user_id: self.registry.user_id_of(sender),
            payload: NormalizedPayload::SpawnRequest {
                archetype_id: presence.archetype_id,
                transform: transform.into(),
            },
        };
        self.deliver_bridge_batch_for_member(sender, room_id, binding, vec![draft]);
        0
    }

    /// Structural stage for authoritative owner input: for each frame the sender
    /// owns (ownership + epoch + finite move), issue a `TransformInput`
    /// normalized event. Nothing integrates here — `apply_owner_input` runs only
    /// on the script's accept. Returns 0 (no synchronous reply).
    fn route_bridge_input(
        &self,
        sender: ParticipantId,
        frames: &[citadel_wire::tsync::InputFrame],
        room_id: RoomId,
        binding: &ScriptBinding,
    ) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        let user_id = self.registry.user_id_of(sender);
        let mut drafts = Vec::new();
        let mut object_ids = Vec::new();
        for frame in frames {
            // Structural: only the owner's own object at the right epoch, with a
            // finite movement intent, becomes an event.
            if !hub.input_ownership_ok(sender.get(), frame.object_id, frame.ownership_epoch) {
                continue;
            }
            if frame.move_velocity.iter().any(|v| !v.is_finite()) || !frame.dt.is_finite() {
                continue;
            }
            object_ids.push(frame.object_id);
            drafts.push(EventDraft {
                participant: sender.get(),
                user_id: user_id.clone(),
                payload: NormalizedPayload::TransformInput {
                    object_id: frame.object_id,
                    ownership_epoch: frame.ownership_epoch,
                    input_seq: frame.input_seq,
                    sim_tick: frame.sim_tick,
                    dt: frame.dt,
                    move_velocity: frame.move_velocity,
                    payload: frame.payload.clone(),
                    fire: frame.fire.map(|fire| FireIntent {
                        origin: fire.origin,
                        direction: fire.direction,
                        weapon: 0,
                    }),
                },
            });
        }
        if drafts.is_empty() {
            return 0;
        }
        self.bind_bridge_objects_and_deliver_for_member(
            sender,
            room_id,
            binding,
            &object_ids,
            drafts,
        );
        0
    }

    /// Record only already-validated per-event decisions at the gateway choke
    /// point. The mapping intentionally omits the event payload, participant,
    /// account, reply, correction, and script commands.
    fn record_validated_decisions(&self, batch: &ValidatedBatch) {
        let Some(recorder) = &self.authoritative_decision_recorder else {
            return;
        };
        for validated in &batch.outcomes {
            let (outcome, reason) = match &validated.decision {
                Decision::Accept => (
                    AuthoritativeDecisionOutcome::Accepted,
                    AuthoritativeDecisionReason::NotApplicable,
                ),
                Decision::Reject { reason_code } => (
                    AuthoritativeDecisionOutcome::Rejected,
                    AuthoritativeDecisionReason::OpaqueCode(*reason_code),
                ),
                Decision::Correct { .. } => (
                    AuthoritativeDecisionOutcome::Corrected,
                    AuthoritativeDecisionReason::NotApplicable,
                ),
            };
            recorder.record(
                AuthoritativeDecisionCorrelation::new(
                    batch.match_id,
                    batch.batch_id,
                    validated.event.event_id,
                ),
                outcome,
                reason,
            );
        }
    }

    /// Materialize one fully validated batch: apply each accepted/corrected
    /// outcome's canonical effect, then apply the script-originated commands
    /// room-scoped. Called only from [`BridgeCommandSink::deliver_command_batch`]
    /// after the validator accepted the whole batch. Returns deliveries.
    fn materialize_validated_batch(&self, room_id: RoomId, batch: ValidatedBatch) -> usize {
        let mut delivered = 0;
        for outcome in batch.outcomes {
            delivered += self.materialize_outcome(room_id, &outcome);
        }
        for command in batch.commands {
            if let ScriptCommand::SetInputAck {
                participant,
                sequence,
            } = command
            {
                delivered += self.send_match_input_ack(room_id, participant, sequence);
                continue;
            }
            let exclude = match &command {
                ScriptCommand::BroadcastMatch { exclude, .. } => {
                    exclude.map(ParticipantId::from_raw)
                }
                _ => None,
            };
            let mut commands = Vec::new();
            push_outbound_from_script_command(&mut commands, command);
            delivered += self.apply_commands_scoped(exclude, Some(room_id), commands);
        }
        delivered
    }

    fn send_match_input_ack(&self, room_id: RoomId, participant: u64, sequence: u64) -> usize {
        let participant = ParticipantId::from_raw(participant);
        let _scope = self.lock_room_scope();
        if self.rooms.room_of(participant) != Some(room_id) {
            return 0;
        }
        let body = MatchInputAck {
            last_processed_sequence: sequence,
        }
        .encode();
        usize::from(self.send_reliable_in_scope_under_scope(
            participant,
            Some(room_id),
            KIND_MATCH_INPUT_ACK,
            body,
        ))
    }

    /// Materialize one validated per-event outcome. `Reject` mutates nothing.
    fn materialize_outcome(&self, room_id: RoomId, outcome: &ValidatedOutcome) -> usize {
        // A replicated-delta event carries a stashed, non-serializable proposal
        // keyed by event id: take it now (dropped unless this outcome accepts).
        let pending_rep = self.take_pending_rep(room_id, outcome.event.event_id);
        match &outcome.decision {
            Decision::Accept => {
                if let Some(validated) = pending_rep {
                    return self.materialize_rep_apply(room_id, validated);
                }
                let _scope = self.lock_room_scope();
                self.materialize_accept_under_scope(
                    room_id,
                    &outcome.event.payload,
                    outcome.event.participant,
                )
            }
            Decision::Correct { correction } => {
                if let (Some(validated), Correction::ReplicatedVars { fields }) =
                    (pending_rep, correction)
                {
                    return self.materialize_rep_apply(
                        room_id,
                        validated.with_corrected_scalar_fields(
                            fields
                                .iter()
                                .map(|field| (field.field_id, field.value.clone().into()))
                                .collect(),
                        ),
                    );
                }
                let _scope = self.lock_room_scope();
                self.materialize_correction_under_scope(
                    room_id,
                    &outcome.event.payload,
                    outcome.event.participant,
                    correction,
                )
            }
            Decision::Reject { .. } => 0,
        }
        // NOTE: InputOutcome::reply delivery is deferred — it needs a dedicated
        // reply wire kind (a citadel-wire + contract-manifest change), tracked
        // for a later step. The validator already bounds reply size.
    }

    /// Take (and remove) the stashed replicated-delta proposal for one event.
    fn take_pending_rep(&self, room_id: RoomId, event_id: u64) -> Option<Validated> {
        self.bridge
            .as_ref()?
            .pending_rep
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(room_id, event_id))
    }

    /// Apply an accepted replicated-delta proposal and fan out the server's
    /// authoritative rebroadcast (never the client's bytes). Returns deliveries.
    fn materialize_rep_apply(&self, room_id: RoomId, validated: Validated) -> usize {
        let Some(rep) = &self.rep else {
            return 0;
        };
        let source = ParticipantId::from_raw(validated.connection_id());
        let _scope = self.lock_room_scope();
        let source_is_bound = self.rep_rooms.lock().ok().is_some_and(|bindings| {
            bindings.objects.get(&validated.object_id()) == Some(&room_id)
                && bindings.connections.get(&source) == Some(&room_id)
        });
        if !source_is_bound || self.rooms.room_of(source) != Some(room_id) {
            return 0;
        }
        let outs = match rep.apply_and_rebroadcast(validated) {
            Ok(outs) => outs,
            Err(_) => return 0,
        };
        let mut delivered = 0;
        for out in outs {
            let Some(object_id) = out.object_id else {
                continue;
            };
            if self.rep_object_room(object_id) != Some(room_id) {
                continue;
            }
            let outbound = Outbound::new(
                if out.reliable {
                    Delivery::Reliable
                } else {
                    Delivery::Unreliable
                },
                Envelope::new(out.kind, out.body),
            );
            let out_bytes = outbound.envelope.body.len() as u64;
            let target = ParticipantId::from_raw(out.participant);
            if self.send_bound_rep_object_under_scope(
                Some(source),
                target,
                object_id,
                room_id,
                &outbound,
            ) {
                self.metrics.record_message_out(out_bytes);
                delivered += 1;
            }
        }
        delivered
    }

    /// Apply the client's own canonical effect while the room transaction gate
    /// is held. A delayed validator answer is discarded unless its originator
    /// is still in the exact issuing room; this prevents an A input becoming B
    /// state after a move, leave, or close.
    fn materialize_accept_under_scope(
        &self,
        room_id: RoomId,
        payload: &NormalizedPayload,
        participant: u64,
    ) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        let participant_id = ParticipantId::from_raw(participant);
        if self.rooms.room_of(participant_id) != Some(room_id) {
            return 0;
        }
        match payload {
            NormalizedPayload::ActorStateReport {
                object_id,
                transform,
            } => {
                hub.apply_owner_state(participant, *object_id, (*transform).into());
                0
            }
            NormalizedPayload::TransformInput {
                object_id,
                ownership_epoch,
                input_seq,
                sim_tick,
                dt,
                move_velocity,
                payload: input_payload,
                fire,
            } => {
                // Reconstruct the wire frame and apply it through the same
                // ownership/epoch/seq gate as the relay path; any rewind reply
                // is unicast reliably to the shooter.
                let frame = citadel_wire::tsync::InputFrame {
                    input_seq: *input_seq,
                    sim_tick: *sim_tick,
                    dt: *dt,
                    object_id: *object_id,
                    ownership_epoch: *ownership_epoch,
                    move_velocity: *move_velocity,
                    payload: input_payload.clone(),
                    fire: fire.map(|fire| citadel_wire::tsync::FireCommand {
                        origin: fire.origin,
                        direction: fire.direction,
                    }),
                };
                // Owner decision 1: the movement integrates, but any fire is not
                // auto-resolved here — the script queried the rewind and decided
                // the consequence during on_input.
                hub.apply_validated_input(participant, &frame);
                0
            }
            NormalizedPayload::SpawnRequest {
                archetype_id,
                transform,
            } => self.do_register_presence_under_scope(
                participant_id,
                Some(room_id),
                *archetype_id,
                (*transform).into(),
            ),
            // Other protected kinds are not yet routed through the bridge, so
            // their accepted effect is materialized where they are wired in a
            // later step. Reaching here is defensive.
            _ => {
                tracing::debug!("bridge accept for an unrouted payload; no effect applied");
                0
            }
        }
    }

    /// Apply the script's substituted value while the originating participant
    /// still belongs to the issuing room.
    fn materialize_correction_under_scope(
        &self,
        room_id: RoomId,
        payload: &NormalizedPayload,
        participant: u64,
        correction: &Correction,
    ) -> usize {
        let Some(hub) = &self.transform else {
            return 0;
        };
        let participant_id = ParticipantId::from_raw(participant);
        if self.rooms.room_of(participant_id) != Some(room_id) {
            return 0;
        }
        match (payload, correction) {
            (
                NormalizedPayload::ActorStateReport { object_id, .. }
                | NormalizedPayload::TransformInput { object_id, .. },
                Correction::Transform(transform),
            ) => {
                hub.set_transform(*object_id, na_to_transform_state(*transform));
                0
            }
            (
                NormalizedPayload::SpawnRequest { .. },
                Correction::Spawn {
                    archetype_id,
                    transform,
                },
            ) => self.do_register_presence_under_scope(
                participant_id,
                Some(room_id),
                *archetype_id,
                (*transform).into(),
            ),
            _ => {
                tracing::debug!("bridge correction for an unrouted payload; no effect applied");
                0
            }
        }
    }

    /// Remove a departed participant's input-admission state immediately so
    /// reconnect/session replacement starts with a fresh server-owned key and
    /// no inactive participant consumes retained limiter capacity.
    fn forget_match_input_admission(&self, room_id: RoomId, participant: ParticipantId) {
        if let Some(bridge) = &self.bridge {
            bridge
                .input_rate_windows
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&(room_id, participant));
        }
    }

    /// Drop a match's ledger (on close), so a late answer for it is rejected as
    /// an unknown batch rather than resurrecting a dead match.
    fn drop_bridge_match(&self, room_id: RoomId) {
        if let Some(bridge) = &self.bridge {
            bridge
                .ledgers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&room_id);
            bridge
                .input_rate_windows
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|(rid, _), _| *rid != room_id);
            // Drop any replicated-delta proposals still awaiting an answer for
            // this match, so a late answer cannot resurrect a dead match.
            bridge
                .pending_rep
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|(rid, _), _| *rid != room_id);
        }
    }
}

/// Convert a validated bridge transform into the hub's transform state.
fn na_to_transform_state(t: BridgeTransform) -> crate::realtime::transform::TransformState {
    crate::realtime::transform::TransformState {
        position: t.position,
        rotation: t.rotation,
        velocity: t.velocity,
    }
}

/// Map one validated [`ScriptCommand`] to the [`OutboundCommand`](crate::runtime::OutboundCommand)s
/// the scoped applier executes. Multicasts expand to one send per recipient;
/// rep-writes have no `OutboundCommand` twin yet and are skipped (the validator
/// already rejects them without the bounds wiring).
fn push_outbound_from_script_command(out: &mut Vec<OutboundCommand>, command: ScriptCommand) {
    match command {
        ScriptCommand::BroadcastMatch {
            kind,
            body,
            unreliable,
            exclude: _,
        } => out.push(OutboundCommand::Broadcast {
            kind,
            body,
            unreliable,
        }),
        ScriptCommand::SendTo {
            participant,
            kind,
            body,
            unreliable,
        } => out.push(OutboundCommand::Send {
            session: participant,
            kind,
            body,
            unreliable,
        }),
        ScriptCommand::SendToMany {
            participants,
            kind,
            body,
            unreliable,
        } => {
            for participant in participants {
                out.push(OutboundCommand::Send {
                    session: participant,
                    kind,
                    body: body.clone(),
                    unreliable,
                });
            }
        }
        ScriptCommand::ApplyTransform {
            object_id,
            transform,
        } => out.push(OutboundCommand::MoveActor {
            object_id,
            position: transform.position,
            rotation: transform.rotation,
            velocity: transform.velocity,
        }),
        ScriptCommand::SpawnActor {
            object_id,
            archetype,
            position,
        } => out.push(OutboundCommand::SpawnActor {
            object_id,
            archetype,
            position,
        }),
        ScriptCommand::DespawnActor { object_id } => {
            out.push(OutboundCommand::DespawnActor { object_id })
        }
        ScriptCommand::SetPhysics { object_id, opts } => out.push(OutboundCommand::SetPhysics {
            object_id,
            opts: opts.map(Into::into),
        }),
        ScriptCommand::ApplyImpulse { object_id, impulse } => {
            out.push(OutboundCommand::ApplyImpulse { object_id, impulse })
        }
        ScriptCommand::SetMoveIntent { object_id, intent } => {
            out.push(OutboundCommand::SetMoveIntent { object_id, intent })
        }
        // `SetInputAck` is consumed by `materialize_validated_batch` before this
        // generic converter. Reaching here is a fail-closed internal drop.
        ScriptCommand::SetInputAck { .. } => {
            tracing::debug!("typed input acknowledgement bypassed its executor; dropped");
        }
        // No OutboundCommand twin yet: rep writes go through RepAuthority, and
        // persist/schedule through the DomainHost seam — both wired in a later
        // step. The validator rejects these until then, so this is defensive.
        ScriptCommand::SetReplicatedVars { .. }
        | ScriptCommand::Persist { .. }
        | ScriptCommand::Schedule { .. } => {
            tracing::debug!("bridge command has no executor yet; skipped");
        }
    }
}

/// The gateway is where an authoritative match's script answers land: a
/// validated batch materializes, an invalid one materializes nothing (batch
/// atomic), and a never-delivered answer (timeout/worker death) leaves the
/// match unmutated until it is closed.
impl BridgeCommandSink for Gateway {
    fn deliver_command_batch(&self, answer: ScriptCommandBatch) {
        let Some(bridge) = &self.bridge else {
            return;
        };
        let room_id = answer.match_id;
        let validated = {
            let context = GatewayBridgeContext {
                gateway: self,
                room_id,
            };
            let mut ledgers = bridge.ledgers.lock().unwrap_or_else(|e| e.into_inner());
            let Some(ledger) = ledgers.get_mut(&room_id) else {
                tracing::debug!(room_id, "bridge answer for an unknown match; dropped");
                return;
            };
            match ledger.validate(&context, &bridge.quotas, &answer) {
                Ok(validated) => validated,
                Err(rejection) => {
                    tracing::debug!(
                        room_id,
                        ?rejection,
                        "bridge batch rejected; nothing materialized"
                    );
                    return;
                }
            }
        };
        // The ledger lock is released before recording/materialization, which
        // lock independent process-local state and the transform hub/registry.
        // Record only validated outcomes; rejected batches never reach here.
        self.record_validated_decisions(&validated);
        self.materialize_validated_batch(room_id, validated);
    }
}

/// The gateway is where an external worker's match results land: command
/// batches apply with room-scoped semantics, a worker-side closure becomes
/// the standard server-side match close (requeue-hinted member notification +
/// room prune). Every close reason maps to the same client-facing outcome by
/// design; the distinct reasons stay in worker diagnostics.
impl crate::runtime::external_worker::MatchCommandSink for Gateway {
    fn apply_match_commands(&self, room_id: u64, commands: Vec<OutboundCommand>) -> usize {
        self.apply_external_match_commands(room_id, commands)
    }

    fn on_match_closed(
        &self,
        room_id: u64,
        _reason: crate::runtime::worker_data_protocol::MatchCloseReason,
    ) {
        self.close_match(room_id);
    }
}

impl PlayerNotificationDelivery for Gateway {
    fn deliver(&self, recipient: &str, notification: &PlayerNotification) {
        let body = match serde_json::to_vec(notification) {
            Ok(body) => body,
            Err(error) => {
                tracing::error!(%error, notification_id = %notification.id, "failed to encode committed player notification for live delivery");
                return;
            }
        };
        for participant in self.registry.participants_for_user(recipient) {
            let outbound = Outbound::reliable(Envelope::new(KIND_NOTIFICATION, body.clone()));
            if self.registry.send_to(participant, &outbound) {
                self.metrics
                    .record_message_out(outbound.envelope.body.len() as u64);
                self.metrics.record_notification_live_delivered();
            } else {
                // `send_to` uses a bounded `try_send`: a full or closed queue is
                // a dropped live attempt, never a failed durable notification.
                self.metrics.record_notification_live_dropped();
                tracing::debug!(%recipient, participant = %participant, notification_id = %notification.id, "player notification live delivery dropped");
            }
        }
    }
}

#[cfg(test)]
struct TestOutboundReceiver {
    reliable: tokio::sync::mpsc::Receiver<Outbound>,
    unreliable: LatestOutboundReceiver,
}

#[cfg(test)]
impl TestOutboundReceiver {
    async fn recv(&mut self) -> Option<Outbound> {
        tokio::select! {
            next = self.reliable.recv() => next,
            next = self.unreliable.recv() => Some(next),
        }
    }

    fn try_recv(&mut self) -> Result<Outbound, tokio::sync::mpsc::error::TryRecvError> {
        self.reliable
            .try_recv()
            .or_else(|_| self.unreliable.try_recv())
    }
}

#[cfg(test)]
mod transform_tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use super::*;
    use crate::realtime::registry::SessionHandle;
    use crate::realtime::transform::{
        RemoteWorldView, TransformHub, TransformHubConfig, TransformState,
    };
    use crate::transport::TransportKind;
    use citadel_wire::room::RoomCreate;
    use citadel_wire::tsync;
    use tokio::sync::mpsc;

    fn gateway_with_hub() -> (Gateway, Arc<TransformHub>) {
        let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub"));
        let gw = Gateway::new().with_transform_hub(Arc::clone(&hub));
        (gw, hub)
    }

    fn gateway_with_player_slots(slots: u32) -> (Gateway, Arc<TransformHub>) {
        let cfg = TransformHubConfig {
            player_slots: slots,
            ..TransformHubConfig::default()
        };
        let hub = Arc::new(TransformHub::new(cfg).expect("hub"));
        let gw = Gateway::new().with_transform_hub(Arc::clone(&hub));
        (gw, hub)
    }

    fn register(gw: &Gateway) -> (ParticipantId, TestOutboundReceiver) {
        let id = gw.next_participant_id();
        let (tx, rx) = mpsc::channel(64);
        let unreliable = gw.registry().register(SessionHandle {
            id,
            kind: TransportKind::Quic,
            outbound: tx,
            identity: None,
        });
        (
            id,
            TestOutboundReceiver {
                reliable: rx,
                unreliable,
            },
        )
    }

    fn register_session(gw: &Gateway) -> (ParticipantId, TestOutboundReceiver) {
        let id = gw.next_participant_id();
        let (tx, rx) = mpsc::channel(64);
        let unreliable = gw.register_session(SessionHandle {
            id,
            kind: TransportKind::Quic,
            outbound: tx,
            identity: None,
        });
        (
            id,
            TestOutboundReceiver {
                reliable: rx,
                unreliable,
            },
        )
    }

    #[tokio::test]
    async fn hello_over_gateway_replies_and_registers_client() {
        let (gw, hub) = gateway_with_hub();
        let (a, mut ra) = register(&gw);
        let sent = gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        assert_eq!(sent, 1, "server replies with its negotiation");
        assert_eq!(hub.client_count(), 1);
        let out = ra.recv().await.expect("hello reply");
        assert_eq!(out.envelope.kind, KIND_TSYNC_HELLO);
        assert_eq!(out.delivery, Delivery::Reliable);
        let hello = tsync::Hello::decode(&out.envelope.body).expect("decodes");
        assert_eq!(hello, TransformHubConfig::default().hello);
    }

    #[tokio::test]
    async fn inbound_after_close_is_rejected_before_application_handling() {
        let (gw, hub) = gateway_with_hub();
        let (participant, mut outbound) = register(&gw);
        assert!(gw.registry().claim_cleanup(participant));

        assert_eq!(
            gw.handle_inbound(participant, &Envelope::new(KIND_TSYNC_HELLO, Vec::new())),
            0
        );
        assert_eq!(hub.client_count(), 0, "closed input must not reach the hub");
        assert!(
            outbound.try_recv().is_err(),
            "closed input produces no reply"
        );
    }

    #[tokio::test]
    async fn hello_in_player_slot_mode_assigns_owned_object_and_announces_role() {
        let (gw, _hub) = gateway_with_player_slots(2);
        let (a, mut ra) = register(&gw);
        // HELLO yields the negotiation reply AND a reliable ROLE assignment.
        let sent = gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        assert_eq!(sent, 2, "HELLO reply + owner-assign ROLE");

        let hello = ra.recv().await.expect("hello reply");
        assert_eq!(hello.envelope.kind, KIND_TSYNC_HELLO);

        let role_out = ra.recv().await.expect("role frame");
        assert_eq!(role_out.envelope.kind, KIND_TSYNC_ROLE);
        assert_eq!(role_out.delivery, Delivery::Reliable, "role is reliable");
        let role = tsync::Role::decode(&role_out.envelope.body).expect("role decodes");
        assert_eq!(role.object_id, 1, "first client gets the lowest slot id");
        assert_eq!(role.owner, a.get(), "assigned to the opting-in participant");
        assert_eq!(role.role, tsync::SyncRole::OwnerPredicted);
        assert_eq!(role.event, tsync::RoleEvent::Assign);
    }

    #[tokio::test]
    async fn player_slots_are_distinct_and_freed_on_disconnect() {
        let (gw, _hub) = gateway_with_player_slots(2);

        let (a, mut ra) = register_session(&gw);
        gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = ra.recv().await; // hello
        let role_a = ra.recv().await.expect("A role");
        let id_a = tsync::Role::decode(&role_a.envelope.body)
            .expect("decode")
            .object_id;

        let (b, mut rb) = register_session(&gw);
        gw.handle_inbound(b, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = rb.recv().await; // hello
        let role_b = rb.recv().await.expect("B role");
        let id_b = tsync::Role::decode(&role_b.envelope.body)
            .expect("decode")
            .object_id;
        assert_ne!(id_a, id_b, "each player owns a distinct object");
        assert_eq!(id_a + id_b, 3, "the two slots are ids 1 and 2");

        // A disconnects: its slot frees, so a new client reuses that id.
        gw.unregister_session(a);
        let (c, mut rc) = register_session(&gw);
        gw.handle_inbound(c, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = rc.recv().await; // hello
        let role_c = rc.recv().await.expect("C role");
        let id_c = tsync::Role::decode(&role_c.envelope.body)
            .expect("decode")
            .object_id;
        assert_eq!(id_c, id_a, "the freed slot id is reused by the next join");
    }

    #[tokio::test]
    async fn default_mode_assigns_no_player_slot() {
        // With player_slots == 0, HELLO replies with only the negotiation (no ROLE).
        let (gw, _hub) = gateway_with_hub();
        let (a, mut ra) = register(&gw);
        let sent = gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        assert_eq!(sent, 1, "just the hello reply, no ownership assignment");
        let hello = ra.recv().await.expect("hello reply");
        assert_eq!(hello.envelope.kind, KIND_TSYNC_HELLO);
        assert!(ra.try_recv().is_err(), "no ROLE frame in default mode");
    }

    #[tokio::test]
    async fn spawn_actor_command_fans_out_npc_and_late_joiner_sees_it() {
        use crate::runtime::lua::OutboundCommand;
        use citadel_wire::na::{NaPresence, NaSpawn, NaSpawnBatch, NaTransform};
        let (gw, _hub) = gateway_with_hub();
        let (_a, mut ra) = register(&gw);
        let (_b, mut rb) = register(&gw);

        // Server spawns an NPC via the Lua command path.
        let npc_id = 0x4000_0000u32;
        let sent = gw.apply_commands(
            None,
            vec![OutboundCommand::SpawnActor {
                object_id: npc_id,
                archetype: 0,
                position: [10.0, 20.0, 30.0],
            }],
        );
        assert_eq!(sent, 2, "NA_SPAWN fanned out to both connected clients");
        let spawn_a = NaSpawn::decode(&ra.recv().await.unwrap().envelope.body).unwrap();
        assert_eq!(spawn_a.object_id, npc_id);
        assert_eq!(spawn_a.owner, 0, "server-owned NPC has owner 0");
        let _ = rb.recv().await;

        // A client that announces presence afterwards sees the NPC in its batch.
        let (c, mut rc) = register(&gw);
        gw.handle_inbound(
            c,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 0,
                    transform: NaTransform::identity(),
                }
                .encode(),
            ),
        );
        let _self_spawn = rc.recv().await; // own spawn first
        let batch = NaSpawnBatch::decode(&rc.recv().await.unwrap().envelope.body).unwrap();
        assert!(
            batch
                .spawns
                .iter()
                .any(|s| s.object_id == npc_id && s.owner == 0),
            "late joiner's presence batch includes the server-owned NPC"
        );

        // Despawn fans out to everyone and drops it from the registry.
        let sent = gw.apply_commands(
            None,
            vec![OutboundCommand::DespawnActor { object_id: npc_id }],
        );
        assert_eq!(sent, 3, "NA_DESPAWN fanned out to all three clients");
    }

    #[test]
    fn physics_commands_apply_to_the_intended_actor_through_the_gateway_drain() {
        use crate::runtime::PhysicsOptions;
        use citadel_physics::{PhysicsConfig, Shape};

        let (gw, hub) = gateway_with_hub();
        hub.spawn_server_simulated(41, TransformState::at([0.0, 20.0, 0.0]));
        hub.spawn_server_simulated(42, TransformState::at([0.0, 20.0, 0.0]));
        let config = PhysicsConfig {
            shape: Shape::Aabb {
                half_extents: [10.0, 10.0, 10.0],
            },
            gravity: 0.0,
            buoyancy: 0.0,
            drag: 0.0,
            max_speed: 1_000.0,
        };

        assert_eq!(
            gw.apply_commands(
                None,
                vec![
                    OutboundCommand::SetPhysics {
                        object_id: 41,
                        opts: Some(PhysicsOptions {
                            enabled: true,
                            config,
                        }),
                    },
                    OutboundCommand::ApplyImpulse {
                        object_id: 41,
                        impulse: [5.0, 30.0, 0.0],
                    },
                    OutboundCommand::SetMoveIntent {
                        object_id: 41,
                        intent: [100.0, 0.0, -50.0],
                    },
                ],
            ),
            0,
            "physics commands have no transport delivery side effect"
        );
        assert_eq!(
            hub.physics_state(42),
            None,
            "only the targeted actor changed"
        );
        let after_impulse = hub.physics_state(41).unwrap();
        assert_eq!(after_impulse.velocity, [5.0, 30.0, 0.0]);

        hub.sim_tick();
        let after_step = hub.physics_state(41).unwrap();
        assert!(after_step.velocity[0] > after_impulse.velocity[0]);
        assert!(after_step.velocity[2] < after_impulse.velocity[2]);
        assert_eq!(after_step.velocity[1], after_impulse.velocity[1]);

        gw.apply_commands(
            None,
            vec![OutboundCommand::SetPhysics {
                object_id: 41,
                opts: None,
            }],
        );
        assert_eq!(hub.physics_state(41), None, "nil options detach the body");
    }

    #[tokio::test]
    async fn room_create_by_name_is_join_or_create() {
        use citadel_wire::room::{RoomCreate, RoomJoined};
        let (gw, _hub) = gateway_with_hub();
        let create = |name: &[u8]| {
            Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: name.to_vec(),
                }
                .encode(),
            )
        };

        // A asks for "lobby" -> creates it.
        let (a, mut ra) = register(&gw);
        gw.handle_inbound(a, &create(b"lobby"));
        let ja = RoomJoined::decode(&ra.recv().await.unwrap().envelope.body).unwrap();

        // B asks for "lobby" -> joins the SAME room (no second room created).
        let (b, mut rb) = register(&gw);
        gw.handle_inbound(b, &create(b"lobby"));
        let jb = RoomJoined::decode(&rb.recv().await.unwrap().envelope.body).unwrap();
        assert_eq!(ja.room_id, jb.room_id, "same name -> same room");
        assert_eq!(gw.rooms().room_count(), 1);

        // C asks for a different name -> a different room.
        let (c, mut rc) = register(&gw);
        gw.handle_inbound(c, &create(b"arena"));
        let jc = RoomJoined::decode(&rc.recv().await.unwrap().envelope.body).unwrap();
        assert_ne!(jc.room_id, ja.room_id, "different name -> different room");
        assert_eq!(gw.rooms().room_count(), 2);
    }

    #[tokio::test]
    async fn built_in_relay_is_scoped_to_the_senders_match() {
        use citadel_wire::room::{RoomCreate, RoomJoined};

        let gw = Gateway::new();
        let create = |name: &[u8]| {
            Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: name.to_vec(),
                }
                .encode(),
            )
        };
        let (a, mut ra) = register(&gw);
        let (b, mut rb) = register(&gw);
        let (c, mut rc) = register(&gw);
        gw.handle_inbound(a, &create(b"lobby"));
        let room = RoomJoined::decode(&ra.recv().await.unwrap().envelope.body)
            .unwrap()
            .room_id;
        gw.handle_inbound(b, &create(b"lobby"));
        let _ = rb.recv().await;
        gw.handle_inbound(c, &create(b"other"));
        let _ = rc.recv().await;

        let sent = gw.handle_inbound(a, &Envelope::new(KIND_POSITION, &b"position"[..]));
        assert_eq!(sent, 1, "only the other lobby member receives the relay");
        assert_eq!(rb.recv().await.unwrap().envelope.kind, KIND_PEER_POSITION);
        assert!(ra.try_recv().is_err(), "sender is excluded");
        assert!(rc.try_recv().is_err(), "a different match receives nothing");
        assert_eq!(gw.rooms().room_of(b), Some(room));
    }

    #[tokio::test]
    async fn networked_actors_are_scoped_to_rooms() {
        use citadel_wire::na::{NaPresence, NaSpawnBatch, NaTransform};
        use citadel_wire::room::{RoomCreate, RoomJoin};
        let (gw, _hub) = gateway_with_hub();
        let t = NaTransform::identity();
        let presence = |arch| {
            Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: arch,
                    transform: t,
                }
                .encode(),
            )
        };

        // A creates room 1 and announces presence.
        let (a, mut ra) = register(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"R1".to_vec(),
                }
                .encode(),
            ),
        );
        let _ = ra.recv().await; // ROOM_JOINED
        gw.handle_inbound(a, &presence(0));
        let _ = ra.recv().await; // self spawn
        let _ = ra.recv().await; // empty batch

        // B joins the SAME room (id 1): B sees A and A is told to spawn B.
        let (b, mut rb) = register(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id: 1 }.encode()),
        );
        let _ = rb.recv().await; // ROOM_JOINED
        let sent = gw.handle_inbound(b, &presence(0));
        assert_eq!(sent, 3, "same room: self + batch + spawn-to-A");
        let _ = rb.recv().await; // B self spawn
        let batch_b = NaSpawnBatch::decode(&rb.recv().await.unwrap().envelope.body).unwrap();
        assert_eq!(batch_b.spawns.len(), 1, "B's batch carries A (same room)");
        assert_eq!(
            ra.recv().await.unwrap().envelope.kind,
            KIND_NA_SPAWN,
            "A is told to spawn B"
        );

        // C creates a DIFFERENT room (id 2): it sees nobody and notifies no peers.
        let (c, mut rc) = register(&gw);
        gw.handle_inbound(
            c,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"R2".to_vec(),
                }
                .encode(),
            ),
        );
        let _ = rc.recv().await; // ROOM_JOINED
        let sent = gw.handle_inbound(c, &presence(0));
        assert_eq!(sent, 2, "different room: only self + empty batch, no peers");
        let _ = rc.recv().await; // C self spawn
        let batch_c = NaSpawnBatch::decode(&rc.recv().await.unwrap().envelope.body).unwrap();
        assert!(batch_c.spawns.is_empty(), "C sees nobody (different room)");
    }

    #[tokio::test]
    async fn networked_actor_batch_rechecks_owner_room_at_enqueue() {
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined};

        let (gw, _hub) = gateway_with_hub();
        let (owner, mut owner_rx) = register(&gw);
        gw.handle_inbound(
            owner,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"A".to_vec(),
                }
                .encode(),
            ),
        );
        let room_a =
            RoomJoined::decode(&owner_rx.recv().await.expect("owner joins A").envelope.body)
                .expect("A join decodes")
                .room_id;
        let (viewer, mut viewer_rx) = register(&gw);
        gw.handle_inbound(
            viewer,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id: room_a }.encode()),
        );
        let _ = viewer_rx.recv().await.expect("viewer joins A");
        let (mover, mut mover_rx) = register(&gw);
        gw.handle_inbound(
            mover,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"B".to_vec(),
                }
                .encode(),
            ),
        );
        let room_b =
            RoomJoined::decode(&mover_rx.recv().await.expect("mover joins B").envelope.body)
                .expect("B join decodes")
                .room_id;

        // The batch captured `owner` while it was in A. It moves before the
        // final enqueue, so A's viewer must not receive that stale spawn.
        gw.rooms()
            .join(owner, room_b)
            .expect("test owner moves before enqueue");
        assert!(
            !gw.send_reliable_in_scope_with_owners(
                viewer,
                Some(room_a),
                &[owner.get()],
                KIND_NA_SPAWN_BATCH,
                b"stale-owner-batch".to_vec(),
            ),
            "owner movement invalidates a captured spawn batch"
        );
        assert!(
            viewer_rx.try_recv().is_err(),
            "no stale spawn batch is queued"
        );
    }

    #[tokio::test]
    async fn transform_snapshots_are_scoped_to_each_room() {
        use citadel_wire::na::{NaPresence, NaTransform};
        use citadel_wire::room::RoomCreate;

        let (gw, hub) = gateway_with_hub();
        let presence = |position| {
            Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 0,
                    transform: NaTransform {
                        position,
                        ..NaTransform::identity()
                    },
                }
                .encode(),
            )
        };

        let (a, mut ra) = register(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"R1".to_vec(),
                }
                .encode(),
            ),
        );
        let _ = ra.recv().await.expect("A room joined");
        gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = ra.recv().await.expect("A hello");
        gw.handle_inbound(a, &presence([10.0, 0.0, 0.0]));
        let _ = ra.recv().await.expect("A self spawn");
        let _ = ra.recv().await.expect("A spawn batch");

        let (b, mut rb) = register(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"R2".to_vec(),
                }
                .encode(),
            ),
        );
        let _ = rb.recv().await.expect("B room joined");
        gw.handle_inbound(b, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = rb.recv().await.expect("B hello");
        gw.handle_inbound(b, &presence([20.0, 0.0, 0.0]));
        let _ = rb.recv().await.expect("B self spawn");
        let _ = rb.recv().await.expect("B spawn batch");

        assert_eq!(gw.transform_tick(), 2, "one snapshot per room member");
        let codec = *hub.codec();
        let mut view_a = RemoteWorldView::new(codec, 60, 20);
        let mut view_b = RemoteWorldView::new(codec, 60, 20);
        assert!(view_a.apply_datagram(&ra.recv().await.expect("A snapshot").envelope.body));
        assert!(view_b.apply_datagram(&rb.recv().await.expect("B snapshot").envelope.body));

        assert!(view_a.object(1).is_some(), "A receives A's object");
        assert!(view_a.object(2).is_none(), "A must not receive B's object");
        assert!(view_b.object(1).is_none(), "B must not receive A's object");
        assert!(view_b.object(2).is_some(), "B receives B's object");
    }

    #[tokio::test]
    async fn snapshot_built_for_previous_room_is_not_delivered_after_move() {
        use crate::realtime::rooms::RoomLabel;

        let (gw, hub) = gateway_with_hub();
        let (participant, mut outbound_rx) = register(&gw);
        let first_room = gw.rooms().create(RoomLabel::with_map("R1"));
        gw.rooms()
            .join(participant, first_room)
            .expect("participant joins first room");
        gw.handle_inbound(participant, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = outbound_rx.recv().await.expect("hello reply");
        hub.spawn_server_simulated(99, TransformState::at([1.0, 2.0, 3.0]));
        hub.sim_tick();
        let snapshot = hub
            .snapshot_tick_scoped(|id| gw.rooms().room_of(ParticipantId::from_raw(id)))
            .pop()
            .expect("snapshot is built for the first room");
        assert_eq!(snapshot.room_scope, Some(first_room));

        let second_room = gw.rooms().create(RoomLabel::with_map("R2"));
        gw.rooms()
            .join(participant, second_room)
            .expect("participant moves rooms");

        assert!(
            !gw.deliver_transform_snapshot(snapshot),
            "a snapshot built under the previous room membership is rejected"
        );
        assert!(
            outbound_rx.try_recv().is_err(),
            "the stale-room snapshot never reaches the moved participant"
        );
    }

    #[tokio::test]
    async fn snapshot_with_owner_moved_to_another_room_is_not_delivered() {
        use crate::realtime::rooms::RoomLabel;

        let (gw, hub) = gateway_with_hub();
        let (viewer_a, mut viewer_a_rx) = register(&gw);
        let (viewer_b, mut viewer_b_rx) = register(&gw);
        let (owner, _owner_rx) = register(&gw);
        let room_a = gw.rooms().create(RoomLabel::with_map("A"));
        let room_b = gw.rooms().create(RoomLabel::with_map("B"));
        for (participant, room) in [(viewer_a, room_a), (viewer_b, room_b), (owner, room_a)] {
            gw.rooms()
                .join(participant, room)
                .expect("participant joins room");
        }
        for receiver in [viewer_a, viewer_b] {
            gw.handle_inbound(receiver, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        }
        let _ = viewer_a_rx.recv().await.expect("A hello reply");
        let _ = viewer_b_rx.recv().await.expect("B hello reply");

        hub.spawn_server_simulated(99, TransformState::at([1.0, 2.0, 3.0]));
        hub.assign_owner(99, owner.get())
            .expect("actor owner assigned");
        hub.sim_tick();
        let stale_for_a = hub
            .snapshot_tick_scoped(|id| gw.rooms().room_of(ParticipantId::from_raw(id)))
            .into_iter()
            .find(|out| out.participant == viewer_a.get())
            .expect("A snapshot built while actor owner is in A");

        gw.rooms()
            .join(owner, room_b)
            .expect("actor owner moves to B");

        assert!(
            !gw.deliver_transform_snapshot(stale_for_a),
            "A must not receive an actor after its owner moved to B"
        );
        assert!(
            viewer_a_rx.try_recv().is_err(),
            "the stale A snapshot must never reach its viewer"
        );

        hub.sim_tick();
        let fresh_for_b = hub
            .snapshot_tick_scoped(|id| gw.rooms().room_of(ParticipantId::from_raw(id)))
            .into_iter()
            .find(|out| out.participant == viewer_b.get())
            .expect("B snapshot built after actor owner moved to B");
        assert!(
            gw.deliver_transform_snapshot(fresh_for_b),
            "B receives the actor in its new room"
        );
        let codec = *hub.codec();
        let mut view_b = RemoteWorldView::new(codec, 60, 20);
        assert!(
            view_b.apply_datagram(&viewer_b_rx.recv().await.expect("B snapshot").envelope.body)
        );
        assert!(view_b.object(99).is_some(), "B receives the moved actor");
    }

    #[tokio::test]
    async fn rep_delta_with_transform_fields_stays_in_the_sender_room() {
        use crate::realtime::netpeer::{
            FieldAuthority, FieldBounds, RepAuthority, RepCondition, RepLayoutBuilder, RepSnapshot,
            TypeTag,
        };
        use citadel_wire::codec::{DEFAULT_WORLD_BOUNDS, QuatMode, VectorQuant, codec_id};
        use citadel_wire::netpeer::{DeltaBunch, FieldDelta, RepFieldCodec, RepSchema, RepValue};
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined};

        let layout = Box::leak(Box::new(
            RepLayoutBuilder::new(91, 1)
                .field(
                    "position",
                    TypeTag::Vector3,
                    codec_id::VECTOR3_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::None,
                    true,
                )
                .field(
                    "rotation",
                    TypeTag::Quat,
                    codec_id::QUAT_SMALLEST3_10,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::None,
                    true,
                )
                .build()
                .expect("transform layout"),
        ));
        let schema = RepSchema::new(
            *layout.schema_hash(),
            vec![
                RepFieldCodec::Vector3(VectorQuant::new(DEFAULT_WORLD_BOUNDS).expect("bounds")),
                RepFieldCodec::Quat(QuatMode::Bits10),
            ],
        )
        .expect("transform schema");
        let rep = Arc::new(RepAuthority::new(Default::default()));
        rep.register_class(91, layout, schema.clone())
            .expect("class registers");
        let gw = Gateway::new().with_rep_authority(Arc::clone(&rep));

        let (a, mut ra) = register_session(&gw);
        let _ = ra.recv().await.expect("A schema bootstrap");
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"R1".to_vec(),
                }
                .encode(),
            ),
        );
        let room_one = RoomJoined::decode(&ra.recv().await.expect("A room joined").envelope.body)
            .expect("room joined decodes")
            .room_id;
        gw.spawn_rep_object(701, 0, 91, Some(a), false, RepSnapshot::new())
            .expect("object spawns in the room-bound replication scope");

        let (same_room, mut same_room_rx) = register_session(&gw);
        let _ = same_room_rx
            .recv()
            .await
            .expect("same-room schema bootstrap");
        assert!(
            same_room_rx.try_recv().is_err(),
            "a roomless registration cannot receive an existing room object's baseline"
        );
        gw.handle_inbound(
            same_room,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id: room_one }.encode()),
        );
        let _ = same_room_rx.recv().await.expect("same-room joined");

        let (other_room, mut other_room_rx) = register_session(&gw);
        let _ = other_room_rx
            .recv()
            .await
            .expect("other-room schema bootstrap");
        assert!(
            other_room_rx.try_recv().is_err(),
            "a future member of another room cannot receive an existing baseline"
        );
        gw.handle_inbound(
            other_room,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"R2".to_vec(),
                }
                .encode(),
            ),
        );
        let _ = other_room_rx.recv().await.expect("other-room joined");

        let mut delta = DeltaBunch::new(701, true, 1, 0);
        delta.set(0, FieldDelta::Value(RepValue::Vector3([12.0, 4.0, 1.0])));
        delta.set(1, FieldDelta::Value(RepValue::Quat([0.0, 0.0, 0.0, 1.0])));
        let delta = delta.encode(&schema).expect("delta encodes");

        let sent = gw.handle_inbound(a, &Envelope::new(KIND_REP_DELTA, delta));
        let same_room_delta = same_room_rx.recv().await.expect("same-room receives delta");
        assert_eq!(same_room_delta.envelope.kind, KIND_REP_DELTA);
        assert!(
            other_room_rx.try_recv().is_err(),
            "a client in another room must never receive A's replicated transform"
        );
        assert_eq!(sent, 1, "only the same-room peer receives the delta");
    }

    #[tokio::test]
    async fn rep_registration_preserves_legacy_roomless_match_without_bridge() {
        use crate::realtime::netpeer::RepAuthority;

        let rep = Arc::new(RepAuthority::new(Default::default()));
        let gw = Gateway::new().with_rep_authority(Arc::clone(&rep));
        let (participant, mut receiver) = register_session(&gw);

        assert_eq!(
            receiver.recv().await.expect("schema frame").envelope.kind,
            citadel_wire::protocol::KIND_REP_SCHEMA,
            "legacy registration still advertises schemas"
        );
        assert!(
            rep.is_joined(participant.get()),
            "a bridge-less gateway preserves the legacy match-0 lifecycle"
        );

        gw.handle_inbound(
            participant,
            &Envelope::new(
                KIND_ROOM_CREATE,
                citadel_wire::room::RoomCreate {
                    params: b"arena".to_vec(),
                }
                .encode(),
            ),
        );
        let _ = receiver.recv().await.expect("room joined");
        assert!(
            rep.is_joined(participant.get()),
            "atomic room admission rebinds the legacy receiver to its room"
        );
    }

    #[tokio::test]
    async fn rep_delta_for_previous_room_object_is_not_delivered_after_move() {
        use crate::realtime::netpeer::{
            FieldAuthority, FieldBounds, RepAuthority, RepCondition, RepLayoutBuilder, RepSnapshot,
            TypeTag,
        };
        use citadel_wire::codec::codec_id;
        use citadel_wire::netpeer::{DeltaBunch, FieldDelta, RepFieldCodec, RepSchema, RepValue};
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined};

        const CLASS: u32 = 92;
        const OBJECT: u32 = 702;
        const MATCH: u64 = 7_777;
        let layout = Box::leak(Box::new(
            RepLayoutBuilder::new(CLASS, 1)
                .field(
                    "health",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .build()
                .expect("layout builds"),
        ));
        let schema = RepSchema::new(
            *layout.schema_hash(),
            vec![RepFieldCodec::IntRange { min: 0, max: 100 }],
        )
        .expect("schema builds");
        let rep = Arc::new(RepAuthority::new(Default::default()));
        rep.register_class(CLASS, layout, schema.clone())
            .expect("class registers");
        let gw = Gateway::new().with_rep_authority(Arc::clone(&rep));

        let (owner, mut owner_rx) = register_session(&gw);
        while owner_rx.try_recv().is_ok() {}
        gw.handle_inbound(
            owner,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"A".to_vec(),
                }
                .encode(),
            ),
        );
        let room_a =
            RoomJoined::decode(&owner_rx.recv().await.expect("owner joins A").envelope.body)
                .expect("A join decodes")
                .room_id;

        let (peer, mut peer_rx) = register_session(&gw);
        while peer_rx.try_recv().is_ok() {}
        gw.handle_inbound(
            peer,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id: room_a }.encode()),
        );
        let _ = peer_rx.recv().await.expect("peer joins A");

        gw.spawn_rep_object(OBJECT, MATCH, CLASS, Some(owner), false, RepSnapshot::new())
            .expect("trusted object spawn");
        gw.join_rep_match(owner, MATCH, false);
        gw.join_rep_match(peer, MATCH, false);
        while owner_rx.try_recv().is_ok() {}
        while peer_rx.try_recv().is_ok() {}

        gw.handle_inbound(
            owner,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"B".to_vec(),
                }
                .encode(),
            ),
        );
        let room_b =
            RoomJoined::decode(&owner_rx.recv().await.expect("owner joins B").envelope.body)
                .expect("B join decodes")
                .room_id;
        gw.handle_inbound(
            peer,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id: room_b }.encode()),
        );
        let _ = peer_rx.recv().await.expect("peer joins B");
        while owner_rx.try_recv().is_ok() {}
        while peer_rx.try_recv().is_ok() {}

        let mut delta = DeltaBunch::new(OBJECT, true, 1, 0);
        delta.set(0, FieldDelta::Value(RepValue::Int(37)));
        let sent = gw.handle_inbound(
            owner,
            &Envelope::new(
                KIND_REP_DELTA,
                delta.encode(&schema).expect("delta encodes"),
            ),
        );
        assert_eq!(sent, 0, "a B member cannot write or receive A's object");
        assert!(
            peer_rx.try_recv().is_err(),
            "a prior-room replicated delta must never reach a moved B peer"
        );
    }

    #[tokio::test]
    async fn rep_bootstrap_uses_room_binding_when_match_id_differs() {
        use crate::realtime::netpeer::{
            FieldAuthority, FieldBounds, RepAuthority, RepCondition, RepLayoutBuilder, RepSnapshot,
            TypeTag,
        };
        use citadel_wire::codec::codec_id;
        use citadel_wire::netpeer::{RepFieldCodec, RepSchema, RepValue};
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined};

        const CLASS: u32 = 93;
        const OBJECT: u32 = 703;
        const MATCH: u64 = 8_888;
        let layout = Box::leak(Box::new(
            RepLayoutBuilder::new(CLASS, 1)
                .field(
                    "health",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .build()
                .expect("layout builds"),
        ));
        let schema = RepSchema::new(
            *layout.schema_hash(),
            vec![RepFieldCodec::IntRange { min: 0, max: 100 }],
        )
        .expect("schema builds");
        let rep = Arc::new(RepAuthority::new(Default::default()));
        rep.register_class(CLASS, layout, schema)
            .expect("class registers");
        let gw = Gateway::new().with_rep_authority(rep);

        // This object exists before any room and is therefore intentionally
        // roomless legacy state. A later direct registry drift must never make
        // that state reachable from a room member.
        let (legacy, mut legacy_rx) = register_session(&gw);
        let _ = legacy_rx.recv().await.expect("legacy initial schema");
        gw.spawn_rep_object(704, 0, CLASS, None, false, {
            let mut initial = RepSnapshot::new();
            initial.set_scalar(0, RepValue::Int(11));
            initial
        })
        .expect("roomless object spawn");

        let (owner, mut owner_rx) = register_session(&gw);
        while owner_rx.try_recv().is_ok() {}
        gw.handle_inbound(
            owner,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"A".to_vec(),
                }
                .encode(),
            ),
        );
        let room_id =
            RoomJoined::decode(&owner_rx.recv().await.expect("owner joins A").envelope.body)
                .expect("A join decodes")
                .room_id;
        assert_ne!(MATCH, room_id, "test requires distinct match and room ids");

        let (peer, mut peer_rx) = register_session(&gw);
        while peer_rx.try_recv().is_ok() {}
        gw.handle_inbound(
            peer,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode()),
        );
        let _ = peer_rx.recv().await.expect("peer joins A");

        gw.spawn_rep_object_in_room(OBJECT, MATCH, room_id, CLASS, false, {
            let mut initial = RepSnapshot::new();
            initial.set_scalar(0, RepValue::Int(10));
            initial
        })
        .expect("trusted server object spawn");
        gw.join_rep_match(peer, MATCH, false);

        let mut frames = Vec::new();
        while let Ok(out) = peer_rx.try_recv() {
            frames.push(out);
        }
        assert!(
            frames.iter().any(|out| out.envelope.kind == KIND_REP_DELTA),
            "a same-room non-owner receives the server object's bootstrap baseline"
        );

        // Simulate an attempted lifecycle drift: RoomRegistry still says A but
        // the connection's trusted replication binding says B. Bootstrap must
        // use the binding as an invariant and drop A's object rather than infer
        // scope from the current room membership alone.
        gw.handle_inbound(
            owner,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"B".to_vec(),
                }
                .encode(),
            ),
        );
        let room_b =
            RoomJoined::decode(&owner_rx.recv().await.expect("owner joins B").envelope.body)
                .expect("B join decodes")
                .room_id;
        gw.rep_rooms
            .lock()
            .expect("replication bindings lock")
            .connections
            .insert(peer, room_b);
        let sent = gw.send_rep_bootstrap(peer);
        assert_eq!(
            sent, 1,
            "only the schema frame is safe under a binding mismatch"
        );
        assert_eq!(
            peer_rx.recv().await.expect("schema refresh").envelope.kind,
            citadel_wire::protocol::KIND_REP_SCHEMA
        );
        assert!(
            peer_rx.try_recv().is_err(),
            "a bound-A object must never bootstrap to a bound-B connection"
        );

        // Deliberately bypass the gateway's trusted join API to model a stale
        // extension built against the old public RoomRegistry accessor. The
        // participant remains joined to RepAuthority match 0 but has no room
        // binding; bootstrap must not use the `(None, None)` fallback.
        gw.rooms()
            .join(legacy, room_id)
            .expect("test-only registry drift");
        assert_eq!(gw.send_rep_bootstrap(legacy), 1);
        assert_eq!(
            legacy_rx.recv().await.expect("schema only").envelope.kind,
            citadel_wire::protocol::KIND_REP_SCHEMA
        );
        assert!(
            legacy_rx.try_recv().is_err(),
            "an unbound object cannot bootstrap to a room member"
        );
    }

    #[tokio::test]
    async fn room_scoped_actor_ids_keep_move_commands_independent() {
        use crate::runtime::lua::OutboundCommand;
        use citadel_wire::na::NaSpawn;
        use citadel_wire::room::{RoomCreate, RoomJoined};

        let (gw, hub) = gateway_with_hub();
        let (a, mut ra) = register(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"R1".to_vec(),
                }
                .encode(),
            ),
        );
        let room_one = RoomJoined::decode(&ra.recv().await.expect("R1 joined").envelope.body)
            .expect("R1 joined decodes")
            .room_id;

        let (b, mut rb) = register(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"R2".to_vec(),
                }
                .encode(),
            ),
        );
        let room_two = RoomJoined::decode(&rb.recv().await.expect("R2 joined").envelope.body)
            .expect("R2 joined decodes")
            .room_id;

        let script_actor_id = 0x4000_002Au32;
        assert_eq!(
            gw.apply_external_match_commands(
                room_one,
                vec![OutboundCommand::SpawnActor {
                    object_id: script_actor_id,
                    archetype: 9,
                    position: [1.0, 0.0, 0.0],
                }],
            ),
            1
        );
        let room_one_actor = NaSpawn::decode(&ra.recv().await.expect("R1 spawn").envelope.body)
            .expect("R1 spawn decodes")
            .object_id;

        assert_eq!(
            gw.apply_external_match_commands(
                room_two,
                vec![OutboundCommand::SpawnActor {
                    object_id: script_actor_id,
                    archetype: 9,
                    position: [2.0, 0.0, 0.0],
                }],
            ),
            1
        );
        let room_two_actor = NaSpawn::decode(&rb.recv().await.expect("R2 spawn").envelope.body)
            .expect("R2 spawn decodes")
            .object_id;

        assert_ne!(
            room_one_actor, room_two_actor,
            "the same script actor id in separate rooms needs independent transforms"
        );
        assert_eq!(
            gw.apply_external_match_commands(
                room_one,
                vec![OutboundCommand::MoveActor {
                    object_id: script_actor_id,
                    position: [11.0, 0.0, 0.0],
                    rotation: [0.0, 0.0, 0.0, 1.0],
                    velocity: [0.0; 3],
                }],
            ),
            0
        );
        assert_eq!(
            hub.get_transform(room_one_actor)
                .expect("R1 actor exists")
                .position,
            [11.0, 0.0, 0.0],
            "MoveActor in R1 moves R1's actor"
        );
        assert_eq!(
            hub.get_transform(room_two_actor)
                .expect("R2 actor exists")
                .position,
            [2.0, 0.0, 0.0],
            "MoveActor in R1 must not mutate R2's actor"
        );
    }

    #[tokio::test]
    async fn transform_snapshots_keep_same_room_objects_visible() {
        use citadel_wire::na::{NaPresence, NaTransform};
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined};

        let (gw, hub) = gateway_with_hub();
        let presence = |position| {
            Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 0,
                    transform: NaTransform {
                        position,
                        ..NaTransform::identity()
                    },
                }
                .encode(),
            )
        };

        let (a, mut ra) = register(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"R1".to_vec(),
                }
                .encode(),
            ),
        );
        let room_id = RoomJoined::decode(&ra.recv().await.expect("A room joined").envelope.body)
            .expect("room joined decodes")
            .room_id;
        gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = ra.recv().await.expect("A hello");
        gw.handle_inbound(a, &presence([10.0, 0.0, 0.0]));
        let _ = ra.recv().await.expect("A self spawn");
        let _ = ra.recv().await.expect("A spawn batch");

        let (b, mut rb) = register(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode()),
        );
        let _ = rb.recv().await.expect("B room joined");
        gw.handle_inbound(b, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = rb.recv().await.expect("B hello");
        gw.handle_inbound(b, &presence([20.0, 0.0, 0.0]));
        let _ = rb.recv().await.expect("B self spawn");
        let _ = rb.recv().await.expect("B spawn batch");
        let _ = ra.recv().await.expect("A learns B's spawn");

        assert_eq!(gw.transform_tick(), 2, "one snapshot per room member");
        let codec = *hub.codec();
        let mut view_a = RemoteWorldView::new(codec, 60, 20);
        let mut view_b = RemoteWorldView::new(codec, 60, 20);
        assert!(view_a.apply_datagram(&ra.recv().await.expect("A snapshot").envelope.body));
        assert!(view_b.apply_datagram(&rb.recv().await.expect("B snapshot").envelope.body));

        for view in [&view_a, &view_b] {
            assert!(view.object(1).is_some(), "same-room A object is visible");
            assert!(view.object(2).is_some(), "same-room B object is visible");
        }
    }

    #[tokio::test]
    async fn room_create_auto_joins_creator_and_delivers_map() {
        use citadel_wire::room::{RoomCreate, RoomJoined};
        let gw = Gateway::new();
        let (a, mut ra) = register(&gw);
        let sent = gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"ForestArena".to_vec(),
                }
                .encode(),
            ),
        );
        assert_eq!(sent, 1, "the creator is auto-joined and gets ROOM_JOINED");
        let msg = ra.recv().await.expect("ROOM_JOINED");
        assert_eq!(msg.envelope.kind, KIND_ROOM_JOINED);
        let joined = RoomJoined::decode(&msg.envelope.body).expect("decode");
        assert_eq!(joined.map, "ForestArena", "params became the room map");
        assert_eq!(gw.rooms().room_of(a), Some(joined.room_id));
    }

    #[tokio::test]
    async fn room_join_existing_delivers_same_label() {
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined};
        let gw = Gateway::new();
        let (a, mut ra) = register(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"Lobby".to_vec(),
                }
                .encode(),
            ),
        );
        let room_id = RoomJoined::decode(&ra.recv().await.unwrap().envelope.body)
            .unwrap()
            .room_id;

        let (b, mut rb) = register(&gw);
        let sent = gw.handle_inbound(
            b,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode()),
        );
        assert_eq!(sent, 1);
        let joined = RoomJoined::decode(&rb.recv().await.unwrap().envelope.body).unwrap();
        assert_eq!(joined.room_id, room_id);
        assert_eq!(joined.map, "Lobby", "B sees the same map as A");
        assert_eq!(gw.rooms().members(room_id).len(), 2);
    }

    #[tokio::test]
    async fn room_join_nonexistent_room_is_dropped() {
        use citadel_wire::room::RoomJoin;
        let gw = Gateway::new();
        let (a, _ra) = register(&gw);
        let sent = gw.handle_inbound(
            a,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id: 999 }.encode()),
        );
        assert_eq!(sent, 0, "no such room -> nothing sent");
        assert_eq!(gw.rooms().room_of(a), None);
    }

    #[tokio::test]
    async fn server_error_match_close_notifies_members_and_prunes_room() {
        use citadel_wire::protocol::KIND_MATCH_CLOSED;
        use citadel_wire::room::{
            MATCH_CLOSE_REASON_SERVER_ERROR, MatchClosed, RoomCreate, RoomJoin, RoomJoined,
        };
        let gw = Gateway::new();
        let (a, mut ra) = register(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"M".to_vec(),
                }
                .encode(),
            ),
        );
        let room_id = RoomJoined::decode(&ra.recv().await.unwrap().envelope.body)
            .unwrap()
            .room_id;
        let (b, mut rb) = register(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode()),
        );
        let _ = rb.recv().await; // B's ROOM_JOINED

        // The match fails server-side: both members are informed with a
        // server-error close carrying the client-prompted requeue hint, and
        // the room is pruned.
        assert_eq!(gw.close_match(room_id), 2);
        for receiver in [&mut ra, &mut rb] {
            let outbound = receiver.recv().await.expect("MATCH_CLOSED delivered");
            assert_eq!(outbound.envelope.kind, KIND_MATCH_CLOSED);
            let closed = MatchClosed::decode(&outbound.envelope.body).unwrap();
            assert_eq!(closed.room_id, room_id);
            assert_eq!(closed.reason, MATCH_CLOSE_REASON_SERVER_ERROR);
            assert!(closed.requeue_hint, "members are returned to matchmaking");
        }
        assert_eq!(gw.rooms().room_count(), 0, "the room is pruned");
        assert_eq!(gw.rooms().room_of(a), None);
        assert_eq!(gw.rooms().room_of(b), None);
        // Closing an already-closed match delivers nothing.
        assert_eq!(gw.close_match(room_id), 0);
    }

    /// Records every match-closure notification the gateway sends its runtime.
    /// Every other surface is inert, mirroring [`Runtime`]'s defaults.
    #[derive(Default)]
    struct ClosureProbeRuntime {
        closed: Mutex<Vec<u64>>,
    }

    impl Runtime for ClosureProbeRuntime {
        fn dispatch(
            &self,
            _sender: u64,
            _user_id: Option<&str>,
            _kind: u16,
            _body: &[u8],
        ) -> Vec<OutboundCommand> {
            Vec::new()
        }

        fn dispatch_lifecycle(
            &self,
            _hook: LifecycleHook,
            _sender: u64,
            _user_id: Option<&str>,
        ) -> Vec<OutboundCommand> {
            Vec::new()
        }

        fn tick(
            &self,
            _dt: std::time::Duration,
            _budget: std::time::Duration,
        ) -> Vec<OutboundCommand> {
            Vec::new()
        }

        fn supports_native_match_lifecycle(&self) -> bool {
            true
        }

        fn on_match_closed(&self, room_id: u64) {
            self.closed.lock().expect("closed lock").push(room_id);
        }

        fn call_rpc(
            &self,
            _sender: u64,
            _user_id: Option<&str>,
            _method: &str,
            _body: &[u8],
        ) -> RpcOutcome {
            RpcOutcome::Err("no rpc handlers".to_owned())
        }

        fn call_room_create(
            &self,
            _sender: u64,
            _user_id: Option<&str>,
            _params: &[u8],
        ) -> Option<crate::runtime::RoomSpec> {
            None
        }

        fn call_room_join(&self, _sender: u64, _user_id: Option<&str>, _room_id: u64) -> bool {
            true
        }

        fn has_tick_handler(&self) -> bool {
            false
        }

        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_millis(50)
        }

        fn introspect(&self) -> crate::runtime::RuntimeIntrospection {
            crate::runtime::RuntimeIntrospection {
                source: "closure-probe".to_owned(),
                reloadable: false,
                deadline_ms: 50,
                rpcs: Vec::new(),
                message_kinds: Vec::new(),
                hooks: Vec::new(),
            }
        }
    }

    #[tokio::test]
    async fn member_exodus_notifies_runtime_of_match_closure() {
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined, RoomLeave};

        let runtime = Arc::new(ClosureProbeRuntime::default());
        let gw = Gateway::with_metrics_and_runtime(
            Arc::new(NodeMetrics::new()),
            Some(Arc::clone(&runtime) as Arc<dyn Runtime>),
        );
        let (a, mut ra) = register_session(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"M".to_vec(),
                }
                .encode(),
            ),
        );
        let room_id = RoomJoined::decode(&ra.recv().await.unwrap().envelope.body)
            .unwrap()
            .room_id;
        let (b, _rb) = register_session(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode()),
        );

        // A leaves explicitly: B remains, so the match is still live and the
        // runtime must keep its execution context.
        gw.handle_inbound(
            a,
            &Envelope::new(KIND_ROOM_LEAVE, RoomLeave { room_id }.encode()),
        );
        assert!(
            runtime.closed.lock().expect("closed lock").is_empty(),
            "a leave that does not empty the room is not a match closure"
        );

        // B disconnects: the room empties and is pruned. The runtime must be
        // told to release the match's execution context, exactly like a
        // server-side close — otherwise a process-hosting adapter leaks the
        // worker-side context (thread + engine state) until worker restart.
        gw.unregister_session(b);
        assert_eq!(gw.rooms().room_count(), 0, "the emptied room is pruned");
        assert_eq!(
            runtime.closed.lock().expect("closed lock").as_slice(),
            &[room_id],
            "emptying the room must notify the runtime exactly once"
        );
    }

    #[tokio::test]
    async fn external_worker_sink_applies_room_scoped_commands_and_closes() {
        use crate::runtime::external_worker::MatchCommandSink;
        use citadel_wire::protocol::KIND_MATCH_CLOSED;
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined};

        let gw = Gateway::new();
        let (a, mut ra) = register(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"M".to_vec(),
                }
                .encode(),
            ),
        );
        let room_id = RoomJoined::decode(&ra.recv().await.unwrap().envelope.body)
            .unwrap()
            .room_id;
        let (b, mut rb) = register(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode()),
        );
        let _ = rb.recv().await; // B's ROOM_JOINED
        // A third participant outside the room must not see the broadcast.
        let (_c, mut rc) = register(&gw);

        // A worker command batch applies with room-scoped semantics.
        let delivered = MatchCommandSink::apply_match_commands(
            &gw,
            room_id,
            vec![OutboundCommand::Broadcast {
                kind: 40,
                body: b"state".to_vec(),
                unreliable: false,
            }],
        );
        assert_eq!(delivered, 2, "both room members receive the broadcast");
        for receiver in [&mut ra, &mut rb] {
            let outbound = receiver.recv().await.expect("room broadcast delivered");
            assert_eq!(outbound.envelope.kind, 40);
            assert_eq!(outbound.envelope.body.as_ref(), b"state");
        }
        assert!(rc.try_recv().is_err(), "outsiders receive nothing");

        // A worker-side closure becomes the standard server-side close.
        MatchCommandSink::on_match_closed(
            &gw,
            room_id,
            crate::runtime::worker_data_protocol::MatchCloseReason::ServerError,
        );
        for receiver in [&mut ra, &mut rb] {
            let outbound = receiver.recv().await.expect("MATCH_CLOSED delivered");
            assert_eq!(outbound.envelope.kind, KIND_MATCH_CLOSED);
        }
        assert_eq!(gw.rooms().room_count(), 0, "the room is pruned");
    }

    #[tokio::test]
    async fn worker_death_closes_all_dependent_matches() {
        use citadel_wire::protocol::KIND_MATCH_CLOSED;
        use citadel_wire::room::{MatchClosed, RoomCreate, RoomJoined};
        let gw = Gateway::new();
        // Two independent live matches, one member each. Distinct create
        // params: same-named creates land in one shared room by design.
        let mut members = Vec::new();
        for name in [b"M1".to_vec(), b"M2".to_vec()] {
            let (p, mut rp) = register(&gw);
            gw.handle_inbound(
                p,
                &Envelope::new(KIND_ROOM_CREATE, RoomCreate { params: name }.encode()),
            );
            let room_id = RoomJoined::decode(&rp.recv().await.unwrap().envelope.body)
                .unwrap()
                .room_id;
            members.push((room_id, rp));
        }
        assert_eq!(gw.rooms().room_count(), 2);

        // The worker died: every dependent match closes the same way.
        assert_eq!(gw.close_all_matches(), 2);
        for (room_id, receiver) in &mut members {
            let outbound = receiver.recv().await.expect("MATCH_CLOSED delivered");
            assert_eq!(outbound.envelope.kind, KIND_MATCH_CLOSED);
            assert_eq!(
                MatchClosed::decode(&outbound.envelope.body)
                    .unwrap()
                    .room_id,
                *room_id
            );
        }
        assert_eq!(gw.rooms().room_count(), 0, "all rooms are pruned");
    }

    #[tokio::test]
    async fn disconnect_notifies_remaining_room_members() {
        use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined, RoomLeave};
        let gw = Gateway::new();
        let (a, mut ra) = register_session(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"M".to_vec(),
                }
                .encode(),
            ),
        );
        let room_id = RoomJoined::decode(&ra.recv().await.unwrap().envelope.body)
            .unwrap()
            .room_id;
        let (b, mut rb) = register_session(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode()),
        );
        let _ = rb.recv().await; // B's ROOM_JOINED

        // A disconnects -> B, still in the room, is notified.
        gw.unregister_session(a);
        let leave = rb.recv().await.expect("ROOM_LEAVE to B");
        assert_eq!(leave.envelope.kind, KIND_ROOM_LEAVE);
        assert_eq!(
            RoomLeave::decode(&leave.envelope.body).unwrap().room_id,
            room_id
        );
        assert_eq!(gw.rooms().members(room_id), vec![b]);
    }

    #[tokio::test]
    async fn sim_advances_every_step_while_snapshots_stay_periodic() {
        // Regression for the tick-rate bug: the sim must advance at sim_hz while
        // snapshots go out at send_rate_hz, so server_tick matches the HELLO's
        // sim_rate_hz and the client's interpolation delay is not inflated.
        let (gw, hub) = gateway_with_hub();
        let (a, mut ra) = register(&gw);
        gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = ra.recv().await; // hello reply
        let mut s = TransformState::at([0.0, 0.0, 0.0]);
        s.velocity = [600.0, 0.0, 0.0];
        hub.spawn_server_simulated(1, s);

        // One snapshot period at snapshot_every = 3: three sim steps, one snapshot.
        let t0 = hub.tick();
        gw.transform_sim_step();
        gw.transform_sim_step();
        gw.transform_sim_step();
        assert_eq!(hub.tick(), t0 + 3, "the sim advances on every step");
        let delivered = gw.transform_snapshot_step();
        assert_eq!(delivered, 1, "one snapshot delivered to the one client");
        assert_eq!(
            hub.tick(),
            t0 + 3,
            "the snapshot step must not advance the sim"
        );
    }

    #[tokio::test]
    async fn na_presence_spawns_self_and_notifies_present_peers() {
        use citadel_wire::na::{NaPresence, NaSpawn, NaSpawnBatch, NaTransform};
        let (gw, _hub) = gateway_with_hub();
        let t = NaTransform::identity();

        // A announces presence: gets its own spawn (id 1) + an empty batch.
        let (a, mut ra) = register(&gw);
        let sent = gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 7,
                    transform: t,
                }
                .encode(),
            ),
        );
        assert_eq!(sent, 2, "self spawn + batch to the owner");
        let self_a = ra.recv().await.expect("A self spawn");
        assert_eq!(self_a.envelope.kind, KIND_NA_SPAWN);
        let sp = NaSpawn::decode(&self_a.envelope.body).expect("decode");
        assert_eq!(sp.owner, a.get(), "the owner learns the spawn is its own");
        assert_eq!(sp.object_id, 1);
        assert_eq!(sp.archetype_id, 7);
        let batch_a = ra.recv().await.expect("A batch");
        assert_eq!(batch_a.envelope.kind, KIND_NA_SPAWN_BATCH);
        assert!(
            NaSpawnBatch::decode(&batch_a.envelope.body)
                .expect("decode")
                .spawns
                .is_empty(),
            "nobody present before A"
        );

        // B announces presence: A must be told to spawn B; B's batch carries A.
        let (b, mut rb) = register(&gw);
        let sent = gw.handle_inbound(
            b,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 9,
                    transform: t,
                }
                .encode(),
            ),
        );
        assert_eq!(sent, 3, "B self + B batch + one peer notify to A");

        let _self_b = rb.recv().await.expect("B self spawn");
        let batch_b = rb.recv().await.expect("B batch");
        let batch = NaSpawnBatch::decode(&batch_b.envelope.body).expect("decode");
        assert_eq!(batch.spawns.len(), 1, "B sees A already present");
        assert_eq!(batch.spawns[0].owner, a.get());
        assert_eq!(batch.spawns[0].archetype_id, 7);

        // A receives B's spawn on the peer-notify path.
        let peer = ra.recv().await.expect("A is told to spawn B");
        assert_eq!(peer.envelope.kind, KIND_NA_SPAWN);
        let sp_b = NaSpawn::decode(&peer.envelope.body).expect("decode");
        assert_eq!(sp_b.owner, b.get());
        assert_eq!(sp_b.object_id, 2);
        assert_eq!(sp_b.archetype_id, 9);
    }

    #[tokio::test]
    async fn na_state_moves_only_the_owned_object() {
        use citadel_wire::na::{NaPresence, NaState, NaTransform};
        let (gw, hub) = gateway_with_hub();
        let t = NaTransform::identity();

        let (a, _ra) = register(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 0,
                    transform: t,
                }
                .encode(),
            ),
        );
        let (b, _rb) = register(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 0,
                    transform: t,
                }
                .encode(),
            ),
        );
        // A owns object 1, B owns object 2.
        let moved = NaTransform {
            position: [5.0, 0.0, 0.0],
            ..NaTransform::identity()
        };
        // A moves its own object -> applied.
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_NA_STATE,
                NaState {
                    object_id: 1,
                    transform: moved,
                }
                .encode(),
            ),
        );
        assert!((hub.get_transform(1).expect("obj 1").position[0] - 5.0).abs() < 1e-3);

        // A tries to move B's object (id 2) -> rejected, B's object stays put.
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_NA_STATE,
                NaState {
                    object_id: 2,
                    transform: NaTransform {
                        position: [9.0, 9.0, 9.0],
                        ..NaTransform::identity()
                    },
                }
                .encode(),
            ),
        );
        assert!(
            hub.get_transform(2).expect("obj 2").position[0].abs() < 1e-3,
            "a client cannot move another player's object"
        );
    }

    #[test]
    fn gateway_records_only_validated_authoritative_decisions() {
        let recorder = Arc::new(AuthoritativeDecisionRecorder::new(8));
        // A Correct decision is validated against real match membership, so the
        // corrected object must live in this match; Accept and Reject never
        // touch it. Without a hub `object_in_match` fails closed and the whole
        // batch is rejected before a single decision is recorded.
        let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub"));
        hub.spawn_server_simulated(12, TransformState::at([0.0, 0.0, 0.0]));
        hub.set_object_room(12, Some(7));
        let gateway = Gateway::new()
            .with_transform_hub(Arc::clone(&hub))
            .with_authoritative_decision_recorder(Arc::clone(&recorder))
            .with_bridge(BridgeQuotas::default(), std::collections::HashSet::new());
        let bridge = gateway.bridge.as_ref().expect("bridge attached");
        let batch = bridge
            .ledgers
            .lock()
            .expect("ledger lock")
            .entry(7)
            .or_insert_with(|| PendingBatchLedger::new(7, 1, 1))
            .issue(
                vec![
                    EventDraft::guest(
                        10,
                        NormalizedPayload::ActorStateReport {
                            object_id: 10,
                            transform: BridgeTransform::identity(),
                        },
                    ),
                    EventDraft::guest(
                        11,
                        NormalizedPayload::ActorStateReport {
                            object_id: 11,
                            transform: BridgeTransform::identity(),
                        },
                    ),
                    EventDraft::guest(
                        12,
                        NormalizedPayload::ActorStateReport {
                            object_id: 12,
                            transform: BridgeTransform::identity(),
                        },
                    ),
                ],
                99,
            );
        let mut answer = ScriptCommandBatch::answering(&batch);
        answer.input_outcomes = vec![
            crate::runtime::InputOutcome {
                event_id: batch.events[0].event_id,
                decision: Decision::Accept,
                reply: None,
            },
            crate::runtime::InputOutcome {
                event_id: batch.events[1].event_id,
                decision: Decision::Reject { reason_code: 17 },
                reply: None,
            },
            crate::runtime::InputOutcome {
                event_id: batch.events[2].event_id,
                decision: Decision::Correct {
                    correction: Correction::Transform(BridgeTransform::identity()),
                },
                reply: None,
            },
        ];

        gateway.deliver_command_batch(answer);

        let records = recorder.records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].correlation.match_id, 7);
        assert_eq!(records[0].correlation.batch_id, batch.batch_id);
        assert_eq!(
            records
                .iter()
                .map(|record| record.correlation.event_id)
                .collect::<Vec<_>>(),
            batch
                .events
                .iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>()
        );
        assert_eq!(records[0].outcome, AuthoritativeDecisionOutcome::Accepted);
        assert_eq!(records[1].outcome, AuthoritativeDecisionOutcome::Rejected);
        assert_eq!(
            records[1].reason,
            AuthoritativeDecisionReason::OpaqueCode(17)
        );
        assert_eq!(records[2].outcome, AuthoritativeDecisionOutcome::Corrected);
        assert_eq!(recorder.metrics().recorded_total, 3);
    }

    #[test]
    fn gateway_does_not_record_a_batch_that_fails_validation() {
        let recorder = Arc::new(AuthoritativeDecisionRecorder::new(8));
        let gateway = Gateway::new()
            .with_authoritative_decision_recorder(Arc::clone(&recorder))
            .with_bridge(BridgeQuotas::default(), std::collections::HashSet::new());
        let bridge = gateway.bridge.as_ref().expect("bridge attached");
        let batch = bridge
            .ledgers
            .lock()
            .expect("ledger lock")
            .entry(7)
            .or_insert_with(|| PendingBatchLedger::new(7, 1, 1))
            .issue(
                vec![EventDraft::guest(10, NormalizedPayload::ParticipantJoined)],
                99,
            );
        let answer = ScriptCommandBatch::answering(&batch);

        gateway.deliver_command_batch(answer);

        assert!(recorder.records().is_empty());
        assert_eq!(recorder.metrics().recorded_total, 0);
    }

    // ---- authoritative bridge: KIND_NA_STATE flows through the validator ----

    fn authoritative_gateway(on_input_src: &str) -> (Arc<Gateway>, Arc<TransformHub>) {
        authoritative_gateway_with_quotas(on_input_src, BridgeQuotas::default())
    }

    fn authoritative_gateway_with_quotas(
        on_input_src: &str,
        quotas: BridgeQuotas,
    ) -> (Arc<Gateway>, Arc<TransformHub>) {
        let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub"));
        let runtime: Arc<dyn Runtime> = Arc::new(
            crate::runtime::LuaRuntime::from_source(
                on_input_src,
                "bridge-test",
                crate::runtime::DEFAULT_DEADLINE_MS,
            )
            .expect("lua runtime"),
        );
        let readiness = Arc::new(GameScriptReadiness::new(SystemClock.now()));
        readiness.record_loaded("sha256:test", SystemClock.now());
        let gw = Arc::new(
            Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
                .with_transform_hub(Arc::clone(&hub))
                .with_optional_script_readiness(readiness)
                .with_bridge(quotas, std::collections::HashSet::new()),
        );
        gw.attach_bridge_sink();
        (gw, hub)
    }

    /// Register a participant, give it a relay-owned object (NA_PRESENCE), and
    /// bind it into an authoritative room. Returns the participant and its
    /// owned object id.
    fn authoritative_member(gw: &Arc<Gateway>) -> (ParticipantId, u32, TestOutboundReceiver) {
        authoritative_member_in(gw, "arena")
    }

    /// Like [`authoritative_member`], but bind to a selected authoritative room
    /// so tests exercise two concurrent match scopes.
    fn authoritative_member_in(
        gw: &Arc<Gateway>,
        room_name: &str,
    ) -> (ParticipantId, u32, TestOutboundReceiver) {
        use citadel_wire::na::{NaPresence, NaTransform};
        let (p, rp) = register(gw);
        gw.handle_inbound(
            p,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 0,
                    transform: NaTransform::identity(),
                }
                .encode(),
            ),
        );
        let object_id = gw
            .transform
            .as_ref()
            .and_then(|h| h.relay_owned_object(p.get()))
            .expect("relay-owned object");
        let binding = ScriptBinding {
            revision_id: "sha256:test".to_owned(),
            generation: 1,
        };
        gw.rooms()
            .join_or_create_bound(p, room_name, Some(binding), || {
                RoomLabel::with_map(room_name)
            })
            .expect("bound room");
        (p, object_id, rp)
    }

    fn na_state_frame(object_id: u32, x: f32) -> Envelope {
        use citadel_wire::na::{NaState, NaTransform};
        Envelope::new(
            KIND_NA_STATE,
            NaState {
                object_id,
                transform: NaTransform {
                    position: [x, 0.0, 0.0],
                    ..NaTransform::identity()
                },
            }
            .encode(),
        )
    }

    #[test]
    fn bridge_rejects_foreign_room_objects_for_commands_and_corrections() {
        let recorder = Arc::new(AuthoritativeDecisionRecorder::new(8));
        let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub"));
        let gateway = Gateway::new()
            .with_authoritative_decision_recorder(Arc::clone(&recorder))
            .with_transform_hub(Arc::clone(&hub))
            .with_bridge(BridgeQuotas::default(), std::collections::HashSet::new());
        let room_a = gateway
            .create_room(RoomLabel::with_map("command-room-a"))
            .expect("room A");
        let room_b = gateway
            .create_room(RoomLabel::with_map("command-room-b"))
            .expect("room B");
        let foreign_object = 91;
        hub.spawn_server_simulated(foreign_object, TransformState::at([3.0, 0.0, 0.0]));
        hub.set_object_room(foreign_object, Some(room_b));

        let issue = |event_id| {
            gateway
                .bridge
                .as_ref()
                .expect("bridge")
                .ledgers
                .lock()
                .expect("ledger lock")
                .entry(room_a)
                .or_insert_with(|| PendingBatchLedger::new(room_a, 1, 1))
                .issue(
                    vec![EventDraft::guest(
                        event_id,
                        NormalizedPayload::ActorStateReport {
                            object_id: foreign_object,
                            transform: BridgeTransform::identity(),
                        },
                    )],
                    1,
                )
        };

        let command_batch = issue(1);
        let mut command_answer = ScriptCommandBatch::answering(&command_batch);
        command_answer.input_outcomes = vec![crate::runtime::InputOutcome {
            event_id: command_batch.events[0].event_id,
            decision: Decision::Accept,
            reply: None,
        }];
        command_answer.commands = vec![ScriptCommand::ApplyTransform {
            object_id: foreign_object,
            transform: BridgeTransform {
                position: [100.0, 0.0, 0.0],
                ..BridgeTransform::identity()
            },
        }];
        gateway.deliver_command_batch(command_answer);

        let correction_batch = issue(2);
        let mut correction_answer = ScriptCommandBatch::answering(&correction_batch);
        correction_answer.input_outcomes = vec![crate::runtime::InputOutcome {
            event_id: correction_batch.events[0].event_id,
            decision: Decision::Correct {
                correction: Correction::Transform(BridgeTransform {
                    position: [200.0, 0.0, 0.0],
                    ..BridgeTransform::identity()
                }),
            },
            reply: None,
        }];
        gateway.deliver_command_batch(correction_answer);

        assert!(
            recorder.records().is_empty(),
            "foreign room commands and corrections must fail validation atomically"
        );
        assert_eq!(
            hub.get_transform(foreign_object)
                .expect("foreign object remains live")
                .position,
            [3.0, 0.0, 0.0],
            "a room A answer cannot mutate room B's bound object"
        );
    }

    #[tokio::test]
    async fn simultaneous_authoritative_rooms_never_cross_deliver_b_state() {
        use citadel_wire::na::{NaPresence, NaSpawn, NaTransform};
        use citadel_wire::tsync::Snapshot;

        // The B owner receives a correction and emits an event.  The test then
        // proves that a recipient in A sees neither that event nor B's spawned
        // entity or corrected transform in its own snapshot.
        let (gw, hub) = authoritative_gateway(
            r#"citadel.on_input(function(_)
                citadel.broadcast(100, "room-local-input", true)
                return {
                    decision = "correct",
                    transform = {
                        position = { x = 700, y = 0, z = 0 },
                        rotation = { x = 0, y = 0, z = 0, w = 1 },
                        velocity = { x = 0, y = 0, z = 0 },
                    },
                }
            end)"#,
        );
        let binding = ScriptBinding {
            revision_id: "sha256:test".to_owned(),
            generation: 1,
        };

        let (a, mut a_rx) = register(&gw);
        let (room_a, _) = gw
            .join_or_create_room_bound(a, "authoritative-A", Some(binding.clone()), || {
                RoomLabel::with_map("authoritative-A")
            })
            .expect("A joins its bound room");
        gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = a_rx.recv().await.expect("A hello reply");
        let (a_peer, mut a_peer_rx) = register(&gw);
        let (peer_room_a, _) = gw
            .join_or_create_room_bound(a_peer, "authoritative-A", Some(binding.clone()), || {
                RoomLabel::with_map("authoritative-A")
            })
            .expect("second A recipient joins the same bound room");
        assert_eq!(peer_room_a, room_a, "both A recipients share room A");
        gw.handle_inbound(a_peer, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = a_peer_rx.recv().await.expect("second A hello reply");

        // Register B's relay-owned object before the room binding, then bind B
        // to its own authoritative room before any state report is accepted.
        let (b, mut b_rx) = register(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 1,
                    transform: NaTransform::identity(),
                }
                .encode(),
            ),
        );
        let b_object = hub
            .relay_owned_object(b.get())
            .expect("B relay-owned object");
        let _ = b_rx.recv().await.expect("B self spawn");
        let _ = b_rx.recv().await.expect("B initial spawn batch");
        let (room_b, _) = gw
            .join_or_create_room_bound(b, "authoritative-B", Some(binding), || {
                RoomLabel::with_map("authoritative-B")
            })
            .expect("B joins its bound room");
        assert_ne!(room_a, room_b, "two authoritative rooms are live together");
        gw.handle_inbound(b, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _ = b_rx.recv().await.expect("B hello reply");

        // Server entities use the same script-visible id in both rooms. Their
        // room-scoped runtime ids must nevertheless remain distinct and local.
        assert_eq!(
            gw.apply_external_match_commands(
                room_a,
                vec![OutboundCommand::SpawnActor {
                    object_id: 41,
                    archetype: 1,
                    position: [10.0, 0.0, 0.0],
                }],
            ),
            2,
            "both A recipients receive their own authoritative entity"
        );
        let a_spawn = a_rx.recv().await.expect("A entity spawn");
        assert_eq!(a_spawn.envelope.kind, KIND_NA_SPAWN);
        let a_entity = NaSpawn::decode(&a_spawn.envelope.body)
            .expect("A entity spawn decodes")
            .object_id;
        let a_peer_spawn = a_peer_rx.recv().await.expect("second A entity spawn");
        assert_eq!(a_peer_spawn.envelope.kind, KIND_NA_SPAWN);
        assert_eq!(
            NaSpawn::decode(&a_peer_spawn.envelope.body)
                .expect("second A entity spawn decodes")
                .object_id,
            a_entity,
            "both A recipients see the same A entity"
        );

        assert_eq!(
            gw.apply_external_match_commands(
                room_b,
                vec![OutboundCommand::SpawnActor {
                    object_id: 41,
                    archetype: 1,
                    position: [20.0, 0.0, 0.0],
                }],
            ),
            1,
            "B receives its own authoritative entity"
        );
        let b_spawn = b_rx.recv().await.expect("B entity spawn");
        assert_eq!(b_spawn.envelope.kind, KIND_NA_SPAWN);
        let b_entity = NaSpawn::decode(&b_spawn.envelope.body)
            .expect("B entity spawn decodes")
            .object_id;
        assert_ne!(
            a_entity, b_entity,
            "scoped entities have independent world ids"
        );
        assert!(
            a_rx.try_recv().is_err(),
            "A never receives B's entity spawn"
        );
        assert!(
            a_peer_rx.try_recv().is_err(),
            "the second A recipient never receives B's entity spawn"
        );

        // This is a real authoritative input batch: B's correction writes only
        // B's object and its script event fans out only to B's current room.
        gw.handle_inbound(b, &na_state_frame(b_object, 999.0));
        let b_event = b_rx.recv().await.expect("B room-local script event");
        assert_eq!(b_event.envelope.kind, 100);
        assert_eq!(
            b_event.envelope.body.as_ref(),
            b"room-local-input".as_slice()
        );
        assert!(
            a_rx.try_recv().is_err(),
            "A never receives B's authoritative event or correction side effect"
        );
        assert!(
            a_peer_rx.try_recv().is_err(),
            "the second A recipient never receives B's authoritative event or correction side effect"
        );
        assert_eq!(
            hub.get_transform(b_object)
                .expect("B object survives correction")
                .position,
            [700.0, 0.0, 0.0],
            "B's script correction reaches only B's authoritative transform"
        );

        gw.transform_sim_step();
        assert_eq!(
            gw.transform_snapshot_step(),
            3,
            "each authoritative-room recipient receives exactly one scoped snapshot"
        );
        let a_snapshot = a_rx.recv().await.expect("A scoped snapshot");
        let a_peer_snapshot = a_peer_rx.recv().await.expect("second A scoped snapshot");
        let b_snapshot = b_rx.recv().await.expect("B scoped snapshot");
        assert_eq!(a_snapshot.envelope.kind, KIND_TSYNC_SNAPSHOT);
        assert_eq!(a_peer_snapshot.envelope.kind, KIND_TSYNC_SNAPSHOT);
        assert_eq!(b_snapshot.envelope.kind, KIND_TSYNC_SNAPSHOT);
        let a_snapshot =
            Snapshot::decode(&a_snapshot.envelope.body, hub.codec()).expect("A snapshot decodes");
        let a_peer_snapshot = Snapshot::decode(&a_peer_snapshot.envelope.body, hub.codec())
            .expect("second A snapshot decodes");
        let b_snapshot =
            Snapshot::decode(&b_snapshot.envelope.body, hub.codec()).expect("B snapshot decodes");
        let a_ids: Vec<_> = a_snapshot
            .updates
            .iter()
            .map(|update| update.object_id)
            .collect();
        let a_peer_ids: Vec<_> = a_peer_snapshot
            .updates
            .iter()
            .map(|update| update.object_id)
            .collect();
        let b_ids: Vec<_> = b_snapshot
            .updates
            .iter()
            .map(|update| update.object_id)
            .collect();
        assert!(
            a_ids.contains(&a_entity),
            "A receives its own entity snapshot"
        );
        assert!(
            a_peer_ids.contains(&a_entity),
            "the second A recipient receives its own entity snapshot"
        );
        assert!(
            b_ids.contains(&b_entity),
            "B receives its own entity snapshot"
        );
        assert!(
            b_ids.contains(&b_object),
            "B receives its corrected transform snapshot"
        );
        assert!(
            !a_ids.contains(&b_entity) && !a_ids.contains(&b_object),
            "A never receives B entities, transforms, snapshots, or corrections: {a_ids:?}"
        );
        assert!(
            !a_peer_ids.contains(&b_entity) && !a_peer_ids.contains(&b_object),
            "the second A recipient never receives B state: {a_peer_ids:?}"
        );
        assert!(
            !b_ids.contains(&a_entity),
            "B never receives A entities or snapshots: {b_ids:?}"
        );
    }

    #[tokio::test]
    async fn authoritative_na_state_is_not_applied_without_a_script_answer() {
        // A1/A6/D-policy: a script that never answers (no on_input handler)
        // must not let the owner's report mutate authoritative state. This is
        // the closure of bypass B4 — the default relay movement mode no longer
        // writes verbatim.
        let (gw, hub) = authoritative_gateway("-- no on_input handler");
        let (p, object_id, _rp) = authoritative_member(&gw);
        gw.handle_inbound(p, &na_state_frame(object_id, 5.0));
        assert!(
            hub.get_transform(object_id).expect("object").position[0].abs() < 1e-3,
            "no answer must not mutate authoritative state"
        );
    }

    #[tokio::test]
    async fn authoritative_na_state_applies_only_after_accept() {
        let (gw, hub) = authoritative_gateway("citadel.on_input(function(e) return nil end)");
        let (p, object_id, _rp) = authoritative_member(&gw);
        gw.handle_inbound(p, &na_state_frame(object_id, 5.0));
        assert!(
            (hub.get_transform(object_id).expect("object").position[0] - 5.0).abs() < 1e-3,
            "an accepted owner report materializes the reported transform"
        );
    }

    #[tokio::test]
    async fn authoritative_na_state_correction_overrides_the_client_value() {
        let (gw, hub) = authoritative_gateway(
            r#"citadel.on_input(function(e)
                return {
                    decision = "correct",
                    transform = {
                        position = { x = 1, y = 2, z = 3 },
                        rotation = { x = 0, y = 0, z = 0, w = 1 },
                        velocity = { x = 0, y = 0, z = 0 },
                    },
                }
            end)"#,
        );
        let (p, object_id, _rp) = authoritative_member(&gw);
        gw.handle_inbound(p, &na_state_frame(object_id, 5.0));
        let pos = hub.get_transform(object_id).expect("object").position;
        assert!(
            (pos[0] - 1.0).abs() < 1e-3
                && (pos[1] - 2.0).abs() < 1e-3
                && (pos[2] - 3.0).abs() < 1e-3,
            "the authoritative state carries the script's value, never the client's: {pos:?}"
        );
    }

    #[tokio::test]
    async fn authoritative_na_state_reject_leaves_state_unchanged() {
        let (gw, hub) = authoritative_gateway("citadel.on_input(function(e) return false end)");
        let (p, object_id, _rp) = authoritative_member(&gw);
        gw.handle_inbound(p, &na_state_frame(object_id, 5.0));
        assert!(
            hub.get_transform(object_id).expect("object").position[0].abs() < 1e-3,
            "a rejected report mutates nothing"
        );
    }

    #[tokio::test]
    async fn authoritative_na_state_for_a_foreign_object_is_dropped_structurally() {
        // The structural stage admits only the owner's own object as an event;
        // a report for another id never reaches the script or the world.
        let (gw, hub) = authoritative_gateway("citadel.on_input(function(e) return nil end)");
        let (p, object_id, _rp) = authoritative_member(&gw);
        let foreign = object_id + 1;
        gw.handle_inbound(p, &na_state_frame(foreign, 9.0));
        assert!(
            hub.get_transform(foreign).is_none(),
            "a foreign-object report is dropped before it becomes an event"
        );
    }

    // ---- M1 / owner decision 3: no legacy relay passthrough in a bound match ----

    /// A script that mutates a transform from `on_message`, for both a custom
    /// kind and `KIND_POSITION`. The message body carries the target object id
    /// (u32, big-endian) so the handler knows which actor to move.
    const LEGACY_MUTATION_SCRIPT: &str = r#"
        local function jump(body)
            citadel.move_actor(string.unpack(">I4", body), 42, 0, 0)
        end
        citadel.on_message(1, function(_ctx, body) jump(body) end)
        citadel.on_message(40, function(_ctx, body) jump(body) end)
    "#;

    /// Same script + transform hub as `authoritative_gateway`, but with no
    /// bridge attached: a non-authoritative deployment where the legacy relay /
    /// dispatch path stays live.
    fn non_authoritative_gateway(src: &str) -> (Arc<Gateway>, Arc<TransformHub>) {
        let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub"));
        let runtime: Arc<dyn Runtime> = Arc::new(
            crate::runtime::LuaRuntime::from_source(
                src,
                "relay-test",
                crate::runtime::DEFAULT_DEADLINE_MS,
            )
            .expect("lua runtime"),
        );
        let gw = Arc::new(
            Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
                .with_transform_hub(Arc::clone(&hub)),
        );
        (gw, hub)
    }

    #[tokio::test]
    async fn authoritative_match_closes_the_legacy_relay_passthrough() {
        // Inside a bound match, custom `on_message` and `KIND_POSITION` must not
        // reach `set_transform` via the legacy `dispatch`/`apply_commands_scoped`
        // path — that would bypass the bridge validator (owner decision 3: no
        // relay/passthrough to mutation or replication in an authoritative match).
        let (gw, hub) = authoritative_gateway(LEGACY_MUTATION_SCRIPT);
        let (p, object_id, _rp) = authoritative_member(&gw);
        let body = object_id.to_be_bytes().to_vec();

        gw.handle_inbound(p, &Envelope::new(KIND_POSITION, body.clone()));
        assert!(
            hub.get_transform(object_id).expect("object").position[0].abs() < 1e-3,
            "KIND_POSITION must not reach set_transform inside an authoritative match"
        );

        gw.handle_inbound(p, &Envelope::new(40, body));
        assert!(
            hub.get_transform(object_id).expect("object").position[0].abs() < 1e-3,
            "a custom-kind on_message must not mutate transform outside the validator"
        );
    }

    #[tokio::test]
    async fn non_authoritative_relay_passthrough_still_mutates() {
        // Relay parity: with no bridge attached, the same legacy path still
        // applies the script's transform mutation — closing the bridge must not
        // change the default unzip-and-run behavior.
        let (gw, hub) = non_authoritative_gateway(LEGACY_MUTATION_SCRIPT);
        let (p, _rp) = register(&gw);
        let object_id = 7u32;
        let body = object_id.to_be_bytes().to_vec();

        hub.set_transform(object_id, TransformState::at([0.0, 0.0, 0.0]));
        gw.handle_inbound(p, &Envelope::new(40, body.clone()));
        assert!(
            (hub.get_transform(object_id).expect("object").position[0] - 42.0).abs() < 1e-3,
            "a non-authoritative deployment still applies the legacy transform mutation"
        );

        hub.set_transform(object_id, TransformState::at([0.0, 0.0, 0.0]));
        gw.handle_inbound(p, &Envelope::new(KIND_POSITION, body));
        assert!(
            (hub.get_transform(object_id).expect("object").position[0] - 42.0).abs() < 1e-3,
            "KIND_POSITION still drives the legacy path when no bridge is attached"
        );
    }

    #[tokio::test]
    async fn legacy_custom_kind_40_remains_available_to_authoritative_on_input() {
        let (gw, _hub) = authoritative_gateway(
            r#"citadel.on_input(function(event)
                if event.kind == "message" and event.message_kind == 40 then
                    citadel.send(event.participant, 404, event.body, false)
                end
                return nil
            end)"#,
        );
        let (participant, _object, mut receiver) = authoritative_member_in(&gw, "match-a");
        while receiver.try_recv().is_ok() {}
        gw.handle_inbound(participant, &Envelope::new(40, b"legacy-custom".to_vec()));
        let delivery = receiver
            .try_recv()
            .expect("legacy custom message delivered to on_input");
        assert_eq!(delivery.envelope.kind, 404);
        assert_eq!(delivery.envelope.body.as_ref(), b"legacy-custom");
    }

    #[tokio::test]
    async fn explicit_match_input_sequences_are_distinguishable_as_fresh_duplicate_or_stale() {
        let (gw, _hub) = authoritative_gateway(
            r#"local last = {}
            citadel.on_input(function(event)
                if event.kind == "message" and event.message_kind == 41 then
                    local previous = last[event.participant_id]
                    local state = "fresh"
                    if previous ~= nil and event.sequence == previous then state = "duplicate"
                    elseif previous ~= nil and event.sequence < previous then state = "stale"
                    end
                    if state == "fresh" then last[event.participant_id] = event.sequence end
                    citadel.send(event.participant, 405, state, false)
                end
                return nil
            end)"#,
        );
        let (participant, _object, mut receiver) = authoritative_member_in(&gw, "match-a");
        while receiver.try_recv().is_ok() {}
        for sequence in [7, 7, 6] {
            let body = MatchInput {
                sequence,
                body: Vec::new(),
            }
            .encode()
            .expect("bounded input");
            gw.handle_inbound_with_metadata(
                participant,
                &Envelope::new(KIND_MATCH_INPUT, body),
                InboundMessageMetadata::reliable(),
            );
        }
        let states: Vec<Vec<u8>> = (0..3)
            .map(|_| {
                let delivery = receiver
                    .try_recv()
                    .expect("one script classification per input");
                assert_eq!(delivery.envelope.kind, 405);
                delivery.envelope.body.to_vec()
            })
            .collect();
        assert_eq!(
            states,
            vec![b"fresh".to_vec(), b"duplicate".to_vec(), b"stale".to_vec()]
        );
    }

    #[tokio::test]
    async fn explicit_match_input_rate_limit_drops_flood_before_pending_growth() {
        let quotas = BridgeQuotas {
            max_match_input_messages_per_minute: 1,
            max_match_input_bytes_per_minute: 64 << 10,
            ..BridgeQuotas::default()
        };
        let (gw, _hub) = authoritative_gateway_with_quotas("", quotas);
        let (participant, _object, _receiver) = authoritative_member_in(&gw, "match-a");
        let body = MatchInput {
            sequence: 1,
            body: vec![1],
        }
        .encode()
        .expect("bounded input");
        gw.handle_inbound_with_metadata(
            participant,
            &Envelope::new(KIND_MATCH_INPUT, body.clone()),
            InboundMessageMetadata::reliable(),
        );
        gw.handle_inbound_with_metadata(
            participant,
            &Envelope::new(KIND_MATCH_INPUT, body),
            InboundMessageMetadata::reliable(),
        );
        let room_id = gw.rooms().room_of(participant).expect("bound room");
        let bridge = gw.bridge.as_ref().expect("bridge");
        let ledgers = bridge.ledgers.lock().expect("ledger lock");
        assert_eq!(
            ledgers
                .get(&room_id)
                .expect("first input issued")
                .pending_len(),
            1,
            "the second input must be rejected before it can retain a pending batch"
        );
    }

    #[tokio::test]
    async fn node_wide_pending_cap_prevents_room_spray() {
        let quotas = BridgeQuotas {
            max_pending_batches: 10,
            max_pending_batches_total: 1,
            max_match_input_messages_per_minute: 10,
            max_match_input_bytes_per_minute: 64 << 10,
            ..BridgeQuotas::default()
        };
        let (gw, _hub) = authoritative_gateway_with_quotas("", quotas);
        let (first, _object, _receiver) = authoritative_member_in(&gw, "match-a");
        let (second, _object, _receiver) = authoritative_member_in(&gw, "match-b");
        for participant in [first, second] {
            let body = MatchInput {
                sequence: 1,
                body: vec![1],
            }
            .encode()
            .expect("bounded input");
            gw.handle_inbound_with_metadata(
                participant,
                &Envelope::new(KIND_MATCH_INPUT, body),
                InboundMessageMetadata::reliable(),
            );
        }
        let bridge = gw.bridge.as_ref().expect("bridge");
        let ledgers = bridge.ledgers.lock().expect("ledger lock");
        assert_eq!(
            ledgers
                .values()
                .map(PendingBatchLedger::pending_len)
                .sum::<usize>(),
            1,
            "a caller cannot evade node capacity by distributing unacknowledged input across rooms"
        );
    }

    #[tokio::test]
    async fn explicit_match_input_pending_cap_drops_when_runtime_does_not_answer() {
        let quotas = BridgeQuotas {
            max_pending_batches: 1,
            max_match_input_messages_per_minute: 10,
            max_match_input_bytes_per_minute: 64 << 10,
            ..BridgeQuotas::default()
        };
        let (gw, _hub) = authoritative_gateway_with_quotas("", quotas);
        let (participant, _object, _receiver) = authoritative_member_in(&gw, "match-a");
        for sequence in [1, 2] {
            let body = MatchInput {
                sequence,
                body: vec![1],
            }
            .encode()
            .expect("bounded input");
            gw.handle_inbound_with_metadata(
                participant,
                &Envelope::new(KIND_MATCH_INPUT, body),
                InboundMessageMetadata::reliable(),
            );
        }
        let room_id = gw.rooms().room_of(participant).expect("bound room");
        let bridge = gw.bridge.as_ref().expect("bridge");
        let ledgers = bridge.ledgers.lock().expect("ledger lock");
        assert_eq!(
            ledgers
                .get(&room_id)
                .expect("first input issued")
                .pending_len(),
            1,
            "the pending-batch cap must bound a runtime that has no on_input handler"
        );
    }

    #[tokio::test]
    async fn optional_runtime_allows_relay_and_binds_authoritative_room_from_lua_request() {
        let runtime: Arc<dyn Runtime> = Arc::new(
            crate::runtime::LuaRuntime::from_source(
                r#"citadel.on_room_create(function(_, params)
                    if params == "auth" then
                        return { map = "Arena", bridge_mode = "authoritative" }
                    end
                    return { map = "Lobby", bridge_mode = "relay" }
                end)"#,
                "per-room-mode-test",
                crate::runtime::DEFAULT_DEADLINE_MS,
            )
            .expect("lua runtime"),
        );
        let readiness = Arc::new(GameScriptReadiness::new(SystemClock.now()));
        readiness.record_loaded("sha256:per-room-test", SystemClock.now());
        let gw = Arc::new(
            Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
                .with_optional_script_readiness(Arc::clone(&readiness))
                .with_bridge(BridgeQuotas::default(), std::collections::HashSet::new()),
        );
        gw.attach_bridge_sink();
        let (participant, mut receiver) = register(&gw);
        gw.handle_inbound(
            participant,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"relay".to_vec(),
                }
                .encode(),
            ),
        );
        let _relay_joined = receiver.try_recv().expect("relay room joined");
        let relay_room = gw.rooms().room_of(participant).expect("relay room");
        assert_eq!(gw.rooms().bridge_mode(relay_room), Some(BridgeMode::Relay));
        assert!(gw.rooms().binding(relay_room).is_none());

        gw.handle_inbound(
            participant,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"auth".to_vec(),
                }
                .encode(),
            ),
        );
        let _auth_joined = receiver.try_recv().expect("authoritative room joined");
        let auth_room = gw.rooms().room_of(participant).expect("authoritative room");
        assert_eq!(
            gw.rooms().bridge_mode(auth_room),
            Some(BridgeMode::Authoritative)
        );
        assert_eq!(
            gw.rooms()
                .binding(auth_room)
                .expect("authoritative binding")
                .revision_id,
            "sha256:per-room-test"
        );
    }

    #[tokio::test]
    async fn optional_trusted_authoritative_creation_rejects_unverified_binding() {
        let runtime: Arc<dyn Runtime> = Arc::new(
            crate::runtime::LuaRuntime::from_source(
                "",
                "trusted-binding-test",
                crate::runtime::DEFAULT_DEADLINE_MS,
            )
            .expect("lua runtime"),
        );
        let readiness = Arc::new(GameScriptReadiness::new(SystemClock.now()));
        readiness.record_loaded("sha256:real", SystemClock.now());
        let gw = Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
            .with_optional_script_readiness(readiness)
            .with_bridge(BridgeQuotas::default(), std::collections::HashSet::new());
        let (participant, _receiver) = register(&gw);
        assert_eq!(
            gw.join_or_create_room_bound(
                participant,
                "forged-binding",
                Some(ScriptBinding {
                    revision_id: "sha256:forged".to_owned(),
                    generation: 1,
                }),
                || RoomLabel::with_map("arena"),
            ),
            Err(JoinError::StaleScript)
        );
        assert_eq!(gw.rooms().room_count(), 0);
    }

    #[tokio::test]
    async fn repeat_named_room_create_joins_existing_without_rerunning_create_hook() {
        let runtime: Arc<dyn Runtime> = Arc::new(
            crate::runtime::LuaRuntime::from_source(
                r#"local calls = 0
                citadel.on_room_create(function()
                    calls = calls + 1
                    if calls == 1 then return { bridge_mode = "relay" } end
                    return { bridge_mode = "authoritative" }
                end)"#,
                "create-once-test",
                crate::runtime::DEFAULT_DEADLINE_MS,
            )
            .expect("lua runtime"),
        );
        let readiness = Arc::new(GameScriptReadiness::new(SystemClock.now()));
        readiness.record_loaded("sha256:create-once", SystemClock.now());
        let gw = Arc::new(
            Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
                .with_optional_script_readiness(readiness)
                .with_bridge(BridgeQuotas::default(), std::collections::HashSet::new()),
        );
        gw.attach_bridge_sink();
        let (first, mut first_rx) = register(&gw);
        let (second, mut second_rx) = register(&gw);
        let create = Envelope::new(
            KIND_ROOM_CREATE,
            RoomCreate {
                params: b"same-name".to_vec(),
            }
            .encode(),
        );
        gw.handle_inbound(first, &create);
        let _first_joined = first_rx.try_recv().expect("first create succeeds");
        let room = gw.rooms().room_of(first).expect("first room");
        assert_eq!(gw.rooms().bridge_mode(room), Some(BridgeMode::Relay));
        gw.handle_inbound(second, &create);
        let _second_joined = second_rx
            .try_recv()
            .expect("second create joins existing room");
        assert_eq!(gw.rooms().room_of(second), Some(room));
        assert_eq!(gw.rooms().bridge_mode(room), Some(BridgeMode::Relay));
    }

    #[tokio::test]
    async fn reload_retires_authoritative_rooms_but_preserves_relay_rooms() {
        let (gw, _hub) = authoritative_gateway("citadel.on_input(function() return nil end)");
        let (relay, mut relay_rx) = register(&gw);
        let (relay_room, _) = gw
            .rooms()
            .join_or_create_with_mode(relay, "relay-reload", BridgeMode::Relay, None, || {
                RoomLabel::with_map("relay")
            })
            .expect("relay room");
        let (authoritative, _object, mut authoritative_rx) =
            authoritative_member_in(&gw, "auth-reload");
        let auth_room = gw
            .rooms()
            .room_of(authoritative)
            .expect("authoritative room");
        while relay_rx.try_recv().is_ok() {}
        while authoritative_rx.try_recv().is_ok() {}

        gw.retire_authoritative_rooms_for_reload();
        assert_eq!(gw.rooms().bridge_mode(relay_room), Some(BridgeMode::Relay));
        assert!(gw.rooms().room_of(relay).is_some());
        assert!(gw.rooms().bridge_mode(auth_room).is_none());
        let closed = authoritative_rx
            .try_recv()
            .expect("authoritative member notified of reload retirement");
        assert_eq!(
            closed.envelope.kind,
            citadel_wire::protocol::KIND_MATCH_CLOSED
        );
        assert!(
            relay_rx.try_recv().is_err(),
            "relay room receives no authoritative close"
        );
    }

    #[tokio::test]
    async fn unhealthy_optional_runtime_rejects_authoritative_room_without_creating_it() {
        let runtime: Arc<dyn Runtime> = Arc::new(
            crate::runtime::LuaRuntime::from_source(
                r#"citadel.on_room_create(function()
                    return { bridge_mode = "authoritative" }
                end)"#,
                "per-room-unhealthy-test",
                crate::runtime::DEFAULT_DEADLINE_MS,
            )
            .expect("lua runtime"),
        );
        let readiness = Arc::new(GameScriptReadiness::new(SystemClock.now()));
        let gw = Arc::new(
            Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
                .with_optional_script_readiness(readiness)
                .with_bridge(BridgeQuotas::default(), std::collections::HashSet::new()),
        );
        let (participant, mut receiver) = register(&gw);
        gw.handle_inbound(
            participant,
            &Envelope::new(
                KIND_ROOM_CREATE,
                RoomCreate {
                    params: b"auth".to_vec(),
                }
                .encode(),
            ),
        );
        let rejection = receiver.try_recv().expect("authoritative request rejected");
        assert_eq!(
            rejection.envelope.kind,
            citadel_wire::protocol::KIND_ROOM_REJECT
        );
        assert!(gw.rooms().room_of(participant).is_none());
        assert_eq!(gw.rooms().room_count(), 0);
    }

    #[tokio::test]
    async fn relay_and_authoritative_rooms_coexist_without_crossing_input_routes() {
        let (gw, _hub) = authoritative_gateway(
            r#"
            citadel.on_message(401, function(ctx, body)
                citadel.send(ctx.sender, 402, body, false)
            end)
            citadel.on_input(function(event)
                if event.kind == "message" and event.message_kind == 41 then
                    citadel.send(event.participant, 403, event.body, false)
                end
                return nil
            end)
            "#,
        );
        let (relay, mut relay_rx) = register(&gw);
        let (relay_room, _) = gw
            .rooms()
            .join_or_create_with_mode(relay, "relay-room", BridgeMode::Relay, None, || {
                RoomLabel::with_map("relay")
            })
            .expect("relay room");
        let (authoritative, _object, mut authoritative_rx) =
            authoritative_member_in(&gw, "auth-room");
        while relay_rx.try_recv().is_ok() {}
        while authoritative_rx.try_recv().is_ok() {}

        gw.handle_inbound(relay, &Envelope::new(401, b"legacy".to_vec()));
        let legacy = relay_rx
            .try_recv()
            .expect("relay room receives legacy handler output");
        assert_eq!(legacy.envelope.kind, 402);
        assert_eq!(legacy.envelope.body.as_ref(), b"legacy");
        assert!(
            authoritative_rx.try_recv().is_err(),
            "relay output stays in relay room"
        );

        let input = MatchInput {
            sequence: 1,
            body: b"v1".to_vec(),
        }
        .encode()
        .expect("bounded input");
        gw.handle_inbound_with_metadata(
            relay,
            &Envelope::new(KIND_MATCH_INPUT, input.clone()),
            InboundMessageMetadata::reliable(),
        );
        assert!(
            relay_rx.try_recv().is_err(),
            "relay rooms reject explicit match input"
        );
        assert!(
            authoritative_rx.try_recv().is_err(),
            "relay input cannot cross to authoritative room"
        );

        gw.handle_inbound_with_metadata(
            authoritative,
            &Envelope::new(KIND_MATCH_INPUT, input),
            InboundMessageMetadata::reliable(),
        );
        let authoritative_delivery = authoritative_rx
            .try_recv()
            .expect("authoritative room receives V1 script output");
        assert_eq!(authoritative_delivery.envelope.kind, 403);
        assert_eq!(authoritative_delivery.envelope.body.as_ref(), b"v1");
        assert_eq!(gw.rooms().bridge_mode(relay_room), Some(BridgeMode::Relay));
    }

    #[tokio::test]
    async fn explicit_match_input_routes_to_authenticated_member_and_private_ack_only() {
        let (gw, _hub) = authoritative_gateway(
            r#"citadel.on_input(function(event)
                if event.kind == "message" and event.message_kind == 41 then
                    assert(event.participant_id == "1")
                    assert(event.sequence == "77")
                    assert(event.body == "opaque\0body")
                    citadel.match.set_input_ack(event.participant_id, event.sequence)
                end
                return nil
            end)"#,
        );
        let (a, _a_object, mut a_rx) = authoritative_member_in(&gw, "match-a");
        let (_b, _b_object, mut b_rx) = authoritative_member_in(&gw, "match-b");
        while a_rx.try_recv().is_ok() {}
        while b_rx.try_recv().is_ok() {}

        let body = MatchInput {
            sequence: 77,
            body: b"opaque\0body".to_vec(),
        }
        .encode()
        .expect("bounded input");
        gw.handle_inbound_with_metadata(
            a,
            &Envelope::new(KIND_MATCH_INPUT, body),
            InboundMessageMetadata::reliable(),
        );

        let ack = a_rx
            .try_recv()
            .expect("sender receives private input acknowledgement");
        assert_eq!(ack.envelope.kind, KIND_MATCH_INPUT_ACK);
        assert_eq!(
            MatchInputAck::decode(&ack.envelope.body).expect("valid private acknowledgement"),
            MatchInputAck {
                last_processed_sequence: 77
            }
        );
        assert!(
            b_rx.try_recv().is_err(),
            "a match input acknowledgement must never cross into another match"
        );
    }

    #[tokio::test]
    async fn room_move_drops_final_old_match_input_state() {
        let (gw, _hub) = authoritative_gateway("");
        let (participant, _object, _receiver) = authoritative_member_in(&gw, "match-a");
        let old_room = gw.rooms().room_of(participant).expect("old room");
        let body = MatchInput {
            sequence: 1,
            body: vec![1],
        }
        .encode()
        .expect("bounded input");
        gw.handle_inbound_with_metadata(
            participant,
            &Envelope::new(KIND_MATCH_INPUT, body),
            InboundMessageMetadata::reliable(),
        );
        let binding = ScriptBinding {
            revision_id: "sha256:test".to_owned(),
            generation: 1,
        };
        gw.join_or_create_room_bound(participant, "match-b", Some(binding), || {
            RoomLabel::with_map("match-b")
        })
        .expect("move to successor room");
        let bridge = gw.bridge.as_ref().expect("bridge");
        assert!(
            !bridge
                .ledgers
                .lock()
                .expect("ledger lock")
                .contains_key(&old_room),
            "moving out of a final old room must release its pending input ledger"
        );
        assert!(
            !bridge
                .input_rate_windows
                .lock()
                .expect("rate lock")
                .contains_key(&(old_room, participant)),
            "moving out of a final old room must release its participant admission state"
        );
    }

    #[tokio::test]
    async fn final_leave_drops_pending_match_input_ledger() {
        let (gw, _hub) = authoritative_gateway("");
        let (participant, _object, _receiver) = authoritative_member_in(&gw, "match-a");
        let room_id = gw.rooms().room_of(participant).expect("bound room");
        let body = MatchInput {
            sequence: 1,
            body: vec![1],
        }
        .encode()
        .expect("bounded input");
        gw.handle_inbound_with_metadata(
            participant,
            &Envelope::new(KIND_MATCH_INPUT, body),
            InboundMessageMetadata::reliable(),
        );
        assert!(
            gw.bridge
                .as_ref()
                .expect("bridge")
                .ledgers
                .lock()
                .expect("ledger lock")
                .contains_key(&room_id),
            "the no-answer input is retained only until lifecycle cleanup"
        );
        gw.leave_room(participant);
        assert!(
            !gw.bridge
                .as_ref()
                .expect("bridge")
                .ledgers
                .lock()
                .expect("ledger lock")
                .contains_key(&room_id),
            "a final leave must release pending input and its capacity"
        );
    }

    #[tokio::test]
    async fn stale_match_input_ack_cannot_leak_across_disconnect_and_successor_room() {
        let (gw, _hub) = authoritative_gateway("citadel.on_input(function() return nil end)");
        let (old_participant, _object, mut old_receiver) = authoritative_member_in(&gw, "match-a");
        while old_receiver.try_recv().is_ok() {}
        let old_room = gw.rooms().room_of(old_participant).expect("old room");
        gw.unregister_session(old_participant);

        let (_successor, _object, mut successor_receiver) = authoritative_member_in(&gw, "match-a");
        while successor_receiver.try_recv().is_ok() {}
        assert_eq!(
            gw.send_match_input_ack(old_room, old_participant.get(), 77),
            0,
            "the disconnected participant is no longer a member of the old match"
        );
        assert!(
            successor_receiver.try_recv().is_err(),
            "a stale acknowledgement cannot be delivered to a successor session or room"
        );
    }

    #[tokio::test]
    async fn client_sent_match_input_ack_is_dropped_before_script_dispatch() {
        let (gw, _hub) = authoritative_gateway(
            r#"citadel.on_input(function(event)
                citadel.broadcast(403, "should-not-run", false)
                return nil
            end)"#,
        );
        let (participant, _object, mut receiver) = authoritative_member_in(&gw, "match-a");
        while receiver.try_recv().is_ok() {}

        gw.handle_inbound_with_metadata(
            participant,
            &Envelope::new(
                KIND_MATCH_INPUT_ACK,
                MatchInputAck {
                    last_processed_sequence: 77,
                }
                .encode(),
            ),
            InboundMessageMetadata::reliable(),
        );
        assert!(
            receiver.try_recv().is_err(),
            "a client cannot inject the server-owned acknowledgement control"
        );
    }

    #[tokio::test]
    async fn explicit_match_input_after_leave_is_dropped_without_acknowledgement() {
        let (gw, _hub) = authoritative_gateway(
            r#"citadel.on_input(function(event)
                if event.kind == "message" and event.message_kind == 41 then
                    citadel.match.set_input_ack(event.participant_id, event.sequence)
                end
                return nil
            end)"#,
        );
        let (participant, _object, mut receiver) = authoritative_member_in(&gw, "match-a");
        while receiver.try_recv().is_ok() {}
        gw.leave_room(participant);
        while receiver.try_recv().is_ok() {}

        let body = MatchInput {
            sequence: 77,
            body: b"late".to_vec(),
        }
        .encode()
        .expect("bounded input");
        assert_eq!(
            gw.handle_inbound_with_metadata(
                participant,
                &Envelope::new(KIND_MATCH_INPUT, body),
                InboundMessageMetadata::reliable(),
            ),
            0
        );
        assert!(
            receiver.try_recv().is_err(),
            "a participant that left its old match cannot receive an old-match acknowledgement"
        );
    }

    #[tokio::test]
    async fn authoritative_custom_message_reaches_only_its_match_as_a_fenced_input() {
        // Two concurrent bound matches prove that a generic client message is
        // normalized through on_input and cannot fan out across the room boundary.
        let (gw, _hub) = authoritative_gateway(
            r#"citadel.on_input(function(event)
                if event.kind == "message" then
                    assert(event.message_kind == 401)
                    assert(event.reliable == true)
                    assert(event.sequence == nil)
                    citadel.broadcast(402, event.body, false)
                end
                return nil
            end)"#,
        );
        let (a, _a_object, mut a_rx) = authoritative_member_in(&gw, "match-a");
        let (_a_peer, _a_peer_object, mut a_peer_rx) = authoritative_member_in(&gw, "match-a");
        let (_b, _b_object, mut b_rx) = authoritative_member_in(&gw, "match-b");
        while a_rx.try_recv().is_ok() {}
        while a_peer_rx.try_recv().is_ok() {}
        while b_rx.try_recv().is_ok() {}

        gw.handle_inbound(a, &Envelope::new(401, b"opaque\0body".to_vec()));

        let sender_delivery = a_rx.try_recv().expect("sender receives scoped command");
        assert_eq!(sender_delivery.envelope.kind, 402);
        assert_eq!(sender_delivery.envelope.body.as_ref(), b"opaque\0body");
        assert_eq!(sender_delivery.delivery, Delivery::Reliable);
        let peer_delivery = a_peer_rx
            .try_recv()
            .expect("same-match peer receives command");
        assert_eq!(peer_delivery.envelope.kind, 402);
        assert_eq!(peer_delivery.envelope.body.as_ref(), b"opaque\0body");
        assert!(
            b_rx.try_recv().is_err(),
            "a bound match's custom input and returned command cannot cross to another match"
        );
    }

    #[tokio::test]
    async fn authoritative_custom_message_from_non_member_never_reaches_on_input() {
        // This uses the gateway's structural entry point with an attacker-owned
        // participant and a server-owned target room. The public inbound route
        // cannot provide that target room, which is exactly why this assertion
        // proves a forged cross-match target is rejected before runtime.
        let (gw, _hub) = authoritative_gateway(
            r#"citadel.on_input(function(event)
                if event.kind == "message" then citadel.broadcast(403, event.body, false) end
                return nil
            end)"#,
        );
        let (member, _object, mut member_rx) = authoritative_member_in(&gw, "match-a");
        let (outsider, mut outsider_rx) = register(&gw);
        while member_rx.try_recv().is_ok() {}
        while outsider_rx.try_recv().is_ok() {}
        let room_id = gw.rooms().room_of(member).expect("member room");
        let binding = gw.rooms().binding(room_id).expect("bound room");

        gw.route_bridge_match_message(
            outsider,
            &Envelope::new(401, b"forged-target".to_vec()),
            room_id,
            &binding,
            InboundMessageMetadata::reliable(),
        );

        assert!(
            member_rx.try_recv().is_err(),
            "non-member input never invokes the script"
        );
        assert!(
            outsider_rx.try_recv().is_err(),
            "non-member receives no authoritative output"
        );
    }

    #[tokio::test]
    async fn authoritative_custom_message_over_limit_is_dropped_before_runtime() {
        let (gw, _hub) = authoritative_gateway(
            r#"citadel.on_input(function(event)
                if event.kind == "message" then citadel.broadcast(404, event.body, false) end
                return nil
            end)"#,
        );
        let (member, _object, mut member_rx) = authoritative_member_in(&gw, "match-a");
        while member_rx.try_recv().is_ok() {}

        gw.handle_inbound(
            member,
            &Envelope::new(401, vec![0; MAX_MATCH_MESSAGE_BODY_BYTES + 1]),
        );

        assert!(
            member_rx.try_recv().is_err(),
            "an oversized opaque body is rejected before on_input can emit commands"
        );
    }

    // ---- authoritative bridge: KIND_TSYNC_INPUT flows through the validator ----

    fn authoritative_gateway_slots(
        on_input_src: &str,
        slots: u32,
    ) -> (Arc<Gateway>, Arc<TransformHub>) {
        let cfg = TransformHubConfig {
            player_slots: slots,
            ..TransformHubConfig::default()
        };
        let hub = Arc::new(TransformHub::new(cfg).expect("hub"));
        let runtime: Arc<dyn Runtime> = Arc::new(
            crate::runtime::LuaRuntime::from_source(
                on_input_src,
                "bridge-test",
                crate::runtime::DEFAULT_DEADLINE_MS,
            )
            .expect("lua runtime"),
        );
        let readiness = Arc::new(GameScriptReadiness::new(SystemClock.now()));
        readiness.record_loaded("sha256:test", SystemClock.now());
        let gw = Arc::new(
            Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
                .with_transform_hub(Arc::clone(&hub))
                .with_optional_script_readiness(readiness)
                .with_bridge(BridgeQuotas::default(), std::collections::HashSet::new()),
        );
        gw.attach_bridge_sink();
        (gw, hub)
    }

    /// HELLO a participant into a player slot (owned object + epoch), then bind
    /// it into an authoritative room. Returns the participant, its object id,
    /// and the assigned ownership epoch.
    async fn authoritative_slot_member(gw: &Arc<Gateway>) -> (ParticipantId, u32, u32) {
        let (p, mut rp) = register(gw);
        gw.handle_inbound(p, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _hello = rp.recv().await.expect("hello reply");
        let role_out = rp.recv().await.expect("role frame");
        let role = tsync::Role::decode(&role_out.envelope.body).expect("role decodes");
        let binding = ScriptBinding {
            revision_id: "sha256:test".to_owned(),
            generation: 1,
        };
        gw.rooms()
            .join_or_create_bound(p, "arena", Some(binding), || RoomLabel::with_map("arena"))
            .expect("bound room");
        (p, role.object_id, role.ownership_epoch)
    }

    fn input_bundle(object_id: u32, epoch: u32, seq: u32, vel_x: f32, dt: f32) -> Envelope {
        let bundle = tsync::InputBundle {
            acked_snapshot_id: 0,
            last_seen_snapshot_id: 0,
            frames: vec![tsync::InputFrame {
                input_seq: seq,
                sim_tick: 0,
                dt,
                object_id,
                ownership_epoch: epoch,
                move_velocity: [vel_x, 0.0, 0.0],
                payload: Vec::new(),
                fire: None,
            }],
        };
        Envelope::new(KIND_TSYNC_INPUT, bundle.encode())
    }

    #[tokio::test]
    async fn authoritative_input_is_not_integrated_without_a_script_answer() {
        let (gw, hub) = authoritative_gateway_slots("-- no on_input handler", 1);
        let (a, object_id, epoch) = authoritative_slot_member(&gw).await;
        let start = hub.get_transform(object_id).expect("object").position[0];
        gw.handle_inbound(a, &input_bundle(object_id, epoch, 1, 100.0, 0.1));
        assert!(
            (hub.get_transform(object_id).expect("object").position[0] - start).abs() < 1e-3,
            "no answer must not integrate owner input (bypass B1 closed)"
        );
    }

    #[tokio::test]
    async fn authoritative_input_integrates_only_after_accept() {
        let (gw, hub) =
            authoritative_gateway_slots("citadel.on_input(function(e) return nil end)", 1);
        let (a, object_id, epoch) = authoritative_slot_member(&gw).await;
        let start = hub.get_transform(object_id).expect("object").position[0];
        gw.handle_inbound(a, &input_bundle(object_id, epoch, 1, 100.0, 0.1));
        assert!(
            hub.get_transform(object_id).expect("object").position[0] > start + 1.0,
            "an accepted input integrates the movement intent"
        );
    }

    #[tokio::test]
    async fn accepted_bridge_input_after_room_move_is_dropped() {
        let (gw, hub) = authoritative_gateway_slots("-- deferred validator", 1);
        let (participant, object_id, ownership_epoch) = authoritative_slot_member(&gw).await;
        let room_a = gw.rooms().room_of(participant).expect("member starts in A");
        let room_b = gw
            .create_room(RoomLabel::with_map("B"))
            .expect("test runtime supports lifecycle");
        gw.join_room(participant, room_b)
            .expect("trusted move to B");

        let payload = NormalizedPayload::TransformInput {
            object_id,
            ownership_epoch,
            input_seq: 1,
            sim_tick: 0,
            dt: 0.1,
            move_velocity: [100.0, 0.0, 0.0],
            payload: Vec::new(),
            fire: None,
        };
        let start = hub.get_transform(object_id).expect("object").position;
        let scope = gw.lock_room_scope();
        assert_eq!(
            gw.materialize_accept_under_scope(room_a, &payload, participant.get()),
            0,
            "a delayed A acceptance is dropped after the participant moves to B"
        );
        drop(scope);
        assert_eq!(
            hub.get_transform(object_id).expect("object").position,
            start,
            "the old-room input cannot become transform state in B"
        );
    }

    #[tokio::test]
    async fn stale_bridge_input_after_a_move_cannot_restore_the_old_room_binding() {
        let (gw, hub) = authoritative_gateway_slots("-- deferred validator", 1);
        let (participant, object_id, ownership_epoch) = authoritative_slot_member(&gw).await;
        let room_a = gw.rooms().room_of(participant).expect("member starts in A");
        let binding_a = gw.rooms().binding(room_a).expect("A binding");
        let (room_b, _) = gw
            .join_or_create_room_bound(participant, "B", Some(binding_a.clone()), || {
                RoomLabel::with_map("B")
            })
            .expect("trusted bound move to B");
        let binding_b = gw.rooms().binding(room_b).expect("B binding");
        let frame = citadel_wire::tsync::InputFrame {
            input_seq: 1,
            sim_tick: 0,
            dt: 0.1,
            object_id,
            ownership_epoch,
            move_velocity: [100.0, 0.0, 0.0],
            payload: Vec::new(),
            fire: None,
        };

        gw.route_bridge_input(
            participant,
            std::slice::from_ref(&frame),
            room_b,
            &binding_b,
        );
        assert_eq!(hub.object_room(object_id), Some(room_b));
        gw.route_bridge_input(participant, &[frame], room_a, &binding_a);

        assert_eq!(
            hub.object_room(object_id),
            Some(room_b),
            "an ingress frame captured before a move cannot restore its old room binding"
        );
    }

    #[tokio::test]
    async fn stale_bridge_na_state_after_a_move_cannot_restore_the_old_room_binding() {
        let (gw, hub) = authoritative_gateway("-- deferred validator");
        let (participant, object_id, _receiver) = authoritative_member(&gw);
        let room_a = gw.rooms().room_of(participant).expect("member starts in A");
        let binding_a = gw.rooms().binding(room_a).expect("A binding");
        let (room_b, _) = gw
            .join_or_create_room_bound(participant, "B", Some(binding_a.clone()), || {
                RoomLabel::with_map("B")
            })
            .expect("trusted bound move to B");
        let binding_b = gw.rooms().binding(room_b).expect("B binding");
        let state = na_state_frame(object_id, 5.0);

        gw.route_bridge_na_state(participant, &state, room_b, &binding_b);
        assert_eq!(hub.object_room(object_id), Some(room_b));
        gw.route_bridge_na_state(participant, &state, room_a, &binding_a);

        assert_eq!(
            hub.object_room(object_id),
            Some(room_b),
            "an ingress report captured before a move cannot restore its old room binding"
        );
    }

    #[tokio::test]
    async fn stale_bridge_na_state_binding_after_a_reload_cannot_bind_an_object() {
        let (gw, hub) = authoritative_gateway("-- deferred validator");
        let (participant, object_id, _receiver) = authoritative_member(&gw);
        let room_id = gw.rooms().room_of(participant).expect("member room");
        let mut stale_binding = gw.rooms().binding(room_id).expect("room binding");
        stale_binding.generation += 1;
        hub.set_object_room(object_id, None);

        gw.route_bridge_na_state(
            participant,
            &na_state_frame(object_id, 5.0),
            room_id,
            &stale_binding,
        );

        assert_eq!(
            hub.object_room(object_id),
            None,
            "a report fenced to a superseded script binding cannot write object_rooms"
        );
    }

    #[tokio::test]
    async fn authoritative_input_reject_integrates_nothing() {
        let (gw, hub) =
            authoritative_gateway_slots("citadel.on_input(function(e) return false end)", 1);
        let (a, object_id, epoch) = authoritative_slot_member(&gw).await;
        let start = hub.get_transform(object_id).expect("object").position[0];
        gw.handle_inbound(a, &input_bundle(object_id, epoch, 1, 100.0, 0.1));
        assert!(
            (hub.get_transform(object_id).expect("object").position[0] - start).abs() < 1e-3,
            "a rejected input integrates nothing"
        );
    }

    #[tokio::test]
    async fn authoritative_input_correction_overrides_the_integrated_value() {
        let (gw, hub) = authoritative_gateway_slots(
            r#"citadel.on_input(function(e)
                return {
                    decision = "correct",
                    transform = {
                        position = { x = 3, y = 0, z = 0 },
                        rotation = { x = 0, y = 0, z = 0, w = 1 },
                        velocity = { x = 0, y = 0, z = 0 },
                    },
                }
            end)"#,
            1,
        );
        let (a, object_id, epoch) = authoritative_slot_member(&gw).await;
        gw.handle_inbound(a, &input_bundle(object_id, epoch, 1, 100.0, 0.1));
        let pos = hub.get_transform(object_id).expect("object").position;
        assert!(
            (pos[0] - 3.0).abs() < 1e-3,
            "the script's corrected transform is materialized, not the client's integration: {pos:?}"
        );
    }

    #[tokio::test]
    async fn authoritative_input_for_a_foreign_object_is_dropped_structurally() {
        let (gw, hub) =
            authoritative_gateway_slots("citadel.on_input(function(e) return nil end)", 1);
        let (a, object_id, epoch) = authoritative_slot_member(&gw).await;
        let foreign = object_id + 100;
        gw.handle_inbound(a, &input_bundle(foreign, epoch, 1, 100.0, 0.1));
        assert!(
            hub.get_transform(foreign).is_none(),
            "input for an unowned object never becomes an event or mutates"
        );
    }

    // ---- authoritative bridge: KIND_NA_PRESENCE flows through the validator ----

    async fn authoritative_room_member(gw: &Arc<Gateway>) -> (ParticipantId, TestOutboundReceiver) {
        let (p, rp) = register(gw);
        let binding = ScriptBinding {
            revision_id: "sha256:test".to_owned(),
            generation: 1,
        };
        gw.rooms()
            .join_or_create_bound(p, "arena", Some(binding), || RoomLabel::with_map("arena"))
            .expect("bound room");
        (p, rp)
    }

    fn presence_frame() -> Envelope {
        use citadel_wire::na::{NaPresence, NaTransform};
        Envelope::new(
            KIND_NA_PRESENCE,
            NaPresence {
                archetype_id: 0,
                transform: NaTransform::identity(),
            }
            .encode(),
        )
    }

    #[tokio::test]
    async fn authoritative_presence_does_not_spawn_without_a_script_answer() {
        let (gw, hub) = authoritative_gateway("-- no on_input handler");
        let (a, _ra) = authoritative_room_member(&gw).await;
        gw.handle_inbound(a, &presence_frame());
        assert!(
            hub.relay_owned_object(a.get()).is_none(),
            "no answer must not register a presence or fan out a spawn (bypass B5 closed)"
        );
    }

    #[tokio::test]
    async fn accepted_bridge_presence_after_room_move_is_dropped() {
        let (gw, hub) = authoritative_gateway("-- deferred validator");
        let (participant, _receiver) = authoritative_room_member(&gw).await;
        let room_a = gw.rooms().room_of(participant).expect("member starts in A");
        let room_b = gw
            .create_room(RoomLabel::with_map("B"))
            .expect("test runtime supports lifecycle");
        gw.join_room(participant, room_b)
            .expect("trusted move to B");

        let payload = NormalizedPayload::SpawnRequest {
            archetype_id: 0,
            transform: BridgeTransform::identity(),
        };
        let scope = gw.lock_room_scope();
        assert_eq!(
            gw.materialize_accept_under_scope(room_a, &payload, participant.get()),
            0,
            "a delayed A spawn request is dropped after the participant moves"
        );
        drop(scope);
        assert!(
            hub.relay_owned_object(participant.get()).is_none(),
            "the stale acceptance cannot spawn a B-visible networked actor"
        );
    }

    #[tokio::test]
    async fn authoritative_presence_spawns_only_after_accept() {
        let (gw, hub) = authoritative_gateway("citadel.on_input(function(e) return nil end)");
        let (a, _ra) = authoritative_room_member(&gw).await;
        gw.handle_inbound(a, &presence_frame());
        assert!(
            hub.relay_owned_object(a.get()).is_some(),
            "an accepted spawn request registers the avatar"
        );
    }

    #[tokio::test]
    async fn authoritative_presence_reject_spawns_nothing() {
        let (gw, hub) = authoritative_gateway("citadel.on_input(function(e) return false end)");
        let (a, _ra) = authoritative_room_member(&gw).await;
        gw.handle_inbound(a, &presence_frame());
        assert!(
            hub.relay_owned_object(a.get()).is_none(),
            "a rejected spawn request registers nothing"
        );
    }

    // ---- authoritative bridge: KIND_REP_DELTA flows through the validator ----

    const REP_CLASS: u32 = 42;
    const REP_OBJ: u32 = 500;
    const REP_MATCH: u64 = 1;
    const REP_HEALTH: u16 = 0;

    fn rep_layout() -> &'static RepLayout {
        use crate::realtime::netpeer::{FieldAuthority, FieldBounds, RepCondition, TypeTag};
        use citadel_wire::codec::codec_id;
        static L: std::sync::OnceLock<RepLayout> = std::sync::OnceLock::new();
        L.get_or_init(|| {
            crate::realtime::netpeer::RepLayoutBuilder::new(REP_CLASS, 1)
                .field(
                    "health",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .build()
                .expect("layout builds")
        })
    }

    fn rep_schema() -> RepSchema {
        RepSchema::new(
            *rep_layout().schema_hash(),
            vec![citadel_wire::netpeer::RepFieldCodec::IntRange { min: 0, max: 100 }],
        )
        .expect("schema builds")
    }

    fn client_health_bunch(result_id: u64, health: i64) -> Vec<u8> {
        let mut b = citadel_wire::netpeer::DeltaBunch::new(REP_OBJ, true, result_id, 0);
        b.set(
            REP_HEALTH,
            FieldDelta::Value(citadel_wire::netpeer::RepValue::Int(health)),
        );
        b.encode(&rep_schema()).expect("client encodes")
    }

    async fn authoritative_rep_gateway(
        on_input_src: &str,
    ) -> (Arc<Gateway>, Arc<RepAuthority>, ParticipantId) {
        let rep = Arc::new(RepAuthority::new(
            crate::realtime::netpeer::RateLimits::default(),
        ));
        let runtime: Arc<dyn Runtime> = Arc::new(
            crate::runtime::LuaRuntime::from_source(
                on_input_src,
                "bridge-test",
                crate::runtime::DEFAULT_DEADLINE_MS,
            )
            .expect("lua runtime"),
        );
        let readiness = Arc::new(GameScriptReadiness::new(SystemClock.now()));
        readiness.record_loaded("sha256:test", SystemClock.now());
        let gw = Arc::new(
            Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
                .with_rep_authority(Arc::clone(&rep))
                .with_optional_script_readiness(readiness)
                .with_bridge(BridgeQuotas::default(), std::collections::HashSet::new()),
        );
        gw.attach_bridge_sink();
        gw.register_rep_class(REP_CLASS, rep_layout(), rep_schema())
            .expect("class registers");
        let (owner, _ro) = register(&gw);
        let binding = ScriptBinding {
            revision_id: "sha256:test".to_owned(),
            generation: 1,
        };
        gw.rooms()
            .join_or_create_bound(owner, "arena", Some(binding), || {
                RoomLabel::with_map("arena")
            })
            .expect("bound room");
        let mut initial = crate::realtime::netpeer::RepSnapshot::new();
        initial.set_scalar(REP_HEALTH, citadel_wire::netpeer::RepValue::Int(10));
        gw.spawn_rep_object(REP_OBJ, REP_MATCH, REP_CLASS, Some(owner), false, initial)
            .expect("trusted lifecycle spawns object");
        gw.join_rep_match(owner, REP_MATCH, false);
        (gw, rep, owner)
    }

    #[tokio::test]
    async fn authoritative_rep_delta_is_not_applied_without_a_script_answer() {
        let (gw, rep, owner) = authoritative_rep_gateway("-- no on_input handler").await;
        gw.handle_inbound(
            owner,
            &Envelope::new(KIND_REP_DELTA, client_health_bunch(1, 37)),
        );
        assert_eq!(
            rep.authoritative_scalar(REP_OBJ, REP_HEALTH),
            Some(citadel_wire::netpeer::RepValue::Int(10)),
            "no answer must not apply the client's rep write (bypass B6 closed)"
        );
    }

    #[tokio::test]
    async fn authoritative_rep_delta_applies_only_after_accept() {
        let (gw, rep, owner) =
            authoritative_rep_gateway("citadel.on_input(function(e) return nil end)").await;
        gw.handle_inbound(
            owner,
            &Envelope::new(KIND_REP_DELTA, client_health_bunch(1, 37)),
        );
        assert_eq!(
            rep.authoritative_scalar(REP_OBJ, REP_HEALTH),
            Some(citadel_wire::netpeer::RepValue::Int(37)),
            "an accepted rep write applies + rebroadcasts authoritatively"
        );
    }

    #[tokio::test]
    async fn authoritative_rep_delta_rebroadcast_stays_in_its_room_after_accept() {
        let (gw, rep, owner) =
            authoritative_rep_gateway("citadel.on_input(function(e) return nil end)").await;
        let room_one = gw
            .rooms()
            .room_of(owner)
            .expect("owner is in the bound room");

        let (same_room, mut same_room_rx) = register(&gw);
        gw.join_rep_match(same_room, REP_MATCH, false);
        let _ = same_room_rx
            .recv()
            .await
            .expect("same-room schema bootstrap");
        gw.join_room(same_room, room_one)
            .expect("same-room peer joins the bound room");

        let (other_room, mut other_room_rx) = register(&gw);
        gw.join_rep_match(other_room, REP_MATCH, false);
        let _ = other_room_rx
            .recv()
            .await
            .expect("other-room schema bootstrap");
        let room_two = gw
            .create_room(RoomLabel::with_map("other"))
            .expect("test runtime supports lifecycle");
        gw.join_room(other_room, room_two)
            .expect("other-room peer joins a different room");

        gw.handle_inbound(
            owner,
            &Envelope::new(KIND_REP_DELTA, client_health_bunch(1, 37)),
        );

        let same_room_delta = same_room_rx.recv().await.expect("same-room delta");
        assert_eq!(same_room_delta.envelope.kind, KIND_REP_DELTA);
        assert!(
            other_room_rx.try_recv().is_err(),
            "an accepted authoritative rep delta must not cross the room fence"
        );
        assert_eq!(
            rep.authoritative_scalar(REP_OBJ, REP_HEALTH),
            Some(citadel_wire::netpeer::RepValue::Int(37)),
            "the bridge still authorizes the accepted write before scoped fan-out"
        );
    }

    #[tokio::test]
    async fn authoritative_rep_delta_reject_applies_nothing() {
        let (gw, rep, owner) =
            authoritative_rep_gateway("citadel.on_input(function(e) return false end)").await;
        gw.handle_inbound(
            owner,
            &Envelope::new(KIND_REP_DELTA, client_health_bunch(1, 37)),
        );
        assert_eq!(
            rep.authoritative_scalar(REP_OBJ, REP_HEALTH),
            Some(citadel_wire::netpeer::RepValue::Int(10)),
            "a rejected rep write applies nothing"
        );
    }

    // ---- B8: player-slot grant is refused inside an authoritative match ----

    #[tokio::test]
    async fn player_slot_grant_is_refused_inside_an_authoritative_match() {
        // A player-slot grant spawns a transform object; inside an authoritative
        // match a (re-)HELLO must reply with the negotiation only and grant no
        // slot, so no Rust-authored spawn happens in-match (the script owns
        // spawns via SpawnRequest). Outside a match the grant is unchanged.
        let (gw, _hub) =
            authoritative_gateway_slots("citadel.on_input(function(e) return nil end)", 2);
        let (a, mut ra) = register(&gw);
        let binding = ScriptBinding {
            revision_id: "sha256:test".to_owned(),
            generation: 1,
        };
        gw.rooms()
            .join_or_create_bound(a, "arena", Some(binding), || RoomLabel::with_map("arena"))
            .expect("bound room");
        let sent = gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        // Exactly one delivery: the negotiation reply. A player-slot grant would
        // deliver a second frame (KIND_TSYNC_ROLE) and make this 2.
        assert_eq!(
            sent, 1,
            "HELLO in an authoritative match replies with the negotiation only, no slot grant"
        );
        let hello = ra.recv().await.expect("negotiation reply");
        assert_eq!(hello.envelope.kind, KIND_TSYNC_HELLO);
    }

    // ---- STEP 2: capability gating (has_capability wired to [runtime.bridge]) ----

    fn authoritative_gateway_caps(
        on_input_src: &str,
        capabilities: std::collections::HashSet<Capability>,
    ) -> (Arc<Gateway>, Arc<TransformHub>) {
        let hub = Arc::new(TransformHub::new(TransformHubConfig::default()).expect("hub"));
        let runtime: Arc<dyn Runtime> = Arc::new(
            crate::runtime::LuaRuntime::from_source(
                on_input_src,
                "bridge-test",
                crate::runtime::DEFAULT_DEADLINE_MS,
            )
            .expect("lua runtime"),
        );
        let gw = Arc::new(
            Gateway::with_metrics_and_runtime(Arc::new(NodeMetrics::new()), Some(runtime))
                .with_transform_hub(Arc::clone(&hub))
                .with_bridge(BridgeQuotas::default(), capabilities),
        );
        gw.attach_bridge_sink();
        (gw, hub)
    }

    /// A member owning a relay object in a `gw` built with specific capabilities.
    fn caps_member(gw: &Arc<Gateway>) -> (ParticipantId, u32) {
        use citadel_wire::na::{NaPresence, NaTransform};
        let (p, _rp) = register(gw);
        gw.handle_inbound(
            p,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 0,
                    transform: NaTransform::identity(),
                }
                .encode(),
            ),
        );
        let object_id = gw
            .transform
            .as_ref()
            .and_then(|h| h.relay_owned_object(p.get()))
            .expect("relay object");
        let binding = ScriptBinding {
            revision_id: "sha256:test".to_owned(),
            generation: 1,
        };
        gw.rooms()
            .join_or_create_bound(p, "arena", Some(binding), || RoomLabel::with_map("arena"))
            .expect("bound room");
        (p, object_id)
    }

    // on_input accepts the report AND emits a capability-gated physics command.
    const PHYSICS_ON_INPUT: &str = r#"citadel.on_input(function(e)
        citadel.apply_impulse(e.object_id, 1, 0, 0)
        return nil
    end)"#;

    #[tokio::test]
    async fn physics_command_without_capability_rejects_the_whole_batch() {
        // No Physics capability granted: the physics command fails the whole
        // batch closed, so even the accepted report does not materialize.
        let (gw, hub) =
            authoritative_gateway_caps(PHYSICS_ON_INPUT, std::collections::HashSet::new());
        let (p, object_id) = caps_member(&gw);
        gw.handle_inbound(p, &na_state_frame(object_id, 5.0));
        assert!(
            hub.get_transform(object_id).expect("object").position[0].abs() < 1e-3,
            "an undeclared physics capability rejects the batch; nothing materializes"
        );
    }

    #[tokio::test]
    async fn physics_command_with_capability_materializes_the_batch() {
        // Physics granted via [runtime.bridge]: the batch validates and the
        // accepted report materializes (the impulse is a no-op without a body).
        let (gw, hub) = authoritative_gateway_caps(
            PHYSICS_ON_INPUT,
            std::iter::once(Capability::Physics).collect(),
        );
        let (p, object_id) = caps_member(&gw);
        gw.handle_inbound(p, &na_state_frame(object_id, 5.0));
        assert!(
            (hub.get_transform(object_id).expect("object").position[0] - 5.0).abs() < 1e-3,
            "a granted physics capability lets the batch materialize"
        );
    }

    #[tokio::test]
    async fn na_disconnect_despawns_the_proxy_for_remaining_peers() {
        use citadel_wire::na::{NaDespawn, NaPresence, NaTransform};
        let (gw, _hub) = gateway_with_hub();
        let t = NaTransform::identity();

        let (a, _ra) = register_session(&gw);
        gw.handle_inbound(
            a,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 0,
                    transform: t,
                }
                .encode(),
            ),
        );
        let (b, mut rb) = register_session(&gw);
        gw.handle_inbound(
            b,
            &Envelope::new(
                KIND_NA_PRESENCE,
                NaPresence {
                    archetype_id: 0,
                    transform: t,
                }
                .encode(),
            ),
        );
        // Drain B's self + batch.
        let _ = rb.recv().await;
        let _ = rb.recv().await;

        // A disconnects: B is told to despawn A's object (id 1).
        gw.unregister_session(a);
        let despawn = rb.recv().await.expect("B receives a despawn");
        assert_eq!(despawn.envelope.kind, KIND_NA_DESPAWN);
        assert_eq!(despawn.delivery, Delivery::Reliable);
        let d = NaDespawn::decode(&despawn.envelope.body).expect("decode");
        assert_eq!(d.object_id, 1, "A's object is despawned on the peer");
    }

    #[tokio::test]
    async fn transform_tick_fans_out_snapshots_and_client_reconstructs() {
        let (gw, hub) = gateway_with_hub();
        let (a, mut ra) = register(&gw);
        // Client A opts into transform sync.
        gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        let _hello = ra.recv().await.expect("hello reply");

        // A moving server object.
        let mut s = TransformState::at([0.0, 0.0, 0.0]);
        s.velocity = [600.0, 0.0, 0.0];
        hub.spawn_server_simulated(1, s);

        let codec = *hub.codec();
        let mut view = RemoteWorldView::new(codec, 60, 20);

        // Several ticks; the snapshot rides the unreliable path.
        for _ in 0..5 {
            let delivered = gw.transform_tick();
            assert_eq!(delivered, 1, "one snapshot to the one client");
            let out = ra.recv().await.expect("snapshot datagram");
            assert_eq!(out.envelope.kind, KIND_TSYNC_SNAPSHOT);
            assert_eq!(out.delivery, Delivery::Unreliable);
            assert!(view.apply_datagram(&out.envelope.body));
            // Ack back through the gateway.
            let ack = view.ack();
            gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_ACK, ack.encode()));
        }

        let obj = view.object(1).expect("client sees the object");
        assert!(
            obj.state.position[0] > 10.0,
            "moved: {}",
            obj.state.position[0]
        );
    }

    #[tokio::test]
    async fn leave_drops_transform_client_state() {
        let (gw, hub) = gateway_with_hub();
        let (a, _ra) = register_session(&gw);
        gw.handle_inbound(a, &Envelope::new(KIND_TSYNC_HELLO, Vec::new()));
        assert_eq!(hub.client_count(), 1);
        gw.unregister_session(a);
        assert_eq!(
            hub.client_count(),
            0,
            "leave drops the client's snapshot state"
        );
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::await_holding_lock)]
    use super::*;
    use crate::realtime::auth::RejectReason;
    use crate::realtime::registry::{ParticipantIdentity, SessionHandle};
    use crate::session::SessionId;
    use crate::storage::UserId;
    use crate::time::TimestampMillis;
    use crate::transport::TransportKind;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    fn response_message() -> crate::repository::ChatMessage {
        crate::repository::ChatMessage {
            id: 7,
            sender: "alice".to_owned(),
            content: "hello".to_owned(),
            created_at_unix_ms: 10,
            updated_at_unix_ms: 11,
            revision: 2,
            last_event_id: 9,
            deleted: false,
        }
    }

    #[test]
    fn durable_chat_mutation_repository_errors_are_private() {
        const SENTINEL: &str = "postgres password=hunter2 table=chat_messages";
        let failure = crate::error::AppError::database(SENTINEL).with_detail(SENTINEL);

        for operation in ["create", "edit", "delete", "moderate"] {
            let (status, body) = DomainRpcServices::chat_mutation_failure(&failure);
            assert_eq!(status, protocol::RPC_STATUS_ERROR, "{operation}");
            assert_eq!(body, b"CHAT_UNAVAILABLE", "{operation}");
            assert!(
                !String::from_utf8_lossy(&body).contains(SENTINEL),
                "{operation} leaked repository detail"
            );
        }
    }

    #[test]
    fn typed_chat_message_mutation_responses_preserve_wire_schema() {
        let message = response_message();
        let create = serde_json::to_value(ChatCreateResponse::from(&message))
            .expect("create response serializes");
        let edit = serde_json::to_value(ChatEditResponse::from(&message))
            .expect("edit response serializes");

        for response in [create, edit] {
            assert_eq!(
                response["message"],
                serde_json::to_value(&message).expect("message")
            );
            assert_eq!(response["event_id"], 9);
            assert_eq!(response.as_object().expect("object").len(), 2);
        }
    }

    #[test]
    fn typed_delete_and_moderate_responses_always_emit_nullable_event_id() {
        let delete_success = serde_json::to_value(ChatDeleteResponse::deleted(7, 9))
            .expect("delete response serializes");
        let delete_noop = serde_json::to_value(ChatDeleteResponse::not_deleted(7))
            .expect("delete no-op serializes");
        let moderate_success = serde_json::to_value(ChatModerateResponse::deleted(7, 9))
            .expect("moderate response serializes");
        let moderate_noop = serde_json::to_value(ChatModerateResponse::not_deleted(7))
            .expect("moderate no-op serializes");

        for response in [delete_success, moderate_success] {
            assert_eq!(
                response,
                serde_json::json!({
                    "deleted": true,
                    "message_id": 7,
                    "event_id": 9,
                })
            );
        }
        for response in [delete_noop, moderate_noop] {
            assert_eq!(
                response,
                serde_json::json!({
                    "deleted": false,
                    "message_id": 7,
                    "event_id": null,
                })
            );
        }
    }

    /// The bridge must reuse the server runtime from a worker thread.
    ///
    /// Every other test in this module runs on a current-thread runtime and so
    /// only exercises the dedicated-thread fallback. This one pins the path the
    /// server actually takes, where `block_in_place` has to hand the worker's
    /// queued tasks off rather than panic or deadlock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn party_block_on_reuses_a_multi_thread_runtime() {
        let from_worker =
            party_block_on(async { Ok::<u32, crate::error::AppError>(7) }).expect("worker thread");
        assert_eq!(from_worker, 7);

        // The tick loop reaches this bridge from the blocking pool, where
        // `block_in_place` is a no-op and `block_on` must still be allowed.
        let from_blocking =
            tokio::task::spawn_blocking(|| party_block_on(async { Ok::<u32, _>(9) }))
                .await
                .expect("blocking task joins")
                .expect("blocking pool");
        assert_eq!(from_blocking, 9);

        // Errors propagate unchanged rather than being remapped by the bridge.
        let failure = party_block_on(async {
            Err::<u32, _>(crate::error::AppError::internal("directory unavailable"))
        })
        .expect_err("error propagates");
        assert_eq!(failure.category(), crate::error::ErrorCategory::Internal);
    }

    /// A current-thread runtime must take the fallback instead of panicking:
    /// `block_in_place` is not permitted there.
    #[tokio::test]
    async fn party_block_on_falls_back_on_a_current_thread_runtime() {
        let value =
            party_block_on(async { Ok::<u32, crate::error::AppError>(11) }).expect("fallback path");
        assert_eq!(value, 11);
    }

    /// And with no runtime at all, which is how plain `#[test]` callers arrive.
    #[test]
    fn party_block_on_falls_back_without_a_runtime() {
        let value =
            party_block_on(async { Ok::<u32, crate::error::AppError>(13) }).expect("fallback path");
        assert_eq!(value, 13);
    }

    /// Build a test authenticated identity for a user id.
    fn test_identity(user_id: &str) -> ParticipantIdentity {
        ParticipantIdentity {
            user_id: UserId::new(user_id).expect("user id"),
            session_id: SessionId::new(format!("session-{user_id}")).expect("session id"),
            expires_at: TimestampMillis::from_unix_millis(9_999_999_999),
        }
    }

    #[test]
    fn session_activation_fences_an_exact_session_replacement_and_rejects_expiry() {
        let gateway = Gateway::new();
        let first = gateway.next_participant_id();
        let replacement = gateway.next_participant_id();
        let (first_tx, _first_rx) = mpsc::channel(8);
        let (replacement_tx, _replacement_rx) = mpsc::channel(8);
        let identity = test_identity("reconnect-player");

        gateway.register_session_at(
            SessionHandle {
                id: first,
                kind: TransportKind::WebSocket,
                outbound: first_tx,
                identity: Some(identity.clone()),
            },
            TimestampMillis::from_unix_millis(10),
        );
        assert!(gateway.accepts_work(first));

        gateway.register_session_at(
            SessionHandle {
                id: replacement,
                kind: TransportKind::WebSocket,
                outbound: replacement_tx,
                identity: Some(identity),
            },
            TimestampMillis::from_unix_millis(10),
        );
        assert!(
            !gateway.accepts_work(first),
            "an activation for the exact same session fences its prior participant"
        );
        assert!(gateway.accepts_work(replacement));

        let expired = gateway.next_participant_id();
        let (expired_tx, _expired_rx) = mpsc::channel(8);
        gateway.register_session_at(
            SessionHandle {
                id: expired,
                kind: TransportKind::WebSocket,
                outbound: expired_tx,
                identity: Some(ParticipantIdentity {
                    user_id: UserId::new("expired-player").expect("user id"),
                    session_id: SessionId::new("expired-session").expect("session id"),
                    expires_at: TimestampMillis::from_unix_millis(10),
                }),
            },
            TimestampMillis::from_unix_millis(10),
        );
        assert!(
            !gateway.accepts_work(expired),
            "a session cannot activate at its exact expiry boundary"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replacement_defers_old_room_release_until_its_inbound_gate_drains() {
        let gateway = Arc::new(Gateway::new());
        let first = gateway.next_participant_id();
        let replacement = gateway.next_participant_id();
        let (first_tx, _first_rx) = mpsc::channel(8);
        let (replacement_tx, _replacement_rx) = mpsc::channel(8);
        let identity = test_identity("replacement-room-player");
        let first_registration = gateway.register_session_at(
            SessionHandle {
                id: first,
                kind: TransportKind::WebSocket,
                outbound: first_tx,
                identity: Some(identity.clone()),
            },
            TimestampMillis::from_unix_millis(10),
        );
        let room_id = gateway.rooms.create(RoomLabel {
            map: "default".to_owned(),
            mode: "replacement-test".to_owned(),
            max_players: 2,
            open: true,
        });
        gateway.rooms.join(first, room_id).expect("join old room");
        let held_receive_gate = first_registration
            .supersession_gate
            .lock()
            .expect("hold old inbound gate");

        let replacement_registration = gateway.register_session_at(
            SessionHandle {
                id: replacement,
                kind: TransportKind::WebSocket,
                outbound: replacement_tx,
                identity: Some(identity),
            },
            TimestampMillis::from_unix_millis(10),
        );
        let cleanup = replacement_registration
            .replaced_cleanup
            .expect("replacement returns old cleanup ticket");
        let cleanup_gateway = Arc::clone(&gateway);
        let cleanup_task = tokio::spawn(async move {
            cleanup.wait_for_inbound_drain().await;
            cleanup_gateway.unregister_session(cleanup.participant_id());
        });

        tokio::task::yield_now().await;
        assert_eq!(
            gateway.rooms.room_of(first),
            Some(room_id),
            "the old generation remains in its room while an admitted inbound handoff drains"
        );
        drop(held_receive_gate);
        tokio::time::timeout(std::time::Duration::from_secs(1), cleanup_task)
            .await
            .expect("old cleanup follows receive gate drain")
            .expect("old cleanup task");
        assert_eq!(
            gateway.rooms.room_of(first),
            None,
            "room release runs only after the old inbound gate drains"
        );
    }

    #[test]
    fn reconnect_grace_resumes_only_the_exact_unexpired_session_once() {
        let gateway = Gateway::new();
        let original = gateway.next_participant_id();
        let resumed = gateway.next_participant_id();
        let sibling = gateway.next_participant_id();
        let (original_tx, _original_rx) = mpsc::channel(8);
        let (resumed_tx, _resumed_rx) = mpsc::channel(8);
        let (sibling_tx, _sibling_rx) = mpsc::channel(8);
        let identity = test_identity("resume-player");
        let secret = ResumeSecret::from_server_bytes(vec![7; 16]).expect("secret");

        gateway.register_session_at(
            SessionHandle {
                id: original,
                kind: TransportKind::WebSocket,
                outbound: original_tx,
                identity: Some(identity.clone()),
            },
            TimestampMillis::from_unix_millis(10),
        );
        assert!(gateway.begin_reconnect_grace(
            original,
            secret.clone(),
            TimestampMillis::from_unix_millis(30),
        ));
        assert!(!gateway.accepts_work(original));

        gateway.resume_session_at(
            SessionHandle {
                id: resumed,
                kind: TransportKind::WebSocket,
                outbound: resumed_tx,
                identity: Some(identity.clone()),
            },
            secret.clone(),
            TimestampMillis::from_unix_millis(20),
        );
        assert!(gateway.accepts_work(resumed));

        gateway.resume_session_at(
            SessionHandle {
                id: sibling,
                kind: TransportKind::WebSocket,
                outbound: sibling_tx,
                identity: Some(ParticipantIdentity {
                    user_id: UserId::new("resume-player").expect("user id"),
                    session_id: SessionId::new("sibling-session").expect("session id"),
                    expires_at: TimestampMillis::from_unix_millis(9_999_999_999),
                }),
            },
            secret,
            TimestampMillis::from_unix_millis(20),
        );
        assert!(
            !gateway.accepts_work(sibling),
            "a resume secret cannot cross from its exact session to a sibling session"
        );
        assert_eq!(
            gateway.expire_reconnect_grace_at(TimestampMillis::from_unix_millis(30)),
            0,
            "a successful resume deterministically removes its grace record"
        );
    }

    #[test]
    fn expired_grace_cleans_exact_session_without_touching_a_sibling() {
        let gateway = Gateway::new();
        let first = gateway.next_participant_id();
        let sibling = gateway.next_participant_id();
        let (first_tx, _first_rx) = mpsc::channel(8);
        let (sibling_tx, _sibling_rx) = mpsc::channel(8);
        let secret = ResumeSecret::from_server_bytes(vec![9; 16]).expect("secret");

        gateway.register_session_at(
            SessionHandle {
                id: first,
                kind: TransportKind::WebSocket,
                outbound: first_tx,
                identity: Some(test_identity("grace-player")),
            },
            TimestampMillis::from_unix_millis(10),
        );
        gateway.register_session_at(
            SessionHandle {
                id: sibling,
                kind: TransportKind::WebSocket,
                outbound: sibling_tx,
                identity: Some(test_identity("sibling-player")),
            },
            TimestampMillis::from_unix_millis(10),
        );
        assert!(gateway.begin_reconnect_grace(
            first,
            secret,
            TimestampMillis::from_unix_millis(30),
        ));
        assert_eq!(
            gateway.expire_reconnect_grace_at(TimestampMillis::from_unix_millis(29)),
            0
        );
        assert_eq!(
            gateway.expire_reconnect_grace_at(TimestampMillis::from_unix_millis(30)),
            1
        );
        assert!(gateway.accepts_work(sibling));
    }

    #[derive(Default)]
    struct DiagnosticsDispatchProbe {
        dispatches: Mutex<usize>,
        joins: Mutex<usize>,
    }

    impl Runtime for DiagnosticsDispatchProbe {
        fn dispatch(
            &self,
            _sender: u64,
            _user_id: Option<&str>,
            _kind: u16,
            _body: &[u8],
        ) -> Vec<OutboundCommand> {
            *self.dispatches.lock().expect("dispatch lock") += 1;
            Vec::new()
        }

        fn dispatch_lifecycle(
            &self,
            hook: LifecycleHook,
            _sender: u64,
            _user_id: Option<&str>,
        ) -> Vec<OutboundCommand> {
            if hook == LifecycleHook::Join {
                *self.joins.lock().expect("join lock") += 1;
            }
            Vec::new()
        }

        fn tick(&self, _dt: Duration, _budget: Duration) -> Vec<OutboundCommand> {
            Vec::new()
        }

        fn call_rpc(
            &self,
            _sender: u64,
            _user_id: Option<&str>,
            _method: &str,
            _body: &[u8],
        ) -> RpcOutcome {
            RpcOutcome::Err("unavailable".to_owned())
        }

        fn call_room_create(
            &self,
            _sender: u64,
            _user_id: Option<&str>,
            _params: &[u8],
        ) -> Option<crate::runtime::RoomSpec> {
            None
        }

        fn call_room_join(&self, _sender: u64, _user_id: Option<&str>, _room_id: u64) -> bool {
            true
        }

        fn has_tick_handler(&self) -> bool {
            false
        }

        fn budget(&self) -> Duration {
            Duration::from_millis(50)
        }

        fn introspect(&self) -> crate::runtime::RuntimeIntrospection {
            crate::runtime::RuntimeIntrospection {
                source: "diagnostics-probe".to_owned(),
                reloadable: false,
                deadline_ms: 50,
                rpcs: Vec::new(),
                message_kinds: Vec::new(),
                hooks: Vec::new(),
            }
        }
    }

    #[test]
    fn replaced_registration_cannot_emit_join_or_underflow_open_gauges() {
        let metrics = Arc::new(NodeMetrics::new());
        let probe = Arc::new(DiagnosticsDispatchProbe::default());
        let gateway = Arc::new(Gateway::with_metrics_and_runtime(
            Arc::clone(&metrics),
            Some(Arc::clone(&probe) as Arc<dyn Runtime>),
        ));
        let first = gateway.next_participant_id();
        let replacement = gateway.next_participant_id();
        let identity = test_identity("generation-owned-registration");
        let (first_ready_tx, first_ready_rx) = std::sync::mpsc::sync_channel(1);
        let (release_first_tx, release_first_rx) = std::sync::mpsc::sync_channel(1);
        let first_gateway = Arc::clone(&gateway);
        let first_identity = identity.clone();
        let first_task = std::thread::spawn(move || {
            let (tx, _rx) = mpsc::channel(8);
            first_gateway.register_session_with_initials_at_after_publish(
                SessionHandle {
                    id: first,
                    kind: TransportKind::WebSocket,
                    outbound: tx,
                    identity: Some(first_identity),
                },
                Vec::new(),
                TimestampMillis::from_unix_millis(10),
                move || {
                    first_ready_tx.send(()).expect("first publish");
                    release_first_rx.recv().expect("release first effects");
                },
            );
        });

        first_ready_rx.recv().expect("first registration publishes");
        let (replacement_tx, _replacement_rx) = mpsc::channel(8);
        gateway.register_session_at(
            SessionHandle {
                id: replacement,
                kind: TransportKind::WebSocket,
                outbound: replacement_tx,
                identity: Some(identity),
            },
            TimestampMillis::from_unix_millis(10),
        );
        release_first_tx
            .send(())
            .expect("release stale registration");
        first_task.join().expect("first registration thread");

        assert_eq!(
            *probe.joins.lock().expect("join lock"),
            1,
            "only the generation that still owns registration may dispatch Join"
        );
        let active = metrics.snapshot();
        assert_eq!(active.participants_active, 1);
        assert_eq!(active.sessions_active, 1);

        gateway.unregister_session(replacement);
        let inactive = metrics.snapshot();
        assert_eq!(
            inactive.participants_active, 0,
            "cleanup cannot underflow participants"
        );
        assert_eq!(
            inactive.sessions_active, 0,
            "cleanup cannot underflow sessions"
        );
    }

    #[tokio::test]
    async fn closed_registration_cannot_emit_join_or_underflow_open_gauges() {
        let metrics = Arc::new(NodeMetrics::new());
        let probe = Arc::new(DiagnosticsDispatchProbe::default());
        let gateway = Arc::new(Gateway::with_metrics_and_runtime(
            Arc::clone(&metrics),
            Some(Arc::clone(&probe) as Arc<dyn Runtime>),
        ));
        let participant = gateway.next_participant_id();
        let identity = test_identity("closed-generation-registration");
        let (published_tx, published_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let registering_gateway = Arc::clone(&gateway);
        let registering_identity = identity.clone();
        let registration = std::thread::spawn(move || {
            let (tx, _rx) = mpsc::channel(8);
            registering_gateway.register_session_with_initials_at_after_publish(
                SessionHandle {
                    id: participant,
                    kind: TransportKind::WebSocket,
                    outbound: tx,
                    identity: Some(registering_identity),
                },
                Vec::new(),
                TimestampMillis::from_unix_millis(10),
                move || {
                    published_tx.send(()).expect("registration publishes");
                    release_rx.recv().expect("release registration effects");
                },
            );
        });

        published_rx.recv().expect("registration publishes");
        assert_eq!(
            gateway
                .disconnect_session(
                    &identity.session_id,
                    "close-pending-registration",
                    None,
                    identity.expires_at,
                    TimestampMillis::from_unix_millis(10),
                )
                .await,
            1
        );
        release_tx.send(()).expect("release closed registration");
        registration.join().expect("registration thread");

        assert_eq!(
            *probe.joins.lock().expect("join lock"),
            0,
            "a close that wins before side effects must suppress Join"
        );
        let gauges = metrics.snapshot();
        assert_eq!(gauges.participants_active, 0);
        assert_eq!(gauges.sessions_active, 0);
    }

    #[test]
    fn diagnostics_controls_are_reserved_before_runtime_dispatch() {
        let probe = Arc::new(DiagnosticsDispatchProbe::default());
        let gateway = Gateway::with_metrics_and_runtime(
            Arc::new(NodeMetrics::new()),
            Some(Arc::clone(&probe) as Arc<dyn Runtime>),
        );
        let participant = gateway.next_participant_id();
        let (tx, _rx) = mpsc::channel(8);
        gateway.register_session(SessionHandle {
            id: participant,
            kind: TransportKind::WebSocket,
            outbound: tx,
            identity: Some(test_identity("diagnostics-player")),
        });
        let offer = gateway
            .issue_diagnostics_server_time(participant, TimestampMillis::from_unix_millis(10))
            .expect("offer");
        let capabilities = Capabilities {
            offer_id: offer.offer_id,
            features: citadel_wire::diagnostics::CAPABILITY_RECORDING,
        };
        gateway.handle_inbound(
            participant,
            &Envelope::new(
                KIND_DIAG_CAPABILITIES,
                capabilities.encode().expect("capabilities"),
            ),
        );
        gateway.handle_inbound(participant, &Envelope::new(KIND_DIAG_START, vec![1]));
        assert_eq!(
            *probe.dispatches.lock().expect("dispatch lock"),
            0,
            "reserved diagnostics controls cannot reach a script handler"
        );
    }

    #[tokio::test]
    async fn native_ingest_flush_uses_one_redacted_grant_per_recording_session() {
        use std::collections::BTreeMap;

        use crate::config::LagDiagnosticsConfig;
        use crate::lag_diagnostics::CaptureParticipant;
        use base64::Engine as _;

        let root = std::env::temp_dir().join(format!("citadel-gateway-lag-{}", Uuid::new_v4()));
        let mut keys = BTreeMap::new();
        keys.insert(
            "current".to_string(),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([6_u8; 32]),
        );
        let ingest = LagDiagnosticsService::new(
            LagDiagnosticsConfig {
                enabled: true,
                raw_root: Some(root.display().to_string()),
                active_key_id: Some("current".to_string()),
                upload_hmac_keys: keys,
                allowed_origins: Vec::new(),
                max_compressed_bytes: 1024 * 1024,
                max_decompressed_bytes: 1024 * 1024,
                max_decompression_ratio: 32,
                max_concurrent_uploads: 2,
                max_raw_bytes: 4 * 1024 * 1024,
                retention_hours: 1,
                shared_raw_store: false,
            },
            "node-a".to_string(),
        )
        .expect("ingest");
        let gateway = Gateway::new();
        let (participant, mut receiver) = register(&gateway, TransportKind::WebSocket);
        let offer = gateway
            .issue_diagnostics_server_time(participant, TimestampMillis::from_unix_millis(10))
            .expect("offer");
        assert!(gateway.diagnostics.accept_capabilities(
            participant,
            Capabilities {
                offer_id: offer.offer_id,
                features: citadel_wire::diagnostics::CAPABILITY_RECORDING,
            },
        ));
        let capture_id = CaptureId::new([4; 16]).expect("capture");
        let start = StartCapture {
            capture_id,
            generation: 1,
            deadline_server_utc_ms: 1_000,
            max_record_bytes: 1_024,
            filters: vec![citadel_wire::diagnostics::PacketFilter {
                kind: KIND_POSITION,
                direction: citadel_wire::diagnostics::PacketDirection::Inbound,
                entity_id: None,
            }],
        };
        gateway
            .start_lag_capture_with_ingest_at(&ingest, start, TimestampMillis::from_unix_millis(10))
            .expect("start");
        assert_eq!(
            receiver.recv().await.expect("start queued").envelope.kind,
            KIND_DIAG_START
        );
        gateway
            .diagnostics
            .apply_status(
                participant,
                CaptureStatus {
                    capture_id,
                    generation: 1,
                    code: citadel_wire::diagnostics::CaptureStatusCode::Recording,
                    attempt_id: 0,
                    recorded_packets: 0,
                    dropped_packets: 0,
                    recorded_bytes: 0,
                },
            )
            .expect("recording");
        let result = gateway
            .flush_lag_capture_with_ingest_at(
                &ingest,
                CaptureFlushPlan {
                    capture_id,
                    generation: 1,
                    attempt_id: 1,
                    upload_deadline_server_utc_ms: 500,
                    max_compressed_bytes: 1_024,
                    required_uploads: 1,
                    analyze: false,
                    participants: vec![CaptureParticipant {
                        participant_id: participant.get(),
                        session_id: "session-1".to_string(),
                        tenant_id: "tenant-a".to_string(),
                        match_id: "match-a".to_string(),
                    }],
                },
                TimestampMillis::from_unix_millis(20),
            )
            .expect("secure flush");
        assert_eq!(result.grants.len(), 1);
        let queued = receiver.recv().await.expect("flush queued").envelope;
        assert_eq!(queued.kind, KIND_DIAG_FLUSH);
        let decoded = FlushCapture::decode(&queued.body).expect("flush body");
        assert_eq!(
            decoded.upload_path,
            citadel_wire::diagnostics::DIAGNOSTICS_UPLOAD_PATH
        );
        assert_ne!(decoded.upload_token, "session-1");
        assert!(format!("{:?}", decoded).contains("[redacted]"));
        let _ = std::fs::remove_dir_all(root);
    }

    fn register(gw: &Gateway, kind: TransportKind) -> (ParticipantId, TestOutboundReceiver) {
        let id = gw.next_participant_id();
        let (tx, rx) = mpsc::channel(8);
        let unreliable = gw.registry().register(SessionHandle {
            id,
            kind,
            outbound: tx,
            identity: None,
        });
        (
            id,
            TestOutboundReceiver {
                reliable: rx,
                unreliable,
            },
        )
    }

    #[tokio::test]
    async fn position_relays_to_peers_tagged_with_sender() {
        let gw = Gateway::new();
        let (a, mut ra) = register(&gw, TransportKind::WebSocket);
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);

        let payload = vec![1u8, 2, 3, 4];
        let relayed_to = gw.handle_inbound(a, &Envelope::new(KIND_POSITION, payload.clone()));
        assert_eq!(relayed_to, 1, "relayed to the one peer");

        // Sender does not receive its own message.
        assert!(ra.try_recv().is_err());

        // Peer receives a PEER_POSITION tagged with the sender id + payload.
        let out = rb.recv().await.expect("peer receives");
        assert_eq!(out.envelope.kind, KIND_PEER_POSITION);
        let (sender_id, rest) = protocol::split_sender(&out.envelope.body).expect("tagged body");
        assert_eq!(sender_id, a.get());
        assert_eq!(rest, &payload[..]);
        assert_eq!(out.delivery, Delivery::Unreliable);
    }

    #[tokio::test]
    async fn unknown_kind_is_dropped() {
        let gw = Gateway::new();
        let (a, _ra) = register(&gw, TransportKind::WebSocket);
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);
        let relayed = gw.handle_inbound(a, &Envelope::new(9999, &b"x"[..]));
        assert_eq!(relayed, 0);
        assert!(rb.try_recv().is_err());
    }

    #[test]
    fn connection_and_session_lifecycle_move_node_gauges() {
        let metrics = Arc::new(NodeMetrics::new());
        let gw = Gateway::with_metrics(Arc::clone(&metrics));

        // Two connections, each registering one session.
        gw.connection_opened();
        gw.connection_opened();
        let (tx1, _r1) = mpsc::channel(8);
        let (tx2, _r2) = mpsc::channel(8);
        let id1 = gw.next_participant_id();
        let id2 = gw.next_participant_id();
        // One participant is a guest; the other is account-bound.
        gw.register_session(SessionHandle {
            id: id1,
            kind: TransportKind::WebSocket,
            outbound: tx1,
            identity: None,
        });
        gw.register_session(SessionHandle {
            id: id2,
            kind: TransportKind::Quic,
            outbound: tx2,
            identity: Some(test_identity("user-2")),
        });

        let snap = metrics.snapshot();
        assert_eq!(snap.connections_active, 2);
        assert_eq!(snap.connections_accepted_total, 2);
        assert_eq!(snap.participants_active, 2, "both participants counted");
        assert_eq!(
            snap.sessions_active, 1,
            "only the authenticated participant counts as a session"
        );

        // The authenticated participant disconnects: participant + session gauges
        // both drop; the guest leaving would only move the participant gauge.
        gw.unregister_session(id2);
        gw.connection_closed();
        let snap = metrics.snapshot();
        assert_eq!(snap.connections_active, 1);
        assert_eq!(snap.connections_accepted_total, 2);
        assert_eq!(snap.participants_active, 1);
        assert_eq!(snap.sessions_active, 0, "the authenticated session ended");
    }

    #[tokio::test]
    async fn legacy_authenticated_registration_stays_active_for_lifecycle_gauges_and_presence() {
        let metrics = Arc::new(NodeMetrics::new());
        let gw = runtime_gateway(Arc::clone(&metrics), LIFECYCLE_TICK_SCRIPT);
        let (peer, mut peer_rx) = register_via_session(&gw, TransportKind::WebSocket);
        let id = gw.next_participant_id();
        let (tx, _rx) = mpsc::channel(8);

        // This legacy registration API remains public for authenticated callers.
        // It must still activate normal join handling rather than being treated
        // as a rejected time-checked registration.
        gw.register_session(SessionHandle {
            id,
            kind: TransportKind::WebSocket,
            outbound: tx,
            identity: Some(test_identity("legacy-user")),
        });
        assert!(gw.accepts_work(id));
        assert_eq!(gw.registry().participants_for_user("legacy-user"), vec![id]);
        let joined = peer_rx.recv().await.expect("legacy join lifecycle event");
        assert_eq!(joined.envelope.kind, 10);
        assert_eq!(joined.envelope.body, id.get().to_be_bytes().to_vec());
        let active = metrics.snapshot();
        assert_eq!(active.participants_active, 2);
        assert_eq!(active.sessions_active, 1);

        gw.unregister_session(id);
        let left = peer_rx.recv().await.expect("legacy leave lifecycle event");
        assert_eq!(left.envelope.kind, 11);
        assert_eq!(left.envelope.body, id.get().to_be_bytes().to_vec());
        let inactive = metrics.snapshot();
        assert_eq!(inactive.participants_active, 1);
        assert_eq!(inactive.sessions_active, 0);
        assert!(gw.accepts_work(peer));
    }

    #[tokio::test]
    async fn relaying_a_message_moves_node_message_counters() {
        let metrics = Arc::new(NodeMetrics::new());
        let gw = Gateway::with_metrics(Arc::clone(&metrics));
        let (a, _ra) = register(&gw, TransportKind::WebSocket);
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);
        let (_c, mut rc) = register(&gw, TransportKind::WebSocket);

        let payload = vec![1u8, 2, 3, 4];
        let relayed = gw.handle_inbound(a, &Envelope::new(KIND_POSITION, payload.clone()));
        assert_eq!(relayed, 2, "relayed to the two peers");

        let snap = metrics.snapshot();
        // One inbound message counted, with its body bytes.
        assert_eq!(snap.messages_in_total, 1);
        assert_eq!(snap.bytes_in_total, payload.len() as u64);
        // One outbound message per delivered peer.
        assert_eq!(snap.messages_out_total, 2);
        assert!(snap.bytes_out_total > 0);

        // Peers actually received the relay (sanity that count matches delivery).
        assert!(rb.recv().await.is_some());
        assert!(rc.recv().await.is_some());
    }

    #[test]
    fn unknown_kind_still_counts_as_one_inbound_message() {
        let metrics = Arc::new(NodeMetrics::new());
        let gw = Gateway::with_metrics(Arc::clone(&metrics));
        let (a, _ra) = register(&gw, TransportKind::WebSocket);
        let relayed = gw.handle_inbound(a, &Envelope::new(9999, &b"x"[..]));
        assert_eq!(relayed, 0);
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_in_total, 1);
        assert_eq!(
            snap.messages_out_total, 0,
            "nothing relayed for unknown kind"
        );
    }

    const RELAY_SCRIPT: &str = r#"
        citadel.on_message(1, function(ctx, body)
            citadel.broadcast(2, string.pack(">I8", ctx.sender) .. body, true)
        end)
    "#;

    fn runtime_gateway(metrics: Arc<NodeMetrics>, src: &str) -> Gateway {
        let rt = crate::runtime::LuaRuntime::from_source(src, "test", 100).expect("script loads");
        Gateway::with_metrics_and_runtime(metrics, Some(Arc::new(rt)))
    }

    #[tokio::test]
    async fn lua_handler_drives_the_relay_end_to_end() {
        let metrics = Arc::new(NodeMetrics::new());
        let gw = runtime_gateway(Arc::clone(&metrics), RELAY_SCRIPT);
        assert!(gw.has_runtime());
        let (a, mut ra) = register(&gw, TransportKind::WebSocket);
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);

        let payload = vec![7u8, 7, 7];
        let relayed = gw.handle_inbound(a, &Envelope::new(KIND_POSITION, payload.clone()));
        assert_eq!(relayed, 1, "script broadcast reached the one peer");

        // Sender does not receive its own message (broadcast excludes sender).
        assert!(ra.try_recv().is_err());

        // Peer receives the script-built PEER_POSITION, tagged + unreliable.
        let out = rb.recv().await.expect("peer receives");
        assert_eq!(out.envelope.kind, KIND_PEER_POSITION);
        assert_eq!(out.delivery, Delivery::Unreliable);
        let (sender_id, rest) = protocol::split_sender(&out.envelope.body).expect("tagged body");
        assert_eq!(sender_id, a.get());
        assert_eq!(rest, &payload[..]);

        // Metrics: one inbound, one outbound (mirrors the built-in relay).
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_in_total, 1);
        assert_eq!(snap.messages_out_total, 1);
    }

    #[tokio::test]
    async fn realtime_before_vetoes_routing_and_after_observes_the_prior_result() {
        let metrics = Arc::new(NodeMetrics::new());
        let gw = runtime_gateway(
            Arc::clone(&metrics),
            r#"
            local prior = "unset"
            citadel.before_realtime(function(ctx, body)
                if body == "block" then return false end
            end)
            citadel.after_realtime(function(ctx, body)
                citadel.broadcast(99, "must-discard")
                prior = (ctx.dropped and "drop" or "pass") .. ":" .. tostring(ctx.delivered) .. ":" .. ctx.body
            end)
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, prior, false)
            end)
        "#,
        );
        let (a, _ra) = register(&gw, TransportKind::WebSocket);
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);

        assert_eq!(
            gw.handle_inbound(a, &Envelope::new(KIND_POSITION, b"block".to_vec())),
            0,
            "before_realtime vetoes before the message handler or fan-out"
        );
        assert!(
            rb.try_recv().is_err(),
            "a veto enqueues no outbound message"
        );

        assert_eq!(
            gw.handle_inbound(a, &Envelope::new(KIND_POSITION, b"pass".to_vec())),
            1
        );
        let out = rb.recv().await.expect("normal handler reaches peer");
        assert_eq!(out.envelope.kind, 2);
        assert_eq!(out.envelope.body.as_ref(), b"drop:0:block");
        assert!(rb.try_recv().is_err(), "after hook commands are discarded");

        assert_eq!(
            gw.handle_inbound(a, &Envelope::new(KIND_POSITION, b"again".to_vec())),
            1
        );
        let out = rb.recv().await.expect("normal handler reaches peer again");
        assert_eq!(
            out.envelope.body.as_ref(),
            b"pass:1:pass",
            "after_realtime observes the preceding synchronous fan-out"
        );

        let snap = metrics.snapshot();
        assert_eq!(
            snap.messages_in_total, 3,
            "vetoed traffic remains observable"
        );
        assert_eq!(snap.messages_out_total, 2, "veto itself has no fan-out");
    }

    const LIFECYCLE_TICK_SCRIPT: &str = r#"
        citadel.on_join(function(ctx)
            citadel.broadcast(10, string.pack(">I8", ctx.sender), false)
        end)
        citadel.on_leave(function(ctx)
            citadel.broadcast(11, string.pack(">I8", ctx.sender), false)
        end)
        citadel.on_tick(function(dt)
            citadel.broadcast(20, "tick", true)
        end)
    "#;

    fn register_via_session(
        gw: &Gateway,
        kind: TransportKind,
    ) -> (ParticipantId, TestOutboundReceiver) {
        let id = gw.next_participant_id();
        let (tx, rx) = mpsc::channel(8);
        let unreliable = gw.register_session(SessionHandle {
            id,
            kind,
            outbound: tx,
            identity: None,
        });
        (
            id,
            TestOutboundReceiver {
                reliable: rx,
                unreliable,
            },
        )
    }

    #[tokio::test]
    async fn on_join_notifies_existing_peers_on_register() {
        let gw = runtime_gateway(Arc::new(NodeMetrics::new()), LIFECYCLE_TICK_SCRIPT);
        // A joins first: no peers to notify.
        let (_a, mut ra) = register_via_session(&gw, TransportKind::WebSocket);
        assert!(
            ra.try_recv().is_err(),
            "no peers yet => A is notified of nobody"
        );

        // B joins: A must receive a PLAYER_JOINED tagged with B's id.
        let (b, mut rb) = register_via_session(&gw, TransportKind::WebSocket);
        let out = ra.recv().await.expect("A is notified of B's join");
        assert_eq!(out.envelope.kind, 10);
        assert_eq!(out.envelope.body, b.get().to_be_bytes().to_vec());
        // B is not notified of its own join.
        assert!(rb.try_recv().is_err());
    }

    #[tokio::test]
    async fn on_leave_notifies_remaining_peers_on_unregister() {
        let gw = runtime_gateway(Arc::new(NodeMetrics::new()), LIFECYCLE_TICK_SCRIPT);
        let (_a, mut ra) = register_via_session(&gw, TransportKind::WebSocket);
        let (b, _rb) = register_via_session(&gw, TransportKind::WebSocket);
        // Drain A's join-notification for B.
        let _ = ra.recv().await.expect("A got B's join");

        // B leaves: A must receive a PLAYER_LEFT tagged with B's id.
        gw.unregister_session(b);
        let out = ra.recv().await.expect("A is notified of B's leave");
        assert_eq!(out.envelope.kind, 11);
        assert_eq!(out.envelope.body, b.get().to_be_bytes().to_vec());
    }

    #[tokio::test]
    async fn tick_broadcasts_to_all_sessions_with_no_sender_excluded() {
        let gw = runtime_gateway(Arc::new(NodeMetrics::new()), LIFECYCLE_TICK_SCRIPT);
        // Register directly (bypassing lifecycle) so only the tick delivers.
        let (_a, mut ra) = register(&gw, TransportKind::WebSocket);
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);

        let delivered = gw.tick(Duration::from_millis(16), Duration::from_millis(50));
        assert_eq!(delivered, 2, "tick reaches every session, no exclusion");
        assert_eq!(ra.recv().await.expect("A gets tick").envelope.kind, 20);
        assert_eq!(rb.recv().await.expect("B gets tick").envelope.kind, 20);
    }

    #[tokio::test]
    async fn erroring_tick_is_isolated_and_does_not_wedge_dispatch() {
        let metrics = Arc::new(NodeMetrics::new());
        let gw = runtime_gateway(
            Arc::clone(&metrics),
            r#"
            citadel.on_tick(function(dt)
                citadel.broadcast(20, "partial", true)
                error("boom in tick")
            end)
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, string.pack(">I8", ctx.sender) .. body, true)
            end)
        "#,
        );
        let (a, _ra) = register(&gw, TransportKind::WebSocket);
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);

        // The erroring tick delivers nothing and does not panic.
        assert_eq!(
            gw.tick(Duration::from_millis(16), Duration::from_millis(50)),
            0
        );
        assert!(rb.try_recv().is_err(), "no partial tick output leaks");

        // Inbound dispatch still works: the runtime is not wedged.
        let relayed = gw.handle_inbound(a, &Envelope::new(KIND_POSITION, vec![9]));
        assert_eq!(relayed, 1, "dispatch works after an isolated tick error");
        assert_eq!(rb.recv().await.expect("B gets relay").envelope.kind, 2);
    }

    const RPC_SCRIPT: &str = r#"
        citadel.on_rpc("ping", function(ctx, body)
            return "pong"
        end)
        citadel.on_rpc("boom", function(ctx, body)
            error("internal detail")
        end)
    "#;

    #[tokio::test]
    async fn rpc_reply_is_correlated_and_sent_to_caller_only() {
        let gw = runtime_gateway(Arc::new(NodeMetrics::new()), RPC_SCRIPT);
        let (a, mut ra) = register(&gw, TransportKind::Quic);
        let (_b, mut rb) = register(&gw, TransportKind::Quic);

        let request = protocol::encode_rpc_request(0xABCD, "ping", b"");
        let delivered = gw.handle_inbound(a, &Envelope::new(KIND_RPC_REQUEST, request));
        assert_eq!(delivered, 1, "exactly one response, to the caller");

        // The caller receives a correlated, reliable OK response.
        let out = ra.recv().await.expect("caller receives its RPC response");
        assert_eq!(out.envelope.kind, KIND_RPC_RESPONSE);
        assert_eq!(out.delivery, Delivery::Reliable);
        let resp = protocol::decode_rpc_response(&out.envelope.body).expect("decodes");
        assert_eq!(resp.request_id, 0xABCD, "response echoes the request id");
        assert!(resp.is_ok());
        assert_eq!(resp.payload, b"pong");

        // The other session receives nothing: an RPC reply is never broadcast.
        assert!(rb.try_recv().is_err(), "RPC reply must not reach peers");
    }

    #[tokio::test]
    async fn rpc_unknown_method_yields_a_correlated_error_response() {
        let gw = runtime_gateway(Arc::new(NodeMetrics::new()), RPC_SCRIPT);
        let (a, mut ra) = register(&gw, TransportKind::Quic);

        let request = protocol::encode_rpc_request(42, "does-not-exist", b"");
        assert_eq!(
            gw.handle_inbound(a, &Envelope::new(KIND_RPC_REQUEST, request)),
            1
        );
        let out = ra.recv().await.expect("error response delivered");
        let resp = protocol::decode_rpc_response(&out.envelope.body).expect("decodes");
        assert_eq!(resp.request_id, 42);
        assert!(!resp.is_ok(), "unknown method => status != 0");
    }

    #[tokio::test]
    async fn rpc_handler_error_is_isolated_and_returns_a_generic_message() {
        let gw = runtime_gateway(Arc::new(NodeMetrics::new()), RPC_SCRIPT);
        let (a, mut ra) = register(&gw, TransportKind::Quic);

        let request = protocol::encode_rpc_request(9, "boom", b"");
        assert_eq!(
            gw.handle_inbound(a, &Envelope::new(KIND_RPC_REQUEST, request)),
            1,
            "an erroring handler still yields exactly one response"
        );
        let out = ra.recv().await.expect("error response delivered");
        let resp = protocol::decode_rpc_response(&out.envelope.body).expect("decodes");
        assert_eq!(resp.request_id, 9);
        assert!(!resp.is_ok());
        let message = String::from_utf8_lossy(resp.payload);
        assert!(
            !message.contains("internal detail"),
            "handler internals must not leak to the caller: {message}"
        );

        // The gateway is not wedged: a subsequent RPC still works.
        let request = protocol::encode_rpc_request(10, "ping", b"");
        assert_eq!(
            gw.handle_inbound(a, &Envelope::new(KIND_RPC_REQUEST, request)),
            1
        );
        let out = ra.recv().await.expect("second response");
        let resp = protocol::decode_rpc_response(&out.envelope.body).expect("decodes");
        assert!(resp.is_ok());
        assert_eq!(resp.payload, b"pong");
    }

    #[tokio::test]
    async fn rpc_malformed_request_is_dropped_without_a_response() {
        let gw = runtime_gateway(Arc::new(NodeMetrics::new()), RPC_SCRIPT);
        let (a, mut ra) = register(&gw, TransportKind::Quic);
        // Too short to hold an RPC header: dropped, no response, no crash.
        let delivered = gw.handle_inbound(a, &Envelope::new(KIND_RPC_REQUEST, vec![0, 1, 2]));
        assert_eq!(delivered, 0);
        assert!(ra.try_recv().is_err());
    }

    #[tokio::test]
    async fn resolve_handshake_guest_only_accepts_guest_and_legacy_first_frame() {
        let gw = Gateway::new(); // guest-only authenticator
        // Explicit guest (empty KIND_AUTH): accepted, no replay.
        let hs = gw
            .resolve_handshake(&Envelope::new(KIND_AUTH, Vec::new()))
            .await;
        assert_eq!(hs.outcome, AuthOutcome::Guest);
        assert!(!hs.replay_first);

        // A legacy first frame (not KIND_AUTH): implicit guest, replay it.
        let hs = gw
            .resolve_handshake(&Envelope::new(KIND_POSITION, vec![1, 2, 3, 4]))
            .await;
        assert_eq!(hs.outcome, AuthOutcome::Guest);
        assert!(hs.replay_first, "legacy first frame is replayed");

        // A token with no session backend fails closed (never a guest fallback).
        let hs = gw
            .resolve_handshake(&Envelope::new(KIND_AUTH, b"some-token".to_vec()))
            .await;
        assert_eq!(hs.outcome, AuthOutcome::Rejected(RejectReason::AuthFailed));
        assert!(!hs.replay_first);
    }

    #[tokio::test]
    async fn ctx_exposes_user_id_for_authenticated_participant() {
        // A script that echoes ctx.user_id (or "GUEST") to peers.
        let gw = runtime_gateway(
            Arc::new(NodeMetrics::new()),
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, ctx.user_id or "GUEST", false)
            end)
        "#,
        );
        // A: authenticated as user-A. B: guest receiver.
        let a = gw.next_participant_id();
        let (tx_a, _ra) = mpsc::channel(8);
        gw.register_session(SessionHandle {
            id: a,
            kind: TransportKind::WebSocket,
            outbound: tx_a,
            identity: Some(test_identity("user-A")),
        });
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);

        gw.handle_inbound(a, &Envelope::new(KIND_POSITION, Vec::new()));
        let out = rb.recv().await.expect("peer receives");
        assert_eq!(out.envelope.kind, 2);
        assert_eq!(
            out.envelope.body.as_ref(),
            b"user-A",
            "authenticated user_id reaches game logic via ctx.user_id"
        );
    }

    #[tokio::test]
    async fn post_handshake_auth_frame_is_reserved_and_never_reaches_lua() {
        // A malicious/curious script tries to capture KIND_AUTH bodies. The
        // gateway must reserve KIND_AUTH post-registration so a token can never be
        // dispatched to game logic.
        let gw = runtime_gateway(
            Arc::new(NodeMetrics::new()),
            r#"
            citadel.before_realtime(function(ctx, body)
                citadel.broadcast(98, body, false)
            end)
            citadel.after_realtime(function(ctx, body)
                citadel.broadcast(97, body, false)
            end)
            citadel.on_message(5, function(ctx, body)
                citadel.broadcast(99, body, false)
            end)
        "#,
        );
        let (a, _ra) = register(&gw, TransportKind::WebSocket);
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);

        // A post-handshake KIND_AUTH carrying a "token" is dropped, not dispatched.
        let relayed = gw.handle_inbound(a, &Envelope::new(KIND_AUTH, b"secret-token".to_vec()));
        assert_eq!(relayed, 0, "reserved auth frame is dropped");
        assert!(
            rb.try_recv().is_err(),
            "the token bytes never reach a peer via a handler or interceptor"
        );
        // KIND_AUTH_RESULT is likewise reserved.
        assert_eq!(
            gw.handle_inbound(a, &Envelope::new(KIND_AUTH_RESULT, b"x".to_vec())),
            0
        );
    }

    #[tokio::test]
    async fn ctx_user_id_is_absent_for_guest_participant() {
        let gw = runtime_gateway(
            Arc::new(NodeMetrics::new()),
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, ctx.user_id or "GUEST", false)
            end)
        "#,
        );
        let (a, _ra) = register(&gw, TransportKind::WebSocket); // guest
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);
        gw.handle_inbound(a, &Envelope::new(KIND_POSITION, Vec::new()));
        let out = rb.recv().await.expect("peer receives");
        assert_eq!(out.envelope.body.as_ref(), b"GUEST", "guest has no user_id");
    }

    #[test]
    fn lua_handler_error_is_isolated_and_relays_nothing() {
        let metrics = Arc::new(NodeMetrics::new());
        let gw = runtime_gateway(
            Arc::clone(&metrics),
            r#"
            citadel.on_message(1, function(ctx, body)
                error("boom")
            end)
        "#,
        );
        let (a, _ra) = register(&gw, TransportKind::WebSocket);
        let (_b, mut rb) = register(&gw, TransportKind::WebSocket);
        // A throwing handler must not crash; nothing is relayed.
        let relayed = gw.handle_inbound(a, &Envelope::new(KIND_POSITION, vec![1, 2]));
        assert_eq!(relayed, 0);
        assert!(rb.try_recv().is_err());
        // Inbound still counted; no outbound.
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_in_total, 1);
        assert_eq!(snap.messages_out_total, 0);
    }
}

/// Built-in domain-feature client RPC (`friends.*`; /0268).
#[cfg(test)]
mod domain_rpc_tests {
    use super::*;
    use crate::matchmaker_cluster::{MatchmakerShardLease, QueueShardId};
    use crate::matchmaker_live::{LiveMatchmakerConfig, LiveMatchmakerNode};
    use crate::matchmaker_transport::{
        MatchmakerControlEndpoint, MatchmakerControlIdentity, TlsMatchmakerHandoffRouter,
    };
    use crate::realtime::registry::{ParticipantIdentity, SessionHandle};
    use crate::repository::{
        ChatRepository, InMemoryBackend, InMemoryChatRepository, InMemoryFriendsRepository,
        InMemoryGroupsRepository, InMemoryLeaderboardsRepository, InMemoryStorageRepository,
        InMemoryWalletRepository,
    };
    use crate::services::matchmaker_directory::StorageMatchmakerLeaseDirectory;
    use crate::services::party_directory::StoragePartyDirectory;
    use crate::services::{
        ChatService, CreateLeaderboardRequest, GroupsService, LeaderboardService, Operator,
        PlayerNotificationService, SortOrder, WalletService,
    };
    use crate::session::{NodeId, OwnershipGeneration, SessionId};
    use crate::storage::UserId;
    use crate::time::{DurationMillis, TimestampMillis};
    use crate::transport::TransportKind;
    use rustls::pki_types::CertificateDer;
    use std::collections::BTreeMap;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// A gateway whose only wiring is an in-memory friends service.
    fn friends_gateway_with_chat_repository() -> (Gateway, Arc<dyn ChatRepository>) {
        let friends = Arc::new(FriendsService::new(Arc::new(
            InMemoryFriendsRepository::new(),
        )));
        let groups = Arc::new(GroupsService::new(
            Arc::new(InMemoryGroupsRepository::new()),
        ));
        let backend = Arc::new(InMemoryBackend::new());
        let chat_repository: Arc<dyn ChatRepository> = Arc::new(InMemoryChatRepository::new());
        let gateway = Gateway::new().with_domain_services(DomainRpcServices {
            chat_authorizer: Arc::new(ChatChannelAuthorizer::new(
                Arc::clone(&friends),
                Arc::clone(&groups),
            )),
            chat_rate_limits: crate::services::ChatRateLimitPolicy::default(),
            chat_presence: Arc::new(ChatPresenceRegistry::new()),
            chat_cluster_presence: None,
            node_id: "test-node".to_owned(),
            friends,
            player_notifications: Arc::new(PlayerNotificationService::new(backend)),
            groups,
            leaderboards: Arc::new(LeaderboardService::new(Arc::new(
                InMemoryLeaderboardsRepository::new(),
            ))),
            chat: Arc::new(ChatService::new(Arc::clone(&chat_repository))),
            wallet: Arc::new(WalletService::new(
                Arc::new(InMemoryWalletRepository::new()),
            )),
        });
        (gateway, chat_repository)
    }

    fn friends_gateway() -> Gateway {
        friends_gateway_with_chat_repository().0
    }

    fn local_chat_dispatcher(
        gateway: &Arc<Gateway>,
        repository: Arc<dyn ChatRepository>,
    ) -> crate::chat_cluster::ChatDeliveryDispatcher {
        let source = NodeId::new("test-node".to_owned()).expect("node id");
        let delivery_gateway = Arc::clone(gateway);
        let delivery_source = source.clone();
        crate::chat_cluster::ChatDeliveryDispatcher::new_with_local_delivery(
            source,
            repository,
            Arc::new(ChatPresenceDirectory::default()),
            Arc::new(move |delivery| {
                Ok(delivery_gateway.deliver_local_chat(&delivery_source, delivery))
            }),
            Arc::new(|_, _| Ok(ChatDeliveryDisposition::Unknown)),
        )
    }

    async fn dispatch_local_chat(
        dispatcher: &crate::chat_cluster::ChatDeliveryDispatcher,
    ) -> crate::chat_cluster::ChatDeliveryDispatchStats {
        tokio::time::timeout(
            Duration::from_secs(2),
            dispatcher.dispatch_once(SystemClock.now(), 16),
        )
        .await
        .expect("chat outbox dispatch before deadline")
        .expect("chat outbox dispatch succeeds")
    }

    #[tokio::test]
    async fn poisoned_local_presence_defers_outbox_then_retry_delivers() {
        let (gateway, repository) = friends_gateway_with_chat_repository();
        let gateway = Arc::new(gateway);
        let (participant, mut outbound) = register(&gateway, Some("alice"));
        let presence = Arc::clone(
            &gateway
                .domain
                .as_ref()
                .expect("domain services")
                .chat_presence,
        );
        presence.join(
            "ch_poisoned_presence",
            participant,
            "alice",
            ChatTarget::CurrentRoom { room_id: 7 },
            4,
        );
        let now = SystemClock.now();
        repository
            .stage_delivery_outbox(crate::repository::ChatDeliveryOutboxRecord {
                origin_node_id: "test-node".to_owned(),
                channel_id: "ch_poisoned_presence".to_owned(),
                event_id: 1,
                authority_epoch: 4,
                payload: serde_json::json!({
                    "version": 1,
                    "type": "message.create",
                    "channel_id": "ch_poisoned_presence",
                    "event_id": 1,
                    "message": {
                        "id": 1,
                        "sender": "alice",
                        "content": "retained",
                        "created_at_unix_ms": now.unix_millis(),
                        "updated_at_unix_ms": now.unix_millis(),
                        "revision": 1,
                        "last_event_id": 1,
                        "deleted": false
                    }
                })
                .to_string(),
                created_at: now,
                expires_at: TimestampMillis::from_unix_millis(u64::MAX),
            })
            .await
            .expect("stage source-local delivery row");
        presence.poison_state_for_test();
        let dispatcher = local_chat_dispatcher(&gateway, Arc::clone(&repository));

        let first = dispatch_local_chat(&dispatcher).await;
        assert_eq!(first.acknowledged, 0);
        assert_eq!(first.deferred, 1);
        assert!(outbound.try_recv().is_err(), "poison must not fan out");
        assert_eq!(
            repository
                .active_delivery_outbox("test-node", now, 8)
                .await
                .expect("retained source-local row")
                .len(),
            1
        );

        let retry = dispatch_local_chat(&dispatcher).await;
        assert_eq!(retry.acknowledged, 1);
        assert_eq!(retry.deferred, 0);
        let delivered = recv_outbound_before_deadline(&mut outbound, "recovered delivery").await;
        assert_eq!(delivered.envelope.kind, KIND_CHAT_EVENT);
        assert!(
            repository
                .active_delivery_outbox("test-node", now, 8)
                .await
                .expect("acknowledged source-local row")
                .is_empty()
        );
    }

    async fn recv_outbound_before_deadline(
        receiver: &mut mpsc::Receiver<Outbound>,
        context: &str,
    ) -> Outbound {
        tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect(context)
            .expect(context)
    }

    /// Register a session, authenticated as `user` unless `user` is `None`.
    fn register(gw: &Gateway, user: Option<&str>) -> (ParticipantId, mpsc::Receiver<Outbound>) {
        register_with_capacity(gw, user, 8)
    }

    fn register_with_capacity(
        gw: &Gateway,
        user: Option<&str>,
        capacity: usize,
    ) -> (ParticipantId, mpsc::Receiver<Outbound>) {
        let id = gw.next_participant_id();
        let (tx, rx) = mpsc::channel(capacity);
        let identity = user.map(|user| ParticipantIdentity {
            user_id: UserId::new(user).expect("user id"),
            session_id: SessionId::new(format!("session-{user}")).expect("session id"),
            expires_at: TimestampMillis::from_unix_millis(9_999_999_999),
        });
        gw.registry().register(SessionHandle {
            id,
            kind: TransportKind::WebSocket,
            outbound: tx,
            identity,
        });
        (id, rx)
    }

    /// Build a `KIND_RPC_REQUEST` envelope with a JSON body.
    fn rpc(request_id: u64, method: &str, body: serde_json::Value) -> Envelope {
        let payload = body.to_string().into_bytes();
        Envelope::new(
            KIND_RPC_REQUEST,
            protocol::encode_rpc_request(request_id, method, &payload),
        )
    }

    /// Await the caller's correlated response: `(request_id, status, payload)`.
    async fn recv(rx: &mut mpsc::Receiver<Outbound>) -> (u64, u8, Vec<u8>) {
        let out = rx.recv().await.expect("rpc response delivered");
        assert_eq!(out.envelope.kind, KIND_RPC_RESPONSE);
        assert_eq!(out.delivery, Delivery::Reliable);
        let resp = protocol::decode_rpc_response(&out.envelope.body).expect("decodes");
        (resp.request_id, resp.status, resp.payload.to_vec())
    }

    fn json(payload: &[u8]) -> serde_json::Value {
        serde_json::from_slice(payload).expect("json body")
    }

    #[tokio::test]
    async fn matchmaker_forms_a_pair_then_requires_owner_bound_handoff_acceptance() {
        let gw = Gateway::new();
        let (alice, mut ra) = register(&gw, Some("alice"));
        let (bob, mut rb) = register(&gw, Some("bob"));
        let request = serde_json::json!({
            "query": "",
            "properties": { "mode": "duo" },
            "min_count": 2,
            "max_count": 2,
            "ttl_ms": 60_000
        });

        gw.handle_inbound(alice, &rpc(1, "matchmaker.add", request.clone()));
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let alice_ticket = json(&body)["ticket_id"]
            .as_str()
            .expect("alice ticket")
            .to_owned();

        gw.handle_inbound(bob, &rpc(2, "matchmaker.add", request));
        let (_, status, body) = recv(&mut rb).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let bob_ticket = json(&body)["ticket_id"]
            .as_str()
            .expect("bob ticket")
            .to_owned();

        let alice_found = ra.recv().await.expect("alice receives match found");
        let bob_found = rb.recv().await.expect("bob receives match found");
        assert_eq!(alice_found.envelope.kind, KIND_MATCHMAKER_MATCHED);
        assert_eq!(bob_found.envelope.kind, KIND_MATCHMAKER_MATCHED);
        let alice_handoff = json(&alice_found.envelope.body);
        let bob_handoff = json(&bob_found.envelope.body);
        assert_eq!(alice_handoff["ticket_id"], alice_ticket);
        assert_eq!(bob_handoff["ticket_id"], bob_ticket);

        let raw_room_id = alice_handoff["match_id"].as_u64().expect("match id");
        assert_eq!(
            gw.handle_inbound(
                bob,
                &Envelope::new(
                    KIND_ROOM_JOIN,
                    citadel_wire::room::RoomJoin {
                        room_id: raw_room_id
                    }
                    .encode(),
                ),
            ),
            0,
            "a raw room id never authorizes matchmaker admission"
        );
        assert!(gw.rooms.snapshot()[0].members.is_empty());

        // Bob cannot use Alice's otherwise opaque token and ticket.
        gw.handle_inbound(
            bob,
            &rpc(
                3,
                "matchmaker.accept",
                serde_json::json!({
                    "ticket_id": alice_ticket,
                    "join_token": alice_handoff["join_token"],
                }),
            ),
        );
        let (_, status, _) = recv(&mut rb).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);

        // A new connection for the same authenticated owner can recover the
        // pending handoff; the old participant id is not the authorization key.
        gw.unregister_session(alice);
        let (alice_reconnected, mut ra_reconnected) = register(&gw, Some("alice"));

        for (participant, receiver, ticket, handoff, request_id) in [
            (
                alice_reconnected,
                &mut ra_reconnected,
                alice_ticket,
                alice_handoff,
                4_u64,
            ),
            (bob, &mut rb, bob_ticket, bob_handoff, 5_u64),
        ] {
            gw.handle_inbound(
                participant,
                &rpc(
                    request_id,
                    "matchmaker.accept",
                    serde_json::json!({
                        "ticket_id": ticket,
                        "join_token": handoff["join_token"],
                    }),
                ),
            );
            let (_, status, accepted) = recv(receiver).await;
            assert_eq!(status, protocol::RPC_STATUS_OK);
            assert_eq!(json(&accepted)["accepted"], true);
            let joined = receiver
                .recv()
                .await
                .expect("accepted participant joins room");
            assert_eq!(joined.envelope.kind, KIND_ROOM_JOINED);
        }
        let rooms = gw.rooms.snapshot();
        assert_eq!(rooms.len(), 1);
        assert_eq!(rooms[0].label.mode, "matchmaker");
        assert_eq!(rooms[0].members, vec![bob, alice_reconnected]);
    }

    #[tokio::test]
    async fn party_ticket_is_atomic_and_each_member_recovers_an_owner_bound_handoff() {
        let gw = Gateway::new();
        let (alice, mut ra) = register(&gw, Some("alice"));
        let (bob, mut rb) = register(&gw, Some("bob"));
        let (charlie, mut rc) = register(&gw, Some("charlie"));

        gw.handle_inbound(alice, &rpc(1, "party.create", serde_json::json!({})));
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let party_id = json(&body)["party_id"]
            .as_str()
            .expect("party id")
            .to_owned();

        gw.handle_inbound(
            alice,
            &rpc(
                2,
                "party.invite",
                serde_json::json!({ "party_id": party_id, "target_user_id": "bob" }),
            ),
        );
        assert_eq!(recv(&mut ra).await.1, protocol::RPC_STATUS_OK);
        gw.handle_inbound(
            bob,
            &rpc(
                3,
                "party.accept",
                serde_json::json!({ "party_id": party_id }),
            ),
        );
        assert_eq!(recv(&mut rb).await.1, protocol::RPC_STATUS_OK);

        let party_request = serde_json::json!({
            "party_id": party_id,
            "min_count": 3,
            "max_count": 3,
            "ttl_ms": 60_000
        });
        gw.handle_inbound(alice, &rpc(4, "matchmaker.add", party_request));
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let party_ticket = json(&body)["ticket_id"]
            .as_str()
            .expect("party ticket")
            .to_owned();

        gw.handle_inbound(
            alice,
            &rpc(
                5,
                "party.invite",
                serde_json::json!({ "party_id": party_id, "target_user_id": "charlie" }),
            ),
        );
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(
            String::from_utf8_lossy(&body),
            "party is queued; cancel its matchmaker ticket first"
        );

        gw.handle_inbound(
            charlie,
            &rpc(
                6,
                "matchmaker.add",
                serde_json::json!({ "min_count": 3, "max_count": 3, "ttl_ms": 60_000 }),
            ),
        );
        let (_, status, _) = recv(&mut rc).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);

        let alice_handoff = json(&ra.recv().await.expect("alice match found").envelope.body);
        let bob_handoff = json(&rb.recv().await.expect("bob match found").envelope.body);
        let charlie_handoff = json(&rc.recv().await.expect("charlie match found").envelope.body);
        assert_eq!(alice_handoff["ticket_id"], party_ticket);
        assert_eq!(bob_handoff["ticket_id"], party_ticket);
        assert_ne!(alice_handoff["join_token"], bob_handoff["join_token"]);
        assert_eq!(
            gw.rooms.snapshot()[0].members.len(),
            0,
            "formation never pre-admits a split party"
        );

        gw.unregister_session(bob);
        let (bob_reconnected, mut rb_reconnected) = register(&gw, Some("bob"));
        for (participant, receiver, handoff, request_id) in [
            (alice, &mut ra, alice_handoff, 6_u64),
            (bob_reconnected, &mut rb_reconnected, bob_handoff, 7_u64),
            (charlie, &mut rc, charlie_handoff, 8_u64),
        ] {
            gw.handle_inbound(
                participant,
                &rpc(
                    request_id,
                    "matchmaker.accept",
                    serde_json::json!({
                        "ticket_id": handoff["ticket_id"],
                        "join_token": handoff["join_token"],
                    }),
                ),
            );
            assert_eq!(recv(receiver).await.1, protocol::RPC_STATUS_OK);
            assert_eq!(
                receiver.recv().await.expect("joined room").envelope.kind,
                KIND_ROOM_JOINED
            );
        }
        assert_eq!(gw.rooms.snapshot()[0].members.len(), 3);
    }

    #[tokio::test]
    async fn matchmaker_ticket_is_authenticated_and_owner_bound() {
        let gw = Gateway::new();
        let (guest, mut guest_rx) = register(&gw, None);
        gw.handle_inbound(
            guest,
            &rpc(
                1,
                "matchmaker.add",
                serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 }),
            ),
        );
        let (_, status, body) = recv(&mut guest_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), "authentication required");

        let (alice, mut ra) = register(&gw, Some("alice"));
        let (bob, mut rb) = register(&gw, Some("bob"));
        let request = serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 });
        gw.handle_inbound(alice, &rpc(2, "matchmaker.add", request));
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let ticket_id = json(&body)["ticket_id"]
            .as_str()
            .expect("ticket id")
            .to_owned();

        gw.handle_inbound(
            bob,
            &rpc(
                3,
                "matchmaker.cancel",
                serde_json::json!({ "ticket_id": ticket_id }),
            ),
        );
        let (_, status, body) = recv(&mut rb).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["cancelled"], false);

        gw.handle_inbound(
            alice,
            &rpc(
                4,
                "matchmaker.cancel",
                serde_json::json!({ "ticket_id": ticket_id }),
            ),
        );
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["cancelled"], true);
    }

    #[tokio::test]
    async fn expired_matchmaker_handoff_rejects_acceptance_and_discards_empty_room() {
        let gw = Gateway::new();
        let (alice, mut ra) = register(&gw, Some("alice"));
        let ticket = TicketId::parse("opaque-ticket").expect("ticket id");
        let room_id = gw.rooms.create(RoomLabel {
            map: "default".to_owned(),
            mode: "matchmaker".to_owned(),
            max_players: 2,
            open: false,
        });
        let now = SystemClock.now();
        let token = JoinToken::generate().expect("system entropy available for test");
        gw.handoffs.lock().expect("handoff lock").pending.insert(
            ticket.clone(),
            vec![PendingMatchHandoff {
                user_id: "alice".to_owned(),
                room_id,
                token: token.clone(),
                expires_at: now,
            }],
        );

        gw.handle_inbound(
            alice,
            &rpc(
                1,
                "matchmaker.accept",
                serde_json::json!({
                    "ticket_id": ticket.as_str(),
                    "join_token": token.as_str(),
                }),
            ),
        );
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(
            String::from_utf8_lossy(&body),
            "match handoff not found or expired"
        );
        assert!(
            gw.rooms.snapshot().is_empty(),
            "expired empty room is pruned"
        );
    }

    #[tokio::test]
    async fn friend_add_then_list_round_trips_for_the_authenticated_caller() {
        let gw = friends_gateway();
        let (alice, mut ra) = register(&gw, Some("alice"));

        gw.handle_inbound(
            alice,
            &rpc(1, "friends.add", serde_json::json!({ "other": "bob" })),
        );
        let (rid, status, body) = recv(&mut ra).await;
        assert_eq!(rid, 1);
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["state"], "invited_sent");

        gw.handle_inbound(alice, &rpc(2, "friends.list", serde_json::json!({})));
        let (rid, status, body) = recv(&mut ra).await;
        assert_eq!(rid, 2);
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let listed = json(&body);
        assert_eq!(listed["friends"][0]["user_id"], "bob");
        assert_eq!(listed["friends"][0]["state"], "invited_sent");
    }

    #[test]
    fn remote_chat_delivery_validates_before_missing_domain_is_unavailable() {
        let gateway = Gateway::new();
        let local_node = NodeId::new("node-b".to_owned()).expect("node");
        let directory = crate::chat_cluster::ChatPresenceDirectory::default();
        let valid = crate::chat_cluster::RemoteChatDelivery {
            event_id: 9,
            channel_id: "ch_remote".to_owned(),
            destination_generation: OwnershipGeneration::new(2),
            authority_epoch: 4,
            payload: r#"{"version":1,"type":"message.create","channel_id":"ch_remote","event_id":9,"message":{"id":1,"sender":"alice","content":"hello","created_at_unix_ms":1000,"updated_at_unix_ms":1000,"revision":1,"last_event_id":9,"deleted":false}}"#.to_owned(),
            deadline: TimestampMillis::from_unix_millis(u64::MAX),
        };
        let invalid_deliveries = [
            crate::chat_cluster::RemoteChatDelivery {
                deadline: TimestampMillis::from_unix_millis(0),
                ..valid.clone()
            },
            crate::chat_cluster::RemoteChatDelivery {
                payload: "not-json".to_owned(),
                ..valid.clone()
            },
            crate::chat_cluster::RemoteChatDelivery {
                payload: "null".to_owned(),
                ..valid.clone()
            },
            crate::chat_cluster::RemoteChatDelivery {
                payload: r#"{"version":1,"type":"message.create","channel_id":"ch_remote","event_id":9,"message":{"id":1,"sender":"alice","content":"hello","created_at_unix_ms":1000,"updated_at_unix_ms":1000,"revision":1,"last_event_id":9,"deleted":false},"force_current":true}"#.to_owned(),
                ..valid.clone()
            },
            crate::chat_cluster::RemoteChatDelivery {
                payload: r#"{"version":1,"type":"message.create","type":"message.create","channel_id":"ch_remote","event_id":9,"message":{"id":1,"sender":"alice","content":"hello","created_at_unix_ms":1000,"updated_at_unix_ms":1000,"revision":1,"last_event_id":9,"deleted":false}}"#.to_owned(),
                ..valid.clone()
            },
            crate::chat_cluster::RemoteChatDelivery {
                payload: r#"{"version":1,"type":"message.create","channel_id":"ch_other","event_id":9,"message":{"last_event_id":9}}"#.to_owned(),
                ..valid.clone()
            },
        ];

        for invalid in invalid_deliveries {
            assert_eq!(
                gateway.deliver_remote_chat(&local_node, &directory, invalid),
                crate::chat_cluster::ChatDeliveryDisposition::Rejected
            );
        }

        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/chat-live-events-v1.json"
        ))
        .expect("canonical chat fixture");
        let exact_multibyte = fixture["content_validation"]
            .as_array()
            .expect("content validation cases")
            .iter()
            .find(|case| case["name"] == "update_multibyte_exactly_2048_utf8_bytes")
            .expect("shared fixture must lock the exact multibyte boundary");
        assert_eq!(exact_multibyte["content_repeat"]["value"], "é");
        assert_eq!(exact_multibyte["content_repeat"]["count"], 1024);
        assert_eq!(exact_multibyte["accepted"], true);
        for case in fixture["content_validation"]
            .as_array()
            .expect("content validation cases")
        {
            let base_name = case["event"].as_str().expect("base event name");
            let mut event = fixture["valid"]
                .as_array()
                .expect("valid events")
                .iter()
                .find(|candidate| candidate["name"] == base_name)
                .expect("content base event")["event"]
                .clone();
            let content = if let Some(content) = case["content"].as_str() {
                content.to_owned()
            } else {
                case["content_repeat"]["value"]
                    .as_str()
                    .expect("repeat value")
                    .repeat(
                        case["content_repeat"]["count"]
                            .as_u64()
                            .and_then(|count| usize::try_from(count).ok())
                            .expect("repeat count"),
                    )
            };
            event["channel_id"] = serde_json::Value::String(valid.channel_id.clone());
            event["event_id"] = serde_json::Value::from(valid.event_id);
            event["message"]["last_event_id"] = serde_json::Value::from(valid.event_id);
            event["message"]["content"] = serde_json::Value::String(content);
            let candidate = crate::chat_cluster::RemoteChatDelivery {
                payload: event.to_string(),
                ..valid.clone()
            };
            let expected = if case["accepted"].as_bool().expect("accepted flag") {
                crate::chat_cluster::ChatDeliveryDisposition::Unavailable
            } else {
                crate::chat_cluster::ChatDeliveryDisposition::Rejected
            };
            assert_eq!(
                gateway.deliver_remote_chat(&local_node, &directory, candidate),
                expected,
                "{}",
                case["name"].as_str().expect("case name")
            );
        }
        assert_eq!(
            gateway.deliver_remote_chat(&local_node, &directory, valid),
            crate::chat_cluster::ChatDeliveryDisposition::Unavailable,
            "a live gateway without domain authority cannot decide subscriber absence"
        );
    }

    #[tokio::test]
    async fn remote_chat_delivery_rechecks_fences_before_local_fanout() {
        let gateway = friends_gateway();
        let (alice, mut alice_rx) = register(&gateway, Some("alice"));
        let local_node = NodeId::new("node-b".to_owned()).expect("node");
        let directory = crate::chat_cluster::ChatPresenceDirectory::default();
        let now = TimestampMillis::from_unix_millis(0);
        gateway.domain.as_ref().expect("domain").chat_presence.join(
            "ch_remote",
            alice,
            "alice",
            crate::services::ChatTarget::CurrentRoom { room_id: 7 },
            4,
        );
        assert_eq!(
            directory.advertise(
                crate::chat_cluster::ChatPresenceLease {
                    channel_id: "ch_remote".to_owned(),
                    node_id: local_node.clone(),
                    generation: OwnershipGeneration::new(2),
                    expires_at: TimestampMillis::from_unix_millis(u64::MAX),
                },
                now,
            ),
            crate::chat_cluster::ChatLeaseUpdate::Applied
        );
        let delivery = crate::chat_cluster::RemoteChatDelivery {
            event_id: 9,
            channel_id: "ch_remote".to_owned(),
            destination_generation: OwnershipGeneration::new(2),
            authority_epoch: 4,
            payload: serde_json::json!({
                "version": 1,
                "type": "message.create",
                "channel_id": "ch_remote",
                "event_id": 9,
                "message": {
                    "id": 1,
                    "sender": "alice",
                    "content": "hello",
                    "created_at_unix_ms": 1000,
                    "updated_at_unix_ms": 1000,
                    "revision": 1,
                    "last_event_id": 9,
                    "deleted": false
                }
            })
            .to_string(),
            deadline: TimestampMillis::from_unix_millis(u64::MAX),
        };
        assert_eq!(
            gateway.deliver_remote_chat(&local_node, &directory, delivery.clone()),
            crate::chat_cluster::ChatDeliveryDisposition::Delivered
        );
        let event = alice_rx.recv().await.expect("remote event");
        assert_eq!(event.envelope.kind, KIND_CHAT_EVENT);

        let invalid_deliveries = [
            (
                "expired deadline",
                crate::chat_cluster::RemoteChatDelivery {
                    deadline: TimestampMillis::from_unix_millis(0),
                    ..delivery.clone()
                },
            ),
            (
                "malformed JSON",
                crate::chat_cluster::RemoteChatDelivery {
                    payload: "not-json".to_owned(),
                    ..delivery.clone()
                },
            ),
            (
                "malformed JSON despite absent authority",
                crate::chat_cluster::RemoteChatDelivery {
                    authority_epoch: 5,
                    payload: "not-json".to_owned(),
                    ..delivery.clone()
                },
            ),
            (
                "non-object JSON",
                crate::chat_cluster::RemoteChatDelivery {
                    payload: "null".to_owned(),
                    ..delivery.clone()
                },
            ),
            (
                "missing correlated fields",
                crate::chat_cluster::RemoteChatDelivery {
                    payload: r#"{"version":1,"type":"message.create"}"#.to_owned(),
                    ..delivery.clone()
                },
            ),
            (
                "wrong version",
                crate::chat_cluster::RemoteChatDelivery {
                    payload: r#"{"version":2,"type":"message.create","channel_id":"ch_remote","event_id":9,"message":{"last_event_id":9}}"#.to_owned(),
                    ..delivery.clone()
                },
            ),
            (
                "wrong type",
                crate::chat_cluster::RemoteChatDelivery {
                    payload: r#"{"version":1,"type":"message.hijack","channel_id":"ch_remote","event_id":9,"message":{"last_event_id":9}}"#.to_owned(),
                    ..delivery.clone()
                },
            ),
            (
                "wrong channel",
                crate::chat_cluster::RemoteChatDelivery {
                    payload: r#"{"version":1,"type":"message.create","channel_id":"ch_other","event_id":9,"message":{"last_event_id":9}}"#.to_owned(),
                    ..delivery.clone()
                },
            ),
            (
                "wrong event",
                crate::chat_cluster::RemoteChatDelivery {
                    payload: r#"{"version":1,"type":"message.create","channel_id":"ch_remote","event_id":10,"message":{"last_event_id":9}}"#.to_owned(),
                    ..delivery.clone()
                },
            ),
            (
                "wrong message last event",
                crate::chat_cluster::RemoteChatDelivery {
                    payload: r#"{"version":1,"type":"message.create","channel_id":"ch_remote","event_id":9,"message":{"last_event_id":10}}"#.to_owned(),
                    ..delivery.clone()
                },
            ),
        ];
        for (reason, invalid) in invalid_deliveries {
            assert_eq!(
                gateway.deliver_remote_chat(&local_node, &directory, invalid),
                crate::chat_cluster::ChatDeliveryDisposition::Rejected,
                "{reason} must fail closed"
            );
            assert!(alice_rx.try_recv().is_err(), "{reason} must not fan out");
        }

        assert_eq!(
            gateway.deliver_remote_chat(
                &local_node,
                &directory,
                crate::chat_cluster::RemoteChatDelivery {
                    destination_generation: OwnershipGeneration::new(3),
                    ..crate::chat_cluster::RemoteChatDelivery {
                        event_id: 10,
                        channel_id: "ch_remote".to_owned(),
                        destination_generation: OwnershipGeneration::new(2),
                        authority_epoch: 4,
                        payload: r#"{"version":1,"type":"message.create","channel_id":"ch_remote","event_id":10,"message":{"id":2,"sender":"alice","content":"again","created_at_unix_ms":1100,"updated_at_unix_ms":1100,"revision":1,"last_event_id":10,"deleted":false}}"#.to_owned(),
                        deadline: TimestampMillis::from_unix_millis(u64::MAX),
                    }
                }
            ),
            crate::chat_cluster::ChatDeliveryDisposition::Stale
        );
        assert!(
            alice_rx.try_recv().is_err(),
            "stale delivery must not fan out"
        );

        assert_eq!(
            gateway.deliver_remote_chat(
                &local_node,
                &directory,
                crate::chat_cluster::RemoteChatDelivery {
                    event_id: 11,
                    channel_id: "ch_remote".to_owned(),
                    destination_generation: OwnershipGeneration::new(2),
                    authority_epoch: 4,
                    payload: "not-json".to_owned(),
                    deadline: TimestampMillis::from_unix_millis(u64::MAX),
                }
            ),
            crate::chat_cluster::ChatDeliveryDisposition::Rejected
        );
        assert!(
            alice_rx.try_recv().is_err(),
            "invalid payload must not fan out"
        );

        assert_eq!(
            gateway.deliver_remote_chat(
                &local_node,
                &directory,
                crate::chat_cluster::RemoteChatDelivery {
                    event_id: 12,
                    channel_id: "ch_remote".to_owned(),
                    destination_generation: OwnershipGeneration::new(2),
                    authority_epoch: 4,
                    payload: "null".to_owned(),
                    deadline: TimestampMillis::from_unix_millis(u64::MAX),
                }
            ),
            crate::chat_cluster::ChatDeliveryDisposition::Rejected
        );
        assert!(
            alice_rx.try_recv().is_err(),
            "scalar payload must not fan out"
        );
    }

    #[tokio::test]
    async fn remote_party_presence_mtls_reauthorizes_and_resyncs_before_live_deltas() {
        let node_a = NodeId::new("presence-node-a").expect("node a");
        let node_b = NodeId::new("presence-node-b").expect("node b");
        let (identity_a, cert_a) = control_identity();
        let (identity_b, cert_b) = control_identity();
        let router_a = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_a.clone(),
                identity_a,
                BTreeMap::from([(node_b.clone(), cert_b)]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router a"),
        );
        let router_b = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_b.clone(),
                identity_b,
                BTreeMap::from([(node_a.clone(), cert_a)]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router b"),
        );
        let storage: Arc<dyn crate::repository::StorageRepository> =
            Arc::new(InMemoryStorageRepository::new());
        let directory = Arc::new(StoragePartyDirectory::new(storage));
        let party_id = PartyId::parse("party-presence-mtls").expect("party id");
        let now = SystemClock.now();
        directory
            .create(
                party_id.clone(),
                "alice",
                node_b.clone(),
                now.checked_add(DurationMillis::from_millis(30_000))
                    .expect("party lease expiry"),
                now,
            )
            .await
            .expect("durable party");
        let gateway_b = Arc::new(Gateway::new().with_storage_party_directory(
            Arc::clone(&directory),
            node_b.clone(),
            Arc::clone(&router_b),
        ));
        gateway_b.register_party_directory_endpoint();
        let listener = router_b
            .serve("127.0.0.1:0".parse().expect("loopback"))
            .expect("listener b");
        router_a.register_endpoint(
            node_b.clone(),
            MatchmakerControlEndpoint {
                address: listener.local_addr(),
                server_name: "localhost".to_owned(),
            },
        );

        let recipient = gateway_b.next_participant_id();
        let (reliable, reliable_rx) = mpsc::channel(4);
        let unreliable = gateway_b.register_session(SessionHandle {
            id: recipient,
            kind: TransportKind::WebSocket,
            outbound: reliable,
            identity: Some(ParticipantIdentity {
                user_id: UserId::new("alice").expect("user"),
                session_id: SessionId::new("presence-session").expect("session"),
                expires_at: TimestampMillis::from_unix_millis(u64::MAX),
            }),
        });
        let mut outbound = TestOutboundReceiver {
            reliable: reliable_rx,
            unreliable,
        };
        let presence = gateway_b.party_presence.as_ref().expect("presence");
        assert_eq!(
            presence.directory.advertise(
                PartyPresenceLease {
                    party_id: party_id.as_str().to_owned(),
                    node_id: node_b.clone(),
                    generation: OwnershipGeneration::new(9),
                    expires_at: TimestampMillis::from_unix_millis(u64::MAX),
                    party_revision: 1,
                },
                now,
            ),
            crate::party_presence::PartyPresenceUpdate::Applied
        );
        assert_eq!(
            presence.directory.advertise(
                PartyPresenceLease {
                    party_id: party_id.as_str().to_owned(),
                    node_id: node_a.clone(),
                    generation: OwnershipGeneration::new(7),
                    expires_at: TimestampMillis::from_unix_millis(u64::MAX),
                    party_revision: 1,
                },
                now,
            ),
            crate::party_presence::PartyPresenceUpdate::Applied
        );
        let delivery = |sequence, members: Vec<String>| RemotePartyPresenceDelivery {
            party_id: party_id.as_str().to_owned(),
            origin_node: node_a.clone(),
            origin_generation: OwnershipGeneration::new(7),
            destination_generation: OwnershipGeneration::new(9),
            snapshot: PartyPresenceSnapshot {
                party_id: party_id.as_str().to_owned(),
                party_revision: 1,
                sequence,
                online_members: members,
            },
            deadline: TimestampMillis::from_unix_millis(u64::MAX),
        };
        assert_eq!(
            router_a
                .deliver_party_presence(&node_b, delivery(1, vec!["alice".to_owned()]))
                .expect("mTLS delivery"),
            PartyPresenceDeliveryDisposition::Delivered
        );
        let first = tokio::time::timeout(Duration::from_secs(2), outbound.unreliable.recv())
            .await
            .expect("snapshot before deadline");
        let first_json: serde_json::Value =
            serde_json::from_slice(&first.envelope.body).expect("json");
        assert_eq!(first_json["type"], "party.presence.delta");
        assert_eq!(first_json["online_members"], serde_json::json!(["alice"]));

        // A local latest-queue drop turns the next authenticated remote update
        // into a reliable resync barrier followed by a fresh snapshot.
        presence
            .local
            .mark_queue_drop(party_id.as_str(), &recipient.to_string());
        assert_eq!(
            router_a
                .deliver_party_presence(&node_b, delivery(2, vec!["alice".to_owned()]))
                .expect("mTLS resync delivery"),
            PartyPresenceDeliveryDisposition::Delivered
        );
        let resync = tokio::time::timeout(Duration::from_secs(2), outbound.reliable.recv())
            .await
            .expect("resync barrier before deadline")
            .expect("session open");
        let resync_json: serde_json::Value =
            serde_json::from_slice(&resync.envelope.body).expect("json");
        assert_eq!(resync_json["type"], "party.presence.resync");
        let snapshot = tokio::time::timeout(Duration::from_secs(2), outbound.unreliable.recv())
            .await
            .expect("snapshot before deadline");
        let snapshot_json: serde_json::Value =
            serde_json::from_slice(&snapshot.envelope.body).expect("json");
        assert_eq!(snapshot_json["type"], "party.presence.snapshot");

        // The receiver fetches the durable aggregate for every delivery; a
        // source cannot disclose a nonmember merely by holding mTLS access.
        assert_eq!(
            router_a
                .deliver_party_presence(&node_b, delivery(3, vec!["mallory".to_owned()]))
                .expect("mTLS rejected delivery"),
            PartyPresenceDeliveryDisposition::Stale
        );
        assert!(
            outbound.try_recv().is_err(),
            "nonmember payload never fans out"
        );
    }

    #[tokio::test]
    async fn chat_join_send_and_leave_use_local_presence_and_reliable_events() {
        let (gateway, chat_repository) = friends_gateway_with_chat_repository();
        let gateway = Arc::new(gateway);
        let dispatcher = local_chat_dispatcher(&gateway, chat_repository);
        let (alice, mut alice_rx) = register(&gateway, Some("alice"));
        let (bob, mut bob_rx) = register(&gateway, Some("bob"));

        // A reciprocal friend request establishes the direct-chat authority.
        gateway.handle_inbound(
            alice,
            &rpc(1, "friends.add", serde_json::json!({"other": "bob"})),
        );
        let _ = recv(&mut alice_rx).await;
        gateway.handle_inbound(
            bob,
            &rpc(2, "friends.add", serde_json::json!({"other": "alice"})),
        );
        let _ = recv(&mut bob_rx).await;

        gateway.handle_inbound(
            alice,
            &rpc(
                3,
                "chat.join",
                serde_json::json!({"target": {"kind": "direct", "other_user_id": "bob"}}),
            ),
        );
        let (_, status, body) = recv(&mut alice_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let channel_id = json(&body)["channel_id"]
            .as_str()
            .expect("channel id")
            .to_owned();

        gateway.handle_inbound(
            bob,
            &rpc(
                4,
                "chat.join",
                serde_json::json!({"target": {"kind": "direct", "other_user_id": "alice"}}),
            ),
        );
        let presence = alice_rx.recv().await.expect("presence event");
        assert_eq!(presence.delivery, Delivery::Reliable);
        assert_eq!(presence.envelope.kind, KIND_CHAT_EVENT);
        assert_eq!(json(&presence.envelope.body)["type"], "presence.join");
        let (_, status, _) = recv(&mut bob_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);

        gateway.handle_inbound(
            alice,
            &rpc(
                5,
                "chat.send",
                serde_json::json!({"channel_id": channel_id, "content": "hello"}),
            ),
        );
        let (_, status, body) = tokio::time::timeout(Duration::from_secs(2), recv(&mut alice_rx))
            .await
            .expect("chat.send response before deadline");
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["event_id"], 1);

        let stats = dispatch_local_chat(&dispatcher).await;
        assert_eq!(stats.loaded, 1);
        assert_eq!(stats.acknowledged, 1);

        let alice_event = tokio::time::timeout(Duration::from_secs(2), alice_rx.recv())
            .await
            .expect("sender live event before deadline")
            .expect("sender session open");
        let bob_event = tokio::time::timeout(Duration::from_secs(2), bob_rx.recv())
            .await
            .expect("recipient live event before deadline")
            .expect("recipient session open");
        for event in [&alice_event, &bob_event] {
            assert_eq!(event.delivery, Delivery::Reliable);
            assert_eq!(event.envelope.kind, KIND_CHAT_EVENT);
            assert_eq!(json(&event.envelope.body)["type"], "message.create");
            assert_eq!(json(&event.envelope.body)["message"]["content"], "hello");
        }

        gateway.handle_inbound(
            bob,
            &rpc(
                6,
                "chat.leave",
                serde_json::json!({"channel_id": channel_id}),
            ),
        );
        let leave = alice_rx.recv().await.expect("leave event");
        assert_eq!(leave.envelope.kind, KIND_CHAT_EVENT);
        assert_eq!(json(&leave.envelope.body)["type"], "presence.leave");
        let (_, status, body) = recv(&mut bob_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["left"], true);

        // A social-graph revocation cleans Alice's remaining local subscription
        // immediately; no subsequent chat operation is needed to stop fan-out.
        gateway.handle_inbound(
            bob,
            &rpc(7, "friends.block", serde_json::json!({"other": "alice"})),
        );
        let (_, status, _) = recv(&mut bob_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let revoked = alice_rx.recv().await.expect("access revoked event");
        assert_eq!(revoked.envelope.kind, KIND_CHAT_EVENT);
        assert_eq!(json(&revoked.envelope.body)["type"], "access.revoked");

        gateway.handle_inbound(
            alice,
            &rpc(
                8,
                "chat.send",
                serde_json::json!({"channel_id": channel_id, "content": "not delivered"}),
            ),
        );
        let (_, status, body) = recv(&mut alice_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), "CHAT_NOT_SUBSCRIBED");
    }

    #[tokio::test]
    async fn chat_typing_is_authorized_ephemeral_and_receiver_expiring() {
        let gateway = friends_gateway();
        let (alice, mut alice_rx) = register(&gateway, Some("alice"));
        let (bob, mut bob_rx) = register(&gateway, Some("bob"));

        for (sender, request_id, other) in [(alice, 1, "bob"), (bob, 2, "alice")] {
            gateway.handle_inbound(
                sender,
                &rpc(
                    request_id,
                    "friends.add",
                    serde_json::json!({"other": other}),
                ),
            );
            let receiver = if sender == alice {
                &mut alice_rx
            } else {
                &mut bob_rx
            };
            let _ = recv(receiver).await;
        }
        gateway.handle_inbound(
            alice,
            &rpc(
                3,
                "chat.join",
                serde_json::json!({"target": {"kind": "direct", "other_user_id": "bob"}}),
            ),
        );
        let (_, status, body) = recv(&mut alice_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let channel_id = json(&body)["channel_id"]
            .as_str()
            .expect("channel id")
            .to_owned();
        gateway.handle_inbound(
            bob,
            &rpc(
                4,
                "chat.join",
                serde_json::json!({"target": {"kind": "direct", "other_user_id": "alice"}}),
            ),
        );
        let _ = alice_rx.recv().await.expect("presence event");
        let _ = recv(&mut bob_rx).await;

        gateway.handle_inbound(
            alice,
            &rpc(
                5,
                "chat.typing",
                serde_json::json!({"channel_id": channel_id, "typing": true}),
            ),
        );
        let event = bob_rx.recv().await.expect("typing event");
        assert_eq!(event.delivery, Delivery::Reliable);
        assert_eq!(event.envelope.kind, KIND_CHAT_EVENT);
        let event = json(&event.envelope.body);
        assert_eq!(event["type"], "typing");
        assert_eq!(event["typing"], true);
        assert_eq!(event["presence"]["user_id"], "alice");
        assert!(event["event_id"].is_null(), "typing is not durable");
        let expiry = event["expires_at"].as_u64().expect("expiry");
        let (_, status, body) = recv(&mut alice_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["expires_at"], expiry);
        assert!(alice_rx.try_recv().is_err(), "the sender is not echoed");

        gateway.handle_inbound(
            alice,
            &rpc(
                6,
                "chat.typing",
                serde_json::json!({"channel_id": channel_id, "typing": false}),
            ),
        );
        let stop = bob_rx.recv().await.expect("typing stop event");
        let stop = json(&stop.envelope.body);
        assert_eq!(stop["type"], "typing");
        assert_eq!(stop["typing"], false);
        assert!(stop["expires_at"].as_u64().expect("stop expiry") <= expiry);
        let _ = recv(&mut alice_rx).await;

        gateway.handle_inbound(
            bob,
            &rpc(
                7,
                "chat.typing",
                serde_json::json!({"channel_id": channel_id, "typing": true}),
            ),
        );
        let alice_event = alice_rx.recv().await.expect("authorized recipient event");
        assert_eq!(
            json(&alice_event.envelope.body)["presence"]["user_id"],
            "bob"
        );
        let _ = recv(&mut bob_rx).await;
    }

    #[tokio::test]
    async fn chat_queue_drop_requires_history_ack_before_live_delivery_resumes() {
        let (gateway, chat_repository) = friends_gateway_with_chat_repository();
        let gateway = Arc::new(gateway);
        let dispatcher = local_chat_dispatcher(&gateway, chat_repository);
        let (alice, mut alice_rx) = register(&gateway, Some("alice"));
        let (bob, mut bob_rx) = register_with_capacity(&gateway, Some("bob"), 1);
        for (sender, request_id, other) in [(alice, 1, "bob"), (bob, 2, "alice")] {
            gateway.handle_inbound(
                sender,
                &rpc(
                    request_id,
                    "friends.add",
                    serde_json::json!({"other": other}),
                ),
            );
            let receiver = if sender == alice {
                &mut alice_rx
            } else {
                &mut bob_rx
            };
            let _ = recv(receiver).await;
        }
        gateway.handle_inbound(
            alice,
            &rpc(
                3,
                "chat.join",
                serde_json::json!({"target": {"kind": "direct", "other_user_id": "bob"}}),
            ),
        );
        let (_, _, body) = recv(&mut alice_rx).await;
        let channel_id = json(&body)["channel_id"]
            .as_str()
            .expect("channel id")
            .to_owned();
        gateway.handle_inbound(
            bob,
            &rpc(
                4,
                "chat.join",
                serde_json::json!({"target": {"kind": "direct", "other_user_id": "alice"}}),
            ),
        );
        let _ = alice_rx.recv().await.expect("presence event");
        let _ = recv(&mut bob_rx).await;

        for (request_id, content) in [(5, "first"), (6, "dropped")] {
            gateway.handle_inbound(
                alice,
                &rpc(
                    request_id,
                    "chat.send",
                    serde_json::json!({"channel_id": channel_id, "content": content}),
                ),
            );
            let (_, status, _) = tokio::time::timeout(Duration::from_secs(2), recv(&mut alice_rx))
                .await
                .expect("chat.send response before deadline");
            assert_eq!(status, protocol::RPC_STATUS_OK);
            let stats = dispatch_local_chat(&dispatcher).await;
            assert_eq!(stats.loaded, 1);
            assert_eq!(stats.acknowledged, 1);
            let sender_event = recv_outbound_before_deadline(&mut alice_rx, "sender event").await;
            assert_eq!(
                json(&sender_event.envelope.body)["message"]["content"],
                content
            );
        }
        let first = recv_outbound_before_deadline(&mut bob_rx, "first queued event").await;
        assert_eq!(json(&first.envelope.body)["type"], "message.create");

        gateway.handle_inbound(
            alice,
            &rpc(
                7,
                "chat.send",
                serde_json::json!({"channel_id": channel_id, "content": "resync"}),
            ),
        );
        let (_, status, _) = tokio::time::timeout(Duration::from_secs(2), recv(&mut alice_rx))
            .await
            .expect("resync chat.send response before deadline");
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let stats = dispatch_local_chat(&dispatcher).await;
        assert_eq!(stats.loaded, 1);
        assert_eq!(stats.acknowledged, 1);
        let _ = recv_outbound_before_deadline(&mut alice_rx, "sender resync event").await;
        let resync = recv_outbound_before_deadline(&mut bob_rx, "resync event").await;
        assert_eq!(resync.envelope.kind, KIND_CHAT_EVENT);
        let resync_body = json(&resync.envelope.body);
        assert_eq!(resync_body["type"], "resync_required");
        assert_eq!(resync_body["watermark_event_id"], 3);

        gateway.handle_inbound(
            bob,
            &rpc(
                8,
                "chat.history",
                serde_json::json!({
                    "channel_id": channel_id,
                    "acknowledge_watermark": 3,
                }),
            ),
        );
        let (_, status, _) = recv(&mut bob_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);

        gateway.handle_inbound(
            alice,
            &rpc(
                9,
                "chat.send",
                serde_json::json!({"channel_id": channel_id, "content": "resumed"}),
            ),
        );
        let (_, status, _) = tokio::time::timeout(Duration::from_secs(2), recv(&mut alice_rx))
            .await
            .expect("resumed chat.send response before deadline");
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let stats = dispatch_local_chat(&dispatcher).await;
        assert_eq!(stats.loaded, 1);
        assert_eq!(stats.acknowledged, 1);
        let _ = recv_outbound_before_deadline(&mut alice_rx, "sender resumed event").await;
        let resumed = recv_outbound_before_deadline(&mut bob_rx, "resumed live event").await;
        assert_eq!(json(&resumed.envelope.body)["type"], "message.create");
        assert_eq!(
            json(&resumed.envelope.body)["message"]["content"],
            "resumed"
        );
    }

    #[tokio::test]
    async fn groups_rpc_enforces_authenticated_role_boundaries() {
        let gw = friends_gateway();
        let (alice, mut ra) = register(&gw, Some("alice"));
        let (bob, mut rb) = register(&gw, Some("bob"));

        gw.handle_inbound(
            alice,
            &rpc(
                1,
                "groups.create",
                serde_json::json!({ "name": "Raiders", "description": "test" }),
            ),
        );
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let group_id = json(&body)["id"].as_u64().expect("group id");
        assert_eq!(json(&body)["members"][0]["role"], "superadmin");

        gw.handle_inbound(
            bob,
            &rpc(
                2,
                "groups.update",
                serde_json::json!({ "group_id": group_id, "open": false }),
            ),
        );
        let (_, status, body) = recv(&mut rb).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert!(String::from_utf8_lossy(&body).contains("permission"));

        gw.handle_inbound(
            alice,
            &rpc(
                3,
                "groups.add_member",
                serde_json::json!({ "group_id": group_id, "user_id": "bob" }),
            ),
        );
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["member_count"], 2);

        gw.handle_inbound(
            bob,
            &rpc(
                4,
                "groups.leave",
                serde_json::json!({ "group_id": group_id }),
            ),
        );
        let (_, status, body) = recv(&mut rb).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["member_count"], 1);

        gw.handle_inbound(
            alice,
            &rpc(
                5,
                "groups.leave",
                serde_json::json!({ "group_id": group_id }),
            ),
        );
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert!(String::from_utf8_lossy(&body).contains("last superadmin"));
    }

    #[tokio::test]
    async fn remaining_domain_rpcs_keep_client_identity_and_persist_results() {
        let leaderboards = Arc::new(LeaderboardService::new(Arc::new(
            InMemoryLeaderboardsRepository::new(),
        )));
        leaderboards
            .create(
                CreateLeaderboardRequest {
                    id: "daily".to_owned(),
                    sort: SortOrder::Desc,
                    operator: Operator::Best,
                    reset_schedule: None,
                },
                SystemClock.now(),
            )
            .await
            .expect("board");
        let friends = Arc::new(FriendsService::new(Arc::new(
            InMemoryFriendsRepository::new(),
        )));
        let groups = Arc::new(GroupsService::new(
            Arc::new(InMemoryGroupsRepository::new()),
        ));
        friends
            .add("alice", "bob", SystemClock.now())
            .await
            .expect("invite");
        friends
            .add("bob", "alice", SystemClock.now())
            .await
            .expect("accept");
        let services = DomainRpcServices {
            chat_authorizer: Arc::new(ChatChannelAuthorizer::new(
                Arc::clone(&friends),
                Arc::clone(&groups),
            )),
            chat_rate_limits: crate::services::ChatRateLimitPolicy::default(),
            chat_presence: Arc::new(ChatPresenceRegistry::new()),
            chat_cluster_presence: None,
            node_id: "test-node".to_owned(),
            friends,
            player_notifications: Arc::new(PlayerNotificationService::new(Arc::new(
                InMemoryBackend::new(),
            ))),
            groups,
            leaderboards,
            chat: Arc::new(ChatService::new(Arc::new(InMemoryChatRepository::new()))),
            wallet: Arc::new(WalletService::new(
                Arc::new(InMemoryWalletRepository::new()),
            )),
        };

        let registry = SessionRegistry::new();
        let alice = ParticipantId::from_raw(1);
        let (status, body) = services
            .dispatch(
                alice,
                &registry,
                "leaderboards.submit",
                Some("alice"),
                None,
                br#"{"board_id":"daily","score":42}"#,
            )
            .await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["record"]["user_id"], "alice");

        let (status, body) = services
            .dispatch(
                alice,
                &registry,
                "wallet.balances",
                Some("alice"),
                None,
                br#"{}"#,
            )
            .await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&body)["balances"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn guest_caller_is_rejected() {
        let gw = friends_gateway();
        let (guest, mut rg) = register(&gw, None);

        gw.handle_inbound(guest, &rpc(7, "friends.list", serde_json::json!({})));
        let (rid, status, body) = recv(&mut rg).await;
        assert_eq!(rid, 7);
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), "authentication required");
    }

    #[tokio::test]
    async fn missing_and_malformed_arguments_error_without_touching_the_service() {
        let gw = friends_gateway();
        let (alice, mut ra) = register(&gw, Some("alice"));

        // Missing required `other`.
        gw.handle_inbound(alice, &rpc(1, "friends.add", serde_json::json!({})));
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(
            String::from_utf8_lossy(&body),
            "missing string field: other"
        );

        // Body is not JSON at all.
        let malformed = Envelope::new(
            KIND_RPC_REQUEST,
            protocol::encode_rpc_request(2, "friends.add", b"not json"),
        );
        gw.handle_inbound(alice, &malformed);
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), "invalid JSON body");
    }

    #[tokio::test]
    async fn self_friendship_surfaces_the_service_validation_error() {
        let gw = friends_gateway();
        let (alice, mut ra) = register(&gw, Some("alice"));

        gw.handle_inbound(
            alice,
            &rpc(1, "friends.add", serde_json::json!({ "other": "alice" })),
        );
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert!(
            String::from_utf8_lossy(&body).contains("cannot befriend yourself"),
            "service validation message reaches the caller"
        );
    }

    #[tokio::test]
    async fn unknown_reserved_method_errors() {
        let gw = friends_gateway();
        let (alice, mut ra) = register(&gw, Some("alice"));

        gw.handle_inbound(alice, &rpc(1, "friends.bogus", serde_json::json!({})));
        let (_, status, body) = recv(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(
            String::from_utf8_lossy(&body),
            "unknown domain method: friends.bogus"
        );
    }

    #[tokio::test]
    async fn committed_player_notification_is_delivered_once_locally_and_reconciles_inbox() {
        let backend = Arc::new(InMemoryBackend::new());
        let inbox = Arc::new(PlayerNotificationService::new(backend));
        let gateway = Arc::new(Gateway::new());
        let delivery: Arc<dyn PlayerNotificationDelivery> = gateway.clone();
        inbox.set_delivery_sink(delivery);
        let (_alice, mut receiver) = register(&gateway, Some("alice"));

        let request = crate::services::SendPlayerNotification {
            recipient: "alice".to_owned(),
            code: 7,
            subject: "reward".to_owned(),
            content: serde_json::json!({"coins": 10}),
            sender: Some("server".to_owned()),
            delivery_key: Some("reward:round-1".to_owned()),
        };
        let first = inbox
            .send(request.clone(), TimestampMillis::from_unix_millis(100))
            .await
            .expect("durable send");
        let live = receiver.recv().await.expect("local live delivery");
        assert_eq!(live.envelope.kind, KIND_NOTIFICATION);
        let delivered: PlayerNotification =
            serde_json::from_slice(&live.envelope.body).expect("notification JSON envelope");
        assert_eq!(delivered.id, first.notification.id);

        let retry = inbox
            .send(request, TimestampMillis::from_unix_millis(200))
            .await
            .expect("idempotent retry");
        assert!(retry.duplicate);
        assert!(
            receiver.try_recv().is_err(),
            "retry must not re-push live delivery"
        );
        let page = inbox.list("alice", 10, None).await.expect("inbox list");
        assert_eq!(page.items, vec![first.notification]);
    }

    #[tokio::test]
    async fn non_domain_method_falls_through_to_the_runtime_path() {
        // No script runtime attached: a non-reserved method takes the existing
        // runtime path and gets the synchronous "runtime not available" reply,
        // proving domain dispatch did not swallow it.
        let gw = friends_gateway();
        let (alice, mut ra) = register(&gw, Some("alice"));

        let sent = gw.handle_inbound(alice, &rpc(1, "ping", serde_json::json!({})));
        assert_eq!(sent, 1, "runtime path replies synchronously");
        let (rid, status, body) = recv(&mut ra).await;
        assert_eq!(rid, 1);
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), "RPC runtime not available");
    }

    fn control_identity() -> (MatchmakerControlIdentity, CertificateDer<'static>) {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("test certificate");
        let key = certificate.key_pair.serialize_der();
        let leaf = CertificateDer::from(certificate.cert);
        (
            MatchmakerControlIdentity::from_der(vec![leaf.clone()], key).expect("control identity"),
            leaf,
        )
    }

    async fn next_outbound(receiver: &mut mpsc::Receiver<Outbound>) -> Outbound {
        tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .expect("outbound message before deadline")
            .expect("outbound channel remains open")
    }

    #[tokio::test]
    async fn live_matchmaker_forwards_remote_tickets_over_mtls_and_fences_stale_admission() {
        let node_a = NodeId::new("node-a").expect("node a");
        let node_b = NodeId::new("node-b").expect("node b");
        let (identity_a, cert_a) = control_identity();
        let (identity_b, cert_b) = control_identity();
        let router_a = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_a.clone(),
                identity_a,
                BTreeMap::from([(node_b.clone(), cert_b.clone())]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router a"),
        );
        let router_b = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_b.clone(),
                identity_b,
                BTreeMap::from([(node_a.clone(), cert_a)]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router b"),
        );
        let storage: Arc<dyn crate::repository::StorageRepository> =
            Arc::new(InMemoryStorageRepository::new());
        let directory = StorageMatchmakerLeaseDirectory::new(storage);
        let now = SystemClock.now();
        let initial_lease = MatchmakerShardLease {
            shard: QueueShardId::new(0),
            owner_node: node_b.clone(),
            generation: OwnershipGeneration::new(1),
            expires_at: now
                .checked_add(DurationMillis::from_millis(500))
                .expect("lease expiry"),
        };
        directory
            .acquire(initial_lease, now)
            .await
            .expect("node b owns initial shard");
        let config_b = LiveMatchmakerConfig {
            node_id: node_b.clone(),
            shard: QueueShardId::new(0),
            lease_ttl: DurationMillis::from_millis(500),
            handoff_ttl: DurationMillis::from_millis(5_000),
            command_timeout: Duration::from_secs(2),
            directory: directory.clone(),
            router: Arc::clone(&router_b),
        };
        let config_a = LiveMatchmakerConfig {
            node_id: node_a.clone(),
            shard: QueueShardId::new(0),
            lease_ttl: DurationMillis::from_millis(500),
            handoff_ttl: DurationMillis::from_millis(5_000),
            command_timeout: Duration::from_secs(2),
            directory: directory.clone(),
            router: Arc::clone(&router_a),
        };
        let live_b = LiveMatchmakerNode::new(config_b).expect("live node b");
        let live_a = LiveMatchmakerNode::new(config_a).expect("live node a");
        live_a
            .start_listener("127.0.0.1:0".parse().expect("loopback"))
            .expect("listener a");
        live_b
            .start_listener("127.0.0.1:0".parse().expect("loopback"))
            .expect("listener b");
        let address_a = live_a.control_listener_addr().expect("listener a present");
        let address_b = live_b.control_listener_addr().expect("listener b present");
        router_a.register_endpoint(
            node_b.clone(),
            MatchmakerControlEndpoint {
                address: address_b,
                server_name: "localhost".to_owned(),
            },
        );
        router_b.register_endpoint(
            node_a.clone(),
            MatchmakerControlEndpoint {
                address: address_a,
                server_name: "localhost".to_owned(),
            },
        );
        let gateway_a = Arc::new(Gateway::new().with_live_matchmaker(Arc::clone(&live_a)));
        let gateway_b = Arc::new(Gateway::new().with_live_matchmaker(Arc::clone(&live_b)));
        gateway_a.register_live_matchmaker_endpoint();
        gateway_b.register_live_matchmaker_endpoint();
        let (alice, mut alice_rx) = register(&gateway_a, Some("alice"));
        let (bob, mut bob_rx) = register(&gateway_b, Some("bob"));
        let request = serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 });

        gateway_b.handle_inbound(bob, &rpc(1, "matchmaker.add", request.clone()));
        assert_eq!(recv(&mut bob_rx).await.1, protocol::RPC_STATUS_OK);
        gateway_a.handle_inbound(alice, &rpc(2, "matchmaker.add", request.clone()));
        let (_, status, alice_ticket_body) = recv(&mut alice_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let alice_ticket = json(&alice_ticket_body)["ticket_id"]
            .as_str()
            .expect("alice ticket")
            .to_owned();
        let alice_handoff = next_outbound(&mut alice_rx).await;
        let bob_handoff = next_outbound(&mut bob_rx).await;
        assert_eq!(alice_handoff.envelope.kind, KIND_MATCHMAKER_MATCHED);
        assert_eq!(bob_handoff.envelope.kind, KIND_MATCHMAKER_MATCHED);
        let alice_match = json(&alice_handoff.envelope.body);
        assert_eq!(alice_match["ticket_id"], alice_ticket);

        gateway_a.handle_inbound(
            alice,
            &rpc(
                3,
                "matchmaker.accept",
                serde_json::json!({
                    "ticket_id": alice_ticket,
                    "join_token": alice_match["join_token"],
                }),
            ),
        );
        assert_eq!(recv(&mut alice_rx).await.1, protocol::RPC_STATUS_OK);
        assert!(
            matches!(
                next_outbound(&mut alice_rx).await.envelope.kind,
                KIND_ROOM_JOINED
            ),
            "a relay remote acceptance emits ROOM_JOINED"
        );
        assert_eq!(gateway_b.rooms.snapshot()[0].remote_member_count, 1);

        // Relay matches retain the complete distributed-matchmaker contract.
        {
            gateway_a.handle_inbound(
                alice,
                &rpc(
                    4,
                    "matchmaker.accept",
                    serde_json::json!({
                        "ticket_id": alice_match["ticket_id"],
                        "join_token": alice_match["join_token"],
                    }),
                ),
            );
            assert_eq!(recv(&mut alice_rx).await.1, protocol::RPC_STATUS_ERROR);

            // A local party may have its indivisible ticket evaluated remotely. Its
            // members reconnect on the session node and recover the remote handoff
            // through status before each one redeems exactly once.
            let (eve, mut eve_rx) = register(&gateway_a, Some("eve"));
            let (frank, mut frank_rx) = register(&gateway_a, Some("frank"));
            let (grace, mut grace_rx) = register(&gateway_b, Some("grace"));
            gateway_a.handle_inbound(eve, &rpc(10, "party.create", serde_json::json!({})));
            let (_, status, party_body) = recv(&mut eve_rx).await;
            assert_eq!(status, protocol::RPC_STATUS_OK);
            let party_id = json(&party_body)["party_id"]
                .as_str()
                .expect("party id")
                .to_owned();
            gateway_a.handle_inbound(
                eve,
                &rpc(
                    11,
                    "party.invite",
                    serde_json::json!({ "party_id": party_id, "target_user_id": "frank" }),
                ),
            );
            assert_eq!(recv(&mut eve_rx).await.1, protocol::RPC_STATUS_OK);
            gateway_a.handle_inbound(
                frank,
                &rpc(
                    12,
                    "party.accept",
                    serde_json::json!({ "party_id": party_id }),
                ),
            );
            assert_eq!(recv(&mut frank_rx).await.1, protocol::RPC_STATUS_OK);
            let trio = serde_json::json!({ "min_count": 3, "max_count": 3, "ttl_ms": 60_000 });
            gateway_b.handle_inbound(grace, &rpc(13, "matchmaker.add", trio.clone()));
            assert_eq!(recv(&mut grace_rx).await.1, protocol::RPC_STATUS_OK);
            gateway_a.handle_inbound(
                eve,
                &rpc(
                    14,
                    "matchmaker.add",
                    serde_json::json!({
                        "party_id": party_id,
                        "min_count": 3,
                        "max_count": 3,
                        "ttl_ms": 60_000,
                    }),
                ),
            );
            let (_, status, party_ticket_body) = recv(&mut eve_rx).await;
            assert_eq!(status, protocol::RPC_STATUS_OK);
            let party_ticket = party_ticket_body.clone();
            let eve_handoff = json(&next_outbound(&mut eve_rx).await.envelope.body);
            let _ = next_outbound(&mut frank_rx).await;
            let grace_handoff = json(&next_outbound(&mut grace_rx).await.envelope.body);
            gateway_a.unregister_session(frank);
            let (frank_reconnected, mut frank_reconnected_rx) = register(&gateway_a, Some("frank"));
            gateway_a.handle_inbound(
                frank_reconnected,
                &rpc(
                    15,
                    "matchmaker.status",
                    serde_json::json!({ "ticket_id": json(&party_ticket)["ticket_id"] }),
                ),
            );
            let (_, status, frank_status) = recv(&mut frank_reconnected_rx).await;
            assert_eq!(status, protocol::RPC_STATUS_OK);
            let frank_handoff = json(&frank_status)["match"].clone();
            for (participant, receiver, handoff, request_id) in [
                (eve, &mut eve_rx, eve_handoff, 16_u64),
                (
                    frank_reconnected,
                    &mut frank_reconnected_rx,
                    frank_handoff,
                    17_u64,
                ),
            ] {
                gateway_a.handle_inbound(
                    participant,
                    &rpc(
                        request_id,
                        "matchmaker.accept",
                        serde_json::json!({
                            "ticket_id": handoff["ticket_id"],
                            "join_token": handoff["join_token"],
                        }),
                    ),
                );
                assert_eq!(
                    recv(receiver).await.1,
                    protocol::RPC_STATUS_OK,
                    "party member acceptance request {request_id} should succeed"
                );
                assert_eq!(
                    next_outbound(receiver).await.envelope.kind,
                    KIND_ROOM_JOINED
                );
            }
            gateway_b.handle_inbound(
                grace,
                &rpc(
                    18,
                    "matchmaker.accept",
                    serde_json::json!({
                        "ticket_id": grace_handoff["ticket_id"],
                        "join_token": grace_handoff["join_token"],
                    }),
                ),
            );
            assert_eq!(recv(&mut grace_rx).await.1, protocol::RPC_STATUS_OK);
            assert_eq!(
                next_outbound(&mut grace_rx).await.envelope.kind,
                KIND_ROOM_JOINED
            );
            assert!(gateway_b.rooms.snapshot().iter().any(|room| {
                room.label.mode == "matchmaker"
                    && room.members.len() == 1
                    && room.remote_member_count == 2
            }));

            let (charlie, mut charlie_rx) = register(&gateway_a, Some("charlie"));
            let (dave, mut dave_rx) = register(&gateway_b, Some("dave"));
            gateway_b.handle_inbound(dave, &rpc(5, "matchmaker.add", request.clone()));
            assert_eq!(recv(&mut dave_rx).await.1, protocol::RPC_STATUS_OK);
            gateway_a.handle_inbound(charlie, &rpc(6, "matchmaker.add", request));
            let (_, status, charlie_ticket_body) = recv(&mut charlie_rx).await;
            assert_eq!(status, protocol::RPC_STATUS_OK);
            let charlie_ticket = json(&charlie_ticket_body)["ticket_id"]
                .as_str()
                .expect("charlie ticket")
                .to_owned();
            let charlie_handoff = json(&next_outbound(&mut charlie_rx).await.envelope.body);
            let _ = next_outbound(&mut dave_rx).await;

            let current = directory
                .read(QueueShardId::new(0), SystemClock.now())
                .await
                .expect("read active b lease")
                .expect("b lease active");
            std::thread::sleep(Duration::from_millis(600));
            let takeover_now = SystemClock.now();
            directory
                .acquire(
                    MatchmakerShardLease {
                        shard: QueueShardId::new(0),
                        owner_node: node_a,
                        generation: OwnershipGeneration::new(current.generation.get() + 1),
                        expires_at: takeover_now
                            .checked_add(DurationMillis::from_millis(5_000))
                            .expect("takeover expiry"),
                    },
                    takeover_now,
                )
                .await
                .expect("durable lease transfer");
            gateway_a.handle_inbound(
                charlie,
                &rpc(
                    7,
                    "matchmaker.accept",
                    serde_json::json!({
                        "ticket_id": charlie_ticket,
                        "join_token": charlie_handoff["join_token"],
                    }),
                ),
            );
            let (_, status, _) = recv(&mut charlie_rx).await;
            assert_eq!(status, protocol::RPC_STATUS_ERROR);
            assert!(
                charlie_rx.try_recv().is_err(),
                "a stale remote acceptance must not emit ROOM_JOINED"
            );
        }
    }

    #[tokio::test]
    async fn live_matchmaker_rejects_script_bound_remote_admission_with_safe_error() {
        let node_a = NodeId::new("node-a").expect("node a");
        let node_b = NodeId::new("node-b").expect("node b");
        let (identity_a, cert_a) = control_identity();
        let (identity_b, cert_b) = control_identity();
        let router_a = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_a.clone(),
                identity_a,
                BTreeMap::from([(node_b.clone(), cert_b.clone())]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router a"),
        );
        let router_b = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_b.clone(),
                identity_b,
                BTreeMap::from([(node_a.clone(), cert_a)]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router b"),
        );
        let storage: Arc<dyn crate::repository::StorageRepository> =
            Arc::new(InMemoryStorageRepository::new());
        let directory = StorageMatchmakerLeaseDirectory::new(storage);
        let now = SystemClock.now();
        directory
            .acquire(
                MatchmakerShardLease {
                    shard: QueueShardId::new(0),
                    owner_node: node_b.clone(),
                    generation: OwnershipGeneration::new(1),
                    expires_at: now
                        .checked_add(DurationMillis::from_millis(500))
                        .expect("lease expiry"),
                },
                now,
            )
            .await
            .expect("node b owns initial shard");
        let live_a = LiveMatchmakerNode::new(LiveMatchmakerConfig {
            node_id: node_a.clone(),
            shard: QueueShardId::new(0),
            lease_ttl: DurationMillis::from_millis(500),
            handoff_ttl: DurationMillis::from_millis(5_000),
            command_timeout: Duration::from_secs(2),
            directory: directory.clone(),
            router: Arc::clone(&router_a),
        })
        .expect("live node a");
        let live_b = LiveMatchmakerNode::new(LiveMatchmakerConfig {
            node_id: node_b.clone(),
            shard: QueueShardId::new(0),
            lease_ttl: DurationMillis::from_millis(500),
            handoff_ttl: DurationMillis::from_millis(5_000),
            command_timeout: Duration::from_secs(2),
            directory: directory.clone(),
            router: Arc::clone(&router_b),
        })
        .expect("live node b");
        live_a
            .start_listener("127.0.0.1:0".parse().expect("loopback"))
            .expect("listener a");
        live_b
            .start_listener("127.0.0.1:0".parse().expect("loopback"))
            .expect("listener b");
        router_a.register_endpoint(
            node_b.clone(),
            MatchmakerControlEndpoint {
                address: live_b.control_listener_addr().expect("listener b present"),
                server_name: "localhost".to_owned(),
            },
        );
        router_b.register_endpoint(
            node_a.clone(),
            MatchmakerControlEndpoint {
                address: live_a.control_listener_addr().expect("listener a present"),
                server_name: "localhost".to_owned(),
            },
        );
        let readiness_a = Arc::new(crate::runtime::GameScriptReadiness::new(SystemClock.now()));
        let readiness_b = Arc::new(crate::runtime::GameScriptReadiness::new(SystemClock.now()));
        readiness_a.record_loaded("sha256:v1", SystemClock.now());
        readiness_b.record_loaded("sha256:v1", SystemClock.now());
        let gateway_a = Arc::new(
            Gateway::new()
                .with_script_readiness(Arc::clone(&readiness_a))
                .with_live_matchmaker(Arc::clone(&live_a)),
        );
        let gateway_b = Arc::new(
            Gateway::new()
                .with_script_readiness(Arc::clone(&readiness_b))
                .with_live_matchmaker(Arc::clone(&live_b)),
        );
        gateway_a.register_live_matchmaker_endpoint();
        gateway_b.register_live_matchmaker_endpoint();
        let (alice, mut alice_rx) = register(&gateway_a, Some("alice"));
        let (bob, mut bob_rx) = register(&gateway_b, Some("bob"));
        let request = serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 });

        gateway_b.handle_inbound(bob, &rpc(1, "matchmaker.add", request.clone()));
        assert_eq!(recv(&mut bob_rx).await.1, protocol::RPC_STATUS_OK);
        gateway_a.handle_inbound(alice, &rpc(2, "matchmaker.add", request));
        let (_, status, alice_ticket_body) = recv(&mut alice_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let alice_handoff = json(&next_outbound(&mut alice_rx).await.envelope.body);
        let _ = next_outbound(&mut bob_rx).await;

        gateway_a.handle_inbound(
            alice,
            &rpc(
                3,
                "matchmaker.accept",
                serde_json::json!({
                    "ticket_id": json(&alice_ticket_body)["ticket_id"],
                    "join_token": alice_handoff["join_token"],
                }),
            ),
        );
        let (_, status, body) = recv(&mut alice_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(
            String::from_utf8_lossy(&body),
            REMOTE_AUTHORITATIVE_ADMISSION_UNAVAILABLE_MESSAGE
        );
        assert!(
            alice_rx.try_recv().is_err(),
            "a refused authoritative admission cannot emit ROOM_JOINED"
        );
        assert_eq!(gateway_b.rooms.snapshot()[0].remote_member_count, 0);
    }

    #[tokio::test]
    async fn durable_party_two_gateways_forward_fence_restart_and_cancel_stale_ticket() {
        let node_a = NodeId::new("party-node-a").expect("node a");
        let node_b = NodeId::new("party-node-b").expect("node b");
        let (identity_a, cert_a) = control_identity();
        let (identity_b, cert_b) = control_identity();
        let router_a = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_a.clone(),
                identity_a,
                BTreeMap::from([(node_b.clone(), cert_b)]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router a"),
        );
        let router_b = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_b.clone(),
                identity_b,
                BTreeMap::from([(node_a.clone(), cert_a)]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router b"),
        );
        let storage: Arc<dyn crate::repository::StorageRepository> =
            Arc::new(InMemoryStorageRepository::new());
        let matchmaker_directory = StorageMatchmakerLeaseDirectory::new(Arc::clone(&storage));
        let party_directory = Arc::new(StoragePartyDirectory::new(Arc::clone(&storage)));
        let now = SystemClock.now();
        matchmaker_directory
            .acquire(
                MatchmakerShardLease {
                    shard: QueueShardId::new(0),
                    owner_node: node_b.clone(),
                    generation: OwnershipGeneration::new(1),
                    expires_at: now
                        .checked_add(DurationMillis::from_millis(5_000))
                        .expect("expiry"),
                },
                now,
            )
            .await
            .expect("b owns shard");
        let party_id = PartyId::parse("party-two-gateway").expect("party id");
        let created_at = SystemClock.now();
        let (_, stale_party_lease) = party_block_on({
            let directory = Arc::clone(&party_directory);
            let party_id = party_id.clone();
            let node_b = node_b.clone();
            async move {
                directory
                    .create(
                        party_id,
                        "alice",
                        node_b,
                        created_at
                            .checked_add(DurationMillis::from_millis(5_000))
                            .expect("expiry"),
                        created_at,
                    )
                    .await
            }
        })
        .expect("durable party created on b");
        let live_a = LiveMatchmakerNode::new(LiveMatchmakerConfig {
            node_id: node_a.clone(),
            shard: QueueShardId::new(0),
            lease_ttl: DurationMillis::from_millis(5_000),
            handoff_ttl: DurationMillis::from_millis(5_000),
            command_timeout: Duration::from_secs(2),
            directory: matchmaker_directory.clone(),
            router: Arc::clone(&router_a),
        })
        .expect("live a");
        let live_b = LiveMatchmakerNode::new(LiveMatchmakerConfig {
            node_id: node_b.clone(),
            shard: QueueShardId::new(0),
            lease_ttl: DurationMillis::from_millis(5_000),
            handoff_ttl: DurationMillis::from_millis(5_000),
            command_timeout: Duration::from_secs(2),
            directory: matchmaker_directory,
            router: Arc::clone(&router_b),
        })
        .expect("live b");
        live_a
            .start_listener("127.0.0.1:0".parse().expect("loopback"))
            .expect("listener a");
        live_b
            .start_listener("127.0.0.1:0".parse().expect("loopback"))
            .expect("listener b");
        router_a.register_endpoint(
            node_b.clone(),
            MatchmakerControlEndpoint {
                address: live_b.control_listener_addr().expect("b address"),
                server_name: "localhost".to_owned(),
            },
        );
        router_b.register_endpoint(
            node_a.clone(),
            MatchmakerControlEndpoint {
                address: live_a.control_listener_addr().expect("a address"),
                server_name: "localhost".to_owned(),
            },
        );
        let gateway_a = Arc::new(
            Gateway::new()
                .with_live_matchmaker(Arc::clone(&live_a))
                .with_storage_party_directory(
                    Arc::clone(&party_directory),
                    node_a.clone(),
                    Arc::clone(&router_a),
                ),
        );
        let gateway_b = Arc::new(
            Gateway::new()
                .with_live_matchmaker(Arc::clone(&live_b))
                .with_storage_party_directory(
                    Arc::clone(&party_directory),
                    node_b.clone(),
                    Arc::clone(&router_b),
                ),
        );
        gateway_a.register_live_matchmaker_endpoint();
        gateway_b.register_live_matchmaker_endpoint();
        gateway_a.register_party_directory_endpoint();
        gateway_b.register_party_directory_endpoint();
        let (alice, mut alice_rx) = register(&gateway_a, Some("alice"));
        let (_bob, _bob_rx) = register(&gateway_b, Some("bob"));

        // Gateway A forwards to B's durable party owner over the real mTLS listener.
        gateway_a.handle_inbound(
            alice,
            &rpc(
                1,
                "party.invite",
                serde_json::json!({"party_id": party_id.as_str(), "target_user_id": "bob"}),
            ),
        );
        assert_eq!(recv(&mut alice_rx).await.1, protocol::RPC_STATUS_OK);
        // Retrying the same remote mutation is idempotent at B's owner.
        gateway_a.handle_inbound(
            alice,
            &rpc(
                1,
                "party.invite",
                serde_json::json!({"party_id": party_id.as_str(), "target_user_id": "bob"}),
            ),
        );
        assert_eq!(json(&recv(&mut alice_rx).await.2)["revision"], 2);

        // Queue admission is a mutation. A must not write ticket_freeze using
        // B's lease locally: it sends the typed command to B and verifies the
        // returned owner fence before using the committed member snapshot.
        let admission = gateway_a
            .durable_party_queue_snapshot(
                gateway_a.durable_parties.as_ref().expect("durable parties"),
                party_id.clone(),
                "alice",
                SystemClock
                    .now()
                    .checked_add(DurationMillis::from_millis(60_000))
                    .expect("expiry"),
                SystemClock.now(),
            )
            .expect("remote owner queue admission");
        assert_eq!(admission.0, vec!["alice".to_owned()]);
        assert_eq!(admission.1.revision, 2);
        assert!(
            gateway_a
                .durable_party_queue_snapshot(
                    gateway_a.durable_parties.as_ref().expect("durable parties"),
                    party_id.clone(),
                    "alice",
                    SystemClock
                        .now()
                        .checked_add(DurationMillis::from_millis(60_000))
                        .expect("expiry"),
                    SystemClock.now(),
                )
                .is_err()
        );

        // Advance the directory's explicit logical clock past the lease rather
        // than sleeping and hoping the mTLS listener was ready in time.
        let takeover_now = created_at
            .checked_add(DurationMillis::from_millis(5_001))
            .expect("logical takeover time");
        let takeover = party_block_on({
            let directory = Arc::clone(&party_directory);
            let party_id = party_id.clone();
            let node_a = node_a.clone();
            async move {
                directory
                    .acquire_or_resolve(
                        &party_id,
                        node_a,
                        takeover_now
                            .checked_add(DurationMillis::from_millis(5_000))
                            .expect("expiry"),
                        takeover_now,
                    )
                    .await
            }
        })
        .expect("takeover");
        assert!(
            matches!(takeover, PartyOwnerResolution::Local(ref lease) if lease.generation > stale_party_lease.generation)
        );
        assert!(matches!(
            router_a.party_command(
                &node_b,
                PartyControlCommand {
                    party_id: party_id.clone(),
                    lease: stale_party_lease,
                    actor: "alice".to_owned(),
                    request_id: "stale".to_owned(),
                    expected_revision: 2,
                    operation: PartyControlOperation::Close,
                }
            ),
            Ok(PartyControlReply::StaleOwnerFence)
        ));

        // A fresh Gateway instance proves recovery is from the shared durable store, not memory.
        let restarted_a = Arc::new(Gateway::new().with_storage_party_directory(
            Arc::clone(&party_directory),
            node_a,
            Arc::clone(&router_a),
        ));
        restarted_a.register_party_directory_endpoint();
        let (restarted_alice, mut restarted_alice_rx) = register(&restarted_a, Some("alice"));
        restarted_a.handle_inbound(
            restarted_alice,
            &rpc(
                3,
                "party.status",
                serde_json::json!({"party_id": party_id.as_str()}),
            ),
        );
        let (_, status, recovered) = recv(&mut restarted_alice_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        assert_eq!(json(&recovered)["revision"], 2);
        assert_eq!(
            party_directory
                .snapshot(&party_id)
                .await
                .expect("recovered snapshot")
                .leader_user_id,
            "alice"
        );

        // The live shard owner is B. It receives a deliberately stale snapshot from A,
        // revalidates it after forwarding, and cancels/refuses the asynchronous ticket.
        assert!(live_a.submit_from_session(
            alice,
            99,
            vec![
                RemoteMatchmakerTicketOwner {
                    user_id: "alice".to_owned(),
                    session_node: NodeId::new("party-node-a").expect("node"),
                },
                RemoteMatchmakerTicketOwner {
                    user_id: "bob".to_owned(),
                    session_node: NodeId::new("party-node-b").expect("node"),
                },
            ],
            TicketRequest {
                query: String::new(),
                properties: BTreeMap::new(),
                min_count: 2,
                max_count: 2,
                count_multiple: 1,
                ttl_ms: 60_000,
                party_id: None,
            },
            Some(PartyAdmissionFence {
                party_id: party_id.as_str().to_owned(),
                leader_user_id: "alice".to_owned(),
                revision: 1,
                owner_generation: 1,
                admission_generation: 1,
                admission_token: 1,
            }),
        ));
        let (_, status, body) = recv(&mut alice_rx).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(
            String::from_utf8_lossy(&body),
            "matchmaker shard is unavailable"
        );
    }

    #[tokio::test]
    async fn owner_failover_emits_one_resync_then_snapshot_before_mutation() {
        let node_a = NodeId::new("failover-node-a").expect("node a");
        let node_b = NodeId::new("failover-node-b").expect("node b");
        let (_identity_a, cert_a) = control_identity();
        let (identity_b, _cert_b) = control_identity();
        let router_b = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_b.clone(),
                identity_b,
                BTreeMap::from([(node_a.clone(), cert_a)]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router b"),
        );
        let storage: Arc<dyn crate::repository::StorageRepository> =
            Arc::new(InMemoryStorageRepository::new());
        let directory = Arc::new(StoragePartyDirectory::new(storage));
        let party_id = PartyId::parse("party-owner-failover").expect("party id");
        let created_at = SystemClock.now();
        let initial_expiry = created_at
            .checked_add(DurationMillis::from_millis(60_000))
            .expect("initial expiry");
        let invite_at = created_at
            .checked_add(DurationMillis::from_millis(1))
            .expect("invite time");
        let takeover_at = created_at
            .checked_add(DurationMillis::from_millis(60_001))
            .expect("takeover time");
        let (created, stale_lease) = directory
            .create(
                party_id.clone(),
                "alice",
                node_a,
                initial_expiry,
                created_at,
            )
            .await
            .expect("initial owner");
        let invited = directory
            .invite(
                &stale_lease,
                "alice",
                "invite-bob",
                "bob",
                created.revision,
                invite_at,
            )
            .await
            .expect("committed membership before owner loss");
        let before_takeover = directory
            .accept(
                &stale_lease,
                "bob",
                "accept-bob",
                invited.revision,
                invite_at
                    .checked_add(DurationMillis::from_millis(1))
                    .expect("accept time"),
            )
            .await
            .expect("bob joins before owner loss");
        let recovery_lease = match directory
            .acquire_or_resolve(
                &party_id,
                node_b.clone(),
                takeover_at
                    .checked_add(DurationMillis::from_millis(60_000))
                    .expect("recovery expiry"),
                takeover_at,
            )
            .await
            .expect("durable higher-generation takeover")
        {
            PartyOwnerResolution::Local(lease) => Some(lease),
            PartyOwnerResolution::Remote(_) => None,
        }
        .expect("expired owner must be replaced");
        assert!(recovery_lease.generation > stale_lease.generation);

        let metrics = Arc::new(NodeMetrics::new());
        let gateway = Gateway::with_metrics(Arc::clone(&metrics)).with_storage_party_directory(
            Arc::clone(&directory),
            node_b,
            router_b,
        );
        let (_alice, mut alice_rx) = register(&gateway, Some("alice"));
        let (_bob, mut bob_rx) = register(&gateway, Some("bob"));

        let stale = PartyControlCommand {
            party_id: party_id.clone(),
            lease: stale_lease,
            actor: "alice".to_owned(),
            request_id: "stale-owner".to_owned(),
            expected_revision: before_takeover.revision,
            operation: PartyControlOperation::Promote {
                target: "bob".to_owned(),
            },
        };
        assert!(
            matches!(
                gateway.apply_remote_party_command(stale),
                PartyControlReply::StaleOwnerFence
            ),
            "the replacement endpoint rejects a delayed old-owner command"
        );

        let command = PartyControlCommand {
            party_id: party_id.clone(),
            lease: recovery_lease.clone(),
            actor: "alice".to_owned(),
            request_id: "promote-after-recovery".to_owned(),
            expected_revision: before_takeover.revision,
            operation: PartyControlOperation::Promote {
                target: "bob".to_owned(),
            },
        };
        let result = gateway.apply_remote_party_command(command.clone());
        let (mutated, reply_lease) = match result {
            PartyControlReply::Snapshot(snapshot, lease) => Some((snapshot, lease)),
            PartyControlReply::StaleOwnerFence
            | PartyControlReply::QueueAdmission(_)
            | PartyControlReply::Rejected => None,
        }
        .expect("recovered mutation must commit once");
        assert_eq!(reply_lease, recovery_lease);
        assert_eq!(mutated.revision, before_takeover.revision + 1);

        // Every current member receives the recovery barrier and committed
        // pre-mutation snapshot before the first recovered mutation can alter
        // the revision. This is deterministic and does not rely on a sleep.
        for receiver in [&mut alice_rx, &mut bob_rx] {
            let resync = receiver.recv().await.expect("resync delivered");
            let resync = json(&resync.envelope.body);
            assert_eq!(resync["type"], "party.resync_required");
            assert_eq!(resync["party_revision"], before_takeover.revision);
            assert_eq!(resync["generation"], recovery_lease.generation.get());
            let snapshot = receiver.recv().await.expect("snapshot delivered");
            let snapshot = json(&snapshot.envelope.body);
            assert_eq!(snapshot["type"], "party.snapshot");
            assert_eq!(snapshot["revision"], before_takeover.revision);
        }

        // Retrying the same owner command gets the durable idempotent result;
        // the restart-safe recovery marker suppresses a second transition.
        assert!(matches!(
            gateway.apply_remote_party_command(command),
            PartyControlReply::Snapshot(snapshot, lease)
                if snapshot == mutated && lease == recovery_lease
        ));
        assert!(alice_rx.try_recv().is_err());
        assert!(bob_rx.try_recv().is_err());
        let metrics = metrics.snapshot();
        assert_eq!(metrics.party_owner_lease_acquire_total, 1);
        assert_eq!(metrics.party_owner_failover_total, 1);
        assert_eq!(metrics.party_resync_total, 1);
        assert_eq!(metrics.party_owner_stale_reject_total, 1);
    }
}

/// The GameScript readiness-gate acceptance suite: with a snapshot that is not
/// `Ready`, nothing lists, creates, or admits on any enforcement surface; every
/// rejection carries the one stable client-safe message; matches are born bound
/// to the gating snapshot's `(revision, generation)`; and `require_script = false`
/// (no gate attached) behavior is unchanged.
#[cfg(test)]
mod script_gate_tests {
    #![allow(clippy::unwrap_used)]

    use std::time::Duration;

    use super::*;
    use crate::matchmaker_cluster::{MatchmakerHandoffRouter, QueueShardId};
    use crate::realtime::registry::{ParticipantIdentity, SessionHandle};
    use crate::runtime::{GameScriptReadiness, RoomSpec};
    use crate::session::SessionId;
    use crate::storage::UserId;
    use crate::transport::TransportKind;
    use citadel_wire::protocol::KIND_ROOM_REJECT;
    use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined, RoomReject};
    use tokio::sync::mpsc;

    fn now() -> TimestampMillis {
        SystemClock.now()
    }

    /// A fresh readiness authority in its boot (`NoScript`, not-ready) state.
    fn boot_readiness() -> Arc<GameScriptReadiness> {
        Arc::new(GameScriptReadiness::new(now()))
    }

    /// A gateway with the readiness gate attached (the `require_script` shape).
    fn gated_gateway(readiness: &Arc<GameScriptReadiness>) -> Gateway {
        Gateway::with_metrics(Arc::new(NodeMetrics::new()))
            .with_script_readiness(Arc::clone(readiness))
    }

    fn register(gw: &Gateway, user: Option<&str>) -> (ParticipantId, mpsc::Receiver<Outbound>) {
        let id = gw.next_participant_id();
        let (tx, rx) = mpsc::channel(16);
        let identity = user.map(|user| ParticipantIdentity {
            user_id: UserId::new(user).expect("user id"),
            session_id: SessionId::new(format!("session-{user}")).expect("session id"),
            expires_at: TimestampMillis::from_unix_millis(9_999_999_999),
        });
        gw.registry().register(SessionHandle {
            id,
            kind: TransportKind::WebSocket,
            outbound: tx,
            identity,
        });
        (id, rx)
    }

    fn rpc(request_id: u64, method: &str, body: serde_json::Value) -> Envelope {
        let payload = body.to_string().into_bytes();
        Envelope::new(
            KIND_RPC_REQUEST,
            protocol::encode_rpc_request(request_id, method, &payload),
        )
    }

    async fn recv_rpc(rx: &mut mpsc::Receiver<Outbound>) -> (u8, Vec<u8>) {
        let out = rx.recv().await.expect("rpc response delivered");
        assert_eq!(out.envelope.kind, KIND_RPC_RESPONSE);
        let resp = protocol::decode_rpc_response(&out.envelope.body).expect("decodes");
        (resp.status, resp.payload.to_vec())
    }

    fn json(payload: &[u8]) -> serde_json::Value {
        serde_json::from_slice(payload).expect("json body")
    }

    fn room_create(name: &[u8]) -> Envelope {
        Envelope::new(
            KIND_ROOM_CREATE,
            RoomCreate {
                params: name.to_vec(),
            }
            .encode(),
        )
    }

    fn room_join(room_id: RoomId) -> Envelope {
        Envelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode())
    }

    struct UnsupportedNativeLifecycleRuntime;

    impl Runtime for UnsupportedNativeLifecycleRuntime {
        fn dispatch(&self, _: u64, _: Option<&str>, _: u16, _: &[u8]) -> Vec<OutboundCommand> {
            Vec::new()
        }

        fn dispatch_lifecycle(
            &self,
            _: LifecycleHook,
            _: u64,
            _: Option<&str>,
        ) -> Vec<OutboundCommand> {
            Vec::new()
        }

        fn tick(&self, _: Duration, _: Duration) -> Vec<OutboundCommand> {
            Vec::new()
        }

        fn call_rpc(&self, _: u64, _: Option<&str>, _: &str, _: &[u8]) -> RpcOutcome {
            RpcOutcome::Err("unused".to_owned())
        }

        fn call_room_create(&self, _: u64, _: Option<&str>, _: &[u8]) -> Option<RoomSpec> {
            None
        }

        fn call_room_join(&self, _: u64, _: Option<&str>, _: u64) -> bool {
            false
        }

        fn has_tick_handler(&self) -> bool {
            false
        }

        fn budget(&self) -> Duration {
            Duration::from_millis(1)
        }

        fn introspect(&self) -> crate::runtime::RuntimeIntrospection {
            crate::runtime::RuntimeIntrospection {
                source: "unsupported-native-lifecycle".to_owned(),
                reloadable: false,
                deadline_ms: 1,
                rpcs: Vec::new(),
                message_kinds: Vec::new(),
                hooks: Vec::new(),
            }
        }
    }

    /// Assert the next frame is the stable, client-safe policy rejection.
    async fn expect_room_reject(rx: &mut mpsc::Receiver<Outbound>, request_kind: u16) {
        let out = rx.recv().await.expect("reject reply delivered");
        assert_eq!(out.envelope.kind, KIND_ROOM_REJECT, "policy reject frame");
        let reject = RoomReject::decode(&out.envelope.body).expect("decodes");
        assert_eq!(reject.request_kind, request_kind);
        assert_eq!(reject.reason, SCRIPT_UNAVAILABLE_MESSAGE);
    }

    #[tokio::test]
    async fn unsupported_native_lifecycle_refuses_local_and_remote_handoffs_before_mutation() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:unsupported", now());
        let gw = Gateway::with_metrics_and_runtime(
            Arc::new(NodeMetrics::new()),
            Some(Arc::new(UnsupportedNativeLifecycleRuntime)),
        )
        .with_script_readiness(readiness);
        let (alice, mut replies) = register(&gw, Some("alice"));

        assert_eq!(
            gw.live_matchmaker_finish_local_accept(alice, 1, 1),
            Err(()),
            "local matchmaker admission must not mutate an unsupported match"
        );
        let (status, body) = recv_rpc(&mut replies).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(
            String::from_utf8_lossy(&body),
            NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE
        );

        assert_eq!(
            gw.live_matchmaker_finish_remote_accept(alice, 2, 2),
            Err(()),
            "remote handoff completion must not bind an unsupported match"
        );
        let (status, body) = recv_rpc(&mut replies).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(
            String::from_utf8_lossy(&body),
            NATIVE_MATCH_LIFECYCLE_UNAVAILABLE_MESSAGE
        );
        assert!(gw.rooms().snapshot().is_empty());
        assert!(
            gw.rep_rooms
                .lock()
                .expect("replication bindings lock")
                .connections
                .is_empty(),
            "neither refusal may leave a room or remote replication binding"
        );
    }

    // ---- Surface 1: KIND_ROOM_CREATE ------------------------------------

    #[tokio::test]
    async fn gate_01_room_create_refuses_with_visible_stable_reject() {
        let readiness = boot_readiness();
        let gw = gated_gateway(&readiness);
        let (a, mut ra) = register(&gw, Some("alice"));
        let sent = gw.handle_inbound(a, &room_create(b"lobby"));
        assert_eq!(sent, 1, "the rejection is visible, not a silent drop");
        expect_room_reject(&mut ra, KIND_ROOM_CREATE).await;
        assert_eq!(gw.rooms().room_count(), 0, "no placeholder room is born");
        assert_eq!(
            gw.metrics.snapshot().script_gate_rejections.room_create,
            1,
            "the surface counted its rejection"
        );
    }

    // ---- Surface 2: KIND_ROOM_JOIN --------------------------------------

    #[tokio::test]
    async fn gate_02_room_join_refuses_when_not_ready() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:v1", now());
        let gw = gated_gateway(&readiness);
        let (a, mut ra) = register(&gw, Some("alice"));
        gw.handle_inbound(a, &room_create(b"lobby"));
        let joined = RoomJoined::decode(&ra.recv().await.expect("joined").envelope.body)
            .expect("room joined");

        readiness.record_degraded(now());
        let (b, mut rb) = register(&gw, Some("bob"));
        let sent = gw.handle_inbound(b, &room_join(joined.room_id));
        assert_eq!(sent, 1);
        expect_room_reject(&mut rb, KIND_ROOM_JOIN).await;
        assert_eq!(
            gw.rooms().members(joined.room_id),
            vec![a],
            "nobody was admitted while degraded"
        );
        assert_eq!(gw.metrics.snapshot().script_gate_rejections.room_join, 1);
    }

    // ---- Surface 3: matchmaker.add (queueing) ---------------------------

    #[tokio::test]
    async fn gate_03_matchmaker_add_refuses_with_the_stable_error() {
        let readiness = boot_readiness();
        let gw = gated_gateway(&readiness);
        let (alice, mut ra) = register(&gw, Some("alice"));
        gw.handle_inbound(
            alice,
            &rpc(
                1,
                "matchmaker.add",
                serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 }),
            ),
        );
        let (status, body) = recv_rpc(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), SCRIPT_UNAVAILABLE_MESSAGE);
        assert_eq!(gw.matchmaker_stats().queued_tickets, 0, "nothing queued");
        assert_eq!(
            gw.metrics
                .snapshot()
                .script_gate_rejections
                .matchmaker_queue,
            1
        );
    }

    // ---- Surface 4: local matchmaker activation (matchmaker_tick) -------

    #[tokio::test]
    async fn gate_04_activation_holds_queued_tickets_until_ready() {
        let readiness = boot_readiness();
        let gw = gated_gateway(&readiness);
        let (alice, mut ra) = register(&gw, Some("alice"));
        let (bob, mut rb) = register(&gw, Some("bob"));
        // Tickets queued out-of-band (as if queued before readiness was lost):
        // the activation surface must still fail closed on its own.
        let request: TicketRequest = serde_json::from_value(
            serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 }),
        )
        .expect("request");
        let t = now();
        for (participant, user) in [(alice, "alice"), (bob, "bob")] {
            let ticket = gw
                .matchmaker
                .add_party(participant, vec![participant], request.clone(), t)
                .expect("queued");
            gw.remember_ticket_owners(
                ticket,
                vec![QueuedTicketOwner {
                    user_id: user.to_owned(),
                    participant,
                }],
            );
        }

        assert_eq!(gw.matchmaker_tick(), 0, "no handoffs while not ready");
        assert_eq!(gw.rooms().room_count(), 0, "no match room is born");
        assert!(ra.try_recv().is_err(), "alice got no MATCHMAKER_MATCHED");
        assert!(rb.try_recv().is_err(), "bob got no MATCHMAKER_MATCHED");
        assert_eq!(gw.matchmaker_stats().queued_tickets, 2, "tickets held");
        assert!(
            gw.metrics
                .snapshot()
                .script_gate_rejections
                .matchmaker_activate
                >= 1
        );

        // Boot-not-ready then Ready: the held cohort forms once a script loads.
        readiness.record_loaded("sha256:v1", now());
        assert_eq!(gw.matchmaker_tick(), 2, "both members receive handoffs");
        assert_eq!(gw.rooms().room_count(), 1);
        let room = &gw.rooms().snapshot()[0];
        assert_eq!(
            room.script_binding,
            Some(ScriptBinding {
                revision_id: "sha256:v1".to_owned(),
                generation: 1,
            }),
            "the match is born bound to the gating snapshot"
        );
    }

    // ---- Surface 5: matchmaker.accept -----------------------------------

    #[tokio::test]
    async fn gate_05_accept_refuses_when_not_ready_and_when_stale() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:v1", now());
        let gw = gated_gateway(&readiness);
        let (alice, mut ra) = register(&gw, Some("alice"));
        let (bob, mut rb) = register(&gw, Some("bob"));
        let request = serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 });
        gw.handle_inbound(alice, &rpc(1, "matchmaker.add", request.clone()));
        let (status, body) = recv_rpc(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_OK);
        let ticket = json(&body)["ticket_id"]
            .as_str()
            .expect("ticket")
            .to_owned();
        gw.handle_inbound(bob, &rpc(2, "matchmaker.add", request));
        let _ = recv_rpc(&mut rb).await;
        let handoff = json(&ra.recv().await.expect("matched").envelope.body);
        let _ = rb.recv().await.expect("bob matched");
        let accept = |request_id: u64| {
            rpc(
                request_id,
                "matchmaker.accept",
                serde_json::json!({
                    "ticket_id": ticket,
                    "join_token": handoff["join_token"],
                }),
            )
        };

        // Not ready: trusted admission is refused with the stable message.
        readiness.record_degraded(now());
        gw.handle_inbound(alice, &accept(3));
        let (status, body) = recv_rpc(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), SCRIPT_UNAVAILABLE_MESSAGE);
        assert!(
            gw.rooms().snapshot()[0].members.is_empty(),
            "no admission while degraded"
        );
        assert_eq!(
            gw.metrics
                .snapshot()
                .script_gate_rejections
                .matchmaker_accept,
            1
        );

        // Recovered under a NEW load: the room's bound revision is superseded,
        // so admission into the stale match is refused with the same message.
        readiness.record_loaded("sha256:v2", now());
        gw.handle_inbound(alice, &accept(4));
        let (status, body) = recv_rpc(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), SCRIPT_UNAVAILABLE_MESSAGE);
        assert!(gw.rooms().snapshot()[0].members.is_empty());
    }

    // ---- Surface 6: live matchmaker formation (room birth) --------------

    #[tokio::test]
    async fn gate_06_live_form_refuses_room_birth() {
        let readiness = boot_readiness();
        let gw = gated_gateway(&readiness);
        assert_eq!(gw.live_matchmaker_create_room(2), Err(()));
        assert_eq!(gw.rooms().room_count(), 0, "no room is born while closed");
        assert_eq!(gw.metrics.snapshot().script_gate_rejections.live_form, 1);

        readiness.record_loaded("sha256:v1", now());
        let room = gw.live_matchmaker_create_room(2).expect("ready opens");
        assert_eq!(
            gw.rooms().binding(room),
            Some(ScriptBinding {
                revision_id: "sha256:v1".to_owned(),
                generation: 1,
            })
        );
    }

    // ---- Surface 7: live acceptance into a locally owned match ----------

    #[tokio::test]
    async fn gate_07_live_accept_local_refuses() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:v1", now());
        let gw = gated_gateway(&readiness);
        let room = gw.live_matchmaker_create_room(2).expect("room");
        let (alice, mut ra) = register(&gw, Some("alice"));

        readiness.record_degraded(now());
        assert_eq!(
            gw.live_matchmaker_finish_local_accept(alice, 7, room),
            Err(())
        );
        let (status, body) = recv_rpc(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), SCRIPT_UNAVAILABLE_MESSAGE);
        assert!(ra.try_recv().is_err(), "no ROOM_JOINED follows a refusal");
        assert!(gw.rooms().members(room).is_empty());
        assert_eq!(
            gw.metrics
                .snapshot()
                .script_gate_rejections
                .live_accept_local,
            1
        );
    }

    // ---- Surface 8: live acceptance completion for a remote match -------

    #[tokio::test]
    async fn gate_08_live_accept_remote_refuses() {
        let readiness = boot_readiness();
        let gw = gated_gateway(&readiness);
        let (alice, mut ra) = register(&gw, Some("alice"));
        assert_eq!(
            gw.live_matchmaker_finish_remote_accept(alice, 8, 42),
            Err(())
        );
        let (status, body) = recv_rpc(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(String::from_utf8_lossy(&body), SCRIPT_UNAVAILABLE_MESSAGE);
        assert!(
            ra.try_recv().is_err(),
            "no accepted/ROOM_JOINED frames while closed"
        );
        assert_eq!(
            gw.metrics
                .snapshot()
                .script_gate_rejections
                .live_accept_remote,
            1
        );
    }

    #[tokio::test]
    async fn authoritative_live_remote_admission_fails_closed_with_safe_error() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:v1", now());
        let gw = gated_gateway(&readiness).with_rep_authority(Arc::new(
            crate::realtime::netpeer::RepAuthority::new(Default::default()),
        ));
        let (alice, mut ra) = register(&gw, Some("alice"));

        assert_eq!(
            gw.live_matchmaker_finish_remote_accept(alice, 8, 42),
            Err(()),
            "a remote owner cannot be confirmed without scoped state routing"
        );

        assert_eq!(
            gw.rep_rooms
                .lock()
                .expect("replication bindings lock")
                .connections
                .get(&alice)
                .copied(),
            None,
            "a refused remote admission cannot leave a replication binding"
        );
        let (status, body) = recv_rpc(&mut ra).await;
        assert_eq!(status, protocol::RPC_STATUS_ERROR);
        assert_eq!(
            String::from_utf8_lossy(&body),
            REMOTE_AUTHORITATIVE_ADMISSION_UNAVAILABLE_MESSAGE,
            "the requester receives an explainable, client-safe admission failure"
        );
        assert!(
            ra.try_recv().is_err(),
            "no ROOM_JOINED or baseline is emitted"
        );
    }

    #[tokio::test]
    async fn live_local_admission_binds_and_bootstraps_before_room_joined() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:v1", now());
        let gw = gated_gateway(&readiness).with_rep_authority(Arc::new(
            crate::realtime::netpeer::RepAuthority::new(Default::default()),
        ));
        let room_id = gw
            .live_matchmaker_create_room(2)
            .expect("ready match forms");
        let (alice, mut ra) = register(&gw, Some("alice"));

        gw.live_matchmaker_finish_local_accept(alice, 9, room_id)
            .expect("ready local admission succeeds");

        assert_eq!(gw.rooms().room_of(alice), Some(room_id));
        assert_eq!(
            gw.rep_rooms
                .lock()
                .expect("replication bindings lock")
                .connections
                .get(&alice)
                .copied(),
            Some(room_id),
            "room and replication binding commit together"
        );
        let _ = recv_rpc(&mut ra).await;
        assert_eq!(
            ra.recv().await.expect("room joined").envelope.kind,
            KIND_ROOM_JOINED
        );
        assert_eq!(
            ra.recv().await.expect("replication schema").envelope.kind,
            citadel_wire::protocol::KIND_REP_SCHEMA,
            "the newly admitted room receives its trusted baseline"
        );
    }

    // ---- Surface 9: live fenced remote admission on the owner node ------

    #[tokio::test]
    async fn gate_09_live_admit_remote_refuses_on_owner() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:v1", now());
        let gw = gated_gateway(&readiness);
        let room = gw.live_matchmaker_create_room(2).expect("room");
        let node_b = NodeId::new("node-b").expect("node id");

        readiness.record_degraded(now());
        assert_eq!(
            gw.live_matchmaker_admit_remote(node_b.clone(), "bob".to_owned(), room),
            Err(())
        );
        assert_eq!(gw.rooms().snapshot()[0].remote_member_count, 0);
        assert_eq!(
            gw.metrics
                .snapshot()
                .script_gate_rejections
                .live_admit_remote,
            1
        );

        // A recovery under a NEW load supersedes the room: still refused.
        readiness.record_loaded("sha256:v2", now());
        assert_eq!(
            gw.live_matchmaker_admit_remote(node_b, "bob".to_owned(), room),
            Err(())
        );
        assert_eq!(gw.rooms().snapshot()[0].remote_member_count, 0);
    }

    // ---- Surface 10 (charter test 17): cluster NodeCommand::AdmitRemote -

    #[tokio::test]
    async fn gate_17_cluster_admit_remote_refuses_on_owner_node() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:v1", now());
        let node_a = NodeId::new("node-a").expect("node a");
        let node_b = NodeId::new("node-b").expect("node b");
        let authority = Arc::new(InMemoryMatchmakerCluster::new());
        let router = Arc::new(InMemoryMatchmakerHandoffRouter::new());
        let lease = MatchmakerShardLease {
            shard: QueueShardId::new(0),
            owner_node: node_a.clone(),
            generation: OwnershipGeneration::new(1),
            expires_at: now()
                .checked_add(DurationMillis::from_millis(60_000))
                .expect("lease expiry"),
        };
        authority.acquire_shard(lease.clone()).expect("shard owned");
        let gw = Arc::new(gated_gateway(&readiness).with_matchmaker_cluster(
            node_a.clone(),
            lease.clone(),
            Arc::clone(&authority),
            Arc::clone(&router),
        ));
        gw.register_matchmaker_cluster_endpoint();

        // Form a cohort while Ready so real owner-bound handoffs exist.
        let (alice, mut ra) = register(&gw, Some("alice"));
        let (bob, mut rb) = register(&gw, Some("bob"));
        let request = serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 });
        gw.handle_inbound(alice, &rpc(1, "matchmaker.add", request.clone()));
        let _ = recv_rpc(&mut ra).await;
        gw.handle_inbound(bob, &rpc(2, "matchmaker.add", request));
        let _ = recv_rpc(&mut rb).await;
        let alice_handoff = json(&ra.recv().await.expect("matched").envelope.body);
        let bob_handoff = json(&rb.recv().await.expect("matched").envelope.body);
        let admission = |handoff: &serde_json::Value, user: &str| RemoteMatchmakerAdmission {
            ticket_id: TicketId::parse(handoff["ticket_id"].as_str().expect("ticket"))
                .expect("ticket id"),
            user_id: user.to_owned(),
            requester_node: node_b.clone(),
            join_token: handoff["join_token"].as_str().expect("token").to_owned(),
            formation_lease: lease.clone(),
        };

        // The owner refuses before consuming capacity: it cannot route scoped
        // state or protected intents back to the requesting session node.
        assert!(
            router
                .admit_remote(&node_a, admission(&alice_handoff, "alice"))
                .is_err(),
            "remote control admission is fail-closed without the data plane"
        );
        assert_eq!(gw.rooms().snapshot()[0].remote_member_count, 0);

        // Owner loses script readiness: the same control-plane path refuses.
        readiness.record_degraded(now());
        assert!(
            router
                .admit_remote(&node_a, admission(&bob_handoff, "bob"))
                .is_err(),
            "owner node fails closed on NodeCommand::AdmitRemote"
        );
        assert_eq!(
            gw.rooms().snapshot()[0].remote_member_count,
            0,
            "no admission is recorded while degraded"
        );
        assert_eq!(
            gw.metrics
                .snapshot()
                .script_gate_rejections
                .cluster_admit_remote,
            1
        );
    }

    // ---- require_script = false: byte-identical relay behavior ----------

    #[tokio::test]
    async fn gate_11_ungated_gateway_behavior_is_unchanged() {
        // No readiness attached (require_script = false): rooms create/join
        // exactly as before, no reject frames, no bindings.
        let gw = Gateway::new();
        let (a, mut ra) = register(&gw, None);
        let sent = gw.handle_inbound(a, &room_create(b"lobby"));
        assert_eq!(sent, 1, "exactly the ROOM_JOINED reply");
        let joined = ra.recv().await.expect("joined");
        assert_eq!(joined.envelope.kind, KIND_ROOM_JOINED);
        let room_id = RoomJoined::decode(&joined.envelope.body)
            .expect("decodes")
            .room_id;
        let (b, mut rb) = register(&gw, None);
        gw.handle_inbound(b, &room_join(room_id));
        assert_eq!(
            rb.recv().await.expect("joined").envelope.kind,
            KIND_ROOM_JOINED
        );
        assert_eq!(gw.rooms().binding(room_id), None, "no binding when ungated");
        let metrics = gw.metrics.snapshot();
        assert_eq!(
            metrics.script_gate_rejections,
            crate::observability::ScriptGateRejectionsSnapshot::default(),
            "no surface ever counted a rejection"
        );
    }

    // ---- Degraded: existing matches held, new ones gated ----------------

    #[tokio::test]
    async fn gate_12_degraded_holds_existing_matches_but_gates_new_ones() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:v1", now());
        let gw = gated_gateway(&readiness);
        let (a, mut ra) = register(&gw, Some("alice"));
        let (b, mut rb) = register(&gw, Some("bob"));
        gw.handle_inbound(a, &room_create(b"lobby"));
        let room_id = RoomJoined::decode(&ra.recv().await.expect("joined").envelope.body)
            .expect("decodes")
            .room_id;
        gw.handle_inbound(b, &room_join(room_id));
        let _ = rb.recv().await.expect("bob joined");

        readiness.record_degraded(now());
        // Held: the existing match keeps its members and is not torn down.
        let mut members = gw.rooms().members(room_id);
        members.sort_unstable();
        assert_eq!(members, vec![a, b]);
        assert_eq!(gw.rooms().room_count(), 1);
        // Gated: no new creation and no new admission while degraded.
        let (c, mut rc) = register(&gw, Some("carol"));
        gw.handle_inbound(c, &room_create(b"lobby2"));
        expect_room_reject(&mut rc, KIND_ROOM_CREATE).await;
        gw.handle_inbound(c, &room_join(room_id));
        expect_room_reject(&mut rc, KIND_ROOM_JOIN).await;
        let mut members = gw.rooms().members(room_id);
        members.sort_unstable();
        assert_eq!(members, vec![a, b]);
        assert_eq!(gw.rooms().room_count(), 1);
    }

    // ---- Stale-revision rooms refuse admission --------------------------

    #[tokio::test]
    async fn gate_13_room_join_into_a_superseded_room_is_refused() {
        let readiness = boot_readiness();
        readiness.record_loaded("sha256:v1", now());
        let gw = gated_gateway(&readiness);
        let (a, mut ra) = register(&gw, Some("alice"));
        gw.handle_inbound(a, &room_create(b"lobby"));
        let room_id = RoomJoined::decode(&ra.recv().await.expect("joined").envelope.body)
            .expect("decodes")
            .room_id;

        // A hot reload loads a new revision: the old room is superseded.
        readiness.record_loaded("sha256:v2", now());
        let (b, mut rb) = register(&gw, Some("bob"));
        gw.handle_inbound(b, &room_join(room_id));
        expect_room_reject(&mut rb, KIND_ROOM_JOIN).await;
        assert_eq!(gw.rooms().members(room_id), vec![a]);

        // Same name lands in the OLD named room's slot: refused too (a gated
        // node never quietly re-admits into a stale named room).
        gw.handle_inbound(b, &room_create(b"lobby"));
        expect_room_reject(&mut rb, KIND_ROOM_CREATE).await;

        // A fresh name creates a fresh room bound to the new revision.
        gw.handle_inbound(b, &room_create(b"lobby-v2"));
        let fresh = RoomJoined::decode(&rb.recv().await.expect("joined").envelope.body)
            .expect("decodes")
            .room_id;
        assert_eq!(
            gw.rooms().binding(fresh),
            Some(ScriptBinding {
                revision_id: "sha256:v2".to_owned(),
                generation: 2,
            })
        );
    }

    // ---- Boot not-ready, then Ready after a successful load -------------

    #[tokio::test]
    async fn gate_14_boot_not_ready_then_ready_after_load() {
        let readiness = boot_readiness();
        let gw = gated_gateway(&readiness);
        let (a, mut ra) = register(&gw, Some("alice"));
        gw.handle_inbound(a, &room_create(b"lobby"));
        expect_room_reject(&mut ra, KIND_ROOM_CREATE).await;
        assert_eq!(gw.rooms().room_count(), 0);

        // A later valid load (hot reload / revision deploy) opens the gate.
        readiness.record_loaded("sha256:v1", now());
        gw.handle_inbound(a, &room_create(b"lobby"));
        let joined = ra.recv().await.expect("joined");
        assert_eq!(joined.envelope.kind, KIND_ROOM_JOINED);
        assert_eq!(gw.rooms().room_count(), 1);
    }

    // ---- Degraded hold expiry (PROVISIONAL window, injectable seam) ------

    #[tokio::test]
    async fn gate_15_degraded_hold_escalates_to_unavailable_on_the_tick() {
        use crate::runtime::ScriptReadinessState;
        let readiness = Arc::new(
            GameScriptReadiness::new(now()).with_degraded_hold(std::time::Duration::from_millis(0)),
        );
        readiness.record_loaded("sha256:v1", now());
        let gw = gated_gateway(&readiness);
        let (a, mut ra) = register(&gw, Some("alice"));
        gw.handle_inbound(a, &room_create(b"lobby"));
        let _ = ra.recv().await.expect("joined");

        readiness.record_degraded(now());
        // The 250ms matchmaker tick is the escalation clock: a zero hold
        // window (injected) escalates on the next tick, existing matches are
        // held, and the gate stays closed either way.
        let _ = gw.matchmaker_tick();
        assert_eq!(
            readiness.snapshot().state,
            ScriptReadinessState::Unavailable
        );
        assert_eq!(gw.rooms().room_count(), 1, "the existing match is held");
        gw.handle_inbound(a, &room_create(b"another"));
        expect_room_reject(&mut ra, KIND_ROOM_CREATE).await;
    }

    // ---- The standing invariant (charter test 18) ------------------------

    #[tokio::test]
    async fn gate_18_no_match_ever_starts_without_a_ready_script() {
        let readiness = boot_readiness();
        let gw = gated_gateway(&readiness);
        let (a, mut ra) = register(&gw, Some("alice"));
        let (b, mut rb) = register(&gw, Some("bob"));

        // Every creation surface, in every non-ready state.
        let t = now();
        let request: TicketRequest = serde_json::from_value(
            serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 }),
        )
        .expect("request");
        let closed_states: [&dyn Fn(&GameScriptReadiness); 5] = [
            &|r| r.record_no_script(t),
            &|r| r.record_validating(t),
            &|r| r.record_activating(t),
            &|r| r.record_degraded(t),
            &|r| r.record_unavailable(t),
        ];
        for close in closed_states {
            close(&readiness);
            // Surface: client room create.
            gw.handle_inbound(a, &room_create(b"never"));
            expect_room_reject(&mut ra, KIND_ROOM_CREATE).await;
            // Surface: matchmaker queue.
            gw.handle_inbound(
                b,
                &rpc(
                    1,
                    "matchmaker.add",
                    serde_json::json!({ "min_count": 2, "max_count": 2, "ttl_ms": 60_000 }),
                ),
            );
            let (status, _) = recv_rpc(&mut rb).await;
            assert_eq!(status, protocol::RPC_STATUS_ERROR);
            // Surface: local activation over externally queued tickets.
            let ticket = gw
                .matchmaker
                .add_party(a, vec![a], request.clone(), t)
                .expect("queued");
            assert_eq!(gw.matchmaker_tick(), 0);
            assert!(
                gw.matchmaker.cancel(a, &ticket, t),
                "ticket held, not consumed"
            );
            // Surface: live formation.
            assert_eq!(gw.live_matchmaker_create_room(2), Err(()));
            assert_eq!(
                gw.rooms().room_count(),
                0,
                "invariant: no match exists without a ready script"
            );
        }
    }
}
