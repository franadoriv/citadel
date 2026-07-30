//! Transport-agnostic session registry for the realtime gateway.
//!
//! Each accepted connection (QUIC or WebSocket) registers a [`SessionHandle`]
//! whose only transport-specific dependency is a bounded
//! `tokio::mpsc::Sender<Outbound>`: the connection's write task drains that
//! channel and writes to its concrete socket. The registry therefore routes
//! purely over abstract outbound sinks and never depends on a concrete
//! transport.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, mpsc};

use citadel_wire::protocol::KIND_PEER_POSITION;

use crate::session::SessionId;
use crate::storage::UserId;
use crate::time::TimestampMillis;
use crate::transport::{Delivery, Envelope, TransportKind};

/// A process-unique realtime participant identity.
///
/// This is the gateway's notion of a connected participant, and is deliberately
/// three-way distinct from the transport-level
/// [`ConnectionId`](crate::transport::ConnectionId) (a socket handle) and the
/// authenticated domain [`SessionId`](crate::session::SessionId) (a validated
/// account session). A participant is minted per connection; binding a
/// participant to an authenticated `session::SessionId` after login is future
/// work once the realtime path consumes the identity/session services.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ParticipantId(u64);

impl ParticipantId {
    /// The raw numeric value (used to tag relayed messages).
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Wrap a raw id, e.g. a target supplied by a script's `citadel.send`.
    ///
    /// The id is not validated against the live registry here; delivery to a
    /// non-existent participant is simply a no-op at fan-out time.
    #[must_use]
    pub const fn from_raw(id: u64) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for ParticipantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "participant-{}", self.0)
    }
}

/// Allocator for process-unique [`ParticipantId`] values.
#[derive(Debug)]
pub struct ParticipantIdGen {
    next: AtomicU64,
}

impl ParticipantIdGen {
    /// Create a fresh generator starting at 1.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate the next session id.
    pub fn next_id(&self) -> ParticipantId {
        ParticipantId(self.next.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for ParticipantIdGen {
    fn default() -> Self {
        Self::new()
    }
}

/// An outbound message destined for a session's write task.
#[derive(Debug, Clone)]
pub struct Outbound {
    /// Delivery intent; the write task maps this to the concrete transport.
    pub delivery: Delivery,
    /// The envelope to send.
    pub envelope: Envelope,
}

impl Outbound {
    /// A reliable outbound envelope.
    #[must_use]
    pub fn reliable(envelope: Envelope) -> Self {
        Self {
            delivery: Delivery::Reliable,
            envelope,
        }
    }

    /// An unreliable outbound envelope.
    #[must_use]
    pub fn unreliable(envelope: Envelope) -> Self {
        Self {
            delivery: Delivery::Unreliable,
            envelope,
        }
    }
}

/// A coalescing mailbox for ephemeral transport state. Sender-tagged peer
/// positions retain one pending envelope per `(kind, sender)`; other state
/// retains one per kind. A slow recipient therefore receives the newest state
/// instead of replaying a stale FIFO backlog.
///
/// Reliable/control envelopes deliberately remain on the bounded Tokio mpsc
/// channel owned by [`SessionHandle`].
#[derive(Debug, Clone)]
pub struct LatestOutboundSender {
    inner: Arc<LatestOutboundInner>,
}

/// Receive the next latest-wins outbound envelope for a transport writer.
#[derive(Debug)]
pub struct LatestOutboundReceiver {
    inner: Arc<LatestOutboundInner>,
}

#[derive(Debug)]
struct LatestOutboundInner {
    pending: Mutex<LatestMailbox>,
    notify: Notify,
}

/// Preserve a pending latest position for every possible peer in the stress
/// simulator while still bounding memory for a slow session.
const MAX_PENDING_UNRELIABLE_KEYS: usize = 1_024;
/// The stress simulator uses a separate position kind while it exercises its
/// authoritative Lua gameplay. Its body has the same sender-tagged layout as
/// the wire protocol's `KIND_PEER_POSITION`, so it needs the same coalescing
/// key to retain one fresh state per peer.
const KIND_STRESS_PEER_POSITION: u16 = 201;
/// The simulator batches its position fan-out into datagram-sized chunks. The
/// first two bytes identify the stable chunk index, so each chunk keeps its
/// own latest state in a slow recipient's mailbox.
const KIND_STRESS_PEER_SNAPSHOT: u16 = 204;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct LatestKey {
    kind: u16,
    source: Option<u64>,
}

#[derive(Debug, Default)]
struct LatestMailbox {
    values: HashMap<LatestKey, Outbound>,
    order: VecDeque<LatestKey>,
}

/// Construct the sender stored by the registry and the receiver owned by one
/// transport write task.
#[must_use]
pub fn latest_outbound_channel() -> (LatestOutboundSender, LatestOutboundReceiver) {
    let inner = Arc::new(LatestOutboundInner {
        pending: Mutex::new(LatestMailbox::default()),
        notify: Notify::new(),
    });
    (
        LatestOutboundSender {
            inner: Arc::clone(&inner),
        },
        LatestOutboundReceiver { inner },
    )
}

impl LatestOutboundSender {
    /// Replace an older pending message of the same state key, preserving only
    /// the latest state. Peer positions use the tagged sender ID as part of the
    /// key, so every visible player retains an independent latest position.
    pub fn replace(&self, outbound: Outbound) -> bool {
        let Ok(mut pending) = self.inner.pending.lock() else {
            return false;
        };
        let key = latest_key(&outbound);
        if let Some(existing) = pending.values.get_mut(&key) {
            *existing = outbound;
        } else {
            if pending.values.len() == MAX_PENDING_UNRELIABLE_KEYS
                && let Some(oldest) = pending.order.pop_front()
            {
                pending.values.remove(&oldest);
            }
            pending.order.push_back(key);
            pending.values.insert(key, outbound);
        }
        drop(pending);
        self.inner.notify.notify_one();
        true
    }
}

impl LatestOutboundReceiver {
    /// Wait for and remove one coalesced envelope. The notification is created
    /// before checking the queue so a concurrent producer cannot be missed.
    pub async fn recv(&self) -> Outbound {
        loop {
            let notified = self.inner.notify.notified();
            if let Ok(mut pending) = self.inner.pending.lock() {
                while let Some(key) = pending.order.pop_front() {
                    if let Some(outbound) = pending.values.remove(&key) {
                        return outbound;
                    }
                }
            }
            notified.await;
        }
    }

    /// Return one coalesced envelope without waiting. This is primarily useful
    /// for deterministic in-process tests; production writers should use
    /// [`Self::recv`] so they sleep efficiently until state is available.
    pub fn try_recv(&self) -> Result<Outbound, mpsc::error::TryRecvError> {
        let Ok(mut pending) = self.inner.pending.lock() else {
            return Err(mpsc::error::TryRecvError::Disconnected);
        };
        while let Some(key) = pending.order.pop_front() {
            if let Some(outbound) = pending.values.remove(&key) {
                return Ok(outbound);
            }
        }
        Err(mpsc::error::TryRecvError::Empty)
    }
}

fn latest_key(outbound: &Outbound) -> LatestKey {
    let source = match outbound.envelope.kind {
        KIND_PEER_POSITION | KIND_STRESS_PEER_POSITION => outbound
            .envelope
            .body
            .get(0..8)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u64::from_be_bytes),
        KIND_STRESS_PEER_SNAPSHOT => outbound
            .envelope
            .body
            .get(0..2)
            .and_then(|bytes| bytes.try_into().ok())
            .map(u16::from_be_bytes)
            .map(u64::from),
        _ => None,
    };
    LatestKey {
        kind: outbound.envelope.kind,
        source,
    }
}

/// The authenticated account bound to a participant by the realtime handshake
///.
///
/// Present only for participants that presented a valid session token at
/// connect; guest/anonymous participants carry `None`. This is the seam that
/// makes the realtime layer account-aware: the `ParticipantId` remains the
/// transport-level participant identity, while this carries the *domain* account
/// (`user_id`) and its session, resolved solely by the
/// [`SessionService`](crate::services::SessionService) — never from client
/// payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantIdentity {
    /// The authenticated account behind the connection.
    pub user_id: UserId,
    /// The session that authenticated the connection.
    pub session_id: SessionId,
    /// When the authenticating session expires (Unix millis). Retained so a
    /// future task can enforce mid-session expiry/revocation on long-lived
    /// sockets; connect-time binding does not yet close on expiry.
    pub expires_at: TimestampMillis,
}

/// A registered session: its id, transport family, outbound sink, and the
/// optional authenticated identity bound at connect.
#[derive(Debug, Clone)]
pub struct SessionHandle {
    /// Participant (transport-level) identity.
    pub id: ParticipantId,
    /// Transport family this session is connected over.
    pub kind: TransportKind,
    /// Bounded outbound channel drained by the connection's write task.
    pub outbound: mpsc::Sender<Outbound>,
    /// The authenticated account bound to this participant, or `None` for a
    /// guest/anonymous participant.
    pub identity: Option<ParticipantIdentity>,
}

impl SessionHandle {
    /// Whether this participant is bound to an authenticated account.
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.identity.is_some()
    }
}

/// In-memory registry of active sessions.
///
/// Single-node, in-memory; this is the local subset of the future
/// session/presence directories. Fan-out delivers to abstract outbound sinks.
#[derive(Debug, Clone, Default)]
pub struct SessionRegistry {
    sessions: Arc<Mutex<HashMap<ParticipantId, SessionHandle>>>,
    unreliable: Arc<Mutex<HashMap<ParticipantId, LatestOutboundSender>>>,
}

impl SessionRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a session handle.
    pub fn register(&self, handle: SessionHandle) -> LatestOutboundReceiver {
        let (sender, receiver) = latest_outbound_channel();
        if let Ok(mut map) = self.unreliable.lock() {
            map.insert(handle.id, sender);
        }
        if let Ok(mut map) = self.sessions.lock() {
            map.insert(handle.id, handle);
        }
        receiver
    }

    /// Unregister a session (on disconnect), returning the removed handle.
    ///
    /// The returned handle lets the caller tell whether an *authenticated*
    /// session ended (so the authenticated-session gauge is decremented exactly
    /// when it was incremented) versus a guest participant.
    pub fn unregister(&self, id: ParticipantId) -> Option<SessionHandle> {
        let removed = match self.sessions.lock() {
            Ok(mut map) => map.remove(&id),
            Err(_) => None,
        };
        if let Ok(mut map) = self.unreliable.lock() {
            map.remove(&id);
        }
        removed
    }

    /// Clone the outbound sink for a participant, if registered.
    ///
    /// Lets an async task deliver a correlated reply to a caller after the
    /// synchronous `handle_inbound` has already returned (built-in domain RPC;
    /// ). The clone keeps the session alive only as a channel endpoint,
    /// not as a registry lock, so the spawned task never contends the registry.
    #[must_use]
    pub fn outbound_of(&self, id: ParticipantId) -> Option<mpsc::Sender<Outbound>> {
        let map = self.sessions.lock().ok()?;
        map.get(&id).map(|handle| handle.outbound.clone())
    }

    /// The authenticated account id bound to a participant, if any.
    ///
    /// Returns the `user_id` string for an authenticated participant, or `None`
    /// for a guest or an unknown participant. Used to populate `ctx.user_id` for
    /// game logic without leaking the full domain identity into the runtime.
    #[must_use]
    pub fn user_id_of(&self, id: ParticipantId) -> Option<String> {
        let map = self.sessions.lock().ok()?;
        let handle = map.get(&id)?;
        handle
            .identity
            .as_ref()
            .map(|identity| identity.user_id.as_str().to_string())
    }

    /// Resolve one active participant for an authenticated account. When an
    /// account has multiple simultaneous connections, select the lowest stable
    /// participant id so party queueing remains deterministic. The account-level
    /// handoff authorization remains valid across any later reconnection.
    #[must_use]
    pub fn participant_for_user(&self, user_id: &str) -> Option<ParticipantId> {
        let map = self.sessions.lock().ok()?;
        map.values()
            .filter(|handle| {
                handle
                    .identity
                    .as_ref()
                    .is_some_and(|identity| identity.user_id.as_str() == user_id)
            })
            .map(|handle| handle.id)
            .min()
    }

    /// Snapshot every currently connected participant bound to an account.
    ///
    /// Notifications fan out to all of a player's local devices. The returned
    /// ids are independent of the registry lock, so callers can perform bounded
    /// `try_send` operations without holding it.
    #[must_use]
    pub fn participants_for_user(&self, user_id: &str) -> Vec<ParticipantId> {
        let Ok(map) = self.sessions.lock() else {
            return Vec::new();
        };
        map.values()
            .filter(|handle| {
                handle
                    .identity
                    .as_ref()
                    .is_some_and(|identity| identity.user_id.as_str() == user_id)
            })
            .map(|handle| handle.id)
            .collect()
    }

    /// Number of registered sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions.lock().map(|m| m.len()).unwrap_or(0)
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Deliver `outbound` to every session except `sender`.
    ///
    /// Reliable traffic uses the bounded per-session channel. Unreliable state
    /// replaces the older message of the same state key for that recipient.
    /// Returns the number of sessions the message was queued to.
    pub fn broadcast_except(&self, sender: ParticipantId, outbound: &Outbound) -> usize {
        let recipients: Vec<ParticipantId> = match self.sessions.lock() {
            Ok(map) => map
                .values()
                .filter(|h| h.id != sender)
                .map(|h| h.id)
                .collect(),
            Err(_) => return 0,
        };
        let mut delivered = 0;
        for recipient in recipients {
            if self.send_to(recipient, outbound) {
                delivered += 1;
            }
        }
        delivered
    }

    /// Deliver `outbound` to every registered session.
    ///
    /// Like [`broadcast_except`] but with no exclusion; used by the server tick,
    /// which has no originating sender. Returns the number of sessions queued to.
    ///
    /// [`broadcast_except`]: SessionRegistry::broadcast_except
    pub fn broadcast_all(&self, outbound: &Outbound) -> usize {
        let recipients: Vec<ParticipantId> = match self.sessions.lock() {
            Ok(map) => map.values().map(|handle| handle.id).collect(),
            Err(_) => return 0,
        };
        let mut delivered = 0;
        for recipient in recipients {
            if self.send_to(recipient, outbound) {
                delivered += 1;
            }
        }
        delivered
    }

    /// Deliver `outbound` to an explicit membership snapshot, optionally leaving
    /// out the originating participant.
    ///
    /// The caller owns the snapshot (normally [`RoomRegistry`](super::rooms::RoomRegistry))
    /// and therefore no room lock is held while bounded session queues are touched.
    /// This is the local half of match-scoped routing; a future remote owner is
    /// resolved before this method is reached.
    pub fn broadcast_members(
        &self,
        members: &[ParticipantId],
        exclude: Option<ParticipantId>,
        outbound: &Outbound,
    ) -> usize {
        let recipients: Vec<ParticipantId> = match self.sessions.lock() {
            Ok(map) => members
                .iter()
                .filter(|&&id| Some(id) != exclude)
                .filter(|id| map.contains_key(id))
                .copied()
                .collect(),
            Err(_) => return 0,
        };
        let mut delivered = 0;
        for recipient in recipients {
            if self.send_to(recipient, outbound) {
                delivered += 1;
            }
        }
        delivered
    }

    /// Deliver `outbound` to a single session by id.
    ///
    /// Best-effort like [`broadcast_except`]: returns `true` if the message was
    /// queued, `false` if the target is unknown or its channel is full/closed.
    ///
    /// [`broadcast_except`]: SessionRegistry::broadcast_except
    pub fn send_to(&self, id: ParticipantId, outbound: &Outbound) -> bool {
        if outbound.delivery == Delivery::Unreliable {
            let sender = self
                .unreliable
                .lock()
                .ok()
                .and_then(|map| map.get(&id).cloned());
            return sender.is_some_and(|sender| sender.replace(outbound.clone()));
        }
        let handle = match self.sessions.lock() {
            Ok(map) => map.get(&id).cloned(),
            Err(_) => None,
        };
        match handle {
            Some(handle) => handle.outbound.try_send(outbound.clone()).is_ok(),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(id: u64, kind: TransportKind) -> (SessionHandle, mpsc::Receiver<Outbound>) {
        let (tx, rx) = mpsc::channel(8);
        (
            SessionHandle {
                id: ParticipantId(id),
                kind,
                outbound: tx,
                identity: None,
            },
            rx,
        )
    }

    #[test]
    fn participant_ids_are_monotonic() {
        let r#gen = ParticipantIdGen::new();
        let a = r#gen.next_id();
        let b = r#gen.next_id();
        assert_ne!(a, b);
        assert_eq!(a.get() + 1, b.get());
        assert_eq!(a.to_string(), "participant-1");
    }

    #[test]
    fn register_and_unregister_track_len() {
        let reg = SessionRegistry::new();
        assert!(reg.is_empty());
        let (h1, _r1) = handle(1, TransportKind::WebSocket);
        let (h2, _r2) = handle(2, TransportKind::Quic);
        reg.register(h1);
        reg.register(h2);
        assert_eq!(reg.len(), 2);
        reg.unregister(ParticipantId(1));
        assert_eq!(reg.len(), 1);
    }

    #[tokio::test]
    async fn broadcast_skips_the_sender() {
        let reg = SessionRegistry::new();
        let (h1, mut r1) = handle(1, TransportKind::WebSocket);
        let (h2, mut r2) = handle(2, TransportKind::WebSocket);
        let (h3, mut r3) = handle(3, TransportKind::WebSocket);
        reg.register(h1);
        reg.register(h2);
        reg.register(h3);

        let out = Outbound::reliable(Envelope::new(2, &b"hi"[..]));
        let delivered = reg.broadcast_except(ParticipantId(1), &out);
        assert_eq!(delivered, 2, "delivered to the two non-sender sessions");

        // Sender's own channel stays empty.
        assert!(r1.try_recv().is_err());
        // Others received exactly the relayed message.
        assert_eq!(r2.recv().await.expect("recv").envelope.kind, 2);
        assert_eq!(r3.recv().await.expect("recv").envelope.kind, 2);
    }

    #[test]
    fn broadcast_to_empty_registry_delivers_to_none() {
        let reg = SessionRegistry::new();
        let out = Outbound::reliable(Envelope::new(1, &b"x"[..]));
        assert_eq!(reg.broadcast_except(ParticipantId(99), &out), 0);
    }

    #[tokio::test]
    async fn send_to_targets_a_single_session() {
        let reg = SessionRegistry::new();
        let (h1, mut r1) = handle(1, TransportKind::WebSocket);
        let (h2, mut r2) = handle(2, TransportKind::WebSocket);
        reg.register(h1);
        reg.register(h2);

        let out = Outbound::reliable(Envelope::new(5, &b"hi"[..]));
        assert!(reg.send_to(ParticipantId(2), &out), "delivered to target");
        // Only the target received it.
        assert!(r1.try_recv().is_err());
        assert_eq!(r2.recv().await.expect("recv").envelope.kind, 5);
    }

    #[tokio::test]
    async fn unreliable_delivery_retains_latest_state_for_each_peer() {
        let reg = SessionRegistry::new();
        let (handle, _reliable) = handle(7, TransportKind::Quic);
        let latest = reg.register(handle);

        let peer_position = |kind, peer: u64, label: &[u8]| {
            let mut body = peer.to_be_bytes().to_vec();
            body.extend_from_slice(label);
            Outbound::unreliable(Envelope::new(kind, body))
        };

        assert!(reg.send_to(
            ParticipantId(7),
            &peer_position(KIND_PEER_POSITION, 11, b"old")
        ));
        assert!(reg.send_to(
            ParticipantId(7),
            &peer_position(KIND_PEER_POSITION, 22, b"peer-two")
        ));
        assert!(reg.send_to(
            ParticipantId(7),
            &peer_position(KIND_PEER_POSITION, 11, b"new")
        ));
        assert!(reg.send_to(
            ParticipantId(7),
            &peer_position(KIND_STRESS_PEER_POSITION, 33, b"sim-old")
        ));
        assert!(reg.send_to(
            ParticipantId(7),
            &peer_position(KIND_STRESS_PEER_POSITION, 33, b"sim-new")
        ));

        let first = latest.recv().await;
        let second = latest.recv().await;
        let third = latest.recv().await;
        assert_eq!(&first.envelope.body[0..8], &11_u64.to_be_bytes());
        assert_eq!(&first.envelope.body[8..], b"new");
        assert_eq!(&second.envelope.body[0..8], &22_u64.to_be_bytes());
        assert_eq!(&second.envelope.body[8..], b"peer-two");
        assert_eq!(third.envelope.kind, KIND_STRESS_PEER_POSITION);
        assert_eq!(&third.envelope.body[0..8], &33_u64.to_be_bytes());
        assert_eq!(&third.envelope.body[8..], b"sim-new");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), latest.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn unreliable_delivery_retains_the_latest_state_for_each_snapshot_chunk() {
        let reg = SessionRegistry::new();
        let (handle, _reliable) = handle(9, TransportKind::Quic);
        let latest = reg.register(handle);
        let chunk = |index: u16, label: &[u8]| {
            let mut body = index.to_be_bytes().to_vec();
            body.extend_from_slice(label);
            Outbound::unreliable(Envelope::new(KIND_STRESS_PEER_SNAPSHOT, body))
        };

        assert!(reg.send_to(ParticipantId(9), &chunk(0, b"old")));
        assert!(reg.send_to(ParticipantId(9), &chunk(1, b"chunk-one")));
        assert!(reg.send_to(ParticipantId(9), &chunk(0, b"new")));

        let first = latest.recv().await;
        let second = latest.recv().await;
        assert_eq!(&first.envelope.body[0..2], &0_u16.to_be_bytes());
        assert_eq!(&first.envelope.body[2..], b"new");
        assert_eq!(&second.envelope.body[0..2], &1_u16.to_be_bytes());
        assert_eq!(&second.envelope.body[2..], b"chunk-one");
    }

    #[tokio::test]
    async fn unreliable_mailbox_evicts_the_oldest_state_key_at_its_bound() {
        let reg = SessionRegistry::new();
        let (handle, _reliable) = handle(8, TransportKind::Quic);
        let latest = reg.register(handle);

        for kind in 1..=u16::try_from(MAX_PENDING_UNRELIABLE_KEYS + 1).expect("kind range") {
            assert!(reg.send_to(
                ParticipantId(8),
                &Outbound::unreliable(Envelope::new(kind, b"state".to_vec()))
            ));
        }

        let first = latest.recv().await;
        assert_eq!(first.envelope.kind, 2, "the oldest key was evicted");
        let mut delivered = 1;
        while latest.try_recv().is_ok() {
            delivered += 1;
        }
        assert_eq!(delivered, MAX_PENDING_UNRELIABLE_KEYS);
    }

    #[test]
    fn send_to_unknown_session_is_a_noop() {
        let reg = SessionRegistry::new();
        let out = Outbound::reliable(Envelope::new(1, &b"x"[..]));
        assert!(!reg.send_to(ParticipantId(404), &out));
    }

    #[test]
    fn participant_id_from_raw_round_trips() {
        assert_eq!(ParticipantId::from_raw(7).get(), 7);
    }
}
