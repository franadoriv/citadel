//! Transport-agnostic session registry for the realtime gateway.
//!
//! Each accepted connection (QUIC or WebSocket) registers a [`SessionHandle`]
//! whose only transport-specific dependency is a bounded
//! `tokio::mpsc::Sender<Outbound>`: the connection's write task drains that
//! channel and writes to its concrete socket. The registry therefore routes
//! purely over abstract outbound sinks and never depends on a concrete
//! transport.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, MutexGuard, Notify, mpsc};

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
    /// The connection-local close fence assigned by the registry at enqueue.
    /// Directly constructed outbound messages are unfenced until routed.
    fence: Option<Arc<AtomicBool>>,
    /// Serializes a final delivery decision with session revocation. This is
    /// per connection, never a registry-wide lock, so a slow peer cannot stall
    /// unrelated sessions.
    delivery_gate: Option<Arc<AsyncMutex<()>>>,
}

impl Outbound {
    /// Construct an envelope with an explicit transport delivery class.
    #[must_use]
    pub fn new(delivery: Delivery, envelope: Envelope) -> Self {
        Self {
            delivery,
            envelope,
            fence: None,
            delivery_gate: None,
        }
    }

    /// A reliable outbound envelope.
    #[must_use]
    pub fn reliable(envelope: Envelope) -> Self {
        Self::new(Delivery::Reliable, envelope)
    }

    /// An unreliable outbound envelope.
    #[must_use]
    pub fn unreliable(envelope: Envelope) -> Self {
        Self::new(Delivery::Unreliable, envelope)
    }

    fn fenced(mut self, fence: Arc<AtomicBool>, delivery_gate: Arc<AsyncMutex<()>>) -> Self {
        self.fence = Some(fence);
        self.delivery_gate = Some(delivery_gate);
        self
    }

    /// Acquire the connection-local delivery lease. The returned lease must be
    /// kept through the actual transport I/O. Revocation acquires this same
    /// gate before lowering the fence: therefore either I/O completes before
    /// revocation linearizes, or the writer observes the lowered fence and does
    /// not begin I/O. Never hold registry/session locks while awaiting it.
    pub async fn acquire_delivery(&self) -> Option<MutexGuard<'_, ()>> {
        let gate = self.delivery_gate.as_ref()?;
        let permit = gate.lock().await;
        self.is_deliverable().then_some(permit)
    }

    /// Whether this envelope may still cross the transport boundary.
    ///
    /// Writers must check this immediately before writing: a close can race an
    /// already dequeued reliable envelope, while latest-wins mailboxes are
    /// cleared eagerly at close.
    #[must_use]
    pub fn is_deliverable(&self) -> bool {
        self.fence
            .as_ref()
            .is_none_or(|fence| fence.load(Ordering::Acquire))
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

    /// Drop all pending latest-wins state during connection close.
    fn clear(&self) {
        if let Ok(mut pending) = self.inner.pending.lock() {
            pending.values.clear();
            pending.order.clear();
        }
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

/// Stable public class for a server-initiated close.  Internal revocation
/// causes deliberately do not cross this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicCloseClass {
    /// The authenticated session is no longer usable.
    SessionEnded,
    /// A trusted runtime removed the connection.
    Removed,
    /// A trusted runtime applied policy.
    Policy,
}

/// An opaque, generation-fenced reference to one local connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionRef {
    id: ParticipantId,
    generation: u64,
}

impl ConnectionRef {
    #[must_use]
    pub(crate) const fn participant_id(self) -> ParticipantId {
        self.id
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseDisposition {
    Closing,
    Duplicate,
    Stale,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Active,
    Closing,
    Closed,
}

/// Per-connection linearization point for application work and teardown.
///
/// `close` wins over all later sends.  Cleanup is claimed separately so a
/// transport teardown racing a revocation runs gateway cleanup exactly once.
#[derive(Debug)]
struct RealtimeConnectionController {
    state: Mutex<ConnectionControl>,
    accepting: Arc<AtomicBool>,
    delivery_gate: Arc<AsyncMutex<()>>,
}

#[derive(Debug)]
struct ConnectionControl {
    state: ConnectionState,
    cleanup_claimed: bool,
    close_ids: Vec<String>,
}

impl RealtimeConnectionController {
    fn new() -> Self {
        Self {
            state: Mutex::new(ConnectionControl {
                state: ConnectionState::Active,
                cleanup_claimed: false,
                close_ids: Vec::new(),
            }),
            accepting: Arc::new(AtomicBool::new(true)),
            delivery_gate: Arc::new(AsyncMutex::new(())),
        }
    }
    fn accepts_work(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }
    fn send(&self, f: impl FnOnce() -> bool) -> bool {
        let Ok(control) = self.state.lock() else {
            return false;
        };
        matches!(control.state, ConnectionState::Active) && f()
    }
    async fn close(&self, command_id: &str) -> CloseDisposition {
        // Lock ordering invariant: delivery gate -> control state. Writers
        // acquire only the gate; registry operations acquire only control
        // state, so there is no cycle and no global I/O serialization.
        let _delivery = self.delivery_gate.lock().await;
        let Ok(mut control) = self.state.lock() else {
            return CloseDisposition::Unknown;
        };
        if control.close_ids.iter().any(|id| id == command_id) {
            return CloseDisposition::Duplicate;
        }
        if matches!(control.state, ConnectionState::Closed) {
            return CloseDisposition::Unknown;
        }
        control.close_ids.push(command_id.to_owned());
        control.state = ConnectionState::Closing;
        // This release is the application/transport cutoff. A writer that
        // dequeued an envelope before close observes it before socket I/O.
        self.accepting.store(false, Ordering::Release);
        CloseDisposition::Closing
    }
    fn claim_cleanup(&self) -> bool {
        let Ok(mut control) = self.state.lock() else {
            return false;
        };
        if control.cleanup_claimed {
            return false;
        }
        control.cleanup_claimed = true;
        control.state = ConnectionState::Closing;
        self.accepting.store(false, Ordering::Release);
        true
    }
    fn finish_cleanup(&self) {
        if let Ok(mut control) = self.state.lock() {
            control.state = ConnectionState::Closed;
        }
        self.accepting.store(false, Ordering::Release);
    }

    fn fence(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.accepting)
    }

    fn delivery_gate(&self) -> Arc<AsyncMutex<()>> {
        Arc::clone(&self.delivery_gate)
    }
}

#[derive(Debug, Clone)]
struct RegisteredSession {
    handle: SessionHandle,
    controller: Arc<RealtimeConnectionController>,
    generation: u64,
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
    sessions: Arc<Mutex<HashMap<ParticipantId, RegisteredSession>>>,
    unreliable: Arc<Mutex<HashMap<ParticipantId, LatestOutboundSender>>>,
    /// Durable revocation has linearized for these exact sessions. Keeping the
    /// tombstone under the session-map lock closes the publication race: a
    /// registration either publishes before the tombstone and is fenced by the
    /// same close, or observes it and never publishes/delivers an auth result.
    revoked_sessions: Arc<Mutex<HashSet<SessionId>>>,
    next_generation: Arc<AtomicU64>,
}

impl SessionRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a session handle.
    pub fn register(&self, handle: SessionHandle) -> LatestOutboundReceiver {
        self.register_with_initial(handle, None)
    }

    /// Register a session and atomically seed its reliable queue.
    ///
    /// The optional initial envelope is fenced by the same controller that owns
    /// the new session *before* the session becomes visible to a concurrent
    /// close. This is for protocol replies, such as an accepted auth result,
    /// which must be first in the reliable queue without bypassing the close
    /// fence through a raw transport sender.
    pub fn register_with_initial(
        &self,
        handle: SessionHandle,
        initial: Option<Outbound>,
    ) -> LatestOutboundReceiver {
        self.register_with_initials(handle, initial.into_iter().collect())
    }

    /// Register a session and atomically seed an ordered reliable prefix.
    ///
    /// This is the multi-envelope form of [`Self::register_with_initial`]. It
    /// preserves the caller-provided order under the same close fence, which is
    /// required for the backwards-compatible `AUTH_RESULT` then `SERVER_TIME`
    /// handshake extension. No application/lifecycle send can overtake it.
    pub fn register_with_initials(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
    ) -> LatestOutboundReceiver {
        let (sender, receiver) = latest_outbound_channel();
        if let Ok(mut map) = self.unreliable.lock() {
            map.insert(handle.id, sender);
        }
        if let Ok(mut map) = self.sessions.lock() {
            let controller = Arc::new(RealtimeConnectionController::new());
            let revoked = handle.identity.as_ref().is_some_and(|identity| {
                self.revoked_sessions
                    .lock()
                    .is_ok_and(|revoked| revoked.contains(&identity.session_id))
            });
            if revoked {
                return receiver;
            }
            for initial in initials {
                // The channel is new and therefore cannot be full. Keep every
                // send best-effort so a closed writer still follows normal
                // transport teardown instead of failing registration.
                let _ = handle
                    .outbound
                    .try_send(initial.fenced(controller.fence(), controller.delivery_gate()));
            }
            let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
            map.insert(
                handle.id,
                RegisteredSession {
                    handle,
                    controller,
                    generation,
                },
            );
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
            Ok(mut map) => map.remove(&id).map(|entry| {
                entry.controller.finish_cleanup();
                entry.handle
            }),
            Err(_) => None,
        };
        if let Ok(mut map) = self.unreliable.lock() {
            map.remove(&id);
        }
        removed
    }

    /// Atomically claim the once-only gateway cleanup for a connection.
    pub fn claim_cleanup(&self, id: ParticipantId) -> bool {
        self.sessions
            .lock()
            .ok()
            .and_then(|map| map.get(&id).map(|entry| entry.controller.claim_cleanup()))
            .unwrap_or(false)
    }

    /// Close all local connections for this exact authenticated session.
    /// A stale generation is harmless and cannot close a replacement connection.
    pub async fn close_session(
        &self,
        session_id: &SessionId,
        command_id: &str,
        expected_generation: Option<u64>,
    ) -> Vec<(ConnectionRef, CloseDisposition)> {
        let entries: Vec<_> = {
            let Ok(map) = self.sessions.lock() else {
                return Vec::new();
            };
            let revocation_targets_live_generation = expected_generation.is_none_or(|generation| {
                map.values().any(|entry| {
                    entry.handle.identity.as_ref().is_some_and(|identity| {
                        &identity.session_id == session_id && entry.generation == generation
                    })
                })
            });
            if revocation_targets_live_generation
                && let Ok(mut revoked) = self.revoked_sessions.lock()
            {
                revoked.insert(session_id.clone());
            }
            map.values()
                .filter(|entry| {
                    entry
                        .handle
                        .identity
                        .as_ref()
                        .is_some_and(|identity| &identity.session_id == session_id)
                })
                .map(|entry| {
                    let reference = ConnectionRef {
                        id: entry.handle.id,
                        generation: entry.generation,
                    };
                    (reference, Arc::clone(&entry.controller), entry.generation)
                })
                .collect()
        };
        let mut closed = Vec::with_capacity(entries.len());
        for (reference, controller, generation) in entries {
            let result = if expected_generation.is_some_and(|value| value != generation) {
                CloseDisposition::Stale
            } else {
                controller.close(command_id).await
            };
            closed.push((reference, result));
        }
        // Registration takes the unreliable then session locks. Clear mailbox
        // state only after releasing the session lock to preserve that order.
        if let Ok(unreliable) = self.unreliable.lock() {
            for (connection, disposition) in &closed {
                if *disposition == CloseDisposition::Closing
                    && let Some(sender) = unreliable.get(&connection.id)
                {
                    sender.clear();
                }
            }
        }
        closed
    }

    /// Current local references for an exact session; never substitutes user id.
    #[must_use]
    pub fn connections_for_session(&self, session_id: &SessionId) -> Vec<ConnectionRef> {
        self.sessions
            .lock()
            .map(|map| {
                map.values()
                    .filter(|entry| {
                        entry
                            .handle
                            .identity
                            .as_ref()
                            .is_some_and(|identity| &identity.session_id == session_id)
                    })
                    .map(|entry| ConnectionRef {
                        id: entry.handle.id,
                        generation: entry.generation,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether an exact participant may begin more application work.
    #[must_use]
    pub fn accepts_work(&self, id: ParticipantId) -> bool {
        self.sessions
            .lock()
            .ok()
            .and_then(|map| map.get(&id).map(|entry| entry.controller.accepts_work()))
            .unwrap_or(false)
    }

    /// The authenticated account id bound to a participant, if any.
    ///
    /// Returns the `user_id` string for an authenticated participant, or `None`
    /// for a guest or an unknown participant. Used to populate `ctx.user_id` for
    /// game logic without leaking the full domain identity into the runtime.
    #[must_use]
    pub fn user_id_of(&self, id: ParticipantId) -> Option<String> {
        let map = self.sessions.lock().ok()?;
        let handle = &map.get(&id)?.handle;
        handle
            .identity
            .as_ref()
            .map(|identity| identity.user_id.as_str().to_string())
    }

    /// Whether this exact live connection is authenticated. Diagnostics uses
    /// this gate before accepting an SDK capability assertion; a guest cannot
    /// opt in through a forged post-auth control frame.
    #[must_use]
    pub fn is_authenticated(&self, id: ParticipantId) -> bool {
        self.sessions
            .lock()
            .ok()
            .and_then(|map| map.get(&id).map(|entry| entry.handle.is_authenticated()))
            .unwrap_or(false)
    }

    /// Snapshot currently connected participant ids. The caller may make
    /// bounded sends after this returns; the snapshot itself never holds a
    /// registry lock across transport work.
    #[must_use]
    pub fn participants(&self) -> Vec<ParticipantId> {
        self.sessions
            .lock()
            .map(|map| map.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Resolve one active participant for an authenticated account. When an
    /// account has multiple simultaneous connections, select the lowest stable
    /// participant id so party queueing remains deterministic. The account-level
    /// handoff authorization remains valid across any later reconnection.
    #[must_use]
    pub fn participant_for_user(&self, user_id: &str) -> Option<ParticipantId> {
        let map = self.sessions.lock().ok()?;
        map.values()
            .filter(|entry| {
                entry
                    .handle
                    .identity
                    .as_ref()
                    .is_some_and(|identity| identity.user_id.as_str() == user_id)
            })
            .map(|entry| entry.handle.id)
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
            .filter(|entry| {
                entry
                    .handle
                    .identity
                    .as_ref()
                    .is_some_and(|identity| identity.user_id.as_str() == user_id)
            })
            .map(|entry| entry.handle.id)
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
                .filter(|entry| entry.handle.id != sender)
                .map(|entry| entry.handle.id)
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
            Ok(map) => map.values().map(|entry| entry.handle.id).collect(),
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
        let entry = match self.sessions.lock() {
            Ok(map) => map.get(&id).cloned(),
            Err(_) => None,
        };
        match entry {
            Some(entry) if outbound.delivery == Delivery::Unreliable => {
                let sender = self
                    .unreliable
                    .lock()
                    .ok()
                    .and_then(|map| map.get(&id).cloned());
                entry.controller.send(|| {
                    sender.is_some_and(|sender| {
                        sender.replace(
                            outbound
                                .clone()
                                .fenced(entry.controller.fence(), entry.controller.delivery_gate()),
                        )
                    })
                })
            }
            Some(entry) => entry.controller.send(|| {
                entry
                    .handle
                    .outbound
                    .try_send(
                        outbound
                            .clone()
                            .fenced(entry.controller.fence(), entry.controller.delivery_gate()),
                    )
                    .is_ok()
            }),
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

    fn authenticated_handle(
        id: u64,
        user: &str,
        session: &str,
    ) -> (SessionHandle, mpsc::Receiver<Outbound>) {
        let (mut handle, receiver) = handle(id, TransportKind::WebSocket);
        handle.identity = Some(ParticipantIdentity {
            user_id: UserId::new(user).expect("user"),
            session_id: SessionId::new(session).expect("session"),
            expires_at: TimestampMillis::from_unix_millis(10_000),
        });
        (handle, receiver)
    }

    #[tokio::test]
    async fn exact_session_close_does_not_conflate_same_user_devices() {
        let registry = SessionRegistry::new();
        let (first, _first_rx) = authenticated_handle(1, "same-user", "session-a");
        let (second, mut second_rx) = authenticated_handle(2, "same-user", "session-b");
        registry.register(first);
        registry.register(second);

        let session_a = SessionId::new("session-a").expect("session");
        let closed = registry.close_session(&session_a, "revoke-1", None).await;
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].1, CloseDisposition::Closing);
        assert!(!registry.send_to(
            ParticipantId(1),
            &Outbound::reliable(Envelope::new(1, b"blocked".to_vec()))
        ));
        assert!(registry.send_to(
            ParticipantId(2),
            &Outbound::reliable(Envelope::new(1, b"allowed".to_vec()))
        ));
        assert_eq!(
            second_rx
                .try_recv()
                .expect("other device remains active")
                .envelope
                .body,
            &b"allowed"[..]
        );
    }

    #[tokio::test]
    async fn duplicate_close_and_stale_generation_are_harmless() {
        let registry = SessionRegistry::new();
        let (handle, _receiver) = authenticated_handle(1, "u", "session-a");
        registry.register(handle);
        let session = SessionId::new("session-a").expect("session");
        let generation = registry.connections_for_session(&session)[0].generation();
        assert_eq!(
            registry
                .close_session(&session, "revoke-1", Some(generation))
                .await[0]
                .1,
            CloseDisposition::Closing
        );
        assert_eq!(
            registry
                .close_session(&session, "revoke-1", Some(generation))
                .await[0]
                .1,
            CloseDisposition::Duplicate
        );
        assert_eq!(
            registry
                .close_session(&session, "revoke-2", Some(generation + 1))
                .await[0]
                .1,
            CloseDisposition::Stale
        );
    }

    #[tokio::test]
    async fn close_fences_every_outbound_class_and_invalidates_queued_delivery() {
        let registry = SessionRegistry::new();
        let (handle, mut reliable) = authenticated_handle(1, "u", "session-a");
        let latest = registry.register(handle);
        let id = ParticipantId(1);

        // Queue both classes before close. Reliable entries may already have
        // reached the transport receiver, so they carry the close fence; latest
        // entries are physically discarded from their mailbox.
        assert!(registry.send_to(id, &Outbound::reliable(Envelope::new(1, &b"queued"[..]))));
        assert!(registry.send_to(id, &Outbound::unreliable(Envelope::new(2, &b"latest"[..]))));
        let session = SessionId::new("session-a").expect("session");
        assert_eq!(
            registry.close_session(&session, "revoke-1", None).await[0].1,
            CloseDisposition::Closing
        );

        assert!(!registry.accepts_work(id));
        assert!(!registry.send_to(id, &Outbound::reliable(Envelope::new(3, &b"reliable"[..]))));
        assert!(!registry.send_to(
            id,
            &Outbound::unreliable(Envelope::new(4, &b"unreliable"[..]))
        ));
        // `send_to` is also the registry-owned path used by delayed raw/domain
        // async replies, so it receives the same controller fence.
        assert!(!registry.send_to(id, &Outbound::reliable(Envelope::new(5, &b"async"[..]))));

        assert!(
            !reliable
                .recv()
                .await
                .expect("queued reliable")
                .is_deliverable(),
            "transport must discard a reliable envelope dequeued before close"
        );
        assert!(matches!(
            latest.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn initial_auth_result_is_fenced_for_every_transport_without_closing_siblings() {
        for kind in [
            TransportKind::WebSocket,
            TransportKind::Quic,
            TransportKind::WebTransport,
        ] {
            let registry = SessionRegistry::new();
            let (revoked, mut revoked_rx) = authenticated_handle(1, "same-user", "session-a");
            let (sibling, mut sibling_rx) = authenticated_handle(2, "same-user", "session-b");
            let auth_result = Outbound::reliable(Envelope::new(
                citadel_wire::protocol::KIND_AUTH_RESULT,
                b"accepted".to_vec(),
            ));

            // This models the transport registration boundary: the auth reply
            // is already in the writer queue, but is controlled by the same
            // fence that a concurrent revocation closes.
            registry.register_with_initial(SessionHandle { kind, ..revoked }, Some(auth_result));
            registry.register(SessionHandle { kind, ..sibling });

            let session_a = SessionId::new("session-a").expect("session");
            assert_eq!(
                registry
                    .close_session(&session_a, "revoke-race", None)
                    .await[0]
                    .1,
                CloseDisposition::Closing,
                "{kind:?} must linearize close after initial auth enqueue"
            );
            let queued_auth = revoked_rx.recv().await.expect("queued auth result");
            assert_eq!(
                queued_auth.envelope.kind,
                citadel_wire::protocol::KIND_AUTH_RESULT
            );
            assert!(
                !queued_auth.is_deliverable(),
                "{kind:?} writer must reject the auth result after revocation"
            );

            assert!(registry.send_to(
                ParticipantId(2),
                &Outbound::reliable(Envelope::new(1, b"sibling-active".to_vec()))
            ));
            assert_eq!(
                sibling_rx
                    .recv()
                    .await
                    .expect("sibling delivery")
                    .envelope
                    .body,
                &b"sibling-active"[..]
            );
        }
    }

    #[tokio::test]
    async fn delivery_lease_and_revocation_have_no_check_then_io_gap_for_every_transport() {
        for kind in [
            TransportKind::WebSocket,
            TransportKind::Quic,
            TransportKind::WebTransport,
        ] {
            let registry = SessionRegistry::new();
            let (handle, mut rx) = authenticated_handle(1, "same-user", "session-a");
            registry.register_with_initial(
                SessionHandle { kind, ..handle },
                Some(Outbound::reliable(Envelope::new(
                    citadel_wire::protocol::KIND_AUTH_RESULT,
                    b"accepted".to_vec(),
                ))),
            );
            let outbound = rx.recv().await.expect("initial auth result");
            // Model a writer that has dequeued the reply and is poised at the
            // transport I/O boundary. Revocation cannot complete until this
            // lease (and therefore the modeled I/O) completes.
            let lease = outbound.acquire_delivery().await.expect("delivery lease");
            let session = SessionId::new("session-a").expect("session");
            let close_registry = registry.clone();
            let close = tokio::spawn(async move {
                close_registry
                    .close_session(&session, "revoke-race", None)
                    .await
            });
            tokio::task::yield_now().await;
            assert!(
                !close.is_finished(),
                "{kind:?} close must wait for in-flight transport I/O rather than race a pre-check"
            );
            drop(lease); // modeled I/O completion is the delivery linearization point
            assert_eq!(
                close.await.expect("close task")[0].1,
                CloseDisposition::Closing
            );
            assert!(
                outbound.acquire_delivery().await.is_none(),
                "{kind:?} no second/late auth-result write is admitted after revocation"
            );
        }
    }

    #[tokio::test]
    async fn revocation_before_publication_blocks_auth_result_but_not_sibling_or_new_generation() {
        let registry = SessionRegistry::new();
        let revoked = SessionId::new("session-a").expect("session");
        // There is deliberately no registration yet: this is the adversarial
        // durable-revoke-before-publication interleaving.
        assert!(
            registry
                .close_session(&revoked, "revoke-before-register", None)
                .await
                .is_empty()
        );

        for kind in [
            TransportKind::WebSocket,
            TransportKind::Quic,
            TransportKind::WebTransport,
        ] {
            let (late, mut late_rx) = authenticated_handle(1, "same-user", "session-a");
            registry.register_with_initial(
                SessionHandle { kind, ..late },
                Some(Outbound::reliable(Envelope::new(
                    citadel_wire::protocol::KIND_AUTH_RESULT,
                    b"must-not-deliver".to_vec(),
                ))),
            );
            assert!(!registry.accepts_work(ParticipantId(1)));
            assert!(
                late_rx.try_recv().is_err(),
                "{kind:?} must not enqueue an auth result after pre-publication revocation"
            );
        }

        let (sibling, mut sibling_rx) = authenticated_handle(2, "same-user", "session-b");
        registry.register(sibling);
        assert!(registry.send_to(
            ParticipantId(2),
            &Outbound::reliable(Envelope::new(1, b"sibling-stays-live".to_vec()))
        ));
        assert_eq!(
            sibling_rx.recv().await.expect("sibling").envelope.body,
            &b"sibling-stays-live"[..]
        );
    }

    #[tokio::test]
    async fn stale_generation_does_not_fence_replacement_initial_auth_result() {
        let registry = SessionRegistry::new();
        let (first, _first_rx) = authenticated_handle(1, "u", "session-a");
        registry.register(first);
        let session = SessionId::new("session-a").expect("session");
        let stale_generation = registry.connections_for_session(&session)[0].generation();

        let (replacement, mut replacement_rx) = authenticated_handle(1, "u", "session-a");
        registry.register_with_initial(
            replacement,
            Some(Outbound::reliable(Envelope::new(
                citadel_wire::protocol::KIND_AUTH_RESULT,
                b"replacement".to_vec(),
            ))),
        );
        assert_eq!(
            registry
                .close_session(&session, "stale-route", Some(stale_generation))
                .await[0]
                .1,
            CloseDisposition::Stale
        );
        assert!(
            replacement_rx
                .recv()
                .await
                .expect("replacement auth result")
                .is_deliverable(),
            "a stale close must not suppress the replacement generation's auth result"
        );
    }

    #[tokio::test]
    async fn stale_generation_cannot_close_reconnected_participant() {
        let registry = SessionRegistry::new();
        let (first, _first_rx) = authenticated_handle(1, "u", "session-a");
        registry.register(first);
        let session = SessionId::new("session-a").expect("session");
        let stale_generation = registry.connections_for_session(&session)[0].generation();

        // Transport ids can be reused after reconnect; only the newest
        // generation is eligible for a routed close command.
        let (replacement, mut replacement_rx) = authenticated_handle(1, "u", "session-a");
        registry.register(replacement);
        assert_eq!(
            registry
                .close_session(&session, "old-route", Some(stale_generation))
                .await[0]
                .1,
            CloseDisposition::Stale
        );
        assert!(registry.send_to(
            ParticipantId(1),
            &Outbound::reliable(Envelope::new(1, &b"replacement-active"[..]))
        ));
        assert_eq!(
            replacement_rx
                .try_recv()
                .expect("replacement delivery")
                .envelope
                .body,
            &b"replacement-active"[..]
        );
    }

    #[test]
    fn cleanup_is_claimed_once_when_close_races_transport_teardown() {
        let registry = SessionRegistry::new();
        let (handle, _receiver) = authenticated_handle(1, "u", "session-a");
        registry.register(handle);
        let id = ParticipantId(1);
        assert!(registry.claim_cleanup(id));
        assert!(!registry.claim_cleanup(id));
        assert!(registry.unregister(id).is_some());
        assert!(registry.unregister(id).is_none());
    }
}
