//! Transport-agnostic session registry for the realtime gateway.
//!
//! Each accepted connection (QUIC or WebSocket) registers a [`SessionHandle`]
//! whose only transport-specific dependency is a bounded
//! `tokio::mpsc::Sender<Outbound>`: the connection's write task drains that
//! channel and writes to its concrete socket. The registry therefore routes
//! purely over abstract outbound sinks and never depends on a concrete
//! transport.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, MutexGuard, Notify, mpsc};

use citadel_wire::protocol::KIND_PEER_POSITION;

use crate::lifecycle::CancellationToken;
use crate::session::SessionId;
use crate::storage::UserId;
use crate::time::{Clock, DurationMillis, SystemClock, TimestampMillis};
use crate::transport::{Delivery, Envelope, TransportKind};

use super::identity::{IdentityLifecycle, ResumeResult, ResumeSecret};

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
/// Reconnect grace is server-bounded even for trusted callers. It is long
/// enough for a transient transport loss, but cannot retain identity/ticket
/// state indefinitely.
const MAX_RECONNECT_GRACE_MS: u64 = 30_000;

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

/// Result of a time-checked authenticated registration. The receiver remains
/// transport-owned even when activation is rejected, so callers can preserve
/// their normal teardown shape without exposing a partially registered socket.
#[derive(Debug)]
pub struct SessionRegistration {
    /// Latest-wins mailbox paired with the attempted transport registration.
    pub unreliable: LatestOutboundReceiver,
    /// Whether this participant became the live connection for its exact
    /// authenticated session (guests are always accepted by this layer).
    pub accepted: bool,
    /// The connection fenced by an accepted activation of the same exact session.
    pub replaced: Option<ConnectionRef>,
    /// Deferred cleanup for the fenced generation, released only after that
    /// generation's inbound supersession gate has drained.
    pub replaced_cleanup: Option<ReplacedTransportCleanup>,
    /// Cancellation signal owned by this transport loop. A later activation of
    /// the exact same session triggers it before the old participant is removed.
    pub superseded: CancellationToken,
    /// Serializes synchronous datagram decode/metrics with same-session
    /// replacement cancellation.
    pub supersession_gate: Arc<Mutex<()>>,
    /// Serializes application writes and WebSocket control flushes with
    /// cancellation. The write owner keeps this asynchronous gate through the
    /// final transport flush; cancellation cannot publish until it releases.
    pub transport_write_gate: Arc<AsyncMutex<()>>,
    /// Set synchronously when cancellation is requested, so a writer that has
    /// not entered the transport-write gate cannot begin another frame while a
    /// previous write drains.
    pub superseding: Arc<AtomicBool>,
    /// Fires only after cancellation releases this generation's inbound gate.
    pub inbound_supersession_drained: CancellationToken,
}

/// A fenced generation's deferred Gateway-cleanup admission.
///
/// The registry publishes this synchronously with replacement; it becomes ready
/// only after the old receive gate drains. Waiting on it never retains a
/// registry lock, so a gateway handoff can safely consult the registry.
#[derive(Debug, Clone)]
pub struct ReplacedTransportCleanup {
    participant_id: ParticipantId,
    inbound_supersession_drained: CancellationToken,
}

impl ReplacedTransportCleanup {
    #[must_use]
    pub(crate) const fn participant_id(&self) -> ParticipantId {
        self.participant_id
    }

    pub async fn wait_for_inbound_drain(&self) {
        self.inbound_supersession_drained.cancelled().await;
    }

    #[must_use]
    pub(crate) fn is_ready(&self) -> bool {
        self.inbound_supersession_drained.is_cancelled()
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
    /// Serializes Gateway-owned open/presence/join effects with replacement and
    /// cleanup for this exact connection generation.
    gateway_registration: Mutex<GatewayRegistrationState>,
}

#[derive(Debug)]
struct ConnectionControl {
    state: ConnectionState,
    cleanup_claimed: bool,
    close_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayRegistrationState {
    Pending,
    Running,
    Registered,
    Closed { registered: bool },
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
            gateway_registration: Mutex::new(GatewayRegistrationState::Pending),
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

    /// Run Gateway registration side effects only while this exact generation
    /// remains current. Replacement and cleanup use the same gate, so neither
    /// can publish a successor or a close between ownership validation and Join.
    fn run_gateway_registration<F, C>(&self, is_current: C, effects: F) -> bool
    where
        F: FnOnce(),
        C: FnOnce() -> bool,
    {
        let Ok(mut registration) = self.gateway_registration.lock() else {
            return false;
        };
        if !matches!(*registration, GatewayRegistrationState::Pending)
            || !self.accepts_work()
            || !is_current()
        {
            return false;
        }
        *registration = GatewayRegistrationState::Running;
        effects();
        *registration = GatewayRegistrationState::Registered;
        true
    }

    /// Prevent this generation from starting Gateway side effects. If an open
    /// sequence is already running this waits for it without holding a registry
    /// lock, because lifecycle dispatch can consult the registry.
    fn retire_gateway_registration(&self) -> bool {
        let Ok(mut registration) = self.gateway_registration.lock() else {
            return false;
        };
        match *registration {
            GatewayRegistrationState::Pending => {
                *registration = GatewayRegistrationState::Closed { registered: false };
                false
            }
            GatewayRegistrationState::Running => unreachable!("registration gate is held"),
            GatewayRegistrationState::Registered => {
                *registration = GatewayRegistrationState::Closed { registered: true };
                true
            }
            GatewayRegistrationState::Closed { registered } => registered,
        }
    }
}

#[derive(Debug, Clone)]
struct RegisteredSession {
    handle: SessionHandle,
    controller: Arc<RealtimeConnectionController>,
    generation: u64,
    lifecycle_generation: Option<u64>,
    superseded: CancellationToken,
    supersession_gate: Arc<Mutex<()>>,
    transport_write_gate: Arc<AsyncMutex<()>>,
    superseding: Arc<AtomicBool>,
    inbound_supersession_drained: CancellationToken,
}

/// The cancellation state of a replaced transport generation. Registry locks
/// protect selecting this exact generation, but they must be released before
/// waiting for either of these transport gates: inbound handoff holds the
/// supersession gate while it consults the registry.
#[derive(Clone)]
struct SupersededTransport {
    controller: Arc<RealtimeConnectionController>,
    superseded: CancellationToken,
    supersession_gate: Arc<Mutex<()>>,
    transport_write_gate: Arc<AsyncMutex<()>>,
    superseding: Arc<AtomicBool>,
    inbound_supersession_drained: CancellationToken,
}

/// Linearize transport cancellation with the receive path for one connection
/// generation. Receive work holds this gate through decode and metrics, so a
/// completed call guarantees that later receive work observes cancellation.
fn cancel_transport_under_gate(
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
    inbound_supersession_drained: &CancellationToken,
) {
    let _gate = supersession_gate.lock();
    superseded.cancel();
    drop(_gate);
    inbound_supersession_drained.cancel();
}

/// Publish cancellation only after every admitted transport write has flushed.
/// `superseding` closes admission before the await, so no additional application
/// frame can start while the already-admitted write drains.
async fn cancel_transport_after_writes(
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
    transport_write_gate: &Arc<AsyncMutex<()>>,
    superseding: &Arc<AtomicBool>,
    inbound_supersession_drained: &CancellationToken,
) {
    superseding.store(true, Ordering::Release);
    let _write = transport_write_gate.lock().await;
    cancel_transport_under_gate(superseded, supersession_gate, inbound_supersession_drained);
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
    /// Serializes every authenticated validation→publication transition. This
    /// makes the lifecycle generation observed during validation inseparable from
    /// the local active-session mapping it publishes.
    authenticated_activation: Arc<Mutex<()>>,
    /// One locally active transport participant per exact account session. This
    /// is intentionally keyed by `SessionId`, never user id: sibling devices
    /// remain independent.
    active_authenticated: Arc<Mutex<HashMap<SessionId, ConnectionRef>>>,
    /// The deterministic, single-node reconnect state machine. It owns resume
    /// ticket consumption and grace expiry; this registry owns local transports.
    identity_lifecycle: IdentityLifecycle,
    /// Durable revocation has linearized for these exact sessions until the
    /// session's authoritative access expiry. Keeping the tombstone under the
    /// session-map lock closes the publication race without retaining a terminal
    /// session id past the time its credential can be accepted.
    revoked_sessions: Arc<Mutex<HashMap<SessionId, TimestampMillis>>>,
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
        if handle.identity.is_some() {
            return self
                .register_legacy_authenticated(handle, initials)
                .unreliable;
        }
        self.register_unauthenticated_with_initials(handle, initials)
    }

    fn register_unauthenticated_with_initials(
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
                    lifecycle_generation: None,
                    superseded: CancellationToken::new(),
                    supersession_gate: Arc::new(Mutex::new(())),
                    transport_write_gate: Arc::new(AsyncMutex::new(())),
                    superseding: Arc::new(AtomicBool::new(false)),
                    inbound_supersession_drained: CancellationToken::new(),
                },
            );
        }
        receiver
    }

    /// Compatibility registration for a caller that has already authenticated
    /// the handle. It intentionally retains no expiry check (the historic API
    /// has no `now` argument), but it must publish through the same exact-session
    /// active map and lifecycle generation as transport registration.
    fn register_legacy_authenticated(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
    ) -> SessionRegistration {
        let Ok(_activation) = self.authenticated_activation.lock() else {
            return Self::rejected_registration();
        };
        let Some(identity) = handle.identity.as_ref() else {
            return Self::rejected_registration();
        };
        if self.is_session_revoked(&identity.session_id, SystemClock.now()) {
            return Self::rejected_registration();
        }
        let Some(lifecycle_generation) = self.identity_lifecycle.activate_at(
            identity.user_id.clone(),
            identity.session_id.clone(),
            handle.id,
            0,
            SystemClock.now(),
        ) else {
            return Self::rejected_registration();
        };
        self.publish_authenticated(
            handle,
            initials,
            lifecycle_generation,
            SystemClock.now(),
            || {},
        )
    }

    /// Register an authenticated transport at a caller-supplied time. It
    /// rejects an identity at its exact expiry boundary, atomically makes one
    /// participant current for the exact `SessionId`, and fences any replaced
    /// participant before exposing the new one. Legacy registration methods
    /// above intentionally retain their historical behavior for compatibility.
    pub fn register_session_at(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
        now: TimestampMillis,
    ) -> SessionRegistration {
        let Some(identity) = handle.identity.as_ref() else {
            return SessionRegistration {
                unreliable: self.register_unauthenticated_with_initials(handle, initials),
                accepted: true,
                replaced: None,
                replaced_cleanup: None,
                superseded: CancellationToken::new(),
                supersession_gate: Arc::new(Mutex::new(())),
                transport_write_gate: Arc::new(AsyncMutex::new(())),
                superseding: Arc::new(AtomicBool::new(false)),
                inbound_supersession_drained: CancellationToken::new(),
            };
        };
        let user_id = identity.user_id.clone();
        let session_id = identity.session_id.clone();
        self.register_authenticated_at(handle, initials, now, user_id, session_id, || {}, || {})
    }

    fn register_authenticated_at<F, H>(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
        now: TimestampMillis,
        user_id: UserId,
        session_id: SessionId,
        after_validation: F,
        before_active_publish: H,
    ) -> SessionRegistration
    where
        F: FnOnce(),
        H: FnOnce(),
    {
        let Ok(_activation) = self.authenticated_activation.lock() else {
            return Self::rejected_registration();
        };
        let Some(identity) = handle.identity.as_ref() else {
            return Self::rejected_registration();
        };
        if identity.user_id != user_id
            || identity.session_id != session_id
            || identity.expires_at <= now
            || self.is_session_revoked(&session_id, now)
        {
            return Self::rejected_registration();
        }
        let Some(lifecycle_generation) = self
            .identity_lifecycle
            .activate_at(user_id, session_id, handle.id, 0, now)
        else {
            return Self::rejected_registration();
        };
        after_validation();
        self.publish_authenticated(
            handle,
            initials,
            lifecycle_generation,
            now,
            before_active_publish,
        )
    }

    #[cfg(test)]
    fn register_session_at_after_validation<F>(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
        now: TimestampMillis,
        after_validation: F,
    ) -> SessionRegistration
    where
        F: FnOnce(),
    {
        let Some(identity) = handle.identity.as_ref() else {
            return SessionRegistration {
                unreliable: self.register_unauthenticated_with_initials(handle, initials),
                accepted: true,
                replaced: None,
                replaced_cleanup: None,
                superseded: CancellationToken::new(),
                supersession_gate: Arc::new(Mutex::new(())),
                transport_write_gate: Arc::new(AsyncMutex::new(())),
                superseding: Arc::new(AtomicBool::new(false)),
                inbound_supersession_drained: CancellationToken::new(),
            };
        };
        let user_id = identity.user_id.clone();
        let session_id = identity.session_id.clone();
        self.register_authenticated_at(
            handle,
            initials,
            now,
            user_id,
            session_id,
            after_validation,
            || {},
        )
    }

    #[cfg(test)]
    fn register_session_at_before_active_publish<F>(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
        now: TimestampMillis,
        before_active_publish: F,
    ) -> SessionRegistration
    where
        F: FnOnce(),
    {
        let Some(identity) = handle.identity.as_ref() else {
            return Self::rejected_registration();
        };
        let user_id = identity.user_id.clone();
        let session_id = identity.session_id.clone();
        self.register_authenticated_at(
            handle,
            initials,
            now,
            user_id,
            session_id,
            || {},
            before_active_publish,
        )
    }

    /// Redeem a one-use resume secret against the exact currently validated
    /// session. `handle.identity` is the result of current authentication, not
    /// client supplied resume metadata; expiry is checked before redemption.
    pub fn resume_session_at(
        &self,
        handle: SessionHandle,
        secret: ResumeSecret,
        now: TimestampMillis,
    ) -> SessionRegistration {
        let Ok(_activation) = self.authenticated_activation.lock() else {
            return Self::rejected_registration();
        };
        let Some(identity) = handle.identity.as_ref() else {
            return Self::rejected_registration();
        };
        if identity.expires_at <= now {
            return Self::rejected_registration();
        }
        if self.is_session_revoked(&identity.session_id, now) {
            return Self::rejected_registration();
        }
        let ResumeResult::Accepted {
            generation: lifecycle_generation,
        } = self.identity_lifecycle.resume(
            secret,
            Some(&identity.user_id),
            Some(&identity.session_id),
            handle.id,
            0,
            now,
        )
        else {
            return Self::rejected_registration();
        };
        self.publish_authenticated(handle, Vec::new(), lifecycle_generation, now, || {})
    }

    fn is_session_revoked(&self, session_id: &SessionId, now: TimestampMillis) -> bool {
        let Ok(mut revoked) = self.revoked_sessions.lock() else {
            return true;
        };
        revoked.retain(|_, expires_at| *expires_at > now);
        revoked.contains_key(session_id)
    }

    fn rejected_registration() -> SessionRegistration {
        let (_, unreliable) = latest_outbound_channel();
        SessionRegistration {
            unreliable,
            accepted: false,
            replaced: None,
            replaced_cleanup: None,
            superseded: CancellationToken::new(),
            supersession_gate: Arc::new(Mutex::new(())),
            transport_write_gate: Arc::new(AsyncMutex::new(())),
            superseding: Arc::new(AtomicBool::new(false)),
            inbound_supersession_drained: CancellationToken::new(),
        }
    }

    fn publish_authenticated<F>(
        &self,
        handle: SessionHandle,
        initials: Vec<Outbound>,
        lifecycle_generation: u64,
        now: TimestampMillis,
        before_active_publish: F,
    ) -> SessionRegistration
    where
        F: FnOnce(),
    {
        let (sender, receiver) = latest_outbound_channel();
        let id = handle.id;
        let Some(session_id) = handle
            .identity
            .as_ref()
            .map(|identity| identity.session_id.clone())
        else {
            return Self::rejected_registration();
        };
        let controller = Arc::new(RealtimeConnectionController::new());
        let superseded = CancellationToken::new();
        let supersession_gate = Arc::new(Mutex::new(()));
        let transport_write_gate = Arc::new(AsyncMutex::new(()));
        let superseding = Arc::new(AtomicBool::new(false));
        let inbound_supersession_drained = CancellationToken::new();
        let previous_before_publish = {
            let Ok(sessions) = self.sessions.lock() else {
                return Self::rejected_registration();
            };
            let Ok(active) = self.active_authenticated.lock() else {
                return Self::rejected_registration();
            };
            active.get(&session_id).and_then(|previous| {
                let entry = sessions.get(&previous.id)?;
                (entry.generation == previous.generation).then(|| SupersededTransport {
                    controller: Arc::clone(&entry.controller),
                    superseded: entry.superseded.clone(),
                    supersession_gate: Arc::clone(&entry.supersession_gate),
                    transport_write_gate: Arc::clone(&entry.transport_write_gate),
                    superseding: Arc::clone(&entry.superseding),
                    inbound_supersession_drained: entry.inbound_supersession_drained.clone(),
                })
            })
        };
        if let Some(previous) = &previous_before_publish {
            // Close Gateway registration ownership and receive admission before
            // exposing the successor through the exact-session active mapping.
            // This deliberately waits without a registry lock: lifecycle Join
            // itself can consult the registry.
            previous.controller.retire_gateway_registration();
            previous
                .controller
                .accepting
                .store(false, Ordering::Release);
            previous.superseding.store(true, Ordering::Release);
        }
        let (replaced, previous) = {
            let Ok(mut sessions) = self.sessions.lock() else {
                return Self::rejected_registration();
            };
            // `close_session` takes the session-map lock before publishing a
            // tombstone. Checking while this lock is held closes the race between
            // current authentication and transport publication for both activate
            // and resume paths.
            if self.is_session_revoked(&session_id, now) {
                return Self::rejected_registration();
            }
            let Ok(mut active) = self.active_authenticated.lock() else {
                return Self::rejected_registration();
            };
            // Do not publish an unreachable mailbox sender. Every fallible
            // validation/publication lock has succeeded before this insertion, so a
            // rejected authenticated activation leaves no `LatestOutboundSender`
            // behind for its participant id.
            let Ok(mut unreliable) = self.unreliable.lock() else {
                return Self::rejected_registration();
            };
            unreliable.insert(id, sender);
            drop(unreliable);
            let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
            let current = ConnectionRef { id, generation };
            before_active_publish();
            let replaced = active.insert(session_id, current);
            let previous = replaced.and_then(|previous| {
                let entry = sessions.get(&previous.id)?;
                (entry.generation == previous.generation).then(|| SupersededTransport {
                    controller: Arc::clone(&entry.controller),
                    superseded: entry.superseded.clone(),
                    supersession_gate: Arc::clone(&entry.supersession_gate),
                    transport_write_gate: Arc::clone(&entry.transport_write_gate),
                    superseding: Arc::clone(&entry.superseding),
                    inbound_supersession_drained: entry.inbound_supersession_drained.clone(),
                })
            });
            for initial in initials {
                let _ = handle
                    .outbound
                    .try_send(initial.fenced(controller.fence(), controller.delivery_gate()));
            }
            sessions.insert(
                id,
                RegisteredSession {
                    handle,
                    controller: Arc::clone(&controller),
                    generation,
                    lifecycle_generation: Some(lifecycle_generation),
                    superseded: superseded.clone(),
                    supersession_gate: Arc::clone(&supersession_gate),
                    transport_write_gate: Arc::clone(&transport_write_gate),
                    superseding: Arc::clone(&superseding),
                    inbound_supersession_drained: inbound_supersession_drained.clone(),
                },
            );
            (replaced, previous)
        };

        let replaced_cleanup = previous.as_ref().map(|previous| ReplacedTransportCleanup {
            participant_id: replaced
                .expect("previous transport has a connection reference")
                .id,
            inbound_supersession_drained: previous.inbound_supersession_drained.clone(),
        });
        if let Some(previous) = previous {
            // Receive admission was synchronously closed before active-map
            // publication. Cancellation may still wait for a previously
            // admitted transport write or inbound handoff. If neither is held,
            // cancellation publishes immediately; otherwise it is scheduled so a
            // synchronous registration never blocks a Tokio worker that must run
            // the in-flight inbound handoff.
            let cancelled_now = match previous.transport_write_gate.try_lock() {
                Ok(_write) => match previous.supersession_gate.try_lock() {
                    Ok(_receive) => {
                        previous.superseded.cancel();
                        drop(_receive);
                        previous.inbound_supersession_drained.cancel();
                        true
                    }
                    Err(_) => false,
                },
                Err(_) => false,
            };
            if !cancelled_now {
                tokio::spawn(async move {
                    cancel_transport_after_writes(
                        &previous.superseded,
                        &previous.supersession_gate,
                        &previous.transport_write_gate,
                        &previous.superseding,
                        &previous.inbound_supersession_drained,
                    )
                    .await;
                });
            }
        }
        SessionRegistration {
            unreliable: receiver,
            accepted: true,
            replaced,
            replaced_cleanup,
            superseded,
            supersession_gate,
            transport_write_gate,
            superseding,
            inbound_supersession_drained,
        }
    }

    /// Issue a single-use resume ticket and mark only this exact active session
    /// suspect. A stale/replaced participant cannot start grace for its successor.
    pub fn begin_reconnect_grace(
        &self,
        id: ParticipantId,
        secret: ResumeSecret,
        requested_until: TimestampMillis,
    ) -> bool {
        self.begin_reconnect_grace_at(id, secret, SystemClock.now(), requested_until)
    }

    /// Deterministic form of [`Self::begin_reconnect_grace`]. The requested
    /// absolute expiry is capped to a fixed server maximum from `now`; an
    /// overflowing bound fails closed rather than turning into an unbounded
    /// ticket.
    pub fn begin_reconnect_grace_at(
        &self,
        id: ParticipantId,
        secret: ResumeSecret,
        now: TimestampMillis,
        requested_until: TimestampMillis,
    ) -> bool {
        let Ok(max_until) = now.checked_add(DurationMillis::from_millis(MAX_RECONNECT_GRACE_MS))
        else {
            return false;
        };
        let until = requested_until.min(max_until);
        let Some((identity, connection, lifecycle_generation)) =
            self.sessions.lock().ok().and_then(|sessions| {
                let entry = sessions.get(&id)?;
                Some((
                    entry.handle.identity.clone()?,
                    ConnectionRef {
                        id,
                        generation: entry.generation,
                    },
                    entry.lifecycle_generation?,
                ))
            })
        else {
            return false;
        };
        if !self
            .active_authenticated
            .lock()
            .is_ok_and(|active| active.get(&identity.session_id) == Some(&connection))
        {
            return false;
        }
        self.identity_lifecycle.issue_resume(
            &identity.session_id,
            lifecycle_generation,
            secret,
            until,
        ) && self
            .identity_lifecycle
            .mark_suspect(&identity.session_id, lifecycle_generation, until)
    }

    /// Sweep only expired grace records. The returned session IDs are exact and
    /// deterministic; callers that own Gateway lifecycle work can then clean
    /// their matching transport participants without touching sibling devices.
    #[must_use]
    pub fn expire_reconnect_grace_at(&self, now: TimestampMillis) -> Vec<SessionId> {
        self.identity_lifecycle.expire_grace(now)
    }

    /// Purge durable session-revocation tombstones at a supplied instant. Both
    /// the publication barrier and identity lifecycle retain the same exact
    /// session ids, so sweep them together to keep memory bounded while idle.
    pub fn expire_revocation_tombstones_at(&self, now: TimestampMillis) -> usize {
        let registry_reclaimed = self
            .revoked_sessions
            .lock()
            .map(|mut tombstones| {
                let before = tombstones.len();
                tombstones.retain(|_, expires_at| *expires_at > now);
                before.saturating_sub(tombstones.len())
            })
            .unwrap_or(0);
        let lifecycle_reclaimed = self.identity_lifecycle.expire_revocations_at(now);
        registry_reclaimed.max(lifecycle_reclaimed)
    }

    #[cfg(test)]
    pub(crate) fn reconnect_grace_count(&self) -> usize {
        self.identity_lifecycle.grace_count()
    }

    #[cfg(test)]
    pub(crate) fn revocation_tombstone_count(&self) -> usize {
        self.revoked_sessions
            .lock()
            .map(|tombstones| tombstones.len())
            .unwrap_or(0)
    }

    /// Run Gateway-owned registration effects if this exact generation is still
    /// the live participant. The controller gate stays held through `effects`;
    /// callers must not hold registry locks while invoking this method.
    pub(crate) fn run_gateway_registration<F>(&self, id: ParticipantId, effects: F) -> bool
    where
        F: FnOnce(),
    {
        let Some((controller, generation, authenticated)) =
            self.sessions.lock().ok().and_then(|sessions| {
                let entry = sessions.get(&id)?;
                Some((
                    Arc::clone(&entry.controller),
                    entry.generation,
                    entry.handle.identity.is_some(),
                ))
            })
        else {
            return false;
        };
        controller.run_gateway_registration(
            || {
                let Ok(sessions) = self.sessions.lock() else {
                    return false;
                };
                let Some(entry) = sessions.get(&id) else {
                    return false;
                };
                if entry.generation != generation
                    || !Arc::ptr_eq(&entry.controller, &controller)
                    || !entry.controller.accepts_work()
                {
                    return false;
                }
                if !authenticated {
                    return true;
                }
                let Some(identity) = &entry.handle.identity else {
                    return false;
                };
                self.active_authenticated.lock().is_ok_and(|active| {
                    active.get(&identity.session_id) == Some(&ConnectionRef { id, generation })
                })
            },
            effects,
        )
    }

    /// Stop this generation from starting Gateway registration side effects and
    /// report whether its open gauges/presence/join effects had completed.
    pub(crate) fn retire_gateway_registration(&self, id: ParticipantId) -> bool {
        self.sessions
            .lock()
            .ok()
            .and_then(|sessions| sessions.get(&id).map(|entry| Arc::clone(&entry.controller)))
            .is_some_and(|controller| controller.retire_gateway_registration())
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
                (entry.handle, entry.generation, entry.lifecycle_generation)
            }),
            Err(_) => None,
        };
        if let Some((handle, generation, lifecycle_generation)) = removed {
            if let Some(identity) = &handle.identity {
                // Ordinary teardown terminally removes only the live generation
                // it owns. `deactivate` preserves the entry if this participant
                // explicitly entered reconnect grace, and ignores a stale
                // teardown after a replacement activation.
                if let Some(lifecycle_generation) = lifecycle_generation {
                    self.identity_lifecycle
                        .deactivate(&identity.session_id, lifecycle_generation);
                }
                if let Ok(mut active) = self.active_authenticated.lock()
                    && active.get(&identity.session_id) == Some(&ConnectionRef { id, generation })
                {
                    active.remove(&identity.session_id);
                }
            }
            if let Ok(mut map) = self.unreliable.lock() {
                map.remove(&id);
            }
            Some(handle)
        } else {
            None
        }
    }

    /// Atomically claim the once-only gateway cleanup for a connection.
    pub fn claim_cleanup(&self, id: ParticipantId) -> bool {
        self.sessions
            .lock()
            .ok()
            .and_then(|map| map.get(&id).map(|entry| entry.controller.claim_cleanup()))
            .unwrap_or(false)
    }

    /// Close all local connections using an expiry from a currently registered
    /// authenticated session. Durable callers that already have the session
    /// record must use [`Self::close_session_at`] so an off-node/grace record
    /// retains its tombstone only to its authoritative expiry.
    pub async fn close_session(
        &self,
        session_id: &SessionId,
        command_id: &str,
        expected_generation: Option<u64>,
    ) -> Vec<(ConnectionRef, CloseDisposition)> {
        let expires_at = self
            .sessions
            .lock()
            .ok()
            .and_then(|sessions| {
                sessions.values().find_map(|entry| {
                    entry.handle.identity.as_ref().and_then(|identity| {
                        (&identity.session_id == session_id).then_some(identity.expires_at)
                    })
                })
            })
            .unwrap_or_else(|| SystemClock.now());
        self.close_session_at(
            session_id,
            command_id,
            expected_generation,
            expires_at,
            SystemClock.now(),
        )
        .await
    }

    /// Close all local connections and retain a durable revocation tombstone no
    /// later than the authoritative access-session expiry.
    pub async fn close_session_at(
        &self,
        session_id: &SessionId,
        command_id: &str,
        expected_generation: Option<u64>,
        expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> Vec<(ConnectionRef, CloseDisposition)> {
        let entries: Vec<_> = {
            let Ok(map) = self.sessions.lock() else {
                return Vec::new();
            };
            // A close without an owner generation is a durable, exact-session
            // revocation. It must outlive the local transport entry: reconnect
            // grace intentionally unregisters that entry while retaining only
            // lifecycle presence and its opaque ticket. A routed close must
            // instead prove its target is still the exact active connection;
            // an older transport can remain in `sessions` while a replacement
            // is current in `active_authenticated`.
            let revocation_targets_live_generation = match expected_generation {
                None => true,
                Some(generation) => map
                    .values()
                    .find_map(|entry| {
                        entry
                            .handle
                            .identity
                            .as_ref()
                            .is_some_and(|identity| {
                                &identity.session_id == session_id && entry.generation == generation
                            })
                            .then_some(ConnectionRef {
                                id: entry.handle.id,
                                generation: entry.generation,
                            })
                    })
                    .is_some_and(|target| {
                        self.active_authenticated
                            .lock()
                            .is_ok_and(|active| active.get(session_id) == Some(&target))
                    }),
            };
            if revocation_targets_live_generation {
                if let Ok(mut revoked) = self.revoked_sessions.lock() {
                    revoked.retain(|_, tombstone_expires_at| *tombstone_expires_at > now);
                    if expires_at > now {
                        revoked.insert(session_id.clone(), expires_at);
                    }
                }
                self.identity_lifecycle.revoke(session_id, expires_at, now);
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
                    (
                        reference,
                        Arc::clone(&entry.controller),
                        entry.generation,
                        entry.superseded.clone(),
                        Arc::clone(&entry.supersession_gate),
                        Arc::clone(&entry.transport_write_gate),
                        Arc::clone(&entry.superseding),
                        entry.inbound_supersession_drained.clone(),
                    )
                })
                .collect()
        };
        let mut closed = Vec::with_capacity(entries.len());
        for (
            reference,
            controller,
            generation,
            superseded,
            supersession_gate,
            transport_write_gate,
            superseding,
            inbound_supersession_drained,
        ) in entries
        {
            let result = if expected_generation.is_some_and(|value| value != generation) {
                CloseDisposition::Stale
            } else {
                // Close Gateway registration ownership before transport
                // cancellation begins. A registration that has published but
                // has not yet run Join now becomes a no-op; an already-running
                // one completes before this close linearizes.
                controller.retire_gateway_registration();
                // Mark admission closed, then wait for an already-admitted
                // application write/control flush before publishing cancellation.
                cancel_transport_after_writes(
                    &superseded,
                    &supersession_gate,
                    &transport_write_gate,
                    &superseding,
                    &inbound_supersession_drained,
                )
                .await;
                controller.close(command_id).await
            };
            closed.push((reference, result));
        }
        // Clear mailbox state only after releasing the session lock: authenticated
        // publication may be waiting for this mailbox lock while it holds that
        // session lock.
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
        let entry = self
            .sessions
            .lock()
            .ok()
            .and_then(|map| map.get(&id).cloned());
        let Some(entry) = entry else {
            return false;
        };
        if !entry.controller.accepts_work() {
            return false;
        }
        let Some(identity) = &entry.handle.identity else {
            return true;
        };
        // Historical registration entry points never populated the newer
        // exact-session lifecycle map. Keep their authenticated participants
        // live (and therefore able to drive gateway lifecycle/gauges/presence)
        // while their controller still enforces durable revocation.
        if entry.lifecycle_generation.is_none() {
            return true;
        }
        self.active_authenticated.lock().is_ok_and(|active| {
            active.get(&identity.session_id)
                == Some(&ConnectionRef {
                    id,
                    generation: entry.generation,
                })
        })
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

    #[test]
    fn normal_authenticated_unregister_removes_lifecycle_presence() {
        let registry = SessionRegistry::new();
        let session = SessionId::new("normal-disconnect").expect("session");
        let (handle, _receiver) = authenticated_handle(1, "user", "normal-disconnect");
        assert!(
            registry
                .register_session_at(handle, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted
        );
        assert!(registry.identity_lifecycle.presence(&session).is_some());

        assert!(registry.unregister(ParticipantId(1)).is_some());

        assert!(
            registry.identity_lifecycle.presence(&session).is_none(),
            "an ordinary disconnect must not retain lifecycle presence"
        );
    }

    #[test]
    fn legacy_authenticated_registration_is_fenced_by_exact_session_activation() {
        let registry = SessionRegistry::new();
        let (legacy, _legacy_rx) = authenticated_handle(1, "user", "shared-session");
        let (current, _current_rx) = authenticated_handle(2, "user", "shared-session");

        registry.register(legacy);
        assert!(
            registry.accepts_work(ParticipantId(1)),
            "a legacy authenticated connection starts active"
        );
        assert!(
            registry
                .register_session_at(current, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted
        );

        assert!(
            !registry.accepts_work(ParticipantId(1)),
            "legacy registration must share the exact-session active fence"
        );
        assert!(registry.accepts_work(ParticipantId(2)));
    }

    #[test]
    fn reconnect_grace_caps_requested_expiry_at_the_server_bound() {
        let registry = SessionRegistry::new();
        let (handle, _receiver) = authenticated_handle(1, "user", "bounded-grace");
        let now = TimestampMillis::from_unix_millis(10);
        assert!(
            registry
                .register_session_at(handle, Vec::new(), now)
                .accepted
        );

        assert!(registry.begin_reconnect_grace_at(
            ParticipantId(1),
            ResumeSecret::from_server_bytes(vec![8; 16]).expect("secret"),
            now,
            TimestampMillis::from_unix_millis(now.unix_millis() + MAX_RECONNECT_GRACE_MS + 1,),
        ));
        assert!(
            registry
                .expire_reconnect_grace_at(TimestampMillis::from_unix_millis(
                    now.unix_millis() + MAX_RECONNECT_GRACE_MS - 1,
                ))
                .is_empty()
        );
        assert_eq!(
            registry.expire_reconnect_grace_at(TimestampMillis::from_unix_millis(
                now.unix_millis() + MAX_RECONNECT_GRACE_MS,
            )),
            vec![SessionId::new("bounded-grace").expect("session")],
            "a trusted caller cannot retain grace beyond the server maximum"
        );
    }

    #[test]
    fn stale_authenticated_unregister_cannot_remove_replacement_presence() {
        let registry = SessionRegistry::new();
        let session = SessionId::new("replacement-disconnect").expect("session");
        let (older, _older_receiver) = authenticated_handle(1, "user", "replacement-disconnect");
        let (replacement, _replacement_receiver) =
            authenticated_handle(2, "user", "replacement-disconnect");
        assert!(
            registry
                .register_session_at(older, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted
        );
        assert!(
            registry
                .register_session_at(
                    replacement,
                    Vec::new(),
                    TimestampMillis::from_unix_millis(10)
                )
                .accepted
        );

        assert!(registry.unregister(ParticipantId(1)).is_some());

        assert_eq!(
            registry
                .identity_lifecycle
                .presence(&session)
                .expect("replacement remains live")
                .participant,
            ParticipantId(2)
        );
    }

    #[test]
    fn rejected_authenticated_publication_does_not_insert_an_unreliable_sender() {
        let registry = SessionRegistry::new();
        let session = SessionId::new("revoke-during-publication").expect("session");
        let (handle, _reliable) = authenticated_handle(1, "user", "revoke-during-publication");
        let revoked_sessions = Arc::clone(&registry.revoked_sessions);
        let lifecycle = registry.identity_lifecycle.clone();
        let revoked_session = session.clone();

        // Validation has already activated the lifecycle state. Model a durable
        // revocation that linearizes immediately before local publication.
        let registration = registry.register_session_at_after_validation(
            handle,
            Vec::new(),
            TimestampMillis::from_unix_millis(10),
            move || {
                revoked_sessions
                    .lock()
                    .expect("revocation tombstone")
                    .insert(
                        revoked_session.clone(),
                        TimestampMillis::from_unix_millis(10_000),
                    );
                lifecycle.revoke(
                    &revoked_session,
                    TimestampMillis::from_unix_millis(10_000),
                    TimestampMillis::from_unix_millis(10),
                );
            },
        );

        assert!(
            !registration.accepted,
            "a revocation that wins before publication must reject the activation"
        );
        assert!(
            !registry
                .unreliable
                .lock()
                .expect("unreliable map")
                .contains_key(&ParticipantId(1)),
            "a rejected publication must not leave an unreachable latest-wins sender"
        );
        assert!(!registry.accepts_work(ParticipantId(1)));
    }

    #[tokio::test]
    async fn durable_revocation_tombstones_expire_at_the_authoritative_session_boundary() {
        let registry = SessionRegistry::new();
        let session = SessionId::new("expiry-bounded-revocation").expect("session");
        let (active, _active_rx) = authenticated_handle(1, "user", "expiry-bounded-revocation");
        let mut active = active;
        active.identity.as_mut().expect("identity").expires_at =
            TimestampMillis::from_unix_millis(100);
        assert!(
            registry
                .register_session_at(active, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted
        );

        registry
            .close_session_at(
                &session,
                "authoritative-expiry",
                None,
                TimestampMillis::from_unix_millis(100),
                TimestampMillis::from_unix_millis(10),
            )
            .await;
        for index in 0..7 {
            let sibling =
                SessionId::new(format!("expiry-bounded-revocation-{index}")).expect("session");
            registry
                .close_session_at(
                    &sibling,
                    "authoritative-expiry",
                    None,
                    TimestampMillis::from_unix_millis(100),
                    TimestampMillis::from_unix_millis(10),
                )
                .await;
        }
        assert_eq!(registry.revocation_tombstone_count(), 8);
        assert_eq!(
            registry.expire_revocation_tombstones_at(TimestampMillis::from_unix_millis(100)),
            8,
            "a deterministic expiry sweep reclaims every expired durable tombstone"
        );
        assert_eq!(registry.revocation_tombstone_count(), 0);

        let (renewed, _renewed_rx) = authenticated_handle(2, "user", "expiry-bounded-revocation");
        assert!(
            registry
                .register_session_at(renewed, Vec::new(), TimestampMillis::from_unix_millis(100))
                .accepted,
            "an expired durable tombstone must not retain a session id beyond its authoritative expiry"
        );
        assert_eq!(
            registry.revocation_tombstone_count(),
            0,
            "activation purges the expired tombstone so repeated expired revocations cannot grow memory"
        );
    }

    #[tokio::test]
    async fn time_checked_register_and_resume_reject_revoked_tombstones() {
        let registry = SessionRegistry::new();
        let revoked = SessionId::new("revoked-session").expect("session");

        // A durable revocation can arrive before a transport reaches either
        // activation path; neither registration nor resume may republish it.
        assert!(
            registry
                .close_session_at(
                    &revoked,
                    "revoke-before-register",
                    None,
                    TimestampMillis::from_unix_millis(10_000),
                    TimestampMillis::from_unix_millis(10),
                )
                .await
                .is_empty()
        );
        let (late, _late_rx) = authenticated_handle(1, "user", "revoked-session");
        assert!(
            !registry
                .register_session_at(late, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted,
            "the time-checked registration path must observe the tombstone"
        );

        let (active, _active_rx) = authenticated_handle(2, "user", "resume-revoked-session");
        assert!(
            registry
                .register_session_at(active, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted
        );
        let secret = ResumeSecret::from_server_bytes(vec![3; 16]).expect("secret");
        assert!(registry.begin_reconnect_grace(
            ParticipantId(2),
            secret.clone(),
            TimestampMillis::from_unix_millis(30),
        ));
        let resume_revoked = SessionId::new("resume-revoked-session").expect("session");
        registry
            .close_session(&resume_revoked, "revoke-before-resume", None)
            .await;
        let (resumed, _resumed_rx) = authenticated_handle(3, "user", "resume-revoked-session");
        assert!(
            !registry
                .resume_session_at(resumed, secret, TimestampMillis::from_unix_millis(20))
                .accepted,
            "the time-checked resume path must observe the tombstone"
        );
    }

    #[tokio::test]
    async fn durable_close_revokes_grace_and_ticket_after_transport_unregisters() {
        let registry = SessionRegistry::new();
        let session = SessionId::new("grace-closed-without-local-session").expect("session");
        let (active, _active_rx) =
            authenticated_handle(1, "user", "grace-closed-without-local-session");
        assert!(
            registry
                .register_session_at(active, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted
        );
        let secret = ResumeSecret::from_server_bytes(vec![6; 16]).expect("secret");
        assert!(registry.begin_reconnect_grace_at(
            ParticipantId(1),
            secret.clone(),
            TimestampMillis::from_unix_millis(10),
            TimestampMillis::from_unix_millis(30),
        ));
        assert!(registry.unregister(ParticipantId(1)).is_some());
        assert_eq!(registry.reconnect_grace_count(), 1);

        // Gateway reconnect grace deliberately removes its local transport before
        // an operator's durable close command can arrive. The durable close must
        // still find and revoke the lifecycle record and its opaque ticket.
        assert!(
            registry
                .close_session(&session, "durable-close-after-unregister", None)
                .await
                .is_empty()
        );
        assert_eq!(registry.reconnect_grace_count(), 0);

        let (resume, _resume_rx) =
            authenticated_handle(2, "user", "grace-closed-without-local-session");
        assert!(
            !registry
                .resume_session_at(resume, secret, TimestampMillis::from_unix_millis(20))
                .accepted,
            "durable close must consume the grace lifecycle and reject its ticket"
        );
    }

    #[test]
    fn poisoned_durable_revocation_lookup_rejects_register_and_resume() {
        let registry = SessionRegistry::new();
        let (active, _active_rx) = authenticated_handle(1, "user", "poisoned-resume");
        assert!(
            registry
                .register_session_at(active, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted
        );
        let secret = ResumeSecret::from_server_bytes(vec![5; 16]).expect("secret");
        assert!(registry.begin_reconnect_grace(
            ParticipantId(1),
            secret.clone(),
            TimestampMillis::from_unix_millis(30),
        ));

        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = registry
                .revoked_sessions
                .lock()
                .expect("revocation lock before poisoning");
            panic!("poison the durable revocation lookup");
        }));
        assert!(poisoned.is_err());

        let (registration, _registration_rx) =
            authenticated_handle(2, "user", "poisoned-registration");
        assert!(
            !registry
                .register_session_at(
                    registration,
                    Vec::new(),
                    TimestampMillis::from_unix_millis(10)
                )
                .accepted,
            "an unreadable durable revocation tombstone must reject registration"
        );

        let (resume, _resume_rx) = authenticated_handle(3, "user", "poisoned-resume");
        assert!(
            !registry
                .resume_session_at(resume, secret, TimestampMillis::from_unix_millis(20))
                .accepted,
            "an unreadable durable revocation tombstone must reject resume"
        );
    }

    #[test]
    fn replacement_fences_old_receive_admission_before_active_mapping_publishes() {
        let registry = SessionRegistry::new();
        let (first, _first_rx) = authenticated_handle(1, "user", "same-session");
        let first =
            registry.register_session_at(first, Vec::new(), TimestampMillis::from_unix_millis(10));
        assert!(first.accepted);
        let (replacement, _replacement_rx) = authenticated_handle(2, "user", "same-session");
        let (at_publish_tx, at_publish_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let replacing_registry = registry.clone();
        let replacement = std::thread::spawn(move || {
            replacing_registry.register_session_at_before_active_publish(
                replacement,
                Vec::new(),
                TimestampMillis::from_unix_millis(10),
                move || {
                    at_publish_tx.send(()).expect("reach active publication");
                    release_rx.recv().expect("release active publication");
                },
            )
        });

        at_publish_rx
            .recv()
            .expect("replacement reaches publication");
        assert!(
            first.superseding.load(Ordering::Acquire),
            "old receive admission must close before the new active mapping publishes"
        );
        release_tx.send(()).expect("release replacement");
        assert!(replacement.join().expect("replacement thread").accepted);
        assert!(registry.accepts_work(ParticipantId(2)));
    }

    #[test]
    fn delayed_activation_cannot_publish_over_a_newer_activation() {
        let registry = SessionRegistry::new();
        let (older, _older_rx) = authenticated_handle(1, "user", "same-session");
        let (newer, _newer_rx) = authenticated_handle(2, "user", "same-session");
        let (older_validated_tx, older_validated_rx) = std::sync::mpsc::sync_channel(1);
        let (release_older_tx, release_older_rx) = std::sync::mpsc::sync_channel(1);
        let older_registry = registry.clone();
        let older = std::thread::spawn(move || {
            older_registry
                .register_session_at_after_validation(
                    older,
                    Vec::new(),
                    TimestampMillis::from_unix_millis(10),
                    move || {
                        older_validated_tx.send(()).expect("signal validation");
                        release_older_rx.recv().expect("release delayed activation");
                    },
                )
                .accepted
        });
        older_validated_rx
            .recv()
            .expect("older activation validated");

        let (newer_started_tx, newer_started_rx) = std::sync::mpsc::sync_channel(1);
        let (newer_done_tx, newer_done_rx) = std::sync::mpsc::sync_channel(1);
        let newer_registry = registry.clone();
        let newer = std::thread::spawn(move || {
            newer_started_tx.send(()).expect("start newer activation");
            let accepted = newer_registry
                .register_session_at(newer, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted;
            newer_done_tx.send(()).expect("complete newer activation");
            accepted
        });
        newer_started_rx.recv().expect("newer activation started");
        assert!(
            newer_done_rx
                .recv_timeout(std::time::Duration::from_millis(20))
                .is_err(),
            "a newer activation must not validate/publish through an older activation"
        );

        release_older_tx.send(()).expect("release older activation");
        assert!(older.join().expect("older activation thread"));
        newer_done_rx.recv().expect("newer activation completes");
        assert!(newer.join().expect("newer activation thread"));
        assert!(
            !registry.accepts_work(ParticipantId(1)),
            "the older activation is fenced after the newer one publishes"
        );
        assert!(registry.accepts_work(ParticipantId(2)));
    }

    #[test]
    fn resume_requires_the_reauthenticated_user_for_the_ticket_session() {
        let registry = SessionRegistry::new();
        let (active, _active_rx) = authenticated_handle(1, "alice", "session-a");
        assert!(
            registry
                .register_session_at(active, Vec::new(), TimestampMillis::from_unix_millis(10))
                .accepted
        );
        let secret = ResumeSecret::from_server_bytes(vec![4; 16]).expect("secret");
        assert!(registry.begin_reconnect_grace(
            ParticipantId(1),
            secret.clone(),
            TimestampMillis::from_unix_millis(30),
        ));

        // The session id is deliberately the same, but its current
        // reauthentication resolves it to another account. A resume ticket is
        // bound to both the exact session and the authenticated user.
        let (forged, _forged_rx) = authenticated_handle(2, "mallory", "session-a");
        assert!(
            !registry
                .resume_session_at(forged, secret, TimestampMillis::from_unix_millis(20))
                .accepted
        );
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
    async fn durable_close_cancels_the_registered_transport_generation() {
        let registry = SessionRegistry::new();
        let (handle, _reliable) = authenticated_handle(1, "u", "session-a");
        let registration =
            registry.register_session_at(handle, Vec::new(), TimestampMillis::from_unix_millis(10));
        assert!(registration.accepted);
        assert!(
            !registration.superseded.is_cancelled(),
            "the live transport starts uncancelled"
        );

        let session = SessionId::new("session-a").expect("session");
        assert_eq!(
            registry
                .close_session(&session, "durable-revoke", None)
                .await[0]
                .1,
            CloseDisposition::Closing
        );
        assert!(
            registration.superseded.is_cancelled(),
            "durable revocation must stop the registered transport loop"
        );
    }

    #[test]
    fn durable_cancellation_waits_for_the_receive_gate() {
        let superseded = CancellationToken::new();
        let inbound_supersession_drained = CancellationToken::new();
        let supersession_gate = Arc::new(Mutex::new(()));
        let held_gate = supersession_gate.lock().expect("hold receive gate");
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (blocked_tx, blocked_rx) = std::sync::mpsc::sync_channel(1);
        let cancellation_gate = Arc::clone(&supersession_gate);
        let cancellation_token = superseded.clone();
        let cancellation_drain = inbound_supersession_drained.clone();
        let cancellation = std::thread::spawn(move || {
            started_tx.send(()).expect("durable close started");
            assert!(matches!(
                cancellation_gate.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            blocked_tx
                .send(())
                .expect("durable close is blocked behind receive work");
            cancel_transport_under_gate(
                &cancellation_token,
                &cancellation_gate,
                &cancellation_drain,
            );
        });

        started_rx.recv().expect("durable close starts");
        blocked_rx
            .recv()
            .expect("durable close waits for receive work");
        assert!(
            !superseded.is_cancelled(),
            "durable cancellation cannot interleave after a receive-path check"
        );

        drop(held_gate);
        cancellation.join().expect("durable cancellation thread");
        assert!(superseded.is_cancelled());
        assert!(inbound_supersession_drained.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replacement_waits_for_admitted_transport_write_before_cancelling() {
        let registry = SessionRegistry::new();
        let (first, _first_reliable) = authenticated_handle(1, "u", "session-a");
        let first_registration =
            registry.register_session_at(first, Vec::new(), TimestampMillis::from_unix_millis(10));
        assert!(first_registration.accepted);
        let write = first_registration.transport_write_gate.lock().await;
        let (replacement, _replacement_reliable) = authenticated_handle(2, "u", "session-a");
        let replacement_registration = registry.register_session_at(
            replacement,
            Vec::new(),
            TimestampMillis::from_unix_millis(10),
        );
        assert!(replacement_registration.accepted);
        tokio::task::yield_now().await;
        assert!(
            !first_registration.superseded.is_cancelled(),
            "replacement cannot cancel while the old application write owns its flush gate"
        );
        assert!(
            first_registration.superseding.load(Ordering::Acquire),
            "replacement synchronously closes old receive admission before deferring cancellation"
        );
        assert!(
            !registry.accepts_work(ParticipantId(1)),
            "a stale inbound handoff is fenced while the old outbound write is still in flight"
        );
        drop(write);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            first_registration.superseded.cancelled(),
        )
        .await
        .expect("replacement cancellation follows the completed write");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replacement_releases_registry_locks_before_waiting_for_inbound_handoff() {
        let registry = SessionRegistry::new();
        let (first, _first_reliable) = authenticated_handle(1, "u", "session-a");
        let first_registration =
            registry.register_session_at(first, Vec::new(), TimestampMillis::from_unix_millis(10));
        assert!(first_registration.accepted);

        // Model an inbound route that has passed its cancellation check and is
        // about to hand off into the gateway. Replacement must wait for this
        // gate without retaining the registry lock that handoff will need.
        let receive_gate = Arc::clone(&first_registration.supersession_gate);
        let held_receive_gate = receive_gate.lock().expect("hold inbound handoff gate");
        let replacing_registry = registry.clone();
        let (replacement, _replacement_reliable) = authenticated_handle(2, "u", "session-a");
        let replace = tokio::spawn(async move {
            replacing_registry.register_session_at(
                replacement,
                Vec::new(),
                TimestampMillis::from_unix_millis(10),
            )
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !first_registration.superseding.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement closes old write admission");
        let replacement = tokio::time::timeout(std::time::Duration::from_secs(1), replace)
            .await
            .expect("replacement must return without waiting for inbound handoff")
            .expect("replacement task");
        assert!(replacement.accepted);
        assert!(
            !first_registration.superseded.is_cancelled(),
            "cancellation cannot publish before the admitted inbound handoff finishes"
        );

        drop(held_receive_gate);
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            first_registration.superseded.cancelled(),
        )
        .await
        .expect("replacement cancels after inbound handoff releases");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_close_waits_for_admitted_transport_write_before_cancelling() {
        let registry = SessionRegistry::new();
        let (handle, _reliable) = authenticated_handle(1, "u", "session-a");
        let registration =
            registry.register_session_at(handle, Vec::new(), TimestampMillis::from_unix_millis(10));
        assert!(registration.accepted);
        let write = registration.transport_write_gate.lock().await;
        let session = SessionId::new("session-a").expect("session");
        let closing_registry = registry.clone();
        let close = tokio::spawn(async move {
            closing_registry
                .close_session(&session, "write-boundary-ordering", None)
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !registration.superseded.is_cancelled(),
            "close cannot cancel while the admitted transport write still owns its flush gate"
        );
        assert!(!close.is_finished(), "close waits for the transport write");
        drop(write);
        assert_eq!(
            close.await.expect("close task")[0].1,
            CloseDisposition::Closing
        );
        assert!(registration.superseded.is_cancelled());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_close_cancels_behind_the_receive_gate_before_publishing_the_fence() {
        let registry = SessionRegistry::new();
        let (handle, _reliable) = authenticated_handle(1, "u", "session-a");
        let registration =
            registry.register_session_at(handle, Vec::new(), TimestampMillis::from_unix_millis(10));
        assert!(registration.accepted);
        let gate = Arc::clone(&registration.supersession_gate);
        let held_gate = gate.lock().expect("hold receive gate");
        let session = SessionId::new("session-a").expect("session");
        let closing_registry = registry.clone();
        let close = tokio::spawn(async move {
            closing_registry
                .close_session(&session, "durable-revoke-ordering", None)
                .await
        });

        tokio::task::yield_now().await;
        assert!(
            !close.is_finished(),
            "durable close must wait for the in-flight receive gate"
        );
        assert!(
            registry.accepts_work(ParticipantId(1)),
            "durable close must not publish Closing/fencing before transport cancellation"
        );
        assert!(
            !registration.superseded.is_cancelled(),
            "transport cancellation waits for the receive gate"
        );

        drop(held_gate);
        assert_eq!(
            close.await.expect("durable close task")[0].1,
            CloseDisposition::Closing
        );
        assert!(registration.superseded.is_cancelled());
        assert!(
            !registry.accepts_work(ParticipantId(1)),
            "the close fence follows transport cancellation"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expected_generation_close_cancels_behind_its_receive_gate_before_publishing_closing() {
        let registry = SessionRegistry::new();
        let (handle, _reliable) = authenticated_handle(1, "u", "session-a");
        let registration =
            registry.register_session_at(handle, Vec::new(), TimestampMillis::from_unix_millis(10));
        assert!(registration.accepted);
        let generation = registry
            .connections_for_session(&SessionId::new("session-a").expect("session"))[0]
            .generation();
        let gate = Arc::clone(&registration.supersession_gate);
        let held_gate = gate.lock().expect("hold exact generation receive gate");
        let session = SessionId::new("session-a").expect("session");
        let closing_registry = registry.clone();
        let close = tokio::spawn(async move {
            closing_registry
                .close_session(&session, "expected-generation-ordering", Some(generation))
                .await
        });

        tokio::task::yield_now().await;
        assert!(
            !close.is_finished(),
            "the matching generation close must wait for its receive gate"
        );
        assert!(
            registry.accepts_work(ParticipantId(1)),
            "the matching generation must acquire its receive gate before publishing Closing"
        );
        assert!(
            !registration.superseded.is_cancelled(),
            "the matching generation transport remains live until the receive gate releases"
        );

        drop(held_gate);
        assert_eq!(
            close.await.expect("expected-generation close task")[0].1,
            CloseDisposition::Closing
        );
        assert!(registration.superseded.is_cancelled());
        assert!(
            !registry.accepts_work(ParticipantId(1)),
            "Closing follows cancellation for the matching generation"
        );
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
        // durable-revoke-before-publication interleaving. The durable command
        // carries the revoked session's authoritative access expiry.
        assert!(
            registry
                .close_session_at(
                    &revoked,
                    "revoke-before-register",
                    None,
                    TimestampMillis::from_unix_millis(10_000),
                    TimestampMillis::from_unix_millis(10),
                )
                .await
                .is_empty()
        );

        for kind in [
            TransportKind::WebSocket,
            TransportKind::Quic,
            TransportKind::WebTransport,
        ] {
            let (late, mut late_rx) = authenticated_handle(1, "same-user", "session-a");
            let registration = registry.register_session_at(
                SessionHandle { kind, ..late },
                vec![Outbound::reliable(Envelope::new(
                    citadel_wire::protocol::KIND_AUTH_RESULT,
                    b"must-not-deliver".to_vec(),
                ))],
                TimestampMillis::from_unix_millis(10),
            );
            assert!(!registration.accepted);
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
    async fn deterministic_publish_replacement_stale_close_does_not_durably_revoke_the_replacement()
    {
        let registry = SessionRegistry::new();
        let session = SessionId::new("session-a").expect("session");
        let now = TimestampMillis::from_unix_millis(10);

        // A delayed routed close has captured the first generation. Publish a
        // replacement before delivering that close, which deterministically
        // models the publish/replacement/stale-close ordering without relying
        // on scheduler timing.
        let (first, _first_rx) = authenticated_handle(1, "u", "session-a");
        let first = registry.register_session_at(first, Vec::new(), now);
        assert!(first.accepted);
        let stale_generation = registry.connections_for_session(&session)[0].generation();

        let (replacement, _replacement_rx) = authenticated_handle(2, "u", "session-a");
        let replacement = registry.register_session_at(replacement, Vec::new(), now);
        assert!(replacement.accepted);
        assert!(registry.accepts_work(ParticipantId(2)));

        let closed = registry
            .close_session_at(
                &session,
                "delayed-stale-route",
                Some(stale_generation),
                TimestampMillis::from_unix_millis(10_000),
                now,
            )
            .await;
        assert!(
            closed.iter().any(|(connection, disposition)| {
                connection.participant_id() == ParticipantId(2)
                    && *disposition == CloseDisposition::Stale
            }),
            "a delayed old-generation close must not close the replacement"
        );
        assert!(
            !replacement.superseded.is_cancelled(),
            "a stale close must not cancel the replacement transport"
        );
        assert_eq!(
            registry
                .identity_lifecycle
                .presence(&session)
                .expect("replacement lifecycle remains active")
                .participant,
            ParticipantId(2)
        );

        // A durable tombstone or lifecycle revoke from the stale close would
        // reject this next publish. It must instead replace the current mapping.
        let (next, _next_rx) = authenticated_handle(3, "u", "session-a");
        assert!(
            registry.register_session_at(next, Vec::new(), now).accepted,
            "the stale close must not durably revoke a newer replacement"
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
