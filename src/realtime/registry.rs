//! Transport-agnostic session registry for the realtime gateway.
//!
//! Each accepted connection (QUIC or WebSocket) registers a [`SessionHandle`]
//! whose only transport-specific dependency is a bounded
//! `tokio::mpsc::Sender<Outbound>`: the connection's write task drains that
//! channel and writes to its concrete socket. The registry therefore routes
//! purely over abstract outbound sinks and never depends on a concrete
//! transport.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

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
    sessions: std::sync::Arc<Mutex<HashMap<ParticipantId, SessionHandle>>>,
}

impl SessionRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a session handle.
    pub fn register(&self, handle: SessionHandle) {
        if let Ok(mut map) = self.sessions.lock() {
            map.insert(handle.id, handle);
        }
    }

    /// Unregister a session (on disconnect), returning the removed handle.
    ///
    /// The returned handle lets the caller tell whether an *authenticated*
    /// session ended (so the authenticated-session gauge is decremented exactly
    /// when it was incremented) versus a guest participant.
    pub fn unregister(&self, id: ParticipantId) -> Option<SessionHandle> {
        match self.sessions.lock() {
            Ok(mut map) => map.remove(&id),
            Err(_) => None,
        }
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
    /// Best-effort: a full per-session channel drops the message for that
    /// session (the transport's own backpressure policy applies downstream).
    /// Returns the number of sessions the message was queued to.
    pub fn broadcast_except(&self, sender: ParticipantId, outbound: &Outbound) -> usize {
        let handles: Vec<SessionHandle> = match self.sessions.lock() {
            Ok(map) => map.values().filter(|h| h.id != sender).cloned().collect(),
            Err(_) => return 0,
        };
        let mut delivered = 0;
        for handle in handles {
            // `try_send` is non-blocking; a full or closed channel is skipped.
            if handle.outbound.try_send(outbound.clone()).is_ok() {
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
        let handles: Vec<SessionHandle> = match self.sessions.lock() {
            Ok(map) => map.values().cloned().collect(),
            Err(_) => return 0,
        };
        let mut delivered = 0;
        for handle in handles {
            if handle.outbound.try_send(outbound.clone()).is_ok() {
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
        let handles: Vec<SessionHandle> = match self.sessions.lock() {
            Ok(map) => members
                .iter()
                .filter(|&&id| Some(id) != exclude)
                .filter_map(|id| map.get(id).cloned())
                .collect(),
            Err(_) => return 0,
        };
        let mut delivered = 0;
        for handle in handles {
            if handle.outbound.try_send(outbound.clone()).is_ok() {
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
