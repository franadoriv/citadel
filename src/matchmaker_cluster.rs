//! Fenced ownership primitives for distributed matchmaker queues.
//!
//! This is intentionally independent of socket transport. A node must acquire
//! the queue shard lease before it may form tickets or admit a handoff; stale
//! owners fail closed. The eventual node router transports the commands while
//! this module remains the single concurrency authority.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::matchmaker::{TicketId, TicketRequest, TicketState};
use crate::session::{NodeId, OwnershipGeneration};
use crate::time::TimestampMillis;

/// Stable shard index within a configured matchmaker queue partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QueueShardId(u16);

impl QueueShardId {
    /// Construct a validated shard index.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Raw partition-local index.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// A time-bounded, generation-fenced shard owner lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchmakerShardLease {
    /// Queue partition held by this node.
    pub shard: QueueShardId,
    /// Current owner node.
    pub owner_node: NodeId,
    /// Monotonically increasing fencing generation.
    pub generation: OwnershipGeneration,
    /// Lease expiry; equality is expired.
    pub expires_at: TimestampMillis,
}

impl MatchmakerShardLease {
    /// Whether the lease remains current at `now`.
    #[must_use]
    pub fn is_current_at(&self, now: TimestampMillis) -> bool {
        now < self.expires_at
    }

    /// Whether two leases represent the same fencing authority, independent of
    /// their renewal expiry. A valid renewal extends `expires_at` without
    /// invalidating handoffs formed under the same owner/generation pair.
    #[must_use]
    pub fn has_same_fence_as(&self, other: &Self) -> bool {
        self.shard == other.shard
            && self.owner_node == other.owner_node
            && self.generation == other.generation
    }
}

/// Resolution relative to a requesting node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchmakerShardOwnership {
    /// The requester owns the current lease.
    Local,
    /// A different node owns the current lease.
    Remote(NodeId),
    /// No current lease exists.
    Unknown,
    /// The supplied expected lease no longer matches.
    Stale,
}

/// Failure when claiming a shard, a formation, or an admission.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MatchmakerClusterError {
    /// A lower/equal conflicting generation attempted to take a shard.
    #[error("matchmaker shard lease is stale or conflicting")]
    LeaseConflict,
    /// The caller's lease is expired, absent, or superseded.
    #[error("matchmaker shard lease is not current")]
    LeaseNotCurrent,
    /// Another owner already formed this ticket.
    #[error("ticket was already formed by a matchmaker owner")]
    AlreadyFormed,
    /// The same account already redeemed this ticket's handoff.
    #[error("ticket handoff was already admitted for this user")]
    AlreadyAdmitted,
}

/// A formed handoff forwarded to the node that owns the recipient's live
/// session. The token is intentionally opaque and redacted from debug output.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteMatchmakerHandoff {
    /// Ticket represented by this user handoff.
    pub ticket_id: TicketId,
    /// Authenticated recipient account.
    pub user_id: String,
    /// Opaque match/room id allocated by the formation owner.
    pub match_id: u64,
    /// One-time random capability to present during admission.
    pub join_token: String,
    /// Capability expiry.
    pub expires_at: TimestampMillis,
    /// Exact lease that formed the ticket; receivers must not replace it with a
    /// newer local lease when redeeming the handoff.
    pub formation_lease: MatchmakerShardLease,
}

impl std::fmt::Debug for RemoteMatchmakerHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteMatchmakerHandoff")
            .field("ticket_id", &self.ticket_id)
            .field("user_id", &self.user_id)
            .field("match_id", &self.match_id)
            .field("join_token", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .field("formation_lease", &self.formation_lease)
            .finish()
    }
}

/// Transport error for inter-node matchmaker delivery.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MatchmakerRouterError {
    /// No registered transport endpoint owns the target node.
    #[error("matchmaker router has no endpoint for node {0}")]
    UnknownDestination(NodeId),
    /// The authenticated node-control connection could not complete before its
    /// bounded deadline. The capability payload is deliberately not retained in
    /// the error so it cannot reach logs.
    #[error("matchmaker router could not reach node {0}")]
    Unavailable(NodeId),
    /// The remote node authenticated the connection but rejected the typed
    /// command (expired/stale/duplicate commands fail closed).
    #[error("matchmaker router command was rejected by node {0}")]
    Rejected(NodeId),
}

/// A redemption request sent from the session-owning node to the match-owning
/// node. The requester id is node-scoped so the owner never mistakes it for a
/// local participant id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteMatchmakerAdmission {
    pub ticket_id: TicketId,
    pub user_id: String,
    pub requester_node: NodeId,
    pub join_token: String,
    pub formation_lease: MatchmakerShardLease,
}

/// One account represented by a remotely submitted ticket. The session node is
/// explicit because a local [`ParticipantId`](crate::realtime::ParticipantId)
/// never crosses a node boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteMatchmakerTicketOwner {
    /// Authenticated account that receives the eventual handoff.
    pub user_id: String,
    /// Node that owns this account's realtime socket.
    pub session_node: NodeId,
}

/// Committed durable-party snapshot carried from the session gateway to the
/// shard owner. The owner revalidates it immediately after its asynchronous
/// queue insertion and cancels the ticket if membership changed in transit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyAdmissionFence {
    pub party_id: String,
    pub leader_user_id: String,
    pub revision: u64,
    pub owner_generation: u64,
    pub admission_generation: u64,
    pub admission_token: u64,
}

/// A ticket forwarded by its session node to the current shard owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteMatchmakerTicketSubmission {
    /// Every indivisible player/party member represented by the ticket.
    pub owners: Vec<RemoteMatchmakerTicketOwner>,
    /// The validated client matching request.
    pub request: TicketRequest,
    /// Present only for a party ticket. This must never be trusted without the
    /// shard owner's post-submit durable revalidation.
    #[serde(default)]
    pub party_admission: Option<PartyAdmissionFence>,
}

/// A cancellation forwarded by the ticket's session node to its shard owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteMatchmakerTicketCancellation {
    /// Opaque ticket returned from submission.
    pub ticket_id: TicketId,
    /// Authenticated leader/account authorized to cancel it.
    pub user_id: String,
}

/// A status lookup forwarded by the ticket's session node to its shard owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteMatchmakerTicketStatus {
    /// Opaque ticket returned from submission.
    pub ticket_id: TicketId,
    /// Authenticated account authorized to inspect it.
    pub user_id: String,
}

/// Callback registered by a shard-owning node for forwarded ticket operations.
pub type TicketSubmissionHandler = Arc<
    dyn Fn(RemoteMatchmakerTicketSubmission) -> Result<TicketId, MatchmakerRouterError>
        + Send
        + Sync,
>;

/// Callback registered by a shard-owning node for cancel/status operations.
pub type TicketCancellationHandler = Arc<
    dyn Fn(RemoteMatchmakerTicketCancellation) -> Result<bool, MatchmakerRouterError> + Send + Sync,
>;

/// Callback registered by a shard-owning node for status operations.
pub type TicketStatusHandler = Arc<
    dyn Fn(RemoteMatchmakerTicketStatus) -> Result<Option<TicketState>, MatchmakerRouterError>
        + Send
        + Sync,
>;

/// Callback registered by a match-owning node in the in-memory router.
pub type AdmissionHandler =
    Arc<dyn Fn(RemoteMatchmakerAdmission) -> Result<u64, MatchmakerRouterError> + Send + Sync>;

/// Minimal inter-node handoff transport. Implementations authenticate and route
/// node-to-node traffic; they never inspect/rewrite the opaque join capability.
pub trait MatchmakerHandoffRouter: Send + Sync {
    /// Forward a formed handoff to the recipient session's owner node.
    fn deliver_handoff(
        &self,
        destination: &NodeId,
        handoff: RemoteMatchmakerHandoff,
    ) -> Result<(), MatchmakerRouterError>;

    /// Drain handoffs delivered to `node`. The receiver persists/serves them
    /// before attempting best-effort realtime notification.
    fn drain_handoffs(&self, node: &NodeId) -> Vec<RemoteMatchmakerHandoff>;

    /// Ask the match-owning node to validate and register a remote admission.
    fn admit_remote(
        &self,
        destination: &NodeId,
        request: RemoteMatchmakerAdmission,
    ) -> Result<u64, MatchmakerRouterError>;
}

/// Deterministic, in-process router for two-node integration tests and local
/// development. Production transports implement the same narrow trait.
#[derive(Default)]
pub struct InMemoryMatchmakerHandoffRouter {
    inboxes: Mutex<HashMap<NodeId, Vec<RemoteMatchmakerHandoff>>>,
    admission_handlers: Mutex<HashMap<NodeId, AdmissionHandler>>,
}

impl std::fmt::Debug for InMemoryMatchmakerHandoffRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryMatchmakerHandoffRouter")
            .field("inboxes", &"[redacted]")
            .field("admission_handlers", &"[registered]")
            .finish()
    }
}

impl InMemoryMatchmakerHandoffRouter {
    /// Construct an empty router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node endpoint before it can receive handoffs.
    pub fn register_node(&self, node: NodeId) {
        if let Ok(mut inboxes) = self.inboxes.lock() {
            inboxes.entry(node).or_default();
        }
    }

    /// Register the match-owner callback for a node. Replacing a handler is
    /// intentional for a node restart in an in-process integration test.
    pub fn register_admission_handler(&self, node: NodeId, handler: AdmissionHandler) {
        if let Ok(mut handlers) = self.admission_handlers.lock() {
            handlers.insert(node, handler);
        }
    }
}

impl MatchmakerHandoffRouter for InMemoryMatchmakerHandoffRouter {
    fn deliver_handoff(
        &self,
        destination: &NodeId,
        handoff: RemoteMatchmakerHandoff,
    ) -> Result<(), MatchmakerRouterError> {
        let mut inboxes = self
            .inboxes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let inbox = inboxes
            .get_mut(destination)
            .ok_or_else(|| MatchmakerRouterError::UnknownDestination(destination.clone()))?;
        inbox.push(handoff);
        Ok(())
    }

    fn drain_handoffs(&self, node: &NodeId) -> Vec<RemoteMatchmakerHandoff> {
        let mut inboxes = self
            .inboxes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inboxes
            .get_mut(node)
            .map(std::mem::take)
            .unwrap_or_default()
    }

    fn admit_remote(
        &self,
        destination: &NodeId,
        request: RemoteMatchmakerAdmission,
    ) -> Result<u64, MatchmakerRouterError> {
        let handler = self
            .admission_handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(destination)
            .cloned()
            .ok_or_else(|| MatchmakerRouterError::UnknownDestination(destination.clone()))?;
        handler(request)
    }
}

#[derive(Debug, Clone)]
struct LeaseState {
    current: Option<MatchmakerShardLease>,
    max_generation: OwnershipGeneration,
}

#[derive(Debug, Clone)]
struct FormationClaim {
    lease: MatchmakerShardLease,
    admitted_users: BTreeSet<String>,
}

#[derive(Debug, Default)]
struct Inner {
    shards: HashMap<QueueShardId, LeaseState>,
    formations: HashMap<TicketId, FormationClaim>,
}

/// Shared in-memory reference authority for a multi-node matchmaker test or a
/// colocated deployment. A durable cluster backend must preserve these exact
/// generation and one-time claim rules.
#[derive(Debug, Default)]
pub struct InMemoryMatchmakerCluster {
    inner: Mutex<Inner>,
}

impl InMemoryMatchmakerCluster {
    /// Create an empty authority.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Claim or renew a shard. A current owner may extend the same generation;
    /// a transfer or a superseding owner needs a strictly higher generation.
    pub fn acquire_shard(&self, lease: MatchmakerShardLease) -> Result<(), MatchmakerClusterError> {
        let mut inner = self.lock();
        match inner.shards.get_mut(&lease.shard) {
            None => {
                inner.shards.insert(
                    lease.shard,
                    LeaseState {
                        max_generation: lease.generation,
                        current: Some(lease),
                    },
                );
                Ok(())
            }
            Some(state)
                if state
                    .current
                    .as_ref()
                    .is_some_and(|current| current.has_same_fence_as(&lease)) =>
            {
                state.max_generation = state.max_generation.max(lease.generation);
                state.current = Some(lease);
                Ok(())
            }
            Some(state) if lease.generation > state.max_generation => {
                state.max_generation = lease.generation;
                state.current = Some(lease);
                Ok(())
            }
            Some(_) => Err(MatchmakerClusterError::LeaseConflict),
        }
    }

    /// Resolve a shard relative to `local_node`, optionally fencing a stale view.
    #[must_use]
    pub fn resolve_shard(
        &self,
        shard: QueueShardId,
        local_node: &NodeId,
        expected: Option<&MatchmakerShardLease>,
        now: TimestampMillis,
    ) -> MatchmakerShardOwnership {
        let inner = self.lock();
        let Some(state) = inner.shards.get(&shard) else {
            return MatchmakerShardOwnership::Unknown;
        };
        let Some(current) = &state.current else {
            return MatchmakerShardOwnership::Unknown;
        };
        if !current.is_current_at(now) {
            return MatchmakerShardOwnership::Unknown;
        }
        if expected.is_some_and(|expected| !expected.has_same_fence_as(current)) {
            return MatchmakerShardOwnership::Stale;
        }
        if &current.owner_node == local_node {
            MatchmakerShardOwnership::Local
        } else {
            MatchmakerShardOwnership::Remote(current.owner_node.clone())
        }
    }

    /// Atomically claim a ticket formation under a current lease. A different
    /// node, retry, or stale generation cannot form the ticket again.
    pub fn claim_formation(
        &self,
        ticket: TicketId,
        lease: &MatchmakerShardLease,
        now: TimestampMillis,
    ) -> Result<(), MatchmakerClusterError> {
        let mut inner = self.lock();
        Self::ensure_current(&inner, lease, now)?;
        if inner.formations.contains_key(&ticket) {
            return Err(MatchmakerClusterError::AlreadyFormed);
        }
        inner.formations.insert(
            ticket,
            FormationClaim {
                lease: lease.clone(),
                admitted_users: BTreeSet::new(),
            },
        );
        Ok(())
    }

    /// Atomically claim every ticket in one formed cohort. Either all tickets
    /// become owned by this lease or none do, preventing a partial party/cohort
    /// claim when another node already formed one member.
    pub fn claim_formations(
        &self,
        tickets: &[TicketId],
        lease: &MatchmakerShardLease,
        now: TimestampMillis,
    ) -> Result<(), MatchmakerClusterError> {
        let mut inner = self.lock();
        Self::ensure_current(&inner, lease, now)?;
        if tickets
            .iter()
            .any(|ticket| inner.formations.contains_key(ticket))
        {
            return Err(MatchmakerClusterError::AlreadyFormed);
        }
        for ticket in tickets {
            inner.formations.insert(
                ticket.clone(),
                FormationClaim {
                    lease: lease.clone(),
                    admitted_users: BTreeSet::new(),
                },
            );
        }
        Ok(())
    }

    /// Atomically redeem one user's handoff only when the original formation
    /// lease is still current and equal. This prevents a stale owner from
    /// admitting after a transfer and prevents duplicate admission retries.
    pub fn claim_admission(
        &self,
        ticket: &TicketId,
        user_id: &str,
        lease: &MatchmakerShardLease,
        now: TimestampMillis,
    ) -> Result<(), MatchmakerClusterError> {
        let mut inner = self.lock();
        Self::ensure_current(&inner, lease, now)?;
        let claim = inner
            .formations
            .get_mut(ticket)
            .ok_or(MatchmakerClusterError::LeaseNotCurrent)?;
        if !claim.lease.has_same_fence_as(lease) {
            return Err(MatchmakerClusterError::LeaseNotCurrent);
        }
        if !claim.admitted_users.insert(user_id.to_owned()) {
            return Err(MatchmakerClusterError::AlreadyAdmitted);
        }
        Ok(())
    }

    fn ensure_current(
        inner: &Inner,
        lease: &MatchmakerShardLease,
        now: TimestampMillis,
    ) -> Result<(), MatchmakerClusterError> {
        let Some(state) = inner.shards.get(&lease.shard) else {
            return Err(MatchmakerClusterError::LeaseNotCurrent);
        };
        if !state
            .current
            .as_ref()
            .is_some_and(|current| current.has_same_fence_as(lease) && current.is_current_at(now))
        {
            return Err(MatchmakerClusterError::LeaseNotCurrent);
        }
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: &str) -> NodeId {
        NodeId::new(value).expect("valid test node")
    }

    fn lease(owner: &str, generation: u64, expiry: u64) -> MatchmakerShardLease {
        MatchmakerShardLease {
            shard: QueueShardId::new(3),
            owner_node: node(owner),
            generation: OwnershipGeneration::new(generation),
            expires_at: TimestampMillis::from_unix_millis(expiry),
        }
    }

    fn ticket(value: &str) -> TicketId {
        TicketId::parse(value).expect("valid test ticket")
    }

    #[test]
    fn lease_fencing_and_resolution_are_explicit() {
        let cluster = InMemoryMatchmakerCluster::new();
        let a1 = lease("node-a", 1, 100);
        cluster.acquire_shard(a1.clone()).expect("initial lease");
        assert_eq!(
            cluster.resolve_shard(
                a1.shard,
                &node("node-b"),
                None,
                TimestampMillis::from_unix_millis(50)
            ),
            MatchmakerShardOwnership::Remote(node("node-a"))
        );
        assert_eq!(
            cluster.acquire_shard(lease("node-b", 1, 100)),
            Err(MatchmakerClusterError::LeaseConflict)
        );
        let b2 = lease("node-b", 2, 200);
        cluster
            .acquire_shard(b2.clone())
            .expect("higher generation transfers");
        assert_eq!(
            cluster.resolve_shard(
                a1.shard,
                &node("node-a"),
                Some(&a1),
                TimestampMillis::from_unix_millis(50)
            ),
            MatchmakerShardOwnership::Stale
        );
        assert_eq!(
            cluster.resolve_shard(
                b2.shard,
                &node("node-b"),
                Some(&b2),
                TimestampMillis::from_unix_millis(200)
            ),
            MatchmakerShardOwnership::Unknown
        );
    }

    #[test]
    fn only_one_current_owner_forms_and_admits_each_ticket_user_once() {
        let cluster = InMemoryMatchmakerCluster::new();
        let a1 = lease("node-a", 1, 100);
        cluster.acquire_shard(a1.clone()).expect("lease");
        let ticket = ticket("ticket-a");
        cluster
            .claim_formation(ticket.clone(), &a1, TimestampMillis::from_unix_millis(10))
            .expect("formation");
        assert_eq!(
            cluster.claim_formation(ticket.clone(), &a1, TimestampMillis::from_unix_millis(10)),
            Err(MatchmakerClusterError::AlreadyFormed)
        );
        cluster
            .claim_admission(&ticket, "alice", &a1, TimestampMillis::from_unix_millis(20))
            .expect("first admission");
        assert_eq!(
            cluster.claim_admission(&ticket, "alice", &a1, TimestampMillis::from_unix_millis(20)),
            Err(MatchmakerClusterError::AlreadyAdmitted)
        );
        let b2 = lease("node-b", 2, 100);
        cluster.acquire_shard(b2.clone()).expect("transfer");
        assert_eq!(
            cluster.claim_admission(&ticket, "bob", &a1, TimestampMillis::from_unix_millis(30)),
            Err(MatchmakerClusterError::LeaseNotCurrent)
        );
        assert_eq!(
            cluster.claim_admission(&ticket, "bob", &b2, TimestampMillis::from_unix_millis(30)),
            Err(MatchmakerClusterError::LeaseNotCurrent),
            "a formation never silently changes owner across a shard transfer"
        );
    }

    #[test]
    fn renewal_preserves_a_formation_fence_but_transfer_invalidates_it() {
        let cluster = InMemoryMatchmakerCluster::new();
        let formed = lease("node-a", 1, 100);
        cluster.acquire_shard(formed.clone()).expect("lease");
        let ticket = ticket("ticket-renewal");
        cluster
            .claim_formation(
                ticket.clone(),
                &formed,
                TimestampMillis::from_unix_millis(10),
            )
            .expect("formation");

        let renewed = lease("node-a", 1, 200);
        cluster.acquire_shard(renewed.clone()).expect("renewal");
        cluster
            .claim_admission(
                &ticket,
                "alice",
                &formed,
                TimestampMillis::from_unix_millis(20),
            )
            .expect("original lease has the same valid fence");

        let transferred = lease("node-b", 2, 300);
        cluster
            .acquire_shard(transferred)
            .expect("transfer with higher generation");
        assert_eq!(
            cluster.claim_admission(
                &ticket,
                "bob",
                &renewed,
                TimestampMillis::from_unix_millis(30)
            ),
            Err(MatchmakerClusterError::LeaseNotCurrent)
        );
    }

    #[test]
    fn cohort_claim_is_all_or_nothing_when_one_ticket_is_already_formed() {
        let cluster = InMemoryMatchmakerCluster::new();
        let lease = lease("node-a", 1, 100);
        cluster.acquire_shard(lease.clone()).expect("lease");
        let first = ticket("ticket-first");
        let second = ticket("ticket-second");
        cluster
            .claim_formation(first.clone(), &lease, TimestampMillis::from_unix_millis(10))
            .expect("first claim");
        assert_eq!(
            cluster.claim_formations(
                &[first, second.clone()],
                &lease,
                TimestampMillis::from_unix_millis(10)
            ),
            Err(MatchmakerClusterError::AlreadyFormed)
        );
        cluster
            .claim_formation(second, &lease, TimestampMillis::from_unix_millis(10))
            .expect("second was not partially claimed");
    }

    #[test]
    fn remote_handoffs_reach_only_the_registered_destination() {
        let router = InMemoryMatchmakerHandoffRouter::new();
        let node_a = node("node-a");
        let node_b = node("node-b");
        router.register_node(node_b.clone());
        let handoff = RemoteMatchmakerHandoff {
            ticket_id: ticket("ticket-remote"),
            user_id: "alice".to_owned(),
            match_id: 42,
            join_token: "secret-capability".to_owned(),
            expires_at: TimestampMillis::from_unix_millis(100),
            formation_lease: lease("node-a", 1, 100),
        };
        assert_eq!(
            router.deliver_handoff(&node_a, handoff.clone()),
            Err(MatchmakerRouterError::UnknownDestination(node_a.clone()))
        );
        router
            .deliver_handoff(&node_b, handoff.clone())
            .expect("remote delivery");
        assert!(router.drain_handoffs(&node_a).is_empty());
        assert_eq!(router.drain_handoffs(&node_b), vec![handoff]);
        assert!(
            router.drain_handoffs(&node_b).is_empty(),
            "drain is one-shot"
        );
    }
}
