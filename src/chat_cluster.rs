//! Narrow cross-node contracts for live chat delivery.
//!
//! This module deliberately models channel-level leases and durable-event
//! delivery only. It never exposes a socket, participant handle, or arbitrary
//! realtime frame to another node. The mTLS transport integration owns the
//! wire representation; this contract is also usable by deterministic
//! two-node tests.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::realtime::chat_presence::ChatPresenceRegistry;
use crate::repository::{ChatDeliveryOutboxRecord, ChatRepository};
use crate::session::{NodeId, OwnershipGeneration};
use crate::time::TimestampMillis;

/// Fenced advertisement of one node's local subscriptions for a channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatPresenceLease {
    /// Opaque durable channel identifier; it is not a participant capability.
    pub channel_id: String,
    /// Node that currently has one or more local subscriptions.
    pub node_id: NodeId,
    /// Monotonic fence which prevents a delayed advertisement from reviving a
    /// withdrawn or replaced one.
    pub generation: OwnershipGeneration,
    /// Exclusive expiry boundary in Unix milliseconds.
    pub expires_at: TimestampMillis,
}

/// Fenced removal of a node's channel-level presence advertisement.
///
/// Like [`ChatPresenceLease`], this contains only a node and a channel. It
/// intentionally cannot name a participant or realtime connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatPresenceWithdrawal {
    /// Opaque durable channel identifier.
    pub channel_id: String,
    /// Node withdrawing its final local subscription for the channel.
    pub node_id: NodeId,
    /// Fence that must still be current for the removal to apply.
    pub generation: OwnershipGeneration,
}

impl ChatPresenceLease {
    /// Whether the advertisement remains usable at `now`.
    #[must_use]
    pub fn is_current_at(&self, now: TimestampMillis) -> bool {
        self.expires_at > now
    }
}

/// Result of changing the fenced, per-channel presence directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChatLeaseUpdate {
    /// The advertisement or withdrawal became the current fence.
    Applied,
    /// A delayed advertisement or withdrawal lost to a newer generation.
    Stale,
}

/// In-memory leased directory for channel-level node advertisements.
///
/// It deliberately records nodes, never participant or socket identifiers. A
/// withdrawal leaves a generation tombstone so a delayed advertisement cannot
/// revive a node after its last local subscriber left.
#[derive(Debug, Default)]
pub struct ChatPresenceDirectory {
    leases: Mutex<BTreeMap<String, BTreeMap<NodeId, ChatPresenceLease>>>,
    fences: Mutex<BTreeMap<(String, NodeId), OwnershipGeneration>>,
}

/// Best-effort publication boundary for a local channel-level lease.
///
/// A failed publication is deliberately not a socket failure and cannot roll
/// back a local join. The next bounded renewal republishes it; recipients that
/// did not observe a lease reconcile from durable history instead.
pub trait ChatPresencePublisher: Send + Sync {
    /// Publish or renew a local lease to every configured peer.
    fn advertise(&self, lease: ChatPresenceLease);
    /// Withdraw a local lease from every configured peer.
    fn withdraw(&self, withdrawal: ChatPresenceWithdrawal);
}

/// Couples local subscription transitions to a fenced, channel/node lease.
///
/// This is intentionally not a participant directory. It stores one
/// generation per local channel and is safe to call on join, leave, disconnect,
/// and periodic renewal paths.
pub struct LocalChatPresenceAnnouncer {
    node_id: NodeId,
    directory: Arc<ChatPresenceDirectory>,
    publisher: Arc<dyn ChatPresencePublisher>,
    lease_ttl_ms: u64,
    generations: Mutex<BTreeMap<String, OwnershipGeneration>>,
    next_generation: AtomicU64,
}

impl std::fmt::Debug for LocalChatPresenceAnnouncer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalChatPresenceAnnouncer")
            .field("node_id", &self.node_id)
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .finish_non_exhaustive()
    }
}

impl LocalChatPresenceAnnouncer {
    /// Build a fenced local announcer. A zero TTL is clamped to one millisecond
    /// so it cannot create an immediately invalid lease.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        directory: Arc<ChatPresenceDirectory>,
        publisher: Arc<dyn ChatPresencePublisher>,
        lease_ttl_ms: u64,
    ) -> Self {
        Self {
            node_id,
            directory,
            publisher,
            lease_ttl_ms: lease_ttl_ms.max(1),
            generations: Mutex::new(BTreeMap::new()),
            next_generation: AtomicU64::new(0),
        }
    }

    /// Announce or renew a channel while it still has at least one local
    /// subscription. Renewals retain their fence; a leave followed by a later
    /// join receives a strictly newer generation.
    pub fn advertise(&self, channel_id: &str, now: TimestampMillis) {
        let generation = self.generations.lock().map_or_else(
            |_| OwnershipGeneration::new(self.next_generation.fetch_add(1, Ordering::Relaxed) + 1),
            |mut generations| {
                *generations.entry(channel_id.to_owned()).or_insert_with(|| {
                    OwnershipGeneration::new(
                        self.next_generation.fetch_add(1, Ordering::Relaxed) + 1,
                    )
                })
            },
        );
        let lease = ChatPresenceLease {
            channel_id: channel_id.to_owned(),
            node_id: self.node_id.clone(),
            generation,
            expires_at: TimestampMillis::from_unix_millis(
                now.unix_millis().saturating_add(self.lease_ttl_ms),
            ),
        };
        let _ = self.directory.advertise(lease.clone(), now);
        self.publisher.advertise(lease);
    }

    /// Withdraw the local lease after its final subscription leaves.
    pub fn withdraw(&self, channel_id: &str) {
        let generation = self
            .generations
            .lock()
            .ok()
            .and_then(|mut generations| generations.remove(channel_id));
        let Some(generation) = generation else {
            return;
        };
        let withdrawal = ChatPresenceWithdrawal {
            channel_id: channel_id.to_owned(),
            node_id: self.node_id.clone(),
            generation,
        };
        let _ = self
            .directory
            .withdraw(channel_id, &self.node_id, generation);
        self.publisher.withdraw(withdrawal);
    }

    /// Renew every channel that still has a local subscription.
    pub fn renew(&self, presence: &ChatPresenceRegistry, now: TimestampMillis) {
        let mut channels = BTreeMap::new();
        for subscription in presence.all_subscriptions() {
            channels.insert(subscription.channel_id, ());
        }
        for channel_id in channels.into_keys() {
            self.advertise(&channel_id, now);
        }
    }
}

impl ChatPresenceDirectory {
    /// Publish or renew one node's channel lease. Expired advertisements never
    /// enter the directory, and an older generation or shorter same-generation
    /// renewal never replaces a newer advertisement or withdrawal fence.
    pub fn advertise(&self, lease: ChatPresenceLease, now: TimestampMillis) -> ChatLeaseUpdate {
        if !lease.is_current_at(now) {
            return ChatLeaseUpdate::Stale;
        }
        let key = (lease.channel_id.clone(), lease.node_id.clone());
        let Ok(mut fences) = self.fences.lock() else {
            return ChatLeaseUpdate::Stale;
        };
        let Ok(mut leases) = self.leases.lock() else {
            return ChatLeaseUpdate::Stale;
        };
        if let Some(current) = leases
            .get(&lease.channel_id)
            .and_then(|nodes| nodes.get(&lease.node_id))
        {
            if lease.generation < current.generation {
                return ChatLeaseUpdate::Stale;
            }
            if lease.generation == current.generation && lease.expires_at <= current.expires_at {
                return ChatLeaseUpdate::Applied;
            }
        } else if fences
            .get(&key)
            .is_some_and(|fence| lease.generation <= *fence)
        {
            return ChatLeaseUpdate::Stale;
        }
        fences
            .entry(key)
            .and_modify(|fence| *fence = (*fence).max(lease.generation))
            .or_insert(lease.generation);
        leases
            .entry(lease.channel_id.clone())
            .or_default()
            .insert(lease.node_id.clone(), lease);
        ChatLeaseUpdate::Applied
    }

    /// Withdraw a node only when its caller still owns the advertised fence.
    pub fn withdraw(
        &self,
        channel_id: &str,
        node_id: &NodeId,
        generation: OwnershipGeneration,
    ) -> ChatLeaseUpdate {
        let key = (channel_id.to_owned(), node_id.clone());
        let Ok(mut fences) = self.fences.lock() else {
            return ChatLeaseUpdate::Stale;
        };
        let Ok(mut leases) = self.leases.lock() else {
            return ChatLeaseUpdate::Stale;
        };
        let current = leases
            .get(channel_id)
            .and_then(|nodes| nodes.get(node_id))
            .map(|lease| lease.generation);
        if current != Some(generation) {
            return ChatLeaseUpdate::Stale;
        }
        let empty = leases
            .get_mut(channel_id)
            .map(|nodes| {
                nodes.remove(node_id);
                nodes.is_empty()
            })
            .unwrap_or(false);
        if empty {
            leases.remove(channel_id);
        }
        fences.insert(key, generation);
        ChatLeaseUpdate::Applied
    }

    /// Return current node destinations once each, dropping expired leases.
    pub fn destinations(&self, channel_id: &str, now: TimestampMillis) -> Vec<ChatPresenceLease> {
        let Ok(mut leases) = self.leases.lock() else {
            return Vec::new();
        };
        let Some(nodes) = leases.get_mut(channel_id) else {
            return Vec::new();
        };
        nodes.retain(|_, lease| lease.is_current_at(now));
        let destinations = nodes.values().cloned().collect();
        if nodes.is_empty() {
            leases.remove(channel_id);
        }
        destinations
    }

    /// Whether one destination still owns the exact lease fence referenced by
    /// a delivery command. Receivers use this before touching local presence.
    #[must_use]
    pub fn matches_destination(
        &self,
        channel_id: &str,
        node_id: &NodeId,
        generation: OwnershipGeneration,
        now: TimestampMillis,
    ) -> bool {
        self.destinations(channel_id, now)
            .into_iter()
            .any(|lease| lease.node_id == *node_id && lease.generation == generation)
    }

    /// Validate a received delivery against both its advertised node fence and
    /// the current local authority epoch. The caller may schedule fan-out only
    /// after this returns [`ChatDeliveryDisposition::Delivered`]; no remote
    /// participant or socket identity is ever involved.
    #[must_use]
    pub fn validate_local_delivery(
        &self,
        local_node: &NodeId,
        delivery: &RemoteChatDelivery,
        local_presence: &ChatPresenceRegistry,
        now: TimestampMillis,
    ) -> ChatDeliveryDisposition {
        if !self.matches_destination(
            &delivery.channel_id,
            local_node,
            delivery.destination_generation,
            now,
        ) {
            return ChatDeliveryDisposition::Stale;
        }
        match local_presence
            .subscribers_at_authority_epoch(&delivery.channel_id, delivery.authority_epoch)
        {
            Ok(subscribers) if subscribers.is_empty() => {
                return ChatDeliveryDisposition::Unknown;
            }
            Err(_) => return ChatDeliveryDisposition::Unavailable,
            Ok(_) => {}
        }
        ChatDeliveryDisposition::Delivered
    }
}

/// One durable event targeted to a currently leased destination node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteChatDelivery {
    /// Stable durable event identity, unique within `channel_id`.
    pub event_id: u64,
    /// Channel whose local subscribers may receive the event.
    pub channel_id: String,
    /// Destination lease fence observed by the source.
    pub destination_generation: OwnershipGeneration,
    /// Authority epoch captured by the source after the durable mutation.
    pub authority_epoch: u64,
    /// Serialized `KIND_CHAT_EVENT` JSON, bounded by the transport.
    pub payload: String,
    /// Deadline after which the receiver must acknowledge a no-op.
    pub deadline: TimestampMillis,
}

/// Build a destination-fenced command from one durable source row.
///
/// The dispatcher supplies only the current lease generation. Event identity,
/// authority fence, payload, and exclusive retry deadline come directly from
/// the committed row so a retry cannot silently widen authorization or its
/// delivery window.
#[must_use]
pub fn remote_delivery_from_outbox(
    record: &ChatDeliveryOutboxRecord,
    destination_generation: OwnershipGeneration,
) -> RemoteChatDelivery {
    RemoteChatDelivery {
        event_id: record.event_id,
        channel_id: record.channel_id.clone(),
        destination_generation,
        authority_epoch: record.authority_epoch,
        payload: record.payload.clone(),
        deadline: record.expires_at,
    }
}

/// A bounded dispatcher view over durable remote-delivery rows.
///
/// The repository owns persistence and transactionality; this structure only
/// keeps the currently loaded retry set bounded while a worker attempts typed
/// commands. It never represents a socket queue.
#[derive(Debug)]
pub struct ChatDeliveryOutbox {
    capacity: usize,
    pending: Mutex<VecDeque<PendingChatDelivery>>,
}

/// One destination-specific retry record loaded from the durable outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingChatDelivery {
    /// Remote node selected from a current channel lease.
    pub destination: NodeId,
    /// The typed durable event to retry until expiry or acknowledgement.
    pub delivery: RemoteChatDelivery,
    /// Attempts made by this dispatcher instance.
    pub attempts: u32,
}

/// Outcome of loading one durable row into the bounded dispatcher.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatOutboxLoad {
    /// The row is available for delivery attempts.
    Loaded,
    /// The same destination command is already pending.
    Duplicate,
    /// The row is already outside its retry window.
    Expired,
    /// The dispatcher is at its explicit in-memory bound.
    Full,
}

impl ChatDeliveryOutbox {
    /// Create a dispatcher with a non-zero in-memory retry bound.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            pending: Mutex::new(VecDeque::new()),
        }
    }

    /// Load one durable row unless it is expired, duplicated, or exceeds the
    /// bounded active retry set.
    pub fn load(
        &self,
        destination: NodeId,
        delivery: RemoteChatDelivery,
        now: TimestampMillis,
    ) -> ChatOutboxLoad {
        if delivery.deadline <= now {
            return ChatOutboxLoad::Expired;
        }
        let Ok(mut pending) = self.pending.lock() else {
            return ChatOutboxLoad::Full;
        };
        pending.retain(|item| item.delivery.deadline > now);
        if pending
            .iter()
            .any(|item| item.destination == destination && item.delivery == delivery)
        {
            return ChatOutboxLoad::Duplicate;
        }
        if pending.len() >= self.capacity {
            return ChatOutboxLoad::Full;
        }
        pending.push_back(PendingChatDelivery {
            destination,
            delivery,
            attempts: 0,
        });
        ChatOutboxLoad::Loaded
    }

    /// Convert and load one durable source row for a current destination lease.
    /// This is the only dispatcher entry point intended for repository rows, so
    /// authority and expiry fences cannot be reconstructed inconsistently.
    pub fn load_record(
        &self,
        destination: NodeId,
        record: &ChatDeliveryOutboxRecord,
        destination_generation: OwnershipGeneration,
        now: TimestampMillis,
    ) -> ChatOutboxLoad {
        self.load(
            destination,
            remote_delivery_from_outbox(record, destination_generation),
            now,
        )
    }

    /// Return at most `limit` active rows for a non-blocking retry pass.
    #[must_use]
    pub fn ready(&self, now: TimestampMillis, limit: usize) -> Vec<PendingChatDelivery> {
        let Ok(mut pending) = self.pending.lock() else {
            return Vec::new();
        };
        pending.retain(|item| item.delivery.deadline > now);
        pending.iter().take(limit).cloned().collect()
    }

    /// Record a bounded retry attempt for one loaded row.
    pub fn record_attempt(&self, item: &PendingChatDelivery) {
        if let Ok(mut pending) = self.pending.lock()
            && let Some(current) = pending.iter_mut().find(|current| {
                current.destination == item.destination && current.delivery == item.delivery
            })
        {
            current.attempts = current.attempts.saturating_add(1);
        }
    }

    /// Remove one row only after the destination acknowledges the typed command.
    pub fn acknowledge(&self, item: &PendingChatDelivery) -> bool {
        let Ok(mut pending) = self.pending.lock() else {
            return false;
        };
        let before = pending.len();
        pending.retain(|current| {
            current.destination != item.destination || current.delivery != item.delivery
        });
        pending.len() != before
    }
}

/// Summary from one bounded durable-delivery pass.
///
/// A source row is acknowledged only after every lease observed in that pass
/// produced a terminal typed result. This deliberately avoids a first-ACK-wins
/// bug when a channel has subscribers on several remote nodes: a transient
/// failure on one node leaves the source row intact, making already-accepted
/// nodes receive a safe duplicate on the next pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ChatDeliveryDispatchStats {
    /// Non-expired source rows read from durable storage.
    pub loaded: usize,
    /// Typed node-control commands attempted.
    pub attempted: usize,
    /// Source rows removed after every observed destination was terminal.
    pub acknowledged: usize,
    /// Source rows retained for a later retry due to an unavailable peer or a
    /// refreshed destination fence.
    pub deferred: usize,
}

/// Repository-backed dispatcher for the deliberately narrow chat command.
///
/// It resolves current channel leases for each pass instead of persisting a
/// participant, socket, or permanent destination list. A new subscriber
/// reconciles through history; an existing leased destination gets at-least-once
/// delivery until the bounded source row expires. The callback is synchronous
/// because the mTLS control router has a finite socket timeout and this worker
/// is run away from the realtime reactor.
type LocalChatDeliverySender =
    dyn Fn(RemoteChatDelivery) -> Result<ChatDeliveryDisposition, ()> + Send + Sync;
type ChatDeliverySender =
    dyn Fn(&NodeId, RemoteChatDelivery) -> Result<ChatDeliveryDisposition, ()> + Send + Sync;

pub struct ChatDeliveryDispatcher {
    source: NodeId,
    repository: Arc<dyn ChatRepository>,
    directory: Arc<ChatPresenceDirectory>,
    deliver_local: Arc<LocalChatDeliverySender>,
    deliver_remote: Arc<ChatDeliverySender>,
}

impl std::fmt::Debug for ChatDeliveryDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatDeliveryDispatcher")
            .field("source", &self.source)
            .field("repository", &"[configured]")
            .field("directory", &"[configured]")
            .finish_non_exhaustive()
    }
}

impl ChatDeliveryDispatcher {
    /// Construct a dispatcher that attempts source-local delivery before any
    /// current remote destinations and before acknowledging the durable row.
    #[must_use]
    pub fn new_with_local_delivery(
        source: NodeId,
        repository: Arc<dyn ChatRepository>,
        directory: Arc<ChatPresenceDirectory>,
        deliver_local: Arc<LocalChatDeliverySender>,
        deliver_remote: Arc<ChatDeliverySender>,
    ) -> Self {
        Self {
            source,
            repository,
            directory,
            deliver_local,
            deliver_remote,
        }
    }

    /// Attempt at most `limit` durable source rows without blocking a socket
    /// reactor. Expiry is deliberately handled by bounded maintenance: history
    /// remains the recovery source once a row is outside its retry window.
    pub async fn dispatch_once(
        &self,
        now: TimestampMillis,
        limit: usize,
    ) -> crate::error::AppResult<ChatDeliveryDispatchStats> {
        let mut stats = ChatDeliveryDispatchStats::default();
        let rows = self
            .repository
            .active_delivery_outbox(self.source.as_str(), now, limit)
            .await?;
        stats.loaded = rows.len();
        for row in rows {
            let mut complete = true;
            let local_delivery = remote_delivery_from_outbox(&row, OwnershipGeneration::new(0));
            match (self.deliver_local)(local_delivery) {
                Ok(
                    ChatDeliveryDisposition::Delivered
                    | ChatDeliveryDisposition::Unknown
                    | ChatDeliveryDisposition::Rejected,
                ) => {}
                Ok(ChatDeliveryDisposition::Stale | ChatDeliveryDisposition::Unavailable)
                | Err(()) => complete = false,
            }
            for lease in self.directory.destinations(&row.channel_id, now) {
                if lease.node_id == self.source {
                    continue;
                }
                stats.attempted += 1;
                let delivery = remote_delivery_from_outbox(&row, lease.generation);
                match (self.deliver_remote)(&lease.node_id, delivery) {
                    Ok(
                        ChatDeliveryDisposition::Delivered
                        | ChatDeliveryDisposition::Unknown
                        | ChatDeliveryDisposition::Rejected,
                    ) => {}
                    // A stale fence must be re-resolved; a transport error is
                    // likewise retryable until the durable deadline.
                    Ok(ChatDeliveryDisposition::Stale | ChatDeliveryDisposition::Unavailable)
                    | Err(()) => complete = false,
                }
            }
            if complete {
                if self
                    .repository
                    .acknowledge_delivery_outbox(
                        self.source.as_str(),
                        &row.channel_id,
                        row.event_id,
                    )
                    .await?
                {
                    stats.acknowledged += 1;
                }
            } else {
                stats.deferred += 1;
            }
        }
        Ok(stats)
    }

    /// Purge a bounded number of already-expired rows. This is separate from
    /// dispatch so an overloaded peer cannot turn cleanup into unbounded work.
    pub async fn cleanup_expired(
        &self,
        now: TimestampMillis,
        limit: usize,
    ) -> crate::error::AppResult<usize> {
        self.repository.cleanup_delivery_outbox(now, limit).await
    }
}

/// Typed outcome of a remote delivery command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChatDeliveryDisposition {
    /// The destination accepted the event (or a safe duplicate).
    Delivered,
    /// The advertised fence is no longer current; re-resolve before retrying.
    Stale,
    /// The destination has no matching local subscription.
    Unknown,
    /// Authentication, bounds, or deadline validation failed closed.
    Rejected,
    /// The destination infrastructure disappeared before it could decide.
    /// Unlike authoritative subscriber absence, this outcome remains retryable.
    Unavailable,
}

/// Narrow command boundary for cross-node chat delivery.
pub trait ChatPresenceRouter: Send + Sync {
    /// Deliver one durable chat event to a destination's local subscriptions.
    fn deliver(
        &self,
        source: &NodeId,
        destination: &NodeId,
        delivery: RemoteChatDelivery,
    ) -> ChatDeliveryDisposition;
}

/// Test-only/reference router that preserves the production command boundary
/// without pretending to provide transport authentication. Production startup
/// binds this contract to the separately authenticated control listener.
#[derive(Default)]
pub struct InMemoryChatPresenceRouter {
    handlers: Mutex<BTreeMap<NodeId, DeliveryHandler>>,
}

impl std::fmt::Debug for InMemoryChatPresenceRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryChatPresenceRouter")
            .field("handlers", &"[registered]")
            .finish()
    }
}

type DeliveryHandler =
    Arc<dyn Fn(&NodeId, RemoteChatDelivery) -> ChatDeliveryDisposition + Send + Sync>;

impl InMemoryChatPresenceRouter {
    /// Register the one local delivery handler for a node.
    pub fn register(&self, node: NodeId, handler: DeliveryHandler) {
        if let Ok(mut handlers) = self.handlers.lock() {
            handlers.insert(node, handler);
        }
    }
}

impl ChatPresenceRouter for InMemoryChatPresenceRouter {
    fn deliver(
        &self,
        source: &NodeId,
        destination: &NodeId,
        delivery: RemoteChatDelivery,
    ) -> ChatDeliveryDisposition {
        let Ok(handlers) = self.handlers.lock() else {
            return ChatDeliveryDisposition::Unavailable;
        };
        handlers
            .get(destination)
            .map_or(ChatDeliveryDisposition::Unavailable, |handler| {
                handler(source, delivery)
            })
    }
}

type ChatDedupeKey = (NodeId, String, u64, OwnershipGeneration, u64);
type ChatDedupeState = (
    BTreeMap<ChatDedupeKey, ChatDeliveryDisposition>,
    VecDeque<ChatDedupeKey>,
);

/// Bounded idempotency cache for one `(source node, channel, event, destination
/// fence, authority epoch)` command. A source must be able to retry the same
/// durable event after it resolves a newer destination fence, while exact
/// retries retain their original disposition.
#[derive(Debug)]
pub struct ChatCommandDedupe {
    capacity: usize,
    seen: Mutex<ChatDedupeState>,
}

impl ChatCommandDedupe {
    /// Construct a cache with an explicit non-zero command bound.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            seen: Mutex::new((BTreeMap::new(), VecDeque::new())),
        }
    }

    /// Return a previous disposition, or remember the newly evaluated one.
    pub fn remember(
        &self,
        source: NodeId,
        delivery: &RemoteChatDelivery,
        disposition: ChatDeliveryDisposition,
    ) -> ChatDeliveryDisposition {
        self.evaluate(source, delivery, || disposition)
    }

    /// Return a cached terminal result or evaluate local delivery without
    /// holding the dedupe mutex. Concurrent misses may both evaluate; the
    /// terminal result first inserted wins. Infrastructure failures remain
    /// uncached so durable delivery can retry.
    pub fn evaluate(
        &self,
        source: NodeId,
        delivery: &RemoteChatDelivery,
        evaluate: impl FnOnce() -> ChatDeliveryDisposition,
    ) -> ChatDeliveryDisposition {
        let key = (
            source,
            delivery.channel_id.clone(),
            delivery.event_id,
            delivery.destination_generation,
            delivery.authority_epoch,
        );
        match self.seen.lock() {
            Ok(state) => {
                if let Some(previous) = state.0.get(&key) {
                    return *previous;
                }
            }
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                *state = (BTreeMap::new(), VecDeque::new());
                self.seen.clear_poison();
                drop(state);
                return ChatDeliveryDisposition::Unavailable;
            }
        }

        let disposition = evaluate();
        if disposition == ChatDeliveryDisposition::Unavailable {
            return disposition;
        }

        let mut state = match self.seen.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                *state = (BTreeMap::new(), VecDeque::new());
                self.seen.clear_poison();
                drop(state);
                return ChatDeliveryDisposition::Unavailable;
            }
        };
        if let Some(previous) = state.0.get(&key) {
            return *previous;
        }
        state.0.insert(key.clone(), disposition);
        state.1.push_back(key);
        while state.1.len() > self.capacity {
            if let Some(expired) = state.1.pop_front() {
                state.0.remove(&expired);
            }
        }
        disposition
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::realtime::registry::ParticipantId;
    use crate::repository::InMemoryChatRepository;
    use crate::services::ChatTarget;
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct RecordingPublisher {
        advertised: Mutex<Vec<ChatPresenceLease>>,
        withdrawn: Mutex<Vec<ChatPresenceWithdrawal>>,
    }

    impl ChatPresencePublisher for RecordingPublisher {
        fn advertise(&self, lease: ChatPresenceLease) {
            self.advertised.lock().expect("publisher lock").push(lease);
        }

        fn withdraw(&self, withdrawal: ChatPresenceWithdrawal) {
            self.withdrawn
                .lock()
                .expect("publisher lock")
                .push(withdrawal);
        }
    }

    fn node(value: &str) -> NodeId {
        NodeId::new(value.to_owned()).expect("test node id")
    }

    fn delivery() -> RemoteChatDelivery {
        RemoteChatDelivery {
            event_id: 7,
            channel_id: "ch_1".to_owned(),
            destination_generation: OwnershipGeneration::new(3),
            authority_epoch: 4,
            payload: r#"{\"type\":\"message.create\"}"#.to_owned(),
            deadline: TimestampMillis::from_unix_millis(100),
        }
    }

    #[test]
    fn dispatcher_exposes_only_the_explicit_local_delivery_constructor() {
        let source = include_str!("chat_cluster.rs");
        let constructors = source
            .split("impl ChatDeliveryDispatcher")
            .nth(1)
            .expect("dispatcher implementation")
            .split("pub async fn dispatch_once")
            .next()
            .expect("constructor section");
        assert!(constructors.contains("pub fn new_with_local_delivery"));
        assert_eq!(
            constructors.matches("pub fn ").count(),
            1,
            "no constructor may install an implicit terminal local-delivery callback"
        );
    }

    #[test]
    fn local_announcer_renews_and_fences_a_leave_then_rejoin() {
        let directory = Arc::new(ChatPresenceDirectory::default());
        let publisher = Arc::new(RecordingPublisher::default());
        let announcer = LocalChatPresenceAnnouncer::new(
            node("node-a"),
            Arc::clone(&directory),
            publisher.clone(),
            50,
        );
        let first = TimestampMillis::from_unix_millis(10);
        announcer.advertise("ch_1", first);
        announcer.advertise("ch_1", TimestampMillis::from_unix_millis(20));
        let advertised = publisher.advertised.lock().expect("publisher lock");
        assert_eq!(advertised.len(), 2);
        assert_eq!(advertised[0].generation, advertised[1].generation);
        assert!(advertised[1].expires_at > advertised[0].expires_at);
        let first_generation = advertised[0].generation;
        drop(advertised);

        announcer.withdraw("ch_1");
        assert!(
            directory
                .destinations("ch_1", TimestampMillis::from_unix_millis(21))
                .is_empty()
        );
        announcer.advertise("ch_1", TimestampMillis::from_unix_millis(22));
        let advertised = publisher.advertised.lock().expect("publisher lock");
        assert!(advertised[2].generation > first_generation);
        assert_eq!(publisher.withdrawn.lock().expect("publisher lock").len(), 1);
    }

    #[test]
    fn durable_outbox_row_preserves_its_authority_fence_and_deadline() {
        let record = ChatDeliveryOutboxRecord {
            origin_node_id: "node-a".to_owned(),
            channel_id: "ch_1".to_owned(),
            event_id: 7,
            authority_epoch: 4,
            payload: r#"{"type":"message.create"}"#.to_owned(),
            created_at: TimestampMillis::from_unix_millis(10),
            expires_at: TimestampMillis::from_unix_millis(100),
        };
        let delivery = remote_delivery_from_outbox(&record, OwnershipGeneration::new(3));

        assert_eq!(delivery.event_id, record.event_id);
        assert_eq!(delivery.authority_epoch, record.authority_epoch);
        assert_eq!(delivery.deadline, record.expires_at);
        assert_eq!(delivery.destination_generation, OwnershipGeneration::new(3));
    }

    #[test]
    fn dispatcher_loads_a_durable_row_with_its_committed_fences() {
        let record = ChatDeliveryOutboxRecord {
            origin_node_id: "node-a".to_owned(),
            channel_id: "ch_1".to_owned(),
            event_id: 8,
            authority_epoch: 9,
            payload: r#"{"type":"message.update"}"#.to_owned(),
            created_at: TimestampMillis::from_unix_millis(1),
            expires_at: TimestampMillis::from_unix_millis(100),
        };
        let outbox = ChatDeliveryOutbox::new(1);

        assert_eq!(
            outbox.load_record(
                node("node-b"),
                &record,
                OwnershipGeneration::new(5),
                TimestampMillis::from_unix_millis(2),
            ),
            ChatOutboxLoad::Loaded
        );
        let pending = outbox.ready(TimestampMillis::from_unix_millis(2), 1);
        assert_eq!(pending[0].delivery.authority_epoch, 9);
        assert_eq!(pending[0].delivery.deadline, record.expires_at);
        assert_eq!(
            pending[0].delivery.destination_generation,
            OwnershipGeneration::new(5)
        );
    }

    #[test]
    fn router_is_channel_typed_and_never_routes_to_unknown_nodes() {
        let router = InMemoryChatPresenceRouter::default();
        router.register(
            node("node-b"),
            Arc::new(|source, command| {
                assert_eq!(source.as_str(), "node-a");
                assert_eq!(command.channel_id, "ch_1");
                ChatDeliveryDisposition::Delivered
            }),
        );
        assert_eq!(
            router.deliver(&node("node-a"), &node("node-b"), delivery()),
            ChatDeliveryDisposition::Delivered
        );
        assert_eq!(
            router.deliver(&node("node-a"), &node("node-c"), delivery()),
            ChatDeliveryDisposition::Unavailable
        );
    }

    #[test]
    fn duplicate_commands_retain_the_first_safe_disposition() {
        let dedupe = ChatCommandDedupe::new(1);
        let command = delivery();
        assert_eq!(
            dedupe.remember(node("node-a"), &command, ChatDeliveryDisposition::Delivered),
            ChatDeliveryDisposition::Delivered
        );
        assert_eq!(
            dedupe.remember(node("node-a"), &command, ChatDeliveryDisposition::Rejected),
            ChatDeliveryDisposition::Delivered
        );
    }

    #[test]
    fn outbox_retries_until_acknowledgement_and_drops_expired_rows() {
        let outbox = ChatDeliveryOutbox::new(1);
        let command = delivery();
        assert_eq!(
            outbox.load(
                node("node-b"),
                command.clone(),
                TimestampMillis::from_unix_millis(1),
            ),
            ChatOutboxLoad::Loaded
        );
        let mut later = command.clone();
        later.event_id = 9;
        assert_eq!(
            outbox.load(node("node-b"), later, TimestampMillis::from_unix_millis(1)),
            ChatOutboxLoad::Full
        );
        let pending = outbox.ready(TimestampMillis::from_unix_millis(2), 1);
        assert_eq!(pending.len(), 1);
        outbox.record_attempt(&pending[0]);
        assert_eq!(
            outbox.ready(TimestampMillis::from_unix_millis(2), 1)[0].attempts,
            1
        );
        assert!(outbox.acknowledge(&pending[0]));
        assert!(
            outbox
                .ready(TimestampMillis::from_unix_millis(2), 1)
                .is_empty()
        );

        let mut expired = command;
        expired.event_id = 8;
        expired.deadline = TimestampMillis::from_unix_millis(2);
        assert_eq!(
            outbox.load(
                node("node-b"),
                expired,
                TimestampMillis::from_unix_millis(2)
            ),
            ChatOutboxLoad::Expired
        );
    }

    #[test]
    fn retry_after_a_stale_destination_fence_is_evaluated_again() {
        let dedupe = ChatCommandDedupe::new(2);
        let mut stale = delivery();
        stale.destination_generation = OwnershipGeneration::new(2);
        assert_eq!(
            dedupe.remember(node("node-a"), &stale, ChatDeliveryDisposition::Stale),
            ChatDeliveryDisposition::Stale
        );

        let current = delivery();
        assert_eq!(
            dedupe.remember(node("node-a"), &current, ChatDeliveryDisposition::Delivered),
            ChatDeliveryDisposition::Delivered,
            "a refreshed lease fence must not inherit the stale response"
        );
    }

    #[test]
    fn infrastructure_unavailable_is_not_deduplicated_across_recovery() {
        let dedupe = ChatCommandDedupe::new(2);
        let command = delivery();
        let evaluations = AtomicUsize::new(0);
        assert_eq!(
            dedupe.evaluate(node("node-a"), &command, || {
                evaluations.fetch_add(1, Ordering::SeqCst);
                ChatDeliveryDisposition::Unavailable
            }),
            ChatDeliveryDisposition::Unavailable
        );
        assert_eq!(
            dedupe.evaluate(node("node-a"), &command, || {
                evaluations.fetch_add(1, Ordering::SeqCst);
                ChatDeliveryDisposition::Delivered
            }),
            ChatDeliveryDisposition::Delivered,
            "a recovered gateway must reevaluate the retained command"
        );
        assert_eq!(evaluations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn poisoned_dedupe_defers_durable_ack_then_recovers_for_delivery() {
        let repository = Arc::new(InMemoryChatRepository::new());
        let now = TimestampMillis::from_unix_millis(10);
        repository
            .stage_delivery_outbox(ChatDeliveryOutboxRecord {
                origin_node_id: "node-a".to_owned(),
                channel_id: "ch_poison".to_owned(),
                event_id: 21,
                authority_epoch: 4,
                payload:
                    r#"{"version":1,"type":"message.create","channel_id":"ch_poison","event_id":21}"#
                        .to_owned(),
                created_at: now,
                expires_at: TimestampMillis::from_unix_millis(100),
            })
            .expect("stage poison-recovery row");
        let dedupe = Arc::new(ChatCommandDedupe::new(2));
        let poison_result = std::panic::catch_unwind({
            let dedupe = Arc::clone(&dedupe);
            move || {
                let _guard = dedupe.seen.lock().expect("dedupe lock before poisoning");
                assert!(
                    dedupe.seen.is_poisoned(),
                    "deliberately poison the dedupe mutex"
                );
            }
        });
        assert!(poison_result.is_err(), "poisoning assertion must unwind");
        let handler_attempts = Arc::new(AtomicUsize::new(0));
        let dispatcher = ChatDeliveryDispatcher::new_with_local_delivery(
            node("node-a"),
            repository.clone(),
            Arc::new(ChatPresenceDirectory::default()),
            {
                let dedupe = Arc::clone(&dedupe);
                let handler_attempts = Arc::clone(&handler_attempts);
                Arc::new(move |delivery| {
                    Ok(dedupe.evaluate(node("node-a"), &delivery, || {
                        handler_attempts.fetch_add(1, Ordering::SeqCst);
                        ChatDeliveryDisposition::Delivered
                    }))
                })
            },
            Arc::new(|_, _| Ok(ChatDeliveryDisposition::Delivered)),
        );

        let first = dispatcher
            .dispatch_once(now, 8)
            .await
            .expect("poisoned pass");
        assert_eq!(first.acknowledged, 0);
        assert_eq!(first.deferred, 1);
        assert_eq!(handler_attempts.load(Ordering::SeqCst), 0);
        assert_eq!(
            repository
                .active_delivery_outbox("node-a", now, 8)
                .expect("row retained after poisoned dedupe")
                .len(),
            1
        );

        let retry = dispatcher
            .dispatch_once(now, 8)
            .await
            .expect("recovered pass");
        assert_eq!(retry.acknowledged, 1);
        assert_eq!(retry.deferred, 0);
        assert_eq!(handler_attempts.load(Ordering::SeqCst), 1);
        assert!(
            repository
                .active_delivery_outbox("node-a", now, 8)
                .expect("row acknowledged after recovered delivery")
                .is_empty()
        );
    }

    #[test]
    fn panicking_delivery_handler_does_not_poison_dedupe_and_retry_delivers() {
        let dedupe = ChatCommandDedupe::new(2);
        let command = delivery();
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dedupe.evaluate(node("node-a"), &command, || {
                assert!(
                    dedupe.seen.is_poisoned(),
                    "deliberately panic in the external delivery handler"
                );
                ChatDeliveryDisposition::Delivered
            })
        }));
        assert!(panic_result.is_err(), "handler assertion must unwind");
        assert!(
            !dedupe.seen.is_poisoned(),
            "external handler panic must not poison the dedupe mutex"
        );
        assert_eq!(
            dedupe.evaluate(node("node-a"), &command, || {
                ChatDeliveryDisposition::Delivered
            }),
            ChatDeliveryDisposition::Delivered
        );
    }

    #[test]
    fn concurrent_duplicates_evaluate_without_holding_the_dedupe_lock() {
        let dedupe = Arc::new(ChatCommandDedupe::new(8));
        let command = Arc::new(delivery());
        let start = Arc::new(Barrier::new(2));
        let evaluating = Arc::new(Barrier::new(2));
        let scheduled = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for _ in 0..2 {
                let dedupe = Arc::clone(&dedupe);
                let command = Arc::clone(&command);
                let start = Arc::clone(&start);
                let evaluating = Arc::clone(&evaluating);
                let scheduled = Arc::clone(&scheduled);
                scope.spawn(move || {
                    start.wait();
                    assert_eq!(
                        dedupe.evaluate(node("node-a"), &command, || {
                            scheduled.fetch_add(1, Ordering::SeqCst);
                            evaluating.wait();
                            ChatDeliveryDisposition::Delivered
                        }),
                        ChatDeliveryDisposition::Delivered
                    );
                });
            }
        });
        assert_eq!(
            scheduled.load(Ordering::SeqCst),
            2,
            "at-least-once concurrent duplicates may both reach external delivery"
        );
    }

    #[test]
    fn directory_prevents_a_delayed_lease_from_reviving_a_withdrawn_node() {
        let directory = ChatPresenceDirectory::default();
        let lease = ChatPresenceLease {
            channel_id: "ch_1".to_owned(),
            node_id: node("node-b"),
            generation: OwnershipGeneration::new(3),
            expires_at: TimestampMillis::from_unix_millis(100),
        };
        assert_eq!(
            directory.advertise(lease.clone(), TimestampMillis::from_unix_millis(1)),
            ChatLeaseUpdate::Applied
        );
        assert_eq!(
            directory.withdraw(&lease.channel_id, &lease.node_id, lease.generation),
            ChatLeaseUpdate::Applied
        );
        assert_eq!(
            directory.advertise(lease, TimestampMillis::from_unix_millis(2)),
            ChatLeaseUpdate::Stale
        );
        assert!(
            directory
                .destinations("ch_1", TimestampMillis::from_unix_millis(2))
                .is_empty()
        );
    }

    #[test]
    fn directory_returns_each_current_node_once_and_expires_lost_leases() {
        let directory = ChatPresenceDirectory::default();
        for (node_id, expiry) in [("node-b", 10), ("node-c", 20)] {
            assert_eq!(
                directory.advertise(
                    ChatPresenceLease {
                        channel_id: "ch_1".to_owned(),
                        node_id: node(node_id),
                        generation: OwnershipGeneration::new(1),
                        expires_at: TimestampMillis::from_unix_millis(expiry),
                    },
                    TimestampMillis::from_unix_millis(1),
                ),
                ChatLeaseUpdate::Applied
            );
        }
        let destinations = directory.destinations("ch_1", TimestampMillis::from_unix_millis(10));
        assert_eq!(destinations.len(), 1);
        assert_eq!(destinations[0].node_id, node("node-c"));
    }

    #[test]
    fn delayed_same_generation_renewal_never_shortens_a_live_lease() {
        let directory = ChatPresenceDirectory::default();
        let node_b = node("node-b");
        let current = ChatPresenceLease {
            channel_id: "ch_1".to_owned(),
            node_id: node_b.clone(),
            generation: OwnershipGeneration::new(3),
            expires_at: TimestampMillis::from_unix_millis(100),
        };
        assert_eq!(
            directory.advertise(current.clone(), TimestampMillis::from_unix_millis(1)),
            ChatLeaseUpdate::Applied
        );
        let mut delayed = current;
        delayed.expires_at = TimestampMillis::from_unix_millis(20);
        assert_eq!(
            directory.advertise(delayed, TimestampMillis::from_unix_millis(2)),
            ChatLeaseUpdate::Applied
        );
        assert!(directory.matches_destination(
            "ch_1",
            &node_b,
            OwnershipGeneration::new(3),
            TimestampMillis::from_unix_millis(50),
        ));
    }

    #[test]
    fn directory_requires_the_exact_destination_generation() {
        let directory = ChatPresenceDirectory::default();
        let node_b = node("node-b");
        directory.advertise(
            ChatPresenceLease {
                channel_id: "ch_1".to_owned(),
                node_id: node_b.clone(),
                generation: OwnershipGeneration::new(4),
                expires_at: TimestampMillis::from_unix_millis(100),
            },
            TimestampMillis::from_unix_millis(1),
        );
        assert!(directory.matches_destination(
            "ch_1",
            &node_b,
            OwnershipGeneration::new(4),
            TimestampMillis::from_unix_millis(2),
        ));
        assert!(!directory.matches_destination(
            "ch_1",
            &node_b,
            OwnershipGeneration::new(3),
            TimestampMillis::from_unix_millis(2),
        ));
    }

    #[test]
    fn local_delivery_requires_matching_lease_and_authority_epoch() {
        let directory = ChatPresenceDirectory::default();
        let node_b = node("node-b");
        directory.advertise(
            ChatPresenceLease {
                channel_id: "ch_1".to_owned(),
                node_id: node_b.clone(),
                generation: OwnershipGeneration::new(3),
                expires_at: TimestampMillis::from_unix_millis(100),
            },
            TimestampMillis::from_unix_millis(1),
        );
        let presence = ChatPresenceRegistry::new();
        presence.join(
            "ch_1",
            ParticipantId::from_raw(1),
            "alice",
            ChatTarget::CurrentRoom { room_id: 7 },
            4,
        );
        assert_eq!(
            directory.validate_local_delivery(
                &node_b,
                &delivery(),
                &presence,
                TimestampMillis::from_unix_millis(2),
            ),
            ChatDeliveryDisposition::Delivered
        );
        let mut revoked = delivery();
        revoked.authority_epoch = 5;
        assert_eq!(
            directory.validate_local_delivery(
                &node_b,
                &revoked,
                &presence,
                TimestampMillis::from_unix_millis(2),
            ),
            ChatDeliveryDisposition::Unknown
        );
        let mut stale = delivery();
        stale.destination_generation = OwnershipGeneration::new(2);
        assert_eq!(
            directory.validate_local_delivery(
                &node_b,
                &stale,
                &presence,
                TimestampMillis::from_unix_millis(2),
            ),
            ChatDeliveryDisposition::Stale
        );
    }

    #[tokio::test]
    async fn concurrent_dispatchers_only_deliver_and_acknowledge_their_origin_rows() {
        let repository = Arc::new(InMemoryChatRepository::new());
        let directory = Arc::new(ChatPresenceDirectory::default());
        let now = TimestampMillis::from_unix_millis(10);
        for (event_id, origin_node_id) in [(21, "node-a"), (22, "node-b")] {
            repository
                .stage_delivery_outbox(ChatDeliveryOutboxRecord {
                    channel_id: "ch_shared".to_owned(),
                    event_id,
                    origin_node_id: origin_node_id.to_owned(),
                    authority_epoch: 4,
                    payload: r#"{"type":"message.create"}"#.to_owned(),
                    created_at: now,
                    expires_at: TimestampMillis::from_unix_millis(100),
                })
                .expect("stage origin-owned row");
        }
        assert_eq!(
            directory.advertise(
                ChatPresenceLease {
                    channel_id: "ch_shared".to_owned(),
                    node_id: node("node-c"),
                    generation: OwnershipGeneration::new(1),
                    expires_at: TimestampMillis::from_unix_millis(100),
                },
                now,
            ),
            ChatLeaseUpdate::Applied
        );

        let deliveries_a = Arc::new(Mutex::new(Vec::new()));
        let deliveries_b = Arc::new(Mutex::new(Vec::new()));
        let dispatcher_a = ChatDeliveryDispatcher::new_with_local_delivery(
            node("node-a"),
            repository.clone(),
            directory.clone(),
            Arc::new(|_| Ok(ChatDeliveryDisposition::Rejected)),
            {
                let deliveries = Arc::clone(&deliveries_a);
                Arc::new(move |_, delivery| {
                    deliveries
                        .lock()
                        .expect("deliveries a lock")
                        .push(delivery.event_id);
                    Ok(ChatDeliveryDisposition::Delivered)
                })
            },
        );
        let dispatcher_b = ChatDeliveryDispatcher::new_with_local_delivery(
            node("node-b"),
            repository.clone(),
            directory,
            Arc::new(|_| Ok(ChatDeliveryDisposition::Rejected)),
            {
                let deliveries = Arc::clone(&deliveries_b);
                Arc::new(move |_, delivery| {
                    deliveries
                        .lock()
                        .expect("deliveries b lock")
                        .push(delivery.event_id);
                    Ok(ChatDeliveryDisposition::Delivered)
                })
            },
        );

        let (stats_a, stats_b) = tokio::join!(
            dispatcher_a.dispatch_once(now, 8),
            dispatcher_b.dispatch_once(now, 8),
        );
        assert_eq!(stats_a.expect("node a dispatch").loaded, 1);
        assert_eq!(stats_b.expect("node b dispatch").loaded, 1);
        assert_eq!(*deliveries_a.lock().expect("deliveries a lock"), vec![21]);
        assert_eq!(*deliveries_b.lock().expect("deliveries b lock"), vec![22]);
        assert!(
            repository
                .active_delivery_outbox("node-a", now, 8)
                .expect("node a rows after ack")
                .is_empty()
        );
        assert!(
            repository
                .active_delivery_outbox("node-b", now, 8)
                .expect("node b rows after ack")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn durable_dispatch_waits_for_every_remote_destination_before_acknowledging() {
        let repository = Arc::new(InMemoryChatRepository::new());
        let directory = Arc::new(ChatPresenceDirectory::default());
        let now = TimestampMillis::from_unix_millis(10);
        repository
            .stage_delivery_outbox(ChatDeliveryOutboxRecord {
                origin_node_id: "node-a".to_owned(),
                channel_id: "ch_1".to_owned(),
                event_id: 12,
                authority_epoch: 4,
                payload: r#"{"type":"message.create"}"#.to_owned(),
                created_at: now,
                expires_at: TimestampMillis::from_unix_millis(100),
            })
            .expect("stage source row");
        for node_id in ["node-b", "node-c"] {
            assert_eq!(
                directory.advertise(
                    ChatPresenceLease {
                        channel_id: "ch_1".to_owned(),
                        node_id: node(node_id),
                        generation: OwnershipGeneration::new(1),
                        expires_at: TimestampMillis::from_unix_millis(100),
                    },
                    now,
                ),
                ChatLeaseUpdate::Applied
            );
        }
        let unavailable = Arc::new(AtomicUsize::new(1));
        let attempted = Arc::new(AtomicUsize::new(0));
        let dispatcher = ChatDeliveryDispatcher::new_with_local_delivery(
            node("node-a"),
            repository.clone(),
            directory,
            Arc::new(|_| Ok(ChatDeliveryDisposition::Rejected)),
            {
                let unavailable = Arc::clone(&unavailable);
                let attempted = Arc::clone(&attempted);
                Arc::new(move |destination, delivery| {
                    attempted.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(delivery.event_id, 12);
                    if destination.as_str() == "node-c"
                        && unavailable.fetch_sub(1, Ordering::SeqCst) > 0
                    {
                        Ok(ChatDeliveryDisposition::Unavailable)
                    } else {
                        Ok(ChatDeliveryDisposition::Delivered)
                    }
                })
            },
        );

        assert_eq!(
            dispatcher.dispatch_once(now, 8).await.expect("first pass"),
            ChatDeliveryDispatchStats {
                loaded: 1,
                attempted: 2,
                acknowledged: 0,
                deferred: 1,
            }
        );
        assert_eq!(
            repository
                .active_delivery_outbox("node-a", now, 8)
                .expect("source retained")
                .len(),
            1
        );
        assert_eq!(
            dispatcher.dispatch_once(now, 8).await.expect("retry pass"),
            ChatDeliveryDispatchStats {
                loaded: 1,
                attempted: 2,
                acknowledged: 1,
                deferred: 0,
            }
        );
        assert_eq!(attempted.load(Ordering::SeqCst), 4);
        assert!(
            repository
                .active_delivery_outbox("node-a", now, 8)
                .expect("source removed after all acks")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn standalone_dispatch_retries_failed_local_delivery_before_acknowledging() {
        let repository = Arc::new(InMemoryChatRepository::new());
        let now = TimestampMillis::from_unix_millis(10);
        repository
            .stage_delivery_outbox(ChatDeliveryOutboxRecord {
                origin_node_id: "node-a".to_owned(),
                channel_id: "ch_local".to_owned(),
                event_id: 14,
                authority_epoch: 4,
                payload:
                    r#"{"version":1,"type":"message.create","channel_id":"ch_local","event_id":14}"#
                        .to_owned(),
                created_at: now,
                expires_at: TimestampMillis::from_unix_millis(100),
            })
            .expect("stage local row");
        let remaining_failures = Arc::new(AtomicUsize::new(1));
        let local_attempts = Arc::new(AtomicUsize::new(0));
        let remote_attempts = Arc::new(AtomicUsize::new(0));
        let dispatcher = ChatDeliveryDispatcher::new_with_local_delivery(
            node("node-a"),
            repository.clone(),
            Arc::new(ChatPresenceDirectory::default()),
            {
                let remaining_failures = Arc::clone(&remaining_failures);
                let local_attempts = Arc::clone(&local_attempts);
                Arc::new(move |delivery| {
                    local_attempts.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(delivery.event_id, 14);
                    if remaining_failures.fetch_sub(1, Ordering::SeqCst) > 0 {
                        Err(())
                    } else {
                        Ok(ChatDeliveryDisposition::Delivered)
                    }
                })
            },
            {
                let remote_attempts = Arc::clone(&remote_attempts);
                Arc::new(move |_, _| {
                    remote_attempts.fetch_add(1, Ordering::SeqCst);
                    Err(())
                })
            },
        );

        let first = dispatcher.dispatch_once(now, 8).await.expect("first pass");
        assert_eq!(first.acknowledged, 0);
        assert_eq!(first.deferred, 1);
        assert_eq!(
            repository
                .active_delivery_outbox("node-a", now, 8)
                .expect("source retained after local failure")
                .len(),
            1
        );

        let retry = dispatcher.dispatch_once(now, 8).await.expect("retry pass");
        assert_eq!(retry.acknowledged, 1);
        assert_eq!(retry.deferred, 0);
        assert_eq!(local_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(remote_attempts.load(Ordering::SeqCst), 0);
        assert!(
            repository
                .active_delivery_outbox("node-a", now, 8)
                .expect("source acknowledged after local success")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn durable_dispatch_expiry_is_purged_without_attempting_remote_delivery() {
        let repository = Arc::new(InMemoryChatRepository::new());
        let directory = Arc::new(ChatPresenceDirectory::default());
        let created_at = TimestampMillis::from_unix_millis(10);
        let expired_at = TimestampMillis::from_unix_millis(20);
        repository
            .stage_delivery_outbox(ChatDeliveryOutboxRecord {
                origin_node_id: "node-a".to_owned(),
                channel_id: "ch_expired".to_owned(),
                event_id: 13,
                authority_epoch: 4,
                payload: r#"{\"type\":\"message.create\"}"#.to_owned(),
                created_at,
                expires_at: expired_at,
            })
            .expect("stage expired source row");
        assert_eq!(
            directory.advertise(
                ChatPresenceLease {
                    channel_id: "ch_expired".to_owned(),
                    node_id: node("node-b"),
                    generation: OwnershipGeneration::new(1),
                    expires_at: TimestampMillis::from_unix_millis(100),
                },
                created_at,
            ),
            ChatLeaseUpdate::Applied
        );
        let attempted = Arc::new(AtomicUsize::new(0));
        let dispatcher = ChatDeliveryDispatcher::new_with_local_delivery(
            node("node-a"),
            repository.clone(),
            directory,
            Arc::new(|_| Ok(ChatDeliveryDisposition::Rejected)),
            {
                let attempted = Arc::clone(&attempted);
                Arc::new(move |_, _| {
                    attempted.fetch_add(1, Ordering::SeqCst);
                    Ok(ChatDeliveryDisposition::Delivered)
                })
            },
        );

        assert_eq!(
            dispatcher
                .dispatch_once(expired_at, 8)
                .await
                .expect("expired rows are skipped"),
            ChatDeliveryDispatchStats::default()
        );
        assert_eq!(attempted.load(Ordering::SeqCst), 0);
        assert_eq!(
            dispatcher
                .cleanup_expired(expired_at, 8)
                .await
                .expect("bounded cleanup"),
            1
        );
        assert!(
            repository
                .active_delivery_outbox("node-a", created_at, 8)
                .expect("outbox query")
                .is_empty()
        );
    }
}
