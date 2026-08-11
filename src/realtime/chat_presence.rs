//! Local, bounded chat-channel subscriptions.
//!
//! This registry deliberately owns no socket. It records the local realtime
//! subscription state while [`SessionRegistry`](super::registry::SessionRegistry)
//! remains the sole owner of bounded outbound queues. Keeping those concerns
//! separate makes a queue drop recoverable without ever turning a durable chat
//! write into a socket transaction.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::realtime::registry::ParticipantId;
use crate::services::ChatTarget;

/// One local subscription to a server-authorized chat channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatSubscription {
    /// Opaque durable channel this local subscription represents.
    pub channel_id: String,
    /// Opaque, process-local subscription identifier returned by `chat.join`.
    pub id: String,
    /// Opaque, process-local presence identifier. It is never a participant id.
    pub presence_id: String,
    /// Local socket participant that owns the subscription.
    pub participant: ParticipantId,
    /// Authenticated account that created the subscription.
    pub user_id: String,
    /// The server-derived target used to reauthorize later operations.
    pub target: ChatTarget,
    /// Authority epoch captured at join. New mutations always reauthorize.
    pub authority_epoch: u64,
    /// Whether the bounded socket queue dropped a live event.
    pub needs_resync: bool,
}

#[derive(Debug, Default)]
struct PresenceState {
    channels: HashMap<String, HashMap<ParticipantId, ChatSubscription>>,
    participants: HashMap<ParticipantId, HashSet<String>>,
}

/// Outcome of an idempotent local join.
#[derive(Debug, Clone)]
pub struct ChatJoin {
    /// The requested subscription.
    pub subscription: ChatSubscription,
    /// Existing channel subscriptions before the join, excluding the joiner.
    pub existing: Vec<ChatSubscription>,
    /// Whether this call established a new subscription.
    pub inserted: bool,
}

/// A subscription removed by an explicit leave or disconnect.
#[derive(Debug, Clone)]
pub struct ChatLeave {
    /// Channel whose local subscription was removed.
    pub channel_id: String,
    /// The removed subscription.
    pub subscription: ChatSubscription,
    /// Remaining local subscriptions in the channel after removal.
    pub remaining: Vec<ChatSubscription>,
}

/// The subscriber authority registry could not be inspected. Callers must
/// treat this as retryable infrastructure failure, never authoritative absence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChatPresenceUnavailable;

/// Process-local chat presence. It is intentionally single-node;
/// owns the leased cross-node directory and typed router.
#[derive(Debug, Default)]
pub struct ChatPresenceRegistry {
    state: Mutex<PresenceState>,
    next_id: AtomicU64,
}

impl ChatPresenceRegistry {
    /// Construct an empty local registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn poison_state_for_test(&self) {
        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self.state.lock().expect("presence lock before poisoning");
            assert!(
                self.state.is_poisoned(),
                "deliberately poison the chat presence mutex"
            );
        }));
        assert!(poison.is_err(), "poisoning assertion must unwind");
    }

    /// Idempotently add one participant to a channel.
    pub fn join(
        &self,
        channel_id: &str,
        participant: ParticipantId,
        user_id: &str,
        target: ChatTarget,
        authority_epoch: u64,
    ) -> ChatJoin {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let channel = state.channels.entry(channel_id.to_owned()).or_default();
        if let Some(subscription) = channel.get(&participant) {
            return ChatJoin {
                subscription: subscription.clone(),
                existing: channel
                    .values()
                    .filter(|entry| entry.participant != participant)
                    .cloned()
                    .collect(),
                inserted: false,
            };
        }
        let serial = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let subscription = ChatSubscription {
            channel_id: channel_id.to_owned(),
            id: format!("sub_{serial:016x}"),
            presence_id: format!("pr_{serial:016x}"),
            participant,
            user_id: user_id.to_owned(),
            target,
            authority_epoch,
            needs_resync: false,
        };
        let existing = channel.values().cloned().collect();
        channel.insert(participant, subscription.clone());
        state
            .participants
            .entry(participant)
            .or_default()
            .insert(channel_id.to_owned());
        ChatJoin {
            subscription,
            existing,
            inserted: true,
        }
    }

    /// Snapshot one participant's subscription, if it owns this channel.
    #[must_use]
    pub fn subscription(
        &self,
        channel_id: &str,
        participant: ParticipantId,
    ) -> Option<ChatSubscription> {
        let state = self.state.lock().ok()?;
        state
            .channels
            .get(channel_id)
            .and_then(|channel| channel.get(&participant))
            .cloned()
    }

    /// Snapshot every local subscription in a channel.
    #[must_use]
    pub fn subscribers(&self, channel_id: &str) -> Vec<ChatSubscription> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        state
            .channels
            .get(channel_id)
            .map(|channel| channel.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Snapshot only subscriptions fenced by the authority epoch captured for a
    /// durable event. Cross-node delivery uses this narrow view after validating
    /// its channel lease, so an event never reaches a subscription that predates
    /// a local authority change.
    pub fn subscribers_at_authority_epoch(
        &self,
        channel_id: &str,
        authority_epoch: u64,
    ) -> Result<Vec<ChatSubscription>, ChatPresenceUnavailable> {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let state = poisoned.into_inner();
                self.state.clear_poison();
                drop(state);
                return Err(ChatPresenceUnavailable);
            }
        };
        Ok(state
            .channels
            .get(channel_id)
            .map(|channel| {
                channel
                    .values()
                    .filter(|subscription| subscription.authority_epoch == authority_epoch)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Snapshot every local subscription. Callers use this only to reconcile a
    /// just-completed authority mutation (block, kick, or room departure), not
    /// as a broadcast routing table.
    #[must_use]
    pub fn all_subscriptions(&self) -> Vec<ChatSubscription> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        state
            .channels
            .values()
            .flat_map(|channel| channel.values().cloned())
            .collect()
    }

    /// Mark one subscription for explicit reconciliation after its queue drops.
    pub fn mark_needs_resync(&self, channel_id: &str, participant: ParticipantId) {
        if let Ok(mut state) = self.state.lock()
            && let Some(subscription) = state
                .channels
                .get_mut(channel_id)
                .and_then(|channel| channel.get_mut(&participant))
        {
            subscription.needs_resync = true;
        }
    }

    /// Clear a resync marker only after the client acknowledges a durable
    /// history watermark. The caller validates the watermark relation.
    pub fn clear_needs_resync(&self, channel_id: &str, participant: ParticipantId) {
        if let Ok(mut state) = self.state.lock()
            && let Some(subscription) = state
                .channels
                .get_mut(channel_id)
                .and_then(|channel| channel.get_mut(&participant))
        {
            subscription.needs_resync = false;
        }
    }

    /// Remove one explicit subscription, returning the remaining recipients.
    pub fn leave(&self, channel_id: &str, participant: ParticipantId) -> Option<ChatLeave> {
        let mut state = self.state.lock().ok()?;
        let channel = state.channels.get_mut(channel_id)?;
        let subscription = channel.remove(&participant)?;
        let remaining = channel.values().cloned().collect();
        let empty = channel.is_empty();
        if empty {
            state.channels.remove(channel_id);
        }
        if let Some(channels) = state.participants.get_mut(&participant) {
            channels.remove(channel_id);
            if channels.is_empty() {
                state.participants.remove(&participant);
            }
        }
        Some(ChatLeave {
            channel_id: channel_id.to_owned(),
            subscription,
            remaining,
        })
    }

    /// Remove every subscription owned by a disconnecting participant.
    pub fn remove_participant(&self, participant: ParticipantId) -> Vec<ChatLeave> {
        let channels = {
            let Ok(state) = self.state.lock() else {
                return Vec::new();
            };
            state
                .participants
                .get(&participant)
                .map(|channels| channels.iter().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
        };
        channels
            .into_iter()
            .filter_map(|channel_id| self.leave(&channel_id, participant))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_is_idempotent_and_leave_cleans_both_indexes() {
        let registry = ChatPresenceRegistry::new();
        let first = registry.join(
            "ch_a",
            ParticipantId::from_raw(1),
            "alice",
            ChatTarget::Direct {
                other_user_id: "bob".to_owned(),
            },
            3,
        );
        let repeated = registry.join(
            "ch_a",
            ParticipantId::from_raw(1),
            "alice",
            ChatTarget::Direct {
                other_user_id: "bob".to_owned(),
            },
            3,
        );
        assert!(first.inserted);
        assert!(!repeated.inserted);
        assert_eq!(first.subscription.id, repeated.subscription.id);
        let leave = registry
            .leave("ch_a", ParticipantId::from_raw(1))
            .expect("left");
        assert_eq!(leave.subscription.user_id, "alice");
        assert!(leave.remaining.is_empty());
        assert!(registry.subscribers("ch_a").is_empty());
    }

    #[test]
    fn disconnect_removes_every_channel_subscription() {
        let registry = ChatPresenceRegistry::new();
        let participant = ParticipantId::from_raw(7);
        for channel in ["ch_a", "ch_b"] {
            registry.join(
                channel,
                participant,
                "alice",
                ChatTarget::CurrentRoom { room_id: 9 },
                0,
            );
        }
        assert_eq!(registry.remove_participant(participant).len(), 2);
        assert!(registry.subscribers("ch_a").is_empty());
        assert!(registry.subscribers("ch_b").is_empty());
    }

    #[test]
    fn authority_fenced_snapshot_excludes_stale_subscriptions() {
        let registry = ChatPresenceRegistry::new();
        registry.join(
            "ch_a",
            ParticipantId::from_raw(1),
            "alice",
            ChatTarget::CurrentRoom { room_id: 9 },
            4,
        );
        registry.join(
            "ch_a",
            ParticipantId::from_raw(2),
            "bob",
            ChatTarget::CurrentRoom { room_id: 9 },
            5,
        );

        let current = registry
            .subscribers_at_authority_epoch("ch_a", 5)
            .expect("healthy authority registry");
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].user_id, "bob");
        assert!(
            registry
                .subscribers_at_authority_epoch("ch_a", 6)
                .expect("healthy authority registry")
                .is_empty()
        );
    }
}
