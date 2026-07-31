//! Typed runtime-cluster propagation contracts.
//!
//! This layer intentionally makes weaker guarantees explicit: runtime events
//! are best-effort one-shot attempts and consumers deduplicate their IDs; cache
//! mutations are last-writer-wins by a fenced version and expire on every node.
//! It is transport-agnostic so the authenticated mTLS control plane can carry
//! it without exposing networking to scripts.

use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::runtime::RuntimeEvent;
use crate::session::{NodeId, OwnershipGeneration};
use crate::time::TimestampMillis;

/// Stable, source-scoped identity for a best-effort runtime event.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RuntimeClusterEventId {
    pub source_node: NodeId,
    pub sequence: u64,
}

/// A remote event. Receivers must treat a duplicate ID as already accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeClusterEvent {
    pub id: RuntimeClusterEventId,
    pub event: RuntimeEvent,
}

/// A version that fences delayed cache propagation from an older owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCacheFence {
    pub owner_node: NodeId,
    pub generation: OwnershipGeneration,
    pub sequence: u64,
}

impl Ord for RuntimeCacheFence {
    fn cmp(&self, other: &Self) -> Ordering {
        self.generation
            .cmp(&other.generation)
            .then_with(|| self.sequence.cmp(&other.sequence))
            .then_with(|| self.owner_node.cmp(&other.owner_node))
    }
}

impl PartialOrd for RuntimeCacheFence {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// One cache replication record. `value = None` is an invalidation/tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCacheMutation {
    pub namespace: String,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub expires_at: TimestampMillis,
    pub fence: RuntimeCacheFence,
}

/// Unfenced cache write submitted to the current global cache writer. The
/// owner assigns its durable fence before it is applied and fanned out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCacheWrite {
    pub namespace: String,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub expires_at: TimestampMillis,
}

/// Bounded duplicate filter for inbound event IDs. Eviction permits a very old
/// duplicate again; it is only a bounded loop/duplicate guard, not delivery
/// durability or an exactly-once guarantee.
#[derive(Debug)]
pub struct RuntimeClusterDedupe {
    seen: BTreeSet<RuntimeClusterEventId>,
    order: VecDeque<RuntimeClusterEventId>,
    capacity: usize,
}

impl RuntimeClusterDedupe {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            seen: BTreeSet::new(),
            order: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// Returns true exactly when this ID was not retained in the bounded filter.
    pub fn accept(&mut self, id: RuntimeClusterEventId) -> bool {
        if !self.seen.insert(id.clone()) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > self.capacity
            && let Some(old) = self.order.pop_front()
        {
            self.seen.remove(&old);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_dedupe_is_explicitly_at_least_once() {
        let node = NodeId::new("node-a").expect("node");
        let first = RuntimeClusterEventId {
            source_node: node.clone(),
            sequence: 1,
        };
        let second = RuntimeClusterEventId {
            source_node: node,
            sequence: 2,
        };
        let mut dedupe = RuntimeClusterDedupe::new(1);
        assert!(dedupe.accept(first.clone()));
        assert!(!dedupe.accept(first.clone()));
        assert!(dedupe.accept(second));
        assert!(
            dedupe.accept(first),
            "old IDs may be delivered again after bounded eviction"
        );
    }
}
