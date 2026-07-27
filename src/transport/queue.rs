//! Per-connection outbound queue with an explicit overflow policy.
//!
//! Every transport connection owns a bounded outbound queue. When the queue is
//! full the behavior is governed by an [`OverflowPolicy`], mirroring the proven
//! pattern from Nakama's per-session outbound channel (`server/session_ws.go`:
//! close-on-full) while making the unreliable-transport case explicit:
//!
//! - [`OverflowPolicy::DropOldest`]: drop the oldest queued item to make room.
//!   Suitable for unreliable, latest-wins traffic (QUIC datagrams).
//! - [`OverflowPolicy::CloseOnFull`]: reject the enqueue and signal that the
//!   connection should be closed. Suitable for reliable, ordered control
//!   traffic where silent loss is unacceptable.
//!
//! This module is transport-agnostic and synchronous so it can be unit tested
//! without a runtime. Concrete transports drain it from their write task.

use std::collections::VecDeque;

/// Policy applied when an outbound enqueue would exceed the queue capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Drop the oldest queued item to admit the new one (unreliable traffic).
    DropOldest,
    /// Reject the new item and signal the connection should close (reliable
    /// traffic).
    CloseOnFull,
}

impl OverflowPolicy {
    /// Stable lowercase token for metrics labels and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DropOldest => "drop_oldest",
            Self::CloseOnFull => "close_on_full",
        }
    }
}

/// Outcome of an [`OutboundQueue::push`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushOutcome<T> {
    /// Item enqueued without exceeding capacity.
    Enqueued,
    /// Item enqueued after dropping the oldest item, which is returned.
    DroppedOldest(T),
    /// Item rejected because the queue was full under
    /// [`OverflowPolicy::CloseOnFull`]; the connection should be closed. The
    /// rejected item is returned so callers can inspect or drop it.
    Rejected(T),
}

/// A bounded outbound queue with a fixed [`OverflowPolicy`].
#[derive(Debug)]
pub struct OutboundQueue<T> {
    items: VecDeque<T>,
    capacity: usize,
    policy: OverflowPolicy,
}

impl<T> OutboundQueue<T> {
    /// Create a queue with the given capacity (clamped to at least 1) and
    /// overflow policy.
    #[must_use]
    pub fn new(capacity: usize, policy: OverflowPolicy) -> Self {
        let capacity = capacity.max(1);
        Self {
            items: VecDeque::with_capacity(capacity),
            capacity,
            policy,
        }
    }

    /// Configured capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Configured overflow policy.
    #[must_use]
    pub fn policy(&self) -> OverflowPolicy {
        self.policy
    }

    /// Current number of queued items (queue depth).
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Whether the queue is at capacity.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.items.len() >= self.capacity
    }

    /// Enqueue `item`, applying the overflow policy when full.
    pub fn push(&mut self, item: T) -> PushOutcome<T> {
        if !self.is_full() {
            self.items.push_back(item);
            return PushOutcome::Enqueued;
        }
        match self.policy {
            OverflowPolicy::DropOldest => {
                let dropped = self
                    .items
                    .pop_front()
                    .expect("queue is full so it has at least one item");
                self.items.push_back(item);
                PushOutcome::DroppedOldest(dropped)
            }
            OverflowPolicy::CloseOnFull => PushOutcome::Rejected(item),
        }
    }

    /// Dequeue the next item to send, if any.
    pub fn pop(&mut self) -> Option<T> {
        self.items.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_below_capacity_enqueues_in_order() {
        let mut q = OutboundQueue::new(3, OverflowPolicy::CloseOnFull);
        assert_eq!(q.push(1), PushOutcome::Enqueued);
        assert_eq!(q.push(2), PushOutcome::Enqueued);
        assert_eq!(q.len(), 2);
        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn drop_oldest_makes_room_and_returns_dropped() {
        let mut q = OutboundQueue::new(2, OverflowPolicy::DropOldest);
        q.push(10);
        q.push(20);
        assert!(q.is_full());
        let outcome = q.push(30);
        assert_eq!(outcome, PushOutcome::DroppedOldest(10));
        assert_eq!(q.len(), 2);
        // Oldest (10) was dropped; 20 and 30 remain in order.
        assert_eq!(q.pop(), Some(20));
        assert_eq!(q.pop(), Some(30));
    }

    #[test]
    fn close_on_full_rejects_and_returns_item() {
        let mut q = OutboundQueue::new(1, OverflowPolicy::CloseOnFull);
        assert_eq!(q.push("a"), PushOutcome::Enqueued);
        assert_eq!(q.push("b"), PushOutcome::Rejected("b"));
        // The original item is retained; the rejected one is not enqueued.
        assert_eq!(q.len(), 1);
        assert_eq!(q.pop(), Some("a"));
    }

    #[test]
    fn capacity_is_clamped_to_at_least_one() {
        let mut q = OutboundQueue::new(0, OverflowPolicy::DropOldest);
        assert_eq!(q.capacity(), 1);
        assert_eq!(q.push(1), PushOutcome::Enqueued);
        assert_eq!(q.push(2), PushOutcome::DroppedOldest(1));
    }

    #[test]
    fn policy_tokens_are_stable() {
        assert_eq!(OverflowPolicy::DropOldest.as_str(), "drop_oldest");
        assert_eq!(OverflowPolicy::CloseOnFull.as_str(), "close_on_full");
    }

    #[test]
    fn empty_and_full_predicates() {
        let mut q = OutboundQueue::new(2, OverflowPolicy::DropOldest);
        assert!(q.is_empty());
        q.push(1);
        assert!(!q.is_empty());
        assert!(!q.is_full());
        q.push(2);
        assert!(q.is_full());
    }
}
