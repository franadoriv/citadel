//! Session ownership and routing types.
//!
//! Ownership is explicit, per `docs/architecture/node-ownership-and-routing.md`.
//! A session is owned by exactly one node under a monotonically increasing
//! [`OwnershipGeneration`] lease. Resolving a [`SessionId`] returns a
//! [`SessionOwnership`] that distinguishes local delivery, remote forwarding, an
//! unknown session, and a stale lease, rather than hiding a node hash inside the
//! id.

use serde::{Deserialize, Serialize};

use crate::time::TimestampMillis;

use super::id::{NodeId, SessionId};

/// A monotonically increasing lease generation for session ownership.
///
/// A higher generation always wins; a lower one presented for the same session
/// is stale (the owner moved or the lease was renewed elsewhere).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OwnershipGeneration(u64);

impl OwnershipGeneration {
    /// Construct a generation value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw generation number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The lease that ties a session to its owning node for a bounded time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOwnerLease {
    /// The node that currently owns the session.
    pub node_id: NodeId,
    /// The lease generation.
    pub generation: OwnershipGeneration,
    /// When the lease expires (Unix millis); after this, ownership is stale.
    pub expires_at: TimestampMillis,
}

impl SessionOwnerLease {
    /// Whether the lease is still valid at `now`.
    #[must_use]
    pub fn is_current_at(&self, now: TimestampMillis) -> bool {
        now < self.expires_at
    }
}

/// A request to resolve which node owns a session.
///
/// `expected`, when present, lets the caller detect a stale view: if the
/// directory holds a newer generation than `expected`, the result is
/// [`SessionOwnership::Stale`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveSessionOwnerRequest {
    /// The session to resolve.
    pub session_id: SessionId,
    /// The lease the caller believes is current, if any.
    pub expected: Option<SessionOwnerLease>,
    /// The current time, used to expire stale leases.
    pub now: TimestampMillis,
}

/// The outcome of resolving session ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionOwnership {
    /// Owned by this node: deliver through the local session registry.
    Local,
    /// Owned by another node: forward through the inter-node router.
    Remote(NodeId),
    /// No live ownership found (missing or expired lease).
    Unknown,
    /// Ownership found but the lease/generation no longer matches expectations.
    Stale,
}

/// A directory entry mapping a session to its current owner lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionDirectoryEntry {
    /// The session.
    pub session_id: SessionId,
    /// Its current owner lease.
    pub owner: SessionOwnerLease,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str) -> NodeId {
        NodeId::new(id).expect("valid node id")
    }

    #[test]
    fn generation_round_trips() {
        assert_eq!(OwnershipGeneration::new(7).get(), 7);
        assert!(OwnershipGeneration::new(2) > OwnershipGeneration::new(1));
    }

    #[test]
    fn lease_currency_follows_expiry() {
        let lease = SessionOwnerLease {
            node_id: node("node-a"),
            generation: OwnershipGeneration::new(1),
            expires_at: TimestampMillis::from_unix_millis(100),
        };
        assert!(lease.is_current_at(TimestampMillis::from_unix_millis(99)));
        assert!(!lease.is_current_at(TimestampMillis::from_unix_millis(100)));
        assert!(!lease.is_current_at(TimestampMillis::from_unix_millis(101)));
    }

    #[test]
    fn ownership_variants_are_distinct() {
        assert_ne!(SessionOwnership::Local, SessionOwnership::Unknown);
        assert_ne!(
            SessionOwnership::Remote(node("node-b")),
            SessionOwnership::Stale
        );
    }
}
