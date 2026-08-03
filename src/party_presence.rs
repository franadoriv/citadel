//! Privacy-safe, node-level party presence propagation.
//!
//! The cluster directory deliberately contains only `(party_id, node_id)` lease
//! metadata.  Account/session state stays on the session-owning node; it is
//! materialized only after the local gateway has already authorized a member.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::session::{NodeId, OwnershipGeneration};
use crate::time::TimestampMillis;

/// One fenced `(party, node)` advertisement.  It intentionally has no member,
/// participant, socket, or invitation field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyPresenceLease {
    pub party_id: String,
    pub node_id: NodeId,
    pub generation: OwnershipGeneration,
    pub expires_at: TimestampMillis,
    /// The owner revision observed while publishing.  It is a bounded
    /// watermark, not a member-level transition log.
    pub party_revision: u64,
}

impl PartyPresenceLease {
    #[must_use]
    pub fn is_current_at(&self, now: TimestampMillis) -> bool {
        self.expires_at > now
    }
}

/// Fenced removal of a node's final local party member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyPresenceWithdrawal {
    pub party_id: String,
    pub node_id: NodeId,
    pub generation: OwnershipGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartyPresenceUpdate {
    Applied,
    Stale,
}

/// The only party-presence payload admitted to the node control plane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartyPresenceCommand {
    Advertise(PartyPresenceLease),
    Withdraw(PartyPresenceWithdrawal),
}

/// Shared lease view used to select one command per destination node.
#[derive(Debug, Default)]
pub struct PartyPresenceDirectory {
    leases: Mutex<BTreeMap<String, BTreeMap<NodeId, PartyPresenceLease>>>,
    tombstones: Mutex<BTreeMap<(String, NodeId), OwnershipGeneration>>,
}

impl PartyPresenceDirectory {
    /// Apply an advertisement.  A tombstone wins equal/older delayed renewals,
    /// so a removed node cannot be resurrected by reordered control traffic.
    pub fn advertise(
        &self,
        lease: PartyPresenceLease,
        now: TimestampMillis,
    ) -> PartyPresenceUpdate {
        if lease.party_id.is_empty() || !lease.is_current_at(now) {
            return PartyPresenceUpdate::Stale;
        }
        let key = (lease.party_id.clone(), lease.node_id.clone());
        let Ok(tombstones) = self.tombstones.lock() else {
            return PartyPresenceUpdate::Stale;
        };
        let Ok(mut leases) = self.leases.lock() else {
            return PartyPresenceUpdate::Stale;
        };
        if tombstones
            .get(&key)
            .is_some_and(|fence| lease.generation <= *fence)
        {
            return PartyPresenceUpdate::Stale;
        }
        if let Some(current) = leases
            .get(&lease.party_id)
            .and_then(|nodes| nodes.get(&lease.node_id))
            && (lease.generation < current.generation
                || (lease.generation == current.generation
                    && lease.expires_at <= current.expires_at))
        {
            return PartyPresenceUpdate::Stale;
        }
        leases
            .entry(lease.party_id.clone())
            .or_default()
            .insert(lease.node_id.clone(), lease);
        PartyPresenceUpdate::Applied
    }

    /// Withdraw only the exact currently advertised fence and retain its
    /// generation as a tombstone.
    pub fn withdraw(&self, withdrawal: PartyPresenceWithdrawal) -> PartyPresenceUpdate {
        let key = (withdrawal.party_id.clone(), withdrawal.node_id.clone());
        let Ok(mut tombstones) = self.tombstones.lock() else {
            return PartyPresenceUpdate::Stale;
        };
        let Ok(mut leases) = self.leases.lock() else {
            return PartyPresenceUpdate::Stale;
        };
        let current = leases
            .get(&withdrawal.party_id)
            .and_then(|nodes| nodes.get(&withdrawal.node_id));
        if current.is_none_or(|lease| lease.generation != withdrawal.generation) {
            return PartyPresenceUpdate::Stale;
        }
        let empty = leases.get_mut(&withdrawal.party_id).is_some_and(|nodes| {
            nodes.remove(&withdrawal.node_id);
            nodes.is_empty()
        });
        if empty {
            leases.remove(&withdrawal.party_id);
        }
        tombstones.insert(key, withdrawal.generation);
        PartyPresenceUpdate::Applied
    }

    /// Return each live destination once.  Lease expiry is fail-closed and
    /// never needs a node-failure notification.
    pub fn destinations(&self, party_id: &str, now: TimestampMillis) -> Vec<PartyPresenceLease> {
        let Ok(mut leases) = self.leases.lock() else {
            return Vec::new();
        };
        let Some(nodes) = leases.get_mut(party_id) else {
            return Vec::new();
        };
        nodes.retain(|_, lease| lease.is_current_at(now));
        let destinations = nodes.values().cloned().collect();
        if nodes.is_empty() {
            leases.remove(party_id);
        }
        destinations
    }

    /// Whether an advertised node still owns one exact, live lease. Both the
    /// source and destination checks on remote deliveries use this same fence.
    #[must_use]
    pub fn matches_destination(
        &self,
        party_id: &str,
        node_id: &NodeId,
        generation: OwnershipGeneration,
        now: TimestampMillis,
    ) -> bool {
        self.destinations(party_id, now)
            .into_iter()
            .any(|lease| lease.node_id == *node_id && lease.generation == generation)
    }
}

/// Member-only state emitted after authorization at the session-owning node.
/// Invitees and nonmembers must never be passed to this type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyPresenceSnapshot {
    pub party_id: String,
    pub party_revision: u64,
    pub sequence: u64,
    pub online_members: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartyPresenceDelivery {
    Snapshot(PartyPresenceSnapshot),
    Delta(PartyPresenceSnapshot),
    ResyncRequired {
        party_id: String,
        party_revision: u64,
        sequence: u64,
    },
}

/// Result of applying one authenticated cross-node member-presence command.
/// This is intentionally distinct from a lease update: the receiver may have
/// a current node lease but no longer have any authorized local members.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartyPresenceDeliveryDisposition {
    Delivered,
    Stale,
    Unauthorized,
    Rejected,
}

/// One source-node snapshot addressed to exactly one currently leased node.
/// The mTLS transport authenticates `origin_node`; `origin_generation` lets
/// the receiver discard a delayed payload after that source withdrew or
/// rejoined. It never carries a participant, socket, or invitation identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemotePartyPresenceDelivery {
    pub party_id: String,
    pub origin_node: NodeId,
    pub origin_generation: OwnershipGeneration,
    pub destination_generation: OwnershipGeneration,
    pub snapshot: PartyPresenceSnapshot,
    pub deadline: TimestampMillis,
}

/// Local member-presence state. Multiple sessions for one account count as one
/// visible member; a saturated recipient is marked for snapshot+resync.
#[derive(Debug, Default)]
pub struct LocalPartyPresence {
    sessions: Mutex<HashMap<(String, String), BTreeSet<u64>>>,
    sequences: Mutex<HashMap<String, u64>>,
    receivers_needing_resync: Mutex<BTreeSet<(String, String)>>,
}

impl LocalPartyPresence {
    pub fn connect(&self, party_id: &str, member: &str, session: u64) -> bool {
        let Ok(mut sessions) = self.sessions.lock() else {
            return false;
        };
        let entry = sessions
            .entry((party_id.to_owned(), member.to_owned()))
            .or_default();
        let was_offline = entry.is_empty();
        entry.insert(session);
        was_offline
    }

    pub fn disconnect(&self, party_id: &str, member: &str, session: u64) -> bool {
        let Ok(mut sessions) = self.sessions.lock() else {
            return false;
        };
        let key = (party_id.to_owned(), member.to_owned());
        let Some(entry) = sessions.get_mut(&key) else {
            return false;
        };
        entry.remove(&session);
        let offline = entry.is_empty();
        if offline {
            sessions.remove(&key);
        }
        offline
    }

    /// Reconcile a durable member snapshot with the locally registered sockets.
    /// Returns whether the visible local set changed. This is deliberately driven
    /// by the durable aggregate so invitees never enter presence state.
    pub fn reconcile(&self, party_id: &str, members: &[(String, u64)]) -> bool {
        let Ok(mut sessions) = self.sessions.lock() else {
            return false;
        };
        let before: BTreeSet<_> = sessions
            .iter()
            .filter(|((party, _), _)| party == party_id)
            .map(|((_, member), ids)| (member.clone(), ids.clone()))
            .collect();
        sessions.retain(|(party, _), _| party != party_id);
        for (member, session) in members {
            sessions
                .entry((party_id.to_owned(), member.clone()))
                .or_default()
                .insert(*session);
        }
        let after: BTreeSet<_> = sessions
            .iter()
            .filter(|((party, _), _)| party == party_id)
            .map(|((_, member), ids)| (member.clone(), ids.clone()))
            .collect();
        before != after
    }

    /// Remove a disconnected socket from every party it had locally joined.
    /// The returned ids let the gateway withdraw final-local-member leases.
    pub fn disconnect_session(&self, session: u64) -> Vec<String> {
        let Ok(mut sessions) = self.sessions.lock() else {
            return Vec::new();
        };
        let affected: BTreeSet<String> = sessions
            .iter()
            .filter(|(_, ids)| ids.contains(&session))
            .map(|((party, _), _)| party.clone())
            .collect();
        for ids in sessions.values_mut() {
            ids.remove(&session);
        }
        sessions.retain(|_, ids| !ids.is_empty());
        affected.into_iter().collect()
    }

    #[must_use]
    pub fn parties_for_session(&self, session: u64) -> Vec<String> {
        self.sessions
            .lock()
            .map(|sessions| {
                sessions
                    .iter()
                    .filter(|(_, ids)| ids.contains(&session))
                    .map(|((party, _), _)| party.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
    }

    #[must_use]
    pub fn has_online_members(&self, party_id: &str) -> bool {
        self.sessions
            .lock()
            .map(|sessions| sessions.keys().any(|(party, _)| party == party_id))
            .unwrap_or(false)
    }

    #[must_use]
    pub fn online_members(&self, party_id: &str) -> Vec<String> {
        let Ok(sessions) = self.sessions.lock() else {
            return Vec::new();
        };
        let mut members: Vec<_> = sessions
            .keys()
            .filter(|(party, _)| party == party_id)
            .map(|(_, member)| member.clone())
            .collect();
        members.sort();
        members
    }

    /// Construct a member-authorized snapshot and increment its party-local
    /// sequence. Callers must verify `recipient_is_member` from the durable
    /// aggregate before calling this method.
    pub fn snapshot_for_member(
        &self,
        party_id: &str,
        party_revision: u64,
        recipient_is_member: bool,
    ) -> Option<PartyPresenceSnapshot> {
        if !recipient_is_member {
            return None;
        }
        let mut online_members: Vec<String> = self
            .sessions
            .lock()
            .ok()
            .map(|sessions| {
                sessions
                    .keys()
                    .filter(|(party, _)| *party == party_id)
                    .map(|(_, member)| member.clone())
                    .collect()
            })
            .unwrap_or_default();
        online_members.sort();
        let sequence = self
            .sequences
            .lock()
            .ok()
            .map(|mut sequences| {
                let next = sequences.entry(party_id.to_owned()).or_insert(0);
                *next = next.saturating_add(1);
                *next
            })
            .unwrap_or(0);
        Some(PartyPresenceSnapshot {
            party_id: party_id.to_owned(),
            party_revision,
            sequence,
            online_members,
        })
    }

    /// Build a snapshot from already-authorized online member ids. The gateway
    /// obtains both the durable member list and the local session list first;
    /// this helper intentionally performs no authorization itself.
    pub fn snapshot_for_online_members(
        &self,
        party_id: &str,
        party_revision: u64,
        mut online_members: Vec<String>,
    ) -> PartyPresenceSnapshot {
        online_members.sort();
        online_members.dedup();
        let sequence = self
            .sequences
            .lock()
            .ok()
            .map(|mut sequences| {
                let next = sequences.entry(party_id.to_owned()).or_insert(0);
                *next = next.saturating_add(1);
                *next
            })
            .unwrap_or(0);
        PartyPresenceSnapshot {
            party_id: party_id.to_owned(),
            party_revision,
            sequence,
            online_members,
        }
    }

    pub fn mark_queue_drop(&self, party_id: &str, recipient: &str) {
        if let Ok(mut pending) = self.receivers_needing_resync.lock() {
            pending.insert((party_id.to_owned(), recipient.to_owned()));
        }
    }

    /// Convert the next delivery into an explicit resync when its bounded queue
    /// dropped. The caller then sends a fresh authorized snapshot before deltas.
    pub fn delivery_for(
        &self,
        recipient: &str,
        snapshot: PartyPresenceSnapshot,
    ) -> PartyPresenceDelivery {
        let pending = self
            .receivers_needing_resync
            .lock()
            .ok()
            .is_some_and(|mut pending| {
                pending.remove(&(snapshot.party_id.clone(), recipient.to_owned()))
            });
        if pending {
            PartyPresenceDelivery::ResyncRequired {
                party_id: snapshot.party_id,
                party_revision: snapshot.party_revision,
                sequence: snapshot.sequence,
            }
        } else {
            PartyPresenceDelivery::Delta(snapshot)
        }
    }
}

/// Client-side monotonic acceptance guard. Snapshot and delta transitions are
/// ordered by `(party revision, presence sequence)` and duplicates are ignored.
#[derive(Debug, Default)]
pub struct PartyPresenceCursor(Mutex<HashMap<String, (u64, u64)>>);

impl PartyPresenceCursor {
    #[must_use]
    pub fn accept(&self, update: &PartyPresenceSnapshot) -> bool {
        let Ok(mut cursors) = self.0.lock() else {
            return false;
        };
        let current = cursors.entry(update.party_id.clone()).or_insert((0, 0));
        let incoming = (update.party_revision, update.sequence);
        if incoming <= *current {
            return false;
        }
        *current = incoming;
        true
    }
}

/// A deliberately tiny bounded fan-out queue. Overflow loses a delta but not
/// correctness because `LocalPartyPresence::mark_queue_drop` forces resync.
#[derive(Debug)]
pub struct BoundedPartyPresenceQueue {
    capacity: usize,
    pending: VecDeque<PartyPresenceDelivery>,
}

impl BoundedPartyPresenceQueue {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            pending: VecDeque::new(),
        }
    }
    pub fn push(&mut self, item: PartyPresenceDelivery) -> bool {
        if self.pending.len() == self.capacity {
            return false;
        }
        self.pending.push_back(item);
        true
    }
    pub fn pop(&mut self) -> Option<PartyPresenceDelivery> {
        self.pending.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(value: &str) -> NodeId {
        NodeId::new(value).expect("test node id is valid")
    }
    fn lease(party: &str, node_id: &str, generation: u64, expiry: u64) -> PartyPresenceLease {
        PartyPresenceLease {
            party_id: party.to_owned(),
            node_id: node(node_id),
            generation: OwnershipGeneration::new(generation),
            expires_at: TimestampMillis::from_unix_millis(expiry),
            party_revision: 4,
        }
    }

    #[test]
    fn lease_withdrawal_tombstone_and_expiry_are_fenced() {
        let directory = PartyPresenceDirectory::default();
        let now = TimestampMillis::from_unix_millis(100);
        assert_eq!(
            directory.advertise(lease("p", "a", 1, 200), now),
            PartyPresenceUpdate::Applied
        );
        assert_eq!(
            directory.withdraw(PartyPresenceWithdrawal {
                party_id: "p".into(),
                node_id: node("a"),
                generation: OwnershipGeneration::new(1)
            }),
            PartyPresenceUpdate::Applied
        );
        assert_eq!(
            directory.advertise(lease("p", "a", 1, 300), now),
            PartyPresenceUpdate::Stale
        );
        assert_eq!(
            directory.advertise(lease("p", "a", 2, 150), now),
            PartyPresenceUpdate::Applied
        );
        assert!(
            directory
                .destinations("p", TimestampMillis::from_unix_millis(150))
                .is_empty()
        );
    }

    #[test]
    fn duplicate_devices_deduplicate_and_nonmembers_receive_nothing() {
        let presence = LocalPartyPresence::default();
        assert!(presence.connect("p", "alice", 1));
        assert!(!presence.connect("p", "alice", 2));
        assert!(presence.connect("p", "bob", 3));
        assert_eq!(presence.snapshot_for_member("p", 7, false), None);
        let snapshot = presence
            .snapshot_for_member("p", 7, true)
            .expect("members receive a snapshot");
        assert_eq!(snapshot.online_members, vec!["alice", "bob"]);
        assert!(!presence.disconnect("p", "alice", 1));
        assert!(presence.disconnect("p", "alice", 2));
    }

    #[test]
    fn queue_drop_forces_resync_and_client_suppresses_reordering() {
        let presence = LocalPartyPresence::default();
        presence.mark_queue_drop("p", "alice");
        let snapshot = presence
            .snapshot_for_member("p", 2, true)
            .expect("members receive a snapshot");
        assert!(matches!(
            presence.delivery_for("alice", snapshot.clone()),
            PartyPresenceDelivery::ResyncRequired { .. }
        ));
        let cursor = PartyPresenceCursor::default();
        assert!(cursor.accept(&snapshot));
        assert!(!cursor.accept(&snapshot));
        let mut queue = BoundedPartyPresenceQueue::new(1);
        assert!(queue.push(PartyPresenceDelivery::Delta(snapshot.clone())));
        assert!(!queue.push(PartyPresenceDelivery::Delta(snapshot)));
    }
}
