//! Authenticated, in-memory parties used by local matchmaker tickets.
//!
//! Parties deliberately store account ids rather than transport participant ids:
//! a formed match handoff is authorized by account, while the gateway resolves
//! each currently connected member only when the party enters the queue.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Opaque party handle used by realtime RPC callers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartyId(String);

impl PartyId {
    /// Validate a party handle received from an untrusted client.
    pub fn parse(value: impl Into<String>) -> Result<Self, PartyError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(PartyError::Validation("invalid party_id".to_owned()));
        }
        Ok(Self(value))
    }

    /// Return the opaque wire representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Mint a new opaque party handle for durable authority creation.
    pub fn generate() -> Result<Self, PartyError> {
        fresh_party_id()
    }
}

/// Read-only party state exposed through RPC responses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartySnapshot {
    /// Opaque party identifier.
    pub party_id: PartyId,
    /// Account that alone may invite, promote, remove, close, and queue it.
    pub leader_user_id: String,
    /// Deterministically ordered member account ids, including the leader.
    pub members: Vec<String>,
    /// Accounts with an unaccepted invitation.
    pub invitations: Vec<String>,
    /// Monotonically increasing authoritative revision. Local parties use zero;
    /// durable party owners fence mutations and queue snapshots with this value.
    #[serde(default)]
    pub revision: u64,
}

#[derive(Debug, Clone)]
struct Party {
    leader_user_id: String,
    members: BTreeSet<String>,
    invitations: BTreeSet<String>,
}

#[derive(Debug, Default)]
struct Inner {
    parties: HashMap<PartyId, Party>,
    membership: HashMap<String, PartyId>,
}

/// Failure at the party authorization or validation boundary.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PartyError {
    /// Input was malformed or violates a cardinality rule.
    #[error("{0}")]
    Validation(String),
    /// No party has the supplied opaque id.
    #[error("party not found")]
    NotFound,
    /// The caller is not a member of the party.
    #[error("party membership required")]
    NotMember,
    /// The caller is a member but not its leader.
    #[error("party leader required")]
    NotLeader,
    /// A user can belong to only one active party.
    #[error("user already belongs to a party")]
    AlreadyInParty,
    /// The target has no invitation to accept.
    #[error("party invitation not found")]
    InvitationNotFound,
    /// The party reached its configured maximum size.
    #[error("party is full")]
    Full,
    /// The OS CSPRNG was unavailable while minting an id.
    #[error("could not generate party id")]
    Entropy,
}

/// Local party membership and invitation registry.
///
/// It is intentionally process-local. The matchmaker's cluster routing task
/// replaces this with a fenced owner directory before parties span nodes.
#[derive(Debug, Default)]
pub struct PartyRegistry {
    inner: Mutex<Inner>,
}

impl PartyRegistry {
    /// Maximum members per party, including its leader.
    pub const MAX_MEMBERS: usize = 8;

    /// Construct an empty local registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a one-member party owned by `leader_user_id`.
    pub fn create(&self, leader_user_id: &str) -> Result<PartySnapshot, PartyError> {
        let mut inner = self.lock();
        if inner.membership.contains_key(leader_user_id) {
            return Err(PartyError::AlreadyInParty);
        }
        let id = fresh_party_id()?;
        let mut members = BTreeSet::new();
        members.insert(leader_user_id.to_owned());
        inner
            .membership
            .insert(leader_user_id.to_owned(), id.clone());
        let party = Party {
            leader_user_id: leader_user_id.to_owned(),
            members,
            invitations: BTreeSet::new(),
        };
        inner.parties.insert(id.clone(), party);
        Self::snapshot_locked(&inner, &id).ok_or(PartyError::NotFound)
    }

    /// Invite a non-member. Only the leader may issue invitations.
    pub fn invite(
        &self,
        leader_user_id: &str,
        id: &PartyId,
        target_user_id: &str,
    ) -> Result<PartySnapshot, PartyError> {
        if target_user_id.is_empty() {
            return Err(PartyError::Validation(
                "target_user_id is required".to_owned(),
            ));
        }
        let mut inner = self.lock();
        if inner.membership.contains_key(target_user_id) {
            return Err(PartyError::AlreadyInParty);
        }
        let party = inner.parties.get_mut(id).ok_or(PartyError::NotFound)?;
        if party.leader_user_id != leader_user_id {
            return Err(PartyError::NotLeader);
        }
        if party.members.len() + party.invitations.len() >= Self::MAX_MEMBERS {
            return Err(PartyError::Full);
        }
        party.invitations.insert(target_user_id.to_owned());
        Self::snapshot_locked(&inner, id).ok_or(PartyError::NotFound)
    }

    /// Accept the caller's outstanding invitation.
    pub fn accept(&self, user_id: &str, id: &PartyId) -> Result<PartySnapshot, PartyError> {
        let mut inner = self.lock();
        if inner.membership.contains_key(user_id) {
            return Err(PartyError::AlreadyInParty);
        }
        let party = inner.parties.get_mut(id).ok_or(PartyError::NotFound)?;
        if !party.invitations.remove(user_id) {
            return Err(PartyError::InvitationNotFound);
        }
        if party.members.len() >= Self::MAX_MEMBERS {
            return Err(PartyError::Full);
        }
        party.members.insert(user_id.to_owned());
        inner.membership.insert(user_id.to_owned(), id.clone());
        Self::snapshot_locked(&inner, id).ok_or(PartyError::NotFound)
    }

    /// Leave a party. The leader closing it makes ownership transfer explicit.
    pub fn leave(&self, user_id: &str, id: &PartyId) -> Result<(), PartyError> {
        let mut inner = self.lock();
        let party = inner.parties.get(id).ok_or(PartyError::NotFound)?;
        if !party.members.contains(user_id) {
            return Err(PartyError::NotMember);
        }
        if party.leader_user_id == user_id {
            return Self::close_locked(&mut inner, user_id, id);
        }
        let party = inner.parties.get_mut(id).ok_or(PartyError::NotFound)?;
        party.members.remove(user_id);
        inner.membership.remove(user_id);
        Ok(())
    }

    /// Transfer leadership to an existing member.
    pub fn promote(
        &self,
        leader_user_id: &str,
        id: &PartyId,
        target_user_id: &str,
    ) -> Result<PartySnapshot, PartyError> {
        let mut inner = self.lock();
        let party = inner.parties.get_mut(id).ok_or(PartyError::NotFound)?;
        if party.leader_user_id != leader_user_id {
            return Err(PartyError::NotLeader);
        }
        if !party.members.contains(target_user_id) {
            return Err(PartyError::NotMember);
        }
        party.leader_user_id = target_user_id.to_owned();
        Self::snapshot_locked(&inner, id).ok_or(PartyError::NotFound)
    }

    /// Remove a non-leader member. Only the leader can do this.
    pub fn remove(
        &self,
        leader_user_id: &str,
        id: &PartyId,
        target_user_id: &str,
    ) -> Result<PartySnapshot, PartyError> {
        let mut inner = self.lock();
        let party = inner.parties.get_mut(id).ok_or(PartyError::NotFound)?;
        if party.leader_user_id != leader_user_id {
            return Err(PartyError::NotLeader);
        }
        if target_user_id == leader_user_id || !party.members.remove(target_user_id) {
            return Err(PartyError::NotMember);
        }
        party.invitations.remove(target_user_id);
        inner.membership.remove(target_user_id);
        Self::snapshot_locked(&inner, id).ok_or(PartyError::NotFound)
    }

    /// Close a party and all outstanding invitations. Only its leader can close it.
    pub fn close(&self, leader_user_id: &str, id: &PartyId) -> Result<(), PartyError> {
        let mut inner = self.lock();
        Self::close_locked(&mut inner, leader_user_id, id)
    }

    /// Return a snapshot only to a member or a user with an invitation.
    pub fn snapshot_for(&self, user_id: &str, id: &PartyId) -> Result<PartySnapshot, PartyError> {
        let inner = self.lock();
        let party = inner.parties.get(id).ok_or(PartyError::NotFound)?;
        if !party.members.contains(user_id) && !party.invitations.contains(user_id) {
            return Err(PartyError::NotMember);
        }
        Self::snapshot_locked(&inner, id).ok_or(PartyError::NotFound)
    }

    /// Resolve queueable members. This deliberately requires the caller to be
    /// the leader so one party cannot submit competing tickets.
    pub fn queue_members(
        &self,
        leader_user_id: &str,
        id: &PartyId,
    ) -> Result<Vec<String>, PartyError> {
        let inner = self.lock();
        let party = inner.parties.get(id).ok_or(PartyError::NotFound)?;
        if party.leader_user_id != leader_user_id {
            return Err(PartyError::NotLeader);
        }
        Ok(party.members.iter().cloned().collect())
    }

    fn close_locked(
        inner: &mut Inner,
        leader_user_id: &str,
        id: &PartyId,
    ) -> Result<(), PartyError> {
        let party = inner.parties.get(id).ok_or(PartyError::NotFound)?;
        if party.leader_user_id != leader_user_id {
            return Err(PartyError::NotLeader);
        }
        let party = inner.parties.remove(id).ok_or(PartyError::NotFound)?;
        for member in party.members {
            inner.membership.remove(&member);
        }
        Ok(())
    }

    fn snapshot_locked(inner: &Inner, id: &PartyId) -> Option<PartySnapshot> {
        let party = inner.parties.get(id)?;
        Some(PartySnapshot {
            party_id: id.clone(),
            leader_user_id: party.leader_user_id.clone(),
            members: party.members.iter().cloned().collect(),
            invitations: party.invitations.iter().cloned().collect(),
            revision: 0,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn fresh_party_id() -> Result<PartyId, PartyError> {
    let mut bytes = [0_u8; 18];
    getrandom::fill(&mut bytes).map_err(|_| PartyError::Entropy)?;
    Ok(PartyId(
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invitation_acceptance_and_leader_queue_authorization_are_owner_bound() {
        let parties = PartyRegistry::new();
        let party = parties.create("alice").expect("party");
        assert_eq!(party.members, ["alice"]);
        parties
            .invite("alice", &party.party_id, "bob")
            .expect("invite");
        assert_eq!(
            parties
                .accept("bob", &party.party_id)
                .expect("accept")
                .members,
            ["alice", "bob"]
        );
        assert_eq!(
            parties.queue_members("bob", &party.party_id),
            Err(PartyError::NotLeader)
        );
        assert_eq!(
            parties
                .queue_members("alice", &party.party_id)
                .expect("members"),
            ["alice", "bob"]
        );
    }

    #[test]
    fn promote_remove_and_close_keep_membership_consistent() {
        let parties = PartyRegistry::new();
        let party = parties.create("alice").expect("party");
        parties
            .invite("alice", &party.party_id, "bob")
            .expect("invite");
        parties.accept("bob", &party.party_id).expect("accept");
        parties
            .promote("alice", &party.party_id, "bob")
            .expect("promote");
        assert_eq!(
            parties.remove("alice", &party.party_id, "bob"),
            Err(PartyError::NotLeader)
        );
        parties
            .remove("bob", &party.party_id, "alice")
            .expect("remove");
        parties.close("bob", &party.party_id).expect("close");
        assert_eq!(
            parties.create("alice").expect("new party").members,
            ["alice"]
        );
    }
}
