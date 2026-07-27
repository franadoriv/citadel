//! Durable fenced authority for distributed matchmaker queue shards.
//!
//! One system-owned object represents one queue shard. Keeping the lease and
//! every formation/admission claim together lets the portable optimistic-write
//! contract atomically claim a whole party cohort on SQLite, PostgreSQL, and
//! CockroachDB. It deliberately does not persist queue contents: the shard
//! owner owns its working index, while this directory is the cross-node safety
//! boundary that prevents two owners from forming or admitting the same ticket.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::matchmaker::TicketId;
use crate::matchmaker_cluster::{MatchmakerShardLease, QueueShardId};
use crate::repository::StorageRepository;
use crate::session::{NodeId, OwnershipGeneration};
use crate::storage::{
    Accessor, Collection, Key, ObjectId, Owner, Permissions, Precondition, StorageValue,
    WriteRequest,
};
use crate::time::TimestampMillis;

const COLLECTION: &str = "citadel.matchmaker.leases";
const MAX_WRITE_RETRIES: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLease {
    node_id: String,
    generation: u64,
    expires_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredFormation {
    lease: StoredLease,
    #[serde(default)]
    admitted_users: BTreeSet<String>,
    formed_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoredShardState {
    #[serde(default)]
    lease: Option<StoredLease>,
    #[serde(default)]
    max_generation: u64,
    #[serde(default)]
    formations: BTreeMap<String, StoredFormation>,
}

/// Portable durable authority for one matchmaker shard. It is backed solely by
/// the configured [`StorageRepository`], so its compare-and-swap semantics are
/// shared by the in-memory reference, SQLite, PostgreSQL, and CockroachDB
/// implementations.
#[derive(Clone)]
pub struct StorageMatchmakerLeaseDirectory {
    storage: Arc<dyn StorageRepository>,
}

/// Current ownership after an atomic read-or-acquire attempt by one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchmakerShardLeaseResolution {
    /// This node renewed or acquired the durable shard fence.
    Local(MatchmakerShardLease),
    /// Another live node still owns the shard; forward the typed command there.
    Remote(MatchmakerShardLease),
}

impl StorageMatchmakerLeaseDirectory {
    #[must_use]
    pub fn new(storage: Arc<dyn StorageRepository>) -> Self {
        Self { storage }
    }

    /// Read the current unexpired lease for a shard.
    pub async fn read(
        &self,
        shard: QueueShardId,
        now: TimestampMillis,
    ) -> AppResult<Option<MatchmakerShardLease>> {
        let (state, _) = self.read_state(shard).await?;
        state
            .lease
            .map(|stored| decode(shard, stored))
            .transpose()
            .map(|lease| lease.filter(|lease| lease.is_current_at(now)))
    }

    /// Acquire a currently unowned/expired shard or renew the caller's current
    /// owner-generation fence. A transfer must use a generation above the
    /// durable high-water mark; a renewal retains its generation so previously
    /// formed handoffs remain redeemable.
    pub async fn acquire(
        &self,
        lease: MatchmakerShardLease,
        now: TimestampMillis,
    ) -> AppResult<()> {
        if !lease.is_current_at(now) {
            return Err(AppError::validation(
                "matchmaker shard lease must expire after acquisition time",
            ));
        }
        for _ in 0..MAX_WRITE_RETRIES {
            let (mut state, expected) = self.read_state(lease.shard).await?;
            let current = state
                .lease
                .clone()
                .map(|stored| decode(lease.shard, stored))
                .transpose()?;
            match current {
                Some(current) if current.is_current_at(now) => {
                    if !current.has_same_fence_as(&lease) {
                        return Err(AppError::conflict(
                            "matchmaker shard lease is stale or conflicting",
                        ));
                    }
                }
                _ if lease.generation.get() <= state.max_generation => {
                    return Err(AppError::conflict(
                        "matchmaker shard lease generation is not above the durable high-water mark",
                    ));
                }
                _ => {}
            }
            state.max_generation = state.max_generation.max(lease.generation.get());
            state.lease = Some(encode(&lease));
            match self.write_state(lease.shard, state, expected).await {
                Ok(()) => return Ok(()),
                Err(error) if error.category() == ErrorCategory::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        Err(AppError::conflict(
            "matchmaker shard lease changed repeatedly while acquiring it",
        ))
    }

    /// Resolve a live owner or atomically acquire an expired/absent shard for
    /// `node`. Callers never invent a generation from local process state: the
    /// next generation comes from the durable high-water mark in the same CAS
    /// object as the lease and formation claims.
    pub async fn acquire_or_resolve(
        &self,
        shard: QueueShardId,
        node: NodeId,
        expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> AppResult<MatchmakerShardLeaseResolution> {
        if expires_at <= now {
            return Err(AppError::validation(
                "matchmaker shard lease must expire after acquisition time",
            ));
        }
        for _ in 0..MAX_WRITE_RETRIES {
            let (mut state, expected) = self.read_state(shard).await?;
            let current = state
                .lease
                .clone()
                .map(|stored| decode(shard, stored))
                .transpose()?;
            if let Some(current) = current.filter(|lease| lease.is_current_at(now)) {
                if current.owner_node != node {
                    return Ok(MatchmakerShardLeaseResolution::Remote(current));
                }
                let renewed = MatchmakerShardLease {
                    shard,
                    owner_node: node.clone(),
                    generation: current.generation,
                    expires_at,
                };
                state.lease = Some(encode(&renewed));
                match self.write_state(shard, state, expected).await {
                    Ok(()) => return Ok(MatchmakerShardLeaseResolution::Local(renewed)),
                    Err(error) if error.category() == ErrorCategory::Conflict => continue,
                    Err(error) => return Err(error),
                }
            } else {
                let generation = state
                    .max_generation
                    .checked_add(1)
                    .ok_or_else(|| AppError::internal("matchmaker lease generation overflowed"))?;
                let acquired = MatchmakerShardLease {
                    shard,
                    owner_node: node.clone(),
                    generation: OwnershipGeneration::new(generation),
                    expires_at,
                };
                state.max_generation = generation;
                state.lease = Some(encode(&acquired));
                match self.write_state(shard, state, expected).await {
                    Ok(()) => return Ok(MatchmakerShardLeaseResolution::Local(acquired)),
                    Err(error) if error.category() == ErrorCategory::Conflict => continue,
                    Err(error) => return Err(error),
                }
            }
        }
        Err(AppError::conflict(
            "matchmaker shard ownership changed repeatedly while resolving it",
        ))
    }

    /// Atomically claim every ticket in a formed cohort. Storing the whole
    /// shard state in one versioned object makes a party claim all-or-nothing
    /// even on portable storage backends without a multi-key transaction API.
    pub async fn claim_formations(
        &self,
        tickets: &[TicketId],
        lease: &MatchmakerShardLease,
        now: TimestampMillis,
    ) -> AppResult<()> {
        if tickets.is_empty() {
            return Ok(());
        }
        for _ in 0..MAX_WRITE_RETRIES {
            let (mut state, expected) = self.read_state(lease.shard).await?;
            ensure_current(&state, lease, now)?;
            if tickets
                .iter()
                .any(|ticket| state.formations.contains_key(ticket.as_str()))
            {
                return Err(AppError::conflict(
                    "matchmaker ticket was already formed by a shard owner",
                ));
            }
            for ticket in tickets {
                state.formations.insert(
                    ticket.as_str().to_owned(),
                    StoredFormation {
                        lease: encode(lease),
                        admitted_users: BTreeSet::new(),
                        formed_at: now.unix_millis(),
                    },
                );
            }
            match self.write_state(lease.shard, state, expected).await {
                Ok(()) => return Ok(()),
                Err(error) if error.category() == ErrorCategory::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        Err(AppError::conflict(
            "matchmaker cohort changed repeatedly while claiming formation",
        ))
    }

    /// Claim a user's handoff redemption at most once. The original formation
    /// lease must still name the current owner/generation fence; a lease renewal
    /// with the same fence is valid, while a transfer is rejected.
    pub async fn claim_admission(
        &self,
        ticket: &TicketId,
        user_id: &str,
        lease: &MatchmakerShardLease,
        now: TimestampMillis,
    ) -> AppResult<()> {
        for _ in 0..MAX_WRITE_RETRIES {
            let (mut state, expected) = self.read_state(lease.shard).await?;
            ensure_current(&state, lease, now)?;
            let claim = state.formations.get_mut(ticket.as_str()).ok_or_else(|| {
                AppError::conflict("matchmaker ticket was not formed by the current owner")
            })?;
            let formation_lease = decode(lease.shard, claim.lease.clone())?;
            if !formation_lease.has_same_fence_as(lease) {
                return Err(AppError::conflict(
                    "matchmaker handoff belongs to a stale shard owner",
                ));
            }
            if !claim.admitted_users.insert(user_id.to_owned()) {
                return Err(AppError::conflict(
                    "matchmaker handoff was already admitted for this user",
                ));
            }
            match self.write_state(lease.shard, state, expected).await {
                Ok(()) => return Ok(()),
                Err(error) if error.category() == ErrorCategory::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        Err(AppError::conflict(
            "matchmaker admission changed repeatedly while claiming it",
        ))
    }

    async fn read_state(&self, shard: QueueShardId) -> AppResult<(StoredShardState, Precondition)> {
        let object_id = id(shard);
        let Some(object) = self.storage.read(&Accessor::Runtime, &object_id).await? else {
            return Ok((StoredShardState::default(), Precondition::MustNotExist));
        };
        let value = object.value.into_json();
        let state = match serde_json::from_value::<StoredShardState>(value.clone()) {
            Ok(state) => state,
            Err(_) => {
                //  persisted only the lease shape. Accepting it during
                // rollout preserves already-held leases while upgrading the
                // object to the atomically claimed state on the next write.
                let lease: StoredLease = serde_json::from_value(value)
                    .map_err(|_| AppError::internal("invalid persisted matchmaker shard state"))?;
                StoredShardState {
                    max_generation: lease.generation,
                    lease: Some(lease),
                    formations: BTreeMap::new(),
                }
            }
        };
        Ok((state, Precondition::Match(object.version)))
    }

    async fn write_state(
        &self,
        shard: QueueShardId,
        state: StoredShardState,
        expected: Precondition,
    ) -> AppResult<()> {
        let value = StorageValue::new(
            serde_json::to_value(state)
                .map_err(|_| AppError::internal("could not serialize matchmaker shard state"))?,
        )?;
        self.storage
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(id(shard), value, Permissions::runtime_only())
                    .expecting(expected),
            )
            .await?;
        Ok(())
    }
}

fn id(shard: QueueShardId) -> ObjectId {
    ObjectId::new(
        Owner::System,
        Collection::new(COLLECTION).expect("static collection is valid"),
        Key::new(format!("shard-{}", shard.get())).expect("derived key is valid"),
    )
}

fn encode(lease: &MatchmakerShardLease) -> StoredLease {
    StoredLease {
        node_id: lease.owner_node.as_str().to_owned(),
        generation: lease.generation.get(),
        expires_at: lease.expires_at.unix_millis(),
    }
}

fn decode(shard: QueueShardId, stored: StoredLease) -> AppResult<MatchmakerShardLease> {
    Ok(MatchmakerShardLease {
        shard,
        owner_node: NodeId::new(stored.node_id)?,
        generation: OwnershipGeneration::new(stored.generation),
        expires_at: TimestampMillis::from_unix_millis(stored.expires_at),
    })
}

fn ensure_current(
    state: &StoredShardState,
    lease: &MatchmakerShardLease,
    now: TimestampMillis,
) -> AppResult<()> {
    let current = state
        .lease
        .clone()
        .map(|stored| decode(lease.shard, stored))
        .transpose()?;
    if !current
        .is_some_and(|current| current.has_same_fence_as(lease) && current.is_current_at(now))
    {
        return Err(AppError::conflict("matchmaker shard lease is not current"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryStorageRepository;

    fn lease(node: &str, generation: u64, expiry: u64) -> MatchmakerShardLease {
        MatchmakerShardLease {
            shard: QueueShardId::new(1),
            owner_node: NodeId::new(node).expect("test node"),
            generation: OwnershipGeneration::new(generation),
            expires_at: TimestampMillis::from_unix_millis(expiry),
        }
    }

    fn ticket(value: &str) -> TicketId {
        TicketId::parse(value).expect("test ticket")
    }

    #[tokio::test]
    async fn persists_fenced_leases_through_the_portable_storage_contract() {
        let directory =
            StorageMatchmakerLeaseDirectory::new(Arc::new(InMemoryStorageRepository::new()));
        let first = lease("node-a", 1, 100);
        directory
            .acquire(first.clone(), TimestampMillis::from_unix_millis(10))
            .await
            .expect("acquire");
        assert!(
            directory
                .acquire(
                    lease("node-b", 1, 100),
                    TimestampMillis::from_unix_millis(10)
                )
                .await
                .is_err()
        );
        assert_eq!(
            directory
                .read(first.shard, TimestampMillis::from_unix_millis(10))
                .await
                .expect("read"),
            Some(first)
        );
        directory
            .acquire(
                lease("node-b", 2, 200),
                TimestampMillis::from_unix_millis(101),
            )
            .await
            .expect("expired transfer");
    }

    #[tokio::test]
    async fn renewal_keeps_the_fence_but_transfer_rejects_old_formations() {
        let storage = Arc::new(InMemoryStorageRepository::new());
        let first = StorageMatchmakerLeaseDirectory::new(storage.clone());
        let restarted = StorageMatchmakerLeaseDirectory::new(storage);
        let a1 = lease("node-a", 1, 100);
        first
            .acquire(a1.clone(), TimestampMillis::from_unix_millis(10))
            .await
            .expect("initial lease");
        let one = ticket("party-one");
        let two = ticket("party-two");
        first
            .claim_formations(
                &[one.clone(), two.clone()],
                &a1,
                TimestampMillis::from_unix_millis(20),
            )
            .await
            .expect("atomic party claim");
        assert!(
            restarted
                .claim_formations(
                    &[two.clone(), ticket("unclaimed")],
                    &a1,
                    TimestampMillis::from_unix_millis(21),
                )
                .await
                .is_err()
        );

        let renewed = lease("node-a", 1, 200);
        restarted
            .acquire(renewed.clone(), TimestampMillis::from_unix_millis(30))
            .await
            .expect("renewal keeps fencing generation");
        restarted
            .claim_admission(&one, "alice", &a1, TimestampMillis::from_unix_millis(40))
            .await
            .expect("original formation lease remains redeemable after renewal");
        assert!(
            restarted
                .claim_admission(
                    &one,
                    "alice",
                    &renewed,
                    TimestampMillis::from_unix_millis(41)
                )
                .await
                .is_err()
        );

        let b2 = lease("node-b", 2, 300);
        restarted
            .acquire(b2, TimestampMillis::from_unix_millis(201))
            .await
            .expect("restart transfer persists high-water generation");
        assert!(
            restarted
                .claim_admission(&two, "bob", &a1, TimestampMillis::from_unix_millis(202))
                .await
                .is_err()
        );
    }
}
