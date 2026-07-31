//! Durable, globally fenced authority for runtime shared-cache propagation.
//!
//! The runtime cache itself remains memory resident. This directory stores only
//! the single cluster writer lease and its monotonic generation so a restart or
//! failover cannot reuse a stale fence.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::repository::StorageRepository;
use crate::session::{NodeId, OwnershipGeneration};
use crate::storage::{
    Accessor, Collection, Key, ObjectId, Owner, Permissions, Precondition, StorageValue,
    WriteRequest,
};
use crate::time::TimestampMillis;

const COLLECTION: &str = "citadel.runtime.cache_lease";
const KEY: &str = "global";
const MAX_WRITE_RETRIES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCacheLease {
    pub owner_node: NodeId,
    pub generation: OwnershipGeneration,
    pub expires_at: TimestampMillis,
    incarnation: String,
}

impl RuntimeCacheLease {
    #[must_use]
    pub fn is_current_at(&self, now: TimestampMillis) -> bool {
        now < self.expires_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeCacheLeaseResolution {
    Local(RuntimeCacheLease),
    Remote(RuntimeCacheLease),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StoredLeaseState {
    #[serde(default)]
    lease: Option<StoredLease>,
    #[serde(default)]
    max_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLease {
    node_id: String,
    generation: u64,
    expires_at: u64,
    #[serde(default)]
    incarnation: String,
}

/// Portable compare-and-swap directory for the one global cache writer.
#[derive(Clone)]
pub struct StorageRuntimeCacheLeaseDirectory {
    storage: Arc<dyn StorageRepository>,
}

impl StorageRuntimeCacheLeaseDirectory {
    #[must_use]
    pub fn new(storage: Arc<dyn StorageRepository>) -> Self {
        Self { storage }
    }

    /// Resolve a live writer or atomically acquire an expired/absent lease.
    /// Renewals retain their generation; transfers always advance the durable
    /// high-water mark.
    pub async fn acquire_or_resolve(
        &self,
        node: NodeId,
        incarnation: &str,
        expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> AppResult<RuntimeCacheLeaseResolution> {
        if expires_at <= now {
            return Err(AppError::validation(
                "runtime cache lease must expire after acquisition time",
            ));
        }
        if incarnation.is_empty() || incarnation.len() > 128 {
            return Err(AppError::validation(
                "runtime cache lease incarnation must be non-empty and bounded",
            ));
        }
        for _ in 0..MAX_WRITE_RETRIES {
            let (mut state, expected) = self.read_state().await?;
            let current = state.lease.clone().map(decode).transpose()?;
            if let Some(current) = current.filter(|lease| lease.is_current_at(now)) {
                if current.owner_node != node {
                    return Ok(RuntimeCacheLeaseResolution::Remote(current));
                }
                // A fresh process using the same configured node id must not
                // evict a still-live incarnation. It waits for expiry just as
                // another node would; otherwise the old process could keep
                // signing its unexpired generation for untouched keys.
                if current.incarnation != incarnation {
                    return Ok(RuntimeCacheLeaseResolution::Remote(current));
                }
                let renewed = RuntimeCacheLease {
                    owner_node: node.clone(),
                    generation: current.generation,
                    expires_at,
                    incarnation: incarnation.to_owned(),
                };
                state.lease = Some(encode(&renewed));
                match self.write_state(state, expected).await {
                    Ok(()) => return Ok(RuntimeCacheLeaseResolution::Local(renewed)),
                    Err(error) if error.category() == ErrorCategory::Conflict => continue,
                    Err(error) => return Err(error),
                }
            } else {
                let generation = state.max_generation.checked_add(1).ok_or_else(|| {
                    AppError::internal("runtime cache lease generation overflowed")
                })?;
                let acquired = RuntimeCacheLease {
                    owner_node: node.clone(),
                    generation: OwnershipGeneration::new(generation),
                    expires_at,
                    incarnation: incarnation.to_owned(),
                };
                state.max_generation = generation;
                state.lease = Some(encode(&acquired));
                match self.write_state(state, expected).await {
                    Ok(()) => return Ok(RuntimeCacheLeaseResolution::Local(acquired)),
                    Err(error) if error.category() == ErrorCategory::Conflict => continue,
                    Err(error) => return Err(error),
                }
            }
        }
        Err(AppError::conflict(
            "runtime cache lease changed repeatedly while resolving it",
        ))
    }

    async fn read_state(&self) -> AppResult<(StoredLeaseState, Precondition)> {
        let Some(object) = self.storage.read(&Accessor::Runtime, &id()).await? else {
            return Ok((StoredLeaseState::default(), Precondition::MustNotExist));
        };
        let state = serde_json::from_value(object.value.into_json())
            .map_err(|_| AppError::internal("invalid persisted runtime cache lease state"))?;
        Ok((state, Precondition::Match(object.version)))
    }

    async fn write_state(&self, state: StoredLeaseState, expected: Precondition) -> AppResult<()> {
        let value = StorageValue::new(
            serde_json::to_value(state)
                .map_err(|_| AppError::internal("could not serialize runtime cache lease"))?,
        )?;
        self.storage
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(id(), value, Permissions::runtime_only()).expecting(expected),
            )
            .await?;
        Ok(())
    }
}

fn id() -> ObjectId {
    ObjectId::new(
        Owner::System,
        Collection::new(COLLECTION).expect("static collection is valid"),
        Key::new(KEY).expect("static key is valid"),
    )
}

fn encode(lease: &RuntimeCacheLease) -> StoredLease {
    StoredLease {
        node_id: lease.owner_node.as_str().to_owned(),
        generation: lease.generation.get(),
        expires_at: lease.expires_at.unix_millis(),
        incarnation: lease.incarnation.clone(),
    }
}

fn decode(stored: StoredLease) -> AppResult<RuntimeCacheLease> {
    Ok(RuntimeCacheLease {
        owner_node: NodeId::new(stored.node_id)?,
        generation: OwnershipGeneration::new(stored.generation),
        expires_at: TimestampMillis::from_unix_millis(stored.expires_at),
        incarnation: stored.incarnation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryStorageRepository;

    #[tokio::test]
    async fn restart_and_failover_advance_the_durable_fence() {
        let storage = Arc::new(InMemoryStorageRepository::new());
        let first = StorageRuntimeCacheLeaseDirectory::new(storage.clone());
        let restarted = StorageRuntimeCacheLeaseDirectory::new(storage);
        let node_a = NodeId::new("node-a").expect("node");
        let node_b = NodeId::new("node-b").expect("node");
        let at = TimestampMillis::from_unix_millis(10);
        let initial = first
            .acquire_or_resolve(
                node_a.clone(),
                "process-a",
                TimestampMillis::from_unix_millis(100),
                at,
            )
            .await
            .expect("acquire");
        assert!(
            matches!(initial, RuntimeCacheLeaseResolution::Local(ref lease) if lease.generation == OwnershipGeneration::new(1))
        );
        assert!(matches!(
            restarted
                .acquire_or_resolve(
                    node_b.clone(),
                    "process-b",
                    TimestampMillis::from_unix_millis(100),
                    at,
                )
                .await
                .expect("resolve"),
            RuntimeCacheLeaseResolution::Remote(ref lease) if lease.owner_node == node_a
        ));
        assert!(matches!(
            restarted
                .acquire_or_resolve(
                    node_b,
                    "process-b",
                    TimestampMillis::from_unix_millis(200),
                    TimestampMillis::from_unix_millis(101),
                )
                .await
                .expect("failover"),
            RuntimeCacheLeaseResolution::Local(ref lease) if lease.generation == OwnershipGeneration::new(2)
        ));
    }

    #[tokio::test]
    async fn a_restarted_writer_waits_for_the_live_incarnation_then_advances_generation() {
        let directory =
            StorageRuntimeCacheLeaseDirectory::new(Arc::new(InMemoryStorageRepository::new()));
        let node = NodeId::new("node-a").expect("node");
        let first = directory
            .acquire_or_resolve(
                node.clone(),
                "process-one",
                TimestampMillis::from_unix_millis(100),
                TimestampMillis::from_unix_millis(10),
            )
            .await
            .expect("first acquire");
        assert!(
            matches!(first, RuntimeCacheLeaseResolution::Local(ref lease) if lease.generation == OwnershipGeneration::new(1))
        );
        let while_live = directory
            .acquire_or_resolve(
                node.clone(),
                "process-two",
                TimestampMillis::from_unix_millis(100),
                TimestampMillis::from_unix_millis(11),
            )
            .await
            .expect("restart resolve");
        assert!(
            matches!(while_live, RuntimeCacheLeaseResolution::Remote(ref lease) if lease.generation == OwnershipGeneration::new(1))
        );
        let restarted = directory
            .acquire_or_resolve(
                node,
                "process-two",
                TimestampMillis::from_unix_millis(200),
                TimestampMillis::from_unix_millis(101),
            )
            .await
            .expect("expired restart acquire");
        assert!(
            matches!(restarted, RuntimeCacheLeaseResolution::Local(ref lease) if lease.generation == OwnershipGeneration::new(2))
        );
    }
}
