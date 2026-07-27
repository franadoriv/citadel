//! Local authority fences shared by chat and social/group mutations.
//!
//! The durable chat mutation path obtains this fence before it checks policy and
//! writes. Friendship or group changes obtain the same fence before changing
//! authority and advance the matching epoch before releasing it. This prevents a
//! local check-then-write race while the later realtime delivery work wires the
//! revocation callbacks to subscriptions and remote nodes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::error::{AppError, AppResult};
use crate::repository::ChatRepository;
use crate::time::{Clock, SystemClock};

/// Shared access-epoch and mutation fence for canonical chat targets.
#[derive(Clone)]
pub struct ChatAccessCoordinator {
    gate: Arc<AsyncMutex<()>>,
    epochs: Arc<Mutex<HashMap<String, u64>>>,
    repository: Option<Arc<dyn ChatRepository>>,
}

impl Default for ChatAccessCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl ChatAccessCoordinator {
    /// Create an empty local authority-fence coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            gate: Arc::new(AsyncMutex::new(())),
            epochs: Arc::new(Mutex::new(HashMap::new())),
            repository: None,
        }
    }

    /// Create a coordinator backed by the selected chat repository. The local
    /// gate still closes same-process check-then-write races; the repository
    /// projection makes the observed epoch durable and visible to peer nodes.
    #[must_use]
    pub fn with_repository(repository: Arc<dyn ChatRepository>) -> Self {
        Self {
            repository: Some(repository),
            ..Self::new()
        }
    }

    /// Serialize an authority-sensitive operation. Hold the returned guard
    /// through the policy check and the durable chat mutation or social change.
    pub async fn fence(&self) -> OwnedMutexGuard<()> {
        Arc::clone(&self.gate).lock_owned().await
    }

    /// Return the current epoch for `access_key`, creating epoch zero lazily.
    pub async fn epoch(&self, access_key: &str) -> AppResult<u64> {
        if let Some(repository) = &self.repository {
            return repository.current_access_epoch(access_key).await;
        }
        let mut epochs = self
            .epochs
            .lock()
            .map_err(|_| AppError::internal("chat access epoch mutex poisoned"))?;
        Ok(*epochs.entry(access_key.to_owned()).or_insert(0))
    }

    /// Advance an access epoch after its authority source changed under a fence.
    pub async fn advance(&self, access_key: &str) -> AppResult<u64> {
        let local_epoch = {
            let mut epochs = self
                .epochs
                .lock()
                .map_err(|_| AppError::internal("chat access epoch mutex poisoned"))?;
            let epoch = epochs.entry(access_key.to_owned()).or_insert(0);
            *epoch = epoch.saturating_add(1);
            *epoch
        };
        if let Some(repository) = &self.repository {
            return repository
                .advance_access_epoch(access_key, SystemClock.now())
                .await;
        }
        Ok(local_epoch)
    }

    /// Stable access key for a direct relationship pair.
    #[must_use]
    pub fn direct_key(first: &str, second: &str) -> String {
        let (lower, higher) = if first < second {
            (first, second)
        } else {
            (second, first)
        };
        format!("direct:{lower}:{higher}")
    }

    /// Stable access key for a group.
    #[must_use]
    pub fn group_key(group_id: u64) -> String {
        format!("group:{group_id}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn epochs_advance_only_after_the_shared_fence_is_held() {
        let coordinator = ChatAccessCoordinator::new();
        let key = ChatAccessCoordinator::direct_key("bob", "alice");
        let _guard = coordinator.fence().await;
        assert_eq!(coordinator.epoch(&key).await.expect("epoch"), 0);
        assert_eq!(coordinator.advance(&key).await.expect("advance"), 1);
        assert_eq!(coordinator.epoch(&key).await.expect("epoch"), 1);
    }
}
