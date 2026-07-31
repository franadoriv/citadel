//! Bounded, node-local mutable runtime cache.
//!
//! Version one is deliberately process-local and non-durable. Entries are
//! isolated by namespace, expire lazily by TTL, and disappear on restart.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::observability::NodeMetrics;
use crate::runtime::cluster::{RuntimeCacheMutation, RuntimeCacheWrite};
use crate::time::{Clock, SystemClock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSharedCachePolicy {
    pub enabled: bool,
    pub max_entries: usize,
    pub max_value_bytes: usize,
    pub max_ttl: Duration,
}

impl From<&crate::config::SharedCacheCapabilityConfig> for RuntimeSharedCachePolicy {
    fn from(config: &crate::config::SharedCacheCapabilityConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_entries: config.max_entries,
            max_value_bytes: config.max_value_bytes,
            max_ttl: Duration::from_millis(config.max_ttl_ms),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSharedCacheValue {
    pub value: Vec<u8>,
    pub version: u64,
    pub expires_in_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeSharedCacheError {
    #[error("runtime shared cache is disabled")]
    Disabled,
    #[error("runtime shared cache namespace is invalid")]
    InvalidNamespace,
    #[error("runtime shared cache key is invalid")]
    InvalidKey,
    #[error("runtime shared cache value exceeds configured size limit")]
    ValueTooLarge,
    #[error("runtime shared cache TTL exceeds configured limit")]
    TtlTooLarge,
    #[error("runtime shared cache entry limit is zero")]
    ZeroEntryLimit,
}

#[derive(Debug)]
struct CacheEntry {
    value: Vec<u8>,
    version: u64,
    expires_at: Instant,
}

#[derive(Debug)]
struct CacheState {
    entries: BTreeMap<(String, String), CacheEntry>,
    next_version: u64,
}

/// App-owned local cache shared across runtime VMs and retained across reloads.
pub struct RuntimeSharedCache {
    policy: RuntimeSharedCachePolicy,
    state: Mutex<CacheState>,
    metrics: Arc<NodeMetrics>,
    cluster_fences: Mutex<BTreeMap<(String, String), crate::runtime::cluster::RuntimeCacheFence>>,
    publisher: Mutex<Option<Arc<dyn RuntimeCachePublisher>>>,
}

impl fmt::Debug for RuntimeSharedCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeSharedCache")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

/// Bounded cluster publication boundary for local cache mutations.
pub trait RuntimeCachePublisher: Send + Sync {
    fn set(
        &self,
        namespace: String,
        key: String,
        value: Vec<u8>,
        expires_at: crate::time::TimestampMillis,
    );
    fn delete(&self, namespace: String, key: String, expires_at: crate::time::TimestampMillis);
}

/// Mutable indirection retained by a runtime VM across source reloads.
pub type RuntimeSharedCacheHandle = Arc<Mutex<Arc<RuntimeSharedCache>>>;

#[must_use]
pub fn disabled_runtime_shared_cache_handle() -> RuntimeSharedCacheHandle {
    Arc::new(Mutex::new(Arc::new(RuntimeSharedCache::new(
        RuntimeSharedCachePolicy::from(&crate::config::SharedCacheCapabilityConfig::default()),
        Arc::new(NodeMetrics::new()),
    ))))
}

pub fn set_runtime_shared_cache(handle: &RuntimeSharedCacheHandle, cache: Arc<RuntimeSharedCache>) {
    *handle.lock().unwrap_or_else(|e| e.into_inner()) = cache;
}

#[must_use]
pub fn runtime_shared_cache(handle: &RuntimeSharedCacheHandle) -> Arc<RuntimeSharedCache> {
    Arc::clone(&handle.lock().unwrap_or_else(|e| e.into_inner()))
}

impl RuntimeSharedCache {
    #[must_use]
    pub fn new(policy: RuntimeSharedCachePolicy, metrics: Arc<NodeMetrics>) -> Self {
        Self {
            policy,
            state: Mutex::new(CacheState {
                entries: BTreeMap::new(),
                next_version: 0,
            }),
            metrics,
            cluster_fences: Mutex::new(BTreeMap::new()),
            publisher: Mutex::new(None),
        }
    }

    pub fn get(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<RuntimeSharedCacheValue>, RuntimeSharedCacheError> {
        self.validate_key(namespace, key)?;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let composite = (namespace.to_owned(), key.to_owned());
        let now = Instant::now();
        if state
            .entries
            .get(&composite)
            .is_some_and(|entry| entry.expires_at <= now)
        {
            state.entries.remove(&composite);
            return Ok(None);
        }
        Ok(state
            .entries
            .get(&composite)
            .map(|entry| RuntimeSharedCacheValue {
                value: entry.value.clone(),
                version: entry.version,
                expires_in_ms: entry.expires_at.saturating_duration_since(now).as_millis() as u64,
            }))
    }

    pub fn set(
        &self,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl: Duration,
    ) -> Result<RuntimeSharedCacheValue, RuntimeSharedCacheError> {
        self.validate_key(namespace, key)?;
        if self.policy.max_entries == 0 {
            return Err(RuntimeSharedCacheError::ZeroEntryLimit);
        }
        if value.len() > self.policy.max_value_bytes {
            return Err(RuntimeSharedCacheError::ValueTooLarge);
        }
        if ttl > self.policy.max_ttl {
            return Err(RuntimeSharedCacheError::TtlTooLarge);
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::purge_expired(&mut state);
        let result = self.set_locked(&mut state, namespace, key, value, ttl);
        drop(state);
        let expires_at = crate::time::TimestampMillis::from_unix_millis(
            SystemClock
                .now()
                .unix_millis()
                .saturating_add(ttl.as_millis() as u64),
        );
        if let Some(publisher) = self
            .publisher
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            publisher.set(
                namespace.to_owned(),
                key.to_owned(),
                result.value.clone(),
                expires_at,
            );
        }
        Ok(result)
    }

    pub fn delete(&self, namespace: &str, key: &str) -> Result<bool, RuntimeSharedCacheError> {
        self.validate_key(namespace, key)?;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::purge_expired(&mut state);
        let deleted = state
            .entries
            .remove(&(namespace.to_owned(), key.to_owned()))
            .is_some();
        drop(state);
        if deleted
            && let Some(publisher) = self
                .publisher
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        {
            publisher.delete(
                namespace.to_owned(),
                key.to_owned(),
                crate::time::TimestampMillis::from_unix_millis(
                    SystemClock
                        .now()
                        .unix_millis()
                        .saturating_add(self.policy.max_ttl.as_millis() as u64),
                ),
            );
        }
        Ok(deleted)
    }

    pub fn compare_and_swap(
        &self,
        namespace: &str,
        key: &str,
        expected_version: Option<u64>,
        value: Vec<u8>,
        ttl: Duration,
    ) -> Result<Option<RuntimeSharedCacheValue>, RuntimeSharedCacheError> {
        self.validate_key(namespace, key)?;
        if self.policy.max_entries == 0 {
            return Err(RuntimeSharedCacheError::ZeroEntryLimit);
        }
        if value.len() > self.policy.max_value_bytes {
            return Err(RuntimeSharedCacheError::ValueTooLarge);
        }
        if ttl > self.policy.max_ttl {
            return Err(RuntimeSharedCacheError::TtlTooLarge);
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::purge_expired(&mut state);
        let version = state
            .entries
            .get(&(namespace.to_owned(), key.to_owned()))
            .map(|entry| entry.version);
        if version != expected_version {
            return Ok(None);
        }
        let result = self.set_locked(&mut state, namespace, key, value, ttl);
        drop(state);
        let expires_at = crate::time::TimestampMillis::from_unix_millis(
            SystemClock
                .now()
                .unix_millis()
                .saturating_add(ttl.as_millis() as u64),
        );
        if let Some(publisher) = self
            .publisher
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            publisher.set(
                namespace.to_owned(),
                key.to_owned(),
                result.value.clone(),
                expires_at,
            );
        }
        Ok(Some(result))
    }

    /// Apply a fenced peer update. This is last-writer-wins propagation, not a
    /// globally linearizable CAS: stale fences and already-expired values fail
    /// closed, while a winning value receives a fresh local CAS version.
    pub fn apply_cluster_mutation(&self, mutation: RuntimeCacheMutation) -> bool {
        if self
            .validate_key(&mutation.namespace, &mutation.key)
            .is_err()
        {
            return false;
        }
        let now = SystemClock.now();
        if mutation.expires_at <= now {
            return false;
        }
        if self.policy.max_entries == 0 {
            return false;
        }
        let remaining_ttl = std::time::Duration::from_millis(
            mutation
                .expires_at
                .unix_millis()
                .saturating_sub(now.unix_millis()),
        );
        if remaining_ttl > self.policy.max_ttl {
            return false;
        }
        let composite = (mutation.namespace.clone(), mutation.key.clone());
        let mut fences = self
            .cluster_fences
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if fences
            .get(&composite)
            .is_some_and(|current| current >= &mutation.fence)
        {
            return false;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::purge_expired(&mut state);
        let accepted = match mutation.value {
            Some(value) if value.len() <= self.policy.max_value_bytes => {
                self.set_locked(
                    &mut state,
                    &mutation.namespace,
                    &mutation.key,
                    value,
                    remaining_ttl,
                );
                true
            }
            None => {
                state.entries.remove(&composite).is_some()
                    || !state.entries.contains_key(&composite)
            }
            _ => false,
        };
        if accepted {
            fences.insert(composite, mutation.fence);
        }
        accepted
    }

    /// Validate an authenticated cluster submission before its writer queue
    /// acknowledges it. This deliberately checks capability policy, but not a
    /// local CAS version: cluster cache ordering is fenced LWW rather than a
    /// globally linearizable CAS.
    #[must_use]
    pub fn can_accept_cluster_write(&self, write: &RuntimeCacheWrite) -> bool {
        if self.validate_key(&write.namespace, &write.key).is_err()
            || write.expires_at <= SystemClock.now()
        {
            return false;
        }
        if write.value.is_some() && self.policy.max_entries == 0 {
            return false;
        }
        if write
            .value
            .as_ref()
            .is_some_and(|value| value.len() > self.policy.max_value_bytes)
        {
            return false;
        }
        let remaining = write
            .expires_at
            .unix_millis()
            .saturating_sub(SystemClock.now().unix_millis());
        remaining <= self.policy.max_ttl.as_millis() as u64
    }

    pub fn set_publisher(&self, publisher: Arc<dyn RuntimeCachePublisher>) {
        *self.publisher.lock().unwrap_or_else(|e| e.into_inner()) = Some(publisher);
    }

    fn set_locked(
        &self,
        state: &mut CacheState,
        namespace: &str,
        key: &str,
        value: Vec<u8>,
        ttl: Duration,
    ) -> RuntimeSharedCacheValue {
        let composite = (namespace.to_owned(), key.to_owned());
        if !state.entries.contains_key(&composite)
            && state.entries.len() >= self.policy.max_entries
            && let Some(oldest) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(key, _)| key.clone())
        {
            state.entries.remove(&oldest);
            self.metrics.record_runtime_shared_cache_eviction();
        }
        state.next_version = state.next_version.wrapping_add(1).max(1);
        let version = state.next_version;
        state.entries.insert(
            composite,
            CacheEntry {
                value: value.clone(),
                version,
                expires_at: Instant::now() + ttl,
            },
        );
        RuntimeSharedCacheValue {
            value,
            version,
            expires_in_ms: ttl.as_millis() as u64,
        }
    }

    fn validate_key(&self, namespace: &str, key: &str) -> Result<(), RuntimeSharedCacheError> {
        if !self.policy.enabled {
            return Err(RuntimeSharedCacheError::Disabled);
        }
        if !is_valid_cache_component(namespace) {
            return Err(RuntimeSharedCacheError::InvalidNamespace);
        }
        if !is_valid_cache_component(key) {
            return Err(RuntimeSharedCacheError::InvalidKey);
        }
        Ok(())
    }

    fn purge_expired(state: &mut CacheState) {
        let now = Instant::now();
        state.entries.retain(|_, entry| entry.expires_at > now);
    }
}

fn is_valid_cache_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 80
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(entries: usize) -> RuntimeSharedCache {
        RuntimeSharedCache::new(
            RuntimeSharedCachePolicy {
                enabled: true,
                max_entries: entries,
                max_value_bytes: 16,
                max_ttl: Duration::from_secs(1),
            },
            Arc::new(NodeMetrics::new()),
        )
    }

    #[test]
    fn namespaces_versions_and_cas_are_isolated() {
        let cache = cache(4);
        let first = cache
            .set(
                "match.one",
                "score",
                b"one".to_vec(),
                Duration::from_secs(1),
            )
            .expect("set");
        cache
            .set(
                "match.two",
                "score",
                b"two".to_vec(),
                Duration::from_secs(1),
            )
            .expect("set");
        assert_eq!(
            cache
                .get("match.one", "score")
                .expect("get")
                .expect("present")
                .value,
            b"one"
        );
        assert!(
            cache
                .compare_and_swap(
                    "match.one",
                    "score",
                    Some(first.version),
                    b"three".to_vec(),
                    Duration::from_secs(1)
                )
                .expect("cas")
                .is_some()
        );
        assert!(
            cache
                .compare_and_swap(
                    "match.one",
                    "score",
                    Some(first.version),
                    b"stale".to_vec(),
                    Duration::from_secs(1)
                )
                .expect("stale cas")
                .is_none()
        );
    }

    #[test]
    fn expiry_and_entry_bound_are_enforced() {
        let metrics = Arc::new(NodeMetrics::new());
        let cache = RuntimeSharedCache::new(
            RuntimeSharedCachePolicy {
                enabled: true,
                max_entries: 1,
                max_value_bytes: 16,
                max_ttl: Duration::from_secs(1),
            },
            Arc::clone(&metrics),
        );
        cache
            .set("match", "short", b"x".to_vec(), Duration::ZERO)
            .expect("set");
        assert!(cache.get("match", "short").expect("get").is_none());
        cache
            .set("match", "first", b"1".to_vec(), Duration::from_secs(1))
            .expect("set");
        cache
            .set("match", "second", b"2".to_vec(), Duration::from_secs(1))
            .expect("set");
        assert!(cache.get("match", "first").expect("get").is_none());
        assert!(cache.get("match", "second").expect("get").is_some());
        assert_eq!(metrics.snapshot().runtime_shared_cache_evictions_total, 1);
    }

    #[test]
    fn zero_entry_policy_never_retains_a_value() {
        let cache = cache(0);
        assert_eq!(
            cache
                .set("match", "key", b"value".to_vec(), Duration::from_secs(1))
                .expect_err("zero-entry cache rejects writes"),
            RuntimeSharedCacheError::ZeroEntryLimit
        );
        assert!(cache.get("match", "key").expect("get").is_none());
    }

    #[test]
    fn cluster_mutations_reject_stale_fences_and_expired_replays() {
        let cache = cache(4);
        let now = SystemClock.now().unix_millis();
        let node = crate::session::NodeId::new("node-a").expect("node");
        let fresh = RuntimeCacheMutation {
            namespace: "match".to_string(),
            key: "score".to_string(),
            value: Some(b"new".to_vec()),
            expires_at: crate::time::TimestampMillis::from_unix_millis(now + 1_000),
            fence: crate::runtime::cluster::RuntimeCacheFence {
                owner_node: node.clone(),
                generation: crate::session::OwnershipGeneration::new(2),
                sequence: 1,
            },
        };
        assert!(cache.apply_cluster_mutation(fresh));
        let stale = RuntimeCacheMutation {
            namespace: "match".to_string(),
            key: "score".to_string(),
            value: Some(b"old".to_vec()),
            expires_at: crate::time::TimestampMillis::from_unix_millis(now + 1_000),
            fence: crate::runtime::cluster::RuntimeCacheFence {
                owner_node: node.clone(),
                generation: crate::session::OwnershipGeneration::new(1),
                sequence: 9,
            },
        };
        assert!(!cache.apply_cluster_mutation(stale));
        assert_eq!(
            cache
                .get("match", "score")
                .expect("get")
                .expect("value")
                .value,
            b"new"
        );
        let expired = RuntimeCacheMutation {
            namespace: "match".to_string(),
            key: "other".to_string(),
            value: Some(b"bad".to_vec()),
            expires_at: crate::time::TimestampMillis::from_unix_millis(now),
            fence: crate::runtime::cluster::RuntimeCacheFence {
                owner_node: node,
                generation: crate::session::OwnershipGeneration::new(3),
                sequence: 1,
            },
        };
        assert!(!cache.apply_cluster_mutation(expired));
        assert!(cache.get("match", "other").expect("get").is_none());
    }
}
