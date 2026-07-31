//! Bounded, node-local runtime event bus.
//!
//! Version one is deliberately best-effort and process-local: an accepted
//! event is queued in FIFO order for the currently loaded runtime only. It is
//! never persisted, replicated, retried, or replayed after a restart. Runtime
//! adapters drain one snapshot after an outer host invocation; events emitted
//! by a subscriber are deferred to the next invocation, preventing recursive
//! delivery from extending a single script call without bound.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::observability::NodeMetrics;

/// Maximum queued events one outer runtime invocation may attempt to deliver.
/// The remaining FIFO entries stay queued for a later invocation.
pub const MAX_RUNTIME_EVENTS_PER_INVOCATION: usize = 64;

/// Operator-owned limits for the node-local event bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeEventPolicy {
    pub enabled: bool,
    pub queue_capacity: usize,
    pub max_event_bytes: usize,
    pub max_events_per_minute: u32,
}

impl Default for RuntimeEventPolicy {
    fn default() -> Self {
        Self::from(&crate::config::RuntimeEventsCapabilityConfig::default())
    }
}

impl From<&crate::config::RuntimeEventsCapabilityConfig> for RuntimeEventPolicy {
    fn from(config: &crate::config::RuntimeEventsCapabilityConfig) -> Self {
        Self {
            enabled: config.enabled,
            queue_capacity: config.queue_capacity,
            max_event_bytes: config.max_event_bytes,
            max_events_per_minute: config.max_events_per_minute,
        }
    }
}

/// One typed, binary-safe event accepted by the runtime event bus.
///
/// `namespace` scopes independent publishers/subscribers; `event_type` is the
/// explicit type discriminator. The payload remains opaque bytes so Lua,
/// Python, and JavaScript have identical binary-safe semantics.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeEvent {
    pub namespace: String,
    pub event_type: String,
    pub payload: Vec<u8>,
}

impl RuntimeEvent {
    /// Build an event after validating its namespace and type grammar.
    pub fn new(
        namespace: impl Into<String>,
        event_type: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<Self, RuntimeEventError> {
        let namespace = namespace.into();
        let event_type = event_type.into();
        if !is_valid_runtime_event_name(&namespace) {
            return Err(RuntimeEventError::InvalidNamespace);
        }
        if !is_valid_runtime_event_name(&event_type) {
            return Err(RuntimeEventError::InvalidType);
        }
        Ok(Self {
            namespace,
            event_type,
            payload,
        })
    }
}

/// Event acceptance result. Drops are intentional best-effort outcomes, not
/// script exceptions, so callers can decide whether to coalesce their work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeEventEmitOutcome {
    Queued,
    DroppedDisabled,
    DroppedRateLimited,
    DroppedQueueFull,
    DroppedPayloadTooLarge,
}

/// Declaration/input validation errors surfaced without leaking node state.
#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq, Eq)]
pub enum RuntimeEventError {
    #[error("runtime event namespace is invalid")]
    InvalidNamespace,
    #[error("runtime event type is invalid")]
    InvalidType,
}

/// Fixed-window counter used once per namespace. It is bounded by active
/// namespaces and deliberately fail-closed if a caller attempts to create too
/// many distinct windows during one minute.
#[derive(Debug, Default)]
struct RuntimeEventRateLimiter {
    windows: Mutex<BTreeMap<String, RuntimeEventRateWindow>>,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeEventRateWindow {
    started: Instant,
    count: u32,
}

impl RuntimeEventRateLimiter {
    fn allow(&self, namespace: &str, max_events_per_minute: u32) -> bool {
        const MAX_TRACKED_NAMESPACES: usize = 10_000;
        let now = Instant::now();
        let mut windows = self
            .windows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        windows.retain(|_, window| now.duration_since(window.started) < Duration::from_secs(60));
        if !windows.contains_key(namespace) && windows.len() >= MAX_TRACKED_NAMESPACES {
            return false;
        }
        let window = windows
            .entry(namespace.to_string())
            .or_insert(RuntimeEventRateWindow {
                started: now,
                count: 0,
            });
        if window.count >= max_events_per_minute {
            return false;
        }
        window.count = window.count.saturating_add(1);
        true
    }
}

/// Shared, fixed-capacity event queue for one node runtime.
pub struct RuntimeEventBus {
    policy: RuntimeEventPolicy,
    queue: Mutex<VecDeque<RuntimeEvent>>,
    rate_limiter: RuntimeEventRateLimiter,
    metrics: Arc<NodeMetrics>,
    publisher: Mutex<Option<Arc<dyn RuntimeEventPublisher>>>,
}

impl fmt::Debug for RuntimeEventBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeEventBus")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

/// Best-effort, bounded cluster publication boundary. Implementations must not
/// block the runtime invocation; a saturated publisher drops the peer attempt.
pub trait RuntimeEventPublisher: Send + Sync {
    fn publish(&self, event: RuntimeEvent);
}

/// Mutable indirection retained by a runtime VM across hot reloads. The
/// production bootstrap replaces its disabled default with the app-owned bus;
/// unit runtimes can do the same without exposing process-global state.
pub type RuntimeEventBusHandle = Arc<Mutex<Arc<RuntimeEventBus>>>;

/// Build a disabled per-runtime handle for constructors and isolated tests.
#[must_use]
pub fn disabled_runtime_event_bus_handle() -> RuntimeEventBusHandle {
    Arc::new(Mutex::new(Arc::new(RuntimeEventBus::new(
        RuntimeEventPolicy::default(),
        Arc::new(NodeMetrics::new()),
    ))))
}

/// Swap the app-owned bus into a runtime handle before serving script calls.
pub fn set_runtime_event_bus(handle: &RuntimeEventBusHandle, bus: Arc<RuntimeEventBus>) {
    *handle
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = bus;
}

/// Snapshot the currently configured bus without holding the handle lock while
/// it processes an event.
#[must_use]
pub fn runtime_event_bus(handle: &RuntimeEventBusHandle) -> Arc<RuntimeEventBus> {
    Arc::clone(
        &handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    )
}

impl RuntimeEventBus {
    /// Build a node-local bus using the operator-owned policy and shared node
    /// metrics. Construction itself exposes no script API.
    #[must_use]
    pub fn new(policy: RuntimeEventPolicy, metrics: Arc<NodeMetrics>) -> Self {
        Self {
            policy,
            queue: Mutex::new(VecDeque::with_capacity(policy.queue_capacity)),
            rate_limiter: RuntimeEventRateLimiter::default(),
            metrics,
            publisher: Mutex::new(None),
        }
    }

    /// Current immutable operator policy.
    #[must_use]
    pub const fn policy(&self) -> RuntimeEventPolicy {
        self.policy
    }

    /// Queue one event, returning a best-effort drop disposition when the bus
    /// cannot accept it. Accepted events preserve FIFO order within this node.
    pub fn emit(&self, event: RuntimeEvent) -> RuntimeEventEmitOutcome {
        self.emit_inner(event, true)
    }

    /// Accept a peer-delivered event without publishing it again.
    pub fn emit_remote(&self, event: RuntimeEvent) -> RuntimeEventEmitOutcome {
        self.emit_inner(event, false)
    }

    pub fn set_publisher(&self, publisher: Arc<dyn RuntimeEventPublisher>) {
        *self.publisher.lock().unwrap_or_else(|e| e.into_inner()) = Some(publisher);
    }

    fn emit_inner(&self, event: RuntimeEvent, publish: bool) -> RuntimeEventEmitOutcome {
        if !self.policy.enabled {
            return self.drop(RuntimeEventEmitOutcome::DroppedDisabled, &event);
        }
        if event.payload.len() > self.policy.max_event_bytes {
            return self.drop(RuntimeEventEmitOutcome::DroppedPayloadTooLarge, &event);
        }
        if !self
            .rate_limiter
            .allow(&event.namespace, self.policy.max_events_per_minute)
        {
            return self.drop(RuntimeEventEmitOutcome::DroppedRateLimited, &event);
        }
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if queue.len() >= self.policy.queue_capacity {
            return self.drop(RuntimeEventEmitOutcome::DroppedQueueFull, &event);
        }
        queue.push_back(event.clone());
        self.metrics.record_runtime_event_queued();
        drop(queue);
        if publish
            && let Some(publisher) = self
                .publisher
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        {
            publisher.publish(event);
        }
        RuntimeEventEmitOutcome::Queued
    }

    /// Drain the events present when this method begins. Adapters call this after
    /// normal command-producing dispatch, lifecycle, or tick handlers return, so
    /// subscriber emissions are queued for a later invocation rather than
    /// delivered reentrantly.
    #[must_use]
    pub fn drain_snapshot(&self) -> Vec<RuntimeEvent> {
        self.drain_snapshot_limit(usize::MAX)
    }

    /// Drain at most `limit` events from the head of the queue. This keeps
    /// delivery work bounded while preserving pending FIFO entries for a later
    /// outer invocation.
    #[must_use]
    pub fn drain_snapshot_limit(&self, limit: usize) -> Vec<RuntimeEvent> {
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let count = queue.len().min(limit);
        queue.drain(..count).collect()
    }

    /// Return an unstarted delivery tail to the queue front. Adapters call
    /// this when their shared deadline expires between events, preserving FIFO
    /// order for the next outer invocation.
    pub fn requeue_front(&self, mut events: Vec<RuntimeEvent>) {
        if events.is_empty() {
            return;
        }
        let mut queue = self
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let free = self.policy.queue_capacity.saturating_sub(queue.len());
        if events.len() > free {
            let dropped = events.len() - free;
            events.truncate(free);
            tracing::warn!(
                dropped,
                "runtime event queue filled before deferred events could be requeued"
            );
            for _ in 0..dropped {
                self.metrics.record_runtime_event_dropped();
            }
        }
        for event in events.into_iter().rev() {
            queue.push_front(event);
        }
    }

    /// Pending queue depth, intended for focused tests and diagnostics.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    fn drop(
        &self,
        outcome: RuntimeEventEmitOutcome,
        event: &RuntimeEvent,
    ) -> RuntimeEventEmitOutcome {
        self.metrics.record_runtime_event_dropped();
        tracing::warn!(
            namespace = %event.namespace,
            event_type = %event.event_type,
            outcome = ?outcome,
            "runtime event dropped by local best-effort bus"
        );
        outcome
    }
}

/// Namespaces and event types share a narrow, portable grammar.
#[must_use]
pub fn is_valid_runtime_event_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 80
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bus(policy: RuntimeEventPolicy) -> RuntimeEventBus {
        RuntimeEventBus::new(policy, Arc::new(NodeMetrics::new()))
    }

    fn event(payload: &[u8]) -> RuntimeEvent {
        RuntimeEvent::new("match.score", "updated", payload.to_vec()).expect("valid event")
    }

    #[test]
    fn names_are_narrow_and_portable() {
        assert!(is_valid_runtime_event_name("match.score-v1"));
        for value in ["", "room/one", "white space", "évent"] {
            assert!(
                !is_valid_runtime_event_name(value),
                "{value} must be rejected"
            );
        }
    }

    #[test]
    fn queue_is_fifo_and_limited_snapshot_defers_remaining_entries() {
        let bus = bus(RuntimeEventPolicy {
            enabled: true,
            queue_capacity: 3,
            max_event_bytes: 8,
            max_events_per_minute: 10,
        });
        assert_eq!(bus.emit(event(b"one")), RuntimeEventEmitOutcome::Queued);
        assert_eq!(bus.emit(event(b"two")), RuntimeEventEmitOutcome::Queued);
        assert_eq!(bus.emit(event(b"three")), RuntimeEventEmitOutcome::Queued);
        let snapshot = bus.drain_snapshot_limit(2);
        assert_eq!(
            snapshot
                .iter()
                .map(|event| &event.payload)
                .collect::<Vec<_>>(),
            vec![&b"one".to_vec(), &b"two".to_vec()]
        );
        assert_eq!(bus.pending_len(), 1);
        assert_eq!(bus.drain_snapshot()[0].payload, b"three".to_vec());
    }

    #[test]
    fn requeued_delivery_tail_returns_to_the_front_in_fifo_order() {
        let bus = bus(RuntimeEventPolicy {
            enabled: true,
            queue_capacity: 3,
            max_event_bytes: 8,
            max_events_per_minute: 10,
        });
        assert_eq!(bus.emit(event(b"one")), RuntimeEventEmitOutcome::Queued);
        assert_eq!(bus.emit(event(b"two")), RuntimeEventEmitOutcome::Queued);
        assert_eq!(bus.emit(event(b"three")), RuntimeEventEmitOutcome::Queued);
        let mut batch = bus.drain_snapshot_limit(3);
        let _started = batch.remove(0);
        bus.requeue_front(batch);
        assert_eq!(
            bus.drain_snapshot()
                .iter()
                .map(|event| event.payload.as_slice())
                .collect::<Vec<_>>(),
            vec![b"two".as_slice(), b"three".as_slice()]
        );
    }

    #[test]
    fn oversized_full_and_rate_limited_events_drop() {
        let limited_bus = bus(RuntimeEventPolicy {
            enabled: true,
            queue_capacity: 1,
            max_event_bytes: 3,
            max_events_per_minute: 1,
        });
        assert_eq!(
            limited_bus.emit(event(b"four")),
            RuntimeEventEmitOutcome::DroppedPayloadTooLarge
        );
        assert_eq!(
            limited_bus.emit(event(b"one")),
            RuntimeEventEmitOutcome::Queued
        );
        assert_eq!(
            limited_bus.emit(event(b"two")),
            RuntimeEventEmitOutcome::DroppedRateLimited
        );
        let bus = bus(RuntimeEventPolicy {
            enabled: true,
            queue_capacity: 1,
            max_event_bytes: 3,
            max_events_per_minute: 2,
        });
        assert_eq!(bus.emit(event(b"one")), RuntimeEventEmitOutcome::Queued);
        assert_eq!(
            bus.emit(event(b"two")),
            RuntimeEventEmitOutcome::DroppedQueueFull
        );
    }
}
