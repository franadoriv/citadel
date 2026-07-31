//! Opt-in volatile deferred storage writes.
//!
//! This service deliberately sits above `StorageRepository`: its acknowledgement
//! means only that an unconditional operation entered a bounded in-memory queue.
//! It never changes the durable, synchronous repository contract.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify, watch};

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::repository::{BackendKind, StorageRepository};
use crate::storage::{Accessor, ObjectId, Precondition, StorageIndexMembership, WriteRequest};

/// Operator configuration for the loss-tolerant deferred writer.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeferredStorageConfig {
    /// Disabled by default: ordinary repository calls remain durable.
    pub enabled: bool,
    /// Only these collections may be admitted to the volatile queue.
    pub collections: Vec<String>,
    pub max_items: usize,
    pub max_bytes: usize,
    pub flush_interval_ms: u64,
    pub flush_batch_items: usize,
    pub shutdown_drain_timeout_ms: u64,
}

impl Default for DeferredStorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            collections: Vec::new(),
            max_items: 1_024,
            max_bytes: 4 * 1024 * 1024,
            flush_interval_ms: 10,
            flush_batch_items: 64,
            shutdown_drain_timeout_ms: 5_000,
        }
    }
}

impl DeferredStorageConfig {
    pub fn validate(&self) -> AppResult<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.collections.is_empty() {
            return Err(AppError::config(
                "storage.deferred.collections must not be empty when enabled",
            ));
        }
        if self.collections.iter().any(|c| c.trim().is_empty()) {
            return Err(AppError::config(
                "storage.deferred.collections must not contain empty values",
            ));
        }
        if self.max_items == 0
            || self.max_bytes == 0
            || self.flush_interval_ms == 0
            || self.flush_batch_items == 0
            || self.shutdown_drain_timeout_ms == 0
        {
            return Err(AppError::config(
                "storage.deferred bounds and intervals must be >= 1 when enabled",
            ));
        }
        Ok(())
    }
}

/// Bounded, label-free counters appropriate for logs/dashboard snapshots.
pub struct DeferredStorageMetrics {
    started: Instant,
    enabled: AtomicBool,
    queued_items: AtomicUsize,
    queued_bytes: AtomicUsize,
    queued_keys: AtomicUsize,
    queued_oldest_age_ms: AtomicU64,
    /// Monotonic milliseconds since `started`, plus one so zero means empty.
    queued_oldest_enqueued_ms: AtomicU64,
    accepted: AtomicU64,
    coalesced: AtomicU64,
    rejected_full: AtomicU64,
    rejected_bytes: AtomicU64,
    committed: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    shutdown_drained: AtomicU64,
    shutdown_abandoned: AtomicU64,
    shutdown_abandoned_bytes: AtomicU64,
    backend_latency_total_ms: AtomicU64,
    backend_latency_last_ms: AtomicU64,
}
impl Default for DeferredStorageMetrics {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            enabled: AtomicBool::new(false),
            queued_items: AtomicUsize::new(0),
            queued_bytes: AtomicUsize::new(0),
            queued_keys: AtomicUsize::new(0),
            queued_oldest_age_ms: AtomicU64::new(0),
            queued_oldest_enqueued_ms: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            coalesced: AtomicU64::new(0),
            rejected_full: AtomicU64::new(0),
            rejected_bytes: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            shutdown_drained: AtomicU64::new(0),
            shutdown_abandoned: AtomicU64::new(0),
            shutdown_abandoned_bytes: AtomicU64::new(0),
            backend_latency_total_ms: AtomicU64::new(0),
            backend_latency_last_ms: AtomicU64::new(0),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct DeferredStorageMetricsSnapshot {
    pub enabled: bool,
    pub queued_items: usize,
    pub queued_bytes: usize,
    pub queued_keys: usize,
    pub queued_oldest_age_ms: u64,
    pub accepted: u64,
    pub coalesced: u64,
    pub rejected_full: u64,
    pub rejected_bytes: u64,
    pub committed: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub shutdown_drained: u64,
    pub shutdown_abandoned: u64,
    pub shutdown_abandoned_bytes: u64,
    pub backend_latency_total_ms: u64,
    pub backend_latency_last_ms: u64,
}
impl DeferredStorageMetrics {
    pub fn snapshot(&self) -> DeferredStorageMetricsSnapshot {
        let oldest_enqueued = self.queued_oldest_enqueued_ms.load(Ordering::Relaxed);
        let queued_oldest_age_ms = if oldest_enqueued == 0 {
            0
        } else {
            self.started
                .elapsed()
                .as_millis()
                .saturating_sub((oldest_enqueued - 1) as u128)
                .min(u64::MAX as u128) as u64
        };
        self.queued_oldest_age_ms
            .store(queued_oldest_age_ms, Ordering::Relaxed);
        DeferredStorageMetricsSnapshot {
            enabled: self.enabled.load(Ordering::Relaxed),
            queued_items: self.queued_items.load(Ordering::Relaxed),
            queued_bytes: self.queued_bytes.load(Ordering::Relaxed),
            queued_keys: self.queued_keys.load(Ordering::Relaxed),
            queued_oldest_age_ms,
            accepted: self.accepted.load(Ordering::Relaxed),
            coalesced: self.coalesced.load(Ordering::Relaxed),
            rejected_full: self.rejected_full.load(Ordering::Relaxed),
            rejected_bytes: self.rejected_bytes.load(Ordering::Relaxed),
            committed: self.committed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            shutdown_drained: self.shutdown_drained.load(Ordering::Relaxed),
            shutdown_abandoned: self.shutdown_abandoned.load(Ordering::Relaxed),
            shutdown_abandoned_bytes: self.shutdown_abandoned_bytes.load(Ordering::Relaxed),
            backend_latency_total_ms: self.backend_latency_total_ms.load(Ordering::Relaxed),
            backend_latency_last_ms: self.backend_latency_last_ms.load(Ordering::Relaxed),
        }
    }
}

/// Receipt for volatile admission. `wait` resolves only after a repository transaction succeeds or fails.
#[derive(Clone)]
pub struct DeferredReceipt(Arc<ReceiptState>);
struct ReceiptState {
    result: watch::Sender<Option<Result<(), (ErrorCategory, String)>>>,
}
impl DeferredReceipt {
    async fn complete(&self, result: AppResult<()>) {
        let saved = result.map_err(|e| (e.category(), e.message().to_owned()));
        // `watch` retains the terminal value for receipts which start waiting
        // after completion, unlike a bare Notify which can lose that wake-up.
        self.0.result.send_replace(Some(saved));
    }
    pub async fn wait(&self) -> AppResult<()> {
        let mut result = self.0.result.subscribe();
        loop {
            if let Some(result) = result.borrow().clone() {
                return result.map_err(|(c, m)| AppError::new(c, m));
            }
            // A terminal result remains observable even if it is written
            // between `borrow` and `changed`, so this cannot strand a waiter.
            let _ = result.changed().await;
        }
    }
}
fn receipt() -> DeferredReceipt {
    let (result, _initial_receiver) = watch::channel(None);
    DeferredReceipt(Arc::new(ReceiptState { result }))
}

enum Operation {
    Write {
        accessor: Accessor,
        request: WriteRequest,
        membership: Option<StorageIndexMembership>,
    },
    Delete {
        accessor: Accessor,
        id: ObjectId,
    },
}
impl Operation {
    fn id(&self) -> &ObjectId {
        match self {
            Self::Write { request, .. } => &request.id,
            Self::Delete { id, .. } => id,
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::Write { request, .. } => {
                request.id.collection.as_str().len()
                    + request.id.key.as_str().len()
                    + request.value.as_json().to_string().len()
                    + 256
            }
            Self::Delete { id, .. } => id.collection.as_str().len() + id.key.as_str().len() + 128,
        }
    }
}
struct Pending {
    operation: Operation,
    bytes: usize,
    receipts: Vec<DeferredReceipt>,
    enqueued_at: Instant,
}
struct Queue {
    accepting: bool,
    items: usize,
    bytes: usize,
    keyed: HashMap<ObjectId, VecDeque<Pending>>,
    ready: VecDeque<ObjectId>,
}

/// Runtime-only writer with per-object FIFO and fair round-robin between object keys.
pub struct DeferredStorageWriter {
    config: DeferredStorageConfig,
    repository: Arc<dyn StorageRepository>,
    queue: Arc<Mutex<Queue>>,
    /// This is set before acquiring the queue lock.  It closes the small race
    /// between cancellation being observed and the queue being drained.
    closing: AtomicBool,
    /// Serializes explicit shutdown with the worker's cancellation path.
    shutdown_gate: Mutex<()>,
    /// Serializes repository transactions, preserving strict per-key order
    /// even when public shutdown begins while the worker is active.
    execution_gate: Mutex<()>,
    /// Published before shutdown waits for active work, allowing it to bound
    /// a transaction that was already in flight when shutdown started.
    shutdown_deadline: watch::Sender<Option<Instant>>,
    wake: Arc<Notify>,
    metrics: Arc<DeferredStorageMetrics>,
}
impl DeferredStorageWriter {
    /// Construct an enabled service. In-memory storage is rejected because admission must not be confused with durability.
    pub fn new(
        config: DeferredStorageConfig,
        repository: Arc<dyn StorageRepository>,
        backend: BackendKind,
    ) -> AppResult<Arc<Self>> {
        config.validate()?;
        if backend == BackendKind::InMemory {
            return Err(AppError::config(
                "storage.deferred requires a durable backend",
            ));
        }
        let metrics = Arc::new(DeferredStorageMetrics::default());
        metrics.enabled.store(config.enabled, Ordering::Relaxed);
        let (shutdown_deadline, _deadline_receiver) = watch::channel(None);
        Ok(Arc::new(Self {
            config,
            repository,
            queue: Arc::new(Mutex::new(Queue {
                accepting: true,
                items: 0,
                bytes: 0,
                keyed: HashMap::new(),
                ready: VecDeque::new(),
            })),
            closing: AtomicBool::new(false),
            shutdown_gate: Mutex::new(()),
            execution_gate: Mutex::new(()),
            shutdown_deadline,
            wake: Arc::new(Notify::new()),
            metrics,
        }))
    }
    pub fn metrics(&self) -> &Arc<DeferredStorageMetrics> {
        &self.metrics
    }
    fn allowed(&self, id: &ObjectId) -> bool {
        self.config.enabled
            && self
                .config
                .collections
                .iter()
                .any(|c| c == id.collection.as_str())
    }
    /// Admit an unconditional write. CAS/create-only calls must use the synchronous repository API.
    pub async fn enqueue_write(
        &self,
        accessor: Accessor,
        request: WriteRequest,
        membership: Option<StorageIndexMembership>,
    ) -> AppResult<DeferredReceipt> {
        if request.expected != Precondition::Any {
            return Err(AppError::validation(
                "deferred storage accepts only Precondition::Any; use synchronous storage for CAS",
            ));
        }
        self.enqueue(Operation::Write {
            accessor,
            request,
            membership,
        })
        .await
    }
    /// Admit an unconditional delete. Versioned deletes must use the synchronous repository API.
    pub async fn enqueue_delete(
        &self,
        accessor: Accessor,
        id: ObjectId,
        expected: Precondition,
    ) -> AppResult<DeferredReceipt> {
        if expected != Precondition::Any {
            return Err(AppError::validation(
                "deferred storage accepts only Precondition::Any; use synchronous storage for CAS",
            ));
        }
        self.enqueue(Operation::Delete { accessor, id }).await
    }
    async fn enqueue(&self, operation: Operation) -> AppResult<DeferredReceipt> {
        if !self.allowed(operation.id()) {
            return Err(AppError::validation(
                "deferred storage is disabled or collection is not allowlisted",
            ));
        }
        let bytes = operation.bytes();
        let id = operation.id().clone();
        let r = receipt();
        // The atomic is intentionally checked both before and after waiting for
        // the mutex: once shutdown starts no later caller may be admitted.
        if self.closing.load(Ordering::Acquire) {
            return Err(AppError::new(
                ErrorCategory::Cancelled,
                "deferred storage is shutting down",
            ));
        }
        let mut q = self.queue.lock().await;
        if self.closing.load(Ordering::Acquire) || !q.accepting {
            return Err(AppError::new(
                ErrorCategory::Cancelled,
                "deferred storage is shutting down",
            ));
        }
        // Same-key unconditional operations always coalesce; all admitted receipts resolve with the final transaction.
        if q.keyed.contains_key(&id) {
            let previous_bytes = q
                .keyed
                .get(&id)
                .and_then(|ops| ops.back())
                .map(|last| last.bytes)
                .expect("keyed queue must contain a final operation");
            let replacement_bytes = q
                .bytes
                .checked_sub(previous_bytes)
                .and_then(|remaining| remaining.checked_add(bytes))
                .ok_or_else(|| {
                    self.metrics.rejected_bytes.fetch_add(1, Ordering::Relaxed);
                    AppError::new(
                        ErrorCategory::Deadline,
                        "deferred storage queue byte accounting overflow; retry later",
                    )
                })?;
            if replacement_bytes > self.config.max_bytes {
                self.metrics.rejected_bytes.fetch_add(1, Ordering::Relaxed);
                return Err(AppError::new(
                    ErrorCategory::Deadline,
                    "deferred storage queue byte budget exceeded; retry later",
                ));
            }
            q.bytes = replacement_bytes;
            let last = q
                .keyed
                .get_mut(&id)
                .and_then(|ops| ops.back_mut())
                .expect("keyed queue must contain a final operation");
            last.operation = operation;
            last.bytes = bytes;
            last.receipts.push(r.clone());
            self.metrics.coalesced.fetch_add(1, Ordering::Relaxed);
            self.metrics.queued_bytes.store(q.bytes, Ordering::Relaxed);
            return Ok(r);
        }
        if q.items >= self.config.max_items {
            self.metrics.rejected_full.fetch_add(1, Ordering::Relaxed);
            return Err(AppError::new(
                ErrorCategory::Deadline,
                "deferred storage queue full; retry later",
            ));
        }
        if q.bytes
            .checked_add(bytes)
            .is_none_or(|total| total > self.config.max_bytes)
        {
            self.metrics.rejected_bytes.fetch_add(1, Ordering::Relaxed);
            return Err(AppError::new(
                ErrorCategory::Deadline,
                "deferred storage queue byte budget exceeded; retry later",
            ));
        }
        q.items = q.items.checked_add(1).ok_or_else(|| {
            self.metrics.rejected_full.fetch_add(1, Ordering::Relaxed);
            AppError::new(
                ErrorCategory::Deadline,
                "deferred storage queue item accounting overflow; retry later",
            )
        })?;
        q.bytes = q.bytes.checked_add(bytes).ok_or_else(|| {
            self.metrics.rejected_bytes.fetch_add(1, Ordering::Relaxed);
            AppError::new(
                ErrorCategory::Deadline,
                "deferred storage queue byte accounting overflow; retry later",
            )
        })?;
        q.keyed.entry(id.clone()).or_default().push_back(Pending {
            operation,
            bytes,
            receipts: vec![r.clone()],
            enqueued_at: Instant::now(),
        });
        q.ready.push_back(id);
        self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
        self.sync_queue_metrics(&q);
        drop(q);
        self.wake.notify_one();
        Ok(r)
    }
    fn sync_queue_metrics(&self, q: &Queue) {
        self.metrics.queued_items.store(q.items, Ordering::Relaxed);
        self.metrics.queued_bytes.store(q.bytes, Ordering::Relaxed);
        self.metrics
            .queued_keys
            .store(q.keyed.len(), Ordering::Relaxed);
        let oldest = q
            .keyed
            .values()
            .flat_map(|ops| ops.iter())
            .map(|p| p.enqueued_at)
            .min();
        self.metrics.queued_oldest_enqueued_ms.store(
            oldest.map_or(0, |at| {
                self.metrics
                    .started
                    .elapsed()
                    .saturating_sub(at.elapsed())
                    .as_millis()
                    .min(u64::MAX as u128) as u64
                    + 1
            }),
            Ordering::Relaxed,
        );
    }
    async fn take(&self) -> Option<Pending> {
        let mut q = self.queue.lock().await;
        let id = q.ready.pop_front()?;
        let (p, empty) = {
            let ops = q.keyed.get_mut(&id)?;
            let p = ops.pop_front()?;
            (p, ops.is_empty())
        };
        q.items = q
            .items
            .checked_sub(1)
            .expect("queue item accounting invariant");
        q.bytes = q
            .bytes
            .checked_sub(p.bytes)
            .expect("queue byte accounting invariant");
        if empty {
            q.keyed.remove(&id);
        } else {
            q.ready.push_back(id);
        }
        self.sync_queue_metrics(&q);
        Some(p)
    }
    async fn execute(&self, p: Pending) {
        let started = Instant::now();
        let mut deadline = self.shutdown_deadline.subscribe();
        let commit = async {
            match p.operation {
                Operation::Write {
                    accessor,
                    request,
                    membership,
                } => self
                    .repository
                    .write_indexed(&accessor, request, membership.as_ref())
                    .await
                    .map(|_| ()),
                Operation::Delete { accessor, id } => {
                    self.repository
                        .delete(&accessor, &id, Precondition::Any)
                        .await
                }
            }
        };
        tokio::pin!(commit);
        let result = loop {
            let limit = *deadline.borrow();
            if let Some(limit) = limit {
                match tokio::time::timeout(
                    limit.saturating_duration_since(Instant::now()),
                    &mut commit,
                )
                .await
                {
                    Ok(result) => break result,
                    Err(_) => {
                        break Err(AppError::new(
                            ErrorCategory::Cancelled,
                            "deferred storage shutdown deadline exceeded; commit outcome is unknown",
                        ));
                    }
                }
            }
            tokio::select! {
                result = &mut commit => break result,
                changed = deadline.changed() => {
                    if changed.is_err() {
                        break Err(AppError::new(ErrorCategory::Cancelled, "deferred storage shutdown coordinator stopped"));
                    }
                }
            }
        };
        let latency_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        self.metrics
            .backend_latency_last_ms
            .store(latency_ms, Ordering::Relaxed);
        self.metrics
            .backend_latency_total_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        if result.is_ok() {
            self.metrics
                .committed
                .fetch_add(p.receipts.len() as u64, Ordering::Relaxed);
        } else if result
            .as_ref()
            .is_err_and(|error| error.category() == ErrorCategory::Cancelled)
        {
            self.metrics
                .cancelled
                .fetch_add(p.receipts.len() as u64, Ordering::Relaxed);
        } else {
            self.metrics
                .failed
                .fetch_add(p.receipts.len() as u64, Ordering::Relaxed);
        }
        for r in p.receipts {
            r.complete(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|e| AppError::new(e.category(), e.message())),
            )
            .await;
        }
    }
    async fn execute_one(&self, p: Pending) {
        let _execution = self.execution_gate.lock().await;
        self.execute(p).await;
    }
    /// Run until shutdown. Cancellation starts the drain coordinator immediately,
    /// even when a repository transaction is currently blocked.
    pub async fn run(
        self: Arc<Self>,
        cancel: crate::lifecycle::CancellationToken,
    ) -> AppResult<()> {
        let shutdown_writer = Arc::clone(&self);
        let shutdown_cancel = cancel.clone();
        let coordinator = tokio::spawn(async move {
            shutdown_cancel.cancelled().await;
            shutdown_writer.shutdown().await
        });
        loop {
            for _ in 0..self.config.flush_batch_items {
                if self.closing.load(Ordering::Acquire) {
                    if cancel.is_cancelled() {
                        return coordinator.await.map_err(|error| {
                            AppError::new(ErrorCategory::Cancelled, error.to_string())
                        })?;
                    }
                    coordinator.abort();
                    return Ok(());
                }
                let Some(p) = self.take().await else {
                    break;
                };
                self.execute_one(p).await;
            }
            if self.closing.load(Ordering::Acquire) {
                if cancel.is_cancelled() {
                    return coordinator.await.map_err(|error| {
                        AppError::new(ErrorCategory::Cancelled, error.to_string())
                    })?;
                }
                coordinator.abort();
                return Ok(());
            }
            tokio::select! {
                _ = cancel.cancelled() => {
                    return coordinator
                        .await
                        .map_err(|error| AppError::new(ErrorCategory::Cancelled, error.to_string()))?;
                },
                _ = self.wake.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(self.config.flush_interval_ms)) => {},
            }
        }
    }
    pub async fn shutdown(&self) -> AppResult<()> {
        // Closing is a real admission barrier, not merely a best-effort flag on
        // the drain loop. Publish the deadline before waiting for the active
        // transaction, so `execute` can terminate an otherwise blocked commit.
        self.closing.store(true, Ordering::Release);
        let _shutdown = self.shutdown_gate.lock().await;
        let deadline =
            Instant::now() + Duration::from_millis(self.config.shutdown_drain_timeout_ms);
        self.shutdown_deadline.send_replace(Some(deadline));
        self.queue.lock().await.accepting = false;

        // There may be no queued work while exactly one operation is active.
        // Wait for that operation through the same deadline before declaring a
        // clean shutdown; execution itself converts a timed-out commit to the
        // terminal unknown-outcome receipt error.
        let active_finished = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            self.execution_gate.lock(),
        )
        .await;
        let deadline_expired = active_finished.is_err();
        drop(active_finished);

        while !deadline_expired && Instant::now() < deadline {
            let Some(p) = self.take().await else {
                return Ok(());
            };
            let n = p.receipts.len() as u64;
            self.execute_one(p).await;
            self.metrics
                .shutdown_drained
                .fetch_add(n, Ordering::Relaxed);
        }

        let (abandoned_receipts, abandoned_bytes) = {
            let mut q = self.queue.lock().await;
            let bytes = q.bytes as u64;
            let receipts = q
                .keyed
                .drain()
                .flat_map(|(_, ops)| ops.into_iter())
                .flat_map(|p| p.receipts.into_iter())
                .collect::<Vec<_>>();
            q.ready.clear();
            q.items = 0;
            q.bytes = 0;
            self.sync_queue_metrics(&q);
            (receipts, bytes)
        };
        let abandoned = abandoned_receipts.len() as u64;
        for r in abandoned_receipts {
            r.complete(Err(AppError::new(
                ErrorCategory::Cancelled,
                "deferred storage shutdown drain deadline exceeded",
            )))
            .await;
        }
        self.metrics
            .cancelled
            .fetch_add(abandoned, Ordering::Relaxed);
        self.metrics
            .shutdown_abandoned
            .fetch_add(abandoned, Ordering::Relaxed);
        self.metrics
            .shutdown_abandoned_bytes
            .fetch_add(abandoned_bytes, Ordering::Relaxed);
        if abandoned > 0 || abandoned_bytes > 0 || deadline_expired || Instant::now() >= deadline {
            Err(AppError::new(
                ErrorCategory::Deadline,
                "deferred storage shutdown drain deadline exceeded",
            ))
        } else {
            Ok(())
        }
    }
}

impl crate::lifecycle::AsyncService for Arc<DeferredStorageWriter> {
    fn name(&self) -> &str {
        "deferred-storage-writer"
    }

    async fn run(self: Box<Self>, cancel: crate::lifecycle::CancellationToken) -> AppResult<()> {
        (*self).run(cancel).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::InMemoryStorageRepository;
    use crate::storage::{
        Collection, CollectionSummary, Key, ListQuery, Owner, Permissions, StorageIndexDefinition,
        StorageIndexField, StorageIndexMembership, StorageIndexName, StorageIndexQuery,
        StorageObject, StorageValue,
    };

    struct ControlledRepository {
        inner: InMemoryStorageRepository,
        fail: bool,
        delay: Duration,
        started: Option<Arc<tokio::sync::Barrier>>,
    }
    #[async_trait::async_trait]
    impl StorageRepository for ControlledRepository {
        async fn read(&self, a: &Accessor, id: &ObjectId) -> AppResult<Option<StorageObject>> {
            self.inner.read(a, id).await
        }
        async fn write(&self, a: &Accessor, r: WriteRequest) -> AppResult<StorageObject> {
            self.inner.write(a, r).await
        }
        async fn write_indexed(
            &self,
            a: &Accessor,
            r: WriteRequest,
            m: Option<&StorageIndexMembership>,
        ) -> AppResult<StorageObject> {
            if let Some(started) = &self.started {
                started.wait().await;
            }
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if self.fail {
                return Err(AppError::database("injected deferred backend failure"));
            }
            self.inner.write_indexed(a, r, m).await
        }
        async fn delete(&self, a: &Accessor, id: &ObjectId, p: Precondition) -> AppResult<()> {
            self.inner.delete(a, id, p).await
        }
        async fn list(
            &self,
            a: &Accessor,
            q: &ListQuery,
        ) -> AppResult<crate::storage::Page<StorageObject>> {
            self.inner.list(a, q).await
        }
        async fn install_index(&self, i: &StorageIndexDefinition) -> AppResult<()> {
            self.inner.install_index(i).await
        }
        async fn query_index(
            &self,
            a: &Accessor,
            q: &StorageIndexQuery,
        ) -> AppResult<Vec<StorageObject>> {
            self.inner.query_index(a, q).await
        }
        async fn list_collections(&self) -> AppResult<Vec<CollectionSummary>> {
            self.inner.list_collections().await
        }
    }

    fn config() -> DeferredStorageConfig {
        DeferredStorageConfig {
            enabled: true,
            collections: vec!["hints".to_owned()],
            max_items: 8,
            max_bytes: 16 * 1024,
            flush_interval_ms: 1,
            flush_batch_items: 8,
            shutdown_drain_timeout_ms: 1_000,
        }
    }

    fn request(value: u64) -> WriteRequest {
        WriteRequest::upsert(
            ObjectId::new(
                Owner::System,
                Collection::new("hints").expect("valid collection"),
                Key::new("hot").expect("valid key"),
            ),
            StorageValue::new(serde_json::json!({"value": value})).expect("object value"),
            Permissions::runtime_only(),
        )
    }

    fn request_with_payload(value: u64, padding: usize) -> WriteRequest {
        let mut request = request(value);
        request.value = StorageValue::new(serde_json::json!({
            "value": value,
            "padding": "x".repeat(padding),
        }))
        .expect("object value");
        request
    }

    #[tokio::test]
    async fn deferred_indexed_write_then_delete_updates_the_index_atomically() {
        let repository = Arc::new(InMemoryStorageRepository::new());
        let index = StorageIndexDefinition::new(
            StorageIndexName::new("hints_by_value").expect("valid index name"),
            Collection::new("hints").expect("valid collection"),
            None,
            vec![StorageIndexField::new("value").expect("valid index field")],
        )
        .expect("valid index definition");
        repository
            .install_index(&index)
            .await
            .expect("install index");
        let membership =
            StorageIndexMembership::include_all([index.name().clone()].into_iter().collect());
        let filters = serde_json::json!({"value": 7});
        let query = StorageIndexQuery::from_json_filters(
            index,
            filters.as_object().expect("object filters"),
            10,
        )
        .expect("valid index query");
        let writer = DeferredStorageWriter::new(config(), repository.clone(), BackendKind::Sqlite)
            .expect("enabled deferred writer");
        let cancel = crate::lifecycle::CancellationToken::new();
        let worker = tokio::spawn(Arc::clone(&writer).run(cancel.clone()));

        writer
            .enqueue_write(Accessor::Runtime, request(7), Some(membership))
            .await
            .expect("admit indexed write")
            .wait()
            .await
            .expect("indexed write commits");
        let indexed = repository
            .query_index(&Accessor::Runtime, &query)
            .await
            .expect("query index after deferred write");
        assert_eq!(indexed.len(), 1);
        assert_eq!(indexed[0].id, request(7).id);

        writer
            .enqueue_delete(Accessor::Runtime, request(7).id, Precondition::Any)
            .await
            .expect("admit delete")
            .wait()
            .await
            .expect("delete commits");
        assert!(
            repository
                .read(&Accessor::Runtime, &request(7).id)
                .await
                .expect("read after deferred delete")
                .is_none(),
            "the base object is deleted with its projection"
        );
        assert!(
            repository
                .query_index(&Accessor::Runtime, &query)
                .await
                .expect("query index after deferred delete")
                .is_empty(),
            "the deleted object is not left in the index"
        );

        cancel.cancel();
        worker.await.expect("worker joins").expect("clean drain");
    }

    #[tokio::test]
    async fn same_key_200_saves_coalesce_and_enqueue_without_waiting_for_commit() {
        // A durable-kind test harness uses the contract-faithful repository so the
        // scheduler can be tested without a database server. Production rejects
        // BackendKind::InMemory at construction.
        let repository = Arc::new(InMemoryStorageRepository::new());
        let writer = DeferredStorageWriter::new(config(), repository.clone(), BackendKind::Sqlite)
            .expect("enabled deferred writer");
        let started = Instant::now();
        let mut receipts = Vec::new();
        for value in 0..200 {
            receipts.push(
                writer
                    .enqueue_write(Accessor::Runtime, request(value), None)
                    .await
                    .expect("bounded same-key admission"),
            );
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(writer.metrics().snapshot().queued_items, 1);
        assert_eq!(writer.metrics().snapshot().coalesced, 199);

        let cancel = crate::lifecycle::CancellationToken::new();
        let worker = tokio::spawn(Arc::clone(&writer).run(cancel.clone()));
        for receipt in receipts {
            receipt.wait().await.expect("final transaction commits");
        }
        cancel.cancel();
        worker.await.expect("worker joins").expect("clean drain");
        let stored = repository
            .read(
                &Accessor::Runtime,
                &ObjectId::new(
                    Owner::System,
                    Collection::new("hints").expect("valid collection"),
                    Key::new("hot").expect("valid key"),
                ),
            )
            .await
            .expect("read succeeds")
            .expect("object exists");
        assert_eq!(stored.value.as_json(), &serde_json::json!({"value": 199}));
    }

    #[tokio::test]
    async fn backend_failure_is_visible_only_on_the_flush_receipt_and_records_latency() {
        let repository = Arc::new(ControlledRepository {
            inner: InMemoryStorageRepository::new(),
            fail: true,
            delay: Duration::from_millis(2),
            started: None,
        });
        let writer = DeferredStorageWriter::new(config(), repository, BackendKind::Sqlite)
            .expect("test setup succeeds");
        let receipt = writer
            .enqueue_write(Accessor::Runtime, request(1), None)
            .await
            .expect("test setup succeeds");
        let cancel = crate::lifecycle::CancellationToken::new();
        let worker = tokio::spawn(Arc::clone(&writer).run(cancel.clone()));
        assert_eq!(
            receipt
                .wait()
                .await
                .expect_err("expected failure")
                .category(),
            ErrorCategory::Database
        );
        cancel.cancel();
        worker
            .await
            .expect("test setup succeeds")
            .expect("test setup succeeds");
        let metrics = writer.metrics().snapshot();
        assert_eq!(metrics.failed, 1);
        assert!(metrics.backend_latency_last_ms >= 2);
    }

    #[tokio::test]
    async fn shutdown_deadline_cancels_unstarted_work_and_documents_volatile_crash_loss() {
        let mut cfg = config();
        cfg.shutdown_drain_timeout_ms = 1;
        let repository = Arc::new(ControlledRepository {
            inner: InMemoryStorageRepository::new(),
            fail: false,
            delay: Duration::from_millis(20),
            started: None,
        });
        let writer = DeferredStorageWriter::new(cfg, repository, BackendKind::Sqlite)
            .expect("test setup succeeds");
        let first = writer
            .enqueue_write(Accessor::Runtime, request(1), None)
            .await
            .expect("test setup succeeds");
        // A second key cannot be coalesced into the in-flight transaction.
        let mut second = request(2);
        second.id.key = Key::new("other").expect("test setup succeeds");
        let second = writer
            .enqueue_write(Accessor::Runtime, second, None)
            .await
            .expect("test setup succeeds");
        assert!(writer.shutdown().await.is_err());
        assert_eq!(
            first
                .wait()
                .await
                .expect_err("in-flight receipt reaches the same deadline")
                .category(),
            ErrorCategory::Cancelled
        );
        assert_eq!(
            second
                .wait()
                .await
                .expect_err("expected failure")
                .category(),
            ErrorCategory::Cancelled
        );
        let metrics = writer.metrics().snapshot();
        assert_eq!(metrics.shutdown_abandoned, 1);
        assert!(metrics.shutdown_abandoned_bytes > 0);
        // This is the deterministic crash-loss model: without `run`/`shutdown`,
        // an accepted receipt remains incomplete and no durable write occurs.
    }

    #[tokio::test]
    async fn uniform_and_hot_key_200_saves_are_bounded_and_non_blocking() {
        let repository = Arc::new(InMemoryStorageRepository::new());
        let mut cfg = config();
        cfg.max_items = 256;
        cfg.max_bytes = 256 * 1024;
        let writer = DeferredStorageWriter::new(cfg, repository, BackendKind::Sqlite)
            .expect("test setup succeeds");
        let started = Instant::now();
        for value in 0..200 {
            let mut r = request(value);
            r.id.key = Key::new(format!("k{value}")).expect("test setup succeeds");
            writer
                .enqueue_write(Accessor::Runtime, r, None)
                .await
                .expect("test setup succeeds");
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(writer.metrics().snapshot().queued_items, 200);
    }

    #[tokio::test]
    async fn concurrent_admission_never_exceeds_the_atomic_byte_or_item_budget() {
        let repository = Arc::new(InMemoryStorageRepository::new());
        let mut cfg = config();
        cfg.max_items = 1;
        cfg.max_bytes = request(0).id.collection.as_str().len()
            + request(0).id.key.as_str().len()
            + request(0).value.as_json().to_string().len()
            + 256;
        let writer = DeferredStorageWriter::new(cfg, repository, BackendKind::Sqlite)
            .expect("test setup succeeds");
        let gate = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();
        for key in ["one", "two"] {
            let writer = Arc::clone(&writer);
            let gate = Arc::clone(&gate);
            tasks.push(tokio::spawn(async move {
                let mut write = request(1);
                write.id.key = Key::new(key).expect("valid key");
                gate.wait().await;
                writer.enqueue_write(Accessor::Runtime, write, None).await
            }));
        }
        gate.wait().await;
        let mut accepted = 0;
        for task in tasks {
            if task.await.expect("task joins").is_ok() {
                accepted += 1;
            }
        }
        assert_eq!(accepted, 1);
        assert_eq!(writer.metrics().snapshot().queued_items, 1);
    }

    #[tokio::test]
    async fn coalescing_reserves_replacement_bytes_without_losing_the_prior_operation() {
        let repository = Arc::new(InMemoryStorageRepository::new());
        let first_request = request(1);
        let mut cfg = config();
        cfg.max_bytes = first_request.id.collection.as_str().len()
            + first_request.id.key.as_str().len()
            + first_request.value.as_json().to_string().len()
            + 256;
        let writer = DeferredStorageWriter::new(cfg, repository.clone(), BackendKind::Sqlite)
            .expect("test setup succeeds");
        let first = writer
            .enqueue_write(Accessor::Runtime, first_request, None)
            .await
            .expect("first admission fits");
        let replacement = writer
            .enqueue_write(Accessor::Runtime, request_with_payload(2, 4096), None)
            .await;
        assert_eq!(
            replacement
                .err()
                .expect("larger coalesced replacement must respect byte bound")
                .category(),
            ErrorCategory::Deadline
        );
        let cancel = crate::lifecycle::CancellationToken::new();
        let worker = tokio::spawn(Arc::clone(&writer).run(cancel.clone()));
        first
            .wait()
            .await
            .expect("original operation remains queued");
        cancel.cancel();
        worker.await.expect("worker joins").expect("clean shutdown");
        let stored = repository
            .read(&Accessor::Runtime, &request(1).id)
            .await
            .expect("read succeeds")
            .expect("original object commits");
        assert_eq!(stored.value.as_json(), &serde_json::json!({"value": 1}));
    }

    #[tokio::test]
    async fn shutdown_is_an_admission_barrier_and_terminal_receipts_are_replayable() {
        let repository = Arc::new(InMemoryStorageRepository::new());
        let writer = DeferredStorageWriter::new(config(), repository, BackendKind::Sqlite)
            .expect("test setup succeeds");
        let receipt = writer
            .enqueue_write(Accessor::Runtime, request(1), None)
            .await
            .expect("admitted before shutdown");
        writer.shutdown().await.expect("queued work drains");
        // Waiting after the worker has completed is the Notify-race regression:
        // a receipt must retain its terminal result rather than waiting forever.
        receipt.wait().await.expect("terminal commit is retained");
        let late = writer
            .enqueue_write(Accessor::Runtime, request(2), None)
            .await;
        assert_eq!(
            late.err().expect("late admission is rejected").category(),
            ErrorCategory::Cancelled
        );
    }

    #[tokio::test]
    async fn supervisor_cancellation_publishes_deadline_during_a_slow_active_commit() {
        let started = Arc::new(tokio::sync::Barrier::new(2));
        let repository = Arc::new(ControlledRepository {
            inner: InMemoryStorageRepository::new(),
            fail: false,
            delay: Duration::from_millis(50),
            started: Some(Arc::clone(&started)),
        });
        let mut cfg = config();
        cfg.shutdown_drain_timeout_ms = 1;
        let writer = DeferredStorageWriter::new(cfg, repository, BackendKind::Sqlite)
            .expect("test setup succeeds");
        let receipt = writer
            .enqueue_write(Accessor::Runtime, request(1), None)
            .await
            .expect("first admitted");
        let cancel = crate::lifecycle::CancellationToken::new();
        let worker = tokio::spawn(Arc::clone(&writer).run(cancel.clone()));
        started.wait().await;
        cancel.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), receipt.wait())
                .await
                .expect("receipt reaches the published deadline")
                .expect_err("blocked commit has an unknown outcome")
                .category(),
            ErrorCategory::Cancelled
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), worker)
                .await
                .expect("supervisor shutdown cannot wait indefinitely")
                .expect("worker joins")
                .expect_err("deadline expiry is reported to the supervisor")
                .category(),
            ErrorCategory::Deadline
        );
    }

    #[tokio::test]
    async fn public_shutdown_waits_for_one_active_transaction_with_no_queued_work() {
        let started = Arc::new(tokio::sync::Barrier::new(2));
        let repository = Arc::new(ControlledRepository {
            inner: InMemoryStorageRepository::new(),
            fail: false,
            delay: Duration::from_millis(50),
            started: Some(Arc::clone(&started)),
        });
        let mut cfg = config();
        cfg.shutdown_drain_timeout_ms = 1;
        let writer = DeferredStorageWriter::new(cfg, repository, BackendKind::Sqlite)
            .expect("test setup succeeds");
        let receipt = writer
            .enqueue_write(Accessor::Runtime, request(1), None)
            .await
            .expect("first admitted");
        let cancel = crate::lifecycle::CancellationToken::new();
        let worker = tokio::spawn(Arc::clone(&writer).run(cancel.clone()));
        started.wait().await;
        assert_eq!(writer.metrics().snapshot().queued_items, 0);
        assert_eq!(
            writer
                .shutdown()
                .await
                .expect_err("active work consumes the drain deadline")
                .category(),
            ErrorCategory::Deadline
        );
        assert_eq!(
            receipt
                .wait()
                .await
                .expect_err("active timed-out commit has unknown outcome")
                .category(),
            ErrorCategory::Cancelled
        );
        cancel.cancel();
        worker.await.expect("worker joins").expect("worker exits");
    }

    #[tokio::test]
    async fn public_shutdown_cannot_overtake_an_in_flight_same_key_operation() {
        let started = Arc::new(tokio::sync::Barrier::new(2));
        let repository = Arc::new(ControlledRepository {
            inner: InMemoryStorageRepository::new(),
            fail: false,
            delay: Duration::from_millis(20),
            started: Some(Arc::clone(&started)),
        });
        let mut cfg = config();
        cfg.shutdown_drain_timeout_ms = 1;
        let writer = DeferredStorageWriter::new(cfg, repository, BackendKind::Sqlite)
            .expect("test setup succeeds");
        let first = writer
            .enqueue_write(Accessor::Runtime, request(1), None)
            .await
            .expect("first admitted");
        let cancel = crate::lifecycle::CancellationToken::new();
        let worker = tokio::spawn(Arc::clone(&writer).run(cancel.clone()));
        started.wait().await;
        // The first write has left the queue and is executing. The second must
        // wait behind it rather than be executed by public shutdown in parallel.
        let second = writer
            .enqueue_write(Accessor::Runtime, request(2), None)
            .await
            .expect("second admitted before shutdown barrier");
        assert_eq!(
            writer
                .shutdown()
                .await
                .expect_err("in-flight work exceeds strict deadline")
                .category(),
            ErrorCategory::Deadline
        );
        assert_eq!(
            first
                .wait()
                .await
                .expect_err("first is deadline-cancelled")
                .category(),
            ErrorCategory::Cancelled
        );
        assert_eq!(
            second
                .wait()
                .await
                .expect_err("second is never overtaken")
                .category(),
            ErrorCategory::Cancelled
        );
        cancel.cancel();
        worker.await.expect("worker joins").expect("worker exits");
    }

    #[tokio::test]
    async fn queued_age_is_measured_when_metrics_are_observed() {
        let repository = Arc::new(InMemoryStorageRepository::new());
        let writer = DeferredStorageWriter::new(config(), repository, BackendKind::Sqlite)
            .expect("test setup succeeds");
        writer
            .enqueue_write(Accessor::Runtime, request(1), None)
            .await
            .expect("admitted");
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(writer.metrics().snapshot().queued_oldest_age_ms >= 1);
        let status_metrics = serde_json::to_value(writer.metrics().snapshot())
            .expect("operator status metrics serialize");
        assert!(status_metrics["queued_items"].is_number());
    }

    #[tokio::test]
    async fn different_payloads_keep_fifo_per_key() {
        let repository = Arc::new(ControlledRepository {
            inner: InMemoryStorageRepository::new(),
            fail: false,
            delay: Duration::from_millis(20),
            started: None,
        });
        let writer = DeferredStorageWriter::new(config(), repository.clone(), BackendKind::Sqlite)
            .expect("test setup succeeds");
        let first = writer
            .enqueue_write(Accessor::Runtime, request(1), None)
            .await
            .expect("first admitted");
        let cancel = crate::lifecycle::CancellationToken::new();
        let worker = tokio::spawn(Arc::clone(&writer).run(cancel.clone()));
        tokio::time::sleep(Duration::from_millis(2)).await;
        let second = writer
            .enqueue_write(Accessor::Runtime, request(2), None)
            .await
            .expect("second admitted after first is in flight");
        first.wait().await.expect("first commits before second");
        second.wait().await.expect("second commits");
        cancel.cancel();
        worker.await.expect("worker joins").expect("clean shutdown");
        let stored = repository
            .inner
            .read(&Accessor::Runtime, &request(2).id)
            .await
            .expect("read succeeds")
            .expect("object exists");
        assert_eq!(stored.value.as_json(), &serde_json::json!({"value": 2}));
    }

    #[test]
    fn config_requires_allowlist_and_positive_bounds() {
        let mut invalid = config();
        invalid.collections.clear();
        assert!(invalid.validate().is_err());
        invalid.collections.push("hints".to_owned());
        invalid.max_bytes = 0;
        assert!(invalid.validate().is_err());
        assert!(
            toml::from_str::<DeferredStorageConfig>(
                "enabled = true\ncollections = [\"hints\"]\nmax_in_flight = 2\n"
            )
            .is_err()
        );
    }
}
