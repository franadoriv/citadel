//! Write-behind persistence for the log family.
//!
//! Enqueue is synchronous, bounded, and never blocks or awaits. Every producer
//! is a context where awaiting is impossible: a script host call holding a VM
//! lock, the `SliceState` mutex guard inside `close_one`, the `map_err` closure
//! that records an authorization refusal. Acknowledgement therefore means only
//! that the record entered a bounded in-memory queue — the same honest contract
//! [`DeferredStorageWriter`](crate::deferred_storage::DeferredStorageWriter)
//! states.
//!
//! The local incident journal is not part of this family. It stays a file on
//! disk (`citadel-errors.jsonl`) and is untouched by anything here.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::MissedTickBehavior;

use crate::authoritative_decision_telemetry::AuthoritativeDecisionRecorder;
use crate::authoritative_telemetry_slices::{TelemetrySlicePolicy, TelemetrySliceService};
use crate::config::LogsConfig;
use crate::error::AppResult;
use crate::ids::NodeIdentity;
use crate::lifecycle::{AsyncService, CancellationToken};
use crate::repository::{
    DurableAuditRepository, DurableAuditRow, DurableMatchLogRepository, DurableMatchRepository,
    DurableSliceRow, DurableTelemetrySliceRepository, MatchClose, MatchLogEntry, MatchOpen,
};
use crate::services::{AuditEntry, AuditLog, AuditSink};
use crate::time::{Clock, SystemClock, TimestampMillis};

/// Console read paths whose own trail entries never reach the durable store.
///
/// `src/http/console_api/mod.rs` records a `console.read` for every authorized
/// machine request, the reads of these routes included. In a bounded ring that
/// is harmless because it evicts; in a table a credential polling one of these
/// routes would write one row per poll, forever, and the rows it writes would
/// be the only thing it ever reads. Dropping them here rather than at the call
/// site keeps the decision in the one place that knows a durable sink exists.
pub const RING_ONLY_AUDIT_TARGETS: &[&str] = &[
    "/console/v1/audit",
    "/console/v1/logs",
    "/console/v1/matchlogs",
];

/// The audit action the central read extractor records.
const CONSOLE_READ_ACTION: &str = "console.read";

/// Shortest gap between two overflow warnings for one queue.
const OVERFLOW_WARN_INTERVAL_MS: u64 = 5_000;

/// One bounded producer queue.
///
/// Overflow drops the *oldest* record and bumps a counter. It never blocks a
/// game tick, never grows without bound, and never fails a console mutation:
/// losing the oldest line of a full queue is strictly better than stalling the
/// thread that wrote it.
#[derive(Debug)]
struct Queue<T> {
    name: &'static str,
    items: Mutex<VecDeque<T>>,
    capacity: usize,
    dropped: AtomicU64,
    last_warned_ms: AtomicU64,
}

impl<T: Clone> Queue<T> {
    fn new(name: &'static str, capacity: usize) -> Self {
        Self {
            name,
            items: Mutex::new(VecDeque::new()),
            capacity: capacity.max(1),
            dropped: AtomicU64::new(0),
            last_warned_ms: AtomicU64::new(0),
        }
    }

    /// Enqueue `item`, returning whether the queue has reached `batch` depth
    /// and the flush service should be woken early.
    fn push(&self, item: T, batch: usize) -> bool {
        let mut items = self.lock();
        let overflowed = items.len() >= self.capacity;
        if overflowed {
            items.pop_front();
        }
        items.push_back(item);
        let depth = items.len();
        drop(items);
        if overflowed {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            self.warn_overflow();
        }
        depth >= batch
    }

    /// Copy up to `limit` records from the front without removing them.
    ///
    /// Peek-then-commit is what makes a failed flush safe to retry: the records
    /// stay queued until the database has accepted them.
    fn peek(&self, limit: usize) -> Vec<T> {
        let items = self.lock();
        items.iter().take(limit.max(1)).cloned().collect()
    }

    /// Remove the `count` records a successful flush accepted.
    fn commit(&self, count: usize) {
        let mut items = self.lock();
        for _ in 0..count.min(items.len()) {
            items.pop_front();
        }
    }

    fn len(&self) -> usize {
        self.lock().len()
    }

    fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Warn at most once per [`OVERFLOW_WARN_INTERVAL_MS`]: a queue that is
    /// overflowing is overflowing every push, and a log line per dropped record
    /// would be its own denial of service.
    fn warn_overflow(&self) {
        let now_ms = SystemClock.now().unix_millis();
        let last = self.last_warned_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < OVERFLOW_WARN_INTERVAL_MS {
            return;
        }
        if self
            .last_warned_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        tracing::warn!(
            queue = self.name,
            capacity = self.capacity,
            dropped_total = self.dropped(),
            "durable log queue is full; dropping the oldest records"
        );
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<T>> {
        // The deque holds no cross-record invariant a panicking writer could
        // break halfway, so a poisoned lock is recovered rather than escalated.
        self.items.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Label-free counters suitable for a dashboard snapshot or a log line.
#[derive(Debug, Default)]
pub struct DurableLogMetrics {
    flushed: AtomicU64,
    flush_failures: AtomicU64,
    pruned: AtomicU64,
    abandoned_on_shutdown: AtomicUsize,
}

/// A point-in-time read of the writer's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableLogMetricsSnapshot {
    /// Records currently waiting in every queue.
    pub queued: usize,
    /// Records dropped by overflow since boot.
    pub dropped: u64,
    /// Records the database has accepted since boot.
    pub flushed: u64,
    /// Failed flush attempts. A failure leaves its batch queued for retry.
    pub flush_failures: u64,
    /// Rows removed by retention since boot.
    pub pruned: u64,
    /// Records still queued when the shutdown drain budget expired.
    pub abandoned_on_shutdown: usize,
}

/// The bounded, non-blocking front of durable logging.
#[derive(Debug)]
pub struct DurableLogWriter {
    identity: Arc<NodeIdentity>,
    config: LogsConfig,
    audit: Queue<DurableAuditRow>,
    slices: Queue<DurableSliceRow>,
    logs: Queue<MatchLogEntry>,
    opens: Queue<MatchOpen>,
    closes: Queue<MatchClose>,
    wake: Arc<Notify>,
    metrics: DurableLogMetrics,
}

impl DurableLogWriter {
    #[must_use]
    pub fn new(identity: Arc<NodeIdentity>, config: LogsConfig) -> Self {
        let audit = Queue::new("audit", config.audit.max_queue_items);
        let slices = Queue::new("telemetry_slices", config.telemetry_slices.max_queue_items);
        let logs = Queue::new("match_logs", config.match_logs.max_queue_items);
        let opens = Queue::new("match_opens", config.matches.max_queue_items);
        let closes = Queue::new("match_closes", config.matches.max_queue_items);
        Self {
            identity,
            config,
            audit,
            slices,
            logs,
            opens,
            closes,
            wake: Arc::new(Notify::new()),
            metrics: DurableLogMetrics::default(),
        }
    }

    /// The node/boot identity every minted id is stamped with.
    #[must_use]
    pub fn identity(&self) -> &Arc<NodeIdentity> {
        &self.identity
    }

    /// The resolved `[logs]` section this writer was built from.
    #[must_use]
    pub fn config(&self) -> &LogsConfig {
        &self.config
    }

    /// The early-flush signal. The flush service waits on it; producers fire it
    /// when a queue reaches `flush_batch_items`.
    #[must_use]
    pub fn wake(&self) -> &Arc<Notify> {
        &self.wake
    }

    /// Queue one console trail entry, minting its durable key.
    ///
    /// Returns whether the record was queued. A `console.read` of one of the
    /// [`RING_ONLY_AUDIT_TARGETS`] is deliberately not — see that constant.
    pub fn enqueue_audit(&self, entry: &AuditEntry, match_id: Option<&str>) -> bool {
        if entry.action == CONSOLE_READ_ACTION
            && RING_ONLY_AUDIT_TARGETS
                .iter()
                .any(|prefix| entry.target.starts_with(prefix))
        {
            return false;
        }
        let row = DurableAuditRow {
            audit_id: self.identity.mint("au1-", entry.time_unix_ms),
            node_id: self.identity.node_id().to_string(),
            match_id: match_id.map(str::to_string),
            entry: entry.clone(),
        };
        self.enqueue(&self.audit, row);
        true
    }

    /// Drain the queued trail into `repository` so a durable read sees it.
    ///
    /// The flush service still owns the periodic drain; this exists because the
    /// console reads the trail from the table while `AuditLog::record` can only
    /// enqueue. Without it an operator sees their own action a flush interval
    /// late — and on an `App` built without the transport supervisor, where no
    /// flush service runs at all, never. Reading the trail is a rare operator
    /// action, so the extra batch insert is bounded and off every hot path.
    ///
    /// Only the passes needed to clear the depth measured on entry are run, so
    /// a producer enqueueing concurrently cannot keep this loop alive.
    pub async fn flush_audit_into(
        &self,
        repository: &Arc<dyn DurableAuditRepository>,
    ) -> AppResult<usize> {
        let limit = self.config.flush_batch_items.max(1);
        let passes = self.audit.len().div_ceil(limit);
        let mut written = 0;
        for _ in 0..passes {
            let repository = Arc::clone(repository);
            let moved = drain(&self.audit, limit, |rows| async move {
                repository.append_batch(&rows).await
            })
            .await?;
            if moved == 0 {
                break;
            }
            written += moved;
        }
        Ok(written)
    }

    /// Queue one closed telemetry slice report.
    pub fn enqueue_slice(&self, row: DurableSliceRow) {
        self.enqueue(&self.slices, row);
    }

    /// Queue one script-written log line.
    pub fn enqueue_log(&self, entry: MatchLogEntry) {
        self.enqueue(&self.logs, entry);
    }

    /// Queue the birth of a match.
    pub fn enqueue_match_open(&self, open: MatchOpen) {
        self.enqueue(&self.opens, open);
    }

    /// Queue the end of a match.
    pub fn enqueue_match_close(&self, close: MatchClose) {
        self.enqueue(&self.closes, close);
    }

    /// Mint a durable id in this node's identity.
    #[must_use]
    pub fn mint(&self, prefix: &str, at_ms: u64) -> String {
        self.identity.mint(prefix, at_ms)
    }

    /// Records dropped by overflow since boot, across every queue. The console
    /// surfaces it so an operator can tell a quiet trail from a lossy one.
    #[must_use]
    pub fn dropped_total(&self) -> u64 {
        self.audit.dropped()
            + self.slices.dropped()
            + self.logs.dropped()
            + self.opens.dropped()
            + self.closes.dropped()
    }

    /// Records waiting in every queue.
    #[must_use]
    pub fn queued_total(&self) -> usize {
        self.audit.len()
            + self.slices.len()
            + self.logs.len()
            + self.opens.len()
            + self.closes.len()
    }

    #[must_use]
    pub fn metrics(&self) -> DurableLogMetricsSnapshot {
        DurableLogMetricsSnapshot {
            queued: self.queued_total(),
            dropped: self.dropped_total(),
            flushed: self.metrics.flushed.load(Ordering::Relaxed),
            flush_failures: self.metrics.flush_failures.load(Ordering::Relaxed),
            pruned: self.metrics.pruned.load(Ordering::Relaxed),
            abandoned_on_shutdown: self.metrics.abandoned_on_shutdown.load(Ordering::Relaxed),
        }
    }

    fn enqueue<T: Clone>(&self, queue: &Queue<T>, item: T) {
        if queue.push(item, self.config.flush_batch_items) {
            self.wake.notify_one();
        }
    }
}

/// The durable stores a flush writes to. Every one is optional: a backend
/// without the capability simply has nothing to flush for that domain.
#[derive(Clone, Default)]
pub struct DurableLogRepositories {
    pub matches: Option<Arc<dyn DurableMatchRepository>>,
    pub match_logs: Option<Arc<dyn DurableMatchLogRepository>>,
    pub telemetry_slices: Option<Arc<dyn DurableTelemetrySliceRepository>>,
    pub audit: Option<Arc<dyn DurableAuditRepository>>,
}

impl DurableLogRepositories {
    /// Whether any durable store is attached at all.
    #[must_use]
    pub fn any(&self) -> bool {
        self.matches.is_some()
            || self.match_logs.is_some()
            || self.telemetry_slices.is_some()
            || self.audit.is_some()
    }
}

impl std::fmt::Debug for DurableLogRepositories {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableLogRepositories")
            .field("matches", &self.matches.is_some())
            .field("match_logs", &self.match_logs.is_some())
            .field("telemetry_slices", &self.telemetry_slices.is_some())
            .field("audit", &self.audit.is_some())
            .finish()
    }
}

/// The supervised task that drains the queues, reaps expired slices, and runs
/// bounded retention.
pub struct DurableLogFlushService {
    writer: Arc<DurableLogWriter>,
    repositories: DurableLogRepositories,
    /// Closing expired slices here rather than only in the two console handlers
    /// is a correctness fix, not an optimization: `closed_at_ms` must be the
    /// time the TTL expired, not the time an operator happened to load a page.
    slices: Option<Arc<TelemetrySliceService>>,
    interval: Duration,
}

impl DurableLogFlushService {
    #[must_use]
    pub fn new(
        writer: Arc<DurableLogWriter>,
        repositories: DurableLogRepositories,
        slices: Option<Arc<TelemetrySliceService>>,
    ) -> Self {
        let interval = Duration::from_millis(writer.config().flush_interval_ms.max(1));
        Self {
            writer,
            repositories,
            slices,
            interval,
        }
    }

    /// Drain one batch per domain in the order that replaces the foreign key
    /// this schema deliberately does not have: a match row is written before
    /// anything referencing it, and its close lands after the rows it bounds.
    ///
    /// A domain that fails stops the pass. Everything still queued is retried
    /// on the next tick, so the ordering holds across retries too.
    async fn flush_once(&self) -> AppResult<usize> {
        let limit = self.writer.config().flush_batch_items;
        let mut written = 0;

        if let Some(repository) = self.repositories.matches.clone() {
            written += drain(&self.writer.opens, limit, |rows| async move {
                repository.open_batch(&rows).await
            })
            .await?;
        }
        if let Some(repository) = self.repositories.match_logs.clone() {
            written += drain(&self.writer.logs, limit, |rows| async move {
                repository.append_batch(&rows).await
            })
            .await?;
        }
        if let Some(repository) = self.repositories.telemetry_slices.clone() {
            written += drain(&self.writer.slices, limit, |rows| async move {
                repository.insert_batch(&rows).await
            })
            .await?;
        }
        if let Some(repository) = self.repositories.audit.clone() {
            written += drain(&self.writer.audit, limit, |rows| async move {
                repository.append_batch(&rows).await
            })
            .await?;
        }
        if let Some(repository) = self.repositories.matches.clone() {
            written += drain(&self.writer.closes, limit, |rows| async move {
                repository.close_batch(&rows).await
            })
            .await?;
        }

        if written > 0 {
            let accepted = u64::try_from(written).unwrap_or(u64::MAX);
            self.writer
                .metrics
                .flushed
                .fetch_add(accepted, Ordering::Relaxed);
        }
        Ok(written)
    }

    /// Bounded retention, oldest first, one batch per table per pass.
    ///
    /// No fenced lease: concurrent pruning across nodes is idempotent and
    /// merely duplicates work. The leaderboard reset lease exists because a
    /// reset mutates shared game state, which a bounded DELETE does not.
    async fn prune(&self, now_ms: u64) {
        let config = self.writer.config();
        let limit = config.prune_batch_limit;
        let mut removed = 0;

        if let Some(repository) = &self.repositories.match_logs {
            removed += pruned(
                "match_logs",
                repository
                    .prune(horizon(now_ms, config.match_logs.retention_days), limit)
                    .await,
            );
        }
        if let Some(repository) = &self.repositories.telemetry_slices {
            removed += pruned(
                "telemetry_slice_reports",
                repository
                    .prune(
                        horizon(now_ms, config.telemetry_slices.retention_days),
                        limit,
                    )
                    .await,
            );
        }
        if let Some(repository) = &self.repositories.audit {
            removed += pruned(
                "console_audit_entries",
                repository
                    .prune(horizon(now_ms, config.audit.retention_days), limit)
                    .await,
            );
        }
        // Last, and with the longest retention: nothing may reference a match
        // row that has already been deleted.
        if let Some(repository) = &self.repositories.matches {
            removed += pruned(
                "matches",
                repository
                    .prune(horizon(now_ms, config.matches.retention_days), limit)
                    .await,
            );
        }

        if removed > 0 {
            let total = u64::try_from(removed).unwrap_or(u64::MAX);
            self.writer
                .metrics
                .pruned
                .fetch_add(total, Ordering::Relaxed);
        }
    }

    /// Flush until the queues stop yielding rows or the shutdown budget runs
    /// out. Queued records live only in memory, so this is what keeps the last
    /// flush interval of the trail across a graceful stop.
    async fn drain_with_timeout(&self) {
        let budget = Duration::from_millis(self.writer.config().shutdown_drain_timeout_ms.max(1));
        let drain = async {
            loop {
                match self.flush_once().await {
                    Ok(0) => return,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "durable log shutdown drain failed; queued records are lost"
                        );
                        return;
                    }
                }
            }
        };
        if tokio::time::timeout(budget, drain).await.is_err() {
            tracing::warn!(
                timeout_ms = self.writer.config().shutdown_drain_timeout_ms,
                "durable log shutdown drain timed out"
            );
        }
        let abandoned = self.writer.queued_total();
        if abandoned > 0 {
            self.writer
                .metrics
                .abandoned_on_shutdown
                .store(abandoned, Ordering::Relaxed);
            tracing::warn!(abandoned, "durable log records abandoned at shutdown");
        }
    }
}

impl AsyncService for DurableLogFlushService {
    fn name(&self) -> &str {
        "durable-log-flush"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let prune_every = Duration::from_secs(self.writer.config().prune_interval_secs.max(1));
        let mut next_prune_ms = SystemClock.now().unix_millis();
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    self.drain_with_timeout().await;
                    return Ok(());
                }
                _ = ticker.tick() => {}
                () = self.writer.wake.notified() => {}
            }
            if let Err(error) = self.flush_once().await {
                self.writer
                    .metrics
                    .flush_failures
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    error = %error,
                    "durable log flush failed; records remain queued for retry"
                );
            }
            let now_ms = SystemClock.now().unix_millis();
            if let Some(slices) = &self.slices {
                slices.reap(now_ms);
            }
            if now_ms >= next_prune_ms {
                self.prune(now_ms).await;
                next_prune_ms = now_ms
                    .saturating_add(u64::try_from(prune_every.as_millis()).unwrap_or(u64::MAX));
            }
        }
    }
}

impl AuditSink for DurableLogWriter {
    /// Enqueue only: `AuditLog::record` is synchronous and is called from
    /// contexts that cannot await, so acknowledgement means the row entered the
    /// bounded queue and nothing more.
    ///
    /// The returned flag is discarded deliberately. `false` means only that a
    /// `console.read` of one of the [`RING_ONLY_AUDIT_TARGETS`] was dropped on
    /// purpose, which is the self-amplification rule working, not a failure.
    fn publish(&self, entry: &AuditEntry, match_id: Option<&str>) {
        let _queued = self.enqueue_audit(entry, match_id);
    }
}

/// Build the console action trail for this node.
///
/// The attach point for the durable audit sink lives here rather than in
/// `App::with_backend` so landing that sink touches this file and the audit
/// service, and not the shared assembly file every subsystem reads from.
#[must_use]
pub fn build_audit_log(capacity: usize, writer: Option<&Arc<DurableLogWriter>>) -> Arc<AuditLog> {
    let log = AuditLog::new(capacity);
    Arc::new(match writer {
        Some(writer) => log.with_sink(Arc::clone(writer) as Arc<dyn AuditSink>),
        None => log,
    })
}

/// Build the bounded telemetry-slice service for this node.
///
/// Same reason as [`build_audit_log`]. Note that a durable configuration must
/// never imply the subsystem is on: the service exists only when
/// `telemetry.authoritative_decisions.enabled` is set, and this is called only
/// from that branch.
#[must_use]
pub fn build_telemetry_slices(
    recorder: Arc<AuthoritativeDecisionRecorder>,
    policy: TelemetrySlicePolicy,
    identity: &NodeIdentity,
    writer: Option<&Arc<DurableLogWriter>>,
    directory: Option<&Arc<crate::match_recorder::MatchRecorder>>,
) -> Arc<TelemetrySliceService> {
    // `with_identity` is applied unconditionally inside `attach_durable_sink`:
    // the salt is what keeps a report id unique across reboots and nodes, and
    // that is true whether or not a durable sink is attached.
    Arc::new(crate::telemetry_slice_persistence::attach_durable_sink(
        TelemetrySliceService::new(recorder, policy),
        identity,
        writer,
        directory,
    ))
}

/// Peek a batch, apply it, and pop only what the database accepted.
///
/// Every insert in this family is idempotent, so a batch that failed after a
/// partial apply is safe to re-send whole on the next tick.
async fn drain<T, F, Fut>(queue: &Queue<T>, limit: usize, apply: F) -> AppResult<usize>
where
    T: Clone,
    F: FnOnce(Vec<T>) -> Fut,
    Fut: std::future::Future<Output = AppResult<usize>>,
{
    let batch = queue.peek(limit);
    if batch.is_empty() {
        return Ok(0);
    }
    let count = batch.len();
    apply(batch).await?;
    queue.commit(count);
    Ok(count)
}

fn horizon(now_ms: u64, retention_days: u32) -> TimestampMillis {
    TimestampMillis::from_unix_millis(LogsConfig::retention_horizon_ms(now_ms, retention_days))
}

/// Retention failures are reported, never fatal: the next pass retries and the
/// table is merely larger than its target in the meantime.
fn pruned(table: &'static str, outcome: AppResult<usize>) -> usize {
    match outcome {
        Ok(removed) => removed,
        Err(error) => {
            tracing::warn!(table, error = %error, "durable log retention pass failed");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repository::LogLevel;

    fn writer() -> DurableLogWriter {
        DurableLogWriter::new(Arc::new(NodeIdentity::new("node-a")), LogsConfig::default())
    }

    fn entry(action: &str, target: &str) -> AuditEntry {
        AuditEntry::new(
            TimestampMillis::from_unix_millis(1_700_000_000_000),
            "ops",
            "admin",
            action,
            target,
            "details",
        )
    }

    fn log_line(index: u64) -> MatchLogEntry {
        MatchLogEntry {
            log_id: format!("ml1-{index:029x}"),
            match_id: None,
            node_id: "node-a".to_string(),
            created_at_ms: index,
            level: LogLevel::Info,
            tag: "world".to_string(),
            message: format!("line {index}"),
            payload_json: None,
        }
    }

    #[test]
    fn an_overflowing_queue_drops_the_oldest_and_counts_it() {
        let queue = Queue::new("test", 2);
        queue.push(1, 128);
        queue.push(2, 128);
        queue.push(3, 128);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.dropped(), 1);
        assert_eq!(queue.peek(10), vec![2, 3], "the newest records survive");
    }

    #[test]
    fn a_peeked_batch_stays_queued_until_it_is_committed() {
        let queue = Queue::new("test", 8);
        queue.push(1, 128);
        queue.push(2, 128);
        let batch = queue.peek(2);
        assert_eq!(queue.len(), 2, "a failed flush leaves its batch queued");
        queue.commit(batch.len());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn reaching_the_batch_depth_asks_for_an_early_flush() {
        let queue = Queue::new("test", 8);
        assert!(!queue.push(1, 2));
        assert!(queue.push(2, 2));
    }

    #[test]
    fn reads_of_the_log_surfaces_never_become_durable_rows() {
        let writer = writer();
        // A machine credential polling the trail would otherwise write one row
        // per poll, and the rows it writes would be all it ever reads.
        assert!(!writer.enqueue_audit(&entry("console.read", "/console/v1/audit"), None));
        assert!(!writer.enqueue_audit(&entry("console.read", "/console/v1/logs/ml1-0"), None));
        assert!(!writer.enqueue_audit(&entry("console.read", "/console/v1/matchlogs"), None));
        assert_eq!(writer.queued_total(), 0);

        assert!(writer.enqueue_audit(&entry("console.read", "/console/v1/accounts"), None));
        // A mutation *of* the trail's own routes is not a read and is kept.
        assert!(writer.enqueue_audit(&entry("accounts.ban", "/console/v1/audit"), None));
        assert_eq!(writer.queued_total(), 2);
    }

    #[test]
    fn a_queued_audit_row_carries_a_minted_key_and_this_node() {
        let writer = writer();
        assert!(writer.enqueue_audit(&entry("accounts.ban", "user-1"), Some("mt1-abc")));
        let queued = writer.audit.peek(1);
        assert_eq!(queued.len(), 1);
        assert!(crate::ids::valid_id(
            &queued[0].audit_id,
            "au1-",
            crate::ids::SHORT_PREFIX_ID_LEN
        ));
        assert_eq!(queued[0].node_id, "node-a");
        assert_eq!(queued[0].match_id.as_deref(), Some("mt1-abc"));
    }

    #[tokio::test]
    async fn a_flush_with_no_repository_writes_nothing_and_keeps_the_queue() {
        let writer = Arc::new(writer());
        writer.enqueue_log(log_line(1));
        let service = DurableLogFlushService::new(
            Arc::clone(&writer),
            DurableLogRepositories::default(),
            None,
        );
        assert_eq!(service.flush_once().await.expect("flush"), 0);
        assert_eq!(
            writer.queued_total(),
            1,
            "a record is never dropped just because no store is attached"
        );
    }

    #[test]
    fn dropped_records_are_counted_across_every_queue() {
        let writer = DurableLogWriter::new(
            Arc::new(NodeIdentity::new("node-a")),
            LogsConfig {
                match_logs: crate::config::MatchLogsConfig {
                    max_queue_items: 1,
                    ..crate::config::MatchLogsConfig::default()
                },
                ..LogsConfig::default()
            },
        );
        writer.enqueue_log(log_line(1));
        writer.enqueue_log(log_line(2));
        assert_eq!(writer.queued_total(), 1);
        assert_eq!(writer.dropped_total(), 1);
        assert_eq!(writer.metrics().dropped, 1);
    }

    #[test]
    fn a_retention_horizon_never_underflows_before_the_epoch() {
        assert_eq!(LogsConfig::retention_horizon_ms(0, 30), 0);
        assert_eq!(
            LogsConfig::retention_horizon_ms(86_400_000 * 31, 30),
            86_400_000
        );
    }
}
