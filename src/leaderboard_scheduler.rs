//! Durable leaderboard-reset scheduler domain primitives.
//!
//! Persistence, lease acquisition, and callback delivery will be implemented by
//! repository adapters. These value types define the fence that every such
//! operation must carry, preventing a stale scheduler worker from committing a
//! rollover after its lease has expired or been replaced.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cron::Schedule;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::repository::{LeaderboardRecord, LeaderboardsRepository};
use crate::time::{Clock, DurationMillis, TimestampMillis};

/// A monotonically increasing token assigned when scheduler authority changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SchedulerFencingToken(u64);

impl SchedulerFencingToken {
    /// Construct a fencing token.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the stored token value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The currently authorized scheduler worker and its bounded lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerLease {
    /// The node holding the lease.
    pub node_id: String,
    /// The fencing token that must accompany work performed under this lease.
    pub fencing_token: SchedulerFencingToken,
    /// First instant at which this lease is no longer valid.
    pub expires_at: TimestampMillis,
}

impl SchedulerLease {
    /// Construct a scheduler lease.
    #[must_use]
    pub fn new(
        node_id: String,
        fencing_token: SchedulerFencingToken,
        expires_at: TimestampMillis,
    ) -> Self {
        Self {
            node_id,
            fencing_token,
            expires_at,
        }
    }

    /// Whether this lease is current at `now`.
    #[must_use]
    pub fn is_current_at(&self, now: TimestampMillis) -> bool {
        now < self.expires_at
    }

    /// Whether a mutation carrying `token` may execute at `now`.
    #[must_use]
    pub fn accepts(&self, token: SchedulerFencingToken, now: TimestampMillis) -> bool {
        self.is_current_at(now) && token == self.fencing_token
    }
}

/// A single scheduled reset occurrence, uniquely identified by board and due time.
///
/// Repository adapters persist this tuple under a unique constraint so a retry or
/// a replacement scheduler node can observe that the rollover was already claimed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ResetEpoch {
    /// The leaderboard being reset.
    pub leaderboard_id: String,
    /// UTC instant at which this schedule occurrence became due.
    pub due_at: TimestampMillis,
}

impl ResetEpoch {
    /// Construct an epoch identity.
    #[must_use]
    pub fn new(leaderboard_id: String, due_at: TimestampMillis) -> Self {
        Self {
            leaderboard_id,
            due_at,
        }
    }
}

/// A durable callback request staged with the reset epoch in the same transition.
///
/// Consumers must acknowledge this item only after their callback completes. The
/// epoch identity makes retries idempotent while the token prevents an old worker
/// from producing a newer owner's callback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResetOutboxRecord {
    /// The unique reset occurrence that was committed.
    pub epoch: ResetEpoch,
    /// The lease authority that staged this callback.
    pub fencing_token: SchedulerFencingToken,
}

/// An immutable copy of the records that existed immediately before one reset.
///
/// The records belong to the epoch rather than to a mutable current board. This
/// makes historical results available after the live board has been cleared.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LeaderboardResetSnapshot {
    /// The unique reset occurrence that produced this snapshot.
    pub epoch: ResetEpoch,
    /// Records as they existed at the atomic rollover boundary.
    pub records: Vec<LeaderboardRecord>,
}

/// Atomic persistence boundary for the reset scheduler.
///
/// Durable implementations must make `claim_epoch` fence the active lease, copy
/// the current board records into an immutable epoch snapshot, clear the live
/// records, and insert the epoch/outbox row in one transaction. The snapshot and
/// deletion are deliberately part of the claim: staging an outbox separately
/// would allow a crash to publish a reset without a historical result (or clear a
/// board without deduplication). Backends that cannot transact across scheduler
/// and leaderboard storage must reject `claim_epoch`; they must not stage a
/// callback-only epoch.
#[async_trait]
pub trait LeaderboardResetRepository: Send + Sync {
    /// Acquire or renew this node's lease. A live lease held by another node
    /// returns `None`; taking an expired lease increments its fencing token.
    async fn acquire_lease(
        &self,
        node_id: &str,
        now: TimestampMillis,
        ttl: DurationMillis,
    ) -> AppResult<Option<SchedulerLease>>;

    /// Atomically fence, snapshot, clear, and record an epoch plus its callback
    /// outbox item under `token`. Returns `false` when the epoch was already
    /// committed, in which case the existing snapshot is preserved.
    async fn claim_epoch(
        &self,
        epoch: ResetEpoch,
        token: SchedulerFencingToken,
        now: TimestampMillis,
    ) -> AppResult<bool>;

    /// Return the immutable pre-reset records for a committed epoch.
    ///
    /// The default is an explicit unsupported boundary for adapters that have not
    /// implemented atomic rollover; such adapters must also reject `claim_epoch`.
    async fn snapshot(&self, epoch: &ResetEpoch) -> AppResult<Option<LeaderboardResetSnapshot>> {
        let _ = epoch;
        Err(AppError::internal(
            "atomic leaderboard rollover snapshots are not supported by this backend",
        ))
    }

    /// Read at most `limit` unacknowledged callback requests.
    async fn pending_outbox(&self, limit: usize) -> AppResult<Vec<ResetOutboxRecord>>;

    /// Mark one callback request delivered. This is idempotent.
    async fn acknowledge_outbox(&self, epoch: &ResetEpoch) -> AppResult<()>;
}

/// Consumer invoked for each committed leaderboard reset callback request.
///
/// Returning an error leaves the outbox record unacknowledged so a later bounded
/// dispatch pass can retry the same epoch. Callback implementations must treat
/// the epoch identity as their idempotency key.
#[async_trait]
pub trait LeaderboardResetCallback: Send + Sync {
    /// Deliver one reset occurrence to the runtime callback host.
    async fn on_leaderboard_reset(
        &self,
        epoch: &ResetEpoch,
        fencing_token: SchedulerFencingToken,
    ) -> AppResult<()>;
}

/// Adapter that delivers durable reset occurrences to the loaded game runtime.
///
/// The runtime owns language-specific locking, deadlines, and error isolation;
/// errors are deliberately returned so the outbox dispatcher leaves failed
/// records durable for retry.
#[derive(Clone)]
pub struct RuntimeLeaderboardResetCallback {
    runtime: Arc<dyn crate::runtime::Runtime>,
}

impl RuntimeLeaderboardResetCallback {
    /// Build a callback bridge over the currently loaded runtime.
    #[must_use]
    pub fn new<R>(runtime: Arc<R>) -> Self
    where
        R: crate::runtime::Runtime,
    {
        Self { runtime }
    }

    /// Build a callback bridge from an erased runtime handle.
    #[must_use]
    pub fn from_runtime(runtime: Arc<dyn crate::runtime::Runtime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl LeaderboardResetCallback for RuntimeLeaderboardResetCallback {
    async fn on_leaderboard_reset(
        &self,
        epoch: &ResetEpoch,
        fencing_token: SchedulerFencingToken,
    ) -> AppResult<()> {
        self.runtime.on_leaderboard_reset(epoch, fencing_token)
    }
}

/// Bounded outbox delivery seam between the scheduler repository and a runtime.
///
/// This intentionally owns no persistence. Production database adapters remain
/// responsible for durable lease, epoch, and outbox transitions through
/// [`LeaderboardResetRepository`].
#[derive(Clone)]
pub struct LeaderboardResetOutboxDispatcher {
    repository: Arc<dyn LeaderboardResetRepository>,
    callback: Arc<dyn LeaderboardResetCallback>,
}

impl LeaderboardResetOutboxDispatcher {
    /// Create a dispatcher over one repository and one callback host.
    #[must_use]
    pub fn new<R, C>(repository: Arc<R>, callback: Arc<C>) -> Self
    where
        R: LeaderboardResetRepository + 'static,
        C: LeaderboardResetCallback + 'static,
    {
        Self {
            repository,
            callback,
        }
    }

    /// Deliver up to `limit` pending records and acknowledge only successful callbacks.
    ///
    /// A callback failure is logged and deliberately retained for the next pass;
    /// one bad record does not prevent later records from being attempted.
    pub async fn dispatch_pending(&self, limit: usize) -> AppResult<usize> {
        let records = self.repository.pending_outbox(limit).await?;
        let mut delivered = 0;
        for record in records {
            match self
                .callback
                .on_leaderboard_reset(&record.epoch, record.fencing_token)
                .await
            {
                Ok(()) => {
                    self.repository.acknowledge_outbox(&record.epoch).await?;
                    delivered += 1;
                }
                Err(error) => tracing::warn!(
                    leaderboard_id = %record.epoch.leaderboard_id,
                    due_at_unix_ms = record.epoch.due_at.unix_millis(),
                    error = %error,
                    "leaderboard reset callback failed; retaining outbox record for retry"
                ),
            }
        }
        Ok(delivered)
    }
}

#[derive(Debug, Default)]
struct InMemoryResetState {
    lease: Option<SchedulerLease>,
    epochs: BTreeSet<ResetEpoch>,
    snapshots: BTreeMap<ResetEpoch, LeaderboardResetSnapshot>,
    outbox: BTreeMap<ResetEpoch, ResetOutboxRecord>,
}

/// Reference implementation of [`LeaderboardResetRepository`].
///
/// It is deliberately volatile, but serializes each lease/epoch/outbox transition
/// under one mutex so tests exercise the same atomic contract durable adapters
/// must provide.
#[derive(Debug, Default)]
pub struct InMemoryLeaderboardResetRepository {
    state: Mutex<InMemoryResetState>,
}

impl InMemoryLeaderboardResetRepository {
    /// Construct an empty scheduler store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> AppResult<std::sync::MutexGuard<'_, InMemoryResetState>> {
        self.state
            .lock()
            .map_err(|_| AppError::internal("leaderboard reset repository mutex poisoned"))
    }
}

#[async_trait]
impl LeaderboardResetRepository for InMemoryLeaderboardResetRepository {
    async fn acquire_lease(
        &self,
        node_id: &str,
        now: TimestampMillis,
        ttl: DurationMillis,
    ) -> AppResult<Option<SchedulerLease>> {
        let expires_at = now.checked_add(ttl)?;
        let mut state = self.state()?;
        match state.lease.as_ref() {
            Some(lease) if lease.is_current_at(now) && lease.node_id != node_id => Ok(None),
            Some(lease) if lease.is_current_at(now) => {
                let renewed =
                    SchedulerLease::new(lease.node_id.clone(), lease.fencing_token, expires_at);
                state.lease = Some(renewed.clone());
                Ok(Some(renewed))
            }
            Some(lease) => {
                let next = lease
                    .fencing_token
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| AppError::internal("scheduler fencing token overflowed"))?;
                let acquired = SchedulerLease::new(
                    node_id.to_string(),
                    SchedulerFencingToken::new(next),
                    expires_at,
                );
                state.lease = Some(acquired.clone());
                Ok(Some(acquired))
            }
            None => {
                let acquired = SchedulerLease::new(
                    node_id.to_string(),
                    SchedulerFencingToken::new(1),
                    expires_at,
                );
                state.lease = Some(acquired.clone());
                Ok(Some(acquired))
            }
        }
    }

    async fn claim_epoch(
        &self,
        epoch: ResetEpoch,
        token: SchedulerFencingToken,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let mut state = self.state()?;
        let lease = state
            .lease
            .as_ref()
            .ok_or_else(|| AppError::conflict("scheduler lease is not held"))?;
        if !lease.accepts(token, now) {
            return Err(AppError::conflict("scheduler lease is no longer current"));
        }
        if !state.epochs.insert(epoch.clone()) {
            return Ok(false);
        }
        state.snapshots.insert(
            epoch.clone(),
            LeaderboardResetSnapshot {
                epoch: epoch.clone(),
                records: Vec::new(),
            },
        );
        state.outbox.insert(
            epoch.clone(),
            ResetOutboxRecord {
                epoch,
                fencing_token: token,
            },
        );
        Ok(true)
    }

    async fn snapshot(&self, epoch: &ResetEpoch) -> AppResult<Option<LeaderboardResetSnapshot>> {
        Ok(self.state()?.snapshots.get(epoch).cloned())
    }

    async fn pending_outbox(&self, limit: usize) -> AppResult<Vec<ResetOutboxRecord>> {
        Ok(self.state()?.outbox.values().take(limit).cloned().collect())
    }

    async fn acknowledge_outbox(&self, epoch: &ResetEpoch) -> AppResult<()> {
        self.state()?.outbox.remove(epoch);
        Ok(())
    }
}

/// Result of one bounded scheduler execution pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderboardResetRun {
    /// The lease token used for claimed work, or zero when another node held it.
    pub fencing_token: SchedulerFencingToken,
    /// Epochs committed in this pass, in caller-supplied due order.
    pub claimed_epochs: Vec<ResetEpoch>,
}

/// Execution boundary that acquires authority before staging reset callbacks.
#[derive(Clone)]
pub struct LeaderboardResetWorker {
    repository: Arc<dyn LeaderboardResetRepository>,
    node_id: String,
    lease_ttl: DurationMillis,
    max_catch_up: usize,
}

impl LeaderboardResetWorker {
    /// Create a worker with an explicit, bounded catch-up budget.
    #[must_use]
    pub fn new<R>(
        repository: Arc<R>,
        node_id: String,
        lease_ttl: DurationMillis,
        max_catch_up: usize,
    ) -> Self
    where
        R: LeaderboardResetRepository + 'static,
    {
        Self {
            repository,
            node_id,
            lease_ttl,
            max_catch_up,
        }
    }

    /// Acquire a lease and stage at most `max_catch_up` due reset callbacks.
    ///
    /// A currently leased peer produces a successful no-op; callers should retry
    /// at their normal scheduler cadence rather than treating that as an error.
    pub async fn run(
        &self,
        now: TimestampMillis,
        due_epochs: &[ResetEpoch],
    ) -> AppResult<LeaderboardResetRun> {
        let Some(lease) = self
            .repository
            .acquire_lease(&self.node_id, now, self.lease_ttl)
            .await?
        else {
            return Ok(LeaderboardResetRun {
                fencing_token: SchedulerFencingToken::new(0),
                claimed_epochs: Vec::new(),
            });
        };
        let mut claimed_epochs = Vec::new();
        for epoch in due_epochs {
            if claimed_epochs.len() == self.max_catch_up {
                break;
            }
            if self
                .repository
                .claim_epoch(epoch.clone(), lease.fencing_token, now)
                .await?
            {
                claimed_epochs.push(epoch.clone());
            }
        }
        Ok(LeaderboardResetRun {
            fencing_token: lease.fencing_token,
            claimed_epochs,
        })
    }
}

/// Result of one scheduler pass after schedule discovery and outbox dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderboardResetSchedulerRun {
    /// Epochs durably claimed during this pass.
    pub claimed_epochs: Vec<ResetEpoch>,
    /// Callback records acknowledged during this pass.
    pub delivered_callbacks: usize,
}

/// Runtime-facing composition of schedule discovery, fenced reset claiming, and
/// bounded callback delivery.
///
/// `run_once` is intentionally clock-injected: the supervised loop supplies the
/// system clock, while tests and embedders can make the catch-up window explicit.
#[derive(Clone)]
pub struct LeaderboardResetSchedulerService {
    leaderboards: Arc<dyn LeaderboardsRepository>,
    worker: LeaderboardResetWorker,
    dispatcher: LeaderboardResetOutboxDispatcher,
    outbox_limit: usize,
}

impl LeaderboardResetSchedulerService {
    /// Construct one scheduler composition for one node.
    #[must_use]
    pub fn new<L, R, C>(
        leaderboards: Arc<L>,
        repository: Arc<R>,
        callback: Arc<C>,
        node_id: String,
        lease_ttl: DurationMillis,
        max_catch_up: usize,
        outbox_limit: usize,
    ) -> Self
    where
        L: LeaderboardsRepository + 'static,
        R: LeaderboardResetRepository + 'static,
        C: LeaderboardResetCallback + 'static,
    {
        let worker =
            LeaderboardResetWorker::new(Arc::clone(&repository), node_id, lease_ttl, max_catch_up);
        let dispatcher = LeaderboardResetOutboxDispatcher::new(repository, callback);
        Self {
            leaderboards,
            worker,
            dispatcher,
            outbox_limit,
        }
    }

    /// Construct from backend-erased repository and callback handles.
    #[must_use]
    pub fn from_repositories(
        leaderboards: Arc<dyn LeaderboardsRepository>,
        repository: Arc<dyn LeaderboardResetRepository>,
        callback: Arc<dyn LeaderboardResetCallback>,
        node_id: String,
        lease_ttl: DurationMillis,
        max_catch_up: usize,
        outbox_limit: usize,
    ) -> Self {
        Self {
            leaderboards,
            worker: LeaderboardResetWorker {
                repository: Arc::clone(&repository),
                node_id,
                lease_ttl,
                max_catch_up,
            },
            dispatcher: LeaderboardResetOutboxDispatcher {
                repository,
                callback,
            },
            outbox_limit,
        }
    }

    /// Discover occurrences in `[catch_up_since, now]`, claim at most the
    /// worker's configured catch-up budget, then deliver a bounded outbox page.
    pub async fn run_once(
        &self,
        now: TimestampMillis,
        catch_up_since: TimestampMillis,
    ) -> AppResult<LeaderboardResetSchedulerRun> {
        let due_epochs = due_reset_epochs(self.leaderboards.as_ref(), catch_up_since, now).await?;
        let run = self.worker.run(now, &due_epochs).await?;
        // Keep delivery bounded independently from reset claiming. A lost lease
        // does not prevent retrying callbacks that were committed by this or a
        // previous healthy owner.
        let delivered_callbacks = self.dispatcher.dispatch_pending(self.outbox_limit).await?;
        Ok(LeaderboardResetSchedulerRun {
            claimed_epochs: run.claimed_epochs,
            delivered_callbacks,
        })
    }
}

/// Supervised cadence for the singleton leaderboard-reset scheduler.
///
/// The loop runs once at startup and thereafter at the supplied interval. Each
/// pass uses its previous successful clock sample as the next catch-up boundary,
/// while durable epoch claims make overlapping or retried passes safe.
pub struct LeaderboardResetSchedulerLoop {
    service: LeaderboardResetSchedulerService,
    interval: Duration,
}

impl LeaderboardResetSchedulerLoop {
    /// Construct a supervised scheduler loop with an explicit operator-owned cadence.
    #[must_use]
    pub fn new(service: LeaderboardResetSchedulerService, interval: Duration) -> Self {
        Self { service, interval }
    }
}

impl crate::lifecycle::AsyncService for LeaderboardResetSchedulerLoop {
    fn name(&self) -> &str {
        "leaderboard-reset-scheduler"
    }

    async fn run(self: Box<Self>, cancel: crate::lifecycle::CancellationToken) -> AppResult<()> {
        let mut last_run = crate::time::SystemClock.now();
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let now = crate::time::SystemClock.now();
                    self.service.run_once(now, last_run).await?;
                    last_run = now;
                }
            }
        }
    }
}

async fn due_reset_epochs(
    leaderboards: &dyn LeaderboardsRepository,
    catch_up_since: TimestampMillis,
    now: TimestampMillis,
) -> AppResult<Vec<ResetEpoch>> {
    let start = timestamp_as_utc(catch_up_since)?;
    let end = timestamp_as_utc(now)?;
    let mut epochs = Vec::new();
    for summary in leaderboards.list().await? {
        let Some(expression) = summary.definition.reset_schedule else {
            continue;
        };
        let schedule = expression
            .parse::<Schedule>()
            .map_err(|_| AppError::internal("persisted leaderboard reset schedule is invalid"))?;
        for occurrence in schedule
            .after(&start)
            .take_while(|occurrence| *occurrence <= end)
        {
            let millis = u64::try_from(occurrence.timestamp_millis()).map_err(|_| {
                AppError::internal("leaderboard reset schedule occurrence predates Unix epoch")
            })?;
            epochs.push(ResetEpoch::new(
                summary.definition.id.clone(),
                TimestampMillis::from_unix_millis(millis),
            ));
        }
    }
    epochs.sort();
    Ok(epochs)
}

fn timestamp_as_utc(timestamp: TimestampMillis) -> AppResult<DateTime<Utc>> {
    let millis = i64::try_from(timestamp.unix_millis())
        .map_err(|_| AppError::internal("timestamp is outside CRON range"))?;
    DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| AppError::internal("timestamp is outside CRON range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_epoch_identity_is_stable_per_board_and_occurrence() {
        let first = ResetEpoch::new(
            "daily".to_string(),
            TimestampMillis::from_unix_millis(60_000),
        );
        let same = ResetEpoch::new(
            "daily".to_string(),
            TimestampMillis::from_unix_millis(60_000),
        );
        let next = ResetEpoch::new(
            "daily".to_string(),
            TimestampMillis::from_unix_millis(120_000),
        );
        assert_eq!(first, same);
        assert_ne!(first, next);
    }

    #[tokio::test]
    async fn worker_fences_stale_nodes_deduplicates_epochs_and_bounds_catch_up() {
        use std::sync::Arc;

        use crate::time::DurationMillis;

        let repository = Arc::new(InMemoryLeaderboardResetRepository::new());
        let first = ResetEpoch::new("daily".to_string(), TimestampMillis::from_unix_millis(10));
        let second = ResetEpoch::new("daily".to_string(), TimestampMillis::from_unix_millis(20));
        let worker_a = LeaderboardResetWorker::new(
            Arc::clone(&repository),
            "node-a".to_string(),
            DurationMillis::from_millis(10),
            1,
        );
        let worker_b = LeaderboardResetWorker::new(
            Arc::clone(&repository),
            "node-b".to_string(),
            DurationMillis::from_millis(10),
            1,
        );

        let first_run = worker_a
            .run(
                TimestampMillis::from_unix_millis(0),
                &[first.clone(), second.clone()],
            )
            .await
            .expect("node a holds the initial lease");
        assert_eq!(first_run.claimed_epochs, vec![first.clone()]);
        assert_eq!(first_run.fencing_token, SchedulerFencingToken::new(1));
        assert_eq!(
            repository.pending_outbox(10).await.expect("outbox").len(),
            1
        );

        assert!(
            worker_b
                .run(
                    TimestampMillis::from_unix_millis(5),
                    std::slice::from_ref(&first)
                )
                .await
                .expect("another node's lease is a normal no-op")
                .claimed_epochs
                .is_empty()
        );

        let failover = worker_b
            .run(
                TimestampMillis::from_unix_millis(10),
                &[first, second.clone()],
            )
            .await
            .expect("expired lease may fail over");
        assert_eq!(failover.fencing_token, SchedulerFencingToken::new(2));
        assert_eq!(failover.claimed_epochs, vec![second]);
        assert_eq!(
            repository.pending_outbox(10).await.expect("outbox").len(),
            2
        );
    }
}
