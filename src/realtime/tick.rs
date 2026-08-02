//! Server game-loop tick service.
//!
//! When a game script registers `citadel.on_tick` and `runtime.tick_hz > 0`, the
//! bootstrap layer spawns a [`LuaTickService`] on the transport [`Supervisor`].
//! It fires a periodic timer and, on each fire, drives one `on_tick(dt)` through
//! the gateway ([`Gateway::tick`]), broadcasting any commands the script emits to
//! all sessions.
//!
//! Concurrency model (reviewed for deadlock/starvation, ):
//!
//! - The tick and inbound message dispatch share the one `Mutex<Lua>`. The tick
//!   acquires it with a **blocking** lock (not `try_lock`): every critical
//!   section — a message handler or a tick — is bounded by a per-invocation
//!   deadline, so the wait is bounded and the game loop is guaranteed to run
//!   rather than be starved out under sustained inbound traffic. Conversely a
//!   heavy tick cannot wedge dispatch because its own deadline bounds how long it
//!   holds the lock.
//! - The blocking Lua call runs on `spawn_blocking`, off the core async workers,
//!   so a tick that runs for its full budget does not park a Tokio worker that
//!   inbound read loops need. Any panic surfaces as a `JoinError` here, is
//!   logged, and the loop continues — a single bad tick never kills the game
//!   loop (and `Gateway::tick` already isolates Lua errors internally).
//! - [`MissedTickBehavior::Skip`] means a slow tick never accumulates a burst of
//!   catch-up ticks; the script always sees the same nominal `dt` and time is
//!   never double-counted.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::MissedTickBehavior;

use crate::error::AppResult;
use crate::lifecycle::{AsyncService, CancellationToken};
use crate::realtime::Gateway;
use crate::{
    chat_cluster::ChatDeliveryDispatcher,
    time::{Clock, SystemClock},
};

/// A read-only, server-owned view of authoritative gameplay time.
///
/// This is simulation time, not wall-clock time: it advances only after a
/// completed authoritative simulation step. `epoch` identifies one hub/match
/// lifetime, so a recreated hub never makes a reused tick number look current.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GameplayClockSnapshot {
    /// Opaque, nonzero identifier for this clock lifetime.
    pub epoch: u64,
    /// Completed authoritative simulation steps in this epoch.
    pub tick: u64,
    /// Effective configured simulation rate for this epoch.
    pub tick_hz: u16,
    /// Saturating simulation elapsed time in microseconds.
    pub elapsed_us: u64,
}

/// Server-owned deterministic gameplay clock.
///
/// The clock deliberately has no wall-clock source. Scheduler delays and skipped
/// interval fires cannot mint catch-up steps or make gameplay time jump.
#[derive(Debug)]
pub struct GameplayClock {
    snapshot: GameplayClockSnapshot,
    /// Remainder for exact rational conversion of ticks to microseconds.
    elapsed_remainder: u32,
}

/// Process-local issuer for automatically assigned gameplay-clock epochs.
///
/// `0` is a terminal exhausted sentinel, never an epoch.  The final usable
/// epoch (`u64::MAX`) is issued once and atomically changes the issuer to that
/// sentinel.  We deliberately do not wrap: a restarted or recreated hub must
/// fail closed rather than make an old epoch appear current again.
#[derive(Debug)]
struct GameplayClockEpochIssuer {
    next: AtomicU64,
}

impl GameplayClockEpochIssuer {
    const fn new(first_epoch: u64) -> Self {
        Self {
            next: AtomicU64::new(first_epoch),
        }
    }

    fn issue(&self) -> Option<u64> {
        let mut current = self.next.load(Ordering::Relaxed);
        loop {
            if current == 0 {
                return None;
            }
            let next = current.checked_add(1).unwrap_or(0);
            match self.next.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(current),
                Err(observed) => current = observed,
            }
        }
    }
}

static GAMEPLAY_CLOCK_EPOCH_ISSUER: GameplayClockEpochIssuer = GameplayClockEpochIssuer::new(1);

impl GameplayClock {
    /// Create a new clock with a freshly issued opaque epoch, failing closed if
    /// the process-local epoch space has been exhausted.
    pub fn try_new(tick_hz: u16) -> Option<Self> {
        Self::try_new_from_issuer(&GAMEPLAY_CLOCK_EPOCH_ISSUER, tick_hz)
    }

    fn try_new_from_issuer(issuer: &GameplayClockEpochIssuer, tick_hz: u16) -> Option<Self> {
        issuer.issue().map(|epoch| Self::with_epoch(epoch, tick_hz))
    }

    /// Create a new clock with a freshly issued opaque epoch.
    ///
    /// Prefer [`Self::try_new`] at service boundaries so exhaustion is handled
    /// explicitly. This compatibility constructor deliberately panics rather
    /// than reusing an epoch if that terminal condition is ignored.
    #[must_use]
    pub fn new(tick_hz: u16) -> Self {
        Self::try_new(tick_hz).expect("gameplay clock epoch space exhausted")
    }

    /// Create a clock with an explicit nonzero epoch. This is primarily useful
    /// for deterministic ownership/recreation tests.
    #[must_use]
    pub fn with_epoch(epoch: u64, tick_hz: u16) -> Self {
        Self {
            snapshot: GameplayClockSnapshot {
                epoch: epoch.max(1),
                tick: 0,
                tick_hz: tick_hz.max(1),
                elapsed_us: 0,
            },
            elapsed_remainder: 0,
        }
    }

    /// Return the current read-only snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> GameplayClockSnapshot {
        self.snapshot
    }

    /// Record exactly one completed simulation step.
    pub fn complete_step(&mut self) {
        self.snapshot.tick = self.snapshot.tick.saturating_add(1);
        let micros_per_second = 1_000_000_u32;
        let numerator = self.elapsed_remainder.saturating_add(micros_per_second);
        let increment = u64::from(numerator / u32::from(self.snapshot.tick_hz));
        self.elapsed_remainder = numerator % u32::from(self.snapshot.tick_hz);
        self.snapshot.elapsed_us = self.snapshot.elapsed_us.saturating_add(increment);
    }
}

/// A periodic task that drives the script's `citadel.on_tick` game loop.
pub struct LuaTickService {
    gateway: Arc<Gateway>,
    /// Wall-clock spacing between ticks (`1 / tick_hz`).
    period: Duration,
    /// Nominal step handed to the script each tick, in the same units as
    /// `period`. Fixed-step: a skipped tick does not inflate `dt`.
    dt: Duration,
    /// Per-tick time budget enforced on the `on_tick` handler.
    budget: Duration,
}

/// Periodic local matchmaker evaluator. It is independent from the game-script
/// tick: a server with no embedded script still expires tickets and completes
/// any cohort that becomes eligible. The deployed period is 250 ms; tests may
/// use a shorter period through [`Self::new`].
pub struct MatchmakerTickService {
    gateway: Arc<Gateway>,
    period: Duration,
}

/// Periodically renews only channel/node chat leases while local subscribers
/// exist. It never inspects socket queues or performs remote fan-out.
pub struct ChatPresenceRenewalService {
    gateway: Arc<Gateway>,
    period: Duration,
}

/// Periodically drains a bounded batch of durable cross-node chat events.
///
/// The worker deliberately runs outside every socket reactor. A failed remote
/// attempt leaves the source row durable until its exclusive deadline; expiry
/// is cleaned in the same bounded pass so clients reconcile from history.
pub struct ChatDeliveryDispatchService {
    dispatcher: Arc<ChatDeliveryDispatcher>,
    period: Duration,
    batch_limit: usize,
    cleanup_limit: usize,
}

impl ChatDeliveryDispatchService {
    /// Build a supervised durable chat delivery worker.
    #[must_use]
    pub fn new(
        dispatcher: Arc<ChatDeliveryDispatcher>,
        period: Duration,
        batch_limit: usize,
        cleanup_limit: usize,
    ) -> Self {
        Self {
            dispatcher,
            period,
            batch_limit: batch_limit.max(1),
            cleanup_limit: cleanup_limit.max(1),
        }
    }
}

impl AsyncService for ChatDeliveryDispatchService {
    fn name(&self) -> &str {
        "chat-delivery-dispatch"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        let mut interval = tokio::time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                _ = interval.tick() => {
                    let now = SystemClock.now();
                    if let Err(error) = self.dispatcher.dispatch_once(now, self.batch_limit).await {
                        tracing::warn!(error = %error, "bounded chat delivery dispatch failed; rows remain durable for retry");
                    }
                    if let Err(error) = self.dispatcher.cleanup_expired(now, self.cleanup_limit).await {
                        tracing::warn!(error = %error, "bounded expired chat delivery cleanup failed");
                    }
                }
            }
        }
        Ok(())
    }
}

impl ChatPresenceRenewalService {
    /// Build a supervised channel-lease renewal worker.
    #[must_use]
    pub fn new(gateway: Arc<Gateway>, period: Duration) -> Self {
        Self { gateway, period }
    }
}

impl AsyncService for ChatPresenceRenewalService {
    fn name(&self) -> &str {
        "chat-presence-renewal"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        let mut interval = tokio::time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                _ = interval.tick() => self.gateway.renew_chat_cluster_presence(),
            }
        }
        Ok(())
    }
}

impl MatchmakerTickService {
    /// Build a supervised local-matchmaker evaluator.
    #[must_use]
    pub fn new(gateway: Arc<Gateway>, period: Duration) -> Self {
        Self { gateway, period }
    }
}

impl LuaTickService {
    /// Build a tick service driving `gateway` at `period` with step `dt` and the
    /// given per-tick `budget`.
    #[must_use]
    pub fn new(gateway: Arc<Gateway>, period: Duration, dt: Duration, budget: Duration) -> Self {
        Self {
            gateway,
            period,
            dt,
            budget,
        }
    }
}

/// A periodic task that drives the authoritative transform-sync loop
///: each fire advances the world one sim step, latches the frame, and
/// fans out one delta snapshot per registered transform-sync client on the
/// unreliable path via [`Gateway::transform_tick`].
///
/// This mirrors [`LuaTickService`]'s cadence discipline
/// ([`MissedTickBehavior::Skip`], fixed-step) but is CPU-light and non-blocking
/// (no Lua lock), so it runs directly on the async runtime rather than
/// `spawn_blocking`. QUIC owns byte-level pacing; this loop only controls *what*
/// is offered per tick (design §6.5).
pub struct TransformTickService {
    gateway: Arc<Gateway>,
    /// Wall-clock spacing between **sim** steps (`1 / sim_hz`). The world advances
    /// every step so `server_tick`/physics run at `sim_hz`.
    sim_period: Duration,
    /// Emit a snapshot every this many sim steps (`round(sim_hz / send_rate_hz)`),
    /// so snapshots go out at ~`send_rate_hz` while the sim ticks at `sim_hz`.
    snapshot_every: u32,
}

impl TransformTickService {
    /// Build a transform-sync tick service that advances the sim every `sim_period`
    /// and emits a snapshot every `snapshot_every` sim steps.
    #[must_use]
    pub fn new(gateway: Arc<Gateway>, sim_period: Duration, snapshot_every: u32) -> Self {
        Self {
            gateway,
            sim_period,
            snapshot_every: snapshot_every.max(1),
        }
    }
}

impl AsyncService for TransformTickService {
    fn name(&self) -> &str {
        "transform-tick"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        let mut interval = tokio::time::interval(self.sim_period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut since_snapshot: u32 = 0;
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                _ = interval.tick() => {
                    // Advance the world every sim step (server_tick/physics @ sim_hz).
                    self.gateway.transform_sim_step();
                    // Emit a snapshot every `snapshot_every` steps (~send_rate_hz).
                    since_snapshot += 1;
                    if since_snapshot >= self.snapshot_every {
                        since_snapshot = 0;
                        self.gateway.transform_snapshot_step();
                    }
                }
            }
        }
        Ok(())
    }
}

impl AsyncService for MatchmakerTickService {
    fn name(&self) -> &str {
        "matchmaker-tick"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        let mut interval = tokio::time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                _ = interval.tick() => {
                    let delivered = self.gateway.matchmaker_tick();
                    tracing::trace!(delivered, "completed local matchmaker evaluation");
                }
            }
        }
        Ok(())
    }
}

impl AsyncService for LuaTickService {
    fn name(&self) -> &str {
        "lua-tick"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        let mut interval = tokio::time::interval(self.period);
        // A slow tick must not pile up catch-up ticks; skip missed ones and keep
        // a steady, fixed-step cadence.
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let dt = self.dt;
        let budget = self.budget;
        loop {
            tokio::select! {
                // Cooperative shutdown: stop promptly when the supervisor cancels.
                () = cancel.cancelled() => break,
                _ = interval.tick() => {
                    let gateway = Arc::clone(&self.gateway);
                    // Run the (blocking, up-to-`budget`) Lua tick off the async
                    // workers. A panic becomes a JoinError; log it and keep going.
                    match tokio::task::spawn_blocking(move || gateway.tick(dt, budget)).await {
                        Ok(_delivered) => {}
                        Err(join_err) => {
                            tracing::error!(
                                error = %join_err,
                                "lua tick task panicked; isolated, game loop continues"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chat_cluster::ChatPresenceDirectory;
    use crate::lifecycle::Supervisor;
    use crate::repository::{ChatDeliveryOutboxRecord, InMemoryChatRepository};
    use crate::session::NodeId;
    use crate::time::TimestampMillis;

    #[test]
    fn gameplay_clock_is_monotonic_exact_and_does_not_read_wall_time() {
        let mut clock = GameplayClock::with_epoch(7, 60);
        assert_eq!(
            clock.snapshot(),
            GameplayClockSnapshot {
                epoch: 7,
                tick: 0,
                tick_hz: 60,
                elapsed_us: 0,
            }
        );

        for _ in 0..60 {
            clock.complete_step();
        }
        assert_eq!(clock.snapshot().tick, 60);
        assert_eq!(clock.snapshot().elapsed_us, 1_000_000);

        // A delayed scheduler fire represents one completed simulation step,
        // never an unbounded wall-clock or catch-up jump.
        clock.complete_step();
        assert_eq!(clock.snapshot().tick, 61);
        assert_eq!(clock.snapshot().elapsed_us, 1_016_666);
    }

    #[test]
    fn gameplay_clock_normalizes_configuration_and_saturates() {
        let mut clock = GameplayClock::with_epoch(0, 0);
        assert_eq!(clock.snapshot().epoch, 1);
        assert_eq!(clock.snapshot().tick_hz, 1);

        clock.snapshot.tick = u64::MAX;
        clock.snapshot.elapsed_us = u64::MAX;
        clock.complete_step();
        assert_eq!(clock.snapshot().tick, u64::MAX);
        assert_eq!(clock.snapshot().elapsed_us, u64::MAX);
    }

    #[test]
    fn gameplay_clock_epoch_issuer_exhausts_without_wrapping_or_reuse() {
        // Construct a boundary-state issuer directly: no impractical allocation
        // loop is needed to prove that the final epoch is emitted just once.
        let issuer = GameplayClockEpochIssuer::new(u64::MAX);

        let final_clock = GameplayClock::try_new_from_issuer(&issuer, 30)
            .expect("the final nonzero epoch is still issuable");
        assert_eq!(final_clock.snapshot().epoch, u64::MAX);
        assert_eq!(issuer.next.load(Ordering::Relaxed), 0);

        assert!(GameplayClock::try_new_from_issuer(&issuer, 30).is_none());
        assert!(issuer.issue().is_none());
    }

    #[test]
    fn gameplay_clock_recreation_uses_a_distinct_epoch() {
        let first = GameplayClock::new(30).snapshot();
        let second = GameplayClock::new(30).snapshot();
        assert_ne!(first.epoch, second.epoch);
        assert_eq!(second.tick, 0);
        assert_eq!(second.elapsed_us, 0);
    }

    #[tokio::test]
    async fn matchmaker_tick_runs_without_a_script_tick_and_stops_cleanly() {
        let gateway = Arc::new(Gateway::new());
        let mut supervisor = Supervisor::new();
        supervisor.spawn(MatchmakerTickService::new(
            Arc::clone(&gateway),
            Duration::from_millis(5),
        ));
        for _ in 0..10 {
            if gateway.matchmaker_stats().evaluations_total > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            gateway.matchmaker_stats().evaluations_total > 0,
            "interval runs independently of an embedded runtime"
        );
        supervisor.shutdown().await.expect("cooperative shutdown");
    }

    #[tokio::test]
    async fn chat_delivery_worker_cleans_expired_rows_and_stops_cleanly() {
        let repository = Arc::new(InMemoryChatRepository::new());
        repository
            .stage_delivery_outbox(ChatDeliveryOutboxRecord {
                channel_id: "channel-1".to_owned(),
                event_id: 1,
                authority_epoch: 0,
                payload: "{}".to_owned(),
                created_at: TimestampMillis::from_unix_millis(1),
                expires_at: TimestampMillis::from_unix_millis(2),
            })
            .expect("stage expired source row");
        let dispatcher = Arc::new(ChatDeliveryDispatcher::new(
            NodeId::new("node-a".to_owned()).expect("node id"),
            repository.clone(),
            Arc::new(ChatPresenceDirectory::default()),
            Arc::new(|_, _| Ok(crate::chat_cluster::ChatDeliveryDisposition::Delivered)),
        ));
        let mut supervisor = Supervisor::new();
        supervisor.spawn(ChatDeliveryDispatchService::new(
            dispatcher,
            Duration::from_millis(5),
            1,
            1,
        ));
        for _ in 0..10 {
            if repository
                .active_delivery_outbox(SystemClock.now(), 1)
                .expect("read outbox")
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            repository
                .active_delivery_outbox(SystemClock.now(), 1)
                .expect("read outbox")
                .is_empty(),
            "the supervised worker runs bounded expiry maintenance"
        );
        supervisor.shutdown().await.expect("cooperative shutdown");
    }
}
