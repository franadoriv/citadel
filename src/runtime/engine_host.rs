//! Worker-side per-match execution host.
//!
//! A deployment runs exactly ONE engine (single-engine invariant: the engine
//! is chosen by main-file detection), so the supervised worker hosts one
//! [`MatchEngine`] and multiplexes every live match through it with a
//! per-match execution context:
//!
//! - **Lua**: a fresh `mlua` state per match (a per-match [`LuaRuntime`]), so
//!   match globals can never leak between matches.
//! - **JS** (`runtime-js`): a fresh rquickjs `Runtime` + `Context` per match
//!   (a per-match `JsRuntime`).
//! - **Python** (`runtime-python`): a per-match namespace on the single
//!   interpreter — documented soft isolation (not yet implemented here).
//!
//! Scheduling is fair round-robin with a one-budget-quantum rule: each
//! scheduling round gives every live match exactly one bounded invocation
//! (one queued event, or one tick when its mailbox is empty). The quantum's
//! bound is the engine's existing per-invocation deadline machinery, so a
//! match that exhausts its budget every quantum cannot delay a neighbor's
//! ticks — it merely burns its own quantum.
//!
//! The watchdog is layered:
//!
//! 1. **Per-invocation deadline** — enforced inside the engine (instruction
//!    hooks / interrupt handlers), reused as the quantum bound.
//! 2. **Per-match overrun policy** — a match whose invocations blow the
//!    deadline repeatedly is closed with a server error; neighbors keep
//!    running.
//! 3. **Stuck-thread quarantine** — a context wedged inside a
//!    non-reclaimable native call is quarantined (the match closes, the
//!    thread is written off) and the host self-reports unhealthy after a
//!    configured number of quarantines.
//! 4. **Engine death** — an unrecoverable engine (e.g. a wedged Python
//!    interpreter) closes every match and marks the host dead so the
//!    supervisor replaces the whole worker process.
//!
//! Failure closure is match-local wherever possible: a closed match emits a
//! [`HostOutput::Closed`] that the gateway turns into member notifications
//! (`KIND_MATCH_CLOSED` with a requeue hint) and a room prune; only engine
//! death escalates to the process level.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use super::worker_data_protocol::MatchCloseReason;
use crate::runtime::{OutboundCommand, Runtime};

/// PROVISIONAL mailbox bound: reuses the existing runtime event queue default
/// (`RuntimeEventsCapabilityConfig::queue_capacity`, 1024) rather than
/// inventing a new number. Replace once per-match inbound burst depth has
/// been measured under real multiplexed-match load.
pub const DEFAULT_MATCH_MAILBOX_CAPACITY: usize = 1_024;

/// PROVISIONAL overrun budget: how many consecutive blown per-invocation
/// deadlines close a match. Replace once the distribution of consecutive
/// deadline overruns produced by healthy production matches is measured.
pub const DEFAULT_MATCH_OVERRUN_LIMIT: u32 = 3;

/// PROVISIONAL quarantine budget (`K`): how many quarantined threads flip the
/// worker to self-reported unhealthy. Replace once quarantine frequency has
/// been measured against a real worker's thread budget.
pub const DEFAULT_QUARANTINED_THREAD_LIMIT: u32 = 2;

/// Injectable per-match scheduling/watchdog policy.
///
/// Mirrors the `WorkerSupervisionPolicy` seam: the worker loop and tests
/// configure every limit the host enforces through this one struct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchSchedulerPolicy {
    mailbox_capacity: usize,
    overrun_limit: u32,
    quarantined_thread_limit: u32,
}

impl MatchSchedulerPolicy {
    #[must_use]
    pub fn with_mailbox_capacity(mut self, mailbox_capacity: usize) -> Self {
        self.mailbox_capacity = mailbox_capacity.max(1);
        self
    }

    #[must_use]
    pub fn with_overrun_limit(mut self, overrun_limit: u32) -> Self {
        self.overrun_limit = overrun_limit.max(1);
        self
    }

    #[must_use]
    pub fn with_quarantined_thread_limit(mut self, quarantined_thread_limit: u32) -> Self {
        self.quarantined_thread_limit = quarantined_thread_limit.max(1);
        self
    }

    pub fn mailbox_capacity(&self) -> usize {
        self.mailbox_capacity
    }

    pub fn overrun_limit(&self) -> u32 {
        self.overrun_limit
    }

    pub fn quarantined_thread_limit(&self) -> u32 {
        self.quarantined_thread_limit
    }
}

impl Default for MatchSchedulerPolicy {
    fn default() -> Self {
        Self {
            mailbox_capacity: DEFAULT_MATCH_MAILBOX_CAPACITY,
            overrun_limit: DEFAULT_MATCH_OVERRUN_LIMIT,
            quarantined_thread_limit: DEFAULT_QUARANTINED_THREAD_LIMIT,
        }
    }
}

/// How one bounded invocation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchFault {
    /// The invocation consumed its entire per-invocation budget (the engine's
    /// deadline machinery aborted it). Charged against the per-match overrun
    /// policy.
    Overrun,
    /// The context is wedged inside a call the engine cannot abort in-thread;
    /// the executing thread must be quarantined and the context abandoned.
    Wedged,
    /// The engine itself is unrecoverable; the worker process must be
    /// replaced.
    EngineDead,
}

/// One inbound invocation of the existing runtime dispatch surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchInvocation {
    /// Originating participant (raw session id).
    pub sender: u64,
    /// Authenticated user id, if any.
    pub user_id: Option<String>,
    /// Wire kind of the inbound envelope.
    pub kind: u16,
    /// Opaque envelope body.
    pub body: Vec<u8>,
}

/// A per-match execution context produced by the deployment's one engine.
pub trait MatchContext: Send {
    /// Run one queued event through the match's context (one quantum).
    fn handle_event(
        &mut self,
        invocation: &MatchInvocation,
    ) -> Result<Vec<OutboundCommand>, MatchFault>;

    /// Advance the match's game loop by `dt` (one quantum).
    fn tick(&mut self, dt: Duration) -> Result<Vec<OutboundCommand>, MatchFault>;
}

/// The deployment's single engine: a factory for per-match contexts.
pub trait MatchEngine: Send {
    /// Stable engine token (`lua` / `js` / `python`).
    fn engine(&self) -> &'static str;

    /// Open a fresh, isolated execution context for `match_id`.
    fn open_match(&mut self, match_id: u64) -> Result<Box<dyn MatchContext>, MatchFault>;
}

/// Why [`EngineHost::open_match`] refused to open a match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostOpenError {
    /// The match is already open.
    AlreadyOpen,
    /// The engine is dead; the worker is awaiting replacement.
    EngineDead,
    /// The engine failed to build the context; a `Closed` output was emitted
    /// so the members are informed and returned to matchmaking.
    ContextFailed,
}

/// Outcome of [`EngineHost::enqueue_event`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    /// The event was queued for the match's next quantum.
    Accepted,
    /// No such open match: the event was dropped and counted. After a worker
    /// restart the match table is empty, so nothing can resume without a
    /// fresh `MatchOpen`.
    UnknownMatch,
    /// The match's bounded mailbox was full: the event was dropped and the
    /// match failed closed (`MailboxOverflow`), leaving neighbors untouched.
    MailboxOverflow,
}

/// One host-produced output, drained by the worker loop into data-plane
/// frames.
#[derive(Debug, Clone, PartialEq)]
pub enum HostOutput {
    /// Commands produced by one quantum of one match.
    Commands {
        match_id: u64,
        commands: Vec<OutboundCommand>,
    },
    /// The match is closed; members must be informed and returned to
    /// matchmaking.
    Closed {
        match_id: u64,
        reason: MatchCloseReason,
    },
}

/// Monotone host counters (observability + tests).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HostCounters {
    /// Events dropped because they targeted no open match.
    pub unknown_match_events: u64,
    /// Events dropped by a full per-match mailbox.
    pub overflowed_events: u64,
    /// Invocations that blew their per-invocation deadline.
    pub deadline_overruns: u64,
    /// Threads written off to stuck-context quarantine.
    pub quarantined_threads: u32,
}

struct MatchSlot {
    context: Box<dyn MatchContext>,
    mailbox: VecDeque<MatchInvocation>,
    consecutive_overruns: u32,
}

/// The worker's match table + fair scheduler + layered watchdog.
pub struct EngineHost {
    engine: Box<dyn MatchEngine>,
    policy: MatchSchedulerPolicy,
    matches: HashMap<u64, MatchSlot>,
    /// Round-robin visit order (ids of open matches).
    order: VecDeque<u64>,
    scheduler_rounds: u64,
    counters: HostCounters,
    engine_dead: bool,
    outputs: Vec<HostOutput>,
}

impl EngineHost {
    #[must_use]
    pub fn new(engine: Box<dyn MatchEngine>, policy: MatchSchedulerPolicy) -> Self {
        Self {
            engine,
            policy,
            matches: HashMap::new(),
            order: VecDeque::new(),
            scheduler_rounds: 0,
            counters: HostCounters::default(),
            engine_dead: false,
            outputs: Vec::new(),
        }
    }

    /// The hosted engine's stable token.
    #[must_use]
    pub fn engine(&self) -> &'static str {
        self.engine.engine()
    }

    /// Whether the host may keep serving matches. False once the engine died
    /// or the quarantine budget is exhausted; the worker self-reports
    /// unhealthy so the supervisor replaces the process.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        !self.engine_dead
            && self.counters.quarantined_threads < self.policy.quarantined_thread_limit()
    }

    /// Whether the single hosted engine is dead.
    #[must_use]
    pub fn engine_dead(&self) -> bool {
        self.engine_dead
    }

    /// Monotone count of completed scheduling rounds.
    #[must_use]
    pub fn scheduler_rounds(&self) -> u64 {
        self.scheduler_rounds
    }

    /// Point-in-time copy of the drop/overrun/quarantine counters.
    #[must_use]
    pub fn counters(&self) -> HostCounters {
        self.counters
    }

    /// Ids of currently open matches, unordered.
    #[must_use]
    pub fn live_match_ids(&self) -> Vec<u64> {
        self.matches.keys().copied().collect()
    }

    /// Scheduler-liveness heartbeat payload.
    #[must_use]
    pub fn heartbeat(&self) -> super::worker_data_protocol::EngineReport {
        super::worker_data_protocol::EngineReport::Heartbeat {
            scheduler_rounds: self.scheduler_rounds,
            live_matches: u32::try_from(self.matches.len()).unwrap_or(u32::MAX),
            quarantined_threads: self.counters.quarantined_threads,
        }
    }

    /// Open a fresh, isolated execution context for `match_id`.
    pub fn open_match(&mut self, match_id: u64) -> Result<(), HostOpenError> {
        if self.engine_dead {
            return Err(HostOpenError::EngineDead);
        }
        if self.matches.contains_key(&match_id) {
            return Err(HostOpenError::AlreadyOpen);
        }
        match self.engine.open_match(match_id) {
            Ok(context) => {
                self.matches.insert(
                    match_id,
                    MatchSlot {
                        context,
                        mailbox: VecDeque::with_capacity(self.policy.mailbox_capacity()),
                        consecutive_overruns: 0,
                    },
                );
                self.order.push_back(match_id);
                Ok(())
            }
            Err(MatchFault::EngineDead) => {
                self.mark_engine_dead();
                Err(HostOpenError::EngineDead)
            }
            Err(_) => {
                // Fail closed toward the members: the match never opened, so
                // they must be informed and returned to matchmaking.
                self.outputs.push(HostOutput::Closed {
                    match_id,
                    reason: MatchCloseReason::ServerError,
                });
                Err(HostOpenError::ContextFailed)
            }
        }
    }

    /// Queue one event for the match's next quantum, fail-closed on overflow.
    pub fn enqueue_event(&mut self, match_id: u64, invocation: MatchInvocation) -> EnqueueOutcome {
        let Some(slot) = self.matches.get_mut(&match_id) else {
            self.counters.unknown_match_events += 1;
            return EnqueueOutcome::UnknownMatch;
        };
        if slot.mailbox.len() >= self.policy.mailbox_capacity() {
            // Fail closed, match-locally: the event is dropped (the mailbox
            // never grows past its bound) and the overflowing match closes;
            // neighbors and the host itself are untouched.
            self.counters.overflowed_events += 1;
            self.close_match(match_id, MatchCloseReason::MailboxOverflow);
            return EnqueueOutcome::MailboxOverflow;
        }
        slot.mailbox.push_back(invocation);
        EnqueueOutcome::Accepted
    }

    /// Run one fair scheduling round: every open match receives exactly one
    /// budget quantum — one queued event, or one tick when its mailbox is
    /// empty. A budget-exhausting match burns only its own quantum.
    pub fn run_round(&mut self, dt: Duration) {
        if self.engine_dead {
            return;
        }
        let round: Vec<u64> = self.order.iter().copied().collect();
        for match_id in round {
            // One quantum per match per round: one queued event, or one tick
            // when the mailbox is empty — never both.
            let Some(slot) = self.matches.get_mut(&match_id) else {
                continue;
            };
            let result = match slot.mailbox.pop_front() {
                Some(invocation) => slot.context.handle_event(&invocation),
                None => slot.context.tick(dt),
            };
            self.settle_quantum(match_id, result);
            if self.engine_dead {
                break;
            }
        }
        self.scheduler_rounds += 1;
    }

    /// Close `match_id` (if open) with `reason`, emitting the closure output.
    pub fn close_match(&mut self, match_id: u64, reason: MatchCloseReason) {
        if self.matches.remove(&match_id).is_some() {
            self.order.retain(|&id| id != match_id);
            self.outputs.push(HostOutput::Closed { match_id, reason });
        }
    }

    /// Orderly shutdown: close every match with [`MatchCloseReason::Shutdown`].
    pub fn shutdown(&mut self) {
        for match_id in self.live_match_ids() {
            self.close_match(match_id, MatchCloseReason::Shutdown);
        }
    }

    /// Drain the outputs produced since the last drain, in order.
    pub fn drain_outputs(&mut self) -> Vec<HostOutput> {
        std::mem::take(&mut self.outputs)
    }

    fn settle_quantum(&mut self, match_id: u64, result: Result<Vec<OutboundCommand>, MatchFault>) {
        match result {
            Ok(commands) => {
                if let Some(slot) = self.matches.get_mut(&match_id) {
                    slot.consecutive_overruns = 0;
                }
                if !commands.is_empty() {
                    self.outputs
                        .push(HostOutput::Commands { match_id, commands });
                }
            }
            Err(MatchFault::Overrun) => {
                self.counters.deadline_overruns += 1;
                let close = if let Some(slot) = self.matches.get_mut(&match_id) {
                    slot.consecutive_overruns = slot.consecutive_overruns.saturating_add(1);
                    slot.consecutive_overruns >= self.policy.overrun_limit()
                } else {
                    false
                };
                if close {
                    self.close_match(match_id, MatchCloseReason::ServerError);
                }
            }
            Err(MatchFault::Wedged) => {
                // The executing thread cannot be reclaimed: write it off,
                // abandon the context, and close only this match.
                self.counters.quarantined_threads =
                    self.counters.quarantined_threads.saturating_add(1);
                self.close_match(match_id, MatchCloseReason::Quarantined);
            }
            Err(MatchFault::EngineDead) => {
                self.mark_engine_dead();
            }
        }
    }

    fn mark_engine_dead(&mut self) {
        self.engine_dead = true;
        for match_id in self.live_match_ids() {
            self.close_match(match_id, MatchCloseReason::EngineDead);
        }
    }
}

/// Per-match context that runs the existing runtime dispatch surface with the
/// engine's own per-invocation deadline machinery as the quantum bound.
///
/// The engine isolates script errors internally (an erroring handler yields
/// no commands); what the host must observe is a *blown budget*, detected by
/// the invocation consuming its entire per-invocation deadline.
struct RuntimeMatchContext<R: Runtime> {
    runtime: R,
}

impl<R: Runtime> MatchContext for RuntimeMatchContext<R> {
    fn handle_event(
        &mut self,
        invocation: &MatchInvocation,
    ) -> Result<Vec<OutboundCommand>, MatchFault> {
        let budget = self.runtime.budget();
        let started = std::time::Instant::now();
        let commands = self.runtime.dispatch(
            invocation.sender,
            invocation.user_id.as_deref(),
            invocation.kind,
            &invocation.body,
        );
        if started.elapsed() >= budget {
            return Err(MatchFault::Overrun);
        }
        Ok(commands)
    }

    fn tick(&mut self, dt: Duration) -> Result<Vec<OutboundCommand>, MatchFault> {
        let budget = self.runtime.budget();
        let started = std::time::Instant::now();
        let commands = self.runtime.tick(dt, budget);
        if started.elapsed() >= budget {
            return Err(MatchFault::Overrun);
        }
        Ok(commands)
    }
}

/// Lua engine: one fresh `mlua` state per match.
///
/// Every match evaluates the deployment's single script into its own
/// [`LuaRuntime`] (its own `mlua::Lua`), so script globals are match-local by
/// construction.
pub struct LuaMatchEngine {
    source: String,
    deadline_ms: u64,
}

impl LuaMatchEngine {
    #[must_use]
    pub fn new(source: impl Into<String>, deadline_ms: u64) -> Self {
        Self {
            source: source.into(),
            deadline_ms,
        }
    }
}

impl MatchEngine for LuaMatchEngine {
    fn engine(&self) -> &'static str {
        "lua"
    }

    fn open_match(&mut self, match_id: u64) -> Result<Box<dyn MatchContext>, MatchFault> {
        let runtime = crate::runtime::LuaRuntime::from_source(
            &self.source,
            format!("match-{match_id}/main.lua"),
            self.deadline_ms,
        )
        .map_err(|_| MatchFault::Wedged)?;
        Ok(Box::new(RuntimeMatchContext { runtime }))
    }
}

/// JS engine: one fresh rquickjs `Runtime` + `Context` per match (each
/// `JsRuntime` owns its own engine instance with its own memory limit).
#[cfg(feature = "runtime-js")]
pub struct JsMatchEngine {
    source: String,
    deadline_ms: u64,
}

#[cfg(feature = "runtime-js")]
impl JsMatchEngine {
    #[must_use]
    pub fn new(source: impl Into<String>, deadline_ms: u64) -> Self {
        Self {
            source: source.into(),
            deadline_ms,
        }
    }
}

#[cfg(feature = "runtime-js")]
impl MatchEngine for JsMatchEngine {
    fn engine(&self) -> &'static str {
        "js"
    }

    fn open_match(&mut self, match_id: u64) -> Result<Box<dyn MatchContext>, MatchFault> {
        let runtime = crate::runtime::JsRuntime::from_source(
            &self.source,
            format!("match-{match_id}/main.js"),
            self.deadline_ms,
        )
        .map_err(|_| MatchFault::Wedged)?;
        Ok(Box::new(RuntimeMatchContext { runtime }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use super::*;

    /// Deterministic scripted engine for scheduler/watchdog tests.
    #[derive(Clone, Copy)]
    enum Behavior {
        /// Counts events and ticks; never faults.
        Counts,
        /// Blows its per-invocation budget on every quantum.
        Overruns,
        /// Wedges in a non-reclaimable call on its first event.
        Wedges,
        /// Kills the whole engine on its first event.
        DiesOnEvent,
    }

    #[derive(Default)]
    struct MatchStats {
        events: AtomicU64,
        ticks: AtomicU64,
    }

    struct FakeContext {
        behavior: Behavior,
        stats: Arc<MatchStats>,
    }

    impl MatchContext for FakeContext {
        fn handle_event(
            &mut self,
            _invocation: &MatchInvocation,
        ) -> Result<Vec<OutboundCommand>, MatchFault> {
            self.stats.events.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                Behavior::Counts => Ok(Vec::new()),
                Behavior::Overruns => Err(MatchFault::Overrun),
                Behavior::Wedges => Err(MatchFault::Wedged),
                Behavior::DiesOnEvent => Err(MatchFault::EngineDead),
            }
        }

        fn tick(&mut self, _dt: Duration) -> Result<Vec<OutboundCommand>, MatchFault> {
            self.stats.ticks.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                Behavior::Overruns => Err(MatchFault::Overrun),
                _ => Ok(Vec::new()),
            }
        }
    }

    /// Engine whose per-match behavior is scripted by match id.
    struct FakeEngine {
        engine: &'static str,
        behaviors: HashMap<u64, Behavior>,
        stats: HashMap<u64, Arc<MatchStats>>,
    }

    impl FakeEngine {
        fn new(engine: &'static str) -> Self {
            Self {
                engine,
                behaviors: HashMap::new(),
                stats: HashMap::new(),
            }
        }

        fn with_match(mut self, match_id: u64, behavior: Behavior) -> Self {
            self.behaviors.insert(match_id, behavior);
            self.stats.insert(match_id, Arc::new(MatchStats::default()));
            self
        }

        fn stats(&self, match_id: u64) -> Arc<MatchStats> {
            Arc::clone(&self.stats[&match_id])
        }
    }

    impl MatchEngine for FakeEngine {
        fn engine(&self) -> &'static str {
            self.engine
        }

        fn open_match(&mut self, match_id: u64) -> Result<Box<dyn MatchContext>, MatchFault> {
            Ok(Box::new(FakeContext {
                behavior: self.behaviors[&match_id],
                stats: Arc::clone(&self.stats[&match_id]),
            }))
        }
    }

    fn invocation(kind: u16) -> MatchInvocation {
        MatchInvocation {
            sender: 7,
            user_id: None,
            kind,
            body: Vec::new(),
        }
    }

    #[test]
    fn heavy_match_does_not_delay_neighbor_ticks() {
        // Match A exhausts its budget on every quantum and has a deep queue;
        // match B is idle and must tick once per round regardless.
        let engine = FakeEngine::new("fake")
            .with_match(1, Behavior::Overruns)
            .with_match(2, Behavior::Counts);
        let a = engine.stats(1);
        let b = engine.stats(2);
        let mut host = EngineHost::new(
            Box::new(engine),
            // Keep A alive for the whole test: the overrun policy is
            // exercised separately below.
            MatchSchedulerPolicy::default().with_overrun_limit(1_000),
        );
        host.open_match(1).expect("open A");
        host.open_match(2).expect("open B");
        for _ in 0..40 {
            assert_eq!(
                host.enqueue_event(1, invocation(1)),
                EnqueueOutcome::Accepted
            );
        }
        const ROUNDS: u64 = 10;
        for _ in 0..ROUNDS {
            host.run_round(Duration::from_millis(16));
        }
        // Fairness is asserted on counts, not wall clocks: B ticked exactly
        // once per round, and A consumed exactly one queued event per round
        // (one budget quantum) despite its 40-deep mailbox.
        assert_eq!(b.ticks.load(Ordering::SeqCst), ROUNDS);
        assert_eq!(a.events.load(Ordering::SeqCst), ROUNDS);
        assert_eq!(host.scheduler_rounds(), ROUNDS);
    }

    #[test]
    fn match_deadline_closes_only_that_match() {
        // A non-yielding match blows its deadline every quantum; after the
        // overrun limit it is closed with a server error while its neighbor
        // keeps ticking and the host stays healthy (worker stays Ready).
        let engine = FakeEngine::new("fake")
            .with_match(1, Behavior::Overruns)
            .with_match(2, Behavior::Counts);
        let b = engine.stats(2);
        let mut host = EngineHost::new(
            Box::new(engine),
            MatchSchedulerPolicy::default().with_overrun_limit(3),
        );
        host.open_match(1).expect("open A");
        host.open_match(2).expect("open B");
        for _ in 0..6 {
            host.run_round(Duration::from_millis(16));
        }
        assert_eq!(host.live_match_ids(), vec![2], "only A may be closed");
        let outputs = host.drain_outputs();
        assert!(
            outputs.contains(&HostOutput::Closed {
                match_id: 1,
                reason: MatchCloseReason::ServerError,
            }),
            "A must be closed as a server error: {outputs:?}"
        );
        assert_eq!(host.counters().deadline_overruns, 3);
        assert_eq!(b.ticks.load(Ordering::SeqCst), 6, "B keeps ticking");
        assert!(host.is_healthy(), "the worker stays Ready");
    }

    #[test]
    fn mailbox_overflow_fails_closed() {
        let engine = FakeEngine::new("fake")
            .with_match(1, Behavior::Counts)
            .with_match(2, Behavior::Counts);
        let b = engine.stats(2);
        let mut host = EngineHost::new(
            Box::new(engine),
            MatchSchedulerPolicy::default().with_mailbox_capacity(2),
        );
        host.open_match(1).expect("open A");
        host.open_match(2).expect("open B");
        assert_eq!(
            host.enqueue_event(1, invocation(1)),
            EnqueueOutcome::Accepted
        );
        assert_eq!(
            host.enqueue_event(1, invocation(1)),
            EnqueueOutcome::Accepted
        );
        // The third event overflows the bounded mailbox: dropped, counted,
        // and the match fails closed without touching its neighbor.
        assert_eq!(
            host.enqueue_event(1, invocation(1)),
            EnqueueOutcome::MailboxOverflow
        );
        assert_eq!(host.counters().overflowed_events, 1);
        assert_eq!(host.live_match_ids(), vec![2]);
        assert!(host.drain_outputs().contains(&HostOutput::Closed {
            match_id: 1,
            reason: MatchCloseReason::MailboxOverflow,
        }));
        host.run_round(Duration::from_millis(16));
        assert_eq!(b.ticks.load(Ordering::SeqCst), 1, "B is unaffected");
        assert!(host.is_healthy());
    }

    #[test]
    fn stuck_native_call_quarantines_thread() {
        // A context wedged in a non-reclaimable call is quarantined: only its
        // match closes, the counter advances, and the host self-reports
        // unhealthy once the quarantine budget (K) is exhausted.
        let engine = FakeEngine::new("fake")
            .with_match(1, Behavior::Wedges)
            .with_match(2, Behavior::Counts)
            .with_match(3, Behavior::Wedges);
        let b = engine.stats(2);
        let mut host = EngineHost::new(
            Box::new(engine),
            MatchSchedulerPolicy::default().with_quarantined_thread_limit(2),
        );
        host.open_match(1).expect("open A");
        host.open_match(2).expect("open B");
        host.enqueue_event(1, invocation(1));
        host.run_round(Duration::from_millis(16));
        assert_eq!(host.counters().quarantined_threads, 1);
        assert!(
            host.drain_outputs().contains(&HostOutput::Closed {
                match_id: 1,
                reason: MatchCloseReason::Quarantined,
            }),
            "the wedged match must close as quarantined"
        );
        assert_eq!(b.ticks.load(Ordering::SeqCst), 1, "neighbor unaffected");
        assert!(host.is_healthy(), "one quarantine stays under K=2");

        host.open_match(3).expect("open C");
        host.enqueue_event(3, invocation(1));
        host.run_round(Duration::from_millis(16));
        assert_eq!(host.counters().quarantined_threads, 2);
        assert!(
            !host.is_healthy(),
            "after K quarantines the worker must self-report unhealthy"
        );
    }

    #[test]
    fn engine_death_closes_every_match_and_reports_dead() {
        // The Python-shaped failure: the single interpreter wedges, the whole
        // engine is unrecoverable, and every dependent match closes so the
        // supervisor can replace the process.
        let engine = FakeEngine::new("python")
            .with_match(1, Behavior::DiesOnEvent)
            .with_match(2, Behavior::Counts);
        let mut host = EngineHost::new(Box::new(engine), MatchSchedulerPolicy::default());
        host.open_match(1).expect("open A");
        host.open_match(2).expect("open B");
        host.enqueue_event(1, invocation(1));
        host.run_round(Duration::from_millis(16));
        assert!(host.engine_dead());
        assert!(!host.is_healthy());
        assert!(host.live_match_ids().is_empty(), "all matches closed");
        let outputs = host.drain_outputs();
        for match_id in [1, 2] {
            assert!(
                outputs.contains(&HostOutput::Closed {
                    match_id,
                    reason: MatchCloseReason::EngineDead,
                }),
                "match {match_id} must close as engine-dead: {outputs:?}"
            );
        }
        // Dead engine: nothing can be opened or scheduled anymore.
        assert_eq!(host.open_match(9), Err(HostOpenError::EngineDead));
        assert_eq!(host.engine(), "python");
    }

    #[test]
    fn restart_does_not_resume_matches() {
        // A replacement worker builds a fresh host: its match table is empty,
        // and traffic for a pre-restart match is dropped and counted until a
        // fresh MatchOpen arrives.
        let engine = FakeEngine::new("fake").with_match(1, Behavior::Counts);
        let mut host = EngineHost::new(Box::new(engine), MatchSchedulerPolicy::default());
        assert!(host.live_match_ids().is_empty());
        assert_eq!(
            host.enqueue_event(1, invocation(1)),
            EnqueueOutcome::UnknownMatch
        );
        assert_eq!(host.counters().unknown_match_events, 1);
        host.open_match(1)
            .expect("MatchOpen re-registers the match");
        assert_eq!(
            host.enqueue_event(1, invocation(1)),
            EnqueueOutcome::Accepted
        );
    }

    #[test]
    fn shutdown_closes_every_match_in_order() {
        let engine = FakeEngine::new("fake")
            .with_match(1, Behavior::Counts)
            .with_match(2, Behavior::Counts);
        let mut host = EngineHost::new(Box::new(engine), MatchSchedulerPolicy::default());
        host.open_match(1).expect("open A");
        host.open_match(2).expect("open B");
        host.shutdown();
        assert!(host.live_match_ids().is_empty());
        let outputs = host.drain_outputs();
        assert_eq!(outputs.len(), 2);
        assert!(outputs.iter().all(|output| matches!(
            output,
            HostOutput::Closed {
                reason: MatchCloseReason::Shutdown,
                ..
            }
        )));
    }

    #[test]
    fn duplicate_open_is_refused() {
        let engine = FakeEngine::new("fake").with_match(1, Behavior::Counts);
        let mut host = EngineHost::new(Box::new(engine), MatchSchedulerPolicy::default());
        host.open_match(1).expect("open");
        assert_eq!(host.open_match(1), Err(HostOpenError::AlreadyOpen));
    }

    // ------------------------------------------------------------------
    // Real-engine isolation and watchdog tests (one per available engine).
    // ------------------------------------------------------------------

    /// Script shared by the isolation tests: a per-VM global counter that a
    /// second match must not observe. Uses the existing dispatch surface
    /// (`citadel.on_message` / `citadel.send`).
    const LUA_COUNTER_SCRIPT: &str = r#"
        count = 0
        citadel.on_message(1, function(ctx, body)
            count = count + 1
            citadel.send(ctx.sender, 99, tostring(count))
        end)
    "#;

    fn sent_bodies(outputs: &[HostOutput], match_id: u64) -> Vec<Vec<u8>> {
        outputs
            .iter()
            .filter_map(|output| match output {
                HostOutput::Commands {
                    match_id: id,
                    commands,
                } if *id == match_id => Some(commands),
                _ => None,
            })
            .flatten()
            .filter_map(|command| match command {
                OutboundCommand::Send { body, .. } => Some(body.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn two_matches_do_not_share_globals() {
        let engine = LuaMatchEngine::new(LUA_COUNTER_SCRIPT, 100);
        let mut host = EngineHost::new(Box::new(engine), MatchSchedulerPolicy::default());
        host.open_match(1).expect("open A");
        host.open_match(2).expect("open B");
        // Two events into A, then one into B: if the mlua state were shared,
        // B would observe A's increments and answer "3".
        host.enqueue_event(1, invocation(1));
        host.run_round(Duration::from_millis(16));
        host.enqueue_event(1, invocation(1));
        host.run_round(Duration::from_millis(16));
        host.enqueue_event(2, invocation(1));
        host.run_round(Duration::from_millis(16));
        let outputs = host.drain_outputs();
        assert_eq!(
            sent_bodies(&outputs, 1),
            vec![b"1".to_vec(), b"2".to_vec()],
            "match A counts its own events"
        );
        assert_eq!(
            sent_bodies(&outputs, 2),
            vec![b"1".to_vec()],
            "match B starts from a fresh mlua state"
        );
    }

    #[cfg(feature = "runtime-js")]
    #[test]
    fn two_matches_do_not_share_globals_js() {
        const JS_COUNTER_SCRIPT: &str = r#"
            let count = 0;
            citadel.on_message(1, (ctx, body) => {
                count += 1;
                citadel.send(ctx.sender, 99, String(count));
            });
        "#;
        let engine = JsMatchEngine::new(JS_COUNTER_SCRIPT, 100);
        let mut host = EngineHost::new(Box::new(engine), MatchSchedulerPolicy::default());
        host.open_match(1).expect("open A");
        host.open_match(2).expect("open B");
        host.enqueue_event(1, invocation(1));
        host.run_round(Duration::from_millis(16));
        host.enqueue_event(1, invocation(1));
        host.run_round(Duration::from_millis(16));
        host.enqueue_event(2, invocation(1));
        host.run_round(Duration::from_millis(16));
        let outputs = host.drain_outputs();
        assert_eq!(sent_bodies(&outputs, 1), vec![b"1".to_vec(), b"2".to_vec()]);
        assert_eq!(
            sent_bodies(&outputs, 2),
            vec![b"1".to_vec()],
            "match B starts from a fresh rquickjs runtime"
        );
    }

    #[test]
    fn lua_non_yielding_loop_closes_only_that_match() {
        // The deployment's one script routes kind 1 into a non-yielding pure
        // Lua loop and kind 2 into a well-behaved counter. Match A receives
        // the poisonous kind; the engine's instruction-hook deadline bounds
        // each quantum, the overrun policy closes A, and B keeps answering.
        const SCRIPT: &str = r#"
            citadel.on_message(1, function(ctx, body)
                while true do end
            end)
            citadel.on_message(2, function(ctx, body)
                citadel.send(ctx.sender, 99, "ok")
            end)
        "#;
        let engine = LuaMatchEngine::new(SCRIPT, 25);
        let mut host = EngineHost::new(
            Box::new(engine),
            MatchSchedulerPolicy::default().with_overrun_limit(2),
        );
        host.open_match(1).expect("open A");
        host.open_match(2).expect("open B");
        for _ in 0..2 {
            host.enqueue_event(1, invocation(1));
            host.enqueue_event(2, invocation(2));
            host.run_round(Duration::from_millis(16));
        }
        assert_eq!(host.live_match_ids(), vec![2], "only A may be closed");
        let outputs = host.drain_outputs();
        assert!(outputs.contains(&HostOutput::Closed {
            match_id: 1,
            reason: MatchCloseReason::ServerError,
        }));
        assert_eq!(
            sent_bodies(&outputs, 2),
            vec![b"ok".to_vec(), b"ok".to_vec()],
            "B keeps serving while A is closed"
        );
        assert!(host.is_healthy(), "the worker stays Ready");
    }
}
