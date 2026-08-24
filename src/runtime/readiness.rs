//! GameScript readiness authority: the single source of truth for "may a
//! match exist right now?".
//!
//! When `runtime.require_script` is enabled, no match may be advertised,
//! created, or admitted into unless a validated GameScript is loaded and its
//! execution backend is healthy. [`GameScriptReadiness`] owns that decision as
//! one atomically readable [`ReadinessSnapshot`]; every enforcement surface
//! reads **one** snapshot through [`GameScriptReadiness::gate`] and, on
//! success, receives the [`ScriptBinding`] a newborn match must be bound to.
//!
//! Design:
//!
//! - **States** ([`ScriptReadinessState`]): `NoScript` (nothing loaded — the
//!   boot-not-ready case for a missing entrypoint), `Validating` (a load is in
//!   flight), `Ready` (loaded and healthy — the only state that opens the
//!   gate), `Activating` (a revision swap is in flight; fail closed during the
//!   swap), `Degraded` (the execution backend lost health; existing matches
//!   are held, new ones are gated), and `Unavailable` (hard failure).
//! - **Identity**: the gate binds matches to `(revision_id, generation)`. The
//!   revision id is the loaded script's content identity (the supervised
//!   worker reports it as `WorkerReady.script_identity`; in-process runtimes
//!   derive an equivalent `sha256:<hex>` content hash at load/reload). The
//!   generation is a local monotonic counter bumped on every successful
//!   (re)load, so admission into a room bound to a superseded load is
//!   refused even when content hashes collide across reloads. The revision
//!   repository's activation pipeline will supersede the generation source
//!   later; the seam is this module's transition methods.
//! - **Sources** ([`ReadinessSource`]): adapters translate runtime-specific
//!   outcomes into authority transitions. [`InProcessRuntimeSource`] covers
//!   the embedded Lua/Python/JS load, reload, and hot-reload outcomes;
//!   [`SupervisedWorkerSource`] (gated on worker-adapter availability) covers
//!   worker authenticate→`WorkerReady`, health monitoring, and restart
//!   recovery.
//! - **Client safety**: every rejection carries the one stable
//!   [`SCRIPT_UNAVAILABLE_MESSAGE`]; state detail (revision ids, paths,
//!   worker errors) is reserved for the operator console and logs.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::observability::NodeMetrics;
use crate::runtime::ReloadOutcome;
use crate::time::TimestampMillis;

/// The one stable, client-safe message carried by every gated rejection.
///
/// Part of the client contract for `require_script` deployments: RPC error
/// replies, `KIND_ROOM_REJECT` reasons, and the console listing error all use
/// exactly this string. It never names revisions, paths, or worker detail.
pub const SCRIPT_UNAVAILABLE_MESSAGE: &str = "game script unavailable";

/// How long a health-degraded backend may hold existing matches before the
/// authority reports `Unavailable`.
///
/// PROVISIONAL: no measured recovery-time distribution exists yet, so this
/// mirrors the worker supervisor's restart budget scale. The hold window is an
/// injectable seam ([`GameScriptReadiness::with_degraded_hold`]); the revision
/// repository's activation pipeline owns the eventual policy.
pub const DEFAULT_DEGRADED_HOLD: Duration = Duration::from_secs(30);

/// Lifecycle states of the GameScript execution surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptReadinessState {
    /// No script has ever loaded (missing entrypoint boots here, not-ready).
    NoScript,
    /// A load/validation is in flight; the gate stays closed until it lands.
    Validating,
    /// A validated script is loaded and its backend is healthy. The only
    /// state in which the gate opens.
    Ready,
    /// A revision swap is in flight. Fail closed for the swap's duration.
    Activating,
    /// The execution backend lost health. Existing matches are held (not torn
    /// down) but nothing new is listed, created, or admitted.
    Degraded,
    /// Hard failure (e.g. the degraded hold window elapsed without recovery).
    Unavailable,
}

impl ScriptReadinessState {
    /// Stable lowercase token for logs, metrics labels, and console JSON.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::NoScript => "no_script",
            Self::Validating => "validating",
            Self::Ready => "ready",
            Self::Activating => "activating",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
        }
    }

    /// Stable numeric value for the readiness state gauge.
    #[must_use]
    pub const fn gauge_value(self) -> i64 {
        match self {
            Self::NoScript => 0,
            Self::Validating => 1,
            Self::Ready => 2,
            Self::Activating => 3,
            Self::Degraded => 4,
            Self::Unavailable => 5,
        }
    }
}

/// The `(revision, generation)` identity a match is born bound to.
///
/// Captured from the one gating snapshot that admitted the match's creation,
/// never re-derived. Admission into a room whose binding no longer matches the
/// currently loaded script is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptBinding {
    /// Content identity of the loaded script (e.g. `sha256:<hex>`).
    pub revision_id: String,
    /// Local monotonic load generation (bumped on every successful (re)load).
    pub generation: u64,
}

/// A point-in-time, atomically captured view of script readiness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessSnapshot {
    /// Current lifecycle state.
    pub state: ScriptReadinessState,
    /// Content identity of the most recently loaded script, if any ever
    /// loaded. Retained through `Degraded` so the console can show what was
    /// running; `None` before the first successful load.
    pub revision_id: Option<String>,
    /// Local monotonic load generation (0 before the first successful load).
    pub generation: u64,
    /// When the current state was entered.
    pub since: TimestampMillis,
}

impl ReadinessSnapshot {
    /// The binding a match born under this snapshot receives, or `None` when
    /// the gate is closed. `Some` if and only if the state is [`Ready`]
    /// (with the invariant that `Ready` always has a revision).
    ///
    /// [`Ready`]: ScriptReadinessState::Ready
    #[must_use]
    pub fn binding(&self) -> Option<ScriptBinding> {
        if self.state != ScriptReadinessState::Ready {
            return None;
        }
        self.revision_id.as_ref().map(|revision_id| ScriptBinding {
            revision_id: revision_id.clone(),
            generation: self.generation,
        })
    }
}

/// Worker recovery posture, mirrored for the console readiness surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryView {
    /// Whether restarts are still available or the circuit breaker is open.
    pub circuit_open: bool,
    /// Consecutive restart failures observed by the supervisor.
    pub consecutive_failures: u32,
    /// The supervisor's restart budget.
    pub restart_limit: u32,
}

#[derive(Debug)]
struct Inner {
    snapshot: ReadinessSnapshot,
    recovery: Option<RecoveryView>,
    degraded_since: Option<TimestampMillis>,
}

/// Server-owned callbacks that run after a committed readiness transition.
/// They are held outside `Inner` so an observer can acquire its own lifecycle
/// lock without a lock-order cycle with the readiness authority.
type ReadinessInvalidation = Arc<dyn Fn(ReadinessSnapshot) + Send + Sync + 'static>;

#[derive(Default)]
struct ReadinessInvalidations(Mutex<Vec<ReadinessInvalidation>>);

impl core::fmt::Debug for ReadinessInvalidations {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("ReadinessInvalidations([registered])")
    }
}

/// The node-local authority every match surface consults before it lists,
/// creates, or admits.
///
/// Interior-mutable (like `RoomRegistry`) so the shared gateway and the
/// runtime sources can drive it behind `&self`. Present on the gateway only
/// when `runtime.require_script` is enabled; its absence preserves the
/// ungated behavior byte for byte.
#[derive(Debug)]
pub struct GameScriptReadiness {
    inner: Mutex<Inner>,
    invalidations: ReadinessInvalidations,
    /// Serializes publication and callback delivery, so a delayed older
    /// transition cannot invalidate capabilities installed by a later load.
    invalidation_serial: Mutex<()>,
    metrics: Option<Arc<NodeMetrics>>,
    degraded_hold: Duration,
}

impl GameScriptReadiness {
    /// A new authority in `NoScript` (not-ready) at `now`.
    #[must_use]
    pub fn new(now: TimestampMillis) -> Self {
        Self {
            inner: Mutex::new(Inner {
                snapshot: ReadinessSnapshot {
                    state: ScriptReadinessState::NoScript,
                    revision_id: None,
                    generation: 0,
                    since: now,
                },
                recovery: None,
                degraded_since: None,
            }),
            invalidations: ReadinessInvalidations::default(),
            invalidation_serial: Mutex::new(()),
            metrics: None,
            degraded_hold: DEFAULT_DEGRADED_HOLD,
        }
    }

    /// Report state transitions to the shared node metrics (readiness gauge).
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<NodeMetrics>) -> Self {
        metrics.set_script_readiness_state(ScriptReadinessState::NoScript.gauge_value());
        self.metrics = Some(metrics);
        self
    }

    /// Override the degraded hold window (test/policy seam; PROVISIONAL
    /// default in [`DEFAULT_DEGRADED_HOLD`]).
    #[must_use]
    pub fn with_degraded_hold(mut self, hold: Duration) -> Self {
        self.degraded_hold = hold;
        self
    }

    /// One atomic snapshot of the current readiness.
    #[must_use]
    pub fn snapshot(&self) -> ReadinessSnapshot {
        self.lock().snapshot.clone()
    }

    /// Register a server-owned callback that runs after every readiness change.
    ///
    /// The callback runs after the readiness mutex is released and receives only
    /// the committed server-owned snapshot, never client-controlled data.
    pub fn register_invalidation(&self, invalidation: ReadinessInvalidation) {
        self.invalidations
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(invalidation);
    }

    /// The worker recovery posture, when a supervised source reported one.
    #[must_use]
    pub fn recovery(&self) -> Option<RecoveryView> {
        self.lock().recovery
    }

    /// The single gate every enforcement surface calls.
    ///
    /// Returns the binding for a match born now, or the stable client-safe
    /// message when the gate is closed. Per-surface rejection counters are
    /// owned by the enforcement points (gateway/console) so a shared metrics
    /// registry never double-counts one refusal.
    pub fn gate(&self) -> Result<ScriptBinding, &'static str> {
        self.lock()
            .snapshot
            .binding()
            .ok_or(SCRIPT_UNAVAILABLE_MESSAGE)
    }

    /// A load/validation began (e.g. a revision deploy is being validated).
    pub fn record_validating(&self, now: TimestampMillis) {
        self.transition(ScriptReadinessState::Validating, now);
    }

    /// A revision swap began; the gate stays closed until the swap lands.
    pub fn record_activating(&self, now: TimestampMillis) {
        self.transition(ScriptReadinessState::Activating, now);
    }

    /// A validated script finished loading: enter `Ready`, adopt the content
    /// identity, and bump the local generation.
    pub fn record_loaded(&self, revision_id: impl Into<String>, now: TimestampMillis) {
        let _serial = self
            .invalidation_serial
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut g = self.lock();
        g.snapshot.state = ScriptReadinessState::Ready;
        g.snapshot.revision_id = Some(revision_id.into());
        g.snapshot.generation += 1;
        g.snapshot.since = now;
        g.degraded_since = None;
        self.publish_gauge(&g);
        let snapshot = g.snapshot.clone();
        drop(g);
        self.invalidate_lifecycle_dependents(snapshot);
    }

    /// No script exists to load (missing/disappeared entrypoint): not-ready,
    /// never a silent relay fallback.
    pub fn record_no_script(&self, now: TimestampMillis) {
        self.transition(ScriptReadinessState::NoScript, now);
    }

    /// The execution backend lost health. Existing matches are held; new
    /// listing/creation/admission is gated. Idempotent while degraded (the
    /// first loss pins `since`).
    pub fn record_degraded(&self, now: TimestampMillis) {
        let _serial = self
            .invalidation_serial
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut g = self.lock();
        if g.snapshot.state != ScriptReadinessState::Degraded {
            g.snapshot.state = ScriptReadinessState::Degraded;
            g.snapshot.since = now;
            g.degraded_since = Some(now);
        }
        self.publish_gauge(&g);
        let snapshot = g.snapshot.clone();
        drop(g);
        self.invalidate_lifecycle_dependents(snapshot);
    }

    /// Hard failure: nothing lists, creates, or admits until a new load.
    pub fn record_unavailable(&self, now: TimestampMillis) {
        self.transition(ScriptReadinessState::Unavailable, now);
    }

    /// Escalate a degraded backend to `Unavailable` once the hold window
    /// elapses without recovery. Returns whether the escalation happened.
    /// No-op in every other state.
    pub fn expire_degraded_hold(&self, now: TimestampMillis) -> bool {
        let _serial = self
            .invalidation_serial
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut g = self.lock();
        let Some(degraded_since) = g.degraded_since else {
            return false;
        };
        if g.snapshot.state != ScriptReadinessState::Degraded {
            return false;
        }
        let hold_ms = u64::try_from(self.degraded_hold.as_millis()).unwrap_or(u64::MAX);
        let deadline = degraded_since.unix_millis().saturating_add(hold_ms);
        if now.unix_millis() < deadline {
            return false;
        }
        g.snapshot.state = ScriptReadinessState::Unavailable;
        g.snapshot.since = now;
        g.degraded_since = None;
        self.publish_gauge(&g);
        let snapshot = g.snapshot.clone();
        drop(g);
        self.invalidate_lifecycle_dependents(snapshot);
        true
    }

    /// Update the mirrored worker recovery posture (console surface).
    pub fn record_recovery(&self, recovery: RecoveryView) {
        self.lock().recovery = Some(recovery);
    }

    fn transition(&self, state: ScriptReadinessState, now: TimestampMillis) {
        let _serial = self
            .invalidation_serial
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut g = self.lock();
        g.snapshot.state = state;
        g.snapshot.since = now;
        if state != ScriptReadinessState::Degraded {
            g.degraded_since = None;
        }
        self.publish_gauge(&g);
        let snapshot = g.snapshot.clone();
        drop(g);
        self.invalidate_lifecycle_dependents(snapshot);
    }

    fn invalidate_lifecycle_dependents(&self, snapshot: ReadinessSnapshot) {
        let callbacks = self
            .invalidations
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        for callback in callbacks {
            callback(snapshot.clone());
        }
    }

    fn publish_gauge(&self, g: &Inner) {
        if let Some(metrics) = &self.metrics {
            metrics.set_script_readiness_state(g.snapshot.state.gauge_value());
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Where readiness transitions come from.
///
/// A source owns the translation from one execution backend's native outcomes
/// (reload enums, worker control frames) into authority transitions; the
/// authority itself stays backend-agnostic.
pub trait ReadinessSource: Send + Sync {
    /// Stable lowercase token naming this source in logs.
    fn source(&self) -> &'static str;

    /// The authority this source publishes into.
    fn authority(&self) -> &Arc<GameScriptReadiness>;
}

/// Derive the `sha256:<hex>` content identity of an on-disk entrypoint.
///
/// This is the in-process analogue of the supervised worker's
/// `WorkerReady.script_identity`. `None` when the file cannot be read — the
/// caller must treat that as not-ready rather than inventing an identity.
///
/// Seam note: once the revision-repository activation pipeline drives
/// deployment, the domain-scoped
/// [`gamescript_revision_content_hash`](crate::repository::gamescript::gamescript_revision_content_hash)
/// of the activated revision becomes the identity source and this
/// file-hash fallback retires with the local generation counter.
#[must_use]
pub fn script_content_identity(entrypoint: &Path) -> Option<String> {
    let bytes = std::fs::read(entrypoint).ok()?;
    let digest = Sha256::digest(&bytes);
    let mut hex = String::with_capacity(7 + digest.len() * 2);
    hex.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }
    Some(hex)
}

/// Readiness source for the embedded (in-process) Lua/Python/JS adapters.
///
/// Fed by `build_runtime` (initial load / missing entrypoint) and by the
/// hot-reload watcher (reload outcomes). The content identity is the
/// entrypoint's `sha256` hash taken at (re)load time.
#[derive(Debug, Clone)]
pub struct InProcessRuntimeSource {
    authority: Arc<GameScriptReadiness>,
}

impl InProcessRuntimeSource {
    /// Wrap `authority` for the embedded adapters.
    #[must_use]
    pub fn new(authority: Arc<GameScriptReadiness>) -> Self {
        Self { authority }
    }

    /// The initial startup load succeeded for `entrypoint`.
    pub fn record_initial_load(&self, entrypoint: &Path, now: TimestampMillis) {
        match script_content_identity(entrypoint) {
            Some(identity) => self.authority.record_loaded(identity, now),
            None => {
                // Loaded from a file that can no longer be hashed: fail
                // closed rather than bind matches to an unverifiable
                // revision.
                tracing::warn!(
                    entrypoint = %entrypoint.display(),
                    "loaded script entrypoint could not be hashed; readiness stays closed"
                );
                self.authority.record_no_script(now);
            }
        }
    }

    /// The configured entrypoint is missing or disappeared before load.
    pub fn record_missing_entrypoint(&self, now: TimestampMillis) {
        self.authority.record_no_script(now);
    }

    /// A hot-reload attempt finished with `outcome` for `entrypoint`.
    ///
    /// A rejected reload keeps the previously loaded script serving, so the
    /// authority intentionally does not leave `Ready` for it.
    pub fn record_reload(&self, outcome: ReloadOutcome, entrypoint: &Path, now: TimestampMillis) {
        match outcome {
            ReloadOutcome::Reloaded => self.record_initial_load(entrypoint, now),
            ReloadOutcome::Rejected | ReloadOutcome::NotReloadable => {}
        }
    }
}

impl ReadinessSource for InProcessRuntimeSource {
    fn source(&self) -> &'static str {
        "in_process_runtime"
    }

    fn authority(&self) -> &Arc<GameScriptReadiness> {
        &self.authority
    }
}

/// Readiness source for the supervised external worker adapter.
///
/// Available wherever the worker supervisor is (unix and windows): worker
/// authenticate→`WorkerReady` opens the gate with the worker-reported
/// `script_identity`, health-monitor failures degrade it, and the restart
/// circuit breaker's posture is mirrored for the console.
#[cfg(any(unix, windows))]
#[derive(Debug, Clone)]
pub struct SupervisedWorkerSource {
    authority: Arc<GameScriptReadiness>,
}

#[cfg(any(unix, windows))]
impl SupervisedWorkerSource {
    /// Wrap `authority` for the supervised worker adapter.
    #[must_use]
    pub fn new(authority: Arc<GameScriptReadiness>) -> Self {
        Self { authority }
    }

    /// The worker authenticated and reported `WorkerReady`.
    ///
    /// A ready frame without a `script_identity` cannot open the gate: a
    /// match must never bind to an unidentified revision.
    pub fn record_worker_ready(&self, script_identity: Option<&str>, now: TimestampMillis) {
        match script_identity {
            Some(identity) if !identity.is_empty() => {
                self.authority.record_loaded(identity, now);
            }
            _ => {
                tracing::warn!("worker ready without a script identity; readiness stays closed");
                self.authority.record_no_script(now);
            }
        }
    }

    /// The health monitor observed a missed/invalid worker health frame.
    pub fn record_health_lost(&self, now: TimestampMillis) {
        self.authority.record_degraded(now);
    }

    /// Mirror the supervisor's restart posture for the console surface.
    /// An open circuit (restart budget exhausted) is a hard failure.
    pub fn record_recovery(
        &self,
        snapshot: &crate::runtime::worker_supervisor::RecoverySnapshot,
        now: TimestampMillis,
    ) {
        let circuit_open = matches!(
            snapshot.status,
            crate::runtime::worker_supervisor::RecoveryStatus::CircuitOpen
        );
        self.authority.record_recovery(RecoveryView {
            circuit_open,
            consecutive_failures: snapshot.consecutive_failures,
            restart_limit: snapshot.restart_limit,
        });
        if circuit_open {
            self.authority.record_unavailable(now);
        }
    }
}

#[cfg(any(unix, windows))]
impl ReadinessSource for SupervisedWorkerSource {
    fn source(&self) -> &'static str {
        "supervised_worker"
    }

    fn authority(&self) -> &Arc<GameScriptReadiness> {
        &self.authority
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn at(ms: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(ms)
    }

    #[test]
    fn readiness_invalidations_are_serialized_with_the_transition_snapshot() {
        let readiness = GameScriptReadiness::new(at(0));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&observed);
        readiness.register_invalidation(Arc::new(move |snapshot| {
            sink.lock()
                .expect("test observer lock")
                .push((snapshot.state, snapshot.generation));
        }));

        readiness.record_loaded("sha256:v1", at(1));
        readiness.record_activating(at(2));
        readiness.record_loaded("sha256:v2", at(3));

        assert_eq!(
            *observed.lock().expect("test observer lock"),
            vec![
                (ScriptReadinessState::Ready, 1),
                (ScriptReadinessState::Activating, 1),
                (ScriptReadinessState::Ready, 2),
            ]
        );
    }

    #[test]
    fn boots_not_ready_and_opens_only_after_a_successful_load() {
        let readiness = GameScriptReadiness::new(at(1));
        let snapshot = readiness.snapshot();
        assert_eq!(snapshot.state, ScriptReadinessState::NoScript);
        assert_eq!(snapshot.generation, 0);
        assert_eq!(snapshot.binding(), None);
        assert_eq!(readiness.gate(), Err(SCRIPT_UNAVAILABLE_MESSAGE));

        readiness.record_loaded("sha256:abc", at(5));
        let binding = readiness.gate().expect("ready gate opens");
        assert_eq!(binding.revision_id, "sha256:abc");
        assert_eq!(binding.generation, 1);
        assert_eq!(readiness.snapshot().state, ScriptReadinessState::Ready);
        assert_eq!(readiness.snapshot().since, at(5));
    }

    #[test]
    fn every_successful_reload_bumps_the_generation() {
        let readiness = GameScriptReadiness::new(at(0));
        readiness.record_loaded("sha256:v1", at(1));
        readiness.record_loaded("sha256:v2", at(2));
        // Same content hash as an earlier load still gets a fresh generation.
        readiness.record_loaded("sha256:v1", at(3));
        let snapshot = readiness.snapshot();
        assert_eq!(snapshot.generation, 3);
        assert_eq!(snapshot.revision_id.as_deref(), Some("sha256:v1"));
    }

    #[test]
    fn non_ready_states_fail_closed_with_the_stable_message() {
        let readiness = GameScriptReadiness::new(at(0));
        readiness.record_loaded("sha256:v1", at(1));
        for close in [
            |r: &GameScriptReadiness| r.record_validating(at(2)),
            |r: &GameScriptReadiness| r.record_activating(at(2)),
            |r: &GameScriptReadiness| r.record_degraded(at(2)),
            |r: &GameScriptReadiness| r.record_unavailable(at(2)),
            |r: &GameScriptReadiness| r.record_no_script(at(2)),
        ] {
            readiness.record_loaded("sha256:v1", at(1));
            close(&readiness);
            assert_eq!(readiness.gate(), Err(SCRIPT_UNAVAILABLE_MESSAGE));
            assert_eq!(readiness.snapshot().binding(), None);
        }
    }

    #[test]
    fn degraded_retains_the_last_revision_for_operators() {
        let readiness = GameScriptReadiness::new(at(0));
        readiness.record_loaded("sha256:v1", at(1));
        readiness.record_degraded(at(2));
        let snapshot = readiness.snapshot();
        assert_eq!(snapshot.state, ScriptReadinessState::Degraded);
        assert_eq!(snapshot.revision_id.as_deref(), Some("sha256:v1"));
        assert_eq!(snapshot.generation, 1);
        assert_eq!(snapshot.since, at(2));
        // A second health loss does not re-pin the degraded start.
        readiness.record_degraded(at(9));
        assert_eq!(readiness.snapshot().since, at(2));
    }

    #[test]
    fn degraded_hold_window_escalates_to_unavailable() {
        let readiness =
            GameScriptReadiness::new(at(0)).with_degraded_hold(Duration::from_millis(100));
        readiness.record_loaded("sha256:v1", at(1));
        readiness.record_degraded(at(10));
        assert!(!readiness.expire_degraded_hold(at(50)), "inside the hold");
        assert_eq!(readiness.snapshot().state, ScriptReadinessState::Degraded);
        assert!(readiness.expire_degraded_hold(at(110)), "hold elapsed");
        assert_eq!(
            readiness.snapshot().state,
            ScriptReadinessState::Unavailable
        );
        // Recovery through a fresh load reopens the gate.
        readiness.record_loaded("sha256:v2", at(200));
        assert!(readiness.gate().is_ok());
        assert!(
            !readiness.expire_degraded_hold(at(999)),
            "no longer degraded"
        );
    }

    #[test]
    fn state_transitions_move_the_readiness_gauge() {
        let metrics = Arc::new(NodeMetrics::new());
        let readiness = GameScriptReadiness::new(at(0)).with_metrics(Arc::clone(&metrics));
        assert_eq!(
            metrics.snapshot().script_readiness_state,
            ScriptReadinessState::NoScript.gauge_value()
        );
        readiness.record_loaded("sha256:v1", at(1));
        assert_eq!(
            metrics.snapshot().script_readiness_state,
            ScriptReadinessState::Ready.gauge_value()
        );
        readiness.record_degraded(at(2));
        assert_eq!(
            metrics.snapshot().script_readiness_state,
            ScriptReadinessState::Degraded.gauge_value()
        );
        assert!(readiness.expire_degraded_hold(at(999_999_999)));
        assert_eq!(
            metrics.snapshot().script_readiness_state,
            ScriptReadinessState::Unavailable.gauge_value()
        );
        // Rejection counters belong to the enforcement points; consulting a
        // closed gate here moves no counter.
        let _ = readiness.gate();
        assert_eq!(
            metrics.snapshot().script_gate_rejections,
            crate::observability::ScriptGateRejectionsSnapshot::default()
        );
    }

    #[test]
    fn in_process_source_hashes_the_entrypoint_content() {
        let dir = std::env::temp_dir().join(format!(
            "citadel-readiness-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let entrypoint = dir.join("main.lua");
        std::fs::write(&entrypoint, b"print('v1')").unwrap();

        let authority = Arc::new(GameScriptReadiness::new(at(0)));
        let source = InProcessRuntimeSource::new(Arc::clone(&authority));
        assert_eq!(source.source(), "in_process_runtime");
        source.record_initial_load(&entrypoint, at(1));
        let first = authority.snapshot();
        assert_eq!(first.state, ScriptReadinessState::Ready);
        let v1 = first.revision_id.clone().unwrap();
        assert!(v1.starts_with("sha256:"), "content identity is a hash");

        // A rejected reload keeps the previous script serving: still Ready.
        source.record_reload(ReloadOutcome::Rejected, &entrypoint, at(2));
        assert_eq!(authority.snapshot(), first);

        // A successful reload of changed content adopts a new identity and
        // bumps the generation.
        std::fs::write(&entrypoint, b"print('v2')").unwrap();
        source.record_reload(ReloadOutcome::Reloaded, &entrypoint, at(3));
        let second = authority.snapshot();
        assert_eq!(second.generation, 2);
        assert_ne!(second.revision_id.as_deref(), Some(v1.as_str()));

        // A disappeared entrypoint is not-ready, never a silent fallback.
        source.record_missing_entrypoint(at(4));
        assert_eq!(authority.snapshot().state, ScriptReadinessState::NoScript);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn supervised_worker_source_follows_ready_health_and_recovery() {
        use crate::runtime::worker_supervisor::{RecoverySnapshot, RecoveryStatus};

        let authority = Arc::new(GameScriptReadiness::new(at(0)));
        let source = SupervisedWorkerSource::new(Arc::clone(&authority));
        assert_eq!(source.source(), "supervised_worker");

        // Ready without an identity cannot open the gate.
        source.record_worker_ready(None, at(1));
        assert_eq!(authority.snapshot().state, ScriptReadinessState::NoScript);

        source.record_worker_ready(Some("sha256:worker-v1"), at(2));
        let snapshot = authority.snapshot();
        assert_eq!(snapshot.state, ScriptReadinessState::Ready);
        assert_eq!(snapshot.revision_id.as_deref(), Some("sha256:worker-v1"));

        source.record_health_lost(at(3));
        assert_eq!(authority.snapshot().state, ScriptReadinessState::Degraded);
        source.record_recovery(
            &RecoverySnapshot {
                status: RecoveryStatus::Available,
                consecutive_failures: 1,
                restart_limit: 3,
                next_restart_delay: None,
            },
            at(4),
        );
        assert_eq!(
            authority.snapshot().state,
            ScriptReadinessState::Degraded,
            "an available restart budget keeps holding"
        );
        assert_eq!(
            authority.recovery(),
            Some(RecoveryView {
                circuit_open: false,
                consecutive_failures: 1,
                restart_limit: 3,
            })
        );

        // A recovered worker reopens the gate on its fresh ready frame.
        source.record_worker_ready(Some("sha256:worker-v1"), at(5));
        assert_eq!(authority.snapshot().state, ScriptReadinessState::Ready);
        assert_eq!(authority.snapshot().generation, 2);

        // An exhausted restart budget is a hard failure.
        source.record_health_lost(at(6));
        source.record_recovery(
            &RecoverySnapshot {
                status: RecoveryStatus::CircuitOpen,
                consecutive_failures: 3,
                restart_limit: 3,
                next_restart_delay: None,
            },
            at(7),
        );
        assert_eq!(
            authority.snapshot().state,
            ScriptReadinessState::Unavailable
        );
    }
}
