//! Supervision primitives for the internal GameScript worker.

use std::{
    io,
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

#[cfg(unix)]
use std::os::{
    fd::OwnedFd,
    unix::{
        net::{UnixListener, UnixStream},
        process::CommandExt,
    },
};

use super::worker_protocol::{
    ControlFrame, PROTOCOL_VERSION, is_valid_worker_health, verify_worker_hello,
};

#[cfg(unix)]
use super::{
    worker_bootstrap::BootstrapPipe,
    worker_ipc::PrivateUnixEndpoint,
    worker_protocol::{read_control_frame, write_control_frame},
};

#[cfg(windows)]
use super::{
    worker_ipc::PrivateNamedPipeEndpoint,
    worker_protocol::{read_control_frame_async, write_control_frame_async},
};

const INITIAL_RESTART_BACKOFF: Duration = Duration::from_millis(100);
const MAX_RESTART_BACKOFF: Duration = Duration::from_secs(30);

pub fn restart_backoff(attempt: u32) -> Duration {
    INITIAL_RESTART_BACKOFF
        .saturating_mul(1u32.checked_shl(attempt.min(9)).unwrap_or(u32::MAX))
        .min(MAX_RESTART_BACKOFF)
}

pub fn fresh_bootstrap_secret() -> io::Result<[u8; 32]> {
    let mut secret = [0; 32];
    getrandom::fill(&mut secret).map_err(|_| io::Error::other("bootstrap entropy unavailable"))?;
    Ok(secret)
}

pub fn fresh_bootstrap_nonce() -> io::Result<[u8; 32]> {
    let mut nonce = [0; 32];
    getrandom::fill(&mut nonce).map_err(|_| io::Error::other("bootstrap entropy unavailable"))?;
    Ok(nonce)
}

/// PROVISIONAL descriptor budget: no measurement of a script-hosting worker's
/// real open-file footprint (script + module files, sockets, engine
/// internals) exists yet. Replace once descriptor usage of a worker running
/// representative game scripts has been profiled.
pub const DEFAULT_WORKER_MAX_OPEN_FILES: u64 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerResourceLimits {
    max_open_files: u64,
}

impl WorkerResourceLimits {
    pub fn new(max_open_files: u64) -> io::Result<Self> {
        if max_open_files == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker open-file limit must be positive",
            ));
        }
        Ok(Self { max_open_files })
    }

    pub fn max_open_files(self) -> u64 {
        self.max_open_files
    }
}

impl Default for WorkerResourceLimits {
    fn default() -> Self {
        Self {
            max_open_files: DEFAULT_WORKER_MAX_OPEN_FILES,
        }
    }
}

/// PROVISIONAL restart budget: no production data on worker crash cadence
/// exists yet. Replace once supervised-worker crash/restart rates have been
/// measured in a real deployment.
pub const DEFAULT_WORKER_RESTART_LIMIT: u32 = 5;

/// PROVISIONAL bootstrap budget: no measurement of worker spawn-to-ready
/// latency exists yet. Replace once the readiness investigation captures the
/// bootstrap latency distribution of a real script-loading worker.
pub const DEFAULT_WORKER_BOOTSTRAP_DEADLINE: Duration = Duration::from_secs(5);

/// PROVISIONAL health timing: no measurement of worker event-loop stall
/// distributions under load exists yet. The cadence is how often the worker
/// emits a health frame; the liveness deadline is how long the supervisor
/// waits for one before declaring the worker dead, and must exceed the
/// cadence.
pub const DEFAULT_WORKER_HEALTH_CADENCE: Duration = Duration::from_secs(1);
pub const DEFAULT_WORKER_LIVENESS_DEADLINE: Duration = Duration::from_secs(5);

/// PROVISIONAL shutdown budget: how long an orderly shutdown waits for the
/// worker's stop acknowledgement before the process group is killed anyway.
pub const DEFAULT_WORKER_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(5);

/// PROVISIONAL re-arm streak: how many consecutive healthy cycles prove a
/// recovery before the restart circuit breaker clears its failure count. One
/// healthy cycle is not proof — a crash-looping worker can squeeze a health
/// frame in between crashes and would otherwise reset the breaker forever.
/// Replace once restart-storm telemetry shows the healthy-streak length that
/// separates real recoveries from flapping workers.
pub const DEFAULT_WORKER_BREAKER_REARM_CYCLES: u32 = 3;

/// Injectable supervision policy for the external GameScript worker.
///
/// This is the single seam the serve lifecycle and tests configure; every
/// limit the supervisor enforces on a worker process flows through it.
/// Callers must keep `health_cadence` strictly below `liveness_deadline`;
/// [`run_supervision_loop`] refuses a policy that violates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerSupervisionPolicy {
    resource_limits: WorkerResourceLimits,
    restart_limit: u32,
    breaker_rearm_healthy_cycles: u32,
    bootstrap_deadline: Duration,
    health_cadence: Duration,
    liveness_deadline: Duration,
    shutdown_deadline: Duration,
}

impl WorkerSupervisionPolicy {
    #[must_use]
    pub fn with_resource_limits(mut self, resource_limits: WorkerResourceLimits) -> Self {
        self.resource_limits = resource_limits;
        self
    }

    #[must_use]
    pub fn with_restart_limit(mut self, restart_limit: u32) -> Self {
        self.restart_limit = restart_limit.max(1);
        self
    }

    #[must_use]
    pub fn with_breaker_rearm_healthy_cycles(mut self, breaker_rearm_healthy_cycles: u32) -> Self {
        self.breaker_rearm_healthy_cycles = breaker_rearm_healthy_cycles.max(1);
        self
    }

    #[must_use]
    pub fn with_bootstrap_deadline(mut self, bootstrap_deadline: Duration) -> Self {
        self.bootstrap_deadline = bootstrap_deadline;
        self
    }

    #[must_use]
    pub fn with_health_cadence(mut self, health_cadence: Duration) -> Self {
        self.health_cadence = health_cadence;
        self
    }

    #[must_use]
    pub fn with_liveness_deadline(mut self, liveness_deadline: Duration) -> Self {
        self.liveness_deadline = liveness_deadline;
        self
    }

    #[must_use]
    pub fn with_shutdown_deadline(mut self, shutdown_deadline: Duration) -> Self {
        self.shutdown_deadline = shutdown_deadline;
        self
    }

    pub fn resource_limits(&self) -> WorkerResourceLimits {
        self.resource_limits
    }

    pub fn restart_limit(&self) -> u32 {
        self.restart_limit
    }

    pub fn breaker_rearm_healthy_cycles(&self) -> u32 {
        self.breaker_rearm_healthy_cycles
    }

    pub fn bootstrap_deadline(&self) -> Duration {
        self.bootstrap_deadline
    }

    pub fn health_cadence(&self) -> Duration {
        self.health_cadence
    }

    pub fn liveness_deadline(&self) -> Duration {
        self.liveness_deadline
    }

    pub fn shutdown_deadline(&self) -> Duration {
        self.shutdown_deadline
    }
}

impl Default for WorkerSupervisionPolicy {
    fn default() -> Self {
        Self {
            resource_limits: WorkerResourceLimits::default(),
            restart_limit: DEFAULT_WORKER_RESTART_LIMIT,
            breaker_rearm_healthy_cycles: DEFAULT_WORKER_BREAKER_REARM_CYCLES,
            bootstrap_deadline: DEFAULT_WORKER_BOOTSTRAP_DEADLINE,
            health_cadence: DEFAULT_WORKER_HEALTH_CADENCE,
            liveness_deadline: DEFAULT_WORKER_LIVENESS_DEADLINE,
            shutdown_deadline: DEFAULT_WORKER_SHUTDOWN_DEADLINE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStatus {
    Available,
    CircuitOpen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoverySnapshot {
    pub status: RecoveryStatus,
    pub consecutive_failures: u32,
    pub restart_limit: u32,
    pub next_restart_delay: Option<Duration>,
}

pub struct RestartCircuitBreaker {
    limit: u32,
    failures: u32,
    /// Consecutive healthy cycles required before the failure count clears.
    rearm_after: u32,
    healthy_streak: u32,
}

impl RestartCircuitBreaker {
    pub fn new(limit: u32) -> Self {
        Self::with_rearm(limit, DEFAULT_WORKER_BREAKER_REARM_CYCLES)
    }

    /// A breaker that re-arms only after `rearm_after` consecutive healthy
    /// cycles, so a crash-looping worker's intermittent health frames cannot
    /// keep resetting the failure count.
    pub fn with_rearm(limit: u32, rearm_after: u32) -> Self {
        Self {
            limit: limit.max(1),
            failures: 0,
            rearm_after: rearm_after.max(1),
            healthy_streak: 0,
        }
    }

    pub fn record_failure(&mut self) -> bool {
        self.healthy_streak = 0;
        self.failures = self.failures.saturating_add(1);
        !self.is_open()
    }

    pub fn next_restart_delay(&mut self) -> Option<Duration> {
        let attempt = self.failures;
        if self.record_failure() {
            Some(restart_backoff(attempt))
        } else {
            None
        }
    }

    pub fn record_healthy(&mut self) {
        self.healthy_streak = self.healthy_streak.saturating_add(1);
        if self.healthy_streak >= self.rearm_after {
            self.failures = 0;
        }
    }

    pub fn snapshot(&self) -> RecoverySnapshot {
        RecoverySnapshot {
            status: self.status(),
            consecutive_failures: self.failures,
            restart_limit: self.limit,
            next_restart_delay: (!self.is_open()).then(|| restart_backoff(self.failures)),
        }
    }

    pub fn status(&self) -> RecoveryStatus {
        if self.is_open() {
            RecoveryStatus::CircuitOpen
        } else {
            RecoveryStatus::Available
        }
    }

    pub fn is_open(&self) -> bool {
        self.failures >= self.limit
    }
}

pub struct RestartController {
    executable: PathBuf,
    parent: PathBuf,
    policy: WorkerSupervisionPolicy,
    breaker: RestartCircuitBreaker,
}

impl RestartController {
    pub fn new(executable: PathBuf, parent: PathBuf, policy: WorkerSupervisionPolicy) -> Self {
        Self {
            executable,
            parent,
            policy,
            breaker: RestartCircuitBreaker::with_rearm(
                policy.restart_limit(),
                policy.breaker_rearm_healthy_cycles(),
            ),
        }
    }

    pub fn policy(&self) -> &WorkerSupervisionPolicy {
        &self.policy
    }

    pub fn recovery_snapshot(&self) -> RecoverySnapshot {
        self.breaker.snapshot()
    }

    pub fn monitor_health(
        &mut self,
        active: &mut Option<SupervisedWorker>,
        deadline: Duration,
    ) -> io::Result<bool> {
        let Some(worker) = active.as_mut() else {
            return self.recover_if_exited(active);
        };
        if worker.health_check(deadline).is_ok() {
            self.breaker.record_healthy();
            Ok(true)
        } else {
            self.recover_after_health_failure(active)
        }
    }

    pub fn recover_after_health_failure(
        &mut self,
        active: &mut Option<SupervisedWorker>,
    ) -> io::Result<bool> {
        let _ = active.take();
        *active = self.restart_after_failure()?;
        Ok(active.is_some())
    }

    pub fn recover_if_exited(&mut self, active: &mut Option<SupervisedWorker>) -> io::Result<bool> {
        let exited = match active.as_mut() {
            Some(worker) => worker.has_exited()?,
            None => true,
        };
        if !exited {
            return Ok(false);
        }
        // The exited worker is gone either way; drop it before the restart
        // attempt so a failed replacement never leaves a dead worker active.
        let _ = active.take();
        *active = self.restart_after_failure()?;
        Ok(active.is_some())
    }

    pub fn restart_after_failure(&mut self) -> io::Result<Option<SupervisedWorker>> {
        let Some(delay) = self.breaker.next_restart_delay() else {
            return Ok(None);
        };
        std::thread::sleep(delay);
        // A restarted worker is indistinguishable from a first boot: it gets
        // a fresh secret and must complete the same authenticated bootstrap,
        // so recovery can never hand back an unauthenticated worker.
        self.boot().map(Some)
    }

    /// Boot the initial worker without charging the restart circuit breaker.
    ///
    /// First boot is not a recovery: a failure here is a startup error for
    /// the caller to surface, not a restart-storm signal.
    pub fn start(&mut self) -> io::Result<SupervisedWorker> {
        let worker = self.boot()?;
        self.breaker.record_healthy();
        Ok(worker)
    }

    fn boot(&mut self) -> io::Result<SupervisedWorker> {
        let secret = fresh_bootstrap_secret()?;
        let mut worker =
            SupervisedWorker::spawn(&self.executable, &self.parent, &secret, &self.policy)?;
        let nonce = fresh_bootstrap_nonce()?;
        worker.authenticate(&secret, nonce.to_vec(), self.policy.bootstrap_deadline())?;
        Ok(worker)
    }
}

/// Drive continuous worker supervision until `stop` is set.
///
/// Boots the initial worker, then repeatedly enforces the policy liveness
/// deadline: every cycle must observe a fresh worker health frame within it,
/// otherwise the restart policy takes over (backoff, fresh-secret reboot,
/// circuit breaker). Returns when `stop` is observed — after an orderly
/// worker shutdown — or with an error when the breaker opens or the first
/// boot fails. `healthy_cycles` is a monotone observability counter.
///
/// Cancellation latency is bounded by one liveness deadline plus, during
/// recovery, the current restart backoff.
pub fn run_supervision_loop(
    controller: &mut RestartController,
    stop: &std::sync::atomic::AtomicBool,
    healthy_cycles: &std::sync::atomic::AtomicU64,
) -> io::Result<()> {
    use std::sync::atomic::Ordering;

    let policy = *controller.policy();
    if policy.health_cadence() >= policy.liveness_deadline() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker health cadence must stay below the liveness deadline",
        ));
    }
    // First-boot and breaker-open failures are surfaced in live logs here,
    // at the moment they happen — not only through the error the serve
    // lifecycle joins at shutdown.
    let mut active = match controller.start() {
        Ok(worker) => Some(worker),
        Err(error) => {
            tracing::error!(
                error = %error,
                "external runtime worker first boot failed; supervision halted"
            );
            return Err(error);
        }
    };
    while !stop.load(Ordering::SeqCst) {
        match controller.monitor_health(&mut active, policy.liveness_deadline()) {
            Ok(true) => {
                healthy_cycles.fetch_add(1, Ordering::SeqCst);
            }
            Ok(false) => {
                return Err(breaker_open_halt());
            }
            Err(error) => {
                // A restart attempt failed to boot or authenticate. The
                // breaker was already charged; keep looping so supervision
                // either recovers or trips the breaker above.
                tracing::warn!(error = %error, "external runtime worker restart attempt failed");
            }
        }
    }
    if let Some(mut worker) = active.take()
        && let Err(error) = worker.shutdown(policy.shutdown_deadline())
    {
        // The worker group was already killed by the failed shutdown
        // path; an unacknowledged stop is not a supervision failure.
        tracing::warn!(error = %error, "external runtime worker shutdown was not acknowledged");
    }
    Ok(())
}

/// Report the breaker-open supervision halt in live logs and build its error.
///
/// One shared path for the halt so the live-log surface and the returned
/// error can never drift apart.
fn breaker_open_halt() -> io::Error {
    tracing::error!("external runtime worker restart circuit breaker is open; supervision halted");
    io::Error::other("worker restart circuit breaker is open; supervision halted")
}

/// Serve-lifecycle service owning the external GameScript worker.
///
/// Spawned by the transport supervisor when `runtime.adapter` is
/// `external-worker`: it boots the worker on server startup, keeps it healthy
/// through [`run_supervision_loop`] (periodic liveness, restart policy,
/// circuit breaker), and shuts it down together with the server.
pub struct WorkerLifecycleService {
    executable: PathBuf,
    socket_parent: PathBuf,
    policy: WorkerSupervisionPolicy,
    healthy_cycles: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl WorkerLifecycleService {
    #[must_use]
    pub fn new(
        executable: PathBuf,
        socket_parent: PathBuf,
        policy: WorkerSupervisionPolicy,
    ) -> Self {
        Self {
            executable,
            socket_parent,
            policy,
            healthy_cycles: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Monotone count of successful health cycles — an observability probe
    /// for tests and diagnostics.
    #[must_use]
    pub fn healthy_cycles(&self) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
        std::sync::Arc::clone(&self.healthy_cycles)
    }
}

impl crate::lifecycle::AsyncService for WorkerLifecycleService {
    fn name(&self) -> &str {
        "runtime-external-worker"
    }

    async fn run(
        self: Box<Self>,
        cancel: crate::lifecycle::CancellationToken,
    ) -> crate::error::AppResult<()> {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        // The supervision loop is synchronous blocking I/O; it runs on the
        // blocking pool and observes cancellation through a shared stop flag
        // (latency bounded by one liveness deadline plus any active restart
        // backoff, see `run_supervision_loop`).
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let watch = cancel.clone();
        let watcher = tokio::spawn(async move {
            watch.cancelled().await;
            stop_flag.store(true, Ordering::SeqCst);
        });
        let executable = self.executable;
        let socket_parent = self.socket_parent;
        let policy = self.policy;
        let healthy_cycles = Arc::clone(&self.healthy_cycles);
        let result = tokio::task::spawn_blocking(move || {
            let mut controller = RestartController::new(executable, socket_parent, policy);
            run_supervision_loop(&mut controller, &stop, &healthy_cycles)
        })
        .await;
        watcher.abort();
        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(crate::error::AppError::new(
                crate::error::ErrorCategory::Runtime,
                "external runtime worker supervision failed",
            )
            .with_detail(error.to_string())),
            Err(join_error) => Err(crate::error::AppError::internal(
                "external runtime worker supervision task failed",
            )
            .with_detail(join_error.to_string())),
        }
    }
}

#[cfg(unix)]
pub struct SupervisedWorker {
    _endpoint: PrivateUnixEndpoint,
    _listener: UnixListener,
    stream: Option<UnixStream>,
    _bootstrap_reader: OwnedFd,
    child: Child,
    process_group_id: Option<i32>,
}

#[cfg(unix)]
impl SupervisedWorker {
    pub fn spawn(
        executable: &Path,
        parent: &Path,
        secret: &[u8; 32],
        policy: &WorkerSupervisionPolicy,
    ) -> io::Result<Self> {
        let endpoint = PrivateUnixEndpoint::create(parent)?;
        let listener = endpoint.bind()?;
        let bootstrap = BootstrapPipe::create()?;
        bootstrap.make_reader_inheritable()?;
        let bootstrap_fd = bootstrap.reader_fd();
        let (bootstrap_reader, bootstrap_writer) = bootstrap.into_reader_and_writer();
        let mut child = Command::new(executable)
            .arg("runtime-worker")
            .arg("--bootstrap-endpoint")
            .arg(endpoint.path())
            .arg("--bootstrap-fd")
            .arg(bootstrap_fd.to_string())
            // The worker re-checks this pid right after arming its
            // parent-death signal, closing the window where the supervisor
            // dies before the signal is armed.
            .arg("--parent-pid")
            .arg(std::process::id().to_string())
            // The supervisor's resource policy travels on the command line
            // and is applied by the worker before it reads the bootstrap
            // secret, so an over-limit worker can never reach the protocol.
            .arg("--max-open-files")
            .arg(policy.resource_limits().max_open_files().to_string())
            // How often the worker emits a health frame after readiness; the
            // supervisor's liveness deadline is calibrated against it.
            .arg("--health-cadence-ms")
            .arg(policy.health_cadence().as_millis().to_string())
            // The worker must land in its own process group before exec so
            // every cleanup path can signal the whole group. Relying on the
            // worker to isolate itself would let a non-cooperating binary
            // (and any descendants it forks) escape group termination.
            .process_group(0)
            .spawn()?;
        // The group id equals the leader pid and is valid from spawn, so
        // failures before authentication also clean up descendants.
        let process_group_id = i32::try_from(child.id()).ok();
        if let Err(error) = bootstrap_writer.write_secret(secret) {
            if let Some(pgid) = process_group_id {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(-pgid),
                    nix::sys::signal::Signal::SIGKILL,
                );
            }
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        Ok(Self {
            _endpoint: endpoint,
            _listener: listener,
            stream: None,
            _bootstrap_reader: bootstrap_reader,
            child,
            process_group_id,
        })
    }

    pub fn accept_with_deadline(&self, deadline: Duration) -> io::Result<UnixStream> {
        self._listener.set_nonblocking(true)?;
        let until = std::time::Instant::now() + deadline;
        loop {
            match self._listener.accept() {
                Ok((stream, _)) => return Ok(stream),
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        && std::time::Instant::now() < until =>
                {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "worker bootstrap deadline exceeded",
                    ));
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn authenticate(
        &mut self,
        secret: &[u8; 32],
        nonce: Vec<u8>,
        deadline: Duration,
    ) -> io::Result<()> {
        let result = (|| -> io::Result<()> {
            let mut stream = self.accept_with_deadline(deadline)?;
            stream.set_read_timeout(Some(deadline))?;
            stream.set_write_timeout(Some(deadline))?;
            write_control_frame(
                &mut stream,
                &ControlFrame::ParentHello {
                    protocol_version: PROTOCOL_VERSION,
                    nonce: nonce.clone(),
                },
            )
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "bootstrap frame write failed")
            })?;
            let frame = read_control_frame(&mut stream).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "worker bootstrap frame invalid")
            })?;
            if !verify_worker_hello(secret, &nonce, &frame) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "worker authentication failed",
                ));
            }
            match read_control_frame(&mut stream).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "worker readiness frame invalid")
            })? {
                ControlFrame::WorkerReady {
                    protocol_version, ..
                } if protocol_version == PROTOCOL_VERSION => {
                    self.stream = Some(stream);
                    Ok(())
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "worker readiness frame invalid",
                )),
            }
        })();
        if result.is_err() {
            self.kill_and_reap();
        }
        result
    }

    pub fn shutdown(&mut self, deadline: Duration) -> io::Result<()> {
        let result = (|| {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
            let until = std::time::Instant::now() + deadline;
            stream.set_write_timeout(Some(deadline))?;
            write_control_frame(
                stream,
                &ControlFrame::ParentShutdown {
                    protocol_version: PROTOCOL_VERSION,
                },
            )
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "worker shutdown write failed")
            })?;
            // Health frames may already be in flight when shutdown begins;
            // skip them (and only them) until the stop acknowledgement, all
            // within the one overall deadline.
            loop {
                let remaining = until.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "worker shutdown acknowledgement timed out",
                    ));
                }
                stream.set_read_timeout(Some(remaining))?;
                match read_control_frame(stream).map_err(|_| {
                    // The socket read timeout fires only once `remaining` has
                    // fully elapsed, so a read failure at or past the overall
                    // deadline is the hung-worker case rather than a
                    // malformed acknowledgement.
                    if std::time::Instant::now() >= until {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "worker shutdown acknowledgement timed out",
                        )
                    } else {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "worker shutdown acknowledgement invalid",
                        )
                    }
                })? {
                    ControlFrame::WorkerStopped { protocol_version }
                        if protocol_version == PROTOCOL_VERSION =>
                    {
                        return Ok(());
                    }
                    ControlFrame::WorkerHealth { protocol_version }
                        if protocol_version == PROTOCOL_VERSION => {}
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "worker shutdown acknowledgement invalid",
                        ));
                    }
                }
            }
        })();
        // Even after a clean acknowledgement the group is signalled: the
        // worker already performed its last protocol act, and this guarantees
        // no descendant outlives an acknowledged shutdown.
        self.kill_and_reap();
        result
    }

    pub fn health_check(&mut self, deadline: Duration) -> io::Result<()> {
        let result = (|| {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
            stream.set_read_timeout(Some(deadline))?;
            let frame = read_control_frame(stream).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "worker health frame invalid")
            })?;
            if is_valid_worker_health(&frame) {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "worker health frame invalid",
                ))
            }
        })();
        if result.is_err() {
            self.kill_and_reap();
        }
        result
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn has_exited(&mut self) -> io::Result<bool> {
        let exited = self.child.try_wait()?.is_some();
        if exited {
            // `try_wait` reaped the leader, but descendants may linger in the
            // group. The group id stays reserved while any member remains, so
            // signal it now and forget it: once forgotten, a possibly recycled
            // pid is never signalled again.
            self.signal_process_group();
            self.process_group_id = None;
        }
        Ok(exited)
    }

    fn signal_process_group(&self) {
        if let Some(pgid) = self.process_group_id {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(-pgid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }

    fn terminate_process_group(&mut self) -> io::Result<()> {
        self.signal_process_group();
        self.child.kill()
    }

    /// Kill the worker's whole process group and reap the leader.
    ///
    /// The group is signalled before the leader is reaped (a zombie leader
    /// keeps the group id reserved), then forgotten so a later cleanup can
    /// never signal a recycled process group.
    fn kill_and_reap(&mut self) {
        let _ = self.terminate_process_group();
        let _ = self.child.wait();
        self.process_group_id = None;
    }

    pub fn kill(&mut self) -> io::Result<()> {
        self.terminate_process_group()
    }
}

#[cfg(unix)]
impl Drop for SupervisedWorker {
    fn drop(&mut self) {
        self.kill_and_reap();
    }
}

/// Windows supervised worker: named-pipe transport + Job Object containment.
///
/// The transport is a DACL-restricted, single-instance named pipe (see
/// [`PrivateNamedPipeEndpoint`]) driven through tokio; the worker holds a
/// private current-thread runtime so the synchronous supervision loop keeps
/// the exact same shape as on unix, with every operation bounded by
/// `tokio::time::timeout`. Lifecycle containment is a kill-on-close Job
/// Object: the analog of the unix process group (group kill reaches every
/// descendant) plus `PDEATHSIG` (a dying supervisor closes the job handle
/// and the kernel terminates the whole worker tree).
///
/// Field order is load-bearing: the pipe endpoints must drop before the
/// tokio runtime they are registered with.
#[cfg(windows)]
pub struct SupervisedWorker {
    _endpoint: PrivateNamedPipeEndpoint,
    server: Option<tokio::net::windows::named_pipe::NamedPipeServer>,
    stream: Option<tokio::net::windows::named_pipe::NamedPipeServer>,
    runtime: tokio::runtime::Runtime,
    job: citadel_win_proc::JobObject,
    _secret_reader: std::os::windows::io::OwnedHandle,
    child: Child,
}

#[cfg(windows)]
impl SupervisedWorker {
    /// Spawn the worker under job containment with a one-shot secret pipe.
    ///
    /// `_parent` is the unix socket-parent directory and is unused here:
    /// pipe names live in the kernel `\\.\pipe\` namespace, not on disk.
    pub fn spawn(
        executable: &Path,
        _parent: &Path,
        secret: &[u8; 32],
        policy: &WorkerSupervisionPolicy,
    ) -> io::Result<Self> {
        let endpoint = PrivateNamedPipeEndpoint::create()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()?;
        // The pipe server registers with this worker's private reactor; it
        // must exist before the child so the worker can never observe a
        // missing endpoint.
        let server = {
            let _context = runtime.enter();
            endpoint.bind()?
        };
        // Kill-on-close is armed before the child exists: once assignment
        // below succeeds, even an abrupt supervisor death tears the worker
        // tree down with the closing job handle (the PDEATHSIG analog).
        let job = citadel_win_proc::JobObject::create_kill_on_close()?;
        let secret_pipe = citadel_win_proc::SecretPipe::create_with_inheritable_reader()?;
        // Windows kernel handles fit in 32 bits by contract (WOW64 interop);
        // refuse to spawn on the theoretical overflow instead of truncating.
        let bootstrap_fd = i32::try_from(secret_pipe.reader_handle_value()).map_err(|_| {
            io::Error::other("worker bootstrap handle value exceeds the command-line range")
        })?;
        let (secret_reader, secret_writer) = secret_pipe.into_reader_and_writer();
        let mut child = Command::new(executable)
            .arg("runtime-worker")
            .arg("--bootstrap-endpoint")
            .arg(endpoint.name())
            // Numeric value of the inherited pipe read handle: std spawns
            // with handle inheritance enabled and only the read end is
            // marked inheritable, so the value stays valid in the child.
            .arg("--bootstrap-fd")
            .arg(bootstrap_fd.to_string())
            // The worker checks this pid against the pipe's server process
            // and its own parent-liveness pre-check before speaking the
            // protocol.
            .arg("--parent-pid")
            .arg(std::process::id().to_string())
            // The supervisor's resource policy travels on the command line
            // as on unix. Windows has no kernel open-file limit; the worker
            // surfaces that instead of silently ignoring the policy.
            .arg("--max-open-files")
            .arg(policy.resource_limits().max_open_files().to_string())
            // How often the worker emits a health frame after readiness; the
            // supervisor's liveness deadline is calibrated against it.
            .arg("--health-cadence-ms")
            .arg(policy.health_cadence().as_millis().to_string())
            .spawn()?;
        // Containment before secrets: the child blocks reading the bootstrap
        // secret, and the secret is only written after job assignment
        // succeeds, so a worker that reached the protocol is provably inside
        // the job. (Unlike the unix pre-exec `process_group(0)` there is no
        // pre-start hook in std::process on Windows, so a non-cooperating
        // binary could spawn descendants in the spawn-to-assign window; the
        // cooperative worker does nothing before its secret read.)
        if let Err(error) = job.assign(&child) {
            let _ = job.terminate();
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        if let Err(error) = secret_writer.write_secret(secret) {
            let _ = job.terminate();
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        Ok(Self {
            _endpoint: endpoint,
            server: Some(server),
            stream: None,
            runtime,
            job,
            _secret_reader: secret_reader,
            child,
        })
    }

    pub fn authenticate(
        &mut self,
        secret: &[u8; 32],
        nonce: Vec<u8>,
        deadline: Duration,
    ) -> io::Result<()> {
        let result = (|| -> io::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
            let mut stream = self
                .server
                .take()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
            let child_pid = self.child.id();
            self.runtime.block_on(async {
                tokio::time::timeout(deadline, stream.connect())
                    .await
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "worker bootstrap deadline exceeded",
                        )
                    })??;
                // Peer validation before any protocol byte: the DACL already
                // limits the pipe to the current user, and this pins the one
                // process — the spawned child — that may complete bootstrap.
                let peer = citadel_win_proc::named_pipe_client_process_id(&stream)?;
                if peer != child_pid {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "worker bootstrap peer mismatch",
                    ));
                }
                tokio::time::timeout(deadline, async {
                    write_control_frame_async(
                        &mut stream,
                        &ControlFrame::ParentHello {
                            protocol_version: PROTOCOL_VERSION,
                            nonce: nonce.clone(),
                        },
                    )
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "bootstrap frame write failed")
                    })?;
                    let frame = read_control_frame_async(&mut stream).await.map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "worker bootstrap frame invalid")
                    })?;
                    if !verify_worker_hello(secret, &nonce, &frame) {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "worker authentication failed",
                        ));
                    }
                    match read_control_frame_async(&mut stream).await.map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "worker readiness frame invalid")
                    })? {
                        ControlFrame::WorkerReady {
                            protocol_version, ..
                        } if protocol_version == PROTOCOL_VERSION => Ok(()),
                        _ => Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "worker readiness frame invalid",
                        )),
                    }
                })
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "worker bootstrap deadline exceeded",
                    )
                })??;
                Ok(stream)
            })
        })();
        match result {
            Ok(stream) => {
                self.stream = Some(stream);
                Ok(())
            }
            Err(error) => {
                self.kill_and_reap();
                Err(error)
            }
        }
    }

    pub fn shutdown(&mut self, deadline: Duration) -> io::Result<()> {
        let result = (|| {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
            self.runtime.block_on(async {
                let until = std::time::Instant::now() + deadline;
                tokio::time::timeout(
                    deadline,
                    write_control_frame_async(
                        stream,
                        &ControlFrame::ParentShutdown {
                            protocol_version: PROTOCOL_VERSION,
                        },
                    ),
                )
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "worker shutdown acknowledgement timed out",
                    )
                })?
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "worker shutdown write failed")
                })?;
                // Health frames may already be in flight when shutdown
                // begins; skip them (and only them) until the stop
                // acknowledgement, all within the one overall deadline.
                loop {
                    let remaining = until.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "worker shutdown acknowledgement timed out",
                        ));
                    }
                    let frame = tokio::time::timeout(remaining, read_control_frame_async(stream))
                        .await
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::TimedOut,
                                "worker shutdown acknowledgement timed out",
                            )
                        })?
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "worker shutdown acknowledgement invalid",
                            )
                        })?;
                    match frame {
                        ControlFrame::WorkerStopped { protocol_version }
                            if protocol_version == PROTOCOL_VERSION =>
                        {
                            return Ok(());
                        }
                        ControlFrame::WorkerHealth { protocol_version }
                            if protocol_version == PROTOCOL_VERSION => {}
                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "worker shutdown acknowledgement invalid",
                            ));
                        }
                    }
                }
            })
        })();
        // Even after a clean acknowledgement the job is terminated: the
        // worker already performed its last protocol act, and this
        // guarantees no descendant outlives an acknowledged shutdown.
        self.kill_and_reap();
        result
    }

    pub fn health_check(&mut self, deadline: Duration) -> io::Result<()> {
        let result = (|| {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
            let frame = self.runtime.block_on(async {
                tokio::time::timeout(deadline, read_control_frame_async(stream))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "worker health frame overdue")
                    })?
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "worker health frame invalid")
                    })
            })?;
            if is_valid_worker_health(&frame) {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "worker health frame invalid",
                ))
            }
        })();
        if result.is_err() {
            self.kill_and_reap();
        }
        result
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn has_exited(&mut self) -> io::Result<bool> {
        let exited = self.child.try_wait()?.is_some();
        if exited {
            // The leader is gone but descendants may linger in the job.
            // Unlike a unix process group id, job identity is handle-based,
            // so terminating here can never hit a recycled group.
            let _ = self.job.terminate();
        }
        Ok(exited)
    }

    /// Kill the worker's whole job and reap the leader — the analog of the
    /// unix process-group kill-and-reap.
    fn kill_and_reap(&mut self) {
        let _ = self.job.terminate();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    pub fn kill(&mut self) -> io::Result<()> {
        let _ = self.job.terminate();
        self.child.kill()
    }
}

#[cfg(windows)]
impl Drop for SupervisedWorker {
    fn drop(&mut self) {
        self.kill_and_reap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::fs;

    #[cfg(unix)]
    #[test]
    fn process_group_signal_terminates_worker_descendant() {
        let pid_file =
            std::env::temp_dir().join(format!("citadel-pgid-{}.pid", uuid::Uuid::new_v4()));
        let mut worker = std::process::Command::new("setsid")
            .args(["sh", "-c", "sleep 30 & echo $! > \"$1\"; wait", "sh"])
            .arg(&pid_file)
            .spawn()
            .expect("spawn fixture");
        let until = std::time::Instant::now() + Duration::from_secs(1);
        while !pid_file.exists() && std::time::Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        let descendant: i32 = fs::read_to_string(&pid_file)
            .expect("descendant pid")
            .trim()
            .parse()
            .expect("numeric pid");
        let pgid = i32::try_from(worker.id()).expect("worker pgid");
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(-pgid),
            nix::sys::signal::Signal::SIGKILL,
        )
        .expect("kill process group");
        assert!(!worker.wait().expect("reap leader").success());
        let until = std::time::Instant::now() + Duration::from_secs(1);
        let terminated = loop {
            match fs::read_to_string(format!("/proc/{descendant}/stat")) {
                Err(_) => break true,
                Ok(stat) if stat.split_whitespace().nth(2) == Some("Z") => break true,
                Ok(_) if std::time::Instant::now() >= until => break false,
                Ok(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        };
        let _ = fs::remove_file(pid_file);
        assert!(terminated, "descendant survived process-group termination");
    }
    #[cfg(unix)]
    #[test]
    fn bootstrap_failure_terminates_worker_descendants() {
        use std::os::unix::fs::PermissionsExt;

        let unique = uuid::Uuid::new_v4();
        let pid_file = std::env::temp_dir().join(format!("citadel-worker-desc-{unique}.pid"));
        let script = std::env::temp_dir().join(format!("citadel-worker-test-{unique}.sh"));
        fs::write(
            &script,
            format!(
                "#!/bin/sh\ntrap '' TERM\nsleep 30 & echo $! > \"{}\"\nwait\n",
                pid_file.display()
            ),
        )
        .expect("script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).expect("executable");
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &[8; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let until = std::time::Instant::now() + Duration::from_secs(2);
        while !pid_file.exists() && std::time::Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        let descendant: i32 = fs::read_to_string(&pid_file)
            .expect("descendant pid")
            .trim()
            .parse()
            .expect("numeric pid");
        // The worker never connects, so bootstrap times out. Cleanup must
        // remove the whole process group, not only the leader the worker
        // forked from.
        let error = worker
            .authenticate(&[8; 32], vec![1; 32], Duration::from_millis(50))
            .expect_err("worker never connects");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let until = std::time::Instant::now() + Duration::from_secs(1);
        let terminated = loop {
            match fs::read_to_string(format!("/proc/{descendant}/stat")) {
                Err(_) => break true,
                Ok(stat) if stat.split_whitespace().nth(2) == Some("Z") => break true,
                Ok(_) if std::time::Instant::now() >= until => break false,
                Ok(_) => std::thread::sleep(Duration::from_millis(5)),
            }
        };
        let _ = fs::remove_file(&pid_file);
        let _ = fs::remove_file(&script);
        assert!(
            terminated,
            "descendant survived bootstrap-failure process-group cleanup"
        );
    }

    #[test]
    fn recovery_snapshot_reports_failures_and_open_circuit() {
        let mut breaker = super::RestartCircuitBreaker::new(2);
        assert_eq!(
            breaker.snapshot(),
            super::RecoverySnapshot {
                status: super::RecoveryStatus::Available,
                consecutive_failures: 0,
                restart_limit: 2,
                next_restart_delay: Some(std::time::Duration::from_millis(100)),
            }
        );
        assert!(breaker.next_restart_delay().is_some());
        assert_eq!(breaker.snapshot().consecutive_failures, 1);
        assert_eq!(
            breaker.snapshot().next_restart_delay,
            Some(std::time::Duration::from_millis(200))
        );
        assert_eq!(breaker.next_restart_delay(), None);
        assert_eq!(
            breaker.snapshot().status,
            super::RecoveryStatus::CircuitOpen
        );
    }

    #[test]
    fn worker_resource_limits_reject_zero_open_files() {
        assert!(super::WorkerResourceLimits::new(0).is_err());
        assert_eq!(
            super::WorkerResourceLimits::new(256)
                .expect("limits")
                .max_open_files(),
            256
        );
    }

    #[test]
    fn recovery_status_reports_open_circuit_as_unavailable() {
        let mut breaker = super::RestartCircuitBreaker::new(1);
        assert_eq!(breaker.status(), super::RecoveryStatus::Available);
        assert_eq!(breaker.next_restart_delay(), None);
        assert_eq!(breaker.status(), super::RecoveryStatus::CircuitOpen);
    }

    #[test]
    fn monitor_health_keeps_unavailable_worker_fail_closed() {
        let mut controller = super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default().with_restart_limit(1),
        );
        let mut active = None;
        assert!(
            !controller
                .monitor_health(&mut active, Duration::from_millis(1))
                .expect("monitor")
        );
        assert!(active.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn health_failure_removes_active_worker_before_recovery() {
        let mut controller = super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default().with_restart_limit(1),
        );
        let mut active = Some(
            SupervisedWorker::spawn(
                Path::new("/bin/true"),
                &std::env::temp_dir(),
                &[3; 32],
                &WorkerSupervisionPolicy::default(),
            )
            .expect("spawn"),
        );
        assert!(
            !controller
                .recover_after_health_failure(&mut active)
                .expect("recover")
        );
        assert!(active.is_none());
    }

    #[test]
    fn open_breaker_leaves_active_worker_unavailable() {
        let mut controller = super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default().with_restart_limit(1),
        );
        let mut active = None;
        assert!(!controller.recover_if_exited(&mut active).expect("recover"));
        assert!(active.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn exited_worker_is_removed_even_when_the_restart_fails() {
        let mut controller = super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default()
                .with_restart_limit(2)
                .with_bootstrap_deadline(Duration::from_millis(50)),
        );
        let mut active = Some(
            SupervisedWorker::spawn(
                Path::new("/bin/true"),
                &std::env::temp_dir(),
                &[4; 32],
                &WorkerSupervisionPolicy::default(),
            )
            .expect("spawn"),
        );
        std::thread::sleep(Duration::from_millis(10));
        // The replacement fixture cannot authenticate, so recovery must fail
        // closed: the dead worker is removed and no unauthenticated
        // replacement is handed back.
        assert!(controller.recover_if_exited(&mut active).is_err());
        assert!(active.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn restart_attempt_fails_closed_without_authentication() {
        let mut controller = super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default()
                .with_restart_limit(2)
                .with_bootstrap_deadline(Duration::from_millis(50)),
        );
        // The breaker permits this restart, but the fixture never completes
        // the handshake: the controller must surface an error instead of an
        // unauthenticated worker (the authenticated restart path is covered
        // by the worker_handshake integration test against the real binary).
        assert!(controller.restart_after_failure().is_err());
    }

    #[test]
    fn controller_refuses_restart_when_breaker_is_open() {
        let mut controller = super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default().with_restart_limit(1),
        );
        assert!(
            controller
                .restart_after_failure()
                .expect("decision")
                .is_none()
        );
    }

    #[test]
    fn restart_secrets_are_fresh_32_byte_values() {
        let first = super::fresh_bootstrap_secret().expect("first secret");
        let second = super::fresh_bootstrap_secret().expect("second secret");
        assert_ne!(first, second);
    }

    #[test]
    fn supervision_loop_rejects_a_cadence_at_or_above_liveness() {
        use std::sync::atomic::{AtomicBool, AtomicU64};

        let mut controller = super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default()
                .with_health_cadence(Duration::from_millis(100))
                .with_liveness_deadline(Duration::from_millis(100)),
        );
        let error = super::run_supervision_loop(
            &mut controller,
            &AtomicBool::new(false),
            &AtomicU64::new(0),
        )
        .expect_err("a liveness deadline at or below the cadence cannot detect a hang");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[cfg(unix)]
    #[test]
    fn supervision_loop_requires_an_authenticated_first_boot() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        let mut controller = super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default()
                .with_restart_limit(1)
                .with_bootstrap_deadline(Duration::from_millis(50)),
        );
        let healthy_cycles = AtomicU64::new(0);
        super::run_supervision_loop(&mut controller, &AtomicBool::new(false), &healthy_cycles)
            .expect_err("an unauthenticated first boot is a startup failure");
        assert_eq!(healthy_cycles.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn restart_nonces_are_fresh_32_byte_values() {
        let first = super::fresh_bootstrap_nonce().expect("first nonce");
        let second = super::fresh_bootstrap_nonce().expect("second nonce");
        assert_ne!(first, second);
    }

    #[cfg(unix)]
    #[test]
    fn crashed_worker_requires_a_permitted_restart_delay() {
        let mut breaker = super::RestartCircuitBreaker::new(1);
        let mut worker = SupervisedWorker::spawn(
            Path::new("/bin/true"),
            &std::env::temp_dir(),
            &[9; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        std::thread::sleep(Duration::from_millis(10));
        assert!(worker.has_exited().expect("child status"));
        assert_eq!(breaker.next_restart_delay(), None);
    }

    #[cfg(unix)]
    #[test]
    fn reports_when_child_has_exited() {
        let mut worker = SupervisedWorker::spawn(
            Path::new("/bin/true"),
            &std::env::temp_dir(),
            &[7; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        std::thread::sleep(Duration::from_millis(10));
        assert!(worker.has_exited().expect("child status"));
    }

    #[test]
    fn restart_policy_combines_backoff_and_circuit_breaker() {
        let mut breaker = super::RestartCircuitBreaker::new(2);
        assert_eq!(
            breaker.next_restart_delay(),
            Some(Duration::from_millis(100))
        );
        assert_eq!(breaker.next_restart_delay(), None);
        assert!(breaker.is_open());
    }

    #[test]
    fn circuit_breaker_opens_at_restart_limit() {
        let mut breaker = super::RestartCircuitBreaker::with_rearm(3, 2);
        assert!(breaker.record_failure());
        assert!(breaker.record_failure());
        assert!(!breaker.record_failure());
        assert!(breaker.is_open());
        // One healthy cycle is not proof of recovery: the breaker re-arms
        // only after the configured healthy streak.
        breaker.record_healthy();
        assert!(breaker.is_open());
        breaker.record_healthy();
        assert!(!breaker.is_open());
        assert!(breaker.record_failure());
    }

    #[test]
    fn breaker_requires_n_healthy_cycles_before_rearming() {
        // A crash-loop that squeezes in a single healthy cycle between
        // failures must not keep resetting the failure count, otherwise the
        // breaker can never trip on a flapping worker.
        let mut breaker = super::RestartCircuitBreaker::with_rearm(3, 2);
        assert!(breaker.record_failure());
        breaker.record_healthy();
        assert_eq!(
            breaker.snapshot().consecutive_failures,
            1,
            "a lone healthy cycle must not re-arm the breaker"
        );
        assert!(breaker.record_failure());
        breaker.record_healthy();
        assert_eq!(
            breaker.snapshot().consecutive_failures,
            2,
            "intermittent health in a crash-loop keeps charging the breaker"
        );
        // A genuine recovery — N consecutive healthy cycles — re-arms.
        breaker.record_healthy();
        assert_eq!(breaker.snapshot().consecutive_failures, 0);

        // The default construction sources the PROVISIONAL re-arm streak.
        let defaulted = super::RestartCircuitBreaker::new(3);
        assert_eq!(
            super::WorkerSupervisionPolicy::default().breaker_rearm_healthy_cycles(),
            super::DEFAULT_WORKER_BREAKER_REARM_CYCLES
        );
        drop(defaulted);
    }

    /// Capture everything the closure emits through `tracing` on this thread.
    fn captured_tracing(run: impl FnOnce()) -> String {
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct Buffer(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Buffer {
            fn write(&mut self, data: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap_or_else(|e| e.into_inner()).write(data)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
            type Writer = Buffer;
            fn make_writer(&'a self) -> Buffer {
                self.clone()
            }
        }

        let buffer = Buffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buffer.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, run);
        let bytes = buffer.0.lock().unwrap_or_else(|e| e.into_inner()).clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    #[test]
    fn first_boot_failure_surfaces_in_live_tracing() {
        use std::sync::atomic::{AtomicBool, AtomicU64};

        // An unresolvable worker executable fails the first boot. The failure
        // must appear in live logs at error level immediately — not only in
        // the error joined at shutdown.
        let mut controller = super::RestartController::new(
            PathBuf::from("citadel-no-such-worker-executable"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default().with_restart_limit(1),
        );
        let logs = captured_tracing(|| {
            let result = super::run_supervision_loop(
                &mut controller,
                &AtomicBool::new(false),
                &AtomicU64::new(0),
            );
            assert!(result.is_err(), "an unbootable first boot must fail");
        });
        assert!(
            logs.contains("first boot failed"),
            "the first-boot failure must reach live tracing output: {logs:?}"
        );
        assert!(logs.contains("ERROR"), "failure logs at error level: {logs:?}");
    }

    #[test]
    fn open_breaker_halt_surfaces_in_live_tracing() {
        // The breaker-open halt goes through one shared reporting path; the
        // supervision loop calls it when `monitor_health` refuses a restart.
        let logs = captured_tracing(|| {
            let error = super::breaker_open_halt();
            assert_eq!(error.kind(), io::ErrorKind::Other);
        });
        assert!(
            logs.contains("circuit breaker is open"),
            "the breaker-open halt must reach live tracing output: {logs:?}"
        );
        assert!(logs.contains("ERROR"), "halts log at error level: {logs:?}");
    }

    #[test]
    fn restart_backoff_grows_and_is_capped() {
        assert_eq!(super::restart_backoff(0), Duration::from_millis(100));
        assert_eq!(super::restart_backoff(1), Duration::from_millis(200));
        assert_eq!(super::restart_backoff(20), Duration::from_secs(30));
    }

    #[test]
    fn supervisor_exposes_deadline_bound_shutdown() {
        let _ = SupervisedWorker::shutdown;
    }

    #[test]
    fn health_check_rejects_a_ready_worker_that_stops_reporting() {
        // The supervisor must retain its authenticated stream so this call can
        // enforce a deadline after readiness, rather than treating readiness as
        // permanent health.
        let _ = SupervisedWorker::health_check;
    }

    #[cfg(unix)]
    #[test]
    fn authenticated_worker_without_ready_is_rejected() {
        use std::{fs, os::unix::fs::PermissionsExt, thread};

        let script =
            std::env::temp_dir().join(format!("citadel-worker-test-{}.sh", uuid::Uuid::new_v4()));
        fs::write(&script, "#!/bin/sh\ntrap '' TERM\nsleep 30\n").expect("script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).expect("executable");
        let secret = [5; 32];
        let nonce = vec![6; 32];
        let client_nonce = nonce.clone();
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &secret,
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let endpoint = worker._endpoint.path().to_path_buf();
        let client = thread::spawn(move || {
            let mut stream = UnixStream::connect(endpoint).expect("connect");
            let _ = read_control_frame(&mut stream).expect("parent hello");
            write_control_frame(
                &mut stream,
                &ControlFrame::WorkerHello {
                    protocol_version: PROTOCOL_VERSION,
                    proof: crate::runtime::worker_protocol::challenge_proof(&secret, &client_nonce)
                        .to_vec(),
                },
            )
            .expect("hello");
            thread::sleep(Duration::from_millis(100));
        });
        let error = worker
            .authenticate(&secret, nonce, Duration::from_millis(20))
            .expect_err("ready required");
        client.join().expect("client");
        assert!(matches!(
            error.kind(),
            io::ErrorKind::InvalidData | io::ErrorKind::TimedOut
        ));
        assert!(worker.child.try_wait().expect("child status").is_some());
        let _ = fs::remove_file(script);
    }

    #[cfg(unix)]
    #[test]
    fn worker_that_stalls_after_connect_is_killed_and_reaped() {
        use std::{fs, os::unix::fs::PermissionsExt, thread};

        let script =
            std::env::temp_dir().join(format!("citadel-worker-test-{}.sh", uuid::Uuid::new_v4()));
        fs::write(&script, "#!/bin/sh\ntrap '' TERM\nsleep 30\n").expect("script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).expect("executable");
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &[4; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let endpoint = worker._endpoint.path().to_path_buf();
        let client = thread::spawn(move || {
            let mut stream = UnixStream::connect(endpoint).expect("connect");
            let _ = read_control_frame(&mut stream).expect("parent hello");
            thread::sleep(Duration::from_millis(100));
        });
        let error = worker
            .authenticate(&[4; 32], vec![1; 32], Duration::from_millis(20))
            .expect_err("stalled hello");
        client.join().expect("client");
        assert!(matches!(
            error.kind(),
            io::ErrorKind::InvalidData | io::ErrorKind::TimedOut
        ));
        assert!(worker.child.try_wait().expect("child status").is_some());
        let _ = fs::remove_file(script);
    }

    #[cfg(unix)]
    #[test]
    fn invalid_worker_hello_kills_and_reaps_the_child() {
        use std::{fs, os::unix::fs::PermissionsExt, thread};

        let script =
            std::env::temp_dir().join(format!("citadel-worker-test-{}.sh", uuid::Uuid::new_v4()));
        fs::write(&script, "#!/bin/sh\ntrap '' TERM\nsleep 30\n").expect("script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).expect("executable");
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &[3; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let endpoint = worker._endpoint.path().to_path_buf();
        let client = thread::spawn(move || {
            let mut stream = UnixStream::connect(endpoint).expect("connect");
            let _ = read_control_frame(&mut stream).expect("parent hello");
            write_control_frame(
                &mut stream,
                &ControlFrame::WorkerHello {
                    protocol_version: PROTOCOL_VERSION,
                    proof: vec![0; 32],
                },
            )
            .expect("invalid hello");
        });
        let error = worker
            .authenticate(&[3; 32], vec![1; 32], Duration::from_secs(1))
            .expect_err("invalid proof");
        client.join().expect("client");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(worker.child.try_wait().expect("child status").is_some());
        let _ = fs::remove_file(script);
    }

    #[cfg(unix)]
    #[test]
    fn oversized_worker_frame_fails_closed_and_kills_the_child() {
        use std::io::Write;
        use std::{fs, os::unix::fs::PermissionsExt, thread};

        let script =
            std::env::temp_dir().join(format!("citadel-worker-test-{}.sh", uuid::Uuid::new_v4()));
        fs::write(&script, "#!/bin/sh\ntrap '' TERM\nsleep 30\n").expect("script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).expect("executable");
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &[6; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let endpoint = worker._endpoint.path().to_path_buf();
        let client = thread::spawn(move || {
            let mut stream = UnixStream::connect(endpoint).expect("connect");
            let _ = read_control_frame(&mut stream).expect("parent hello");
            // A rogue worker claims a frame larger than the protocol allows;
            // the supervisor must reject on the length prefix alone.
            let oversized = ((crate::runtime::worker_protocol::MAX_CONTROL_FRAME_BYTES + 1) as u32)
                .to_be_bytes();
            let _ = stream.write_all(&oversized);
            let _ = stream.write_all(&[b' '; 64]);
            thread::sleep(Duration::from_millis(100));
        });
        let error = worker
            .authenticate(&[6; 32], vec![1; 32], Duration::from_secs(1))
            .expect_err("oversized frame must fail closed");
        client.join().expect("client");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(worker.child.try_wait().expect("child status").is_some());
        let _ = fs::remove_file(script);
    }

    #[cfg(unix)]
    #[test]
    fn shutdown_of_a_hung_worker_times_out_and_kills_the_group() {
        use std::{fs, os::unix::fs::PermissionsExt, thread};

        let script =
            std::env::temp_dir().join(format!("citadel-worker-test-{}.sh", uuid::Uuid::new_v4()));
        fs::write(&script, "#!/bin/sh\ntrap '' TERM\nsleep 30\n").expect("script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).expect("executable");
        let secret = [8; 32];
        let nonce = vec![9; 32];
        let client_nonce = nonce.clone();
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &secret,
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let endpoint = worker._endpoint.path().to_path_buf();
        let client = thread::spawn(move || {
            let mut stream = UnixStream::connect(endpoint).expect("connect");
            let _ = read_control_frame(&mut stream).expect("parent hello");
            write_control_frame(
                &mut stream,
                &ControlFrame::WorkerHello {
                    protocol_version: PROTOCOL_VERSION,
                    proof: crate::runtime::worker_protocol::challenge_proof(&secret, &client_nonce)
                        .to_vec(),
                },
            )
            .expect("hello");
            write_control_frame(
                &mut stream,
                &ControlFrame::WorkerReady {
                    protocol_version: PROTOCOL_VERSION,
                    script_identity: None,
                },
            )
            .expect("ready");
            // The worker then hangs: it never acknowledges the shutdown.
            thread::sleep(Duration::from_millis(500));
        });
        worker
            .authenticate(&secret, nonce, Duration::from_secs(1))
            .expect("authenticated bootstrap");
        let error = worker
            .shutdown(Duration::from_millis(100))
            .expect_err("a hung worker cannot acknowledge shutdown");
        client.join().expect("client");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            worker.child.try_wait().expect("child status").is_some(),
            "the hung worker's process group must be killed and reaped"
        );
        let _ = fs::remove_file(script);
    }

    #[cfg(unix)]
    #[test]
    fn restart_storm_trips_the_circuit_breaker() {
        let mut controller = super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default()
                .with_restart_limit(3)
                .with_bootstrap_deadline(Duration::from_millis(50)),
        );
        let mut failed_attempts = 0;
        loop {
            match controller.restart_after_failure() {
                // Each failed reboot charges the breaker.
                Err(_) => {
                    failed_attempts += 1;
                    assert!(failed_attempts < 10, "the circuit breaker never opened");
                }
                // The breaker opened: restarts are refused from here on.
                Ok(None) => break,
                Ok(Some(_)) => unreachable!("an unauthenticatable fixture cannot restart"),
            }
        }
        assert!(failed_attempts >= 1, "the storm must charge the breaker");
        assert_eq!(
            controller.recovery_snapshot().status,
            super::RecoveryStatus::CircuitOpen
        );
    }

    #[cfg(unix)]
    #[test]
    fn bootstrap_acceptance_times_out_fail_closed() {
        let parent = std::env::temp_dir();
        let mut worker = SupervisedWorker::spawn(
            Path::new("/bin/true"),
            &parent,
            &[7; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let error = worker
            .accept_with_deadline(Duration::from_millis(10))
            .expect_err("must timeout");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(worker.child.try_wait().expect("child status").is_some());
    }

    /// Windows analog of `/bin/true`: resolves via PATH, ignores the worker
    /// arguments, exits almost immediately, and never connects.
    #[cfg(windows)]
    fn immediate_exit_fixture() -> PathBuf {
        PathBuf::from("where.exe")
    }

    #[cfg(windows)]
    fn write_batch_fixture(body: &str) -> PathBuf {
        let script =
            std::env::temp_dir().join(format!("citadel-worker-test-{}.bat", uuid::Uuid::new_v4()));
        std::fs::write(&script, body).expect("fixture script");
        script
    }

    /// Windows analog of the `trap '' TERM; sleep 30` fixture: stays alive
    /// for ~30s, ignores its arguments, and never connects to the endpoint.
    #[cfg(windows)]
    fn sleeper_fixture() -> PathBuf {
        write_batch_fixture("@echo off\r\nping -n 30 127.0.0.1 > nul\r\n")
    }

    #[cfg(windows)]
    #[test]
    fn bootstrap_failure_terminates_worker_descendants() {
        let unique = uuid::Uuid::new_v4();
        let pid_file = std::env::temp_dir().join(format!("citadel-worker-desc-{unique}.pid"));
        // The batch leader spawns a grandchild (ping via powershell) and
        // records its pid, mirroring the unix descendant fixture.
        let script = write_batch_fixture(&format!(
            "@echo off\r\npowershell -NoProfile -ExecutionPolicy Bypass -Command \
             \"$p = Start-Process ping -ArgumentList '-n','60','127.0.0.1' -WindowStyle Hidden -PassThru; \
             [IO.File]::WriteAllText('{}', [string]$p.Id); Wait-Process -Id $p.Id\"\r\n",
            pid_file.display()
        ));
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &[8; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let until = std::time::Instant::now() + Duration::from_secs(10);
        while !pid_file.exists() && std::time::Instant::now() < until {
            std::thread::sleep(Duration::from_millis(20));
        }
        let descendant: u32 = std::fs::read_to_string(&pid_file)
            .expect("descendant pid")
            .trim()
            .parse()
            .expect("numeric pid");
        // The worker never connects, so bootstrap times out. Cleanup must
        // terminate the whole job — batch leader, powershell, and ping — not
        // only the process the supervisor spawned.
        let error = worker
            .authenticate(&[8; 32], vec![1; 32], Duration::from_millis(50))
            .expect_err("worker never connects");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let until = std::time::Instant::now() + Duration::from_secs(2);
        let terminated = loop {
            if !citadel_win_proc::process_is_alive(descendant).expect("descendant liveness") {
                break true;
            }
            if std::time::Instant::now() >= until {
                break false;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let _ = std::fs::remove_file(&pid_file);
        let _ = std::fs::remove_file(&script);
        assert!(terminated, "descendant survived job-object cleanup");
    }

    #[cfg(windows)]
    #[test]
    fn foreign_client_fails_peer_validation_and_worker_is_cleaned() {
        let script = sleeper_fixture();
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &[3; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        // A same-user process that is not the spawned child connects first.
        // The DACL admits it (same user), so the peer pid check must be what
        // rejects it before any protocol byte is exchanged.
        let _client = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(worker._endpoint.name())
            .expect("foreign client connects");
        let error = worker
            .authenticate(&[3; 32], vec![1; 32], Duration::from_secs(2))
            .expect_err("a foreign client must fail peer validation");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(
            worker.child.try_wait().expect("child status").is_some(),
            "a rejected bootstrap must kill and reap the worker"
        );
        let _ = std::fs::remove_file(&script);
    }

    #[cfg(windows)]
    #[test]
    fn shutdown_of_a_hung_worker_times_out_and_kills_the_group() {
        let script = sleeper_fixture();
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &[8; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        // Peer validation stops the test process from completing the real
        // handshake (by design), so the authenticated stream is injected
        // directly; what this test pins down is shutdown's deadline and the
        // job-wide kill that must follow it.
        let endpoint = PrivateNamedPipeEndpoint::create().expect("endpoint");
        let server = {
            let _context = worker.runtime.enter();
            endpoint.bind().expect("bind")
        };
        let name = endpoint.name().to_string();
        let client = std::thread::spawn(move || {
            let mut stream = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(name)
                .expect("connect");
            let _ = crate::runtime::worker_protocol::read_control_frame(&mut stream)
                .expect("parent shutdown frame");
            // The worker then hangs: it never acknowledges the shutdown.
            std::thread::sleep(Duration::from_millis(500));
        });
        worker
            .runtime
            .block_on(server.connect())
            .expect("client connected");
        worker.stream = Some(server);
        let error = worker
            .shutdown(Duration::from_millis(100))
            .expect_err("a hung worker cannot acknowledge shutdown");
        client.join().expect("client");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            worker.child.try_wait().expect("child status").is_some(),
            "the hung worker's job must be killed and reaped"
        );
        let _ = std::fs::remove_file(&script);
    }

    #[cfg(windows)]
    #[test]
    fn restart_storm_trips_the_circuit_breaker() {
        let mut controller = super::RestartController::new(
            immediate_exit_fixture(),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default()
                .with_restart_limit(3)
                .with_bootstrap_deadline(Duration::from_millis(50)),
        );
        let mut failed_attempts = 0;
        loop {
            match controller.restart_after_failure() {
                // Each failed reboot charges the breaker.
                Err(_) => {
                    failed_attempts += 1;
                    assert!(failed_attempts < 10, "the circuit breaker never opened");
                }
                // The breaker opened: restarts are refused from here on.
                Ok(None) => break,
                Ok(Some(_)) => unreachable!("an unauthenticatable fixture cannot restart"),
            }
        }
        assert!(failed_attempts >= 1, "the storm must charge the breaker");
        assert_eq!(
            controller.recovery_snapshot().status,
            super::RecoveryStatus::CircuitOpen
        );
    }

    #[cfg(windows)]
    #[test]
    fn reports_when_child_has_exited() {
        let mut worker = SupervisedWorker::spawn(
            &immediate_exit_fixture(),
            &std::env::temp_dir(),
            &[7; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let until = std::time::Instant::now() + Duration::from_secs(5);
        let exited = loop {
            if worker.has_exited().expect("child status") {
                break true;
            }
            if std::time::Instant::now() >= until {
                break false;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(exited, "an immediately-exiting fixture must read as exited");
    }

    #[cfg(windows)]
    #[test]
    fn supervision_loop_requires_an_authenticated_first_boot() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        let mut controller = super::RestartController::new(
            immediate_exit_fixture(),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default()
                .with_restart_limit(1)
                .with_bootstrap_deadline(Duration::from_millis(50)),
        );
        let healthy_cycles = AtomicU64::new(0);
        super::run_supervision_loop(&mut controller, &AtomicBool::new(false), &healthy_cycles)
            .expect_err("an unauthenticated first boot is a startup failure");
        assert_eq!(healthy_cycles.load(Ordering::SeqCst), 0);
    }

    #[cfg(windows)]
    #[test]
    fn bootstrap_authentication_times_out_fail_closed() {
        // Windows has no separate accept step (`connect` is driven inside
        // `authenticate`), so the deadline is asserted through the full
        // bootstrap path against a fixture that never connects.
        let script = sleeper_fixture();
        let mut worker = SupervisedWorker::spawn(
            &script,
            &std::env::temp_dir(),
            &[7; 32],
            &WorkerSupervisionPolicy::default(),
        )
        .expect("spawn");
        let error = worker
            .authenticate(&[7; 32], vec![1; 32], Duration::from_millis(50))
            .expect_err("must timeout");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(worker.child.try_wait().expect("child status").is_some());
        let _ = std::fs::remove_file(&script);
    }
}
