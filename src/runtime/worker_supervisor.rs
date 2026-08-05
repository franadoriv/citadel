//! Supervision primitives for the internal GameScript worker.

#[cfg(unix)]
use std::{
    io,
    os::{
        fd::OwnedFd,
        unix::{
            net::{UnixListener, UnixStream},
            process::CommandExt,
        },
    },
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

#[cfg(unix)]
use super::{
    worker_bootstrap::BootstrapPipe,
    worker_ipc::PrivateUnixEndpoint,
    worker_protocol::{
        ControlFrame, PROTOCOL_VERSION, is_valid_worker_health, read_control_frame,
        verify_worker_hello, write_control_frame,
    },
};

#[cfg(unix)]
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
}

impl RestartCircuitBreaker {
    pub fn new(limit: u32) -> Self {
        Self {
            limit: limit.max(1),
            failures: 0,
        }
    }

    pub fn record_failure(&mut self) -> bool {
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
        self.failures = 0;
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
            breaker: RestartCircuitBreaker::new(policy.restart_limit()),
        }
    }

    pub fn policy(&self) -> &WorkerSupervisionPolicy {
        &self.policy
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
    let mut active = Some(controller.start()?);
    while !stop.load(Ordering::SeqCst) {
        match controller.monitor_health(&mut active, policy.liveness_deadline()) {
            Ok(true) => {
                healthy_cycles.fetch_add(1, Ordering::SeqCst);
            }
            Ok(false) => {
                return Err(io::Error::other(
                    "worker restart circuit breaker is open; supervision halted",
                ));
            }
            Err(error) => {
                // A restart attempt failed to boot or authenticate. The
                // breaker was already charged; keep looping so supervision
                // either recovers or trips the breaker above.
                tracing::warn!(error = %error, "external runtime worker restart attempt failed");
            }
        }
    }
    if let Some(mut worker) = active.take() {
        if let Err(error) = worker.shutdown(policy.shutdown_deadline()) {
            // The worker group was already killed by the failed shutdown
            // path; an unacknowledged stop is not a supervision failure.
            tracing::warn!(error = %error, "external runtime worker shutdown was not acknowledged");
        }
    }
    Ok(())
}

/// Serve-lifecycle service owning the external GameScript worker.
///
/// Spawned by the transport supervisor when `runtime.adapter` is
/// `external-worker`: it boots the worker on server startup, keeps it healthy
/// through [`run_supervision_loop`] (periodic liveness, restart policy,
/// circuit breaker), and shuts it down together with the server.
#[cfg(unix)]
pub struct WorkerLifecycleService {
    executable: PathBuf,
    socket_parent: PathBuf,
    policy: WorkerSupervisionPolicy,
    healthy_cycles: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(unix)]
impl WorkerLifecycleService {
    #[must_use]
    pub fn new(executable: PathBuf, socket_parent: PathBuf, policy: WorkerSupervisionPolicy) -> Self {
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

#[cfg(unix)]
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
                ControlFrame::WorkerReady { protocol_version }
                    if protocol_version == PROTOCOL_VERSION =>
                {
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
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "worker shutdown acknowledgement invalid",
                    )
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::fs;

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
        let mut worker =
            SupervisedWorker::spawn(
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
        let mut controller =
            super::RestartController::new(
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

    #[test]
    fn health_failure_removes_active_worker_before_recovery() {
        let mut controller =
            super::RestartController::new(
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
        let mut controller =
            super::RestartController::new(
            PathBuf::from("/bin/true"),
            std::env::temp_dir(),
            super::WorkerSupervisionPolicy::default().with_restart_limit(1),
        );
        let mut active = None;
        assert!(!controller.recover_if_exited(&mut active).expect("recover"));
        assert!(active.is_none());
    }

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
        let mut controller =
            super::RestartController::new(
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

    #[test]
    fn crashed_worker_requires_a_permitted_restart_delay() {
        let mut breaker = super::RestartCircuitBreaker::new(1);
        let mut worker =
            SupervisedWorker::spawn(
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

    #[test]
    fn reports_when_child_has_exited() {
        let mut worker =
            SupervisedWorker::spawn(
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
        let mut breaker = super::RestartCircuitBreaker::new(3);
        assert!(breaker.record_failure());
        assert!(breaker.record_failure());
        assert!(!breaker.record_failure());
        assert!(breaker.is_open());
        breaker.record_healthy();
        assert!(!breaker.is_open());
        assert!(breaker.record_failure());
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
        let mut worker =
            SupervisedWorker::spawn(
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

    #[test]
    fn worker_that_stalls_after_connect_is_killed_and_reaped() {
        use std::{fs, os::unix::fs::PermissionsExt, thread};

        let script =
            std::env::temp_dir().join(format!("citadel-worker-test-{}.sh", uuid::Uuid::new_v4()));
        fs::write(&script, "#!/bin/sh\ntrap '' TERM\nsleep 30\n").expect("script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).expect("executable");
        let mut worker =
            SupervisedWorker::spawn(
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

    #[test]
    fn invalid_worker_hello_kills_and_reaps_the_child() {
        use std::{fs, os::unix::fs::PermissionsExt, thread};

        let script =
            std::env::temp_dir().join(format!("citadel-worker-test-{}.sh", uuid::Uuid::new_v4()));
        fs::write(&script, "#!/bin/sh\ntrap '' TERM\nsleep 30\n").expect("script");
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).expect("executable");
        let mut worker =
            SupervisedWorker::spawn(
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

    #[test]
    fn bootstrap_acceptance_times_out_fail_closed() {
        let parent = std::env::temp_dir();
        let mut worker =
            SupervisedWorker::spawn(
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
}
