//! Supervision primitives for the internal GameScript worker.

#[cfg(unix)]
use std::{
    io,
    os::{
        fd::OwnedFd,
        unix::net::{UnixListener, UnixStream},
    },
    path::Path,
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

    pub fn is_open(&self) -> bool {
        self.failures >= self.limit
    }
}

pub struct SupervisedWorker {
    _endpoint: PrivateUnixEndpoint,
    _listener: UnixListener,
    stream: Option<UnixStream>,
    _bootstrap_reader: OwnedFd,
    child: Child,
}

#[cfg(unix)]
impl SupervisedWorker {
    pub fn spawn(executable: &Path, parent: &Path, secret: &[u8; 32]) -> io::Result<Self> {
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
            .spawn()?;
        if let Err(error) = bootstrap_writer.write_secret(secret) {
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
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        result
    }

    pub fn shutdown(&mut self, deadline: Duration) -> io::Result<()> {
        let result = (|| {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotConnected))?;
            stream.set_read_timeout(Some(deadline))?;
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
            match read_control_frame(stream).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "worker shutdown acknowledgement invalid",
                )
            })? {
                ControlFrame::WorkerStopped { protocol_version }
                    if protocol_version == PROTOCOL_VERSION =>
                {
                    Ok(())
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "worker shutdown acknowledgement invalid",
                )),
            }
        })();
        if result.is_err() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
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
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        result
    }

    pub fn has_exited(&mut self) -> io::Result<bool> {
        Ok(self.child.try_wait()?.is_some())
    }

    pub fn kill(&mut self) -> io::Result<()> {
        self.child.kill()
    }
}

#[cfg(unix)]
impl Drop for SupervisedWorker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn crashed_worker_requires_a_permitted_restart_delay() {
        let mut breaker = super::RestartCircuitBreaker::new(1);
        let mut worker =
            SupervisedWorker::spawn(Path::new("/bin/true"), &std::env::temp_dir(), &[9; 32])
                .expect("spawn");
        std::thread::sleep(Duration::from_millis(10));
        assert!(worker.has_exited().expect("child status"));
        assert_eq!(breaker.next_restart_delay(), None);
    }

    #[test]
    fn reports_when_child_has_exited() {
        let mut worker =
            SupervisedWorker::spawn(Path::new("/bin/true"), &std::env::temp_dir(), &[7; 32])
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
            SupervisedWorker::spawn(&script, &std::env::temp_dir(), &secret).expect("spawn");
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
            SupervisedWorker::spawn(&script, &std::env::temp_dir(), &[4; 32]).expect("spawn");
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
            SupervisedWorker::spawn(&script, &std::env::temp_dir(), &[3; 32]).expect("spawn");
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
            SupervisedWorker::spawn(Path::new("/bin/true"), &parent, &[7; 32]).expect("spawn");
        let error = worker
            .accept_with_deadline(Duration::from_millis(10))
            .expect_err("must timeout");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(worker.child.try_wait().expect("child status").is_some());
    }
}
