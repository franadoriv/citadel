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
        ControlFrame, PROTOCOL_VERSION, read_control_frame, verify_worker_hello,
        write_control_frame,
    },
};

#[cfg(unix)]
pub struct SupervisedWorker {
    _endpoint: PrivateUnixEndpoint,
    _listener: UnixListener,
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
            if verify_worker_hello(secret, &nonce, &frame) {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "worker authentication failed",
                ))
            }
        })();
        if result.is_err() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        result
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
    fn bootstrap_acceptance_times_out_fail_closed() {
        let parent = std::env::temp_dir();
        let worker =
            SupervisedWorker::spawn(Path::new("/bin/true"), &parent, &[7; 32]).expect("spawn");
        let error = worker
            .accept_with_deadline(Duration::from_millis(10))
            .expect_err("must timeout");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
}
