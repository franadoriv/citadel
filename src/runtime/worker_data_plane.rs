//! Parent-side IPC pump for the match data plane.
//!
//! The supervisor owns the control connection (hello, health, shutdown); this
//! module owns the second, data-sized connection that carries one worker
//! generation's match frames. The endpoint reuses the platform's private
//! transport ([`PrivateUnixEndpoint`] / [`PrivateNamedPipeEndpoint`]) and the
//! connection is authenticated with the same challenge-proof handshake as the
//! control plane, against the same per-generation bootstrap secret — a
//! squatted or foreign endpoint never sees a single data frame.
//!
//! After the handshake the connection is pumped by a dedicated thread running
//! a private current-thread tokio runtime (Windows named pipes are async-only;
//! unix reuses the same shape), bridging to synchronous callers through two
//! bounded channels: gateway→worker frames go in through [`DataPlaneSender`]
//! (a [`FrameSender`]), worker→gateway frames come out of a standard receiver
//! that the [`ExternalWorkerRuntime`](super::external_worker::ExternalWorkerRuntime)
//! receive pump drains. Both directions fail closed on overflow: a full
//! channel drops the frame instead of blocking match dispatch or growing
//! without bound.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};

use super::external_worker::{FrameSendError, FrameSender};
use super::worker_data_protocol::{DataFrame, read_data_frame_async, write_data_frame_async};
use super::worker_protocol::{
    ControlFrame, PROTOCOL_VERSION, read_control_frame_async, verify_worker_hello,
    write_control_frame_async,
};

/// Bound for both bridge channels. Reuses the per-match mailbox default (the
/// worker cannot usefully queue more than its mailboxes hold) rather than
/// inventing a new number.
const CHANNEL_CAPACITY: usize = super::engine_host::DEFAULT_MATCH_MAILBOX_CAPACITY;

/// Sender half handed to the [`ExternalWorkerRuntime`]: frames pushed here are
/// written to the worker by the pump thread.
pub struct DataPlaneSender {
    tx: tokio::sync::mpsc::Sender<DataFrame>,
}

impl FrameSender for DataPlaneSender {
    fn send(&self, frame: DataFrame) -> Result<(), FrameSendError> {
        self.tx.try_send(frame).map_err(|_| FrameSendError)
    }
}

/// One established, authenticated data-plane connection.
pub struct DataPlaneConnection {
    /// Gateway→worker direction (install into the runtime adapter).
    pub sender: Arc<DataPlaneSender>,
    /// Worker→gateway direction (feed the adapter's receive pump).
    pub frames: std::sync::mpsc::Receiver<DataFrame>,
}

/// Authenticate the freshly accepted data connection (parent side).
///
/// Mirrors the control-plane bootstrap: the parent challenges with a fresh
/// nonce, the worker proves knowledge of this generation's bootstrap secret.
async fn authenticate_data_stream<S>(
    stream: &mut S,
    secret: &[u8; 32],
    deadline: Duration,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let nonce = super::worker_supervisor::fresh_bootstrap_nonce()?;
    tokio::time::timeout(deadline, async {
        write_control_frame_async(
            stream,
            &ControlFrame::ParentHello {
                protocol_version: PROTOCOL_VERSION,
                nonce: nonce.to_vec(),
            },
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "data-plane hello write failed"))?;
        let frame = read_control_frame_async(stream).await.map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "data-plane hello frame invalid")
        })?;
        if !verify_worker_hello(secret, &nonce, &frame) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "data-plane authentication failed",
            ));
        }
        Ok(())
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "data-plane handshake deadline exceeded",
        )
    })?
}

/// Pump frames in both directions until either side of the connection ends.
async fn pump_frames<S>(
    stream: S,
    mut outbound: tokio::sync::mpsc::Receiver<DataFrame>,
    inbound: std::sync::mpsc::SyncSender<DataFrame>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let reader = async {
        while let Ok(frame) = read_data_frame_async(&mut read_half).await {
            // Fail closed on a saturated bridge: dropping the frame never
            // blocks the pump; sequence validation downstream surfaces the
            // gap.
            if inbound.try_send(frame).is_err() {
                tracing::warn!("data-plane inbound bridge full; dropping a worker frame");
            }
        }
    };
    let writer = async {
        while let Some(frame) = outbound.recv().await {
            if write_data_frame_async(&mut write_half, &frame)
                .await
                .is_err()
            {
                break;
            }
        }
    };
    tokio::select! {
        () = reader => {}
        () = writer => {}
    }
}

/// Build the bridge channels plus the connection handle around them.
fn bridge() -> (
    tokio::sync::mpsc::Receiver<DataFrame>,
    std::sync::mpsc::SyncSender<DataFrame>,
    DataPlaneConnection,
) {
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
    let (inbound_tx, inbound_rx) = std::sync::mpsc::sync_channel(CHANNEL_CAPACITY);
    let connection = DataPlaneConnection {
        sender: Arc::new(DataPlaneSender { tx: outbound_tx }),
        frames: inbound_rx,
    };
    (outbound_rx, inbound_tx, connection)
}

/// Unix data plane: accept on the parent-created private endpoint, then
/// authenticate and hand the connection to a pump thread.
#[cfg(unix)]
pub fn establish_unix_data_plane(
    listener: &std::os::unix::net::UnixListener,
    secret: &[u8; 32],
    deadline: Duration,
) -> io::Result<DataPlaneConnection> {
    listener.set_nonblocking(true)?;
    let until = std::time::Instant::now() + deadline;
    let stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    && std::time::Instant::now() < until =>
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "data-plane connect deadline exceeded",
                ));
            }
            Err(error) => return Err(error),
        }
    };
    stream.set_nonblocking(true)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;
    let mut stream = {
        let _context = runtime.enter();
        tokio::net::UnixStream::from_std(stream)?
    };
    runtime.block_on(authenticate_data_stream(&mut stream, secret, deadline))?;
    let (outbound_rx, inbound_tx, connection) = bridge();
    std::thread::Builder::new()
        .name("citadel-worker-data-pump".to_owned())
        .spawn(move || {
            runtime.block_on(pump_frames(stream, outbound_rx, inbound_tx));
        })?;
    Ok(connection)
}

/// Windows data plane: the endpoint must be bound before the child process is
/// spawned and the tokio pipe server is tied to its creating runtime, so the
/// whole lifecycle lives on one dedicated thread. [`WindowsDataPlane::start`]
/// binds and returns once the endpoint exists; the thread then waits for the
/// spawned child's pid, accepts, validates the pipe peer, authenticates, and
/// pumps.
#[cfg(windows)]
pub struct WindowsDataPlane {
    /// Full pipe name for the worker's command line.
    pub endpoint: String,
    pid_tx: std::sync::mpsc::SyncSender<u32>,
    ready_rx: std::sync::mpsc::Receiver<io::Result<DataPlaneConnection>>,
}

#[cfg(windows)]
impl WindowsDataPlane {
    /// Create and bind the private data endpoint on its pump thread.
    pub fn start(secret: [u8; 32], deadline: Duration) -> io::Result<Self> {
        let endpoint = super::worker_ipc::PrivateNamedPipeEndpoint::create()?;
        let name = endpoint.name().to_string();
        let (bound_tx, bound_rx) = std::sync::mpsc::sync_channel::<io::Result<()>>(1);
        let (pid_tx, pid_rx) = std::sync::mpsc::sync_channel::<u32>(1);
        let (ready_tx, ready_rx) =
            std::sync::mpsc::sync_channel::<io::Result<DataPlaneConnection>>(1);
        std::thread::Builder::new()
            .name("citadel-worker-data-pump".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = bound_tx.send(Err(error));
                        return;
                    }
                };
                let server = {
                    let _context = runtime.enter();
                    match endpoint.bind() {
                        Ok(server) => server,
                        Err(error) => {
                            let _ = bound_tx.send(Err(error));
                            return;
                        }
                    }
                };
                if bound_tx.send(Ok(())).is_err() {
                    return;
                }
                // The child is spawned only after the bind signal, so a
                // worker can never observe a missing endpoint.
                let Ok(child_pid) = pid_rx.recv() else {
                    return;
                };
                let established = runtime.block_on(async {
                    let mut server = server;
                    tokio::time::timeout(deadline, server.connect())
                        .await
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::TimedOut,
                                "data-plane connect deadline exceeded",
                            )
                        })??;
                    // Peer validation before any protocol byte, exactly like
                    // the control plane: only the spawned child may speak.
                    let peer = citadel_win_proc::named_pipe_client_process_id(&server)?;
                    if peer != child_pid {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "data-plane peer mismatch",
                        ));
                    }
                    authenticate_data_stream(&mut server, &secret, deadline).await?;
                    Ok(server)
                });
                match established {
                    Ok(server) => {
                        let (outbound_rx, inbound_tx, connection) = bridge();
                        if ready_tx.send(Ok(connection)).is_err() {
                            return;
                        }
                        runtime.block_on(pump_frames(server, outbound_rx, inbound_tx));
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                }
            })?;
        match bound_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                endpoint: name,
                pid_tx,
                ready_rx,
            }),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(io::Error::other("data-plane pump thread died during bind")),
        }
    }

    /// Tell the pump thread which child pid may complete the connection.
    pub fn set_child_pid(&self, pid: u32) {
        let _ = self.pid_tx.send(pid);
    }

    /// Wait for the authenticated connection (bounded by `deadline`).
    pub fn establish(&mut self, deadline: Duration) -> io::Result<DataPlaneConnection> {
        match self.ready_rx.recv_timeout(deadline) {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "data-plane handshake deadline exceeded",
            )),
        }
    }
}
