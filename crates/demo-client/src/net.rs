//! Background QUIC networking for the demo, decoupled from the render loop.
//!
//! macroquad owns the main thread and its own frame loop, while QUIC needs a
//! tokio runtime. This module runs the [`citadel_client::QuicClient`] on a
//! dedicated thread with a multi-thread runtime and bridges to the render loop
//! with std channels: outgoing positions go in via
//! [`NetHandle::send_position`], and relayed peer envelopes come back via
//! [`NetHandle::try_recv_peer`].
//!
//! Nothing here depends on macroquad, so the bridging logic stays simple and the
//! render loop only polls non-blocking channels.

use std::net::SocketAddr;
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::thread::JoinHandle;

use citadel_client::quic::ClientTls;
use citadel_client::{Envelope, QuicClient};

use crate::state::{Pos, position_envelope};

/// A command sent from the render loop to the network thread.
enum Command {
    /// Send our latest position to the server (relayed to peers).
    SendPosition(Pos),
}

/// Handle to the background network thread.
pub struct NetHandle {
    tx: Sender<Command>,
    peer_rx: Receiver<Envelope>,
    status_rx: Receiver<String>,
    _thread: JoinHandle<()>,
}

impl NetHandle {
    /// Spawn the network thread and connect to `server_addr`.
    ///
    /// `insecure` selects dev TLS that skips certificate verification (for the
    /// server's self-signed dev cert). `server_name` is used for SNI.
    #[must_use]
    pub fn spawn(server_addr: SocketAddr, server_name: String, insecure: bool) -> Self {
        let (tx, rx) = channel::<Command>();
        let (peer_tx, peer_rx) = channel::<Envelope>();
        let (status_tx, status_rx) = channel::<String>();

        let thread = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = status_tx.send(format!("runtime error: {e}"));
                    return;
                }
            };
            runtime.block_on(network_loop(
                server_addr,
                server_name,
                insecure,
                rx,
                peer_tx,
                status_tx,
            ));
        });

        Self {
            tx,
            peer_rx,
            status_rx,
            _thread: thread,
        }
    }

    /// Queue our latest position to be sent. Non-blocking.
    pub fn send_position(&self, pos: Pos) {
        let _ = self.tx.send(Command::SendPosition(pos));
    }

    /// Poll for a relayed peer envelope from the server. Non-blocking.
    #[must_use]
    pub fn try_recv_peer(&self) -> Option<Envelope> {
        self.peer_rx.try_recv().ok()
    }

    /// Poll for the latest status message, if any. Non-blocking.
    #[must_use]
    pub fn try_recv_status(&self) -> Option<String> {
        let mut latest = None;
        while let Ok(s) = self.status_rx.try_recv() {
            latest = Some(s);
        }
        latest
    }
}

async fn network_loop(
    server_addr: SocketAddr,
    server_name: String,
    insecure: bool,
    rx: Receiver<Command>,
    peer_tx: Sender<Envelope>,
    status_tx: Sender<String>,
) {
    // The demo defaults to insecure dev TLS (self-signed server cert). A pinned
    // cert path is a future improvement.
    let _ = insecure;
    let tls = ClientTls::insecure_skip_verification();

    let client = match QuicClient::connect(server_addr, &server_name, tls).await {
        Ok(c) => {
            let _ = status_tx.send(format!("connected to {server_addr}"));
            c
        }
        Err(e) => {
            let _ = status_tx.send(format!("connect failed: {e}"));
            return;
        }
    };

    loop {
        // Drain any queued position commands (keep only the latest).
        let mut latest: Option<Pos> = None;
        loop {
            match rx.try_recv() {
                Ok(Command::SendPosition(p)) => latest = Some(p),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    client.close();
                    return;
                }
            }
        }
        if let Some(pos) = latest {
            let _ = client.send_unreliable(&position_envelope(pos));
        }

        // Receive relayed peer datagrams with a short timeout so we stay
        // responsive to outgoing commands.
        let recv =
            tokio::time::timeout(std::time::Duration::from_millis(8), client.recv_datagram()).await;
        if let Ok(Ok(env)) = recv {
            let _ = peer_tx.send(env);
        }
    }
}
