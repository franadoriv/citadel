//! QUIC transport for Citadel's realtime layer.
//!
//! Built on `quinn`, this is the primary real-time transport: unreliable
//! datagrams for hot-path game state and reliable bidirectional streams for
//! control, all over TLS 1.3. It implements both [`Listener`] (synchronous
//! identity) and [`AsyncService`] (so the lifecycle [`Supervisor`] starts and
//! gracefully stops it).
//!
//! Behavior: each accepted connection mints a realtime session in the shared
//! [`Gateway`], registers a bounded outbound channel, and spawns a write task
//! that drains that channel to the socket (unreliable -> datagram, reliable ->
//! a fresh uni stream). Inbound envelopes (datagrams and bidi streams) are
//! forwarded to [`Gateway::handle_inbound`], which relays them to OTHER
//! sessions. The connection is unregistered on disconnect.
//!
//! Concurrency model: one accept loop awaits the cancellation token; each
//! accepted connection is handled by its own task that concurrently drives the
//! inbound paths (datagrams, bidi streams, uni streams) and an outbound write
//! task fed by the gateway.
//!
//! [`Supervisor`]: crate::lifecycle::Supervisor

pub mod tls;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use citadel_wire::protocol::{KIND_AUTH_RESULT, KIND_DIAG_SERVER_TIME};
use quinn::{Connection as QuinnConnection, Endpoint};
use tokio::sync::mpsc;

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::lifecycle::{AsyncService, CancellationToken};
use crate::realtime::{Gateway, LatestOutboundReceiver, Outbound, SessionHandle};
use crate::time::{Clock, SystemClock};
use crate::transport::codec::{Envelope, decode_datagram, decode_framed};
use crate::transport::metrics::TransportMetrics;
use crate::transport::{
    Connection, ConnectionId, ConnectionIdGen, Delivery, Listener, PeerAddr, TransportKind,
};

/// Per-connection outbound channel capacity (envelopes).
const OUTBOUND_CAPACITY: usize = 1024;
/// Bound on a single inbound stream read (also used for the handshake stream).
const MAX_STREAM_BYTES: usize = 16 * 1024 * 1024;

pub use tls::SelfSignedCert;

/// A bound QUIC server endpoint, ready to be run as an [`AsyncService`].
pub struct QuicServer {
    endpoint: Endpoint,
    local_addr: SocketAddr,
    name: String,
    ids: Arc<ConnectionIdGen>,
    metrics: TransportMetrics,
    gateway: Arc<Gateway>,
    /// How long to wait for the client's `KIND_AUTH` handshake frame.
    handshake_timeout: Duration,
}

impl QuicServer {
    /// Bind a QUIC endpoint with a private gateway (tests/standalone use).
    pub fn bind(bind: SocketAddr, cert: &SelfSignedCert) -> AppResult<Self> {
        Self::bind_with_gateway(bind, cert, Arc::new(Gateway::new()))
    }

    /// Bind a QUIC endpoint at `bind` using `cert` for TLS, sharing `gateway`.
    ///
    /// The shared gateway is how multiple transports (QUIC + WebSocket) route to
    /// one room.
    pub fn bind_with_gateway(
        bind: SocketAddr,
        cert: &SelfSignedCert,
        gateway: Arc<Gateway>,
    ) -> AppResult<Self> {
        let server_config = tls::server_config(cert)?;
        let endpoint = Endpoint::server(server_config, bind).map_err(|e| {
            AppError::new(
                ErrorCategory::Transport,
                format!("failed to bind QUIC endpoint on {bind}"),
            )
            .with_detail(e.to_string())
        })?;
        let local_addr = endpoint.local_addr().map_err(|e| {
            AppError::new(
                ErrorCategory::Transport,
                "failed to read QUIC local address",
            )
            .with_detail(e.to_string())
        })?;
        Ok(Self {
            endpoint,
            local_addr,
            name: "quic".to_string(),
            ids: Arc::new(ConnectionIdGen::new()),
            metrics: TransportMetrics::new(),
            gateway,
            handshake_timeout: Duration::from_millis(5_000),
        })
    }

    /// Set the realtime auth handshake timeout. Wired from
    /// `transport.auth.handshake_timeout_ms` at startup.
    #[must_use]
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// The local socket address the endpoint is bound to.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// A clone of the shared metrics handle for this transport.
    #[must_use]
    pub fn metrics(&self) -> TransportMetrics {
        self.metrics.clone()
    }
}

impl Listener for QuicServer {
    fn transport_kind(&self) -> TransportKind {
        TransportKind::Quic
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl AsyncService for QuicServer {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        tracing::info!(addr = %self.local_addr, "QUIC listener accepting connections");
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    tracing::info!("QUIC listener shutting down");
                    break;
                }
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        // Endpoint closed.
                        break;
                    };
                    let ids = Arc::clone(&self.ids);
                    let conn_cancel = cancel.clone();
                    let metrics = self.metrics.clone();
                    let gateway = Arc::clone(&self.gateway);
                    let handshake_timeout = self.handshake_timeout;
                    tokio::spawn(async move {
                        match incoming.await {
                            Ok(connection) => {
                                let id = ids.next_id();
                                if let Err(e) = handle_connection(id, connection, conn_cancel, metrics, gateway, handshake_timeout).await {
                                    tracing::debug!(error = %e, "QUIC connection ended with error");
                                }
                            }
                            Err(e) => {
                                tracing::debug!(error = %e, "QUIC handshake failed");
                            }
                        }
                    });
                }
            }
        }
        // Stop accepting and let in-flight connections drain briefly.
        self.endpoint.close(0u32.into(), b"shutdown");
        self.endpoint.wait_idle().await;
        Ok(())
    }
}

/// A handle to an accepted QUIC connection, implementing [`Connection`].
pub struct QuicConnection {
    id: ConnectionId,
    peer: PeerAddr,
    inner: QuinnConnection,
}

impl QuicConnection {
    /// Send an envelope as an unreliable datagram.
    pub fn send_datagram(&self, env: &Envelope) -> AppResult<()> {
        self.inner
            .send_datagram(env.encode_datagram())
            .map_err(|e| {
                AppError::new(ErrorCategory::Transport, "failed to send QUIC datagram")
                    .with_detail(e.to_string())
            })
    }

    /// The underlying quinn connection (for stream operations).
    #[must_use]
    pub fn quinn(&self) -> &QuinnConnection {
        &self.inner
    }
}

impl Connection for QuicConnection {
    fn id(&self) -> ConnectionId {
        self.id
    }
    fn peer_addr(&self) -> PeerAddr {
        self.peer
    }
    fn transport_kind(&self) -> TransportKind {
        TransportKind::Quic
    }
}

/// Drive one accepted connection: run the authenticated handshake, and — only
/// once accepted — register a session, run the gateway-fed outbound write task,
/// and route inbound envelopes to the gateway.
///
/// The handshake gates registration exactly as on WebSocket: the
/// connection stays pending (no session/`on_join`/routing/gauge) until the first
/// **reliable** envelope resolves. Datagrams received before the handshake are
/// dropped (never buffered): the token must arrive on the reliable path.
async fn handle_connection(
    id: ConnectionId,
    connection: QuinnConnection,
    cancel: CancellationToken,
    metrics: TransportMetrics,
    gateway: Arc<Gateway>,
    handshake_timeout: Duration,
) -> AppResult<()> {
    let peer = PeerAddr::new(connection.remote_address());
    metrics.connection_opened();
    gateway.connection_opened();
    tracing::debug!(conn = %id, peer = %peer, "QUIC connection established; awaiting auth handshake");

    // Phase 1 (PENDING_AUTH): await the first reliable-stream envelope. No
    // participant is registered until this resolves.
    let first_frames = match tokio::time::timeout(
        handshake_timeout,
        read_handshake_frames(&connection, &cancel, &metrics),
    )
    .await
    {
        Ok(Some(frames)) => frames,
        Ok(None) => {
            metrics.connection_closed();
            gateway.connection_closed();
            return Ok(());
        }
        Err(_elapsed) => {
            tracing::debug!(conn = %id, "QUIC auth handshake timed out; closing");
            connection.close(0u32.into(), b"auth timeout");
            metrics.connection_closed();
            gateway.connection_closed();
            return Ok(());
        }
    };

    let Some((first, queued)) = first_frames.split_first() else {
        metrics.connection_closed();
        gateway.connection_closed();
        return Ok(());
    };
    let handshake = gateway.resolve_handshake(first).await;

    if !handshake.outcome.is_accepted() {
        // Rejection: send exactly one KIND_AUTH_RESULT on a reliable stream and
        // close. Never register.
        send_reliable_envelope(
            &connection,
            &Envelope::new(KIND_AUTH_RESULT, handshake.outcome.result_body()),
        )
        .await;
        connection.close(0u32.into(), b"auth rejected");
        metrics.connection_closed();
        gateway.connection_closed();
        tracing::debug!(conn = %id, "QUIC auth handshake rejected; connection closed");
        return Ok(());
    }

    // Phase 2 (REGISTERED): bind identity and register.
    let session_id = gateway.next_participant_id();
    let identity = handshake.outcome.identity();
    let authenticated = identity.is_some();
    let (tx, rx) = mpsc::channel::<Outbound>(OUTBOUND_CAPACITY);
    // The registry fences this protocol reply before publishing the session,
    // retaining reliable-first ordering without a raw, unfenced sender path.
    let mut initials = Vec::with_capacity(2);
    if !handshake.replay_first {
        initials.push(Outbound::reliable(Envelope::new(
            KIND_AUTH_RESULT,
            handshake.outcome.result_body(),
        )));
    }
    match gateway.issue_diagnostics_server_time(session_id, SystemClock.now()) {
        Ok(server_time) => match server_time.encode() {
            Ok(body) => initials.push(Outbound::reliable(Envelope::new(
                KIND_DIAG_SERVER_TIME,
                body,
            ))),
            Err(error) => {
                tracing::error!(conn = %id, %session_id, error = %error, "failed to encode diagnostics server-time offer")
            }
        },
        Err(error) => {
            tracing::error!(conn = %id, %session_id, error = %error, "failed to issue diagnostics server-time offer")
        }
    }
    let unreliable = gateway.register_session_with_initials(
        SessionHandle {
            id: session_id,
            kind: TransportKind::Quic,
            outbound: tx,
            identity,
        },
        initials,
    );
    if !gateway.accepts_work(session_id) {
        gateway.abandon_diagnostics_session(session_id);
        connection.close(0u32.into(), b"session revoked");
        metrics.connection_closed();
        gateway.connection_closed();
        return Ok(());
    }
    tracing::debug!(
        conn = %id, %session_id, authenticated,
        "QUIC connection authenticated; session registered"
    );

    // Replay the first frame for a legacy client, then any queued frames.
    if handshake.replay_first {
        metrics.envelope_received();
        gateway.handle_inbound(session_id, first);
    }
    for env in queued {
        metrics.envelope_received();
        gateway.handle_inbound(session_id, env);
    }

    // Outbound write task: drains the gateway-fed channel to the socket.
    let write_conn = connection.clone();
    let write_metrics = metrics.clone();
    let write_cancel = cancel.clone();
    let writer = tokio::spawn(async move {
        outbound_writer(write_conn, rx, unreliable, write_metrics, write_cancel).await;
    });

    // Inbound: datagrams + accepted bi/uni streams routed to the gateway.
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            datagram = connection.read_datagram() => {
                match datagram {
                    Ok(bytes) => match decode_datagram(&bytes) {
                        Ok(env) => {
                            metrics.envelope_received();
                            gateway.handle_inbound(session_id, &env);
                        }
                        Err(e) => {
                            metrics.decode_error();
                            tracing::debug!(conn = %id, error = %e, "bad datagram");
                        }
                    },
                    Err(_) => break, // connection closed
                }
            }
            stream = connection.accept_bi() => {
                match stream {
                    Ok((_send, recv)) => {
                        spawn_inbound_stream(session_id, recv, metrics.clone(), Arc::clone(&gateway));
                    }
                    Err(_) => break,
                }
            }
            stream = connection.accept_uni() => {
                match stream {
                    Ok(recv) => {
                        spawn_inbound_stream(session_id, recv, metrics.clone(), Arc::clone(&gateway));
                    }
                    Err(_) => break,
                }
            }
        }
    }

    gateway.unregister_session(session_id);
    writer.abort();
    metrics.connection_closed();
    gateway.connection_closed();
    tracing::debug!(conn = %id, %session_id, "QUIC connection closed");
    Ok(())
}

/// Spawn a task that reads framed envelopes from one inbound stream and routes
/// each to the gateway.
fn spawn_inbound_stream(
    session_id: crate::realtime::ParticipantId,
    recv: quinn::RecvStream,
    metrics: TransportMetrics,
    gateway: Arc<Gateway>,
) {
    tokio::spawn(async move {
        if let Err(e) = read_inbound_stream(session_id, recv, &metrics, &gateway).await {
            tracing::debug!(%session_id, error = %e, "QUIC inbound stream ended");
        }
    });
}

/// Read all framed envelopes from a reliable stream and route them.
async fn read_inbound_stream(
    session_id: crate::realtime::ParticipantId,
    mut recv: quinn::RecvStream,
    metrics: &TransportMetrics,
    gateway: &Gateway,
) -> AppResult<()> {
    let data = recv.read_to_end(MAX_STREAM_BYTES).await.map_err(|e| {
        AppError::new(ErrorCategory::Transport, "failed to read QUIC stream")
            .with_detail(e.to_string())
    })?;
    let mut buf = BytesMut::from(&data[..]);
    while let Some(env) = decode_framed(&mut buf)? {
        metrics.envelope_received();
        gateway.handle_inbound(session_id, &env);
    }
    Ok(())
}

/// Await the first reliable-stream envelope of the connection (the handshake),
/// dropping any datagrams that arrive before auth resolves.
///
/// Returns the frames decoded from the first stream that yields at least one, or
/// `None` if the connection closes / is cancelled before then. Datagrams are
/// dropped (never buffered): the `KIND_AUTH` handshake must arrive on the
/// reliable path.
async fn read_handshake_frames(
    connection: &QuinnConnection,
    cancel: &CancellationToken,
    metrics: &TransportMetrics,
) -> Option<Vec<Envelope>> {
    loop {
        tokio::select! {
            () = cancel.cancelled() => return None,
            datagram = connection.read_datagram() => {
                match datagram {
                    Ok(_bytes) => {
                        tracing::debug!("QUIC datagram received before auth handshake; dropped");
                        continue;
                    }
                    Err(_) => return None,
                }
            }
            stream = connection.accept_uni() => {
                match stream {
                    Ok(recv) => {
                        if let Some(frames) = read_stream_frames(recv, metrics).await {
                            return Some(frames);
                        }
                    }
                    Err(_) => return None,
                }
            }
            stream = connection.accept_bi() => {
                match stream {
                    Ok((_send, recv)) => {
                        if let Some(frames) = read_stream_frames(recv, metrics).await {
                            return Some(frames);
                        }
                    }
                    Err(_) => return None,
                }
            }
        }
    }
}

/// Read one reliable stream to end and decode its framed envelopes.
///
/// Returns `Some(frames)` when at least one frame decodes, `None` on a read
/// error or an empty/undecodable stream (so the caller keeps waiting for a real
/// handshake stream).
async fn read_stream_frames(
    mut recv: quinn::RecvStream,
    metrics: &TransportMetrics,
) -> Option<Vec<Envelope>> {
    let data = recv.read_to_end(MAX_STREAM_BYTES).await.ok()?;
    let mut buf = BytesMut::from(&data[..]);
    let mut frames = Vec::new();
    loop {
        match decode_framed(&mut buf) {
            Ok(Some(env)) => {
                metrics.envelope_received();
                frames.push(env);
            }
            Ok(None) => break,
            Err(_) => {
                metrics.decode_error();
                break;
            }
        }
    }
    if frames.is_empty() {
        None
    } else {
        Some(frames)
    }
}

/// Best-effort send of a single reliable envelope on a fresh uni stream, used to
/// deliver a `KIND_AUTH_RESULT` rejection before the connection is closed.
async fn send_reliable_envelope(connection: &QuinnConnection, env: &Envelope) {
    if let Ok(mut send) = connection.open_uni().await {
        let frame = env.encode_framed();
        if send.write_all(&frame).await.is_ok() {
            let _ = send.finish();
        }
    }
}

/// Drain the gateway-fed outbound channel to the socket: unreliable envelopes go
/// as datagrams, reliable ones as fresh uni streams.
async fn outbound_writer(
    connection: QuinnConnection,
    mut rx: mpsc::Receiver<Outbound>,
    unreliable: LatestOutboundReceiver,
    metrics: TransportMetrics,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            next = rx.recv() => {
                let Some(out) = next else { break };
                write_outbound(&connection, out, &metrics).await;
            }
            out = unreliable.recv() => {
                write_outbound(&connection, out, &metrics).await;
            }
        }
    }
}

async fn write_outbound(connection: &QuinnConnection, out: Outbound, metrics: &TransportMetrics) {
    let Some(_delivery) = out.acquire_delivery().await else {
        return;
    };
    match out.delivery {
        Delivery::Unreliable => {
            if connection
                .send_datagram(out.envelope.encode_datagram())
                .is_ok()
            {
                metrics.envelope_sent();
            }
        }
        Delivery::Reliable => match connection.open_uni().await {
            Ok(mut send) => {
                let frame = out.envelope.encode_framed();
                if send.write_all(&frame).await.is_ok() && send.finish().is_ok() {
                    metrics.envelope_sent();
                }
            }
            Err(error) => {
                tracing::debug!(%error, "failed to open QUIC uni stream");
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn loopback() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
    }

    #[tokio::test]
    async fn server_binds_and_reports_local_addr() {
        let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("cert");
        let server = QuicServer::bind(loopback(), &cert).expect("bind");
        assert_eq!(server.transport_kind(), TransportKind::Quic);
        assert_ne!(server.local_addr().port(), 0, "ephemeral port assigned");
    }
}
