//! WebTransport transport for browser action clients.
//!
//! Built on `web-transport-quinn` (HTTP/3 + WebTransport over our existing
//! `quinn` stack), this exposes the SAME realtime gateway room to browsers as
//! native QUIC and WebSocket. A browser WebTransport client interoperates with
//! native QUIC and WebSocket peers because every transport shares one
//! [`Gateway`] and the `citadel-wire` codec/protocol.
//!
//! WebTransport negotiates the HTTP/3 ALPN `h3`, which differs from native
//! QUIC's `citadel/0`, so it runs on its own endpoint/port (separate
//! `[transport.webtransport]` bind). Browsers require a CA-trusted certificate
//! or a `serverCertificateHashes` pin (ECDSA P-256, <= 14 day validity); see
//! [`cert`].
//!
//! Behavior mirrors `src/transport/quic`: each accepted session mints a gateway
//! session, registers a bounded outbound channel, and spawns a write task that
//! drains it to the session (unreliable -> datagram, reliable -> uni stream).
//! Inbound datagrams and streams are routed to the gateway, which relays them to
//! OTHER sessions.

pub mod cert;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use citadel_wire::Envelope;
use citadel_wire::protocol::KIND_AUTH_RESULT;
use tokio::sync::mpsc;
use web_transport_quinn::{Server, Session};

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::lifecycle::{AsyncService, CancellationToken};
use crate::realtime::{Gateway, Outbound, ParticipantId, SessionHandle};
use crate::transport::codec::{decode_datagram, decode_framed};
use crate::transport::metrics::TransportMetrics;
use crate::transport::{Delivery, Listener, TransportKind};

pub use cert::WebTransportDevCert;

/// Per-connection outbound channel capacity (envelopes).
const OUTBOUND_CAPACITY: usize = 1024;
/// Bound on a single inbound stream read.
const MAX_STREAM_BYTES: usize = 16 * 1024 * 1024;

/// A bound WebTransport server, ready to run as an [`AsyncService`].
pub struct WebTransportServer {
    server: Server,
    local_addr: SocketAddr,
    name: String,
    metrics: TransportMetrics,
    gateway: Arc<Gateway>,
    cert_sha256_base64: String,
    /// How long to wait for the client's `KIND_AUTH` handshake frame.
    handshake_timeout: Duration,
}

impl WebTransportServer {
    /// Bind a WebTransport endpoint with a private gateway (tests/standalone).
    pub fn bind(bind: SocketAddr, cert: &WebTransportDevCert) -> AppResult<Self> {
        Self::bind_with_gateway(bind, cert, Arc::new(Gateway::new()))
    }

    /// Bind a WebTransport endpoint at `bind` using `cert`, sharing `gateway`.
    pub fn bind_with_gateway(
        bind: SocketAddr,
        cert: &WebTransportDevCert,
        gateway: Arc<Gateway>,
    ) -> AppResult<Self> {
        let server = web_transport_quinn::ServerBuilder::new()
            .with_addr(bind)
            .with_certificate(cert.cert_chain.clone(), cert.key()?)
            .map_err(|e| {
                AppError::new(
                    ErrorCategory::Transport,
                    format!("failed to bind WebTransport endpoint on {bind}"),
                )
                .with_detail(e.to_string())
            })?;
        let local_addr = server.local_addr().map_err(|e| {
            AppError::new(
                ErrorCategory::Transport,
                "failed to read WebTransport local address",
            )
            .with_detail(e.to_string())
        })?;
        Ok(Self {
            server,
            local_addr,
            name: "webtransport".to_string(),
            metrics: TransportMetrics::new(),
            gateway,
            cert_sha256_base64: cert.cert_sha256_base64(),
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

    /// The dev cert SHA-256 (base64) for the browser `serverCertificateHashes`.
    #[must_use]
    pub fn cert_sha256_base64(&self) -> &str {
        &self.cert_sha256_base64
    }
}

impl Listener for WebTransportServer {
    fn transport_kind(&self) -> TransportKind {
        TransportKind::WebTransport
    }
    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl AsyncService for WebTransportServer {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(mut self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        tracing::info!(
            addr = %self.local_addr,
            cert_sha256_base64 = %self.cert_sha256_base64,
            "WebTransport listener accepting sessions (browser serverCertificateHashes shown)"
        );
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    tracing::info!("WebTransport listener shutting down");
                    break;
                }
                request = self.server.accept() => {
                    let Some(request) = request else { break };
                    let gateway = Arc::clone(&self.gateway);
                    let metrics = self.metrics.clone();
                    let conn_cancel = cancel.clone();
                    let handshake_timeout = self.handshake_timeout;
                    tokio::spawn(async move {
                        match request.ok().await {
                            Ok(session) => {
                                if let Err(e) = handle_session(session, conn_cancel, metrics, gateway, handshake_timeout).await {
                                    tracing::debug!(error = %e, "WebTransport session ended with error");
                                }
                            }
                            Err(e) => tracing::debug!(error = %e, "WebTransport handshake failed"),
                        }
                    });
                }
            }
        }
        Ok(())
    }
}

/// Drive one WebTransport session: run the authenticated handshake, and — only
/// once accepted — register a gateway session, run the gateway-fed write task,
/// and route inbound envelopes to the gateway.
///
/// Mirrors the QUIC/WebSocket gating: pending until the first
/// reliable-stream envelope resolves; datagrams before auth are dropped.
async fn handle_session(
    session: Session,
    cancel: CancellationToken,
    metrics: TransportMetrics,
    gateway: Arc<Gateway>,
    handshake_timeout: Duration,
) -> AppResult<()> {
    metrics.connection_opened();
    gateway.connection_opened();
    tracing::debug!("WebTransport session established; awaiting auth handshake");

    // Phase 1 (PENDING_AUTH): await the first reliable-stream envelope.
    let first_frames = match tokio::time::timeout(
        handshake_timeout,
        read_handshake_frames(&session, &cancel, &metrics),
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
            tracing::debug!("WebTransport auth handshake timed out; closing");
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
        send_reliable_envelope(
            &session,
            &Envelope::new(KIND_AUTH_RESULT, handshake.outcome.result_body()),
        )
        .await;
        metrics.connection_closed();
        gateway.connection_closed();
        tracing::debug!("WebTransport auth handshake rejected; session closed");
        return Ok(());
    }

    // Phase 2 (REGISTERED): bind identity and register.
    let session_id = gateway.next_participant_id();
    let identity = handshake.outcome.identity();
    let authenticated = identity.is_some();
    let (tx, rx) = mpsc::channel::<Outbound>(OUTBOUND_CAPACITY);
    // Acknowledge only when the client actually sent a KIND_AUTH frame (a legacy
    // implicit guest never asked for auth).
    if !handshake.replay_first {
        let ack = Outbound::reliable(Envelope::new(
            KIND_AUTH_RESULT,
            handshake.outcome.result_body(),
        ));
        let _ = tx.try_send(ack);
    }
    gateway.register_session(SessionHandle {
        id: session_id,
        kind: TransportKind::WebTransport,
        outbound: tx,
        identity,
    });
    tracing::debug!(%session_id, authenticated, "WebTransport session authenticated; registered");

    if handshake.replay_first {
        metrics.envelope_received();
        gateway.handle_inbound(session_id, first);
    }
    for env in queued {
        metrics.envelope_received();
        gateway.handle_inbound(session_id, env);
    }

    let write_session = session.clone();
    let write_metrics = metrics.clone();
    let write_cancel = cancel.clone();
    let writer = tokio::spawn(async move {
        outbound_writer(write_session, rx, write_metrics, write_cancel).await;
    });

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            datagram = session.read_datagram() => {
                match datagram {
                    Ok(bytes) => match decode_datagram(&bytes) {
                        Ok(env) => {
                            metrics.envelope_received();
                            gateway.handle_inbound(session_id, &env);
                        }
                        Err(e) => {
                            metrics.decode_error();
                            tracing::debug!(%session_id, error = %e, "bad WebTransport datagram");
                        }
                    },
                    Err(_) => break,
                }
            }
            stream = session.accept_uni() => {
                match stream {
                    Ok(recv) => spawn_inbound_stream(session_id, recv, metrics.clone(), Arc::clone(&gateway)),
                    Err(_) => break,
                }
            }
            stream = session.accept_bi() => {
                match stream {
                    Ok((_send, recv)) => spawn_inbound_stream(session_id, recv, metrics.clone(), Arc::clone(&gateway)),
                    Err(_) => break,
                }
            }
        }
    }

    gateway.unregister_session(session_id);
    writer.abort();
    metrics.connection_closed();
    gateway.connection_closed();
    tracing::debug!(%session_id, "WebTransport session closed");
    Ok(())
}

/// Spawn a task that reads framed envelopes from one inbound stream and routes
/// each to the gateway.
fn spawn_inbound_stream(
    session_id: ParticipantId,
    recv: web_transport_quinn::RecvStream,
    metrics: TransportMetrics,
    gateway: Arc<Gateway>,
) {
    tokio::spawn(async move {
        if let Err(e) = read_inbound_stream(session_id, recv, &metrics, &gateway).await {
            tracing::debug!(%session_id, error = %e, "WebTransport inbound stream ended");
        }
    });
}

async fn read_inbound_stream(
    session_id: ParticipantId,
    mut recv: web_transport_quinn::RecvStream,
    metrics: &TransportMetrics,
    gateway: &Gateway,
) -> AppResult<()> {
    let data = recv.read_to_end(MAX_STREAM_BYTES).await.map_err(|e| {
        AppError::new(
            ErrorCategory::Transport,
            "failed to read WebTransport stream",
        )
        .with_detail(e.to_string())
    })?;
    let mut buf = BytesMut::from(&data[..]);
    while let Some(env) = decode_framed(&mut buf)? {
        metrics.envelope_received();
        gateway.handle_inbound(session_id, &env);
    }
    Ok(())
}

/// Await the first reliable-stream envelope of the session (the handshake),
/// dropping any datagrams that arrive before auth resolves.
async fn read_handshake_frames(
    session: &Session,
    cancel: &CancellationToken,
    metrics: &TransportMetrics,
) -> Option<Vec<Envelope>> {
    loop {
        tokio::select! {
            () = cancel.cancelled() => return None,
            datagram = session.read_datagram() => {
                match datagram {
                    Ok(_bytes) => {
                        tracing::debug!("WebTransport datagram received before auth handshake; dropped");
                        continue;
                    }
                    Err(_) => return None,
                }
            }
            stream = session.accept_uni() => {
                match stream {
                    Ok(recv) => {
                        if let Some(frames) = read_stream_frames(recv, metrics).await {
                            return Some(frames);
                        }
                    }
                    Err(_) => return None,
                }
            }
            stream = session.accept_bi() => {
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
async fn read_stream_frames(
    mut recv: web_transport_quinn::RecvStream,
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
/// deliver a `KIND_AUTH_RESULT` rejection before the session is closed.
async fn send_reliable_envelope(session: &Session, env: &Envelope) {
    if let Ok(mut send) = session.open_uni().await {
        let frame = env.encode_framed();
        if send.write_all(&frame).await.is_ok() {
            let _ = send.finish();
        }
    }
}

/// Drain the gateway-fed outbound channel to the session: unreliable envelopes
/// go as datagrams, reliable ones as fresh uni streams.
async fn outbound_writer(
    session: Session,
    mut rx: mpsc::Receiver<Outbound>,
    metrics: TransportMetrics,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            next = rx.recv() => {
                let Some(out) = next else { break };
                match out.delivery {
                    Delivery::Unreliable => {
                        if session.send_datagram(out.envelope.encode_datagram()).is_ok() {
                            metrics.envelope_sent();
                        }
                    }
                    Delivery::Reliable => {
                        match session.open_uni().await {
                            Ok(mut send) => {
                                let frame = out.envelope.encode_framed();
                                if send.write_all(&frame).await.is_ok() {
                                    let _ = send.finish();
                                    metrics.envelope_sent();
                                }
                            }
                            Err(e) => tracing::debug!(error = %e, "failed to open WebTransport uni stream"),
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn server_binds_and_reports_local_addr() {
        // Binding builds a quinn endpoint, which requires a tokio runtime.
        let cert = WebTransportDevCert::generate(&["localhost".to_string()]).expect("cert");
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let server = WebTransportServer::bind(bind, &cert).expect("bind");
        assert_eq!(server.transport_kind(), TransportKind::WebTransport);
        assert_ne!(server.local_addr().port(), 0);
        assert_eq!(server.cert_sha256_base64().len(), 44);
    }
}
