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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use citadel_wire::Envelope;
use citadel_wire::protocol::{KIND_AUTH_RESULT, KIND_DIAG_SERVER_TIME};
use tokio::sync::mpsc;
use web_transport_quinn::{Server, Session};

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::lifecycle::{AsyncService, CancellationToken};
use crate::realtime::{Gateway, LatestOutboundReceiver, Outbound, ParticipantId, SessionHandle};
use crate::time::{Clock, SystemClock};
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
    // The registry owns and fences the pre-registration auth acknowledgement.
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
                tracing::error!(%session_id, error = %error, "failed to encode diagnostics server-time offer")
            }
        },
        Err(error) => {
            tracing::error!(%session_id, error = %error, "failed to issue diagnostics server-time offer")
        }
    }
    let registration = gateway.register_session_with_initials_at(
        SessionHandle {
            id: session_id,
            kind: TransportKind::WebTransport,
            outbound: tx,
            identity,
        },
        initials,
        SystemClock.now(),
    );
    let unreliable = registration.unreliable;
    let superseded = registration.superseded;
    let supersession_gate = registration.supersession_gate;
    let transport_write_gate = registration.transport_write_gate;
    let superseding = registration.superseding;
    let replaced_cleanup = registration.replaced_cleanup;
    let inbound_supersession_drained = registration.inbound_supersession_drained;
    if let Some(cleanup) = replaced_cleanup {
        let cleanup_gateway = Arc::clone(&gateway);
        tokio::spawn(async move {
            cleanup.wait_for_inbound_drain().await;
            cleanup_gateway.unregister_session(cleanup.participant_id());
        });
    }
    if !gateway.accepts_work(session_id) {
        gateway.abandon_diagnostics_session(session_id);
        metrics.connection_closed();
        gateway.connection_closed();
        return Ok(());
    }
    tracing::debug!(%session_id, authenticated, "WebTransport session authenticated; registered");

    if handshake.replay_first
        && !route_envelope(
            session_id,
            first,
            &metrics,
            &gateway,
            &superseded,
            &supersession_gate,
        )
    {
        session.close(0, b"session replaced");
        if superseded.is_cancelled() || superseding.load(std::sync::atomic::Ordering::Acquire) {
            inbound_supersession_drained.cancelled().await;
        }
        gateway.unregister_session(session_id);
        metrics.connection_closed();
        gateway.connection_closed();
        return Ok(());
    }
    for env in queued {
        if !route_envelope(
            session_id,
            env,
            &metrics,
            &gateway,
            &superseded,
            &supersession_gate,
        ) {
            session.close(0, b"session replaced");
            if superseded.is_cancelled() || superseding.load(std::sync::atomic::Ordering::Acquire) {
                inbound_supersession_drained.cancelled().await;
            }
            gateway.unregister_session(session_id);
            metrics.connection_closed();
            gateway.connection_closed();
            return Ok(());
        }
    }

    let write_session = session.clone();
    let write_metrics = metrics.clone();
    let write_cancel = cancel.clone();
    let write_superseded = superseded.clone();
    let write_gate = Arc::clone(&transport_write_gate);
    let write_superseding = Arc::clone(&superseding);
    let writer = tokio::spawn(async move {
        outbound_writer(
            write_session,
            rx,
            unreliable,
            write_metrics,
            write_cancel,
            write_superseded,
            write_gate,
            write_superseding,
        )
        .await;
    });

    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            () = superseded.cancelled() => {
                session.close(0, b"session replaced");
                break;
            }
            datagram = session.read_datagram() => {
                match datagram {
                    Ok(bytes) => {
                        if !route_datagram(session_id, &bytes, &metrics, &gateway, &superseded, &supersession_gate) {
                            session.close(0, b"session replaced");
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            stream = session.accept_uni() => {
                match stream {
                    Ok(recv) => spawn_inbound_stream(
                        session_id,
                        recv,
                        metrics.clone(),
                        Arc::clone(&gateway),
                        superseded.clone(),
                        Arc::clone(&supersession_gate),
                    ),
                    Err(_) => break,
                }
            }
            stream = session.accept_bi() => {
                match stream {
                    Ok((_send, recv)) => spawn_inbound_stream(
                        session_id,
                        recv,
                        metrics.clone(),
                        Arc::clone(&gateway),
                        superseded.clone(),
                        Arc::clone(&supersession_gate),
                    ),
                    Err(_) => break,
                }
            }
        }
    }

    if superseded.is_cancelled() || superseding.load(std::sync::atomic::Ordering::Acquire) {
        inbound_supersession_drained.cancelled().await;
    }
    gateway.unregister_session(session_id);
    writer.abort();
    metrics.connection_closed();
    gateway.connection_closed();
    tracing::debug!(%session_id, "WebTransport session closed");
    Ok(())
}

/// Route a datagram only while this exact transport generation remains current.
///
/// A `select!` may choose a concurrently ready datagram after a replacement has
/// cancelled the session. Check the fence here, immediately before decode and
/// metrics mutation, rather than relying on branch selection order.
fn route_datagram(
    session_id: ParticipantId,
    bytes: &[u8],
    metrics: &TransportMetrics,
    gateway: &Gateway,
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
) -> bool {
    let Ok(_gate) = supersession_gate.lock() else {
        return false;
    };
    if superseded.is_cancelled() || !gateway.accepts_work(session_id) {
        return false;
    }
    match decode_datagram(bytes) {
        Ok(env) => {
            metrics.envelope_received();
            gateway.handle_inbound_with_metadata(
                session_id,
                &env,
                crate::realtime::InboundMessageMetadata::unreliable(),
            );
        }
        Err(error) => {
            metrics.decode_error();
            tracing::debug!(%session_id, %error, "bad WebTransport datagram");
        }
    }
    true
}

/// Route a post-handshake frame that was decoded before registration only while
/// this transport generation remains current. Queued/replayed frames share the
/// same cancellation gate as stream and datagram bytes.
fn route_envelope(
    session_id: ParticipantId,
    env: &Envelope,
    metrics: &TransportMetrics,
    gateway: &Gateway,
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
) -> bool {
    route_envelope_with_before_handoff(
        session_id,
        env,
        metrics,
        gateway,
        superseded,
        supersession_gate,
        || {},
    )
}

fn route_envelope_with_before_handoff<F>(
    session_id: ParticipantId,
    env: &Envelope,
    metrics: &TransportMetrics,
    gateway: &Gateway,
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
    before_handoff: F,
) -> bool
where
    F: FnOnce(),
{
    let Ok(_gate) = supersession_gate.lock() else {
        return false;
    };
    if superseded.is_cancelled() || !gateway.accepts_work(session_id) {
        return false;
    }
    before_handoff();
    metrics.envelope_received();
    gateway.handle_inbound_with_metadata(
        session_id,
        env,
        crate::realtime::InboundMessageMetadata::reliable(),
    );
    true
}

/// Spawn a task that reads framed envelopes from one inbound stream and routes
/// each to the gateway.
fn spawn_inbound_stream(
    session_id: ParticipantId,
    recv: web_transport_quinn::RecvStream,
    metrics: TransportMetrics,
    gateway: Arc<Gateway>,
    superseded: CancellationToken,
    supersession_gate: Arc<Mutex<()>>,
) {
    tokio::spawn(async move {
        if let Err(e) = read_inbound_stream(
            session_id,
            recv,
            &metrics,
            &gateway,
            &superseded,
            &supersession_gate,
        )
        .await
        {
            tracing::debug!(%session_id, error = %e, "WebTransport inbound stream ended");
        }
    });
}

async fn read_inbound_stream(
    session_id: ParticipantId,
    mut recv: web_transport_quinn::RecvStream,
    metrics: &TransportMetrics,
    gateway: &Gateway,
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
) -> AppResult<()> {
    let data = tokio::select! {
        () = superseded.cancelled() => return Ok(()),
        result = recv.read_to_end(MAX_STREAM_BYTES) => result,
    }
    .map_err(|e| {
        AppError::new(
            ErrorCategory::Transport,
            "failed to read WebTransport stream",
        )
        .with_detail(e.to_string())
    })?;
    let mut buf = BytesMut::from(&data[..]);
    while route_framed_envelope(
        session_id,
        &mut buf,
        metrics,
        gateway,
        superseded,
        supersession_gate,
    )? {}
    Ok(())
}

/// Decode and route one reliable frame only while this transport remains current.
///
/// The same per-generation gate used by datagrams is held across the cancellation
/// check, frame decode, metrics mutation, and gateway handoff. Replacement takes
/// that gate before cancelling, so it cannot linearize in the gap after an
/// inbound stream checks cancellation but before it decodes client-controlled
/// bytes or records them as received.
fn route_framed_envelope(
    session_id: ParticipantId,
    buf: &mut BytesMut,
    metrics: &TransportMetrics,
    gateway: &Gateway,
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
) -> AppResult<bool> {
    route_framed_envelope_with_before_decode(
        session_id,
        buf,
        metrics,
        gateway,
        superseded,
        supersession_gate,
        || {},
    )
}

fn route_framed_envelope_with_before_decode<F>(
    session_id: ParticipantId,
    buf: &mut BytesMut,
    metrics: &TransportMetrics,
    gateway: &Gateway,
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
    before_decode: F,
) -> AppResult<bool>
where
    F: FnOnce(),
{
    let Ok(_gate) = supersession_gate.lock() else {
        return Ok(false);
    };
    if superseded.is_cancelled() || !gateway.accepts_work(session_id) {
        return Ok(false);
    }
    before_decode();
    let Some(env) = decode_framed(buf)? else {
        return Ok(false);
    };
    metrics.envelope_received();
    gateway.handle_inbound_with_metadata(
        session_id,
        &env,
        crate::realtime::InboundMessageMetadata::reliable(),
    );
    Ok(true)
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
#[allow(clippy::too_many_arguments)]
async fn outbound_writer(
    session: Session,
    mut rx: mpsc::Receiver<Outbound>,
    unreliable: LatestOutboundReceiver,
    metrics: TransportMetrics,
    cancel: CancellationToken,
    superseded: CancellationToken,
    transport_write_gate: Arc<tokio::sync::Mutex<()>>,
    superseding: Arc<std::sync::atomic::AtomicBool>,
) {
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            () = superseded.cancelled() => break,
            next = rx.recv() => {
                let Some(out) = next else { break };
                write_outbound(&session, out, &metrics, &superseded, &superseding, &transport_write_gate).await;
            }
            out = unreliable.recv() => {
                write_outbound(&session, out, &metrics, &superseded, &superseding, &transport_write_gate).await;
            }
        }
    }
}

async fn write_outbound(
    session: &Session,
    out: Outbound,
    metrics: &TransportMetrics,
    superseded: &CancellationToken,
    superseding: &std::sync::atomic::AtomicBool,
    transport_write_gate: &tokio::sync::Mutex<()>,
) {
    let Some(_delivery) = out.acquire_delivery().await else {
        return;
    };
    match out.delivery {
        Delivery::Unreliable => {
            if crate::transport::write_if_current(
                superseded,
                superseding,
                transport_write_gate,
                || async { session.send_datagram(out.envelope.encode_datagram()) },
            )
            .await
            .is_some_and(|result| result.is_ok())
            {
                metrics.envelope_sent();
            }
        }
        Delivery::Reliable => {
            let frame = out.envelope.encode_framed();
            if crate::transport::write_if_current(
                superseded,
                superseding,
                transport_write_gate,
                || async {
                    let Ok(mut send) = session.open_uni().await else {
                        return Err(());
                    };
                    if send.write_all(&frame).await.is_ok() {
                        let _ = send.finish();
                        Ok(())
                    } else {
                        Err(())
                    }
                },
            )
            .await
            .is_some_and(|result| result.is_ok())
            {
                metrics.envelope_sent();
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

    #[tokio::test]
    async fn outbound_write_waits_before_supersession_is_published() {
        let superseded = CancellationToken::new();
        let superseding = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let writer_token = superseded.clone();
        let writer_superseding = Arc::clone(&superseding);
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move {
            crate::transport::write_if_current(
                &writer_token,
                &writer_superseding,
                &writer_gate,
                || async {
                    entered_tx.send(()).expect("write entered");
                    release_rx.await.expect("write released");
                },
            )
            .await
        });
        entered_rx.await.expect("application write admitted");
        superseding.store(true, std::sync::atomic::Ordering::Release);
        let cancellation_gate = Arc::clone(&gate);
        let cancellation_token = superseded.clone();
        let cancellation = tokio::spawn(async move {
            let _write = cancellation_gate.lock().await;
            cancellation_token.cancel();
        });
        tokio::task::yield_now().await;
        assert!(
            !superseded.is_cancelled(),
            "WebTransport supersession cannot publish while an admitted application frame writes"
        );
        release_tx.send(()).expect("release application write");
        assert!(writer.await.expect("writer task").is_some());
        cancellation.await.expect("cancellation task");
        assert!(superseded.is_cancelled());
    }

    #[tokio::test]
    async fn superseding_reliable_delivery_does_not_admit_a_webtransport_stream_open() {
        let superseded = CancellationToken::new();
        let superseding = std::sync::atomic::AtomicBool::new(true);
        let transport_write_gate = tokio::sync::Mutex::new(());
        let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_opens = Arc::clone(&opens);

        let admitted = crate::transport::write_if_current(
            &superseded,
            &superseding,
            &transport_write_gate,
            || async move {
                // This closure is the reliable write admission and includes
                // `session.open_uni().await` in `write_outbound`.
                observed_opens.fetch_add(1, std::sync::atomic::Ordering::Release);
            },
        )
        .await;
        assert!(admitted.is_none());
        assert_eq!(opens.load(std::sync::atomic::Ordering::Acquire), 0);
    }

    #[test]
    fn replacement_gate_prevents_post_check_datagram_interleaving() {
        let superseded = CancellationToken::new();
        let supersession_gate = Arc::new(Mutex::new(()));
        let held = supersession_gate.lock().expect("gate");
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let replacement_gate = Arc::clone(&supersession_gate);
        let replacement_token = superseded.clone();
        let replacement = std::thread::spawn(move || {
            started_tx.send(()).expect("replacement started");
            let _gate = replacement_gate.lock().expect("gate");
            replacement_token.cancel();
        });
        started_rx
            .recv()
            .expect("replacement is waiting on the gate");
        assert!(
            !superseded.is_cancelled(),
            "replacement cannot cancel between a datagram's gate/check and decode/metrics"
        );
        drop(held);
        replacement.join().expect("replacement thread");
        assert!(superseded.is_cancelled());

        let metrics = TransportMetrics::new();
        assert!(!route_datagram(
            ParticipantId::from_raw(1),
            &[0],
            &metrics,
            &Gateway::new(),
            &superseded,
            &supersession_gate,
        ));
        assert_eq!(metrics.snapshot(), Default::default());
    }

    #[test]
    fn superseded_ready_datagram_is_not_decoded_or_counted() {
        let superseded = CancellationToken::new();
        superseded.cancel();
        let metrics = TransportMetrics::new();
        let gateway = Gateway::new();

        assert!(
            !route_datagram(
                ParticipantId::from_raw(1),
                &[0],
                &metrics,
                &gateway,
                &superseded,
                &Arc::new(Mutex::new(())),
            ),
            "a ready datagram selected after replacement must be rejected before decode"
        );
        assert_eq!(
            metrics.snapshot(),
            Default::default(),
            "a superseded datagram must not mutate decode or envelope counters"
        );
    }

    #[test]
    fn reliable_stream_gate_makes_replacement_linearizable_before_decode_and_metrics() {
        let superseded = CancellationToken::new();
        let supersession_gate = Arc::new(Mutex::new(()));
        let metrics = TransportMetrics::new();
        let gateway = Gateway::new();
        let session_id = gateway.next_participant_id();
        let (outbound, _outbound_rx) = mpsc::channel(8);
        gateway.register_session(SessionHandle {
            id: session_id,
            kind: TransportKind::WebTransport,
            outbound,
            identity: None,
        });
        let framed =
            Envelope::new(citadel_wire::protocol::KIND_POSITION, b"late".to_vec()).encode_framed();
        let mut bytes = BytesMut::from(&framed[..]);
        let (start_tx, start_rx) = std::sync::mpsc::sync_channel(1);
        let (blocked_tx, blocked_rx) = std::sync::mpsc::sync_channel(1);
        let replacement_gate = Arc::clone(&supersession_gate);
        let replacement_token = superseded.clone();
        let replacement = std::thread::spawn(move || {
            start_rx.recv().expect("start replacement");
            assert!(matches!(
                replacement_gate.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            blocked_tx.send(()).expect("replacement is blocked");
            let _gate = replacement_gate.lock().expect("gate");
            replacement_token.cancel();
        });

        assert!(
            route_framed_envelope_with_before_decode(
                session_id,
                &mut bytes,
                &metrics,
                &gateway,
                &superseded,
                &supersession_gate,
                || {
                    start_tx.send(()).expect("start replacement");
                    blocked_rx.recv().expect("replacement checked gate");
                },
            )
            .expect("frame routes before replacement linearizes")
        );
        assert_eq!(metrics.snapshot().envelopes_received, 1);
        replacement.join().expect("replacement thread");
        assert!(superseded.is_cancelled());
    }

    #[test]
    fn superseded_ready_reliable_stream_is_not_decoded_or_counted() {
        let superseded = CancellationToken::new();
        superseded.cancel();
        let metrics = TransportMetrics::new();
        let gateway = Gateway::new();
        let framed =
            Envelope::new(citadel_wire::protocol::KIND_POSITION, b"late".to_vec()).encode_framed();
        let mut bytes = BytesMut::from(&framed[..]);

        assert!(
            !route_framed_envelope(
                ParticipantId::from_raw(1),
                &mut bytes,
                &metrics,
                &gateway,
                &superseded,
                &Arc::new(Mutex::new(())),
            )
            .expect("supersession is an orderly stream stop"),
            "a ready reliable stream selected after replacement must stop before decode"
        );
        assert_eq!(
            metrics.snapshot(),
            Default::default(),
            "a superseded reliable stream must not mutate decode or envelope counters"
        );
    }

    #[test]
    fn superseded_queued_replay_is_not_counted_or_routed() {
        let superseded = CancellationToken::new();
        superseded.cancel();
        let metrics = TransportMetrics::new();
        let gateway = Gateway::new();
        let queued = Envelope::new(citadel_wire::protocol::KIND_POSITION, b"late".to_vec());

        assert!(
            !route_envelope(
                ParticipantId::from_raw(1),
                &queued,
                &metrics,
                &gateway,
                &superseded,
                &Arc::new(Mutex::new(())),
            ),
            "a replacement that wins before replay must suppress the queued frame"
        );
        assert_eq!(metrics.snapshot(), Default::default());
    }

    #[test]
    fn queued_replay_holds_the_supersession_gate_through_gateway_handoff() {
        let superseded = CancellationToken::new();
        let gate = Arc::new(Mutex::new(()));
        let metrics = TransportMetrics::new();
        let gateway = Gateway::new();
        let session_id = gateway.next_participant_id();
        let (outbound, _outbound_rx) = mpsc::channel(8);
        gateway.register_session(SessionHandle {
            id: session_id,
            kind: TransportKind::WebTransport,
            outbound,
            identity: None,
        });
        let queued = Envelope::new(citadel_wire::protocol::KIND_POSITION, b"queued".to_vec());
        let (start_tx, start_rx) = std::sync::mpsc::sync_channel(1);
        let (blocked_tx, blocked_rx) = std::sync::mpsc::sync_channel(1);
        let replacement_gate = Arc::clone(&gate);
        let replacement_token = superseded.clone();
        let replacement = std::thread::spawn(move || {
            start_rx.recv().expect("start replacement");
            assert!(matches!(
                replacement_gate.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ));
            blocked_tx.send(()).expect("replacement blocked");
            let _gate = replacement_gate.lock().expect("gate");
            replacement_token.cancel();
        });

        assert!(route_envelope_with_before_handoff(
            session_id,
            &queued,
            &metrics,
            &gateway,
            &superseded,
            &gate,
            || {
                start_tx.send(()).expect("start replacement");
                blocked_rx.recv().expect("replacement checked gate");
            },
        ));
        assert_eq!(metrics.snapshot().envelopes_received, 1);
        replacement.join().expect("replacement thread");
        assert!(superseded.is_cancelled());
    }
}
