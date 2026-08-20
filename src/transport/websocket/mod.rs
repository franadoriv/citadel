//! WebSocket fallback transport for Citadel's realtime layer.
//!
//! Built on `tokio-tungstenite`, this is the reliable, ordered fallback for
//! browsers without WebTransport and for networks that block UDP/QUIC. It
//! carries the typed realtime [`Envelope`](crate::transport::Envelope) inside
//! binary WebSocket messages using the framed codec, and implements both
//! [`Listener`] (synchronous identity) and [`AsyncService`] (so the lifecycle
//! [`Supervisor`](crate::lifecycle::Supervisor) starts and stops it).
//!
//! Behavior: each accepted connection mints a realtime session in the shared
//! [`Gateway`], registers a bounded outbound channel, and runs a write task that
//! drains that channel to the socket as binary framed messages. Inbound binary
//! messages are decoded and routed to [`Gateway::handle_inbound`], which relays
//! them to OTHER sessions. WebSocket is reliable-only (no unreliable datagrams).
//!
//! Concurrency model: one accept loop on a tokio `TcpListener` awaiting the
//! cancellation token; each accepted TCP connection is upgraded to WebSocket and
//! handled by its own task that reads inbound and concurrently writes outbound.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use std::time::Duration;

use citadel_wire::Envelope;
use citadel_wire::protocol::{KIND_AUTH_RESULT, KIND_DIAG_SERVER_TIME};

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::error_reporting;
use crate::lifecycle::{AsyncService, CancellationToken};
use crate::realtime::{Gateway, Outbound, SessionHandle};
use crate::time::{Clock, SystemClock};
use crate::transport::codec::decode_framed;
use crate::transport::metrics::TransportMetrics;
use crate::transport::{
    Connection, ConnectionId, ConnectionIdGen, Listener, PeerAddr, TransportKind,
};

/// Per-connection outbound channel capacity (envelopes).
const OUTBOUND_CAPACITY: usize = 1024;

/// A bound WebSocket server, ready to run as an [`AsyncService`].
pub struct WebSocketServer {
    listener: TcpListener,
    local_addr: SocketAddr,
    name: String,
    ids: Arc<ConnectionIdGen>,
    metrics: TransportMetrics,
    gateway: Arc<Gateway>,
    /// How long to wait for the client's `KIND_AUTH` handshake frame.
    handshake_timeout: Duration,
    heartbeat_interval: Option<Duration>,
    heartbeat_timeout: Duration,
}

impl WebSocketServer {
    /// Bind a WebSocket listener with a private gateway (tests/standalone use).
    pub async fn bind(bind: SocketAddr) -> AppResult<Self> {
        Self::bind_with_gateway(bind, Arc::new(Gateway::new())).await
    }

    /// Bind a WebSocket listener at `bind`, sharing `gateway` with other
    /// transports so they route to one room.
    pub async fn bind_with_gateway(bind: SocketAddr, gateway: Arc<Gateway>) -> AppResult<Self> {
        let listener = TcpListener::bind(bind).await.map_err(|e| {
            AppError::new(
                ErrorCategory::Transport,
                format!("failed to bind WebSocket listener on {bind}"),
            )
            .with_detail(e.to_string())
        })?;
        let local_addr = listener.local_addr().map_err(|e| {
            AppError::new(
                ErrorCategory::Transport,
                "failed to read WebSocket local address",
            )
            .with_detail(e.to_string())
        })?;
        Ok(Self {
            listener,
            local_addr,
            name: "websocket".to_string(),
            ids: Arc::new(ConnectionIdGen::new()),
            metrics: TransportMetrics::new(),
            gateway,
            handshake_timeout: Duration::from_millis(5_000),
            heartbeat_interval: Some(Duration::from_secs(15)),
            heartbeat_timeout: Duration::from_secs(45),
        })
    }

    /// Set the realtime auth handshake timeout. Wired from
    /// `transport.auth.handshake_timeout_ms` at startup.
    #[must_use]
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Configure native Ping/Pong liveness after authentication. A zero
    /// interval disables probing; timeout is clamped to one millisecond.
    #[must_use]
    pub fn with_liveness(mut self, interval: Duration, timeout: Duration) -> Self {
        self.heartbeat_interval = (!interval.is_zero()).then_some(interval);
        self.heartbeat_timeout = timeout.max(Duration::from_millis(1));
        self
    }

    /// The local socket address the listener is bound to.
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

impl Listener for WebSocketServer {
    fn transport_kind(&self) -> TransportKind {
        TransportKind::WebSocket
    }
    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl AsyncService for WebSocketServer {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        tracing::info!(addr = %self.local_addr, "WebSocket listener accepting connections");
        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    tracing::info!("WebSocket listener shutting down");
                    break;
                }
                accepted = self.listener.accept() => {
                    match accepted {
                        Ok((stream, peer)) => {
                            let id = self.ids.next_id();
                            let context = WebSocketConnectionContext {
                                id,
                                cancel: cancel.clone(),
                                metrics: self.metrics.clone(),
                                gateway: Arc::clone(&self.gateway),
                                handshake_timeout: self.handshake_timeout,
                                heartbeat_interval: self.heartbeat_interval,
                                heartbeat_timeout: self.heartbeat_timeout,
                            };
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, peer, context).await {
                                    tracing::debug!(conn = %id, error = %e, "WebSocket connection ended");
                                }
                            });
                        }
                        Err(e) => {
                            tracing::debug!(error = %e, "WebSocket accept failed");
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// A handle to an accepted WebSocket connection, implementing [`Connection`].
pub struct WebSocketConnection {
    id: ConnectionId,
    peer: PeerAddr,
}

impl Connection for WebSocketConnection {
    fn id(&self) -> ConnectionId {
        self.id
    }
    fn peer_addr(&self) -> PeerAddr {
        self.peer
    }
    fn transport_kind(&self) -> TransportKind {
        TransportKind::WebSocket
    }
}

/// Per-connection services and limits captured when the accept loop spawns a
/// WebSocket task. Grouping them keeps the connection entry point coherent as
/// transport capabilities evolve.
struct WebSocketConnectionContext {
    id: ConnectionId,
    cancel: CancellationToken,
    metrics: TransportMetrics,
    gateway: Arc<Gateway>,
    handshake_timeout: Duration,
    heartbeat_interval: Option<Duration>,
    heartbeat_timeout: Duration,
}

/// Upgrade one TCP stream to WebSocket, run the authenticated handshake, and —
/// only once the connection is accepted — register a session, run the
/// gateway-fed write task, and route inbound envelopes to the gateway.
///
/// The handshake gates registration: the connection stays in a
/// pending state (no `SessionHandle`, no `on_join`, no routing, no gauge) until
/// the first envelope resolves. A rejection sends one `KIND_AUTH_RESULT` and
/// closes with no participant/session state ever created.
async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    context: WebSocketConnectionContext,
) -> AppResult<()> {
    let WebSocketConnectionContext {
        id,
        cancel,
        metrics,
        gateway,
        handshake_timeout,
        heartbeat_interval,
        heartbeat_timeout,
    } = context;
    let _conn = WebSocketConnection {
        id,
        peer: PeerAddr::new(peer),
    };
    let ws = tokio_tungstenite::accept_async(stream).await.map_err(|e| {
        AppError::new(ErrorCategory::Transport, "WebSocket handshake failed")
            .with_detail(e.to_string())
    })?;
    metrics.connection_opened();
    gateway.connection_opened();
    tracing::debug!(conn = %id, %peer, "WebSocket connection established; awaiting auth handshake");

    let (mut writer, mut reader) = ws.split();

    // Phase 1 (PENDING_AUTH): await the first envelope, bounded by the deadline.
    // No participant is registered until this resolves.
    let first_frames = match tokio::time::timeout(
        handshake_timeout,
        read_first_frames(&mut reader, &cancel, &metrics),
    )
    .await
    {
        Ok(Ok(Some(frames))) => frames,
        // Clean close / cancellation / no frame before the deadline: tear down
        // the bare connection, nothing was ever registered.
        Ok(Ok(None)) => {
            let _ = writer.close().await;
            metrics.connection_closed();
            gateway.connection_closed();
            return Ok(());
        }
        Ok(Err(e)) => {
            let _ = writer.close().await;
            metrics.connection_closed();
            gateway.connection_closed();
            return Err(e);
        }
        Err(_elapsed) => {
            tracing::debug!(conn = %id, "WebSocket auth handshake timed out; closing");
            let _ = writer.close().await;
            metrics.connection_closed();
            gateway.connection_closed();
            return Ok(());
        }
    };

    // The first frame is the handshake; any frames batched behind it in the same
    // message are queued for post-registration dispatch.
    let Some((first, queued)) = first_frames.split_first() else {
        // Unreachable: read_first_frames only returns Some for a non-empty batch.
        let _ = writer.close().await;
        metrics.connection_closed();
        gateway.connection_closed();
        return Ok(());
    };
    let handshake = gateway.resolve_handshake(first).await;

    if !handshake.outcome.is_accepted() {
        // Rejection: send exactly one KIND_AUTH_RESULT and close. Never register.
        let body = handshake.outcome.result_body();
        let frame = Envelope::new(KIND_AUTH_RESULT, body).encode_framed();
        let _ = writer.send(Message::Binary(frame.to_vec())).await;
        let _ = writer.close().await;
        metrics.connection_closed();
        gateway.connection_closed();
        tracing::debug!(conn = %id, "WebSocket auth handshake rejected; connection closed");
        return Ok(());
    }

    // Phase 2 (REGISTERED): bind identity and register.
    let session_id = gateway.next_participant_id();
    let identity = handshake.outcome.identity();
    let authenticated = identity.is_some();
    let (tx, mut rx) = mpsc::channel::<Outbound>(OUTBOUND_CAPACITY);
    // Seed the accepted auth result through the registry-owned fence before
    // publishing this session. A revocation racing the writer can therefore
    // invalidate it just like every later outbound envelope.
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
    let registration = gateway.register_session_with_initials_at(
        SessionHandle {
            id: session_id,
            kind: TransportKind::WebSocket,
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
    tracing::debug!(
        conn = %id, %session_id, authenticated,
        "WebSocket connection authenticated; session registered"
    );

    let result: AppResult<()> = async {
        // Replay the first frame for a pre-handshake (legacy) client, then any
        // frames batched behind it, so nothing sent before registration is lost.
        if handshake.replay_first {
            if !route_envelope(
                session_id,
                first,
                &metrics,
                &gateway,
                &superseded,
                &supersession_gate,
            ) {
                return Ok(());
            }
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
                return Ok(());
            }
        }

        let mut heartbeat = heartbeat_interval.map(tokio::time::interval);
        if let Some(ticker) = &mut heartbeat {
            // Tokio intervals tick immediately; consume that instant so the
            // first probe occurs after a full configured interval.
            ticker.tick().await;
        }
        let mut pong_deadline = None;
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = superseded.cancelled() => break,
                () = async {
                    match pong_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    if !route_heartbeat_timeout(
                        &metrics,
                        &gateway,
                        &superseded,
                        &supersession_gate,
                    ) {
                        break;
                    }
                    error_reporting::report_app_error(
                        "transport.websocket.liveness",
                        &AppError::new(ErrorCategory::Transport, "WebSocket liveness timeout"),
                    );
                    tracing::warn!(conn = %id, %session_id, "WebSocket liveness timeout; closing unresponsive peer");
                    break;
                }
                () = async {
                    match &mut heartbeat {
                        Some(ticker) => {
                            ticker.tick().await;
                        }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    // At most one probe is outstanding. The deadline branch
                    // above owns failure, so no probe queue can accumulate.
                    if pong_deadline.is_none() {
                        if !send_heartbeat_ping(
                            &mut writer,
                            &superseded,
                            &superseding,
                            &transport_write_gate,
                        )
                        .await?
                        {
                            break;
                        }
                        metrics.ping_sent();
                        gateway.node_metrics().record_websocket_ping_sent();
                        pong_deadline = Some(tokio::time::Instant::now() + heartbeat_timeout);
                    }
                }
                // Outbound: relay messages from the gateway to this peer.
                out = rx.recv() => {
                    let Some(out) = out else { break };
                    let Some(_delivery) = out.acquire_delivery().await else { continue };
                    let frame = out.envelope.encode_framed();
                    if let Some(result) = crate::transport::write_if_current(
                        &superseded,
                        &superseding,
                        &transport_write_gate,
                        || async { writer.send(Message::Binary(frame.to_vec())).await },
                    )
                    .await
                    {
                        result.map_err(send_err)?;
                        metrics.envelope_sent();
                    }
                }
                // Unreliable state is coalesced by its state key. Peer
                // positions include their sender ID in that key, so a browser
                // retains the newest state for every visible peer without a
                // stale FIFO replay.
                out = unreliable.recv() => {
                    let Some(_delivery) = out.acquire_delivery().await else { continue };
                    let frame = out.envelope.encode_framed();
                    if let Some(result) = crate::transport::write_if_current(
                        &superseded,
                        &superseding,
                        &transport_write_gate,
                        || async { writer.send(Message::Binary(frame.to_vec())).await },
                    )
                    .await
                    {
                        result.map_err(send_err)?;
                        metrics.envelope_sent();
                    }
                }
                // Inbound: decode framed envelopes and route to the gateway.
                msg = reader.next() => {
                    let Some(msg) = msg else { break };
                    let msg = msg.map_err(|e| {
                        AppError::new(ErrorCategory::Transport, "WebSocket read error")
                            .with_detail(e.to_string())
                    })?;
                    match msg {
                        Message::Binary(data) => {
                            let mut buf = BytesMut::from(&data[..]);
                            while route_framed_envelope(
                                session_id,
                                &mut buf,
                                &metrics,
                                &gateway,
                                &superseded,
                                &supersession_gate,
                            )? {
                            }
                        }
                        Message::Close(_) => break,
                        Message::Ping(payload) => {
                            if !reply_to_ping(
                                &mut writer,
                                payload,
                                &superseded,
                                &superseding,
                                &transport_write_gate,
                            )
                            .await?
                            {
                                break;
                            }
                        }
                        Message::Pong(_) => {
                            if pong_deadline.is_some()
                                && route_pong_received(
                                    &metrics,
                                    &gateway,
                                    &superseded,
                                    &supersession_gate,
                                )
                            {
                                pong_deadline = None;
                            }
                        }
                        // Text and other frames are ignored in the binary-only MVP.
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }
    .await;

    if superseded.is_cancelled() || superseding.load(std::sync::atomic::Ordering::Acquire) {
        inbound_supersession_drained.cancelled().await;
    }
    gateway.unregister_session(session_id);
    let _ = writer.close().await;
    metrics.connection_closed();
    gateway.connection_closed();
    tracing::debug!(conn = %id, %session_id, "WebSocket connection closed");
    result
}

/// Route one post-handshake envelope only while this exact transport generation
/// remains current. The supersession gate is the decode/metric/gateway
/// linearization point shared with replacement and durable revocation.
fn route_envelope(
    session_id: crate::realtime::ParticipantId,
    env: &Envelope,
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
    metrics.envelope_received();
    gateway.handle_inbound(session_id, env);
    true
}

/// Decode and route one WebSocket frame only while its transport remains
/// current. The gate covers decode, metrics, and gateway handoff, not merely a
/// speculative cancellation check before decoding client-controlled bytes.
fn route_framed_envelope(
    session_id: crate::realtime::ParticipantId,
    buf: &mut BytesMut,
    metrics: &TransportMetrics,
    gateway: &Gateway,
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
) -> AppResult<bool> {
    let Ok(_gate) = supersession_gate.lock() else {
        return Ok(false);
    };
    if superseded.is_cancelled() || !gateway.accepts_work(session_id) {
        return Ok(false);
    }
    let Some(env) = (match decode_framed(buf) {
        Ok(env) => env,
        Err(error) => {
            metrics.decode_error();
            return Err(error);
        }
    }) else {
        return Ok(false);
    };
    metrics.envelope_received();
    gateway.handle_inbound(session_id, &env);
    Ok(true)
}

/// Reply to a Ping only while the transport remains current. The shared async
/// write gate stays held through `poll_ready`, `start_send`, and `flush`; a
/// supersession or close cannot publish in the gap after queue admission.
async fn reply_to_ping<S>(
    writer: &mut S,
    payload: Vec<u8>,
    superseded: &CancellationToken,
    superseding: &std::sync::atomic::AtomicBool,
    transport_write_gate: &tokio::sync::Mutex<()>,
) -> AppResult<bool>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let Some(result) = crate::transport::write_if_current(
        superseded,
        superseding,
        transport_write_gate,
        || async {
            futures_util::future::poll_fn(|cx| writer.poll_ready_unpin(cx)).await?;
            writer.start_send_unpin(Message::Pong(payload))?;
            writer.flush().await
        },
    )
    .await
    else {
        return Ok(false);
    };
    result.map_err(send_err)?;
    Ok(true)
}

/// Send a server heartbeat probe only while the transport remains current. A
/// heartbeat Ping is a control write just like a Pong reply, so its queueing and
/// flush share the cancellation/write linearization gate with application frames.
async fn send_heartbeat_ping<S>(
    writer: &mut S,
    superseded: &CancellationToken,
    superseding: &std::sync::atomic::AtomicBool,
    transport_write_gate: &tokio::sync::Mutex<()>,
) -> AppResult<bool>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let Some(result) = crate::transport::write_if_current(
        superseded,
        superseding,
        transport_write_gate,
        || async { writer.send(Message::Ping(Vec::new())).await },
    )
    .await
    else {
        return Ok(false);
    };
    result.map_err(send_err)?;
    Ok(true)
}

/// Record a liveness timeout only while this exact transport generation remains
/// current. A `select!` can pick a ready deadline after supersession, so this
/// check is adjacent to the metrics rather than relying on branch order.
fn route_heartbeat_timeout(
    metrics: &TransportMetrics,
    gateway: &Gateway,
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
) -> bool {
    let Ok(_gate) = supersession_gate.lock() else {
        return false;
    };
    if superseded.is_cancelled() {
        return false;
    }
    metrics.liveness_timeout();
    gateway.node_metrics().record_websocket_liveness_timeout();
    true
}

/// Record a WebSocket Pong only while this exact transport generation remains
/// current. Keep both transport and node metrics beneath the cancellation gate
/// so a revocation cannot count a concurrently ready control frame.
fn route_pong_received(
    metrics: &TransportMetrics,
    gateway: &Gateway,
    superseded: &CancellationToken,
    supersession_gate: &Arc<Mutex<()>>,
) -> bool {
    let Ok(_gate) = supersession_gate.lock() else {
        return false;
    };
    if superseded.is_cancelled() {
        return false;
    }
    metrics.pong_received();
    gateway.node_metrics().record_websocket_pong_received();
    true
}

/// Read from the socket until a binary message yields at least one decoded
/// framed envelope, returning all frames in that message.
///
/// Returns `Ok(None)` on a clean close, cancellation, or exhausted stream before
/// any frame arrives. Non-binary frames (text/ping/pong) are ignored during the
/// handshake phase. A decode error is surfaced so the caller can tear down.
async fn read_first_frames(
    reader: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
    cancel: &CancellationToken,
    metrics: &TransportMetrics,
) -> AppResult<Option<Vec<Envelope>>> {
    loop {
        let msg = tokio::select! {
            () = cancel.cancelled() => return Ok(None),
            msg = reader.next() => msg,
        };
        let Some(msg) = msg else { return Ok(None) };
        let msg = msg.map_err(|e| {
            AppError::new(ErrorCategory::Transport, "WebSocket read error")
                .with_detail(e.to_string())
        })?;
        match msg {
            Message::Binary(data) => {
                let mut buf = BytesMut::from(&data[..]);
                let mut frames = Vec::new();
                loop {
                    match decode_framed(&mut buf) {
                        Ok(Some(env)) => {
                            metrics.envelope_received();
                            frames.push(env);
                        }
                        Ok(None) => break,
                        Err(e) => {
                            metrics.decode_error();
                            return Err(e);
                        }
                    }
                }
                if !frames.is_empty() {
                    return Ok(Some(frames));
                }
                // An empty binary message carries no handshake; keep waiting.
            }
            Message::Close(_) => return Ok(None),
            // Ignore text/ping/pong while waiting for the handshake frame.
            _ => {}
        }
    }
}

fn send_err(e: impl std::fmt::Display) -> AppError {
    AppError::new(ErrorCategory::Transport, "WebSocket write error").with_detail(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::net::{IpAddr, Ipv4Addr};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct FlushBlockingSink {
        flush_started: Option<tokio::sync::oneshot::Sender<()>>,
        flush_release: tokio::sync::oneshot::Receiver<()>,
        expect_ping: bool,
    }

    impl futures_util::Sink<Message> for FlushBlockingSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            assert_eq!(
                matches!(message, Message::Ping(_)),
                self.expect_ping,
                "control write has the expected frame kind"
            );
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if let Some(started) = self.flush_started.take() {
                started.send(()).expect("flush observer alive");
            }
            match Pin::new(&mut self.flush_release).poll(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(_)) => Poll::Ready(Err(std::io::Error::other("flush cancelled"))),
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.poll_flush(cx)
        }
    }

    #[tokio::test]
    async fn server_binds_and_reports_local_addr() {
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let server = WebSocketServer::bind(bind).await.expect("bind");
        assert_eq!(server.transport_kind(), TransportKind::WebSocket);
        assert_ne!(server.local_addr().port(), 0);
    }

    #[tokio::test]
    async fn websocket_outbound_write_waits_before_supersession_is_published() {
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
            "WebSocket supersession cannot publish while an admitted application frame writes"
        );
        release_tx.send(()).expect("release application write");
        assert!(writer.await.expect("writer task").is_some());
        cancellation.await.expect("cancellation task");
        assert!(superseded.is_cancelled());
    }

    #[tokio::test]
    async fn ping_reply_flush_is_linearized_with_same_session_replacement() {
        let superseded = CancellationToken::new();
        let superseding = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let transport_write_gate = Arc::new(tokio::sync::Mutex::new(()));
        let (flush_started_tx, flush_started_rx) = tokio::sync::oneshot::channel();
        let (flush_release_tx, flush_release_rx) = tokio::sync::oneshot::channel();
        let mut sink = FlushBlockingSink {
            flush_started: Some(flush_started_tx),
            flush_release: flush_release_rx,
            expect_ping: false,
        };
        let pong_token = superseded.clone();
        let pong_superseding = Arc::clone(&superseding);
        let pong_gate = Arc::clone(&transport_write_gate);
        let pong = tokio::spawn(async move {
            reply_to_ping(
                &mut sink,
                b"ping".to_vec(),
                &pong_token,
                &pong_superseding,
                &pong_gate,
            )
            .await
        });
        flush_started_rx.await.expect("Pong reached async flush");
        superseding.store(true, std::sync::atomic::Ordering::Release);
        let cancellation_gate = Arc::clone(&transport_write_gate);
        let cancellation_token = superseded.clone();
        let cancellation = tokio::spawn(async move {
            let _write = cancellation_gate.lock().await;
            cancellation_token.cancel();
        });
        tokio::task::yield_now().await;
        assert!(
            !superseded.is_cancelled(),
            "replacement must wait for the admitted Pong's asynchronous flush"
        );
        flush_release_tx.send(()).expect("release flush");
        assert!(pong.await.expect("Pong task").expect("Pong reply"));
        cancellation.await.expect("cancellation task");
        assert!(superseded.is_cancelled());
    }

    #[tokio::test]
    async fn heartbeat_ping_flush_is_linearized_with_same_session_replacement() {
        let superseded = CancellationToken::new();
        let superseding = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let transport_write_gate = Arc::new(tokio::sync::Mutex::new(()));
        let (flush_started_tx, flush_started_rx) = tokio::sync::oneshot::channel();
        let (flush_release_tx, flush_release_rx) = tokio::sync::oneshot::channel();
        let mut sink = FlushBlockingSink {
            flush_started: Some(flush_started_tx),
            flush_release: flush_release_rx,
            expect_ping: true,
        };
        let ping_token = superseded.clone();
        let ping_superseding = Arc::clone(&superseding);
        let ping_gate = Arc::clone(&transport_write_gate);
        let ping = tokio::spawn(async move {
            send_heartbeat_ping(&mut sink, &ping_token, &ping_superseding, &ping_gate).await
        });
        flush_started_rx
            .await
            .expect("Ping reached asynchronous flush");
        superseding.store(true, std::sync::atomic::Ordering::Release);
        let cancellation_gate = Arc::clone(&transport_write_gate);
        let cancellation_token = superseded.clone();
        let cancellation = tokio::spawn(async move {
            let _write = cancellation_gate.lock().await;
            cancellation_token.cancel();
        });
        tokio::task::yield_now().await;
        assert!(
            !superseded.is_cancelled(),
            "replacement must wait for the admitted heartbeat Ping's flush"
        );
        flush_release_tx.send(()).expect("release flush");
        assert!(ping.await.expect("Ping task").expect("heartbeat Ping"));
        cancellation.await.expect("cancellation task");
        assert!(superseded.is_cancelled());
    }

    #[test]
    fn superseded_heartbeat_does_not_update_liveness_metrics() {
        let superseded = CancellationToken::new();
        superseded.cancel();
        let metrics = TransportMetrics::new();
        let gateway = Gateway::new();
        let gate = Arc::new(Mutex::new(()));

        assert!(
            !route_heartbeat_timeout(&metrics, &gateway, &superseded, &gate),
            "a timeout branch selected after replacement must not record liveness"
        );
        assert!(
            !route_pong_received(&metrics, &gateway, &superseded, &gate),
            "a Pong branch selected after replacement must not record liveness"
        );
        assert_eq!(metrics.snapshot(), Default::default());
    }

    #[test]
    fn superseded_websocket_frame_and_queued_replay_are_not_counted() {
        let superseded = CancellationToken::new();
        superseded.cancel();
        let metrics = TransportMetrics::new();
        let gateway = Gateway::new();
        let gate = Arc::new(Mutex::new(()));
        let queued = Envelope::new(citadel_wire::protocol::KIND_POSITION, b"queued".to_vec());
        let framed = queued.encode_framed();
        let mut buf = BytesMut::from(&framed[..]);

        assert!(!route_envelope(
            crate::realtime::ParticipantId::from_raw(1),
            &queued,
            &metrics,
            &gateway,
            &superseded,
            &gate,
        ));
        assert!(
            !route_framed_envelope(
                crate::realtime::ParticipantId::from_raw(1),
                &mut buf,
                &metrics,
                &gateway,
                &superseded,
                &gate,
            )
            .expect("cancellation is an orderly stop")
        );
        assert_eq!(
            metrics.snapshot(),
            Default::default(),
            "neither queued nor framed WebSocket work may decode or count after replacement"
        );
    }

    #[tokio::test]
    async fn superseded_websocket_control_frames_are_neither_replied_to_nor_counted() {
        let superseded = CancellationToken::new();
        superseded.cancel();
        let metrics = TransportMetrics::new();
        let gateway = Gateway::new();
        let gate = Arc::new(Mutex::new(()));
        let transport_write_gate = tokio::sync::Mutex::new(());
        let superseding = std::sync::atomic::AtomicBool::new(false);

        let mut wrote = false;
        assert!(
            crate::transport::write_if_current(
                &superseded,
                &superseding,
                &transport_write_gate,
                || async { wrote = true },
            )
            .await
            .is_none(),
            "a superseded WebSocket Ping must not produce a Pong write"
        );
        assert!(!wrote, "the superseded Pong write callback must not run");
        assert!(
            !route_pong_received(&metrics, &gateway, &superseded, &gate),
            "a superseded WebSocket Pong must not mutate liveness metrics"
        );
        assert_eq!(
            metrics.snapshot(),
            Default::default(),
            "a superseded control frame must not be replied to or counted"
        );
    }
}
