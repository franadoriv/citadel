//! Narrow mutually-authenticated control transport for matchmaker commands.
//!
//! This is intentionally not a generic realtime proxy. A caller can only send
//! a formed handoff to a session node or ask a match owner to redeem one. Every
//! request carries its source/destination node id and a short deadline; mutual
//! TLS authenticates the peer certificate and a configured certificate pin
//! binds that peer to the claimed [`NodeId`].

use std::collections::{BTreeMap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::chat_cluster::{
    ChatCommandDedupe, ChatDeliveryDisposition, ChatLeaseUpdate, ChatPresenceLease,
    ChatPresenceWithdrawal, RemoteChatDelivery,
};
use crate::error::{AppError, AppResult, ErrorCategory};
use crate::matchmaker::{TicketId, TicketState};
use crate::matchmaker_cluster::{
    AdmissionHandler, MatchmakerHandoffRouter, MatchmakerRouterError, PartyAdmissionFence,
    RemoteMatchmakerAdmission, RemoteMatchmakerHandoff, RemoteMatchmakerTicketCancellation,
    RemoteMatchmakerTicketStatus, RemoteMatchmakerTicketSubmission, TicketCancellationHandler,
    TicketStatusHandler, TicketSubmissionHandler,
};
use crate::party::{PartyId, PartySnapshot};
use crate::party_presence::{
    PartyPresenceCommand, PartyPresenceDeliveryDisposition, PartyPresenceLease,
    PartyPresenceUpdate, PartyPresenceWithdrawal, RemotePartyPresenceDelivery,
};
use crate::runtime::cluster::{RuntimeCacheMutation, RuntimeCacheWrite, RuntimeClusterEvent};
use crate::services::party_directory::PartyOwnerLease;
use crate::session::NodeId;
use crate::time::{Clock, SystemClock, TimestampMillis};

const CONTROL_PROTOCOL_VERSION: u16 = 1;
const MAX_FRAME_BYTES: usize = 64 * 1024;
const MAX_DEDUPED_COMMANDS: usize = 4_096;
const MAX_PENDING_CONNECTIONS: usize = 128;
const CONTROL_WORKERS: usize = 2;

/// An operator-registered TLS endpoint for one authenticated node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchmakerControlEndpoint {
    /// TCP address of the node's matchmaker-only control listener.
    pub address: SocketAddr,
    /// DNS name verified by TLS before the certificate fingerprint is pinned to
    /// the configured node id.
    pub server_name: String,
}

/// In-memory certificate/key material for a node-control listener.
///
/// Bootstrap/config code owns loading PEM files; the transport holds typed DER
/// material so secrets never need to be logged or serialized with commands.
#[derive(Clone)]
pub struct MatchmakerControlIdentity {
    cert_chain: Vec<CertificateDer<'static>>,
    key_der: Vec<u8>,
}

impl std::fmt::Debug for MatchmakerControlIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatchmakerControlIdentity")
            .field("cert_chain_len", &self.cert_chain.len())
            .field("private_key", &"[redacted]")
            .finish()
    }
}

impl MatchmakerControlIdentity {
    /// Build a control identity from a certificate chain and PKCS#8 key bytes.
    pub fn from_der(cert_chain: Vec<CertificateDer<'static>>, key_der: Vec<u8>) -> AppResult<Self> {
        if cert_chain.is_empty() || key_der.is_empty() {
            return Err(AppError::config(
                "matchmaker control TLS certificate and private key are required",
            ));
        }
        Ok(Self {
            cert_chain,
            key_der,
        })
    }

    /// Load the local certificate chain and PKCS#8 private key from PEM files.
    pub fn from_pem_files(
        certificate_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> AppResult<Self> {
        let cert_chain = read_certificate_chain(certificate_path.as_ref())?;
        let key_bytes = std::fs::read(key_path.as_ref()).map_err(|error| {
            AppError::config(format!(
                "cannot read matchmaker control private key: {}",
                key_path.as_ref().display()
            ))
            .with_detail(error.to_string())
        })?;
        let mut key_reader = std::io::Cursor::new(key_bytes);
        let key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|error| tls_error("invalid matchmaker control private key PEM", error))?
            .ok_or_else(|| AppError::config("matchmaker control private key PEM is empty"))?;
        Self::from_der(cert_chain, key.secret_der().to_vec())
    }

    /// SHA-256 fingerprint of the leaf certificate used for node-id pinning.
    #[must_use]
    pub fn fingerprint(&self) -> [u8; 32] {
        fingerprint(&self.cert_chain[0])
    }

    fn private_key(&self) -> PrivateKeyDer<'static> {
        PrivatePkcs8KeyDer::from(self.key_der.clone()).into()
    }
}

/// Load a peer's leaf certificate from PEM for endpoint pinning.
pub fn read_matchmaker_control_certificate(
    certificate_path: impl AsRef<Path>,
) -> AppResult<CertificateDer<'static>> {
    read_certificate_chain(certificate_path.as_ref()).and_then(|mut certificates| {
        certificates
            .drain(..1)
            .next()
            .ok_or_else(|| AppError::config("matchmaker control certificate PEM is empty"))
    })
}

fn read_certificate_chain(path: &Path) -> AppResult<Vec<CertificateDer<'static>>> {
    let bytes = std::fs::read(path).map_err(|error| {
        AppError::config(format!(
            "cannot read matchmaker control certificate: {}",
            path.display()
        ))
        .with_detail(error.to_string())
    })?;
    let mut reader = std::io::Cursor::new(bytes);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| tls_error("invalid matchmaker control certificate PEM", error))?;
    if certificates.is_empty() {
        return Err(AppError::config(
            "matchmaker control certificate PEM is empty",
        ));
    }
    Ok(certificates)
}

/// A bounded TCP listener owned by [`TlsMatchmakerHandoffRouter`]. Dropping it
/// stops accepting commands and joins the small fixed worker pool.
pub struct RunningMatchmakerControlListener {
    address: SocketAddr,
    shutdown: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
    workers: Vec<JoinHandle<()>>,
}

impl RunningMatchmakerControlListener {
    /// Actual address after binding (useful when tests bind port `0`).
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for RunningMatchmakerControlListener {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum NodeCommand {
    SubmitTicket(RemoteMatchmakerTicketSubmission),
    CancelTicket(RemoteMatchmakerTicketCancellation),
    TicketStatus(RemoteMatchmakerTicketStatus),
    DeliverHandoff(RemoteMatchmakerHandoff),
    AdmitRemote(RemoteMatchmakerAdmission),
    DeliverChat(RemoteChatDelivery),
    AdvertiseChatPresence(ChatPresenceLease),
    WithdrawChatPresence(ChatPresenceWithdrawal),
    AdvertisePartyPresence(PartyPresenceLease),
    WithdrawPartyPresence(PartyPresenceWithdrawal),
    DeliverPartyPresence(RemotePartyPresenceDelivery),
    DeliverRuntimeEvent(RuntimeClusterEvent),
    ApplyRuntimeCacheMutation(RuntimeCacheMutation),
    SubmitRuntimeCacheWrite(RuntimeCacheWrite),
    Party(PartyControlCommand),
}

/// One fenced party mutation forwarded only to the durable party owner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartyControlCommand {
    pub party_id: PartyId,
    pub lease: PartyOwnerLease,
    pub actor: String,
    pub request_id: String,
    pub expected_revision: u64,
    pub operation: PartyControlOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartyControlOperation {
    Invite {
        target: String,
    },
    Accept,
    Leave,
    Promote {
        target: String,
    },
    Remove {
        target: String,
    },
    Close,
    /// Freeze one committed membership revision before ticket admission. Like
    /// every other party mutation, this must execute on the fenced owner.
    QueueAdmission {
        ticket_expires_at: TimestampMillis,
    },
    /// Replace a pre-ticket reservation expiry with the exact expiry of the
    /// authoritative ticket. The original admission remains the complete
    /// token/generation fence, so delayed shard work cannot renew a newer one.
    RenewQueueAdmission {
        admission: PartyAdmissionFence,
        ticket_expires_at: TimestampMillis,
    },
    /// Cleanup is also a party mutation: it is always routed to the current
    /// owner and succeeds only for the exact admission token/fence.
    ReleaseQueueAdmission {
        admission: PartyAdmissionFence,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartyQueueAdmission {
    pub members: Vec<String>,
    pub revision: u64,
    /// Echoed by the owner so the caller can reject a reply from a stale or
    /// misrouted lease rather than admitting a ticket from an unfenced view.
    pub lease: PartyOwnerLease,
    pub admission_generation: u64,
    pub admission_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartyControlReply {
    /// Every successful mutation reply is bound to the command's owner fence.
    /// Callers must reject a delayed or misrouted reply whose lease differs.
    Snapshot(PartySnapshot, PartyOwnerLease),
    QueueAdmission(PartyQueueAdmission),
    /// The caller must re-resolve the owner before retrying this command.
    StaleOwnerFence,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlEnvelope {
    protocol_version: u16,
    command_id: String,
    source_node: NodeId,
    destination_node: NodeId,
    issued_at_ms: u64,
    deadline_ms: u64,
    trace_context: Option<String>,
    command: NodeCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ControlResponse {
    TicketSubmitted {
        ticket_id: TicketId,
    },
    TicketCancelled {
        cancelled: bool,
    },
    TicketStatus {
        state: Option<TicketState>,
    },
    Delivered,
    Admitted {
        match_id: u64,
    },
    AuthoritativeAdmissionUnavailable,
    Rejected,
    ChatDelivery {
        disposition: ChatDeliveryDisposition,
    },
    ChatPresence {
        update: ChatLeaseUpdate,
    },
    PartyPresence {
        update: PartyPresenceUpdate,
    },
    PartyPresenceDelivery {
        disposition: PartyPresenceDeliveryDisposition,
    },
    RuntimePropagation {
        accepted: bool,
    },
    Party {
        reply: PartyControlReply,
    },
}

#[derive(Debug, Default)]
struct CommandDedupe {
    responses: BTreeMap<(NodeId, String), ControlResponse>,
    order: VecDeque<(NodeId, String)>,
}

impl CommandDedupe {
    fn get(&self, source: &NodeId, command_id: &str) -> Option<ControlResponse> {
        self.responses
            .get(&(source.clone(), command_id.to_owned()))
            .cloned()
    }

    fn insert(&mut self, source: NodeId, command_id: String, response: ControlResponse) {
        let key = (source, command_id);
        if self.responses.insert(key.clone(), response).is_none() {
            self.order.push_back(key);
        }
        while self.order.len() > MAX_DEDUPED_COMMANDS {
            if let Some(evicted) = self.order.pop_front() {
                self.responses.remove(&evicted);
            }
        }
    }
}

struct RouterState {
    local_node: NodeId,
    endpoints: Mutex<BTreeMap<NodeId, MatchmakerControlEndpoint>>,
    peer_fingerprints: BTreeMap<NodeId, [u8; 32]>,
    client_config: Arc<ClientConfig>,
    server_config: Arc<ServerConfig>,
    timeout: Duration,
    next_command: AtomicU64,
    inbox: Mutex<Vec<RemoteMatchmakerHandoff>>,
    handoff_handler: Mutex<Option<HandoffHandler>>,
    admission_handler: Mutex<Option<AdmissionHandler>>,
    submission_handler: Mutex<Option<TicketSubmissionHandler>>,
    cancellation_handler: Mutex<Option<TicketCancellationHandler>>,
    status_handler: Mutex<Option<TicketStatusHandler>>,
    chat_delivery_handler: Mutex<Option<ChatDeliveryHandler>>,
    chat_presence_handler: Mutex<Option<ChatPresenceHandler>>,
    party_presence_handler: Mutex<Option<PartyPresenceHandler>>,
    party_presence_delivery_handler: Mutex<Option<PartyPresenceDeliveryHandler>>,
    party_handler: Mutex<Option<PartyControlHandler>>,
    chat_dedupe: ChatCommandDedupe,
    runtime_event_handler: Mutex<Option<RuntimeEventHandler>>,
    runtime_cache_handler: Mutex<Option<RuntimeCacheMutationHandler>>,
    runtime_cache_write_handler: Mutex<Option<RuntimeCacheWriteHandler>>,
    dedupe: Mutex<CommandDedupe>,
}

/// Callback invoked on the receiving session node before a handoff command is
/// acknowledged. The callback must enqueue/persist the handoff; it must not
/// block on a client socket.
pub type HandoffHandler =
    Arc<dyn Fn(RemoteMatchmakerHandoff) -> Result<(), MatchmakerRouterError> + Send + Sync>;
/// Callback for a fenced party command after mTLS source authentication.
pub type PartyControlHandler =
    Arc<dyn Fn(NodeId, PartyControlCommand) -> PartyControlReply + Send + Sync>;

/// Callback invoked for one typed, durable chat event. It receives no remote
/// socket or participant capability and must validate its local lease fence
/// before scheduling local delivery.
pub type ChatDeliveryHandler =
    Arc<dyn Fn(NodeId, RemoteChatDelivery) -> ChatDeliveryDisposition + Send + Sync>;

/// Callback for one authenticated channel-level presence advertisement or
/// withdrawal. It accepts only the fenced lease metadata, never a remote
/// session or participant handle.
pub type ChatPresenceHandler =
    Arc<dyn Fn(NodeId, ChatPresenceCommand) -> ChatLeaseUpdate + Send + Sync>;
/// Callback for one authenticated privacy-safe party/node lease transition.
/// It receives no member, participant, socket, or invitation identity.
pub type PartyPresenceHandler =
    Arc<dyn Fn(NodeId, PartyPresenceCommand) -> PartyPresenceUpdate + Send + Sync>;
/// Receiver for one member-only party presence snapshot. mTLS authenticates
/// the source; the callback must still reauthorize local durable membership.
pub type PartyPresenceDeliveryHandler = Arc<
    dyn Fn(NodeId, RemotePartyPresenceDelivery) -> PartyPresenceDeliveryDisposition + Send + Sync,
>;

/// Receiver for one authenticated, at-least-once runtime event.
pub type RuntimeEventHandler = Arc<dyn Fn(NodeId, RuntimeClusterEvent) -> bool + Send + Sync>;
/// Receiver for one fenced runtime cache mutation or invalidation.
pub type RuntimeCacheMutationHandler =
    Arc<dyn Fn(NodeId, RuntimeCacheMutation) -> bool + Send + Sync>;
/// Receiver for a cache write that the current global writer must fence.
pub type RuntimeCacheWriteHandler = Arc<dyn Fn(NodeId, RuntimeCacheWrite) -> bool + Send + Sync>;

/// Typed update transported to peers so every node can resolve channel
/// destinations from the same fenced lease view.
#[derive(Debug, Clone)]
pub enum ChatPresenceCommand {
    /// Publish or renew one node's lease.
    Advertise(ChatPresenceLease),
    /// Withdraw a node's final local subscription.
    Withdraw(ChatPresenceWithdrawal),
}

/// An authenticated, real TCP/TLS implementation of
/// [`MatchmakerHandoffRouter`]. Endpoint maps are explicit registration, not
/// discovery trust: a node must still present the pinned mutual-TLS certificate
/// before its command is accepted.
pub struct TlsMatchmakerHandoffRouter {
    state: Arc<RouterState>,
}

impl std::fmt::Debug for TlsMatchmakerHandoffRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsMatchmakerHandoffRouter")
            .field("local_node", &self.state.local_node)
            .field("endpoints", &"[registered]")
            .field("timeout", &self.state.timeout)
            .finish()
    }
}

impl TlsMatchmakerHandoffRouter {
    /// Build a router for mutually pinned self-signed development certificates.
    /// Production callers with a private CA use [`Self::new_with_trust_roots`]
    /// so TLS trust anchors and node-id leaf pins stay distinct.
    pub fn new(
        local_node: NodeId,
        identity: MatchmakerControlIdentity,
        trusted_peer_certificates: BTreeMap<NodeId, CertificateDer<'static>>,
        endpoints: BTreeMap<NodeId, MatchmakerControlEndpoint>,
        timeout: Duration,
    ) -> AppResult<Self> {
        let trust_roots = trusted_peer_certificates.values().cloned().collect();
        Self::new_with_trust_roots(
            local_node,
            identity,
            trusted_peer_certificates,
            trust_roots,
            endpoints,
            timeout,
        )
    }

    /// Build a router from explicit peer leaf pins and private-CA trust roots.
    /// The TLS verifier trusts only `trust_roots`; once TLS succeeds, the peer's
    /// presented leaf fingerprint must also match its configured [`NodeId`].
    pub fn new_with_trust_roots(
        local_node: NodeId,
        identity: MatchmakerControlIdentity,
        trusted_peer_certificates: BTreeMap<NodeId, CertificateDer<'static>>,
        trust_roots: Vec<CertificateDer<'static>>,
        endpoints: BTreeMap<NodeId, MatchmakerControlEndpoint>,
        timeout: Duration,
    ) -> AppResult<Self> {
        if timeout.is_zero() {
            return Err(AppError::config(
                "matchmaker control transport timeout must be greater than zero",
            ));
        }
        let mut roots = RootCertStore::empty();
        for certificate in trust_roots {
            roots.add(certificate.clone()).map_err(|error| {
                tls_error(
                    "failed to add trusted matchmaker control certificate",
                    error,
                )
            })?;
        }
        let peer_fingerprints = trusted_peer_certificates
            .iter()
            .map(|(node, certificate)| (node.clone(), fingerprint(certificate)))
            .collect();
        let client_config = ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(identity.cert_chain.clone(), identity.private_key())
            .map_err(|error| tls_error("invalid matchmaker control client identity", error))?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|error| tls_error("invalid matchmaker control client verifier", error))?;
        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(identity.cert_chain.clone(), identity.private_key())
            .map_err(|error| tls_error("invalid matchmaker control server identity", error))?;
        Ok(Self {
            state: Arc::new(RouterState {
                local_node,
                endpoints: Mutex::new(endpoints),
                peer_fingerprints,
                client_config: Arc::new(client_config),
                server_config: Arc::new(server_config),
                timeout,
                next_command: AtomicU64::new(1),
                inbox: Mutex::new(Vec::new()),
                handoff_handler: Mutex::new(None),
                admission_handler: Mutex::new(None),
                submission_handler: Mutex::new(None),
                cancellation_handler: Mutex::new(None),
                status_handler: Mutex::new(None),
                chat_delivery_handler: Mutex::new(None),
                chat_presence_handler: Mutex::new(None),
                party_presence_handler: Mutex::new(None),
                party_presence_delivery_handler: Mutex::new(None),
                party_handler: Mutex::new(None),
                chat_dedupe: ChatCommandDedupe::new(MAX_DEDUPED_COMMANDS),
                runtime_event_handler: Mutex::new(None),
                runtime_cache_handler: Mutex::new(None),
                runtime_cache_write_handler: Mutex::new(None),
                dedupe: Mutex::new(CommandDedupe::default()),
            }),
        })
    }

    /// Install the owning node's trusted admission handler before serving
    /// control traffic. Replacing it intentionally supports a local gateway
    /// restart while the listener stays bound.
    pub fn register_admission_handler(&self, handler: AdmissionHandler) {
        if let Ok(mut slot) = self.state.admission_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Register or replace one authenticated control endpoint. Discovery may
    /// supply candidates, but callers must still supply this explicit node-id
    /// binding and the peer certificate pin configured at construction.
    pub fn register_endpoint(&self, node: NodeId, endpoint: MatchmakerControlEndpoint) {
        if let Ok(mut endpoints) = self.state.endpoints.lock() {
            endpoints.insert(node, endpoint);
        }
    }

    /// Install the session-node delivery callback. Without a callback, received
    /// handoffs stay available through [`MatchmakerHandoffRouter::drain_handoffs`]
    /// for tests and explicitly polled local tooling.
    pub fn register_handoff_handler(&self, handler: HandoffHandler) {
        if let Ok(mut slot) = self.state.handoff_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the current shard owner's ticket-submission handler.
    pub fn register_submission_handler(&self, handler: TicketSubmissionHandler) {
        if let Ok(mut slot) = self.state.submission_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the current shard owner's cancellation handler.
    pub fn register_cancellation_handler(&self, handler: TicketCancellationHandler) {
        if let Ok(mut slot) = self.state.cancellation_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the current shard owner's status handler.
    pub fn register_status_handler(&self, handler: TicketStatusHandler) {
        if let Ok(mut slot) = self.state.status_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the local typed chat delivery boundary. This command shares the
    /// existing mTLS identity, deadline, payload framing, and dedupe envelope,
    /// but does not widen it into arbitrary realtime forwarding.
    pub fn register_chat_delivery_handler(&self, handler: ChatDeliveryHandler) {
        if let Ok(mut slot) = self.state.chat_delivery_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the local fenced channel-presence directory update boundary.
    /// The mTLS source identity is rechecked before the callback is invoked.
    pub fn register_chat_presence_handler(&self, handler: ChatPresenceHandler) {
        if let Ok(mut slot) = self.state.chat_presence_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the local party presence directory boundary. The authenticated
    /// source must match the node named in the lease before this callback runs.
    pub fn register_party_presence_handler(&self, handler: PartyPresenceHandler) {
        if let Ok(mut slot) = self.state.party_presence_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the authenticated cross-node member-presence receiver. It is a
    /// typed snapshot command, never a generic realtime forwarding surface.
    pub fn register_party_presence_delivery_handler(&self, handler: PartyPresenceDeliveryHandler) {
        if let Ok(mut slot) = self.state.party_presence_delivery_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the local runtime-event receiver. It must tolerate a duplicate
    /// event ID because the bounded control-plane dedupe is not durable.
    pub fn register_runtime_event_handler(&self, handler: RuntimeEventHandler) {
        if let Ok(mut slot) = self.state.runtime_event_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the local fenced cache mutation receiver.
    pub fn register_runtime_cache_handler(&self, handler: RuntimeCacheMutationHandler) {
        if let Ok(mut slot) = self.state.runtime_cache_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the current global cache writer's submission boundary.
    pub fn register_runtime_cache_write_handler(&self, handler: RuntimeCacheWriteHandler) {
        if let Ok(mut slot) = self.state.runtime_cache_write_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Install the typed fenced party owner handler.
    pub fn register_party_handler(&self, handler: PartyControlHandler) {
        if let Ok(mut slot) = self.state.party_handler.lock() {
            *slot = Some(handler);
        }
    }

    /// Bind the node-control listener with a fixed, bounded worker pool.
    pub fn serve(
        self: &Arc<Self>,
        bind: SocketAddr,
    ) -> AppResult<RunningMatchmakerControlListener> {
        let listener = TcpListener::bind(bind).map_err(|error| {
            AppError::new(
                ErrorCategory::Transport,
                format!("failed to bind matchmaker control listener on {bind}"),
            )
            .with_detail(error.to_string())
        })?;
        let address = listener.local_addr().map_err(|error| {
            AppError::new(
                ErrorCategory::Transport,
                "failed to read matchmaker control listener address",
            )
            .with_detail(error.to_string())
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            AppError::new(
                ErrorCategory::Transport,
                "failed to configure matchmaker control listener",
            )
            .with_detail(error.to_string())
        })?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::sync_channel(MAX_PENDING_CONNECTIONS);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(CONTROL_WORKERS);
        for worker_index in 0..CONTROL_WORKERS {
            let state = Arc::clone(&self.state);
            let receiver = Arc::clone(&receiver);
            let stop = Arc::clone(&shutdown);
            let worker = thread::Builder::new()
                .name(format!("citadel-matchmaker-control-{worker_index}"))
                .spawn(move || {
                    while !stop.load(Ordering::Acquire) {
                        let connection = receiver.lock().ok().and_then(|receiver| {
                            receiver.recv_timeout(Duration::from_millis(25)).ok()
                        });
                        if let Some(connection) = connection {
                            handle_connection(&state, connection);
                        }
                    }
                })
                .map_err(|error| {
                    AppError::new(
                        ErrorCategory::Transport,
                        "failed to spawn matchmaker control worker",
                    )
                    .with_detail(error.to_string())
                })?;
            workers.push(worker);
        }
        let stop = Arc::clone(&shutdown);
        let accept = thread::Builder::new()
            .name("citadel-matchmaker-control-accept".to_owned())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((connection, _)) => {
                            if connection.set_nonblocking(false).is_err() {
                                continue;
                            }
                            if sender.try_send(connection).is_err() {
                                // Saturation is deliberately fail-closed: a
                                // client retries its idempotent command before
                                // deadline instead of this listener growing an
                                // unbounded work queue.
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => thread::sleep(Duration::from_millis(5)),
                    }
                }
            })
            .map_err(|error| {
                AppError::new(
                    ErrorCategory::Transport,
                    "failed to spawn matchmaker control accept loop",
                )
                .with_detail(error.to_string())
            })?;
        Ok(RunningMatchmakerControlListener {
            address,
            shutdown,
            accept: Some(accept),
            workers,
        })
    }

    fn send(
        &self,
        destination: &NodeId,
        command: NodeCommand,
    ) -> Result<ControlResponse, MatchmakerRouterError> {
        if destination == &self.state.local_node {
            return dispatch(&self.state, self.state.local_node.clone(), command);
        }
        let endpoint = self
            .state
            .endpoints
            .lock()
            .ok()
            .and_then(|endpoints| endpoints.get(destination).cloned())
            .ok_or_else(|| MatchmakerRouterError::UnknownDestination(destination.clone()))?;
        let now = SystemClock.now().unix_millis();
        let deadline = now.saturating_add(
            self.state
                .timeout
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        let command_id = self.state.next_command.fetch_add(1, Ordering::Relaxed);
        let envelope = ControlEnvelope {
            protocol_version: CONTROL_PROTOCOL_VERSION,
            command_id: format!("{}-{command_id}", self.state.local_node.as_str()),
            source_node: self.state.local_node.clone(),
            destination_node: destination.clone(),
            issued_at_ms: now,
            deadline_ms: deadline,
            trace_context: None,
            command,
        };
        let bytes = serde_json::to_vec(&envelope)
            .map_err(|_| MatchmakerRouterError::Unavailable(destination.clone()))?;
        let stream = TcpStream::connect_timeout(&endpoint.address, self.state.timeout)
            .map_err(|_| MatchmakerRouterError::Unavailable(destination.clone()))?;
        let _ = stream.set_read_timeout(Some(self.state.timeout));
        let _ = stream.set_write_timeout(Some(self.state.timeout));
        let server_name = ServerName::try_from(endpoint.server_name.clone())
            .map_err(|_| MatchmakerRouterError::Unavailable(destination.clone()))?;
        let connection = ClientConnection::new(Arc::clone(&self.state.client_config), server_name)
            .map_err(|_| MatchmakerRouterError::Unavailable(destination.clone()))?;
        let mut tls = StreamOwned::new(connection, stream);
        write_frame(&mut tls, &bytes)
            .map_err(|_| MatchmakerRouterError::Unavailable(destination.clone()))?;
        if authenticated_peer(&self.state, tls.conn.peer_certificates())
            != Some(destination.clone())
        {
            return Err(MatchmakerRouterError::Unavailable(destination.clone()));
        }
        let response = read_frame(&mut tls)
            .map_err(|_| MatchmakerRouterError::Unavailable(destination.clone()))?;
        serde_json::from_slice(&response)
            .map_err(|_| MatchmakerRouterError::Unavailable(destination.clone()))
    }

    /// Forward a validated local ticket to the node that currently owns its
    /// shard. This uses the same mTLS envelope, deadline, and dedupe cache as
    /// handoff/admission traffic.
    pub fn submit_ticket(
        &self,
        destination: &NodeId,
        submission: RemoteMatchmakerTicketSubmission,
    ) -> Result<TicketId, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::SubmitTicket(submission))? {
            ControlResponse::TicketSubmitted { ticket_id } => Ok(ticket_id),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Forward a ticket cancellation to its shard owner.
    pub fn cancel_ticket(
        &self,
        destination: &NodeId,
        cancellation: RemoteMatchmakerTicketCancellation,
    ) -> Result<bool, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::CancelTicket(cancellation))? {
            ControlResponse::TicketCancelled { cancelled } => Ok(cancelled),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Read ticket state from its current shard owner. The client-visible match
    /// handoff remains on the session node and is intentionally not returned on
    /// this node-control surface.
    pub fn ticket_status(
        &self,
        destination: &NodeId,
        status: RemoteMatchmakerTicketStatus,
    ) -> Result<Option<TicketState>, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::TicketStatus(status))? {
            ControlResponse::TicketStatus { state } => Ok(state),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Send a typed durable chat event to one destination node.
    pub fn deliver_chat(
        &self,
        destination: &NodeId,
        delivery: RemoteChatDelivery,
    ) -> Result<ChatDeliveryDisposition, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::DeliverChat(delivery))? {
            ControlResponse::ChatDelivery { disposition } => Ok(disposition),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Broadcast one local channel-presence lease to a configured peer.
    pub fn advertise_chat_presence(
        &self,
        destination: &NodeId,
        lease: ChatPresenceLease,
    ) -> Result<ChatLeaseUpdate, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::AdvertiseChatPresence(lease))? {
            ControlResponse::ChatPresence { update } => Ok(update),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Broadcast one local channel-presence withdrawal to a configured peer.
    pub fn withdraw_chat_presence(
        &self,
        destination: &NodeId,
        withdrawal: ChatPresenceWithdrawal,
    ) -> Result<ChatLeaseUpdate, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::WithdrawChatPresence(withdrawal))? {
            ControlResponse::ChatPresence { update } => Ok(update),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Broadcast one privacy-safe party/node lease to a configured peer.
    pub fn advertise_party_presence(
        &self,
        destination: &NodeId,
        lease: PartyPresenceLease,
    ) -> Result<PartyPresenceUpdate, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::AdvertisePartyPresence(lease))? {
            ControlResponse::PartyPresence { update } => Ok(update),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Broadcast one fenced final-local-member withdrawal to a configured peer.
    pub fn withdraw_party_presence(
        &self,
        destination: &NodeId,
        withdrawal: PartyPresenceWithdrawal,
    ) -> Result<PartyPresenceUpdate, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::WithdrawPartyPresence(withdrawal))? {
            ControlResponse::PartyPresence { update } => Ok(update),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Deliver one source-node party-presence snapshot to a destination lease.
    pub fn deliver_party_presence(
        &self,
        destination: &NodeId,
        delivery: RemotePartyPresenceDelivery,
    ) -> Result<PartyPresenceDeliveryDisposition, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::DeliverPartyPresence(delivery))? {
            ControlResponse::PartyPresenceDelivery { disposition } => Ok(disposition),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Deliver one typed runtime event over the authenticated control plane.
    pub fn deliver_runtime_event(
        &self,
        destination: &NodeId,
        event: RuntimeClusterEvent,
    ) -> Result<bool, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::DeliverRuntimeEvent(event))? {
            ControlResponse::RuntimePropagation { accepted } => Ok(accepted),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Apply one fenced cache value or invalidation on a peer.
    pub fn apply_runtime_cache_mutation(
        &self,
        destination: &NodeId,
        mutation: RuntimeCacheMutation,
    ) -> Result<bool, MatchmakerRouterError> {
        match self.send(
            destination,
            NodeCommand::ApplyRuntimeCacheMutation(mutation),
        )? {
            ControlResponse::RuntimePropagation { accepted } => Ok(accepted),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Forward one local cache mutation to the current global cache writer.
    pub fn submit_runtime_cache_write(
        &self,
        destination: &NodeId,
        write: RuntimeCacheWrite,
    ) -> Result<bool, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::SubmitRuntimeCacheWrite(write))? {
            ControlResponse::RuntimePropagation { accepted } => Ok(accepted),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Route one party command to its currently resolved owner over mTLS.
    pub fn party_command(
        &self,
        destination: &NodeId,
        command: PartyControlCommand,
    ) -> Result<PartyControlReply, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::Party(command))? {
            ControlResponse::Party { reply } => Ok(reply),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    /// Snapshot configured peer nodes for a narrow typed broadcaster.
    ///
    /// This does not expose endpoints or transport internals to the caller.
    #[must_use]
    pub fn peer_nodes(&self) -> Vec<NodeId> {
        self.state
            .endpoints
            .lock()
            .map(|endpoints| endpoints.keys().cloned().collect())
            .unwrap_or_default()
    }
}

impl MatchmakerHandoffRouter for TlsMatchmakerHandoffRouter {
    fn deliver_handoff(
        &self,
        destination: &NodeId,
        handoff: RemoteMatchmakerHandoff,
    ) -> Result<(), MatchmakerRouterError> {
        match self.send(destination, NodeCommand::DeliverHandoff(handoff))? {
            ControlResponse::Delivered => Ok(()),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }

    fn drain_handoffs(&self, node: &NodeId) -> Vec<RemoteMatchmakerHandoff> {
        if node != &self.state.local_node {
            return Vec::new();
        }
        self.state
            .inbox
            .lock()
            .map(|mut inbox| std::mem::take(&mut *inbox))
            .unwrap_or_default()
    }

    fn admit_remote(
        &self,
        destination: &NodeId,
        request: RemoteMatchmakerAdmission,
    ) -> Result<u64, MatchmakerRouterError> {
        match self.send(destination, NodeCommand::AdmitRemote(request))? {
            ControlResponse::Admitted { match_id } => Ok(match_id),
            ControlResponse::AuthoritativeAdmissionUnavailable => Err(
                MatchmakerRouterError::AuthoritativeAdmissionUnavailable(destination.clone()),
            ),
            _ => Err(MatchmakerRouterError::Rejected(destination.clone())),
        }
    }
}

fn handle_connection(state: &Arc<RouterState>, stream: TcpStream) {
    let _ = stream.set_read_timeout(Some(state.timeout));
    let _ = stream.set_write_timeout(Some(state.timeout));
    let Ok(connection) = ServerConnection::new(Arc::clone(&state.server_config)) else {
        return;
    };
    let mut tls = StreamOwned::new(connection, stream);
    let Ok(frame) = read_frame(&mut tls) else {
        return;
    };
    let response = match serde_json::from_slice::<ControlEnvelope>(&frame) {
        Ok(envelope) => receive(
            state,
            authenticated_peer(state, tls.conn.peer_certificates()),
            envelope,
        ),
        Err(_) => ControlResponse::Rejected,
    };
    if let Ok(bytes) = serde_json::to_vec(&response) {
        let _ = write_frame(&mut tls, &bytes);
    }
}

fn receive(
    state: &RouterState,
    authenticated_source: Option<NodeId>,
    envelope: ControlEnvelope,
) -> ControlResponse {
    if envelope.protocol_version != CONTROL_PROTOCOL_VERSION
        || authenticated_source.as_ref() != Some(&envelope.source_node)
        || envelope.destination_node != state.local_node
        || envelope.deadline_ms <= SystemClock.now().unix_millis()
        || envelope.command_id.is_empty()
        || envelope.command_id.len() > 256
        || envelope
            .trace_context
            .as_ref()
            .is_some_and(|trace| trace.len() > 4_096)
    {
        return ControlResponse::Rejected;
    }
    if let Ok(dedupe) = state.dedupe.lock()
        && let Some(response) = dedupe.get(&envelope.source_node, &envelope.command_id)
    {
        return response;
    }
    let response = dispatch(state, envelope.source_node.clone(), envelope.command)
        .unwrap_or(ControlResponse::Rejected);
    if let Ok(mut dedupe) = state.dedupe.lock() {
        dedupe.insert(envelope.source_node, envelope.command_id, response.clone());
    }
    response
}

fn dispatch(
    state: &RouterState,
    source: NodeId,
    command: NodeCommand,
) -> Result<ControlResponse, MatchmakerRouterError> {
    match command {
        NodeCommand::SubmitTicket(submission) => {
            if submission
                .owners
                .iter()
                .any(|owner| owner.session_node != source)
            {
                return Ok(ControlResponse::Rejected);
            }
            let handler = state
                .submission_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            match handler.and_then(|handler| handler(submission).ok()) {
                Some(ticket_id) => Ok(ControlResponse::TicketSubmitted { ticket_id }),
                None => Ok(ControlResponse::Rejected),
            }
        }
        NodeCommand::CancelTicket(cancellation) => {
            let handler = state
                .cancellation_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            match handler.and_then(|handler| handler(cancellation).ok()) {
                Some(cancelled) => Ok(ControlResponse::TicketCancelled { cancelled }),
                None => Ok(ControlResponse::Rejected),
            }
        }
        NodeCommand::TicketStatus(status) => {
            let handler = state
                .status_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            match handler.and_then(|handler| handler(status).ok()) {
                Some(state) => Ok(ControlResponse::TicketStatus { state }),
                None => Ok(ControlResponse::Rejected),
            }
        }
        NodeCommand::DeliverHandoff(handoff) => {
            let handoff_handler = state
                .handoff_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            if let Some(handler) = handoff_handler {
                handler(handoff)?;
                return Ok(ControlResponse::Delivered);
            }
            let mut inbox = state
                .inbox
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?;
            if !inbox.contains(&handoff) {
                inbox.push(handoff);
            }
            Ok(ControlResponse::Delivered)
        }
        NodeCommand::AdmitRemote(request) => {
            if request.requester_node != source {
                return Ok(ControlResponse::Rejected);
            }
            let handler = state
                .admission_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            match handler.map(|handler| handler(request)) {
                Some(Ok(match_id)) => Ok(ControlResponse::Admitted { match_id }),
                Some(Err(MatchmakerRouterError::AuthoritativeAdmissionUnavailable(_))) => {
                    Ok(ControlResponse::AuthoritativeAdmissionUnavailable)
                }
                Some(Err(_)) | None => Ok(ControlResponse::Rejected),
            }
        }
        NodeCommand::DeliverRuntimeEvent(event) => {
            if event.id.source_node != source
                || event.event.payload.len() > MAX_FRAME_BYTES / 2
                || crate::runtime::RuntimeEvent::new(
                    event.event.namespace.clone(),
                    event.event.event_type.clone(),
                    event.event.payload.clone(),
                )
                .is_err()
            {
                return Ok(ControlResponse::RuntimePropagation { accepted: false });
            }
            let handler = state
                .runtime_event_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::RuntimePropagation {
                accepted: handler.is_some_and(|handler| handler(source, event)),
            })
        }
        NodeCommand::ApplyRuntimeCacheMutation(mutation) => {
            if mutation.fence.owner_node != source
                || mutation.namespace.len() > 80
                || mutation.key.len() > 80
                || mutation
                    .value
                    .as_ref()
                    .is_some_and(|value| value.len() > MAX_FRAME_BYTES / 2)
            {
                return Ok(ControlResponse::RuntimePropagation { accepted: false });
            }
            let handler = state
                .runtime_cache_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::RuntimePropagation {
                accepted: handler.is_some_and(|handler| handler(source, mutation)),
            })
        }
        NodeCommand::SubmitRuntimeCacheWrite(write) => {
            if write.namespace.len() > 80
                || write.key.len() > 80
                || write
                    .value
                    .as_ref()
                    .is_some_and(|value| value.len() > MAX_FRAME_BYTES / 2)
            {
                return Ok(ControlResponse::RuntimePropagation { accepted: false });
            }
            let handler = state
                .runtime_cache_write_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::RuntimePropagation {
                accepted: handler.is_some_and(|handler| handler(source, write)),
            })
        }
        NodeCommand::DeliverChat(delivery) => {
            if delivery.channel_id.is_empty()
                || delivery.channel_id.len() > 512
                || delivery.payload.len() > MAX_FRAME_BYTES / 2
                || delivery.deadline
                    <= TimestampMillis::from_unix_millis(SystemClock.now().unix_millis())
            {
                return Ok(ControlResponse::ChatDelivery {
                    disposition: ChatDeliveryDisposition::Rejected,
                });
            }
            let handler = state
                .chat_delivery_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::ChatDelivery {
                disposition: state.chat_dedupe.evaluate(source.clone(), &delivery, || {
                    handler.map_or(ChatDeliveryDisposition::Unknown, |handler| {
                        handler(source, delivery.clone())
                    })
                }),
            })
        }
        NodeCommand::AdvertiseChatPresence(lease) => {
            if lease.node_id != source
                || lease.channel_id.is_empty()
                || lease.channel_id.len() > 512
            {
                return Ok(ControlResponse::Rejected);
            }
            let handler = state
                .chat_presence_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::ChatPresence {
                update: handler.map_or(ChatLeaseUpdate::Stale, |handler| {
                    handler(source, ChatPresenceCommand::Advertise(lease))
                }),
            })
        }
        NodeCommand::WithdrawChatPresence(withdrawal) => {
            if withdrawal.node_id != source
                || withdrawal.channel_id.is_empty()
                || withdrawal.channel_id.len() > 512
            {
                return Ok(ControlResponse::Rejected);
            }
            let handler = state
                .chat_presence_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::ChatPresence {
                update: handler.map_or(ChatLeaseUpdate::Stale, |handler| {
                    handler(source, ChatPresenceCommand::Withdraw(withdrawal))
                }),
            })
        }
        NodeCommand::AdvertisePartyPresence(lease) => {
            if lease.node_id != source || lease.party_id.is_empty() || lease.party_id.len() > 128 {
                return Ok(ControlResponse::Rejected);
            }
            let handler = state
                .party_presence_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::PartyPresence {
                update: handler.map_or(PartyPresenceUpdate::Stale, |handler| {
                    handler(source, PartyPresenceCommand::Advertise(lease))
                }),
            })
        }
        NodeCommand::WithdrawPartyPresence(withdrawal) => {
            if withdrawal.node_id != source
                || withdrawal.party_id.is_empty()
                || withdrawal.party_id.len() > 128
            {
                return Ok(ControlResponse::Rejected);
            }
            let handler = state
                .party_presence_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::PartyPresence {
                update: handler.map_or(PartyPresenceUpdate::Stale, |handler| {
                    handler(source, PartyPresenceCommand::Withdraw(withdrawal))
                }),
            })
        }
        NodeCommand::DeliverPartyPresence(delivery) => {
            let now = SystemClock.now();
            if delivery.origin_node != source
                || delivery.party_id.is_empty()
                || delivery.party_id.len() > 128
                || delivery.snapshot.party_id != delivery.party_id
                || delivery.snapshot.online_members.len() > 8
                || delivery.deadline <= now
            {
                return Ok(ControlResponse::PartyPresenceDelivery {
                    disposition: PartyPresenceDeliveryDisposition::Rejected,
                });
            }
            let handler = state
                .party_presence_delivery_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::PartyPresenceDelivery {
                disposition: handler
                    .map_or(PartyPresenceDeliveryDisposition::Rejected, |handler| {
                        handler(source, delivery)
                    }),
            })
        }
        NodeCommand::Party(command) => {
            if command.lease.owner_node != state.local_node
                || command.party_id != command.lease.party_id
                || command.actor.is_empty()
                || command.request_id.is_empty()
            {
                return Ok(ControlResponse::Party {
                    reply: PartyControlReply::Rejected,
                });
            }
            let handler = state
                .party_handler
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(state.local_node.clone()))?
                .clone();
            Ok(ControlResponse::Party {
                reply: handler.map_or(PartyControlReply::Rejected, |handler| {
                    handler(source, command)
                }),
            })
        }
    }
}

fn authenticated_peer(
    state: &RouterState,
    certificates: Option<&[CertificateDer<'static>]>,
) -> Option<NodeId> {
    let certificate = certificates?.first()?;
    let certificate_fingerprint = fingerprint(certificate);
    state
        .peer_fingerprints
        .iter()
        .find_map(|(node, expected)| (*expected == certificate_fingerprint).then(|| node.clone()))
}

fn fingerprint(certificate: &CertificateDer<'_>) -> [u8; 32] {
    Sha256::digest(certificate.as_ref()).into()
}

fn read_frame(stream: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = usize::try_from(u32::from_be_bytes(length)).unwrap_or(usize::MAX);
    if length > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "matchmaker control frame exceeds limit",
        ));
    }
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body)?;
    Ok(body)
}

fn write_frame(stream: &mut impl Write, body: &[u8]) -> std::io::Result<()> {
    let length = u32::try_from(body.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "matchmaker control frame exceeds u32 length",
        )
    })?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "matchmaker control frame exceeds limit",
        ));
    }
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn tls_error(message: &str, source: impl std::fmt::Display) -> AppError {
    AppError::new(ErrorCategory::Transport, message).with_detail(source.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matchmaker::TicketId;
    use crate::matchmaker_cluster::{MatchmakerShardLease, QueueShardId};
    use crate::realtime::chat_presence::ChatPresenceRegistry;
    use crate::realtime::registry::ParticipantId;
    use crate::services::ChatTarget;
    use crate::services::party_directory::PartyOwnerLease;
    use crate::session::OwnershipGeneration;
    use crate::time::TimestampMillis;
    use std::sync::atomic::AtomicUsize;

    fn node(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node")
    }

    fn identity() -> (MatchmakerControlIdentity, CertificateDer<'static>) {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("test certificate");
        let key = certificate.key_pair.serialize_der();
        let leaf = CertificateDer::from(certificate.cert);
        (
            MatchmakerControlIdentity::from_der(vec![leaf.clone()], key).expect("identity"),
            leaf,
        )
    }

    fn lease() -> MatchmakerShardLease {
        MatchmakerShardLease {
            shard: QueueShardId::new(4),
            owner_node: node("node-b"),
            generation: OwnershipGeneration::new(2),
            expires_at: TimestampMillis::from_unix_millis(u64::MAX),
        }
    }

    fn chat_delivery() -> RemoteChatDelivery {
        RemoteChatDelivery {
            event_id: 44,
            channel_id: "channel-42".to_owned(),
            destination_generation: OwnershipGeneration::new(3),
            authority_epoch: 8,
            payload: r#"{\"type\":\"message.create\"}"#.to_owned(),
            deadline: TimestampMillis::from_unix_millis(u64::MAX),
        }
    }

    #[test]
    fn chat_presence_commands_reject_a_forged_node_claim_before_the_handler() {
        let (_, cert_a) = identity();
        let (identity_b, _) = identity();
        let node_a = node("node-a");
        let node_b = node("node-b");
        let router = TlsMatchmakerHandoffRouter::new(
            node_b.clone(),
            identity_b,
            BTreeMap::from([(node_a.clone(), cert_a)]),
            BTreeMap::new(),
            Duration::from_secs(2),
        )
        .expect("router");
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_handler = Arc::clone(&calls);
        router.register_chat_presence_handler(Arc::new(move |_, _| {
            calls_for_handler.fetch_add(1, Ordering::SeqCst);
            ChatLeaseUpdate::Applied
        }));

        let forged_lease = ChatPresenceLease {
            channel_id: "channel-forged".to_owned(),
            node_id: node_b.clone(),
            generation: OwnershipGeneration::new(1),
            expires_at: TimestampMillis::from_unix_millis(u64::MAX),
        };
        assert!(matches!(
            dispatch(
                &router.state,
                node_a.clone(),
                NodeCommand::AdvertiseChatPresence(forged_lease.clone()),
            ),
            Ok(ControlResponse::Rejected)
        ));
        assert!(matches!(
            dispatch(
                &router.state,
                node_a,
                NodeCommand::WithdrawChatPresence(ChatPresenceWithdrawal {
                    channel_id: forged_lease.channel_id,
                    node_id: node_b,
                    generation: OwnershipGeneration::new(1),
                }),
            ),
            Ok(ControlResponse::Rejected)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn runtime_propagation_rejects_forged_source_fences_before_handlers() {
        let (_, cert_a) = identity();
        let (identity_b, _) = identity();
        let node_a = node("node-a");
        let node_b = node("node-b");
        let router = TlsMatchmakerHandoffRouter::new(
            node_b.clone(),
            identity_b,
            BTreeMap::from([(node_a.clone(), cert_a)]),
            BTreeMap::new(),
            Duration::from_secs(2),
        )
        .expect("router");
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_event = Arc::clone(&calls);
        router.register_runtime_event_handler(Arc::new(move |_, _| {
            calls_for_event.fetch_add(1, Ordering::SeqCst);
            true
        }));
        let calls_for_cache = Arc::clone(&calls);
        router.register_runtime_cache_handler(Arc::new(move |_, _| {
            calls_for_cache.fetch_add(1, Ordering::SeqCst);
            true
        }));
        let event =
            crate::runtime::RuntimeEvent::new("match", "changed", Vec::new()).expect("event");
        assert!(matches!(
            dispatch(
                &router.state,
                node_a.clone(),
                NodeCommand::DeliverRuntimeEvent(RuntimeClusterEvent {
                    id: crate::runtime::cluster::RuntimeClusterEventId {
                        source_node: node_b.clone(),
                        sequence: 1,
                    },
                    event,
                }),
            ),
            Ok(ControlResponse::RuntimePropagation { accepted: false })
        ));
        assert!(matches!(
            dispatch(
                &router.state,
                node_a,
                NodeCommand::ApplyRuntimeCacheMutation(RuntimeCacheMutation {
                    namespace: "match".to_owned(),
                    key: "score".to_owned(),
                    value: Some(Vec::new()),
                    expires_at: TimestampMillis::from_unix_millis(u64::MAX),
                    fence: crate::runtime::cluster::RuntimeCacheFence {
                        owner_node: node_b,
                        generation: OwnershipGeneration::new(1),
                        sequence: 1,
                    },
                }),
            ),
            Ok(ControlResponse::RuntimePropagation { accepted: false })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn mutual_tls_transport_delivers_and_admits_over_a_real_listener() {
        let (identity_a, cert_a) = identity();
        let (identity_b, cert_b) = identity();
        let node_a = node("node-a");
        let node_b = node("node-b");
        let chat_deliveries = Arc::new(AtomicUsize::new(0));
        let party_presence_deliveries = Arc::new(AtomicUsize::new(0));
        let runtime_events = Arc::new(AtomicUsize::new(0));
        let runtime_mutations = Arc::new(AtomicUsize::new(0));
        let runtime_writes = Arc::new(AtomicUsize::new(0));
        let chat_directory = Arc::new(crate::chat_cluster::ChatPresenceDirectory::default());
        let chat_presence = Arc::new(ChatPresenceRegistry::new());
        chat_presence.join(
            "channel-42",
            ParticipantId::from_raw(1),
            "bob",
            ChatTarget::CurrentRoom { room_id: 1 },
            8,
        );
        assert_eq!(
            chat_directory.advertise(
                crate::chat_cluster::ChatPresenceLease {
                    channel_id: "channel-42".to_owned(),
                    node_id: node_b.clone(),
                    generation: OwnershipGeneration::new(3),
                    expires_at: TimestampMillis::from_unix_millis(u64::MAX),
                },
                TimestampMillis::from_unix_millis(0),
            ),
            crate::chat_cluster::ChatLeaseUpdate::Applied
        );
        let router_b = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_b.clone(),
                identity_b,
                BTreeMap::from([(node_a.clone(), cert_a.clone())]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router b"),
        );
        let runtime_events_for_handler = Arc::clone(&runtime_events);
        router_b.register_runtime_event_handler(Arc::new(move |source, event| {
            source.as_str() == "node-a" && event.id.source_node == source && {
                runtime_events_for_handler.fetch_add(1, Ordering::SeqCst);
                true
            }
        }));
        let runtime_mutations_for_handler = Arc::clone(&runtime_mutations);
        router_b.register_runtime_cache_handler(Arc::new(move |source, mutation| {
            source.as_str() == "node-a" && mutation.fence.owner_node == source && {
                runtime_mutations_for_handler.fetch_add(1, Ordering::SeqCst);
                true
            }
        }));
        let runtime_writes_for_handler = Arc::clone(&runtime_writes);
        router_b.register_runtime_cache_write_handler(Arc::new(move |source, write| {
            source.as_str() == "node-a" && write.namespace == "match" && {
                runtime_writes_for_handler.fetch_add(1, Ordering::SeqCst);
                true
            }
        }));
        router_b.register_admission_handler(Arc::new(|request| {
            (request.ticket_id.as_str() == "remote-ticket")
                .then_some(99)
                .ok_or(MatchmakerRouterError::Rejected(node("node-b")))
        }));
        // A routed party command is a distinct typed mTLS frame. The receiving
        // owner can force a re-resolution when its durable fence changed.
        router_b.register_party_handler(Arc::new(|source, command| {
            assert_eq!(source.as_str(), "node-a");
            assert_eq!(command.party_id.as_str(), "party-control");
            PartyControlReply::StaleOwnerFence
        }));
        let chat_deliveries_for_handler = Arc::clone(&chat_deliveries);
        let chat_directory_for_handler = Arc::clone(&chat_directory);
        let chat_presence_for_handler = Arc::clone(&chat_presence);
        let delivery_node = node_b.clone();
        router_b.register_chat_delivery_handler(Arc::new(move |source, delivery| {
            assert_eq!(source.as_str(), "node-a");
            assert_eq!(delivery.channel_id, "channel-42");
            let disposition = chat_directory_for_handler.validate_local_delivery(
                &delivery_node,
                &delivery,
                &chat_presence_for_handler,
                TimestampMillis::from_unix_millis(0),
            );
            if disposition != ChatDeliveryDisposition::Delivered {
                return disposition;
            }
            chat_deliveries_for_handler.fetch_add(1, Ordering::SeqCst);
            ChatDeliveryDisposition::Delivered
        }));
        let party_presence_deliveries_for_handler = Arc::clone(&party_presence_deliveries);
        router_b.register_party_presence_delivery_handler(Arc::new(move |source, delivery| {
            if source.as_str() != "node-a"
                || delivery.origin_node != source
                || delivery.party_id != "party-42"
                || delivery.snapshot.party_id != "party-42"
                || delivery.snapshot.online_members != vec!["alice".to_owned()]
            {
                return crate::party_presence::PartyPresenceDeliveryDisposition::Rejected;
            }
            party_presence_deliveries_for_handler.fetch_add(1, Ordering::SeqCst);
            crate::party_presence::PartyPresenceDeliveryDisposition::Delivered
        }));
        let chat_directory_for_presence = Arc::clone(&chat_directory);
        router_b.register_chat_presence_handler(Arc::new(move |_source, command| match command {
            ChatPresenceCommand::Advertise(lease) => {
                chat_directory_for_presence.advertise(lease, TimestampMillis::from_unix_millis(0))
            }
            ChatPresenceCommand::Withdraw(withdrawal) => chat_directory_for_presence.withdraw(
                &withdrawal.channel_id,
                &withdrawal.node_id,
                withdrawal.generation,
            ),
        }));
        let listener = router_b
            .serve("127.0.0.1:0".parse().expect("loopback"))
            .expect("listener");
        let router_a = TlsMatchmakerHandoffRouter::new(
            node_a.clone(),
            identity_a,
            BTreeMap::from([(node_b.clone(), cert_b)]),
            BTreeMap::from([(
                node_b.clone(),
                MatchmakerControlEndpoint {
                    address: listener.local_addr(),
                    server_name: "localhost".to_owned(),
                },
            )]),
            Duration::from_secs(2),
        )
        .expect("router a");
        assert!(matches!(
            router_a.party_command(
                &node_b,
                PartyControlCommand {
                    party_id: PartyId::parse("party-control").expect("party"),
                    lease: PartyOwnerLease {
                        party_id: PartyId::parse("party-control").expect("party"),
                        owner_node: node_b.clone(),
                        generation: OwnershipGeneration::new(7),
                        expires_at: TimestampMillis::from_unix_millis(u64::MAX),
                    },
                    actor: "alice".to_owned(),
                    request_id: "party-command-1".to_owned(),
                    expected_revision: 1,
                    operation: PartyControlOperation::Accept,
                },
            ),
            Ok(PartyControlReply::StaleOwnerFence)
        ));
        let remote_channel_lease = ChatPresenceLease {
            channel_id: "channel-advertised".to_owned(),
            node_id: node_a.clone(),
            generation: OwnershipGeneration::new(6),
            expires_at: TimestampMillis::from_unix_millis(u64::MAX),
        };
        assert_eq!(
            router_a
                .advertise_chat_presence(&node_b, remote_channel_lease.clone())
                .expect("mutual TLS chat presence advertisement"),
            ChatLeaseUpdate::Applied
        );
        assert!(chat_directory.matches_destination(
            "channel-advertised",
            &node_a,
            OwnershipGeneration::new(6),
            TimestampMillis::from_unix_millis(0),
        ));
        assert_eq!(
            router_a
                .withdraw_chat_presence(
                    &node_b,
                    ChatPresenceWithdrawal {
                        channel_id: remote_channel_lease.channel_id,
                        node_id: node_a.clone(),
                        generation: OwnershipGeneration::new(6),
                    },
                )
                .expect("mutual TLS chat presence withdrawal"),
            ChatLeaseUpdate::Applied
        );
        assert!(!chat_directory.matches_destination(
            "channel-advertised",
            &node_a,
            OwnershipGeneration::new(6),
            TimestampMillis::from_unix_millis(0),
        ));
        assert_eq!(
            router_a
                .deliver_party_presence(
                    &node_b,
                    crate::party_presence::RemotePartyPresenceDelivery {
                        party_id: "party-42".to_owned(),
                        origin_node: node_a.clone(),
                        origin_generation: OwnershipGeneration::new(4),
                        destination_generation: OwnershipGeneration::new(9),
                        snapshot: crate::party_presence::PartyPresenceSnapshot {
                            party_id: "party-42".to_owned(),
                            party_revision: 3,
                            sequence: 1,
                            online_members: vec!["alice".to_owned()],
                        },
                        deadline: TimestampMillis::from_unix_millis(u64::MAX),
                    },
                )
                .expect("mutual TLS party-presence delivery"),
            crate::party_presence::PartyPresenceDeliveryDisposition::Delivered
        );
        assert_eq!(party_presence_deliveries.load(Ordering::SeqCst), 1);
        let handoff = RemoteMatchmakerHandoff {
            ticket_id: TicketId::parse("remote-ticket").expect("ticket"),
            user_id: "alice".to_owned(),
            match_id: 99,
            join_token: "opaque-capability".to_owned(),
            expires_at: TimestampMillis::from_unix_millis(u64::MAX),
            formation_lease: lease(),
        };
        router_a
            .deliver_handoff(&node_b, handoff.clone())
            .expect("mutual TLS handoff delivery");
        assert_eq!(router_b.drain_handoffs(&node_b), vec![handoff.clone()]);
        let admission = RemoteMatchmakerAdmission {
            ticket_id: handoff.ticket_id,
            user_id: "alice".to_owned(),
            requester_node: node_a.clone(),
            join_token: handoff.join_token,
            formation_lease: handoff.formation_lease,
        };
        assert_eq!(
            router_a
                .admit_remote(&node_b, admission)
                .expect("mutual TLS remote admission"),
            99
        );
        assert_eq!(
            router_a
                .deliver_chat(&node_b, chat_delivery())
                .expect("mutual TLS chat delivery"),
            ChatDeliveryDisposition::Delivered
        );
        assert_eq!(
            router_a
                .deliver_chat(&node_b, chat_delivery())
                .expect("duplicate chat delivery response"),
            ChatDeliveryDisposition::Delivered
        );
        assert_eq!(chat_deliveries.load(Ordering::SeqCst), 1);
        let mut stale_delivery = chat_delivery();
        stale_delivery.event_id = 45;
        stale_delivery.destination_generation = OwnershipGeneration::new(2);
        assert_eq!(
            router_a
                .deliver_chat(&node_b, stale_delivery.clone())
                .expect("stale chat delivery response"),
            ChatDeliveryDisposition::Stale
        );
        assert_eq!(chat_deliveries.load(Ordering::SeqCst), 1);
        stale_delivery.destination_generation = OwnershipGeneration::new(3);
        assert_eq!(
            router_a
                .deliver_chat(&node_b, stale_delivery)
                .expect("refreshed lease retry response"),
            ChatDeliveryDisposition::Delivered
        );
        assert_eq!(chat_deliveries.load(Ordering::SeqCst), 2);
        let mut revoked_delivery = chat_delivery();
        revoked_delivery.event_id = 47;
        revoked_delivery.authority_epoch = 9;
        assert_eq!(
            router_a
                .deliver_chat(&node_b, revoked_delivery)
                .expect("revoked authority response"),
            ChatDeliveryDisposition::Unknown
        );
        assert_eq!(chat_deliveries.load(Ordering::SeqCst), 2);
        let mut expired_delivery = chat_delivery();
        expired_delivery.deadline = TimestampMillis::from_unix_millis(0);
        assert_eq!(
            router_a
                .deliver_chat(&node_b, expired_delivery)
                .expect("expired chat delivery response"),
            ChatDeliveryDisposition::Rejected
        );
        let mut oversized_delivery = chat_delivery();
        oversized_delivery.event_id = 46;
        oversized_delivery.payload = "x".repeat(MAX_FRAME_BYTES / 2 + 1);
        assert_eq!(
            router_a
                .deliver_chat(&node_b, oversized_delivery)
                .expect("oversized chat delivery response"),
            ChatDeliveryDisposition::Rejected,
            "the authenticated listener rejects a bounded typed command before it reaches local delivery"
        );
        assert_eq!(chat_deliveries.load(Ordering::SeqCst), 2);
        let runtime_event =
            crate::runtime::RuntimeEvent::new("match", "score.changed", b"payload".to_vec())
                .expect("runtime event");
        assert!(
            router_a
                .deliver_runtime_event(
                    &node_b,
                    RuntimeClusterEvent {
                        id: crate::runtime::cluster::RuntimeClusterEventId {
                            source_node: node_a.clone(),
                            sequence: 1,
                        },
                        event: runtime_event,
                    },
                )
                .expect("mutual TLS runtime event")
        );
        assert!(
            router_a
                .apply_runtime_cache_mutation(
                    &node_b,
                    RuntimeCacheMutation {
                        namespace: "match".to_owned(),
                        key: "score".to_owned(),
                        value: Some(b"9".to_vec()),
                        expires_at: TimestampMillis::from_unix_millis(u64::MAX),
                        fence: crate::runtime::cluster::RuntimeCacheFence {
                            owner_node: node_a.clone(),
                            generation: OwnershipGeneration::new(1),
                            sequence: 1,
                        },
                    },
                )
                .expect("mutual TLS runtime cache mutation")
        );
        assert!(
            router_a
                .submit_runtime_cache_write(
                    &node_b,
                    RuntimeCacheWrite {
                        namespace: "match".to_owned(),
                        key: "score".to_owned(),
                        value: None,
                        expires_at: TimestampMillis::from_unix_millis(u64::MAX),
                    },
                )
                .expect("mutual TLS runtime cache write")
        );
        assert_eq!(runtime_events.load(Ordering::SeqCst), 1);
        assert_eq!(runtime_mutations.load(Ordering::SeqCst), 1);
        assert_eq!(runtime_writes.load(Ordering::SeqCst), 1);
        drop(listener);
    }
}
