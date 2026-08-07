//! Live, durable, two-node matchmaker coordination.
//!
//! Realtime handlers only enqueue work here. A dedicated bounded worker owns
//! the queue index and may wait on storage/TLS control commands, so a client
//! socket's reactor never blocks on a remote shard owner. The worker is the
//! bridge between durable fencing, the narrow mTLS transport, and a local
//! [`Gateway`](crate::realtime::Gateway).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::thread;
use std::time::Duration;

use base64::Engine as _;

use crate::matchmaker::{Matchmaker, TicketId, TicketRequest, TicketState};
use crate::matchmaker_cluster::{
    MatchmakerHandoffRouter, MatchmakerRouterError, PartyAdmissionFence, QueueShardId,
    RemoteMatchmakerAdmission, RemoteMatchmakerHandoff, RemoteMatchmakerTicketCancellation,
    RemoteMatchmakerTicketOwner, RemoteMatchmakerTicketStatus, RemoteMatchmakerTicketSubmission,
};
use crate::matchmaker_transport::{RunningMatchmakerControlListener, TlsMatchmakerHandoffRouter};
use crate::realtime::gateway::REMOTE_AUTHORITATIVE_ADMISSION_UNAVAILABLE_MESSAGE;
use crate::realtime::{Gateway, ParticipantId, ParticipantIdGen};
use crate::services::matchmaker_directory::{
    MatchmakerShardLeaseResolution, StorageMatchmakerLeaseDirectory,
};
use crate::session::NodeId;
use crate::time::{Clock, DurationMillis, SystemClock, TimestampMillis};

const DEFAULT_WORK_QUEUE_CAPACITY: usize = 512;

/// Production configuration for one node's live matchmaker participant.
#[derive(Clone)]
pub struct LiveMatchmakerConfig {
    /// Stable local node identity, bound to the control-plane certificate.
    pub node_id: NodeId,
    /// Queue partition this node resolves/owns or forwards to its live owner.
    pub shard: QueueShardId,
    /// Durable shard-lease duration. Renewals retain their owner/generation.
    pub lease_ttl: DurationMillis,
    /// Player handoff capability duration.
    pub handoff_ttl: DurationMillis,
    /// Bounded wait for the control-plane worker when it receives a remote
    /// command from an authenticated peer.
    pub command_timeout: Duration,
    /// Durable portable authority shared by all nodes in the cluster.
    pub directory: StorageMatchmakerLeaseDirectory,
    /// The node's mutually-authenticated control router.
    pub router: Arc<TlsMatchmakerHandoffRouter>,
}

struct LiveState {
    owners: HashMap<TicketId, Vec<RemoteMatchmakerTicketOwner>>,
    leaders: HashMap<TicketId, ParticipantId>,
    /// Durable party queue freezes, keyed by the authoritative ticket. This
    /// lets the shard release the exact frozen revision on both cancellation
    /// and normal match formation, including when the session gateway is
    /// remote.
    party_admissions: HashMap<TicketId, PartyAdmissionFence>,
    locations: HashMap<TicketId, NodeId>,
    received: HashMap<(String, String), RemoteMatchmakerHandoff>,
    formed: HashMap<(String, String), RemoteMatchmakerHandoff>,
}

impl LiveState {
    fn handoff_key(ticket: &TicketId, user_id: &str) -> (String, String) {
        (ticket.as_str().to_owned(), user_id.to_owned())
    }
}

struct LiveInner {
    config: LiveMatchmakerConfig,
    index: Matchmaker,
    synthetic_participants: ParticipantIdGen,
    state: Mutex<LiveState>,
    gateway: Mutex<Weak<Gateway>>,
}

/// Live cluster adapter held by the gateway when `[cluster]` matchmaker mode is
/// enabled. It is intentionally separate from the single-node local index.
pub struct LiveMatchmakerNode {
    inner: Arc<LiveInner>,
    jobs: mpsc::SyncSender<Job>,
    listener: Mutex<Option<RunningMatchmakerControlListener>>,
}

enum Job {
    SubmitSession {
        sender: ParticipantId,
        request_id: u64,
        owners: Vec<RemoteMatchmakerTicketOwner>,
        request: TicketRequest,
        party_admission: Option<PartyAdmissionFence>,
    },
    CancelSession {
        sender: ParticipantId,
        request_id: u64,
        user_id: String,
        ticket_id: TicketId,
    },
    StatusSession {
        sender: ParticipantId,
        request_id: u64,
        user_id: String,
        ticket_id: TicketId,
    },
    AcceptSession {
        sender: ParticipantId,
        request_id: u64,
        user_id: String,
        ticket_id: TicketId,
        join_token: String,
    },
    RemoteSubmit {
        submission: RemoteMatchmakerTicketSubmission,
        reply: mpsc::Sender<Result<TicketId, MatchmakerRouterError>>,
    },
    RemoteCancel {
        cancellation: RemoteMatchmakerTicketCancellation,
        reply: mpsc::Sender<Result<bool, MatchmakerRouterError>>,
    },
    RemoteStatus {
        status: RemoteMatchmakerTicketStatus,
        reply: mpsc::Sender<Result<Option<TicketState>, MatchmakerRouterError>>,
    },
    ReceiveHandoffOneWay {
        handoff: RemoteMatchmakerHandoff,
    },
    RemoteAdmission {
        request: RemoteMatchmakerAdmission,
        reply: mpsc::Sender<Result<u64, MatchmakerRouterError>>,
    },
}

impl LiveMatchmakerNode {
    /// Construct and start the bounded worker. The node-control listener is
    /// started separately once its configured bind address is available.
    pub fn new(config: LiveMatchmakerConfig) -> crate::error::AppResult<Arc<Self>> {
        let (jobs, receiver) = mpsc::sync_channel(DEFAULT_WORK_QUEUE_CAPACITY);
        let inner = Arc::new(LiveInner {
            config,
            index: Matchmaker::new(),
            synthetic_participants: ParticipantIdGen::new(),
            state: Mutex::new(LiveState {
                owners: HashMap::new(),
                leaders: HashMap::new(),
                party_admissions: HashMap::new(),
                locations: HashMap::new(),
                received: HashMap::new(),
                formed: HashMap::new(),
            }),
            gateway: Mutex::new(Weak::new()),
        });
        let node = Arc::new(Self {
            inner: Arc::clone(&inner),
            jobs,
            listener: Mutex::new(None),
        });
        Self::install_router_handlers(&node);
        thread::Builder::new()
            .name(format!(
                "citadel-live-matchmaker-{}",
                inner.config.node_id.as_str()
            ))
            .spawn(move || run_worker(inner, receiver))
            .map_err(|error| {
                crate::error::AppError::new(
                    crate::error::ErrorCategory::Transport,
                    "could not spawn live matchmaker worker",
                )
                .with_detail(error.to_string())
            })?;
        Ok(node)
    }

    /// Attach the local gateway after its `Arc` exists. The weak reference
    /// preserves gateway teardown semantics; no control-plane worker keeps a
    /// dead realtime service alive.
    pub fn attach_gateway(&self, gateway: Weak<Gateway>) {
        if let Ok(mut slot) = self.inner.gateway.lock() {
            *slot = gateway;
        }
    }

    /// Stable identity of the local session node used on forwarded tickets.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.inner.config.node_id
    }

    /// Whether this local account still owns a queued live ticket. Party
    /// mutation uses this as the same safety gate as the single-node index.
    #[must_use]
    pub fn has_queued_ticket_for_user(&self, user_id: &str) -> bool {
        self.inner.state.lock().is_ok_and(|state| {
            state
                .owners
                .values()
                .any(|owners| owners.iter().any(|owner| owner.user_id == user_id))
        })
    }

    /// Start the real mTLS listener used by other session/shard nodes.
    pub fn start_listener(&self, bind: std::net::SocketAddr) -> crate::error::AppResult<()> {
        let mut listener = self.listener.lock().map_err(|_| {
            crate::error::AppError::internal("live matchmaker listener lock poisoned")
        })?;
        if listener.is_some() {
            return Err(crate::error::AppError::config(
                "live matchmaker control listener is already started",
            ));
        }
        let router = Arc::clone(&self.inner.config.router);
        *listener = Some(router.serve(bind)?);
        Ok(())
    }

    /// Bound control listener address, if this node has started it.
    #[must_use]
    pub fn control_listener_addr(&self) -> Option<std::net::SocketAddr> {
        self.listener.lock().ok().and_then(|listener| {
            listener
                .as_ref()
                .map(RunningMatchmakerControlListener::local_addr)
        })
    }

    /// Enqueue a local client's ticket request. Returns `false` on saturation;
    /// the gateway turns that into a retryable RPC error without blocking its
    /// transport task.
    pub fn submit_from_session(
        &self,
        sender: ParticipantId,
        request_id: u64,
        owners: Vec<RemoteMatchmakerTicketOwner>,
        request: TicketRequest,
        party_admission: Option<PartyAdmissionFence>,
    ) -> bool {
        self.jobs
            .try_send(Job::SubmitSession {
                sender,
                request_id,
                owners,
                request,
                party_admission,
            })
            .is_ok()
    }

    /// Enqueue a local client's ticket cancellation.
    pub fn cancel_from_session(
        &self,
        sender: ParticipantId,
        request_id: u64,
        user_id: String,
        ticket_id: TicketId,
    ) -> bool {
        self.jobs
            .try_send(Job::CancelSession {
                sender,
                request_id,
                user_id,
                ticket_id,
            })
            .is_ok()
    }

    /// Enqueue a local client's ticket status lookup.
    pub fn status_from_session(
        &self,
        sender: ParticipantId,
        request_id: u64,
        user_id: String,
        ticket_id: TicketId,
    ) -> bool {
        self.jobs
            .try_send(Job::StatusSession {
                sender,
                request_id,
                user_id,
                ticket_id,
            })
            .is_ok()
    }

    /// Enqueue a local client's handoff redemption.
    pub fn accept_from_session(
        &self,
        sender: ParticipantId,
        request_id: u64,
        user_id: String,
        ticket_id: TicketId,
        join_token: String,
    ) -> bool {
        self.jobs
            .try_send(Job::AcceptSession {
                sender,
                request_id,
                user_id,
                ticket_id,
                join_token,
            })
            .is_ok()
    }

    fn install_router_handlers(node: &Arc<Self>) {
        let router = Arc::clone(&node.inner.config.router);
        let stopped_node = node.inner.config.node_id.clone();
        let weak = Arc::downgrade(node);
        router.register_submission_handler(Arc::new(move |submission| {
            weak.upgrade()
                .ok_or_else(|| MatchmakerRouterError::Unavailable(stopped_node.clone()))?
                .request_remote_submit(submission)
        }));
        let stopped_node = node.inner.config.node_id.clone();
        let weak = Arc::downgrade(node);
        router.register_cancellation_handler(Arc::new(move |cancellation| {
            weak.upgrade()
                .ok_or_else(|| MatchmakerRouterError::Unavailable(stopped_node.clone()))?
                .request_remote_cancel(cancellation)
        }));
        let stopped_node = node.inner.config.node_id.clone();
        let weak = Arc::downgrade(node);
        router.register_status_handler(Arc::new(move |status| {
            weak.upgrade()
                .ok_or_else(|| MatchmakerRouterError::Unavailable(stopped_node.clone()))?
                .request_remote_status(status)
        }));
        let stopped_node = node.inner.config.node_id.clone();
        let weak = Arc::downgrade(node);
        router.register_handoff_handler(Arc::new(move |handoff| {
            weak.upgrade()
                .ok_or_else(|| MatchmakerRouterError::Unavailable(stopped_node.clone()))?
                .enqueue_remote_handoff(handoff)
        }));
        let stopped_node = node.inner.config.node_id.clone();
        let weak = Arc::downgrade(node);
        router.register_admission_handler(Arc::new(move |request| {
            weak.upgrade()
                .ok_or_else(|| MatchmakerRouterError::Unavailable(stopped_node.clone()))?
                .request_remote_admission(request)
        }));
    }

    fn request_remote_submit(
        &self,
        submission: RemoteMatchmakerTicketSubmission,
    ) -> Result<TicketId, MatchmakerRouterError> {
        let (reply, receiver) = mpsc::channel();
        self.jobs
            .try_send(Job::RemoteSubmit { submission, reply })
            .map_err(|_| MatchmakerRouterError::Unavailable(self.inner.config.node_id.clone()))?;
        receiver
            .recv_timeout(self.inner.config.command_timeout)
            .map_err(|_| MatchmakerRouterError::Unavailable(self.inner.config.node_id.clone()))?
    }

    fn request_remote_cancel(
        &self,
        cancellation: RemoteMatchmakerTicketCancellation,
    ) -> Result<bool, MatchmakerRouterError> {
        let (reply, receiver) = mpsc::channel();
        self.jobs
            .try_send(Job::RemoteCancel {
                cancellation,
                reply,
            })
            .map_err(|_| MatchmakerRouterError::Unavailable(self.inner.config.node_id.clone()))?;
        receiver
            .recv_timeout(self.inner.config.command_timeout)
            .map_err(|_| MatchmakerRouterError::Unavailable(self.inner.config.node_id.clone()))?
    }

    fn request_remote_status(
        &self,
        status: RemoteMatchmakerTicketStatus,
    ) -> Result<Option<TicketState>, MatchmakerRouterError> {
        let (reply, receiver) = mpsc::channel();
        self.jobs
            .try_send(Job::RemoteStatus { status, reply })
            .map_err(|_| MatchmakerRouterError::Unavailable(self.inner.config.node_id.clone()))?;
        receiver
            .recv_timeout(self.inner.config.command_timeout)
            .map_err(|_| MatchmakerRouterError::Unavailable(self.inner.config.node_id.clone()))?
    }

    fn enqueue_remote_handoff(
        &self,
        handoff: RemoteMatchmakerHandoff,
    ) -> Result<(), MatchmakerRouterError> {
        self.jobs
            .try_send(Job::ReceiveHandoffOneWay { handoff })
            .map_err(|_| MatchmakerRouterError::Unavailable(self.inner.config.node_id.clone()))
    }

    fn request_remote_admission(
        &self,
        request: RemoteMatchmakerAdmission,
    ) -> Result<u64, MatchmakerRouterError> {
        let (reply, receiver) = mpsc::channel();
        self.jobs
            .try_send(Job::RemoteAdmission { request, reply })
            .map_err(|_| MatchmakerRouterError::Unavailable(self.inner.config.node_id.clone()))?;
        receiver
            .recv_timeout(self.inner.config.command_timeout)
            .map_err(|_| MatchmakerRouterError::Unavailable(self.inner.config.node_id.clone()))?
    }
}

fn run_worker(inner: Arc<LiveInner>, receiver: mpsc::Receiver<Job>) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(%error, "could not build live matchmaker worker runtime");
            return;
        }
    };
    while let Ok(job) = receiver.recv() {
        runtime.block_on(handle_job(&inner, job));
    }
}

async fn handle_job(inner: &LiveInner, job: Job) {
    match job {
        Job::SubmitSession {
            sender,
            request_id,
            owners,
            request,
            party_admission,
        } => {
            let result = submit_from_session(inner, owners, request, party_admission).await;
            if let Some(gateway) = gateway(inner) {
                match result {
                    Ok(ticket_id) => gateway.live_matchmaker_reply(
                        sender,
                        request_id,
                        true,
                        serde_json::json!({ "ticket_id": ticket_id.as_str() }).to_string(),
                    ),
                    Err(_) => gateway.live_matchmaker_reply(
                        sender,
                        request_id,
                        false,
                        "matchmaker shard is unavailable".to_owned(),
                    ),
                }
            }
        }
        Job::CancelSession {
            sender,
            request_id,
            user_id,
            ticket_id,
        } => {
            let cancelled = cancel_from_session(inner, user_id, ticket_id)
                .await
                .unwrap_or(false);
            if let Some(gateway) = gateway(inner) {
                gateway.live_matchmaker_reply(
                    sender,
                    request_id,
                    true,
                    serde_json::json!({ "cancelled": cancelled }).to_string(),
                );
            }
        }
        Job::StatusSession {
            sender,
            request_id,
            user_id,
            ticket_id,
        } => {
            let result = status_from_session(inner, &user_id, &ticket_id).await;
            if let Some(gateway) = gateway(inner) {
                match result {
                    Some(value) => gateway.live_matchmaker_reply(sender, request_id, true, value),
                    None => gateway.live_matchmaker_reply(
                        sender,
                        request_id,
                        false,
                        "ticket not found".to_owned(),
                    ),
                }
            }
        }
        Job::AcceptSession {
            sender,
            request_id,
            user_id,
            ticket_id,
            join_token,
        } => accept_from_session(inner, sender, request_id, user_id, ticket_id, join_token).await,
        Job::RemoteSubmit { submission, reply } => {
            let _ = reply.send(authoritative_submit(inner, submission).await);
        }
        Job::RemoteCancel {
            cancellation,
            reply,
        } => {
            let _ = reply.send(authoritative_cancel(inner, cancellation).await);
        }
        Job::RemoteStatus { status, reply } => {
            let _ = reply.send(authoritative_status(inner, status));
        }
        Job::ReceiveHandoffOneWay { handoff } => {
            let _ = store_received_handoff(inner, handoff);
        }
        Job::RemoteAdmission { request, reply } => {
            let _ = reply.send(admit_remote(inner, request).await);
        }
    }
}

async fn resolve_owner(
    inner: &LiveInner,
    now: TimestampMillis,
) -> Result<MatchmakerShardLeaseResolution, MatchmakerRouterError> {
    let expires_at = now
        .checked_add(inner.config.lease_ttl)
        .map_err(|_| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))?;
    inner
        .config
        .directory
        .acquire_or_resolve(
            inner.config.shard,
            inner.config.node_id.clone(),
            expires_at,
            now,
        )
        .await
        .map_err(|_| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))
}

async fn submit_from_session(
    inner: &LiveInner,
    owners: Vec<RemoteMatchmakerTicketOwner>,
    request: TicketRequest,
    party_admission: Option<PartyAdmissionFence>,
) -> Result<TicketId, MatchmakerRouterError> {
    let now = SystemClock.now();
    match resolve_owner(inner, now).await? {
        MatchmakerShardLeaseResolution::Local(_) => {
            authoritative_submit(
                inner,
                RemoteMatchmakerTicketSubmission {
                    owners,
                    request,
                    party_admission,
                },
            )
            .await
        }
        MatchmakerShardLeaseResolution::Remote(owner) => {
            let ticket_id = inner.config.router.submit_ticket(
                &owner.owner_node,
                RemoteMatchmakerTicketSubmission {
                    owners,
                    request,
                    party_admission,
                },
            )?;
            if let Ok(mut state) = inner.state.lock() {
                state.locations.insert(ticket_id.clone(), owner.owner_node);
            }
            Ok(ticket_id)
        }
    }
}

async fn authoritative_submit(
    inner: &LiveInner,
    submission: RemoteMatchmakerTicketSubmission,
) -> Result<TicketId, MatchmakerRouterError> {
    if submission.owners.is_empty()
        || submission
            .owners
            .iter()
            .any(|owner| owner.session_node == inner.config.node_id && owner.user_id.is_empty())
    {
        return Err(MatchmakerRouterError::Rejected(
            inner.config.node_id.clone(),
        ));
    }
    let now = SystemClock.now();
    let lease = match resolve_owner(inner, now).await? {
        MatchmakerShardLeaseResolution::Local(lease) => lease,
        MatchmakerShardLeaseResolution::Remote(_) => {
            return Err(MatchmakerRouterError::Rejected(
                inner.config.node_id.clone(),
            ));
        }
    };
    let party_admission = submission.party_admission.clone();
    let ticket_expires_at = now
        .checked_add(DurationMillis::from_millis(submission.request.ttl_ms))
        .map_err(|_| MatchmakerRouterError::Rejected(inner.config.node_id.clone()))?;
    if ticket_expires_at <= now {
        return Err(MatchmakerRouterError::Rejected(
            inner.config.node_id.clone(),
        ));
    }
    // The initial admission only protects the asynchronous handoff. Renew it
    // through the current party owner before a ticket becomes live, using the
    // exact same timestamp that add_party will persist on the ticket.
    if let Some(admission) = party_admission.as_ref()
        && !gateway(inner).is_some_and(|gateway| {
            gateway
                .live_matchmaker_renew_party_admission(admission, ticket_expires_at)
                .is_ok()
        })
    {
        if let Some(gateway) = gateway(inner) {
            gateway.live_matchmaker_release_party_admission(admission);
        }
        return Err(MatchmakerRouterError::Rejected(
            inner.config.node_id.clone(),
        ));
    }
    let members = (0..submission.owners.len())
        .map(|_| inner.synthetic_participants.next_id())
        .collect::<Vec<_>>();
    let leader = members[0];
    let ticket_id = match inner
        .index
        .add_party(leader, members, submission.request, now)
    {
        Ok(ticket_id) => ticket_id,
        Err(_) => {
            // Renewal succeeded, but no ticket was created. Do not make the
            // client wait for its requested TTL to retry an invalid/rejected
            // admission; exact-fenced cleanup cannot affect a newer ticket.
            if let Some(admission) = party_admission.as_ref()
                && let Some(gateway) = gateway(inner)
            {
                gateway.live_matchmaker_release_party_admission(admission);
            }
            return Err(MatchmakerRouterError::Rejected(
                inner.config.node_id.clone(),
            ));
        }
    };
    // This runs on the shard-owner worker after any forwarding delay and after
    // queue insertion. A changed party revision must not survive as a queued
    // ticket: cancel before publishing the ticket or forming a cohort.
    if let Some(admission) = party_admission.as_ref()
        && !gateway(inner)
            .is_some_and(|gateway| gateway.live_matchmaker_revalidate_party_admission(admission))
    {
        let _ = inner.index.cancel(leader, &ticket_id, SystemClock.now());
        if let Some(gateway) = gateway(inner) {
            gateway.live_matchmaker_release_party_admission(admission);
        }
        return Err(MatchmakerRouterError::Rejected(
            inner.config.node_id.clone(),
        ));
    }
    if let Ok(mut state) = inner.state.lock() {
        state.owners.insert(ticket_id.clone(), submission.owners);
        state.leaders.insert(ticket_id.clone(), leader);
        if let Some(admission) = party_admission {
            state.party_admissions.insert(ticket_id.clone(), admission);
        }
        state
            .locations
            .insert(ticket_id.clone(), inner.config.node_id.clone());
    }
    form_matches(inner, &lease, now).await?;
    Ok(ticket_id)
}

async fn form_matches(
    inner: &LiveInner,
    lease: &crate::matchmaker_cluster::MatchmakerShardLease,
    now: TimestampMillis,
) -> Result<(), MatchmakerRouterError> {
    let Some(gateway) = gateway(inner) else {
        return Err(MatchmakerRouterError::Unavailable(
            inner.config.node_id.clone(),
        ));
    };
    for formed in inner.index.preview(now) {
        // Fail closed before any durable claim: a shard owner whose
        // GameScript is not ready must not give birth to a match room, and a
        // previewed cohort must stay queued (able to form once the script
        // recovers) rather than be consumed by a doomed formation.
        let room_id = gateway
            .live_matchmaker_create_room(formed.participants.len())
            .map_err(|()| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))?;
        if let Err(_error) = inner
            .config
            .directory
            .claim_formations(&formed.tickets, lease, now)
            .await
        {
            let _ = gateway.discard_empty_room(room_id);
            return Err(MatchmakerRouterError::Rejected(
                inner.config.node_id.clone(),
            ));
        }
        if !inner
            .index
            .commit_formations(std::slice::from_ref(&formed), now)
        {
            let _ = gateway.discard_empty_room(room_id);
            return Err(MatchmakerRouterError::Rejected(
                inner.config.node_id.clone(),
            ));
        }
        let expires_at = now
            .checked_add(inner.config.handoff_ttl)
            .map_err(|_| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))?;
        let mut handoffs = Vec::new();
        let mut release_admissions = Vec::new();
        {
            let mut state = inner
                .state
                .lock()
                .map_err(|_| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))?;
            for ticket_id in &formed.tickets {
                if let Some(admission) = state.party_admissions.remove(ticket_id) {
                    release_admissions.push(admission);
                }
                let owners = state
                    .owners
                    .remove(ticket_id)
                    .ok_or_else(|| MatchmakerRouterError::Rejected(inner.config.node_id.clone()))?;
                for owner in owners {
                    let handoff = RemoteMatchmakerHandoff {
                        ticket_id: ticket_id.clone(),
                        user_id: owner.user_id,
                        match_id: room_id,
                        join_token: fresh_join_token().map_err(|_| {
                            MatchmakerRouterError::Unavailable(inner.config.node_id.clone())
                        })?,
                        expires_at,
                        formation_lease: lease.clone(),
                    };
                    state.formed.insert(
                        LiveState::handoff_key(&handoff.ticket_id, &handoff.user_id),
                        handoff.clone(),
                    );
                    handoffs.push((owner.session_node, handoff));
                }
            }
        }
        // The tickets are now matched rather than queued.  The durable
        // revision/leader fence makes this harmless if a delayed cleanup
        // races a later party admission.
        for admission in release_admissions {
            gateway.live_matchmaker_release_party_admission(&admission);
        }
        for (session_node, handoff) in handoffs {
            if session_node == inner.config.node_id {
                store_received_handoff(inner, handoff)?;
            } else {
                inner
                    .config
                    .router
                    .deliver_handoff(&session_node, handoff)?;
            }
        }
    }
    Ok(())
}

async fn cancel_from_session(
    inner: &LiveInner,
    user_id: String,
    ticket_id: TicketId,
) -> Result<bool, MatchmakerRouterError> {
    let owner = inner
        .state
        .lock()
        .ok()
        .and_then(|state| state.locations.get(&ticket_id).cloned())
        .ok_or_else(|| MatchmakerRouterError::Rejected(inner.config.node_id.clone()))?;
    let cancellation = RemoteMatchmakerTicketCancellation { ticket_id, user_id };
    if owner == inner.config.node_id {
        authoritative_cancel(inner, cancellation).await
    } else {
        inner.config.router.cancel_ticket(&owner, cancellation)
    }
}

async fn authoritative_cancel(
    inner: &LiveInner,
    cancellation: RemoteMatchmakerTicketCancellation,
) -> Result<bool, MatchmakerRouterError> {
    let now = SystemClock.now();
    if !matches!(
        resolve_owner(inner, now).await?,
        MatchmakerShardLeaseResolution::Local(_)
    ) {
        return Err(MatchmakerRouterError::Rejected(
            inner.config.node_id.clone(),
        ));
    }
    let (cancelled, release) = {
        let mut state = inner
            .state
            .lock()
            .map_err(|_| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))?;
        let authorized = state
            .owners
            .get(&cancellation.ticket_id)
            .and_then(|owners| owners.first())
            .is_some_and(|leader| leader.user_id == cancellation.user_id);
        let Some(leader) = state.leaders.get(&cancellation.ticket_id).copied() else {
            return Ok(false);
        };
        let cancelled = authorized && inner.index.cancel(leader, &cancellation.ticket_id, now);
        if cancelled {
            state.owners.remove(&cancellation.ticket_id);
            state.leaders.remove(&cancellation.ticket_id);
            (
                cancelled,
                state.party_admissions.remove(&cancellation.ticket_id),
            )
        } else {
            (cancelled, None)
        }
    };
    if let Some(admission) = release
        && let Some(gateway) = gateway(inner)
    {
        gateway.live_matchmaker_release_party_admission(&admission);
    }
    Ok(cancelled)
}

async fn status_from_session(
    inner: &LiveInner,
    user_id: &str,
    ticket_id: &TicketId,
) -> Option<String> {
    if let Ok(state) = inner.state.lock()
        && let Some(handoff) = state
            .received
            .get(&LiveState::handoff_key(ticket_id, user_id))
            .filter(|handoff| handoff.expires_at > SystemClock.now())
    {
        return Some(
            serde_json::json!({
                "state": "matched",
                "match": {
                    "ticket_id": ticket_id.as_str(),
                    "match_id": handoff.match_id,
                    "join_token": handoff.join_token,
                    "expires_at": handoff.expires_at.unix_millis(),
                }
            })
            .to_string(),
        );
    }
    let owner = inner
        .state
        .lock()
        .ok()
        .and_then(|state| state.locations.get(ticket_id).cloned())?;
    let status = RemoteMatchmakerTicketStatus {
        ticket_id: ticket_id.clone(),
        user_id: user_id.to_owned(),
    };
    let state = if owner == inner.config.node_id {
        authoritative_status(inner, status).ok()?
    } else {
        inner.config.router.ticket_status(&owner, status).ok()?
    }?;
    Some(serde_json::json!({ "state": state_name(state) }).to_string())
}

fn authoritative_status(
    inner: &LiveInner,
    status: RemoteMatchmakerTicketStatus,
) -> Result<Option<TicketState>, MatchmakerRouterError> {
    let now = SystemClock.now();
    let state = inner
        .state
        .lock()
        .map_err(|_| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))?;
    let authorized = state
        .owners
        .get(&status.ticket_id)
        .and_then(|owners| owners.first())
        .is_some_and(|leader| leader.user_id == status.user_id)
        || state
            .formed
            .contains_key(&LiveState::handoff_key(&status.ticket_id, &status.user_id));
    let result = state
        .leaders
        .get(&status.ticket_id)
        .copied()
        .and_then(|leader| authorized.then(|| inner.index.state(leader, &status.ticket_id, now)))
        .flatten();
    Ok(result)
}

fn store_received_handoff(
    inner: &LiveInner,
    handoff: RemoteMatchmakerHandoff,
) -> Result<(), MatchmakerRouterError> {
    let now = SystemClock.now();
    if handoff.expires_at <= now {
        return Err(MatchmakerRouterError::Rejected(
            inner.config.node_id.clone(),
        ));
    }
    let inserted = inner
        .state
        .lock()
        .map_err(|_| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))?
        .received
        .insert(
            LiveState::handoff_key(&handoff.ticket_id, &handoff.user_id),
            handoff.clone(),
        )
        .is_none();
    if inserted && let Some(gateway) = gateway(inner) {
        gateway.live_matchmaker_notify(&handoff);
    }
    Ok(())
}

async fn accept_from_session(
    inner: &LiveInner,
    sender: ParticipantId,
    request_id: u64,
    user_id: String,
    ticket_id: TicketId,
    join_token: String,
) {
    let handoff = inner.state.lock().ok().and_then(|state| {
        state
            .received
            .get(&LiveState::handoff_key(&ticket_id, &user_id))
            .cloned()
    });
    let Some(handoff) = handoff.filter(|handoff| handoff.expires_at > SystemClock.now()) else {
        if let Some(gateway) = gateway(inner) {
            gateway.live_matchmaker_reply(
                sender,
                request_id,
                false,
                "match handoff not found or expired".to_owned(),
            );
        }
        return;
    };
    if handoff.join_token != join_token {
        if let Some(gateway) = gateway(inner) {
            gateway.live_matchmaker_reply(
                sender,
                request_id,
                false,
                "invalid match join token".to_owned(),
            );
        }
        return;
    }
    let admission = RemoteMatchmakerAdmission {
        ticket_id: ticket_id.clone(),
        user_id: user_id.clone(),
        requester_node: inner.config.node_id.clone(),
        join_token,
        formation_lease: handoff.formation_lease.clone(),
    };
    let result = if handoff.formation_lease.owner_node == inner.config.node_id {
        admit_local(inner, sender, request_id, admission).await
    } else {
        inner
            .config
            .router
            .admit_remote(&handoff.formation_lease.owner_node, admission)
            .and_then(|match_id| {
                gateway(inner)
                    .ok_or_else(|| {
                        MatchmakerRouterError::Unavailable(inner.config.node_id.clone())
                    })?
                    .live_matchmaker_finish_remote_accept(sender, request_id, match_id)
                    .map_err(|_| MatchmakerRouterError::Rejected(inner.config.node_id.clone()))?;
                Ok(match_id)
            })
    };
    match result {
        Ok(_) => {
            if let Ok(mut state) = inner.state.lock() {
                state
                    .received
                    .remove(&LiveState::handoff_key(&ticket_id, &user_id));
            }
        }
        Err(error) => {
            if let Some(gateway) = gateway(inner) {
                let message = match error {
                    MatchmakerRouterError::AuthoritativeAdmissionUnavailable(_) => {
                        REMOTE_AUTHORITATIVE_ADMISSION_UNAVAILABLE_MESSAGE
                    }
                    _ => "match admission failed",
                };
                gateway.live_matchmaker_reply(sender, request_id, false, message.to_owned());
            }
        }
    }
}

async fn admit_local(
    inner: &LiveInner,
    sender: ParticipantId,
    request_id: u64,
    request: RemoteMatchmakerAdmission,
) -> Result<u64, MatchmakerRouterError> {
    let match_id = admit_remote(inner, request).await?;
    gateway(inner)
        .ok_or_else(|| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))?
        .live_matchmaker_finish_local_accept(sender, request_id, match_id)
        .map_err(|_| MatchmakerRouterError::Rejected(inner.config.node_id.clone()))?;
    Ok(match_id)
}

async fn admit_remote(
    inner: &LiveInner,
    request: RemoteMatchmakerAdmission,
) -> Result<u64, MatchmakerRouterError> {
    let now = SystemClock.now();
    let handoff = inner
        .state
        .lock()
        .ok()
        .and_then(|state| {
            state
                .formed
                .get(&LiveState::handoff_key(
                    &request.ticket_id,
                    &request.user_id,
                ))
                .cloned()
        })
        .filter(|handoff| {
            handoff.expires_at > now
                && handoff.join_token == request.join_token
                && handoff
                    .formation_lease
                    .has_same_fence_as(&request.formation_lease)
        })
        .ok_or_else(|| MatchmakerRouterError::Rejected(inner.config.node_id.clone()))?;
    // Script-bound matches need an owner-to-session state/intent relay that
    // does not exist yet. Relay matches keep their established cross-node
    // admission path.
    if request.requester_node != inner.config.node_id
        && gateway(inner)
            .is_some_and(|gateway| gateway.remote_match_requires_state_relay(handoff.match_id))
    {
        return Err(MatchmakerRouterError::AuthoritativeAdmissionUnavailable(
            inner.config.node_id.clone(),
        ));
    }
    inner
        .config
        .directory
        .claim_admission(
            &request.ticket_id,
            &request.user_id,
            &request.formation_lease,
            now,
        )
        .await
        .map_err(|_| MatchmakerRouterError::Rejected(inner.config.node_id.clone()))?;
    if request.requester_node != inner.config.node_id {
        gateway(inner)
            .ok_or_else(|| MatchmakerRouterError::Unavailable(inner.config.node_id.clone()))?
            .live_matchmaker_admit_remote(request.requester_node, request.user_id, handoff.match_id)
            .map_err(|_| MatchmakerRouterError::Rejected(inner.config.node_id.clone()))?;
    }
    Ok(handoff.match_id)
}

fn gateway(inner: &LiveInner) -> Option<Arc<Gateway>> {
    inner
        .gateway
        .lock()
        .ok()
        .and_then(|gateway| gateway.upgrade())
}

fn fresh_join_token() -> Result<String, getrandom::Error> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

const fn state_name(state: TicketState) -> &'static str {
    match state {
        TicketState::Queued => "queued",
        TicketState::Matched => "matched",
        TicketState::Removed => "removed",
    }
}

#[cfg(test)]
// A `panic!` in a match arm reports which resolution was returned, which an
// `assert!(matches!(..))` cannot.
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use rustls::pki_types::CertificateDer;

    use crate::matchmaker_cluster::MatchmakerShardLease;
    use crate::matchmaker_transport::MatchmakerControlIdentity;
    use crate::repository::InMemoryStorageRepository;
    use crate::session::OwnershipGeneration;

    fn control_identity() -> (MatchmakerControlIdentity, CertificateDer<'static>) {
        let certificate =
            rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("certificate");
        let key = certificate.key_pair.serialize_der();
        let leaf = CertificateDer::from(certificate.cert);
        (
            MatchmakerControlIdentity::from_der(vec![leaf.clone()], key).expect("control identity"),
            leaf,
        )
    }

    fn directory() -> StorageMatchmakerLeaseDirectory {
        let storage: Arc<dyn crate::repository::StorageRepository> =
            Arc::new(InMemoryStorageRepository::new());
        StorageMatchmakerLeaseDirectory::new(storage)
    }

    /// Build a live node with a well-formed control router.
    ///
    /// The router needs at least one trust anchor to construct, so a throwaway
    /// peer is registered. No connection is ever made: these tests drive the
    /// durable fencing path, which resolves before any peer is contacted.
    fn node(node_id: &str, directory: &StorageMatchmakerLeaseDirectory) -> Arc<LiveMatchmakerNode> {
        let node_id = NodeId::new(node_id).expect("node id");
        let (identity, _cert) = control_identity();
        let (_peer_identity, peer_cert) = control_identity();
        let peer = NodeId::new("unused-peer").expect("peer id");
        let router = Arc::new(
            TlsMatchmakerHandoffRouter::new(
                node_id.clone(),
                identity,
                BTreeMap::from([(peer, peer_cert)]),
                BTreeMap::new(),
                Duration::from_secs(2),
            )
            .expect("router"),
        );
        LiveMatchmakerNode::new(LiveMatchmakerConfig {
            node_id,
            shard: QueueShardId::new(0),
            lease_ttl: DurationMillis::from_millis(500),
            handoff_ttl: DurationMillis::from_millis(5_000),
            command_timeout: Duration::from_secs(2),
            directory: directory.clone(),
            router,
        })
        .expect("live node")
    }

    #[tokio::test]
    async fn resolve_owner_acquires_an_unowned_shard() {
        let directory = directory();
        let node = node("node-a", &directory);
        let now = SystemClock.now();

        match resolve_owner(&node.inner, now).await.expect("resolution") {
            MatchmakerShardLeaseResolution::Local(lease) => {
                assert_eq!(lease.owner_node.as_str(), "node-a");
                assert_eq!(lease.shard, QueueShardId::new(0));
            }
            MatchmakerShardLeaseResolution::Remote(owner) => {
                panic!("an unowned shard must resolve locally, got remote {owner:?}")
            }
        }
    }

    #[tokio::test]
    async fn resolve_owner_defers_to_a_live_remote_owner() {
        let directory = directory();
        let now = SystemClock.now();
        let remote = NodeId::new("node-b").expect("node b");
        directory
            .acquire(
                MatchmakerShardLease {
                    shard: QueueShardId::new(0),
                    owner_node: remote.clone(),
                    generation: OwnershipGeneration::new(1),
                    expires_at: now
                        .checked_add(DurationMillis::from_millis(5_000))
                        .expect("expiry"),
                },
                now,
            )
            .await
            .expect("node b takes the shard");

        let node = node("node-a", &directory);
        match resolve_owner(&node.inner, now).await.expect("resolution") {
            MatchmakerShardLeaseResolution::Remote(owner) => {
                assert_eq!(owner.owner_node, remote, "must forward to the live owner");
            }
            MatchmakerShardLeaseResolution::Local(lease) => panic!(
                "a live remote lease must not be stolen; acquired {:?}",
                lease.owner_node
            ),
        }
    }

    /// The load-bearing invariant: an owner that has been superseded must not be
    /// able to form matches, even though it still holds a lease value in memory.
    ///
    /// Without this fence, two nodes could each believe they own the shard after
    /// a partition and form overlapping matches from the same tickets.
    #[tokio::test]
    async fn a_superseded_owner_cannot_claim_formations() {
        let directory = directory();
        let now = SystemClock.now();
        let ttl = DurationMillis::from_millis(500);

        // node-a owns the shard and holds its lease.
        let node_a = node("node-a", &directory);
        let stale_lease = match resolve_owner(&node_a.inner, now).await.expect("resolution") {
            MatchmakerShardLeaseResolution::Local(lease) => lease,
            MatchmakerShardLeaseResolution::Remote(owner) => {
                panic!("node-a should own an unowned shard, got {owner:?}")
            }
        };

        // The lease lapses and node-b takes over with a higher generation.
        let after_expiry = now
            .checked_add(ttl)
            .expect("expiry")
            .checked_add(ttl)
            .expect("lapse");
        let node_b = node("node-b", &directory);
        let fresh_lease = match resolve_owner(&node_b.inner, after_expiry)
            .await
            .expect("resolution")
        {
            MatchmakerShardLeaseResolution::Local(lease) => lease,
            MatchmakerShardLeaseResolution::Remote(owner) => {
                panic!("node-b should take an expired shard, got {owner:?}")
            }
        };
        assert!(
            fresh_lease.generation > stale_lease.generation,
            "takeover must bump the fencing generation: {:?} then {:?}",
            stale_lease.generation,
            fresh_lease.generation
        );

        // node-a, still holding its old lease, is now fenced out.
        let tickets = [TicketId::parse("ticket-1").expect("ticket id")];
        let fenced = directory
            .claim_formations(&tickets, &stale_lease, after_expiry)
            .await;
        assert!(
            fenced.is_err(),
            "a superseded owner must not be able to claim a formation"
        );

        // The current owner still can, so the fence rejects staleness rather
        // than formation itself.
        directory
            .claim_formations(&tickets, &fresh_lease, after_expiry)
            .await
            .expect("the current owner forms the match");
    }

    /// A ticket may only be formed once, so a retry after a partial failure
    /// cannot place the same players into a second room.
    #[tokio::test]
    async fn a_ticket_cannot_be_formed_twice() {
        let directory = directory();
        let now = SystemClock.now();
        let node = node("node-a", &directory);
        let lease = match resolve_owner(&node.inner, now).await.expect("resolution") {
            MatchmakerShardLeaseResolution::Local(lease) => lease,
            MatchmakerShardLeaseResolution::Remote(owner) => {
                panic!("expected local ownership, got {owner:?}")
            }
        };
        let tickets = [TicketId::parse("ticket-1").expect("ticket id")];

        directory
            .claim_formations(&tickets, &lease, now)
            .await
            .expect("first formation is claimed");
        let repeat = directory.claim_formations(&tickets, &lease, now).await;
        assert!(
            repeat.is_err(),
            "the same ticket must not be formed a second time"
        );
    }

    #[tokio::test]
    async fn an_idle_node_reports_no_queued_ticket() {
        let directory = directory();
        let node = node("node-a", &directory);
        assert!(!node.has_queued_ticket_for_user("alice"));
    }
}
