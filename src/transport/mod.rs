//! Wire-agnostic transport abstraction for Citadel's realtime layer.
//!
//! Citadel's realtime gateway must not depend on a concrete wire protocol. This
//! module defines the small set of types and traits that every transport
//! implements:
//!
//! - [`Envelope`] and the codec ([`codec`]) define the unit of communication.
//! - [`OutboundQueue`] and [`OverflowPolicy`] ([`queue`]) define per-connection
//!   backpressure.
//! - [`ConnectionId`], [`PeerAddr`], and [`TransportKind`] identify connections.
//! - [`Listener`] and [`Connection`] are the async-free shape of an accepted
//!   connection and a listener handle; concrete transports (QUIC via `quinn`,
//!   WebSocket via `tokio-tungstenite`) implement the async behavior in their
//!   own modules and reuse these types.
//!
//! The traits here are intentionally minimal and object-safe-friendly so the
//! gateway can hold heterogeneous transports behind a common boundary. The
//! product decision is QUIC-first with a WebSocket fallback; both reuse this
//! abstraction.

pub mod codec;
pub mod metrics;
pub mod queue;
pub mod quic;
pub mod websocket;
pub mod webtransport;

pub use metrics::{TransportMetrics, TransportMetricsSnapshot};

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub use codec::{Envelope, decode_datagram, decode_framed};
pub use queue::{OutboundQueue, OverflowPolicy, PushOutcome};

use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use crate::app::App;
use crate::chat_cluster::{
    ChatDeliveryDispatcher, ChatPresenceDirectory, ChatPresenceLease, ChatPresencePublisher,
    ChatPresenceWithdrawal, LocalChatPresenceAnnouncer,
};
use crate::config::{
    Config, LuaExecutionMode, NetworkPeerConfig, RuntimeLanguage, TransformSyncConfig,
};
use crate::error::{AppError, AppResult, ErrorCategory};
use crate::lifecycle::{AsyncService, CancellationToken, Supervisor};
use crate::matchmaker_cluster::QueueShardId;
use crate::matchmaker_live::{LiveMatchmakerConfig, LiveMatchmakerNode};
use crate::matchmaker_transport::{
    ChatPresenceCommand, MatchmakerControlEndpoint, MatchmakerControlIdentity,
    TlsMatchmakerHandoffRouter, read_matchmaker_control_certificate,
};
use crate::realtime::Gateway;
use crate::realtime::netpeer::{RateLimits, RepAuthority, RepInterestConfig};
use crate::realtime::transform::{TransformHub, TransformHubConfig, TransformState};
#[cfg(feature = "runtime-js")]
use crate::runtime::JsRuntime;
#[cfg(feature = "runtime-python")]
use crate::runtime::PythonRuntime;
use crate::runtime::cache_lease::{
    RuntimeCacheLease, RuntimeCacheLeaseResolution, StorageRuntimeCacheLeaseDirectory,
};
use crate::runtime::cluster::RuntimeCacheWrite;
use crate::runtime::{
    LuaRuntime, Runtime, RuntimeCachePublisher, RuntimeEvent, RuntimeEventPublisher,
};
use crate::services::ChatChannelAuthorizer;
use crate::services::matchmaker_directory::StorageMatchmakerLeaseDirectory;
use crate::services::party_directory::StoragePartyDirectory;
use crate::session::NodeId;
use crate::time::{Clock, DurationMillis, SystemClock, TimestampMillis};

/// Typed, best-effort broadcaster for the local channel-presence announcer.
/// It deliberately discards a transient peer error: the supervised renewer
/// republishes the lease and durable history is the recovery path.
struct ControlChatPresencePublisher {
    router: Arc<TlsMatchmakerHandoffRouter>,
    peers: Vec<NodeId>,
}

/// Bounded asynchronous fan-out for runtime events. A saturated peer queue is
/// deliberately a best-effort drop; the local event remains accepted.
struct ControlRuntimeEventPublisher {
    sender: mpsc::SyncSender<RuntimeEvent>,
}

impl ControlRuntimeEventPublisher {
    fn new(router: Arc<TlsMatchmakerHandoffRouter>, peers: Vec<NodeId>, source: NodeId) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<RuntimeEvent>(256);
        let _ = thread::Builder::new()
            .name("citadel-runtime-event-fanout".to_owned())
            .spawn(move || {
                let mut sequence = 0u64;
                while let Ok(event) = receiver.recv() {
                    sequence = sequence.wrapping_add(1).max(1);
                    let id = crate::runtime::cluster::RuntimeClusterEventId {
                        source_node: source.clone(),
                        sequence,
                    };
                    for peer in &peers {
                        let _ = router.deliver_runtime_event(
                            peer,
                            crate::runtime::cluster::RuntimeClusterEvent {
                                id: id.clone(),
                                event: event.clone(),
                            },
                        );
                    }
                }
            });
        Self { sender }
    }
}

impl RuntimeEventPublisher for ControlRuntimeEventPublisher {
    fn publish(&self, event: RuntimeEvent) {
        let _ = self.sender.try_send(event);
    }
}

#[derive(Clone)]
struct RuntimeCacheLeaseState {
    lease: RuntimeCacheLease,
    local: bool,
}

type RuntimeCacheLeaseSlot = Arc<Mutex<Option<RuntimeCacheLeaseState>>>;

fn runtime_cache_lease_incarnation() -> AppResult<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| {
        AppError::new(
            ErrorCategory::Internal,
            "could not create runtime cache lease incarnation",
        )
        .with_detail(error.to_string())
    })?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

struct ControlRuntimeCachePublisher {
    sender: mpsc::SyncSender<RuntimeCacheWrite>,
}
impl ControlRuntimeCachePublisher {
    fn new(
        router: Arc<TlsMatchmakerHandoffRouter>,
        peers: Vec<NodeId>,
        source: NodeId,
        lease: RuntimeCacheLeaseSlot,
        cache: Arc<crate::runtime::RuntimeSharedCache>,
    ) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<RuntimeCacheWrite>(256);
        let _ = thread::Builder::new()
            .name("citadel-runtime-cache-fanout".to_owned())
            .spawn(move || {
                let mut sequence = 0u64;
                while let Ok(write) = receiver.recv() {
                    if !cache.can_accept_cluster_write(&write) {
                        continue;
                    }
                    let current = lease.lock().ok().and_then(|lease| lease.clone());
                    let Some(current) =
                        current.filter(|state| state.lease.is_current_at(SystemClock.now()))
                    else {
                        continue;
                    };
                    if !current.local {
                        if current.lease.owner_node != source {
                            let _ =
                                router.submit_runtime_cache_write(&current.lease.owner_node, write);
                        }
                        continue;
                    }
                    if current.lease.owner_node != source {
                        continue;
                    }
                    sequence = sequence.wrapping_add(1).max(1);
                    let mutation = crate::runtime::cluster::RuntimeCacheMutation {
                        namespace: write.namespace,
                        key: write.key,
                        value: write.value,
                        expires_at: write.expires_at,
                        fence: crate::runtime::cluster::RuntimeCacheFence {
                            owner_node: source.clone(),
                            generation: current.lease.generation,
                            sequence,
                        },
                    };
                    // Record the same fence locally before publishing it, so a
                    // delayed packet from a previous owner cannot overwrite a
                    // successful local write after a failover.
                    let _ = cache.apply_cluster_mutation(mutation.clone());
                    for peer in &peers {
                        let _ = router.apply_runtime_cache_mutation(peer, mutation.clone());
                    }
                }
            });
        Self { sender }
    }

    fn submit(&self, write: RuntimeCacheWrite) -> bool {
        self.sender.try_send(write).is_ok()
    }
}
impl RuntimeCachePublisher for ControlRuntimeCachePublisher {
    fn set(&self, namespace: String, key: String, value: Vec<u8>, expires_at: TimestampMillis) {
        let _ = self.submit(RuntimeCacheWrite {
            namespace,
            key,
            value: Some(value),
            expires_at,
        });
    }
    fn delete(&self, namespace: String, key: String, expires_at: TimestampMillis) {
        let _ = self.submit(RuntimeCacheWrite {
            namespace,
            key,
            value: None,
            expires_at,
        });
    }
}

/// Renews or resolves the one durable runtime-cache writer lease. If storage
/// is unavailable, publishers stop once their last lease expires instead of
/// continuing to sign mutations with a stale fence.
struct RuntimeCacheLeaseRenewalService {
    directory: StorageRuntimeCacheLeaseDirectory,
    node: NodeId,
    incarnation: String,
    ttl_ms: u64,
    slot: RuntimeCacheLeaseSlot,
}

impl RuntimeCacheLeaseRenewalService {
    fn new(
        directory: StorageRuntimeCacheLeaseDirectory,
        node: NodeId,
        incarnation: String,
        ttl_ms: u64,
        slot: RuntimeCacheLeaseSlot,
    ) -> Self {
        Self {
            directory,
            node,
            incarnation,
            ttl_ms,
            slot,
        }
    }

    async fn refresh(&self) -> AppResult<()> {
        let now = SystemClock.now();
        let expires_at =
            TimestampMillis::from_unix_millis(now.unix_millis().saturating_add(self.ttl_ms));
        let resolution = self
            .directory
            .acquire_or_resolve(self.node.clone(), &self.incarnation, expires_at, now)
            .await?;
        let state = match resolution {
            RuntimeCacheLeaseResolution::Local(lease) => {
                RuntimeCacheLeaseState { lease, local: true }
            }
            RuntimeCacheLeaseResolution::Remote(lease) => RuntimeCacheLeaseState {
                lease,
                local: false,
            },
        };
        if let Ok(mut slot) = self.slot.lock() {
            *slot = Some(state);
        }
        Ok(())
    }
}

impl AsyncService for RuntimeCacheLeaseRenewalService {
    fn name(&self) -> &str {
        "runtime-cache-lease-renewal"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        let mut interval = tokio::time::interval(Duration::from_millis((self.ttl_ms / 2).max(1)));
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    if let Err(error) = self.refresh().await {
                        tracing::warn!(error = %error, "runtime cache lease renewal failed");
                    }
                }
            }
        }
    }
}

impl ChatPresencePublisher for ControlChatPresencePublisher {
    fn advertise(&self, lease: ChatPresenceLease) {
        for peer in &self.peers {
            let _ = self.router.advertise_chat_presence(peer, lease.clone());
        }
    }

    fn withdraw(&self, withdrawal: ChatPresenceWithdrawal) {
        for peer in &self.peers {
            let _ = self.router.withdraw_chat_presence(peer, withdrawal.clone());
        }
    }
}

/// Build a [`Supervisor`] with every transport the app's config enables.
///
/// The returned supervisor shares `cancel`, so cancelling it stops all started
/// transports. Disabled transports are skipped. Returns a
/// [`Transport`](ErrorCategory::Transport) error if an enabled transport cannot
/// be bound.
///
/// All enabled transports share a single [`Gateway`], so a message from a QUIC
/// client is relayed to WebSocket clients and vice versa (one global room).
///
/// QUIC currently uses a generated self-signed development certificate; real
/// certificate provisioning is a later operational task.
pub async fn start_enabled(app: &App, cancel: CancellationToken) -> AppResult<Supervisor> {
    let mut supervisor = Supervisor::with_token(cancel);
    let cfg = &app.config().transport;
    let storage_indexes = app.config().storage.index_definitions()?;
    // Load the embedded game-logic runtime if a `game/` script is
    // present; otherwise the gateway uses its built-in relay.
    // The persisted domain services reachable from game logic (friends and
    // storage host APIs; /).
    let domain_host: Arc<dyn crate::runtime::DomainHost> = Arc::new(
        crate::runtime::ServiceDomainHost::new(
            Arc::clone(app.friends()),
            app.backend().storage_repository(),
        )
        .with_storage_indexes(storage_indexes)
        .with_player_notifications(Arc::clone(app.player_notifications()))
        .with_groups(Arc::clone(app.groups()))
        .with_leaderboards(Arc::clone(app.leaderboards()))
        .with_tournaments(Arc::new(crate::services::TournamentDiscoveryService::new(
            app.backend().tournaments_repository(),
        )))
        .with_chat(Arc::clone(app.chat()))
        .with_chat_authorizer(Arc::new(ChatChannelAuthorizer::with_access_coordinator(
            Arc::clone(app.friends()),
            Arc::clone(app.groups()),
            Arc::clone(app.chat_access()),
        )))
        .with_chat_rate_limits(crate::services::ChatRateLimitPolicy::new(
            app.config().chat.limits.clone(),
        ))
        .with_node_id(app.config().server.node_id.clone())
        .with_wallet(Arc::clone(app.wallet())),
    );
    // Load cooked geometry before the runtime so scripts can inspect it through
    // the read-only map host API.
    let maps = Arc::new(crate::maps::MapCatalog::load_dir(std::path::Path::new(
        &app.config().runtime.maps_dir,
    )));
    tracing::info!(
        maps_dir = %app.config().runtime.maps_dir,
        loaded = maps.len(),
        "map catalog loaded"
    );
    // Build the transform hub before the script runtime so every adapter can
    // install its synchronous `physics_state` read handle. The gateway receives
    // the same shared hub below.
    let transform_hub = build_transform_hub(&cfg.transform_sync);
    // GameScript readiness gate (`runtime.require_script`): the authority is
    // created before the runtime loads so a missing entrypoint boots the node
    // not-ready instead of silently falling back to the relay. `None` keeps
    // ungated deployments byte-identical.
    let script_readiness = app.config().runtime.require_script.then(|| {
        Arc::new(
            crate::runtime::GameScriptReadiness::new(SystemClock.now())
                .with_metrics(Arc::clone(app.metrics())),
        )
    });
    let readiness_source = script_readiness
        .as_ref()
        .map(|authority| crate::runtime::InProcessRuntimeSource::new(Arc::clone(authority)));
    let BuiltRuntime {
        runtime,
        external: external_runtime,
    } = build_runtime_with_readiness(
        app.config(),
        Some(domain_host),
        Arc::clone(&maps),
        transform_hub.as_ref().map(|(hub, _, _)| Arc::clone(hub)),
        Arc::clone(app.runtime_event_bus()),
        Arc::clone(app.runtime_shared_cache()),
        readiness_source.as_ref(),
    )?;
    // Build the realtime authenticator from the node's session service and the
    // configured auth stance: the handshake validates a presented
    // token against this service and binds the resolved account.
    let auth_cfg = &cfg.auth;
    let authenticator = crate::realtime::Authenticator::new(
        Some(Arc::clone(app.session_service())),
        auth_cfg.require_auth,
        auth_cfg.allow_guests,
    );
    let handshake_timeout = auth_cfg.handshake_timeout();
    // Share the node metrics registry so realtime lifecycle moves the dashboard
    // gauges (connections, sessions, messages relayed). See .
    let mut gateway = Gateway::with_metrics_runtime_auth(
        Arc::clone(app.metrics()),
        runtime.clone(),
        authenticator,
    );
    if let Some(readiness) = &script_readiness {
        gateway = gateway.with_script_readiness(Arc::clone(readiness));
    }

    gateway = gateway.with_maps(maps);
    if let Some(rep) = build_network_peer(&cfg.network_peer) {
        gateway = gateway.with_rep_authority(rep);
    }

    // Expose the persisted domain features to game clients via built-in RPC
    // methods (`friends.*`, …; ). Guests are rejected per-call; the
    // caller identity is the authenticated session's account.
    gateway = gateway.with_domain_services(crate::realtime::DomainRpcServices {
        friends: Arc::clone(app.friends()),
        player_notifications: Arc::clone(app.player_notifications()),
        groups: Arc::clone(app.groups()),
        leaderboards: Arc::clone(app.leaderboards()),
        chat: Arc::clone(app.chat()),
        chat_authorizer: Arc::new(
            crate::services::ChatChannelAuthorizer::with_access_coordinator(
                Arc::clone(app.friends()),
                Arc::clone(app.groups()),
                Arc::clone(app.chat_access()),
            ),
        ),
        chat_rate_limits: crate::services::ChatRateLimitPolicy::new(
            app.config().chat.limits.clone(),
        ),
        chat_presence: Arc::new(crate::realtime::ChatPresenceRegistry::new()),
        chat_cluster_presence: None,
        node_id: app.config().server.node_id.clone(),
        wallet: Arc::clone(app.wallet()),
    });

    // Attach the authoritative transform-sync hub when enabled, and
    // spawn any built-in demo movers so the two-client demo works script-free.
    let mut transform_tick_period = None;
    if let Some((hub, sim_period, snapshot_every)) = transform_hub {
        gateway = gateway.with_transform_hub(Arc::clone(&hub));
        transform_tick_period = Some((sim_period, snapshot_every));
    }

    // The durable cross-node matchmaker is deliberately wired before the
    // gateway becomes shared. Its worker owns storage/TLS waits; this function
    // only supplies the operator-configured mTLS identity and endpoint map.
    let mut chat_cluster_directory = None;
    let mut chat_cluster_router = None;
    let mut chat_cluster_node = None;
    let mut runtime_cache_lease_renewal = None;
    if app.config().cluster.enabled {
        let cluster = &app.config().cluster;
        let local_node = NodeId::new(app.config().server.node_id.clone())?;
        let identity = MatchmakerControlIdentity::from_pem_files(
            &cluster.tls.certificate_file,
            &cluster.tls.private_key_file,
        )?;
        let mut trusted_certificates = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        for peer in &cluster.peers {
            let node = NodeId::new(peer.node_id.clone())?;
            trusted_certificates.insert(
                node.clone(),
                read_matchmaker_control_certificate(&peer.certificate_file)?,
            );
            let address = peer.control_addr.parse::<SocketAddr>().map_err(|error| {
                AppError::config("cluster.peers.control_addr must be a socket address")
                    .with_detail(error.to_string())
            })?;
            endpoints.insert(
                node,
                MatchmakerControlEndpoint {
                    address,
                    server_name: peer.server_name.clone(),
                },
            );
        }
        let cluster_ca = read_matchmaker_control_certificate(&cluster.tls.ca_certificate_file)?;
        let router = Arc::new(TlsMatchmakerHandoffRouter::new_with_trust_roots(
            local_node.clone(),
            identity,
            trusted_certificates,
            vec![cluster_ca],
            endpoints,
            Duration::from_millis(cluster.command_timeout_ms),
        )?);
        let cache_lease_directory =
            StorageRuntimeCacheLeaseDirectory::new(app.backend().storage_repository());
        let cache_lease_slot: RuntimeCacheLeaseSlot = Arc::new(Mutex::new(None));
        let cache_lease_incarnation = runtime_cache_lease_incarnation()?;
        let now = SystemClock.now();
        let initial_cache_lease = cache_lease_directory
            .acquire_or_resolve(
                local_node.clone(),
                &cache_lease_incarnation,
                TimestampMillis::from_unix_millis(
                    now.unix_millis().saturating_add(cluster.lease_ttl_ms),
                ),
                now,
            )
            .await?;
        if let Ok(mut slot) = cache_lease_slot.lock() {
            *slot = Some(match initial_cache_lease {
                RuntimeCacheLeaseResolution::Local(lease) => {
                    RuntimeCacheLeaseState { lease, local: true }
                }
                RuntimeCacheLeaseResolution::Remote(lease) => RuntimeCacheLeaseState {
                    lease,
                    local: false,
                },
            });
        }
        let remote_event_bus = Arc::clone(app.runtime_event_bus());
        app.runtime_event_bus()
            .set_publisher(Arc::new(ControlRuntimeEventPublisher::new(
                Arc::clone(&router),
                router.peer_nodes(),
                local_node.clone(),
            )));
        let remote_cache = Arc::clone(app.runtime_shared_cache());
        let cache_publisher = Arc::new(ControlRuntimeCachePublisher::new(
            Arc::clone(&router),
            router.peer_nodes(),
            local_node.clone(),
            Arc::clone(&cache_lease_slot),
            Arc::clone(&remote_cache),
        ));
        app.runtime_shared_cache()
            .set_publisher(cache_publisher.clone());
        let remote_event_dedupe = Arc::new(Mutex::new(
            crate::runtime::cluster::RuntimeClusterDedupe::new(4_096),
        ));
        router.register_runtime_event_handler(Arc::new(move |_source, event| {
            let accepted = remote_event_dedupe
                .lock()
                .map(|mut dedupe| dedupe.accept(event.id))
                .unwrap_or(false);
            accepted
                && matches!(
                    remote_event_bus.emit_remote(event.event),
                    crate::runtime::RuntimeEventEmitOutcome::Queued
                )
        }));
        router.register_runtime_cache_handler(Arc::new(move |_source, mutation| {
            remote_cache.apply_cluster_mutation(mutation)
        }));
        let cache_write_publisher = Arc::clone(&cache_publisher);
        let cache_write_lease = Arc::clone(&cache_lease_slot);
        let cache_for_write_validation = Arc::clone(app.runtime_shared_cache());
        let write_owner = local_node.clone();
        router.register_runtime_cache_write_handler(Arc::new(move |_source, write| {
            cache_for_write_validation.can_accept_cluster_write(&write)
                && cache_write_lease
                    .lock()
                    .ok()
                    .and_then(|lease| lease.clone())
                    .is_some_and(|state| {
                        state.local
                            && state.lease.owner_node == write_owner
                            && state.lease.is_current_at(SystemClock.now())
                    })
                && cache_write_publisher.submit(write)
        }));
        runtime_cache_lease_renewal = Some(RuntimeCacheLeaseRenewalService::new(
            cache_lease_directory,
            local_node.clone(),
            cache_lease_incarnation,
            cluster.lease_ttl_ms,
            cache_lease_slot,
        ));
        // Construct and attach durable party authority before any listener is
        // bound. The gateway retains the local registry only for standalone
        // compatibility; cluster party RPCs use this directory and mTLS router.
        gateway = gateway.with_storage_party_directory(
            Arc::new(StoragePartyDirectory::new(
                app.backend().storage_repository(),
            )),
            local_node.clone(),
            Arc::clone(&router),
        );
        // The directory is intentionally channel/node scoped. The control
        // router authenticates the source before it reaches this handler, and
        // the handler independently checks that an advertised node matches that
        // authenticated source. No participant or socket capability crosses
        // this boundary.
        let chat_directory = Arc::new(ChatPresenceDirectory::default());
        let directory_for_presence = Arc::clone(&chat_directory);
        router.register_chat_presence_handler(Arc::new(move |source, command| match command {
            ChatPresenceCommand::Advertise(lease) if lease.node_id == source => {
                directory_for_presence.advertise(lease, crate::time::SystemClock.now())
            }
            ChatPresenceCommand::Withdraw(withdrawal) if withdrawal.node_id == source => {
                directory_for_presence.withdraw(
                    &withdrawal.channel_id,
                    &withdrawal.node_id,
                    withdrawal.generation,
                )
            }
            _ => crate::chat_cluster::ChatLeaseUpdate::Stale,
        }));
        let chat_presence_publisher = Arc::new(ControlChatPresencePublisher {
            peers: router.peer_nodes(),
            router: Arc::clone(&router),
        });
        gateway = gateway.with_chat_cluster_presence(Arc::new(LocalChatPresenceAnnouncer::new(
            local_node.clone(),
            Arc::clone(&chat_directory),
            chat_presence_publisher,
            cluster.lease_ttl_ms,
        )));
        let live = LiveMatchmakerNode::new(LiveMatchmakerConfig {
            node_id: local_node.clone(),
            shard: QueueShardId::new(cluster.matchmaker_shard),
            lease_ttl: DurationMillis::from_millis(cluster.lease_ttl_ms),
            handoff_ttl: DurationMillis::from_millis(cluster.handoff_ttl_ms),
            command_timeout: Duration::from_millis(cluster.command_timeout_ms),
            directory: StorageMatchmakerLeaseDirectory::new(app.backend().storage_repository()),
            router: Arc::clone(&router),
        })?;
        let control_bind = cluster
            .control_bind
            .parse::<SocketAddr>()
            .map_err(|error| {
                AppError::config("cluster.control_bind must be a socket address")
                    .with_detail(error.to_string())
            })?;
        live.start_listener(control_bind)?;
        gateway = gateway.with_live_matchmaker(live);
        chat_cluster_directory = Some(chat_directory);
        chat_cluster_router = Some(router);
        chat_cluster_node = Some(local_node);
    }

    let gateway = Arc::new(gateway);
    gateway.register_live_matchmaker_endpoint();
    gateway.register_party_directory_endpoint();
    // Under the external-worker adapter the worker's match results land on
    // the gateway: attach it (weakly — the gateway owns the runtime, so a
    // strong reference here would leak the cycle) as the command sink.
    if let Some(external) = &external_runtime {
        external.attach_sink(Arc::downgrade(&gateway)
            as std::sync::Weak<dyn crate::runtime::external_worker::MatchCommandSink>);
    }
    let mut chat_delivery_dispatcher = None;
    if let (Some(directory), Some(router), Some(local_node)) = (
        chat_cluster_directory,
        chat_cluster_router,
        chat_cluster_node,
    ) {
        let gateway = Arc::downgrade(&gateway);
        let delivery_local_node = local_node.clone();
        let delivery_directory = Arc::clone(&directory);
        router.register_chat_delivery_handler(Arc::new(move |_source, delivery| {
            gateway.upgrade().map_or(
                crate::chat_cluster::ChatDeliveryDisposition::Unknown,
                |gateway| {
                    gateway.deliver_remote_chat(&delivery_local_node, &delivery_directory, delivery)
                },
            )
        }));
        let delivery_router = Arc::clone(&router);
        chat_delivery_dispatcher = Some(Arc::new(ChatDeliveryDispatcher::new(
            local_node.clone(),
            app.backend().chat_repository(),
            directory,
            Arc::new(move |destination, delivery| {
                delivery_router
                    .deliver_chat(destination, delivery)
                    .map_err(|_| ())
            }),
        )));
    }

    // A player notification is persisted before this process-local sink is
    // called. A full/disconnected outbound queue merely drops its live attempt;
    // the recipient recovers through the durable inbox.
    let notification_delivery: Arc<dyn crate::services::PlayerNotificationDelivery> =
        gateway.clone();
    app.player_notifications()
        .set_delivery_sink(notification_delivery);

    // Expose the gateway to the HTTP surface (console Matches section reads
    // live room state through this seam; ).
    app.attach_realtime_gateway(Arc::clone(&gateway));

    // The ticket index has a single local leader in this deployment. Keep its
    // 250 ms lifecycle independent from an optional game-script tick so TTL
    // cleanup and formation do not stop when no script registered on_tick.
    if !app.config().cluster.enabled {
        supervisor.spawn(crate::realtime::MatchmakerTickService::new(
            Arc::clone(&gateway),
            std::time::Duration::from_millis(250),
        ));
    } else {
        // Renew well before the exclusive lease deadline. The worker is
        // independent from socket traffic and all remote calls remain bounded
        // control-plane commands.
        let renewal_ms = (app.config().cluster.lease_ttl_ms / 2).max(1);
        supervisor.spawn(crate::realtime::ChatPresenceRenewalService::new(
            Arc::clone(&gateway),
            std::time::Duration::from_millis(renewal_ms),
        ));
        if let Some(dispatcher) = chat_delivery_dispatcher {
            supervisor.spawn(crate::realtime::ChatDeliveryDispatchService::new(
                dispatcher,
                std::time::Duration::from_millis(renewal_ms),
                64,
                64,
            ));
        }
        if let Some(renewal) = runtime_cache_lease_renewal {
            supervisor.spawn(renewal);
        }
    }

    // Spawn the transform-sync snapshot loop when the hub is attached.
    if let Some((sim_period, snapshot_every)) = transform_tick_period {
        tracing::info!(
            send_rate_hz = cfg.transform_sync.send_rate_hz,
            sim_hz = cfg.transform_sync.sim_hz,
            snapshot_every,
            "starting authoritative transform-sync snapshot loop"
        );
        supervisor.spawn(crate::realtime::TransformTickService::new(
            Arc::clone(&gateway),
            sim_period,
            snapshot_every,
        ));
    }

    // One scheduler process is supervised alongside the transports. Its durable
    // backend lease makes this safe across nodes; the runtime bridge invokes the
    // script hook that the selected language adapter actually registered.
    if let Some(runtime) = runtime.as_ref() {
        let callback: Arc<dyn crate::leaderboard_scheduler::LeaderboardResetCallback> = Arc::new(
            crate::leaderboard_scheduler::RuntimeLeaderboardResetCallback::from_runtime(
                Arc::clone(runtime),
            ),
        );
        let scheduler =
            crate::leaderboard_scheduler::LeaderboardResetSchedulerService::from_repositories(
                app.backend().leaderboards_repository(),
                app.leaderboard_reset_repository(),
                callback,
                app.node_id().to_owned(),
                DurationMillis::from_millis(120_000),
                128,
                128,
            );
        supervisor.spawn(
            crate::leaderboard_scheduler::LeaderboardResetSchedulerLoop::new(
                scheduler,
                Duration::from_secs(60),
            ),
        );
    }

    // Spawn the server game-loop tick only when the script actually
    // registered `citadel.on_tick` and the operator enabled a tick rate.
    maybe_spawn_tick(
        &mut supervisor,
        &gateway,
        runtime.as_deref(),
        &app.config().runtime,
    );

    // Spawn the script hot-reload watcher only when opt-in via
    // `runtime.hot_reload` and a reloadable on-disk script is actually loaded.
    maybe_spawn_reload(
        &mut supervisor,
        runtime.as_ref(),
        &app.config().runtime,
        readiness_source.as_ref(),
    );

    // Supervise the external GameScript worker process when the operator
    // opted into the external-worker adapter (unix and windows; config
    // validation rejects it elsewhere). With a present script entrypoint the
    // worker hosts the per-match engine contexts and the lifecycle service
    // installs each generation's data plane into the runtime adapter; the
    // worker's lifecycle (boot, periodic health, restart policy, shutdown)
    // is owned by the same supervisor as every other service either way.
    #[cfg(any(unix, windows))]
    if app.config().runtime.enabled
        && app.config().runtime.adapter == crate::config::RuntimeAdapter::ExternalWorker
    {
        let executable = std::env::current_exe().map_err(|error| {
            AppError::new(
                ErrorCategory::Runtime,
                "failed to resolve the server executable for the runtime worker",
            )
            .with_detail(error.to_string())
        })?;
        tracing::info!(
            executable = %executable.display(),
            "starting supervised external runtime worker"
        );
        // Worker-death closure: when a worker generation ends abruptly, every
        // dependent match closes the same match-local way (members receive
        // the requeue-hinted server-error close, rooms are pruned) before any
        // replacement — which never resumes old matches — starts serving.
        struct CloseMatchesOnGenerationEnd {
            gateway: Arc<crate::realtime::Gateway>,
        }
        impl crate::runtime::worker_supervisor::WorkerGenerationObserver for CloseMatchesOnGenerationEnd {
            fn worker_generation_ended(&self) {
                let notified = self.gateway.close_all_matches();
                tracing::warn!(
                    notified,
                    "external runtime worker generation ended; dependent matches closed"
                );
            }
        }
        let mut service = crate::runtime::worker_supervisor::WorkerLifecycleService::new(
            executable,
            std::env::temp_dir(),
            crate::runtime::worker_supervisor::WorkerSupervisionPolicy::default(),
        )
        .with_generation_observer(std::sync::Arc::new(CloseMatchesOnGenerationEnd {
            gateway: Arc::clone(&gateway),
        }));
        if let Some(external) = &external_runtime {
            service = service.with_data_plane(
                crate::runtime::worker_supervisor::WorkerDataPlaneBridge::new(Arc::clone(external)),
            );
        }
        if let Some(authority) = &script_readiness {
            // Worker boot (WorkerReady.script_identity), health loss, and
            // restart-circuit posture drive the readiness gate.
            service = service.with_readiness(crate::runtime::SupervisedWorkerSource::new(
                Arc::clone(authority),
            ));
        }
        supervisor.spawn(service);
    }

    if cfg.quic.enabled {
        let bind = parse_bind("transport.quic.bind", &cfg.quic.bind)?;
        let cert = if cfg.tls.is_configured() {
            quic::SelfSignedCert::from_pem(
                std::path::Path::new(
                    cfg.tls
                        .certificate_file
                        .as_deref()
                        .expect("validated TLS certificate path"),
                ),
                std::path::Path::new(
                    cfg.tls
                        .private_key_file
                        .as_deref()
                        .expect("validated TLS key path"),
                ),
            )?
        } else {
            quic::SelfSignedCert::generate(&["localhost".to_string()])?
        };
        let server = quic::QuicServer::bind_with_gateway(bind, &cert, Arc::clone(&gateway))?
            .with_handshake_timeout(handshake_timeout);
        tracing::info!(addr = %server.local_addr(), tls = if cfg.tls.is_configured() { "PEM" } else { "development self-signed" }, "starting QUIC transport");
        supervisor.spawn(server);
    }

    if cfg.websocket.enabled {
        let bind = parse_bind("transport.websocket.bind", &cfg.websocket.bind)?;
        let server = websocket::WebSocketServer::bind_with_gateway(bind, Arc::clone(&gateway))
            .await?
            .with_handshake_timeout(handshake_timeout)
            .with_liveness(
                Duration::from_millis(cfg.websocket.heartbeat_interval_ms),
                Duration::from_millis(cfg.websocket.heartbeat_timeout_ms.max(1)),
            );
        tracing::info!(addr = %server.local_addr(), "starting WebSocket fallback transport");
        supervisor.spawn(server);
    }

    if cfg.webtransport.enabled {
        let bind = parse_bind("transport.webtransport.bind", &cfg.webtransport.bind)?;
        let cert = if cfg.tls.is_configured() {
            webtransport::WebTransportDevCert::from_pem(
                std::path::Path::new(
                    cfg.tls
                        .certificate_file
                        .as_deref()
                        .expect("validated TLS certificate path"),
                ),
                std::path::Path::new(
                    cfg.tls
                        .private_key_file
                        .as_deref()
                        .expect("validated TLS key path"),
                ),
            )?
        } else {
            webtransport::WebTransportDevCert::generate(&["localhost".to_string()])?
        };
        let server =
            webtransport::WebTransportServer::bind_with_gateway(bind, &cert, Arc::clone(&gateway))?
                .with_handshake_timeout(handshake_timeout);
        if cfg.tls.is_configured() {
            tracing::info!(addr = %server.local_addr(), "starting WebTransport transport with PEM TLS");
        } else {
            tracing::info!(
                addr = %server.local_addr(),
                cert_sha256_base64 = %server.cert_sha256_base64(),
                "starting WebTransport transport (dev cert; pass cert hash to the browser)"
            );
        }
        supervisor.spawn(server);
    }

    Ok(supervisor)
}

/// Build the production NetworkPeer authority only when explicitly enabled.
/// Class/object registration is intentionally left to trusted server lifecycle
/// code, keeping client frames unable to author schemas or objects.
fn build_network_peer(cfg: &NetworkPeerConfig) -> Option<Arc<RepAuthority>> {
    if !cfg.enabled {
        return None;
    }
    let interest = RepInterestConfig {
        cell_size: cfg.interest_cell_size as f32,
        inner: cfg.interest_inner as f32,
        outer: cfg.interest_outer as f32,
    };
    let authority = RepAuthority::with_interest(RateLimits::default(), interest)
        .with_shared_quantized_state(cfg.shared_quantized_state);
    tracing::info!(
        cell_size = cfg.interest_cell_size,
        inner = cfg.interest_inner,
        outer = cfg.interest_outer,
        shared_quantized_state = cfg.shared_quantized_state,
        "NetworkPeer authority enabled"
    );
    Some(Arc::new(authority))
}

/// Build the authoritative transform-sync hub from config, spawn any
/// built-in demo movers, and return the hub plus the snapshot send period. Returns
/// `None` when transform sync is disabled, or when the negotiated world bounds are
/// degenerate (logged; the rest of the node still starts).
fn build_transform_hub(
    cfg: &TransformSyncConfig,
) -> Option<(Arc<TransformHub>, std::time::Duration, u32)> {
    let sim_period = cfg.sim_period()?;
    let snapshot_every = cfg.snapshot_every();
    let mut hub_cfg = TransformHubConfig {
        budget: cfg.budget,
        sim_dt: cfg.sim_dt(),
        player_slots: cfg.player_slots,
        predicted_authoritative_archetypes: cfg.predicted_authoritative_archetypes.clone(),
        ..TransformHubConfig::default()
    };
    hub_cfg.hello.send_rate_hz = cfg.send_rate_hz.max(1);
    hub_cfg.hello.sim_rate_hz = cfg.sim_hz.max(1);
    let hub = match TransformHub::new(hub_cfg) {
        Ok(hub) => Arc::new(hub),
        Err(e) => {
            tracing::error!(error = %e, "transform-sync disabled: invalid world bounds");
            return None;
        }
    };
    // Player-slot mode takes precedence: the gateway assigns each connecting
    // client an owner-predicted object on `HELLO`, so we spawn no demo movers
    // (they would collide on the same low object ids). Otherwise spawn the
    // built-in server-simulated demo movers (straight-line paths, no game
    // script) so a two-client demo shows smooth interpolation out of the box.
    if cfg.player_slots == 0 {
        for i in 0..cfg.demo_movers {
            let offset = (i as f32) * 200.0;
            let mut state = TransformState::at([offset, 0.0, 0.0]);
            // Alternate direction per mover so they visibly cross.
            let dir = if i % 2 == 0 { 1.0 } else { -1.0 };
            state.velocity = [0.0, dir * 300.0, 0.0];
            hub.spawn_server_simulated(1 + i as u32, state);
        }
    }
    tracing::info!(
        demo_movers = if cfg.player_slots == 0 {
            cfg.demo_movers
        } else {
            0
        },
        player_slots = cfg.player_slots,
        send_rate_hz = cfg.send_rate_hz,
        sim_hz = cfg.sim_hz,
        "transform sync enabled"
    );
    Some((hub, sim_period, snapshot_every))
}

/// Spawn the [`LuaTickService`] when a game loop is both configured and present.
///
/// Requires `runtime.tick_hz > 0` and a loaded script that registered
/// `citadel.on_tick`. When `tick_hz` is set but no handler exists, log once and
/// skip — an operator's tick rate should not spin an empty loop.
fn maybe_spawn_tick(
    supervisor: &mut Supervisor,
    gateway: &Arc<Gateway>,
    runtime: Option<&dyn Runtime>,
    rc: &crate::config::RuntimeConfig,
) {
    let Some(period) = rc.tick_period() else {
        return; // tick_hz == 0: disabled.
    };
    let Some(runtime) = runtime else {
        return; // built-in relay: no script, no tick.
    };
    if !runtime.has_tick_handler() {
        tracing::info!(
            tick_hz = rc.tick_hz,
            "runtime.tick_hz is set but the script has no citadel.on_tick handler; not starting the tick loop"
        );
        return;
    }
    let budget = rc.tick_budget(period);
    tracing::info!(
        tick_hz = rc.tick_hz,
        period_ms = period.as_millis() as u64,
        budget_ms = budget.as_millis() as u64,
        "starting embedded Lua on_tick game loop"
    );
    supervisor.spawn(crate::realtime::LuaTickService::new(
        Arc::clone(gateway),
        period,
        period,
        budget,
    ));
}

/// Spawn the [`LuaReloadService`](crate::realtime::LuaReloadService) when script
/// hot-reload is both opted into and applicable.
///
/// Requires `runtime.hot_reload` and a loaded, on-disk-backed runtime (an
/// in-memory or absent script is not reloadable). The watched path is the same
/// selected entrypoint the runtime loaded. A no-op otherwise, so the off/no-
/// `game/` behavior is unchanged.
fn maybe_spawn_reload(
    supervisor: &mut Supervisor,
    runtime: Option<&Arc<dyn Runtime>>,
    rc: &crate::config::RuntimeConfig,
    readiness: Option<&crate::runtime::InProcessRuntimeSource>,
) {
    let Some(interval) = rc.hot_reload_interval() else {
        return; // hot_reload disabled (the default).
    };
    let Some(runtime) = runtime else {
        return; // built-in relay: no script to watch.
    };
    if !runtime.is_reloadable() {
        return; // runtime has no backing file (should not happen via load()).
    }
    let path = match rc.resolve_selection() {
        Ok(Some(selection)) => selection.entrypoint,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "hot-reload selection changed after startup; not starting watcher"
            );
            return;
        }
    };
    tracing::info!(
        script = %path.display(),
        poll_ms = interval.as_millis() as u64,
        "starting embedded script hot-reload watcher"
    );
    let mut service = crate::realtime::LuaReloadService::new(Arc::clone(runtime), path, interval);
    if let Some(source) = readiness {
        // Successful reloads adopt the new content identity and bump the
        // gate generation; rejected reloads keep the serving script Ready.
        service = service.with_readiness(source.clone());
    }
    supervisor.spawn(service);
}

/// Build the embedded script runtime from config, or `None` for the built-in
/// relay.
///
/// Disabling the runtime, or an enabled runtime with no selected entrypoint,
/// both resolve to `None` (the built-in relay) and are logged once at startup.
/// A present but broken script surfaces as a [`Runtime`](ErrorCategory::Runtime)
/// error so the operator sees a real misconfiguration instead of a silent
/// fallback.
pub(crate) fn validate_runtime_for_check(config: &Config) -> AppResult<()> {
    let _built = build_runtime(
        config,
        None,
        Arc::new(crate::maps::MapCatalog::empty()),
        None,
        Arc::new(crate::runtime::RuntimeEventBus::new(
            crate::runtime::RuntimeEventPolicy::from(&config.runtime.capabilities.events),
            Arc::new(crate::observability::NodeMetrics::new()),
        )),
        Arc::new(crate::runtime::RuntimeSharedCache::new(
            crate::runtime::RuntimeSharedCachePolicy::from(
                &config.runtime.capabilities.shared_cache,
            ),
            Arc::new(crate::observability::NodeMetrics::new()),
        )),
    )?;
    Ok(())
}

/// The runtime selected by `build_runtime`: the gateway-facing trait object
/// plus, under the external-worker adapter, the concrete adapter handle the
/// worker lifecycle wires the data plane into.
struct BuiltRuntime {
    runtime: Option<Arc<dyn Runtime>>,
    external: Option<Arc<crate::runtime::external_worker::ExternalWorkerRuntime>>,
}

impl BuiltRuntime {
    fn none() -> Self {
        Self {
            runtime: None,
            external: None,
        }
    }

    fn embedded(runtime: Arc<dyn Runtime>) -> Self {
        Self {
            runtime: Some(runtime),
            external: None,
        }
    }
}

/// Build the external-worker runtime adapter for a resolved script selection.
///
/// The build must carry the selected engine (single-binary invariant: the
/// worker is this same executable), so a language whose feature is compiled
/// out is a configuration error here, exactly like the embedded loaders.
fn build_external_runtime(
    rc: &crate::config::RuntimeConfig,
    selection: &crate::config::RuntimeSelection,
) -> AppResult<BuiltRuntime> {
    match selection.language {
        RuntimeLanguage::Lua => {}
        #[cfg(feature = "runtime-python")]
        RuntimeLanguage::Python => {}
        #[cfg(not(feature = "runtime-python"))]
        RuntimeLanguage::Python => {
            return Err(AppError::new(
                ErrorCategory::Config,
                format!(
                    "runtime.language 'python' selected from {} but this build was compiled without the 'runtime-python' feature",
                    selection.entrypoint.display()
                ),
            ));
        }
        #[cfg(feature = "runtime-js")]
        RuntimeLanguage::Js => {}
        #[cfg(not(feature = "runtime-js"))]
        RuntimeLanguage::Js => {
            return Err(AppError::new(
                ErrorCategory::Config,
                format!(
                    "runtime.language 'js' selected from {} but this build was compiled without the 'runtime-js' feature",
                    selection.entrypoint.display()
                ),
            ));
        }
    }
    let tick_ms = rc
        .tick_period()
        .map(|period| u64::try_from(period.as_millis()).unwrap_or(u64::MAX).max(1))
        .unwrap_or(crate::runtime::external_worker::DEFAULT_MATCH_ROUND_CADENCE_MS);
    let external = Arc::new(
        crate::runtime::external_worker::ExternalWorkerRuntime::load(
            crate::runtime::external_worker::WorkerScriptSpec {
                language: selection.language,
                entrypoint: selection.entrypoint.clone(),
                deadline_ms: rc.deadline_ms,
                tick_ms,
            },
        )?,
    );
    tracing::info!(
        entrypoint = %selection.entrypoint.display(),
        language = selection.language.as_str(),
        script_identity = external.identity(),
        tick_ms,
        "selected external-worker game runtime"
    );
    Ok(BuiltRuntime {
        runtime: Some(Arc::clone(&external) as Arc<dyn Runtime>),
        external: Some(external),
    })
}

fn build_runtime(
    config: &Config,
    domain: Option<Arc<dyn crate::runtime::DomainHost>>,
    maps: Arc<crate::maps::MapCatalog>,
    transform_hub: Option<Arc<TransformHub>>,
    event_bus: Arc<crate::runtime::RuntimeEventBus>,
    shared_cache: Arc<crate::runtime::RuntimeSharedCache>,
) -> AppResult<BuiltRuntime> {
    build_runtime_with_readiness(
        config,
        domain,
        maps,
        transform_hub,
        event_bus,
        shared_cache,
        None,
    )
}

/// [`build_runtime`] plus GameScript readiness reporting.
///
/// With a `readiness` source attached (`runtime.require_script`), a missing or
/// disappeared entrypoint records `NoScript` — the node boots **not-ready**
/// and refuses match surfaces instead of silently serving the relay — and a
/// successful load records the entrypoint's content identity as `Ready`. A
/// present-but-broken script remains the same hard startup error as before.
/// The external-worker adapter returns before the in-process recording: its
/// readiness is driven by the supervised worker's boot
/// (`WorkerReady.script_identity`) via `SupervisedWorkerSource`, so the node
/// boots not-ready and opens the gate only when the worker reports in.
#[allow(clippy::too_many_arguments)]
fn build_runtime_with_readiness(
    config: &Config,
    domain: Option<Arc<dyn crate::runtime::DomainHost>>,
    maps: Arc<crate::maps::MapCatalog>,
    transform_hub: Option<Arc<TransformHub>>,
    event_bus: Arc<crate::runtime::RuntimeEventBus>,
    shared_cache: Arc<crate::runtime::RuntimeSharedCache>,
    readiness: Option<&crate::runtime::InProcessRuntimeSource>,
) -> AppResult<BuiltRuntime> {
    let rc = &config.runtime;
    if !rc.enabled {
        // Config validation rejects require_script without an enabled
        // runtime, so this arm is always ungated.
        tracing::info!("embedded runtime disabled; using the built-in relay");
        return Ok(BuiltRuntime::none());
    }
    if rc.lua_execution_mode == LuaExecutionMode::Trusted {
        tracing::warn!(
            scripts_dir = %rc.scripts_dir,
            "Lua trusted mode is enabled: game scripts have full access to the extended Lua standard library and can access this machine"
        );
    }
    let Some(selection) = rc.resolve_selection()? else {
        if let Some(source) = readiness {
            // Never a silent relay under require_script: the node boots
            // not-ready and refuses match surfaces until a script loads.
            source.record_missing_entrypoint(SystemClock.now());
            tracing::warn!(
                scripts_dir = %rc.scripts_dir,
                "runtime.require_script is enabled but no game entrypoint was found; booting NOT READY — matches are refused until a valid script loads"
            );
        } else if let Some(language) = rc.language {
            tracing::info!(
                scripts_dir = %rc.scripts_dir,
                language = language.as_str(),
                "runtime language configured but no matching entrypoint found; using the built-in relay fallback"
            );
        } else {
            tracing::info!(
                scripts_dir = %rc.scripts_dir,
                "no game entrypoint found; using the built-in relay fallback"
            );
        }
        return Ok(BuiltRuntime::none());
    };
    if selection.adapter == crate::config::RuntimeAdapter::ExternalWorker {
        return build_external_runtime(rc, &selection);
    }
    tracing::info!(
        scripts_dir = %rc.scripts_dir,
        entrypoint = %selection.entrypoint.display(),
        language = selection.language.as_str(),
        adapter = selection.adapter.as_str(),
        tier = selection.tier.as_str(),
        source = selection.source.as_str(),
        "selected embedded game runtime"
    );
    let built = match selection.language {
        RuntimeLanguage::Lua => {
            match LuaRuntime::load_with_static_data_and_mode_and_capability_policies(
                Path::new(&rc.scripts_dir),
                rc.deadline_ms,
                rc.static_data_dir.as_deref().map(Path::new),
                rc.static_data_max_file_bytes,
                rc.lua_execution_mode,
                crate::runtime::outbound_http::OutboundHttpPolicy::from(
                    &rc.capabilities.outbound_http,
                ),
                crate::runtime::RuntimeHttpEndpointPolicy::from(
                    &rc.capabilities.custom_http_endpoints,
                ),
            )? {
                Some(runtime) => {
                    tracing::info!(
                        scripts_dir = %rc.scripts_dir,
                        deadline_ms = rc.deadline_ms,
                        lua_execution_mode = rc.lua_execution_mode.as_str(),
                        "loaded embedded Lua game runtime"
                    );
                    // Expose the persisted domain features (friends, …) to game logic
                    // when services are attached.
                    let runtime = match domain {
                        Some(host) => runtime.with_domain_host(host),
                        None => runtime,
                    }
                    .with_maps(maps)
                    .with_event_bus(Arc::clone(&event_bus))
                    .with_shared_cache(Arc::clone(&shared_cache));
                    let runtime = match transform_hub {
                        Some(hub) => runtime.with_transform_hub(hub),
                        None => runtime,
                    };
                    let runtime: Arc<dyn Runtime> = Arc::new(runtime);
                    Ok(BuiltRuntime::embedded(runtime))
                }
                None => {
                    tracing::info!(
                        scripts_dir = %rc.scripts_dir,
                        "selected Lua entrypoint disappeared before load; using the built-in relay fallback"
                    );
                    Ok(BuiltRuntime::none())
                }
            }
        }
        RuntimeLanguage::Python => Ok(
            match load_python_runtime(rc, domain, maps, transform_hub, event_bus, shared_cache)? {
                Some(runtime) => BuiltRuntime::embedded(runtime),
                None => BuiltRuntime::none(),
            },
        ),
        RuntimeLanguage::Js => Ok(
            match load_js_runtime(rc, domain, maps, transform_hub, event_bus, shared_cache)? {
                Some(runtime) => BuiltRuntime::embedded(runtime),
                None => BuiltRuntime::none(),
            },
        ),
    }?;
    if let Some(source) = readiness {
        match &built.runtime {
            // The gate opens on the loaded entrypoint's content identity —
            // the in-process analogue of `WorkerReady.script_identity`.
            Some(_) => source.record_initial_load(&selection.entrypoint, SystemClock.now()),
            // One of the per-language "entrypoint disappeared before load"
            // arms fired: not-ready, never a silent relay.
            None => {
                source.record_missing_entrypoint(SystemClock.now());
                tracing::warn!(
                    scripts_dir = %rc.scripts_dir,
                    entrypoint = %selection.entrypoint.display(),
                    "runtime.require_script is enabled and the selected entrypoint disappeared before load; booting NOT READY — matches are refused until a valid script loads"
                );
            }
        }
    }
    Ok(built)
}

#[cfg(feature = "runtime-python")]
fn load_python_runtime(
    rc: &crate::config::RuntimeConfig,
    domain: Option<Arc<dyn crate::runtime::DomainHost>>,
    maps: Arc<crate::maps::MapCatalog>,
    transform_hub: Option<Arc<TransformHub>>,
    event_bus: Arc<crate::runtime::RuntimeEventBus>,
    shared_cache: Arc<crate::runtime::RuntimeSharedCache>,
) -> AppResult<Option<Arc<dyn Runtime>>> {
    match PythonRuntime::load_with_static_data_and_capability_policies(
        Path::new(&rc.scripts_dir),
        rc.deadline_ms,
        rc.static_data_dir.as_deref().map(Path::new),
        rc.static_data_max_file_bytes,
        crate::runtime::outbound_http::OutboundHttpPolicy::from(&rc.capabilities.outbound_http),
        crate::runtime::RuntimeHttpEndpointPolicy::from(&rc.capabilities.custom_http_endpoints),
    )? {
        Some(runtime) => {
            tracing::info!(
                scripts_dir = %rc.scripts_dir,
                deadline_ms = rc.deadline_ms,
                "loaded embedded Python game runtime"
            );
            let runtime = match domain {
                Some(host) => runtime.with_domain_host(host),
                None => runtime,
            }
            .with_maps(maps)
            .with_event_bus(event_bus)
            .with_shared_cache(shared_cache);
            let runtime = match transform_hub {
                Some(hub) => runtime.with_transform_hub(hub),
                None => runtime,
            };
            let runtime: Arc<dyn Runtime> = Arc::new(runtime);
            Ok(Some(runtime))
        }
        None => {
            tracing::info!(
                scripts_dir = %rc.scripts_dir,
                "selected Python entrypoint disappeared before load; using the built-in relay fallback"
            );
            Ok(None)
        }
    }
}

#[cfg(not(feature = "runtime-python"))]
fn load_python_runtime(
    rc: &crate::config::RuntimeConfig,
    _domain: Option<Arc<dyn crate::runtime::DomainHost>>,
    _maps: Arc<crate::maps::MapCatalog>,
    _transform_hub: Option<Arc<TransformHub>>,
    _event_bus: Arc<crate::runtime::RuntimeEventBus>,
    _shared_cache: Arc<crate::runtime::RuntimeSharedCache>,
) -> AppResult<Option<Arc<dyn Runtime>>> {
    Err(AppError::new(
        ErrorCategory::Config,
        format!(
            "runtime.language 'python' selected from {}/{} but this build was compiled without the 'runtime-python' feature",
            rc.scripts_dir, "main.py"
        ),
    ))
}

#[cfg(feature = "runtime-js")]
fn load_js_runtime(
    rc: &crate::config::RuntimeConfig,
    domain: Option<Arc<dyn crate::runtime::DomainHost>>,
    maps: Arc<crate::maps::MapCatalog>,
    transform_hub: Option<Arc<TransformHub>>,
    event_bus: Arc<crate::runtime::RuntimeEventBus>,
    shared_cache: Arc<crate::runtime::RuntimeSharedCache>,
) -> AppResult<Option<Arc<dyn Runtime>>> {
    match JsRuntime::load_with_static_data_and_capability_policies(
        Path::new(&rc.scripts_dir),
        rc.deadline_ms,
        rc.static_data_dir.as_deref().map(Path::new),
        rc.static_data_max_file_bytes,
        crate::runtime::outbound_http::OutboundHttpPolicy::from(&rc.capabilities.outbound_http),
        crate::runtime::RuntimeHttpEndpointPolicy::from(&rc.capabilities.custom_http_endpoints),
    )? {
        Some(runtime) => {
            tracing::info!(
                scripts_dir = %rc.scripts_dir,
                deadline_ms = rc.deadline_ms,
                "loaded embedded QuickJS game runtime"
            );
            let runtime = match domain {
                Some(host) => runtime.with_domain_host(host),
                None => runtime,
            }
            .with_maps(maps)
            .with_event_bus(event_bus)
            .with_shared_cache(shared_cache);
            let runtime = match transform_hub {
                Some(hub) => runtime.with_transform_hub(hub),
                None => runtime,
            };
            let runtime: Arc<dyn Runtime> = Arc::new(runtime);
            Ok(Some(runtime))
        }
        None => {
            tracing::info!(
                scripts_dir = %rc.scripts_dir,
                "selected JavaScript entrypoint disappeared before load; using the built-in relay fallback"
            );
            Ok(None)
        }
    }
}

#[cfg(not(feature = "runtime-js"))]
fn load_js_runtime(
    rc: &crate::config::RuntimeConfig,
    _domain: Option<Arc<dyn crate::runtime::DomainHost>>,
    _maps: Arc<crate::maps::MapCatalog>,
    _transform_hub: Option<Arc<TransformHub>>,
    _event_bus: Arc<crate::runtime::RuntimeEventBus>,
    _shared_cache: Arc<crate::runtime::RuntimeSharedCache>,
) -> AppResult<Option<Arc<dyn Runtime>>> {
    Err(AppError::new(
        ErrorCategory::Config,
        format!(
            "runtime.language 'js' selected from {}/{} but this build was compiled without the 'runtime-js' feature",
            rc.scripts_dir, "main.js"
        ),
    ))
}

/// Parse a transport bind address, mapping failure to a `Config` error.
fn parse_bind(field: &str, value: &str) -> AppResult<SocketAddr> {
    value.parse().map_err(|e: std::net::AddrParseError| {
        AppError::new(
            ErrorCategory::Config,
            format!("{field} is not a valid socket address: {value}"),
        )
        .with_detail(e.to_string())
    })
}

/// The family of a transport, used for metrics labels, logs, and selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportKind {
    /// QUIC: unreliable datagrams + reliable streams, TLS 1.3 (primary).
    Quic,
    /// WebTransport over QUIC: browser action-client path.
    WebTransport,
    /// WebSocket over TCP: fallback and lobby/control.
    WebSocket,
}

impl TransportKind {
    /// Stable lowercase token for metrics labels and structured logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quic => "quic",
            Self::WebTransport => "webtransport",
            Self::WebSocket => "websocket",
        }
    }

    /// Whether this transport offers an unreliable datagram delivery mode.
    ///
    /// QUIC and WebTransport expose unreliable datagrams for hot-path game
    /// state; WebSocket is reliable-only.
    #[must_use]
    pub const fn supports_unreliable(self) -> bool {
        matches!(self, Self::Quic | Self::WebTransport)
    }
}

impl fmt::Display for TransportKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Delivery mode requested for an outbound [`Envelope`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// Best-effort, may be dropped or reordered (QUIC datagram). Hot path.
    Unreliable,
    /// Reliable and ordered (QUIC stream / WebSocket). Control path.
    Reliable,
}

impl Delivery {
    /// The default outbound-queue overflow policy for this delivery mode.
    ///
    /// Unreliable traffic drops the oldest item (latest-wins); reliable traffic
    /// closes the connection rather than silently lose ordered data.
    #[must_use]
    pub const fn overflow_policy(self) -> OverflowPolicy {
        match self {
            Self::Unreliable => OverflowPolicy::DropOldest,
            Self::Reliable => OverflowPolicy::CloseOnFull,
        }
    }
}

/// A process-unique identifier for an accepted transport connection.
///
/// Monotonic within a process; not stable across restarts and not a session id.
/// Session binding is layered above the transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

impl ConnectionId {
    /// The raw numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "conn-{}", self.0)
    }
}

/// Allocator for process-unique [`ConnectionId`] values.
#[derive(Debug, Default)]
pub struct ConnectionIdGen {
    next: AtomicU64,
}

impl ConnectionIdGen {
    /// Create a fresh generator starting at 1.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate the next connection id.
    pub fn next_id(&self) -> ConnectionId {
        ConnectionId(self.next.fetch_add(1, Ordering::Relaxed))
    }
}

/// The remote peer address of a connection.
///
/// Wraps a [`SocketAddr`] but is a distinct type so call sites are explicit and
/// so future non-IP transports can extend it without changing call signatures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerAddr(SocketAddr);

impl PeerAddr {
    /// Wrap a socket address as a peer address.
    #[must_use]
    pub const fn new(addr: SocketAddr) -> Self {
        Self(addr)
    }

    /// The underlying socket address.
    #[must_use]
    pub const fn socket_addr(self) -> SocketAddr {
        self.0
    }
}

impl fmt::Display for PeerAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SocketAddr> for PeerAddr {
    fn from(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

/// An accepted, identified transport connection.
///
/// Concrete transports implement this for their connection handle. The trait is
/// deliberately small: identity, peer, transport kind, and whether the
/// connection can carry unreliable datagrams. Send/receive APIs are added per
/// transport because their async shapes differ (datagram vs stream); the
/// shared, testable contract is identity and capability.
pub trait Connection {
    /// Process-unique connection id.
    fn id(&self) -> ConnectionId;

    /// Remote peer address.
    fn peer_addr(&self) -> PeerAddr;

    /// Transport family.
    fn transport_kind(&self) -> TransportKind;

    /// Whether unreliable datagram delivery is available on this connection.
    fn supports_unreliable(&self) -> bool {
        self.transport_kind().supports_unreliable()
    }
}

/// A bound listener for a transport.
///
/// Concrete transports implement the async `accept` loop themselves; this trait
/// captures the shared, synchronous identity used by the bootstrap layer and
/// observability: which transport family and which local address.
pub trait Listener {
    /// Transport family this listener serves.
    fn transport_kind(&self) -> TransportKind;

    /// Local socket address the listener is bound to.
    fn local_addr(&self) -> SocketAddr;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn transport_kind_tokens_are_stable() {
        assert_eq!(TransportKind::Quic.as_str(), "quic");
        assert_eq!(TransportKind::WebTransport.as_str(), "webtransport");
        assert_eq!(TransportKind::WebSocket.as_str(), "websocket");
    }

    #[test]
    fn only_quic_family_supports_unreliable() {
        assert!(TransportKind::Quic.supports_unreliable());
        assert!(TransportKind::WebTransport.supports_unreliable());
        assert!(!TransportKind::WebSocket.supports_unreliable());
    }

    #[test]
    fn delivery_maps_to_expected_overflow_policy() {
        assert_eq!(
            Delivery::Unreliable.overflow_policy(),
            OverflowPolicy::DropOldest
        );
        assert_eq!(
            Delivery::Reliable.overflow_policy(),
            OverflowPolicy::CloseOnFull
        );
    }

    #[test]
    fn connection_ids_are_monotonic_and_unique() {
        let id_gen = ConnectionIdGen::new();
        let a = id_gen.next_id();
        let b = id_gen.next_id();
        assert_ne!(a, b);
        assert_eq!(a.get() + 1, b.get());
        assert_eq!(a.to_string(), "conn-1");
    }

    #[test]
    fn peer_addr_round_trips_socket_addr() {
        let addr = loopback(7350);
        let peer = PeerAddr::new(addr);
        assert_eq!(peer.socket_addr(), addr);
        assert_eq!(PeerAddr::from(addr), peer);
        assert_eq!(peer.to_string(), "127.0.0.1:7350");
    }

    // A minimal concrete Connection to exercise the trait's default method.
    struct StubConn {
        id: ConnectionId,
        peer: PeerAddr,
        kind: TransportKind,
    }

    impl Connection for StubConn {
        fn id(&self) -> ConnectionId {
            self.id
        }
        fn peer_addr(&self) -> PeerAddr {
            self.peer
        }
        fn transport_kind(&self) -> TransportKind {
            self.kind
        }
    }

    #[test]
    fn connection_default_unreliable_follows_transport_kind() {
        let id_gen = ConnectionIdGen::new();
        let quic = StubConn {
            id: id_gen.next_id(),
            peer: PeerAddr::new(loopback(1)),
            kind: TransportKind::Quic,
        };
        let ws = StubConn {
            id: id_gen.next_id(),
            peer: PeerAddr::new(loopback(2)),
            kind: TransportKind::WebSocket,
        };
        assert!(quic.supports_unreliable());
        assert!(!ws.supports_unreliable());
    }
}
