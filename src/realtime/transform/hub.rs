//! `TransformHub`: the gateway-facing owner of the transform world, the per-client
//! snapshot builders, and the negotiated codec.
//!
//! The hub is transport-agnostic (like the [`Gateway`](crate::realtime::Gateway)
//! itself): it does not touch sockets. It owns the **sim tick** (advance + latch),
//! the **snapshot tick** (build one delta snapshot per client from the latched
//! frame), and the `HELLO`/`ACK` control handling; the gateway pumps its output
//! envelopes through the [`SessionRegistry`](crate::realtime::SessionRegistry)
//! with the right delivery mode (snapshots unreliable, hello/role reliable).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use citadel_map::CollisionMesh;
use citadel_physics::{GroundHit, RaycastHit, StaticTriBvh};
use citadel_wire::na::{NaSpawn, NaSpawnBatch, NaTransform};
use citadel_wire::tsync::{self, Hello, InputBundle, RewindResult, TransformCodec};

use crate::realtime::tick::{GameplayClock, GameplayClockSnapshot};
use crate::runtime::PhysicsOptions;

use super::ObjectId;
use super::authority::{TransformAuthority, TransformState};
use super::congestion::{CongestionConfig, CongestionController, CongestionSignals};
use super::rewind::{
    self, HitRay, HitTarget, LagProfile, RewindConfig, compute_rewind_tick, lag_comp_enabled,
};
use super::snapshot::ClientSnapshotState;
use super::world::{Frame, PhysicsState, TransformWorld};

/// Static configuration for a [`TransformHub`].
#[derive(Debug, Clone)]
pub struct TransformHubConfig {
    /// Server-advertised negotiation params (world bounds, precision, rates).
    pub hello: Hello,
    /// Interest-grid cell size (world units).
    pub cell_size: f32,
    /// AOI inner (enter) radius.
    pub aoi_inner: f32,
    /// AOI outer (exit) radius.
    pub aoi_outer: f32,
    /// Max object updates per snapshot (QUIC owns byte pacing; this is the
    /// application budget/priority knob — design §6.5). `0` = unbounded; used as a
    /// hard cap on top of the per-client adaptive budget.
    pub budget: usize,
    /// Seconds advanced per sim tick.
    pub sim_dt: f32,
    /// Lag-compensation config (rewind clamp, RTT cutoff, capsule radius, §5.2).
    pub rewind: RewindConfig,
    /// Adaptive-congestion config (two-mode send rate + budget, §6.5).
    pub congestion: CongestionConfig,
    /// Client-owned player-object pool size (`0` = disabled). When `> 0`, the
    /// gateway hands each connecting client ownership of one object from the id
    /// range `1..=player_slots` via [`TransformHub::assign_player_slot`].
    pub player_slots: u32,
    /// Networked-Actor archetypes that use the existing owner-input pipeline
    /// rather than client-authoritative `KIND_NA_STATE` relay. Unlisted
    /// archetypes remain Relay for backwards compatibility.
    pub predicted_authoritative_archetypes: Vec<u16>,
}

impl Default for TransformHubConfig {
    fn default() -> Self {
        Self {
            hello: Hello::default(),
            // A generous cell + AOI so the default demo sees the whole small world.
            cell_size: 5000.0,
            aoi_inner: 1_000_000.0,
            aoi_outer: 1_000_000.0,
            // A full baseline of 16 current object updates fits under the
            // conservative 1,200-byte QUIC datagram payload budget.
            budget: 16,
            sim_dt: 1.0 / 60.0,
            rewind: RewindConfig::default(),
            congestion: CongestionConfig::default(),
            player_slots: 0,
            predicted_authoritative_archetypes: Vec::new(),
        }
    }
}

struct ClientEntry {
    builder: ClientSnapshotState,
    viewer_pos: [f32; 3],
    /// Per-client adaptive congestion controller (design §6.5).
    congestion: CongestionController,
    /// Server-measured latency used to compute rewind time (design §5.2).
    lag: LagProfile,
    /// Set only after a valid dedicated v2 manifest. v1 remains the default.
    v2_clock: bool,
}

/// The owner-movement authority selected by trusted server configuration for a
/// Networked-Actor archetype. It is deliberately not a client-supplied field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerMovementMode {
    /// Legacy/default movement: the owner reports raw transforms via
    /// `KIND_NA_STATE`; suitable for prototypes and co-op.
    Relay,
    /// The owner sends sequenced `KIND_TSYNC_INPUT` frames, which the server
    /// validates, applies, and acknowledges in snapshots for reconciliation.
    PredictedAuthoritative,
}

/// A participant's registered networked-actor presence.
struct NaEntry {
    object_id: ObjectId,
    archetype_id: u16,
    mode: OwnerMovementMode,
}

struct HubInner {
    world: TransformWorld,
    gameplay_clock: GameplayClock,
    latest: Arc<Frame>,
    /// Per-loaded-map collision broadphases. Building is command/map-change work,
    /// never simulation-tick work.
    physics_bvhs: HashMap<String, Arc<StaticTriBvh>>,
    clients: HashMap<u64, ClientEntry>,
    /// Participant -> the player object id it owns, when running in player-slot
    /// mode (`config.player_slots > 0`). Empty otherwise.
    assigned_slots: HashMap<u64, ObjectId>,
    /// Participant -> its networked-actor presence (relay mode). Populated when a
    /// client announces `KIND_NA_PRESENCE`; empty otherwise.
    na_presence: HashMap<u64, NaEntry>,
    /// Server-owned object -> authoritative room. Absent entries are legacy
    /// node-global objects, retained for relay compatibility.
    object_rooms: HashMap<ObjectId, u64>,
    /// Aggregate-only v2 input-hint diagnostics. These intentionally carry no
    /// participant, epoch, tick, or hint values, so telemetry cannot become an
    /// input-derived label/cardinality sink.
    input_hint_metrics: InputHintMetrics,
}

/// Bounded aggregate diagnostics for untrusted v2 input hints.
///
/// A syntactically valid, epoch-fenced hint is both `accepted` at the wire
/// boundary and `ignored` by authority: its values never influence simulation,
/// scheduling, authorization, latency, or rewind. `rejected` covers malformed,
/// unnegotiated, and stale-epoch input without exposing a reason to clients.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InputHintMetrics {
    pub accepted: u64,
    pub rejected: u64,
    pub ignored: u64,
}

/// The frames a [`TransformHub::register_presence`] produces for the gateway to
/// deliver (all reliable). See the method for the ordering guarantee.
pub struct PresenceRegistration {
    /// The object id assigned to the newcomer.
    pub object_id: ObjectId,
    /// The newcomer's own spawn — send this to the owner **first** so it learns
    /// its object id (and latches its participant id from `owner`).
    pub self_spawn: NaSpawn,
    /// Every other actor already present — send to the newcomer after `self_spawn`.
    pub batch: NaSpawnBatch,
    /// The newcomer's spawn to broadcast to every participant in `peers`.
    pub peer_spawn: NaSpawn,
    /// Participants that must receive `peer_spawn` (everyone present but the
    /// newcomer).
    pub peers: Vec<u64>,
    /// Reliable role assignment for a predicted-authoritative owner. It must be
    /// delivered after `self_spawn`, so the engine has bound its local actor to
    /// `object_id` before the existing transform component receives the role.
    pub owner_role: Option<tsync::Role>,
}

fn na_to_state(t: NaTransform) -> TransformState {
    TransformState {
        position: t.position,
        rotation: t.rotation,
        velocity: t.velocity,
    }
}

fn state_to_na(s: TransformState) -> NaTransform {
    NaTransform {
        position: s.position,
        rotation: s.rotation,
        velocity: s.velocity,
    }
}

/// One outbound transform-sync frame the gateway must deliver.
#[derive(Debug, Clone)]
pub struct HubOutbound {
    /// Target participant (raw id).
    pub participant: u64,
    /// Envelope kind (`KIND_TSYNC_*`).
    pub kind: u16,
    /// Encoded body.
    pub body: Vec<u8>,
    /// `true` for the unreliable snapshot hot path, `false` for control frames.
    pub unreliable: bool,
}

/// The transform-sync hub. Cheap to clone the handle via `Arc<TransformHub>`.
pub struct TransformHub {
    codec: TransformCodec,
    config: TransformHubConfig,
    inner: Mutex<HubInner>,
}

impl std::fmt::Debug for TransformHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransformHub")
            .field("codec", &self.codec)
            .finish_non_exhaustive()
    }
}

impl TransformHub {
    /// Build a hub from `config`, deriving the shared codec from the negotiated
    /// [`Hello`]. Fails only if the world bounds are degenerate.
    pub fn new(config: TransformHubConfig) -> Result<Self, tsync::TsyncError> {
        let codec = TransformCodec::from_hello(&config.hello)?;
        let gameplay_tick_hz = u16::from(config.hello.sim_rate_hz.max(1));
        let mut world = TransformWorld::new(config.cell_size);
        world.set_bounds(config.hello.position_bounds);
        // ~1 s of rewind history at the negotiated sim rate.
        world.set_rewind_capacity_ticks(usize::from(config.hello.sim_rate_hz.max(1)));
        let latest = world.latch();
        Ok(Self {
            codec,
            config,
            inner: Mutex::new(HubInner {
                world,
                gameplay_clock: GameplayClock::try_new(gameplay_tick_hz).ok_or(
                    tsync::TsyncError::OutOfRange("gameplay clock epoch exhausted"),
                )?,
                latest,
                physics_bvhs: HashMap::new(),
                clients: HashMap::new(),
                assigned_slots: HashMap::new(),
                na_presence: HashMap::new(),
                object_rooms: HashMap::new(),
                input_hint_metrics: InputHintMetrics::default(),
            }),
        })
    }

    /// The server's negotiation body to send in reply to a client `HELLO`.
    #[must_use]
    pub fn hello_body(&self) -> Vec<u8> {
        self.config.hello.encode()
    }

    /// The negotiated codec (mirrors the client's).
    #[must_use]
    pub fn codec(&self) -> &TransformCodec {
        &self.codec
    }

    /// Spawn (or replace) an authority.
    pub fn spawn(&self, authority: TransformAuthority) {
        if let Ok(mut g) = self.inner.lock() {
            g.world.spawn(authority);
        }
    }

    /// Spawn a server-simulated, velocity-replicated object (demo/tests helper).
    pub fn spawn_server_simulated(&self, id: ObjectId, state: TransformState) {
        if let Ok(mut g) = self.inner.lock() {
            g.world.spawn_server_simulated(id, state);
        }
    }

    /// Authoritatively set an object's transform (the `ServerSimulated` write
    /// path, e.g. from a Lua host call). Creates the object if absent.
    pub fn set_transform(&self, id: ObjectId, state: TransformState) {
        if let Ok(mut g) = self.inner.lock() {
            g.world.set_transform(id, state);
        }
    }

    /// Read an object's authoritative transform.
    #[must_use]
    pub fn get_transform(&self, id: ObjectId) -> Option<TransformState> {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.world.get_transform(id))
    }

    /// Select the collision mesh for physics bodies in the active transform
    /// world. A BVH is built once per map key and then reused for later
    /// selections; `None` keeps bodies in free-fall mode.
    pub fn set_physics_map(&self, map: Option<(&str, &CollisionMesh)>) {
        if let Ok(mut g) = self.inner.lock() {
            match map {
                Some((map_name, collision)) => {
                    let bvh = Arc::clone(
                        g.physics_bvhs
                            .entry(map_name.to_owned())
                            .or_insert_with(|| Arc::new(StaticTriBvh::new(collision))),
                    );
                    g.world.set_physics_bvh(Some(bvh));
                }
                None => g.world.set_physics_bvh(None),
            }
        }
    }

    /// Attach, reconfigure, or detach a server-simulated actor's physics body.
    /// `None` and disabled options detach the body.
    pub fn set_physics(&self, id: ObjectId, opts: Option<PhysicsOptions>) {
        if let Ok(mut g) = self.inner.lock() {
            let config = opts.and_then(|opts| opts.enabled.then_some(opts.config));
            g.world.set_physics(id, config);
        }
    }

    /// Add an instantaneous velocity change to a bodied server-simulated actor.
    pub fn apply_impulse(&self, id: ObjectId, impulse: [f32; 3]) {
        if let Ok(mut g) = self.inner.lock() {
            g.world.apply_impulse(id, impulse);
        }
    }

    /// Set a bodied server-simulated actor's desired control velocity.
    pub fn set_move_intent(&self, id: ObjectId, intent: [f32; 3]) {
        if let Ok(mut g) = self.inner.lock() {
            g.world.set_move_intent(id, intent);
        }
    }

    /// Read the current state of a bodied actor.
    #[must_use]
    pub fn physics_state(&self, id: ObjectId) -> Option<PhysicsState> {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.world.physics_state(id))
    }

    /// Cast a finite ray against the active room map's collision mesh.
    #[must_use]
    pub fn raycast(&self, origin: [f32; 3], direction: [f32; 3]) -> Option<RaycastHit> {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.world.raycast(origin, direction))
    }

    /// Test a sphere against the active room map's collision mesh.
    #[must_use]
    pub fn sphere_overlap(&self, centre: [f32; 3], radius: f32) -> bool {
        self.inner
            .lock()
            .ok()
            .is_some_and(|g| g.world.sphere_overlap(centre, radius))
    }

    /// Find a walkable static-map surface below `origin` in the active room.
    #[must_use]
    pub fn ground_height(&self, origin: [f32; 3], max_distance: f32) -> Option<GroundHit> {
        self.inner
            .lock()
            .ok()
            .and_then(|g| g.world.ground_height(origin, max_distance))
    }

    /// Despawn an object.
    pub fn despawn(&self, id: ObjectId) {
        if let Ok(mut g) = self.inner.lock() {
            g.world.despawn(id);
            g.object_rooms.remove(&id);
        }
    }

    /// Associate a server-owned object with its authoritative room. `None`
    /// restores the legacy node-global visibility used by relay-only deployments.
    pub fn set_object_room(&self, id: ObjectId, room_id: Option<u64>) {
        if let Ok(mut g) = self.inner.lock() {
            match room_id {
                Some(room_id) => {
                    g.object_rooms.insert(id, room_id);
                }
                None => {
                    g.object_rooms.remove(&id);
                }
            }
        }
    }

    /// Set where a client observes the world from (its avatar/camera). Defaults
    /// to the origin until set.
    pub fn set_viewer(&self, participant: u64, pos: [f32; 3]) {
        if let Ok(mut g) = self.inner.lock()
            && let Some(c) = g.clients.get_mut(&participant)
        {
            c.viewer_pos = pos;
        }
    }

    /// Register a client for transform sync (on its `HELLO`). Idempotent.
    pub fn register_client(&self, participant: u64) {
        if let Ok(mut g) = self.inner.lock() {
            let congestion_config = self.config.congestion;
            g.clients.entry(participant).or_insert_with(|| ClientEntry {
                builder: ClientSnapshotState::new(self.config.aoi_inner, self.config.aoi_outer),
                viewer_pos: [0.0; 3],
                congestion: CongestionController::new(congestion_config),
                lag: LagProfile {
                    owd_ticks: 0.0,
                    interp_delay_ticks: 0.0,
                    rtt_ms: 0.0,
                },
                v2_clock: false,
            });
        }
    }

    /// Assign/hand off ownership of an object to `participant` as
    /// `OwnerPredicted`, bumping `ownership_epoch`, and return the reliable
    /// [`Role`](tsync::Role) frame every peer must receive (design §2.2). Returns
    /// `None` if the object does not exist.
    #[must_use]
    pub fn assign_owner(&self, id: ObjectId, participant: u64) -> Option<tsync::Role> {
        let mut g = self.inner.lock().ok()?;
        let epoch = g.world.assign_owner(id, participant)?;
        let gen_epoch = g.world.get_gen_epoch(id).unwrap_or(0);
        Some(tsync::Role {
            object_id: id,
            role: super::SyncRole::OwnerPredicted,
            owner: participant,
            ownership_epoch: epoch,
            gen_epoch,
            event: tsync::RoleEvent::Handoff,
        })
    }

    /// Hand `participant` a client-owned player object and return the
    /// `(object_id, Role)` to announce, or `None` when player-slot mode is off
    /// (`config.player_slots == 0`) or every slot is taken.
    ///
    /// Player-slot mode (config `player_slots > 0`) allocates the lowest free id
    /// in `1..=player_slots`, spawns an idle object there, and assigns it to the
    /// participant as `OwnerPredicted`. Idempotent per participant: a repeat call
    /// (e.g. a re-`HELLO`) returns the existing slot with a fresh `Role` rather
    /// than allocating another. The returned [`Role`](tsync::Role) must be sent
    /// reliably to the owner so its client flips the matching object to
    /// owner-predicted; every other client keeps seeing the object interpolated
    /// through the normal snapshot path (no `Role` needed).
    #[must_use]
    pub fn assign_player_slot(&self, participant: u64) -> Option<(ObjectId, tsync::Role)> {
        let capacity = self.config.player_slots;
        if capacity == 0 {
            return None;
        }
        let mut g = self.inner.lock().ok()?;
        // Idempotent: reuse an existing assignment, re-announcing its current epoch.
        let id = if let Some(&existing) = g.assigned_slots.get(&participant) {
            existing
        } else {
            // Lowest free id in 1..=capacity not currently assigned.
            let id = (1..=capacity)
                .find(|candidate| !g.assigned_slots.values().any(|&owned| owned == *candidate))?;
            // Spawn the object idle at a per-slot offset (so multiple players do
            // not overlap at the origin) before handing it to the owner.
            let spawn = TransformState::at([(id as f32 - 1.0) * 300.0, 0.0, 0.0]);
            g.world.spawn_server_simulated(id, spawn);
            g.assigned_slots.insert(participant, id);
            id
        };
        let epoch = g.world.assign_owner(id, participant)?;
        let gen_epoch = g.world.get_gen_epoch(id).unwrap_or(0);
        Some((
            id,
            tsync::Role {
                object_id: id,
                role: super::SyncRole::OwnerPredicted,
                owner: participant,
                ownership_epoch: epoch,
                gen_epoch,
                event: tsync::RoleEvent::Assign,
            },
        ))
    }

    /// Release the player object a participant owned (on disconnect), despawning
    /// it so the slot id frees for the next join. A no-op when the participant
    /// held no slot.
    pub fn release_player_slot(&self, participant: u64) {
        if let Ok(mut g) = self.inner.lock()
            && let Some(id) = g.assigned_slots.remove(&participant)
        {
            g.world.despawn(id);
        }
    }

    /// Register a client's **networked-actor presence** from its
    /// `KIND_NA_PRESENCE`, and return the spawn frames the gateway must deliver
    /// (all reliable). Returns `None` only if the hub lock is poisoned.
    ///
    /// The server selects the mode from `predicted_authoritative_archetypes`.
    /// Relay objects are marked owner-predicted only internally so
    /// [`advance`](TransformWorld::advance) does not integrate their raw owner
    /// reports. Predicted-authoritative objects receive a reliable role assignment
    /// and use the normal validated owner-input path instead.
    ///
    /// Idempotent per participant: a re-announce keeps the same object id and just
    /// refreshes the transform/archetype.
    ///
    /// Delivery ordering the gateway must honor: send `self_spawn` to the owner
    /// **before** anything else so the owner learns its object id (reliable QUIC
    /// streams preserve this order), then `batch` to the owner, then `peer_spawn`
    /// to each participant in `peers`.
    #[must_use]
    pub fn register_presence(
        &self,
        participant: u64,
        archetype_id: u16,
        transform: NaTransform,
    ) -> Option<PresenceRegistration> {
        let mut g = self.inner.lock().ok()?;
        let state = na_to_state(transform);
        let mode = if self
            .config
            .predicted_authoritative_archetypes
            .contains(&archetype_id)
        {
            OwnerMovementMode::PredictedAuthoritative
        } else {
            OwnerMovementMode::Relay
        };

        let object_id = if let Some(entry) = g.na_presence.get_mut(&participant) {
            // Re-announce: keep the id, refresh archetype + transform.
            entry.archetype_id = archetype_id;
            entry.mode = mode;
            let id = entry.object_id;
            g.world.set_transform(id, state);
            id
        } else {
            // Lowest positive id not already held by another presence.
            let mut id: u32 = 1;
            while g.na_presence.values().any(|e| e.object_id == id) {
                id += 1;
            }
            g.world.spawn_server_simulated(id, state);
            g.na_presence.insert(
                participant,
                NaEntry {
                    object_id: id,
                    archetype_id,
                    mode,
                },
            );
            id
        };

        let owner_role = match mode {
            OwnerMovementMode::Relay => {
                // Relay must not be integrated by the authoritative sim; it is
                // driven solely by validated ownership-gated NA_STATE reports.
                let _ = g.world.assign_owner(object_id, participant);
                None
            }
            OwnerMovementMode::PredictedAuthoritative => {
                let epoch = g.world.assign_owner(object_id, participant)?;
                let gen_epoch = g.world.get_gen_epoch(object_id).unwrap_or(0);
                Some(tsync::Role {
                    object_id,
                    role: super::SyncRole::OwnerPredicted,
                    owner: participant,
                    ownership_epoch: epoch,
                    gen_epoch,
                    event: tsync::RoleEvent::Assign,
                })
            }
        };

        let self_spawn = NaSpawn {
            object_id,
            archetype_id,
            owner: participant,
            transform,
        };

        // Snapshot the other presences first (releasing the map borrow) so we can
        // then read each one's current transform from the world.
        let others: Vec<(u64, ObjectId, u16)> = g
            .na_presence
            .iter()
            .filter(|&(&p, _)| p != participant)
            .map(|(&p, e)| (p, e.object_id, e.archetype_id))
            .collect();
        let mut peers = Vec::with_capacity(others.len());
        let mut batch_spawns = Vec::with_capacity(others.len());
        for (p, oid, arch) in others {
            peers.push(p);
            let transform = g
                .world
                .get_transform(oid)
                .map(state_to_na)
                .unwrap_or_else(NaTransform::identity);
            batch_spawns.push(NaSpawn {
                object_id: oid,
                archetype_id: arch,
                owner: p,
                transform,
            });
        }

        Some(PresenceRegistration {
            object_id,
            self_spawn,
            batch: NaSpawnBatch {
                spawns: batch_spawns,
            },
            peer_spawn: self_spawn,
            peers,
            owner_role,
        })
    }

    /// Apply an owner's `KIND_NA_STATE` transform report (relay mode). Rejected
    /// unless `participant` actually owns `object_id` per the presence registry, so
    /// a client can never move another player's actor. Returns whether it applied.
    pub fn apply_owner_state(
        &self,
        participant: u64,
        object_id: ObjectId,
        transform: NaTransform,
    ) -> bool {
        let Ok(mut g) = self.inner.lock() else {
            return false;
        };
        match g.na_presence.get(&participant) {
            Some(entry)
                if entry.object_id == object_id && entry.mode == OwnerMovementMode::Relay =>
            {
                g.world.set_transform(object_id, na_to_state(transform));
                true
            }
            _ => false,
        }
    }

    /// Release a participant's networked-actor presence (on disconnect),
    /// despawning its object. Returns the freed object id so the gateway can
    /// broadcast a `KIND_NA_DESPAWN`, or `None` if it had no presence.
    #[must_use]
    pub fn release_presence(&self, participant: u64) -> Option<ObjectId> {
        let mut g = self.inner.lock().ok()?;
        let entry = g.na_presence.remove(&participant)?;
        g.world.despawn(entry.object_id);
        Some(entry.object_id)
    }

    /// The participants that currently hold a networked-actor presence (for the
    /// gateway to fan a despawn out to). Excludes `except`.
    #[must_use]
    pub fn presence_peers(&self, except: u64) -> Vec<u64> {
        self.inner
            .lock()
            .map(|g| {
                g.na_presence
                    .keys()
                    .copied()
                    .filter(|&p| p != except)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Mark an object hit-eligible so it records a rewind buffer (design §7.2).
    pub fn set_hit_eligible(&self, id: ObjectId, eligible: bool) {
        if let Ok(mut g) = self.inner.lock() {
            g.world.set_hit_eligible(id, eligible);
        }
    }

    /// Update the server-measured latency profile the rewind time is computed
    /// from (design §5.2). In production this is fed from QUIC path stats; tests
    /// set it directly.
    pub fn set_lag_profile(&self, participant: u64, lag: LagProfile) {
        if let Ok(mut g) = self.inner.lock()
            && let Some(c) = g.clients.get_mut(&participant)
        {
            c.lag = lag;
        }
    }

    /// Feed a client's congestion signals over `dt_secs`, stepping its adaptive
    /// send-rate/budget with hysteresis (design §6.5).
    pub fn observe_congestion(&self, participant: u64, signals: &CongestionSignals, dt_secs: f64) {
        if let Ok(mut g) = self.inner.lock()
            && let Some(c) = g.clients.get_mut(&participant)
        {
            c.congestion.observe(signals, dt_secs);
        }
    }

    /// The current adaptive send rate (pps) for a client (tests/metrics).
    #[must_use]
    pub fn client_send_rate_hz(&self, participant: u64) -> Option<u8> {
        self.inner.lock().ok().and_then(|g| {
            g.clients
                .get(&participant)
                .map(|c| c.congestion.send_rate_hz())
        })
    }

    /// Drop a client's snapshot state (on disconnect).
    pub fn unregister_client(&self, participant: u64) {
        if let Ok(mut g) = self.inner.lock() {
            g.clients.remove(&participant);
        }
    }

    /// Handle a client `HELLO`: register the client and return the server's
    /// negotiation reply to send reliably.
    #[must_use]
    pub fn handle_hello(&self, participant: u64) -> HubOutbound {
        self.register_client(participant);
        HubOutbound {
            participant,
            kind: citadel_wire::protocol::KIND_TSYNC_HELLO,
            body: self.hello_body(),
            unreliable: false,
        }
    }

    /// Negotiate the dedicated epoch-bearing v2 layout. Invalid or unknown
    /// manifests do not register/downgrade a client, and a v1 client is never
    /// switched by a v1 HELLO.
    pub fn handle_v2_hello(&self, participant: u64, body: &[u8]) -> Option<HubOutbound> {
        let manifest = tsync::V2Manifest::decode(body).ok()?;
        self.register_client(participant);
        let mut g = self.inner.lock().ok()?;
        g.clients.get_mut(&participant)?.v2_clock = true;
        Some(HubOutbound {
            participant,
            kind: citadel_wire::protocol::KIND_TSYNC_V2_HELLO,
            body: manifest.encode().to_vec(),
            unreliable: false,
        })
    }

    /// Handle a client `ACK` body, advancing that client's confirmed baseline.
    pub fn handle_ack(&self, participant: u64, body: &[u8]) {
        let Ok(ack) = tsync::Ack::decode(body) else {
            return;
        };
        if let Ok(mut g) = self.inner.lock()
            && let Some(c) = g.clients.get_mut(&participant)
        {
            c.builder.apply_ack(&ack);
        }
    }

    /// Handle a client `KIND_TSYNC_INPUT` bundle (design §5.1, §5.2, P2).
    ///
    /// Decodes the redundant input bundle, advances the client's confirmed
    /// baseline from the piggybacked ack, and applies each input frame **in seq
    /// order** (the per-object tracker dedups redundant resends and tracks the
    /// contiguous ack). A frame carrying a fire command that is *newly applied*
    /// triggers exactly one lag-compensated hit test: the **server computes and
    /// clamps** the rewind time from its own [`LagProfile`] (never the client's
    /// timestamp), rewinds hit-eligible objects, resolves favor-the-shooter, and
    /// returns the authoritative [`RewindResult`] reliably. Lag comp is disabled
    /// above the RTT cutoff (the shot resolves at present state). Returns the
    /// reliable rewind replies to deliver.
    #[must_use]
    pub fn handle_input(&self, participant: u64, body: &[u8]) -> Vec<HubOutbound> {
        let mut out = Vec::new();
        let Ok(bundle) = InputBundle::decode(body) else {
            return out;
        };
        let Ok(mut g) = self.inner.lock() else {
            return out;
        };
        // Fold the piggybacked snapshot ack into the client's baseline.
        if bundle.acked_snapshot_id != 0
            && let Some(c) = g.clients.get_mut(&participant)
        {
            c.builder.apply_ack(&tsync::Ack {
                acked_snapshot_id: bundle.acked_snapshot_id,
                history: 0,
            });
        }

        // Apply frames strictly in seq order so a reordered bundle still applies
        // deterministically (the queue buffers + releases in contiguous order).
        let mut frames: Vec<tsync::InputFrame> = bundle.frames;
        frames.sort_by_key(|f| f.input_seq);
        let current_tick = g.world.tick();
        let rewind_cfg = self.config.rewind;
        for f in &frames {
            // Applying may release a contiguous run of previously-buffered frames;
            // resolve a fire per released frame (each exactly once, in seq order).
            let released = g.world.apply_owner_input(participant, f);
            for applied in &released {
                let Some(fire) = applied.fire else {
                    continue;
                };
                // Server-computed, clamped rewind time (client time never trusted).
                let lag = g
                    .clients
                    .get(&participant)
                    .map(|c| c.lag)
                    .unwrap_or(LagProfile {
                        owd_ticks: 0.0,
                        interp_delay_ticks: 0.0,
                        rtt_ms: 0.0,
                    });
                let rewind_tick = if lag_comp_enabled(&lag, &rewind_cfg) {
                    compute_rewind_tick(current_tick, &lag, &rewind_cfg)
                } else {
                    f64::from(current_tick) // above the cutoff: resolve at present
                };
                let ray = HitRay {
                    origin: fire.origin,
                    direction: fire.direction,
                };
                let shooter_object = applied.object_id;
                let radius = rewind_cfg.hit_radius_cm;
                let targets = g
                    .world
                    .rewind_centers(rewind_tick)
                    .into_iter()
                    .filter(|&(id, _)| id != shooter_object)
                    .map(|(id, center)| HitTarget {
                        object_id: id,
                        center,
                        radius,
                    });
                let result = match rewind::resolve_hit(&ray, targets) {
                    Some(h) => RewindResult {
                        input_seq: applied.input_seq,
                        hit: true,
                        object_id: h.object_id,
                        hit_point: h.point,
                        rewind_tick: rewind_tick.round().max(0.0) as u32,
                    },
                    None => RewindResult {
                        input_seq: applied.input_seq,
                        hit: false,
                        object_id: 0,
                        hit_point: [0.0; 3],
                        rewind_tick: rewind_tick.round().max(0.0) as u32,
                    },
                };
                out.push(HubOutbound {
                    participant,
                    kind: citadel_wire::protocol::KIND_TSYNC_REWIND,
                    body: result.encode(),
                    unreliable: false,
                });
            }
        }
        out
    }

    /// Handle an epoch-fenced v2 input. Hints are decoded only to bound and
    /// validate the wire; they are deliberately not passed into simulation,
    /// scheduling, authorization, latency, or rewind calculations.
    #[must_use]
    pub fn handle_v2_input(&self, participant: u64, body: &[u8]) -> Vec<HubOutbound> {
        let Ok((epoch, _hint, bundle)) = tsync::InputDiagnosticHint::decode_v2(body) else {
            if let Ok(mut g) = self.inner.lock() {
                g.input_hint_metrics.rejected = g.input_hint_metrics.rejected.saturating_add(1);
            }
            return Vec::new();
        };
        let Ok(mut g) = self.inner.lock() else {
            return Vec::new();
        };
        let valid = g.clients.get(&participant).is_some_and(|c| c.v2_clock)
            && g.gameplay_clock.snapshot().epoch == epoch;
        if valid {
            g.input_hint_metrics.accepted = g.input_hint_metrics.accepted.saturating_add(1);
            // Deliberately count the authority decision separately from decode:
            // no hint field is retained or passed into gameplay.
            g.input_hint_metrics.ignored = g.input_hint_metrics.ignored.saturating_add(1);
        } else {
            g.input_hint_metrics.rejected = g.input_hint_metrics.rejected.saturating_add(1);
        }
        drop(g);
        if !valid {
            return Vec::new();
        }
        self.handle_input(participant, &bundle.encode())
    }

    /// Return aggregate-only v2 input-hint diagnostics for tests/observability.
    #[must_use]
    pub fn input_hint_metrics(&self) -> InputHintMetrics {
        self.inner
            .lock()
            .map(|g| g.input_hint_metrics)
            .unwrap_or_default()
    }

    /// Advance the world one sim tick and latch the frame (design §7.5). The
    /// snapshot tick reads only the latched frame.
    pub fn sim_tick(&self) {
        if let Ok(mut g) = self.inner.lock() {
            let dt = self.config.sim_dt;
            g.world.advance(dt);
            g.latest = g.world.latch();
            g.gameplay_clock.complete_step();
        }
    }

    /// Read the authoritative gameplay clock for this hub. It is independent of
    /// snapshot cadence and advances only when [`Self::sim_tick`] completes.
    ///
    /// Returns `None` if the hub mutex has been poisoned. A poisoned hub has no
    /// trustworthy clock state, so callers must treat it as unavailable rather
    /// than accepting a fabricated or potentially stale epoch.
    #[must_use]
    pub fn gameplay_clock(&self) -> Option<GameplayClockSnapshot> {
        self.inner.lock().ok().map(|g| g.gameplay_clock.snapshot())
    }

    /// Build one delta snapshot per client from the latched frame using the
    /// legacy node-global scope. The gateway uses
    /// [`Self::snapshot_tick_scoped`] when room membership is available.
    #[must_use]
    pub fn snapshot_tick(&self) -> Vec<HubOutbound> {
        self.snapshot_tick_scoped(|_| None)
    }

    /// Build one delta snapshot per client from the latched frame, restricting
    /// each to objects in its room. `room_of` comes from the gateway's room
    /// registry; both roomless participants share the relay-compatible scope.
    /// Returns unreliable snapshot envelopes for the gateway to fan out.
    /// Encoding errors for a single client are skipped (never poison the whole
    /// tick).
    #[must_use]
    pub fn snapshot_tick_scoped(
        &self,
        mut room_of: impl FnMut(u64) -> Option<u64>,
    ) -> Vec<HubOutbound> {
        let mut out = Vec::new();
        let Ok(mut g) = self.inner.lock() else {
            return out;
        };
        let frame = Arc::clone(&g.latest);
        let object_rooms = g.object_rooms.clone();
        let gameplay_clock = g.gameplay_clock.snapshot();
        let hard_cap = self.config.budget;
        let mut room_frames = HashMap::new();
        for (&participant, client) in g.clients.iter_mut() {
            let viewer_room = room_of(participant);
            let scoped_frame = room_frames.entry(viewer_room).or_insert_with(|| {
                Arc::new(frame.filtered(|object| {
                    match object.owner {
                        0 => object_rooms
                            .get(&object.object_id)
                            .is_none_or(|room_id| Some(*room_id) == viewer_room),
                        owner => room_of(owner) == viewer_room,
                    }
                }))
            });
            // The adaptive controller owns the per-client budget + coarse send
            // rate (design §6.5); an optional hub-level hard cap tightens it.
            let adaptive_budget = client.congestion.budget();
            let budget = match (adaptive_budget, hard_cap) {
                (0, cap) => cap,
                (b, 0) => b,
                (b, cap) => b.min(cap),
            };
            let send_rate = client.congestion.send_rate_hz();
            let Some(snapshot) = client.builder.build(
                scoped_frame,
                participant,
                client.viewer_pos,
                budget,
                send_rate,
            ) else {
                continue;
            };
            let encoded = if client.v2_clock {
                tsync::SnapshotV2 {
                    clock: tsync::GameplayClockMetadata {
                        epoch: gameplay_clock.epoch,
                        tick: gameplay_clock.tick,
                        tick_hz: gameplay_clock.tick_hz,
                    },
                    snapshot,
                }
                .encode(&self.codec)
                .map(|body| (citadel_wire::protocol::KIND_TSYNC_V2_SNAPSHOT, body))
            } else {
                snapshot
                    .encode(&self.codec)
                    .map(|body| (citadel_wire::protocol::KIND_TSYNC_SNAPSHOT, body))
            };
            match encoded {
                Ok((kind, body)) => out.push(HubOutbound {
                    participant,
                    kind,
                    body,
                    unreliable: true,
                }),
                Err(e) => {
                    tracing::debug!(participant, error = %e, "skipped a client snapshot encode");
                }
            }
        }
        out
    }

    /// The number of registered transform-sync clients (tests/metrics).
    #[must_use]
    pub fn client_count(&self) -> usize {
        self.inner.lock().map(|g| g.clients.len()).unwrap_or(0)
    }

    /// The current sim tick (tests).
    #[must_use]
    pub fn tick(&self) -> u32 {
        self.inner.lock().map(|g| g.world.tick()).unwrap_or(0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::realtime::transform::{RemoteWorldView, SyncRole};

    fn hub() -> TransformHub {
        TransformHub::new(TransformHubConfig::default()).expect("hub")
    }

    #[test]
    fn v2_input_hint_metrics_are_aggregate_and_authority_ignored() {
        let hub = hub();
        let manifest = tsync::V2Manifest::clock().encode();
        assert!(hub.handle_v2_hello(11, &manifest).is_some());
        let epoch = hub.gameplay_clock().expect("clock").epoch;
        let bundle = InputBundle {
            acked_snapshot_id: 0,
            last_seen_snapshot_id: 0,
            frames: Vec::new(),
        };
        let valid = tsync::InputDiagnosticHint {
            last_observed_tick: 123_456,
            flags: 0,
        }
        .encode_v2(epoch, &bundle)
        .expect("valid diagnostic hint");
        assert!(hub.handle_v2_input(11, &valid).is_empty());
        assert!(hub.handle_v2_input(11, &[0]).is_empty());
        let metrics = hub.input_hint_metrics();
        assert_eq!(metrics.accepted, 1);
        assert_eq!(metrics.ignored, 1, "hint values never reach authority");
        assert_eq!(metrics.rejected, 1);
    }

    #[test]
    fn hello_registers_and_replies() {
        let hub = hub();
        let reply = hub.handle_hello(1);
        assert_eq!(reply.kind, citadel_wire::protocol::KIND_TSYNC_HELLO);
        assert!(!reply.unreliable);
        assert_eq!(hub.client_count(), 1);
        // The reply decodes to the server's negotiation.
        let hello = Hello::decode(&reply.body).expect("hello decodes");
        assert_eq!(hello, TransformHubConfig::default().hello);
    }

    #[test]
    fn scoped_snapshot_matches_legacy_output_for_single_room() {
        let legacy = hub();
        let scoped = hub();
        for hub in [&legacy, &scoped] {
            let _ = hub.handle_hello(1);
            let _ = hub.handle_hello(2);
            hub.spawn_server_simulated(1, TransformState::at([10.0, 0.0, 0.0]));
            hub.spawn_server_simulated(2, TransformState::at([20.0, 0.0, 0.0]));
            let _ = hub.assign_owner(1, 1);
            let _ = hub.assign_owner(2, 2);
            hub.sim_tick();
        }

        let mut legacy_outbound = legacy.snapshot_tick();
        let mut scoped_outbound = scoped.snapshot_tick_scoped(|_| Some(7));
        legacy_outbound.sort_by_key(|out| out.participant);
        scoped_outbound.sort_by_key(|out| out.participant);

        assert_eq!(scoped_outbound.len(), legacy_outbound.len());
        for (scoped, legacy) in scoped_outbound.iter().zip(&legacy_outbound) {
            assert_eq!(scoped.participant, legacy.participant);
            assert_eq!(scoped.kind, legacy.kind);
            assert_eq!(scoped.body, legacy.body);
        }
    }

    #[test]
    fn gameplay_clock_tracks_completed_steps_at_the_effective_sim_rate() {
        let config = TransformHubConfig {
            sim_dt: 1.0 / 30.0,
            hello: Hello {
                sim_rate_hz: 30,
                ..Hello::default()
            },
            ..TransformHubConfig::default()
        };
        let hub = TransformHub::new(config).expect("hub");
        let initial = hub.gameplay_clock().expect("clock is available");
        assert_eq!(initial.tick_hz, 30);
        assert_eq!(initial.tick, 0);

        hub.sim_tick();
        hub.sim_tick();
        let clock = hub.gameplay_clock().expect("clock is available");
        assert_eq!(clock.epoch, initial.epoch);
        assert_eq!(clock.tick, 2);
        assert_eq!(clock.elapsed_us, 66_666);
    }

    #[test]
    #[allow(clippy::panic)]
    fn gameplay_clock_is_unavailable_after_mutex_poisoning() {
        let hub = hub();
        let initial = hub.gameplay_clock().expect("clock is available");

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    let _guard = hub.inner.lock().expect("lock is initially healthy");
                    panic!("deliberately poison hub state");
                });
            });
        }));

        assert_eq!(hub.gameplay_clock(), None);
        // `initial` is intentionally not returned: a caller cannot obtain stale
        // state through the hub API once its clock is unavailable.
        assert_ne!(initial.epoch, 0);
    }

    #[test]
    fn player_slot_assignment_is_idempotent_and_bounded() {
        let cfg = TransformHubConfig {
            player_slots: 2,
            ..TransformHubConfig::default()
        };
        let hub = TransformHub::new(cfg).expect("hub");

        // First participant -> slot 1, owner-predicted, epoch advances.
        let (id1, role1) = hub.assign_player_slot(100).expect("slot for 100");
        assert_eq!(id1, 1);
        assert_eq!(role1.owner, 100);
        assert_eq!(role1.role, SyncRole::OwnerPredicted);
        assert!(role1.ownership_epoch >= 1);

        // Idempotent: a repeat (e.g. a re-HELLO) keeps the same object id.
        let (id1_again, _) = hub.assign_player_slot(100).expect("same slot");
        assert_eq!(id1_again, id1, "re-assign returns the existing slot");

        // Second participant -> the other slot.
        let (id2, _) = hub.assign_player_slot(200).expect("slot for 200");
        assert_eq!(id2, 2);

        // Pool exhausted: a third participant gets nothing.
        assert!(
            hub.assign_player_slot(300).is_none(),
            "no free slot once the pool is full"
        );

        // Releasing frees the id for reuse by the next join.
        hub.release_player_slot(100);
        let (reused, _) = hub.assign_player_slot(300).expect("freed slot reused");
        assert_eq!(
            reused, 1,
            "the released id is handed to the next participant"
        );
    }

    #[test]
    fn player_slot_mode_off_by_default() {
        // Default config has player_slots == 0, so no assignment happens.
        assert!(hub().assign_player_slot(1).is_none());
    }

    #[test]
    fn na_presence_assigns_ids_and_batches_present_actors() {
        let hub = hub();
        let t = NaTransform::identity();

        // First joiner: gets id 1, no peers yet, empty batch.
        let r1 = hub.register_presence(1, 7, t).expect("presence 1");
        assert_eq!(r1.object_id, 1);
        assert_eq!(r1.self_spawn.owner, 1);
        assert_eq!(r1.self_spawn.archetype_id, 7);
        assert!(r1.peers.is_empty(), "no one to notify yet");
        assert!(
            r1.batch.spawns.is_empty(),
            "nobody present before the first"
        );

        // Second joiner: gets id 2, must notify participant 1, batch carries 1.
        let r2 = hub.register_presence(2, 9, t).expect("presence 2");
        assert_eq!(r2.object_id, 2);
        assert_eq!(r2.peers, vec![1], "participant 1 is told to spawn 2");
        assert_eq!(r2.batch.spawns.len(), 1, "the newcomer sees participant 1");
        let seen = &r2.batch.spawns[0];
        assert_eq!(seen.owner, 1);
        assert_eq!(seen.object_id, 1);
        assert_eq!(seen.archetype_id, 7);
    }

    #[test]
    fn na_presence_is_idempotent_per_participant() {
        let hub = hub();
        let t = NaTransform::identity();
        let a = hub.register_presence(1, 1, t).expect("first");
        let b = hub.register_presence(1, 1, t).expect("re-announce");
        assert_eq!(a.object_id, b.object_id, "re-announce keeps the id");
    }

    #[test]
    fn na_owner_state_requires_ownership() {
        let hub = hub();
        let t = NaTransform::identity();
        let r1 = hub.register_presence(1, 0, t).expect("p1");
        let _ = hub.register_presence(2, 0, t).expect("p2");

        // The owner may move its own object.
        assert!(hub.apply_owner_state(1, r1.object_id, t));
        // A different participant may NOT move participant 1's object.
        assert!(!hub.apply_owner_state(2, r1.object_id, t));
        // An unknown object is rejected too.
        assert!(!hub.apply_owner_state(1, 999, t));
    }

    #[test]
    fn na_relay_object_is_not_integrated_by_advance() {
        let hub = hub();
        let r = hub
            .register_presence(1, 0, NaTransform::identity())
            .expect("presence");
        // Report a moving state: position at origin but non-zero velocity.
        let moving = NaTransform {
            position: [0.0, 0.0, 0.0],
            rotation: [0.0, 0.0, 0.0, 1.0],
            velocity: [600.0, 0.0, 0.0],
        };
        assert!(hub.apply_owner_state(1, r.object_id, moving));
        // A relay object is owner-predicted server-side, so advance must NOT
        // integrate its velocity — its position stays where the owner reported it.
        hub.sim_tick();
        let s = hub.get_transform(r.object_id).expect("object exists");
        assert!(
            s.position[0].abs() < 1e-3,
            "relay object should not drift on its own: x={}",
            s.position[0]
        );
    }

    #[test]
    fn na_presence_release_despawns_and_frees_the_id() {
        let hub = hub();
        let t = NaTransform::identity();
        let r1 = hub.register_presence(1, 0, t).expect("p1");
        assert!(hub.get_transform(r1.object_id).is_some());

        let freed = hub.release_presence(1).expect("released");
        assert_eq!(freed, r1.object_id);
        assert!(hub.get_transform(freed).is_none(), "object despawned");
        assert!(
            hub.release_presence(1).is_none(),
            "second release is a no-op"
        );

        // The freed id is reused by the next joiner.
        let r2 = hub.register_presence(2, 0, t).expect("p2");
        assert_eq!(r2.object_id, freed, "lowest free id reused");
    }

    #[test]
    fn end_to_end_two_clients_see_a_moving_object() {
        let hub = hub();
        let _ = hub.handle_hello(1);
        let _ = hub.handle_hello(2);
        // A server-simulated object moving on +x.
        let mut s = TransformState::at([0.0, 0.0, 0.0]);
        s.velocity = [600.0, 0.0, 0.0];
        hub.spawn_server_simulated(10, s);

        let codec = *hub.codec();
        let mut view1 = RemoteWorldView::new(codec, 60, 20);
        let mut view2 = RemoteWorldView::new(codec, 60, 20);

        // Run several sim+snapshot ticks; feed both clients; ack back.
        for _ in 0..10 {
            hub.sim_tick();
            for outbound in hub.snapshot_tick() {
                let view = if outbound.participant == 1 {
                    &mut view1
                } else {
                    &mut view2
                };
                assert!(view.apply_datagram(&outbound.body));
                let ack = view.ack();
                hub.handle_ack(outbound.participant, &ack.encode());
            }
        }

        // Both clients reconstruct the object and see it advanced on +x.
        let o1 = view1.object(10).expect("client 1 sees object");
        let o2 = view2.object(10).expect("client 2 sees object");
        assert!(
            o1.state.position[0] > 50.0,
            "moved: {}",
            o1.state.position[0]
        );
        assert!(o2.state.position[0] > 50.0);
        // After the first ack, snapshots are deltas (base != 0).
    }

    #[test]
    fn owner_input_advances_object_and_snapshot_carries_ack() {
        use citadel_wire::tsync::{InputBundle, InputFrame};

        let hub = hub();
        let _ = hub.handle_hello(1);
        // Spawn an object and hand ownership to participant 1.
        hub.spawn(TransformAuthority::new(
            5,
            SyncRole::ServerSimulated,
            TransformState::default(),
        ));
        let role = hub.assign_owner(5, 1).expect("owner assigned");
        assert_eq!(role.event, tsync::RoleEvent::Handoff);
        let epoch = role.ownership_epoch;

        // Feed two in-order owner inputs (a redundant bundle carrying both).
        let bundle = InputBundle {
            acked_snapshot_id: 0,
            last_seen_snapshot_id: 0,
            frames: vec![
                InputFrame {
                    input_seq: 1,
                    sim_tick: 1,
                    dt: 0.1,
                    object_id: 5,
                    ownership_epoch: epoch,
                    move_velocity: [500.0, 0.0, 0.0],
                    payload: vec![],
                    fire: None,
                },
                InputFrame {
                    input_seq: 2,
                    sim_tick: 2,
                    dt: 0.1,
                    object_id: 5,
                    ownership_epoch: epoch,
                    move_velocity: [500.0, 0.0, 0.0],
                    payload: vec![],
                    fire: None,
                },
            ],
        };
        let replies = hub.handle_input(1, &bundle.encode());
        assert!(replies.is_empty(), "no fire => no rewind reply");
        // Object advanced ~ +100 cm (2 * 500 * 0.1).
        let s = hub.get_transform(5).expect("object");
        assert!((s.position[0] - 100.0).abs() < 1e-2, "x={}", s.position[0]);

        // The owner's snapshot echoes last_input_seq = 2 (highest contiguous).
        let codec = *hub.codec();
        let mut view = RemoteWorldView::new(codec, 60, 20);
        hub.sim_tick();
        for out in hub.snapshot_tick() {
            if out.participant == 1 {
                assert!(view.apply_datagram(&out.body));
            }
        }
        assert_eq!(view.owner_ack(5), Some(2));
    }

    #[test]
    fn fire_command_resolves_favor_the_shooter_and_respects_cutoff() {
        use citadel_wire::tsync::{FireCommand, InputBundle, InputFrame, RewindResult};

        let hub = hub();
        let _ = hub.handle_hello(1);
        // Shooter object owned by participant 1 at the origin.
        hub.spawn(TransformAuthority::new(
            1,
            SyncRole::ServerSimulated,
            TransformState::at([0.0, 0.0, 0.0]),
        ));
        let role = hub.assign_owner(1, 1).expect("owner");
        let epoch = role.ownership_epoch;

        // A hit-eligible target on the shooter's +x ray, drifting off it on +y so
        // that in the past it straddles the ray (hittable) but at present it has
        // moved clear of it (a miss). 300 cm/s => 5 cm/tick at 60 Hz.
        let mut target = TransformState::at([100.0, 0.0, 0.0]);
        target.velocity = [0.0, 300.0, 0.0];
        hub.spawn_server_simulated(2, target);
        hub.set_hit_eligible(2, true);

        // Build rewind history: advance many ticks so the target drifts off the ray.
        for _ in 0..40 {
            hub.sim_tick();
        }
        let present_y = hub.get_transform(2).unwrap().position[1];
        assert!(
            present_y > 150.0,
            "target drifted off the ray: y={present_y}"
        );

        // The shooter saw the target on the ray a while ago. Configure lag so the
        // server rewinds deep into the past where the target was hittable.
        hub.set_lag_profile(
            1,
            LagProfile {
                owd_ticks: 20.0,
                interp_delay_ticks: 18.0,
                rtt_ms: 120.0, // below the cutoff
            },
        );
        let fire_bundle = |seq: u32| InputBundle {
            acked_snapshot_id: 0,
            last_seen_snapshot_id: 0,
            frames: vec![InputFrame {
                input_seq: seq,
                sim_tick: 0,
                dt: 0.0,
                object_id: 1,
                ownership_epoch: epoch,
                move_velocity: [0.0; 3],
                payload: vec![],
                fire: Some(FireCommand {
                    origin: [0.0, 0.0, 0.0],
                    direction: [1.0, 0.0, 0.0],
                }),
            }],
        };
        let replies = hub.handle_input(1, &fire_bundle(1).encode());
        assert_eq!(replies.len(), 1, "exactly one rewind reply");
        let result = RewindResult::decode(&replies[0].body).unwrap();
        assert!(result.hit, "favor-the-shooter hit against the rewound pos");
        assert_eq!(result.object_id, 2);
        assert!(!replies[0].unreliable, "rewind result is reliable");

        // Above the RTT cutoff, lag comp disables: the shot resolves at present
        // state (target long gone from the ray at x≈100), so it misses.
        hub.set_lag_profile(
            1,
            LagProfile {
                owd_ticks: 20.0,
                interp_delay_ticks: 18.0,
                rtt_ms: 400.0, // above the cutoff
            },
        );
        let replies2 = hub.handle_input(1, &fire_bundle(2).encode());
        assert_eq!(replies2.len(), 1);
        let miss = RewindResult::decode(&replies2[0].body).unwrap();
        assert!(!miss.hit, "above cutoff resolves at present => miss");
    }

    #[test]
    fn adaptive_congestion_steps_send_rate_under_pressure() {
        let hub = hub();
        let _ = hub.handle_hello(1);
        assert_eq!(hub.client_send_rate_hz(1), Some(20));
        // Sustained bad signals step the client to the floor rate.
        let bad = CongestionSignals {
            datagram_loss: 0.3,
            ..Default::default()
        };
        hub.observe_congestion(1, &bad, 0.6);
        hub.observe_congestion(1, &bad, 0.6);
        assert_eq!(hub.client_send_rate_hz(1), Some(10));
    }

    #[test]
    fn predicted_networked_actor_uses_input_pipeline_and_rejects_relay_state() {
        use citadel_wire::tsync::{InputBundle, InputFrame};

        let hub = TransformHub::new(TransformHubConfig {
            predicted_authoritative_archetypes: vec![42],
            ..TransformHubConfig::default()
        })
        .expect("hub");
        let _ = hub.handle_hello(7);
        let initial = NaTransform::identity();
        let registration = hub.register_presence(7, 42, initial).expect("presence");
        let role = registration.owner_role.expect("predicted owner role");
        assert_eq!(role.object_id, registration.object_id);
        assert_eq!(role.owner, 7);

        // A client cannot downgrade an authoritative archetype to the legacy raw
        // transform path; Relay stays opt-in only through server policy.
        assert!(!hub.apply_owner_state(7, registration.object_id, initial));

        let input = InputBundle {
            acked_snapshot_id: 0,
            last_seen_snapshot_id: 0,
            frames: vec![InputFrame {
                input_seq: 1,
                sim_tick: 1,
                dt: 0.1,
                object_id: registration.object_id,
                ownership_epoch: role.ownership_epoch,
                move_velocity: [250.0, 0.0, 0.0],
                payload: Vec::new(),
                fire: None,
            }],
        };
        assert!(hub.handle_input(7, &input.encode()).is_empty());
        assert!(
            hub.get_transform(registration.object_id)
                .expect("authoritative object")
                .position[0]
                > 20.0
        );

        let codec = *hub.codec();
        let mut view = RemoteWorldView::new(codec, 60, 20);
        hub.sim_tick();
        for out in hub.snapshot_tick() {
            if out.participant == 7 {
                assert!(view.apply_datagram(&out.body));
            }
        }
        assert_eq!(view.owner_ack(registration.object_id), Some(1));
    }

    #[test]
    fn unlisted_networked_actor_preserves_relay_default() {
        let hub = hub();
        let initial = NaTransform::identity();
        let registration = hub.register_presence(3, 9, initial).expect("presence");
        assert!(
            registration.owner_role.is_none(),
            "Relay remains the default"
        );

        let mut reported = initial;
        reported.position[0] = 123.0;
        assert!(hub.apply_owner_state(3, registration.object_id, reported));
        assert_eq!(
            hub.get_transform(registration.object_id)
                .expect("relay transform")
                .position[0],
            123.0
        );
    }

    #[test]
    fn physics_map_bvh_is_cached_across_selection_and_sim_ticks() {
        let hub = hub();
        let mesh = citadel_map::CollisionMesh {
            vertices: vec![[0.0, 0.0, 0.0], [100.0, 0.0, 0.0], [0.0, 0.0, 100.0]],
            triangles: vec![[0, 1, 2]],
        };

        hub.set_physics_map(Some(("Arena", &mesh)));
        let first = {
            let guard = hub.inner.lock().unwrap();
            assert_eq!(guard.physics_bvhs.len(), 1);
            std::sync::Arc::clone(guard.physics_bvhs.get("Arena").unwrap())
        };
        hub.sim_tick();
        hub.set_physics_map(Some(("Arena", &mesh)));

        let guard = hub.inner.lock().unwrap();
        assert_eq!(guard.physics_bvhs.len(), 1);
        assert!(std::sync::Arc::ptr_eq(
            &first,
            guard.physics_bvhs.get("Arena").unwrap()
        ));
    }
}
