//! The authoritative world and its latched, immutable per-tick [`Frame`].

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use citadel_physics::{
    GroundHit, PhysicsBody, PhysicsConfig, RaycastHit, StaticTriBvh, ground_height, raycast,
    sphere_overlap, step,
};
use citadel_wire::codec::WorldBounds;
use citadel_wire::interest::InterestGrid;
use citadel_wire::tsync::InputFrame;

use super::authority::{TransformAuthority, TransformState};
use super::input::{self, InputLimits, OwnerInputQueue};
use super::rewind::RewindBuffer;
use super::{ObjectId, SyncRole};

/// One object as captured in an immutable [`Frame`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrameObject {
    /// Object id.
    pub object_id: ObjectId,
    /// Replication generation at this tick.
    pub gen_epoch: u16,
    /// Authoritative transform at this tick.
    pub state: TransformState,
    /// Role at this tick.
    pub role: SyncRole,
    /// Owner (raw participant id, `0` = server-owned).
    pub owner: u64,
    /// Whether velocity is replicated for this object.
    pub replicate_velocity: bool,
    /// Base network priority.
    pub priority: f32,
    /// Highest contiguous owner input seq applied (echoed to the owner, design §5.1).
    pub last_input_seq: u32,
}

/// An immutable snapshot of the whole world after a completed sim tick (design
/// §7.5). The snapshot tick reads **only** a `Frame`, never the live world, so a
/// single client snapshot can never mix object A at tick N with object B at tick
/// N+1. The frame carries its own [`InterestGrid`] built from exactly these
/// object positions, so area-of-interest queries are coherent with the state.
#[derive(Debug, Clone)]
pub struct Frame {
    /// The completed sim tick this frame describes.
    pub tick: u32,
    /// Every object, ordered by id (stable snapshot ordering).
    pub objects: Vec<FrameObject>,
    /// Interest grid built from exactly `objects`' positions.
    grid: InterestGrid,
}

/// The current observable state of an opt-in physics body.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PhysicsState {
    /// Whether the most recent physics step found walkable ground.
    pub grounded: bool,
    /// Authoritative world position in centimetres.
    pub position: [f32; 3],
    /// Authoritative linear velocity in centimetres per second.
    pub velocity: [f32; 3],
}

impl FrameObject {
    /// Whether `participant` is this object's predicting owner (design §5.1): an
    /// [`OwnerPredicted`](SyncRole::OwnerPredicted) object with a matching,
    /// non-zero owner.
    #[must_use]
    pub fn owner_matches(&self, participant: u64) -> bool {
        self.role == SyncRole::OwnerPredicted && self.owner != 0 && self.owner == participant
    }
}

impl Frame {
    /// The interest grid coherent with this frame's object positions.
    #[must_use]
    pub fn grid(&self) -> &InterestGrid {
        &self.grid
    }

    /// Look up a frame object by id (linear; frames are small in P1).
    #[must_use]
    pub fn object(&self, id: ObjectId) -> Option<&FrameObject> {
        self.objects.iter().find(|o| o.object_id == id)
    }

    /// Return a frame containing only objects accepted by `retain`. Its interest
    /// grid is rebuilt from that same subset, preserving AOI coherence.
    #[must_use]
    pub(crate) fn filtered(&self, mut retain: impl FnMut(&FrameObject) -> bool) -> Self {
        let mut grid = self.grid.clone();
        let mut objects = Vec::with_capacity(self.objects.len());
        for object in &self.objects {
            if retain(object) {
                objects.push(*object);
            } else {
                grid.remove(u64::from(object.object_id));
            }
        }
        Self {
            tick: self.tick,
            objects,
            grid,
        }
    }
}

/// The authoritative transform world: the set of [`TransformAuthority`] plus the
/// live [`InterestGrid`]. Mutated only by the sim tick; the snapshot tick reads
/// the latched [`Frame`].
#[derive(Debug)]
pub struct TransformWorld {
    objects: BTreeMap<ObjectId, TransformAuthority>,
    grid: InterestGrid,
    /// Number of attached bodies. A zero value preserves the pre-physics advance
    /// path exactly, apart from this one integer comparison per tick.
    physics_bodies: usize,
    /// Static collision for the active loaded map, built outside the tick path.
    physics_bvh: Option<Arc<StaticTriBvh>>,
    tick: u32,
    /// Per-object in-order owner-input queues (design §5.1, P2).
    input_queues: HashMap<ObjectId, OwnerInputQueue>,
    /// Per-object rewind history for hit-eligible objects (design §7.2, P2).
    rewind_buffers: HashMap<ObjectId, RewindBuffer>,
    /// Rewind ring capacity in ticks (~1 s at the sim rate).
    rewind_capacity_ticks: usize,
    /// Server clamps for owner input (speed/timestep bounds, design §5.1).
    input_limits: InputLimits,
    /// World bounds inputs are clamped into (from the negotiated codec).
    bounds: WorldBounds,
    /// Test-only proof that a bodyless advance did not enter the physics branch.
    #[cfg(test)]
    last_advance_used_physics: bool,
}

impl TransformWorld {
    /// A new empty world whose interest grid uses `cell_size` world units, with
    /// default input limits/bounds and a ~1 s (60-tick) rewind ring.
    #[must_use]
    pub fn new(cell_size: f32) -> Self {
        Self {
            objects: BTreeMap::new(),
            grid: InterestGrid::new(cell_size),
            physics_bodies: 0,
            physics_bvh: None,
            tick: 0,
            input_queues: HashMap::new(),
            rewind_buffers: HashMap::new(),
            rewind_capacity_ticks: 60,
            input_limits: InputLimits::default(),
            bounds: citadel_wire::codec::DEFAULT_WORLD_BOUNDS,
            #[cfg(test)]
            last_advance_used_physics: false,
        }
    }

    /// Configure the world bounds owner input is clamped into (from `HELLO`).
    pub fn set_bounds(&mut self, bounds: WorldBounds) {
        self.bounds = bounds;
    }

    /// Configure the rewind ring length in ticks (~1 s at the sim rate).
    pub fn set_rewind_capacity_ticks(&mut self, ticks: usize) {
        self.rewind_capacity_ticks = ticks.max(1);
    }

    /// Configure the owner-input validation/clamp limits.
    pub fn set_input_limits(&mut self, limits: InputLimits) {
        self.input_limits = limits;
    }

    /// The current sim tick.
    #[must_use]
    pub fn tick(&self) -> u32 {
        self.tick
    }

    /// Number of live objects.
    #[must_use]
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Whether the world has no objects.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Number of actors with an attached physics body.
    #[must_use]
    pub fn physics_body_count(&self) -> usize {
        self.physics_bodies
    }

    /// Set the static-triangle broadphase used by bodies in this world.
    ///
    /// The hub builds and caches this value only when the selected map changes;
    /// the simulation tick only borrows it.
    pub fn set_physics_bvh(&mut self, bvh: Option<Arc<StaticTriBvh>>) {
        self.physics_bvh = bvh;
    }

    /// Cast a finite ray against the currently selected static map, if any.
    #[must_use]
    pub fn raycast(&self, origin: [f32; 3], direction: [f32; 3]) -> Option<RaycastHit> {
        raycast(self.physics_bvh.as_deref()?, origin, direction)
    }

    /// Return whether a sphere overlaps the currently selected static map.
    #[must_use]
    pub fn sphere_overlap(&self, centre: [f32; 3], radius: f32) -> bool {
        self.physics_bvh
            .as_deref()
            .is_some_and(|bvh| sphere_overlap(bvh, centre, radius))
    }

    /// Find a walkable static-map surface directly below `origin`.
    #[must_use]
    pub fn ground_height(&self, origin: [f32; 3], max_distance: f32) -> Option<GroundHit> {
        ground_height(self.physics_bvh.as_deref()?, origin, max_distance)
    }

    /// Spawn (or replace) an authority and index it in the grid. Replacing an
    /// existing id bumps its `gen_epoch` (respawn/id-reuse) so clients discard
    /// deltas against the prior generation.
    pub fn spawn(&mut self, mut authority: TransformAuthority) {
        if authority.role != SyncRole::ServerSimulated {
            authority.body = None;
        }
        let previous_had_body = if let Some(prev) = self.objects.get(&authority.object_id) {
            authority.gen_epoch = prev.gen_epoch.wrapping_add(1);
            prev.body.is_some()
        } else {
            false
        };
        if previous_had_body {
            self.physics_bodies = self.physics_bodies.saturating_sub(1);
        }
        if authority.body.is_some() {
            self.physics_bodies += 1;
        }
        let pos = authority.current.position;
        self.grid
            .insert_or_move(u64::from(authority.object_id), pos);
        self.objects.insert(authority.object_id, authority);
    }

    /// Convenience: spawn a server-simulated object at `state` with velocity
    /// replicated, for demos/tests.
    pub fn spawn_server_simulated(&mut self, id: ObjectId, state: TransformState) {
        let mut a = TransformAuthority::new(id, SyncRole::ServerSimulated, state);
        a.replicate_velocity = true;
        self.spawn(a);
    }

    /// Despawn an object, removing it from the grid and dropping its owner-input
    /// and rewind state.
    pub fn despawn(&mut self, id: ObjectId) {
        if let Some(authority) = self.objects.remove(&id) {
            if authority.body.is_some() {
                self.physics_bodies = self.physics_bodies.saturating_sub(1);
            }
            self.grid.remove(u64::from(id));
            self.input_queues.remove(&id);
            self.rewind_buffers.remove(&id);
        }
    }

    /// Assign/hand off ownership of an object to `participant` as
    /// [`OwnerPredicted`](SyncRole::OwnerPredicted), bumping `ownership_epoch`
    /// (monotonic, guards reordered handoffs) and resetting the owner-input
    /// tracker so the new owner starts a fresh input stream (design §2.2). No-op
    /// if the object does not exist. Returns the new `ownership_epoch`.
    pub fn assign_owner(&mut self, id: ObjectId, participant: u64) -> Option<u32> {
        let (epoch, detached_body) = {
            let a = self.objects.get_mut(&id)?;
            a.owner = participant;
            a.role = SyncRole::OwnerPredicted;
            a.ownership_epoch = a.ownership_epoch.wrapping_add(1);
            a.last_input_seq = 0;
            (a.ownership_epoch, a.body.take().is_some())
        };
        if detached_body {
            self.physics_bodies = self.physics_bodies.saturating_sub(1);
        }
        // Fresh input stream for the new owner (drops any buffered pre-handoff
        // frames; no replay of pre-handoff input, design §2.2).
        self.input_queues.insert(id, OwnerInputQueue::new());
        Some(epoch)
    }

    /// Mark an object hit-eligible so it records a [`RewindBuffer`] each sim tick
    /// (opt-in per object, design §7.2). No-op if the object does not exist.
    pub fn set_hit_eligible(&mut self, id: ObjectId, eligible: bool) {
        if let Some(a) = self.objects.get_mut(&id) {
            a.hit_eligible = eligible;
            if eligible {
                self.rewind_buffers
                    .entry(id)
                    .or_insert_with(|| RewindBuffer::new(self.rewind_capacity_ticks));
            } else {
                self.rewind_buffers.remove(&id);
            }
        }
    }

    /// Validate one owner input frame from `sender` and apply the resulting
    /// **in-order** run of released frames to the owning authority (design §5.1).
    ///
    /// The object must exist, be owned by the sender, and match the frame's
    /// `ownership_epoch`. The frame is then offered to the object's in-order queue:
    /// duplicates/stale seqs are ignored, out-of-order frames are buffered, and
    /// only the contiguous run starting at the current watermark is integrated
    /// (so the authoritative state reflects exactly `last_input_seq` inputs — the
    /// invariant the client's rollback+replay depends on). Returns the frames that
    /// were released and integrated this call, **in seq order** (each carrying any
    /// fire command to resolve exactly once).
    pub fn apply_owner_input(&mut self, sender: u64, frame: &InputFrame) -> Vec<InputFrame> {
        let Some(authority) = self.objects.get(&frame.object_id) else {
            return Vec::new();
        };
        // Validate ownership + epoch before buffering anything (untrusted input).
        if !authority.is_owned_by(sender) || frame.ownership_epoch != authority.ownership_epoch {
            return Vec::new();
        }
        let queue = self.input_queues.entry(frame.object_id).or_default();
        let (_outcome, released) = queue.offer(frame.clone());
        if released.is_empty() {
            return released;
        }
        let watermark = queue.last_contiguous();
        // Integrate the released run in order, then publish the new ack + grid pos.
        if let Some(authority) = self.objects.get_mut(&frame.object_id) {
            for f in &released {
                input::integrate_owner_frame(authority, f, &self.input_limits, &self.bounds);
            }
            authority.last_input_seq = watermark;
            let pos = authority.current.position;
            self.grid.insert_or_move(u64::from(frame.object_id), pos);
        }
        released
    }

    /// Sample a hit-eligible object's rewound transform at fractional `tick`.
    #[must_use]
    pub fn sample_rewind(&self, id: ObjectId, tick: f64) -> Option<TransformState> {
        self.rewind_buffers.get(&id)?.sample_at(tick)
    }

    /// Iterate hit-eligible objects and their rewound center at `tick` (for a
    /// lag-compensated hit test). Objects with no sample at `tick` are skipped.
    #[must_use]
    pub fn rewind_centers(&self, tick: f64) -> Vec<(ObjectId, [f32; 3])> {
        let mut out = Vec::new();
        for (&id, buf) in &self.rewind_buffers {
            if let Some(s) = buf.sample_at(tick) {
                out.push((id, s.position));
            }
        }
        out.sort_by_key(|&(id, _)| id);
        out
    }

    /// Authoritatively set an object's transform (the `ServerSimulated` write
    /// path, e.g. from Lua). Creates a server-simulated object if absent.
    pub fn set_transform(&mut self, id: ObjectId, state: TransformState) {
        match self.objects.get_mut(&id) {
            Some(a) => {
                a.current = state;
                self.grid.insert_or_move(u64::from(id), state.position);
            }
            None => self.spawn_server_simulated(id, state),
        }
    }

    /// Read an object's authoritative transform.
    #[must_use]
    pub fn get_transform(&self, id: ObjectId) -> Option<TransformState> {
        self.objects.get(&id).map(|a| a.current)
    }

    /// Attach/reconfigure a body from `config`, or detach it when `config` is
    /// `None`. Only `ServerSimulated` actors may hold bodies.
    pub fn set_physics(&mut self, id: ObjectId, config: Option<PhysicsConfig>) {
        let Some(authority) = self.objects.get_mut(&id) else {
            return;
        };
        if authority.role != SyncRole::ServerSimulated {
            return;
        }
        match config {
            Some(config) => {
                if authority.body.is_none() {
                    self.physics_bodies += 1;
                }
                authority.body = Some(Box::new(PhysicsBody::from_config(config)));
                authority.replicate_velocity = true;
            }
            None => {
                if authority.body.take().is_some() {
                    self.physics_bodies = self.physics_bodies.saturating_sub(1);
                }
            }
        }
    }

    /// Add a velocity change that the next physics step will move with.
    pub fn apply_impulse(&mut self, id: ObjectId, impulse: [f32; 3]) {
        let Some(authority) = self.objects.get_mut(&id) else {
            return;
        };
        if authority.role != SyncRole::ServerSimulated || authority.body.is_none() {
            return;
        }
        for (velocity, delta) in authority.current.velocity.iter_mut().zip(impulse) {
            *velocity += delta;
        }
    }

    /// Set the desired control velocity for a body's next physics step.
    pub fn set_move_intent(&mut self, id: ObjectId, intent: [f32; 3]) {
        let Some(authority) = self.objects.get_mut(&id) else {
            return;
        };
        if authority.role != SyncRole::ServerSimulated {
            return;
        }
        if let Some(body) = authority.body.as_deref_mut() {
            body.move_intent = intent;
        }
    }

    /// Read an actor's current physics state, or `None` when it has no body.
    #[must_use]
    pub fn physics_state(&self, id: ObjectId) -> Option<PhysicsState> {
        let authority = self.objects.get(&id)?;
        let body = authority.body.as_deref()?;
        Some(PhysicsState {
            grounded: body.grounded,
            position: authority.current.position,
            velocity: authority.current.velocity,
        })
    }

    /// Mutable access to an authority (for role/ownership edits).
    pub fn authority_mut(&mut self, id: ObjectId) -> Option<&mut TransformAuthority> {
        self.objects.get_mut(&id)
    }

    /// An object's current replication generation, if it exists.
    #[must_use]
    pub fn get_gen_epoch(&self, id: ObjectId) -> Option<u16> {
        self.objects.get(&id).map(|a| a.gen_epoch)
    }

    /// Advance one sim tick: kinematically integrate velocities, reindex the
    /// grid, and bump the tick counter. Objects whose velocity is zero do not
    /// move. Call [`latch`](TransformWorld::latch) afterward to publish the frame.
    pub fn advance(&mut self, dt: f32) {
        #[cfg(test)]
        {
            self.last_advance_used_physics = false;
        }
        if self.physics_bodies == 0 {
            self.advance_without_physics(dt);
        } else {
            #[cfg(test)]
            {
                self.last_advance_used_physics = true;
            }
            self.advance_with_physics(dt);
        }
        self.finish_advance();
    }

    /// The original integration loop, retained as the bodyless fast path.
    fn advance_without_physics(&mut self, dt: f32) {
        for a in self.objects.values_mut() {
            // `OwnerPredicted` objects are driven by validated owner input
            // (`apply_owner_input`), which already integrates position; the
            // server must NOT also integrate their recorded velocity here or the
            // object double-moves (velocity is kept only for remote interp). All
            // other roles advance kinematically.
            if a.role == SyncRole::OwnerPredicted {
                continue;
            }
            let before = a.current.position;
            a.integrate(dt);
            if a.current.position != before {
                self.grid
                    .insert_or_move(u64::from(a.object_id), a.current.position);
            }
        }
    }

    /// Integrate ordinary actors as before and physics-step attached bodies.
    fn advance_with_physics(&mut self, dt: f32) {
        let bvh = self.physics_bvh.as_deref();
        for a in self.objects.values_mut() {
            if a.role == SyncRole::OwnerPredicted {
                continue;
            }
            let before = a.current.position;
            if a.role == SyncRole::ServerSimulated {
                if let Some(body) = a.body.as_deref_mut() {
                    step(
                        body,
                        &mut a.current.position,
                        &mut a.current.velocity,
                        dt,
                        bvh,
                    );
                    a.replicate_velocity = true;
                } else {
                    a.integrate(dt);
                }
            } else {
                a.integrate(dt);
            }
            if a.current.position != before {
                self.grid
                    .insert_or_move(u64::from(a.object_id), a.current.position);
            }
        }
    }

    fn finish_advance(&mut self) {
        self.tick = self.tick.wrapping_add(1);
        // Record the post-advance state into each hit-eligible object's rewind
        // ring at the new tick (design §7.2). Owner-input advances (which happen
        // between ticks) are captured here too since they mutate `current`.
        let tick = self.tick;
        for (&id, buf) in &mut self.rewind_buffers {
            if let Some(a) = self.objects.get(&id) {
                buf.record(tick, a.current);
            }
        }
    }

    /// Latch the current world into an immutable [`Frame`] (double-buffer). The
    /// frame clones the grid so area-of-interest queries against it are coherent
    /// and cannot observe a later mutation.
    #[must_use]
    pub fn latch(&self) -> Arc<Frame> {
        let objects = self
            .objects
            .values()
            .map(|a| FrameObject {
                object_id: a.object_id,
                gen_epoch: a.gen_epoch,
                state: a.current,
                role: a.role,
                owner: a.owner,
                replicate_velocity: a.replicate_velocity,
                priority: a.priority,
                last_input_seq: a.last_input_seq,
            })
            .collect();
        Arc::new(Frame {
            tick: self.tick,
            objects,
            grid: self.grid.clone(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use citadel_map::CollisionMesh;

    fn aabb_config(gravity: f32) -> PhysicsConfig {
        PhysicsConfig {
            shape: citadel_physics::Shape::Aabb {
                half_extents: [10.0, 10.0, 10.0],
            },
            gravity,
            buoyancy: 0.0,
            drag: 0.0,
            max_speed: 10_000.0,
        }
    }

    fn floor_mesh() -> CollisionMesh {
        CollisionMesh {
            vertices: vec![
                [-500.0, 0.0, -500.0],
                [500.0, 0.0, -500.0],
                [500.0, 0.0, 500.0],
                [-500.0, 0.0, 500.0],
            ],
            triangles: vec![[0, 1, 2], [0, 2, 3]],
        }
    }

    fn wall_mesh() -> CollisionMesh {
        CollisionMesh {
            vertices: vec![
                [0.0, -100.0, -500.0],
                [0.0, 300.0, -500.0],
                [0.0, 300.0, 500.0],
                [0.0, -100.0, 500.0],
            ],
            triangles: vec![[0, 1, 2], [0, 2, 3]],
        }
    }

    fn bodyless_world() -> TransformWorld {
        let mut world = TransformWorld::new(100.0);
        let mut moving = TransformState::at([10.0, 20.0, 30.0]);
        moving.velocity = [40.0, -5.0, 15.0];
        world.spawn_server_simulated(1, moving);
        world.spawn_server_simulated(2, TransformState::at([-10.0, 0.0, 5.0]));
        let mut owner = TransformAuthority::new(
            3,
            SyncRole::OwnerPredicted,
            TransformState::at([1.0, 2.0, 3.0]),
        );
        owner.current.velocity = [100.0, 200.0, 300.0];
        world.spawn(owner);
        world
    }

    fn advance_like_pre_physics(world: &mut TransformWorld, dt: f32) {
        for authority in world.objects.values_mut() {
            if authority.role == SyncRole::OwnerPredicted {
                continue;
            }
            let before = authority.current.position;
            authority.integrate(dt);
            if authority.current.position != before {
                world
                    .grid
                    .insert_or_move(u64::from(authority.object_id), authority.current.position);
            }
        }
        world.tick = world.tick.wrapping_add(1);
    }

    #[test]
    fn spawn_advance_latch_is_coherent() {
        let mut w = TransformWorld::new(1000.0);
        let mut s = TransformState::at([0.0, 0.0, 0.0]);
        s.velocity = [100.0, 0.0, 0.0];
        w.spawn_server_simulated(1, s);
        w.advance(0.1); // move +10cm on x
        let frame = w.latch();
        assert_eq!(frame.tick, 1);
        let obj = frame.object(1).expect("object present");
        assert!((obj.state.position[0] - 10.0).abs() < 1e-3);
        // The frame's grid sees the object at the new position.
        assert_eq!(frame.grid().position(1), Some(obj.state.position));
    }

    #[test]
    fn respawn_bumps_gen_epoch() {
        let mut w = TransformWorld::new(1000.0);
        w.spawn_server_simulated(7, TransformState::default());
        let g0 = w.latch().object(7).unwrap().gen_epoch;
        w.spawn_server_simulated(7, TransformState::at([5.0, 0.0, 0.0]));
        let g1 = w.latch().object(7).unwrap().gen_epoch;
        assert_eq!(g1, g0.wrapping_add(1), "respawn bumps generation");
    }

    #[test]
    fn set_and_get_transform_round_trip() {
        let mut w = TransformWorld::new(1000.0);
        let s = TransformState::at([12.0, 34.0, 56.0]);
        w.set_transform(3, s);
        assert_eq!(w.get_transform(3), Some(s));
        w.despawn(3);
        assert_eq!(w.get_transform(3), None);
    }

    #[test]
    fn latched_frame_is_immutable_against_later_mutation() {
        let mut w = TransformWorld::new(1000.0);
        w.set_transform(1, TransformState::at([0.0, 0.0, 0.0]));
        let frame = w.latch();
        // Mutate the live world after latching.
        w.set_transform(1, TransformState::at([999.0, 0.0, 0.0]));
        // The latched frame still holds the old position.
        assert_eq!(frame.object(1).unwrap().state.position, [0.0, 0.0, 0.0]);
    }

    #[test]
    fn bodyless_advance_uses_the_legacy_integration_fast_path() {
        let mut actual = bodyless_world();
        let mut expected = bodyless_world();

        actual.advance(0.25);
        advance_like_pre_physics(&mut expected, 0.25);

        assert_eq!(actual.physics_body_count(), 0);
        assert!(
            !actual.last_advance_used_physics,
            "a bodyless world must not enter the physics branch"
        );
        assert_eq!(actual.tick, expected.tick);
        for object_id in [1, 2, 3] {
            assert_eq!(
                actual.get_transform(object_id),
                expected.get_transform(object_id),
                "object {object_id} must retain the pre-physics behavior"
            );
            assert_eq!(
                actual.grid.position(u64::from(object_id)),
                expected.grid.position(u64::from(object_id))
            );
        }
    }

    #[test]
    fn bodied_actor_free_falls_lands_jumps_and_slides() {
        let mut falling = TransformWorld::new(100.0);
        falling.spawn_server_simulated(1, TransformState::at([0.0, 30.0, 0.0]));
        falling.set_physics(1, Some(aabb_config(100.0)));

        // No map means the actor takes the crate's gravity-only free-fall path.
        falling.advance(0.1);
        let free_fall = falling.physics_state(1).unwrap();
        assert_eq!(free_fall.position, [0.0, 30.0, 0.0]);
        assert_eq!(free_fall.velocity, [0.0, -10.0, 0.0]);
        assert!(!free_fall.grounded);

        falling.set_physics_bvh(Some(std::sync::Arc::new(StaticTriBvh::new(&floor_mesh()))));
        for _ in 0..32 {
            falling.advance(0.1);
        }
        let resting = falling.physics_state(1).unwrap();
        assert!(resting.grounded, "the floor mesh grounds the actor");
        assert!((resting.position[1] - 10.0).abs() < 0.01);
        assert_eq!(resting.velocity[1], 0.0);
        assert!(falling.authority_mut(1).unwrap().replicate_velocity);

        falling.apply_impulse(1, [0.0, 40.0, 0.0]);
        falling.advance(0.1);
        let jumping = falling.physics_state(1).unwrap();
        assert!(jumping.position[1] > resting.position[1]);
        assert!(!jumping.grounded, "leaving the floor clears grounded");

        let mut sliding = TransformWorld::new(100.0);
        let mut state = TransformState::at([-10.0, 10.0, 0.0]);
        state.velocity = [100.0, 0.0, 50.0];
        sliding.spawn_server_simulated(2, state);
        sliding.set_physics(2, Some(aabb_config(0.0)));
        sliding.set_physics_bvh(Some(std::sync::Arc::new(StaticTriBvh::new(&wall_mesh()))));
        sliding.advance(0.1);
        let wall_slide = sliding.physics_state(2).unwrap();
        assert!(wall_slide.position[0] <= -10.0);
        assert!(
            wall_slide.position[2] > 4.9,
            "motion continues along the wall"
        );
        assert_eq!(wall_slide.velocity[0], 0.0);
        assert_eq!(wall_slide.velocity[2], 50.0);
    }

    #[test]
    fn owner_predicted_actors_cannot_gain_or_run_physics_bodies() {
        let mut world = TransformWorld::new(100.0);
        let mut state = TransformState::at([1.0, 2.0, 3.0]);
        state.velocity = [100.0, 0.0, 0.0];
        world.spawn_server_simulated(1, state);
        world.assign_owner(1, 99).unwrap();

        world.set_physics(1, Some(aabb_config(980.0)));
        assert_eq!(world.physics_body_count(), 0);
        assert_eq!(world.physics_state(1), None);

        world.advance(0.25);
        assert_eq!(world.get_transform(1), Some(state));
        assert!(
            !world.last_advance_used_physics,
            "an owner-predicted actor cannot make the world enter physics"
        );
    }
}
