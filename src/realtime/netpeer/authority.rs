//! The `NetworkPeer` server authority pipeline (, design §7): the trust
//! boundary that treats every inbound `KIND_REP_DELTA` as **untrusted input**,
//! validates it, applies the validated values to the authoritative entity, and
//! **re-derives and rebroadcasts the server's own delta** — never the client's
//! bytes.
//!
//! # The pipeline (§7.1), cheap-reject-first / decode-values-last
//!
//! ```text
//! inbound KIND_REP_DELTA (conn)
//!   1. FRAME       hard caps + non-panicking decode ( codec).
//!   2. HEADER+MASK parse object_id, is_full, tokens, changed_mask — NO values yet
//!                  ([`citadel_wire::netpeer::DeltaBunch::peek_header`]).
//!   3. RESOLVE     (conn's match, object_id) -> authoritative object; unknown /
//!                  cross-match / not-registered = cheap reject before value decode.
//!   4. OWNERSHIP   server-resolved owner == conn AND every masked field is
//!                  ClientOwned; guests may not mutate persistent objects.
//!   5. RATE+BUDGET per-connection AGGREGATE token buckets (bunches/bytes/fields/
//!                  items per sec) + per-bunch hard caps; charged even on reject.
//!   6. DECODE+BOUNDS decode ONLY now, validate each value against the server's
//!                  compiled FieldBounds (finite floats, post-dequant range/clamp).
//!   7. APPLY       re-check the owner epoch under the lock (TOCTOU), write
//!                  validated values, a veto hook may veto (leaving the value
//!                  unchanged and correcting the owner).
//!   8. REBROADCAST server re-encodes ITS OWN authoritative delta to peers,
//!                  honoring COND_*; rebroadcast bytes charged to the originating
//!                  budget (no amplification).
//! ```
//!
//! Key invariant: **the server never rebroadcasts the client's bytes.** Rejected
//! fields/objects/values never reach a peer because they are rejected before step
//! 6; accepted values are applied to authoritative state and the server emits a
//! fresh, server-stamped [`DeltaBunch`] from that state.
//!
//! Rejections are **coarse and fail closed** (the [`RejectReason`](crate::realtime::auth::RejectReason)
//! posture): the fine-grained [`RepReject`] here is for metrics/tests only; the
//! gateway surfaces every reject as the same uniform drop with **no per-field
//! detail and no distinct reply**, so a client cannot use reject *content* as an
//! oracle (§7.3). Reject *timing* is only best-effort uniform — cheap rejects
//! (unknown/cross-match, before header work) return sooner than deep ones; a
//! residual timing side-channel on object/class existence is a known limitation
//! deferred to the interest/relevancy pass.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use citadel_wire::baseline::{AckField, BaselineAllocator};
use citadel_wire::interest::{InterestGrid, RelevanceSet};
use citadel_wire::netpeer::{
    DeltaBunch, FieldDelta, MAX_ENVELOPE_ALLOC, PreparedDeltaValues, RepAck, RepSchema, RepValue,
};
use citadel_wire::protocol::KIND_REP_DELTA;

use super::delta::{ObjectReplicator, RepSnapshot};
use super::layout::{FieldAuthority, FieldBounds, RepCondition, RepLayout, TypeTag};

/// The coarse, non-leaking reason a bunch was rejected. Mirrors the auth-handshake
/// posture: the *client* never learns which check failed (the gateway drops every
/// reject uniformly). This enum exists only for server metrics and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepReject {
    /// Malformed frame / caps exceeded / schema mismatch on a full snapshot.
    Frame,
    /// The connection is not joined to any match.
    NoMatch,
    /// The `object_id` is unknown to the server.
    UnknownObject,
    /// The object is not in the connection's match (cross-match spoof).
    CrossMatch,
    /// The connection is not the server-resolved owner of the object.
    NotOwner,
    /// A masked field's authority is `ServerOnly` (client may not propose it).
    ServerOnlyField,
    /// A guest attempted to mutate a persistent object.
    Guest,
    /// A per-connection aggregate rate/budget or per-bunch hard cap was exceeded.
    Rate,
    /// A decoded value violated the server's compiled `FieldBounds`.
    Bounds,
    /// A `ClientOwned` field changed faster than its minimum change interval.
    Cooldown,
    /// The bunch's `result_id` is stale (<= the highest already applied).
    Stale,
    /// A delta arrived for an `(conn, object)` with no established full-snapshot
    /// binding, or a schema/downgrade mismatch.
    SchemaBinding,
    /// The owner epoch changed between validate and apply (ownership transferred).
    Toctou,
    /// A client-owned collection delta (unsupported on the inbound path this phase).
    UnsupportedCollection,
}

/// Per-connection aggregate rate/budget caps (design §7.1 step 5, finding 27). All
/// buckets are **per connection across all objects** so many objects cannot
/// multiply the budget, plus per-bunch hard caps.
#[derive(Debug, Clone, Copy)]
pub struct RateLimits {
    /// Max inbound bunches per second.
    pub bunches_per_sec: f64,
    /// Max inbound (and rebroadcast-charged) bytes per second.
    pub bytes_per_sec: f64,
    /// Max changed fields per second.
    pub fields_per_sec: f64,
    /// Max changed collection items per second.
    pub items_per_sec: f64,
    /// Per-bunch hard cap on body bytes.
    pub max_bunch_bytes: usize,
    /// Per-bunch hard cap on changed field count.
    pub max_bunch_fields: usize,
    /// Minimum change interval per `ClientOwned` field, milliseconds (cooldown).
    pub field_cooldown_ms: u64,
}

/// Shared-grid settings for `NetworkPeer` relevancy.
///
/// Object positions are indexed in the same [`InterestGrid`] primitive used by
/// transform sync. A receiver enters an object's relevancy set at `inner` and
/// leaves only past `outer`; leaving removes its sender-side baseline, so a later
/// re-entry always receives a full snapshot.
#[derive(Debug, Clone, Copy)]
pub struct RepInterestConfig {
    /// Uniform-grid cell size in world units.
    pub cell_size: f32,
    /// Distance in world units at which an object becomes relevant.
    pub inner: f32,
    /// Distance in world units at which an object stops being relevant.
    pub outer: f32,
}

impl Default for RepInterestConfig {
    fn default() -> Self {
        Self {
            cell_size: 100.0,
            inner: 100.0,
            outer: 125.0,
        }
    }
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            bunches_per_sec: 240.0,
            bytes_per_sec: 256.0 * 1024.0,
            fields_per_sec: 2048.0,
            items_per_sec: 4096.0,
            max_bunch_bytes: 64 * 1024,
            max_bunch_fields: 1024,
            field_cooldown_ms: 0,
        }
    }
}

/// A refilling token bucket. `tokens` may go negative when charged forcibly (a
/// rebroadcast charged against the originating budget), which throttles subsequent
/// inbound work until it refills — bounding amplification (finding 28).
#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    cap: f64,
    refill_per_ms: f64,
    last_ms: u64,
}

impl Bucket {
    fn new(cap: f64, now_ms: u64) -> Self {
        Self {
            tokens: cap,
            cap,
            refill_per_ms: cap / 1000.0,
            last_ms: now_ms,
        }
    }

    fn refill(&mut self, now_ms: u64) {
        if now_ms > self.last_ms {
            let elapsed = (now_ms - self.last_ms) as f64;
            self.tokens = (self.tokens + elapsed * self.refill_per_ms).min(self.cap);
            self.last_ms = now_ms;
        }
    }

    /// Charge `amount`, returning `true` if the budget allowed it. A reject still
    /// deducts nothing (the caller charged the cheaper buckets first), but the
    /// attempt is metered by the earlier buckets.
    fn charge(&mut self, now_ms: u64, amount: f64) -> bool {
        self.refill(now_ms);
        if self.tokens >= amount {
            self.tokens -= amount;
            true
        } else {
            false
        }
    }

    /// Force-charge `amount` even past zero (rebroadcast amplification accounting).
    fn charge_forced(&mut self, now_ms: u64, amount: f64) {
        self.refill(now_ms);
        self.tokens -= amount;
    }
}

/// Per-connection budget: one bucket per aggregate dimension. The `items` bucket
/// is reserved for the inbound-collection path (rejected this phase, +);
/// its cap still participates in [`RateLimits`] so the contract is stable.
#[derive(Debug, Clone, Copy)]
struct ConnBudget {
    bunches: Bucket,
    bytes: Bucket,
    fields: Bucket,
}

impl ConnBudget {
    fn new(limits: &RateLimits, now_ms: u64) -> Self {
        Self {
            bunches: Bucket::new(limits.bunches_per_sec, now_ms),
            bytes: Bucket::new(limits.bytes_per_sec, now_ms),
            fields: Bucket::new(limits.fields_per_sec, now_ms),
        }
    }
}

/// A veto hook (the Lua/game-logic reconciliation analogue, §7.4). Given the
/// validated proposal, it returns the set of `field_id`s to **veto**: those are
/// left at their authoritative value and a correction is sent back to the owner;
/// the rest apply normally. An empty result accepts everything.
pub trait RepVetoHook: Send + Sync {
    /// Decide which proposed fields to veto.
    fn veto(&self, ctx: RepVetoContext<'_>) -> Vec<u16>;
}

/// The context passed to a [`RepVetoHook`].
#[derive(Debug)]
pub struct RepVetoContext<'a> {
    /// The proposing connection (raw participant id).
    pub conn: u64,
    /// The target object.
    pub object_id: u32,
    /// The proposed, already bounds-validated `(field_id, value)` changes.
    pub proposed: &'a [(u16, RepValue)],
}

/// An outbound replication frame the gateway must deliver to one participant.
#[derive(Debug, Clone, PartialEq)]
pub struct RepOutbound {
    /// Target participant (raw id).
    pub participant: u64,
    /// Envelope kind (`KIND_REP_DELTA`).
    pub kind: u16,
    /// Server-encoded body (never the client's bytes).
    pub body: Vec<u8>,
    /// Whether to deliver reliably (`NetworkPeer` state is reliable by default).
    pub reliable: bool,
}

/// A registered replicated class: its immutable [`RepLayout`] (authority / bounds /
/// conditions per field) plus the matching wire [`RepSchema`] (per-field codecs).
/// Both are bound so a bunch decodes bit-for-bit and its fields validate.
#[derive(Debug)]
struct RepClass {
    layout: &'static RepLayout,
    schema: RepSchema,
    /// Every layout/schema pair this class will strictly decode. The current
    /// version is always present; older versions are admitted only by the opt-in
    /// append-only registration path.
    accepted: BTreeMap<u32, (&'static RepLayout, RepSchema)>,
    min_accepted_version: u32,
    compat: bool,
}

/// One authoritative replicated object.
struct ObjectEntry {
    match_id: u64,
    class_id: u32,
    /// Bumped every time this `object_id` is (re)spawned, so a [`Validated`]
    /// captured against a since-replaced object is rejected at apply (finding 5:
    /// object-id reuse between validate and apply).
    generation: u64,
    /// The server-resolved owner (raw participant id), or `None` (server-only).
    owner: Option<u64>,
    /// Bumped on every ownership transfer; the apply step re-checks it (TOCTOU).
    owner_epoch: u64,
    /// Whether the object is persistent (guests may not mutate persistent objects).
    persistent: bool,
    /// Full authoritative state (all fields).
    authoritative: RepSnapshot,
    /// Sender-side baseline machinery for the server's own rebroadcast. Its
    /// `current` holds only the **peer-visible** projection (COND_* applied).
    replicator: ObjectReplicator,
    /// Last change time per field (ms) for the `ClientOwned` cooldown.
    field_last_change_ms: BTreeMap<u16, u64>,
}

/// Per-connection pipeline state.
struct ConnEntry {
    match_id: u64,
    is_guest: bool,
    budget: ConnBudget,
    /// `(conn, object) -> bound layout_version` established by a full snapshot; a
    /// delta requires an existing binding (finding: schema binding).
    bindings: BTreeMap<u32, u32>,
    /// Stale guard: the highest `result_id` applied per object (finding: stale).
    highest_applied: BTreeMap<u32, u64>,
    /// Position from which this connection observes replicated objects.
    viewer_pos: [f32; 3],
    /// Hysteretic relevance state over the shared object grid.
    relevance: RelevanceSet,
}

struct Inner {
    classes: BTreeMap<u32, RepClass>,
    objects: BTreeMap<u32, ObjectEntry>,
    conns: BTreeMap<u64, ConnEntry>,
    matches: BTreeMap<u64, BTreeSet<u64>>,
    alloc: BaselineAllocator,
    limits: RateLimits,
    veto: Option<Arc<dyn RepVetoHook>>,
    interest: InterestGrid,
    interest_config: RepInterestConfig,
    last_fanout_width: usize,
    /// Opt-in server-only reuse of quantized field payloads during one fan-out.
    /// Per-receiver baselines, tokens, and pending snapshots remain separate.
    shared_quantized_state: bool,
    #[cfg(test)]
    last_shared_quantize_count: usize,
}

/// The receiver-specific parts of a rebroadcast delta which determine whether its
/// already-quantized values can be reused safely. `base_id` identifies the
/// receiver's acked baseline; a full snapshot deliberately uses `0`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SharedBaselineSignature {
    is_full: bool,
    base_id: u64,
    changed_fields: Vec<u16>,
}

/// A cheap point-in-time view of NetworkPeer authority load.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepAuthorityMetrics {
    /// Total authoritative NetworkPeer objects registered across all matches.
    pub total_objects: usize,
    /// Authoritative NetworkPeer object counts keyed by match id.
    pub objects_per_match: BTreeMap<u64, usize>,
    /// Number of relevant peers that received a delta in the latest rebroadcast.
    pub last_fanout_width: usize,
}

/// A side-effect-free validation result (steps 2-6). Carries everything the apply
/// step needs, plus the owner epoch captured at validate time so apply can detect a
/// concurrent ownership transfer (TOCTOU).
#[derive(Debug, Clone)]
pub struct Validated {
    conn: u64,
    object_id: u32,
    class_id: u32,
    match_id: u64,
    generation: u64,
    is_full: bool,
    bound_layout_version: u32,
    result_id: u64,
    owner_epoch: u64,
    now_ms: u64,
    fields: Vec<(u16, RepValue)>,
}

impl Validated {
    /// The validated `(field_id, value)` proposals (bounds-checked/clamped).
    #[must_use]
    pub fn fields(&self) -> &[(u16, RepValue)] {
        &self.fields
    }

    /// The target object id.
    #[must_use]
    pub fn object_id(&self) -> u32 {
        self.object_id
    }
}

/// The `NetworkPeer` server authority: the untrusted-input trust boundary. Cheap to
/// share via `Arc<RepAuthority>`; all state lives behind one mutex so apply is
/// serialized (the "apply lock", §7.1 step 7).
pub struct RepAuthority {
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for RepAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepAuthority").finish_non_exhaustive()
    }
}

impl RepAuthority {
    /// Build an authority with the given aggregate rate/budget limits.
    #[must_use]
    pub fn new(limits: RateLimits) -> Self {
        Self::with_interest(limits, RepInterestConfig::default())
    }

    /// Build an authority with explicit shared-grid AOI settings.
    #[must_use]
    pub fn with_interest(limits: RateLimits, interest_config: RepInterestConfig) -> Self {
        Self {
            inner: Mutex::new(Inner {
                classes: BTreeMap::new(),
                objects: BTreeMap::new(),
                conns: BTreeMap::new(),
                matches: BTreeMap::new(),
                alloc: BaselineAllocator::new(),
                limits,
                veto: None,
                interest: InterestGrid::new(interest_config.cell_size),
                interest_config,
                last_fanout_width: 0,
                shared_quantized_state: false,
                #[cfg(test)]
                last_shared_quantize_count: 0,
            }),
        }
    }

    /// Opt into reuse of quantized field values during a single authoritative
    /// rebroadcast. Disabled by default because it is useful only once measured
    /// replicated-actor counts and fan-out approach roughly 100–300 actors.
    #[must_use]
    pub fn with_shared_quantized_state(self, enabled: bool) -> Self {
        if let Ok(mut g) = self.inner.lock() {
            g.shared_quantized_state = enabled;
        }
        self
    }

    /// Attach a veto hook (game-logic / Lua reconciliation, §7.4).
    #[must_use]
    pub fn with_veto(self, hook: Arc<dyn RepVetoHook>) -> Self {
        if let Ok(mut g) = self.inner.lock() {
            g.veto = Some(hook);
        }
        self
    }

    /// Register a replicated class: its layout (authority/bounds/cond) and the
    /// matching wire schema (codecs). Rejects a schema whose identity hash does not
    /// match the layout's — they must be the same class.
    ///
    /// # Errors
    /// Returns [`RepReject::SchemaBinding`] if the schema hash / layout version does
    /// not match the layout.
    pub fn register_class(
        &self,
        class_id: u32,
        layout: &'static RepLayout,
        schema: RepSchema,
    ) -> Result<(), RepReject> {
        validate_schema_binding(layout, &schema)?;
        let version = layout.layout_version();
        let mut accepted = BTreeMap::new();
        accepted.insert(version, (layout, schema.clone()));
        if let Ok(mut g) = self.inner.lock() {
            g.classes.insert(
                class_id,
                RepClass {
                    layout,
                    schema,
                    accepted,
                    min_accepted_version: version,
                    compat: false,
                },
            );
        }
        Ok(())
    }

    /// Register a class with an explicit, append-only compatibility map. Strict
    /// [`Self::register_class`] remains the default; this opt-in path accepts only
    /// the supplied older layouts, each decoded against its own exact schema.
    ///
    /// # Errors
    /// Returns [`RepReject::SchemaBinding`] when a schema/layout pair does not
    /// bind, an older version is not an exact prefix of `current_layout`, versions
    /// are duplicated/out of order, or the minimum accepted version is invalid.
    pub fn register_class_compat(
        &self,
        class_id: u32,
        current_layout: &'static RepLayout,
        current_schema: RepSchema,
        older: Vec<(&'static RepLayout, RepSchema)>,
        min_accepted_version: u32,
    ) -> Result<(), RepReject> {
        validate_schema_binding(current_layout, &current_schema)?;
        if min_accepted_version > current_layout.layout_version() {
            return Err(RepReject::Bounds);
        }

        let mut accepted = BTreeMap::new();
        for (older_layout, older_schema) in older {
            validate_schema_binding(older_layout, &older_schema)?;
            if older_layout.layout_version() >= current_layout.layout_version()
                || older_layout.fields().len() > current_layout.fields().len()
                || older_layout.fields() != &current_layout.fields()[..older_layout.fields().len()]
                || accepted
                    .insert(older_layout.layout_version(), (older_layout, older_schema))
                    .is_some()
            {
                return Err(RepReject::SchemaBinding);
            }
        }
        if let Some(&lowest) = accepted.keys().next()
            && min_accepted_version > lowest
        {
            return Err(RepReject::SchemaBinding);
        }
        if accepted
            .insert(
                current_layout.layout_version(),
                (current_layout, current_schema.clone()),
            )
            .is_some()
        {
            return Err(RepReject::SchemaBinding);
        }

        if let Ok(mut g) = self.inner.lock() {
            g.classes.insert(
                class_id,
                RepClass {
                    layout: current_layout,
                    schema: current_schema,
                    accepted,
                    min_accepted_version,
                    compat: true,
                },
            );
        }
        Ok(())
    }

    /// Join a connection to a match (matches land with ; this is the
    /// scoping seam the pipeline resolves against). `is_guest` connections may only
    /// mutate ephemeral (non-persistent) objects.
    pub fn join_match(&self, conn: u64, match_id: u64, is_guest: bool) {
        if let Ok(mut g) = self.inner.lock() {
            let now = 0;
            let limits = g.limits;
            g.matches.entry(match_id).or_default().insert(conn);
            g.conns.entry(conn).or_insert_with(|| ConnEntry {
                match_id,
                is_guest,
                budget: ConnBudget::new(&limits, now),
                bindings: BTreeMap::new(),
                highest_applied: BTreeMap::new(),
                viewer_pos: [0.0; 3],
                relevance: RelevanceSet::new(),
            });
        }
    }

    /// Drop a connection (disconnect / relevancy exit).
    pub fn leave(&self, conn: u64) {
        if let Ok(mut g) = self.inner.lock() {
            if let Some(entry) = g.conns.remove(&conn)
                && let Some(members) = g.matches.get_mut(&entry.match_id)
            {
                members.remove(&conn);
            }
            for obj in g.objects.values_mut() {
                obj.replicator.remove_receiver(conn);
            }
        }
    }

    /// Set the world position used to determine which objects `conn` can receive.
    /// Returns false when the connection has not joined a match.
    pub fn set_viewer_position(&self, conn: u64, pos: [f32; 3]) -> bool {
        let Ok(mut g) = self.inner.lock() else {
            return false;
        };
        let Some(entry) = g.conns.get_mut(&conn) else {
            return false;
        };
        entry.viewer_pos = pos;
        refresh_relevance(&mut g);
        true
    }

    /// Set an authoritative object's world position in the shared interest grid.
    /// Returns false when `object_id` is unknown.
    pub fn set_object_position(&self, object_id: u32, pos: [f32; 3]) -> bool {
        let Ok(mut g) = self.inner.lock() else {
            return false;
        };
        if !g.objects.contains_key(&object_id) {
            return false;
        }
        g.interest.insert_or_move(u64::from(object_id), pos);
        refresh_relevance(&mut g);
        true
    }

    /// Despawn an authoritative object and clear its interest-grid entry.
    ///
    /// The next spawn with this id starts with fresh connection bindings and a
    /// fresh sender baseline, so it cannot inherit a pre-despawn delta chain.
    pub fn despawn_object(&self, object_id: u32) -> bool {
        let Ok(mut g) = self.inner.lock() else {
            return false;
        };
        if g.objects.remove(&object_id).is_none() {
            return false;
        }
        g.interest.remove(u64::from(object_id));
        for conn in g.conns.values_mut() {
            conn.bindings.remove(&object_id);
            conn.highest_applied.remove(&object_id);
        }
        refresh_relevance(&mut g);
        true
    }

    /// Spawn an authoritative object owned by `owner` in `match_id` of class
    /// `class_id`, with `initial` authoritative state.
    ///
    /// # Errors
    /// Returns [`RepReject::UnknownObject`] if `class_id` is not registered.
    pub fn spawn_object(
        &self,
        object_id: u32,
        match_id: u64,
        class_id: u32,
        owner: Option<u64>,
        persistent: bool,
        initial: RepSnapshot,
    ) -> Result<(), RepReject> {
        let mut g = self.inner.lock().map_err(|_| RepReject::Frame)?;
        let schema = g
            .classes
            .get(&class_id)
            .ok_or(RepReject::UnknownObject)?
            .schema
            .clone();
        let mut replicator = ObjectReplicator::new(object_id, schema);
        // Seed the peer-visible projection with the initial state so a newly
        // relevant peer's first (full) snapshot carries current values.
        let layout = g.classes.get(&class_id).map(|c| c.layout);
        if let Some(layout) = layout {
            for (&fid, value) in &initial.scalars {
                if layout
                    .field(fid)
                    .map(|d| peer_visible(d.cond))
                    .unwrap_or(false)
                {
                    replicator.current_mut().set_scalar(fid, value.clone());
                }
            }
        }
        // A re-spawn of a live id gets a fresh generation so any in-flight
        // Validated captured against the old object is rejected at apply.
        let generation = g
            .objects
            .get(&object_id)
            .map(|e| e.generation + 1)
            .unwrap_or(1);
        // A new object generation must not inherit the previous object's per-
        // connection baseline/binding state keyed by this id, or a legitimate fresh
        // delta would be dropped as stale (or a stale binding would skip the required
        // full snapshot). Reset it for every connection (review finding).
        if generation > 1 {
            for conn_entry in g.conns.values_mut() {
                conn_entry.highest_applied.remove(&object_id);
                conn_entry.bindings.remove(&object_id);
            }
        }
        // The caller may move it immediately with `set_object_position`; a stable
        // origin default preserves the existing single-room behavior until then.
        g.interest.insert_or_move(u64::from(object_id), [0.0; 3]);
        g.objects.insert(
            object_id,
            ObjectEntry {
                match_id,
                class_id,
                generation,
                owner,
                owner_epoch: 1,
                persistent,
                authoritative: initial,
                replicator,
                field_last_change_ms: BTreeMap::new(),
            },
        );
        refresh_relevance(&mut g);
        Ok(())
    }

    /// Transfer ownership of `object_id` to `new_owner`, bumping the owner epoch so
    /// an in-flight [`Validated`] captured under the old owner is rejected at apply
    /// (TOCTOU, finding 21). Returns the new epoch, or `None` if unknown.
    pub fn assign_owner(&self, object_id: u32, new_owner: Option<u64>) -> Option<u64> {
        let mut g = self.inner.lock().ok()?;
        let obj = g.objects.get_mut(&object_id)?;
        obj.owner = new_owner;
        obj.owner_epoch += 1;
        Some(obj.owner_epoch)
    }

    /// The authoritative value of a scalar field (tests / diagnostics).
    #[must_use]
    pub fn authoritative_scalar(&self, object_id: u32, field_id: u16) -> Option<RepValue> {
        let g = self.inner.lock().ok()?;
        g.objects
            .get(&object_id)
            .and_then(|o| o.authoritative.scalars.get(&field_id).cloned())
    }

    /// Snapshot NetworkPeer object and fan-out telemetry. This samples the maps
    /// already held by the authority lock; it does not add per-field hot-path work.
    #[must_use]
    pub fn metrics(&self) -> RepAuthorityMetrics {
        let Ok(g) = self.inner.lock() else {
            return RepAuthorityMetrics::default();
        };
        let mut objects_per_match = BTreeMap::new();
        for object in g.objects.values() {
            *objects_per_match.entry(object.match_id).or_insert(0) += 1;
        }
        RepAuthorityMetrics {
            total_objects: g.objects.len(),
            objects_per_match,
            last_fanout_width: g.last_fanout_width,
        }
    }

    /// The remaining byte-budget tokens for a connection (tests: amplification).
    #[must_use]
    pub fn bytes_tokens(&self, conn: u64) -> Option<f64> {
        let g = self.inner.lock().ok()?;
        g.conns.get(&conn).map(|c| c.budget.bytes.tokens)
    }

    /// The full pipeline for one inbound `KIND_REP_DELTA`: validate then, on
    /// success, apply + rebroadcast. Returns the server-stamped outbound frames
    /// (empty on any reject — the coarse, uniform, no-oracle outcome).
    pub fn handle_delta(&self, conn: u64, body: &[u8], now_ms: u64) -> Vec<RepOutbound> {
        match self.validate(conn, body, now_ms) {
            Ok(validated) => self.apply_and_rebroadcast(validated).unwrap_or_default(),
            Err(_reject) => Vec::new(),
        }
    }

    /// Steps 2-6: schema/resolve/ownership/rate/bounds. Side-effect-free on
    /// authoritative state (it charges the rate buckets — reject is metered — but
    /// never mutates an object). Returns a [`Validated`] proposal or a coarse
    /// [`RepReject`].
    ///
    /// # Errors
    /// Returns the coarse [`RepReject`] for the first failing check.
    pub fn validate(&self, conn: u64, body: &[u8], now_ms: u64) -> Result<Validated, RepReject> {
        let mut g = self.inner.lock().map_err(|_| RepReject::Frame)?;

        // Per-bunch hard byte cap (cheap, before anything else).
        if body.len() > g.limits.max_bunch_bytes {
            return Err(RepReject::Rate);
        }

        // Charge the cheap aggregate buckets first (bunches, bytes) — metered even
        // if a later check rejects, so probing is not free.
        {
            let limits = g.limits;
            let conn_entry = g.conns.get_mut(&conn).ok_or(RepReject::NoMatch)?;
            if !conn_entry.budget.bunches.charge(now_ms, 1.0) {
                return Err(RepReject::Rate);
            }
            if !conn_entry.budget.bytes.charge(now_ms, body.len() as f64) {
                return Err(RepReject::Rate);
            }
            let _ = limits;
        }

        // Step 3 RESOLVE: object_id -> object, scoped to the conn's match.
        let object_id = DeltaBunch::peek_object_id(body).ok_or(RepReject::Frame)?;
        let conn_match = g
            .conns
            .get(&conn)
            .map(|c| c.match_id)
            .ok_or(RepReject::NoMatch)?;
        let conn_guest = g.conns.get(&conn).map(|c| c.is_guest).unwrap_or(true);
        let (class_id, obj_owner, obj_persistent, owner_epoch, generation, last_change) = {
            let obj = g.objects.get(&object_id).ok_or(RepReject::UnknownObject)?;
            if obj.match_id != conn_match {
                return Err(RepReject::CrossMatch);
            }
            (
                obj.class_id,
                obj.owner,
                obj.persistent,
                obj.owner_epoch,
                obj.generation,
                obj.field_last_change_ms.clone(),
            )
        };

        // Select one exact schema before parsing the remainder of the header. A
        // full snapshot declares its version; a delta is pinned to the version a
        // prior full snapshot established for this (conn, object). No relaxed
        // current-schema decode is ever attempted.
        let is_full = DeltaBunch::peek_is_full(body).ok_or(RepReject::Frame)?;
        let (schema, layout, bound_layout_version) = {
            let class = g.classes.get(&class_id).ok_or(RepReject::UnknownObject)?;
            if !class.compat {
                // Preserve strict mode's original path and ordering exactly: every
                // full snapshot is parsed against the one current schema, so a
                // mismatched embedded hash/version remains a frame reject.
                (
                    class.schema.clone(),
                    class.layout,
                    class.layout.layout_version(),
                )
            } else {
                let version = if is_full {
                    let (_, version) =
                        DeltaBunch::peek_full_schema(body).ok_or(RepReject::Frame)?;
                    if version < class.min_accepted_version {
                        return Err(RepReject::SchemaBinding);
                    }
                    version
                } else {
                    let bound = g
                        .conns
                        .get(&conn)
                        .and_then(|entry| entry.bindings.get(&object_id).copied())
                        .ok_or(RepReject::SchemaBinding)?;
                    if bound < class.min_accepted_version {
                        return Err(RepReject::SchemaBinding);
                    }
                    bound
                };
                let (layout, schema) = class
                    .accepted
                    .get(&version)
                    .ok_or(RepReject::SchemaBinding)?;
                (schema.clone(), *layout, version)
            }
        };

        // Step 2 HEADER+MASK: parse header + changed_mask, NO value decode yet.
        let header = DeltaBunch::peek_header(body, &schema).map_err(|_| RepReject::Frame)?;

        // Per-bunch field cap.
        if header.changed_fields.len() > g.limits.max_bunch_fields {
            return Err(RepReject::Rate);
        }

        // Step 4 OWNERSHIP: server-resolved owner + every masked field ClientOwned.
        if obj_owner != Some(conn) {
            return Err(RepReject::NotOwner);
        }
        if conn_guest && obj_persistent {
            return Err(RepReject::Guest);
        }
        for &fid in &header.changed_fields {
            let desc = layout.field(fid).ok_or(RepReject::Frame)?;
            if desc.authority != FieldAuthority::ClientOwned {
                return Err(RepReject::ServerOnlyField);
            }
            // Reject collection-coded fields BEFORE any value/collection decode
            // (finding 3): inbound client collections are unsupported this phase, so
            // an attacker cannot force collection allocation work then be rejected.
            if schema
                .field(fid)
                .map(|c| c.is_collection())
                .unwrap_or(false)
            {
                return Err(RepReject::UnsupportedCollection);
            }
        }

        // Schema binding + stale guard.
        {
            let conn_entry = g.conns.get(&conn).ok_or(RepReject::NoMatch)?;
            if !header.is_full {
                // A delta must diff against an established full-snapshot baseline for
                // this (conn, object), pinned to the bound layout version — a missing
                // or downgraded binding is rejected (finding 2: schema binding).
                match conn_entry.bindings.get(&object_id) {
                    Some(&bound) if bound == bound_layout_version => {}
                    _ => return Err(RepReject::SchemaBinding),
                }
            }
            let highest = conn_entry
                .highest_applied
                .get(&object_id)
                .copied()
                .unwrap_or(0);
            if header.result_id <= highest {
                return Err(RepReject::Stale);
            }
        }

        // Step 5 (cont.): charge the changed-fields bucket.
        {
            let conn_entry = g.conns.get_mut(&conn).ok_or(RepReject::NoMatch)?;
            if !conn_entry
                .budget
                .fields
                .charge(now_ms, header.changed_fields.len() as f64)
            {
                return Err(RepReject::Rate);
            }
        }

        // Step 6 DECODE+BOUNDS: only now decode values (all masked fields are owned
        // + ClientOwned), then validate/clamp each against the compiled bounds.
        let mut alloc_budget = MAX_ENVELOPE_ALLOC;
        let bunch =
            DeltaBunch::decode(body, &schema, &mut alloc_budget).map_err(|_| RepReject::Frame)?;

        let cooldown_ms = g.limits.field_cooldown_ms;
        let mut fields: Vec<(u16, RepValue)> = Vec::with_capacity(bunch.changes.len());
        for (&fid, delta) in &bunch.changes {
            let desc = layout.field(fid).ok_or(RepReject::Frame)?;
            match delta {
                FieldDelta::Collection(_) => return Err(RepReject::UnsupportedCollection),
                FieldDelta::Value(v) => {
                    let validated = validate_bounds(v, desc.bounds, desc.type_tag)?;
                    // Cooldown: a ClientOwned field must not change faster than its
                    // minimum interval (finding 31).
                    if cooldown_ms > 0
                        && let Some(&last) = last_change.get(&fid)
                        && now_ms.saturating_sub(last) < cooldown_ms
                    {
                        return Err(RepReject::Cooldown);
                    }
                    fields.push((fid, validated));
                }
            }
        }

        Ok(Validated {
            conn,
            object_id,
            class_id,
            match_id: conn_match,
            generation,
            is_full: header.is_full,
            bound_layout_version,
            result_id: header.result_id,
            owner_epoch,
            now_ms,
            fields,
        })
    }

    /// Step 7 + 8: re-check the owner epoch under the apply lock (TOCTOU), write the
    /// validated values to authoritative state (a veto hook may veto), then
    /// **re-derive and rebroadcast the server's own delta** to peers and correct the
    /// owner for any vetoed field. Rebroadcast bytes are charged to the originating
    /// connection's budget (no amplification).
    ///
    /// # Errors
    /// Returns [`RepReject::Toctou`] if ownership changed since validate, or a coarse
    /// reject if the object/class vanished.
    pub fn apply_and_rebroadcast(&self, v: Validated) -> Result<Vec<RepOutbound>, RepReject> {
        let mut g = self.inner.lock().map_err(|_| RepReject::Frame)?;

        // TOCTOU re-check under the lock: the object must be the SAME object
        // (generation), same match/class, still owned by this conn at the same
        // epoch, and the guest/persistent invariant must still hold (findings
        // 1/5/8). Any drift since validate rejects the whole apply.
        {
            let obj = g
                .objects
                .get(&v.object_id)
                .ok_or(RepReject::UnknownObject)?;
            if obj.generation != v.generation
                || obj.match_id != v.match_id
                || obj.class_id != v.class_id
                || obj.owner != Some(v.conn)
                || obj.owner_epoch != v.owner_epoch
            {
                return Err(RepReject::Toctou);
            }
            let is_guest = g.conns.get(&v.conn).map(|c| c.is_guest).unwrap_or(true);
            if is_guest && obj.persistent {
                return Err(RepReject::Guest);
            }
        }

        // Stale re-check under the apply lock (finding 1): two validated deltas
        // could both pass validate before either recorded `highest_applied`; here we
        // reject any whose token is not strictly newer than what has been applied,
        // and only ever advance the guard monotonically.
        {
            let conn_entry = g.conns.get(&v.conn).ok_or(RepReject::NoMatch)?;
            let highest = conn_entry
                .highest_applied
                .get(&v.object_id)
                .copied()
                .unwrap_or(0);
            if v.result_id <= highest {
                return Err(RepReject::Stale);
            }
        }

        // Veto hook (§7.4): decide which fields to leave unchanged. A panicking hook
        // is caught and fails closed (veto everything), never poisoning the lock
        // (finding 8).
        let vetoed: BTreeSet<u16> = match g.veto.clone() {
            Some(hook) => {
                let ctx_fields = v.fields.clone();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hook.veto(RepVetoContext {
                        conn: v.conn,
                        object_id: v.object_id,
                        proposed: &ctx_fields,
                    })
                }));
                match result {
                    Ok(vetoed) => vetoed.into_iter().collect(),
                    Err(_) => {
                        tracing::warn!("rep veto hook panicked; failing closed (veto all)");
                        v.fields.iter().map(|(fid, _)| *fid).collect()
                    }
                }
            }
            None => BTreeSet::new(),
        };

        let layout = g
            .classes
            .get(&v.class_id)
            .ok_or(RepReject::UnknownObject)?
            .layout;

        // Apply non-vetoed values to authoritative state + peer-visible projection.
        {
            let obj = g
                .objects
                .get_mut(&v.object_id)
                .ok_or(RepReject::UnknownObject)?;
            for (fid, value) in &v.fields {
                if vetoed.contains(fid) {
                    continue;
                }
                obj.authoritative.set_scalar(*fid, value.clone());
                obj.field_last_change_ms.insert(*fid, v.now_ms);
                if layout
                    .field(*fid)
                    .map(|d| peer_visible(d.cond))
                    .unwrap_or(false)
                {
                    obj.replicator.current_mut().set_scalar(*fid, value.clone());
                }
            }
        }

        // Record the stale guard + schema binding now that the bunch applied.
        if let Some(conn_entry) = g.conns.get_mut(&v.conn) {
            conn_entry.highest_applied.insert(v.object_id, v.result_id);
            if v.is_full {
                conn_entry
                    .bindings
                    .insert(v.object_id, v.bound_layout_version);
            }
        }

        // Step 8 REBROADCAST: server re-encodes ITS OWN delta only to peers that
        // are relevant in the shared InterestGrid. Refresh first so exits drop
        // receiver baselines and a later re-entry begins with a full snapshot.
        refresh_relevance(&mut g);
        let match_id = g.objects.get(&v.object_id).map(|o| o.match_id).unwrap_or(0);
        let peers: Vec<u64> = g
            .matches
            .get(&match_id)
            .map(|m| {
                m.iter()
                    .copied()
                    .filter(|&c| {
                        c != v.conn
                            && g.conns
                                .get(&c)
                                .is_some_and(|conn| conn.relevance.contains(u64::from(v.object_id)))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut out: Vec<RepOutbound> = Vec::new();
        let schema = g
            .classes
            .get(&v.class_id)
            .ok_or(RepReject::UnknownObject)?
            .schema
            .clone();

        // Amplification cap (finding 4/28): charge each rebroadcast body to the
        // originating connection's byte budget INLINE, and stop the fan-out once a
        // single inbound delta has spent more than one second's worth of that
        // budget. A tiny owner delta in a huge match can therefore never balloon
        // into unbounded per-peer allocation or OOM.
        let amp_cap = g.limits.bytes_per_sec;
        let mut amp_spent = 0.0f64;
        let mut fanout_width = 0usize;
        let shared_quantized_state = g.shared_quantized_state;
        let mut shared_values: BTreeMap<SharedBaselineSignature, PreparedDeltaValues> =
            BTreeMap::new();
        #[cfg(test)]
        let mut shared_quantize_count = 0usize;
        for peer in peers {
            if amp_spent > amp_cap {
                break;
            }
            // Split the borrow: allocator + object + conns (for inline charging).
            let Inner {
                objects,
                alloc,
                conns,
                ..
            } = &mut *g;
            let Some(obj) = objects.get_mut(&v.object_id) else {
                continue;
            };
            obj.replicator.add_receiver(peer);
            if let Some(built) = obj.replicator.build_delta(peer, alloc) {
                let body = if shared_quantized_state {
                    let signature = SharedBaselineSignature {
                        is_full: built.bunch.is_full,
                        base_id: built.bunch.base_id,
                        changed_fields: built.bunch.changes.keys().copied().collect(),
                    };
                    if !shared_values.contains_key(&signature) {
                        #[cfg(test)]
                        {
                            shared_quantize_count += 1;
                        }
                        match built.bunch.prepare_values(&schema) {
                            Ok(prepared) => {
                                shared_values.insert(signature.clone(), prepared);
                            }
                            Err(_) => continue,
                        }
                    }
                    shared_values
                        .get(&signature)
                        .ok_or(())
                        .and_then(|prepared| {
                            built
                                .bunch
                                .encode_with_prepared_values(&schema, prepared)
                                .map_err(|_| ())
                        })
                } else {
                    built.bunch.encode(&schema).map_err(|_| ())
                };
                match body {
                    Ok(body) => {
                        let bytes = body.len() as f64;
                        amp_spent += bytes;
                        if let Some(conn_entry) = conns.get_mut(&v.conn) {
                            conn_entry.budget.bytes.charge_forced(v.now_ms, bytes);
                        }
                        out.push(RepOutbound {
                            participant: peer,
                            kind: KIND_REP_DELTA,
                            body,
                            reliable: true,
                        });
                        fanout_width += 1;
                    }
                    Err(_) => {
                        // A server encode error never poisons the whole apply.
                    }
                }
            }
        }
        #[cfg(test)]
        {
            g.last_shared_quantize_count = shared_quantize_count;
        }
        g.last_fanout_width = fanout_width;
        tracing::debug!(
            object_id = v.object_id,
            fanout_width,
            "NetworkPeer authoritative delta rebroadcast"
        );

        // Veto correction to the owner (RepNotify analogue): a full snapshot of the
        // vetoed fields' authoritative (unchanged) values, so the cheating client
        // sees its illegal change corrected.
        if !vetoed.is_empty() {
            let (correction, result_id) = {
                let Inner { objects, alloc, .. } = &mut *g;
                let obj = objects.get(&v.object_id).ok_or(RepReject::UnknownObject)?;
                let result = alloc.allocate().map_err(|_| RepReject::Frame)?;
                let mut bunch = DeltaBunch::new(v.object_id, true, result.get(), 0);
                for fid in &vetoed {
                    if let Some(value) = obj.authoritative.scalars.get(fid) {
                        bunch.set(*fid, FieldDelta::Value(value.clone()));
                    }
                }
                (bunch, result.get())
            };
            let _ = result_id;
            if let Ok(body) = correction.encode(&schema) {
                // Charge the owner correction to the originating budget too.
                if let Some(conn_entry) = g.conns.get_mut(&v.conn) {
                    conn_entry
                        .budget
                        .bytes
                        .charge_forced(v.now_ms, body.len() as f64);
                }
                out.push(RepOutbound {
                    participant: v.conn,
                    kind: KIND_REP_DELTA,
                    body,
                    reliable: true,
                });
            }
        }

        Ok(out)
    }

    #[cfg(test)]
    fn last_shared_quantize_count(&self) -> usize {
        self.inner
            .lock()
            .map(|g| g.last_shared_quantize_count)
            .unwrap_or_default()
    }

    /// Handle an inbound `KIND_REP_ACK`: advance the server's rebroadcast baselines
    /// for the acked objects (the client acking server-stamped deltas). Malformed
    /// bodies are dropped.
    pub fn handle_ack(&self, conn: u64, body: &[u8]) {
        let Ok(ack) = RepAck::decode(body) else {
            return;
        };
        let Ok(mut g) = self.inner.lock() else {
            return;
        };
        for entry in ack.entries {
            if let Some(obj) = g.objects.get_mut(&entry.object_id) {
                let mut field = AckField::new();
                field.ack(entry.acked_result_id);
                obj.replicator.on_ack(conn, &field);
            }
        }
    }
}

/// Refresh every connection's relevance set over the common object grid and
/// invalidate sender baselines for every exited object. The grid is global today,
/// so match scoping remains the caller's fan-out condition; an object from another
/// match may be tracked in a set but can never be sent across the match boundary.
fn refresh_relevance(g: &mut Inner) {
    let config = g.interest_config;
    let mut exited = Vec::new();
    for (&conn, entry) in &mut g.conns {
        let delta =
            entry
                .relevance
                .update(&g.interest, entry.viewer_pos, config.inner, config.outer);
        exited.extend(delta.exited.into_iter().map(|id| (conn, id as u32)));
    }
    for (conn, object_id) in exited {
        if let Some(obj) = g.objects.get_mut(&object_id) {
            obj.replicator.remove_receiver(conn);
        }
    }
}

/// Whether a field with condition `cond` is visible to a peer (a non-owner viewing
/// the object as a simulated proxy) in the single-room rebroadcast. Full per-role
/// interest filtering is ; this is the coarse projection.
fn peer_visible(cond: RepCondition) -> bool {
    matches!(
        cond,
        RepCondition::None | RepCondition::SkipOwner | RepCondition::SimulatedOnly
    )
}

/// Validate (and, for near-boundary scalars, clamp) a decoded value against the
/// server's compiled bounds (design §4, §7.1 step 6). Rejects NaN/Inf, gross
/// out-of-range, type mismatch, and over-length blobs.
fn validate_bounds(
    value: &RepValue,
    bounds: FieldBounds,
    type_tag: TypeTag,
) -> Result<RepValue, RepReject> {
    match (value, type_tag) {
        (RepValue::Bool(_), TypeTag::Bool) => Ok(value.clone()),
        (RepValue::Int(i), TypeTag::Int | TypeTag::Uint | TypeTag::Enum) => {
            if let FieldBounds::IntRange { min, max } = bounds
                && (*i < min || *i > max)
            {
                return Err(RepReject::Bounds);
            }
            Ok(RepValue::Int(*i))
        }
        (RepValue::Scalar(f), TypeTag::Scalar) => {
            if !f.is_finite() {
                return Err(RepReject::Bounds);
            }
            if let FieldBounds::ScalarRange {
                min,
                max,
                values_per_unit,
            } = bounds
            {
                // Fail closed on a degenerate range so `clamp` can never panic
                // (register_class also rejects this at setup; defense in depth).
                if !min.is_finite() || !max.is_finite() || min > max {
                    return Err(RepReject::Bounds);
                }
                let step = if values_per_unit == 0 {
                    0.0
                } else {
                    (max - min) / values_per_unit as f32
                };
                if *f < min - step || *f > max + step {
                    return Err(RepReject::Bounds);
                }
                // Clamp a near-boundary (post-dequantization rounding) value into
                // the exact range (finding 25): a legal wire code that rounds to
                // max + epsilon is corrected, not rejected.
                return Ok(RepValue::Scalar(f.clamp(min, max)));
            }
            Ok(RepValue::Scalar(*f))
        }
        (RepValue::Vector3(p), TypeTag::Vector3) => {
            if p.iter().any(|c| !c.is_finite()) {
                return Err(RepReject::Bounds);
            }
            Ok(value.clone())
        }
        (RepValue::Quat(q), TypeTag::Quat) => {
            if q.iter().any(|c| !c.is_finite()) {
                return Err(RepReject::Bounds);
            }
            Ok(value.clone())
        }
        (RepValue::Bytes(b), TypeTag::Bytes) => {
            if let FieldBounds::MaxLen { max_len } = bounds
                && b.len() > max_len as usize
            {
                return Err(RepReject::Bounds);
            }
            Ok(value.clone())
        }
        // Any value/type mismatch is a hostile/ malformed proposal.
        _ => Err(RepReject::Bounds),
    }
}

fn validate_schema_binding(layout: &RepLayout, schema: &RepSchema) -> Result<(), RepReject> {
    // Bind schema <-> layout: same identity, same field count.
    if schema.schema_hash() != layout.schema_hash() || schema.num_fields() != layout.len() {
        return Err(RepReject::SchemaBinding);
    }
    // Fail closed on malformed server-side bounds so the hot path (validate_bounds)
    // can never panic on a degenerate range (finding 8).
    for field in layout.fields() {
        match field.bounds {
            FieldBounds::IntRange { min, max } if min > max => return Err(RepReject::Bounds),
            FieldBounds::ScalarRange { min, max, .. }
                if !min.is_finite() || !max.is_finite() || min > max =>
            {
                return Err(RepReject::Bounds);
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::float_cmp)]
mod tests {
    use super::*;
    use crate::realtime::netpeer::layout::{RepLayoutBuilder, TypeTag};
    use citadel_wire::codec::{ScalarQuant, codec_id};
    use citadel_wire::netpeer::{RepAck, RepAckEntry, RepFieldCodec};
    use std::sync::OnceLock;

    const OBJ: u32 = 100;
    const MATCH: u64 = 1;
    const OWNER: u64 = 1; // participant A (owner)
    const PEER: u64 = 2; // participant B (peer receiver)
    const FAR_PEER: u64 = 3; // participant C (outside the object's AOI)
    const EXTRA_PEER: u64 = 4; // participant D (additional peer receiver)
    const CLASS: u32 = 7;
    const COMPAT_CLASS: u32 = 8;
    const COMPAT_OBJ: u32 = 200;
    const F_COMPAT_HEALTH: u16 = 0;
    const F_COMPAT_EMOTE: u16 = 1;
    const F_COMPAT_CRITICAL: u16 = 2;

    // Fields: 0 health(Int 0..100, ClientOwned, None), 1 team(Int 0..8, ServerOnly,
    // None), 2 emote(Scalar 0..1, ClientOwned, None), 3 secret(Int 0..100,
    // ClientOwned, OwnerOnly), 4 name(Bytes<=16, ClientOwned, None).
    const F_HEALTH: u16 = 0;
    const F_TEAM: u16 = 1;
    const F_SECRET: u16 = 3;

    fn layout() -> &'static RepLayout {
        static L: OnceLock<RepLayout> = OnceLock::new();
        L.get_or_init(|| {
            RepLayoutBuilder::new(CLASS, 1)
                .field(
                    "health",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .field(
                    "team",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::IntRange { min: 0, max: 8 },
                    true,
                )
                .field(
                    "emote",
                    TypeTag::Scalar,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::ScalarRange {
                        min: 0.0,
                        max: 1.0,
                        values_per_unit: 1024,
                    },
                    true,
                )
                .field(
                    "secret",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::OwnerOnly,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .field(
                    "name",
                    TypeTag::Bytes,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::MaxLen { max_len: 16 },
                    false,
                )
                .build()
                .unwrap()
        })
    }

    fn schema() -> RepSchema {
        RepSchema::new(
            *layout().schema_hash(),
            vec![
                RepFieldCodec::IntRange { min: 0, max: 100 },
                RepFieldCodec::IntRange { min: 0, max: 8 },
                RepFieldCodec::Scalar(ScalarQuant::new(0.0, 1.0, 1024).unwrap()),
                RepFieldCodec::IntRange { min: 0, max: 100 },
                RepFieldCodec::Bytes { max_len: 16 },
            ],
        )
        .unwrap()
    }

    fn compat_v1_layout() -> &'static RepLayout {
        static LAYOUT: OnceLock<RepLayout> = OnceLock::new();
        LAYOUT.get_or_init(|| {
            RepLayoutBuilder::new(COMPAT_CLASS, 1)
                .field(
                    "health",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .build()
                .unwrap()
        })
    }

    fn compat_v2_layout() -> &'static RepLayout {
        static LAYOUT: OnceLock<RepLayout> = OnceLock::new();
        LAYOUT.get_or_init(|| {
            RepLayoutBuilder::new(COMPAT_CLASS, 2)
                .field(
                    "health",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .field(
                    "emote",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 8 },
                    true,
                )
                .field(
                    "critical",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .build()
                .unwrap()
        })
    }

    fn compat_v3_layout() -> &'static RepLayout {
        static LAYOUT: OnceLock<RepLayout> = OnceLock::new();
        LAYOUT.get_or_init(|| {
            RepLayoutBuilder::new(COMPAT_CLASS, 3)
                .field(
                    "health",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .field(
                    "emote",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 8 },
                    true,
                )
                .field(
                    "critical",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .field(
                    "v3_extra",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::IntRange { min: 0, max: 8 },
                    true,
                )
                .build()
                .unwrap()
        })
    }

    fn compat_nonprefix_v1_layout() -> &'static RepLayout {
        static LAYOUT: OnceLock<RepLayout> = OnceLock::new();
        LAYOUT.get_or_init(|| {
            RepLayoutBuilder::new(COMPAT_CLASS, 1)
                .field(
                    "health_changed",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::IntRange { min: 0, max: 100 },
                    true,
                )
                .build()
                .unwrap()
        })
    }

    fn compat_schema(layout: &RepLayout) -> RepSchema {
        let codecs = layout
            .fields()
            .iter()
            .map(|field| match field.id {
                F_COMPAT_HEALTH | F_COMPAT_CRITICAL => RepFieldCodec::IntRange { min: 0, max: 100 },
                _ => RepFieldCodec::IntRange { min: 0, max: 8 },
            })
            .collect();
        RepSchema::new(*layout.schema_hash(), codecs).unwrap()
    }

    fn compat_bunch(
        schema: &RepSchema,
        is_full: bool,
        result_id: u64,
        fields: &[(u16, RepValue)],
    ) -> Vec<u8> {
        let mut bunch =
            DeltaBunch::new(COMPAT_OBJ, is_full, result_id, if is_full { 0 } else { 1 });
        for (field_id, value) in fields {
            bunch.set(*field_id, FieldDelta::Value(value.clone()));
        }
        bunch.encode(schema).unwrap()
    }

    fn compat_authority() -> RepAuthority {
        let authority = RepAuthority::new(RateLimits::default());
        authority
            .register_class_compat(
                COMPAT_CLASS,
                compat_v2_layout(),
                compat_schema(compat_v2_layout()),
                vec![(compat_v1_layout(), compat_schema(compat_v1_layout()))],
                1,
            )
            .unwrap();
        let mut initial = RepSnapshot::new();
        initial.set_scalar(F_COMPAT_HEALTH, RepValue::Int(100));
        initial.set_scalar(F_COMPAT_EMOTE, RepValue::Int(4));
        initial.set_scalar(F_COMPAT_CRITICAL, RepValue::Int(99));
        authority
            .spawn_object(COMPAT_OBJ, MATCH, COMPAT_CLASS, Some(OWNER), false, initial)
            .unwrap();
        authority.join_match(OWNER, MATCH, false);
        authority.join_match(PEER, MATCH, false);
        authority
    }

    /// A ready authority with the class registered, an object owned by A in MATCH,
    /// and A + B joined to MATCH.
    fn authority() -> RepAuthority {
        authority_with(RateLimits::default(), false)
    }

    fn authority_with(limits: RateLimits, persistent: bool) -> RepAuthority {
        authority_with_shared_state(limits, persistent, false)
    }

    fn authority_with_shared_state(
        limits: RateLimits,
        persistent: bool,
        shared_quantized_state: bool,
    ) -> RepAuthority {
        let a = RepAuthority::new(limits).with_shared_quantized_state(shared_quantized_state);
        a.register_class(CLASS, layout(), schema()).unwrap();
        a.spawn_object(
            OBJ,
            MATCH,
            CLASS,
            Some(OWNER),
            persistent,
            RepSnapshot::new(),
        )
        .unwrap();
        a.join_match(OWNER, MATCH, false);
        a.join_match(PEER, MATCH, false);
        a
    }

    fn ack_receiver(authority: &RepAuthority, receiver: u64, result_id: u64) {
        authority.handle_ack(
            receiver,
            &RepAck {
                entries: vec![RepAckEntry {
                    object_id: OBJ,
                    acked_result_id: result_id,
                    history: 0,
                }],
            }
            .encode()
            .unwrap(),
        );
    }

    fn outgoing_by_receiver(out: &[RepOutbound]) -> BTreeMap<u64, DeltaBunch> {
        out.iter()
            .map(|frame| (frame.participant, decode(&frame.body)))
            .collect()
    }

    fn outgoing_bodies(out: &[RepOutbound]) -> BTreeMap<u64, Vec<u8>> {
        out.iter()
            .map(|frame| (frame.participant, frame.body.clone()))
            .collect()
    }

    /// A standalone client bunch blob setting `fields`.
    fn bunch(
        object_id: u32,
        is_full: bool,
        result_id: u64,
        base_id: u64,
        fields: &[(u16, RepValue)],
    ) -> Vec<u8> {
        let mut b = DeltaBunch::new(object_id, is_full, result_id, base_id);
        for (fid, v) in fields {
            b.set(*fid, FieldDelta::Value(v.clone()));
        }
        b.encode(&schema()).unwrap()
    }

    fn full_health(result_id: u64, health: i64) -> Vec<u8> {
        bunch(
            OBJ,
            true,
            result_id,
            0,
            &[(F_HEALTH, RepValue::Int(health))],
        )
    }

    fn decode(body: &[u8]) -> DeltaBunch {
        let mut budget = MAX_ENVELOPE_ALLOC;
        DeltaBunch::decode(body, &schema(), &mut budget).unwrap()
    }

    #[test]
    fn strict_registration_rejects_an_older_full_snapshot() {
        let authority = RepAuthority::new(RateLimits::default());
        authority
            .register_class(
                COMPAT_CLASS,
                compat_v2_layout(),
                compat_schema(compat_v2_layout()),
            )
            .unwrap();
        authority
            .spawn_object(
                COMPAT_OBJ,
                MATCH,
                COMPAT_CLASS,
                Some(OWNER),
                false,
                RepSnapshot::new(),
            )
            .unwrap();
        authority.join_match(OWNER, MATCH, false);
        let old_full = compat_bunch(
            &compat_schema(compat_v1_layout()),
            true,
            10,
            &[(F_COMPAT_HEALTH, RepValue::Int(80))],
        );
        assert_eq!(
            authority.validate(OWNER, &old_full, 1000).unwrap_err(),
            RepReject::Frame
        );
    }

    #[test]
    fn compat_old_client_keeps_appended_defaults_and_cannot_set_server_only() {
        let authority = compat_authority();
        let old_schema = compat_schema(compat_v1_layout());
        let old_full = compat_bunch(
            &old_schema,
            true,
            10,
            &[(F_COMPAT_HEALTH, RepValue::Int(80))],
        );
        assert!(!authority.handle_delta(OWNER, &old_full, 1000).is_empty());
        assert_eq!(
            authority.authoritative_scalar(COMPAT_OBJ, F_COMPAT_HEALTH),
            Some(RepValue::Int(80))
        );
        // Fields appended after v1 retain server-initialized authoritative values;
        // absence in an old snapshot never selects a value.
        assert_eq!(
            authority.authoritative_scalar(COMPAT_OBJ, F_COMPAT_EMOTE),
            Some(RepValue::Int(4))
        );
        assert_eq!(
            authority.authoritative_scalar(COMPAT_OBJ, F_COMPAT_CRITICAL),
            Some(RepValue::Int(99))
        );

        // The old-version binding selects v1 for subsequent deltas, rather than
        // decoding the v1 mask with v2's field table.
        let old_delta = compat_bunch(
            &old_schema,
            false,
            11,
            &[(F_COMPAT_HEALTH, RepValue::Int(70))],
        );
        assert!(!authority.handle_delta(OWNER, &old_delta, 1001).is_empty());
        assert_eq!(
            authority.authoritative_scalar(COMPAT_OBJ, F_COMPAT_HEALTH),
            Some(RepValue::Int(70))
        );

        let server_only = compat_bunch(
            &compat_schema(compat_v2_layout()),
            true,
            12,
            &[(F_COMPAT_CRITICAL, RepValue::Int(1))],
        );
        assert_eq!(
            authority.validate(OWNER, &server_only, 1002).unwrap_err(),
            RepReject::ServerOnlyField
        );
        assert_eq!(
            authority.authoritative_scalar(COMPAT_OBJ, F_COMPAT_CRITICAL),
            Some(RepValue::Int(99))
        );
    }

    #[test]
    fn compat_rejects_a_structural_prefix_below_the_minimum_version() {
        let authority = RepAuthority::new(RateLimits::default());
        authority
            .register_class_compat(
                COMPAT_CLASS,
                compat_v3_layout(),
                compat_schema(compat_v3_layout()),
                vec![(compat_v2_layout(), compat_schema(compat_v2_layout()))],
                2,
            )
            .unwrap();
        authority
            .spawn_object(
                COMPAT_OBJ,
                MATCH,
                COMPAT_CLASS,
                Some(OWNER),
                false,
                RepSnapshot::new(),
            )
            .unwrap();
        authority.join_match(OWNER, MATCH, false);
        let v1_full = compat_bunch(
            &compat_schema(compat_v1_layout()),
            true,
            10,
            &[(F_COMPAT_HEALTH, RepValue::Int(80))],
        );
        assert_eq!(
            authority.validate(OWNER, &v1_full, 1000).unwrap_err(),
            RepReject::SchemaBinding
        );
    }

    #[test]
    fn compat_registration_rejects_non_prefix_layout() {
        let authority = RepAuthority::new(RateLimits::default());
        assert_eq!(
            authority.register_class_compat(
                COMPAT_CLASS,
                compat_v2_layout(),
                compat_schema(compat_v2_layout()),
                vec![(
                    compat_nonprefix_v1_layout(),
                    compat_schema(compat_nonprefix_v1_layout()),
                )],
                1,
            ),
            Err(RepReject::SchemaBinding)
        );
    }

    #[test]
    fn compat_still_rejects_trailing_encoding() {
        let authority = compat_authority();
        let mut body = compat_bunch(
            &compat_schema(compat_v1_layout()),
            true,
            10,
            &[(F_COMPAT_HEALTH, RepValue::Int(80))],
        );
        body.push(0);
        assert_eq!(
            authority.validate(OWNER, &body, 1000).unwrap_err(),
            RepReject::Frame
        );
    }

    #[test]
    fn metrics_report_objects_per_match_and_actual_fanout() {
        let authority = authority();
        authority.join_match(FAR_PEER, MATCH, false);
        authority
            .spawn_object(101, MATCH, CLASS, Some(OWNER), false, RepSnapshot::new())
            .unwrap();
        authority
            .spawn_object(102, MATCH, CLASS, Some(OWNER), false, RepSnapshot::new())
            .unwrap();
        assert_eq!(authority.metrics().total_objects, 3);
        assert_eq!(authority.metrics().objects_per_match.get(&MATCH), Some(&3));

        let out = authority.handle_delta(OWNER, &full_health(10, 80), 1000);
        assert_eq!(out.len(), 2);
        assert_eq!(authority.metrics().last_fanout_width, 2);
    }

    #[test]
    fn owner_valid_applies_and_rebroadcasts_to_peer() {
        let a = authority();
        let out = a.handle_delta(OWNER, &full_health(10, 80), 1000);
        // The peer gets exactly one server-stamped rebroadcast.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].participant, PEER);
        let peer_bunch = decode(&out[0].body);
        assert_eq!(
            peer_bunch.changes.get(&F_HEALTH),
            Some(&FieldDelta::Value(RepValue::Int(80)))
        );
        // Authoritative state updated.
        assert_eq!(
            a.authoritative_scalar(OBJ, F_HEALTH),
            Some(RepValue::Int(80))
        );
    }

    #[test]
    fn server_rebroadcasts_its_own_bytes_not_the_client_bytes() {
        let a = authority();
        let client_body = full_health(10, 80);
        let out = a.handle_delta(OWNER, &client_body, 1000);
        assert_eq!(out.len(), 1);
        // The server re-encoded with its own server-issued token, so the bytes
        // differ from the client's, yet decode to the same authoritative value.
        assert_ne!(
            out[0].body, client_body,
            "server must not relay client bytes"
        );
        assert_eq!(
            decode(&out[0].body).changes.get(&F_HEALTH),
            Some(&FieldDelta::Value(RepValue::Int(80)))
        );
    }

    #[test]
    fn non_owner_is_rejected() {
        let a = authority();
        // B tries to change A's object.
        let out = a.handle_delta(PEER, &full_health(10, 80), 1000);
        assert!(out.is_empty());
        assert_eq!(a.authoritative_scalar(OBJ, F_HEALTH), None);
    }

    #[test]
    fn server_only_field_from_client_is_rejected() {
        let a = authority();
        let body = bunch(OBJ, true, 10, 0, &[(F_TEAM, RepValue::Int(3))]);
        assert_eq!(
            a.validate(OWNER, &body, 1000).unwrap_err(),
            RepReject::ServerOnlyField
        );
    }

    #[test]
    fn out_of_range_int_clamps_via_codec_and_applies() {
        let a = authority();
        // The client proposes 150; the IntRange codec saturates to 100 on encode,
        // the server decodes 100, bounds-validates, and applies the clamp.
        let out = a.handle_delta(OWNER, &full_health(10, 150), 1000);
        assert_eq!(out.len(), 1);
        assert_eq!(
            a.authoritative_scalar(OBJ, F_HEALTH),
            Some(RepValue::Int(100))
        );
    }

    #[test]
    fn bounds_rejects_nan_and_gross_out_of_range_and_clamps_near_boundary() {
        let sr = FieldBounds::ScalarRange {
            min: 0.0,
            max: 1.0,
            values_per_unit: 1024,
        };
        assert_eq!(
            validate_bounds(&RepValue::Scalar(f32::NAN), sr, TypeTag::Scalar),
            Err(RepReject::Bounds)
        );
        assert_eq!(
            validate_bounds(&RepValue::Scalar(f32::INFINITY), sr, TypeTag::Scalar),
            Err(RepReject::Bounds)
        );
        assert_eq!(
            validate_bounds(&RepValue::Scalar(5.0), sr, TypeTag::Scalar),
            Err(RepReject::Bounds)
        );
        // A value a hair over max (post-dequantization rounding) is clamped, not rejected.
        match validate_bounds(&RepValue::Scalar(1.0004), sr, TypeTag::Scalar).unwrap() {
            RepValue::Scalar(v) => assert_eq!(v, 1.0),
            _ => panic!(),
        }
        // Gross out-of-range int rejected.
        assert_eq!(
            validate_bounds(
                &RepValue::Int(500),
                FieldBounds::IntRange { min: 0, max: 100 },
                TypeTag::Int
            ),
            Err(RepReject::Bounds)
        );
    }

    #[test]
    fn cross_match_object_is_rejected() {
        let a = RepAuthority::new(RateLimits::default());
        a.register_class(CLASS, layout(), schema()).unwrap();
        a.spawn_object(OBJ, MATCH, CLASS, Some(OWNER), false, RepSnapshot::new())
            .unwrap();
        // The owner connection is in a DIFFERENT match than the object.
        a.join_match(OWNER, 999, false);
        assert_eq!(
            a.validate(OWNER, &full_health(10, 80), 1000).unwrap_err(),
            RepReject::CrossMatch
        );
    }

    #[test]
    fn unknown_object_is_rejected() {
        let a = authority();
        let body = bunch(999, true, 10, 0, &[(F_HEALTH, RepValue::Int(80))]);
        assert_eq!(
            a.validate(OWNER, &body, 1000).unwrap_err(),
            RepReject::UnknownObject
        );
    }

    #[test]
    fn guest_cannot_mutate_persistent_object() {
        let a = RepAuthority::new(RateLimits::default());
        a.register_class(CLASS, layout(), schema()).unwrap();
        a.spawn_object(OBJ, MATCH, CLASS, Some(OWNER), true, RepSnapshot::new())
            .unwrap();
        a.join_match(OWNER, MATCH, true); // guest owner
        assert_eq!(
            a.validate(OWNER, &full_health(10, 80), 1000).unwrap_err(),
            RepReject::Guest
        );
    }

    #[test]
    fn guest_may_mutate_ephemeral_object() {
        let a = RepAuthority::new(RateLimits::default());
        a.register_class(CLASS, layout(), schema()).unwrap();
        a.spawn_object(OBJ, MATCH, CLASS, Some(OWNER), false, RepSnapshot::new())
            .unwrap();
        a.join_match(OWNER, MATCH, true); // guest, ephemeral object
        assert!(a.validate(OWNER, &full_health(10, 80), 1000).is_ok());
    }

    #[test]
    fn owner_epoch_toctou_recheck_rejects_at_apply() {
        let a = authority();
        let validated = a.validate(OWNER, &full_health(10, 80), 1000).unwrap();
        // Ownership transfers between validate and apply: the epoch moves.
        a.assign_owner(OBJ, Some(PEER));
        assert_eq!(
            a.apply_and_rebroadcast(validated).unwrap_err(),
            RepReject::Toctou
        );
        // Nothing was applied.
        assert_eq!(a.authoritative_scalar(OBJ, F_HEALTH), None);
    }

    #[test]
    fn stale_result_id_is_dropped() {
        let a = authority();
        assert!(!a.handle_delta(OWNER, &full_health(10, 80), 1000).is_empty());
        // A replay with the same (or older) result_id is stale.
        assert_eq!(
            a.validate(OWNER, &full_health(10, 90), 1001).unwrap_err(),
            RepReject::Stale
        );
        assert_eq!(
            a.validate(OWNER, &full_health(5, 90), 1001).unwrap_err(),
            RepReject::Stale
        );
    }

    #[test]
    fn delta_without_established_full_is_rejected() {
        let a = authority();
        // A non-full bunch with no prior full snapshot for (conn, object).
        let body = bunch(OBJ, false, 10, 5, &[(F_HEALTH, RepValue::Int(80))]);
        assert_eq!(
            a.validate(OWNER, &body, 1000).unwrap_err(),
            RepReject::SchemaBinding
        );
    }

    #[test]
    fn aggregate_rate_budget_cuts_the_flood() {
        let limits = RateLimits {
            bunches_per_sec: 2.0,
            ..RateLimits::default()
        };
        let a = authority_with(limits, false);
        // Same instant: only 2 bunches fit the bucket, the 3rd is dropped.
        assert!(!a.handle_delta(OWNER, &full_health(10, 10), 1000).is_empty());
        // result_id must strictly increase to avoid the stale guard.
        let _ = a.validate(OWNER, &full_health(11, 11), 1000);
        assert_eq!(
            a.validate(OWNER, &full_health(12, 12), 1000).unwrap_err(),
            RepReject::Rate
        );
    }

    #[test]
    fn cond_owneronly_field_is_not_rebroadcast_to_peers() {
        let a = authority();
        let body = bunch(
            OBJ,
            true,
            10,
            0,
            &[(F_HEALTH, RepValue::Int(70)), (F_SECRET, RepValue::Int(42))],
        );
        let out = a.handle_delta(OWNER, &body, 1000);
        assert_eq!(out.len(), 1);
        let peer = decode(&out[0].body);
        // The peer sees health (COND None) but never the OwnerOnly secret.
        assert!(peer.changes.contains_key(&F_HEALTH));
        assert!(!peer.changes.contains_key(&F_SECRET));
        // Both are authoritative on the server, though.
        assert_eq!(
            a.authoritative_scalar(OBJ, F_SECRET),
            Some(RepValue::Int(42))
        );
    }

    #[test]
    fn rebroadcast_bytes_are_charged_to_the_originating_budget() {
        let a = authority();
        let before = a.bytes_tokens(OWNER).unwrap();
        let out = a.handle_delta(OWNER, &full_health(10, 80), 1000);
        let rebroadcast: usize = out.iter().map(|o| o.body.len()).sum();
        let after = a.bytes_tokens(OWNER).unwrap();
        // The budget dropped by at least the inbound bytes + the rebroadcast bytes
        // (amplification is charged back to the originator).
        assert!(
            before - after >= rebroadcast as f64,
            "amplification not charged"
        );
        assert!(rebroadcast > 0);
    }

    #[test]
    fn rejections_are_uniform_no_oracle() {
        // Two different internal rejects produce the identical external outcome
        // (no output), so the client cannot distinguish which check failed.
        let a = authority();
        let server_only = bunch(OBJ, true, 10, 0, &[(F_TEAM, RepValue::Int(3))]);
        let unknown = bunch(999, true, 10, 0, &[(F_HEALTH, RepValue::Int(80))]);
        assert_eq!(a.handle_delta(OWNER, &server_only, 1000), Vec::new());
        assert_eq!(a.handle_delta(OWNER, &unknown, 1000), Vec::new());
    }

    #[test]
    fn veto_leaves_value_unchanged_and_corrects_the_owner() {
        struct VetoHealth;
        impl RepVetoHook for VetoHealth {
            fn veto(&self, ctx: RepVetoContext<'_>) -> Vec<u16> {
                ctx.proposed.iter().map(|(fid, _)| *fid).collect()
            }
        }
        let a = RepAuthority::new(RateLimits::default()).with_veto(Arc::new(VetoHealth));
        a.register_class(CLASS, layout(), schema()).unwrap();
        let mut initial = RepSnapshot::new();
        initial.set_scalar(F_HEALTH, RepValue::Int(100));
        a.spawn_object(OBJ, MATCH, CLASS, Some(OWNER), false, initial)
            .unwrap();
        a.join_match(OWNER, MATCH, false);
        a.join_match(PEER, MATCH, false);

        let out = a.handle_delta(OWNER, &full_health(10, 50), 1000);
        // Authoritative value is unchanged (vetoed): the illegal 50 never lands.
        assert_eq!(
            a.authoritative_scalar(OBJ, F_HEALTH),
            Some(RepValue::Int(100))
        );
        // The owner receives a correction carrying the real (unchanged) value.
        let owner_frame = out
            .iter()
            .find(|o| o.participant == OWNER)
            .expect("owner is corrected");
        assert_eq!(
            decode(&owner_frame.body).changes.get(&F_HEALTH),
            Some(&FieldDelta::Value(RepValue::Int(100)))
        );
        // The illegal value 50 never reaches anyone — every frame reflects 100.
        for o in &out {
            if let Some(FieldDelta::Value(RepValue::Int(v))) =
                decode(&o.body).changes.get(&F_HEALTH)
            {
                assert_eq!(*v, 100, "vetoed value must never be broadcast");
            }
        }
    }

    #[test]
    fn ack_advances_baseline_so_next_rebroadcast_is_a_delta() {
        let a = authority();
        // First change -> peer gets a full snapshot.
        let out = a.handle_delta(OWNER, &full_health(10, 80), 1000);
        let first = decode(&out[0].body);
        assert!(first.is_full);
        // Peer acks the server's result_id.
        let ack = RepAck {
            entries: vec![RepAckEntry {
                object_id: OBJ,
                acked_result_id: first.result_id,
                history: 0,
            }],
        };
        a.handle_ack(PEER, &ack.encode().unwrap());
        // Second change -> now a delta against the acked baseline.
        let out2 = a.handle_delta(OWNER, &full_health(11, 90), 1001);
        let second = decode(&out2[0].body);
        assert!(!second.is_full, "post-ack rebroadcast is a delta");
        assert_eq!(second.base_id, first.result_id);
    }

    #[test]
    fn shared_state_matches_default_bytes_and_decoded_bunches_across_baselines_and_churn() {
        let off = authority_with_shared_state(RateLimits::default(), false, false);
        let on = authority_with_shared_state(RateLimits::default(), false, true);
        for authority in [&off, &on] {
            authority.join_match(FAR_PEER, MATCH, false);
            authority.join_match(EXTRA_PEER, MATCH, false);
        }

        // Newly relevant receivers all receive the same full snapshot.
        let off_first = off.handle_delta(OWNER, &full_health(10, 80), 1000);
        let on_first = on.handle_delta(OWNER, &full_health(10, 80), 1000);
        assert_eq!(outgoing_bodies(&on_first), outgoing_bodies(&off_first));
        assert_eq!(
            outgoing_by_receiver(&on_first),
            outgoing_by_receiver(&off_first)
        );
        let first = outgoing_by_receiver(&on_first);
        ack_receiver(&off, PEER, first[&PEER].result_id);
        ack_receiver(&on, PEER, first[&PEER].result_id);

        // One acked receiver and two unacked receivers exercise divergent full /
        // delta baselines. Ack two different result ids for the next round.
        let off_second = off.handle_delta(OWNER, &full_health(11, 70), 1001);
        let on_second = on.handle_delta(OWNER, &full_health(11, 70), 1001);
        assert_eq!(outgoing_bodies(&on_second), outgoing_bodies(&off_second));
        assert_eq!(
            outgoing_by_receiver(&on_second),
            outgoing_by_receiver(&off_second)
        );
        let second = outgoing_by_receiver(&on_second);
        assert!(!second[&PEER].is_full);
        assert!(second[&FAR_PEER].is_full);
        for authority in [&off, &on] {
            ack_receiver(authority, PEER, second[&PEER].result_id);
            ack_receiver(authority, FAR_PEER, second[&FAR_PEER].result_id);
            assert!(authority.set_object_position(OBJ, [0.0, 0.0, 0.0]));
            assert!(authority.set_viewer_position(EXTRA_PEER, [1_000.0, 0.0, 0.0]));
        }

        // The two acked peers now have distinct acknowledged tokens, while the
        // churned receiver is irrelevant.
        let off_third = off.handle_delta(OWNER, &full_health(12, 60), 1002);
        let on_third = on.handle_delta(OWNER, &full_health(12, 60), 1002);
        assert_eq!(outgoing_bodies(&on_third), outgoing_bodies(&off_third));
        assert_eq!(
            outgoing_by_receiver(&on_third),
            outgoing_by_receiver(&off_third)
        );
        let third = outgoing_by_receiver(&on_third);
        assert_ne!(third[&PEER].base_id, third[&FAR_PEER].base_id);
        assert!(!third.contains_key(&EXTRA_PEER));

        // Re-entering invalidates only EXTRA_PEER's state; it receives a full
        // snapshot while the other receiver bookkeeping remains per-peer.
        for authority in [&off, &on] {
            assert!(authority.set_viewer_position(EXTRA_PEER, [0.0, 0.0, 0.0]));
        }
        let off_fourth = off.handle_delta(OWNER, &full_health(13, 50), 1003);
        let on_fourth = on.handle_delta(OWNER, &full_health(13, 50), 1003);
        assert_eq!(outgoing_bodies(&on_fourth), outgoing_bodies(&off_fourth));
        assert_eq!(
            outgoing_by_receiver(&on_fourth),
            outgoing_by_receiver(&off_fourth)
        );
        assert!(outgoing_by_receiver(&on_fourth)[&EXTRA_PEER].is_full);
    }

    #[test]
    fn shared_state_quantizes_once_per_same_baseline_signature() {
        let a = authority_with_shared_state(RateLimits::default(), false, true);
        a.join_match(FAR_PEER, MATCH, false);
        a.join_match(EXTRA_PEER, MATCH, false);

        let out = a.handle_delta(OWNER, &full_health(10, 80), 1000);
        assert_eq!(out.len(), 3);
        assert_eq!(a.last_shared_quantize_count(), 1);
    }

    #[test]
    fn shared_state_quantizes_once_per_distinct_baseline_signature() {
        let a = authority_with_shared_state(RateLimits::default(), false, true);
        a.join_match(FAR_PEER, MATCH, false);
        a.join_match(EXTRA_PEER, MATCH, false);

        let first = outgoing_by_receiver(&a.handle_delta(OWNER, &full_health(10, 80), 1000));
        for receiver in [PEER, FAR_PEER, EXTRA_PEER] {
            ack_receiver(&a, receiver, first[&receiver].result_id);
        }
        let out = a.handle_delta(OWNER, &full_health(11, 70), 1001);
        assert!(out.iter().all(|frame| !decode(&frame.body).is_full));
        assert_eq!(a.last_shared_quantize_count(), 3);
    }

    #[test]
    fn shared_state_keeps_tokens_and_acks_per_receiver() {
        let a = authority_with_shared_state(RateLimits::default(), false, true);
        a.join_match(FAR_PEER, MATCH, false);

        let first = outgoing_by_receiver(&a.handle_delta(OWNER, &full_health(10, 80), 1000));
        assert_ne!(first[&PEER].result_id, first[&FAR_PEER].result_id);
        ack_receiver(&a, PEER, first[&PEER].result_id);

        let second = outgoing_by_receiver(&a.handle_delta(OWNER, &full_health(11, 70), 1001));
        assert!(!second[&PEER].is_full);
        assert_eq!(second[&PEER].base_id, first[&PEER].result_id);
        assert!(second[&FAR_PEER].is_full);
        assert_ne!(second[&PEER].result_id, second[&FAR_PEER].result_id);

        ack_receiver(&a, PEER, second[&PEER].result_id);
        // A stale ack cannot regress the receiver's acknowledged baseline.
        ack_receiver(&a, PEER, first[&PEER].result_id);
        let third = outgoing_by_receiver(&a.handle_delta(OWNER, &full_health(12, 60), 1002));
        assert!(!third[&PEER].is_full);
        assert_eq!(third[&PEER].base_id, second[&PEER].result_id);
    }

    #[test]
    fn interest_grid_only_fans_out_to_relevant_receivers() {
        let a = authority();
        a.join_match(FAR_PEER, MATCH, false);
        assert!(a.set_object_position(OBJ, [0.0, 0.0, 0.0]));
        assert!(a.set_viewer_position(PEER, [0.0, 0.0, 0.0]));
        assert!(a.set_viewer_position(FAR_PEER, [1_000.0, 0.0, 0.0]));

        let out = a.handle_delta(OWNER, &full_health(10, 80), 1000);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].participant, PEER);
        assert!(decode(&out[0].body).is_full);
    }

    #[test]
    fn relevance_exit_invalidates_baseline_and_reentry_is_full() {
        let a = authority();
        assert!(a.set_object_position(OBJ, [0.0, 0.0, 0.0]));
        assert!(a.set_viewer_position(PEER, [0.0, 0.0, 0.0]));

        let first = a.handle_delta(OWNER, &full_health(10, 80), 1000);
        let first_bunch = decode(&first[0].body);
        assert!(first_bunch.is_full);
        a.handle_ack(
            PEER,
            &RepAck {
                entries: vec![RepAckEntry {
                    object_id: OBJ,
                    acked_result_id: first_bunch.result_id,
                    history: 0,
                }],
            }
            .encode()
            .unwrap(),
        );

        // Crossing the outer radius removes PEER from the object's sender state.
        assert!(a.set_viewer_position(PEER, [1_000.0, 0.0, 0.0]));
        assert!(a.handle_delta(OWNER, &full_health(11, 70), 1001).is_empty());

        // Re-entering means a new receiver state, so no old baseline may be used.
        assert!(a.set_viewer_position(PEER, [0.0, 0.0, 0.0]));
        let reentered = a.handle_delta(OWNER, &full_health(12, 60), 1002);
        assert_eq!(reentered.len(), 1);
        assert!(
            decode(&reentered[0].body).is_full,
            "relevance re-entry must start from a full snapshot"
        );
    }

    #[test]
    fn despawn_clears_interest_and_per_connection_baselines() {
        let a = authority();
        let first = a.handle_delta(OWNER, &full_health(10, 80), 1000);
        let first_bunch = decode(&first[0].body);
        a.handle_ack(
            PEER,
            &RepAck {
                entries: vec![RepAckEntry {
                    object_id: OBJ,
                    acked_result_id: first_bunch.result_id,
                    history: 0,
                }],
            }
            .encode()
            .unwrap(),
        );

        assert!(a.despawn_object(OBJ));
        a.spawn_object(OBJ, MATCH, CLASS, Some(OWNER), false, RepSnapshot::new())
            .unwrap();
        let respawned = a.handle_delta(OWNER, &full_health(11, 70), 1001);
        assert_eq!(respawned.len(), 1);
        assert!(decode(&respawned[0].body).is_full);
    }

    #[test]
    fn stale_is_rechecked_under_the_apply_lock() {
        // Two deltas both pass validate (neither recorded highest yet); applying the
        // newer then the older must reject the older at the apply-lock re-check.
        let a = authority();
        let v10 = a.validate(OWNER, &full_health(10, 40), 1000).unwrap();
        let v11 = a.validate(OWNER, &full_health(11, 60), 1000).unwrap();
        assert!(a.apply_and_rebroadcast(v11).is_ok());
        assert_eq!(
            a.apply_and_rebroadcast(v10).unwrap_err(),
            RepReject::Stale,
            "an older validated delta cannot regress state"
        );
        assert_eq!(
            a.authoritative_scalar(OBJ, F_HEALTH),
            Some(RepValue::Int(60))
        );
    }

    #[test]
    fn respawn_between_validate_and_apply_is_rejected() {
        let a = authority();
        let v = a.validate(OWNER, &full_health(10, 40), 1000).unwrap();
        // The object id is re-spawned (a different object now lives here).
        a.spawn_object(OBJ, MATCH, CLASS, Some(OWNER), false, RepSnapshot::new())
            .unwrap();
        assert_eq!(
            a.apply_and_rebroadcast(v).unwrap_err(),
            RepReject::Toctou,
            "a validated delta cannot apply to a re-spawned object"
        );
    }

    #[test]
    fn respawn_resets_per_conn_baseline_and_binding() {
        // After applying a high result_id, a re-spawn of the same object id must
        // clear the connection's stale guard + binding so the new object accepts a
        // fresh low-token full snapshot (it is a different object).
        let a = authority();
        assert!(!a.handle_delta(OWNER, &full_health(50, 80), 1000).is_empty());
        // Same result_id would be stale on the old object.
        assert_eq!(
            a.validate(OWNER, &full_health(50, 80), 1001).unwrap_err(),
            RepReject::Stale
        );
        // Re-spawn: a brand new object at this id.
        a.spawn_object(OBJ, MATCH, CLASS, Some(OWNER), false, RepSnapshot::new())
            .unwrap();
        // A fresh full snapshot with a LOW token is now accepted (state was reset).
        assert!(a.validate(OWNER, &full_health(1, 40), 1002).is_ok());
    }

    #[test]
    fn veto_hook_panic_fails_closed() {
        struct Boom;
        impl RepVetoHook for Boom {
            fn veto(&self, _ctx: RepVetoContext<'_>) -> Vec<u16> {
                panic!("hostile hook");
            }
        }
        let a = RepAuthority::new(RateLimits::default()).with_veto(Arc::new(Boom));
        a.register_class(CLASS, layout(), schema()).unwrap();
        let mut initial = RepSnapshot::new();
        initial.set_scalar(F_HEALTH, RepValue::Int(100));
        a.spawn_object(OBJ, MATCH, CLASS, Some(OWNER), false, initial)
            .unwrap();
        a.join_match(OWNER, MATCH, false);
        // The panic is caught; the proposal is not applied (fail closed) and the
        // authority is not poisoned (a later call still works).
        let _ = a.handle_delta(OWNER, &full_health(10, 50), 1000);
        assert_eq!(
            a.authoritative_scalar(OBJ, F_HEALTH),
            Some(RepValue::Int(100))
        );
        assert!(a.bytes_tokens(OWNER).is_some(), "lock not poisoned");
    }

    #[test]
    fn register_class_rejects_degenerate_bounds() {
        static BAD: OnceLock<RepLayout> = OnceLock::new();
        let bad = BAD.get_or_init(|| {
            RepLayoutBuilder::new(77, 1)
                .field(
                    "x",
                    TypeTag::Scalar,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::ScalarRange {
                        min: 1.0,
                        max: 0.0, // max < min
                        values_per_unit: 16,
                    },
                    true,
                )
                .build()
                .unwrap()
        });
        let sch = RepSchema::new(
            *bad.schema_hash(),
            vec![RepFieldCodec::Scalar(
                ScalarQuant::new(0.0, 1.0, 16).unwrap(),
            )],
        )
        .unwrap();
        let a = RepAuthority::new(RateLimits::default());
        assert_eq!(
            a.register_class(77, bad, sch).unwrap_err(),
            RepReject::Bounds
        );
    }

    #[test]
    fn collection_from_client_is_rejected() {
        // A class whose only field is a client-owned collection: an inbound
        // collection delta is rejected this phase (documented gap).
        static CL: OnceLock<RepLayout> = OnceLock::new();
        let cl = CL.get_or_init(|| {
            RepLayoutBuilder::new(9, 1)
                .field(
                    "items",
                    TypeTag::Int,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ClientOwned,
                    FieldBounds::MaxCardinality { max_items: 16 },
                    true,
                )
                .build()
                .unwrap()
        });
        let sch = RepSchema::new(
            *cl.schema_hash(),
            vec![RepFieldCodec::Collection {
                item: Box::new(RepFieldCodec::IntRange { min: 0, max: 100 }),
                max_items: 16,
            }],
        )
        .unwrap();
        let a = RepAuthority::new(RateLimits::default());
        a.register_class(9, cl, sch.clone()).unwrap();
        a.spawn_object(200, MATCH, 9, Some(OWNER), false, RepSnapshot::new())
            .unwrap();
        a.join_match(OWNER, MATCH, false);

        use citadel_wire::netpeer::{CollItem, CollectionDelta, RepId};
        let mut b = DeltaBunch::new(200, true, 10, 0);
        b.set(
            0,
            FieldDelta::Collection(CollectionDelta {
                removed: vec![],
                added: vec![CollItem {
                    id: RepId {
                        index: 0,
                        generation: 0,
                    },
                    key: 1,
                    value: RepValue::Int(5),
                }],
                changed: vec![],
            }),
        );
        let body = b.encode(&sch).unwrap();
        assert_eq!(
            a.validate(OWNER, &body, 1000).unwrap_err(),
            RepReject::UnsupportedCollection
        );
    }
}
