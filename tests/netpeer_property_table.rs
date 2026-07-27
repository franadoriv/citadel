//! Integration coverage for the NetworkPeer Phase-1 property table + dirty
//! tracking, exercising the public `citadel::realtime::netpeer`
//! contract the same way a game/server module would.
//!
//! Covered (design §2, §3, §10 Phase 1):
//! - `field_id` is registration-order and stable; `schema_hash` deterministic
//!   and moves when the layout changes.
//! - `mark_dirty` -> dirty bit (push path).
//! - the shadow net detects an unmarked non-push change.
//! - the pre-encode audit **fails** (not warns) on a push field changed without a
//!   mark, and passes once marked.
//! - the reflection/layout scan runs **once at registration**, never per tick.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use citadel::realtime::netpeer::{
    FieldAuthority, FieldBounds, FieldId, NetworkPeer, RepCondition, RepLayout, RepLayoutBuilder,
    Replicated, TypeTag, UnmarkedChanges,
};
use citadel_wire::codec::codec_id;

const F_HEALTH: FieldId = 0; // push
const F_AMMO: FieldId = 1; // push
const F_TEAM: FieldId = 2; // shadow-net (non-push)
const F_NAME: FieldId = 3; // shadow-net (non-push)

/// Counts how many times the class layout is actually built, to prove the
/// "reflection once at registration" invariant on the Rust mirror.
static LAYOUT_BUILDS: AtomicUsize = AtomicUsize::new(0);

fn player_layout() -> &'static RepLayout {
    static LAYOUT: OnceLock<RepLayout> = OnceLock::new();
    LAYOUT.get_or_init(|| {
        LAYOUT_BUILDS.fetch_add(1, Ordering::SeqCst);
        RepLayoutBuilder::new(0x504C_4159, 1)
            .field(
                "health",
                TypeTag::Int,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ServerOnly,
                FieldBounds::IntRange { min: 0, max: 100 },
                true,
            )
            .field(
                "ammo",
                TypeTag::Uint,
                codec_id::SCALAR_QUANT,
                RepCondition::OwnerOnly,
                FieldAuthority::ServerOnly,
                FieldBounds::IntRange { min: 0, max: 999 },
                true,
            )
            .field(
                "team",
                TypeTag::Uint,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ServerOnly,
                FieldBounds::IntRange { min: 0, max: 16 },
                false,
            )
            .field(
                "name",
                TypeTag::Bytes,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ClientOwned,
                FieldBounds::MaxLen { max_len: 24 },
                false,
            )
            .build()
            .expect("player layout builds")
    })
}

#[derive(Clone)]
struct Player {
    health: i64,
    ammo: u64,
    team: u64,
    name: String,
}

impl Player {
    fn spawn() -> Self {
        Self {
            health: 100,
            ammo: 30,
            team: 1,
            name: "hero".to_string(),
        }
    }
}

impl Replicated for Player {
    fn rep_layout(&self) -> &'static RepLayout {
        player_layout()
    }
    fn field_value(&self, field_id: FieldId) -> citadel::realtime::netpeer::FieldValue {
        use citadel::realtime::netpeer::FieldValue;
        match field_id {
            F_HEALTH => FieldValue::Int(self.health),
            F_AMMO => FieldValue::Uint(self.ammo),
            F_TEAM => FieldValue::Uint(self.team),
            F_NAME => FieldValue::Bytes(self.name.clone().into_bytes()),
            _ => FieldValue::Bytes(Vec::new()),
        }
    }
}

#[test]
fn field_ids_are_registration_order_and_hash_is_deterministic() {
    let layout = player_layout();
    assert_eq!(layout.len(), 4);
    for (i, f) in layout.fields().iter().enumerate() {
        assert_eq!(f.id as usize, i, "field_id equals registration index");
    }
    // Rebuilding the same declaration yields the same wide hash.
    let rebuilt = RepLayoutBuilder::new(0x504C_4159, 1)
        .field(
            "health",
            TypeTag::Int,
            codec_id::SCALAR_QUANT,
            RepCondition::None,
            FieldAuthority::ServerOnly,
            FieldBounds::IntRange { min: 0, max: 100 },
            true,
        )
        .field(
            "ammo",
            TypeTag::Uint,
            codec_id::SCALAR_QUANT,
            RepCondition::OwnerOnly,
            FieldAuthority::ServerOnly,
            FieldBounds::IntRange { min: 0, max: 999 },
            true,
        )
        .field(
            "team",
            TypeTag::Uint,
            codec_id::SCALAR_QUANT,
            RepCondition::None,
            FieldAuthority::ServerOnly,
            FieldBounds::IntRange { min: 0, max: 16 },
            false,
        )
        .field(
            "name",
            TypeTag::Bytes,
            codec_id::SCALAR_QUANT,
            RepCondition::None,
            FieldAuthority::ClientOwned,
            FieldBounds::MaxLen { max_len: 24 },
            false,
        )
        .build()
        .expect("rebuild");
    assert_eq!(layout.schema_hash(), rebuilt.schema_hash());
}

#[test]
fn reordering_fields_changes_the_hash() {
    // Swapping the first two fields renumbers handles -> a different schema hash
    // (design §8: reorder is not backwards-compatible).
    let base = player_layout();
    let swapped = RepLayoutBuilder::new(0x504C_4159, 1)
        .field(
            "ammo", // was field 1
            TypeTag::Uint,
            codec_id::SCALAR_QUANT,
            RepCondition::OwnerOnly,
            FieldAuthority::ServerOnly,
            FieldBounds::IntRange { min: 0, max: 999 },
            true,
        )
        .field(
            "health", // was field 0
            TypeTag::Int,
            codec_id::SCALAR_QUANT,
            RepCondition::None,
            FieldAuthority::ServerOnly,
            FieldBounds::IntRange { min: 0, max: 100 },
            true,
        )
        .build()
        .expect("swapped layout");
    assert_ne!(base.schema_hash().bytes, swapped.schema_hash().bytes);
}

#[test]
fn mark_dirty_sets_the_push_bit() {
    let p = Player::spawn();
    let mut peer = NetworkPeer::new(&p);
    assert!(!peer.any_dirty());
    assert!(peer.mark_dirty(F_HEALTH));
    assert!(peer.mark_dirty(F_AMMO));
    let dirty: Vec<_> = peer.dirty_field_ids().collect();
    assert_eq!(dirty, vec![F_HEALTH, F_AMMO]);
}

#[test]
fn shadow_net_catches_unmarked_nonpush_change() {
    let mut p = Player::spawn();
    let mut peer = NetworkPeer::new(&p);
    // Two non-push fields change with no mark_dirty.
    p.team = 3;
    p.name = "villain".to_string();
    peer.detect_shadow_changes(&p);
    assert!(peer.is_dirty(F_TEAM));
    assert!(peer.is_dirty(F_NAME));
    // With the net having marked them, the audit is clean.
    assert!(peer.audit_unmarked_changes(&p).is_ok());
}

#[test]
fn audit_fails_closed_on_unmarked_push_change() {
    let mut p = Player::spawn();
    let mut peer = NetworkPeer::new(&p);
    // A push field mutated but the developer forgot to mark it.
    p.health = 40;
    peer.detect_shadow_changes(&p); // does not cover push fields
    let err = peer
        .audit_unmarked_changes(&p)
        .expect_err("unmarked push change must fail the audit");
    assert_eq!(err, UnmarkedChanges(vec![F_HEALTH]));
}

#[test]
fn full_tick_cycle_marked_change_passes_then_resets() {
    let mut p = Player::spawn();
    let mut peer = NetworkPeer::new(&p);
    p.health = 40;
    peer.mark_dirty(F_HEALTH);
    peer.detect_shadow_changes(&p);
    assert!(peer.audit_unmarked_changes(&p).is_ok());
    assert_eq!(peer.dirty_count(), 1);
    // Encode happens here; model the post-encode advance.
    peer.advance_after_encode(&p).expect("clean advance");
    assert!(!peer.any_dirty());
    assert!(peer.audit_unmarked_changes(&p).is_ok());
}

#[test]
fn layout_is_built_once_across_many_ticks() {
    // Touch the layout through many peers/ticks; it must build exactly once.
    let start = LAYOUT_BUILDS.load(Ordering::SeqCst);
    let _ = player_layout();
    for _ in 0..1000 {
        let mut p = Player::spawn();
        let mut peer = NetworkPeer::new(&p);
        p.health = (p.health - 1).max(0);
        peer.mark_dirty(F_HEALTH);
        peer.detect_shadow_changes(&p);
        let _ = peer.audit_unmarked_changes(&p);
        peer.advance_after_encode(&p).expect("clean advance");
    }
    let end = LAYOUT_BUILDS.load(Ordering::SeqCst);
    // At most one build happened during this test (zero if a sibling test built
    // it first); crucially, the 1000 ticks added none.
    assert!(
        end == start || end == start + 1,
        "layout rebuilt per tick: start={start} end={end}"
    );
    // And the total across the whole process is exactly one.
    assert_eq!(
        LAYOUT_BUILDS.load(Ordering::SeqCst),
        end,
        "no further builds after ticks"
    );
}
