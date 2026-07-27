//! The `NetworkPeer` property table: the Rust mirror of the Unreal
//! `FCitadelRepLayout` (NetworkPeer design §2). A [`RepLayout`] is built **once**
//! per replicated class at registration and cached (see the crate-level note on
//! never rebuilding per frame); it carries the ordered [`FieldDesc`] table whose
//! index is the stable `field_id`, plus the wide canonical [`SchemaHash`] (from
//! `citadel-wire`, ) computed over the ordered layout tuples.
//!
//! Phase 1 (this task) defines the table and the identity hash only. The wire
//! encode/decode of a `DeltaBunch` is .
//!
//! # Field identity in the schema hash (adversarial review, )
//!
//! The shared `citadel_wire::schema::LayoutField` preimage (frozen by )
//! carries `(field_id, type_tag, codec_id, cond, authority, bounds_shape)` but no
//! property-name slot. Two *structurally identical* fields (same type/codec/cond/
//! authority/bounds) swapped in registration order would otherwise produce an
//! identical hash while silently reassigning `field_id ↔ property`. To close that
//! hole without modifying the frozen shared preimage, NetworkPeer folds a stable
//! **per-field name key** into the `bounds_shape` slot (which  leaves
//! "codec-defined"), so any field's identity change moves the hash. Every SDK
//! reproduces the fold from the property name (Unreal: `FProperty::GetName`).

use citadel_wire::schema::{self, LayoutField, SchemaError, SchemaHash};

/// Stable handle for a replicated field: its index in the ordered [`RepLayout`]
/// table. Never sent as a name on the wire (design §2.2).
pub type FieldId = u16;

/// The type discriminant of a replicated field. Feeds the `type_tag` slot of the
/// [`SchemaHash`] preimage, so changing a field's type changes the schema hash.
///
/// Values are stable wire/contract identities and must match the Unreal
/// `ECitadelFieldType` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TypeTag {
    /// A single boolean.
    Bool = 1,
    /// A signed integer (`i8`..=`i64`).
    Int = 2,
    /// An unsigned integer (`u8`..=`u64`).
    Uint = 3,
    /// A scalar `f32` (bounded/quantized when `bounds` is a scalar range).
    Scalar = 4,
    /// A three-component position vector.
    Vector3 = 5,
    /// A quaternion rotation.
    Quat = 6,
    /// A length-delimited byte blob (string / small packed struct).
    Bytes = 7,
    /// An enumeration (encoded as a bounded integer).
    Enum = 8,
}

impl TypeTag {
    /// The stable numeric discriminant used in the schema-hash preimage.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

/// Who receives a replicated field (the UE `COND_*` analogue, design §2.2).
/// Rebroadcast honors these per-receiver in /0180; Phase 1 only records
/// them so they participate in the schema hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum RepCondition {
    /// Sent to every relevant receiver.
    #[default]
    None = 0,
    /// Sent only to the owning connection.
    OwnerOnly = 1,
    /// Sent to everyone except the owner.
    SkipOwner = 2,
    /// Sent once, in the initial full snapshot.
    InitialOnly = 3,
    /// Sent only to receivers viewing the object as a simulated proxy.
    SimulatedOnly = 4,
    /// Sent only to the autonomous (predicting) proxy.
    AutonomousOnly = 5,
    /// Gated by a custom, per-object delegate.
    Custom = 6,
    /// Never replicated (local-only field kept in the table for stable ids).
    Never = 7,
}

impl RepCondition {
    /// The stable numeric discriminant used in the schema-hash preimage.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Whether a client may propose changes to a field (design §2.2, §7.2). Default
/// [`FieldAuthority::ServerOnly`]; cosmetics/inputs opt into
/// [`FieldAuthority::ClientOwned`]. The server never trusts the client-declared
/// copy; this value is compiled into the server schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum FieldAuthority {
    /// Authoritative on the server; a client proposal to change it is rejected.
    #[default]
    ServerOnly = 0,
    /// The owning connection may propose values (still bounds/rate checked).
    ClientOwned = 1,
}

impl FieldAuthority {
    /// The stable numeric discriminant used in the schema-hash preimage.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// The server-side validation envelope for a field (design §2.2). Compiled into
/// the schema; the client cannot override it. Phase 1 stores it so it feeds the
/// schema hash via [`FieldBounds::shape`]; enforcement lands in .
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FieldBounds {
    /// No declared bounds.
    None,
    /// Inclusive integer range `[min, max]`.
    IntRange {
        /// Inclusive minimum.
        min: i64,
        /// Inclusive maximum.
        max: i64,
    },
    /// Inclusive scalar range `[min, max]` at `values_per_unit` codes per unit
    /// (mirrors `citadel_wire::codec::ScalarQuant` parameters).
    ScalarRange {
        /// Inclusive minimum.
        min: f32,
        /// Inclusive maximum.
        max: f32,
        /// Quantization codes per unit.
        values_per_unit: u32,
    },
    /// Maximum length in bytes of a [`TypeTag::Bytes`] field.
    MaxLen {
        /// Inclusive maximum length.
        max_len: u32,
    },
    /// Maximum element count of a collection field.
    MaxCardinality {
        /// Inclusive maximum item count.
        max_items: u32,
    },
}

/// FNV-1a offset basis (64-bit).
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a prime (64-bit).
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Deterministic 64-bit FNV-1a fold. Chosen over a cryptographic hash because the
/// values it packs only need to *change when their input changes* and be
/// reproducible across languages; FNV-1a is trivially re-implementable in
/// C++/C#/GDScript.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// The stable per-field key derived from a property name. Never sent on the wire;
/// it only participates in the schema-hash `bounds_shape` fold so a field's
/// identity change moves the hash. Every SDK derives it from the property name
/// with this exact FNV-1a-over-UTF-8 construction.
#[must_use]
pub fn stable_key_from_name(name: &str) -> u64 {
    fnv1a(name.as_bytes())
}

impl FieldBounds {
    /// A canonical fixed-width encoding of the bounds alone (no field identity):
    /// the discriminant in the top byte and a 56-bit FNV-1a fold of the
    /// little-endian parameter bytes in the low bits. [`FieldBounds::None`] is the
    /// reserved all-zero shape.
    ///
    /// This is a diagnostic/base value; the value actually placed in the schema
    /// hash's `bounds_shape` slot is [`combined_bounds_shape`], which also folds in
    /// the field's stable key.
    #[must_use]
    pub fn shape(&self) -> u64 {
        let (disc, payload): (u8, Vec<u8>) = match *self {
            FieldBounds::None => return 0,
            FieldBounds::IntRange { min, max } => {
                let mut p = Vec::with_capacity(16);
                p.extend_from_slice(&min.to_le_bytes());
                p.extend_from_slice(&max.to_le_bytes());
                (1, p)
            }
            FieldBounds::ScalarRange {
                min,
                max,
                values_per_unit,
            } => {
                let mut p = Vec::with_capacity(12);
                // Bit patterns so the fold is exact and language-independent.
                p.extend_from_slice(&min.to_bits().to_le_bytes());
                p.extend_from_slice(&max.to_bits().to_le_bytes());
                p.extend_from_slice(&values_per_unit.to_le_bytes());
                (2, p)
            }
            FieldBounds::MaxLen { max_len } => (3, max_len.to_le_bytes().to_vec()),
            FieldBounds::MaxCardinality { max_items } => (4, max_items.to_le_bytes().to_vec()),
        };
        let fold = fnv1a(&payload) & 0x00FF_FFFF_FFFF_FFFF;
        (u64::from(disc) << 56) | fold
    }
}

/// The value placed in the schema-hash `bounds_shape` slot: a **full 64-bit**
/// FNV-1a fold over the base [`FieldBounds::shape`] and the field's stable key.
/// Full width (not the 56-bit diagnostic form) minimizes bounds-collision risk,
/// and folding the key binds field identity so a same-shaped reorder still moves
/// the hash. Reproduced bit-for-bit by every SDK.
#[must_use]
pub fn combined_bounds_shape(bounds_shape: u64, stable_key: u64) -> u64 {
    let mut buf = [0u8; 16];
    buf[..8].copy_from_slice(&bounds_shape.to_le_bytes());
    buf[8..].copy_from_slice(&stable_key.to_le_bytes());
    fnv1a(&buf)
}

/// One replicated field's descriptor in a [`RepLayout`]. Built once at
/// registration; immutable thereafter (design §2.2).
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDesc {
    /// Stable handle = index in the ordered table.
    pub id: FieldId,
    /// Property name. Not sent on the wire; its [`stable_key_from_name`] fold binds
    /// this field's identity into the schema hash (see the module note).
    pub name: String,
    /// Field type discriminant.
    pub type_tag: TypeTag,
    /// The `citadel_wire::codec::codec_id` used to (de)serialize the field
    /// (encode lands in ; recorded now so it feeds the schema hash).
    pub codec_id: u16,
    /// Replication condition (`COND_*` analogue).
    pub cond: RepCondition,
    /// Field authority (`ServerOnly` / `ClientOwned`).
    pub authority: FieldAuthority,
    /// Server-side validation envelope.
    pub bounds: FieldBounds,
    /// Whether the field is on the push (`mark_dirty`) fast path (`true`) or must
    /// fall to the mandatory shadow-diff safety net (`false`) — design §3.
    pub push_based: bool,
}

impl FieldDesc {
    /// The stable per-field key (folded into the schema hash).
    #[must_use]
    pub fn stable_key(&self) -> u64 {
        stable_key_from_name(&self.name)
    }

    fn as_layout_field(&self) -> LayoutField {
        LayoutField {
            field_id: self.id,
            type_tag: self.type_tag.as_u16(),
            codec_id: self.codec_id,
            cond: self.cond.as_u8(),
            authority: self.authority.as_u8(),
            bounds_shape: combined_bounds_shape(self.bounds.shape(), self.stable_key()),
        }
    }
}

/// An error building a [`RepLayout`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LayoutError {
    /// The class declared more replicated fields than a `field_id: u16` can index.
    #[error("class declared {count} replicated fields; the maximum is {max}")]
    TooManyFields {
        /// Declared field count.
        count: usize,
        /// Maximum supported.
        max: usize,
    },
    /// The canonical schema-hash computation rejected the layout (should not
    /// happen for a builder-produced layout, which is always ascending/unique).
    #[error("schema hash rejected the layout: {0}")]
    Schema(#[from] SchemaError),
}

/// Maximum number of replicated fields per class: `field_id` is a `u16`, so ids
/// `0..=u16::MAX` allow exactly `u16::MAX + 1` fields.
pub const MAX_FIELDS: usize = (u16::MAX as usize) + 1;

/// The immutable, per-class replicated-field table + its canonical identity hash.
/// The Unreal `FCitadelRepLayout` and this struct must agree field-for-field so
/// their [`SchemaHash`] values match (design §2.2, §6).
///
/// The table can only be produced by [`RepLayoutBuilder`], which guarantees the
/// `fields[i].id == i` invariant the dirty mask and shadow buffer rely on; the
/// fields are not publicly constructible to prevent a noncanonical layout.
#[derive(Debug, Clone, PartialEq)]
pub struct RepLayout {
    class_id: u32,
    schema_hash: SchemaHash,
    layout_version: u32,
    fields: Vec<FieldDesc>,
}

impl RepLayout {
    /// Stable class identity.
    #[must_use]
    pub fn class_id(&self) -> u32 {
        self.class_id
    }

    /// The explicit layout version paired with the hash.
    #[must_use]
    pub fn layout_version(&self) -> u32 {
        self.layout_version
    }

    /// The ordered field table; `fields[i].id == i`.
    #[must_use]
    pub fn fields(&self) -> &[FieldDesc] {
        &self.fields
    }

    /// The field descriptor for `id`, if present.
    #[must_use]
    pub fn field(&self, id: FieldId) -> Option<&FieldDesc> {
        self.fields.get(id as usize)
    }

    /// Number of replicated fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Whether the class has no replicated fields.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// The wide canonical schema hash.
    #[must_use]
    pub fn schema_hash(&self) -> &SchemaHash {
        &self.schema_hash
    }

    /// Iterate the ids of push-model (fast-path) fields.
    pub fn push_field_ids(&self) -> impl Iterator<Item = FieldId> + '_ {
        self.fields.iter().filter(|f| f.push_based).map(|f| f.id)
    }

    /// Iterate the ids of shadow-net (non-push) fields.
    pub fn shadow_field_ids(&self) -> impl Iterator<Item = FieldId> + '_ {
        self.fields.iter().filter(|f| !f.push_based).map(|f| f.id)
    }
}

/// Builds a [`RepLayout`] by appending fields in registration order. Each
/// [`RepLayoutBuilder::field`] call assigns the next `field_id` (the registration
/// index), mirroring the Unreal reflection walk over `CPF_Net` properties.
#[derive(Debug, Clone)]
pub struct RepLayoutBuilder {
    class_id: u32,
    layout_version: u32,
    fields: Vec<FieldDesc>,
}

impl RepLayoutBuilder {
    /// Start a layout for `class_id` at `layout_version`.
    #[must_use]
    pub fn new(class_id: u32, layout_version: u32) -> Self {
        Self {
            class_id,
            layout_version,
            fields: Vec::new(),
        }
    }

    /// Append a field in registration order. The `field_id` is assigned
    /// automatically as the current table length. `name` is the property name; it
    /// is never sent on the wire but binds the field's identity into the schema
    /// hash. Reordering or inserting a field mid-list renumbers later handles and
    /// changes the schema hash (design §8; evolution is ).
    // A field descriptor genuinely carries this many independent attributes; a
    // wrapper struct would only move the same arguments to the call site.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn field(
        mut self,
        name: impl Into<String>,
        type_tag: TypeTag,
        codec_id: u16,
        cond: RepCondition,
        authority: FieldAuthority,
        bounds: FieldBounds,
        push_based: bool,
    ) -> Self {
        // Saturate the id; build re-checks the count and returns a typed error
        // rather than allowing a truncated/duplicate handle.
        let id = u16::try_from(self.fields.len()).unwrap_or(u16::MAX);
        self.fields.push(FieldDesc {
            id,
            name: name.into(),
            type_tag,
            codec_id,
            cond,
            authority,
            bounds,
            push_based,
        });
        self
    }

    /// Finalize the layout, computing the canonical schema hash over the ordered
    /// tuples. Fails if the class declared more fields than a `u16` can index.
    pub fn build(self) -> Result<RepLayout, LayoutError> {
        if self.fields.len() > MAX_FIELDS {
            return Err(LayoutError::TooManyFields {
                count: self.fields.len(),
                max: MAX_FIELDS,
            });
        }
        let layout_fields: Vec<LayoutField> =
            self.fields.iter().map(FieldDesc::as_layout_field).collect();
        // Builder-assigned ids are strictly ascending and unique, so schema_hash
        // never returns NonCanonicalFields here.
        let schema_hash = schema::schema_hash(self.layout_version, &layout_fields)?;
        Ok(RepLayout {
            class_id: self.class_id,
            schema_hash,
            layout_version: self.layout_version,
            fields: self.fields,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use citadel_wire::codec::codec_id;

    fn sample() -> RepLayout {
        RepLayoutBuilder::new(1, 1)
            .field(
                "alive",
                TypeTag::Bool,
                codec_id::BOOL,
                RepCondition::None,
                FieldAuthority::ServerOnly,
                FieldBounds::None,
                true,
            )
            .field(
                "health",
                TypeTag::Int,
                codec_id::SCALAR_QUANT,
                RepCondition::SkipOwner,
                FieldAuthority::ServerOnly,
                FieldBounds::IntRange { min: 0, max: 100 },
                true,
            )
            .field(
                "name",
                TypeTag::Bytes,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ClientOwned,
                FieldBounds::MaxLen { max_len: 32 },
                false,
            )
            .build()
            .unwrap()
    }

    #[test]
    fn field_id_is_registration_order() {
        let layout = sample();
        assert_eq!(layout.len(), 3);
        assert_eq!(layout.field(0).unwrap().id, 0);
        assert_eq!(layout.field(1).unwrap().id, 1);
        assert_eq!(layout.field(2).unwrap().id, 2);
        assert!(layout.field(3).is_none());
    }

    #[test]
    fn push_and_shadow_partition() {
        let layout = sample();
        let push: Vec<_> = layout.push_field_ids().collect();
        let shadow: Vec<_> = layout.shadow_field_ids().collect();
        assert_eq!(push, vec![0, 1]);
        assert_eq!(shadow, vec![2]);
    }

    #[test]
    fn schema_hash_is_deterministic() {
        let a = sample();
        let b = sample();
        assert_eq!(a.schema_hash(), b.schema_hash());
        assert_eq!(a.schema_hash().layout_version, 1);
    }

    #[test]
    fn changing_a_field_changes_the_hash() {
        let base = sample();
        let mutated = RepLayoutBuilder::new(1, 1)
            .field(
                "alive",
                TypeTag::Bool,
                codec_id::BOOL,
                RepCondition::None,
                FieldAuthority::ServerOnly,
                FieldBounds::None,
                true,
            )
            .field(
                "health",
                TypeTag::Int,
                codec_id::VECTOR3_QUANT, // changed codec
                RepCondition::SkipOwner,
                FieldAuthority::ServerOnly,
                FieldBounds::IntRange { min: 0, max: 100 },
                true,
            )
            .field(
                "name",
                TypeTag::Bytes,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ClientOwned,
                FieldBounds::MaxLen { max_len: 32 },
                false,
            )
            .build()
            .unwrap();
        assert_ne!(base.schema_hash().bytes, mutated.schema_hash().bytes);
    }

    #[test]
    fn changing_bounds_changes_the_hash() {
        let base = sample();
        let wider = RepLayoutBuilder::new(1, 1)
            .field(
                "alive",
                TypeTag::Bool,
                codec_id::BOOL,
                RepCondition::None,
                FieldAuthority::ServerOnly,
                FieldBounds::None,
                true,
            )
            .field(
                "health",
                TypeTag::Int,
                codec_id::SCALAR_QUANT,
                RepCondition::SkipOwner,
                FieldAuthority::ServerOnly,
                FieldBounds::IntRange { min: 0, max: 200 }, // widened
                true,
            )
            .field(
                "name",
                TypeTag::Bytes,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ClientOwned,
                FieldBounds::MaxLen { max_len: 32 },
                false,
            )
            .build()
            .unwrap();
        assert_ne!(base.schema_hash().bytes, wider.schema_hash().bytes);
    }

    #[test]
    fn renaming_a_field_changes_the_hash() {
        // Field identity is bound into the hash, so renaming (even with identical
        // type/codec/bounds) moves it — this is what closes the same-shaped
        // reorder hole (review finding, module note).
        let base = sample();
        let renamed = RepLayoutBuilder::new(1, 1)
            .field(
                "alive",
                TypeTag::Bool,
                codec_id::BOOL,
                RepCondition::None,
                FieldAuthority::ServerOnly,
                FieldBounds::None,
                true,
            )
            .field(
                "hp", // renamed from "health"; same shape otherwise
                TypeTag::Int,
                codec_id::SCALAR_QUANT,
                RepCondition::SkipOwner,
                FieldAuthority::ServerOnly,
                FieldBounds::IntRange { min: 0, max: 100 },
                true,
            )
            .field(
                "name",
                TypeTag::Bytes,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ClientOwned,
                FieldBounds::MaxLen { max_len: 32 },
                false,
            )
            .build()
            .unwrap();
        assert_ne!(base.schema_hash().bytes, renamed.schema_hash().bytes);
    }

    #[test]
    fn same_shaped_reorder_changes_the_hash() {
        // Two fields with identical type/codec/cond/authority/bounds but different
        // names; swapping their registration order must change the hash because
        // the folded name key binds identity to position.
        let mk = |a: &str, b: &str| {
            RepLayoutBuilder::new(9, 1)
                .field(
                    a,
                    TypeTag::Scalar,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::ScalarRange {
                        min: 0.0,
                        max: 1.0,
                        values_per_unit: 256,
                    },
                    true,
                )
                .field(
                    b,
                    TypeTag::Scalar,
                    codec_id::SCALAR_QUANT,
                    RepCondition::None,
                    FieldAuthority::ServerOnly,
                    FieldBounds::ScalarRange {
                        min: 0.0,
                        max: 1.0,
                        values_per_unit: 256,
                    },
                    true,
                )
                .build()
                .unwrap()
        };
        let pos_x_first = mk("pos_x", "pos_y");
        let pos_y_first = mk("pos_y", "pos_x");
        assert_ne!(
            pos_x_first.schema_hash().bytes,
            pos_y_first.schema_hash().bytes
        );
    }

    #[test]
    fn layout_version_changes_the_hash() {
        let v1 = sample();
        let v2 = RepLayoutBuilder::new(1, 2)
            .field(
                "alive",
                TypeTag::Bool,
                codec_id::BOOL,
                RepCondition::None,
                FieldAuthority::ServerOnly,
                FieldBounds::None,
                true,
            )
            .field(
                "health",
                TypeTag::Int,
                codec_id::SCALAR_QUANT,
                RepCondition::SkipOwner,
                FieldAuthority::ServerOnly,
                FieldBounds::IntRange { min: 0, max: 100 },
                true,
            )
            .field(
                "name",
                TypeTag::Bytes,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ClientOwned,
                FieldBounds::MaxLen { max_len: 32 },
                false,
            )
            .build()
            .unwrap();
        assert_ne!(v1.schema_hash().bytes, v2.schema_hash().bytes);
    }

    #[test]
    fn bounds_shape_none_is_zero_and_others_differ() {
        assert_eq!(FieldBounds::None.shape(), 0);
        let a = FieldBounds::IntRange { min: 0, max: 100 }.shape();
        let b = FieldBounds::IntRange { min: 0, max: 101 }.shape();
        assert_ne!(a, b);
        assert_eq!(a >> 56, 1);
        assert_eq!(FieldBounds::MaxLen { max_len: 1 }.shape() >> 56, 3);
    }

    #[test]
    fn stable_key_is_name_derived_and_distinct() {
        assert_eq!(
            stable_key_from_name("health"),
            stable_key_from_name("health")
        );
        assert_ne!(stable_key_from_name("health"), stable_key_from_name("hp"));
    }

    #[test]
    fn empty_layout_has_stable_hash() {
        let a = RepLayoutBuilder::new(7, 1).build().unwrap();
        let b = RepLayoutBuilder::new(7, 1).build().unwrap();
        assert!(a.is_empty());
        assert_eq!(a.schema_hash(), b.schema_hash());
    }
}
