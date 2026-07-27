//! `schema_hash`: the wide canonical layout hash shared by both advanced-netcode
//! tracks (, NetworkPeer §6). A 128-bit BLAKE3-derived digest over the
//! ordered replicated-field layout plus an explicit `layout_version`, so two
//! independently built SDKs detect any layout divergence and cannot silently
//! decode a mismatched class.
//!
//! # Canonical input encoding (adversarial review, )
//!
//! The digest is over a fully-pinned byte preimage so it is reproducible in any
//! language:
//!
//! ```text
//! preimage = DOMAIN_TAG
//!          || layout_version : u32 little-endian
//!          || field_count    : u32 little-endian
//!          || for each field, in strictly ascending field_id order:
//!               field_id     : u16 LE
//!               type_tag     : u16 LE
//!               codec_id     : u16 LE
//!               cond         : u8
//!               authority    : u8
//!               bounds_shape : u64 LE
//! digest = first 16 bytes of BLAKE3(preimage)
//! ```
//!
//! Fields MUST be provided in strictly ascending `field_id` order with no
//! duplicates; [`schema_hash`] rejects any other ordering so a permuted layout
//! cannot collide with a canonical one. Every member is fixed-width
//! little-endian with no padding. A 128-bit truncation is collision-safe for a
//! schema-identity/downgrade-protection use (it is not a MAC).

/// Domain-separation tag + version marker for the schema-hash preimage. Bumping
/// the trailing version invalidates all prior digests deliberately.
pub const SCHEMA_HASH_DOMAIN: &[u8] = b"citadel.schema.v1";

/// Width of a [`SchemaHash`] digest in bytes (128 bits).
pub const SCHEMA_HASH_BYTES: usize = 16;

/// Stable identifier of the schema-hash algorithm, recorded in the client
/// contract (`contract.json`) so SDKs pin the exact construction.
pub const SCHEMA_HASH_ALGORITHM: &str = "blake3-128/citadel.schema.v1";

/// One replicated field's identity in a class layout. All members feed the
/// [`schema_hash`] preimage; `bounds_shape` is a codec-defined fixed-width
/// encoding of the field's validation envelope (range/len/cardinality) so a
/// bounds change also changes the hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutField {
    /// Stable handle = index in the ordered table.
    pub field_id: u16,
    /// Field type discriminant (feature-task defined).
    pub type_tag: u16,
    /// The [`crate::codec::codec_id`] used to (de)serialize the field.
    pub codec_id: u16,
    /// Replication condition (`COND_*` analogue) discriminant.
    pub cond: u8,
    /// Field authority (`ServerOnly` / `ClientOwned`) discriminant.
    pub authority: u8,
    /// Codec-defined fixed-width shape of the field's validation bounds.
    pub bounds_shape: u64,
}

/// A wide canonical schema digest and the layout version it was computed for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaHash {
    /// 128-bit BLAKE3-derived digest of the ordered layout.
    pub bytes: [u8; SCHEMA_HASH_BYTES],
    /// The explicit layout version paired with the digest.
    pub layout_version: u32,
}

impl SchemaHash {
    /// Whether this layout satisfies a server-enforced minimum accepted version.
    /// A client cannot negotiate down to a weaker/older layout.
    #[must_use]
    pub fn accepts_min_version(&self, min_version: u32) -> bool {
        self.layout_version >= min_version
    }

    /// Lowercase hex of the digest (for logs, fixtures, and the contract).
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(SCHEMA_HASH_BYTES * 2);
        for b in self.bytes {
            s.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
            s.push(char::from_digit((b & 0xF) as u32, 16).unwrap_or('0'));
        }
        s
    }
}

/// An error computing a [`schema_hash`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    /// Fields were not in strictly ascending `field_id` order (or contained a
    /// duplicate), so the preimage would not be canonical.
    #[error("layout fields must be strictly ascending by field_id with no duplicates")]
    NonCanonicalFields,
    /// The field count did not fit the fixed-width `u32` preimage slot.
    #[error("layout has too many fields for the u32 count slot")]
    TooManyFields,
}

/// Compute the canonical 128-bit schema hash for an ordered field layout.
///
/// `fields` must be in strictly ascending `field_id` order with no duplicates.
pub fn schema_hash(layout_version: u32, fields: &[LayoutField]) -> Result<SchemaHash, SchemaError> {
    if fields.len() > u32::MAX as usize {
        return Err(SchemaError::TooManyFields);
    }
    // Enforce canonical ordering (strictly ascending, unique).
    for pair in fields.windows(2) {
        if pair[1].field_id <= pair[0].field_id {
            return Err(SchemaError::NonCanonicalFields);
        }
    }

    let mut hasher = blake3::Hasher::new();
    hasher.update(SCHEMA_HASH_DOMAIN);
    hasher.update(&layout_version.to_le_bytes());
    hasher.update(&(fields.len() as u32).to_le_bytes());
    for f in fields {
        hasher.update(&f.field_id.to_le_bytes());
        hasher.update(&f.type_tag.to_le_bytes());
        hasher.update(&f.codec_id.to_le_bytes());
        hasher.update(&[f.cond]);
        hasher.update(&[f.authority]);
        hasher.update(&f.bounds_shape.to_le_bytes());
    }
    let full = hasher.finalize();
    let mut bytes = [0u8; SCHEMA_HASH_BYTES];
    bytes.copy_from_slice(&full.as_bytes()[..SCHEMA_HASH_BYTES]);
    Ok(SchemaHash {
        bytes,
        layout_version,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::codec::codec_id;

    fn sample_layout() -> Vec<LayoutField> {
        vec![
            LayoutField {
                field_id: 0,
                type_tag: 1,
                codec_id: codec_id::BOOL,
                cond: 0,
                authority: 0,
                bounds_shape: 0,
            },
            LayoutField {
                field_id: 1,
                type_tag: 2,
                codec_id: codec_id::SCALAR_QUANT,
                cond: 1,
                authority: 1,
                bounds_shape: 0x0000_0064_0000_0000, // e.g. max_len=100
            },
            LayoutField {
                field_id: 2,
                type_tag: 3,
                codec_id: codec_id::VECTOR3_QUANT,
                cond: 0,
                authority: 0,
                bounds_shape: 8,
            },
        ]
    }

    #[test]
    fn hash_is_deterministic() {
        let layout = sample_layout();
        let a = schema_hash(1, &layout).unwrap();
        let b = schema_hash(1, &layout).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.bytes.len(), SCHEMA_HASH_BYTES);
    }

    #[test]
    fn layout_version_changes_hash() {
        let layout = sample_layout();
        let a = schema_hash(1, &layout).unwrap();
        let b = schema_hash(2, &layout).unwrap();
        assert_ne!(a.bytes, b.bytes);
        assert_eq!(a.layout_version, 1);
        assert_eq!(b.layout_version, 2);
    }

    #[test]
    fn field_change_changes_hash() {
        let layout = sample_layout();
        let base = schema_hash(1, &layout).unwrap();
        let mut mutated = layout.clone();
        mutated[1].codec_id = codec_id::QUAT_SMALLEST3_10;
        let changed = schema_hash(1, &mutated).unwrap();
        assert_ne!(base.bytes, changed.bytes);
    }

    #[test]
    fn non_canonical_order_is_rejected() {
        let layout = sample_layout();
        let mut swapped = layout.clone();
        swapped.swap(0, 1);
        assert_eq!(
            schema_hash(1, &swapped),
            Err(SchemaError::NonCanonicalFields)
        );
        // Duplicate field_id also rejected.
        let dup = vec![layout[0], layout[0]];
        assert_eq!(schema_hash(1, &dup), Err(SchemaError::NonCanonicalFields));
    }

    #[test]
    fn min_version_gate() {
        let h = schema_hash(3, &sample_layout()).unwrap();
        assert!(h.accepts_min_version(2));
        assert!(h.accepts_min_version(3));
        assert!(!h.accepts_min_version(4));
    }

    #[test]
    fn empty_layout_hashes() {
        // A class with no replicated fields still has a stable identity.
        let h = schema_hash(1, &[]).unwrap();
        assert_eq!(h.bytes.len(), SCHEMA_HASH_BYTES);
    }
}
