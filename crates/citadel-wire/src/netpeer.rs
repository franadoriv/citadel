//! NetworkPeer `DeltaBunch` frame bodies (, kinds 13-15 reserved by
//! ).
//!
//! This module defines the *bodies* of the NetworkPeer replication envelopes on
//! the reserved [`crate::protocol`] kind range (`KIND_REP_DELTA` /
//! `KIND_REP_ACK` / `KIND_REP_SCHEMA`); the discriminants live in `protocol.rs`.
//! Everything reuses the frozen  foundation rather than reinventing it:
//! the MSB-first [`crate::bits::BitWriter`]/[`crate::bits::BitReader`], the
//! quantized [`crate::codec`] set, the 128-bit [`crate::schema::SchemaHash`], and
//! the [`crate::baseline`] token/ack model.
//!
//! # The DeltaBunch (design §5)
//!
//! One bit-packed packet per actor per tick batches all of that actor's dirty
//! fields: an `object_id`, an explicit `is_full` flag, the server-issued nonzero
//! `result_id` this bunch establishes, the `base_id` it was diffed against
//! (absent on a full snapshot), the `schema_hash` (present only on `is_full`), a
//! `changed_mask` over the class's fixed field count, and the length-delimited
//! quantized values — plus FastArray-style keyed collection add/remove/change
//! blocks (design §3.3).
//!
//! # Hostile-input hardening (adversarial review, )
//!
//! The framed decoder is a pre-validation attack surface (design §6): it runs
//! before any schema/ownership/rate check, so it must never panic,
//! over-read, or allocate on attacker-controlled lengths. The rules enforced
//! here:
//!
//! - **Per-bunch isolation.** Each coalesced bunch is length-prefixed and decoded
//!   in its own byte window with a canonical [`BitReader::finish`], so a hostile
//!   length inside one bunch cannot shift the cursor into the next bunch and still
//!   pass overall padding (finding 1/12).
//! - **Canonical, type-bounded varints.** Every varint rejects overlong (non-
//!   minimal) encodings, caps its group count, and refuses to overflow `u64`
//!   (finding 2).
//! - **`result_id` is always nonzero; `base_id == 0` means full/none** (finding
//!   3). A non-full bunch with a zero `base_id`, or any bunch with a zero
//!   `result_id`, is rejected.
//! - **Bound-before-consume.** Byte-blob and collection counts are checked against
//!   the schema cap *and* a hard cap before any allocation; a running per-envelope
//!   allocation budget bounds total work (finding 5/E).
//! - **Non-canonical collection ops are rejected**: duplicate `rep_id`s within or
//!   across removed/added/changed, or a total over the hard cap (finding 7).
//! - **Whole-envelope atomicity.** [`decode_bunches`] returns all bunches or an
//!   error; a later malformed bunch never leaves earlier ones half-applied
//!   (finding 12).
//! - **Schema gate.** A full snapshot carries the wide `schema_hash` +
//!   `layout_version`; a mismatch against the local schema fails the whole bunch
//!   closed (finding F), the same posture as the auth handshake.

use std::collections::BTreeMap;

use crate::bits::{BitError, BitReader, BitWriter};
use crate::codec::{CodecError, QuatMode, ScalarQuant, VectorQuant, ceil_log2};
use crate::schema::{SCHEMA_HASH_BYTES, SchemaHash};

/// Object-id wire width in bits (match-unique network id, not a pointer).
pub const OBJECT_ID_BITS: u32 = 32;
/// `layout_version` wire width in bits (present on full snapshots).
pub const LAYOUT_VERSION_BITS: u32 = 32;
/// Maximum groups in a bit-packed varint (`ceil(64 / 7) == 10`).
pub const VARINT_MAX_GROUPS: u32 = 10;
/// Maximum coalesced bunches in one `KIND_REP_DELTA` envelope.
pub const MAX_BUNCHES_PER_ENVELOPE: usize = 4096;
/// Hard cap on a single collection op set (removed/added/changed) regardless of
/// the schema's declared `max_items`. Bounds allocation even if a schema declares
/// an enormous cardinality.
pub const MAX_COLLECTION_OPS: usize = 65_536;
/// Hard cap on a length-delimited byte field regardless of the schema `max_len`.
pub const MAX_BYTES_FIELD_LEN: usize = 1 << 20;
/// Per-envelope total allocation budget (collection items + byte-field bytes).
/// Bounds total decode work across all coalesced bunches (finding E).
pub const MAX_ENVELOPE_ALLOC: usize = 1 << 22;
/// Maximum acked entries in one `KIND_REP_ACK` body.
pub const MAX_ACK_ENTRIES: usize = 8192;
/// Maximum class entries in one `KIND_REP_SCHEMA` table.
pub const MAX_SCHEMA_ENTRIES: usize = 8192;

/// An error encoding or decoding a NetworkPeer replication body. Never panics;
/// every failure mode is a value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RepWireError {
    /// A body was shorter than its fixed framing requires.
    #[error("rep body too short: needed {needed}, got {got}")]
    TooShort {
        /// Bytes required.
        needed: usize,
        /// Bytes present.
        got: usize,
    },
    /// A varint used more than [`VARINT_MAX_GROUPS`] groups.
    #[error("varint too long (exceeded {VARINT_MAX_GROUPS} groups)")]
    VarintTooLong,
    /// A varint overflowed `u64`.
    #[error("varint overflowed u64")]
    VarintOverflow,
    /// A varint used a non-minimal (overlong) encoding.
    #[error("varint is not canonical (overlong encoding)")]
    VarintNonCanonical,
    /// A non-full bunch named a zero `base_id`, or any bunch a zero `result_id`.
    #[error("baseline token was zero (result_id must be nonzero; base_id zero => full)")]
    ZeroBaselineToken,
    /// A field's `RepValue` did not match its schema codec kind.
    #[error("value kind does not match the field codec at field {field_id}")]
    ValueKindMismatch {
        /// The offending field id.
        field_id: u16,
    },
    /// A `changed_mask` bit referenced a field id outside the schema.
    #[error("changed field {field_id} is outside the schema ({num_fields} fields)")]
    FieldOutOfRange {
        /// The offending field id.
        field_id: u16,
        /// The schema field count.
        num_fields: usize,
    },
    /// A byte-field length exceeded the schema cap or the hard cap.
    #[error("byte field length {len} exceeds cap {cap}")]
    BytesTooLong {
        /// Declared length.
        len: usize,
        /// Effective cap.
        cap: usize,
    },
    /// A collection op count exceeded the schema `max_items` or the hard cap.
    #[error("collection op count {count} exceeds cap {cap}")]
    CollectionTooLarge {
        /// Declared count.
        count: usize,
        /// Effective cap.
        cap: usize,
    },
    /// The same `rep_id` appeared more than once within or across a collection's
    /// removed/added/changed sets.
    #[error("duplicate rep_id in collection delta")]
    DuplicateRepId,
    /// The coalesced-bunch count exceeded [`MAX_BUNCHES_PER_ENVELOPE`].
    #[error("bunch count {count} exceeds cap {MAX_BUNCHES_PER_ENVELOPE}")]
    TooManyBunches {
        /// Declared count.
        count: usize,
    },
    /// The per-envelope allocation budget was exhausted.
    #[error("decode allocation budget exhausted")]
    AllocBudgetExceeded,
    /// A full snapshot's `schema_hash`/`layout_version` did not match the local
    /// schema — the layout diverged (fail closed, design §6).
    #[error("schema hash / layout version mismatch on full snapshot")]
    SchemaMismatch,
    /// The schema itself was malformed (e.g. a nested collection).
    #[error("invalid rep schema: {0}")]
    InvalidSchema(&'static str),
    /// A quantized codec rejected a value or code.
    #[error("rep codec error: {0}")]
    Codec(String),
    /// The underlying bit reader/writer failed (overrun / non-canonical padding).
    #[error("rep bit error: {0}")]
    Bit(String),
}

impl From<CodecError> for RepWireError {
    fn from(e: CodecError) -> Self {
        RepWireError::Codec(e.to_string())
    }
}

impl From<BitError> for RepWireError {
    fn from(e: BitError) -> Self {
        RepWireError::Bit(e.to_string())
    }
}

/// The per-field codec of a replicated class, in registration (`field_id`) order.
/// This is the wire-level projection of a `RepLayout` `FieldDesc`; both ends build
/// the identical set so a bunch decodes bit-for-bit.
#[derive(Debug, Clone, PartialEq)]
pub enum RepFieldCodec {
    /// A single boolean (1 bit).
    Bool,
    /// A bounded integer `[min, max]`, fixed-width `ceil_log2(range + 1)` bits;
    /// decode rejects an out-of-range code.
    IntRange {
        /// Inclusive minimum.
        min: i64,
        /// Inclusive maximum.
        max: i64,
    },
    /// A bounded fixed-point scalar (shared [`ScalarQuant`]).
    Scalar(ScalarQuant),
    /// A quantized position vector (shared [`VectorQuant`]).
    Vector3(VectorQuant),
    /// A smallest-three quaternion at the given grade.
    Quat(QuatMode),
    /// A length-delimited byte blob (string / small packed struct), capped.
    Bytes {
        /// Inclusive maximum length in bytes.
        max_len: u32,
    },
    /// A FastArray-style keyed collection whose items use `item` (design §3.3).
    /// `item` may not itself be a collection (depth is capped at 1).
    Collection {
        /// The per-item scalar codec.
        item: Box<RepFieldCodec>,
        /// Inclusive maximum item count.
        max_items: u32,
    },
}

impl RepFieldCodec {
    /// Whether this codec is a collection.
    #[must_use]
    pub fn is_collection(&self) -> bool {
        matches!(self, RepFieldCodec::Collection { .. })
    }

    /// Fixed bit width for `IntRange` (0 range => 0 bits).
    fn int_range_bits(min: i64, max: i64) -> Result<u32, RepWireError> {
        let range = i128::from(max) - i128::from(min);
        if range < 0 {
            return Err(RepWireError::InvalidSchema("IntRange max < min"));
        }
        let range = range as u128;
        if range > u128::from(u64::MAX) {
            return Err(RepWireError::InvalidSchema("IntRange span exceeds u64"));
        }
        if range == u128::from(u64::MAX) {
            return Ok(64);
        }
        Ok(ceil_log2((range as u64) + 1))
    }

    /// Validate the codec is well-formed (used at schema build).
    fn validate(&self, depth: u8) -> Result<(), RepWireError> {
        match self {
            RepFieldCodec::IntRange { min, max } => {
                Self::int_range_bits(*min, *max)?;
                Ok(())
            }
            RepFieldCodec::Collection { item, .. } => {
                if depth >= 1 || item.is_collection() {
                    return Err(RepWireError::InvalidSchema("nested collection"));
                }
                item.validate(depth + 1)
            }
            _ => Ok(()),
        }
    }
}

/// The wire schema of a replicated class: the ordered per-field codecs plus the
/// class identity ([`SchemaHash`]). Both ends must agree field-for-field.
#[derive(Debug, Clone, PartialEq)]
pub struct RepSchema {
    schema_hash: SchemaHash,
    fields: Vec<RepFieldCodec>,
}

impl RepSchema {
    /// Build a schema from the class identity hash and ordered field codecs,
    /// validating each codec (rejects nested collections / malformed int ranges).
    pub fn new(schema_hash: SchemaHash, fields: Vec<RepFieldCodec>) -> Result<Self, RepWireError> {
        for f in &fields {
            f.validate(0)?;
        }
        Ok(Self {
            schema_hash,
            fields,
        })
    }

    /// The wide canonical class identity.
    #[must_use]
    pub fn schema_hash(&self) -> &SchemaHash {
        &self.schema_hash
    }

    /// Number of replicated fields (the `changed_mask` width).
    #[must_use]
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// The codec for `field_id`, if present.
    #[must_use]
    pub fn field(&self, field_id: u16) -> Option<&RepFieldCodec> {
        self.fields.get(field_id as usize)
    }
}

/// A single scalar field value (or a collection item value). Floats are the
/// logical values; quantization is applied by the field codec on encode.
#[derive(Debug, Clone, PartialEq)]
pub enum RepValue {
    /// A boolean.
    Bool(bool),
    /// A bounded integer (widened to `i64`).
    Int(i64),
    /// A scalar `f32`.
    Scalar(f32),
    /// A position vector.
    Vector3([f32; 3]),
    /// A rotation quaternion `(x, y, z, w)`.
    Quat([f32; 4]),
    /// A length-delimited byte blob.
    Bytes(Vec<u8>),
}

/// A generation-tagged stable collection element id (design §3.3). The `gen`
/// counter makes a reused slot a distinct id so a stale removal cannot be
/// confused with a fresh insert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepId {
    /// Slot index within the collection.
    pub index: u32,
    /// Generation; bumped when a slot is reused after removal.
    pub generation: u32,
}

/// One collection element carried in an add/change op: its id, its `rep_key`
/// (bumped on edit, `u64` so it effectively never wraps), and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct CollItem {
    /// The element id.
    pub id: RepId,
    /// The element key (edit counter).
    pub key: u64,
    /// The element value (encoded with the collection's item codec).
    pub value: RepValue,
}

/// The keyed delta for one collection field (design §3.3).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CollectionDelta {
    /// Ids removed since the base (compact list; survivors are not re-sent).
    pub removed: Vec<RepId>,
    /// Elements added since the base (full item).
    pub added: Vec<CollItem>,
    /// Elements changed since the base (full item; item-delta is a later opt).
    pub changed: Vec<CollItem>,
}

impl CollectionDelta {
    fn total_ops(&self) -> usize {
        self.removed.len() + self.added.len() + self.changed.len()
    }
}

/// A single changed field's payload: a scalar value or a collection keyed delta.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldDelta {
    /// A scalar field's new value.
    Value(RepValue),
    /// A collection field's keyed delta.
    Collection(CollectionDelta),
}

/// One bit-packed replication packet for one actor for one tick (design §5).
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaBunch {
    /// Match-unique replicated-object id (network id, not a pointer).
    pub object_id: u32,
    /// Explicit full-snapshot flag (NOT overloaded on `base_id == 0`).
    pub is_full: bool,
    /// The server-issued nonzero token this bunch establishes; acks name it.
    pub result_id: u64,
    /// The token this bunch was diffed against; `0` iff `is_full` (no base).
    pub base_id: u64,
    /// The changed fields, keyed by `field_id`, ascending.
    pub changes: BTreeMap<u16, FieldDelta>,
}

/// The bit-exact, quantized values section of a [`DeltaBunch`].
///
/// This is deliberately not a wire frame: the enclosing bunch still supplies its
/// per-receiver object id and baseline tokens. It exists so a server fan-out can
/// reuse authoritative field quantization when multiple receivers have the same
/// baseline signature.
#[derive(Debug, Clone)]
pub struct PreparedDeltaValues {
    field_ids: Vec<u16>,
    bytes: Vec<u8>,
    bit_len: u64,
}

impl DeltaBunch {
    /// A new empty bunch header. `result_id` must be nonzero; `base_id` is `0`
    /// for a full snapshot, otherwise the nonzero base token.
    #[must_use]
    pub fn new(object_id: u32, is_full: bool, result_id: u64, base_id: u64) -> Self {
        Self {
            object_id,
            is_full,
            result_id,
            base_id,
            changes: BTreeMap::new(),
        }
    }

    /// Insert a changed field.
    pub fn set(&mut self, field_id: u16, delta: FieldDelta) {
        self.changes.insert(field_id, delta);
    }

    /// Encode this bunch to a standalone, byte-aligned, bit-packed blob against
    /// `schema`. Full snapshots embed the schema identity.
    pub fn encode(&self, schema: &RepSchema) -> Result<Vec<u8>, RepWireError> {
        if self.result_id == 0 {
            return Err(RepWireError::ZeroBaselineToken);
        }
        if !self.is_full && self.base_id == 0 {
            return Err(RepWireError::ZeroBaselineToken);
        }
        // Validate every change against the schema before emitting any bits.
        for (&field_id, delta) in &self.changes {
            let codec = schema
                .field(field_id)
                .ok_or(RepWireError::FieldOutOfRange {
                    field_id,
                    num_fields: schema.num_fields(),
                })?;
            match (codec, delta) {
                (RepFieldCodec::Collection { .. }, FieldDelta::Collection(_)) => {}
                (RepFieldCodec::Collection { .. }, FieldDelta::Value(_))
                | (_, FieldDelta::Collection(_)) => {
                    return Err(RepWireError::ValueKindMismatch { field_id });
                }
                (_, FieldDelta::Value(v)) => value_matches(codec, v)
                    .then_some(())
                    .ok_or(RepWireError::ValueKindMismatch { field_id })?,
            }
        }

        let mut w = BitWriter::new();
        w.write_bits(u64::from(self.object_id), OBJECT_ID_BITS)?;
        w.write_bool(self.is_full)?;
        write_bit_varint(&mut w, self.result_id)?;
        if self.is_full {
            for &b in &schema.schema_hash().bytes {
                w.write_bits(u64::from(b), 8)?;
            }
            w.write_bits(
                u64::from(schema.schema_hash().layout_version),
                LAYOUT_VERSION_BITS,
            )?;
        } else {
            write_bit_varint(&mut w, self.base_id)?;
        }

        // changed_mask: exactly num_fields bits, MSB = field 0.
        let num_fields = schema.num_fields();
        for field_id in 0..num_fields {
            let set = self.changes.contains_key(&(field_id as u16));
            w.write_bool(set)?;
        }

        // Values / collection blocks in ascending field order.
        for (&field_id, delta) in &self.changes {
            let codec = schema
                .field(field_id)
                .ok_or(RepWireError::FieldOutOfRange {
                    field_id,
                    num_fields,
                })?;
            match delta {
                FieldDelta::Value(v) => encode_value(&mut w, codec, v)?,
                FieldDelta::Collection(c) => encode_collection(&mut w, codec, c)?,
            }
        }
        Ok(w.into_bytes())
    }

    /// Quantize and encode only this bunch's values section for reuse in another
    /// bunch with the same changed-field set.
    ///
    /// The returned payload has no header or changed mask and cannot be sent on
    /// its own. [`Self::encode_with_prepared_values`] validates that it is used
    /// with precisely the same changed fields before splicing it into a bunch.
    pub fn prepare_values(&self, schema: &RepSchema) -> Result<PreparedDeltaValues, RepWireError> {
        // Validate before emitting so a failed preparation cannot leave a partial
        // shared payload around for another receiver.
        for (&field_id, delta) in &self.changes {
            let codec = schema
                .field(field_id)
                .ok_or(RepWireError::FieldOutOfRange {
                    field_id,
                    num_fields: schema.num_fields(),
                })?;
            match (codec, delta) {
                (RepFieldCodec::Collection { .. }, FieldDelta::Collection(_)) => {}
                (RepFieldCodec::Collection { .. }, FieldDelta::Value(_))
                | (_, FieldDelta::Collection(_)) => {
                    return Err(RepWireError::ValueKindMismatch { field_id });
                }
                (_, FieldDelta::Value(v)) => value_matches(codec, v)
                    .then_some(())
                    .ok_or(RepWireError::ValueKindMismatch { field_id })?,
            }
        }

        let mut values = BitWriter::new();
        for (&field_id, delta) in &self.changes {
            let codec = schema
                .field(field_id)
                .ok_or(RepWireError::FieldOutOfRange {
                    field_id,
                    num_fields: schema.num_fields(),
                })?;
            match delta {
                FieldDelta::Value(v) => encode_value(&mut values, codec, v)?,
                FieldDelta::Collection(c) => encode_collection(&mut values, codec, c)?,
            }
        }
        let (bytes, bit_len) = values.finish();
        Ok(PreparedDeltaValues {
            field_ids: self.changes.keys().copied().collect(),
            bytes,
            bit_len,
        })
    }

    /// Encode this bunch while reusing a quantized values section prepared from a
    /// bunch with the identical changed-field set.
    pub fn encode_with_prepared_values(
        &self,
        schema: &RepSchema,
        prepared: &PreparedDeltaValues,
    ) -> Result<Vec<u8>, RepWireError> {
        if self.result_id == 0 || (!self.is_full && self.base_id == 0) {
            return Err(RepWireError::ZeroBaselineToken);
        }
        if self.changes.len() != prepared.field_ids.len()
            || !self
                .changes
                .keys()
                .copied()
                .eq(prepared.field_ids.iter().copied())
        {
            return Err(RepWireError::InvalidSchema(
                "prepared values do not match the bunch changed fields",
            ));
        }

        let mut w = BitWriter::new();
        w.write_bits(u64::from(self.object_id), OBJECT_ID_BITS)?;
        w.write_bool(self.is_full)?;
        write_bit_varint(&mut w, self.result_id)?;
        if self.is_full {
            for &b in &schema.schema_hash().bytes {
                w.write_bits(u64::from(b), 8)?;
            }
            w.write_bits(
                u64::from(schema.schema_hash().layout_version),
                LAYOUT_VERSION_BITS,
            )?;
        } else {
            write_bit_varint(&mut w, self.base_id)?;
        }
        for field_id in 0..schema.num_fields() {
            w.write_bool(self.changes.contains_key(&(field_id as u16)))?;
        }
        w.write_packed(&prepared.bytes, prepared.bit_len)?;
        Ok(w.into_bytes())
    }

    /// Decode a standalone bunch blob produced by [`DeltaBunch::encode`] against
    /// `schema`, with a running per-envelope allocation `budget`.
    pub fn decode(
        body: &[u8],
        schema: &RepSchema,
        budget: &mut usize,
    ) -> Result<Self, RepWireError> {
        let mut r = BitReader::over_bytes(body);
        let bunch = Self::decode_from(&mut r, schema, budget)?;
        // Canonical termination: only zero pad to the byte boundary may remain.
        r.finish()?;
        Ok(bunch)
    }

    fn decode_from(
        r: &mut BitReader<'_>,
        schema: &RepSchema,
        budget: &mut usize,
    ) -> Result<Self, RepWireError> {
        let header = DeltaHeader::parse(r, schema)?;
        Self::decode_values(r, schema, budget, header)
    }

    /// Read just the `object_id` (the first 32 bits, MSB-first) from a bunch blob
    /// without any schema. The untrusted-input pipeline resolves `object_id ->
    /// class` first (design §7.1 step 3), then uses the object's server-fixed schema
    /// to parse the rest of the header. Returns `None` if the body is too short to
    /// hold the id.
    #[must_use]
    pub fn peek_object_id(body: &[u8]) -> Option<u32> {
        if body.len() < 4 {
            return None;
        }
        Some(u32::from_be_bytes([body[0], body[1], body[2], body[3]]))
    }

    /// Read the fixed-position `is_full` flag without a schema or decoding field
    /// values. Returns `None` when the body cannot contain the flag.
    #[must_use]
    pub fn peek_is_full(body: &[u8]) -> Option<bool> {
        body.get((OBJECT_ID_BITS / 8) as usize)
            .map(|byte| byte & 0x80 != 0)
    }

    /// Read a full snapshot's embedded schema identity without a [`RepSchema`] or
    /// decoding field values. This is deliberately a header-only read used by a
    /// server to choose the one exact accepted schema before strict decoding.
    ///
    /// Returns `None` for delta bunches and for malformed or truncated full
    /// headers. It does not relax or replace [`Self::decode`]: callers must still
    /// decode the whole bunch against the selected schema, which rejects trailing
    /// bytes and noncanonical encodings.
    #[must_use]
    pub fn peek_full_schema(body: &[u8]) -> Option<([u8; SCHEMA_HASH_BYTES], u32)> {
        let mut r = BitReader::over_bytes(body);
        r.read_bits(OBJECT_ID_BITS).ok()?;
        if !r.read_bool().ok()? {
            return None;
        }
        let result_id = read_bit_varint(&mut r).ok()?;
        if result_id == 0 {
            return None;
        }
        let mut bytes = [0u8; SCHEMA_HASH_BYTES];
        for byte in &mut bytes {
            *byte = r.read_bits(8).ok()? as u8;
        }
        let layout_version = r.read_bits(LAYOUT_VERSION_BITS).ok()? as u32;
        Some((bytes, layout_version))
    }

    /// Parse only the header + `changed_mask` of a standalone bunch blob, WITHOUT
    /// decoding any field values (design §7.1 step 2). This is the cheap-reject
    /// surface: the untrusted-input pipeline resolves the object, checks ownership
    /// and rate against the returned [`DeltaHeader`], and only calls
    /// [`DeltaBunch::decode`]/[`DeltaBunch::decode_gated`] to decode values once
    /// those cheap checks pass. A full snapshot's embedded `schema_hash` is still
    /// checked here (fail closed), so a mismatched layout is rejected before any
    /// value work.
    pub fn peek_header(body: &[u8], schema: &RepSchema) -> Result<DeltaHeader, RepWireError> {
        let mut r = BitReader::over_bytes(body);
        DeltaHeader::parse(&mut r, schema)
    }

    /// Decode a standalone bunch, invoking `gate` after the header + `changed_mask`
    /// are parsed but **before** any field value is decoded (design §7.1: cheap-
    /// reject-first / decode-values-last). If `gate` returns `Err`, no value is
    /// decoded and the error is surfaced as [`DecodeGateError::Gate`]; a malformed
    /// frame surfaces as [`DecodeGateError::Wire`].
    pub fn decode_gated<F, E>(
        body: &[u8],
        schema: &RepSchema,
        budget: &mut usize,
        gate: F,
    ) -> Result<Self, DecodeGateError<E>>
    where
        F: FnOnce(&DeltaHeader) -> Result<(), E>,
    {
        let mut r = BitReader::over_bytes(body);
        let header = DeltaHeader::parse(&mut r, schema).map_err(DecodeGateError::Wire)?;
        gate(&header).map_err(DecodeGateError::Gate)?;
        let bunch =
            Self::decode_values(&mut r, schema, budget, header).map_err(DecodeGateError::Wire)?;
        r.finish().map_err(|e| DecodeGateError::Wire(e.into()))?;
        Ok(bunch)
    }

    fn decode_values(
        r: &mut BitReader<'_>,
        schema: &RepSchema,
        budget: &mut usize,
        header: DeltaHeader,
    ) -> Result<Self, RepWireError> {
        let num_fields = schema.num_fields();
        let mut changes = BTreeMap::new();
        for field_id in header.changed_fields {
            // field_id < num_fields by construction, so this never misses.
            let codec = schema
                .field(field_id)
                .ok_or(RepWireError::FieldOutOfRange {
                    field_id,
                    num_fields,
                })?;
            let delta = if codec.is_collection() {
                FieldDelta::Collection(decode_collection(r, codec, budget)?)
            } else {
                FieldDelta::Value(decode_value(r, codec, budget)?)
            };
            changes.insert(field_id, delta);
        }

        Ok(Self {
            object_id: header.object_id,
            is_full: header.is_full,
            result_id: header.result_id,
            base_id: header.base_id,
            changes,
        })
    }
}

/// The header + changed-field set of a [`DeltaBunch`], parsed without decoding any
/// field values (design §7.1 step 2, cheap-reject-first). The untrusted-input
/// pipeline resolves/authorizes against this before decoding values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaHeader {
    /// Match-unique replicated-object id.
    pub object_id: u32,
    /// Whether this is a full snapshot.
    pub is_full: bool,
    /// The server-issued nonzero token this bunch establishes.
    pub result_id: u64,
    /// The token diffed against; `0` iff `is_full`.
    pub base_id: u64,
    /// The field ids the `changed_mask` marked, ascending. Values not yet decoded.
    pub changed_fields: Vec<u16>,
}

impl DeltaHeader {
    fn parse(r: &mut BitReader<'_>, schema: &RepSchema) -> Result<Self, RepWireError> {
        let object_id = r.read_bits(OBJECT_ID_BITS)? as u32;
        let is_full = r.read_bool()?;
        let result_id = read_bit_varint(r)?;
        if result_id == 0 {
            return Err(RepWireError::ZeroBaselineToken);
        }
        let base_id = if is_full {
            let mut bytes = [0u8; SCHEMA_HASH_BYTES];
            for b in &mut bytes {
                *b = r.read_bits(8)? as u8;
            }
            let layout_version = r.read_bits(LAYOUT_VERSION_BITS)? as u32;
            let expected = schema.schema_hash();
            if bytes != expected.bytes || layout_version != expected.layout_version {
                return Err(RepWireError::SchemaMismatch);
            }
            0
        } else {
            let base = read_bit_varint(r)?;
            if base == 0 {
                return Err(RepWireError::ZeroBaselineToken);
            }
            base
        };

        let num_fields = schema.num_fields();
        let mut changed_fields = Vec::new();
        for field_id in 0..num_fields {
            if r.read_bool()? {
                changed_fields.push(field_id as u16);
            }
        }
        Ok(Self {
            object_id,
            is_full,
            result_id,
            base_id,
            changed_fields,
        })
    }
}

/// The outcome of [`DeltaBunch::decode_gated`]: either a malformed frame or a
/// rejection from the caller-supplied gate (cheap-reject before value decode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeGateError<E> {
    /// The frame was malformed (framing/varint/schema/overrun).
    Wire(RepWireError),
    /// The caller's gate rejected the bunch after the header was parsed.
    Gate(E),
}

/// Encode one or more bunches into a single `KIND_REP_DELTA` envelope body.
///
/// Layout: `bunch_count` (byte varint) then, per bunch, `byte_len` (byte varint)
/// followed by the bunch's standalone blob. Each bunch stays isolated so a
/// hostile length in one cannot corrupt the next (finding 1).
pub fn encode_bunches(bunches: &[DeltaBunch], schema: &RepSchema) -> Result<Vec<u8>, RepWireError> {
    if bunches.len() > MAX_BUNCHES_PER_ENVELOPE {
        return Err(RepWireError::TooManyBunches {
            count: bunches.len(),
        });
    }
    let mut out = Vec::new();
    write_uvarint(&mut out, bunches.len() as u64);
    for bunch in bunches {
        let blob = bunch.encode(schema)?;
        write_uvarint(&mut out, blob.len() as u64);
        out.extend_from_slice(&blob);
    }
    Ok(out)
}

/// Decode a `KIND_REP_DELTA` envelope body into its bunches. All-or-nothing: a
/// malformed later bunch aborts the whole envelope with no partial result
/// (finding 12).
pub fn decode_bunches(body: &[u8], schema: &RepSchema) -> Result<Vec<DeltaBunch>, RepWireError> {
    let mut pos = 0usize;
    let count = read_uvarint(body, &mut pos)? as usize;
    if count > MAX_BUNCHES_PER_ENVELOPE {
        return Err(RepWireError::TooManyBunches { count });
    }
    let mut budget = MAX_ENVELOPE_ALLOC;
    let mut bunches = Vec::with_capacity(count.min(64));
    for _ in 0..count {
        let len = read_uvarint(body, &mut pos)? as usize;
        let end = pos.checked_add(len).ok_or(RepWireError::TooShort {
            needed: len,
            got: body.len().saturating_sub(pos),
        })?;
        if end > body.len() {
            return Err(RepWireError::TooShort {
                needed: len,
                got: body.len().saturating_sub(pos),
            });
        }
        let bunch = DeltaBunch::decode(&body[pos..end], schema, &mut budget)?;
        bunches.push(bunch);
        pos = end;
    }
    if pos != body.len() {
        return Err(RepWireError::TooShort {
            needed: pos,
            got: body.len(),
        });
    }
    Ok(bunches)
}

// --- field value codec --------------------------------------------------------

fn value_matches(codec: &RepFieldCodec, v: &RepValue) -> bool {
    matches!(
        (codec, v),
        (RepFieldCodec::Bool, RepValue::Bool(_))
            | (RepFieldCodec::IntRange { .. }, RepValue::Int(_))
            | (RepFieldCodec::Scalar(_), RepValue::Scalar(_))
            | (RepFieldCodec::Vector3(_), RepValue::Vector3(_))
            | (RepFieldCodec::Quat(_), RepValue::Quat(_))
            | (RepFieldCodec::Bytes { .. }, RepValue::Bytes(_))
    )
}

fn encode_value(
    w: &mut BitWriter,
    codec: &RepFieldCodec,
    v: &RepValue,
) -> Result<(), RepWireError> {
    match (codec, v) {
        (RepFieldCodec::Bool, RepValue::Bool(b)) => {
            w.write_bool(*b)?;
        }
        (RepFieldCodec::IntRange { min, max }, RepValue::Int(val)) => {
            let bits = RepFieldCodec::int_range_bits(*min, *max)?;
            // Encode saturates to the range (never wraps).
            let clamped = (*val).clamp(*min, *max);
            let code = (i128::from(clamped) - i128::from(*min)) as u128 as u64;
            w.write_bits(code, bits)?;
        }
        (RepFieldCodec::Scalar(q), RepValue::Scalar(s)) => q.write(w, *s)?,
        (RepFieldCodec::Vector3(q), RepValue::Vector3(p)) => q.write(w, *p)?,
        (RepFieldCodec::Quat(mode), RepValue::Quat(quat)) => {
            crate::codec::encode_quat(w, *quat, *mode)?;
        }
        (RepFieldCodec::Bytes { max_len }, RepValue::Bytes(bytes)) => {
            let cap = (*max_len as usize).min(MAX_BYTES_FIELD_LEN);
            if bytes.len() > cap {
                return Err(RepWireError::BytesTooLong {
                    len: bytes.len(),
                    cap,
                });
            }
            write_bit_varint(w, bytes.len() as u64)?;
            for &b in bytes {
                w.write_bits(u64::from(b), 8)?;
            }
        }
        _ => return Err(RepWireError::InvalidSchema("value/codec mismatch")),
    }
    Ok(())
}

fn decode_value(
    r: &mut BitReader<'_>,
    codec: &RepFieldCodec,
    budget: &mut usize,
) -> Result<RepValue, RepWireError> {
    Ok(match codec {
        RepFieldCodec::Bool => RepValue::Bool(r.read_bool()?),
        RepFieldCodec::IntRange { min, max } => {
            let bits = RepFieldCodec::int_range_bits(*min, *max)?;
            let code = r.read_bits(bits)?;
            let range = (i128::from(*max) - i128::from(*min)) as u128;
            if u128::from(code) > range {
                return Err(RepWireError::Codec(format!(
                    "int code {code} out of range 0..={range}"
                )));
            }
            RepValue::Int((i128::from(*min) + i128::from(code)) as i64)
        }
        RepFieldCodec::Scalar(q) => RepValue::Scalar(q.read(r)?),
        RepFieldCodec::Vector3(q) => RepValue::Vector3(q.read(r)?),
        RepFieldCodec::Quat(mode) => RepValue::Quat(crate::codec::decode_quat(r, *mode)?),
        RepFieldCodec::Bytes { max_len } => {
            let cap = (*max_len as usize).min(MAX_BYTES_FIELD_LEN);
            let len = read_bit_varint(r)? as usize;
            if len > cap {
                return Err(RepWireError::BytesTooLong { len, cap });
            }
            spend(budget, len)?;
            let mut bytes = Vec::with_capacity(len);
            for _ in 0..len {
                bytes.push(r.read_bits(8)? as u8);
            }
            RepValue::Bytes(bytes)
        }
        RepFieldCodec::Collection { .. } => {
            return Err(RepWireError::InvalidSchema("collection decoded as value"));
        }
    })
}

// --- collection codec ---------------------------------------------------------

fn encode_collection(
    w: &mut BitWriter,
    codec: &RepFieldCodec,
    c: &CollectionDelta,
) -> Result<(), RepWireError> {
    let (item, max_items) = match codec {
        RepFieldCodec::Collection { item, max_items } => (item.as_ref(), *max_items as usize),
        _ => {
            return Err(RepWireError::InvalidSchema(
                "collection on non-collection field",
            ));
        }
    };
    let cap = max_items.min(MAX_COLLECTION_OPS);
    // Reject non-canonical ops before emitting (finding 7): a hard total-ops cap
    // and unique rep_ids across all three sets.
    if c.total_ops() > MAX_COLLECTION_OPS {
        return Err(RepWireError::CollectionTooLarge {
            count: c.total_ops(),
            cap: MAX_COLLECTION_OPS,
        });
    }
    check_unique_rep_ids(c)?;

    encode_id_list(w, &c.removed, cap)?;
    encode_item_list(w, item, &c.added, cap)?;
    encode_item_list(w, item, &c.changed, cap)?;
    Ok(())
}

fn decode_collection(
    r: &mut BitReader<'_>,
    codec: &RepFieldCodec,
    budget: &mut usize,
) -> Result<CollectionDelta, RepWireError> {
    let (item, max_items) = match codec {
        RepFieldCodec::Collection { item, max_items } => (item.as_ref(), *max_items as usize),
        _ => {
            return Err(RepWireError::InvalidSchema(
                "collection on non-collection field",
            ));
        }
    };
    let cap = max_items.min(MAX_COLLECTION_OPS);
    let removed = decode_id_list(r, cap, budget)?;
    let added = decode_item_list(r, item, cap, budget)?;
    let changed = decode_item_list(r, item, cap, budget)?;
    let out = CollectionDelta {
        removed,
        added,
        changed,
    };
    if out.total_ops() > MAX_COLLECTION_OPS {
        return Err(RepWireError::CollectionTooLarge {
            count: out.total_ops(),
            cap: MAX_COLLECTION_OPS,
        });
    }
    check_unique_rep_ids(&out)?;
    Ok(out)
}

fn check_unique_rep_ids(c: &CollectionDelta) -> Result<(), RepWireError> {
    let mut seen = std::collections::BTreeSet::new();
    for id in &c.removed {
        if !seen.insert(*id) {
            return Err(RepWireError::DuplicateRepId);
        }
    }
    for it in c.added.iter().chain(c.changed.iter()) {
        if !seen.insert(it.id) {
            return Err(RepWireError::DuplicateRepId);
        }
    }
    Ok(())
}

fn encode_id_list(w: &mut BitWriter, ids: &[RepId], cap: usize) -> Result<(), RepWireError> {
    if ids.len() > cap {
        return Err(RepWireError::CollectionTooLarge {
            count: ids.len(),
            cap,
        });
    }
    write_bit_varint(w, ids.len() as u64)?;
    for id in ids {
        write_bit_varint(w, u64::from(id.index))?;
        write_bit_varint(w, u64::from(id.generation))?;
    }
    Ok(())
}

fn decode_id_list(
    r: &mut BitReader<'_>,
    cap: usize,
    budget: &mut usize,
) -> Result<Vec<RepId>, RepWireError> {
    let count = read_bit_varint(r)? as usize;
    if count > cap {
        return Err(RepWireError::CollectionTooLarge { count, cap });
    }
    spend(budget, count)?;
    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        let index = read_u32_varint(r)?;
        let generation = read_u32_varint(r)?;
        ids.push(RepId { index, generation });
    }
    Ok(ids)
}

fn encode_item_list(
    w: &mut BitWriter,
    item: &RepFieldCodec,
    items: &[CollItem],
    cap: usize,
) -> Result<(), RepWireError> {
    if items.len() > cap {
        return Err(RepWireError::CollectionTooLarge {
            count: items.len(),
            cap,
        });
    }
    write_bit_varint(w, items.len() as u64)?;
    for it in items {
        write_bit_varint(w, u64::from(it.id.index))?;
        write_bit_varint(w, u64::from(it.id.generation))?;
        write_bit_varint(w, it.key)?;
        encode_value(w, item, &it.value)?;
    }
    Ok(())
}

fn decode_item_list(
    r: &mut BitReader<'_>,
    item: &RepFieldCodec,
    cap: usize,
    budget: &mut usize,
) -> Result<Vec<CollItem>, RepWireError> {
    let count = read_bit_varint(r)? as usize;
    if count > cap {
        return Err(RepWireError::CollectionTooLarge { count, cap });
    }
    spend(budget, count)?;
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let index = read_u32_varint(r)?;
        let generation = read_u32_varint(r)?;
        let key = read_bit_varint(r)?;
        let value = decode_value(r, item, budget)?;
        items.push(CollItem {
            id: RepId { index, generation },
            key,
            value,
        });
    }
    Ok(items)
}

fn spend(budget: &mut usize, n: usize) -> Result<(), RepWireError> {
    match budget.checked_sub(n) {
        Some(rest) => {
            *budget = rest;
            Ok(())
        }
        None => Err(RepWireError::AllocBudgetExceeded),
    }
}

// --- ack + schema-table bodies ------------------------------------------------

/// One receiver's ack of an object's baseline: the object and the newest
/// `result_id` it applied plus the shared 32-bit ack history window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepAckEntry {
    /// The object acked.
    pub object_id: u32,
    /// The newest applied `result_id` (`0` = nothing yet).
    pub acked_result_id: u64,
    /// The 32-bit ack history preceding `acked_result_id`.
    pub history: u32,
}

/// The `KIND_REP_ACK` body: a list of per-object baseline acks.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepAck {
    /// The acked entries.
    pub entries: Vec<RepAckEntry>,
}

impl RepAck {
    /// Encode the ack body: `count` varint then per entry `object_id` (32 bits),
    /// `acked_result_id` varint, `history` (32 bits).
    pub fn encode(&self) -> Result<Vec<u8>, RepWireError> {
        if self.entries.len() > MAX_ACK_ENTRIES {
            return Err(RepWireError::CollectionTooLarge {
                count: self.entries.len(),
                cap: MAX_ACK_ENTRIES,
            });
        }
        let mut w = BitWriter::new();
        write_bit_varint(&mut w, self.entries.len() as u64)?;
        for e in &self.entries {
            w.write_bits(u64::from(e.object_id), OBJECT_ID_BITS)?;
            write_bit_varint(&mut w, e.acked_result_id)?;
            w.write_bits(u64::from(e.history), 32)?;
        }
        Ok(w.into_bytes())
    }

    /// Decode an ack body produced by [`RepAck::encode`].
    pub fn decode(body: &[u8]) -> Result<Self, RepWireError> {
        let mut r = BitReader::over_bytes(body);
        let count = read_bit_varint(&mut r)? as usize;
        if count > MAX_ACK_ENTRIES {
            return Err(RepWireError::CollectionTooLarge {
                count,
                cap: MAX_ACK_ENTRIES,
            });
        }
        let mut entries = Vec::with_capacity(count.min(64));
        for _ in 0..count {
            let object_id = r.read_bits(OBJECT_ID_BITS)? as u32;
            let acked_result_id = read_bit_varint(&mut r)?;
            let history = r.read_bits(32)? as u32;
            entries.push(RepAckEntry {
                object_id,
                acked_result_id,
                history,
            });
        }
        r.finish()?;
        Ok(Self { entries })
    }
}

/// One class's schema identity, sent server→client on join so a client can gate
/// its own encode/decode on a matching layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepSchemaEntry {
    /// The class id.
    pub class_id: u32,
    /// The 128-bit schema hash.
    pub schema_hash: [u8; SCHEMA_HASH_BYTES],
    /// The layout version.
    pub layout_version: u32,
}

/// The `KIND_REP_SCHEMA` body: a `class_id → schema_hash` table.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepSchemaTable {
    /// The class entries.
    pub entries: Vec<RepSchemaEntry>,
}

impl RepSchemaTable {
    /// Encode the schema table.
    pub fn encode(&self) -> Result<Vec<u8>, RepWireError> {
        if self.entries.len() > MAX_SCHEMA_ENTRIES {
            return Err(RepWireError::CollectionTooLarge {
                count: self.entries.len(),
                cap: MAX_SCHEMA_ENTRIES,
            });
        }
        let mut w = BitWriter::new();
        write_bit_varint(&mut w, self.entries.len() as u64)?;
        for e in &self.entries {
            w.write_bits(u64::from(e.class_id), 32)?;
            for &b in &e.schema_hash {
                w.write_bits(u64::from(b), 8)?;
            }
            w.write_bits(u64::from(e.layout_version), LAYOUT_VERSION_BITS)?;
        }
        Ok(w.into_bytes())
    }

    /// Decode a schema table produced by [`RepSchemaTable::encode`].
    pub fn decode(body: &[u8]) -> Result<Self, RepWireError> {
        let mut r = BitReader::over_bytes(body);
        let count = read_bit_varint(&mut r)? as usize;
        if count > MAX_SCHEMA_ENTRIES {
            return Err(RepWireError::CollectionTooLarge {
                count,
                cap: MAX_SCHEMA_ENTRIES,
            });
        }
        let mut entries = Vec::with_capacity(count.min(64));
        for _ in 0..count {
            let class_id = r.read_bits(32)? as u32;
            let mut schema_hash = [0u8; SCHEMA_HASH_BYTES];
            for b in &mut schema_hash {
                *b = r.read_bits(8)? as u8;
            }
            let layout_version = r.read_bits(LAYOUT_VERSION_BITS)? as u32;
            entries.push(RepSchemaEntry {
                class_id,
                schema_hash,
                layout_version,
            });
        }
        r.finish()?;
        Ok(Self { entries })
    }
}

// --- varints ------------------------------------------------------------------

/// Write a bit-packed LEB128 varint: groups of `[continuation:1][data:7]`,
/// least-significant group first.
fn write_bit_varint(w: &mut BitWriter, mut value: u64) -> Result<(), RepWireError> {
    loop {
        let group = value & 0x7F;
        value >>= 7;
        let more = value != 0;
        w.write_bool(more)?;
        w.write_bits(group, 7)?;
        if !more {
            break;
        }
    }
    Ok(())
}

/// Read a canonical bit-packed varint. Rejects overlong (non-minimal) encodings,
/// more than [`VARINT_MAX_GROUPS`] groups, and `u64` overflow (finding 2).
fn read_bit_varint(r: &mut BitReader<'_>) -> Result<u64, RepWireError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut groups: u32 = 0;
    loop {
        if groups >= VARINT_MAX_GROUPS {
            return Err(RepWireError::VarintTooLong);
        }
        let more = r.read_bool()?;
        let group = r.read_bits(7)?;
        // The final group must fit the remaining bits of a u64 without overflow.
        if shift >= 64 {
            return Err(RepWireError::VarintOverflow);
        }
        if shift == 63 && group > 1 {
            return Err(RepWireError::VarintOverflow);
        }
        value |= group << shift;
        groups += 1;
        if !more {
            // Canonical: a multi-group encoding must not end with an all-zero
            // top group (that would be an overlong encoding of a smaller value).
            if groups > 1 && group == 0 {
                return Err(RepWireError::VarintNonCanonical);
            }
            return Ok(value);
        }
        shift += 7;
    }
}

/// Read a bit varint that must fit `u32` (collection index / gen).
fn read_u32_varint(r: &mut BitReader<'_>) -> Result<u32, RepWireError> {
    let v = read_bit_varint(r)?;
    u32::try_from(v).map_err(|_| RepWireError::VarintOverflow)
}

/// Write a canonical byte-oriented LEB128 varint (envelope framing).
fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            out.push(byte | 0x80);
        } else {
            out.push(byte);
            break;
        }
    }
}

/// Read a canonical byte-oriented LEB128 varint. Rejects overlong encodings,
/// more than 10 bytes, and `u64` overflow.
fn read_uvarint(buf: &[u8], pos: &mut usize) -> Result<u64, RepWireError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut bytes: u32 = 0;
    loop {
        if bytes >= VARINT_MAX_GROUPS {
            return Err(RepWireError::VarintTooLong);
        }
        let byte = *buf.get(*pos).ok_or(RepWireError::TooShort {
            needed: *pos + 1,
            got: buf.len(),
        })?;
        *pos += 1;
        let data = u64::from(byte & 0x7F);
        if shift >= 64 || (shift == 63 && data > 1) {
            return Err(RepWireError::VarintOverflow);
        }
        value |= data << shift;
        bytes += 1;
        if byte & 0x80 == 0 {
            if bytes > 1 && data == 0 {
                return Err(RepWireError::VarintNonCanonical);
            }
            return Ok(value);
        }
        shift += 7;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::codec::{DEFAULT_WORLD_BOUNDS, codec_id};
    use crate::schema::{LayoutField, schema_hash};

    fn hash(v: u32) -> SchemaHash {
        schema_hash(
            v,
            &[LayoutField {
                field_id: 0,
                type_tag: 1,
                codec_id: codec_id::BOOL,
                cond: 0,
                authority: 0,
                bounds_shape: 0,
            }],
        )
        .unwrap()
    }

    fn scalar_schema() -> RepSchema {
        RepSchema::new(
            hash(1),
            vec![
                RepFieldCodec::Bool,
                RepFieldCodec::IntRange { min: 0, max: 100 },
                RepFieldCodec::Scalar(ScalarQuant::new(-1.0, 1.0, 1024).unwrap()),
                RepFieldCodec::Vector3(VectorQuant::new(DEFAULT_WORLD_BOUNDS).unwrap()),
                RepFieldCodec::Quat(QuatMode::Bits10),
                RepFieldCodec::Bytes { max_len: 32 },
            ],
        )
        .unwrap()
    }

    fn coll_schema() -> RepSchema {
        RepSchema::new(
            hash(1),
            vec![
                RepFieldCodec::IntRange { min: 0, max: 255 },
                RepFieldCodec::Collection {
                    item: Box::new(RepFieldCodec::IntRange { min: 0, max: 1000 }),
                    max_items: 64,
                },
            ],
        )
        .unwrap()
    }

    fn rt(bunch: &DeltaBunch, schema: &RepSchema) -> DeltaBunch {
        let mut budget = MAX_ENVELOPE_ALLOC;
        let blob = bunch.encode(schema).unwrap();
        DeltaBunch::decode(&blob, schema, &mut budget).unwrap()
    }

    #[test]
    fn scalar_delta_round_trip() {
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(42, false, 7, 3);
        b.set(0, FieldDelta::Value(RepValue::Bool(true)));
        b.set(1, FieldDelta::Value(RepValue::Int(73)));
        b.set(5, FieldDelta::Value(RepValue::Bytes(b"hi".to_vec())));
        let back = rt(&b, &schema);
        assert_eq!(back.object_id, 42);
        assert!(!back.is_full);
        assert_eq!(back.result_id, 7);
        assert_eq!(back.base_id, 3);
        assert_eq!(back.changes.len(), 3);
        assert_eq!(back.changes[&0], FieldDelta::Value(RepValue::Bool(true)));
        assert_eq!(back.changes[&1], FieldDelta::Value(RepValue::Int(73)));
        assert_eq!(
            back.changes[&5],
            FieldDelta::Value(RepValue::Bytes(b"hi".to_vec()))
        );
    }

    #[test]
    fn prepared_values_splice_is_bit_exact() {
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(42, true, 7, 0);
        b.set(0, FieldDelta::Value(RepValue::Bool(true)));
        b.set(1, FieldDelta::Value(RepValue::Int(73)));
        b.set(5, FieldDelta::Value(RepValue::Bytes(b"hi".to_vec())));

        let prepared = b.prepare_values(&schema).unwrap();
        assert_eq!(
            b.encode_with_prepared_values(&schema, &prepared).unwrap(),
            b.encode(&schema).unwrap()
        );
    }

    #[test]
    fn full_snapshot_embeds_and_checks_schema() {
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(1, true, 1, 0);
        b.set(1, FieldDelta::Value(RepValue::Int(100)));
        let blob = b.encode(&schema).unwrap();
        // Same schema decodes.
        let mut budget = MAX_ENVELOPE_ALLOC;
        let back = DeltaBunch::decode(&blob, &schema, &mut budget).unwrap();
        assert!(back.is_full);
        assert_eq!(back.base_id, 0);
        // A schema whose hash differs (different layout_version) is rejected.
        let other = RepSchema::new(hash(2), scalar_schema_fields()).unwrap();
        let mut budget = MAX_ENVELOPE_ALLOC;
        assert_eq!(
            DeltaBunch::decode(&blob, &other, &mut budget),
            Err(RepWireError::SchemaMismatch)
        );
    }

    fn scalar_schema_fields() -> Vec<RepFieldCodec> {
        vec![
            RepFieldCodec::Bool,
            RepFieldCodec::IntRange { min: 0, max: 100 },
            RepFieldCodec::Scalar(ScalarQuant::new(-1.0, 1.0, 1024).unwrap()),
            RepFieldCodec::Vector3(VectorQuant::new(DEFAULT_WORLD_BOUNDS).unwrap()),
            RepFieldCodec::Quat(QuatMode::Bits10),
            RepFieldCodec::Bytes { max_len: 32 },
        ]
    }

    #[test]
    fn zero_result_id_is_rejected() {
        let schema = scalar_schema();
        let b = DeltaBunch::new(1, false, 0, 3);
        assert_eq!(b.encode(&schema), Err(RepWireError::ZeroBaselineToken));
    }

    #[test]
    fn non_full_with_zero_base_is_rejected() {
        let schema = scalar_schema();
        let b = DeltaBunch::new(1, false, 5, 0);
        assert_eq!(b.encode(&schema), Err(RepWireError::ZeroBaselineToken));
    }

    #[test]
    fn collection_round_trip_all_ops() {
        let schema = coll_schema();
        let mut b = DeltaBunch::new(9, false, 4, 1);
        b.set(0, FieldDelta::Value(RepValue::Int(200)));
        let coll = CollectionDelta {
            removed: vec![
                RepId {
                    index: 1,
                    generation: 0,
                },
                RepId {
                    index: 2,
                    generation: 3,
                },
            ],
            added: vec![CollItem {
                id: RepId {
                    index: 5,
                    generation: 0,
                },
                key: 1,
                value: RepValue::Int(500),
            }],
            changed: vec![CollItem {
                id: RepId {
                    index: 3,
                    generation: 1,
                },
                key: 42,
                value: RepValue::Int(999),
            }],
        };
        b.set(1, FieldDelta::Collection(coll.clone()));
        let back = rt(&b, &schema);
        assert_eq!(back.changes[&1], FieldDelta::Collection(coll));
    }

    #[test]
    fn removal_heavy_collection_does_not_resend_survivors() {
        // A removal-heavy delta (guards the FastArray over-replication edge): many
        // removed ids, no added/changed, so the survivors are never in the frame.
        let schema = coll_schema();
        let mut b = DeltaBunch::new(1, false, 2, 1);
        let removed: Vec<RepId> = (0..50)
            .map(|i| RepId {
                index: i,
                generation: 0,
            })
            .collect();
        b.set(
            1,
            FieldDelta::Collection(CollectionDelta {
                removed: removed.clone(),
                added: vec![],
                changed: vec![],
            }),
        );
        let back = rt(&b, &schema);
        match &back.changes[&1] {
            FieldDelta::Collection(c) => {
                assert_eq!(c.removed, removed);
                assert!(c.added.is_empty() && c.changed.is_empty());
            }
            _ => panic!("expected collection"),
        }
    }

    #[test]
    fn rep_id_reuse_across_generations_is_distinct() {
        // Remove (7, gen 0), add (7, gen 1): a reused slot is a distinct id, so
        // the two are not treated as duplicates.
        let schema = coll_schema();
        let mut b = DeltaBunch::new(1, false, 2, 1);
        b.set(
            1,
            FieldDelta::Collection(CollectionDelta {
                removed: vec![RepId {
                    index: 7,
                    generation: 0,
                }],
                added: vec![CollItem {
                    id: RepId {
                        index: 7,
                        generation: 1,
                    },
                    key: 1,
                    value: RepValue::Int(1),
                }],
                changed: vec![],
            }),
        );
        let back = rt(&b, &schema);
        assert!(matches!(back.changes[&1], FieldDelta::Collection(_)));
    }

    #[test]
    fn duplicate_rep_id_across_sets_is_rejected() {
        let schema = coll_schema();
        let mut b = DeltaBunch::new(1, false, 2, 1);
        b.set(
            1,
            FieldDelta::Collection(CollectionDelta {
                removed: vec![RepId {
                    index: 7,
                    generation: 0,
                }],
                added: vec![CollItem {
                    id: RepId {
                        index: 7,
                        generation: 0,
                    }, // same id as removed
                    key: 1,
                    value: RepValue::Int(1),
                }],
                changed: vec![],
            }),
        );
        assert_eq!(b.encode(&schema), Err(RepWireError::DuplicateRepId));
    }

    #[test]
    fn rep_key_wrap_high_value_round_trips() {
        // A near-wrap u64 rep_key survives the varint round-trip intact.
        let schema = coll_schema();
        let mut b = DeltaBunch::new(1, false, 2, 1);
        b.set(
            1,
            FieldDelta::Collection(CollectionDelta {
                removed: vec![],
                added: vec![CollItem {
                    id: RepId {
                        index: 0,
                        generation: 0,
                    },
                    key: u64::MAX - 1,
                    value: RepValue::Int(1),
                }],
                changed: vec![],
            }),
        );
        let back = rt(&b, &schema);
        match &back.changes[&1] {
            FieldDelta::Collection(c) => assert_eq!(c.added[0].key, u64::MAX - 1),
            _ => panic!(),
        }
    }

    #[test]
    fn bad_length_aborts_whole_bunch_no_partial() {
        // A truncated body must abort the whole bunch, never yield a partial one.
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(1, false, 2, 1);
        b.set(0, FieldDelta::Value(RepValue::Bool(true)));
        b.set(5, FieldDelta::Value(RepValue::Bytes(b"abcd".to_vec())));
        let mut blob = b.encode(&schema).unwrap();
        blob.truncate(blob.len() - 1);
        let mut budget = MAX_ENVELOPE_ALLOC;
        assert!(DeltaBunch::decode(&blob, &schema, &mut budget).is_err());
    }

    #[test]
    fn oversized_byte_field_is_rejected_on_encode() {
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(1, false, 2, 1);
        b.set(5, FieldDelta::Value(RepValue::Bytes(vec![0u8; 100]))); // cap 32
        assert!(matches!(
            b.encode(&schema),
            Err(RepWireError::BytesTooLong { .. })
        ));
    }

    #[test]
    fn coalesced_bunches_round_trip_and_isolate() {
        let schema = scalar_schema();
        let mut a = DeltaBunch::new(1, false, 2, 1);
        a.set(0, FieldDelta::Value(RepValue::Bool(true)));
        let mut c = DeltaBunch::new(2, true, 1, 0);
        c.set(1, FieldDelta::Value(RepValue::Int(50)));
        let body = encode_bunches(&[a.clone(), c.clone()], &schema).unwrap();
        let back = decode_bunches(&body, &schema).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0], a);
        assert_eq!(back[1], c);
    }

    #[test]
    fn coalesced_trailing_junk_is_rejected() {
        let schema = scalar_schema();
        let a = DeltaBunch::new(1, false, 2, 1);
        let mut body = encode_bunches(&[a], &schema).unwrap();
        body.push(0xFF); // trailing junk after the last bunch
        assert!(decode_bunches(&body, &schema).is_err());
    }

    #[test]
    fn varint_round_trip_and_canonical() {
        for v in [
            0u64,
            1,
            127,
            128,
            300,
            u32::MAX as u64,
            u64::MAX - 1,
            u64::MAX,
        ] {
            let mut w = BitWriter::new();
            write_bit_varint(&mut w, v).unwrap();
            let bytes = w.into_bytes();
            let mut r = BitReader::over_bytes(&bytes);
            assert_eq!(read_bit_varint(&mut r).unwrap(), v);
        }
    }

    #[test]
    fn varint_overlong_is_rejected() {
        // Two groups encoding the value 0 (continuation set on the first) is a
        // non-minimal encoding of 0 and must be rejected.
        let mut w = BitWriter::new();
        w.write_bool(true).unwrap(); // continuation
        w.write_bits(0, 7).unwrap();
        w.write_bool(false).unwrap();
        w.write_bits(0, 7).unwrap(); // overlong top group == 0
        let bytes = w.into_bytes();
        let mut r = BitReader::over_bytes(&bytes);
        assert_eq!(
            read_bit_varint(&mut r),
            Err(RepWireError::VarintNonCanonical)
        );
    }

    #[test]
    fn byte_uvarint_round_trip_and_canonical() {
        for v in [0u64, 1, 127, 128, 16384, u64::MAX] {
            let mut out = Vec::new();
            write_uvarint(&mut out, v);
            let mut pos = 0;
            assert_eq!(read_uvarint(&out, &mut pos).unwrap(), v);
            assert_eq!(pos, out.len());
        }
        // Overlong: [0x80, 0x00] encodes 0 in two bytes.
        let mut pos = 0;
        assert_eq!(
            read_uvarint(&[0x80, 0x00], &mut pos),
            Err(RepWireError::VarintNonCanonical)
        );
    }

    #[test]
    fn collection_count_over_cap_is_rejected_before_alloc() {
        // Hand-craft a body claiming a huge removed count for a small-cap field.
        let schema = coll_schema(); // field 1 max_items = 64
        let mut w = BitWriter::new();
        w.write_bits(1, OBJECT_ID_BITS).unwrap(); // object_id
        w.write_bool(false).unwrap(); // is_full
        write_bit_varint(&mut w, 2).unwrap(); // result_id
        write_bit_varint(&mut w, 1).unwrap(); // base_id
        // mask: field 0 clear, field 1 set (2 fields).
        w.write_bool(false).unwrap();
        w.write_bool(true).unwrap();
        // collection removed_count = 1000 (> cap 64).
        write_bit_varint(&mut w, 1000).unwrap();
        let blob = w.into_bytes();
        let mut budget = MAX_ENVELOPE_ALLOC;
        assert!(matches!(
            DeltaBunch::decode(&blob, &schema, &mut budget),
            Err(RepWireError::CollectionTooLarge { .. })
        ));
    }

    #[test]
    fn nested_collection_schema_is_rejected() {
        let err = RepSchema::new(
            hash(1),
            vec![RepFieldCodec::Collection {
                item: Box::new(RepFieldCodec::Collection {
                    item: Box::new(RepFieldCodec::Bool),
                    max_items: 4,
                }),
                max_items: 4,
            }],
        );
        assert!(matches!(err, Err(RepWireError::InvalidSchema(_))));
    }

    #[test]
    fn value_kind_mismatch_is_rejected() {
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(1, false, 2, 1);
        // field 0 is Bool; give it an Int.
        b.set(0, FieldDelta::Value(RepValue::Int(1)));
        assert_eq!(
            b.encode(&schema),
            Err(RepWireError::ValueKindMismatch { field_id: 0 })
        );
    }

    #[test]
    fn ack_round_trips() {
        let ack = RepAck {
            entries: vec![
                RepAckEntry {
                    object_id: 1,
                    acked_result_id: 9,
                    history: 0b1011,
                },
                RepAckEntry {
                    object_id: 2,
                    acked_result_id: 100,
                    history: 0,
                },
            ],
        };
        assert_eq!(RepAck::decode(&ack.encode().unwrap()).unwrap(), ack);
    }

    #[test]
    fn schema_table_round_trips() {
        let table = RepSchemaTable {
            entries: vec![RepSchemaEntry {
                class_id: 7,
                schema_hash: hash(3).bytes,
                layout_version: 3,
            }],
        };
        assert_eq!(
            RepSchemaTable::decode(&table.encode().unwrap()).unwrap(),
            table
        );
    }

    #[test]
    fn peek_header_parses_without_decoding_values() {
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(42, false, 7, 3);
        b.set(1, FieldDelta::Value(RepValue::Int(73)));
        b.set(5, FieldDelta::Value(RepValue::Bytes(b"hi".to_vec())));
        let blob = b.encode(&schema).unwrap();
        let header = DeltaBunch::peek_header(&blob, &schema).unwrap();
        assert_eq!(header.object_id, 42);
        assert!(!header.is_full);
        assert_eq!(header.result_id, 7);
        assert_eq!(header.base_id, 3);
        assert_eq!(header.changed_fields, vec![1, 5]);
    }

    #[test]
    fn peek_header_checks_full_snapshot_schema() {
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(1, true, 1, 0);
        b.set(1, FieldDelta::Value(RepValue::Int(50)));
        let blob = b.encode(&schema).unwrap();
        let other = RepSchema::new(hash(2), scalar_schema_fields()).unwrap();
        assert_eq!(
            DeltaBunch::peek_header(&blob, &other),
            Err(RepWireError::SchemaMismatch)
        );
    }

    #[test]
    fn peek_full_schema_reads_full_snapshot_only() {
        let schema = scalar_schema();
        let full = DeltaBunch::new(42, true, 7, 0).encode(&schema).unwrap();
        assert_eq!(
            DeltaBunch::peek_full_schema(&full),
            Some((
                schema.schema_hash().bytes,
                schema.schema_hash().layout_version
            ))
        );

        let delta = DeltaBunch::new(42, false, 8, 7).encode(&schema).unwrap();
        assert_eq!(DeltaBunch::peek_full_schema(&delta), None);
    }

    #[test]
    fn decode_gated_skips_values_when_gate_rejects() {
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(9, false, 4, 1);
        b.set(1, FieldDelta::Value(RepValue::Int(50)));
        let blob = b.encode(&schema).unwrap();
        let mut budget = MAX_ENVELOPE_ALLOC;
        // Gate rejects on object id; values are never decoded.
        let out: Result<DeltaBunch, DecodeGateError<&str>> =
            DeltaBunch::decode_gated(&blob, &schema, &mut budget, |h| {
                assert_eq!(h.object_id, 9);
                assert_eq!(h.changed_fields, vec![1]);
                Err("not-owner")
            });
        assert_eq!(out, Err(DecodeGateError::Gate("not-owner")));
    }

    #[test]
    fn decode_gated_decodes_values_when_gate_accepts() {
        let schema = scalar_schema();
        let mut b = DeltaBunch::new(9, false, 4, 1);
        b.set(1, FieldDelta::Value(RepValue::Int(50)));
        let blob = b.encode(&schema).unwrap();
        let mut budget = MAX_ENVELOPE_ALLOC;
        let out: Result<DeltaBunch, DecodeGateError<()>> =
            DeltaBunch::decode_gated(&blob, &schema, &mut budget, |_| Ok(()));
        let bunch = out.expect("gate accepts");
        assert_eq!(
            bunch.changes.get(&1),
            Some(&FieldDelta::Value(RepValue::Int(50)))
        );
    }

    #[test]
    fn int_range_decode_rejects_out_of_range_code() {
        // A 3-field int range 0..=4 uses 3 bits (code_count 5); code 5,6,7 are
        // invalid and must be rejected, not clamped.
        let schema =
            RepSchema::new(hash(1), vec![RepFieldCodec::IntRange { min: 0, max: 4 }]).unwrap();
        let mut w = BitWriter::new();
        w.write_bits(9, OBJECT_ID_BITS).unwrap();
        w.write_bool(false).unwrap();
        write_bit_varint(&mut w, 2).unwrap();
        write_bit_varint(&mut w, 1).unwrap();
        w.write_bool(true).unwrap(); // field 0 set
        w.write_bits(6, 3).unwrap(); // code 6 > range 4
        let blob = w.into_bytes();
        let mut budget = MAX_ENVELOPE_ALLOC;
        assert!(matches!(
            DeltaBunch::decode(&blob, &schema, &mut budget),
            Err(RepWireError::Codec(_))
        ));
    }
}
