//! C ABI over the shared `citadel-wire` quantized codecs.
//!
//! These entrypoints expose the exact same codec kernel the Rust server and SDK
//! use, so every native engine (Unity/Unreal/Godot) quantizes through ONE
//! implementation and cannot drift from the wire contract. The functions are
//! thin, pure wrappers: they call directly into `citadel_wire::codec`, so a
//! byte-for-byte parity test against the native path holds by construction (see
//! `tests/codec_ffi_parity.rs`). Every entrypoint is `catch_unwind`-guarded so
//! no panic can cross the boundary.

use std::panic::{AssertUnwindSafe, catch_unwind};

use citadel_wire::codec::{QuatMode, ScalarQuant, decode_quat_components, encode_quat_components};
use citadel_wire::netpeer::{
    DeltaBunch, FieldDelta, MAX_ENVELOPE_ALLOC, RepFieldCodec, RepSchema, RepValue,
};
use citadel_wire::schema::{LayoutField, SCHEMA_HASH_BYTES, SchemaHash, schema_hash};

use crate::CitadelStatus;

/// Run a codec FFI body, mapping a panic to [`CitadelStatus::Internal`].
fn guard<F: FnOnce() -> CitadelStatus>(f: F) -> CitadelStatus {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(status) => status,
        Err(_) => CitadelStatus::Internal,
    }
}

/// Quantize a scalar to its integer code using the bounded fixed-point codec.
///
/// Writes the code to `*out_code`. Returns [`CitadelStatus::InvalidArgument`]
/// for a null out-pointer, an invalid spec (`max <= min`, `values_per_unit == 0`,
/// non-finite bound), or a `NaN` value (`±Inf` saturate to the bounds).
///
/// # Safety
/// `out_code` must be a valid, writable `*mut u64`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_quantize_scalar(
    min: f32,
    max: f32,
    values_per_unit: u32,
    value: f32,
    out_code: *mut u64,
) -> CitadelStatus {
    guard(|| {
        if out_code.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        let spec = match ScalarQuant::new(min, max, values_per_unit) {
            Ok(s) => s,
            Err(_) => return CitadelStatus::InvalidArgument,
        };
        match spec.encode_value(value) {
            Ok(code) => {
                // SAFETY: `out_code` is non-null (checked) and caller-writable.
                unsafe { *out_code = code };
                CitadelStatus::Ok
            }
            Err(_) => CitadelStatus::InvalidArgument,
        }
    })
}

/// Dequantize a scalar code back to its value using the bounded fixed-point
/// codec. Writes the value to `*out_value`. Returns
/// [`CitadelStatus::InvalidArgument`] for a null out-pointer, an invalid spec,
/// or a code outside the codec's valid range (a malformed frame).
///
/// # Safety
/// `out_value` must be a valid, writable `*mut f32`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_dequantize_scalar(
    min: f32,
    max: f32,
    values_per_unit: u32,
    code: u64,
    out_value: *mut f32,
) -> CitadelStatus {
    guard(|| {
        if out_value.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        let spec = match ScalarQuant::new(min, max, values_per_unit) {
            Ok(s) => s,
            Err(_) => return CitadelStatus::InvalidArgument,
        };
        match spec.decode_value(code) {
            Ok(value) => {
                // SAFETY: `out_value` is non-null (checked) and caller-writable.
                unsafe { *out_value = value };
                CitadelStatus::Ok
            }
            Err(_) => CitadelStatus::InvalidArgument,
        }
    })
}

/// Encode a quaternion `(x, y, z, w)` to its smallest-three code representation.
///
/// `quat` points to 4 floats `[x, y, z, w]`. `bits_per_component` is `9`, `10`,
/// or `15`. Writes the 2-bit dropped index to `*out_index` and the three kept
/// component codes to `out_codes[0..3]`. Degenerate inputs (`NaN`/zero-norm)
/// encode as identity. Returns [`CitadelStatus::InvalidArgument`] on a null
/// pointer or unsupported `bits_per_component`.
///
/// # Safety
/// `quat` must point to 4 readable floats; `out_index` to a writable `u8`;
/// `out_codes` to 3 writable `u64`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_quat_encode_components(
    quat: *const f32,
    bits_per_component: u32,
    out_index: *mut u8,
    out_codes: *mut u64,
) -> CitadelStatus {
    guard(|| {
        if quat.is_null() || out_index.is_null() || out_codes.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        let Some(mode) = QuatMode::from_bits(bits_per_component) else {
            return CitadelStatus::InvalidArgument;
        };
        // SAFETY: caller guarantees `quat` points to 4 readable floats.
        let q = unsafe { std::slice::from_raw_parts(quat, 4) };
        let (index, codes) = encode_quat_components([q[0], q[1], q[2], q[3]], mode);
        // SAFETY: out pointers are non-null (checked) and sized per the contract.
        unsafe {
            *out_index = index;
            std::ptr::copy_nonoverlapping(codes.as_ptr(), out_codes, 3);
        }
        CitadelStatus::Ok
    })
}

/// Decode a smallest-three quaternion from its code representation into
/// `out_quat[0..4]` = `[x, y, z, w]`. `index` is masked to `0..=3` and codes to
/// the mode width, so no input panics or produces `NaN`. Returns
/// [`CitadelStatus::InvalidArgument`] on a null pointer or unsupported
/// `bits_per_component`.
///
/// # Safety
/// `codes` must point to 3 readable `u64`; `out_quat` to 4 writable floats.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_quat_decode_components(
    index: u8,
    codes: *const u64,
    bits_per_component: u32,
    out_quat: *mut f32,
) -> CitadelStatus {
    guard(|| {
        if codes.is_null() || out_quat.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        let Some(mode) = QuatMode::from_bits(bits_per_component) else {
            return CitadelStatus::InvalidArgument;
        };
        // SAFETY: caller guarantees `codes` points to 3 readable u64.
        let c = unsafe { std::slice::from_raw_parts(codes, 3) };
        let quat = decode_quat_components(index, [c[0], c[1], c[2]], mode);
        // SAFETY: `out_quat` points to 4 writable floats (contract).
        unsafe { std::ptr::copy_nonoverlapping(quat.as_ptr(), out_quat, 4) };
        CitadelStatus::Ok
    })
}

// --- schema_hash: close the Phase-1 gap ---------------------------
//
// Phase 1 left the Unreal `SchemaHash` zeroed because the BLAKE3-128 digest could
// not be reimplemented natively. This entrypoint exposes the exact shared
// `citadel_wire::schema::schema_hash` so an SDK computes the SAME 128-bit class
// identity the server does — the `bounds_shape` FNV fold is already reproduced
// natively (see CitadelNetworkPeer.cpp), so only the digest crosses the ABI.

/// One replicated field's identity tuple, matching
/// `citadel_wire::schema::LayoutField` (the `schema_hash` preimage). `#[repr(C)]`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CitadelSchemaField {
    /// Stable handle = index in the ordered table.
    pub field_id: u16,
    /// Field type discriminant.
    pub type_tag: u16,
    /// Codec id used to (de)serialize the field.
    pub codec_id: u16,
    /// Replication condition discriminant.
    pub cond: u8,
    /// Field authority discriminant.
    pub authority: u8,
    /// Codec-defined fixed-width bounds shape (folds the field's stable key).
    pub bounds_shape: u64,
}

/// Compute the wide canonical 128-bit schema hash for an ordered field layout,
/// writing the digest to `out_hash[0..16]`. Returns [`CitadelStatus::InvalidArgument`]
/// for a null pointer, or if the fields are not strictly ascending by `field_id`
/// (the canonical-ordering requirement) or exceed the count slot.
///
/// # Safety
/// `fields` must point to `count` readable [`CitadelSchemaField`]; `out_hash`
/// must point to at least 16 writable bytes (or `fields` may be null iff
/// `count == 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_schema_hash(
    layout_version: u32,
    fields: *const CitadelSchemaField,
    count: usize,
    out_hash: *mut u8,
) -> CitadelStatus {
    guard(|| {
        if out_hash.is_null() || (fields.is_null() && count != 0) {
            return CitadelStatus::InvalidArgument;
        }
        let src = if count == 0 {
            &[][..]
        } else {
            // SAFETY: caller guarantees `fields` points to `count` readable structs.
            unsafe { std::slice::from_raw_parts(fields, count) }
        };
        let layout: Vec<LayoutField> = src
            .iter()
            .map(|f| LayoutField {
                field_id: f.field_id,
                type_tag: f.type_tag,
                codec_id: f.codec_id,
                cond: f.cond,
                authority: f.authority,
                bounds_shape: f.bounds_shape,
            })
            .collect();
        match schema_hash(layout_version, &layout) {
            Ok(h) => {
                // SAFETY: `out_hash` has >= 16 writable bytes (contract).
                unsafe {
                    std::ptr::copy_nonoverlapping(h.bytes.as_ptr(), out_hash, SCHEMA_HASH_BYTES)
                };
                CitadelStatus::Ok
            }
            Err(_) => CitadelStatus::InvalidArgument,
        }
    })
}

// --- DeltaBunch encoder -------------------------------------------
//
// A builder so a native SDK encodes a client->server DeltaBunch through the ONE
// shared wire encoder (`citadel_wire::netpeer::DeltaBunch::encode`), guaranteeing
// bit-identical framing without reimplementing the BitWriter or codecs in C++.
// Each `add_*` self-describes its codec, so the encoder never needs the full
// server schema — only the field count (for the changed_mask width) and, on a
// full snapshot, the caller-computed `schema_hash`.
//
// Collection (keyed-delta) encode from a client is intentionally NOT exposed here
// (client-owned collections are rare); the server retains full collection support
// in `citadel_wire::netpeer`. See docs known gaps.

/// Opaque DeltaBunch encoder handle exposed to C as `CitadelRepEncoder *`.
pub struct CitadelRepEncoder {
    object_id: u32,
    is_full: bool,
    result_id: u64,
    base_id: u64,
    num_fields: usize,
    schema_hash: [u8; SCHEMA_HASH_BYTES],
    layout_version: u32,
    entries: Vec<(u16, RepFieldCodec, RepValue)>,
    failed: bool,
}

/// A C-friendly schema entry for the scalar NetworkPeer codecs exposed by this
/// ABI. `kind`: 0=bool, 1=int range, 2=scalar, 3=bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CitadelRepCodec {
    pub kind: u8,
    pub int_min: i64,
    pub int_max: i64,
    pub scalar_min: f32,
    pub scalar_max: f32,
    pub values_per_unit: u32,
    pub max_len: u32,
}

/// One decoded scalar value. `kind` uses the same tags as [`CitadelRepCodec`].
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CitadelRepFieldValue {
    pub field_id: u16,
    pub kind: u8,
    pub bool_value: bool,
    pub int_value: i64,
    pub scalar_value: f32,
    pub bytes_len: usize,
}

/// Opaque decoded DeltaBunch. Bytes are read with
/// [`citadel_rep_decoded_field_bytes`].
pub struct CitadelRepDecoded {
    bunch: DeltaBunch,
}

fn ffi_schema(
    hash: *const u8,
    layout_version: u32,
    specs: &[CitadelRepCodec],
) -> Option<RepSchema> {
    if hash.is_null() {
        return None;
    }
    let mut fields = Vec::with_capacity(specs.len());
    for spec in specs {
        fields.push(match spec.kind {
            0 => RepFieldCodec::Bool,
            1 => RepFieldCodec::IntRange {
                min: spec.int_min,
                max: spec.int_max,
            },
            2 => RepFieldCodec::Scalar(
                ScalarQuant::new(spec.scalar_min, spec.scalar_max, spec.values_per_unit).ok()?,
            ),
            3 => RepFieldCodec::Bytes {
                max_len: spec.max_len,
            },
            _ => return None,
        });
    }
    let mut bytes = [0; SCHEMA_HASH_BYTES];
    // SAFETY: the FFI entrypoint requires a readable 16-byte hash.
    unsafe { std::ptr::copy_nonoverlapping(hash, bytes.as_mut_ptr(), SCHEMA_HASH_BYTES) };
    RepSchema::new(
        SchemaHash {
            bytes,
            layout_version,
        },
        fields,
    )
    .ok()
}

/// Decode one authoritative DeltaBunch with the caller's ordered field codecs.
/// `schema_hash` is the canonical 16-byte identity for this ordered field table.
///
/// # Safety
/// `body`, `schema_hash`, and `codecs` must point to their declared readable
/// ranges (or be null only for a zero length); `out_decoded` must be writable.
#[allow(clippy::undocumented_unsafe_blocks)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_decode(
    body: *const u8,
    body_len: usize,
    schema_hash: *const u8,
    layout_version: u32,
    codecs: *const CitadelRepCodec,
    codec_count: usize,
    out_decoded: *mut *mut CitadelRepDecoded,
) -> CitadelStatus {
    guard(|| {
        if out_decoded.is_null()
            || schema_hash.is_null()
            || (body.is_null() && body_len != 0)
            || (codecs.is_null() && codec_count != 0)
        {
            return CitadelStatus::InvalidArgument;
        }
        let body = if body_len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(body, body_len) }
        };
        let codecs = if codec_count == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(codecs, codec_count) }
        };
        let Some(schema) = ffi_schema(schema_hash, layout_version, codecs) else {
            return CitadelStatus::InvalidArgument;
        };
        let mut budget = MAX_ENVELOPE_ALLOC;
        let Ok(bunch) = DeltaBunch::decode(body, &schema, &mut budget) else {
            return CitadelStatus::Receive;
        };
        unsafe {
            *out_decoded = Box::into_raw(Box::new(CitadelRepDecoded { bunch }));
        }
        CitadelStatus::Ok
    })
}

/// # Safety
/// `decoded` must be a live decoded handle and every output pointer writable.
#[allow(clippy::undocumented_unsafe_blocks)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_decoded_header(
    decoded: *const CitadelRepDecoded,
    out_object_id: *mut u32,
    out_is_full: *mut bool,
    out_result_id: *mut u64,
    out_base_id: *mut u64,
) -> CitadelStatus {
    guard(|| {
        let Some(decoded) = (unsafe { decoded.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if out_object_id.is_null()
            || out_is_full.is_null()
            || out_result_id.is_null()
            || out_base_id.is_null()
        {
            return CitadelStatus::InvalidArgument;
        }
        unsafe {
            *out_object_id = decoded.bunch.object_id;
            *out_is_full = decoded.bunch.is_full;
            *out_result_id = decoded.bunch.result_id;
            *out_base_id = decoded.bunch.base_id;
        }
        CitadelStatus::Ok
    })
}

/// # Safety
/// `decoded` must be a live decoded handle or null.
#[allow(clippy::undocumented_unsafe_blocks)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_decoded_field_count(
    decoded: *const CitadelRepDecoded,
) -> usize {
    // SAFETY: the caller must provide a live decoded handle or null.
    unsafe { decoded.as_ref() }.map_or(0, |d| d.bunch.changes.len())
}

/// # Safety
/// `decoded` must be live and `out` writable.
#[allow(clippy::undocumented_unsafe_blocks)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_decoded_field_at(
    decoded: *const CitadelRepDecoded,
    index: usize,
    out: *mut CitadelRepFieldValue,
) -> CitadelStatus {
    guard(|| {
        let Some(decoded) = (unsafe { decoded.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        let Some((&field_id, delta)) = decoded.bunch.changes.iter().nth(index) else {
            return CitadelStatus::InvalidArgument;
        };
        let Some(value) = (match delta {
            FieldDelta::Value(value) => Some(value),
            FieldDelta::Collection(_) => None,
        }) else {
            return CitadelStatus::InvalidArgument;
        };
        let Some(out) = (unsafe { out.as_mut() }) else {
            return CitadelStatus::InvalidArgument;
        };
        *out = match value {
            RepValue::Bool(value) => CitadelRepFieldValue {
                field_id,
                kind: 0,
                bool_value: *value,
                int_value: 0,
                scalar_value: 0.0,
                bytes_len: 0,
            },
            RepValue::Int(value) => CitadelRepFieldValue {
                field_id,
                kind: 1,
                bool_value: false,
                int_value: *value,
                scalar_value: 0.0,
                bytes_len: 0,
            },
            RepValue::Scalar(value) => CitadelRepFieldValue {
                field_id,
                kind: 2,
                bool_value: false,
                int_value: 0,
                scalar_value: *value,
                bytes_len: 0,
            },
            RepValue::Bytes(value) => CitadelRepFieldValue {
                field_id,
                kind: 3,
                bool_value: false,
                int_value: 0,
                scalar_value: 0.0,
                bytes_len: value.len(),
            },
            _ => return CitadelStatus::InvalidArgument,
        };
        CitadelStatus::Ok
    })
}

/// # Safety
/// `decoded` must be live; `buf` and `out_len` must cover their declared ranges.
#[allow(clippy::undocumented_unsafe_blocks)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_decoded_field_bytes(
    decoded: *const CitadelRepDecoded,
    index: usize,
    buf: *mut u8,
    cap: usize,
    out_len: *mut usize,
) -> CitadelStatus {
    guard(|| {
        let Some(decoded) = (unsafe { decoded.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if out_len.is_null() || (buf.is_null() && cap != 0) {
            return CitadelStatus::InvalidArgument;
        }
        let Some((_, FieldDelta::Value(RepValue::Bytes(bytes)))) =
            decoded.bunch.changes.iter().nth(index)
        else {
            return CitadelStatus::InvalidArgument;
        };
        unsafe {
            *out_len = bytes.len();
        }
        let copy = bytes.len().min(cap);
        if copy > 0 {
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, copy);
            }
        }
        if copy < bytes.len() {
            CitadelStatus::Again
        } else {
            CitadelStatus::Ok
        }
    })
}

/// # Safety
/// `decoded` must be null or an unfreed handle returned by `citadel_rep_decode`.
#[allow(clippy::undocumented_unsafe_blocks)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_decoded_free(decoded: *mut CitadelRepDecoded) {
    if !decoded.is_null() {
        unsafe {
            drop(Box::from_raw(decoded));
        }
    }
}

/// Create a DeltaBunch encoder. `is_full` selects a full snapshot (`base_id`
/// ignored, must set the schema via [`citadel_rep_encoder_set_schema`]);
/// otherwise `base_id` must be the nonzero base token. `result_id` must be
/// nonzero. `num_fields` is the class's field count (the changed_mask width).
/// Returns null on invalid arguments (zero `result_id`, or non-full with zero
/// `base_id`).
#[unsafe(no_mangle)]
pub extern "C" fn citadel_rep_encoder_new(
    object_id: u32,
    is_full: bool,
    result_id: u64,
    base_id: u64,
    num_fields: usize,
) -> *mut CitadelRepEncoder {
    let result = catch_unwind(AssertUnwindSafe(|| {
        if result_id == 0 || (!is_full && base_id == 0) {
            return std::ptr::null_mut();
        }
        Box::into_raw(Box::new(CitadelRepEncoder {
            object_id,
            is_full,
            result_id,
            base_id,
            num_fields,
            schema_hash: [0u8; SCHEMA_HASH_BYTES],
            layout_version: 0,
            entries: Vec::new(),
            failed: false,
        }))
    }));
    result.unwrap_or(std::ptr::null_mut())
}

/// Set the full-snapshot schema identity (the `hash` from [`citadel_schema_hash`]
/// and its `layout_version`). Required before finishing an `is_full` bunch.
///
/// # Safety
/// `enc` must be a live encoder; `hash` must point to 16 readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_encoder_set_schema(
    enc: *mut CitadelRepEncoder,
    hash: *const u8,
    layout_version: u32,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `enc` is a live encoder handle.
        let Some(e) = (unsafe { enc.as_mut() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if hash.is_null() {
            e.failed = true;
            return CitadelStatus::InvalidArgument;
        }
        // SAFETY: caller guarantees `hash` points to 16 readable bytes.
        let src = unsafe { std::slice::from_raw_parts(hash, SCHEMA_HASH_BYTES) };
        e.schema_hash.copy_from_slice(src);
        e.layout_version = layout_version;
        CitadelStatus::Ok
    })
}

fn encoder_push(
    enc: *mut CitadelRepEncoder,
    field_id: u16,
    codec: RepFieldCodec,
    value: RepValue,
) -> CitadelStatus {
    // SAFETY: caller guarantees `enc` is a live encoder handle.
    let Some(e) = (unsafe { enc.as_mut() }) else {
        return CitadelStatus::InvalidArgument;
    };
    if usize::from(field_id) >= e.num_fields {
        e.failed = true;
        return CitadelStatus::InvalidArgument;
    }
    e.entries.push((field_id, codec, value));
    CitadelStatus::Ok
}

/// Add a boolean field to the bunch.
///
/// # Safety
/// `enc` must be a live encoder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_encoder_add_bool(
    enc: *mut CitadelRepEncoder,
    field_id: u16,
    value: bool,
) -> CitadelStatus {
    guard(|| encoder_push(enc, field_id, RepFieldCodec::Bool, RepValue::Bool(value)))
}

/// Add a bounded integer field (encoded in `ceil_log2(max-min+1)` bits).
///
/// # Safety
/// `enc` must be a live encoder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_encoder_add_int(
    enc: *mut CitadelRepEncoder,
    field_id: u16,
    min: i64,
    max: i64,
    value: i64,
) -> CitadelStatus {
    guard(|| {
        encoder_push(
            enc,
            field_id,
            RepFieldCodec::IntRange { min, max },
            RepValue::Int(value),
        )
    })
}

/// Add a bounded fixed-point scalar field.
///
/// # Safety
/// `enc` must be a live encoder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_encoder_add_scalar(
    enc: *mut CitadelRepEncoder,
    field_id: u16,
    min: f32,
    max: f32,
    values_per_unit: u32,
    value: f32,
) -> CitadelStatus {
    guard(|| {
        let Ok(quant) = ScalarQuant::new(min, max, values_per_unit) else {
            // SAFETY: caller guarantees `enc` is live.
            if let Some(e) = unsafe { enc.as_mut() } {
                e.failed = true;
            }
            return CitadelStatus::InvalidArgument;
        };
        encoder_push(
            enc,
            field_id,
            RepFieldCodec::Scalar(quant),
            RepValue::Scalar(value),
        )
    })
}

/// Add a length-delimited byte field (capped at `max_len`).
///
/// # Safety
/// `enc` must be a live encoder handle; `data` must point to `len` readable bytes
/// (or be null iff `len == 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_encoder_add_bytes(
    enc: *mut CitadelRepEncoder,
    field_id: u16,
    max_len: u32,
    data: *const u8,
    len: usize,
) -> CitadelStatus {
    guard(|| {
        if data.is_null() && len != 0 {
            return CitadelStatus::InvalidArgument;
        }
        let bytes = if len == 0 {
            Vec::new()
        } else {
            // SAFETY: caller guarantees `data` points to `len` readable bytes.
            unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
        };
        encoder_push(
            enc,
            field_id,
            RepFieldCodec::Bytes { max_len },
            RepValue::Bytes(bytes),
        )
    })
}

/// Finish the bunch, encoding it into the caller's `buf` (capacity `cap`). Writes
/// the encoded length to `*out_len` and sets `*out_truncated` if it did not fit.
/// The encoder is NOT freed; call [`citadel_rep_encoder_free`] after. Returns
/// [`CitadelStatus::InvalidArgument`] if a prior `add_*`/`set_schema` failed or an
/// argument was invalid; [`CitadelStatus::Internal`] if encoding itself failed.
///
/// # Safety
/// `enc` must be a live encoder handle. `out_len`/`out_truncated` must be writable;
/// `buf` must point to at least `cap` writable bytes (or be null iff `cap == 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_encoder_finish(
    enc: *mut CitadelRepEncoder,
    buf: *mut u8,
    cap: usize,
    out_len: *mut usize,
    out_truncated: *mut bool,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `enc` is a live encoder handle.
        let Some(e) = (unsafe { enc.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if out_len.is_null() || out_truncated.is_null() || (buf.is_null() && cap != 0) {
            return CitadelStatus::InvalidArgument;
        }
        if e.failed {
            return CitadelStatus::InvalidArgument;
        }
        // Synthesize a schema: real codecs for the changed fields, Bool placeholders
        // for the rest (placeholders are never encoded — their mask bit is 0). The
        // caller-provided hash is what a full snapshot embeds.
        let mut fields = vec![RepFieldCodec::Bool; e.num_fields];
        let mut bunch = DeltaBunch::new(e.object_id, e.is_full, e.result_id, e.base_id);
        for (field_id, codec, value) in &e.entries {
            fields[*field_id as usize] = codec.clone();
            bunch.set(*field_id, FieldDelta::Value(value.clone()));
        }
        let schema_id = SchemaHash {
            bytes: e.schema_hash,
            layout_version: e.layout_version,
        };
        let Ok(schema) = RepSchema::new(schema_id, fields) else {
            return CitadelStatus::Internal;
        };
        let blob = match bunch.encode(&schema) {
            Ok(b) => b,
            Err(_) => return CitadelStatus::InvalidArgument,
        };
        let copy = blob.len().min(cap);
        let truncated = blob.len() > cap;
        if copy > 0 {
            // SAFETY: `buf` has >= cap >= copy writable bytes (checked).
            unsafe { std::ptr::copy_nonoverlapping(blob.as_ptr(), buf, copy) };
        }
        // SAFETY: out pointers are non-null (checked) and caller-writable.
        unsafe {
            *out_len = blob.len();
            *out_truncated = truncated;
        }
        CitadelStatus::Ok
    })
}

/// Free a DeltaBunch encoder. Passing null is a no-op.
///
/// # Safety
/// `enc` must be a handle from [`citadel_rep_encoder_new`] not already freed, or
/// null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_rep_encoder_free(enc: *mut CitadelRepEncoder) {
    let _ = guard(|| {
        if !enc.is_null() {
            // SAFETY: caller guarantees `enc` came from `_new` and is not yet freed.
            drop(unsafe { Box::from_raw(enc) });
        }
        CitadelStatus::Ok
    });
}
