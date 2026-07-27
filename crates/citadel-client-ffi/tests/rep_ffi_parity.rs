//! C-ABI ↔ native parity for the NetworkPeer schema_hash + DeltaBunch encoder
//!.
//!
//! Proves the FFI `citadel_schema_hash` matches `citadel_wire::schema::schema_hash`
//! (so an SDK computes the SAME 128-bit class identity the server does — the
//! Phase-1 gap where the Unreal SchemaHash was zeroed) and that a bunch encoded
//! through the FFI builder decodes bit-for-bit with the native wire decoder.
#![allow(clippy::unwrap_used, clippy::undocumented_unsafe_blocks)]

use citadel_client_ffi::CitadelStatus;
use citadel_client_ffi::codec_ffi::{
    CitadelRepCodec, CitadelRepFieldValue, CitadelSchemaField, citadel_rep_decode,
    citadel_rep_decoded_field_at, citadel_rep_decoded_field_count, citadel_rep_decoded_free,
    citadel_rep_decoded_header, citadel_rep_encoder_add_bool, citadel_rep_encoder_add_bytes,
    citadel_rep_encoder_add_int, citadel_rep_encoder_finish, citadel_rep_encoder_free,
    citadel_rep_encoder_new, citadel_rep_encoder_set_schema, citadel_schema_hash,
};
use citadel_wire::netpeer::{
    DeltaBunch, FieldDelta, MAX_ENVELOPE_ALLOC, RepFieldCodec, RepSchema, RepValue,
};
use citadel_wire::schema::{LayoutField, SchemaHash, schema_hash};

fn native_layout() -> Vec<LayoutField> {
    vec![
        LayoutField {
            field_id: 0,
            type_tag: 1,
            codec_id: 1,
            cond: 0,
            authority: 1,
            bounds_shape: 0,
        },
        LayoutField {
            field_id: 1,
            type_tag: 2,
            codec_id: 2,
            cond: 2,
            authority: 0,
            bounds_shape: 0x0102_0304_0506_0708,
        },
        LayoutField {
            field_id: 2,
            type_tag: 7,
            codec_id: 2,
            cond: 0,
            authority: 1,
            bounds_shape: 0xDEAD_BEEF,
        },
    ]
}

fn ffi_fields(layout: &[LayoutField]) -> Vec<CitadelSchemaField> {
    layout
        .iter()
        .map(|f| CitadelSchemaField {
            field_id: f.field_id,
            type_tag: f.type_tag,
            codec_id: f.codec_id,
            cond: f.cond,
            authority: f.authority,
            bounds_shape: f.bounds_shape,
        })
        .collect()
}

#[test]
fn schema_hash_ffi_matches_native() {
    let layout = native_layout();
    let expected = schema_hash(3, &layout).unwrap();
    let fields = ffi_fields(&layout);
    let mut out = [0u8; 16];
    let st = unsafe { citadel_schema_hash(3, fields.as_ptr(), fields.len(), out.as_mut_ptr()) };
    assert_eq!(st, CitadelStatus::Ok);
    assert_eq!(out, expected.bytes, "FFI schema_hash must match native");
}

#[test]
fn schema_hash_ffi_rejects_non_canonical_order() {
    // Non-ascending field_ids: schema_hash rejects, FFI maps to InvalidArgument.
    let mut layout = native_layout();
    layout.swap(0, 1);
    let fields = ffi_fields(&layout);
    let mut out = [0u8; 16];
    let st = unsafe { citadel_schema_hash(1, fields.as_ptr(), fields.len(), out.as_mut_ptr()) };
    assert_eq!(st, CitadelStatus::InvalidArgument);
}

#[test]
fn schema_hash_ffi_empty_layout() {
    let expected = schema_hash(1, &[]).unwrap();
    let mut out = [0u8; 16];
    let st = unsafe { citadel_schema_hash(1, std::ptr::null(), 0, out.as_mut_ptr()) };
    assert_eq!(st, CitadelStatus::Ok);
    assert_eq!(out, expected.bytes);
}

#[test]
fn schema_hash_ffi_null_out_is_rejected() {
    let fields = ffi_fields(&native_layout());
    let st = unsafe { citadel_schema_hash(1, fields.as_ptr(), fields.len(), std::ptr::null_mut()) };
    assert_eq!(st, CitadelStatus::InvalidArgument);
}

#[test]
fn ffi_encoder_delta_decodes_with_native() {
    // Encode a non-full bunch through the FFI builder, decode with the native wire
    // decoder against the matching schema.
    let enc = citadel_rep_encoder_new(1234, false, 7, 3, 4);
    assert!(!enc.is_null());
    assert_eq!(
        unsafe { citadel_rep_encoder_add_bool(enc, 0, true) },
        CitadelStatus::Ok
    );
    assert_eq!(
        unsafe { citadel_rep_encoder_add_int(enc, 1, 0, 100, 73) },
        CitadelStatus::Ok
    );
    assert_eq!(
        unsafe { citadel_rep_encoder_add_bytes(enc, 3, 16, b"hi".as_ptr(), 2) },
        CitadelStatus::Ok
    );

    let mut buf = [0u8; 256];
    let mut len = 0usize;
    let mut truncated = true;
    let st = unsafe {
        citadel_rep_encoder_finish(enc, buf.as_mut_ptr(), buf.len(), &mut len, &mut truncated)
    };
    assert_eq!(st, CitadelStatus::Ok);
    assert!(!truncated);
    unsafe { citadel_rep_encoder_free(enc) };

    // Native decode against the same 4-field schema (field 2 is unused Bool).
    let schema = RepSchema::new(
        SchemaHash {
            bytes: [0u8; 16],
            layout_version: 0,
        },
        vec![
            RepFieldCodec::Bool,
            RepFieldCodec::IntRange { min: 0, max: 100 },
            RepFieldCodec::Bool,
            RepFieldCodec::Bytes { max_len: 16 },
        ],
    )
    .unwrap();
    let mut budget = MAX_ENVELOPE_ALLOC;
    let back = DeltaBunch::decode(&buf[..len], &schema, &mut budget).unwrap();
    assert_eq!(back.object_id, 1234);
    assert!(!back.is_full);
    assert_eq!(back.result_id, 7);
    assert_eq!(back.base_id, 3);
    assert_eq!(back.changes[&0], FieldDelta::Value(RepValue::Bool(true)));
    assert_eq!(back.changes[&1], FieldDelta::Value(RepValue::Int(73)));
    assert_eq!(
        back.changes[&3],
        FieldDelta::Value(RepValue::Bytes(b"hi".to_vec()))
    );
}

#[test]
fn ffi_encoder_full_snapshot_embeds_schema_hash() {
    // A full snapshot must embed the caller-computed schema_hash and decode under
    // the matching schema, closing the Phase-1 zeroed-hash gap.
    let layout = native_layout();
    let native_hash = schema_hash(1, &layout).unwrap();
    let fields = ffi_fields(&layout);
    let mut hash = [0u8; 16];
    unsafe { citadel_schema_hash(1, fields.as_ptr(), fields.len(), hash.as_mut_ptr()) };

    let enc = citadel_rep_encoder_new(5, true, 1, 0, 3);
    assert!(!enc.is_null());
    assert_eq!(
        unsafe { citadel_rep_encoder_set_schema(enc, hash.as_ptr(), 1) },
        CitadelStatus::Ok
    );
    assert_eq!(
        unsafe { citadel_rep_encoder_add_int(enc, 1, 0, 100, 42) },
        CitadelStatus::Ok
    );
    let mut buf = [0u8; 256];
    let mut len = 0usize;
    let mut truncated = true;
    let st = unsafe {
        citadel_rep_encoder_finish(enc, buf.as_mut_ptr(), buf.len(), &mut len, &mut truncated)
    };
    assert_eq!(st, CitadelStatus::Ok);
    unsafe { citadel_rep_encoder_free(enc) };

    // Decode with a schema carrying the SAME native hash: must succeed.
    let good = RepSchema::new(
        native_hash,
        vec![
            RepFieldCodec::Bool,
            RepFieldCodec::IntRange { min: 0, max: 100 },
            RepFieldCodec::Bool,
        ],
    )
    .unwrap();
    let mut budget = MAX_ENVELOPE_ALLOC;
    let back = DeltaBunch::decode(&buf[..len], &good, &mut budget).unwrap();
    assert!(back.is_full);
    assert_eq!(back.changes[&1], FieldDelta::Value(RepValue::Int(42)));

    // Decode with a divergent-layout schema (different hash): must fail closed.
    let bad = RepSchema::new(
        schema_hash(2, &layout).unwrap(),
        vec![
            RepFieldCodec::Bool,
            RepFieldCodec::IntRange { min: 0, max: 100 },
            RepFieldCodec::Bool,
        ],
    )
    .unwrap();
    let mut budget = MAX_ENVELOPE_ALLOC;
    assert!(DeltaBunch::decode(&buf[..len], &bad, &mut budget).is_err());
}

#[test]
fn ffi_encoder_rejects_zero_result_id() {
    assert!(citadel_rep_encoder_new(1, false, 0, 3, 2).is_null());
    assert!(citadel_rep_encoder_new(1, false, 5, 0, 2).is_null());
}

#[test]
fn ffi_decoder_reads_the_shared_encoder_output() {
    let enc = citadel_rep_encoder_new(9, false, 4, 3, 2);
    assert!(!enc.is_null());
    assert_eq!(
        unsafe { citadel_rep_encoder_add_int(enc, 1, 0, 100, 77) },
        CitadelStatus::Ok
    );
    let mut body = [0u8; 64];
    let mut len = 0;
    let mut truncated = false;
    assert_eq!(
        unsafe {
            citadel_rep_encoder_finish(enc, body.as_mut_ptr(), body.len(), &mut len, &mut truncated)
        },
        CitadelStatus::Ok
    );
    unsafe { citadel_rep_encoder_free(enc) };
    let codecs = [
        CitadelRepCodec {
            kind: 0,
            int_min: 0,
            int_max: 0,
            scalar_min: 0.0,
            scalar_max: 0.0,
            values_per_unit: 0,
            max_len: 0,
        },
        CitadelRepCodec {
            kind: 1,
            int_min: 0,
            int_max: 100,
            scalar_min: 0.0,
            scalar_max: 0.0,
            values_per_unit: 0,
            max_len: 0,
        },
    ];
    let mut decoded = std::ptr::null_mut();
    assert_eq!(
        unsafe {
            citadel_rep_decode(
                body.as_ptr(),
                len,
                [0u8; 16].as_ptr(),
                0,
                codecs.as_ptr(),
                codecs.len(),
                &mut decoded,
            )
        },
        CitadelStatus::Ok
    );
    let mut object = 0;
    let mut full = true;
    let mut result = 0;
    let mut base = 0;
    assert_eq!(
        unsafe {
            citadel_rep_decoded_header(decoded, &mut object, &mut full, &mut result, &mut base)
        },
        CitadelStatus::Ok
    );
    assert_eq!((object, full, result, base), (9, false, 4, 3));
    assert_eq!(unsafe { citadel_rep_decoded_field_count(decoded) }, 1);
    let mut value = CitadelRepFieldValue {
        field_id: 0,
        kind: 0,
        bool_value: false,
        int_value: 0,
        scalar_value: 0.0,
        bytes_len: 0,
    };
    assert_eq!(
        unsafe { citadel_rep_decoded_field_at(decoded, 0, &mut value) },
        CitadelStatus::Ok
    );
    assert_eq!((value.field_id, value.kind, value.int_value), (1, 1, 77));
    unsafe { citadel_rep_decoded_free(decoded) };
}
