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
    CitadelRepCodec, CitadelRepCodecV3, CitadelRepCollectionOp, CitadelRepDecodeFieldCodecV3,
    CitadelRepDecodedCollectionOp, CitadelRepFieldValue, CitadelSchemaField, citadel_rep_decode,
    citadel_rep_decode_with_collections, citadel_rep_decoded_collection_at,
    citadel_rep_decoded_collection_count, citadel_rep_decoded_collection_field_id,
    citadel_rep_decoded_collection_op_bytes, citadel_rep_decoded_field_at,
    citadel_rep_decoded_field_count, citadel_rep_decoded_free, citadel_rep_decoded_header,
    citadel_rep_encoder_add_bool, citadel_rep_encoder_add_bytes,
    citadel_rep_encoder_add_collection, citadel_rep_encoder_add_int, citadel_rep_encoder_finish,
    citadel_rep_encoder_free, citadel_rep_encoder_new, citadel_rep_encoder_set_schema,
    citadel_schema_hash,
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
fn ffi_encoder_collection_uses_shared_keyed_delta_and_fails_as_a_transaction() {
    let enc = citadel_rep_encoder_new(55, false, 7, 3, 1);
    let item = CitadelRepCodecV3 {
        kind: 0,
        int_min: 0,
        int_max: 0,
        scalar_min: 0.0,
        scalar_max: 0.0,
        values_per_unit: 0,
        max_len: 0,
        vector_bounds: 0.0,
        quat_bits: 0,
    };
    let ops = [
        CitadelRepCollectionOp {
            op: 0,
            value_kind: 0,
            _reserved: [0; 6],
            rep_index: 1,
            rep_generation: 2,
            rep_key: 0,
            int_value: 0,
            floats: [0.0; 4],
            bytes: std::ptr::null(),
            bytes_len: 0,
        },
        CitadelRepCollectionOp {
            op: 1,
            value_kind: 0,
            _reserved: [0; 6],
            rep_index: 4,
            rep_generation: 3,
            rep_key: 9,
            int_value: 1,
            floats: [0.0; 4],
            bytes: std::ptr::null(),
            bytes_len: 0,
        },
    ];
    assert_eq!(
        unsafe { citadel_rep_encoder_add_collection(enc, 0, item, 4, ops.as_ptr(), ops.len()) },
        CitadelStatus::Ok
    );
    let mut bytes = [0u8; 128];
    let mut len = 0;
    let mut truncated = true;
    assert_eq!(
        unsafe {
            citadel_rep_encoder_finish(
                enc,
                bytes.as_mut_ptr(),
                bytes.len(),
                &mut len,
                &mut truncated,
            )
        },
        CitadelStatus::Ok
    );
    unsafe { citadel_rep_encoder_free(enc) };
    let schema = RepSchema::new(
        SchemaHash {
            bytes: [0; 16],
            layout_version: 0,
        },
        vec![RepFieldCodec::Collection {
            item: Box::new(RepFieldCodec::Bool),
            max_items: 4,
        }],
    )
    .unwrap();
    let mut budget = MAX_ENVELOPE_ALLOC;
    let decoded = DeltaBunch::decode(&bytes[..len], &schema, &mut budget).unwrap();
    assert!(
        matches!(&decoded.changes[&0], FieldDelta::Collection(delta) if delta.removed.len() == 1 && delta.added[0].id.index == 4 && delta.added[0].key == 9)
    );

    let enc = citadel_rep_encoder_new(55, false, 8, 3, 1);
    let bad = [CitadelRepCollectionOp { op: 9, ..ops[0] }];
    assert_eq!(
        unsafe { citadel_rep_encoder_add_collection(enc, 0, item, 4, bad.as_ptr(), 1) },
        CitadelStatus::InvalidArgument
    );
    assert_eq!(
        unsafe {
            citadel_rep_encoder_finish(
                enc,
                bytes.as_mut_ptr(),
                bytes.len(),
                &mut len,
                &mut truncated,
            )
        },
        CitadelStatus::InvalidArgument
    );
    unsafe { citadel_rep_encoder_free(enc) };

    // The ABI rejects malformed keyed transactions before they become an
    // encoder entry: ids are unique across op kinds and add/change keys are
    // nonzero generation-edit counters.
    let enc = citadel_rep_encoder_new(55, false, 8, 3, 1);
    let duplicate = [
        ops[0],
        CitadelRepCollectionOp {
            op: 1,
            value_kind: 0,
            rep_key: 1,
            int_value: 1,
            ..ops[0]
        },
    ];
    assert_eq!(
        unsafe {
            citadel_rep_encoder_add_collection(enc, 0, item, 4, duplicate.as_ptr(), duplicate.len())
        },
        CitadelStatus::InvalidArgument
    );
    unsafe { citadel_rep_encoder_free(enc) };

    let enc = citadel_rep_encoder_new(55, false, 8, 3, 1);
    let zero_key = [CitadelRepCollectionOp {
        op: 1,
        value_kind: 0,
        rep_key: 0,
        int_value: 1,
        ..ops[0]
    }];
    assert_eq!(
        unsafe {
            citadel_rep_encoder_add_collection(enc, 0, item, 4, zero_key.as_ptr(), zero_key.len())
        },
        CitadelStatus::InvalidArgument
    );
    unsafe { citadel_rep_encoder_free(enc) };
}

#[test]
fn ffi_decoder_iterates_keyed_collection_operations_without_borrowing_bytes() {
    let item = CitadelRepCodecV3 {
        kind: 3,
        int_min: 0,
        int_max: 0,
        scalar_min: 0.0,
        scalar_max: 0.0,
        values_per_unit: 0,
        max_len: 16,
        vector_bounds: 0.0,
        quat_bits: 0,
    };
    let add = b"add";
    let change = b"change";
    let operations = [
        CitadelRepCollectionOp {
            op: 0,
            value_kind: 0,
            _reserved: [0; 6],
            rep_index: 1,
            rep_generation: 2,
            rep_key: 0,
            int_value: 0,
            floats: [0.0; 4],
            bytes: std::ptr::null(),
            bytes_len: 0,
        },
        CitadelRepCollectionOp {
            op: 1,
            value_kind: 3,
            _reserved: [0; 6],
            rep_index: 2,
            rep_generation: 3,
            rep_key: 4,
            int_value: 0,
            floats: [0.0; 4],
            bytes: add.as_ptr(),
            bytes_len: add.len(),
        },
        CitadelRepCollectionOp {
            op: 2,
            value_kind: 3,
            _reserved: [0; 6],
            rep_index: 3,
            rep_generation: 4,
            rep_key: 5,
            int_value: 0,
            floats: [0.0; 4],
            bytes: change.as_ptr(),
            bytes_len: change.len(),
        },
    ];
    let enc = citadel_rep_encoder_new(55, false, 7, 3, 1);
    assert!(!enc.is_null());
    assert_eq!(
        unsafe {
            citadel_rep_encoder_add_collection(
                enc,
                0,
                item,
                8,
                operations.as_ptr(),
                operations.len(),
            )
        },
        CitadelStatus::Ok
    );
    let mut body = [0u8; 256];
    let mut len = 0;
    let mut truncated = true;
    assert_eq!(
        unsafe {
            citadel_rep_encoder_finish(enc, body.as_mut_ptr(), body.len(), &mut len, &mut truncated)
        },
        CitadelStatus::Ok
    );
    unsafe { citadel_rep_encoder_free(enc) };

    let codecs = [CitadelRepDecodeFieldCodecV3 {
        codec: item,
        collection_item_codec: item,
        collection_max_items: 8,
        is_collection: true,
        _reserved: [0; 3],
    }];
    let mut decoded = std::ptr::null_mut();
    assert_eq!(
        unsafe {
            citadel_rep_decode_with_collections(
                body.as_ptr(),
                len,
                [0; 16].as_ptr(),
                0,
                codecs.as_ptr(),
                codecs.len(),
                &mut decoded,
            )
        },
        CitadelStatus::Ok
    );
    let mut count = 0;
    assert_eq!(
        unsafe { citadel_rep_decoded_collection_count(decoded, 0, &mut count) },
        CitadelStatus::Ok
    );
    assert_eq!(count, 3);
    let mut op = CitadelRepDecodedCollectionOp {
        op: 99,
        value_kind: 99,
        _reserved: [0; 6],
        rep_index: 0,
        rep_generation: 0,
        rep_key: 0,
        int_value: 0,
        floats: [0.0; 4],
        bytes_len: 0,
    };
    assert_eq!(
        unsafe { citadel_rep_decoded_collection_at(decoded, 0, 0, &mut op) },
        CitadelStatus::Ok
    );
    assert_eq!(
        (
            op.op,
            op.rep_index,
            op.rep_generation,
            op.rep_key,
            op.bytes_len
        ),
        (0, 1, 2, 0, 0)
    );
    assert_eq!(
        unsafe { citadel_rep_decoded_collection_at(decoded, 0, 1, &mut op) },
        CitadelStatus::Ok
    );
    assert_eq!(
        (
            op.op,
            op.value_kind,
            op.rep_index,
            op.rep_generation,
            op.rep_key,
            op.bytes_len
        ),
        (1, 3, 2, 3, 4, add.len())
    );
    let mut out_len = 0;
    let mut too_small = [0; 2];
    assert_eq!(
        unsafe {
            citadel_rep_decoded_collection_op_bytes(
                decoded,
                0,
                1,
                too_small.as_mut_ptr(),
                too_small.len(),
                &mut out_len,
            )
        },
        CitadelStatus::Again
    );
    assert_eq!(out_len, add.len());
    let mut copied = [0; 6];
    assert_eq!(
        unsafe {
            citadel_rep_decoded_collection_op_bytes(
                decoded,
                0,
                2,
                copied.as_mut_ptr(),
                copied.len(),
                &mut out_len,
            )
        },
        CitadelStatus::Ok
    );
    assert_eq!(&copied[..out_len], change);
    assert_eq!(
        unsafe { citadel_rep_decoded_collection_at(decoded, 0, 3, &mut op) },
        CitadelStatus::InvalidArgument
    );
    assert_eq!(
        unsafe { citadel_rep_decoded_collection_count(decoded, 1, &mut count) },
        CitadelStatus::InvalidArgument
    );
    unsafe { citadel_rep_decoded_free(decoded) };
}

#[test]
fn ffi_decoder_maps_sparse_collection_changed_field_to_source_field_id() {
    let int_codec = CitadelRepCodecV3 {
        kind: 1,
        int_min: 0,
        int_max: 100,
        scalar_min: 0.0,
        scalar_max: 0.0,
        values_per_unit: 0,
        max_len: 0,
        vector_bounds: 0.0,
        quat_bits: 0,
    };
    let bytes_codec = CitadelRepCodecV3 {
        kind: 3,
        int_min: 0,
        int_max: 0,
        scalar_min: 0.0,
        scalar_max: 0.0,
        values_per_unit: 0,
        max_len: 8,
        vector_bounds: 0.0,
        quat_bits: 0,
    };
    let operation = CitadelRepCollectionOp {
        op: 0,
        value_kind: 0,
        _reserved: [0; 6],
        rep_index: 8,
        rep_generation: 3,
        rep_key: 0,
        int_value: 0,
        floats: [0.0; 4],
        bytes: std::ptr::null(),
        bytes_len: 0,
    };
    let enc = citadel_rep_encoder_new(55, false, 7, 3, 4);
    assert!(!enc.is_null());
    assert_eq!(
        unsafe { citadel_rep_encoder_add_int(enc, 0, 0, 100, 42) },
        CitadelStatus::Ok
    );
    assert_eq!(
        unsafe { citadel_rep_encoder_add_collection(enc, 3, bytes_codec, 8, &operation, 1) },
        CitadelStatus::Ok
    );
    let mut body = [0u8; 128];
    let mut len = 0;
    let mut truncated = true;
    assert_eq!(
        unsafe {
            citadel_rep_encoder_finish(enc, body.as_mut_ptr(), body.len(), &mut len, &mut truncated)
        },
        CitadelStatus::Ok
    );
    unsafe { citadel_rep_encoder_free(enc) };

    let scalar = CitadelRepDecodeFieldCodecV3 {
        codec: int_codec,
        collection_item_codec: int_codec,
        collection_max_items: 0,
        is_collection: false,
        _reserved: [0; 3],
    };
    let collection = CitadelRepDecodeFieldCodecV3 {
        codec: bytes_codec,
        collection_item_codec: bytes_codec,
        collection_max_items: 8,
        is_collection: true,
        _reserved: [0; 3],
    };
    let codecs = [scalar, scalar, scalar, collection];
    let mut decoded = std::ptr::null_mut();
    assert_eq!(
        unsafe {
            citadel_rep_decode_with_collections(
                body.as_ptr(),
                len,
                [0; 16].as_ptr(),
                0,
                codecs.as_ptr(),
                codecs.len(),
                &mut decoded,
            )
        },
        CitadelStatus::Ok
    );
    assert_eq!(unsafe { citadel_rep_decoded_field_count(decoded) }, 2);
    let mut field_id = 0;
    assert_eq!(
        unsafe { citadel_rep_decoded_collection_field_id(decoded, 1, &mut field_id) },
        CitadelStatus::Ok
    );
    assert_eq!(
        field_id, 3,
        "must return the schema field id, not ordinal 1"
    );
    assert_eq!(
        unsafe { citadel_rep_decoded_collection_field_id(decoded, 0, &mut field_id) },
        CitadelStatus::InvalidArgument,
        "scalar changed fields cannot be treated as collections"
    );
    assert_eq!(
        unsafe { citadel_rep_decoded_collection_field_id(decoded, 2, &mut field_id) },
        CitadelStatus::InvalidArgument
    );
    assert_eq!(
        unsafe { citadel_rep_decoded_collection_field_id(decoded, 1, std::ptr::null_mut()) },
        CitadelStatus::InvalidArgument
    );
    unsafe { citadel_rep_decoded_free(decoded) };
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

#[test]
fn legacy_v2_decode_uses_exact_40_byte_descriptors_without_v3_fields() {
    use std::mem::{align_of, size_of};

    // This is the binary contract for arrays passed to legacy citadel_rep_decode.
    // Keeping the typed v3 fields out of this type makes an out-of-bounds v3 read
    // impossible: the legacy schema builder receives only CitadelRepCodec values.
    assert_eq!(size_of::<CitadelRepCodec>(), 40);
    assert_eq!(align_of::<CitadelRepCodec>(), 8);
    assert_eq!(size_of::<CitadelRepCodecV3>(), 48);

    let encoder = citadel_rep_encoder_new(7, false, 2, 1, 1);
    assert!(!encoder.is_null());
    assert_eq!(
        unsafe { citadel_rep_encoder_add_bool(encoder, 0, true) },
        CitadelStatus::Ok
    );
    let mut body = [0_u8; 64];
    let mut len = 0;
    let mut truncated = true;
    assert_eq!(
        unsafe {
            citadel_rep_encoder_finish(
                encoder,
                body.as_mut_ptr(),
                body.len(),
                &mut len,
                &mut truncated,
            )
        },
        CitadelStatus::Ok
    );
    unsafe { citadel_rep_encoder_free(encoder) };

    let v2_descriptors = [CitadelRepCodec {
        kind: 0,
        int_min: 0,
        int_max: 0,
        scalar_min: 0.0,
        scalar_max: 0.0,
        values_per_unit: 0,
        max_len: 0,
    }];
    let mut decoded = std::ptr::null_mut();
    assert_eq!(
        unsafe {
            citadel_rep_decode(
                body.as_ptr(),
                len,
                [0; 16].as_ptr(),
                0,
                v2_descriptors.as_ptr(),
                v2_descriptors.len(),
                &mut decoded,
            )
        },
        CitadelStatus::Ok
    );
    unsafe { citadel_rep_decoded_free(decoded) };
}
