//! Published bit-level test vectors — the cross-SDK layout lock.
//!
//! `crates/citadel-wire/tests/wire_vectors.json` is the machine-readable ground
//! truth every SDK (Rust, Unreal/C++, Unity/C#, Godot/GDScript) must reproduce
//! byte-for-byte. It pins, per codec + params: the exact output bytes (hex), the
//! total `bit_len` (so payload bits are distinguished from final zero padding),
//! the intermediate codes, and the decoded value with its error bound. Ack and
//! `schema_hash` vectors are pinned the same way.
//!
//! This file is BOTH the generator and the stale-guard, mirroring
//! `contract_manifest.rs`:
//!
//! - Normal `cargo test`: [`wire_vectors_json_is_in_sync`] renders the fixture
//!   from the canonical Rust codecs and asserts the checked-in file matches — a
//!   drifted layout fails CI.
//! - Regenerate: `CITADEL_REGEN_VECTORS=1 cargo test -p citadel-wire --test
//!   wire_vectors`.
//!
//! In addition, hand-computed inline anchors ([`anchor_*`]) independently prove
//! the bit order / byte layout so the fixture cannot silently encode the wrong
//! thing, and the invalid/non-canonical cases pin the required reject behavior
//! (overrun-no-advance, nonzero padding, out-of-range code).
#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use citadel_wire::baseline::AckField;
use citadel_wire::bits::{BitError, BitReader, BitWriter};
use citadel_wire::codec::{
    CodecError, DEFAULT_WORLD_BOUNDS, IDENTITY_QUAT, QuatMode, ScalarQuant, VectorQuant, codec_id,
};
use citadel_wire::schema::{LayoutField, SCHEMA_HASH_ALGORITHM, schema_hash};
use serde_json::{Value, json};

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0xF) as u32, 16).unwrap());
    }
    s
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("wire_vectors.json")
}

// --- vector builders --------------------------------------------------------

fn scalar_vector(min: f32, max: f32, vpu: u32, value: f32) -> Value {
    let s = ScalarQuant::new(min, max, vpu).unwrap();
    let code = s.encode_value(value).unwrap();
    let mut w = BitWriter::new();
    s.write(&mut w, value).unwrap();
    let (bytes, bit_len) = w.finish();
    let mut r = BitReader::new(&bytes, bit_len);
    let decoded = s.read(&mut r).unwrap();
    r.finish().unwrap();
    json!({
        "min": min,
        "max": max,
        "values_per_unit": vpu,
        "bits": s.bits(),
        "code_count": s.code_count(),
        "value": value,
        "code": code,
        "bit_len": bit_len,
        "bytes_hex": hex(&bytes),
        "decoded": decoded,
        "error_max": 0.5 / f64::from(vpu),
    })
}

fn vector_vector(pos: [f32; 3]) -> Value {
    let v = VectorQuant::new(DEFAULT_WORLD_BOUNDS).unwrap();
    let mut w = BitWriter::new();
    v.write(&mut w, pos).unwrap();
    let (bytes, bit_len) = w.finish();
    let mut r = BitReader::new(&bytes, bit_len);
    let decoded = v.read(&mut r).unwrap();
    r.finish().unwrap();
    json!({
        "bounds_min": DEFAULT_WORLD_BOUNDS.min,
        "bounds_max": DEFAULT_WORLD_BOUNDS.max,
        "values_per_unit": DEFAULT_WORLD_BOUNDS.values_per_unit,
        "bits": v.bits(),
        "position": pos,
        "bit_len": bit_len,
        "bytes_hex": hex(&bytes),
        "decoded": decoded,
    })
}

fn mode_name(mode: QuatMode) -> &'static str {
    match mode {
        QuatMode::Bits9 => "Bits9",
        QuatMode::Bits10 => "Bits10",
        QuatMode::Bits15 => "Bits15",
    }
}

fn quat_vector(input: [f32; 4], mode: QuatMode) -> Value {
    let n = mode.bits_per_component();
    let mut w = BitWriter::new();
    citadel_wire::codec::encode_quat(&mut w, input, mode).unwrap();
    let (bytes, bit_len) = w.finish();

    // Re-read the raw layout (index + three component codes) to pin ordering.
    let mut raw = BitReader::new(&bytes, bit_len);
    let index = raw.read_bits(2).unwrap();
    let codes: Vec<u64> = (0..3).map(|_| raw.read_bits(n).unwrap()).collect();

    let mut r = BitReader::new(&bytes, bit_len);
    let decoded = citadel_wire::codec::decode_quat(&mut r, mode).unwrap();
    r.finish().unwrap();
    json!({
        "mode": mode_name(mode),
        "codec_id": mode.codec_id(),
        "total_bits": mode.total_bits(),
        "input": input,
        "dropped_index": index,
        "component_codes": codes,
        "bit_len": bit_len,
        "bytes_hex": hex(&bytes),
        "decoded": decoded,
    })
}

fn ack_vector(acks: &[u64]) -> Value {
    let mut a = AckField::new();
    for &id in acks {
        a.ack(id);
    }
    let (latest, history) = a.to_wire();
    let round = AckField::from_wire(latest, history).unwrap();
    assert_eq!(round, a);
    json!({
        "acks": acks,
        "latest": latest,
        "history": history,
        "history_hex": format!("{history:08x}"),
    })
}

fn layout_field(field_id: u16, codec_id: u16) -> LayoutField {
    LayoutField {
        field_id,
        type_tag: field_id.wrapping_mul(7) ^ 0x1234,
        codec_id,
        cond: (field_id % 4) as u8,
        authority: (field_id % 2) as u8,
        bounds_shape: u64::from(field_id) << 8 | 0x55,
    }
}

fn schema_vector(layout_version: u32, fields: &[LayoutField]) -> Value {
    let h = schema_hash(layout_version, fields).unwrap();
    let field_json: Vec<Value> = fields
        .iter()
        .map(|f| {
            json!({
                "field_id": f.field_id,
                "type_tag": f.type_tag,
                "codec_id": f.codec_id,
                "cond": f.cond,
                "authority": f.authority,
                "bounds_shape": f.bounds_shape,
            })
        })
        .collect();
    json!({
        "layout_version": layout_version,
        "fields": field_json,
        "digest_hex": h.to_hex(),
    })
}

fn render_fixture() -> String {
    let manifest = json!({
        "_comment": " cross-SDK bit-level ground truth. Regenerate with \
            CITADEL_REGEN_VECTORS=1 cargo test -p citadel-wire --test wire_vectors",
        "bit_order": "msb-first-within-byte",
        "padding": "zero-fill-to-byte-boundary; non-canonical (nonzero pad or trailing byte) rejected on decode",
        "endianness_note": "multi-byte values are packed as big-endian bit runs by the MSB-first bit writer, not byte-wise LE/BE",
        "quat_component_order": {
            "index_meaning": "0=x,1=y,2=z,3=w; the dropped (largest) component index",
            "kept_order": "the three non-dropped components in ascending source index",
            "sqrt_half": citadel_wire::codec::SQRT_HALF,
        },
        "schema_hash": {
            "algorithm": SCHEMA_HASH_ALGORITHM,
            "vectors": [
                schema_vector(1, &[]),
                schema_vector(1, &[
                    layout_field(0, codec_id::BOOL),
                    layout_field(1, codec_id::SCALAR_QUANT),
                    layout_field(2, codec_id::VECTOR3_QUANT),
                    layout_field(3, codec_id::QUAT_SMALLEST3_10),
                ]),
                schema_vector(2, &[
                    layout_field(0, codec_id::BOOL),
                    layout_field(1, codec_id::SCALAR_QUANT),
                    layout_field(2, codec_id::VECTOR3_QUANT),
                    layout_field(3, codec_id::QUAT_SMALLEST3_10),
                ]),
            ],
        },
        "scalar": [
            // Small hand-verifiable range: min=0 max=4 vpu=1 => steps=4, bits=3.
            scalar_vector(0.0, 4.0, 1, 0.0),
            scalar_vector(0.0, 4.0, 1, 4.0),
            scalar_vector(0.0, 4.0, 1, 2.5),   // half-step rounds up
            scalar_vector(0.0, 4.0, 1, 100.0), // above max saturates
            scalar_vector(0.0, 4.0, 1, -100.0),// below min saturates
            // Symmetric signed range.
            scalar_vector(-100.0, 100.0, 10, 0.0),
            scalar_vector(-100.0, 100.0, 10, 37.3),
            scalar_vector(-100.0, 100.0, 10, -100.0),
            scalar_vector(-100.0, 100.0, 10, 100.0),
            // Default world axes.
            scalar_vector(DEFAULT_WORLD_BOUNDS.min[0], DEFAULT_WORLD_BOUNDS.max[0], 8, 0.0),
            scalar_vector(DEFAULT_WORLD_BOUNDS.min[2], DEFAULT_WORLD_BOUNDS.max[2], 8, 12345.0),
        ],
        "vector": [
            vector_vector([0.0, 0.0, 0.0]),
            vector_vector(DEFAULT_WORLD_BOUNDS.min),
            vector_vector(DEFAULT_WORLD_BOUNDS.max),
            vector_vector([1234.5, -9876.25, 42.0]),
        ],
        "quat": [
            quat_vector(IDENTITY_QUAT, QuatMode::Bits9),
            quat_vector(IDENTITY_QUAT, QuatMode::Bits10),
            quat_vector(IDENTITY_QUAT, QuatMode::Bits15),
            // Each axis largest.
            quat_vector([0.9239, 0.3827, 0.0, 0.0], QuatMode::Bits10),
            quat_vector([0.0, 0.9239, 0.3827, 0.0], QuatMode::Bits10),
            quat_vector([0.0, 0.0, 0.9239, 0.3827], QuatMode::Bits10),
            quat_vector([0.3827, 0.0, 0.0, 0.9239], QuatMode::Bits10),
            // Tie-break: all equal => drop index 0.
            quat_vector([0.5, 0.5, 0.5, 0.5], QuatMode::Bits15),
            // Negative largest => sign-canonicalized.
            quat_vector([0.1, 0.1, 0.1, -0.9797], QuatMode::Bits15),
            // Degenerate inputs fall back to identity.
            quat_vector([f32::NAN, 0.0, 0.0, 1.0], QuatMode::Bits10),
            quat_vector([0.0, 0.0, 0.0, 0.0], QuatMode::Bits10),
        ],
        "ack": [
            ack_vector(&[10]),
            ack_vector(&[10, 8]),
            ack_vector(&[5, 6]),
            ack_vector(&[1, 32]),  // delta 31
            ack_vector(&[1, 33]),  // delta 32 (old latest at top bit)
            ack_vector(&[1, 34]),  // delta 33 (old latest falls out)
        ],
    });
    let mut s = serde_json::to_string_pretty(&manifest).unwrap();
    s.push('\n');
    s
}

#[test]
fn wire_vectors_json_is_in_sync() {
    let expected = render_fixture();
    let path = fixture_path();
    if std::env::var_os("CITADEL_REGEN_VECTORS").is_some() {
        std::fs::write(&path, &expected).unwrap();
        eprintln!("regenerated {}", path.display());
        return;
    }
    let actual = std::fs::read_to_string(&path).expect(
        "read tests/wire_vectors.json; regenerate with \
         CITADEL_REGEN_VECTORS=1 cargo test -p citadel-wire --test wire_vectors",
    );
    assert_eq!(
        actual, expected,
        "tests/wire_vectors.json is stale vs the canonical codecs; regenerate \
         with CITADEL_REGEN_VECTORS=1 cargo test -p citadel-wire --test wire_vectors"
    );
}

// --- hand-computed inline anchors (independent of the generator) ------------

#[test]
fn anchor_scalar_3bit_layout() {
    // min=0 max=4 vpu=1 => steps=4, code_count=5, bits=3. A 3-bit code C is
    // written MSB-first into the top of the first byte => byte == C << 5.
    let s = ScalarQuant::new(0.0, 4.0, 1).unwrap();
    assert_eq!(s.bits(), 3);
    for (value, code, byte) in [
        (0.0f32, 0u64, 0x00u8),
        (4.0, 4, 0x80), // 100 << 5
        (2.5, 3, 0x60), // half-step rounds up: 011 << 5
        (100.0, 4, 0x80),
        (-100.0, 0, 0x00),
    ] {
        assert_eq!(s.encode_value(value).unwrap(), code, "code for {value}");
        let mut w = BitWriter::new();
        s.write(&mut w, value).unwrap();
        let (bytes, bit_len) = w.finish();
        assert_eq!(bit_len, 3);
        assert_eq!(bytes, vec![byte], "byte for {value}");
    }
}

#[test]
fn anchor_quat_identity_bits10_layout() {
    // Identity: w largest (index 3 => 0b11); each kept component is 0.0, whose
    // 10-bit code is 512 (0b1000000000). Concatenated MSB-first this is exactly
    // E0 08 02 00 over 32 bits.
    let mut w = BitWriter::new();
    citadel_wire::codec::encode_quat(&mut w, IDENTITY_QUAT, QuatMode::Bits10).unwrap();
    let (bytes, bit_len) = w.finish();
    assert_eq!(bit_len, 32);
    assert_eq!(bytes, vec![0xE0, 0x08, 0x02, 0x00]);
}

#[test]
fn anchor_ack_history_bits() {
    // ack 10 then 8: offset 2 => history bit 1 set => history == 0b10.
    let mut a = AckField::new();
    a.ack(10);
    a.ack(8);
    assert_eq!(a.to_wire(), (10, 0b10));
}

// --- invalid / non-canonical cases (review finding 20) -----------------------

#[test]
fn invalid_overrun_does_not_advance_cursor() {
    let mut w = BitWriter::new();
    w.write_bits(0b101, 3).unwrap();
    let (bytes, bit_len) = w.finish();
    let mut r = BitReader::new(&bytes, bit_len);
    let before = r.bit_pos();
    assert!(matches!(r.read_bits(4), Err(BitError::Overrun { .. })));
    assert_eq!(r.bit_pos(), before);
    assert_eq!(r.read_bits(3).unwrap(), 0b101);
}

#[test]
fn invalid_nonzero_padding_rejected() {
    // One payload bit set, pad bits dirty.
    let bytes = [0b1000_0001u8];
    let mut r = BitReader::over_bytes(&bytes);
    assert_eq!(r.read_bits(1).unwrap(), 1);
    assert_eq!(r.finish(), Err(BitError::NonCanonicalPadding));
}

#[test]
fn invalid_scalar_code_out_of_range_rejected() {
    // steps=4, code_count=5, bits=3 => codes 5,6,7 are invalid.
    let s = ScalarQuant::new(0.0, 4.0, 1).unwrap();
    assert!(s.decode_value(4).is_ok());
    for bad in [5u64, 6, 7] {
        assert!(matches!(
            s.decode_value(bad),
            Err(CodecError::InvalidCode { .. })
        ));
    }
}
