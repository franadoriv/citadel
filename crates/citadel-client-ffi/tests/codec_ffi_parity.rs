//! C-ABI ↔ native codec parity.
//!
//! Proves the `citadel-client-ffi` codec entrypoints produce bit-identical
//! results to the native `citadel_wire::codec` path, so every SDK that calls the
//! C ABI shares exactly one implementation and cannot diverge or produce NaN.
//!
//! Every `unsafe` call passes valid pointers to stack locals (or a deliberate
//! null to exercise the null-check), so the crate-wide unsafe-doc lint is
//! relaxed for this test.
#![allow(clippy::unwrap_used, clippy::undocumented_unsafe_blocks)]

use citadel_client_ffi::CitadelStatus;
use citadel_client_ffi::codec_ffi::{
    citadel_dequantize_scalar, citadel_quantize_scalar, citadel_quat_decode_components,
    citadel_quat_encode_components,
};
use citadel_wire::codec::{QuatMode, ScalarQuant, decode_quat_components, encode_quat_components};

#[test]
fn scalar_ffi_matches_native() {
    let specs = [
        (0.0f32, 4.0f32, 1u32),
        (-100.0, 100.0, 10),
        (-262144.0, 262144.0, 8),
        (-32768.0, 32768.0, 8),
    ];
    let values = [
        0.0f32,
        1.0,
        -1.0,
        2.5,
        37.3,
        1e9,
        -1e9,
        f32::INFINITY,
        f32::NEG_INFINITY,
    ];
    for (min, max, vpu) in specs {
        let native = ScalarQuant::new(min, max, vpu).unwrap();
        for &value in &values {
            let expected_code = native.encode_value(value).unwrap();
            let mut ffi_code = 0u64;
            let st = unsafe { citadel_quantize_scalar(min, max, vpu, value, &mut ffi_code) };
            assert_eq!(st, CitadelStatus::Ok);
            assert_eq!(ffi_code, expected_code, "code {min}/{max}/{vpu} v={value}");

            let expected_val = native.decode_value(expected_code).unwrap();
            let mut ffi_val = 0.0f32;
            let st = unsafe { citadel_dequantize_scalar(min, max, vpu, ffi_code, &mut ffi_val) };
            assert_eq!(st, CitadelStatus::Ok);
            assert_eq!(
                ffi_val.to_bits(),
                expected_val.to_bits(),
                "value round-trip"
            );
        }
    }
}

#[test]
fn scalar_ffi_rejects_nan_and_bad_spec() {
    let mut code = 0u64;
    // NaN value.
    assert_eq!(
        unsafe { citadel_quantize_scalar(0.0, 1.0, 1, f32::NAN, &mut code) },
        CitadelStatus::InvalidArgument
    );
    // Bad spec.
    assert_eq!(
        unsafe { citadel_quantize_scalar(1.0, 1.0, 1, 0.5, &mut code) },
        CitadelStatus::InvalidArgument
    );
    // Null out-pointer.
    assert_eq!(
        unsafe { citadel_quantize_scalar(0.0, 1.0, 1, 0.5, std::ptr::null_mut()) },
        CitadelStatus::InvalidArgument
    );
}

#[test]
fn quat_ffi_matches_native() {
    let quats = [
        [0.0f32, 0.0, 0.0, 1.0],
        [0.9239, 0.3827, 0.0, 0.0],
        [0.0, 0.9239, 0.3827, 0.0],
        [0.0, 0.0, 0.9239, 0.3827],
        [0.3827, 0.0, 0.0, 0.9239],
        [0.5, 0.5, 0.5, 0.5],
        [0.1, 0.1, 0.1, -0.9797],
        [f32::NAN, 0.0, 0.0, 1.0],
        [0.0, 0.0, 0.0, 0.0],
    ];
    for bits in [9u32, 10, 15] {
        let mode = QuatMode::from_bits(bits).unwrap();
        for q in quats {
            let (exp_index, exp_codes) = encode_quat_components(q, mode);

            let mut index = 0u8;
            let mut codes = [0u64; 3];
            let st = unsafe {
                citadel_quat_encode_components(q.as_ptr(), bits, &mut index, codes.as_mut_ptr())
            };
            assert_eq!(st, CitadelStatus::Ok);
            assert_eq!(index, exp_index, "index bits={bits} q={q:?}");
            assert_eq!(codes, exp_codes, "codes bits={bits} q={q:?}");

            let exp_quat = decode_quat_components(index, codes, mode);
            let mut out = [0.0f32; 4];
            let st = unsafe {
                citadel_quat_decode_components(index, codes.as_ptr(), bits, out.as_mut_ptr())
            };
            assert_eq!(st, CitadelStatus::Ok);
            for i in 0..4 {
                assert_eq!(out[i].to_bits(), exp_quat[i].to_bits(), "decode bit-exact");
                assert!(out[i].is_finite(), "never NaN");
            }
        }
    }
}

#[test]
fn quat_ffi_rejects_bad_bits() {
    let q = [0.0f32, 0.0, 0.0, 1.0];
    let mut index = 0u8;
    let mut codes = [0u64; 3];
    assert_eq!(
        unsafe { citadel_quat_encode_components(q.as_ptr(), 7, &mut index, codes.as_mut_ptr()) },
        CitadelStatus::InvalidArgument
    );
    let mut out = [0.0f32; 4];
    assert_eq!(
        unsafe { citadel_quat_decode_components(0, codes.as_ptr(), 7, out.as_mut_ptr()) },
        CitadelStatus::InvalidArgument
    );
}
