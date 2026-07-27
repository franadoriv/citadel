//! The closed, versioned quantized codec set shared by both advanced-netcode
//! tracks: bounded fixed-point scalar, a three-axis position vector
//! built from it, and the smallest-three quaternion codec. Defined once here so
//! transform-sync and NetworkPeer (and every SDK) quantize identically.
//!
//! # Canonical rules (adversarial review, )
//!
//! - **Integer `ceil_log2`** ([`ceil_log2`]) picks the bit width, never a float
//!   `log2`, so SDKs cannot disagree at powers of two.
//! - **Inclusive fixed-point.** `steps = round((max-min) * values_per_unit)` and
//!   the representable code range is `0..=steps` (`steps + 1` codes), so both
//!   `min` and `max` are exactly representable. `bits = ceil_log2(steps + 1)`.
//! - **`round(x) = floor(x + 0.5)` in `f64`** ([`round_half_up`]) for every
//!   quantizer, so midpoint rounding is identical across languages.
//! - **Encode saturates, decode rejects.** Out-of-bounds inputs clamp to the
//!   bounds on encode (never wrap); an out-of-range *code* on decode is a
//!   malformed frame and returns [`CodecError::InvalidCode`] (never silently
//!   clamped — that would alias distinct byte strings).
//! - **NaN never survives.** Scalar encode rejects NaN (`±Inf` saturate);
//!   quaternion encode/decode fall back to identity on any non-finite/zero-norm
//!   input, and all quaternion math runs in `f64`.

use crate::bits::{BitError, BitReader, BitWriter, mask_for};

/// Stable codec identifiers. Each participates in the `schema_hash`
/// ([`crate::schema`]) so a class that quantizes a field differently than the
/// server expects is rejected at handshake, not silently corrupted.
///
/// These ids are part of the client contract (`contract.json`) and MUST be
/// stable across SDKs.
pub mod codec_id {
    /// A single boolean, one bit.
    pub const BOOL: u16 = 1;
    /// Bounded fixed-point scalar ([`super::ScalarQuant`]).
    pub const SCALAR_QUANT: u16 = 2;
    /// Three bounded fixed-point scalars (position, [`super::WorldBounds`]).
    pub const VECTOR3_QUANT: u16 = 3;
    /// Smallest-three quaternion, 9 bits/component (interpolation grade).
    pub const QUAT_SMALLEST3_9: u16 = 4;
    /// Smallest-three quaternion, 10 bits/component (animation grade, default).
    pub const QUAT_SMALLEST3_10: u16 = 5;
    /// Smallest-three quaternion, 15 bits/component (physics/state grade).
    pub const QUAT_SMALLEST3_15: u16 = 6;
}

/// `1 / sqrt(2)` to `f64` precision: the bound on every non-largest quaternion
/// component after smallest-three canonicalization.
pub const SQRT_HALF: f64 = core::f64::consts::FRAC_1_SQRT_2;

/// Minimum quaternion norm treated as valid; below this the input is degenerate
/// and encodes as identity.
const QUAT_MIN_NORM: f64 = 1e-6;

/// An error from a quantized codec. Never panics; all failure modes are values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// A scalar input (or bound) was NaN. `±Inf` saturate and are not an error.
    NonFinite,
    /// A decoded code was outside the codec's valid `0..code_count` range — a
    /// malformed frame.
    InvalidCode {
        /// The offending code.
        code: u64,
        /// Number of valid codes (`0..code_count`).
        code_count: u64,
    },
    /// The codec was constructed with invalid parameters (e.g. `max <= min`).
    InvalidSpec(&'static str),
    /// The underlying bit reader/writer failed.
    Bit(BitError),
}

impl core::fmt::Display for CodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CodecError::NonFinite => write!(f, "non-finite scalar value"),
            CodecError::InvalidCode { code, code_count } => {
                write!(f, "quantized code {code} out of range 0..{code_count}")
            }
            CodecError::InvalidSpec(s) => write!(f, "invalid codec spec: {s}"),
            CodecError::Bit(e) => write!(f, "bit stream error: {e}"),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<BitError> for CodecError {
    fn from(e: BitError) -> Self {
        CodecError::Bit(e)
    }
}

/// Integer ceiling of `log2(count)`: the number of bits needed to represent
/// `count` distinct codes `0..count`. `ceil_log2(0) == ceil_log2(1) == 0`.
///
/// Computed with `leading_zeros` (no floating point) so it is bit-exact and
/// identical across languages.
#[must_use]
#[inline]
pub const fn ceil_log2(count: u64) -> u32 {
    if count <= 1 {
        0
    } else {
        64 - (count - 1).leading_zeros()
    }
}

/// Cross-language deterministic rounding: `floor(x + 0.5)` in `f64`. Used for
/// every quantizer so midpoint values round identically regardless of the
/// platform's default float rounding mode.
#[must_use]
#[inline]
pub fn round_half_up(x: f64) -> f64 {
    (x + 0.5).floor()
}

/// A bounded fixed-point scalar codec: maps `[min, max]` onto `steps + 1`
/// evenly spaced codes, inclusive of both endpoints.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScalarQuant {
    min: f64,
    max: f64,
    values_per_unit: u32,
    /// Number of quantization intervals; codes range `0..=steps`.
    steps: u64,
    /// Bits to hold codes `0..=steps` (`ceil_log2(steps + 1)`).
    bits: u32,
}

impl ScalarQuant {
    /// Construct a scalar codec over `[min, max]` at `values_per_unit` codes per
    /// canonical unit (centimeters). Rejects non-finite bounds, `max <= min`, or
    /// `values_per_unit == 0`.
    pub fn new(min: f32, max: f32, values_per_unit: u32) -> Result<Self, CodecError> {
        if !min.is_finite() || !max.is_finite() {
            return Err(CodecError::NonFinite);
        }
        // Both bounds are finite here, so this comparison is total.
        if max <= min {
            return Err(CodecError::InvalidSpec("max must be greater than min"));
        }
        if values_per_unit == 0 {
            return Err(CodecError::InvalidSpec("values_per_unit must be >= 1"));
        }
        let min = f64::from(min);
        let max = f64::from(max);
        let steps = round_half_up((max - min) * f64::from(values_per_unit)) as u64;
        let steps = steps.max(1);
        let bits = ceil_log2(steps + 1);
        Ok(Self {
            min,
            max,
            values_per_unit,
            steps,
            bits,
        })
    }

    /// Bits this codec writes/reads per value.
    #[must_use]
    pub fn bits(&self) -> u32 {
        self.bits
    }

    /// Number of valid codes (`steps + 1`).
    #[must_use]
    pub fn code_count(&self) -> u64 {
        self.steps + 1
    }

    /// Quantize `value` to its code. `±Inf` saturate to the bounds; `NaN` is
    /// rejected. The value is clamped to `[min, max]` (never wraps).
    pub fn encode_value(&self, value: f32) -> Result<u64, CodecError> {
        if value.is_nan() {
            return Err(CodecError::NonFinite);
        }
        let v = f64::from(value).clamp(self.min, self.max);
        let scaled = (v - self.min) * f64::from(self.values_per_unit);
        let code = round_half_up(scaled) as u64;
        Ok(code.min(self.steps))
    }

    /// Dequantize a code. A code `> steps` is a malformed frame and is rejected
    /// (never clamped — that would alias distinct encodings).
    pub fn decode_value(&self, code: u64) -> Result<f32, CodecError> {
        if code > self.steps {
            return Err(CodecError::InvalidCode {
                code,
                code_count: self.code_count(),
            });
        }
        let value = self.min + code as f64 / f64::from(self.values_per_unit);
        Ok(value as f32)
    }

    /// Encode `value` into `w`.
    pub fn write(&self, w: &mut BitWriter, value: f32) -> Result<(), CodecError> {
        let code = self.encode_value(value)?;
        w.write_bits(code, self.bits)?;
        Ok(())
    }

    /// Decode a value from `r`.
    pub fn read(&self, r: &mut BitReader<'_>) -> Result<f32, CodecError> {
        let code = r.read_bits(self.bits)?;
        self.decode_value(code)
    }
}

/// Per-axis world bounds + precision for position quantization.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorldBounds {
    /// Per-axis minimum (cm).
    pub min: [f32; 3],
    /// Per-axis maximum (cm).
    pub max: [f32; 3],
    /// Codes per centimeter (per axis).
    pub values_per_unit: u32,
}

/// The default interpolation-grade world bounds (transform-sync §6.1):
/// ±262144 cm on X/Y, ±32768 cm on Z, at 8 codes/cm (~0.625 mm precision).
pub const DEFAULT_WORLD_BOUNDS: WorldBounds = WorldBounds {
    min: [-262144.0, -262144.0, -32768.0],
    max: [262144.0, 262144.0, 32768.0],
    values_per_unit: 8,
};

/// A three-axis position codec composed of three [`ScalarQuant`]s.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VectorQuant {
    axes: [ScalarQuant; 3],
}

impl VectorQuant {
    /// Build a position codec from world bounds. Each axis must satisfy the
    /// [`ScalarQuant::new`] preconditions.
    pub fn new(bounds: WorldBounds) -> Result<Self, CodecError> {
        Ok(Self {
            axes: [
                ScalarQuant::new(bounds.min[0], bounds.max[0], bounds.values_per_unit)?,
                ScalarQuant::new(bounds.min[1], bounds.max[1], bounds.values_per_unit)?,
                ScalarQuant::new(bounds.min[2], bounds.max[2], bounds.values_per_unit)?,
            ],
        })
    }

    /// Total bits across the three axes.
    #[must_use]
    pub fn bits(&self) -> u32 {
        self.axes.iter().map(ScalarQuant::bits).sum()
    }

    /// Per-axis codec (0=x, 1=y, 2=z).
    #[must_use]
    pub fn axis(&self, i: usize) -> &ScalarQuant {
        &self.axes[i]
    }

    /// Encode `[x, y, z]` into `w`.
    pub fn write(&self, w: &mut BitWriter, pos: [f32; 3]) -> Result<(), CodecError> {
        for (axis, &v) in self.axes.iter().zip(pos.iter()) {
            axis.write(w, v)?;
        }
        Ok(())
    }

    /// Decode `[x, y, z]` from `r`.
    pub fn read(&self, r: &mut BitReader<'_>) -> Result<[f32; 3], CodecError> {
        Ok([
            self.axes[0].read(r)?,
            self.axes[1].read(r)?,
            self.axes[2].read(r)?,
        ])
    }
}

/// Smallest-three quaternion precision modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuatMode {
    /// 9 bits/component, 29 bits total (interpolation grade).
    Bits9,
    /// 10 bits/component, 32 bits total (animation grade, default).
    Bits10,
    /// 15 bits/component, 47 bits total (physics/state grade).
    Bits15,
}

impl QuatMode {
    /// Bits per stored component.
    #[must_use]
    pub const fn bits_per_component(self) -> u32 {
        match self {
            QuatMode::Bits9 => 9,
            QuatMode::Bits10 => 10,
            QuatMode::Bits15 => 15,
        }
    }

    /// Total wire bits: a 2-bit dropped-component index + three components.
    #[must_use]
    pub const fn total_bits(self) -> u32 {
        2 + 3 * self.bits_per_component()
    }

    /// The stable [`codec_id`] for this mode.
    #[must_use]
    pub const fn codec_id(self) -> u16 {
        match self {
            QuatMode::Bits9 => codec_id::QUAT_SMALLEST3_9,
            QuatMode::Bits10 => codec_id::QUAT_SMALLEST3_10,
            QuatMode::Bits15 => codec_id::QUAT_SMALLEST3_15,
        }
    }

    /// Resolve a mode from its bits-per-component (`9`, `10`, or `15`). Any other
    /// value is unsupported.
    #[must_use]
    pub const fn from_bits(bits_per_component: u32) -> Option<Self> {
        match bits_per_component {
            9 => Some(QuatMode::Bits9),
            10 => Some(QuatMode::Bits10),
            15 => Some(QuatMode::Bits15),
            _ => None,
        }
    }
}

/// The canonical identity quaternion `(x, y, z, w) = (0, 0, 0, 1)`.
pub const IDENTITY_QUAT: [f32; 4] = [0.0, 0.0, 0.0, 1.0];

/// Signed unit quantizer over `[-SQRT_HALF, +SQRT_HALF]` with `2^n` levels.
/// Endpoints map exactly to `0` and `2^n - 1`.
fn quat_component_code(v: f64, n: u32) -> u64 {
    let levels = 1u64 << n; // n <= 15, no overflow
    let span = 2.0 * SQRT_HALF;
    let normalized = (v + SQRT_HALF) / span; // 0..=1 for in-range v
    let code = round_half_up(normalized * (levels - 1) as f64);
    // Clamp defensively; canonicalized components are always in range.
    code.clamp(0.0, (levels - 1) as f64) as u64
}

/// Inverse of [`quat_component_code`].
fn quat_component_value(code: u64, n: u32) -> f64 {
    let levels = 1u64 << n;
    let span = 2.0 * SQRT_HALF;
    (code as f64 / (levels - 1) as f64) * span - SQRT_HALF
}

/// The code-level result of smallest-three canonicalization: the 2-bit dropped
/// (largest) component index and the three kept-component codes (ascending
/// source-index order). This is the divergence-prone numeric kernel shared by
/// the bit path here and the C ABI ([`crate::codec`] via `citadel-client-ffi`),
/// so every SDK computes it identically.
pub fn encode_quat_components(quat: [f32; 4], mode: QuatMode) -> (u8, [u64; 3]) {
    let n = mode.bits_per_component();
    let (index, kept) = canonicalize_quat(quat);
    (
        index as u8,
        [
            quat_component_code(kept[0], n),
            quat_component_code(kept[1], n),
            quat_component_code(kept[2], n),
        ],
    )
}

/// Reconstruct a quaternion from its smallest-three code representation. The
/// dropped component uses `sqrt(max(0, 1 - a^2 - b^2 - c^2))` (clamped so it is
/// never `NaN`); the result is renormalized in `f64` and falls back to identity
/// on a degenerate reconstruction. `index` is masked to `0..=3` and each code is
/// masked to the mode width, so no input can panic or diverge.
#[must_use]
pub fn decode_quat_components(index: u8, codes: [u64; 3], mode: QuatMode) -> [f32; 4] {
    let n = mode.bits_per_component();
    let index = (index & 0b11) as usize;
    let kept = [
        quat_component_value(codes[0] & mask_for(n), n),
        quat_component_value(codes[1] & mask_for(n), n),
        quat_component_value(codes[2] & mask_for(n), n),
    ];
    let sum_sq = kept[0] * kept[0] + kept[1] * kept[1] + kept[2] * kept[2];
    let largest = (1.0 - sum_sq).max(0.0).sqrt();

    // Reassemble: `largest` at `index`, the three kept components into the other
    // slots in ascending order (0=x,1=y,2=z,3=w).
    let mut q = [0.0f64; 4];
    q[index] = largest;
    let mut k = 0;
    for (i, slot) in q.iter_mut().enumerate() {
        if i == index {
            continue;
        }
        *slot = kept[k];
        k += 1;
    }

    let norm = (q[0] * q[0] + q[1] * q[1] + q[2] * q[2] + q[3] * q[3]).sqrt();
    if !norm.is_finite() || norm < QUAT_MIN_NORM {
        return IDENTITY_QUAT;
    }
    [
        (q[0] / norm) as f32,
        (q[1] / norm) as f32,
        (q[2] / norm) as f32,
        (q[3] / norm) as f32,
    ]
}

/// Encode a quaternion `(x, y, z, w)` via smallest-three into a bit stream.
///
/// Non-finite or zero-norm inputs encode as [`IDENTITY_QUAT`]. See
/// [`encode_quat_components`] for the shared numeric kernel.
pub fn encode_quat(w: &mut BitWriter, quat: [f32; 4], mode: QuatMode) -> Result<(), CodecError> {
    let n = mode.bits_per_component();
    let (index, codes) = encode_quat_components(quat, mode);
    // 2-bit dropped-component index, then the three kept components (ascending
    // source-index order) each as an n-bit code.
    w.write_bits(u64::from(index), 2)?;
    for &code in &codes {
        w.write_bits(code, n)?;
    }
    Ok(())
}

/// Decode a smallest-three quaternion from a bit stream. See
/// [`decode_quat_components`] for the shared reconstruction kernel.
pub fn decode_quat(r: &mut BitReader<'_>, mode: QuatMode) -> Result<[f32; 4], CodecError> {
    let n = mode.bits_per_component();
    let index = r.read_bits(2)? as u8;
    let codes = [r.read_bits(n)?, r.read_bits(n)?, r.read_bits(n)?];
    Ok(decode_quat_components(index, codes, mode))
}

/// Normalize + canonicalize a quaternion for smallest-three encoding, returning
/// `(dropped_index, [three kept components in ascending source-index order])`.
fn canonicalize_quat(quat: [f32; 4]) -> (usize, [f64; 3]) {
    // Reject non-finite input.
    if quat.iter().any(|c| !c.is_finite()) {
        return canonicalize_quat(IDENTITY_QUAT);
    }
    let q = [
        f64::from(quat[0]),
        f64::from(quat[1]),
        f64::from(quat[2]),
        f64::from(quat[3]),
    ];
    let norm = (q[0] * q[0] + q[1] * q[1] + q[2] * q[2] + q[3] * q[3]).sqrt();
    if !norm.is_finite() || norm < QUAT_MIN_NORM {
        // Identity: largest is w (index 3), kept x/y/z = 0.
        return (3, [0.0, 0.0, 0.0]);
    }
    let mut q = [q[0] / norm, q[1] / norm, q[2] / norm, q[3] / norm];

    // Largest magnitude, tie-break to the lowest index.
    let mut index = 0usize;
    let mut best = q[0].abs();
    for (i, &val) in q.iter().enumerate().skip(1) {
        let m = val.abs();
        if m > best {
            best = m;
            index = i;
        }
    }
    // Sign-canonicalize so the dropped component is non-negative.
    if q[index] < 0.0 {
        for c in q.iter_mut() {
            *c = -*c;
        }
    }
    let mut kept = [0.0f64; 3];
    let mut k = 0;
    for (i, &c) in q.iter().enumerate() {
        if i == index {
            continue;
        }
        kept[k] = c;
        k += 1;
    }
    (index, kept)
}

/// Convenience: a mask covering the low `bits` of a code, exposed for callers
/// that pre-validate raw codes. Delegates to [`mask_for`].
#[must_use]
pub fn code_mask(bits: u32) -> u64 {
    mask_for(bits)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn ceil_log2_matches_expected() {
        assert_eq!(ceil_log2(0), 0);
        assert_eq!(ceil_log2(1), 0);
        assert_eq!(ceil_log2(2), 1);
        assert_eq!(ceil_log2(3), 2);
        assert_eq!(ceil_log2(4), 2);
        assert_eq!(ceil_log2(5), 3);
        assert_eq!(ceil_log2(1 << 20), 20);
        assert_eq!(ceil_log2((1 << 20) + 1), 21);
    }

    #[test]
    fn round_half_up_is_deterministic() {
        assert_eq!(round_half_up(0.5), 1.0);
        assert_eq!(round_half_up(1.5), 2.0);
        assert_eq!(round_half_up(2.5), 3.0); // not banker's rounding
        assert_eq!(round_half_up(2.4999), 2.0);
    }

    #[test]
    fn default_bounds_bit_budget() {
        let v = VectorQuant::new(DEFAULT_WORLD_BOUNDS).unwrap();
        // Inclusive endpoints: XY steps=4_194_304 => 23 bits; Z steps=524_288 => 20.
        assert_eq!(v.axis(0).bits(), 23);
        assert_eq!(v.axis(1).bits(), 23);
        assert_eq!(v.axis(2).bits(), 20);
        assert_eq!(v.bits(), 66);
    }

    #[test]
    fn scalar_endpoints_and_midpoint_round_trip() {
        let s = ScalarQuant::new(-100.0, 100.0, 10).unwrap();
        for &(v, tol) in &[(-100.0, 0.05), (0.0, 0.05), (100.0, 0.05), (37.3, 0.05)] {
            let code = s.encode_value(v).unwrap();
            let back = s.decode_value(code).unwrap();
            assert!((back - v).abs() <= tol, "v={v} back={back}");
        }
        // Exact endpoints reachable.
        assert_eq!(s.decode_value(0).unwrap(), -100.0);
        assert_eq!(s.decode_value(s.code_count() - 1).unwrap(), 100.0);
    }

    #[test]
    fn scalar_saturates_out_of_bounds_never_wraps() {
        let s = ScalarQuant::new(0.0, 10.0, 4).unwrap();
        // Above max saturates to the max code.
        assert_eq!(s.encode_value(1000.0).unwrap(), s.steps);
        // Below min saturates to code 0.
        assert_eq!(s.encode_value(-1000.0).unwrap(), 0);
        // +Inf/-Inf saturate; NaN is rejected.
        assert_eq!(s.encode_value(f32::INFINITY).unwrap(), s.steps);
        assert_eq!(s.encode_value(f32::NEG_INFINITY).unwrap(), 0);
        assert_eq!(s.encode_value(f32::NAN), Err(CodecError::NonFinite));
    }

    #[test]
    fn scalar_decode_rejects_out_of_range_code() {
        let s = ScalarQuant::new(0.0, 3.0, 1).unwrap(); // steps=3, code_count=4, bits=2
        assert_eq!(s.bits(), 2);
        assert!(s.decode_value(3).is_ok());
        // code_count is a power of two here, so all 2-bit codes are valid.
        // Use a non-power-of-two range to exercise rejection.
        let s = ScalarQuant::new(0.0, 4.0, 1).unwrap(); // steps=4, code_count=5, bits=3
        assert_eq!(s.bits(), 3);
        assert!(s.decode_value(4).is_ok());
        assert!(matches!(
            s.decode_value(5),
            Err(CodecError::InvalidCode { code: 5, .. })
        ));
    }

    #[test]
    fn scalar_bad_spec_rejected() {
        assert!(ScalarQuant::new(1.0, 1.0, 1).is_err());
        assert!(ScalarQuant::new(2.0, 1.0, 1).is_err());
        assert!(ScalarQuant::new(0.0, 1.0, 0).is_err());
        assert!(ScalarQuant::new(f32::NAN, 1.0, 1).is_err());
    }

    #[test]
    fn vector_round_trip_within_precision() {
        let v = VectorQuant::new(DEFAULT_WORLD_BOUNDS).unwrap();
        let mut w = BitWriter::new();
        let pos = [1234.5, -9876.25, 42.0];
        v.write(&mut w, pos).unwrap();
        let (bytes, bit_len) = w.finish();
        let mut r = BitReader::new(&bytes, bit_len);
        let back = v.read(&mut r).unwrap();
        // 8 codes/cm => ~0.0625 cm error bound.
        for i in 0..3 {
            assert!((back[i] - pos[i]).abs() <= 0.0625, "axis {i}");
        }
    }

    fn norm4(q: [f32; 4]) -> f32 {
        (q[0] * q[0] + q[1] * q[1] + q[2] * q[2] + q[3] * q[3]).sqrt()
    }

    fn dot4(a: [f32; 4], b: [f32; 4]) -> f32 {
        a[0] * b[0] + a[1] * b[1] + a[2] * b[2] + a[3] * b[3]
    }

    /// Whether `a` and `b` are the same rotation (unit-normalized, sign-agnostic).
    fn same_rotation(a: [f32; 4], b: [f32; 4]) -> bool {
        let na = norm4(a);
        let nb = norm4(b);
        na > 0.0 && nb > 0.0 && (dot4(a, b) / (na * nb)).abs() > 0.999
    }

    fn quat_round_trip(q: [f32; 4], mode: QuatMode) -> [f32; 4] {
        let mut w = BitWriter::new();
        encode_quat(&mut w, q, mode).unwrap();
        let (bytes, bit_len) = w.finish();
        assert_eq!(bit_len, u64::from(mode.total_bits()));
        let mut r = BitReader::new(&bytes, bit_len);
        let out = decode_quat(&mut r, mode).unwrap();
        r.finish().unwrap();
        out
    }

    #[test]
    fn quat_identity_round_trips() {
        let out = quat_round_trip(IDENTITY_QUAT, QuatMode::Bits10);
        assert!((norm4(out) - 1.0).abs() < 1e-4);
        // Same rotation (allow global sign).
        assert!(same_rotation(out, IDENTITY_QUAT));
    }

    #[test]
    fn quat_each_axis_largest_round_trips() {
        // A quaternion where each component in turn is the largest.
        let cases = [
            [0.9239, 0.3827, 0.0, 0.0],
            [0.0, 0.9239, 0.3827, 0.0],
            [0.0, 0.0, 0.9239, 0.3827],
            [0.3827, 0.0, 0.0, 0.9239],
        ];
        for q in cases {
            let out = quat_round_trip(q, QuatMode::Bits15);
            // Same rotation within a tight angular bound (allow global sign).
            assert!(same_rotation(out, q), "q={q:?} out={out:?}");
            assert!((norm4(out) - 1.0).abs() < 1e-4);
        }
    }

    #[test]
    fn quat_all_equal_uses_lowest_index_tiebreak() {
        // (0.5,0.5,0.5,0.5): all equal magnitude => drop index 0 (x).
        let q = [0.5, 0.5, 0.5, 0.5];
        let (index, _kept) = canonicalize_quat(q);
        assert_eq!(index, 0);
        let out = quat_round_trip(q, QuatMode::Bits15);
        assert!(same_rotation(out, q));
    }

    #[test]
    fn quat_negative_largest_is_sign_canonicalized() {
        // w is largest and negative => whole quat negated so dropped w >= 0.
        let q = [0.1, 0.1, 0.1, -0.9797];
        let (index, _kept) = canonicalize_quat(q);
        assert_eq!(index, 3);
        let out = quat_round_trip(q, QuatMode::Bits15);
        // Represents the same rotation (global sign is irrelevant).
        assert!(same_rotation(out, q), "out={out:?}");
    }

    #[test]
    fn quat_non_finite_and_zero_norm_fall_back_to_identity() {
        let out = quat_round_trip([f32::NAN, 0.0, 0.0, 1.0], QuatMode::Bits10);
        assert!(same_rotation(out, IDENTITY_QUAT));
        let out = quat_round_trip([0.0, 0.0, 0.0, 0.0], QuatMode::Bits10);
        assert!(same_rotation(out, IDENTITY_QUAT));
    }

    #[test]
    fn quat_decode_never_nan_for_any_bits() {
        // Feed all-ones component codes (max magnitude) so 1 - sum_sq < 0; the
        // clamp must keep the reconstructed component real.
        for mode in [QuatMode::Bits9, QuatMode::Bits10, QuatMode::Bits15] {
            let n = mode.bits_per_component();
            let mut w = BitWriter::new();
            w.write_bits(0, 2).unwrap();
            for _ in 0..3 {
                w.write_bits(mask_for(n), n).unwrap();
            }
            let (bytes, bit_len) = w.finish();
            let mut r = BitReader::new(&bytes, bit_len);
            let out = decode_quat(&mut r, mode).unwrap();
            assert!(out.iter().all(|c| c.is_finite()), "mode={mode:?}");
        }
    }
}
