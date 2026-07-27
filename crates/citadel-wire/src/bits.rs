//! `BitWriter` / `BitReader`: the shared self-synchronizing bit-packing layer
//! for Citadel netcode (, the `FBitWriter`/`FBitReader` analogue).
//!
//! Both advanced-netcode tracks (transform-sync and NetworkPeer) pack their
//! frames through this one implementation so the two SDKs can never encode
//! different widths or bit orders. The contract is pinned by bit-level test
//! vectors (`crates/citadel-wire/tests/wire_vectors.rs`) so an independently
//! built C#/GDScript/C++ SDK produces identical bytes.
//!
//! # Bit order (canonical)
//!
//! Bits are written **most-significant-first within each byte**. Writing an
//! `n`-bit value emits its most-significant of those `n` bits first. This is the
//! conventional network bit order and, together with deterministic zero padding
//! to a byte boundary, is what makes the stream self-synchronizing across
//! independently built SDKs.
//!
//! # Safety properties (adversarial review, )
//!
//! - `n == 64` is handled without any shift-by-width (which is UB/panic in most
//!   languages): masks are built with [`mask_for`].
//! - [`BitReader::read_bits`] is **bound-before-consume**: it checks the cursor
//!   against the reader's exact bit length *before* advancing, so a truncated
//!   final field aborts with [`BitError::Overrun`] and never over-reads. On
//!   error the cursor is left unchanged, so a failed field read is transactional
//!   for the caller (snapshot [`BitReader::bit_pos`], restore on abort).
//! - Padding is **canonical**: [`BitReader::finish`] rejects a stream with
//!   nonzero trailing pad bits or any unconsumed whole byte, so two distinct
//!   byte strings can never decode to the same logical frame.

use core::fmt;

/// An error produced by the bit reader/writer. Never panics; all failure modes
/// are values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitError {
    /// A read asked for more bits than remain in the stream. The cursor is left
    /// unchanged (bound-before-consume).
    Overrun {
        /// Bits requested.
        needed: u32,
        /// Bits still available.
        remaining: u64,
    },
    /// A read or write requested more than 64 bits in a single call.
    TooManyBits {
        /// Bits requested.
        requested: u32,
    },
    /// [`BitReader::finish`] found nonzero trailing pad bits or an unconsumed
    /// whole byte — the encoding is not canonical.
    NonCanonicalPadding,
    /// A packed bit slice declared more payload bits than it contains.
    PackedBitsTooShort {
        /// Declared payload bits.
        declared: u64,
        /// Available bits in the provided bytes.
        available: u64,
    },
}

impl fmt::Display for BitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BitError::Overrun { needed, remaining } => {
                write!(
                    f,
                    "bit read overrun: needed {needed}, {remaining} remaining"
                )
            }
            BitError::TooManyBits { requested } => {
                write!(f, "bit op requested {requested} bits (max 64)")
            }
            BitError::NonCanonicalPadding => {
                write!(f, "non-canonical trailing padding in bit stream")
            }
            BitError::PackedBitsTooShort {
                declared,
                available,
            } => {
                write!(
                    f,
                    "packed bit slice declares {declared} bits but has {available}"
                )
            }
        }
    }
}

impl std::error::Error for BitError {}

/// A mask selecting the low `n` bits of a `u64`, safe for `n == 0..=64`.
///
/// `1u64 << 64` is undefined/panicking, so `n == 64` is special-cased.
#[must_use]
#[inline]
pub const fn mask_for(n: u32) -> u64 {
    if n == 0 {
        0
    } else if n >= 64 {
        u64::MAX
    } else {
        (1u64 << n) - 1
    }
}

/// A most-significant-first bit writer accumulating into a `Vec<u8>`.
///
/// The final partial byte is zero-padded to a byte boundary by [`finish`].
///
/// [`finish`]: BitWriter::finish
#[derive(Debug, Default, Clone)]
pub struct BitWriter {
    bytes: Vec<u8>,
    /// Number of bits already written into the last (partial) byte, `0..8`. When
    /// `0` the stream is byte-aligned and a fresh byte is pushed on the next
    /// write.
    bit_in_byte: u8,
    /// Total bits written.
    bit_len: u64,
}

impl BitWriter {
    /// A new, empty writer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            bytes: Vec::new(),
            bit_in_byte: 0,
            bit_len: 0,
        }
    }

    /// A new writer pre-reserving `bytes` of backing capacity.
    #[must_use]
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(bytes),
            bit_in_byte: 0,
            bit_len: 0,
        }
    }

    /// Total number of bits written so far (excludes padding added by
    /// [`finish`](BitWriter::finish)).
    #[must_use]
    pub fn bit_len(&self) -> u64 {
        self.bit_len
    }

    /// Write the low `n` bits of `value`, most-significant of those `n` bits
    /// first. `n == 0` is a no-op. Returns [`BitError::TooManyBits`] for
    /// `n > 64`.
    pub fn write_bits(&mut self, value: u64, n: u32) -> Result<(), BitError> {
        if n > 64 {
            return Err(BitError::TooManyBits { requested: n });
        }
        if n == 0 {
            return Ok(());
        }
        let value = value & mask_for(n);
        // Emit MSB-first: bit index (n-1) down to 0.
        let mut remaining = n;
        while remaining > 0 {
            if self.bit_in_byte == 0 {
                self.bytes.push(0);
            }
            let free = 8 - self.bit_in_byte; // free bits in the current byte
            let take = free.min(remaining as u8);
            // The next `take` source bits are the top `take` of the remaining
            // `remaining` bits.
            let shift = remaining - take as u32;
            let chunk = ((value >> shift) & mask_for(take as u32)) as u8;
            // Place `chunk` so its MSB aligns with the current write position.
            let dest_shift = free - take;
            if let Some(last) = self.bytes.last_mut() {
                *last |= chunk << dest_shift;
            }
            self.bit_in_byte = (self.bit_in_byte + take) % 8;
            remaining -= take as u32;
        }
        self.bit_len += n as u64;
        Ok(())
    }

    /// Write a single boolean as one bit.
    pub fn write_bool(&mut self, value: bool) -> Result<(), BitError> {
        self.write_bits(u64::from(value), 1)
    }

    /// Append exactly `bit_len` most-significant-first bits from `bytes`.
    ///
    /// This permits a previously encoded bit payload to be spliced into a new
    /// enclosing stream without changing its bit alignment or its zero padding.
    pub fn write_packed(&mut self, bytes: &[u8], bit_len: u64) -> Result<(), BitError> {
        let available = (bytes.len() as u64).saturating_mul(8);
        if bit_len > available {
            return Err(BitError::PackedBitsTooShort {
                declared: bit_len,
                available,
            });
        }
        let mut remaining = bit_len;
        for &byte in bytes {
            if remaining == 0 {
                break;
            }
            let take = remaining.min(8) as u32;
            self.write_bits(u64::from(byte >> (8 - take)), take)?;
            remaining -= u64::from(take);
        }
        Ok(())
    }

    /// Finalize the stream, zero-padding the final partial byte to a byte
    /// boundary, and return the backing bytes plus the exact bit length.
    ///
    /// The bit length is what a [`BitReader`] needs to reject truncated final
    /// fields; callers that frame the bytes without also carrying the bit length
    /// rely on a fixed schema field count plus [`BitReader::finish`] for
    /// canonicality.
    #[must_use]
    pub fn finish(self) -> (Vec<u8>, u64) {
        (self.bytes, self.bit_len)
    }

    /// Finalize and return only the padded bytes (the common wire case).
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// A most-significant-first bit reader over a byte slice.
///
/// Constructed with an explicit `bit_len` bound (`<= bytes.len * 8`) so the
/// reader knows where real payload ends and zero padding begins. Use
/// [`BitReader::over_bytes`] when the entire slice is payload up to its final
/// byte boundary.
#[derive(Debug, Clone)]
pub struct BitReader<'a> {
    bytes: &'a [u8],
    /// Total readable payload bits (`<= bytes.len * 8`).
    bit_len: u64,
    /// Absolute bit cursor into `bytes`.
    bit_pos: u64,
}

impl<'a> BitReader<'a> {
    /// A reader over `bytes` treating exactly `bit_len` bits as payload.
    ///
    /// `bit_len` is clamped to `bytes.len * 8`.
    #[must_use]
    pub fn new(bytes: &'a [u8], bit_len: u64) -> Self {
        let max = (bytes.len() as u64) * 8;
        Self {
            bytes,
            bit_len: bit_len.min(max),
            bit_pos: 0,
        }
    }

    /// A reader treating the whole slice (all `bytes.len * 8` bits) as
    /// payload. Trailing sub-byte padding, if any, is validated by
    /// [`finish`](BitReader::finish).
    #[must_use]
    pub fn over_bytes(bytes: &'a [u8]) -> Self {
        Self::new(bytes, (bytes.len() as u64) * 8)
    }

    /// Current absolute bit cursor. Snapshot this before a fallible multi-field
    /// decode and restore with [`seek`](BitReader::seek) to roll back on abort.
    #[must_use]
    pub fn bit_pos(&self) -> u64 {
        self.bit_pos
    }

    /// Bits still available to read.
    #[must_use]
    pub fn bits_remaining(&self) -> u64 {
        self.bit_len.saturating_sub(self.bit_pos)
    }

    /// Restore the cursor to a previously captured [`bit_pos`](BitReader::bit_pos).
    /// Positions past `bit_len` are clamped.
    pub fn seek(&mut self, bit_pos: u64) {
        self.bit_pos = bit_pos.min(self.bit_len);
    }

    /// Read `n` bits MSB-first into the low `n` bits of the result.
    ///
    /// Bound-before-consume: if fewer than `n` bits remain the cursor is left
    /// unchanged and [`BitError::Overrun`] is returned. `n == 0` yields `0`.
    pub fn read_bits(&mut self, n: u32) -> Result<u64, BitError> {
        if n > 64 {
            return Err(BitError::TooManyBits { requested: n });
        }
        if n == 0 {
            return Ok(0);
        }
        let remaining = self.bits_remaining();
        if remaining < n as u64 {
            return Err(BitError::Overrun {
                needed: n,
                remaining,
            });
        }
        let mut result: u64 = 0;
        let mut left = n;
        while left > 0 {
            let byte_index = (self.bit_pos / 8) as usize;
            let bit_in_byte = (self.bit_pos % 8) as u8;
            let free = 8 - bit_in_byte; // unread bits in this byte, from MSB side
            let take = free.min(left as u8);
            let byte = self.bytes[byte_index];
            // Extract the top `take` unread bits of this byte.
            let src_shift = free - take;
            let chunk = (byte >> src_shift) & (mask_for(take as u32) as u8);
            result = (result << take) | u64::from(chunk);
            self.bit_pos += u64::from(take);
            left -= take as u32;
        }
        Ok(result)
    }

    /// Read a single bit as a boolean.
    pub fn read_bool(&mut self) -> Result<bool, BitError> {
        Ok(self.read_bits(1)? != 0)
    }

    /// Assert canonical termination: every remaining bit up to the next byte
    /// boundary must be zero and no whole payload byte may be left unconsumed.
    ///
    /// This makes the encoding canonical (review finding 2): a frame with
    /// nonzero pad bits or trailing junk is rejected rather than silently
    /// accepted, so distinct byte strings cannot decode to the same frame.
    pub fn finish(&self) -> Result<(), BitError> {
        let remaining = self.bits_remaining();
        if remaining >= 8 {
            return Err(BitError::NonCanonicalPadding);
        }
        if remaining == 0 {
            return Ok(());
        }
        // 1..=7 pad bits: all must be zero.
        let byte_index = (self.bit_pos / 8) as usize;
        let bit_in_byte = (self.bit_pos % 8) as u8;
        let pad = self.bytes[byte_index] & (mask_for(u32::from(8 - bit_in_byte)) as u8);
        if pad != 0 {
            return Err(BitError::NonCanonicalPadding);
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_mixed_widths() {
        let mut w = BitWriter::new();
        w.write_bits(0b101, 3).unwrap();
        w.write_bits(0xABCD, 16).unwrap();
        w.write_bool(true).unwrap();
        w.write_bits(0, 0).unwrap(); // no-op
        w.write_bits(0x3F, 6).unwrap();
        let (bytes, bit_len) = w.finish();
        assert_eq!(bit_len, 3 + 16 + 1 + 6);

        let mut r = BitReader::new(&bytes, bit_len);
        assert_eq!(r.read_bits(3).unwrap(), 0b101);
        assert_eq!(r.read_bits(16).unwrap(), 0xABCD);
        assert!(r.read_bool().unwrap());
        assert_eq!(r.read_bits(6).unwrap(), 0x3F);
        r.finish().unwrap();
    }

    #[test]
    fn msb_first_byte_layout_is_fixed() {
        // 0b1 in 1 bit lands in the top bit of the first byte => 0x80.
        let mut w = BitWriter::new();
        w.write_bits(1, 1).unwrap();
        let bytes = w.into_bytes();
        assert_eq!(bytes, vec![0x80]);

        // 0xA (1010) as 4 bits then 0x5 (0101) as 4 bits => 0xA5.
        let mut w = BitWriter::new();
        w.write_bits(0xA, 4).unwrap();
        w.write_bits(0x5, 4).unwrap();
        assert_eq!(w.into_bytes(), vec![0xA5]);
    }

    #[test]
    fn full_64_bit_value_round_trips() {
        let mut w = BitWriter::new();
        w.write_bits(u64::MAX, 64).unwrap();
        w.write_bits(0x1234_5678_9ABC_DEF0, 64).unwrap();
        let (bytes, bit_len) = w.finish();
        assert_eq!(bit_len, 128);
        let mut r = BitReader::new(&bytes, bit_len);
        assert_eq!(r.read_bits(64).unwrap(), u64::MAX);
        assert_eq!(r.read_bits(64).unwrap(), 0x1234_5678_9ABC_DEF0);
        r.finish().unwrap();
    }

    #[test]
    fn write_and_read_reject_more_than_64_bits() {
        let mut w = BitWriter::new();
        assert_eq!(
            w.write_bits(0, 65),
            Err(BitError::TooManyBits { requested: 65 })
        );
        let bytes = [0u8; 16];
        let mut r = BitReader::over_bytes(&bytes);
        assert_eq!(
            r.read_bits(65),
            Err(BitError::TooManyBits { requested: 65 })
        );
    }

    #[test]
    fn overrun_leaves_cursor_unchanged() {
        let mut w = BitWriter::new();
        w.write_bits(0b1011, 4).unwrap();
        let (bytes, bit_len) = w.finish();
        let mut r = BitReader::new(&bytes, bit_len);
        let before = r.bit_pos();
        // Only 4 payload bits; asking for 5 overruns without consuming.
        assert!(matches!(
            r.read_bits(5),
            Err(BitError::Overrun {
                needed: 5,
                remaining: 4
            })
        ));
        assert_eq!(r.bit_pos(), before, "overrun did not advance the cursor");
        // The 4 real bits are still readable.
        assert_eq!(r.read_bits(4).unwrap(), 0b1011);
    }

    #[test]
    fn truncated_final_field_overruns_not_reads_padding() {
        // Payload is 4 bits but the byte holds 8; with the exact bit_len the
        // reader refuses to treat the 4 pad bits as a real field.
        let mut w = BitWriter::new();
        w.write_bits(0b1111, 4).unwrap();
        let (bytes, bit_len) = w.finish();
        assert_eq!(bytes.len(), 1);
        let mut r = BitReader::new(&bytes, bit_len);
        assert_eq!(r.read_bits(4).unwrap(), 0b1111);
        assert!(matches!(r.read_bits(4), Err(BitError::Overrun { .. })));
    }

    #[test]
    fn finish_rejects_nonzero_padding() {
        // One payload bit, but the pad bits are dirtied. Over the whole byte the
        // reader must see the nonzero padding and reject it.
        let bytes = [0b1000_0001u8];
        let mut r = BitReader::over_bytes(&bytes);
        assert_eq!(r.read_bits(1).unwrap(), 1);
        assert_eq!(r.finish(), Err(BitError::NonCanonicalPadding));
    }

    #[test]
    fn finish_rejects_unconsumed_whole_byte() {
        let bytes = [0x00u8, 0x00u8];
        let mut r = BitReader::new(&bytes, 16);
        assert_eq!(r.read_bits(4).unwrap(), 0);
        // 12 bits remain (>= 8) => trailing junk / unconsumed data.
        assert_eq!(r.finish(), Err(BitError::NonCanonicalPadding));
    }

    #[test]
    fn seek_restores_cursor_for_transactional_abort() {
        let mut w = BitWriter::new();
        w.write_bits(0xDEAD, 16).unwrap();
        let (bytes, bit_len) = w.finish();
        let mut r = BitReader::new(&bytes, bit_len);
        let checkpoint = r.bit_pos();
        assert_eq!(r.read_bits(8).unwrap(), 0xDE);
        r.seek(checkpoint);
        assert_eq!(r.read_bits(16).unwrap(), 0xDEAD);
    }

    #[test]
    fn mask_for_edges() {
        assert_eq!(mask_for(0), 0);
        assert_eq!(mask_for(1), 0b1);
        assert_eq!(mask_for(63), (1u64 << 63) - 1);
        assert_eq!(mask_for(64), u64::MAX);
        assert_eq!(mask_for(100), u64::MAX);
    }
}
