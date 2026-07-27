//! [`DirtyMask`]: the fixed-capacity bitset backing `NetworkPeer` push-model
//! change tracking (design §3.1). One bit per `field_id`; a set bit means "this
//! field changed since the last encode". Kept dependency-free (a `Vec<u64>` of
//! words) rather than pulling in `fixedbitset`, per the project's
//! earn-your-dependency rule.

/// A fixed-capacity bitset indexed by `field_id`.
///
/// Operations are O(1) for a single bit and O(words) for a scan; setting a bit
/// out of range is a no-op that reports `false`, so a mis-registered field can
/// never index out of bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirtyMask {
    words: Vec<u64>,
    bits: usize,
}

const WORD_BITS: usize = 64;

impl DirtyMask {
    /// A mask holding `bits` fields, all clean.
    #[must_use]
    pub fn new(bits: usize) -> Self {
        let words = bits.div_ceil(WORD_BITS);
        Self {
            words: vec![0; words],
            bits,
        }
    }

    /// Capacity in bits (the layout's field count).
    #[must_use]
    pub fn len(&self) -> usize {
        self.bits
    }

    /// Whether the mask has zero capacity.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bits == 0
    }

    /// Set bit `index`. Returns `true` if the index was in range (and is now
    /// set), `false` if it was out of range (no-op).
    pub fn set(&mut self, index: usize) -> bool {
        if index >= self.bits {
            return false;
        }
        self.words[index / WORD_BITS] |= 1u64 << (index % WORD_BITS);
        true
    }

    /// Whether bit `index` is set. Out-of-range indices read as `false`.
    #[must_use]
    pub fn get(&self, index: usize) -> bool {
        if index >= self.bits {
            return false;
        }
        (self.words[index / WORD_BITS] >> (index % WORD_BITS)) & 1 == 1
    }

    /// Clear every bit.
    pub fn clear(&mut self) {
        for w in &mut self.words {
            *w = 0;
        }
    }

    /// Whether no bit is set.
    #[must_use]
    pub fn none_set(&self) -> bool {
        self.words.iter().all(|&w| w == 0)
    }

    /// Number of set bits. Returns `usize` so an arbitrarily large mask cannot
    /// overflow the count (a builder-produced layout is capped well below this).
    #[must_use]
    pub fn count_set(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Iterate the indices of set bits in ascending order.
    pub fn iter_set(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(wi, &word)| {
            (0..WORD_BITS).filter_map(move |bit| {
                if (word >> bit) & 1 == 1 {
                    Some(wi * WORD_BITS + bit)
                } else {
                    None
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_clear_roundtrip() {
        let mut m = DirtyMask::new(130);
        assert_eq!(m.len(), 130);
        assert!(m.none_set());
        assert!(m.set(0));
        assert!(m.set(63));
        assert!(m.set(64));
        assert!(m.set(129));
        assert!(m.get(0) && m.get(63) && m.get(64) && m.get(129));
        assert!(!m.get(1));
        assert_eq!(m.count_set(), 4);
        let set: Vec<_> = m.iter_set().collect();
        assert_eq!(set, vec![0, 63, 64, 129]);
        m.clear();
        assert!(m.none_set());
        assert_eq!(m.count_set(), 0);
    }

    #[test]
    fn out_of_range_is_a_noop() {
        let mut m = DirtyMask::new(8);
        assert!(!m.set(8));
        assert!(!m.set(100));
        assert!(!m.get(8));
        assert!(m.none_set());
    }

    #[test]
    fn empty_mask() {
        let mut m = DirtyMask::new(0);
        assert!(m.is_empty());
        assert!(!m.set(0));
        assert!(m.none_set());
    }
}
