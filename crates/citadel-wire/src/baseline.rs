//! The shared per-connection baseline + ack model: server-issued
//! **monotonic, nonzero** baseline tokens, a 32-bit ack window, and a
//! single-ordered per-`(object, receiver)` tracker that only advances on a
//! currently-outstanding, strictly-newer token.
//!
//! Both advanced-netcode tracks share this one concept (transform-sync absolute
//! `snapshot_id`/`base_snapshot_id`; NetworkPeer server-issued `baseline_id` +
//! explicit `is_full`). The stores are per role; the model/type is shared.
//!
//! # Hardening (adversarial review, )
//!
//! - Tokens are `NonZeroU64`; `0` is reserved for "none"/`is_full`, so a stale
//!   ack naming `0` can never be a valid target.
//! - The allocator uses a **checked** increment and refuses to wrap.
//! - The 32-bit window shift handles the `delta == 32` and `delta > 32` edges
//!   without shift-by-width UB.
//! - [`BaselineTracker::apply_ack`] processes the whole ack window (latest +
//!   history), advances only to outstanding, strictly-newer tokens, and can
//!   never regress on a stale/unknown/forged id.

use std::collections::BTreeSet;
use std::num::NonZeroU64;

/// A server-issued baseline token. Always nonzero and monotonically increasing
/// per connection; `0` is reserved for "no baseline" / a full snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BaselineId(NonZeroU64);

impl BaselineId {
    /// Wrap a raw value, rejecting `0`.
    #[must_use]
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    /// The raw nonzero value.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// An error minting a baseline token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BaselineError {
    /// The `u64` id space is exhausted; the connection must be reset (a new
    /// epoch), never wrapped back to a reusable id.
    #[error("baseline id space exhausted")]
    Exhausted,
}

/// Mints strictly-increasing, nonzero baseline tokens for one connection/role.
#[derive(Debug, Clone)]
pub struct BaselineAllocator {
    next: u64,
}

impl Default for BaselineAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl BaselineAllocator {
    /// A fresh allocator whose first token is `1`.
    #[must_use]
    pub fn new() -> Self {
        Self { next: 1 }
    }

    /// Mint the next token, or [`BaselineError::Exhausted`] at the end of the
    /// `u64` space (checked; never wraps to a reusable id).
    ///
    /// `next == 0` is the exhaustion sentinel: after minting `u64::MAX` the
    /// cursor is set to `0`, so the following call fails rather than wrapping to
    /// a previously issued id.
    pub fn allocate(&mut self) -> Result<BaselineId, BaselineError> {
        let value = self.next;
        let id = BaselineId::new(value).ok_or(BaselineError::Exhausted)?;
        // On overflow, mark exhausted (sentinel 0) but still return this valid id.
        self.next = value.checked_add(1).unwrap_or(0);
        Ok(id)
    }

    /// The value the next [`allocate`](BaselineAllocator::allocate) will mint.
    #[must_use]
    pub fn peek_next(&self) -> u64 {
        self.next
    }
}

/// Number of prior ids the [`AckField`] history covers.
pub const ACK_HISTORY_BITS: u32 = 32;

/// A 32-bit sliding ack window: a `latest` acked absolute id plus a bitfield of
/// the 32 ids immediately preceding it (Quake/Gaffer style), so a lost ack does
/// not lose the acknowledgement of an earlier id.
///
/// Bit `k` (`0..=31`) of `history` means the id `latest - 1 - k` was acked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AckField {
    latest: Option<u64>,
    history: u32,
}

impl AckField {
    /// An empty window (nothing acked yet).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The most-recently acked absolute id, if any.
    #[must_use]
    pub fn latest(&self) -> Option<u64> {
        self.latest
    }

    /// The raw history bitfield.
    #[must_use]
    pub fn history(&self) -> u32 {
        self.history
    }

    /// Record an ack for absolute id `id` (`0` is ignored — not a valid token).
    pub fn ack(&mut self, id: u64) {
        if id == 0 {
            return;
        }
        match self.latest {
            None => {
                self.latest = Some(id);
                self.history = 0;
            }
            Some(latest) => {
                if id > latest {
                    let delta = id - latest;
                    self.history = shift_history(self.history, delta);
                    self.latest = Some(id);
                } else if id < latest {
                    let offset = latest - id; // >= 1
                    if offset <= ACK_HISTORY_BITS as u64 {
                        self.history |= 1u32 << (offset as u32 - 1);
                    }
                    // Older than the window => already implicitly superseded.
                }
                // id == latest: no-op.
            }
        }
    }

    /// Whether absolute id `id` has been acked within the window.
    #[must_use]
    pub fn is_acked(&self, id: u64) -> bool {
        match self.latest {
            Some(latest) if id == latest => true,
            Some(latest) if id < latest => {
                let offset = latest - id;
                offset <= ACK_HISTORY_BITS as u64
                    && (self.history & (1u32 << (offset as u32 - 1))) != 0
            }
            _ => false,
        }
    }

    /// Iterate every absolute id represented as acked in this window (the latest
    /// plus each set history bit).
    pub fn iter_acked(&self) -> impl Iterator<Item = u64> + '_ {
        let latest = self.latest;
        let history = self.history;
        latest
            .into_iter()
            .chain((0..ACK_HISTORY_BITS).filter_map(move |k| {
                let l = latest?;
                if history & (1u32 << k) != 0 {
                    // bit k => id = latest - 1 - k
                    l.checked_sub(1 + u64::from(k)).filter(|&id| id >= 1)
                } else {
                    None
                }
            }))
    }

    /// Encode to the wire pair `(latest, history)`; `latest == 0` means "none".
    #[must_use]
    pub fn to_wire(&self) -> (u64, u32) {
        (self.latest.unwrap_or(0), self.history)
    }

    /// Decode from the wire pair. `latest == 0` with a nonzero `history` is
    /// malformed and rejected.
    pub fn from_wire(latest: u64, history: u32) -> Result<Self, AckError> {
        if latest == 0 {
            if history != 0 {
                return Err(AckError::NoneWithHistory);
            }
            return Ok(Self::new());
        }
        Ok(Self {
            latest: Some(latest),
            history,
        })
    }
}

/// An error decoding an [`AckField`] from the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AckError {
    /// `latest == 0` (none) paired with a nonzero history bitfield.
    #[error("ack window with no latest id carried a nonzero history")]
    NoneWithHistory,
}

/// Shift the ack history when a strictly-newer id arrives, folding the old
/// `latest` into the correct bit. Handles the `delta == 32` and `delta > 32`
/// edges without shift-by-width UB.
fn shift_history(history: u32, delta: u64) -> u32 {
    if delta == 0 {
        history
    } else if delta < ACK_HISTORY_BITS as u64 {
        // Old latest lands at bit (delta - 1).
        (history << delta) | (1u32 << (delta as u32 - 1))
    } else if delta == ACK_HISTORY_BITS as u64 {
        // Old latest lands exactly at the top bit; everything else falls off.
        1u32 << (ACK_HISTORY_BITS - 1)
    } else {
        // Old latest falls entirely out of the window.
        0
    }
}

/// Single-ordered baseline state for one `(object, receiver)` pair. The server
/// records the tokens it has issued and advances `last_acked` only to a token it
/// actually issued and that is strictly newer than the current one.
#[derive(Debug, Clone, Default)]
pub struct BaselineTracker {
    last_acked: Option<BaselineId>,
    outstanding: BTreeSet<u64>,
}

impl BaselineTracker {
    /// A fresh tracker with no acked or outstanding tokens.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that the server issued `id` to this receiver (the delta was sent).
    pub fn issue(&mut self, id: BaselineId) {
        self.outstanding.insert(id.get());
    }

    /// The last token this receiver has provably acknowledged.
    #[must_use]
    pub fn last_acked(&self) -> Option<BaselineId> {
        self.last_acked
    }

    /// Whether `id` is still outstanding (issued, not yet acked/superseded).
    #[must_use]
    pub fn is_outstanding(&self, id: BaselineId) -> bool {
        self.outstanding.contains(&id.get())
    }

    /// Apply an ack window. Advances `last_acked` to the newest id that is both
    /// currently outstanding and strictly newer than the current `last_acked`;
    /// a stale/unknown/forged id can never regress or advance the baseline.
    /// Returns the new `last_acked` if it advanced.
    pub fn apply_ack(&mut self, ack: &AckField) -> Option<BaselineId> {
        let floor = self.last_acked.map_or(0, BaselineId::get);
        let mut best = floor;
        for id in ack.iter_acked() {
            if id > best && self.outstanding.contains(&id) {
                best = id;
            }
        }
        if best > floor {
            let advanced = BaselineId::new(best);
            self.last_acked = advanced;
            // Prune everything at or below the new baseline; those are settled.
            self.outstanding = self.outstanding.split_off(&(best + 1));
            advanced
        } else {
            None
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn allocator_is_monotonic_and_nonzero() {
        let mut a = BaselineAllocator::new();
        let first = a.allocate().unwrap();
        let second = a.allocate().unwrap();
        assert_eq!(first.get(), 1);
        assert_eq!(second.get(), 2);
        assert!(second > first);
    }

    #[test]
    fn allocator_refuses_to_wrap() {
        let mut a = BaselineAllocator::new();
        a.next = u64::MAX;
        let last = a.allocate().unwrap();
        assert_eq!(last.get(), u64::MAX);
        assert_eq!(a.allocate(), Err(BaselineError::Exhausted));
    }

    #[test]
    fn baseline_id_rejects_zero() {
        assert!(BaselineId::new(0).is_none());
        assert!(BaselineId::new(1).is_some());
    }

    #[test]
    fn ack_window_basic_and_history() {
        let mut w = AckField::new();
        w.ack(10);
        assert_eq!(w.latest(), Some(10));
        assert!(w.is_acked(10));
        // Ack an older id inside the window.
        w.ack(8);
        assert!(w.is_acked(8)); // offset 2 => bit 1
        assert!(!w.is_acked(9));
    }

    #[test]
    fn ack_window_forward_shift_sets_old_latest_bit() {
        let mut w = AckField::new();
        w.ack(5);
        w.ack(6); // delta 1 => old latest (5) at bit 0
        assert!(w.is_acked(6));
        assert!(w.is_acked(5));
        assert!(!w.is_acked(4));
    }

    #[test]
    fn ack_window_delta_edges_31_32_33() {
        // delta 31.
        let mut w = AckField::new();
        w.ack(1);
        w.ack(1 + 31);
        assert!(w.is_acked(32));
        assert!(w.is_acked(1)); // offset 31 => bit 30
        // delta 32: old latest lands at top bit, still in window (offset 32).
        let mut w = AckField::new();
        w.ack(1);
        w.ack(1 + 32);
        assert!(w.is_acked(33));
        assert!(w.is_acked(1)); // offset 32 => bit 31
        assert!(!w.is_acked(2));
        // delta 33: old latest falls out of the 32-wide window.
        let mut w = AckField::new();
        w.ack(1);
        w.ack(1 + 33);
        assert!(w.is_acked(34));
        assert!(!w.is_acked(1));
    }

    #[test]
    fn ack_window_wire_round_trip_and_none_guard() {
        let mut w = AckField::new();
        w.ack(100);
        w.ack(98);
        let (latest, history) = w.to_wire();
        let back = AckField::from_wire(latest, history).unwrap();
        assert_eq!(back, w);
        // None with history is malformed.
        assert_eq!(AckField::from_wire(0, 0).unwrap(), AckField::new());
        assert_eq!(AckField::from_wire(0, 1), Err(AckError::NoneWithHistory));
    }

    #[test]
    fn tracker_advances_only_to_outstanding_newer() {
        let mut alloc = BaselineAllocator::new();
        let b1 = alloc.allocate().unwrap();
        let b2 = alloc.allocate().unwrap();
        let b3 = alloc.allocate().unwrap();
        let mut t = BaselineTracker::new();
        t.issue(b1);
        t.issue(b2);
        t.issue(b3);

        let mut ack = AckField::new();
        ack.ack(b2.get());
        assert_eq!(t.apply_ack(&ack), Some(b2));
        assert_eq!(t.last_acked(), Some(b2));
        assert!(!t.is_outstanding(b1)); // pruned
        assert!(!t.is_outstanding(b2)); // pruned
        assert!(t.is_outstanding(b3));
    }

    #[test]
    fn tracker_ignores_stale_and_forged_acks() {
        let mut alloc = BaselineAllocator::new();
        let b1 = alloc.allocate().unwrap();
        let b2 = alloc.allocate().unwrap();
        let mut t = BaselineTracker::new();
        t.issue(b1);
        t.issue(b2);

        let mut ack = AckField::new();
        ack.ack(b2.get());
        t.apply_ack(&ack);
        assert_eq!(t.last_acked(), Some(b2));

        // A stale ack for b1 cannot regress.
        let mut stale = AckField::new();
        stale.ack(b1.get());
        assert_eq!(t.apply_ack(&stale), None);
        assert_eq!(t.last_acked(), Some(b2));

        // A forged ack for a never-issued id (b2+50) is ignored.
        let mut forged = AckField::new();
        forged.ack(b2.get() + 50);
        assert_eq!(t.apply_ack(&forged), None);
        assert_eq!(t.last_acked(), Some(b2));
    }

    #[test]
    fn tracker_recovers_via_history_when_latest_ack_is_lost() {
        // The receiver acked b3 but that ack's `latest` names an id we never
        // issued (forged/garbage); the history bit for the real b3 still lands.
        let mut alloc = BaselineAllocator::new();
        let b1 = alloc.allocate().unwrap();
        let b2 = alloc.allocate().unwrap();
        let b3 = alloc.allocate().unwrap();
        let mut t = BaselineTracker::new();
        t.issue(b1);
        t.issue(b2);
        t.issue(b3);

        let mut ack = AckField::new();
        ack.ack(b3.get());
        ack.ack(b2.get());
        // Only outstanding, real ids advance; newest wins.
        assert_eq!(t.apply_ack(&ack), Some(b3));
    }
}
