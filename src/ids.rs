//! Sortable, collision-free durable identifiers for per-node log streams.
//!
//! An id is `<prefix>-<13 hex millis><4 hex node salt><12 hex boot sequence>`.
//! Lexicographic order equals chronological order, so keyset paging rides the
//! primary key with no secondary sort column.
//!
//! The salt is what makes the id safe as a `PRIMARY KEY`: a bare per-process
//! counter restarts at zero on every boot, so two runs of the same node — or
//! two nodes minting in the same millisecond — would collide on the second run.

use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};

/// Hex digits of the millisecond component. 13 digits address 2^52 ms, which
/// runs past the year 144,000.
const MILLIS_HEX: u32 = 13;
/// Hex digits of the per-boot sequence.
const SEQUENCE_HEX: u32 = 12;

const MILLIS_MASK: u64 = (1 << (MILLIS_HEX * 4)) - 1;
const SEQUENCE_MASK: u64 = (1 << (SEQUENCE_HEX * 4)) - 1;

/// Total id length for the `mt1-` / `ml1-` / `au1-` family.
pub const SHORT_PREFIX_ID_LEN: usize = 33;
/// Total id length for the `ats1-` family.
pub const SLICE_REPORT_ID_LEN: usize = 34;

/// The identity a single running node mints durable ids under.
#[derive(Debug)]
pub struct NodeIdentity {
    node_id: String,
    boot_id: String,
    salt: u16,
    seq: AtomicU64,
}

impl NodeIdentity {
    /// Mint a fresh boot identity. `boot_id` is `bt1-<uuid simple>`; `salt` is
    /// the first 16 bits of `sha256(node_id ‖ boot_id)`.
    #[must_use]
    pub fn new(node_id: impl Into<String>) -> Self {
        let node_id = node_id.into();
        let boot_id = format!("bt1-{}", uuid::Uuid::new_v4().simple());
        let mut hasher = Sha256::new();
        hasher.update(node_id.as_bytes());
        hasher.update(boot_id.as_bytes());
        let digest = hasher.finalize();
        let salt = u16::from_be_bytes([digest[0], digest[1]]);
        Self {
            node_id,
            boot_id,
            salt,
            seq: AtomicU64::new(0),
        }
    }

    /// The configured node id this identity mints under.
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// The identifier of this process run. A restart mints a new one.
    #[must_use]
    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    /// The 16-bit node/boot salt embedded in every minted id.
    ///
    /// Exposed for subsystems that mint their own ids in the same shape without
    /// depending on this module — the authoritative telemetry slice service is
    /// compiled standalone by one of its tests and cannot import from here.
    #[must_use]
    pub fn salt(&self) -> u16 {
        self.salt
    }

    /// Mint `<prefix><13 hex millis><4 hex salt><12 hex sequence>`.
    ///
    /// `at_ms` is truncated to 52 bits and the sequence to 48, so the result is
    /// always exactly `prefix.len() + 29` bytes wide.
    #[must_use]
    pub fn mint(&self, prefix: &str, at_ms: u64) -> String {
        let sequence = self.seq.fetch_add(1, Ordering::Relaxed) & SEQUENCE_MASK;
        format!(
            "{prefix}{:013x}{:04x}{:012x}",
            at_ms & MILLIS_MASK,
            self.salt,
            sequence
        )
    }
}

/// Whether `value` is a well-formed id of this family.
///
/// `len` is the total id length including the prefix. Callers validate a cursor
/// or a path parameter with this before it reaches a query, so a malformed id
/// is a `400`/`404` decision rather than a database round trip.
#[must_use]
pub fn valid_id(value: &str, prefix: &str, len: usize) -> bool {
    value.len() == len
        && value.starts_with(prefix)
        && value[prefix.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
}

/// Narrow a domain `u64` to SQL `i64`.
///
/// # Errors
/// Returns an internal error when the value exceeds `i64::MAX`; SQL has no
/// unsigned integer and a silent wrap would corrupt an ordering column.
pub fn sql_i64(value: u64, what: &'static str) -> AppResult<i64> {
    i64::try_from(value).map_err(|_| AppError::internal(format!("{what} out of range")))
}

/// Widen a SQL `i64` back to a domain `u64`.
///
/// # Errors
/// Returns an internal error for a negative value, which no writer of these
/// tables can produce and therefore signals a corrupted row.
pub fn sql_u64(value: i64, what: &'static str) -> AppResult<u64> {
    u64::try_from(value).map_err(|_| AppError::internal(format!("{what} is negative")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minted_ids_are_fixed_width_and_lexicographically_chronological() {
        let identity = NodeIdentity::new("node-a");
        let early = identity.mint("mt1-", 1_700_000_000_000);
        let late = identity.mint("mt1-", 1_700_000_001_000);
        assert_eq!(early.len(), SHORT_PREFIX_ID_LEN);
        assert_eq!(late.len(), SHORT_PREFIX_ID_LEN);
        assert!(early < late);
        assert!(valid_id(&early, "mt1-", SHORT_PREFIX_ID_LEN));
        assert!(valid_id(&late, "mt1-", SHORT_PREFIX_ID_LEN));
    }

    #[test]
    fn the_slice_report_prefix_widens_the_id_by_one() {
        let identity = NodeIdentity::new("node-a");
        let report = identity.mint("ats1-", 1);
        assert_eq!(report.len(), SLICE_REPORT_ID_LEN);
        assert!(valid_id(&report, "ats1-", SLICE_REPORT_ID_LEN));
    }

    #[test]
    fn the_sequence_separates_ids_minted_in_one_millisecond() {
        let identity = NodeIdentity::new("node-a");
        let first = identity.mint("ml1-", 42);
        let second = identity.mint("ml1-", 42);
        assert_ne!(first, second);
        assert!(first < second);
    }

    #[test]
    fn two_boots_of_one_node_never_mint_the_same_id() {
        // This is the defect a bare per-boot counter has as a PRIMARY KEY: the
        // sequence restarts at zero, so only the salt separates the runs.
        let first = NodeIdentity::new("node-a");
        let second = NodeIdentity::new("node-a");
        assert_ne!(first.boot_id(), second.boot_id());
        assert_ne!(first.salt(), second.salt());
        assert_ne!(first.mint("mt1-", 7), second.mint("mt1-", 7));
    }

    #[test]
    fn validation_rejects_wrong_length_prefix_and_non_hex() {
        assert!(!valid_id("mt1-short", "mt1-", SHORT_PREFIX_ID_LEN));
        assert!(!valid_id(&"x".repeat(33), "mt1-", SHORT_PREFIX_ID_LEN));
        let mut malformed = NodeIdentity::new("node-a").mint("mt1-", 1);
        malformed.replace_range(4..5, "z");
        assert!(!valid_id(&malformed, "mt1-", SHORT_PREFIX_ID_LEN));
    }

    #[test]
    fn sql_narrowing_rejects_values_sql_cannot_hold() {
        assert_eq!(sql_i64(5, "room id").expect("in range"), 5);
        assert!(sql_i64(u64::MAX, "room id").is_err());
        assert_eq!(sql_u64(5, "room id").expect("in range"), 5);
        assert!(sql_u64(-1, "room id").is_err());
    }
}
