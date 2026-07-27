//! Deterministic time primitives for domain contracts.
//!
//! Lifecycle logic (session expiry, refresh windows, ownership leases) must be
//! unit-testable without touching the wall clock. To make that possible, domain
//! types and service requests never call [`std::time::SystemTime::now`]
//! internally: they accept an explicit `now: TimestampMillis`. The [`Clock`]
//! trait is the single injectable seam where a concrete "current time" is read,
//! so production wiring can use [`SystemClock`] while tests use [`FixedClock`]
//! or pass fixed timestamps directly.
//!
//! Times are Unix epoch milliseconds. Millisecond resolution is enough for
//! session TTLs and lease windows and keeps the type a plain `u64`.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};

/// A point in time as Unix epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimestampMillis(u64);

impl TimestampMillis {
    /// Construct a timestamp from Unix epoch milliseconds.
    #[must_use]
    pub const fn from_unix_millis(value: u64) -> Self {
        Self(value)
    }

    /// The raw Unix epoch milliseconds.
    #[must_use]
    pub const fn unix_millis(self) -> u64 {
        self.0
    }

    /// Add a duration, rejecting overflow.
    ///
    /// # Errors
    /// Returns an [`ErrorCategory::Internal`](crate::error::ErrorCategory::Internal)
    /// error if the addition overflows `u64` milliseconds.
    pub fn checked_add(self, duration: DurationMillis) -> AppResult<Self> {
        self.0
            .checked_add(duration.0)
            .map(Self)
            .ok_or_else(|| AppError::internal("timestamp addition overflowed"))
    }

    /// Subtract a duration, rejecting underflow.
    ///
    /// # Errors
    /// Returns an [`ErrorCategory::Internal`](crate::error::ErrorCategory::Internal)
    /// error if the subtraction underflows below the Unix epoch.
    pub fn checked_sub(self, duration: DurationMillis) -> AppResult<Self> {
        self.0
            .checked_sub(duration.0)
            .map(Self)
            .ok_or_else(|| AppError::internal("timestamp subtraction underflowed"))
    }
}

/// A non-negative duration in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DurationMillis(u64);

impl DurationMillis {
    /// Construct a duration from milliseconds (zero allowed).
    #[must_use]
    pub const fn from_millis(value: u64) -> Self {
        Self(value)
    }

    /// Construct a strictly positive duration, rejecting zero.
    ///
    /// Session and refresh TTLs must be positive; a zero TTL would produce a
    /// session that is already expired at issue time.
    ///
    /// # Errors
    /// Returns an [`ErrorCategory::Validation`](crate::error::ErrorCategory::Validation)
    /// error naming `field` when `value` is zero.
    pub fn nonzero(value: u64, field: &'static str) -> AppResult<Self> {
        if value == 0 {
            return Err(AppError::validation(format!(
                "{field} must be greater than zero"
            )));
        }
        Ok(Self(value))
    }

    /// The raw milliseconds.
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }
}

/// The injectable source of the current time.
///
/// This is the only place production code reads "now"; everything downstream
/// takes an explicit [`TimestampMillis`] so it stays deterministic.
pub trait Clock {
    /// The current time.
    fn now(&self) -> TimestampMillis;
}

/// A [`Clock`] backed by the system wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> TimestampMillis {
        // A clock set before the Unix epoch is treated as the epoch rather than
        // panicking; this seam is intentionally infallible for callers.
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        TimestampMillis::from_unix_millis(millis)
    }
}

/// A [`Clock`] that always returns a fixed time, for deterministic tests.
#[derive(Debug, Clone, Copy)]
pub struct FixedClock(TimestampMillis);

impl FixedClock {
    /// Construct a clock pinned to `at`.
    #[must_use]
    pub const fn new(at: TimestampMillis) -> Self {
        Self(at)
    }
}

impl Clock for FixedClock {
    fn now(&self) -> TimestampMillis {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_round_trips_millis() {
        let ts = TimestampMillis::from_unix_millis(1_700_000_000_000);
        assert_eq!(ts.unix_millis(), 1_700_000_000_000);
    }

    #[test]
    fn checked_add_and_sub_detect_bounds() {
        let ts = TimestampMillis::from_unix_millis(10);
        let five = DurationMillis::from_millis(5);
        assert_eq!(ts.checked_add(five).expect("no overflow").unix_millis(), 15);
        assert_eq!(ts.checked_sub(five).expect("no underflow").unix_millis(), 5);

        let max = TimestampMillis::from_unix_millis(u64::MAX);
        assert!(max.checked_add(DurationMillis::from_millis(1)).is_err());
        assert!(
            TimestampMillis::from_unix_millis(0)
                .checked_sub(DurationMillis::from_millis(1))
                .is_err()
        );
    }

    #[test]
    fn nonzero_duration_rejects_zero() {
        assert!(DurationMillis::nonzero(0, "session_ttl").is_err());
        assert_eq!(
            DurationMillis::nonzero(1, "session_ttl")
                .expect("positive")
                .as_millis(),
            1
        );
    }

    #[test]
    fn fixed_clock_returns_pinned_time() {
        let at = TimestampMillis::from_unix_millis(42);
        assert_eq!(FixedClock::new(at).now(), at);
    }

    #[test]
    fn system_clock_is_after_epoch() {
        // Sanity: the system clock produces a plausible, post-epoch value.
        assert!(SystemClock.now().unix_millis() > 1_600_000_000_000);
    }
}
