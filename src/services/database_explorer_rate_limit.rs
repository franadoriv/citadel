//! Per-operator admission control for the read-only database explorer.
//!
//! The explorer can issue relatively expensive metadata and row reads. Console
//! bearer authentication is still the authorization boundary; this small
//! process-local limiter bounds accidental refresh loops and a stolen operator
//! token without putting raw filter values or database identifiers in state.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(60);
const LIMIT: u32 = 60;

#[derive(Debug, Default)]
pub struct DatabaseExplorerRateLimiter {
    entries: Mutex<HashMap<String, Window>>,
}

#[derive(Debug, Clone, Copy)]
struct Window {
    started: Instant,
    used: u32,
}

impl DatabaseExplorerRateLimiter {
    /// Admit one request for an authenticated operator, or return the whole
    /// number of seconds until that operator's fixed window resets.
    pub fn admit(&self, operator: &str) -> Result<(), u64> {
        let now = Instant::now();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|_, window| now.duration_since(window.started) < WINDOW);
        let window = entries.entry(operator.to_owned()).or_insert(Window {
            started: now,
            used: 0,
        });
        if window.used >= LIMIT {
            let remaining = WINDOW.saturating_sub(now.duration_since(window.started));
            return Err(remaining.as_secs().max(1));
        }
        window.used += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_each_operator_independently() {
        let limiter = DatabaseExplorerRateLimiter::default();
        for _ in 0..LIMIT {
            assert_eq!(limiter.admit("viewer"), Ok(()));
        }
        assert!(limiter.admit("viewer").is_err());
        assert_eq!(limiter.admit("admin"), Ok(()));
    }
}
