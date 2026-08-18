//! Bounded, node-local custom metrics for trusted game logic.
//!
//! Script adapters must route metric calls through this Rust-owned registry. It
//! admits only stable names and a fixed number of series; it deliberately has no
//! arbitrary labels, so game data cannot create unbounded Prometheus cardinality.

use std::collections::BTreeMap;
use std::sync::Mutex;

/// Maximum number of custom metric names per node.
pub const MAX_CUSTOM_METRICS: usize = 128;
const MAX_METRIC_NAME_LEN: usize = 48;

#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeMetricSnapshot {
    Counter {
        name: String,
        value: u64,
    },
    Gauge {
        name: String,
        value: f64,
    },
    Timer {
        name: String,
        count: u64,
        sum_seconds: f64,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RuntimeMetricError {
    #[error("runtime metric name is invalid")]
    InvalidName,
    #[error("runtime metric limit reached")]
    LimitReached,
    #[error("runtime metric type cannot change")]
    TypeMismatch,
    #[error("runtime metric value must be finite")]
    NonFiniteValue,
}

#[derive(Debug)]
enum RuntimeMetric {
    Counter(u64),
    Gauge(f64),
    Timer { count: u64, sum_seconds: f64 },
}

/// Rust-owned runtime custom-metrics registry.
#[derive(Debug, Default)]
pub struct RuntimeMetrics {
    entries: Mutex<BTreeMap<String, RuntimeMetric>>,
}

impl RuntimeMetrics {
    /// Add `value` to a named monotonic counter.
    pub fn counter(&self, name: &str, value: u64) -> Result<(), RuntimeMetricError> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match entries.get_mut(name) {
            Some(RuntimeMetric::Counter(current)) => *current = current.saturating_add(value),
            Some(_) => return Err(RuntimeMetricError::TypeMismatch),
            None => {
                Self::admit(&entries, name)?;
                entries.insert(name.to_owned(), RuntimeMetric::Counter(value));
            }
        }
        Ok(())
    }

    /// Set a named instantaneous gauge.
    pub fn gauge(&self, name: &str, value: f64) -> Result<(), RuntimeMetricError> {
        if !value.is_finite() {
            return Err(RuntimeMetricError::NonFiniteValue);
        }
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match entries.get_mut(name) {
            Some(RuntimeMetric::Gauge(current)) => *current = value,
            Some(_) => return Err(RuntimeMetricError::TypeMismatch),
            None => {
                Self::admit(&entries, name)?;
                entries.insert(name.to_owned(), RuntimeMetric::Gauge(value));
            }
        }
        Ok(())
    }

    /// Observe a non-negative duration in seconds.
    pub fn timer(&self, name: &str, seconds: f64) -> Result<(), RuntimeMetricError> {
        if !seconds.is_finite() || seconds < 0.0 {
            return Err(RuntimeMetricError::NonFiniteValue);
        }
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match entries.get_mut(name) {
            Some(RuntimeMetric::Timer { count, sum_seconds }) => {
                *count = count.saturating_add(1);
                *sum_seconds += seconds;
            }
            Some(_) => return Err(RuntimeMetricError::TypeMismatch),
            None => {
                Self::admit(&entries, name)?;
                entries.insert(
                    name.to_owned(),
                    RuntimeMetric::Timer {
                        count: 1,
                        sum_seconds: seconds,
                    },
                );
            }
        }
        Ok(())
    }

    /// Return stable, sorted snapshots for Prometheus rendering.
    #[must_use]
    pub fn snapshot(&self) -> Vec<RuntimeMetricSnapshot> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(name, metric)| match metric {
                RuntimeMetric::Counter(value) => RuntimeMetricSnapshot::Counter {
                    name: name.clone(),
                    value: *value,
                },
                RuntimeMetric::Gauge(value) => RuntimeMetricSnapshot::Gauge {
                    name: name.clone(),
                    value: *value,
                },
                RuntimeMetric::Timer { count, sum_seconds } => RuntimeMetricSnapshot::Timer {
                    name: name.clone(),
                    count: *count,
                    sum_seconds: *sum_seconds,
                },
            })
            .collect()
    }

    fn admit(
        entries: &BTreeMap<String, RuntimeMetric>,
        name: &str,
    ) -> Result<(), RuntimeMetricError> {
        if !is_valid_runtime_metric_name(name) {
            return Err(RuntimeMetricError::InvalidName);
        }
        if entries.len() >= MAX_CUSTOM_METRICS {
            return Err(RuntimeMetricError::LimitReached);
        }
        Ok(())
    }
}

/// Allow only a concise, lower-case game-defined name without labels or IDs.
#[must_use]
pub fn is_valid_runtime_metric_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_METRIC_NAME_LEN
        && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        && !name.ends_with('_')
}
