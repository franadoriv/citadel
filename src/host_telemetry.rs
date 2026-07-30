//! Cross-platform host telemetry for the operator status surface.
//!
//! The collector intentionally owns one [`sysinfo::System`] instance for the
//! life of the application. CPU usage is a delta between refreshes, so creating
//! a system probe for each HTTP request would report misleading values. The
//! dashboard refreshes every five seconds; the small, targeted refreshes here
//! are therefore suitable for the live operator view and are not a metrics
//! history or alerting system.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde::Serialize;
use sysinfo::{
    Disks, MINIMUM_CPU_UPDATE_INTERVAL, ProcessRefreshKind, ProcessesToUpdate, System,
    get_current_pid,
};

/// Bound OS polling even when the public status endpoint is requested more
/// frequently than the dashboard's five-second refresh cadence.
const SNAPSHOT_MINIMUM_INTERVAL: Duration = Duration::from_secs(1);

/// Host resource readings exposed by `/status`.
///
/// A missing value means the operating system did not make that reading
/// available. It deliberately differs from zero: zero remains a valid CPU
/// utilization and a valid amount of used disk space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HostTelemetrySnapshot {
    /// Host-wide CPU utilization, rounded to the nearest whole percent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_usage_percent: Option<u8>,
    /// Host physical-memory capacity and pressure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryTelemetrySnapshot>,
    /// Aggregate capacity across mounted filesystems reported by the OS. This
    /// is not a single physical-disk capacity: mount and overlay layouts can
    /// reference shared backing storage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<StorageTelemetrySnapshot>,
}

impl HostTelemetrySnapshot {
    /// A safe initial value used while the first OS sample is being collected.
    #[must_use]
    pub const fn unavailable() -> Self {
        Self {
            cpu_usage_percent: None,
            memory: None,
            storage: None,
        }
    }
}

/// Host physical-memory capacity in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MemoryTelemetrySnapshot {
    /// Total physical memory visible to the host.
    pub total_bytes: u64,
    /// Memory currently available for new work.
    pub available_bytes: u64,
    /// `total_bytes - available_bytes`; this is the host pressure value shown
    /// in the dashboard.
    pub used_bytes: u64,
    /// Resident memory attributed to this Citadel process, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_used_bytes: Option<u64>,
}

/// Aggregate mounted-filesystem capacity in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StorageTelemetrySnapshot {
    /// Sum of capacity across mounted filesystems with a nonzero size. This
    /// can include shared backing storage more than once on overlay or bind
    /// mount layouts.
    pub total_bytes: u64,
    /// Sum of space currently available to the process across those filesystems.
    pub available_bytes: u64,
    /// `total_bytes - available_bytes`.
    pub used_bytes: u64,
    /// Number of mounted filesystems included in this aggregate.
    pub mounted_filesystems: u32,
}

/// Shared, non-blocking access point for host telemetry.
///
/// Refreshes run on Tokio's blocking pool because disk enumeration and process
/// inspection can block on an operating-system call. A concurrent console
/// request returns the last published sample while one refresh is in flight,
/// keeping the asynchronous HTTP workers free to service gameplay traffic.
pub struct HostTelemetryService {
    collector: Mutex<HostTelemetryCollector>,
    cached: RwLock<HostTelemetrySnapshot>,
    refresh_in_progress: AtomicBool,
}

impl HostTelemetryService {
    /// Create a shared telemetry service with an initially unavailable sample.
    #[must_use]
    pub fn new() -> Self {
        Self {
            collector: Mutex::new(HostTelemetryCollector::new()),
            cached: RwLock::new(HostTelemetrySnapshot::unavailable()),
            refresh_in_progress: AtomicBool::new(false),
        }
    }

    /// Return the latest sample and, when idle, refresh it outside HTTP workers.
    pub async fn snapshot(self: &Arc<Self>) -> HostTelemetrySnapshot {
        let fallback = self.cached_snapshot();
        if self
            .refresh_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return fallback;
        }

        let service = Arc::clone(self);
        let refreshed = tokio::task::spawn_blocking(move || {
            let snapshot = match service.collector.lock() {
                Ok(mut collector) => collector.snapshot(),
                // Preserve availability if a collector operation panicked.
                Err(poisoned) => poisoned.into_inner().snapshot(),
            };
            match service.cached.write() {
                Ok(mut cached) => *cached = snapshot.clone(),
                Err(poisoned) => *poisoned.into_inner() = snapshot.clone(),
            }
            snapshot
        })
        .await;

        self.refresh_in_progress.store(false, Ordering::Release);
        refreshed.unwrap_or(fallback)
    }

    fn cached_snapshot(&self) -> HostTelemetrySnapshot {
        match self.cached.read() {
            Ok(snapshot) => snapshot.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

impl Default for HostTelemetryService {
    fn default() -> Self {
        Self::new()
    }
}

/// Stateful, low-overhead collector for [`HostTelemetrySnapshot`].
///
/// This type is kept behind [`HostTelemetryService`] and is not exposed
/// directly because all callers should share its CPU baseline.
pub struct HostTelemetryCollector {
    system: System,
    disks: Disks,
    current_pid: Option<sysinfo::Pid>,
    last_cpu_refresh: Instant,
    last_resource_refresh: Option<Instant>,
    last_snapshot: Option<HostTelemetrySnapshot>,
}

impl HostTelemetryCollector {
    /// Create the reusable OS collectors and establish the CPU baseline.
    #[must_use]
    pub fn new() -> Self {
        let mut system = System::new();
        system.refresh_cpu_usage();
        system.refresh_memory();

        let current_pid = get_current_pid().ok();
        if let Some(pid) = current_pid {
            let pids = [pid];
            system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&pids),
                true,
                ProcessRefreshKind::nothing().with_memory(),
            );
        }

        Self {
            system,
            disks: Disks::new_with_refreshed_list(),
            current_pid,
            last_cpu_refresh: Instant::now(),
            last_resource_refresh: None,
            last_snapshot: None,
        }
    }

    /// Refresh only the resource facts the dashboard needs.
    #[must_use]
    pub fn snapshot(&mut self) -> HostTelemetrySnapshot {
        if let (Some(last_refresh), Some(snapshot)) =
            (self.last_resource_refresh, self.last_snapshot.as_ref())
            && last_refresh.elapsed() < SNAPSHOT_MINIMUM_INTERVAL
        {
            return snapshot.clone();
        }

        self.system.refresh_memory();
        self.refresh_current_process();
        self.disks.refresh(true);

        let cpu_usage_percent = if self.last_cpu_refresh.elapsed() >= MINIMUM_CPU_UPDATE_INTERVAL {
            self.system.refresh_cpu_usage();
            self.last_cpu_refresh = Instant::now();
            cpu_percent(self.system.global_cpu_usage())
        } else {
            None
        };

        let snapshot = HostTelemetrySnapshot {
            cpu_usage_percent,
            memory: MemoryTelemetrySnapshot::from_system(
                self.system.total_memory(),
                self.system.available_memory(),
                self.current_process_memory(),
            ),
            storage: aggregate_storage(
                self.disks
                    .iter()
                    .map(|disk| (disk.total_space(), disk.available_space())),
            ),
        };
        self.last_resource_refresh = Some(Instant::now());
        self.last_snapshot = Some(snapshot.clone());
        snapshot
    }

    fn refresh_current_process(&mut self) {
        if let Some(pid) = self.current_pid {
            let pids = [pid];
            self.system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&pids),
                true,
                ProcessRefreshKind::nothing().with_memory(),
            );
        }
    }

    fn current_process_memory(&self) -> Option<u64> {
        self.current_pid
            .and_then(|pid| self.system.process(pid).map(sysinfo::Process::memory))
    }
}

impl Default for HostTelemetryCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryTelemetrySnapshot {
    fn from_system(
        total_bytes: u64,
        available_bytes: u64,
        process_used_bytes: Option<u64>,
    ) -> Option<Self> {
        (total_bytes > 0).then(|| Self {
            total_bytes,
            available_bytes: available_bytes.min(total_bytes),
            used_bytes: total_bytes.saturating_sub(available_bytes),
            process_used_bytes,
        })
    }
}

fn cpu_percent(usage: f32) -> Option<u8> {
    usage
        .is_finite()
        .then(|| usage.clamp(0.0, 100.0).round() as u8)
}

fn aggregate_storage<I>(spaces: I) -> Option<StorageTelemetrySnapshot>
where
    I: IntoIterator<Item = (u64, u64)>,
{
    let mut total_bytes = 0_u64;
    let mut available_bytes = 0_u64;
    let mut mounted_filesystems = 0_u32;

    for (total, available) in spaces.into_iter().filter(|(total, _)| *total > 0) {
        total_bytes = total_bytes.saturating_add(total);
        available_bytes = available_bytes.saturating_add(available.min(total));
        mounted_filesystems = mounted_filesystems.saturating_add(1);
    }

    (mounted_filesystems > 0).then(|| StorageTelemetrySnapshot {
        total_bytes,
        available_bytes: available_bytes.min(total_bytes),
        used_bytes: total_bytes.saturating_sub(available_bytes),
        mounted_filesystems,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_percent_clamps_and_rounds_a_valid_reading() {
        assert_eq!(cpu_percent(-1.0), Some(0));
        assert_eq!(cpu_percent(12.5), Some(13));
        assert_eq!(cpu_percent(101.0), Some(100));
        assert_eq!(cpu_percent(f32::NAN), None);
    }

    #[test]
    fn memory_snapshot_uses_available_memory_as_pressure_boundary() {
        let snapshot = MemoryTelemetrySnapshot::from_system(100, 35, Some(9))
            .expect("nonzero total memory is reportable");
        assert_eq!(snapshot.used_bytes, 65);
        assert_eq!(snapshot.process_used_bytes, Some(9));
        assert!(MemoryTelemetrySnapshot::from_system(0, 0, None).is_none());
    }

    #[test]
    fn storage_aggregate_ignores_zero_capacity_and_clamps_available_space() {
        let snapshot = aggregate_storage([(0, 0), (100, 35), (50, 70)])
            .expect("nonzero filesystems are reportable");
        assert_eq!(snapshot.total_bytes, 150);
        assert_eq!(snapshot.available_bytes, 85);
        assert_eq!(snapshot.used_bytes, 65);
        assert_eq!(snapshot.mounted_filesystems, 2);
    }

    #[test]
    fn storage_aggregate_is_absent_without_filesystems() {
        assert!(aggregate_storage([(0, 0)]).is_none());
    }

    #[test]
    fn unavailable_snapshot_distinguishes_pending_collection_from_zero_use() {
        let snapshot = HostTelemetrySnapshot::unavailable();
        assert!(snapshot.cpu_usage_percent.is_none());
        assert!(snapshot.memory.is_none());
        assert!(snapshot.storage.is_none());
    }
}
