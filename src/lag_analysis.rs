//! Versioned, bounded analysis for private lag-capture artifacts.
//!
//! The upload service deliberately treats a capture as opaque bytes.  This
//! module is the separate, fail-closed consumer of those bytes: it accepts only
//! the exact CLAG v1 representation emitted by the supported SDK recorder and
//! produces compact, derived observations.  It does not infer one-way latency,
//! RTT, path asymmetry, or network packet loss; packet identifiers only yield
//! observed gaps, duplicates, and arrival reordering.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::time::TimestampMillis;
use crate::{lag_diagnostics::LagDiagnosticsService, lag_diagnostics::PrivateAnalysisArtifact};

/// Decoder version recorded with every report.
pub const CLAG_DECODER_VERSION: u16 = 1;
/// Analyzer version recorded with every report.
pub const LAG_ANALYZER_VERSION: u16 = 1;
/// The only supported CLAG header size.
pub const CLAG_HEADER_BYTES: usize = 128;
/// The only supported CLAG record size.
pub const CLAG_RECORD_BYTES: usize = 48;
/// Results with fewer intervals than this are explicitly insufficient.
pub const MIN_SPACING_SAMPLES: u32 = 3;
/// Upper bound for distinct participant/kind/direction/delivery/epoch groups
/// retained in one report. A hostile epoch per row must not grow Console JSON
/// with the artifact row count.
pub const MAX_REPORT_SUMMARIES: usize = 32;

const CLAG_HEADER_FLAGS: u16 = 0x0007;
const CLAG_FLAG_METADATA_ONLY: u16 = 0x0001;
const CLAG_FLAG_SERVER_CLOCK: u16 = 0x0004;
const DELIVERY_RELIABLE: u8 = 0x02;
const DELIVERY_DATAGRAM: u8 = 0x04;
const HISTOGRAM_EDGES_US: [u32; 8] = [
    1_000, 5_000, 10_000, 20_000, 33_333, 50_000, 100_000, 250_000,
];

/// A syntactically valid CLAG artifact rejected for a semantic decoder reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClagDecodeError {
    #[error("unsupported lag-capture format")]
    Unsupported,
    #[error("invalid lag-capture artifact")]
    Invalid,
}

/// A bounded server-clock correlation segment stored in CLAG v1's fixed header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockSegment {
    /// Local monotonic microseconds after recording began.
    pub elapsed_us: u32,
    /// Server UTC microseconds associated with this local instant.
    pub server_utc_us: u64,
    /// Half-delay uncertainty around the UTC mapping, in microseconds.
    pub uncertainty_us: u32,
}

/// Validated CLAG header metadata.  No raw paths or client identity appear.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClagHeader {
    /// Capture UUID encoded by the client; this must be checked against the
    /// server-side artifact manifest before a report is stored.
    pub capture_id: [u8; 16],
    /// The low 32 bits of the server-issued capture generation used by v1.
    pub generation: u32,
    pub record_count: u32,
    pub accepted_records: u64,
    pub overwritten_records: u64,
    pub skipped_filter_records: u64,
    pub skipped_malformed_records: u64,
    pub metadata_only: bool,
    pub server_clock_at_start_utc_us: Option<u64>,
    pub initial_clock_uncertainty_us: Option<u32>,
    pub clock_segments: Vec<ClockSegment>,
}

/// One metadata-only observation.  Payload bytes are never represented here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClagRecord {
    pub elapsed_us: u32,
    pub packet_kind: u16,
    pub direction: Direction,
    pub delivery: DeliveryMode,
    pub body_bytes: u32,
    pub packet_id: u32,
    pub base_packet_id: u32,
    pub server_tick: u32,
    pub tick_hz: u16,
    pub metadata_flags: u16,
    pub gameplay_epoch: u64,
}

/// A fully decoded v1 artifact.  It is bounded by the artifact's exact row
/// count and callers must apply an upload-size bound before constructing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedClag {
    pub header: ClagHeader,
    pub records: Vec<ClagRecord>,
}

/// Direction is a closed v1 enum, not a free-form client string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Inbound,
    Outbound,
}

/// Delivery mode is a closed v1 enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryMode {
    Reliable,
    Datagram,
}

/// Report lifecycle values intentionally distinguish absent/insufficient
/// evidence from a zero-valued metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LagReportStatus {
    NoAnalysis,
    Pending,
    NoData,
    Partial,
    Complete,
    Failed,
}

/// Quality attached to a compact metric group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricQuality {
    pub status: String,
    pub sample_count: u32,
    pub excluded_count: u32,
    pub overwritten_count: u64,
    pub malformed_count: u64,
    pub clock_uncertain: bool,
}

/// Fixed versioned histogram.  The final count represents values above the
/// final edge.  Units are always microseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixedHistogram {
    pub unit: String,
    pub edges_us: Vec<u32>,
    pub counts: Vec<u32>,
    pub overflow_count: u32,
}

/// Stable participant/kind/direction/delivery/epoch key used for one summary.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObservationKey {
    /// Opaque server-side participant handle, never an account name or IP.
    pub participant: String,
    pub packet_kind: u16,
    pub direction: Direction,
    pub delivery: DeliveryMode,
    pub gameplay_epoch: u64,
}

/// An analysis group exposes only derived timing and identifier observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LagObservationSummary {
    pub key: ObservationKey,
    /// A label intentionally avoiding latency terminology.
    pub metric_label: String,
    pub unit: String,
    pub quality: MetricQuality,
    pub mean_interarrival_us: Option<u64>,
    pub p50_interarrival_us: Option<u32>,
    pub p95_interarrival_us: Option<u32>,
    pub p99_interarrival_us: Option<u32>,
    pub histogram: FixedHistogram,
    /// Only available if the server-provided send rate is known and non-zero.
    pub cadence_residual_p95_us: Option<u32>,
    pub observed_id_gap: u64,
    pub duplicate_id: u32,
    pub arrival_reorder: u32,
}

/// A deliberately bounded timeline window.  It contains aggregate counts, not
/// individual packet rows, so it is safe for Console chart APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LagTimelineWindow {
    pub key: ObservationKey,
    pub start_elapsed_us: u32,
    pub end_elapsed_us: u32,
    pub sample_count: u32,
    pub mean_interarrival_us: Option<u64>,
    pub observed_id_gap: u64,
    pub duplicate_id: u32,
    pub arrival_reorder: u32,
}

/// An immutable report-derived row.  It deliberately excludes raw bytes,
/// filenames/paths, MIME, token/JTI, payloads, IP addresses, and user agents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LagReport {
    pub report_id: String,
    pub capture_id: String,
    pub generation: u64,
    pub artifact_digest_sha256: String,
    pub decoder_version: u16,
    pub analyzer_version: u16,
    pub options_hash: String,
    pub status: LagReportStatus,
    pub raw_available: bool,
    pub created_at: TimestampMillis,
    pub quality: MetricQuality,
    pub summaries: Vec<LagObservationSummary>,
    pub windows: Vec<LagTimelineWindow>,
    /// A newer options/analyzer run can identify its immutable predecessor.
    pub supersedes_report_id: Option<String>,
}

/// Compact report-only capture grouping used by the Console capture keyset.
/// It intentionally contains no artifact locator, raw availability, or raw
/// bytes, so it remains useful after private artifact deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LagReportCaptureOverview {
    pub capture_id: String,
    pub generation: u64,
    pub report_count: u32,
    pub latest_report_status: LagReportStatus,
    pub latest_report_created_at: TimestampMillis,
}

/// Server-known analysis policy.  This is canonically hashed into the job key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalysisOptions {
    /// Non-zero outbound send cadence for the selected stream, when known.
    pub send_rate_hz: Option<u16>,
    /// Maximum returned time windows; clamped to a small fixed upper bound.
    pub max_windows: u16,
}

impl Default for AnalysisOptions {
    fn default() -> Self {
        Self {
            send_rate_hz: None,
            max_windows: 64,
        }
    }
}

impl AnalysisOptions {
    /// A deterministic digest over a fully specified compact option shape.
    #[must_use]
    pub fn canonical_hash(self) -> String {
        let rate = self.send_rate_hz.unwrap_or(0);
        let windows = self.max_windows.clamp(1, 64);
        let canonical = format!("v1;send_rate_hz={rate};max_windows={windows}");
        hex_sha256(canonical.as_bytes())
    }
}

/// The idempotency identity for a worker request.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AnalysisIdentity {
    pub capture_id: String,
    pub generation: u64,
    pub artifact_digest_sha256: String,
    pub analyzer_version: u16,
    pub options_hash: String,
}

/// Bounded local report repository contract.  Durable adapters may implement
/// the same rules, while tests use the in-memory reference store.
pub trait LagReportRepository: Send + Sync {
    fn get_by_identity(&self, identity: &AnalysisIdentity) -> Option<LagReport>;
    /// Latest immutable predecessor for the same source artifact, regardless
    /// of analyzer/options identity. Used only to link a new regeneration;
    /// the predecessor is never mutated.
    fn latest_for_artifact(
        &self,
        capture_id: &str,
        generation: u64,
        artifact_digest_sha256: &str,
    ) -> Option<LagReport>;
    fn insert_immutable(&self, identity: AnalysisIdentity, report: LagReport) -> LagReport;
    /// Update only the current raw-retention projection. It must never rewrite
    /// the immutable derived report status or metrics.
    fn mark_raw_unavailable(&self, capture_id: &str, generation: u64, artifact_digest_sha256: &str);
    fn list(&self, after_report_id: Option<&str>, limit: usize) -> Vec<LagReport>;
}

/// In-memory reference implementation.  It models immutable/idempotent report
/// rows without retaining raw artifact data.
#[derive(Default)]
pub struct InMemoryLagReportRepository {
    reports: Mutex<BTreeMap<AnalysisIdentity, LagReport>>,
    /// Exact raw-source tombstones. Keeping this separate from an immutable
    /// report prevents a delete/retention race from being undone by a later
    /// worker insert for the same artifact identity.
    raw_unavailable: Mutex<BTreeSet<(String, u64, String)>>,
}

/// Result of asking the bounded worker for one analysis identity.  A caller
/// cannot distinguish a missing private artifact from a malformed one through
/// this enum; only the trusted native command receives the compact status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnalysisWorkResult {
    NoAnalysis,
    Existing(LagReport),
    Completed(LagReport),
    Joined,
    Busy,
    RawUnavailable,
    Failed,
}

/// Native-only request shape. Participant attribution is derived from the
/// private manifest, never supplied by an operator or client request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactAnalysisRequest {
    pub artifact_id: String,
    pub analyze: bool,
    pub options: AnalysisOptions,
}

/// A bounded, idempotent worker facade. The asynchronous submission API moves
/// private artifact loading and CPU-bound decoding to `spawn_blocking`; its
/// lease set is deliberately separate from ingest so uploads never wait for
/// analysis.
#[derive(Clone)]
pub struct LagAnalysisWorker {
    repository: Arc<dyn LagReportRepository>,
    max_in_flight: usize,
    /// Admission is intentionally non-waiting: a full worker returns `Busy`
    /// instead of accumulating an unbounded task queue beside ingest.
    async_slots: Arc<Semaphore>,
    leases: Arc<Mutex<BTreeSet<AnalysisIdentity>>>,
}

impl InMemoryLagReportRepository {
    /// Read one report by its opaque report id. This is a Console read helper;
    /// it does not expose any raw artifact locator or payload.
    #[must_use]
    pub fn find_by_report_id(&self, report_id: &str) -> Option<LagReport> {
        self.reports
            .lock()
            .ok()?
            .values()
            .find(|report| report.report_id == report_id)
            .cloned()
    }

    /// Capture-id keyset scan over every immutable report, grouped before the
    /// caller applies its page. This avoids making later capture ids unreachable
    /// merely because another capture accumulated more than one report.
    #[must_use]
    pub fn list_capture_overviews(
        &self,
        after_capture_id: Option<&str>,
        limit: usize,
    ) -> Vec<LagReportCaptureOverview> {
        let Some(reports) = self.reports.lock().ok() else {
            return Vec::new();
        };
        let mut captures = BTreeMap::<String, LagReportCaptureOverview>::new();
        for report in reports.values() {
            match captures.entry(report.capture_id.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(LagReportCaptureOverview {
                        capture_id: report.capture_id.clone(),
                        generation: report.generation,
                        report_count: 1,
                        latest_report_status: report.status,
                        latest_report_created_at: report.created_at,
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let capture = entry.get_mut();
                    capture.generation = capture.generation.max(report.generation);
                    capture.report_count = capture.report_count.saturating_add(1);
                    if report.created_at >= capture.latest_report_created_at {
                        capture.latest_report_status = report.status;
                        capture.latest_report_created_at = report.created_at;
                    }
                }
            }
        }
        captures
            .into_values()
            .filter(|capture| {
                after_capture_id.is_none_or(|after| capture.capture_id.as_str() > after)
            })
            .take(limit.clamp(1, 101))
            .collect()
    }
}

impl LagAnalysisWorker {
    #[must_use]
    pub fn new(repository: Arc<dyn LagReportRepository>, max_in_flight: usize) -> Self {
        Self {
            repository,
            max_in_flight: max_in_flight.clamp(1, 16),
            async_slots: Arc::new(Semaphore::new(max_in_flight.clamp(1, 16))),
            leases: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    /// Submit one analysis without occupying Tokio's core executor. The
    /// admission limit is deliberately fail-fast (a bounded zero-length queue)
    /// so slow decoding can never delay ingest or consume arbitrary memory.
    ///
    /// The service is passed as an `Arc` because loading/decompression and CLAG
    /// decoding happen inside `spawn_blocking`; no raw path or raw bytes escape
    /// the private ingest boundary.
    pub async fn analyze_artifact_async(
        &self,
        ingest: Arc<LagDiagnosticsService>,
        request: ArtifactAnalysisRequest,
        now: TimestampMillis,
    ) -> AnalysisWorkResult {
        if !request.analyze {
            return AnalysisWorkResult::NoAnalysis;
        }
        let permit = match self.try_acquire_async_slot() {
            Some(permit) => permit,
            None => return AnalysisWorkResult::Busy,
        };
        let worker = self.clone();
        match tokio::task::spawn_blocking(move || {
            let _permit = permit;
            worker.analyze_artifact(&ingest, request, now)
        })
        .await
        {
            Ok(result) => result,
            Err(_) => AnalysisWorkResult::Failed,
        }
    }

    /// Analyze exactly one digest-verified private artifact. `analyze=false`
    /// returns without allocating or persisting a report row.
    pub fn analyze_artifact(
        &self,
        ingest: &LagDiagnosticsService,
        request: ArtifactAnalysisRequest,
        now: TimestampMillis,
    ) -> AnalysisWorkResult {
        if !request.analyze {
            return AnalysisWorkResult::NoAnalysis;
        }
        let artifact = match ingest.load_private_analysis_artifact(&request.artifact_id) {
            Ok(value) => value,
            Err(_) => return AnalysisWorkResult::RawUnavailable,
        };
        self.analyze_loaded(artifact, request.options, now)
    }

    fn try_acquire_async_slot(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.async_slots).try_acquire_owned().ok()
    }

    #[cfg(test)]
    async fn analyze_loaded_async(
        &self,
        artifact: PrivateAnalysisArtifact,
        options: AnalysisOptions,
        now: TimestampMillis,
    ) -> AnalysisWorkResult {
        let Some(permit) = self.try_acquire_async_slot() else {
            return AnalysisWorkResult::Busy;
        };
        let worker = self.clone();
        match tokio::task::spawn_blocking(move || {
            let _permit = permit;
            worker.analyze_loaded(artifact, options, now)
        })
        .await
        {
            Ok(result) => result,
            Err(_) => AnalysisWorkResult::Failed,
        }
    }

    fn analyze_loaded(
        &self,
        artifact: PrivateAnalysisArtifact,
        options: AnalysisOptions,
        now: TimestampMillis,
    ) -> AnalysisWorkResult {
        let participant = artifact.participant.clone();
        if participant.is_empty()
            || participant.len() > 256
            || participant.chars().any(char::is_control)
        {
            return AnalysisWorkResult::Failed;
        }
        let capture_id = hex_bytes(&artifact.capture_id);
        let identity = AnalysisIdentity {
            capture_id: capture_id.clone(),
            generation: artifact.generation,
            artifact_digest_sha256: artifact.digest_sha256.clone(),
            analyzer_version: LAG_ANALYZER_VERSION,
            options_hash: options.canonical_hash(),
        };
        if let Some(report) = self.repository.get_by_identity(&identity) {
            return AnalysisWorkResult::Existing(report);
        }
        let supersedes_report_id = self
            .repository
            .latest_for_artifact(
                &identity.capture_id,
                identity.generation,
                &identity.artifact_digest_sha256,
            )
            .map(|report| report.report_id);
        {
            let Ok(mut leases) = self.leases.lock() else {
                return AnalysisWorkResult::Failed;
            };
            if !leases.insert(identity.clone()) {
                return AnalysisWorkResult::Joined;
            }
            if leases.len() > self.max_in_flight {
                leases.remove(&identity);
                return AnalysisWorkResult::Busy;
            }
        }
        let result = (|| {
            let decoded = decode_clag_v1(
                &artifact.clag_bytes,
                artifact.capture_id,
                artifact.generation,
            )
            .map_err(|_| ())?;
            let report_id_input = format!(
                "{};{};{};{};{}",
                identity.capture_id,
                identity.generation,
                identity.artifact_digest_sha256,
                identity.analyzer_version,
                identity.options_hash,
            );
            let report = analyze_clag(
                capture_id,
                artifact.generation,
                artifact.digest_sha256,
                format!("lr1-{}", &hex_sha256(report_id_input.as_bytes())[..24]),
                vec![(participant, decoded)],
                options,
                now,
                true,
                supersedes_report_id,
            );
            Ok::<_, ()>(self.repository.insert_immutable(identity.clone(), report))
        })();
        if let Ok(mut leases) = self.leases.lock() {
            leases.remove(&identity);
        }
        match result {
            Ok(report) => AnalysisWorkResult::Completed(report),
            Err(()) => AnalysisWorkResult::Failed,
        }
    }
}

impl LagReportRepository for InMemoryLagReportRepository {
    fn get_by_identity(&self, identity: &AnalysisIdentity) -> Option<LagReport> {
        self.reports.lock().ok()?.get(identity).cloned()
    }

    fn latest_for_artifact(
        &self,
        capture_id: &str,
        generation: u64,
        artifact_digest_sha256: &str,
    ) -> Option<LagReport> {
        self.reports
            .lock()
            .ok()?
            .values()
            .filter(|report| {
                report.capture_id == capture_id
                    && report.generation == generation
                    && report.artifact_digest_sha256 == artifact_digest_sha256
            })
            .max_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.report_id.cmp(&right.report_id))
            })
            .cloned()
    }

    fn insert_immutable(&self, identity: AnalysisIdentity, mut report: LagReport) -> LagReport {
        let unavailable = self.raw_unavailable.lock().ok().is_some_and(|tombstones| {
            tombstones.contains(&(
                identity.capture_id.clone(),
                identity.generation,
                identity.artifact_digest_sha256.clone(),
            ))
        });
        if unavailable {
            report.raw_available = false;
        }
        let Ok(mut reports) = self.reports.lock() else {
            return report;
        };
        reports
            .entry(identity)
            .or_insert_with(|| report.clone())
            .clone()
    }

    fn mark_raw_unavailable(
        &self,
        capture_id: &str,
        generation: u64,
        artifact_digest_sha256: &str,
    ) {
        let Ok(mut tombstones) = self.raw_unavailable.lock() else {
            return;
        };
        tombstones.insert((
            capture_id.to_string(),
            generation,
            artifact_digest_sha256.to_string(),
        ));
        let Ok(mut reports) = self.reports.lock() else {
            return;
        };
        for report in reports.values_mut() {
            if report.capture_id == capture_id
                && report.generation == generation
                && report.artifact_digest_sha256 == artifact_digest_sha256
            {
                // A completed report remains immutable.  Availability is a
                // separate current-state projection; it does not rewrite its
                // derived analysis status or metric values.
                report.raw_available = false;
            }
        }
    }

    fn list(&self, after_report_id: Option<&str>, limit: usize) -> Vec<LagReport> {
        let Some(reports) = self.reports.lock().ok() else {
            return Vec::new();
        };
        let mut values = reports.values().cloned().collect::<Vec<_>>();
        values.sort_by(|left, right| left.report_id.cmp(&right.report_id));
        values
            .into_iter()
            .filter(|report| after_report_id.is_none_or(|after| report.report_id.as_str() > after))
            // Console uses one extra row for a truthful keyset `next_after`.
            .take(limit.clamp(1, 101))
            .collect()
    }
}

/// Decode an exact, already decompressed CLAG v1 artifact.
///
/// `expected_capture_id` and `expected_generation` come from the authenticated
/// server-side manifest.  Matching them here prevents an artifact from being
/// analyzed under another capture identity even if a storage bug exposed bytes.
pub fn decode_clag_v1(
    bytes: &[u8],
    expected_capture_id: [u8; 16],
    expected_generation: u64,
) -> Result<DecodedClag, ClagDecodeError> {
    if expected_generation > u64::from(u32::MAX) {
        return Err(ClagDecodeError::Invalid);
    }
    if bytes.len() < CLAG_HEADER_BYTES || &bytes[..4] != b"CLAG" {
        return Err(ClagDecodeError::Unsupported);
    }
    if be_u16(bytes, 4)? != CLAG_DECODER_VERSION
        || usize::from(be_u16(bytes, 6)?) != CLAG_HEADER_BYTES
        || usize::from(be_u16(bytes, 8)?) != CLAG_RECORD_BYTES
    {
        return Err(ClagDecodeError::Unsupported);
    }
    let flags = be_u16(bytes, 10)?;
    if flags & !CLAG_HEADER_FLAGS != 0 || flags & CLAG_FLAG_METADATA_ONLY == 0 {
        return Err(ClagDecodeError::Invalid);
    }
    let count = be_u32(bytes, 12)?;
    let expected_len = CLAG_HEADER_BYTES
        .checked_add(
            usize::try_from(count)
                .map_err(|_| ClagDecodeError::Invalid)?
                .checked_mul(CLAG_RECORD_BYTES)
                .ok_or(ClagDecodeError::Invalid)?,
        )
        .ok_or(ClagDecodeError::Invalid)?;
    if bytes.len() != expected_len {
        return Err(ClagDecodeError::Invalid);
    }
    let mut capture_id = [0_u8; 16];
    capture_id.copy_from_slice(field(bytes, 48, 16)?);
    if capture_id == [0; 16]
        || capture_id != expected_capture_id
        || u64::from(be_u32(bytes, 64)?) != expected_generation
    {
        return Err(ClagDecodeError::Invalid);
    }
    let accepted = be_u64(bytes, 16)?;
    let overwritten = be_u64(bytes, 24)?;
    let skipped_filter = be_u64(bytes, 32)?;
    let skipped_malformed = be_u64(bytes, 40)?;
    if accepted != u64::from(count).saturating_add(overwritten) {
        return Err(ClagDecodeError::Invalid);
    }
    let clock_flag = flags & CLAG_FLAG_SERVER_CLOCK != 0;
    let initial_uncertainty = be_u32(bytes, 68)?;
    let initial_server_utc = be_u64(bytes, 72)?;
    if clock_flag != (initial_server_utc != 0) {
        return Err(ClagDecodeError::Invalid);
    }
    let mut segments = Vec::new();
    let mut previous_elapsed = None;
    for index in 0..3 {
        let offset = 80 + (index * 16);
        let elapsed = be_u32(bytes, offset)?;
        let utc = be_u64(bytes, offset + 4)?;
        let uncertainty = be_u32(bytes, offset + 12)?;
        if elapsed == 0 && utc == 0 && uncertainty == 0 {
            continue;
        }
        if !clock_flag || utc == 0 || previous_elapsed.is_some_and(|last| elapsed <= last) {
            return Err(ClagDecodeError::Invalid);
        }
        previous_elapsed = Some(elapsed);
        segments.push(ClockSegment {
            elapsed_us: elapsed,
            server_utc_us: utc,
            uncertainty_us: uncertainty,
        });
    }
    let header = ClagHeader {
        capture_id,
        generation: be_u32(bytes, 64)?,
        record_count: count,
        accepted_records: accepted,
        overwritten_records: overwritten,
        skipped_filter_records: skipped_filter,
        skipped_malformed_records: skipped_malformed,
        metadata_only: true,
        server_clock_at_start_utc_us: clock_flag.then_some(initial_server_utc),
        initial_clock_uncertainty_us: clock_flag.then_some(initial_uncertainty),
        clock_segments: segments,
    };
    let mut records =
        Vec::with_capacity(usize::try_from(count).map_err(|_| ClagDecodeError::Invalid)?);
    let mut previous_elapsed = None;
    for index in 0..usize::try_from(count).map_err(|_| ClagDecodeError::Invalid)? {
        let offset = CLAG_HEADER_BYTES + (index * CLAG_RECORD_BYTES);
        let record = decode_record(field(bytes, offset, CLAG_RECORD_BYTES)?)?;
        if previous_elapsed.is_some_and(|last| record.elapsed_us < last) {
            return Err(ClagDecodeError::Invalid);
        }
        previous_elapsed = Some(record.elapsed_us);
        records.push(record);
    }
    Ok(DecodedClag { header, records })
}

/// Derive one immutable report from validated decoded artifacts.  Each item is
/// an opaque participant handle paired with one artifact; callers retain raw
/// storage outside this function.
#[allow(clippy::too_many_arguments)] // Report identity and immutable provenance are explicit API inputs.
pub fn analyze_clag(
    capture_id: String,
    generation: u64,
    artifact_digest_sha256: String,
    report_id: String,
    inputs: Vec<(String, DecodedClag)>,
    options: AnalysisOptions,
    now: TimestampMillis,
    raw_available: bool,
    supersedes_report_id: Option<String>,
) -> LagReport {
    let options_hash = options.canonical_hash();
    if inputs.is_empty() {
        return LagReport {
            report_id,
            capture_id,
            generation,
            artifact_digest_sha256,
            decoder_version: CLAG_DECODER_VERSION,
            analyzer_version: LAG_ANALYZER_VERSION,
            options_hash,
            status: LagReportStatus::NoData,
            raw_available,
            created_at: now,
            quality: empty_quality("no_data"),
            summaries: Vec::new(),
            windows: Vec::new(),
            supersedes_report_id,
        };
    }
    let mut groups = BTreeMap::<ObservationKey, GroupAccumulator>::new();
    let mut overall = empty_quality("complete");
    for (participant, artifact) in inputs {
        overall.overwritten_count = overall
            .overwritten_count
            .saturating_add(artifact.header.overwritten_records);
        overall.malformed_count = overall
            .malformed_count
            .saturating_add(artifact.header.skipped_malformed_records);
        overall.clock_uncertain |= artifact.header.initial_clock_uncertainty_us.is_none()
            || artifact
                .header
                .initial_clock_uncertainty_us
                .unwrap_or(u32::MAX)
                != 0;
        for record in artifact.records {
            let key = ObservationKey {
                participant: participant.clone(),
                packet_kind: record.packet_kind,
                direction: record.direction,
                delivery: record.delivery,
                gameplay_epoch: record.gameplay_epoch,
            };
            if let Some(group) = groups.get_mut(&key) {
                group.push(record);
            } else if groups.len() < MAX_REPORT_SUMMARIES {
                groups.insert(key.clone(), GroupAccumulator::new(artifact.header.clone()));
                groups
                    .get_mut(&key)
                    .expect("inserted group exists")
                    .push(record);
            } else {
                overall.excluded_count = overall.excluded_count.saturating_add(1);
                overall.status = "partial".to_string();
            }
        }
    }
    let mut summaries = Vec::with_capacity(groups.len().min(MAX_REPORT_SUMMARIES));
    let mut windows = Vec::new();
    for (key, accumulator) in groups {
        let (summary, mut group_windows) = accumulator.finish(key, options);
        overall.sample_count = overall
            .sample_count
            .saturating_add(summary.quality.sample_count);
        overall.excluded_count = overall
            .excluded_count
            .saturating_add(summary.quality.excluded_count);
        if summary.quality.status != "complete" {
            overall.status = "partial".to_string();
        }
        summaries.push(summary);
        windows.append(&mut group_windows);
    }
    windows.truncate(usize::from(options.max_windows.clamp(1, 64)));
    let status = if summaries.is_empty() {
        LagReportStatus::NoData
    } else if overall.status == "partial"
        || overall.excluded_count != 0
        || overall.overwritten_count != 0
        || overall.malformed_count != 0
    {
        LagReportStatus::Partial
    } else {
        LagReportStatus::Complete
    };
    LagReport {
        report_id,
        capture_id,
        generation,
        artifact_digest_sha256,
        decoder_version: CLAG_DECODER_VERSION,
        analyzer_version: LAG_ANALYZER_VERSION,
        options_hash,
        status,
        raw_available,
        created_at: now,
        quality: overall,
        summaries,
        windows,
        supersedes_report_id,
    }
}

#[derive(Clone)]
struct GroupAccumulator {
    header: ClagHeader,
    records: Vec<ClagRecord>,
}

impl GroupAccumulator {
    fn new(header: ClagHeader) -> Self {
        Self {
            header,
            records: Vec::new(),
        }
    }

    fn push(&mut self, record: ClagRecord) {
        self.records.push(record);
    }

    fn finish(
        self,
        key: ObservationKey,
        options: AnalysisOptions,
    ) -> (LagObservationSummary, Vec<LagTimelineWindow>) {
        let mut spacing = Vec::new();
        let mut residual = Vec::new();
        let mut gaps = 0_u64;
        let mut duplicates = 0_u32;
        let mut reorder = 0_u32;
        let expected_cadence = options
            .send_rate_hz
            .filter(|rate| *rate != 0)
            .map(|rate| 1_000_000_u32 / u32::from(rate));
        for pair in self.records.windows(2) {
            let interval = pair[1].elapsed_us.saturating_sub(pair[0].elapsed_us);
            spacing.push(interval);
            if let Some(cadence) = expected_cadence {
                residual.push(interval.abs_diff(cadence));
            }
            let delta = pair[1].packet_id.wrapping_sub(pair[0].packet_id);
            if delta == 0 {
                duplicates = duplicates.saturating_add(1);
            } else if delta < 0x8000_0000 {
                gaps = gaps.saturating_add(u64::from(delta.saturating_sub(1)));
            } else {
                reorder = reorder.saturating_add(1);
            }
        }
        let mut sorted = spacing.clone();
        sorted.sort_unstable();
        let quality_status =
            if spacing.len() < usize::try_from(MIN_SPACING_SAMPLES).unwrap_or(usize::MAX) {
                "insufficient_samples"
            } else if self.header.overwritten_records != 0
                || self.header.skipped_malformed_records != 0
                || self.header.initial_clock_uncertainty_us.is_none()
            {
                "partial"
            } else {
                "complete"
            };
        let quality = MetricQuality {
            status: quality_status.to_string(),
            sample_count: u32::try_from(spacing.len()).unwrap_or(u32::MAX),
            excluded_count: 0,
            overwritten_count: self.header.overwritten_records,
            malformed_count: self.header.skipped_malformed_records,
            clock_uncertain: self.header.initial_clock_uncertainty_us.is_none()
                || self.header.initial_clock_uncertainty_us.unwrap_or(u32::MAX) != 0,
        };
        let histogram = histogram(&spacing);
        let summary = LagObservationSummary {
            key: key.clone(),
            metric_label: "arrival_spacing_dispersion".to_string(),
            unit: "microseconds".to_string(),
            quality,
            mean_interarrival_us: mean(&spacing),
            p50_interarrival_us: percentile(&sorted, 50),
            p95_interarrival_us: percentile(&sorted, 95),
            p99_interarrival_us: percentile(&sorted, 99),
            histogram,
            cadence_residual_p95_us: expected_cadence.and_then(|_| {
                residual.sort_unstable();
                percentile(&residual, 95)
            }),
            observed_id_gap: gaps,
            duplicate_id: duplicates,
            arrival_reorder: reorder,
        };
        let windows = windowed(key, &self.records);
        (summary, windows)
    }
}

fn decode_record(bytes: &[u8]) -> Result<ClagRecord, ClagDecodeError> {
    let packet_kind = be_u16(bytes, 4)?;
    let direction = match *field(bytes, 6, 1)?
        .first()
        .ok_or(ClagDecodeError::Invalid)?
    {
        0 => Direction::Inbound,
        1 => Direction::Outbound,
        _ => return Err(ClagDecodeError::Invalid),
    };
    let delivery = match *field(bytes, 7, 1)?
        .first()
        .ok_or(ClagDecodeError::Invalid)?
    {
        DELIVERY_RELIABLE => DeliveryMode::Reliable,
        DELIVERY_DATAGRAM => DeliveryMode::Datagram,
        _ => return Err(ClagDecodeError::Invalid),
    };
    if !matches!(packet_kind, 8 | 9 | 10 | 30 | 31)
        || be_u32(bytes, 20)? != 0
        || be_u16(bytes, 30)? & !0x0003 != 0
    {
        return Err(ClagDecodeError::Invalid);
    }
    let metadata_flags = be_u16(bytes, 30)?;
    let epoch = be_u64(bytes, 32)?;
    let last_observed = be_u64(bytes, 40)?;
    let is_v2 = matches!(packet_kind, 30 | 31);
    if is_v2 == (metadata_flags & 0x0002 == 0)
        || (!is_v2 && (epoch != 0 || last_observed != 0 || metadata_flags & 0x0002 != 0))
    {
        return Err(ClagDecodeError::Invalid);
    }
    // Keep the decoder aligned with the recorder's immutable local policy.
    // These aren't merely analytics labels: accepting a direction that the SDK
    // never records would turn a corrupted row into a plausible observation.
    match packet_kind {
        8 if direction == Direction::Inbound
            && metadata_flags == 0x0001
            && tick_hz(bytes)? == 0 => {}
        30 if direction == Direction::Inbound
            && metadata_flags == 0x0003
            && tick_hz(bytes)? != 0 => {}
        9 | 10
            if direction == Direction::Outbound && metadata_flags == 0 && tick_hz(bytes)? == 0 => {}
        31 if direction == Direction::Outbound
            && metadata_flags == 0x0002
            && tick_hz(bytes)? == 0 => {}
        _ => return Err(ClagDecodeError::Invalid),
    }
    Ok(ClagRecord {
        elapsed_us: be_u32(bytes, 0)?,
        packet_kind,
        direction,
        delivery,
        body_bytes: be_u32(bytes, 8)?,
        packet_id: be_u32(bytes, 12)?,
        base_packet_id: be_u32(bytes, 16)?,
        server_tick: be_u32(bytes, 24)?,
        tick_hz: be_u16(bytes, 28)?,
        metadata_flags,
        gameplay_epoch: epoch,
    })
}

fn tick_hz(bytes: &[u8]) -> Result<u16, ClagDecodeError> {
    be_u16(bytes, 28)
}

fn windowed(key: ObservationKey, records: &[ClagRecord]) -> Vec<LagTimelineWindow> {
    let Some(first) = records.first() else {
        return Vec::new();
    };
    let Some(last) = records.last() else {
        return Vec::new();
    };
    let mut spacing = Vec::new();
    let mut gaps = 0_u64;
    let mut duplicates = 0_u32;
    let mut reorder = 0_u32;
    for pair in records.windows(2) {
        spacing.push(pair[1].elapsed_us.saturating_sub(pair[0].elapsed_us));
        let delta = pair[1].packet_id.wrapping_sub(pair[0].packet_id);
        if delta == 0 {
            duplicates = duplicates.saturating_add(1);
        } else if delta < 0x8000_0000 {
            gaps = gaps.saturating_add(u64::from(delta.saturating_sub(1)));
        } else {
            reorder = reorder.saturating_add(1);
        }
    }
    vec![LagTimelineWindow {
        key,
        start_elapsed_us: first.elapsed_us,
        end_elapsed_us: last.elapsed_us,
        sample_count: u32::try_from(spacing.len()).unwrap_or(u32::MAX),
        mean_interarrival_us: mean(&spacing),
        observed_id_gap: gaps,
        duplicate_id: duplicates,
        arrival_reorder: reorder,
    }]
}

fn histogram(samples: &[u32]) -> FixedHistogram {
    let mut counts = vec![0_u32; HISTOGRAM_EDGES_US.len()];
    let mut overflow = 0_u32;
    for sample in samples {
        if let Some(index) = HISTOGRAM_EDGES_US.iter().position(|edge| sample <= edge) {
            counts[index] = counts[index].saturating_add(1);
        } else {
            overflow = overflow.saturating_add(1);
        }
    }
    FixedHistogram {
        unit: "microseconds".to_string(),
        edges_us: HISTOGRAM_EDGES_US.to_vec(),
        counts,
        overflow_count: overflow,
    }
}

fn mean(samples: &[u32]) -> Option<u64> {
    (!samples.is_empty()).then(|| {
        samples.iter().map(|value| u64::from(*value)).sum::<u64>()
            / u64::try_from(samples.len()).unwrap_or(1)
    })
}

fn percentile(sorted: &[u32], percent: u32) -> Option<u32> {
    if sorted.is_empty() || percent == 0 || percent > 100 {
        return None;
    }
    let rank = (u64::try_from(sorted.len())
        .ok()?
        .saturating_mul(u64::from(percent))
        .saturating_add(99))
        / 100;
    sorted
        .get(usize::try_from(rank.saturating_sub(1)).ok()?)
        .copied()
}

fn empty_quality(status: &str) -> MetricQuality {
    MetricQuality {
        status: status.to_string(),
        sample_count: 0,
        excluded_count: 0,
        overwritten_count: 0,
        malformed_count: 0,
        clock_uncertain: true,
    }
}

fn field(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8], ClagDecodeError> {
    bytes
        .get(offset..offset.checked_add(len).ok_or(ClagDecodeError::Invalid)?)
        .ok_or(ClagDecodeError::Invalid)
}

fn be_u16(bytes: &[u8], offset: usize) -> Result<u16, ClagDecodeError> {
    Ok(u16::from_be_bytes(
        field(bytes, offset, 2)?
            .try_into()
            .map_err(|_| ClagDecodeError::Invalid)?,
    ))
}

fn be_u32(bytes: &[u8], offset: usize) -> Result<u32, ClagDecodeError> {
    Ok(u32::from_be_bytes(
        field(bytes, offset, 4)?
            .try_into()
            .map_err(|_| ClagDecodeError::Invalid)?,
    ))
}

fn be_u64(bytes: &[u8], offset: usize) -> Result<u64, ClagDecodeError> {
    Ok(u64::from_be_bytes(
        field(bytes, offset, 8)?
            .try_into()
            .map_err(|_| ClagDecodeError::Invalid)?,
    ))
}

fn hex_sha256(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_bytes(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capture() -> [u8; 16] {
        [7; 16]
    }

    fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
        bytes[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
    }
    fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }
    fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
    }

    fn artifact(rows: &[(u32, u32)]) -> Vec<u8> {
        let mut bytes = vec![0; CLAG_HEADER_BYTES + (rows.len() * CLAG_RECORD_BYTES)];
        bytes[..4].copy_from_slice(b"CLAG");
        put_u16(&mut bytes, 4, 1);
        put_u16(&mut bytes, 6, CLAG_HEADER_BYTES as u16);
        put_u16(&mut bytes, 8, CLAG_RECORD_BYTES as u16);
        put_u16(
            &mut bytes,
            10,
            CLAG_FLAG_METADATA_ONLY | CLAG_FLAG_SERVER_CLOCK,
        );
        put_u32(&mut bytes, 12, rows.len() as u32);
        put_u64(&mut bytes, 16, rows.len() as u64);
        bytes[48..64].copy_from_slice(&capture());
        put_u32(&mut bytes, 64, 9);
        put_u64(&mut bytes, 72, 1_700_000_000_000_000);
        for (index, (elapsed, packet_id)) in rows.iter().enumerate() {
            let base = CLAG_HEADER_BYTES + (index * CLAG_RECORD_BYTES);
            put_u32(&mut bytes, base, *elapsed);
            put_u16(&mut bytes, base + 4, 8);
            bytes[base + 6] = 0;
            bytes[base + 7] = DELIVERY_DATAGRAM;
            put_u32(&mut bytes, base + 12, *packet_id);
            put_u16(&mut bytes, base + 30, 1);
        }
        bytes
    }

    #[test]
    fn decoder_requires_exact_shape_and_manifest_binding() {
        let bytes = artifact(&[(0, 1), (10_000, 2)]);
        let decoded = decode_clag_v1(&bytes, capture(), 9).expect("v1 fixture");
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[1].packet_id, 2);
        assert_eq!(
            decode_clag_v1(&bytes[..bytes.len() - 1], capture(), 9),
            Err(ClagDecodeError::Invalid)
        );
        assert_eq!(
            decode_clag_v1(&bytes, [8; 16], 9),
            Err(ClagDecodeError::Invalid)
        );
        assert_eq!(
            decode_clag_v1(&bytes, capture(), u64::from(u32::MAX) + 1),
            Err(ClagDecodeError::Invalid)
        );
    }

    #[test]
    fn decoder_rejects_unknown_delivery_and_non_monotonic_rows() {
        let mut bytes = artifact(&[(10, 1), (20, 2)]);
        bytes[CLAG_HEADER_BYTES + 7] = 0;
        assert_eq!(
            decode_clag_v1(&bytes, capture(), 9),
            Err(ClagDecodeError::Invalid)
        );
        let bytes = artifact(&[(20, 1), (10, 2)]);
        assert_eq!(
            decode_clag_v1(&bytes, capture(), 9),
            Err(ClagDecodeError::Invalid)
        );
    }

    #[test]
    fn analysis_labels_observed_identifier_events_without_packet_loss() {
        let decoded = decode_clag_v1(
            &artifact(&[
                (0, u32::MAX),
                (10_000, 0),
                (35_000, 3),
                (45_000, 3),
                (55_000, 2),
            ]),
            capture(),
            9,
        )
        .expect("fixture");
        let report = analyze_clag(
            "capture".to_string(),
            9,
            "digest".to_string(),
            "report".to_string(),
            vec![("pseudonym-1".to_string(), decoded)],
            AnalysisOptions {
                send_rate_hz: Some(100),
                max_windows: 4,
            },
            TimestampMillis::from_unix_millis(1),
            true,
            None,
        );
        assert_eq!(report.summaries.len(), 1);
        let summary = &report.summaries[0];
        assert_eq!(summary.metric_label, "arrival_spacing_dispersion");
        assert_eq!(summary.observed_id_gap, 2);
        assert_eq!(summary.duplicate_id, 1);
        assert_eq!(summary.arrival_reorder, 1);
        assert!(summary.cadence_residual_p95_us.is_some());
    }

    #[test]
    fn analysis_caps_distinct_epoch_groups_and_marks_dropped_rows_partial() {
        let records = (0..(MAX_REPORT_SUMMARIES + 5))
            .map(|index| ClagRecord {
                elapsed_us: u32::try_from(index)
                    .unwrap_or(u32::MAX)
                    .saturating_mul(1_000),
                packet_kind: 30,
                direction: Direction::Inbound,
                delivery: DeliveryMode::Datagram,
                body_bytes: 0,
                packet_id: u32::try_from(index).unwrap_or(u32::MAX),
                base_packet_id: 0,
                server_tick: 0,
                tick_hz: 60,
                metadata_flags: 0x0003,
                gameplay_epoch: u64::try_from(index).unwrap_or(u64::MAX),
            })
            .collect::<Vec<_>>();
        let decoded = DecodedClag {
            header: ClagHeader {
                capture_id: capture(),
                generation: 9,
                record_count: u32::try_from(records.len()).unwrap_or(u32::MAX),
                accepted_records: u64::try_from(records.len()).unwrap_or(u64::MAX),
                overwritten_records: 0,
                skipped_filter_records: 0,
                skipped_malformed_records: 0,
                metadata_only: true,
                server_clock_at_start_utc_us: Some(1),
                initial_clock_uncertainty_us: Some(0),
                clock_segments: Vec::new(),
            },
            records,
        };
        let report = analyze_clag(
            "capture".to_string(),
            9,
            "digest".to_string(),
            "report".to_string(),
            vec![("participant".to_string(), decoded)],
            AnalysisOptions::default(),
            TimestampMillis::from_unix_millis(1),
            true,
            None,
        );
        assert_eq!(report.summaries.len(), MAX_REPORT_SUMMARIES);
        assert_eq!(report.quality.excluded_count, 5);
        assert_eq!(report.status, LagReportStatus::Partial);
        assert!(report.windows.len() <= 64);
    }

    #[test]
    fn nearest_rank_and_options_hash_are_deterministic() {
        assert_eq!(percentile(&[1, 2, 3, 4], 95), Some(4));
        assert_eq!(percentile(&[1, 2, 3, 4], 50), Some(2));
        assert_eq!(
            AnalysisOptions::default().canonical_hash(),
            AnalysisOptions::default().canonical_hash()
        );
    }

    #[test]
    fn repository_projects_raw_expiry_only_to_its_source_artifact() {
        let repo = InMemoryLagReportRepository::default();
        let identity = AnalysisIdentity {
            capture_id: "c".to_string(),
            generation: 1,
            artifact_digest_sha256: "d".to_string(),
            analyzer_version: 1,
            options_hash: "o".to_string(),
        };
        let report = analyze_clag(
            "c".to_string(),
            1,
            "d".to_string(),
            "r".to_string(),
            Vec::new(),
            AnalysisOptions::default(),
            TimestampMillis::from_unix_millis(1),
            true,
            None,
        );
        let stored = repo.insert_immutable(identity.clone(), report);
        let other_identity = AnalysisIdentity {
            artifact_digest_sha256: "e".to_string(),
            options_hash: "p".to_string(),
            ..identity.clone()
        };
        let other_report = analyze_clag(
            "c".to_string(),
            1,
            "e".to_string(),
            "s".to_string(),
            Vec::new(),
            AnalysisOptions::default(),
            TimestampMillis::from_unix_millis(1),
            true,
            None,
        );
        repo.insert_immutable(other_identity.clone(), other_report);
        repo.mark_raw_unavailable("c", 1, "d");
        let late_identity = AnalysisIdentity {
            capture_id: "c".to_string(),
            generation: 1,
            artifact_digest_sha256: "d".to_string(),
            analyzer_version: 1,
            options_hash: "late".to_string(),
        };
        let late = repo.insert_immutable(
            late_identity,
            analyze_clag(
                "c".to_string(),
                1,
                "d".to_string(),
                "late".to_string(),
                Vec::new(),
                AnalysisOptions::default(),
                TimestampMillis::from_unix_millis(2),
                true,
                None,
            ),
        );
        let read = repo.get_by_identity(&identity).expect("report");
        let other = repo
            .get_by_identity(&other_identity)
            .expect("unrelated artifact report");
        assert_eq!(stored.status, LagReportStatus::NoData);
        assert_eq!(read.status, LagReportStatus::NoData);
        assert!(!read.raw_available);
        assert!(!late.raw_available);
        assert!(other.raw_available);
    }

    #[test]
    fn capture_overview_keyset_does_not_hide_reports_after_the_first_hundred() {
        let repo = InMemoryLagReportRepository::default();
        for index in 1..=102_u64 {
            let capture_id = format!("{index:032x}");
            let identity = AnalysisIdentity {
                capture_id: capture_id.clone(),
                generation: 1,
                artifact_digest_sha256: format!("{index:064x}"),
                analyzer_version: 1,
                options_hash: format!("{:064x}", index.saturating_add(10_000)),
            };
            let report = analyze_clag(
                capture_id.clone(),
                1,
                identity.artifact_digest_sha256.clone(),
                format!("lr1-{index:024x}"),
                Vec::new(),
                AnalysisOptions::default(),
                TimestampMillis::from_unix_millis(index),
                false,
                None,
            );
            repo.insert_immutable(identity, report);
        }
        let first = repo.list_capture_overviews(None, 100);
        assert_eq!(first.len(), 100);
        let cursor = first.last().expect("full page").capture_id.clone();
        let second = repo.list_capture_overviews(Some(&cursor), 100);
        assert_eq!(second.len(), 2);
        assert!(second.iter().all(|capture| capture.capture_id > cursor));
    }

    #[test]
    fn worker_report_id_uses_the_complete_analysis_identity() {
        let repository = Arc::new(InMemoryLagReportRepository::default());
        let worker = LagAnalysisWorker::new(repository, 1);
        let first = worker.analyze_loaded(
            PrivateAnalysisArtifact {
                capture_id: capture(),
                generation: 9,
                participant: "p-test".to_string(),
                digest_sha256: "a".repeat(64),
                clag_bytes: artifact(&[]),
            },
            AnalysisOptions::default(),
            TimestampMillis::from_unix_millis(1),
        );
        let second = worker.analyze_loaded(
            PrivateAnalysisArtifact {
                capture_id: capture(),
                generation: 9,
                participant: "p-test".to_string(),
                digest_sha256: "b".repeat(64),
                clag_bytes: artifact(&[]),
            },
            AnalysisOptions::default(),
            TimestampMillis::from_unix_millis(1),
        );
        assert!(matches!(&first, AnalysisWorkResult::Completed(_)));
        assert!(matches!(&second, AnalysisWorkResult::Completed(_)));
        let AnalysisWorkResult::Completed(first) = first else {
            return;
        };
        let AnalysisWorkResult::Completed(second) = second else {
            return;
        };
        assert_ne!(first.report_id, second.report_id);
    }

    #[test]
    fn analyze_false_does_not_attempt_a_raw_load_or_create_a_row() {
        let repository = Arc::new(InMemoryLagReportRepository::default());
        let worker = LagAnalysisWorker::new(repository.clone(), 1);
        let disabled = LagDiagnosticsService::new(
            crate::config::LagDiagnosticsConfig::default(),
            "node".to_string(),
        )
        .expect("disabled ingest");
        assert_eq!(
            worker.analyze_artifact(
                &disabled,
                ArtifactAnalysisRequest {
                    artifact_id: "not-loaded".to_string(),
                    analyze: false,
                    options: AnalysisOptions::default(),
                },
                TimestampMillis::from_unix_millis(1),
            ),
            AnalysisWorkResult::NoAnalysis,
        );
        assert!(repository.list(None, 10).is_empty());
    }

    #[tokio::test]
    async fn async_worker_returns_no_analysis_before_loading_or_admission() {
        let repository = Arc::new(InMemoryLagReportRepository::default());
        let worker = LagAnalysisWorker::new(repository.clone(), 1);
        let disabled = Arc::new(
            LagDiagnosticsService::new(
                crate::config::LagDiagnosticsConfig::default(),
                "node".to_string(),
            )
            .expect("disabled ingest"),
        );
        // Hold the only worker slot to prove disabled analysis does not queue
        // or consult the private loader at all.
        let permit = Arc::clone(&worker.async_slots)
            .try_acquire_owned()
            .expect("worker slot");
        let result = worker
            .analyze_artifact_async(
                disabled,
                ArtifactAnalysisRequest {
                    artifact_id: "not-loaded".to_string(),
                    analyze: false,
                    options: AnalysisOptions::default(),
                },
                TimestampMillis::from_unix_millis(1),
            )
            .await;
        drop(permit);
        assert_eq!(result, AnalysisWorkResult::NoAnalysis);
        assert!(repository.list(None, 10).is_empty());
    }

    #[tokio::test]
    async fn async_worker_returns_busy_without_touching_ingest_when_at_capacity() {
        let worker = LagAnalysisWorker::new(Arc::new(InMemoryLagReportRepository::default()), 1);
        let disabled = Arc::new(
            LagDiagnosticsService::new(
                crate::config::LagDiagnosticsConfig::default(),
                "node".to_string(),
            )
            .expect("disabled ingest"),
        );
        let permit = Arc::clone(&worker.async_slots)
            .try_acquire_owned()
            .expect("worker slot");
        let result = worker
            .analyze_artifact_async(
                disabled,
                ArtifactAnalysisRequest {
                    artifact_id: "not-loaded".to_string(),
                    analyze: true,
                    options: AnalysisOptions::default(),
                },
                TimestampMillis::from_unix_millis(1),
            )
            .await;
        drop(permit);
        assert_eq!(result, AnalysisWorkResult::Busy);
    }

    #[tokio::test]
    async fn async_worker_is_idempotent_for_the_same_complete_identity() {
        let worker = LagAnalysisWorker::new(Arc::new(InMemoryLagReportRepository::default()), 2);
        let artifact = || PrivateAnalysisArtifact {
            capture_id: capture(),
            generation: 9,
            participant: "p-test".to_string(),
            digest_sha256: "a".repeat(64),
            clag_bytes: artifact(&[]),
        };
        let first = worker
            .analyze_loaded_async(
                artifact(),
                AnalysisOptions::default(),
                TimestampMillis::from_unix_millis(1),
            )
            .await;
        let second = worker
            .analyze_loaded_async(
                artifact(),
                AnalysisOptions::default(),
                TimestampMillis::from_unix_millis(2),
            )
            .await;
        assert!(matches!(&first, AnalysisWorkResult::Completed(_)));
        assert!(matches!(&second, AnalysisWorkResult::Existing(_)));
        let AnalysisWorkResult::Completed(first) = first else {
            return;
        };
        let AnalysisWorkResult::Existing(second) = second else {
            return;
        };
        assert_eq!(first.report_id, second.report_id);
    }

    #[tokio::test]
    async fn regenerated_options_link_an_immutable_predecessor() {
        let worker = LagAnalysisWorker::new(Arc::new(InMemoryLagReportRepository::default()), 2);
        let artifact = || PrivateAnalysisArtifact {
            capture_id: capture(),
            generation: 9,
            participant: "participant".to_string(),
            digest_sha256: "a".repeat(64),
            clag_bytes: artifact(&[]),
        };
        let first = worker
            .analyze_loaded_async(
                artifact(),
                AnalysisOptions::default(),
                TimestampMillis::from_unix_millis(1),
            )
            .await;
        let second = worker
            .analyze_loaded_async(
                artifact(),
                AnalysisOptions {
                    send_rate_hz: Some(30),
                    max_windows: 2,
                },
                TimestampMillis::from_unix_millis(2),
            )
            .await;
        assert!(matches!(&first, AnalysisWorkResult::Completed(_)));
        assert!(matches!(&second, AnalysisWorkResult::Completed(_)));
        let AnalysisWorkResult::Completed(first) = first else {
            return;
        };
        let AnalysisWorkResult::Completed(second) = second else {
            return;
        };
        assert_eq!(first.supersedes_report_id, None);
        assert_eq!(
            second.supersedes_report_id.as_deref(),
            Some(first.report_id.as_str())
        );
    }
}
