//! Private capture lifecycle, one-use upload capabilities, and raw artifact
//! storage for lag diagnostics.
//!
//! This module is intentionally independent of the database. A capture report
//! may later reference an opaque artifact handle, but raw packet bytes, client
//! filenames, upload tokens, JTI values, and filesystem paths never become
//! database rows.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use citadel_wire::diagnostics::{
    CaptureId, DIAGNOSTICS_UPLOAD_PATH, FlushCapture, UploadContentEncoding, UploadContentType,
};
use flate2::read::MultiGzDecoder;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::LagDiagnosticsConfig;
use crate::time::TimestampMillis;

type HmacSha256 = Hmac<Sha256>;

const TOKEN_VERSION: &str = "v1";
const TOKEN_AUDIENCE: &str = "citadel-lag-upload";
const CLAG_HEADER_BYTES: usize = 128;
const CLAG_RECORD_BYTES: usize = 48;
const CLAG_VERSION: u16 = 1;
const CLAG_KNOWN_FLAGS: u16 = 0x0007;

/// A sanitized server-side capture failure. HTTP callers receive the same
/// opaque rejection for every variant, avoiding capture/session/token oracles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LagDiagnosticsError {
    #[error("lag diagnostics are disabled")]
    Disabled,
    #[error("invalid lag diagnostics request")]
    InvalidRequest,
    #[error("lag diagnostics upload is not currently accepted")]
    NotFlushing,
    #[error("lag diagnostics upload was rejected")]
    Rejected,
    #[error("lag diagnostics private storage is unavailable")]
    Storage,
}

/// Server-known binding for exactly one realtime participant. These strings are
/// opaque trusted identities from the match/session owner; they are signed into
/// the grant so a token cannot be replayed across participant, tenant, match,
/// or session boundaries.
#[derive(Clone, PartialEq, Eq)]
pub struct CaptureParticipant {
    pub participant_id: u64,
    pub session_id: String,
    pub tenant_id: String,
    pub match_id: String,
}

impl std::fmt::Debug for CaptureParticipant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureParticipant")
            .field("participant_id", &self.participant_id)
            .field("session_id", &"[redacted]")
            .field("tenant_id", &"[redacted]")
            .field("match_id", &"[redacted]")
            .finish()
    }
}

/// Trusted native request to transition a registered capture into `Flushing`
/// and mint one FLUSH body per expected uploader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureFlushPlan {
    pub capture_id: CaptureId,
    pub generation: u64,
    pub attempt_id: u64,
    pub upload_deadline_server_utc_ms: u64,
    pub max_compressed_bytes: u32,
    /// Minimum number of server-accepted artifacts required to seal this
    /// attempt. This is selected from authenticated `Recording` statuses,
    /// never from transport queue outcomes.
    pub required_uploads: u32,
    /// Whether sealing this attempt should submit each accepted artifact to the
    /// bounded report worker. `false` preserves raw capture only and never
    /// creates a report row.
    pub analyze: bool,
    pub participants: Vec<CaptureParticipant>,
}

/// Outcome that can be passed to the realtime gateway: every participant gets a
/// distinct signed token and the exact same permanent, credential-free path.
#[derive(Clone, PartialEq, Eq)]
pub struct CaptureFlushGrant {
    pub participant_id: u64,
    pub flush: FlushCapture,
}

impl std::fmt::Debug for CaptureFlushGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureFlushGrant")
            .field("participant_id", &self.participant_id)
            .field("flush", &self.flush)
            .finish()
    }
}

/// Opaque artifact identity safe to retain in a later report row. It is not a
/// path and cannot be used to derive one without the private manifest store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawArtifactReceipt {
    pub artifact_id: String,
    pub digest_sha256: String,
    pub compressed_bytes: u64,
    pub decompressed_bytes: u64,
    pub record_count: u32,
    /// Private post-publish work for the HTTP adapter. IDs are opaque artifact
    /// handles, not paths; this vector is non-empty only when the FLUSH plan
    /// requested analysis and its server-selected quorum sealed.
    #[serde(skip)]
    pub(crate) analysis_artifact_ids: Vec<String>,
}

/// Opaque, crate-private analysis input. The ingest boundary verifies the
/// private manifest and compressed digest before any decoder sees CLAG bytes;
/// filesystem paths and raw handles never cross into a console/report API.
pub(crate) struct PrivateAnalysisArtifact {
    pub capture_id: [u8; 16],
    pub generation: u64,
    /// Stable only within this capture and derived server-side from private
    /// manifest metadata. It is not a player/account identifier.
    pub participant: String,
    pub digest_sha256: String,
    pub clag_bytes: Vec<u8>,
}

/// Private metadata for an operator-authorized raw artifact action. The
/// `handle` is an opaque server-generated capability label, never a filesystem
/// path or a client filename. This type deliberately does not implement
/// `Serialize`: the Console boundary owns a redacted projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrivateRawArtifact {
    pub handle: String,
    pub capture_id: String,
    pub generation: u64,
    /// Digest is private service metadata used to project retention for the
    /// exact report source. It is never serialized into a Console raw view.
    pub digest_sha256: String,
    pub participant_id: u64,
    pub received_utc_ms: u64,
    pub compressed_bytes: u64,
    pub record_count: u32,
}

/// Raw bytes are only available to the native Console HTTP adapter after an
/// authenticated admin check. They never appear in a JSON response type.
pub(crate) struct PrivateRawArtifactDownload {
    pub bytes: Vec<u8>,
}

/// Bounded private capture index for the Console overview. It holds only
/// aggregate retention accounting and never a raw path, handle, payload, or
/// participant identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrivateCaptureOverview {
    pub capture_id: String,
    pub generation: u64,
    pub raw_artifact_count: u32,
    pub raw_compressed_bytes: u64,
    pub latest_received_utc_ms: u64,
}

#[derive(Clone)]
struct RawRoot {
    root: PathBuf,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct CaptureKey {
    capture_id: [u8; 16],
    generation: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CapturePhase {
    Recording,
    Minting,
    Flushing,
    Sealed,
    Expired,
}

struct CaptureRuntime {
    phase: CapturePhase,
    recording_deadline_utc_ms: u64,
    upload_deadline_utc_ms: u64,
    attempt_id: u64,
    required_uploads: u32,
    analyze: bool,
    expected: HashMap<u64, String>,
    published: HashSet<String>,
    published_artifact_ids: Vec<String>,
}

#[derive(Default)]
struct ServiceState {
    captures: HashMap<CaptureKey, CaptureRuntime>,
    in_flight: HashSet<String>,
}

/// The exact serialised/signed content of an upload capability. It is private
/// to this module and has a redacted Debug implementation through its holder.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UploadClaims {
    version: u8,
    issuer: String,
    audience: String,
    capture_id: String,
    generation: u64,
    attempt_id: u64,
    participant_id: u64,
    session_id: String,
    tenant_id: String,
    match_id: String,
    jti: String,
    expires_utc_ms: u64,
    max_compressed_bytes: u32,
    max_decompressed_bytes: u32,
    content_type: u8,
    content_encoding: u8,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingMarker {
    version: u8,
    grant_fingerprint: String,
    capture_id: String,
    generation: u64,
    attempt_id: u64,
    participant_id: u64,
    expires_utc_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseMarker {
    version: u8,
    grant_fingerprint: String,
    expires_utc_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumedMarker {
    version: u8,
    grant_fingerprint: String,
    outcome: String,
    completed_utc_ms: u64,
}

/// Private on-disk metadata. It names no client path and contains no token or
/// JTI. `grant_fingerprint` is a one-way SHA-256 fingerprint used solely for
/// crash recovery of the durable replay marker.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawArtifactManifest {
    version: u8,
    artifact_id: String,
    capture_id: String,
    generation: u64,
    attempt_id: u64,
    participant_id: u64,
    received_utc_ms: u64,
    digest_sha256: String,
    compressed_bytes: u64,
    decompressed_bytes: u64,
    record_count: u32,
    content_type: String,
    content_encoding: String,
    grant_fingerprint: String,
    report_referenced: bool,
}

pub(crate) struct UploadLease {
    claims: UploadClaims,
    fingerprint: String,
    staging_path: PathBuf,
    compressed_cap: u64,
}

impl std::fmt::Debug for UploadLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadLease")
            .field("capture_id", &self.claims.capture_id)
            .field("generation", &self.claims.generation)
            .field("attempt_id", &self.claims.attempt_id)
            .field("participant_id", &self.claims.participant_id)
            .field("token", &"[redacted]")
            .finish()
    }
}

impl UploadLease {
    pub(crate) fn staging_path(&self) -> &Path {
        &self.staging_path
    }

    pub(crate) const fn compressed_cap(&self) -> u64 {
        self.compressed_cap
    }
}

/// A stateful, node-local ingestion service. Enabling this service in cluster
/// mode without a shared raw store is rejected by configuration validation;
/// that fail-closed boundary prevents a request from landing on a node that
/// cannot see the capture state or replay marker.
pub struct LagDiagnosticsService {
    config: LagDiagnosticsConfig,
    node_id: String,
    keys: BTreeMap<String, Vec<u8>>,
    raw_root: Option<RawRoot>,
    state: Mutex<ServiceState>,
}

impl std::fmt::Debug for LagDiagnosticsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LagDiagnosticsService")
            .field("enabled", &self.config.enabled)
            .field("raw_root_configured", &self.raw_root.is_some())
            .field("key_count", &self.keys.len())
            .finish_non_exhaustive()
    }
}

impl LagDiagnosticsService {
    /// Build a disabled service or validate and prepare the private filesystem
    /// hierarchy for an explicitly enabled deployment.
    pub fn new(config: LagDiagnosticsConfig, node_id: String) -> Result<Self, LagDiagnosticsError> {
        if !config.enabled {
            return Ok(Self {
                config,
                node_id,
                keys: BTreeMap::new(),
                raw_root: None,
                state: Mutex::new(ServiceState::default()),
            });
        }
        let root = config
            .raw_root
            .as_ref()
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .ok_or(LagDiagnosticsError::InvalidRequest)?;
        let mut keys = BTreeMap::new();
        for (key_id, encoded) in &config.upload_hmac_keys {
            let key = URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| LagDiagnosticsError::InvalidRequest)?;
            if key.len() < 32 || !valid_key_id(key_id) {
                return Err(LagDiagnosticsError::InvalidRequest);
            }
            keys.insert(key_id.clone(), key);
        }
        let active_key_id = config
            .active_key_id
            .as_deref()
            .filter(|key_id| keys.contains_key(*key_id))
            .ok_or(LagDiagnosticsError::InvalidRequest)?;
        if !valid_key_id(active_key_id) {
            return Err(LagDiagnosticsError::InvalidRequest);
        }
        let raw_root = RawRoot::open(root)?;
        Ok(Self {
            config,
            node_id,
            keys,
            raw_root: Some(raw_root),
            state: Mutex::new(ServiceState::default()),
        })
    }

    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Register a trusted native START. The realtime gateway calls this only
    /// after it has accepted the capture request; it is never reachable from a
    /// script or HTTP request.
    pub fn register_recording(
        &self,
        capture_id: CaptureId,
        generation: u64,
        recording_deadline_utc_ms: u64,
    ) -> Result<(), LagDiagnosticsError> {
        self.require_enabled()?;
        // CLAG v1 serializes the capture generation in a fixed u32 header
        // field.  Refuse an unrepresentable native generation rather than
        // silently truncating it and later accepting the artifact under a
        // different full-width identity.
        if generation == 0 || generation > u64::from(u32::MAX) || recording_deadline_utc_ms == 0 {
            return Err(LagDiagnosticsError::InvalidRequest);
        }
        let key = CaptureKey::new(capture_id, generation);
        let mut state = self
            .state
            .lock()
            .map_err(|_| LagDiagnosticsError::Storage)?;
        if state.captures.contains_key(&key) {
            return Err(LagDiagnosticsError::InvalidRequest);
        }
        state.captures.insert(
            key,
            CaptureRuntime {
                phase: CapturePhase::Recording,
                recording_deadline_utc_ms,
                upload_deadline_utc_ms: 0,
                attempt_id: 0,
                required_uploads: 0,
                analyze: false,
                expected: HashMap::new(),
                published: HashSet::new(),
                published_artifact_ids: Vec::new(),
            },
        );
        Ok(())
    }

    /// Undo a native START reservation when the realtime gateway rejects the
    /// request before any client could observe it. Once `Flushing` begins this
    /// operation is deliberately refused so durable replay markers remain the
    /// source of truth.
    pub fn discard_recording(
        &self,
        capture_id: CaptureId,
        generation: u64,
    ) -> Result<(), LagDiagnosticsError> {
        self.require_enabled()?;
        let key = CaptureKey::new(capture_id, generation);
        let mut state = self
            .state
            .lock()
            .map_err(|_| LagDiagnosticsError::Storage)?;
        if state
            .captures
            .get(&key)
            .is_some_and(|capture| capture.phase == CapturePhase::Recording)
        {
            state.captures.remove(&key);
            Ok(())
        } else {
            Err(LagDiagnosticsError::NotFlushing)
        }
    }

    /// Open a capture's durable `Flushing` window and mint a unique grant for
    /// every client that actually reached realtime `Recording`. A caller must
    /// issue these exact FLUSH bodies one-to-one; cloning a token across clients
    /// is rejected by both the signed binding and replay marker.
    pub fn open_flush(
        &self,
        plan: CaptureFlushPlan,
        now: TimestampMillis,
    ) -> Result<Vec<CaptureFlushGrant>, LagDiagnosticsError> {
        self.require_enabled()?;
        validate_flush_plan(&plan, &self.config, now)?;
        let key = CaptureKey::new(plan.capture_id, plan.generation);
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| LagDiagnosticsError::Storage)?;
            let capture = state
                .captures
                .get_mut(&key)
                .ok_or(LagDiagnosticsError::NotFlushing)?;
            if capture.phase != CapturePhase::Recording
                || now.unix_millis() >= capture.recording_deadline_utc_ms
            {
                return Err(LagDiagnosticsError::NotFlushing);
            }
            capture.phase = CapturePhase::Minting;
        }

        match self.mint_flush_grants(&plan) {
            Ok(grants) => {
                let expected = grants
                    .iter()
                    .map(|grant| {
                        (
                            grant.participant_id,
                            grant_fingerprint_from_token(&grant.flush.upload_token)
                                .expect("locally minted token has payload"),
                        )
                    })
                    .collect();
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| LagDiagnosticsError::Storage)?;
                let capture = state
                    .captures
                    .get_mut(&key)
                    .ok_or(LagDiagnosticsError::NotFlushing)?;
                capture.phase = CapturePhase::Flushing;
                capture.upload_deadline_utc_ms = plan.upload_deadline_server_utc_ms;
                capture.attempt_id = plan.attempt_id;
                capture.required_uploads = plan.required_uploads;
                capture.analyze = plan.analyze;
                capture.expected = expected;
                capture.published.clear();
                capture.published_artifact_ids.clear();
                Ok(grants)
            }
            Err(error) => {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| LagDiagnosticsError::Storage)?;
                if let Some(capture) = state.captures.get_mut(&key)
                    && capture.phase == CapturePhase::Minting
                {
                    capture.phase = CapturePhase::Recording;
                }
                Err(error)
            }
        }
    }

    /// Admit a request after every cheap HTTP precheck. The route remains
    /// mounted permanently; disabled, wrong-state, foreign, expired, replayed,
    /// and malformed grants all collapse to `Rejected` at its public boundary.
    pub(crate) fn begin_upload(
        &self,
        authorization: Option<&str>,
        content_type: Option<&str>,
        content_encoding: Option<&str>,
        content_length: Option<u64>,
        origin: Option<&str>,
        now: TimestampMillis,
    ) -> Result<UploadLease, LagDiagnosticsError> {
        self.require_enabled()?;
        if origin.is_some_and(|value| !self.cors_origin_allowed(value))
            || content_type != Some(UploadContentType::CitadelLagCapture.as_str())
            || content_encoding != Some(UploadContentEncoding::Gzip.as_str())
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        let token = authorization
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|value| !value.is_empty() && !value.contains(char::is_whitespace))
            .ok_or(LagDiagnosticsError::Rejected)?;
        let claims = self.verify_token(token)?;
        if claims.expires_utc_ms <= now.unix_millis()
            || claims.max_compressed_bytes == 0
            || claims.max_compressed_bytes > self.config.max_compressed_bytes
            || claims.max_decompressed_bytes == 0
            || claims.max_decompressed_bytes > self.config.max_decompressed_bytes
            || claims.content_type != UploadContentType::CitadelLagCapture as u8
            || claims.content_encoding != UploadContentEncoding::Gzip as u8
            || content_length.is_some_and(|length| length > u64::from(claims.max_compressed_bytes))
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        let fingerprint = fingerprint(&claims.jti);
        let key = claims.capture_key()?;
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| LagDiagnosticsError::Storage)?;
            let capture = state
                .captures
                .get(&key)
                .ok_or(LagDiagnosticsError::Rejected)?;
            if capture.phase != CapturePhase::Flushing
                || capture.attempt_id != claims.attempt_id
                || capture.upload_deadline_utc_ms != claims.expires_utc_ms
                || capture.expected.get(&claims.participant_id) != Some(&fingerprint)
                || now.unix_millis() >= capture.upload_deadline_utc_ms
                || state.in_flight.contains(&fingerprint)
                || state.in_flight.len() >= usize::from(self.config.max_concurrent_uploads)
            {
                return Err(LagDiagnosticsError::Rejected);
            }
            state.in_flight.insert(fingerprint.clone());
        }
        let prepared = self.prepare_lease(&claims, &fingerprint, now);
        if prepared.is_err() {
            self.release_in_flight(&fingerprint);
        }
        prepared
    }

    /// Validate a fully streamed private staging file, atomically publish it
    /// with a durable manifest, and consume the grant. This is blocking file
    /// work and is called from the HTTP adapter's bounded blocking task.
    pub(crate) fn validate_and_publish(
        &self,
        lease: UploadLease,
        compressed_bytes: u64,
        digest: [u8; 32],
        now: TimestampMillis,
    ) -> Result<RawArtifactReceipt, LagDiagnosticsError> {
        if compressed_bytes == 0 || compressed_bytes > lease.compressed_cap {
            self.consume_failed(&lease, now, "rejected")?;
            return Err(LagDiagnosticsError::Rejected);
        }
        let validation = validate_gzip_clag(
            &lease.staging_path,
            &lease.claims,
            compressed_bytes,
            self.config.max_decompression_ratio,
        );
        let summary = match validation {
            Ok(summary) => summary,
            Err(()) => {
                self.consume_failed(&lease, now, "rejected")?;
                return Err(LagDiagnosticsError::Rejected);
            }
        };
        let root = self.root()?;
        let capture_label = capture_label_from_claims(&lease.claims)?;
        let raw_path = root.raw_artifact_path(
            &capture_label,
            lease.claims.participant_id,
            lease.claims.attempt_id,
        )?;
        let manifest_path = root.manifest_path(
            &capture_label,
            lease.claims.participant_id,
            lease.claims.attempt_id,
        )?;
        if raw_path.exists() || manifest_path.exists() {
            self.consume_failed(&lease, now, "rejected")?;
            return Err(LagDiagnosticsError::Rejected);
        }
        fsync_file(&lease.staging_path)?;
        let artifact_id = format!("lc1-{}", Uuid::new_v4().simple());
        let manifest = RawArtifactManifest {
            version: 1,
            artifact_id: artifact_id.clone(),
            capture_id: lease.claims.capture_id.clone(),
            generation: lease.claims.generation,
            attempt_id: lease.claims.attempt_id,
            participant_id: lease.claims.participant_id,
            received_utc_ms: now.unix_millis(),
            digest_sha256: hex(&digest),
            compressed_bytes,
            decompressed_bytes: summary.decompressed_bytes,
            record_count: summary.record_count,
            content_type: UploadContentType::CitadelLagCapture.as_str().to_string(),
            content_encoding: UploadContentEncoding::Gzip.as_str().to_string(),
            grant_fingerprint: lease.fingerprint.clone(),
            report_referenced: false,
        };
        let manifest_temp = root.manifest_temp_path(&capture_label)?;
        write_json_new(&manifest_temp, &manifest)?;
        if fs::rename(&lease.staging_path, &raw_path).is_err() {
            let _ = fs::remove_file(&manifest_temp);
            self.consume_failed(&lease, now, "storage_failed")?;
            return Err(LagDiagnosticsError::Storage);
        }
        if fs::rename(&manifest_temp, &manifest_path).is_err() {
            // Recovery deletes an unmanifested raw artifact rather than ever
            // treating it as published.
            self.consume_failed(&lease, now, "storage_failed")?;
            return Err(LagDiagnosticsError::Storage);
        }
        self.consume_marker(&lease.fingerprint, now, "published")?;
        self.release_in_flight(&lease.fingerprint);
        let analysis_artifact_ids = self.mark_published(&lease, &artifact_id, now)?;
        Ok(RawArtifactReceipt {
            artifact_id,
            digest_sha256: hex(&digest),
            compressed_bytes,
            decompressed_bytes: summary.decompressed_bytes,
            record_count: summary.record_count,
            analysis_artifact_ids,
        })
    }

    /// Terminally consume an interrupted or invalid upload attempt. A client
    /// must receive a fresh FLUSH attempt/token; a bearer is never resurrected.
    pub(crate) fn reject_upload(&self, lease: &UploadLease, now: TimestampMillis) {
        let _ = self.consume_failed(lease, now, "rejected");
    }

    /// Return whether an exact Origin may receive CORS response headers. Empty
    /// configuration intentionally means no browser CORS permission at all.
    #[must_use]
    pub fn cors_origin_allowed(&self, origin: &str) -> bool {
        self.config.enabled
            && self
                .config
                .allowed_origins
                .iter()
                .any(|configured| configured == origin)
    }

    /// Recover interrupted staging/leases and ensure no orphaned partial file is
    /// ever presented as published. A restarted node fails closed for active
    /// capture control; this method only reconciles private filesystem state.
    pub fn recover(&self, now: TimestampMillis) -> Result<(), LagDiagnosticsError> {
        if !self.config.enabled {
            return Ok(());
        }
        let root = self.root()?;
        for marker in root.list_files("leases")? {
            let fingerprint = marker
                .file_stem()
                .and_then(|value| value.to_str())
                .filter(|value| valid_fingerprint(value))
                .ok_or(LagDiagnosticsError::Storage)?
                .to_string();
            let _ = fs::remove_file(marker);
            let _ = root.remove_staging_for(&fingerprint);
            let _ = self.consume_marker(&fingerprint, now, "interrupted");
        }
        for marker in root.list_files("pending")? {
            let pending: PendingMarker = read_json(&marker)?;
            let fingerprint = pending.grant_fingerprint;
            if !valid_fingerprint(&fingerprint) {
                return Err(LagDiagnosticsError::Storage);
            }
            let raw = root.raw_artifact_path(
                &pending.capture_id,
                pending.participant_id,
                pending.attempt_id,
            )?;
            let manifest = root.manifest_path(
                &pending.capture_id,
                pending.participant_id,
                pending.attempt_id,
            )?;
            if raw.is_file() && manifest.is_file() {
                self.consume_marker(&fingerprint, now, "published")?;
            } else if pending.expires_utc_ms <= now.unix_millis() {
                let _ = fs::remove_file(raw);
                let _ = fs::remove_file(manifest);
                self.consume_marker(&fingerprint, now, "expired")?;
            }
        }
        root.remove_orphaned_staging()?;
        root.remove_unmanifested_raw()?;
        Ok(())
    }

    /// Expire record or upload windows from trusted server maintenance. Every
    /// outstanding upload capability is durably consumed, so a late body cannot
    /// become valid merely because a stale in-memory lease was reaped.
    pub fn expire_deadlines(&self, now: TimestampMillis) -> Result<usize, LagDiagnosticsError> {
        if !self.config.enabled {
            return Ok(0);
        }
        let mut fingerprints = Vec::new();
        let mut expired = 0;
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| LagDiagnosticsError::Storage)?;
            for capture in state.captures.values_mut() {
                let deadline = match capture.phase {
                    CapturePhase::Recording | CapturePhase::Minting => {
                        capture.recording_deadline_utc_ms
                    }
                    CapturePhase::Flushing => capture.upload_deadline_utc_ms,
                    CapturePhase::Sealed | CapturePhase::Expired => continue,
                };
                if deadline <= now.unix_millis() {
                    capture.phase = CapturePhase::Expired;
                    fingerprints.extend(
                        capture
                            .expected
                            .values()
                            .filter(|fingerprint| !capture.published.contains(*fingerprint))
                            .cloned(),
                    );
                    expired += 1;
                }
            }
        }
        for fingerprint in fingerprints {
            self.consume_marker(&fingerprint, now, "expired")?;
            self.release_in_flight(&fingerprint);
        }
        Ok(expired)
    }

    /// Remove expired, unreferenced raw artifacts. This compatibility wrapper
    /// reports only a count; application composition should instead use
    /// [`Self::expired_retention_candidates`] and project each digest as
    /// unavailable before calling [`Self::remove_retention_candidate`].
    pub fn enforce_retention(&self, now: TimestampMillis) -> Result<usize, LagDiagnosticsError> {
        let candidates = self.expired_retention_candidates(now)?;
        let mut removed = 0;
        for candidate in &candidates {
            if self.remove_retention_candidate(candidate)? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Identify expired raw artifacts without exposing their paths or deleting
    /// any bytes. The application marks the exact `(capture, generation,
    /// digest)` report projection unavailable before it removes one of these
    /// candidates, keeping persistence conservative across a storage failure.
    pub(crate) fn expired_retention_candidates(
        &self,
        now: TimestampMillis,
    ) -> Result<Vec<PrivateRawArtifact>, LagDiagnosticsError> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }
        let root = self.root()?;
        let cutoff = now
            .unix_millis()
            .saturating_sub(self.config.retention_hours.saturating_mul(3_600_000));
        let mut candidates = Vec::new();
        for manifest_path in root.recursive_files("manifests")? {
            let manifest: RawArtifactManifest = match read_json(&manifest_path) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if manifest.report_referenced || manifest.received_utc_ms > cutoff {
                continue;
            }
            candidates.push(raw_descriptor(&manifest)?);
        }
        candidates.sort_by(|left, right| left.handle.cmp(&right.handle));
        Ok(candidates)
    }

    /// Remove exactly one candidate previously obtained from
    /// [`Self::expired_retention_candidates`]. A missing artifact means a
    /// concurrent delete already made the raw evidence unavailable; all other
    /// corruption fails closed and leaves the already-conservative report
    /// projection unchanged.
    pub(crate) fn remove_retention_candidate(
        &self,
        candidate: &PrivateRawArtifact,
    ) -> Result<bool, LagDiagnosticsError> {
        self.require_enabled()?;
        let (root, manifest) = match self.find_private_raw_artifact(&candidate.handle) {
            Ok(value) => value,
            Err(LagDiagnosticsError::Rejected) => return Ok(false),
            Err(error) => return Err(error),
        };
        let descriptor = raw_descriptor(&manifest)?;
        if descriptor.capture_id != candidate.capture_id
            || descriptor.generation != candidate.generation
            || descriptor.digest_sha256 != candidate.digest_sha256
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        let raw_path = root.raw_artifact_path(
            &manifest.capture_id,
            manifest.participant_id,
            manifest.attempt_id,
        )?;
        match fs::symlink_metadata(&raw_path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(LagDiagnosticsError::Storage);
                }
                fs::remove_file(&raw_path).map_err(|_| LagDiagnosticsError::Storage)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(LagDiagnosticsError::Storage),
        }
        let manifest_path = root.manifest_path(
            &manifest.capture_id,
            manifest.participant_id,
            manifest.attempt_id,
        )?;
        let metadata = match fs::symlink_metadata(&manifest_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
            Err(_) => return Err(LagDiagnosticsError::Storage),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(LagDiagnosticsError::Storage);
        }
        fs::remove_file(manifest_path).map_err(|_| LagDiagnosticsError::Storage)?;
        Ok(true)
    }

    /// Load one retained artifact for the internal analysis worker only. This
    /// scans private manifests by opaque artifact id, verifies the on-disk
    /// compressed digest, and returns bounded decompressed CLAG bytes without
    /// exposing a raw filename or filesystem path to callers.
    pub(crate) fn load_private_analysis_artifact(
        &self,
        artifact_id: &str,
    ) -> Result<PrivateAnalysisArtifact, LagDiagnosticsError> {
        self.require_enabled()?;
        if !valid_analysis_artifact_id(artifact_id) {
            return Err(LagDiagnosticsError::Rejected);
        }
        let root = self.root()?;
        let mut selected = None;
        for manifest_path in root.recursive_files("manifests")? {
            let manifest: RawArtifactManifest = read_json(&manifest_path)?;
            if manifest.artifact_id == artifact_id && selected.replace(manifest).is_some() {
                return Err(LagDiagnosticsError::Storage);
            }
        }
        let manifest = selected.ok_or(LagDiagnosticsError::Rejected)?;
        let capture =
            decode_capture_label(&manifest.capture_id).ok_or(LagDiagnosticsError::Storage)?;
        let raw_path = root.raw_artifact_path(
            &manifest.capture_id,
            manifest.participant_id,
            manifest.attempt_id,
        )?;
        let metadata =
            fs::symlink_metadata(&raw_path).map_err(|_| LagDiagnosticsError::Rejected)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() != manifest.compressed_bytes
            || metadata.len() > u64::from(self.config.max_compressed_bytes)
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        let compressed = fs::read(&raw_path).map_err(|_| LagDiagnosticsError::Rejected)?;
        let digest = hex(&Sha256::digest(&compressed));
        if digest != manifest.digest_sha256 {
            return Err(LagDiagnosticsError::Rejected);
        }
        let mut decoder = MultiGzDecoder::new(&compressed[..]);
        let mut clag_bytes = Vec::with_capacity(
            usize::try_from(
                manifest
                    .decompressed_bytes
                    .min(u64::from(self.config.max_decompressed_bytes)),
            )
            .map_err(|_| LagDiagnosticsError::Rejected)?,
        );
        let mut chunk = [0_u8; 8 * 1024];
        loop {
            let read = decoder
                .read(&mut chunk)
                .map_err(|_| LagDiagnosticsError::Rejected)?;
            if read == 0 {
                break;
            }
            if clag_bytes.len().saturating_add(read)
                > usize::try_from(self.config.max_decompressed_bytes)
                    .map_err(|_| LagDiagnosticsError::Rejected)?
            {
                return Err(LagDiagnosticsError::Rejected);
            }
            clag_bytes.extend_from_slice(&chunk[..read]);
        }
        if u64::try_from(clag_bytes.len()).map_err(|_| LagDiagnosticsError::Rejected)?
            != manifest.decompressed_bytes
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        Ok(PrivateAnalysisArtifact {
            capture_id: capture,
            generation: manifest.generation,
            participant: self.analysis_participant_pseudonym(&manifest)?,
            digest_sha256: manifest.digest_sha256,
            clag_bytes,
        })
    }

    /// List retained artifacts for one validated capture without disclosing
    /// filesystem names. The caller must still apply Console role checks before
    /// projecting this metadata to an operator response.
    pub(crate) fn list_private_raw_artifacts(
        &self,
        capture_id: &str,
        after_handle: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PrivateRawArtifact>, LagDiagnosticsError> {
        self.require_enabled()?;
        if !valid_capture_label(capture_id) {
            return Err(LagDiagnosticsError::Rejected);
        }
        let root = self.root()?;
        let mut result = Vec::new();
        for manifest_path in root.recursive_files("manifests")? {
            let manifest: RawArtifactManifest = read_json(&manifest_path)?;
            if manifest.capture_id != capture_id {
                continue;
            }
            let descriptor = raw_descriptor(&manifest)?;
            let raw_path = root.raw_artifact_path(
                &manifest.capture_id,
                manifest.participant_id,
                manifest.attempt_id,
            )?;
            let metadata = match fs::symlink_metadata(&raw_path) {
                Ok(metadata) => metadata,
                // A delete may have completed its terminal raw removal just
                // before best-effort manifest cleanup. Such a manifest names
                // no downloadable/regenerable artifact and is omitted.
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => return Err(LagDiagnosticsError::Storage),
            };
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || metadata.len() != manifest.compressed_bytes
            {
                return Err(LagDiagnosticsError::Storage);
            }
            result.push(descriptor);
        }
        result.sort_by(|left, right| left.handle.cmp(&right.handle));
        Ok(result
            .into_iter()
            .filter(|artifact| after_handle.is_none_or(|after| artifact.handle.as_str() > after))
            // Console asks for at most page-size plus one to determine whether
            // a stable keyset cursor exists; this private helper is never a
            // public unbounded listing surface.
            .take(limit.clamp(1, 101))
            .collect())
    }

    /// List capture-level raw-retention aggregates in capture-id keyset order.
    /// This remains inside private storage so the Console can redact retention
    /// details for viewers without ever receiving filesystem locations.
    pub(crate) fn list_private_capture_overviews(
        &self,
        after_capture_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PrivateCaptureOverview>, LagDiagnosticsError> {
        self.require_enabled()?;
        if after_capture_id.is_some_and(|value| !valid_capture_label(value)) {
            return Err(LagDiagnosticsError::Rejected);
        }
        let root = self.root()?;
        let mut captures = BTreeMap::<String, PrivateCaptureOverview>::new();
        for manifest_path in root.recursive_files("manifests")? {
            let manifest: RawArtifactManifest = read_json(&manifest_path)?;
            let descriptor = raw_descriptor(&manifest)?;
            let raw_path = root.raw_artifact_path(
                &manifest.capture_id,
                manifest.participant_id,
                manifest.attempt_id,
            )?;
            let metadata = match fs::symlink_metadata(&raw_path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => return Err(LagDiagnosticsError::Storage),
            };
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || metadata.len() != descriptor.compressed_bytes
            {
                return Err(LagDiagnosticsError::Storage);
            }
            let entry =
                captures
                    .entry(descriptor.capture_id.clone())
                    .or_insert(PrivateCaptureOverview {
                        capture_id: descriptor.capture_id,
                        generation: descriptor.generation,
                        raw_artifact_count: 0,
                        raw_compressed_bytes: 0,
                        latest_received_utc_ms: 0,
                    });
            entry.generation = entry.generation.max(descriptor.generation);
            entry.raw_artifact_count = entry.raw_artifact_count.saturating_add(1);
            entry.raw_compressed_bytes = entry
                .raw_compressed_bytes
                .saturating_add(descriptor.compressed_bytes);
            entry.latest_received_utc_ms =
                entry.latest_received_utc_ms.max(descriptor.received_utc_ms);
        }
        Ok(captures
            .into_values()
            .filter(|capture| {
                after_capture_id.is_none_or(|after| capture.capture_id.as_str() > after)
            })
            .take(limit.clamp(1, 101))
            .collect())
    }

    /// Read an exact opaque raw handle after checking its manifest binding,
    /// regular-file type, configured byte bound, and digest. The caller is
    /// responsible for role enforcement and returns bytes as an attachment,
    /// never JSON.
    pub(crate) fn download_private_raw_artifact(
        &self,
        capture_id: &str,
        handle: &str,
    ) -> Result<PrivateRawArtifactDownload, LagDiagnosticsError> {
        let (root, manifest) = self.find_private_raw_artifact(handle)?;
        if manifest.capture_id != capture_id || !valid_capture_label(capture_id) {
            return Err(LagDiagnosticsError::Rejected);
        }
        let raw_path = root.raw_artifact_path(
            &manifest.capture_id,
            manifest.participant_id,
            manifest.attempt_id,
        )?;
        let metadata =
            fs::symlink_metadata(&raw_path).map_err(|_| LagDiagnosticsError::Rejected)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() != manifest.compressed_bytes
            || metadata.len() > u64::from(self.config.max_compressed_bytes)
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        let bytes = fs::read(&raw_path).map_err(|_| LagDiagnosticsError::Rejected)?;
        if hex(&Sha256::digest(&bytes)) != manifest.digest_sha256 {
            return Err(LagDiagnosticsError::Rejected);
        }
        Ok(PrivateRawArtifactDownload { bytes })
    }

    /// Check that a handle still names a regular retained artifact for exactly
    /// one capture. This metadata-only operation avoids an unnecessary raw
    /// read before the analysis worker performs its single verified load.
    pub(crate) fn inspect_private_raw_artifact(
        &self,
        capture_id: &str,
        handle: &str,
    ) -> Result<PrivateRawArtifact, LagDiagnosticsError> {
        let (root, manifest) = self.find_private_raw_artifact(handle)?;
        if manifest.capture_id != capture_id || !valid_capture_label(capture_id) {
            return Err(LagDiagnosticsError::Rejected);
        }
        let raw_path = root.raw_artifact_path(
            &manifest.capture_id,
            manifest.participant_id,
            manifest.attempt_id,
        )?;
        let metadata =
            fs::symlink_metadata(&raw_path).map_err(|_| LagDiagnosticsError::Rejected)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() != manifest.compressed_bytes
            || metadata.len() > u64::from(self.config.max_compressed_bytes)
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        raw_descriptor(&manifest)
    }

    /// Permanently remove an exact opaque raw artifact. The manifest is removed
    /// too, so the private loader can no longer regenerate a report from it.
    /// Callers retain the returned capture/generation metadata only long enough
    /// to update the report availability projection.
    pub(crate) fn delete_private_raw_artifact(
        &self,
        capture_id: &str,
        handle: &str,
    ) -> Result<PrivateRawArtifact, LagDiagnosticsError> {
        let (root, manifest) = self.find_private_raw_artifact(handle)?;
        if manifest.capture_id != capture_id || !valid_capture_label(capture_id) {
            return Err(LagDiagnosticsError::Rejected);
        }
        let descriptor = raw_descriptor(&manifest)?;
        let raw_path = root.raw_artifact_path(
            &manifest.capture_id,
            manifest.participant_id,
            manifest.attempt_id,
        )?;
        let raw_metadata =
            fs::symlink_metadata(&raw_path).map_err(|_| LagDiagnosticsError::Rejected)?;
        if raw_metadata.file_type().is_symlink() || !raw_metadata.is_file() {
            return Err(LagDiagnosticsError::Rejected);
        }
        let manifest_path = root.manifest_path(
            &manifest.capture_id,
            manifest.participant_id,
            manifest.attempt_id,
        )?;
        let manifest_metadata =
            fs::symlink_metadata(&manifest_path).map_err(|_| LagDiagnosticsError::Rejected)?;
        if manifest_metadata.file_type().is_symlink() || !manifest_metadata.is_file() {
            return Err(LagDiagnosticsError::Rejected);
        }
        fs::remove_file(&raw_path).map_err(|_| LagDiagnosticsError::Storage)?;
        // A raw deletion is terminal for analysis even if a subsequent
        // best-effort manifest cleanup loses a storage race. Return the
        // descriptor so the Console can immediately mark report availability
        // false; recovery/retention later discard an unreachable manifest.
        let _ = fs::remove_file(&manifest_path);
        Ok(descriptor)
    }

    fn find_private_raw_artifact(
        &self,
        handle: &str,
    ) -> Result<(RawRoot, RawArtifactManifest), LagDiagnosticsError> {
        self.require_enabled()?;
        if !valid_analysis_artifact_id(handle) {
            return Err(LagDiagnosticsError::Rejected);
        }
        let root = self.root()?.clone();
        let mut selected = None;
        for manifest_path in root.recursive_files("manifests")? {
            let manifest: RawArtifactManifest = read_json(&manifest_path)?;
            if manifest.artifact_id == handle && selected.replace(manifest).is_some() {
                return Err(LagDiagnosticsError::Storage);
            }
        }
        let manifest = selected.ok_or(LagDiagnosticsError::Rejected)?;
        raw_descriptor(&manifest)?;
        Ok((root, manifest))
    }

    fn mint_flush_grants(
        &self,
        plan: &CaptureFlushPlan,
    ) -> Result<Vec<CaptureFlushGrant>, LagDiagnosticsError> {
        let key_id = self
            .config
            .active_key_id
            .as_deref()
            .ok_or(LagDiagnosticsError::InvalidRequest)?;
        let mut issued: Vec<(CaptureFlushGrant, String)> =
            Vec::with_capacity(plan.participants.len());
        let root = self.root()?;
        for participant in &plan.participants {
            let claims = UploadClaims {
                version: 1,
                issuer: self.node_id.clone(),
                audience: TOKEN_AUDIENCE.to_string(),
                capture_id: capture_label(plan.capture_id),
                generation: plan.generation,
                attempt_id: plan.attempt_id,
                participant_id: participant.participant_id,
                session_id: participant.session_id.clone(),
                tenant_id: participant.tenant_id.clone(),
                match_id: participant.match_id.clone(),
                jti: random_jti()?,
                expires_utc_ms: plan.upload_deadline_server_utc_ms,
                max_compressed_bytes: plan.max_compressed_bytes,
                max_decompressed_bytes: self.config.max_decompressed_bytes,
                content_type: UploadContentType::CitadelLagCapture as u8,
                content_encoding: UploadContentEncoding::Gzip as u8,
            };
            let token = self.sign_token(key_id, &claims)?;
            let fingerprint = fingerprint(&claims.jti);
            let pending = PendingMarker {
                version: 1,
                grant_fingerprint: fingerprint,
                capture_id: claims.capture_id.clone(),
                generation: claims.generation,
                attempt_id: claims.attempt_id,
                participant_id: claims.participant_id,
                expires_utc_ms: claims.expires_utc_ms,
            };
            if let Err(error) =
                write_json_new(&root.pending_path(&pending.grant_fingerprint)?, &pending)
            {
                for (_, prior_fingerprint) in &issued {
                    let _ = fs::remove_file(root.pending_path(prior_fingerprint)?);
                }
                return Err(error);
            }
            issued.push((
                CaptureFlushGrant {
                    participant_id: participant.participant_id,
                    flush: FlushCapture {
                        capture_id: plan.capture_id,
                        generation: plan.generation,
                        attempt_id: plan.attempt_id,
                        upload_deadline_server_utc_ms: plan.upload_deadline_server_utc_ms,
                        max_compressed_bytes: plan.max_compressed_bytes,
                        content_type: UploadContentType::CitadelLagCapture,
                        content_encoding: UploadContentEncoding::Gzip,
                        upload_path: DIAGNOSTICS_UPLOAD_PATH.to_string(),
                        upload_token: token,
                    },
                },
                pending.grant_fingerprint,
            ));
        }
        Ok(issued.into_iter().map(|(grant, _)| grant).collect())
    }

    fn sign_token(
        &self,
        key_id: &str,
        claims: &UploadClaims,
    ) -> Result<String, LagDiagnosticsError> {
        let key = self
            .keys
            .get(key_id)
            .ok_or(LagDiagnosticsError::InvalidRequest)?;
        let payload = serde_json::to_vec(claims).map_err(|_| LagDiagnosticsError::Storage)?;
        let payload = URL_SAFE_NO_PAD.encode(payload);
        let signing_input = format!("{TOKEN_VERSION}.{key_id}.{payload}");
        let mut mac = HmacSha256::new_from_slice(key).map_err(|_| LagDiagnosticsError::Storage)?;
        mac.update(signing_input.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!("{signing_input}.{signature}"))
    }

    fn verify_token(&self, token: &str) -> Result<UploadClaims, LagDiagnosticsError> {
        let mut parts = token.split('.');
        let (Some(version), Some(key_id), Some(payload), Some(signature), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(LagDiagnosticsError::Rejected);
        };
        if version != TOKEN_VERSION || !valid_key_id(key_id) || payload.len() > 3_072 {
            return Err(LagDiagnosticsError::Rejected);
        }
        let key = self.keys.get(key_id).ok_or(LagDiagnosticsError::Rejected)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| LagDiagnosticsError::Rejected)?;
        let mut mac = HmacSha256::new_from_slice(key).map_err(|_| LagDiagnosticsError::Storage)?;
        mac.update(format!("{version}.{key_id}.{payload}").as_bytes());
        mac.verify_slice(&signature)
            .map_err(|_| LagDiagnosticsError::Rejected)?;
        let claims: UploadClaims = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(payload)
                .map_err(|_| LagDiagnosticsError::Rejected)?,
        )
        .map_err(|_| LagDiagnosticsError::Rejected)?;
        if claims.version != 1
            || claims.issuer != self.node_id
            || claims.audience != TOKEN_AUDIENCE
            || !valid_capture_label(&claims.capture_id)
            || claims.generation == 0
            || claims.attempt_id == 0
            || claims.participant_id == 0
            || !valid_bound_id(&claims.session_id)
            || !valid_bound_id(&claims.tenant_id)
            || !valid_bound_id(&claims.match_id)
            || !valid_jti(&claims.jti)
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        Ok(claims)
    }

    fn prepare_lease(
        &self,
        claims: &UploadClaims,
        fingerprint: &str,
        now: TimestampMillis,
    ) -> Result<UploadLease, LagDiagnosticsError> {
        let root = self.root()?;
        let pending: PendingMarker = read_json(&root.pending_path(fingerprint)?)?;
        if pending.version != 1
            || pending.grant_fingerprint != fingerprint
            || pending.capture_id != claims.capture_id
            || pending.generation != claims.generation
            || pending.attempt_id != claims.attempt_id
            || pending.participant_id != claims.participant_id
            || pending.expires_utc_ms != claims.expires_utc_ms
            || root.consumed_path(fingerprint)?.exists()
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        if root.raw_size_bytes()?
            > self
                .config
                .max_raw_bytes
                .saturating_sub(u64::from(claims.max_compressed_bytes))
        {
            return Err(LagDiagnosticsError::Rejected);
        }
        write_json_new(
            &root.lease_path(fingerprint)?,
            &LeaseMarker {
                version: 1,
                grant_fingerprint: fingerprint.to_string(),
                expires_utc_ms: now.unix_millis().saturating_add(60_000),
            },
        )?;
        let staging_path = root.staging_path(fingerprint)?;
        let staging = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging_path)
            .map_err(|_| LagDiagnosticsError::Rejected)?;
        staging
            .sync_all()
            .map_err(|_| LagDiagnosticsError::Storage)?;
        Ok(UploadLease {
            claims: claims.clone(),
            fingerprint: fingerprint.to_string(),
            staging_path,
            compressed_cap: u64::from(claims.max_compressed_bytes),
        })
    }

    fn consume_failed(
        &self,
        lease: &UploadLease,
        now: TimestampMillis,
        outcome: &str,
    ) -> Result<(), LagDiagnosticsError> {
        let _ = fs::remove_file(&lease.staging_path);
        self.consume_marker(&lease.fingerprint, now, outcome)?;
        self.release_in_flight(&lease.fingerprint);
        Ok(())
    }

    fn consume_marker(
        &self,
        fingerprint: &str,
        now: TimestampMillis,
        outcome: &str,
    ) -> Result<(), LagDiagnosticsError> {
        let root = self.root()?;
        let consumed = root.consumed_path(fingerprint)?;
        if !consumed.exists() {
            write_json_new(
                &consumed,
                &ConsumedMarker {
                    version: 1,
                    grant_fingerprint: fingerprint.to_string(),
                    outcome: outcome.to_string(),
                    completed_utc_ms: now.unix_millis(),
                },
            )?;
        }
        let _ = fs::remove_file(root.pending_path(fingerprint)?);
        let _ = fs::remove_file(root.lease_path(fingerprint)?);
        Ok(())
    }

    fn mark_published(
        &self,
        lease: &UploadLease,
        artifact_id: &str,
        now: TimestampMillis,
    ) -> Result<Vec<String>, LagDiagnosticsError> {
        let key = lease.claims.capture_key()?;
        let to_consume = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| LagDiagnosticsError::Storage)?;
            let capture = state
                .captures
                .get_mut(&key)
                .ok_or(LagDiagnosticsError::NotFlushing)?;
            if !capture
                .expected
                .values()
                .any(|fingerprint| fingerprint == &lease.fingerprint)
            {
                return Err(LagDiagnosticsError::Rejected);
            }
            capture.published.insert(lease.fingerprint.clone());
            capture.published_artifact_ids.push(artifact_id.to_string());
            if capture.published.len() < capture.required_uploads as usize {
                (Vec::new(), Vec::new())
            } else {
                capture.phase = CapturePhase::Sealed;
                let to_consume = capture
                    .expected
                    .values()
                    .filter(|fingerprint| !capture.published.contains(*fingerprint))
                    .cloned()
                    .collect();
                let analysis = if capture.analyze {
                    capture.published_artifact_ids.clone()
                } else {
                    Vec::new()
                };
                (to_consume, analysis)
            }
        };
        for fingerprint in to_consume.0 {
            self.consume_marker(&fingerprint, now, "quorum_sealed")?;
            self.release_in_flight(&fingerprint);
        }
        Ok(to_consume.1)
    }

    fn release_in_flight(&self, fingerprint: &str) {
        if let Ok(mut state) = self.state.lock() {
            state.in_flight.remove(fingerprint);
        }
    }

    fn require_enabled(&self) -> Result<(), LagDiagnosticsError> {
        self.config
            .enabled
            .then_some(())
            .ok_or(LagDiagnosticsError::Disabled)
    }

    fn root(&self) -> Result<&RawRoot, LagDiagnosticsError> {
        self.raw_root.as_ref().ok_or(LagDiagnosticsError::Disabled)
    }
}

impl CaptureKey {
    const fn new(capture_id: CaptureId, generation: u64) -> Self {
        Self {
            capture_id: capture_id.bytes(),
            generation,
        }
    }
}

impl UploadClaims {
    fn capture_key(&self) -> Result<CaptureKey, LagDiagnosticsError> {
        let bytes = decode_capture_label(&self.capture_id).ok_or(LagDiagnosticsError::Rejected)?;
        Ok(CaptureKey {
            capture_id: bytes,
            generation: self.generation,
        })
    }
}

impl RawRoot {
    fn open(root: PathBuf) -> Result<Self, LagDiagnosticsError> {
        fs::create_dir_all(&root).map_err(|_| LagDiagnosticsError::Storage)?;
        reject_symlink(&root)?;
        let root = fs::canonicalize(root).map_err(|_| LagDiagnosticsError::Storage)?;
        let value = Self { root };
        for name in [
            "staging",
            "raw",
            "manifests",
            "pending",
            "consumed",
            "leases",
        ] {
            value.dir(name)?;
        }
        Ok(value)
    }

    fn dir(&self, name: &str) -> Result<PathBuf, LagDiagnosticsError> {
        let path = safe_child(&self.root, name)?;
        fs::create_dir_all(&path).map_err(|_| LagDiagnosticsError::Storage)?;
        reject_symlink(&path)?;
        Ok(path)
    }

    fn capture_dir(&self, top: &str, capture: &str) -> Result<PathBuf, LagDiagnosticsError> {
        let top = self.dir(top)?;
        let path = safe_child(&top, capture)?;
        fs::create_dir_all(&path).map_err(|_| LagDiagnosticsError::Storage)?;
        reject_symlink(&path)?;
        Ok(path)
    }

    fn pending_path(&self, fingerprint: &str) -> Result<PathBuf, LagDiagnosticsError> {
        safe_child(&self.dir("pending")?, &format!("{fingerprint}.json"))
    }

    fn consumed_path(&self, fingerprint: &str) -> Result<PathBuf, LagDiagnosticsError> {
        safe_child(&self.dir("consumed")?, &format!("{fingerprint}.json"))
    }

    fn lease_path(&self, fingerprint: &str) -> Result<PathBuf, LagDiagnosticsError> {
        safe_child(&self.dir("leases")?, &format!("{fingerprint}.json"))
    }

    fn staging_path(&self, fingerprint: &str) -> Result<PathBuf, LagDiagnosticsError> {
        safe_child(&self.dir("staging")?, &format!("{fingerprint}.partial"))
    }

    fn raw_artifact_path(
        &self,
        capture: &str,
        participant: u64,
        attempt: u64,
    ) -> Result<PathBuf, LagDiagnosticsError> {
        safe_child(
            &self.capture_dir("raw", capture)?,
            &format!("p{participant}-a{attempt}.clag.gz"),
        )
    }

    fn manifest_path(
        &self,
        capture: &str,
        participant: u64,
        attempt: u64,
    ) -> Result<PathBuf, LagDiagnosticsError> {
        safe_child(
            &self.capture_dir("manifests", capture)?,
            &format!("p{participant}-a{attempt}.json"),
        )
    }

    fn manifest_temp_path(&self, capture: &str) -> Result<PathBuf, LagDiagnosticsError> {
        safe_child(
            &self.capture_dir("manifests", capture)?,
            &format!(".{}.tmp", Uuid::new_v4().simple()),
        )
    }

    fn list_files(&self, name: &str) -> Result<Vec<PathBuf>, LagDiagnosticsError> {
        let mut result = Vec::new();
        for entry in fs::read_dir(self.dir(name)?).map_err(|_| LagDiagnosticsError::Storage)? {
            let entry = entry.map_err(|_| LagDiagnosticsError::Storage)?;
            let type_ = entry
                .file_type()
                .map_err(|_| LagDiagnosticsError::Storage)?;
            if type_.is_symlink() {
                return Err(LagDiagnosticsError::Storage);
            }
            if type_.is_file() {
                result.push(entry.path());
            }
        }
        Ok(result)
    }

    fn recursive_files(&self, name: &str) -> Result<Vec<PathBuf>, LagDiagnosticsError> {
        let mut files = Vec::new();
        collect_regular_files(&self.dir(name)?, &mut files)?;
        Ok(files)
    }

    fn raw_size_bytes(&self) -> Result<u64, LagDiagnosticsError> {
        self.recursive_files("raw")?
            .into_iter()
            .try_fold(0_u64, |sum, path| {
                let length = fs::metadata(path)
                    .map_err(|_| LagDiagnosticsError::Storage)?
                    .len();
                Ok(sum.saturating_add(length))
            })
    }

    fn remove_staging_for(&self, fingerprint: &str) -> Result<(), LagDiagnosticsError> {
        let path = self.staging_path(fingerprint)?;
        if path.exists() {
            fs::remove_file(path).map_err(|_| LagDiagnosticsError::Storage)?;
        }
        Ok(())
    }

    fn remove_orphaned_staging(&self) -> Result<(), LagDiagnosticsError> {
        for path in self.list_files("staging")? {
            fs::remove_file(path).map_err(|_| LagDiagnosticsError::Storage)?;
        }
        Ok(())
    }

    fn remove_unmanifested_raw(&self) -> Result<(), LagDiagnosticsError> {
        for raw in self.recursive_files("raw")? {
            let relative = raw
                .strip_prefix(self.dir("raw")?)
                .map_err(|_| LagDiagnosticsError::Storage)?;
            let name = relative
                .file_name()
                .and_then(|value| value.to_str())
                .and_then(|value| value.strip_suffix(".clag.gz"))
                .ok_or(LagDiagnosticsError::Storage)?;
            let manifest = self
                .dir("manifests")?
                .join(relative.parent().ok_or(LagDiagnosticsError::Storage)?)
                .join(format!("{name}.json"));
            if !manifest.is_file() {
                fs::remove_file(raw).map_err(|_| LagDiagnosticsError::Storage)?;
            }
        }
        Ok(())
    }
}

fn validate_flush_plan(
    plan: &CaptureFlushPlan,
    config: &LagDiagnosticsConfig,
    now: TimestampMillis,
) -> Result<(), LagDiagnosticsError> {
    if plan.generation == 0
        || plan.attempt_id == 0
        || plan.upload_deadline_server_utc_ms <= now.unix_millis()
        || plan.max_compressed_bytes == 0
        || plan.max_compressed_bytes > config.max_compressed_bytes
        || plan.required_uploads == 0
        || usize::try_from(plan.required_uploads)
            .ok()
            .is_none_or(|required| required > plan.participants.len())
        || plan.participants.is_empty()
        || plan.participants.len() > 16_384
    {
        return Err(LagDiagnosticsError::InvalidRequest);
    }
    let mut ids = HashSet::new();
    for participant in &plan.participants {
        if participant.participant_id == 0
            || !ids.insert(participant.participant_id)
            || !valid_bound_id(&participant.session_id)
            || !valid_bound_id(&participant.tenant_id)
            || !valid_bound_id(&participant.match_id)
        {
            return Err(LagDiagnosticsError::InvalidRequest);
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct ClagSummary {
    decompressed_bytes: u64,
    record_count: u32,
}

fn validate_gzip_clag(
    staging_path: &Path,
    claims: &UploadClaims,
    compressed_bytes: u64,
    ratio: u32,
) -> Result<ClagSummary, ()> {
    let file = File::open(staging_path).map_err(|_| ())?;
    let mut reader = BoundedReader::new(
        MultiGzDecoder::new(file),
        u64::from(claims.max_decompressed_bytes),
    );
    let mut header = [0_u8; CLAG_HEADER_BYTES];
    reader.read_exact(&mut header).map_err(|_| ())?;
    if &header[0..4] != b"CLAG"
        || u16::from_be_bytes([header[4], header[5]]) != CLAG_VERSION
        || usize::from(u16::from_be_bytes([header[6], header[7]])) != CLAG_HEADER_BYTES
        || usize::from(u16::from_be_bytes([header[8], header[9]])) != CLAG_RECORD_BYTES
        || u16::from_be_bytes([header[10], header[11]]) & !CLAG_KNOWN_FLAGS != 0
        || header[48..64] != decode_capture_label(&claims.capture_id).ok_or(())?[..]
        || u32::from_be_bytes([header[64], header[65], header[66], header[67]])
            != u32::try_from(claims.generation).map_err(|_| ())?
    {
        return Err(());
    }
    let record_count = u32::from_be_bytes([header[12], header[13], header[14], header[15]]);
    let required = (CLAG_HEADER_BYTES as u64)
        .checked_add(u64::from(record_count).saturating_mul(CLAG_RECORD_BYTES as u64))
        .ok_or(())?;
    if required > u64::from(claims.max_decompressed_bytes) {
        return Err(());
    }
    let mut record = [0_u8; CLAG_RECORD_BYTES];
    for _ in 0..record_count {
        reader.read_exact(&mut record).map_err(|_| ())?;
    }
    let mut trailing = [0_u8; 1];
    if reader.read(&mut trailing).map_err(|_| ())? != 0 {
        return Err(());
    }
    let decompressed_bytes = reader.seen;
    if decompressed_bytes != required
        || decompressed_bytes > compressed_bytes.saturating_mul(u64::from(ratio))
    {
        return Err(());
    }
    Ok(ClagSummary {
        decompressed_bytes,
        record_count,
    })
}

struct BoundedReader<R> {
    inner: R,
    seen: u64,
    limit: u64,
}

impl<R> BoundedReader<R> {
    const fn new(inner: R, limit: u64) -> Self {
        Self {
            inner,
            seen: 0,
            limit,
        }
    }
}

impl<R: Read> Read for BoundedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.limit.saturating_sub(self.seen);
        if remaining == 0 && !buf.is_empty() {
            let mut probe = [0_u8; 1];
            if self.inner.read(&mut probe)? != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "decompression limit",
                ));
            }
            return Ok(0);
        }
        let allowed = buf.len().min(remaining as usize);
        let count = self.inner.read(&mut buf[..allowed])?;
        self.seen = self.seen.saturating_add(count as u64);
        Ok(count)
    }
}

fn write_json_new<T: Serialize>(path: &Path, value: &T) -> Result<(), LagDiagnosticsError> {
    if let Some(parent) = path.parent() {
        reject_symlink(parent)?;
    }
    let bytes = serde_json::to_vec(value).map_err(|_| LagDiagnosticsError::Storage)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| LagDiagnosticsError::Rejected)?;
    file.write_all(&bytes)
        .map_err(|_| LagDiagnosticsError::Storage)?;
    file.sync_all().map_err(|_| LagDiagnosticsError::Storage)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, LagDiagnosticsError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(LagDiagnosticsError::Storage);
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(LagDiagnosticsError::Rejected);
        }
        Err(_) => return Err(LagDiagnosticsError::Storage),
    }
    let bytes = fs::read(path).map_err(|_| LagDiagnosticsError::Rejected)?;
    serde_json::from_slice(&bytes).map_err(|_| LagDiagnosticsError::Rejected)
}

fn fsync_file(path: &Path) -> Result<(), LagDiagnosticsError> {
    reject_symlink(path)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|_| LagDiagnosticsError::Storage)?
        .sync_all()
        .map_err(|_| LagDiagnosticsError::Storage)
}

fn reject_symlink(path: &Path) -> Result<(), LagDiagnosticsError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| LagDiagnosticsError::Storage)?;
    if metadata.file_type().is_symlink() {
        return Err(LagDiagnosticsError::Storage);
    }
    Ok(())
}

fn safe_child(parent: &Path, component: &str) -> Result<PathBuf, LagDiagnosticsError> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.contains(['/', '\\'])
        || component.contains('\0')
    {
        return Err(LagDiagnosticsError::InvalidRequest);
    }
    reject_symlink(parent)?;
    Ok(parent.join(component))
}

fn collect_regular_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), LagDiagnosticsError> {
    reject_symlink(dir)?;
    for entry in fs::read_dir(dir).map_err(|_| LagDiagnosticsError::Storage)? {
        let entry = entry.map_err(|_| LagDiagnosticsError::Storage)?;
        let kind = entry
            .file_type()
            .map_err(|_| LagDiagnosticsError::Storage)?;
        if kind.is_symlink() {
            return Err(LagDiagnosticsError::Storage);
        }
        if kind.is_dir() {
            collect_regular_files(&entry.path(), out)?;
        } else if kind.is_file() {
            out.push(entry.path());
        }
    }
    Ok(())
}

fn random_jti() -> Result<String, LagDiagnosticsError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| LagDiagnosticsError::Storage)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn fingerprint(value: &str) -> String {
    hex(&Sha256::digest(value.as_bytes()))
}

fn grant_fingerprint_from_token(token: &str) -> Result<String, LagDiagnosticsError> {
    let payload = token
        .split('.')
        .nth(2)
        .ok_or(LagDiagnosticsError::Storage)?;
    let claims: UploadClaims = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| LagDiagnosticsError::Storage)?,
    )
    .map_err(|_| LagDiagnosticsError::Storage)?;
    Ok(fingerprint(&claims.jti))
}

fn capture_label(capture_id: CaptureId) -> String {
    hex(&capture_id.bytes())
}

fn capture_label_from_claims(claims: &UploadClaims) -> Result<String, LagDiagnosticsError> {
    valid_capture_label(&claims.capture_id)
        .then(|| claims.capture_id.clone())
        .ok_or(LagDiagnosticsError::Rejected)
}

fn valid_capture_label(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn decode_capture_label(value: &str) -> Option<[u8; 16]> {
    if !valid_capture_label(value) {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = (chunk[0] as char).to_digit(16)? as u8;
        let low = (chunk[1] as char).to_digit(16)? as u8;
        bytes[index] = high << 4 | low;
    }
    Some(bytes)
}

fn valid_key_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_bound_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

fn valid_jti(value: &str) -> bool {
    value.len() == 22
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_fingerprint(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_analysis_artifact_id(value: &str) -> bool {
    value.len() == 36
        && value.starts_with("lc1-")
        && value[4..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn raw_descriptor(
    manifest: &RawArtifactManifest,
) -> Result<PrivateRawArtifact, LagDiagnosticsError> {
    if !valid_analysis_artifact_id(&manifest.artifact_id)
        || !valid_capture_label(&manifest.capture_id)
        || !valid_fingerprint(&manifest.digest_sha256)
        || manifest.generation == 0
        || manifest.attempt_id == 0
        || manifest.participant_id == 0
        || manifest.compressed_bytes == 0
    {
        return Err(LagDiagnosticsError::Storage);
    }
    Ok(PrivateRawArtifact {
        handle: manifest.artifact_id.clone(),
        capture_id: manifest.capture_id.clone(),
        generation: manifest.generation,
        digest_sha256: manifest.digest_sha256.clone(),
        participant_id: manifest.participant_id,
        received_utc_ms: manifest.received_utc_ms,
        compressed_bytes: manifest.compressed_bytes,
        record_count: manifest.record_count,
    })
}

impl LagDiagnosticsService {
    /// Produce a report-only participant cohort label that cannot be reversed
    /// by a Console viewer who knows the capture id and guesses numeric
    /// participant identifiers. The key is server-local signing material and
    /// never enters a manifest, a report, or any response.
    fn analysis_participant_pseudonym(
        &self,
        manifest: &RawArtifactManifest,
    ) -> Result<String, LagDiagnosticsError> {
        let key_id = self
            .config
            .active_key_id
            .as_deref()
            .ok_or(LagDiagnosticsError::Storage)?;
        let key = self.keys.get(key_id).ok_or(LagDiagnosticsError::Storage)?;
        let mut mac = HmacSha256::new_from_slice(key).map_err(|_| LagDiagnosticsError::Storage)?;
        mac.update(b"citadel-lag-diagnostics-participant-v1\0");
        mac.update(manifest.capture_id.as_bytes());
        mac.update(&manifest.participant_id.to_be_bytes());
        let tag = mac.finalize().into_bytes();
        Ok(format!("p-{}", &hex(&tag)[..20]))
    }
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write as _;

    use base64::Engine as _;
    use flate2::Compression;
    use flate2::write::GzEncoder;

    use super::*;

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("citadel-lag-capture-{}", Uuid::new_v4()));
            Self(path)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn config(root: &Path) -> LagDiagnosticsConfig {
        let mut keys = BTreeMap::new();
        keys.insert("current".to_string(), URL_SAFE_NO_PAD.encode([9_u8; 32]));
        LagDiagnosticsConfig {
            enabled: true,
            raw_root: Some(root.display().to_string()),
            active_key_id: Some("current".to_string()),
            upload_hmac_keys: keys,
            allowed_origins: vec!["https://game.example".to_string()],
            max_compressed_bytes: 1_024 * 1_024,
            max_decompressed_bytes: 1_024 * 1_024,
            max_decompression_ratio: 32,
            max_concurrent_uploads: 2,
            max_raw_bytes: 4 * 1_024 * 1_024,
            retention_hours: 1,
            shared_raw_store: false,
        }
    }

    fn capture(seed: u8) -> CaptureId {
        CaptureId::new([seed; 16]).expect("capture")
    }

    fn participant() -> CaptureParticipant {
        CaptureParticipant {
            participant_id: 7,
            session_id: "session-7".to_string(),
            tenant_id: "tenant-a".to_string(),
            match_id: "match-a".to_string(),
        }
    }

    fn participant_with_id(participant_id: u64) -> CaptureParticipant {
        CaptureParticipant {
            participant_id,
            session_id: format!("session-{participant_id}"),
            tenant_id: "tenant-a".to_string(),
            match_id: "match-a".to_string(),
        }
    }

    #[test]
    fn rejects_generation_that_clag_v1_cannot_bind_exactly() {
        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        assert_eq!(
            service.register_recording(capture(1), u64::from(u32::MAX) + 1, 1_000),
            Err(LagDiagnosticsError::InvalidRequest)
        );
    }

    fn open(
        service: &LagDiagnosticsService,
        capture_id: CaptureId,
        deadline: u64,
    ) -> CaptureFlushGrant {
        service
            .register_recording(capture_id, 1, 1_000)
            .expect("recording");
        service
            .open_flush(
                CaptureFlushPlan {
                    capture_id,
                    generation: 1,
                    attempt_id: 1,
                    upload_deadline_server_utc_ms: deadline,
                    max_compressed_bytes: 1_024 * 1_024,
                    required_uploads: 1,
                    analyze: false,
                    participants: vec![participant()],
                },
                TimestampMillis::from_unix_millis(10),
            )
            .expect("open flush")
            .pop()
            .expect("grant")
    }

    fn clag(capture_id: CaptureId, generation: u64, records: u32) -> Vec<u8> {
        let mut plain = vec![0_u8; CLAG_HEADER_BYTES + CLAG_RECORD_BYTES * records as usize];
        plain[0..4].copy_from_slice(b"CLAG");
        plain[4..6].copy_from_slice(&CLAG_VERSION.to_be_bytes());
        plain[6..8].copy_from_slice(&(CLAG_HEADER_BYTES as u16).to_be_bytes());
        plain[8..10].copy_from_slice(&(CLAG_RECORD_BYTES as u16).to_be_bytes());
        plain[10..12].copy_from_slice(&0x0005_u16.to_be_bytes());
        plain[12..16].copy_from_slice(&records.to_be_bytes());
        plain[16..24].copy_from_slice(&u64::from(records).to_be_bytes());
        plain[48..64].copy_from_slice(&capture_id.bytes());
        plain[64..68].copy_from_slice(&(generation as u32).to_be_bytes());
        gzip(&plain)
    }

    fn gzip(plain: &[u8]) -> Vec<u8> {
        let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
        gzip.write_all(plain).expect("gzip input");
        gzip.finish().expect("gzip finish")
    }

    fn stage(lease: &UploadLease, payload: &[u8]) {
        let mut file = OpenOptions::new()
            .write(true)
            .open(lease.staging_path())
            .expect("stage open");
        file.write_all(payload).expect("stage write");
        file.sync_all().expect("stage sync");
    }

    fn lease(service: &LagDiagnosticsService, token: &str, now: u64) -> UploadLease {
        service
            .begin_upload(
                Some(&format!("Bearer {token}")),
                Some(UploadContentType::CitadelLagCapture.as_str()),
                Some(UploadContentEncoding::Gzip.as_str()),
                None,
                Some("https://game.example"),
                TimestampMillis::from_unix_millis(now),
            )
            .expect("lease")
    }

    #[test]
    fn publishes_valid_clag_once_and_recovery_keeps_manifested_artifact() {
        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        let capture_id = capture(1);
        let grant = open(&service, capture_id, 500);
        let payload = clag(capture_id, 1, 2);
        let first = lease(&service, &grant.flush.upload_token, 20);
        stage(&first, &payload);
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        let receipt = service
            .validate_and_publish(
                first,
                payload.len() as u64,
                digest,
                TimestampMillis::from_unix_millis(20),
            )
            .expect("publish");
        assert_eq!(receipt.record_count, 2);
        let second = service.begin_upload(
            Some(&format!("Bearer {}", grant.flush.upload_token)),
            Some(UploadContentType::CitadelLagCapture.as_str()),
            Some(UploadContentEncoding::Gzip.as_str()),
            None,
            None,
            TimestampMillis::from_unix_millis(21),
        );
        assert!(
            matches!(second, Err(LagDiagnosticsError::Rejected)),
            "{second:?}"
        );
        service
            .recover(TimestampMillis::from_unix_millis(22))
            .expect("recover");
        assert_eq!(
            service
                .enforce_retention(TimestampMillis::from_unix_millis(3_700_000))
                .expect("retention"),
            1
        );
    }

    #[test]
    fn corrupt_payload_and_concurrent_replay_are_terminally_rejected() {
        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        let grant = open(&service, capture(2), 500);
        let first = lease(&service, &grant.flush.upload_token, 20);
        assert!(matches!(
            service.begin_upload(
                Some(&format!("Bearer {}", grant.flush.upload_token)),
                Some(UploadContentType::CitadelLagCapture.as_str()),
                Some(UploadContentEncoding::Gzip.as_str()),
                None,
                None,
                TimestampMillis::from_unix_millis(20),
            ),
            Err(LagDiagnosticsError::Rejected)
        ));
        let corrupt = b"not a gzip capture";
        stage(&first, corrupt);
        let digest: [u8; 32] = Sha256::digest(corrupt).into();
        assert_eq!(
            service.validate_and_publish(
                first,
                corrupt.len() as u64,
                digest,
                TimestampMillis::from_unix_millis(21)
            ),
            Err(LagDiagnosticsError::Rejected)
        );
        let second = service.begin_upload(
            Some(&format!("Bearer {}", grant.flush.upload_token)),
            Some(UploadContentType::CitadelLagCapture.as_str()),
            Some(UploadContentEncoding::Gzip.as_str()),
            None,
            None,
            TimestampMillis::from_unix_millis(22),
        );
        assert!(
            matches!(second, Err(LagDiagnosticsError::Rejected)),
            "{second:?}"
        );
    }

    #[test]
    fn gzip_bomb_and_foreign_clag_are_terminally_rejected() {
        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        let grant = open(&service, capture(4), 500);
        let bomb = gzip(&vec![0_u8; 1_024 * 1_024 + 1]);
        let first = lease(&service, &grant.flush.upload_token, 20);
        stage(&first, &bomb);
        assert_eq!(
            service.validate_and_publish(
                first,
                bomb.len() as u64,
                Sha256::digest(&bomb).into(),
                TimestampMillis::from_unix_millis(21),
            ),
            Err(LagDiagnosticsError::Rejected)
        );

        let grant = open(&service, capture(5), 500);
        let foreign = clag(capture(6), 1, 0);
        let second = lease(&service, &grant.flush.upload_token, 22);
        stage(&second, &foreign);
        assert_eq!(
            service.validate_and_publish(
                second,
                foreign.len() as u64,
                Sha256::digest(&foreign).into(),
                TimestampMillis::from_unix_millis(23),
            ),
            Err(LagDiagnosticsError::Rejected)
        );
    }

    #[test]
    fn recovery_consumes_an_interrupted_lease() {
        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        let grant = open(&service, capture(7), 500);
        let lease = lease(&service, &grant.flush.upload_token, 20);
        let staging_path = lease.staging_path().to_path_buf();
        stage(&lease, &clag(capture(7), 1, 0));

        service
            .recover(TimestampMillis::from_unix_millis(21))
            .expect("recover interrupted lease");
        assert!(!staging_path.exists());
        assert!(matches!(
            service.begin_upload(
                Some(&format!("Bearer {}", grant.flush.upload_token)),
                Some(UploadContentType::CitadelLagCapture.as_str()),
                Some(UploadContentEncoding::Gzip.as_str()),
                None,
                None,
                TimestampMillis::from_unix_millis(22),
            ),
            Err(LagDiagnosticsError::Rejected)
        ));
    }

    #[test]
    fn authenticated_recording_quorum_seals_and_consumes_remaining_grants() {
        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        let capture_id = capture(8);
        service
            .register_recording(capture_id, 1, 1_000)
            .expect("recording");
        let mut grants = service
            .open_flush(
                CaptureFlushPlan {
                    capture_id,
                    generation: 1,
                    attempt_id: 1,
                    upload_deadline_server_utc_ms: 500,
                    max_compressed_bytes: 1_024 * 1_024,
                    required_uploads: 1,
                    analyze: false,
                    participants: vec![participant_with_id(7), participant_with_id(8)],
                },
                TimestampMillis::from_unix_millis(10),
            )
            .expect("open flush");
        let first = grants.remove(0);
        let second = grants.remove(0);
        let payload = clag(capture_id, 1, 0);
        let lease = lease(&service, &first.flush.upload_token, 20);
        stage(&lease, &payload);
        let receipt = service
            .validate_and_publish(
                lease,
                payload.len() as u64,
                Sha256::digest(&payload).into(),
                TimestampMillis::from_unix_millis(21),
            )
            .expect("quorum artifact");
        assert!(receipt.analysis_artifact_ids.is_empty());
        assert!(matches!(
            service.begin_upload(
                Some(&format!("Bearer {}", second.flush.upload_token)),
                Some(UploadContentType::CitadelLagCapture.as_str()),
                Some(UploadContentEncoding::Gzip.as_str()),
                None,
                None,
                TimestampMillis::from_unix_millis(22),
            ),
            Err(LagDiagnosticsError::Rejected)
        ));
    }

    #[test]
    fn analysis_ids_are_released_only_after_an_opted_in_quorum_seals() {
        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        let capture_id = capture(10);
        service
            .register_recording(capture_id, 1, 1_000)
            .expect("recording");
        let mut grants = service
            .open_flush(
                CaptureFlushPlan {
                    capture_id,
                    generation: 1,
                    attempt_id: 1,
                    upload_deadline_server_utc_ms: 500,
                    max_compressed_bytes: 1_024 * 1_024,
                    required_uploads: 2,
                    analyze: true,
                    participants: vec![participant_with_id(7), participant_with_id(8)],
                },
                TimestampMillis::from_unix_millis(10),
            )
            .expect("open flush");
        let first = grants.remove(0);
        let second = grants.remove(0);
        let payload = clag(capture_id, 1, 0);

        let first_lease = lease(&service, &first.flush.upload_token, 20);
        stage(&first_lease, &payload);
        let first_receipt = service
            .validate_and_publish(
                first_lease,
                payload.len() as u64,
                Sha256::digest(&payload).into(),
                TimestampMillis::from_unix_millis(21),
            )
            .expect("first artifact");
        assert!(first_receipt.analysis_artifact_ids.is_empty());

        let second_lease = lease(&service, &second.flush.upload_token, 22);
        stage(&second_lease, &payload);
        let second_receipt = service
            .validate_and_publish(
                second_lease,
                payload.len() as u64,
                Sha256::digest(&payload).into(),
                TimestampMillis::from_unix_millis(23),
            )
            .expect("second artifact");
        assert_eq!(second_receipt.analysis_artifact_ids.len(), 2);
    }

    #[test]
    fn analysis_participant_pseudonym_is_keyed_not_an_enumerable_hash() {
        let root = TestRoot::new();
        let first = LagDiagnosticsService::new(config(&root.0), "node-a".to_string())
            .expect("first service");
        let mut rotated_config = config(&root.0);
        rotated_config
            .upload_hmac_keys
            .insert("current".to_string(), URL_SAFE_NO_PAD.encode([10_u8; 32]));
        let second = LagDiagnosticsService::new(rotated_config, "node-a".to_string())
            .expect("second service");
        let manifest = RawArtifactManifest {
            version: 1,
            artifact_id: format!("lc1-{}", "a".repeat(32)),
            capture_id: capture_label(capture(12)),
            generation: 1,
            attempt_id: 1,
            participant_id: 7,
            received_utc_ms: 1,
            digest_sha256: "b".repeat(64),
            compressed_bytes: 1,
            decompressed_bytes: 128,
            record_count: 0,
            content_type: UploadContentType::CitadelLagCapture.as_str().to_string(),
            content_encoding: UploadContentEncoding::Gzip.as_str().to_string(),
            grant_fingerprint: "c".repeat(64),
            report_referenced: false,
        };
        let first_label = first
            .analysis_participant_pseudonym(&manifest)
            .expect("first label");
        let second_label = second
            .analysis_participant_pseudonym(&manifest)
            .expect("second label");
        let enumerable = format!(
            "p-{}",
            &hex(&Sha256::digest(
                format!("{}:{}", manifest.capture_id, manifest.participant_id).as_bytes()
            ))[..16]
        );
        assert_ne!(first_label, enumerable);
        assert_ne!(first_label, second_label);
    }

    #[test]
    fn deadline_expires_and_consumes_an_unfinished_attempt() {
        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        let grant = open(&service, capture(9), 20);
        assert_eq!(
            service.expire_deadlines(TimestampMillis::from_unix_millis(20)),
            Ok(1)
        );
        assert!(matches!(
            service.begin_upload(
                Some(&format!("Bearer {}", grant.flush.upload_token)),
                Some(UploadContentType::CitadelLagCapture.as_str()),
                Some(UploadContentEncoding::Gzip.as_str()),
                None,
                None,
                TimestampMillis::from_unix_millis(20),
            ),
            Err(LagDiagnosticsError::Rejected)
        ));
    }

    #[test]
    fn signed_claims_bind_identity_and_key_rotation_fails_closed_after_retirement() {
        let root = TestRoot::new();
        let mut initial = config(&root.0);
        initial.upload_hmac_keys.clear();
        initial
            .upload_hmac_keys
            .insert("old".to_string(), URL_SAFE_NO_PAD.encode([1_u8; 32]));
        initial
            .upload_hmac_keys
            .insert("new".to_string(), URL_SAFE_NO_PAD.encode([2_u8; 32]));
        initial.active_key_id = Some("old".to_string());
        let service = LagDiagnosticsService::new(initial.clone(), "node-a".to_string())
            .expect("old key service");
        let grant = open(&service, capture(10), 500);
        let claims = service
            .verify_token(&grant.flush.upload_token)
            .expect("locally minted grant verifies");
        assert_eq!(claims.capture_id, capture_label(capture(10)));
        assert_eq!(claims.session_id, "session-7");
        assert_eq!(claims.tenant_id, "tenant-a");
        assert_eq!(claims.match_id, "match-a");
        assert_eq!(claims.participant_id, 7);
        assert!(matches!(
            service.verify_token(&format!("{}x", grant.flush.upload_token)),
            Err(LagDiagnosticsError::Rejected)
        ));

        let mut overlap = initial.clone();
        overlap.active_key_id = Some("new".to_string());
        let rotated =
            LagDiagnosticsService::new(overlap.clone(), "node-a".to_string()).expect("overlap");
        assert!(rotated.verify_token(&grant.flush.upload_token).is_ok());

        overlap.upload_hmac_keys.remove("old");
        let retired = LagDiagnosticsService::new(overlap, "node-a".to_string()).expect("retired");
        assert!(matches!(
            retired.verify_token(&grant.flush.upload_token),
            Err(LagDiagnosticsError::Rejected)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn recovery_rejects_symlinked_private_markers() {
        use std::os::unix::fs::symlink;

        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        let link = root.0.join("leases").join("untrusted.json");
        symlink(root.0.join("outside"), &link).expect("symlink");
        assert_eq!(
            service.recover(TimestampMillis::from_unix_millis(20)),
            Err(LagDiagnosticsError::Storage)
        );
    }

    #[test]
    fn private_analysis_handle_has_an_exact_bounded_shape() {
        assert!(valid_analysis_artifact_id(&format!(
            "lc1-{}",
            "a".repeat(32)
        )));
        assert!(!valid_analysis_artifact_id("lc1-"));
        assert!(!valid_analysis_artifact_id(&format!(
            "lc1-{}",
            "a".repeat(31)
        )));
        assert!(!valid_analysis_artifact_id(&format!(
            "lc1-{}",
            "g".repeat(32)
        )));
        assert!(!valid_analysis_artifact_id(&format!(
            "other-{}",
            "a".repeat(30)
        )));
    }

    #[cfg(unix)]
    #[test]
    fn private_analysis_loader_rejects_a_symlinked_raw_artifact() {
        use std::os::unix::fs::symlink;

        let test_root = TestRoot::new();
        let service = LagDiagnosticsService::new(config(&test_root.0), "node-a".to_string())
            .expect("service");
        let capture_id = capture(11);
        let capture_label = capture_label(capture_id);
        let artifact_id = format!("lc1-{}", "a".repeat(32));
        let root = service.root().expect("raw root");
        let raw_path = root
            .raw_artifact_path(&capture_label, 7, 1)
            .expect("raw path");
        symlink(test_root.0.join("outside"), &raw_path).expect("raw symlink");
        let manifest = RawArtifactManifest {
            version: 1,
            artifact_id: artifact_id.clone(),
            capture_id: capture_label.clone(),
            generation: 1,
            attempt_id: 1,
            participant_id: 7,
            received_utc_ms: 1,
            digest_sha256: "0".repeat(64),
            compressed_bytes: 0,
            decompressed_bytes: 0,
            record_count: 0,
            content_type: UploadContentType::CitadelLagCapture.as_str().to_string(),
            content_encoding: UploadContentEncoding::Gzip.as_str().to_string(),
            grant_fingerprint: "0".repeat(64),
            report_referenced: false,
        };
        write_json_new(
            &root
                .manifest_path(&capture_label, 7, 1)
                .expect("manifest path"),
            &manifest,
        )
        .expect("manifest");

        assert!(matches!(
            service.load_private_analysis_artifact(&artifact_id),
            Err(LagDiagnosticsError::Rejected)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn console_raw_operations_reject_a_symlinked_published_artifact() {
        use std::os::unix::fs::symlink;

        let test_root = TestRoot::new();
        let service = LagDiagnosticsService::new(config(&test_root.0), "node-a".to_string())
            .expect("service");
        let capture_id = capture(12);
        let grant = open(&service, capture_id, 500);
        let payload = clag(capture_id, 1, 0);
        let lease = lease(&service, &grant.flush.upload_token, 20);
        stage(&lease, &payload);
        let receipt = service
            .validate_and_publish(
                lease,
                payload.len() as u64,
                Sha256::digest(&payload).into(),
                TimestampMillis::from_unix_millis(21),
            )
            .expect("publish");
        let label = capture_label(capture_id);
        let raw_path = service
            .root()
            .expect("root")
            .raw_artifact_path(&label, 7, 1)
            .expect("raw path");
        fs::remove_file(&raw_path).expect("replace raw with symlink");
        symlink(test_root.0.join("outside"), &raw_path).expect("raw symlink");

        assert_eq!(
            service.list_private_raw_artifacts(&label, None, 10),
            Err(LagDiagnosticsError::Storage)
        );
        assert!(matches!(
            service.download_private_raw_artifact(&label, &receipt.artifact_id),
            Err(LagDiagnosticsError::Rejected)
        ));
    }

    #[test]
    fn expiry_wrong_headers_and_traversal_are_rejected_without_storage() {
        let root = TestRoot::new();
        let service =
            LagDiagnosticsService::new(config(&root.0), "node-a".to_string()).expect("service");
        let grant = open(&service, capture(3), 20);
        assert!(matches!(
            service.begin_upload(
                Some(&format!("Bearer {}", grant.flush.upload_token)),
                Some("application/octet-stream"),
                Some(UploadContentEncoding::Gzip.as_str()),
                None,
                None,
                TimestampMillis::from_unix_millis(11),
            ),
            Err(LagDiagnosticsError::Rejected)
        ));
        assert!(matches!(
            service.begin_upload(
                Some(&format!("Bearer {}", grant.flush.upload_token)),
                Some(UploadContentType::CitadelLagCapture.as_str()),
                Some(UploadContentEncoding::Gzip.as_str()),
                None,
                None,
                TimestampMillis::from_unix_millis(20),
            ),
            Err(LagDiagnosticsError::Rejected)
        ));
        assert_eq!(
            safe_child(&root.0, "../outside"),
            Err(LagDiagnosticsError::InvalidRequest)
        );
    }
}
