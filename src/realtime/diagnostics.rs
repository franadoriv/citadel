//! Trusted server-side state for opt-in lag diagnostics.
//!
//! This module owns correlation offers and capture lifecycle state, but never
//! owns recorder buffers, upload credentials, raw files, or report storage.
//! Those responsibilities intentionally remain in their later layers.

use std::collections::HashMap;
use std::sync::Mutex;

use citadel_wire::diagnostics::{
    CAPABILITY_RECORDING, Capabilities, CaptureId, CaptureStatus, CaptureStatusCode, ClockSync,
    FlushCapture, ServerTime, StartCapture,
};

use crate::realtime::registry::ParticipantId;
use crate::time::TimestampMillis;

/// Native capture lifecycle failure. These errors are trusted API results and
/// never cross the untrusted gameplay/script message surface.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LagCaptureError {
    /// A caller constructed a request that cannot be represented by the
    /// versioned diagnostics wire contract.
    #[error("invalid lag capture request")]
    InvalidRequest,
    /// A capture is already active and must not be overwritten by a concurrent START.
    #[error("a lag capture is already active")]
    AlreadyActive,
    /// No locally opted-in, capable session existed in the captured population.
    #[error("no capable lag-diagnostics participants")]
    NoCapableParticipants,
    /// The requested deadline was already elapsed at the server.
    #[error("lag capture deadline has already elapsed")]
    DeadlineElapsed,
    /// A server-time offer counter exhausted rather than wrapping/reusing an offer.
    #[error("lag diagnostics offer id space exhausted")]
    OfferExhausted,
    /// No active capture has the referenced id.
    #[error("unknown lag capture")]
    UnknownCapture,
    /// The referenced capture id exists but its immutable generation differs.
    #[error("stale lag capture generation")]
    StaleGeneration,
    /// The participant did not complete the authenticated capability offer flow.
    #[error("session is not diagnostics-capable")]
    NotCapable,
    /// The participant did not belong to this capture's start snapshot.
    #[error("session was not selected for this lag capture")]
    NotParticipant,
    /// The client made a state transition that cannot follow its current state.
    #[error("invalid lag capture state transition")]
    InvalidTransition,
    /// The requested flush does not match the active capture.
    #[error("invalid lag capture flush request")]
    InvalidFlush,
    /// A FLUSH that carries a bearer capability must be minted per selected
    /// participant by the capture-ingest layer, never cloned by this base API.
    #[error("lag capture flush requires per-participant upload grants")]
    PerParticipantGrantRequired,
    /// The configured private capture ingest service was unavailable.
    #[error("lag capture ingest service is unavailable")]
    IngestUnavailable,
}

/// Native start result. `requested` means only that a bounded local queue
/// accepted START; it never means that a client received it or will upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LagCaptureStart {
    /// Immutable server request that was started.
    pub request: StartCapture,
    /// Sessions to which START was queued successfully.
    pub requested: Vec<ParticipantId>,
    /// Connected sessions whose local SDK did not assert the capability.
    pub ineligible: Vec<ParticipantId>,
    /// Capable sessions whose local outbound queue rejected START.
    pub enqueue_failed: Vec<ParticipantId>,
}

/// Native flush result. `requested` contains only clients that had previously
/// authenticated `Recording`; a server never expects upload from a mere queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LagCaptureFlush {
    /// Flush request delivered to eligible recording clients.
    pub request: FlushCapture,
    /// Clients to which FLUSH entered the bounded queue.
    pub requested: Vec<ParticipantId>,
    /// Started clients that could not accept FLUSH into their outbound queue.
    pub enqueue_failed: Vec<ParticipantId>,
}

/// A participant's server-observed lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagCaptureParticipantState {
    /// SDK never asserted local diagnostics capability for this connection.
    Ineligible,
    /// SDK asserted capability but START did not enter its local outbound queue.
    EnqueueFailed,
    /// START entered the outbound queue. This is not a client acknowledgement.
    Requested,
    /// Client authenticated that it accepted START and is recording.
    Recording,
    /// FLUSH entered the outbound queue after the client had started recording.
    FlushRequested,
    /// Client authenticated that it began its upload attempt.
    UploadStarted,
    /// Client authenticated that the upload completed; ingest still verifies it.
    Uploaded,
    /// Client reported a local failure or declined the request.
    Failed,
    /// Connection left before its terminal state.
    Disconnected,
    /// Server UTC capture deadline elapsed before a terminal client status.
    TimedOut,
}

impl LagCaptureParticipantState {
    /// Whether this state represents a client that has actually started and is
    /// therefore part of the expected-upload denominator.
    #[must_use]
    pub const fn is_started(self) -> bool {
        matches!(
            self,
            Self::Recording | Self::FlushRequested | Self::UploadStarted | Self::Uploaded
        )
    }

    const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Uploaded
                | Self::Failed
                | Self::Disconnected
                | Self::TimedOut
                | Self::EnqueueFailed
        )
    }
}

/// Snapshot of one selected participant, including only bounded status counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LagCaptureParticipantStatus {
    /// Connection identity fixed at START.
    pub participant: ParticipantId,
    /// Current server-observed lifecycle state.
    pub state: LagCaptureParticipantState,
    /// Whether this client ever authenticated `Recording`. This historical
    /// denominator survives a later failure, disconnect, or timeout.
    pub recording_confirmed: bool,
    /// Last client-reported retained record count.
    pub recorded_packets: u32,
    /// Last client-reported recorder drops.
    pub dropped_packets: u32,
    /// Last client-reported raw recording bytes.
    pub recorded_bytes: u32,
}

/// Native status view. It deliberately reports queue, client confirmation, and
/// upload observation separately so operators cannot confuse them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LagCaptureStatus {
    /// Immutable started request.
    pub request: StartCapture,
    /// Participant lifecycle states in deterministic participant-id order.
    pub participants: Vec<LagCaptureParticipantStatus>,
}

impl LagCaptureStatus {
    /// Count clients that authenticated recording at least once.
    #[must_use]
    pub fn started_count(&self) -> usize {
        self.participants
            .iter()
            .filter(|participant| participant.recording_confirmed)
            .count()
    }

    /// Count clients that authenticated an upload completion.
    #[must_use]
    pub fn uploaded_count(&self) -> usize {
        self.participants
            .iter()
            .filter(|participant| participant.state == LagCaptureParticipantState::Uploaded)
            .count()
    }
}

#[derive(Debug, Clone, Copy)]
struct SessionCapability {
    offer_id: u64,
    recording_enabled: bool,
}

#[derive(Debug, Clone, Copy)]
struct ParticipantCapture {
    state: LagCaptureParticipantState,
    recording_confirmed: bool,
    recorded_packets: u32,
    dropped_packets: u32,
    recorded_bytes: u32,
    flush_attempt_id: Option<u64>,
}

#[derive(Debug, Clone)]
struct ActiveCapture {
    request: StartCapture,
    participants: HashMap<ParticipantId, ParticipantCapture>,
}

#[derive(Debug, Default)]
struct State {
    next_offer_id: u64,
    sessions: HashMap<ParticipantId, SessionCapability>,
    active: Option<ActiveCapture>,
    /// Most recently terminal capture, retained as status evidence while a
    /// subsequent match may begin a new active capture.
    last_completed: Option<ActiveCapture>,
}

/// Thread-safe state holder attached to [`super::Gateway`].
#[derive(Debug, Default)]
pub struct LagCaptureManager {
    state: Mutex<State>,
}

impl LagCaptureManager {
    /// Issue the post-auth `SERVER_TIME` offer for one newly registered session.
    /// The offer is session-bound and must be echoed before capability is accepted.
    pub fn issue_server_time(
        &self,
        participant: ParticipantId,
        now: TimestampMillis,
    ) -> Result<ServerTime, LagCaptureError> {
        // A production system clock is post-epoch. Fail closed instead of
        // generating an invalid wire body in a pathological test/runtime.
        if now.unix_millis() == 0 {
            return Err(LagCaptureError::OfferExhausted);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| LagCaptureError::OfferExhausted)?;
        let next = if state.next_offer_id == 0 {
            1
        } else {
            state.next_offer_id
        };
        let Some(after) = next.checked_add(1) else {
            return Err(LagCaptureError::OfferExhausted);
        };
        state.next_offer_id = after;
        state.sessions.insert(
            participant,
            SessionCapability {
                offer_id: next,
                recording_enabled: false,
            },
        );
        Ok(ServerTime {
            offer_id: next,
            server_utc_ms: now.unix_millis(),
        })
    }

    /// Accept local SDK capability only when it echoes this session's offer.
    /// Caller additionally verifies the connection is authenticated.
    pub fn accept_capabilities(
        &self,
        participant: ParticipantId,
        capabilities: Capabilities,
    ) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let Some(session) = state.sessions.get_mut(&participant) else {
            return false;
        };
        if session.offer_id != capabilities.offer_id || !capabilities.recording_enabled() {
            return false;
        }
        session.recording_enabled = capabilities.features & CAPABILITY_RECORDING != 0;
        true
    }

    /// Whether the participant completed the server-time/capabilities flow.
    #[must_use]
    pub fn is_capable(&self, participant: ParticipantId) -> bool {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.sessions.get(&participant).copied())
            .is_some_and(|session| session.recording_enabled)
    }

    /// Begin a capture with a snapshot of connected participants. Returns only
    /// capability-qualified candidates; the Gateway records queue outcomes.
    pub fn begin(
        &self,
        request: StartCapture,
        connected: &[ParticipantId],
        now: TimestampMillis,
    ) -> Result<(Vec<ParticipantId>, Vec<ParticipantId>), LagCaptureError> {
        if request.deadline_server_utc_ms <= now.unix_millis() {
            return Err(LagCaptureError::DeadlineElapsed);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| LagCaptureError::AlreadyActive)?;
        if state.active.is_some() {
            return Err(LagCaptureError::AlreadyActive);
        }
        let mut participants = HashMap::with_capacity(connected.len());
        let mut capable = Vec::new();
        let mut ineligible = Vec::new();
        for participant in connected {
            let is_capable = state
                .sessions
                .get(participant)
                .is_some_and(|session| session.recording_enabled);
            let lifecycle = if is_capable {
                capable.push(*participant);
                LagCaptureParticipantState::Requested
            } else {
                ineligible.push(*participant);
                LagCaptureParticipantState::Ineligible
            };
            participants.insert(
                *participant,
                ParticipantCapture {
                    state: lifecycle,
                    recording_confirmed: false,
                    recorded_packets: 0,
                    dropped_packets: 0,
                    recorded_bytes: 0,
                    flush_attempt_id: None,
                },
            );
        }
        if capable.is_empty() {
            return Err(LagCaptureError::NoCapableParticipants);
        }
        state.active = Some(ActiveCapture {
            request,
            participants,
        });
        Ok((capable, ineligible))
    }

    /// Record a local queue outcome for START. Queue success remains only
    /// `Requested`; it never synthesizes the client's `Recording` status.
    pub fn mark_start_enqueue(&self, participant: ParticipantId, queued: bool) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(active) = state.active.as_mut() else {
            return;
        };
        let Some(entry) = active.participants.get_mut(&participant) else {
            return;
        };
        if entry.state == LagCaptureParticipantState::Requested && !queued {
            entry.state = LagCaptureParticipantState::EnqueueFailed;
        }
    }

    /// Validate a client clock probe. Clock correlation is only accepted from a
    /// capability-qualified selected client; it cannot wake or enable a recorder.
    pub fn accepts_clock_sync(&self, participant: ParticipantId) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        let Some(active) = state.active.as_ref() else {
            return false;
        };
        matches!(
            active
                .participants
                .get(&participant)
                .map(|entry| entry.state),
            Some(LagCaptureParticipantState::Requested | LagCaptureParticipantState::Recording)
        )
    }

    /// Apply a client STATUS after binding it to the exact capture and connection.
    pub fn apply_status(
        &self,
        participant: ParticipantId,
        status: CaptureStatus,
    ) -> Result<(), LagCaptureError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| LagCaptureError::UnknownCapture)?;
        let active = state
            .active
            .as_mut()
            .ok_or(LagCaptureError::UnknownCapture)?;
        if active.request.capture_id != status.capture_id {
            return Err(LagCaptureError::UnknownCapture);
        }
        if active.request.generation != status.generation {
            return Err(LagCaptureError::StaleGeneration);
        }
        let entry = active
            .participants
            .get_mut(&participant)
            .ok_or(LagCaptureError::NotParticipant)?;
        let next = match (entry.state, status.code) {
            (LagCaptureParticipantState::Requested, CaptureStatusCode::Recording)
                if status.attempt_id == 0 =>
            {
                LagCaptureParticipantState::Recording
            }
            (LagCaptureParticipantState::FlushRequested, CaptureStatusCode::UploadStarted)
                if entry.flush_attempt_id == Some(status.attempt_id) =>
            {
                LagCaptureParticipantState::UploadStarted
            }
            (LagCaptureParticipantState::UploadStarted, CaptureStatusCode::Uploaded)
                if entry.flush_attempt_id == Some(status.attempt_id) =>
            {
                LagCaptureParticipantState::Uploaded
            }
            (
                LagCaptureParticipantState::Requested
                | LagCaptureParticipantState::Recording
                | LagCaptureParticipantState::FlushRequested
                | LagCaptureParticipantState::UploadStarted,
                CaptureStatusCode::Failed,
            ) if status.attempt_id == 0 || entry.flush_attempt_id == Some(status.attempt_id) => {
                LagCaptureParticipantState::Failed
            }
            _ => return Err(LagCaptureError::InvalidTransition),
        };
        entry.state = next;
        if status.code == CaptureStatusCode::Recording {
            entry.recording_confirmed = true;
        }
        entry.recorded_packets = status.recorded_packets;
        entry.dropped_packets = status.dropped_packets;
        entry.recorded_bytes = status.recorded_bytes;
        Ok(())
    }

    /// Select exactly the clients that authenticated recording before FLUSH.
    pub fn prepare_flush(
        &self,
        request: &FlushCapture,
        now: TimestampMillis,
    ) -> Result<Vec<ParticipantId>, LagCaptureError> {
        self.prepare_flush_identity(
            request.capture_id,
            request.generation,
            request.attempt_id,
            request.upload_deadline_server_utc_ms,
            now,
        )
    }

    /// Transition the currently recording clients into a pending FLUSH using
    /// identity fields alone. Capture ingestion uses this before it mints one
    /// distinct signed `FlushCapture` body per selected participant.
    pub fn prepare_flush_identity(
        &self,
        capture_id: CaptureId,
        generation: u64,
        attempt_id: u64,
        upload_deadline_server_utc_ms: u64,
        now: TimestampMillis,
    ) -> Result<Vec<ParticipantId>, LagCaptureError> {
        if upload_deadline_server_utc_ms <= now.unix_millis() {
            return Err(LagCaptureError::DeadlineElapsed);
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| LagCaptureError::UnknownCapture)?;
        let active = state
            .active
            .as_mut()
            .ok_or(LagCaptureError::UnknownCapture)?;
        if active.request.capture_id != capture_id || active.request.generation != generation {
            return Err(LagCaptureError::InvalidFlush);
        }
        let mut targets = Vec::new();
        for (participant, entry) in &mut active.participants {
            if entry.state == LagCaptureParticipantState::Recording {
                entry.state = LagCaptureParticipantState::FlushRequested;
                entry.flush_attempt_id = Some(attempt_id);
                targets.push(*participant);
            }
        }
        Ok(targets)
    }

    /// Revert a not-yet-delivered flush set back to `Recording`. This is used
    /// only when trusted grant minting fails before a FLUSH envelope is sent.
    pub fn rollback_flush_identity(&self, capture_id: CaptureId, generation: u64, attempt_id: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(active) = state.active.as_mut() else {
            return;
        };
        if active.request.capture_id != capture_id || active.request.generation != generation {
            return;
        }
        for entry in active.participants.values_mut() {
            if entry.state == LagCaptureParticipantState::FlushRequested
                && entry.flush_attempt_id == Some(attempt_id)
            {
                entry.state = LagCaptureParticipantState::Recording;
                entry.flush_attempt_id = None;
            }
        }
    }

    /// Record a local FLUSH enqueue failure without treating it as client input.
    pub fn mark_flush_enqueue(&self, participant: ParticipantId, queued: bool) {
        if queued {
            return;
        }
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(active) = state.active.as_mut() else {
            return;
        };
        if let Some(entry) = active.participants.get_mut(&participant)
            && entry.state == LagCaptureParticipantState::FlushRequested
        {
            entry.state = LagCaptureParticipantState::EnqueueFailed;
        }
    }

    /// Mark one disconnected selected participant without changing others.
    pub fn disconnect(&self, participant: ParticipantId) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.sessions.remove(&participant);
        if let Some(active) = state.active.as_mut()
            && let Some(entry) = active.participants.get_mut(&participant)
            && !entry.state.is_terminal()
            && entry.state != LagCaptureParticipantState::Ineligible
        {
            entry.state = LagCaptureParticipantState::Disconnected;
        }
        drop(state);
        let _ = self.finish_if_terminal();
    }

    /// Mark non-terminal participants as timed out once the server UTC capture
    /// deadline has elapsed. A caller may invoke this from its normal match-end
    /// or capture-maintenance loop; it never fabricates an upload result.
    pub fn expire_deadline(&self, now: TimestampMillis) -> usize {
        let expired = {
            let Ok(mut state) = self.state.lock() else {
                return 0;
            };
            let Some(active) = state.active.as_mut() else {
                return 0;
            };
            if now.unix_millis() < active.request.deadline_server_utc_ms {
                return 0;
            }
            let mut expired = 0;
            for entry in active.participants.values_mut() {
                if !entry.state.is_terminal()
                    && entry.state != LagCaptureParticipantState::Ineligible
                {
                    entry.state = LagCaptureParticipantState::TimedOut;
                    expired += 1;
                }
            }
            expired
        };
        let _ = self.finish_if_terminal();
        expired
    }

    /// Move an all-settled active capture into a retained terminal snapshot.
    /// This frees the manager for a later match without losing the last status.
    pub fn finish_if_terminal(&self) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let settled = state.active.as_ref().is_some_and(|active| {
            active.participants.values().all(|entry| {
                entry.state.is_terminal() || entry.state == LagCaptureParticipantState::Ineligible
            })
        });
        if !settled {
            return false;
        }
        state.last_completed = state.active.take();
        true
    }

    /// Forget a server-time offer that never became a live session.
    pub fn abandon_session(&self, participant: ParticipantId) {
        if let Ok(mut state) = self.state.lock() {
            state.sessions.remove(&participant);
        }
    }

    /// Return an immutable lifecycle snapshot for the exact active capture.
    #[must_use]
    pub fn status(&self, capture_id: CaptureId) -> Option<LagCaptureStatus> {
        let state = self.state.lock().ok()?;
        let active = state
            .active
            .as_ref()
            .filter(|active| active.request.capture_id == capture_id)
            .or_else(|| {
                state
                    .last_completed
                    .as_ref()
                    .filter(|completed| completed.request.capture_id == capture_id)
            })?;
        let mut participants: Vec<_> = active
            .participants
            .iter()
            .map(|(participant, entry)| LagCaptureParticipantStatus {
                participant: *participant,
                state: entry.state,
                recording_confirmed: entry.recording_confirmed,
                recorded_packets: entry.recorded_packets,
                dropped_packets: entry.dropped_packets,
                recorded_bytes: entry.recorded_bytes,
            })
            .collect();
        participants.sort_by_key(|participant| participant.participant);
        Some(LagCaptureStatus {
            request: active.request.clone(),
            participants,
        })
    }

    /// Build the server half of a client clock probe. The caller captures UTC
    /// around the parse/enqueue boundary, never a gameplay tick.
    #[must_use]
    pub fn reply_clock_sync(
        request: ClockSync,
        server_received_utc_us: u64,
        server_sent_utc_us: u64,
    ) -> Option<ClockSync> {
        let ClockSync::Request {
            sequence,
            client_sent_mono_us,
        } = request
        else {
            return None;
        };
        if server_received_utc_us == 0
            || server_sent_utc_us == 0
            || server_sent_utc_us < server_received_utc_us
        {
            return None;
        }
        Some(ClockSync::Response {
            sequence,
            client_sent_mono_us,
            server_received_utc_us,
            server_sent_utc_us,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel_wire::diagnostics::{
        DIAGNOSTICS_UPLOAD_PATH, PacketDirection, PacketFilter, UploadContentEncoding,
        UploadContentType,
    };

    fn id() -> CaptureId {
        CaptureId::new([1; 16]).expect("id")
    }

    fn start(deadline: u64) -> StartCapture {
        start_with(id(), deadline)
    }

    fn start_with(capture_id: CaptureId, deadline: u64) -> StartCapture {
        StartCapture {
            capture_id,
            generation: 1,
            deadline_server_utc_ms: deadline,
            max_record_bytes: 1024,
            filters: vec![PacketFilter {
                kind: 9,
                direction: PacketDirection::Inbound,
                entity_id: None,
            }],
        }
    }

    fn capabilities(offer_id: u64) -> Capabilities {
        Capabilities {
            offer_id,
            features: CAPABILITY_RECORDING,
        }
    }

    #[test]
    fn capability_is_bound_to_its_server_time_offer() {
        let manager = LagCaptureManager::default();
        let participant = ParticipantId::from_raw(1);
        let offer = manager
            .issue_server_time(participant, TimestampMillis::from_unix_millis(10))
            .expect("offer");
        assert!(!manager.accept_capabilities(participant, capabilities(offer.offer_id + 1)));
        assert!(manager.accept_capabilities(participant, capabilities(offer.offer_id)));
        assert!(manager.is_capable(participant));
    }

    #[test]
    fn concurrent_start_cannot_overwrite_active_capture() {
        let manager = LagCaptureManager::default();
        let participant = ParticipantId::from_raw(1);
        let offer = manager
            .issue_server_time(participant, TimestampMillis::from_unix_millis(10))
            .expect("offer");
        assert!(manager.accept_capabilities(participant, capabilities(offer.offer_id)));
        manager
            .begin(
                start(20),
                &[participant],
                TimestampMillis::from_unix_millis(10),
            )
            .expect("start");
        assert_eq!(
            manager.begin(
                start(30),
                &[participant],
                TimestampMillis::from_unix_millis(10)
            ),
            Err(LagCaptureError::AlreadyActive)
        );
    }

    #[test]
    fn only_client_recording_enters_expected_flush_set() {
        let manager = LagCaptureManager::default();
        let participant = ParticipantId::from_raw(1);
        let offer = manager
            .issue_server_time(participant, TimestampMillis::from_unix_millis(10))
            .expect("offer");
        assert!(manager.accept_capabilities(participant, capabilities(offer.offer_id)));
        manager
            .begin(
                start(100),
                &[participant],
                TimestampMillis::from_unix_millis(10),
            )
            .expect("start");
        let flush = FlushCapture {
            capture_id: id(),
            generation: 1,
            attempt_id: 1,
            upload_deadline_server_utc_ms: 50,
            max_compressed_bytes: 1_024,
            content_type: UploadContentType::CitadelLagCapture,
            content_encoding: UploadContentEncoding::Gzip,
            upload_path: DIAGNOSTICS_UPLOAD_PATH.to_string(),
            upload_token: "fixture-token.01".to_string(),
        };
        assert!(
            manager
                .prepare_flush(&flush, TimestampMillis::from_unix_millis(11))
                .expect("flush")
                .is_empty()
        );
        manager
            .apply_status(
                participant,
                CaptureStatus {
                    capture_id: id(),
                    generation: 1,
                    code: CaptureStatusCode::Recording,
                    attempt_id: 0,
                    recorded_packets: 4,
                    dropped_packets: 1,
                    recorded_bytes: 64,
                },
            )
            .expect("recording");
        assert_eq!(
            manager.apply_status(
                participant,
                CaptureStatus {
                    capture_id: id(),
                    generation: 1,
                    code: CaptureStatusCode::UploadStarted,
                    attempt_id: 1,
                    recorded_packets: 4,
                    dropped_packets: 1,
                    recorded_bytes: 64,
                },
            ),
            Err(LagCaptureError::InvalidTransition),
            "upload progress cannot precede a server FLUSH"
        );
        assert_eq!(
            manager
                .prepare_flush(&flush, TimestampMillis::from_unix_millis(11))
                .expect("flush"),
            vec![participant]
        );
    }

    #[test]
    fn status_is_session_and_generation_fenced() {
        let manager = LagCaptureManager::default();
        let participant = ParticipantId::from_raw(1);
        let other = ParticipantId::from_raw(2);
        let offer = manager
            .issue_server_time(participant, TimestampMillis::from_unix_millis(10))
            .expect("offer");
        assert!(manager.accept_capabilities(participant, capabilities(offer.offer_id)));
        manager
            .begin(
                start(100),
                &[participant],
                TimestampMillis::from_unix_millis(10),
            )
            .expect("start");
        let mut status = CaptureStatus {
            capture_id: id(),
            generation: 2,
            code: CaptureStatusCode::Recording,
            attempt_id: 0,
            recorded_packets: 0,
            dropped_packets: 0,
            recorded_bytes: 0,
        };
        assert_eq!(
            manager.apply_status(participant, status),
            Err(LagCaptureError::StaleGeneration)
        );
        status.generation = 1;
        assert_eq!(
            manager.apply_status(other, status),
            Err(LagCaptureError::NotParticipant)
        );
        manager
            .apply_status(participant, status)
            .expect("bound status");
        let summary = manager.status(id()).expect("summary");
        assert_eq!(summary.started_count(), 1);
    }

    #[test]
    fn terminal_capture_retains_started_denominator_and_releases_next_start() {
        let manager = LagCaptureManager::default();
        let participant = ParticipantId::from_raw(1);
        let offer = manager
            .issue_server_time(participant, TimestampMillis::from_unix_millis(10))
            .expect("offer");
        assert!(manager.accept_capabilities(participant, capabilities(offer.offer_id)));
        let first_id = id();
        manager
            .begin(
                start_with(first_id, 100),
                &[participant],
                TimestampMillis::from_unix_millis(10),
            )
            .expect("start");
        manager
            .apply_status(
                participant,
                CaptureStatus {
                    capture_id: first_id,
                    generation: 1,
                    code: CaptureStatusCode::Recording,
                    attempt_id: 0,
                    recorded_packets: 2,
                    dropped_packets: 0,
                    recorded_bytes: 16,
                },
            )
            .expect("recording");
        manager
            .apply_status(
                participant,
                CaptureStatus {
                    capture_id: first_id,
                    generation: 1,
                    code: CaptureStatusCode::Failed,
                    attempt_id: 0,
                    recorded_packets: 2,
                    dropped_packets: 1,
                    recorded_bytes: 16,
                },
            )
            .expect("failed terminal state");
        assert!(manager.finish_if_terminal());
        let completed = manager.status(first_id).expect("retained status");
        assert_eq!(completed.started_count(), 1);
        assert_eq!(
            completed.participants[0].state,
            LagCaptureParticipantState::Failed
        );
        let next_id = CaptureId::new([2; 16]).expect("different id");
        assert!(
            manager
                .begin(
                    start_with(next_id, 200),
                    &[participant],
                    TimestampMillis::from_unix_millis(10),
                )
                .is_ok()
        );
    }
}
