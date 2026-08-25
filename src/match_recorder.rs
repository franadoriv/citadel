//! Bridges the synchronous realtime lifecycle funnel to the durable log writer.
//!
//! It holds the process-local `RoomId -> match_id` directory that lets a
//! script's `citadel.log.write` be attributed to a match without widening the
//! runtime scope thread-local from a `Cell<Option<u64>>` to a `String`, and
//! without extending `NativeMatchContext`, whose whole trustworthiness rests on
//! the match id being chosen by the gateway and not supplied by game code.
//!
//! The server owns match open and close. Nothing here is reachable from a
//! script except [`MatchRecorder::set_result`], which stamps one nullable
//! column on a row the server already opened.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::durable_logs::DurableLogWriter;
use crate::realtime::{RoomId, RoomSnapshot};
use crate::repository::{LogLevel, MatchClose, MatchLogEntry, MatchOpen};
use crate::runtime::NativeMatchLifecycleHook;
use crate::time::{Clock, SystemClock};

/// Widest `result_json` a script may stamp on its match.
///
/// Server-side backstop for the host adapters' own validation: the column is
/// author-supplied, and a bounded queue must not be filled by one document.
pub const MAX_RESULT_JSON_BYTES: usize = 4_096;

/// Rooms tracked at once before the oldest entry is evicted.
///
/// Belt and braces. Entries are removed when the match ends, and a room prunes
/// the instant it empties, so this bound only matters if a lifecycle path ever
/// fails to fire its `Ended`.
const DEFAULT_DIRECTORY_CAPACITY: usize = 4_096;

/// Why the gateway ended a match.
///
/// The three variants are the only reasons `src/realtime/gateway.rs` produces,
/// and the `termination_reason` CHECK constraint on `matches` is this enum's
/// mirror. Anything unrecognized is rejected here and never written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchTerminationReason {
    /// The last member left.
    FinalDeparture,
    /// The server closed the room.
    ServerClosed,
    /// A matchmaker formation was abandoned before it played.
    FormationAbandoned,
}

impl MatchTerminationReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FinalDeparture => "final_departure",
            Self::ServerClosed => "server_closed",
            Self::FormationAbandoned => "formation_abandoned",
        }
    }

    /// Parse a gateway termination reason, or `None` for anything outside the
    /// vocabulary the schema accepts.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "final_departure" => Some(Self::FinalDeparture),
            "server_closed" => Some(Self::ServerClosed),
            "formation_abandoned" => Some(Self::FormationAbandoned),
            _ => None,
        }
    }
}

/// Why a script-facing recorder call was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchRecorderError {
    /// The call ran outside a match-scoped callback, so there is no row to
    /// stamp. A log line may be unscoped; a match result may not.
    NoActiveMatch,
    /// The result document exceeds [`MAX_RESULT_JSON_BYTES`].
    ResultTooLarge,
}

impl MatchRecorderError {
    /// The message the host adapters raise verbatim.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::NoActiveMatch => "match results require a match-scoped context",
            Self::ResultTooLarge => "match result exceeds the maximum size",
        }
    }
}

impl std::fmt::Display for MatchRecorderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for MatchRecorderError {}

/// What the directory remembers about one live match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchDirectoryEntry {
    /// The durable server-minted identity, stable for the room's whole life.
    pub match_id: String,
    /// Highest local + remote membership seen. Participant identities are
    /// deliberately not retained: only the count reaches the record.
    pub peak_participants: u32,
    /// How many admissions the room accepted over its life.
    pub join_total: u32,
    /// The script's own result document, applied when the server closes.
    pub result_json: Option<String>,
}

impl MatchDirectoryEntry {
    fn new(match_id: String) -> Self {
        Self {
            match_id,
            peak_participants: 0,
            join_total: 0,
            result_json: None,
        }
    }
}

#[derive(Debug, Default)]
struct Directory {
    entries: HashMap<RoomId, MatchDirectoryEntry>,
    /// Insertion order, so eviction drops the oldest surviving room.
    order: VecDeque<RoomId>,
}

/// The process-local match directory and the lifecycle funnel's record emitter.
#[derive(Debug)]
pub struct MatchRecorder {
    writer: Arc<DurableLogWriter>,
    directory: RwLock<Directory>,
    capacity: usize,
}

impl MatchRecorder {
    #[must_use]
    pub fn new(writer: Arc<DurableLogWriter>) -> Self {
        Self::with_capacity(writer, DEFAULT_DIRECTORY_CAPACITY)
    }

    #[must_use]
    pub fn with_capacity(writer: Arc<DurableLogWriter>, capacity: usize) -> Self {
        Self {
            writer,
            directory: RwLock::new(Directory::default()),
            capacity: capacity.max(1),
        }
    }

    /// The write-behind queue every record this recorder emits lands in.
    #[must_use]
    pub fn writer(&self) -> &Arc<DurableLogWriter> {
        &self.writer
    }

    /// Bind `room_id` to its durable match identity, reporting whether this
    /// call is the one that started tracking it.
    ///
    /// Idempotent: re-binding an already-tracked room keeps the counters it has
    /// accumulated and answers `false`, which is what keeps a lifecycle path
    /// that fires `Created` twice from queuing two open records.
    pub fn bind(&self, room_id: RoomId, match_id: String) -> bool {
        let mut directory = self.write();
        if directory.entries.contains_key(&room_id) {
            return false;
        }
        while directory.order.len() >= self.capacity {
            match directory.order.pop_front() {
                Some(evicted) => {
                    directory.entries.remove(&evicted);
                }
                None => break,
            }
        }
        directory
            .entries
            .insert(room_id, MatchDirectoryEntry::new(match_id));
        directory.order.push_back(room_id);
        true
    }

    /// Count one admission and raise the peak membership watermark.
    ///
    /// `participants` is the room's local member count plus its remote member
    /// count. The lifecycle context's participant list is per-connection and
    /// excludes remote members, so it is the wrong source for this number.
    pub fn observe_join(&self, room_id: RoomId, participants: usize) {
        let peak = u32::try_from(participants).unwrap_or(u32::MAX);
        let mut directory = self.write();
        if let Some(entry) = directory.entries.get_mut(&room_id) {
            entry.join_total = entry.join_total.saturating_add(1);
            entry.peak_participants = entry.peak_participants.max(peak);
        }
    }

    /// Stop tracking `room_id`, returning what the directory knew about it.
    pub fn unbind(&self, room_id: RoomId) -> Option<MatchDirectoryEntry> {
        let mut directory = self.write();
        let entry = directory.entries.remove(&room_id);
        if entry.is_some() {
            directory.order.retain(|tracked| *tracked != room_id);
        }
        entry
    }

    /// The directory row for `room_id`, while its match is still open.
    #[must_use]
    pub fn entry(&self, room_id: RoomId) -> Option<MatchDirectoryEntry> {
        self.read().entries.get(&room_id).cloned()
    }

    /// Resolve a room id to its durable match id.
    ///
    /// Returns `None` when the caller is not match-scoped — a global `on_tick`,
    /// a scheduled job, or a game with no match concept at all. That is a
    /// supported case, not a failure: the log row simply carries no match.
    #[must_use]
    pub fn match_id_of(&self, room_id: RoomId) -> Option<String> {
        self.read()
            .entries
            .get(&room_id)
            .map(|entry| entry.match_id.clone())
    }

    /// Rooms currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.read().entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stamp a script-supplied result on the match `room_id` is playing.
    ///
    /// Held until the server closes the match, so the result reaches the row in
    /// the same write that ends it and can never resurrect a closed match.
    ///
    /// # Errors
    /// [`MatchRecorderError::NoActiveMatch`] outside a match-scoped callback,
    /// [`MatchRecorderError::ResultTooLarge`] beyond [`MAX_RESULT_JSON_BYTES`].
    pub fn set_result(
        &self,
        room_id: RoomId,
        result_json: String,
    ) -> Result<(), MatchRecorderError> {
        if result_json.len() > MAX_RESULT_JSON_BYTES {
            return Err(MatchRecorderError::ResultTooLarge);
        }
        let mut directory = self.write();
        let entry = directory
            .entries
            .get_mut(&room_id)
            .ok_or(MatchRecorderError::NoActiveMatch)?;
        entry.result_json = Some(result_json);
        Ok(())
    }

    /// Observe a lifecycle transition before the script's handler runs.
    ///
    /// The split with [`Self::observe_after`] is load-bearing: the handler runs
    /// inside the gateway's dispatch, so the room must already be in the
    /// directory before that call and must survive until `on_match_ended`
    /// returns.
    ///
    /// `clock_epoch` is the hub-wide gameplay clock the gateway resolved for
    /// this dispatch; it is stored for forensic correlation only.
    pub fn observe_before(
        &self,
        hook: NativeMatchLifecycleHook,
        room: &RoomSnapshot,
        termination_reason: Option<MatchTerminationReason>,
        clock_epoch: u64,
        now_ms: u64,
    ) {
        match hook {
            NativeMatchLifecycleHook::Created => {
                if self.bind(room.id, room.match_id.clone()) {
                    self.writer
                        .enqueue_match_open(self.open_record(room, clock_epoch, now_ms));
                }
            }
            NativeMatchLifecycleHook::Join => {
                self.observe_join(room.id, room.members.len() + room.remote_member_count);
            }
            NativeMatchLifecycleHook::Ended => {
                let Some(reason) = termination_reason else {
                    // The schema accepts three reasons and the gateway produces
                    // exactly those. A close with none would be rejected by the
                    // CHECK, so the row stays open rather than failing a flush.
                    tracing::warn!(
                        room_id = room.id,
                        "match ended without a termination reason; no durable close was queued"
                    );
                    return;
                };
                // The entry may be gone if the directory bound overflowed, in
                // which case the open row exists but its counters do not. The
                // close is still queued: an UPDATE that matches nothing is a
                // no-op, and a match left open forever is not.
                let entry = self.entry(room.id);
                self.writer.enqueue_match_close(Self::close_record(
                    room,
                    entry.as_ref(),
                    reason,
                    now_ms,
                ));
            }
            NativeMatchLifecycleHook::Started
            | NativeMatchLifecycleHook::Leave
            | NativeMatchLifecycleHook::Tick => {}
        }
    }

    /// Observe a lifecycle transition after the script's handler returned.
    pub fn observe_after(&self, hook: NativeMatchLifecycleHook, room_id: RoomId) {
        if hook == NativeMatchLifecycleHook::Ended {
            self.unbind(room_id);
        }
    }

    /// The row a room's birth writes.
    ///
    /// `(node_id, boot_id, room_id)` is what reconstructs the RAM identity of a
    /// per-process room counter, so all three come from this node's own
    /// identity rather than from anything the snapshot carries.
    fn open_record(&self, room: &RoomSnapshot, clock_epoch: u64, now_ms: u64) -> MatchOpen {
        let identity = self.writer.identity();
        MatchOpen {
            match_id: room.match_id.clone(),
            node_id: identity.node_id().to_string(),
            boot_id: identity.boot_id().to_string(),
            room_id: room.id,
            name: room.name.clone(),
            map: room.label.map.clone(),
            mode: room.label.mode.clone(),
            max_players: room.label.max_players,
            script_revision_id: room
                .script_binding
                .as_ref()
                .map(|binding| binding.revision_id.clone()),
            script_generation: room
                .script_binding
                .as_ref()
                .map(|binding| binding.generation),
            clock_epoch,
            opened_at_ms: now_ms,
        }
    }

    /// The row a room's last departure writes. Counters come from the
    /// directory, which is the only place a match's history is accumulated;
    /// without an entry the record still closes, with nothing to report.
    fn close_record(
        room: &RoomSnapshot,
        entry: Option<&MatchDirectoryEntry>,
        reason: MatchTerminationReason,
        now_ms: u64,
    ) -> MatchClose {
        MatchClose {
            match_id: room.match_id.clone(),
            closed_at_ms: now_ms,
            termination_reason: reason.as_str().to_string(),
            peak_participants: entry.map_or(0, |entry| entry.peak_participants),
            join_total: entry.map_or(0, |entry| entry.join_total),
            result_json: entry.and_then(|entry| entry.result_json.clone()),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, Directory> {
        // The directory holds no cross-entry invariant a panicking writer could
        // break halfway, so a poisoned lock is recovered rather than escalated.
        self.directory
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, Directory> {
        self.directory
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// The narrow write handle the script runtimes are given.
///
/// It pairs the queue with the directory so `citadel.log.write` resolves its
/// own match scope, and exposes nothing else: a script can write a log line and
/// stamp a result, and cannot open, close, or rewrite a match record.
#[derive(Debug, Clone)]
pub struct MatchLogWriter {
    recorder: Arc<MatchRecorder>,
}

impl MatchLogWriter {
    #[must_use]
    pub fn new(recorder: Arc<MatchRecorder>) -> Self {
        Self { recorder }
    }

    /// Queue one script-written log line.
    ///
    /// `room_id` is the active runtime scope, or `None` outside a match-scoped
    /// callback — the line is stored either way, with `match_id` left `NULL`.
    /// Arguments are validated by the calling host adapter; this only mints the
    /// id and enqueues.
    pub fn write(
        &self,
        room_id: Option<RoomId>,
        level: LogLevel,
        tag: &str,
        message: &str,
        payload_json: Option<&str>,
    ) {
        let writer = self.recorder.writer();
        let created_at_ms = SystemClock.now().unix_millis();
        writer.enqueue_log(MatchLogEntry {
            log_id: writer.mint("ml1-", created_at_ms),
            match_id: room_id.and_then(|room_id| self.recorder.match_id_of(room_id)),
            node_id: writer.identity().node_id().to_string(),
            created_at_ms,
            level,
            tag: tag.to_string(),
            message: message.to_string(),
            payload_json: payload_json.map(str::to_string),
        });
    }

    /// Stamp `citadel.match.set_result` on the caller's match.
    ///
    /// # Errors
    /// [`MatchRecorderError::NoActiveMatch`] when the caller is not
    /// match-scoped; a match result, unlike a log line, requires a match.
    pub fn set_result(
        &self,
        room_id: Option<RoomId>,
        result_json: String,
    ) -> Result<(), MatchRecorderError> {
        let room_id = room_id.ok_or(MatchRecorderError::NoActiveMatch)?;
        self.recorder.set_result(room_id, result_json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LogsConfig;
    use crate::ids::NodeIdentity;

    fn recorder(capacity: usize) -> Arc<MatchRecorder> {
        let writer = Arc::new(DurableLogWriter::new(
            Arc::new(NodeIdentity::new("node-a")),
            LogsConfig::default(),
        ));
        Arc::new(MatchRecorder::with_capacity(writer, capacity))
    }

    #[test]
    fn termination_reasons_round_trip_and_reject_anything_else() {
        for reason in [
            MatchTerminationReason::FinalDeparture,
            MatchTerminationReason::ServerClosed,
            MatchTerminationReason::FormationAbandoned,
        ] {
            assert_eq!(MatchTerminationReason::parse(reason.as_str()), Some(reason));
        }
        assert_eq!(MatchTerminationReason::parse("abandoned"), None);
        assert_eq!(MatchTerminationReason::parse(""), None);
    }

    #[test]
    fn the_directory_resolves_a_room_to_its_match_and_forgets_it_on_unbind() {
        let recorder = recorder(8);
        recorder.bind(7, "mt1-a".to_string());
        assert_eq!(recorder.match_id_of(7).as_deref(), Some("mt1-a"));
        // A room with no match is a supported caller, not a failure.
        assert_eq!(recorder.match_id_of(8), None);
        assert_eq!(
            recorder.unbind(7).map(|entry| entry.match_id).as_deref(),
            Some("mt1-a")
        );
        assert_eq!(recorder.match_id_of(7), None);
        assert!(recorder.is_empty());
    }

    #[test]
    fn binding_a_tracked_room_again_keeps_its_counters() {
        let recorder = recorder(8);
        recorder.bind(1, "mt1-a".to_string());
        recorder.observe_join(1, 3);
        recorder.bind(1, "mt1-b".to_string());
        let entry = recorder.entry(1).expect("entry");
        assert_eq!(entry.match_id, "mt1-a", "a room's identity never changes");
        assert_eq!(entry.join_total, 1);
    }

    #[test]
    fn joins_accumulate_and_the_peak_only_rises() {
        let recorder = recorder(8);
        recorder.bind(1, "mt1-a".to_string());
        recorder.observe_join(1, 1);
        recorder.observe_join(1, 4);
        recorder.observe_join(1, 2);
        let entry = recorder.entry(1).expect("entry");
        assert_eq!(entry.join_total, 3);
        assert_eq!(entry.peak_participants, 4);
    }

    #[test]
    fn the_directory_is_bounded_and_evicts_the_oldest_room() {
        let recorder = recorder(2);
        recorder.bind(1, "mt1-a".to_string());
        recorder.bind(2, "mt1-b".to_string());
        recorder.bind(3, "mt1-c".to_string());
        assert_eq!(recorder.len(), 2);
        assert_eq!(recorder.match_id_of(1), None);
        assert_eq!(recorder.match_id_of(3).as_deref(), Some("mt1-c"));
    }

    #[test]
    fn a_result_needs_a_match_and_is_bounded() {
        let recorder = recorder(8);
        assert_eq!(
            recorder.set_result(1, "{}".to_string()),
            Err(MatchRecorderError::NoActiveMatch)
        );
        recorder.bind(1, "mt1-a".to_string());
        assert_eq!(
            recorder.set_result(1, "x".repeat(MAX_RESULT_JSON_BYTES + 1)),
            Err(MatchRecorderError::ResultTooLarge)
        );
        recorder
            .set_result(1, "{\"winner\":\"kitsune\"}".to_string())
            .expect("stamp");
        assert_eq!(
            recorder.entry(1).and_then(|entry| entry.result_json),
            Some("{\"winner\":\"kitsune\"}".to_string())
        );
    }

    #[test]
    fn a_script_log_outside_a_match_is_queued_with_no_match_id() {
        let recorder = recorder(8);
        let writer = MatchLogWriter::new(Arc::clone(&recorder));
        writer.write(None, LogLevel::Info, "world", "tick", None);
        recorder.bind(4, "mt1-a".to_string());
        writer.write(
            Some(4),
            LogLevel::Warn,
            "combat",
            "hit",
            Some("{\"dmg\":3}"),
        );
        assert_eq!(recorder.writer().queued_total(), 2);
    }

    #[test]
    fn a_script_result_outside_a_match_is_refused() {
        let recorder = recorder(8);
        let writer = MatchLogWriter::new(recorder);
        assert_eq!(
            writer.set_result(None, "{}".to_string()),
            Err(MatchRecorderError::NoActiveMatch)
        );
    }
}
