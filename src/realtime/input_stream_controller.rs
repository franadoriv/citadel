//! Server-owned leases for authoritative input streams.
//!
//! The controller is transport-neutral: it binds lifecycle context supplied by
//! its server-side caller, but does not authenticate that caller or establish
//! ownership of the supplied room, participant, binding, or clock values.
//! Gateway integration must establish that authority separately. The controller
//! mints the opaque token and stream ID itself, and keeps all active lifecycle
//! state under one mutex so mint, renew, and revoke are atomic.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use citadel_wire::authoritative_input::INPUT_STREAM_TOKEN_BYTES;
pub use citadel_wire::authoritative_input::InputStreamToken;

use super::{ParticipantId, RoomId};

type LeaseKey = (RoomId, ParticipantId);

/// The next server-issued stream ID, shared by every controller instance.
static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// Source of opaque bytes used by the controller.
///
/// This remains private so production callers cannot substitute predictable
/// entropy. Unit tests inject deterministic sources through the test-only
/// constructor below.
trait InputStreamTokenSource: Send {
    fn fill(&mut self, bytes: &mut [u8; INPUT_STREAM_TOKEN_BYTES]) -> Result<(), TokenSourceError>;
}

/// A token source could not provide bytes for a lease transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenSourceError {
    /// Operating-system entropy was unavailable.
    Unavailable,
}

struct CsprngTokenSource;

impl InputStreamTokenSource for CsprngTokenSource {
    fn fill(&mut self, bytes: &mut [u8; INPUT_STREAM_TOKEN_BYTES]) -> Result<(), TokenSourceError> {
        getrandom::fill(bytes).map_err(|_| TokenSourceError::Unavailable)
    }
}

/// Limits retained server-owned leases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputStreamControllerConfig {
    max_retained_leases: usize,
    max_queued_inputs: usize,
    max_queued_inputs_per_lease: usize,
}

impl InputStreamControllerConfig {
    /// Build a controller limit. A zero cap rejects every mint.
    ///
    /// The default ingress queue holds two frames per retained lease while each
    /// exact lease may hold only one. That keeps the per-lease capacity strictly
    /// below the aggregate capacity, including for the smallest live controller,
    /// so callers that do not override limits retain a fair bounded baseline.
    #[must_use]
    pub const fn new(max_retained_leases: usize) -> Self {
        Self {
            max_retained_leases,
            max_queued_inputs: max_retained_leases.saturating_mul(2),
            max_queued_inputs_per_lease: max_retained_leases,
        }
    }

    /// Override the bounded, not-yet-drained stream-input queue capacity.
    #[must_use]
    pub const fn with_queued_input_capacity(mut self, max_queued_inputs: usize) -> Self {
        self.max_queued_inputs = max_queued_inputs;
        self
    }

    /// Override the bounded queue capacity for one exact live lease. This cap
    /// is independent from the global cap so one participant cannot occupy the
    /// whole fixed-tick backlog.
    #[must_use]
    pub const fn with_per_lease_queued_input_capacity(
        mut self,
        max_queued_inputs_per_lease: usize,
    ) -> Self {
        self.max_queued_inputs_per_lease = max_queued_inputs_per_lease;
        self
    }
}

/// A server-minted input-stream lease bound to one exact lifecycle tuple.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct InputStreamLease {
    room: RoomId,
    participant: ParticipantId,
    stream_id: u64,
    binding_generation: u64,
    clock_epoch: u64,
    token: InputStreamToken,
}

impl fmt::Debug for InputStreamLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InputStreamLease")
            .field("match_id", &self.room)
            .field("participant_id", &self.participant.get())
            .field("stream_id", &self.stream_id)
            .field("binding_generation", &self.binding_generation)
            .field("clock_epoch", &self.clock_epoch)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl InputStreamLease {
    /// The room identity bound into this lease; gateway validates its authority.
    #[must_use]
    pub const fn match_id(&self) -> RoomId {
        self.room
    }

    /// The server-owned participant bound into this lease.
    #[must_use]
    pub(crate) const fn participant(&self) -> ParticipantId {
        self.participant
    }

    /// The globally monotonic server-issued stream identity for this lease.
    #[must_use]
    pub const fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// The binding generation bound into this lease.
    #[must_use]
    pub const fn binding_generation(&self) -> u64 {
        self.binding_generation
    }

    /// The gameplay-clock epoch bound into this lease.
    #[must_use]
    pub const fn clock_epoch(&self) -> u64 {
        self.clock_epoch
    }

    /// The opaque server-issued token.
    #[must_use]
    pub const fn token(&self) -> InputStreamToken {
        self.token
    }
}

/// Why a lease operation did not mutate controller state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputStreamControllerError {
    /// The exact `(RoomId, ParticipantId)` stream already has a current lease.
    ActiveLease,
    /// A renew or revoke did not present the exact current lifecycle tuple.
    StaleLease,
    /// Retaining another active lease would exceed the configured cap.
    RetainedLeaseCapacity,
    /// The aggregate stream-input queue reached its configured bound.
    GlobalQueueCapacity,
    /// One exact lease reached its configured queue bound.
    LeaseQueueCapacity,
    /// A retransmission exactly matched a frame already queued for this lease.
    DuplicateSequence,
    /// A reused sequence named a different kind or opaque payload.
    ConflictingSequence,
    /// A sequence was already consumed by a fixed-tick drain for this lease.
    StaleSequence,
    /// The token source failed before a new lease could be installed.
    TokenSource(TokenSourceError),
    /// The source produced an all-zero token, which is rejected fail-closed.
    InvalidToken,
    /// The candidate token is already held by an active lease.
    TokenReuse,
    /// The server-wide stream ID space is exhausted.
    StreamIdExhausted,
    /// A poisoned controller mutex cannot safely establish an atomic transition.
    Unavailable,
}

struct State {
    source: Box<dyn InputStreamTokenSource>,
    leases: HashMap<LeaseKey, LeaseState>,
    queued_count: usize,
    /// Lease keys with nonempty queues, once each, in deterministic service
    /// order. A fixed-tick drain removes one lowest sequence then moves the
    /// key to the tail, so no lease can monopolize a tick.
    round_robin: VecDeque<LeaseKey>,
    /// Deterministic unit-test seam for a failure at one exact revoke boundary.
    /// It is compiled out of production builds and deliberately leaves all
    /// controller state untouched when it triggers.
    #[cfg(test)]
    fail_revoke_after: Option<usize>,
}

struct LeaseState {
    lease: InputStreamLease,
    queued: BTreeMap<u64, QueuedInput>,
    last_consumed_sequence: u64,
    acknowledged_sequence: u64,
    decided_sequences: BTreeSet<u64>,
}

/// One stream-input frame accepted against an exact server-owned lease.
///
/// This is deliberately private: the first stream-input slice only establishes
/// bounded, fenced acceptance. Script draining, receipts, and telemetry follow in
/// later slices and cannot observe an entry after its lease retires.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueuedInput {
    pub(crate) lease: InputStreamLease,
    pub(crate) sequence: u64,
    pub(crate) original_custom_kind: u16,
    pub(crate) body: Vec<u8>,
}

/// Atomic server-owned controller for active input-stream leases.
pub struct InputStreamController {
    config: InputStreamControllerConfig,
    state: Mutex<State>,
}

impl InputStreamController {
    /// Construct an empty controller backed by operating-system CSPRNG entropy.
    #[must_use]
    pub fn new(config: InputStreamControllerConfig) -> Self {
        Self::from_source(config, CsprngTokenSource)
    }

    #[cfg(test)]
    fn with_token_source<S>(config: InputStreamControllerConfig, source: S) -> Self
    where
        S: InputStreamTokenSource + 'static,
    {
        Self::from_source(config, source)
    }

    fn from_source<S>(config: InputStreamControllerConfig, source: S) -> Self
    where
        S: InputStreamTokenSource + 'static,
    {
        Self {
            config,
            state: Mutex::new(State {
                source: Box::new(source),
                leases: HashMap::new(),
                queued_count: 0,
                round_robin: VecDeque::new(),
                #[cfg(test)]
                fail_revoke_after: None,
            }),
        }
    }

    /// Fail exactly one `revoke` after `successful_revocations` exact revokes.
    /// This exists solely to prove multi-member gateway teardown cannot expose a
    /// half-retired stream-input capability.
    #[cfg(test)]
    pub(crate) fn fail_revoke_after_for_test(&self, successful_revocations: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.fail_revoke_after = Some(successful_revocations);
        }
    }

    /// Atomically mint the first lease for one exact room/participant stream.
    pub fn mint(
        &self,
        room: RoomId,
        participant: ParticipantId,
        binding_generation: u64,
        clock_epoch: u64,
    ) -> Result<InputStreamLease, InputStreamControllerError> {
        let mut state = self.lock_state()?;
        let key = (room, participant);
        if state.leases.contains_key(&key) {
            return Err(InputStreamControllerError::ActiveLease);
        }
        if state.leases.len() >= self.config.max_retained_leases {
            return Err(InputStreamControllerError::RetainedLeaseCapacity);
        }

        let token = issue_token(&mut *state.source)?;
        if token_is_active(&state.leases, token) {
            return Err(InputStreamControllerError::TokenReuse);
        }
        let lease = InputStreamLease {
            room,
            participant,
            stream_id: issue_stream_id()?,
            binding_generation,
            clock_epoch,
            token,
        };
        state.leases.insert(
            key,
            LeaseState {
                lease,
                queued: BTreeMap::new(),
                last_consumed_sequence: 0,
                acknowledged_sequence: 0,
                decided_sequences: BTreeSet::new(),
            },
        );
        Ok(lease)
    }

    /// Atomically replace a current lease's token, invalidating the old lease.
    pub fn renew(
        &self,
        current: &InputStreamLease,
    ) -> Result<InputStreamLease, InputStreamControllerError> {
        let mut state = self.lock_state()?;
        let key = (current.room, current.participant);
        if state.leases.get(&key).map(|slot| &slot.lease) != Some(current) {
            return Err(InputStreamControllerError::StaleLease);
        }

        let token = issue_token(&mut *state.source)?;
        if token_is_active(&state.leases, token) {
            return Err(InputStreamControllerError::TokenReuse);
        }
        let replacement = InputStreamLease { token, ..*current };
        let previous = state.leases.insert(
            key,
            LeaseState {
                lease: replacement,
                queued: BTreeMap::new(),
                last_consumed_sequence: 0,
                acknowledged_sequence: 0,
                decided_sequences: BTreeSet::new(),
            },
        );
        state.queued_count -= previous.expect("exact lease was checked").queued.len();
        state.round_robin.retain(|queued_key| *queued_key != key);
        Ok(replacement)
    }

    /// Atomically revoke only the exact current lifecycle tuple.
    pub fn revoke(&self, current: &InputStreamLease) -> Result<(), InputStreamControllerError> {
        let mut state = self.lock_state()?;
        let key = (current.room, current.participant);
        if state.leases.get(&key).map(|slot| &slot.lease) != Some(current) {
            return Err(InputStreamControllerError::StaleLease);
        }
        #[cfg(test)]
        if let Some(remaining) = state.fail_revoke_after.as_mut() {
            if *remaining == 0 {
                state.fail_revoke_after = None;
                return Err(InputStreamControllerError::Unavailable);
            }
            *remaining -= 1;
        }
        let removed = state.leases.remove(&key).expect("exact lease was checked");
        state.queued_count -= removed.queued.len();
        state.round_robin.retain(|queued_key| *queued_key != key);
        Ok(())
    }

    /// Validate and enqueue a stream-input frame against exactly one active lease.
    ///
    /// The gateway has already derived the lease tuple from its server-owned
    /// room, membership, binding, and clock scope. Requiring the complete lease
    /// image here makes revocation an immediate fail-closed ingress boundary:
    /// once `revoke` wins this controller lock, a stale token cannot queue and
    /// any entries for that exact stream are purged.
    pub(crate) fn enqueue_if_current(
        &self,
        lease: InputStreamLease,
        sequence: u64,
        original_custom_kind: u16,
        body: Vec<u8>,
    ) -> Result<(), InputStreamControllerError> {
        let mut state = self.lock_state()?;
        let key = (lease.room, lease.participant);
        let Some(slot) = state.leases.get(&key) else {
            return Err(InputStreamControllerError::StaleLease);
        };
        if slot.lease != lease {
            return Err(InputStreamControllerError::StaleLease);
        }
        if sequence <= slot.last_consumed_sequence {
            return Err(InputStreamControllerError::StaleSequence);
        }
        if let Some(existing) = slot.queued.get(&sequence) {
            return if existing.original_custom_kind == original_custom_kind && existing.body == body
            {
                Err(InputStreamControllerError::DuplicateSequence)
            } else {
                Err(InputStreamControllerError::ConflictingSequence)
            };
        }
        if slot.queued.len() >= self.config.max_queued_inputs_per_lease {
            return Err(InputStreamControllerError::LeaseQueueCapacity);
        }
        if state.queued_count >= self.config.max_queued_inputs {
            return Err(InputStreamControllerError::GlobalQueueCapacity);
        }
        let slot = state
            .leases
            .get_mut(&key)
            .expect("exact lease was checked while controller lock is held");
        let was_empty = slot.queued.is_empty();
        slot.queued.insert(
            sequence,
            QueuedInput {
                lease,
                sequence,
                original_custom_kind,
                body,
            },
        );
        state.queued_count += 1;
        if was_empty {
            state.round_robin.push_back(key);
        }
        Ok(())
    }

    /// Consume at most `max_inputs` accepted frames at the fixed server tick.
    ///
    /// Draining advances per-lease sequence state before returning the frame:
    /// once a tick owns a sequence it can never be re-enqueued, even if later
    /// script dispatch fails. Entries are selected round-robin across live
    /// leases and in ascending sequence order within each lease.
    pub(crate) fn drain_for_fixed_tick(
        &self,
        max_inputs: usize,
    ) -> Result<Vec<QueuedInput>, InputStreamControllerError> {
        let mut state = self.lock_state()?;
        let mut drained = Vec::with_capacity(max_inputs.min(state.queued_count));
        while drained.len() < max_inputs {
            let Some(key) = state.round_robin.pop_front() else {
                break;
            };
            let Some((input, has_remaining)) = state.leases.get_mut(&key).and_then(|slot| {
                let (sequence, input) = slot.queued.pop_first()?;
                debug_assert!(sequence > slot.last_consumed_sequence);
                slot.last_consumed_sequence = sequence;
                Some((input, !slot.queued.is_empty()))
            }) else {
                continue;
            };
            state.queued_count -= 1;
            if has_remaining {
                state.round_robin.push_back(key);
            }
            drained.push(input);
        }
        Ok(drained)
    }

    /// Mark one already-drained input as authoritatively decided and return the
    /// highest contiguous decided sequence for its exact current lease.
    ///
    /// Receipt acknowledgement is controller-owned so a stale bridge answer
    /// cannot advance a replacement stream or produce a duplicate receipt.
    pub(crate) fn acknowledge_if_current(
        &self,
        lease: InputStreamLease,
        sequence: u64,
    ) -> Result<u64, InputStreamControllerError> {
        let mut state = self.lock_state()?;
        let key = (lease.room, lease.participant);
        let Some(slot) = state.leases.get_mut(&key) else {
            return Err(InputStreamControllerError::StaleLease);
        };
        if slot.lease != lease || sequence == 0 || sequence > slot.last_consumed_sequence {
            return Err(InputStreamControllerError::StaleLease);
        }
        if !slot.decided_sequences.insert(sequence) {
            return Err(InputStreamControllerError::StaleSequence);
        }
        while let Some(next) = slot.acknowledged_sequence.checked_add(1)
            && slot.decided_sequences.remove(&next)
        {
            slot.acknowledged_sequence = next;
        }
        Ok(slot.acknowledged_sequence)
    }

    /// Restore one exact lease during a gateway-owned failed room transition.
    ///
    /// This is deliberately narrower than `mint`: it never creates client
    /// controlled material and is only usable by the trusted gateway to put back
    /// the opaque tuple it captured before revoking it. A different active lease
    /// or any token collision fails closed rather than overwriting state.
    pub(crate) fn restore(
        &self,
        lease: InputStreamLease,
    ) -> Result<(), InputStreamControllerError> {
        let mut state = self.lock_state()?;
        let key = (lease.room, lease.participant);
        match state.leases.get(&key) {
            Some(current) if current.lease == lease => return Ok(()),
            Some(_) => return Err(InputStreamControllerError::ActiveLease),
            None => {}
        }
        if state.leases.len() >= self.config.max_retained_leases {
            return Err(InputStreamControllerError::RetainedLeaseCapacity);
        }
        if token_is_active(&state.leases, lease.token) {
            return Err(InputStreamControllerError::TokenReuse);
        }
        state.leases.insert(
            key,
            LeaseState {
                lease,
                queued: BTreeMap::new(),
                last_consumed_sequence: 0,
                acknowledged_sequence: 0,
                decided_sequences: BTreeSet::new(),
            },
        );
        Ok(())
    }

    /// Remove every lease from a room after server-owned room teardown.
    pub fn retire_room(&self, room: RoomId) -> Result<usize, InputStreamControllerError> {
        self.retire_where(|lease| lease.room == room)
    }

    /// Remove every lease for a disconnected or removed participant.
    pub fn retire_participant(
        &self,
        participant: ParticipantId,
    ) -> Result<usize, InputStreamControllerError> {
        self.retire_where(|lease| lease.participant == participant)
    }

    /// Remove leases for a binding generation only within the retired room.
    pub fn retire_binding_generation(
        &self,
        room: RoomId,
        binding_generation: u64,
    ) -> Result<usize, InputStreamControllerError> {
        self.retire_where(|lease| {
            lease.room == room && lease.binding_generation == binding_generation
        })
    }

    /// Remove leases for a clock epoch only within the retired room.
    pub fn retire_clock_epoch(
        &self,
        room: RoomId,
        clock_epoch: u64,
    ) -> Result<usize, InputStreamControllerError> {
        self.retire_where(|lease| lease.room == room && lease.clock_epoch == clock_epoch)
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, State>, InputStreamControllerError> {
        self.state
            .lock()
            .map_err(|_| InputStreamControllerError::Unavailable)
    }

    fn retire_where(
        &self,
        mut predicate: impl FnMut(&InputStreamLease) -> bool,
    ) -> Result<usize, InputStreamControllerError> {
        let mut state = self.lock_state()?;
        let retained_before = state.leases.len();
        state.leases.retain(|_, slot| !predicate(&slot.lease));
        state.queued_count = state.leases.values().map(|slot| slot.queued.len()).sum();
        let live_queued: HashSet<_> = state
            .leases
            .iter()
            .filter_map(|(key, slot)| (!slot.queued.is_empty()).then_some(*key))
            .collect();
        state.round_robin.retain(|key| live_queued.contains(key));
        Ok(retained_before - state.leases.len())
    }

    /// Number of retained active leases, or unavailable if state is poisoned.
    pub fn active_lease_count(&self) -> Result<usize, InputStreamControllerError> {
        Ok(self.lock_state()?.leases.len())
    }

    /// Number of stream-input frames that passed exact lease validation and await a later
    /// server-owned drain stage. This exposes no token or payload material.
    pub fn queued_input_count(&self) -> Result<usize, InputStreamControllerError> {
        Ok(self.lock_state()?.queued_count)
    }
}

fn issue_stream_id() -> Result<u64, InputStreamControllerError> {
    NEXT_STREAM_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| InputStreamControllerError::StreamIdExhausted)
}

fn token_is_active(leases: &HashMap<LeaseKey, LeaseState>, token: InputStreamToken) -> bool {
    leases.values().any(|slot| slot.lease.token == token)
}

fn issue_token(
    source: &mut dyn InputStreamTokenSource,
) -> Result<InputStreamToken, InputStreamControllerError> {
    let mut bytes = [0_u8; INPUT_STREAM_TOKEN_BYTES];
    source
        .fill(&mut bytes)
        .map_err(InputStreamControllerError::TokenSource)?;
    InputStreamToken::new(bytes).map_err(|_| InputStreamControllerError::InvalidToken)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    #[derive(Debug)]
    struct RepeatedTokens;

    impl InputStreamTokenSource for RepeatedTokens {
        fn fill(
            &mut self,
            bytes: &mut [u8; INPUT_STREAM_TOKEN_BYTES],
        ) -> Result<(), TokenSourceError> {
            bytes.fill(7);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct TokenSequence {
        tokens: VecDeque<[u8; INPUT_STREAM_TOKEN_BYTES]>,
    }

    impl TokenSequence {
        fn new<const N: usize>(tokens: [[u8; INPUT_STREAM_TOKEN_BYTES]; N]) -> Self {
            Self {
                tokens: tokens.into(),
            }
        }
    }

    impl InputStreamTokenSource for TokenSequence {
        fn fill(
            &mut self,
            bytes: &mut [u8; INPUT_STREAM_TOKEN_BYTES],
        ) -> Result<(), TokenSourceError> {
            *bytes = self
                .tokens
                .pop_front()
                .ok_or(TokenSourceError::Unavailable)?;
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailOnceThenToken {
        failed: bool,
    }

    impl InputStreamTokenSource for FailOnceThenToken {
        fn fill(
            &mut self,
            bytes: &mut [u8; INPUT_STREAM_TOKEN_BYTES],
        ) -> Result<(), TokenSourceError> {
            if !self.failed {
                self.failed = true;
                return Err(TokenSourceError::Unavailable);
            }
            bytes.fill(9);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct PanicTokens;

    impl InputStreamTokenSource for PanicTokens {
        fn fill(
            &mut self,
            _bytes: &mut [u8; INPUT_STREAM_TOKEN_BYTES],
        ) -> Result<(), TokenSourceError> {
            panic!("test token source panics while the controller lock is held");
        }
    }

    #[test]
    fn repeated_source_cannot_reuse_an_active_token() {
        let controller = InputStreamController::with_token_source(
            InputStreamControllerConfig::new(2),
            RepeatedTokens,
        );
        let first = controller
            .mint(11, ParticipantId::from_raw(1), 3, 5)
            .expect("first token installs");

        assert_eq!(
            controller.mint(12, ParticipantId::from_raw(2), 3, 5),
            Err(InputStreamControllerError::TokenReuse)
        );
        assert_eq!(controller.active_lease_count(), Ok(1));
        assert_eq!(controller.revoke(&first), Ok(()));
        assert!(
            controller
                .mint(12, ParticipantId::from_raw(2), 3, 5)
                .is_ok()
        );
    }

    #[test]
    fn renew_and_revoke_require_every_lease_binding_field_to_match() {
        let controller = InputStreamController::with_token_source(
            InputStreamControllerConfig::new(1),
            RepeatedTokens,
        );
        let lease = controller
            .mint(11, ParticipantId::from_raw(1), 3, 5)
            .expect("initial lease installs");
        let unrelated_token = InputStreamToken::new([8; INPUT_STREAM_TOKEN_BYTES])
            .expect("nonzero test token is valid");

        for altered in [
            InputStreamLease { room: 12, ..lease },
            InputStreamLease {
                participant: ParticipantId::from_raw(2),
                ..lease
            },
            InputStreamLease {
                stream_id: lease.stream_id + 1,
                ..lease
            },
            InputStreamLease {
                token: unrelated_token,
                ..lease
            },
            InputStreamLease {
                binding_generation: 4,
                ..lease
            },
            InputStreamLease {
                clock_epoch: 6,
                ..lease
            },
        ] {
            assert_eq!(
                controller.renew(&altered),
                Err(InputStreamControllerError::StaleLease)
            );
            assert_eq!(
                controller.revoke(&altered),
                Err(InputStreamControllerError::StaleLease)
            );
        }

        assert_eq!(controller.revoke(&lease), Ok(()));
    }

    #[test]
    fn failed_mint_and_renew_leave_the_current_lease_unchanged() {
        let controller = InputStreamController::with_token_source(
            InputStreamControllerConfig::new(1),
            FailOnceThenToken { failed: false },
        );

        assert_eq!(
            controller.mint(11, ParticipantId::from_raw(1), 3, 5),
            Err(InputStreamControllerError::TokenSource(
                TokenSourceError::Unavailable
            ))
        );
        assert_eq!(controller.active_lease_count(), Ok(0));
        let lease = controller
            .mint(11, ParticipantId::from_raw(1), 3, 5)
            .expect("failed mint did not reserve the lease");

        assert_eq!(
            controller.renew(&lease),
            Err(InputStreamControllerError::TokenReuse)
        );
        assert_eq!(controller.revoke(&lease), Ok(()));
    }

    #[test]
    fn fixed_tick_drain_is_round_robin_bounded_and_sequence_exact() {
        let controller = InputStreamController::with_token_source(
            InputStreamControllerConfig::new(2)
                .with_queued_input_capacity(3)
                .with_per_lease_queued_input_capacity(2),
            TokenSequence::new([[1; INPUT_STREAM_TOKEN_BYTES], [2; INPUT_STREAM_TOKEN_BYTES]]),
        );
        let first = controller
            .mint(11, ParticipantId::from_raw(1), 3, 5)
            .expect("first lease");
        let second = controller
            .mint(11, ParticipantId::from_raw(2), 3, 5)
            .expect("second lease");

        assert_eq!(
            controller.enqueue_if_current(first, 1, 700, vec![1]),
            Ok(())
        );
        assert_eq!(
            controller.enqueue_if_current(first, 2, 700, vec![2]),
            Ok(())
        );
        assert_eq!(
            controller.enqueue_if_current(second, 1, 701, vec![3]),
            Ok(())
        );
        assert_eq!(
            controller.enqueue_if_current(second, 2, 701, vec![4]),
            Err(InputStreamControllerError::GlobalQueueCapacity)
        );
        assert_eq!(
            controller.enqueue_if_current(first, 2, 700, vec![2]),
            Err(InputStreamControllerError::DuplicateSequence)
        );
        assert_eq!(
            controller.enqueue_if_current(first, 2, 702, vec![2]),
            Err(InputStreamControllerError::ConflictingSequence)
        );

        let drained = controller.drain_for_fixed_tick(3).expect("drain");
        assert_eq!(
            drained
                .iter()
                .map(|input| (input.lease, input.sequence, input.body.clone()))
                .collect::<Vec<_>>(),
            vec![
                (first, 1, vec![1]),
                (second, 1, vec![3]),
                (first, 2, vec![2])
            ],
            "one frame per lease per round prevents a saturated lease from monopolizing a tick"
        );
        assert_eq!(controller.queued_input_count(), Ok(0));
        assert_eq!(
            controller.enqueue_if_current(first, 1, 700, vec![1]),
            Err(InputStreamControllerError::StaleSequence)
        );
    }

    #[test]
    fn revoke_purges_queued_frames_and_their_sequence_history_with_the_lease() {
        let controller = InputStreamController::with_token_source(
            InputStreamControllerConfig::new(1),
            RepeatedTokens,
        );
        let lease = controller
            .mint(11, ParticipantId::from_raw(1), 3, 5)
            .expect("lease");
        assert_eq!(
            controller.enqueue_if_current(lease, 1, 700, vec![1]),
            Ok(())
        );
        assert_eq!(controller.revoke(&lease), Ok(()));
        assert_eq!(controller.queued_input_count(), Ok(0));
        assert_eq!(
            controller.enqueue_if_current(lease, 2, 700, vec![2]),
            Err(InputStreamControllerError::StaleLease)
        );
    }

    #[test]
    fn poisoned_controller_returns_unavailable_without_hiding_counts() {
        let controller = InputStreamController::with_token_source(
            InputStreamControllerConfig::new(1),
            PanicTokens,
        );

        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                let _ = controller.mint(11, ParticipantId::from_raw(1), 3, 5);
            }))
            .is_err()
        );

        assert_eq!(
            controller.active_lease_count(),
            Err(InputStreamControllerError::Unavailable)
        );
        assert_eq!(
            controller.retire_room(11),
            Err(InputStreamControllerError::Unavailable)
        );
        assert_eq!(
            controller.retire_participant(ParticipantId::from_raw(1)),
            Err(InputStreamControllerError::Unavailable)
        );
        assert_eq!(
            controller.retire_binding_generation(11, 3),
            Err(InputStreamControllerError::Unavailable)
        );
        assert_eq!(
            controller.retire_clock_epoch(11, 5),
            Err(InputStreamControllerError::Unavailable)
        );
        assert_eq!(
            controller.mint(11, ParticipantId::from_raw(1), 3, 5),
            Err(InputStreamControllerError::Unavailable)
        );
    }
}
