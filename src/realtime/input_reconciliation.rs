//! Server-owned, per-room and per-participant ordering for sequenced inputs.
//!
//! This module is intentionally transport- and payload-neutral. Callers supply
//! room and participant identities resolved by the server; it only releases a
//! contiguous prefix to the authoritative consumer. A caller must also present
//! the opaque stream incarnation issued by this server, so an input delayed from
//! a reset stream cannot enter its replacement.

use std::collections::{BTreeMap, HashMap};

use super::{ParticipantId, RoomId};

type StreamKey = (RoomId, ParticipantId);

/// Bounds for server-owned sequenced-input streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputReconciliationConfig {
    max_ahead: u64,
    max_buffered: usize,
    max_retained_streams: usize,
}

impl InputReconciliationConfig {
    /// Construct the per-stream ahead/buffer bounds and global retained-stream capacity.
    ///
    /// The retained-stream cap bounds every per-stream allocation: fences,
    /// watermarks, incarnations, and any out-of-order payload queue. Capacity
    /// is released only by the server-owned [`InputReconciliation::retire`]
    /// lifecycle operation.
    #[must_use]
    pub const fn new(max_ahead: u64, max_buffered: usize, max_retained_streams: usize) -> Self {
        Self {
            max_ahead,
            max_buffered,
            max_retained_streams,
        }
    }
}

/// An opaque, server-issued incarnation for one input stream instance.
///
/// Its raw value is intentionally not exposed: it is created only by
/// [`InputReconciliation::start`] and [`InputReconciliation::reset`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InputStreamIncarnation(u64);

/// An input released to the authoritative consumer in contiguous sequence order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleasedInput<T> {
    /// The server-ordered, non-zero sequence number.
    pub sequence: u64,
    /// The input payload. Ownership moves out at release; the queue retains none.
    pub payload: T,
}

/// Why an input or lifecycle operation did not mutate its server-owned stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputRejection {
    /// Sequence zero is reserved as the absence of input.
    SequenceZero,
    /// An identical input with this sequence is already waiting behind a gap.
    ExactDuplicate,
    /// Different payloads claimed the same sequence while it was waiting behind a gap.
    ConflictingDuplicate,
    /// The sequence is at or below the already released contiguous watermark.
    Stale,
    /// The sequence sits beyond this queue's configured ahead window.
    AheadWindow,
    /// A new gap would exceed this queue's bounded out-of-order buffer.
    BufferOverflow,
    /// Starting a stream would exceed the global retained-fence capacity.
    RetainedStreamCapacity,
    /// No fresh opaque incarnation can be issued without wrapping its namespace.
    IncarnationExhausted,
    /// The supplied stream incarnation is absent or has been superseded by a reset.
    StaleIncarnation,
}

/// The result of offering one input to its server-owned queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputDisposition<T> {
    /// One or more inputs are now contiguous and ready for authoritative handling.
    Released(Vec<ReleasedInput<T>>),
    /// The input is not yet contiguous.
    Buffered,
    /// The input was not retained or released.
    Rejected(InputRejection),
}

#[derive(Debug)]
struct StreamFence {
    incarnation: InputStreamIncarnation,
    last_released: u64,
}

#[derive(Debug)]
struct Queue<T> {
    pending: BTreeMap<u64, T>,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            pending: BTreeMap::new(),
        }
    }
}

/// Server-owned queues keyed by the room and participant that own each input stream.
///
/// `queues` contains only streams with out-of-order payloads. A contiguous
/// release drains and removes its queue immediately, but its fence remains until
/// the server explicitly retires the stream. This keeps delayed input fenced
/// while enforcing one global cap over every retained fence, watermark,
/// incarnation, and payload queue.
#[derive(Debug)]
pub struct InputReconciliation<T> {
    config: InputReconciliationConfig,
    fences: HashMap<StreamKey, StreamFence>,
    queues: HashMap<StreamKey, Queue<T>>,
    next_incarnation: u64,
}

impl<T> InputReconciliation<T> {
    /// Create empty server-owned input queues.
    #[must_use]
    pub fn new(config: InputReconciliationConfig) -> Self {
        Self {
            config,
            fences: HashMap::new(),
            queues: HashMap::new(),
            next_incarnation: 0,
        }
    }

    /// Start a server-owned stream, returning its opaque current incarnation.
    ///
    /// Starting an already-known stream is idempotent; only a matching
    /// [`Self::reset`] can replace its incarnation. A server must call
    /// [`Self::retire`] after its close/leave lifecycle has made the stream
    /// unreachable; otherwise the retained fence continues to reject delayed
    /// input and count against the global cap.
    pub fn start(
        &mut self,
        room: RoomId,
        participant: ParticipantId,
    ) -> Result<InputStreamIncarnation, InputRejection> {
        let key = (room, participant);
        if let Some(fence) = self.fences.get(&key) {
            return Ok(fence.incarnation);
        }
        if self.fences.len() >= self.config.max_retained_streams {
            return Err(InputRejection::RetainedStreamCapacity);
        }
        let incarnation = self.issue_incarnation()?;
        self.fences.insert(
            key,
            StreamFence {
                incarnation,
                last_released: 0,
            },
        );
        Ok(incarnation)
    }

    /// Reset one stream only when `incarnation` is its current server-issued fence.
    ///
    /// Buffered payloads are discarded only after a fresh incarnation is
    /// successfully issued. A delayed reset carrying an old incarnation is
    /// rejected fail-closed. Exhaustion also fails closed without changing the
    /// current fence, watermark, or buffer.
    pub fn reset(
        &mut self,
        room: RoomId,
        participant: ParticipantId,
        incarnation: InputStreamIncarnation,
    ) -> Result<InputStreamIncarnation, InputRejection> {
        let key = (room, participant);
        if self
            .fences
            .get(&key)
            .is_none_or(|fence| fence.incarnation != incarnation)
        {
            return Err(InputRejection::StaleIncarnation);
        }

        let replacement = self.issue_incarnation()?;
        self.queues.remove(&key);
        let fence = self
            .fences
            .get_mut(&key)
            .expect("current incarnation fence was checked before reset");
        fence.incarnation = replacement;
        fence.last_released = 0;
        Ok(replacement)
    }

    /// Retire one server-owned stream after its authoritative close/leave lifecycle.
    ///
    /// Retirement releases its payload queue, fence, watermark, and current
    /// incarnation together. It requires the current fence so a delayed close
    /// cannot retire a replacement stream. Input with a retired incarnation is
    /// rejected even if the same `(room, participant)` key is later started
    /// again, because incarnations never wrap.
    pub fn retire(
        &mut self,
        room: RoomId,
        participant: ParticipantId,
        incarnation: InputStreamIncarnation,
    ) -> Result<(), InputRejection> {
        let key = (room, participant);
        if self
            .fences
            .get(&key)
            .is_none_or(|fence| fence.incarnation != incarnation)
        {
            return Err(InputRejection::StaleIncarnation);
        }

        self.queues.remove(&key);
        self.fences.remove(&key);
        Ok(())
    }

    /// Offer one input for a server-resolved `(room, participant)` stream.
    pub fn offer(
        &mut self,
        room: RoomId,
        participant: ParticipantId,
        incarnation: InputStreamIncarnation,
        sequence: u64,
        payload: T,
    ) -> InputDisposition<T>
    where
        T: Eq,
    {
        if sequence == 0 {
            return InputDisposition::Rejected(InputRejection::SequenceZero);
        }

        let key = (room, participant);
        let Some(fence) = self.fences.get(&key) else {
            return InputDisposition::Rejected(InputRejection::StaleIncarnation);
        };
        if fence.incarnation != incarnation {
            return InputDisposition::Rejected(InputRejection::StaleIncarnation);
        }
        if sequence <= fence.last_released {
            return InputDisposition::Rejected(InputRejection::Stale);
        }
        if sequence - fence.last_released > self.config.max_ahead {
            return InputDisposition::Rejected(InputRejection::AheadWindow);
        }

        if let Some(queue) = self.queues.get(&key)
            && let Some(existing) = queue.pending.get(&sequence)
        {
            return InputDisposition::Rejected(if existing == &payload {
                InputRejection::ExactDuplicate
            } else {
                InputRejection::ConflictingDuplicate
            });
        }

        if fence.last_released.checked_add(1) == Some(sequence) {
            self.release_contiguous(key, sequence, payload)
        } else {
            let buffered_len = self.queues.get(&key).map_or(0, |queue| queue.pending.len());
            if buffered_len >= self.config.max_buffered {
                return InputDisposition::Rejected(InputRejection::BufferOverflow);
            }
            self.queues
                .entry(key)
                .or_default()
                .pending
                .insert(sequence, payload);
            InputDisposition::Buffered
        }
    }

    /// The current server-owned incarnation, if the stream has been started.
    #[must_use]
    pub fn incarnation(
        &self,
        room: RoomId,
        participant: ParticipantId,
    ) -> Option<InputStreamIncarnation> {
        self.fences
            .get(&(room, participant))
            .map(|fence| fence.incarnation)
    }

    /// Number of streams currently retaining out-of-order payloads.
    #[must_use]
    pub fn active_stream_count(&self) -> usize {
        self.queues.len()
    }

    /// Number of streams retaining any fence, watermark, or incarnation state.
    #[must_use]
    pub fn retained_stream_count(&self) -> usize {
        self.fences.len()
    }

    /// The highest contiguous sequence released for this server-owned stream.
    #[must_use]
    pub fn last_released(&self, room: RoomId, participant: ParticipantId) -> Option<u64> {
        self.fences
            .get(&(room, participant))
            .map(|fence| fence.last_released)
    }

    /// Number of out-of-order inputs buffered for this stream.
    #[must_use]
    pub fn buffered_len(&self, room: RoomId, participant: ParticipantId) -> usize {
        self.queues
            .get(&(room, participant))
            .map_or(0, |queue| queue.pending.len())
    }

    fn issue_incarnation(&mut self) -> Result<InputStreamIncarnation, InputRejection> {
        let next = self
            .next_incarnation
            .checked_add(1)
            .ok_or(InputRejection::IncarnationExhausted)?;
        self.next_incarnation = next;
        Ok(InputStreamIncarnation(next))
    }

    fn release_contiguous(
        &mut self,
        key: StreamKey,
        sequence: u64,
        payload: T,
    ) -> InputDisposition<T> {
        let mut released = vec![ReleasedInput { sequence, payload }];
        let fence = self
            .fences
            .get_mut(&key)
            .expect("current incarnation fence was checked before release");
        fence.last_released = sequence;

        if let Some(mut queue) = self.queues.remove(&key) {
            while let Some(next) = fence.last_released.checked_add(1)
                && let Some(payload) = queue.pending.remove(&next)
            {
                fence.last_released = next;
                released.push(ReleasedInput {
                    sequence: next,
                    payload,
                });
            }
            if !queue.pending.is_empty() {
                self.queues.insert(key, queue);
            }
        }

        InputDisposition::Released(released)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::realtime::{ParticipantId, RoomId};

    const DEFAULT_MAX_AHEAD: u64 = 8;
    const DEFAULT_MAX_BUFFERED: usize = 4;
    const DEFAULT_MAX_ACTIVE_STREAMS: usize = 4;

    fn queues() -> InputReconciliation<&'static str> {
        InputReconciliation::new(InputReconciliationConfig::new(
            DEFAULT_MAX_AHEAD,
            DEFAULT_MAX_BUFFERED,
            DEFAULT_MAX_ACTIVE_STREAMS,
        ))
    }

    fn stream(
        queues: &mut InputReconciliation<&'static str>,
        room: RoomId,
        participant: ParticipantId,
    ) -> InputStreamIncarnation {
        queues
            .start(room, participant)
            .expect("test stream starts within its retained-fence capacity")
    }

    #[test]
    fn reset_discards_one_streams_watermark_and_buffer() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let initial = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, initial, 3, "discarded"),
            InputDisposition::Buffered
        );
        let replacement = queues
            .reset(room, participant, initial)
            .expect("current stream resets");
        assert_ne!(replacement, initial);
        assert_eq!(queues.last_released(room, participant), Some(0));
        assert_eq!(queues.buffered_len(room, participant), 0);
        assert_eq!(queues.active_stream_count(), 0);
        assert_eq!(
            queues.offer(room, participant, replacement, 1, "fresh"),
            InputDisposition::Released(vec![ReleasedInput {
                sequence: 1,
                payload: "fresh",
            }])
        );
    }

    #[test]
    fn rejects_a_new_gap_when_the_out_of_order_buffer_is_full() {
        let mut queues = InputReconciliation::new(InputReconciliationConfig::new(8, 1, 4));
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let incarnation = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, incarnation, 3, "held"),
            InputDisposition::Buffered
        );
        assert_eq!(
            queues.offer(room, participant, incarnation, 4, "overflow"),
            InputDisposition::Rejected(InputRejection::BufferOverflow)
        );
        assert_eq!(queues.buffered_len(room, participant), 1);
    }

    #[test]
    fn rejects_an_input_beyond_the_ahead_window_without_creating_an_active_queue() {
        let mut queues = InputReconciliation::new(InputReconciliationConfig::new(2, 4, 4));
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let incarnation = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, incarnation, 3, "too-far"),
            InputDisposition::Rejected(InputRejection::AheadWindow)
        );
        assert_eq!(queues.last_released(room, participant), Some(0));
        assert_eq!(queues.buffered_len(room, participant), 0);
        assert_eq!(queues.active_stream_count(), 0);
    }

    #[test]
    fn rejects_a_sequence_at_or_below_the_released_watermark_as_stale() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let incarnation = stream(&mut queues, room, participant);

        assert!(matches!(
            queues.offer(room, participant, incarnation, 1, "first"),
            InputDisposition::Released(_)
        ));
        assert_eq!(
            queues.offer(room, participant, incarnation, 1, "first"),
            InputDisposition::Rejected(InputRejection::Stale)
        );
        assert_eq!(queues.last_released(room, participant), Some(1));
        assert_eq!(queues.buffered_len(room, participant), 0);
    }

    #[test]
    fn rejects_a_conflicting_duplicate_while_it_is_buffered() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let incarnation = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, incarnation, 3, "held"),
            InputDisposition::Buffered
        );
        assert_eq!(
            queues.offer(room, participant, incarnation, 3, "replacement"),
            InputDisposition::Rejected(InputRejection::ConflictingDuplicate)
        );
        assert_eq!(
            queues.offer(room, participant, incarnation, 1, "first"),
            InputDisposition::Released(vec![ReleasedInput {
                sequence: 1,
                payload: "first",
            }])
        );
        assert_eq!(
            queues.offer(room, participant, incarnation, 2, "second"),
            InputDisposition::Released(vec![
                ReleasedInput {
                    sequence: 2,
                    payload: "second",
                },
                ReleasedInput {
                    sequence: 3,
                    payload: "held",
                },
            ])
        );
    }

    #[test]
    fn rejects_an_exact_duplicate_while_it_is_buffered() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let incarnation = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, incarnation, 3, "held"),
            InputDisposition::Buffered
        );
        assert_eq!(
            queues.offer(room, participant, incarnation, 3, "held"),
            InputDisposition::Rejected(InputRejection::ExactDuplicate)
        );
        assert_eq!(queues.buffered_len(room, participant), 1);
    }

    #[test]
    fn rejects_sequence_zero_without_creating_an_active_queue() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let incarnation = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, incarnation, 0, "invalid"),
            InputDisposition::Rejected(InputRejection::SequenceZero)
        );
        assert_eq!(queues.last_released(room, participant), Some(0));
        assert_eq!(queues.buffered_len(room, participant), 0);
        assert_eq!(queues.active_stream_count(), 0);
    }

    #[test]
    fn buffers_a_gap_and_releases_the_contiguous_run_when_filled() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let incarnation = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, incarnation, 3, "third"),
            InputDisposition::Buffered
        );
        assert_eq!(queues.buffered_len(room, participant), 1);
        assert_eq!(
            queues.offer(room, participant, incarnation, 1, "first"),
            InputDisposition::Released(vec![ReleasedInput {
                sequence: 1,
                payload: "first",
            }])
        );
        assert_eq!(
            queues.offer(room, participant, incarnation, 2, "second"),
            InputDisposition::Released(vec![
                ReleasedInput {
                    sequence: 2,
                    payload: "second",
                },
                ReleasedInput {
                    sequence: 3,
                    payload: "third",
                },
            ])
        );
        assert_eq!(queues.last_released(room, participant), Some(3));
        assert_eq!(queues.buffered_len(room, participant), 0);
    }

    #[test]
    fn accepts_and_releases_contiguous_input() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let incarnation = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, incarnation, 1, "first"),
            InputDisposition::Released(vec![ReleasedInput {
                sequence: 1,
                payload: "first",
            }])
        );
        assert_eq!(queues.last_released(room, participant), Some(1));
        assert_eq!(queues.buffered_len(room, participant), 0);
    }

    #[test]
    fn retained_fence_capacity_rejects_churn_until_server_retires_the_stream() {
        let mut queues = InputReconciliation::new(InputReconciliationConfig::new(8, 4, 2));
        let room = 7_u64 as RoomId;
        let first = ParticipantId::from_raw(11);
        let second = ParticipantId::from_raw(12);
        let third = ParticipantId::from_raw(13);
        let first_incarnation = stream(&mut queues, room, first);
        let second_incarnation = stream(&mut queues, room, second);

        assert_eq!(
            queues.offer(room, first, first_incarnation, 2, "first held"),
            InputDisposition::Buffered
        );
        assert_eq!(
            queues.offer(room, second, second_incarnation, 2, "second held"),
            InputDisposition::Buffered
        );
        assert_eq!(queues.active_stream_count(), 2);
        for raw in 13_u64..=64 {
            let churned_participant = ParticipantId::from_raw(raw);
            assert_eq!(
                queues.start(room, churned_participant),
                Err(InputRejection::RetainedStreamCapacity)
            );
        }
        assert_eq!(queues.retained_stream_count(), 2);
        assert_eq!(queues.active_stream_count(), 2);

        assert_eq!(queues.retire(room, first, first_incarnation), Ok(()));
        assert_eq!(queues.retained_stream_count(), 1);
        assert_eq!(queues.active_stream_count(), 1);
        assert_eq!(
            queues.offer(room, first, first_incarnation, 1, "delayed retired input"),
            InputDisposition::Rejected(InputRejection::StaleIncarnation)
        );

        let third_incarnation = stream(&mut queues, room, third);
        assert_eq!(
            queues.offer(room, third, third_incarnation, 2, "third held"),
            InputDisposition::Buffered
        );
        assert_eq!(queues.active_stream_count(), 2);
    }

    #[test]
    fn retirement_rejects_delayed_input_after_the_same_key_is_started_again() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let retired = stream(&mut queues, room, participant);

        assert_eq!(queues.retire(room, participant, retired), Ok(()));
        let replacement = stream(&mut queues, room, participant);
        assert_ne!(retired, replacement);
        assert_eq!(
            queues.offer(room, participant, retired, 1, "delayed retired input"),
            InputDisposition::Rejected(InputRejection::StaleIncarnation)
        );
        assert_eq!(
            queues.offer(room, participant, replacement, 1, "replacement input"),
            InputDisposition::Released(vec![ReleasedInput {
                sequence: 1,
                payload: "replacement input",
            }])
        );
    }

    #[test]
    fn incarnation_exhaustion_fails_closed_without_discarding_a_reset_stream() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let current = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, current, 2, "held"),
            InputDisposition::Buffered
        );
        queues.next_incarnation = u64::MAX;

        assert_eq!(
            queues.reset(room, participant, current),
            Err(InputRejection::IncarnationExhausted)
        );
        assert_eq!(queues.incarnation(room, participant), Some(current));
        assert_eq!(queues.last_released(room, participant), Some(0));
        assert_eq!(queues.buffered_len(room, participant), 1);
        assert_eq!(queues.active_stream_count(), 1);
        assert_eq!(queues.retained_stream_count(), 1);
        assert_eq!(
            queues.offer(room, participant, current, 1, "first"),
            InputDisposition::Released(vec![
                ReleasedInput {
                    sequence: 1,
                    payload: "first",
                },
                ReleasedInput {
                    sequence: 2,
                    payload: "held",
                },
            ])
        );

        let another_participant = ParticipantId::from_raw(12);
        assert_eq!(
            queues.start(room, another_participant),
            Err(InputRejection::IncarnationExhausted)
        );
        assert_eq!(queues.incarnation(room, another_participant), None);
        assert_eq!(queues.retained_stream_count(), 1);
    }

    #[test]
    fn contiguous_release_removes_the_empty_queue_and_keeps_the_watermark() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let incarnation = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, incarnation, 3, "third"),
            InputDisposition::Buffered
        );
        assert_eq!(queues.active_stream_count(), 1);
        assert!(matches!(
            queues.offer(room, participant, incarnation, 1, "first"),
            InputDisposition::Released(_)
        ));
        assert!(matches!(
            queues.offer(room, participant, incarnation, 2, "second"),
            InputDisposition::Released(_)
        ));

        assert_eq!(queues.active_stream_count(), 0);
        assert_eq!(queues.buffered_len(room, participant), 0);
        assert_eq!(queues.last_released(room, participant), Some(3));
    }

    #[test]
    fn delayed_old_incarnation_input_and_reset_are_rejected_after_reset() {
        let mut queues = queues();
        let room = 7_u64 as RoomId;
        let participant = ParticipantId::from_raw(11);
        let old = stream(&mut queues, room, participant);

        assert_eq!(
            queues.offer(room, participant, old, 2, "discarded"),
            InputDisposition::Buffered
        );
        let replacement = queues
            .reset(room, participant, old)
            .expect("current stream resets");
        assert_ne!(old, replacement);
        assert_eq!(queues.active_stream_count(), 0);

        assert_eq!(
            queues.offer(room, participant, old, 1, "delayed old input"),
            InputDisposition::Rejected(InputRejection::StaleIncarnation)
        );
        assert_eq!(
            queues.reset(room, participant, old),
            Err(InputRejection::StaleIncarnation)
        );
        assert_eq!(
            queues.offer(room, participant, replacement, 1, "replacement input"),
            InputDisposition::Released(vec![ReleasedInput {
                sequence: 1,
                payload: "replacement input",
            }])
        );
    }
}
