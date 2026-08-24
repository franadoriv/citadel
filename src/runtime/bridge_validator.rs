//! Batch validator for the authoritative gameplay bridge.
//!
//! Rust is the sole authority on whether a [`ScriptCommandBatch`] materializes.
//! This module implements the §3.5 enforcement matrix from
//! `INV-20260805-GAMESCRIPT-AUTHORITATIVE-COMMAND-AND-REPLICATION`: fencing,
//! idempotency, correlation completeness, scope, ownership/bounds, quotas,
//! capability, and structural decode — every failure fail-closed and
//! **batch-atomic** (owner decision 2). A single invalid, out-of-scope,
//! over-quota, or unauthorized command rejects the *entire* batch; nothing in
//! it materializes, ever. There is no partial application.
//!
//! The [`PendingBatchLedger`] is the per-match record the gateway keeps: the
//! issued event ids, the `batch_id` watermark, and the (generation, clock
//! epoch) binding the events were issued under. A reload advances the
//! generation and clears every pending batch, so a stale-generation answer can
//! never resurrect a superseded turn.
//!
//! The validator is deliberately transport- and gateway-agnostic: match
//! membership, object scope, replicated bounds, and capability are supplied
//! through the [`BridgeMatchContext`] trait so the logic is unit-testable in
//! isolation and wired to the real `RoomRegistry`/`RepAuthority` at the gateway.

use std::collections::HashMap;

use citadel_wire::protocol::{KIND_AUTHORITATIVE_INPUT, KIND_DIAG_STATUS};

use super::bridge_protocol::{
    BridgeRepField, BridgeRepValue, Correction, Decision, GS_BRIDGE_PROTOCOL_VERSION, InputOutcome,
    NormalizedEvent, NormalizedEventBatch, NormalizedPayload, ScriptCommand, ScriptCommandBatch,
};

/// The highest wire kind currently reserved for infrastructure/typed frames. A
/// script may not emit a raw message on any kind `<=` this: reserved kinds are
/// reachable only through typed commands (§3.5). Sourced from the protocol
/// registry so a newly reserved kind extends the guard automatically.
pub const MAX_RESERVED_KIND: u16 = KIND_AUTHORITATIVE_INPUT;

/// A capability a match's revision manifest must declare before the matching
/// [`ScriptCommand`] family is permitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    /// Storage writes via [`ScriptCommand::Persist`].
    Persist,
    /// Deferred re-entry via [`ScriptCommand::Schedule`].
    Schedule,
    /// Kinematic body control ([`ScriptCommand::SetPhysics`] /
    /// [`ApplyImpulse`](ScriptCommand::ApplyImpulse) /
    /// [`SetMoveIntent`](ScriptCommand::SetMoveIntent)).
    Physics,
}

/// Per-batch quotas. Every value is config-sourced at the gateway; the defaults
/// here are PROVISIONAL precedents (mirroring the existing per-invocation caps
/// in `runtime::append_runtime_event_commands` and `lua.rs`), to be replaced by
/// bench-harness p99-plus-headroom measurements (Fase 5). No value is a
/// silently-invented budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BridgeQuotas {
    /// Max [`ScriptCommand`]s per batch. Precedent: `MAX_OUTBOUND_COMMANDS`.
    pub max_commands: usize,
    /// Max aggregate command body bytes per batch. Precedent: 1 MiB aggregate.
    pub max_command_body_bytes: usize,
    /// Max bytes for one [`InputOutcome::reply`]. Precedent: `MAX_OUTBOUND_BODY_BYTES`.
    pub max_reply_bytes: usize,
    /// Max recipients in one [`ScriptCommand::SendToMany`].
    pub max_recipients: usize,
    /// Max [`ScriptCommand::Persist`] ops per batch.
    pub max_persist_ops: usize,
    /// Max [`ScriptCommand::Schedule`] ops per batch.
    pub max_schedule_ops: usize,
}

impl Default for BridgeQuotas {
    fn default() -> Self {
        // PROVISIONAL — measure first (bench harness). These reuse the existing
        // command-sink precedents so review sees no invented numbers.
        Self {
            max_commands: 1_024,
            max_command_body_bytes: 1 << 20,
            max_reply_bytes: 64 << 10,
            max_recipients: 1_024,
            max_persist_ops: 64,
            max_schedule_ops: 64,
        }
    }
}

/// The match-scoped facts the validator queries. Supplied by the gateway
/// (real `RoomRegistry`/`RepAuthority`) or a test double.
pub trait BridgeMatchContext {
    /// Whether `participant` is a current member of this match.
    fn is_member(&self, participant: u64) -> bool;
    /// Whether `object_id` belongs to this match's world.
    fn object_in_match(&self, object_id: u32) -> bool;
    /// Whether `value` is within the replicated field's declared `FieldBounds`.
    /// Script values are exact — out-of-bounds is rejected, never clamped.
    fn rep_value_in_bounds(&self, object_id: u32, field_id: u16, value: &BridgeRepValue) -> bool;
    /// Whether the match's revision declares `capability`.
    fn has_capability(&self, capability: Capability) -> bool;
}

/// Why a batch was rejected. Every variant is a whole-batch reject (RB): no
/// command in the batch materialized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchRejection {
    /// `protocol_version` is not [`GS_BRIDGE_PROTOCOL_VERSION`].
    UnsupportedProtocol {
        /// The version the answer declared.
        got: u16,
    },
    /// `generation` is not the match's active generation (stale activation).
    StaleGeneration {
        /// The generation the answer declared.
        got: u64,
        /// The match's active generation.
        active: u64,
    },
    /// `match_id` is not this ledger's match.
    CrossMatch {
        /// The match id the answer declared.
        got: u64,
        /// This ledger's match id.
        expected: u64,
    },
    /// `clock_epoch` is not the match's current gameplay-clock epoch.
    CrossEpoch {
        /// The epoch the answer declared.
        got: u64,
        /// The match's current epoch.
        expected: u64,
    },
    /// `batch_id` is not a pending batch (never issued, already answered, or
    /// replayed).
    UnknownOrDuplicateBatch {
        /// The batch id the answer declared.
        got: u64,
    },
    /// `tick` does not match the answered batch's issue tick (staleness).
    StaleTick {
        /// The tick the answer declared.
        got: u64,
        /// The tick the batch was issued at.
        expected: u64,
    },
    /// An issued event received no outcome.
    MissingOutcome {
        /// The unanswered event.
        event_id: u64,
    },
    /// An event received more than one outcome.
    DuplicateOutcome {
        /// The doubly-answered event.
        event_id: u64,
    },
    /// An outcome answers an event this batch never issued.
    ForeignEventId {
        /// The foreign event id.
        event_id: u64,
    },
    /// A `Correct` decision does not match its event's payload type.
    IncompatibleCorrection {
        /// The event that was mis-corrected.
        event_id: u64,
    },
    /// A message command targets a reserved wire kind.
    ReservedKind {
        /// The offending kind.
        kind: u16,
    },
    /// A message recipient is not a current match member.
    RecipientNotMember {
        /// The offending participant.
        participant: u64,
    },
    /// A state-mutation command targets an object outside this match.
    ObjectNotInMatch {
        /// The offending object.
        object_id: u32,
    },
    /// A replicated value is outside its field's declared bounds.
    ReplicatedValueOutOfBounds {
        /// The object.
        object_id: u32,
        /// The field.
        field_id: u16,
    },
    /// The revision does not declare a capability the batch requires.
    MissingCapability {
        /// The undeclared capability.
        capability: Capability,
    },
    /// The batch exceeds a per-batch quota.
    QuotaExceeded {
        /// Which quota.
        quota: Quota,
    },
}

/// Which quota a [`BatchRejection::QuotaExceeded`] tripped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quota {
    /// [`BridgeQuotas::max_commands`].
    CommandCount,
    /// [`BridgeQuotas::max_command_body_bytes`].
    CommandBodyBytes,
    /// [`BridgeQuotas::max_reply_bytes`].
    ReplyBytes,
    /// [`BridgeQuotas::max_recipients`].
    Recipients,
    /// [`BridgeQuotas::max_persist_ops`].
    PersistOps,
    /// [`BridgeQuotas::max_schedule_ops`].
    ScheduleOps,
}

/// One event paired with the script's validated decision for it. The executor
/// materializes `Accept`/`Correct` against `event`'s canonical effect.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedOutcome {
    /// The originally issued event.
    pub event: NormalizedEvent,
    /// The script's decision.
    pub decision: Decision,
    /// The bounded reply to unicast to the event's sender, if any.
    pub reply: Option<Vec<u8>>,
}

/// A fully validated, trusted batch ready for materialization. Produced only
/// when every §3.5 check passed; the executor applies it wholesale.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidatedBatch {
    /// The match this batch belongs to.
    pub match_id: u64,
    /// The tick answered.
    pub tick: u64,
    /// The batch id answered.
    pub batch_id: u64,
    /// Per-event validated outcomes.
    pub outcomes: Vec<ValidatedOutcome>,
    /// The validated script-originated commands.
    pub commands: Vec<ScriptCommand>,
}

/// A batch issued to the script and awaiting its answer.
#[derive(Debug, Clone)]
struct PendingBatch {
    tick: u64,
    events: Vec<NormalizedEvent>,
    reserved_kind_mode: ReservedKindMode,
}

/// Reserved-kind policy captured when the server issues a bridge batch.
///
/// Legacy custom kinds 40 and 41 remain available unless the originating
/// session opted into the post-auth authoritative-input capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReservedKindMode {
    Legacy,
    AuthoritativeInput,
}

/// A draft event the gateway hands the ledger; the ledger assigns the
/// monotonic `event_id`.
#[derive(Debug, Clone, PartialEq)]
pub struct EventDraft {
    /// Originating participant (raw session id).
    pub participant: u64,
    /// Resolved account id, if authenticated.
    pub user_id: Option<String>,
    /// The typed intent.
    pub payload: NormalizedPayload,
}

impl EventDraft {
    /// A draft with no resolved account (guest).
    #[must_use]
    pub fn guest(participant: u64, payload: NormalizedPayload) -> Self {
        Self {
            participant,
            user_id: None,
            payload,
        }
    }
}

/// The per-match pending-batch ledger: issued event ids, the `batch_id`
/// watermark, and the (generation, clock epoch) binding. The gateway owns one
/// per authoritative match.
#[derive(Debug)]
pub struct PendingBatchLedger {
    match_id: u64,
    generation: u64,
    clock_epoch: u64,
    next_event_id: u64,
    next_batch_id: u64,
    pending: HashMap<u64, PendingBatch>,
}

impl PendingBatchLedger {
    /// A fresh ledger bound to `match_id`, `generation`, and `clock_epoch`.
    #[must_use]
    pub fn new(match_id: u64, generation: u64, clock_epoch: u64) -> Self {
        Self {
            match_id,
            generation,
            clock_epoch,
            next_event_id: 1,
            next_batch_id: 1,
            pending: HashMap::new(),
        }
    }

    /// This ledger's match id.
    #[must_use]
    pub fn match_id(&self) -> u64 {
        self.match_id
    }

    /// The active activation generation.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The current gameplay-clock epoch.
    #[must_use]
    pub fn clock_epoch(&self) -> u64 {
        self.clock_epoch
    }

    /// Number of batches awaiting an answer.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Whether this exact batch id still awaits an answer. This exposes only a
    /// server-generated correlation state so callers can clean up companion
    /// pending records after validation consumed an invalid batch.
    #[must_use]
    pub fn has_pending_batch(&self, batch_id: u64) -> bool {
        self.pending.contains_key(&batch_id)
    }

    /// Advance to a new activation generation (a reload). Every pending batch is
    /// dropped, so a stale-generation answer can never materialize.
    pub fn advance_generation(&mut self, generation: u64) {
        self.generation = generation;
        self.pending.clear();
    }

    /// Set a new gameplay-clock epoch (a clock reset). Pending batches keyed to
    /// the old epoch can no longer be answered (their `clock_epoch` no longer
    /// matches), so they are dropped.
    pub fn set_clock_epoch(&mut self, clock_epoch: u64) {
        self.clock_epoch = clock_epoch;
        self.pending.clear();
    }

    /// Issue a batch: assign each draft a monotonic `event_id`, assign the
    /// `batch_id`, record the pending batch, and return the fenced
    /// [`NormalizedEventBatch`] to deliver. `tick` is the gameplay-clock tick.
    pub fn issue(&mut self, drafts: Vec<EventDraft>, tick: u64) -> NormalizedEventBatch {
        self.issue_with_reserved_kind_mode(drafts, tick, ReservedKindMode::Legacy)
    }

    /// Issue a batch under the exact reserved-kind policy the server selected
    /// for its originating session. This policy is stored with the pending batch
    /// so an asynchronous script response cannot be reinterpreted after a
    /// session negotiates or disconnects.
    pub fn issue_with_reserved_kind_mode(
        &mut self,
        drafts: Vec<EventDraft>,
        tick: u64,
        reserved_kind_mode: ReservedKindMode,
    ) -> NormalizedEventBatch {
        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;
        let events: Vec<NormalizedEvent> = drafts
            .into_iter()
            .map(|draft| {
                let event_id = self.next_event_id;
                self.next_event_id += 1;
                NormalizedEvent {
                    event_id,
                    participant: draft.participant,
                    user_id: draft.user_id,
                    payload: draft.payload,
                }
            })
            .collect();
        self.pending.insert(
            batch_id,
            PendingBatch {
                tick,
                events: events.clone(),
                reserved_kind_mode,
            },
        );
        NormalizedEventBatch {
            protocol_version: GS_BRIDGE_PROTOCOL_VERSION,
            generation: self.generation,
            match_id: self.match_id,
            clock_epoch: self.clock_epoch,
            tick,
            batch_id,
            events,
        }
    }

    /// Validate `answer` against this ledger and `context`, batch-atomically.
    ///
    /// Fencing failures (protocol/generation/match/epoch) and an unknown
    /// `batch_id` leave the ledger untouched. Once a real pending batch is
    /// matched it is consumed **exactly once** — whether validation succeeds or
    /// a content check fails — so a duplicate delivery, and a retry of a
    /// malformed answer, are both rejected as unknown/duplicate. On success the
    /// [`ValidatedBatch`] is the executor's materialization plan.
    pub fn validate(
        &mut self,
        context: &dyn BridgeMatchContext,
        quotas: &BridgeQuotas,
        answer: &ScriptCommandBatch,
    ) -> Result<ValidatedBatch, BatchRejection> {
        // ---- fencing (ledger untouched on failure) ----
        if answer.protocol_version != GS_BRIDGE_PROTOCOL_VERSION {
            return Err(BatchRejection::UnsupportedProtocol {
                got: answer.protocol_version,
            });
        }
        if answer.generation != self.generation {
            return Err(BatchRejection::StaleGeneration {
                got: answer.generation,
                active: self.generation,
            });
        }
        if answer.match_id != self.match_id {
            return Err(BatchRejection::CrossMatch {
                got: answer.match_id,
                expected: self.match_id,
            });
        }
        if answer.clock_epoch != self.clock_epoch {
            return Err(BatchRejection::CrossEpoch {
                got: answer.clock_epoch,
                expected: self.clock_epoch,
            });
        }

        // ---- idempotency: consume the pending batch exactly once ----
        let Some(pending) = self.pending.remove(&answer.batch_id) else {
            return Err(BatchRejection::UnknownOrDuplicateBatch {
                got: answer.batch_id,
            });
        };
        // From here the batch is consumed; any failure is still a whole-batch
        // reject, and the same batch_id can never be answered again.

        if answer.tick != pending.tick {
            return Err(BatchRejection::StaleTick {
                got: answer.tick,
                expected: pending.tick,
            });
        }

        validate_content(
            &pending,
            context,
            quotas,
            answer,
            pending.reserved_kind_mode,
        )
    }
}

/// The correlation + per-command validation over an already-matched pending
/// batch. Split out so the ledger method stays focused on fencing/idempotency.
fn validate_content(
    pending: &PendingBatch,
    context: &dyn BridgeMatchContext,
    quotas: &BridgeQuotas,
    answer: &ScriptCommandBatch,
    reserved_kind_mode: ReservedKindMode,
) -> Result<ValidatedBatch, BatchRejection> {
    // ---- correlation completeness: exactly one outcome per issued event ----
    let mut event_by_id: HashMap<u64, &NormalizedEvent> = HashMap::new();
    for event in &pending.events {
        event_by_id.insert(event.event_id, event);
    }
    let mut answered: HashMap<u64, &InputOutcome> = HashMap::new();
    for outcome in &answer.input_outcomes {
        if !event_by_id.contains_key(&outcome.event_id) {
            return Err(BatchRejection::ForeignEventId {
                event_id: outcome.event_id,
            });
        }
        if answered.insert(outcome.event_id, outcome).is_some() {
            return Err(BatchRejection::DuplicateOutcome {
                event_id: outcome.event_id,
            });
        }
    }
    for event in &pending.events {
        if !answered.contains_key(&event.event_id) {
            return Err(BatchRejection::MissingOutcome {
                event_id: event.event_id,
            });
        }
    }

    // ---- per-outcome validation (correction type + reply bounds + bounds) ----
    let mut outcomes = Vec::with_capacity(pending.events.len());
    for event in &pending.events {
        let outcome = answered[&event.event_id];
        if let Some(reply) = &outcome.reply
            && reply.len() > quotas.max_reply_bytes
        {
            return Err(BatchRejection::QuotaExceeded {
                quota: Quota::ReplyBytes,
            });
        }
        if let Decision::Correct { correction } = &outcome.decision {
            validate_correction(event, correction, context)?;
        }
        outcomes.push(ValidatedOutcome {
            event: event.clone(),
            decision: outcome.decision.clone(),
            reply: outcome.reply.clone(),
        });
    }

    // ---- per-command validation (scope, bounds, capability, quotas) ----
    if answer.commands.len() > quotas.max_commands {
        return Err(BatchRejection::QuotaExceeded {
            quota: Quota::CommandCount,
        });
    }
    let mut body_bytes: usize = 0;
    let mut persist_ops: usize = 0;
    let mut schedule_ops: usize = 0;
    for command in &answer.commands {
        validate_command(command, context, reserved_kind_mode)?;
        body_bytes = body_bytes.saturating_add(command_body_bytes(command));
        if body_bytes > quotas.max_command_body_bytes {
            return Err(BatchRejection::QuotaExceeded {
                quota: Quota::CommandBodyBytes,
            });
        }
        match command {
            ScriptCommand::SendToMany { participants, .. } => {
                if participants.len() > quotas.max_recipients {
                    return Err(BatchRejection::QuotaExceeded {
                        quota: Quota::Recipients,
                    });
                }
            }
            ScriptCommand::Persist { .. } => {
                persist_ops += 1;
                if persist_ops > quotas.max_persist_ops {
                    return Err(BatchRejection::QuotaExceeded {
                        quota: Quota::PersistOps,
                    });
                }
            }
            ScriptCommand::Schedule { .. } => {
                schedule_ops += 1;
                if schedule_ops > quotas.max_schedule_ops {
                    return Err(BatchRejection::QuotaExceeded {
                        quota: Quota::ScheduleOps,
                    });
                }
            }
            _ => {}
        }
    }

    Ok(ValidatedBatch {
        match_id: answer.match_id,
        tick: answer.tick,
        batch_id: answer.batch_id,
        outcomes,
        commands: answer.commands.clone(),
    })
}

/// A `Correct` decision must substitute a value of the event's own kind and
/// stay in scope/bounds.
fn validate_correction(
    event: &NormalizedEvent,
    correction: &Correction,
    context: &dyn BridgeMatchContext,
) -> Result<(), BatchRejection> {
    let compatible = matches!(
        (&event.payload, correction),
        (
            NormalizedPayload::TransformInput { .. },
            Correction::Transform(_)
        ) | (
            NormalizedPayload::ActorStateReport { .. },
            Correction::Transform(_)
        ) | (
            NormalizedPayload::ReplicatedVarWrite { .. },
            Correction::ReplicatedVars { .. }
        ) | (
            NormalizedPayload::SpawnRequest { .. },
            Correction::Spawn { .. }
        )
    );
    if !compatible {
        return Err(BatchRejection::IncompatibleCorrection {
            event_id: event.event_id,
        });
    }
    let object_id = match &event.payload {
        NormalizedPayload::TransformInput { object_id, .. }
        | NormalizedPayload::ActorStateReport { object_id, .. }
        | NormalizedPayload::ReplicatedVarWrite { object_id, .. } => Some(*object_id),
        NormalizedPayload::SpawnRequest { .. }
        | NormalizedPayload::MatchMessage { .. }
        | NormalizedPayload::ParticipantJoined
        | NormalizedPayload::ParticipantLeft => None,
    };
    if let Some(object_id) = object_id
        && !context.object_in_match(object_id)
    {
        return Err(BatchRejection::ObjectNotInMatch { object_id });
    }
    // A replicated correction must stay within the field bounds of the event's
    // own object (script values are exact).
    if let (
        NormalizedPayload::ReplicatedVarWrite { object_id, .. },
        Correction::ReplicatedVars { fields },
    ) = (&event.payload, correction)
    {
        check_rep_fields_in_bounds(*object_id, fields, context)?;
    }
    Ok(())
}

/// Scope/bounds/capability/reserved-kind checks for one script-originated
/// command.
fn validate_command(
    command: &ScriptCommand,
    context: &dyn BridgeMatchContext,
    reserved_kind_mode: ReservedKindMode,
) -> Result<(), BatchRejection> {
    match command {
        // State mutations on an existing object must target this match's world.
        ScriptCommand::ApplyTransform { object_id, .. }
        | ScriptCommand::DespawnActor { object_id }
        | ScriptCommand::SetPhysics { object_id, .. }
        | ScriptCommand::ApplyImpulse { object_id, .. }
        | ScriptCommand::SetMoveIntent { object_id, .. } => {
            if !context.object_in_match(*object_id) {
                return Err(BatchRejection::ObjectNotInMatch {
                    object_id: *object_id,
                });
            }
            if let ScriptCommand::SetPhysics { .. }
            | ScriptCommand::ApplyImpulse { .. }
            | ScriptCommand::SetMoveIntent { .. } = command
            {
                require_capability(context, Capability::Physics)?;
            }
        }
        ScriptCommand::SetReplicatedVars { object_id, fields } => {
            if !context.object_in_match(*object_id) {
                return Err(BatchRejection::ObjectNotInMatch {
                    object_id: *object_id,
                });
            }
            check_rep_fields_in_bounds(*object_id, fields, context)?;
        }
        // A spawn creates a new object; it does not pre-exist in the match.
        ScriptCommand::SpawnActor { .. } => {}
        // Messaging: reserved-kind guard + every recipient must be a member.
        ScriptCommand::SendTo {
            participant, kind, ..
        } => {
            reject_reserved_kind(*kind, reserved_kind_mode)?;
            if !context.is_member(*participant) {
                return Err(BatchRejection::RecipientNotMember {
                    participant: *participant,
                });
            }
        }
        ScriptCommand::SendToMany {
            participants, kind, ..
        } => {
            reject_reserved_kind(*kind, reserved_kind_mode)?;
            for participant in participants {
                if !context.is_member(*participant) {
                    return Err(BatchRejection::RecipientNotMember {
                        participant: *participant,
                    });
                }
            }
        }
        ScriptCommand::BroadcastMatch { kind, exclude, .. } => {
            reject_reserved_kind(*kind, reserved_kind_mode)?;
            if let Some(participant) = exclude
                && !context.is_member(*participant)
            {
                return Err(BatchRejection::RecipientNotMember {
                    participant: *participant,
                });
            }
        }
        // Bounded host effects require a declared capability.
        ScriptCommand::Persist { .. } => require_capability(context, Capability::Persist)?,
        ScriptCommand::Schedule { .. } => require_capability(context, Capability::Schedule)?,
    }
    Ok(())
}

fn check_rep_fields_in_bounds(
    object_id: u32,
    fields: &[BridgeRepField],
    context: &dyn BridgeMatchContext,
) -> Result<(), BatchRejection> {
    for field in fields {
        if !context.rep_value_in_bounds(object_id, field.field_id, &field.value) {
            return Err(BatchRejection::ReplicatedValueOutOfBounds {
                object_id,
                field_id: field.field_id,
            });
        }
    }
    Ok(())
}

fn require_capability(
    context: &dyn BridgeMatchContext,
    capability: Capability,
) -> Result<(), BatchRejection> {
    if context.has_capability(capability) {
        Ok(())
    } else {
        Err(BatchRejection::MissingCapability { capability })
    }
}

fn reject_reserved_kind(
    kind: u16,
    reserved_kind_mode: ReservedKindMode,
) -> Result<(), BatchRejection> {
    let reserved = kind <= KIND_DIAG_STATUS
        || (reserved_kind_mode == ReservedKindMode::AuthoritativeInput
            && kind <= MAX_RESERVED_KIND);
    if reserved {
        Err(BatchRejection::ReservedKind { kind })
    } else {
        Ok(())
    }
}

/// Aggregate body-byte contribution of one command (opaque payload bytes only).
fn command_body_bytes(command: &ScriptCommand) -> usize {
    match command {
        ScriptCommand::SendTo { body, .. }
        | ScriptCommand::SendToMany { body, .. }
        | ScriptCommand::BroadcastMatch { body, .. }
        | ScriptCommand::Schedule { payload: body, .. } => body.len(),
        ScriptCommand::Persist { op } => op.value.len(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::bridge_protocol::BridgeTransform;

    /// A permissive test context: everyone is a member, every object is in the
    /// match, all values in bounds, all capabilities declared — individual
    /// tests override one dimension to prove the corresponding reject.
    struct TestContext {
        members: Option<Vec<u64>>,
        objects: Option<Vec<u32>>,
        out_of_bounds: Vec<(u32, u16)>,
        capabilities: Vec<Capability>,
    }

    impl TestContext {
        fn permissive() -> Self {
            Self {
                members: None,
                objects: None,
                out_of_bounds: Vec::new(),
                capabilities: vec![
                    Capability::Persist,
                    Capability::Schedule,
                    Capability::Physics,
                ],
            }
        }
    }

    impl BridgeMatchContext for TestContext {
        fn is_member(&self, participant: u64) -> bool {
            match &self.members {
                Some(m) => m.contains(&participant),
                None => true,
            }
        }
        fn object_in_match(&self, object_id: u32) -> bool {
            match &self.objects {
                Some(o) => o.contains(&object_id),
                None => true,
            }
        }
        fn rep_value_in_bounds(&self, object_id: u32, field_id: u16, _v: &BridgeRepValue) -> bool {
            !self.out_of_bounds.contains(&(object_id, field_id))
        }
        fn has_capability(&self, capability: Capability) -> bool {
            self.capabilities.contains(&capability)
        }
    }

    fn transform() -> BridgeTransform {
        BridgeTransform::identity()
    }

    fn input_draft(participant: u64, object_id: u32) -> EventDraft {
        EventDraft::guest(
            participant,
            NormalizedPayload::TransformInput {
                object_id,
                ownership_epoch: 1,
                input_seq: 1,
                sim_tick: 1,
                dt: 0.016,
                move_velocity: [1.0, 0.0, 0.0],
                payload: Vec::new(),
                fire: None,
            },
        )
    }

    /// Answer every issued event with Accept (the common happy-path prefix).
    fn accept_all(batch: &NormalizedEventBatch) -> ScriptCommandBatch {
        let mut answer = ScriptCommandBatch::answering(batch);
        for event in &batch.events {
            answer.input_outcomes.push(InputOutcome {
                event_id: event.event_id,
                decision: Decision::Accept,
                reply: None,
            });
        }
        answer
    }

    #[test]
    fn accepts_a_well_formed_batch() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let answer = accept_all(&batch);
        let validated = ledger
            .validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer,
            )
            .expect("well-formed batch accepted");
        assert_eq!(validated.outcomes.len(), 1);
        assert_eq!(validated.batch_id, batch.batch_id);
        assert_eq!(ledger.pending_len(), 0, "batch consumed on success");
    }

    // ---- B16 unknown protocol version ----
    #[test]
    fn unknown_protocol_version_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.protocol_version = GS_BRIDGE_PROTOCOL_VERSION + 1;
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::UnsupportedProtocol {
                got: GS_BRIDGE_PROTOCOL_VERSION + 1
            })
        );
        assert_eq!(
            ledger.pending_len(),
            1,
            "fencing failure leaves batch pending"
        );
    }

    // ---- B10 stale generation ----
    #[test]
    fn stale_generation_batch_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let answer = accept_all(&batch);
        // A reload activates generation 6 and clears the pending batch.
        ledger.advance_generation(6);
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::StaleGeneration { got: 5, active: 6 })
        );
    }

    // ---- B11 cross-match ----
    #[test]
    fn cross_match_batch_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.match_id = 2;
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::CrossMatch {
                got: 2,
                expected: 1
            })
        );
        assert_eq!(ledger.pending_len(), 1);
    }

    // ---- B12 cross-epoch ----
    #[test]
    fn cross_epoch_batch_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let answer = accept_all(&batch);
        // A clock reset bumps the epoch between issue and answer.
        ledger.set_clock_epoch(10);
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::CrossEpoch {
                got: 9,
                expected: 10
            })
        );
    }

    // ---- B13 duplicate batch_id, exactly once ----
    #[test]
    fn duplicate_batch_id_rejected_exactly_once() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let answer = accept_all(&batch);
        assert!(
            ledger
                .validate(
                    &TestContext::permissive(),
                    &BridgeQuotas::default(),
                    &answer
                )
                .is_ok()
        );
        // Second delivery of the same batch is rejected; effects never double.
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::UnknownOrDuplicateBatch {
                got: batch.batch_id
            })
        );
    }

    // ---- B14 missing outcome ----
    #[test]
    fn missing_input_outcome_rejects_whole_batch() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7), input_draft(1002, 8)], 100);
        let mut answer = ScriptCommandBatch::answering(&batch);
        // Answer only the first event, and add an otherwise-valid command.
        answer.input_outcomes.push(InputOutcome {
            event_id: batch.events[0].event_id,
            decision: Decision::Accept,
            reply: None,
        });
        answer.commands.push(ScriptCommand::BroadcastMatch {
            kind: 100,
            body: vec![1],
            unreliable: false,
            exclude: None,
        });
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::MissingOutcome {
                event_id: batch.events[1].event_id
            })
        );
    }

    // ---- B15 foreign event id ----
    #[test]
    fn foreign_event_id_rejects_whole_batch() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.input_outcomes.push(InputOutcome {
            event_id: 99_999,
            decision: Decision::Accept,
            reply: None,
        });
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::ForeignEventId { event_id: 99_999 })
        );
    }

    #[test]
    fn duplicate_outcome_rejects_whole_batch() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.input_outcomes.push(InputOutcome {
            event_id: batch.events[0].event_id,
            decision: Decision::Accept,
            reply: None,
        });
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::DuplicateOutcome {
                event_id: batch.events[0].event_id
            })
        );
    }

    // ---- B19 send to foreign participant ----
    #[test]
    fn send_to_foreign_participant_rejects_batch() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.commands.push(ScriptCommand::SendTo {
            participant: 4242,
            kind: 100,
            body: vec![1],
            unreliable: false,
        });
        let ctx = TestContext {
            members: Some(vec![1001]),
            ..TestContext::permissive()
        };
        assert_eq!(
            ledger.validate(&ctx, &BridgeQuotas::default(), &answer),
            Err(BatchRejection::RecipientNotMember { participant: 4242 })
        );
    }

    // ---- B20 one foreign recipient rejects the whole SendToMany ----
    #[test]
    fn recipient_list_with_one_foreign_member_rejects_whole_batch() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.commands.push(ScriptCommand::SendToMany {
            participants: vec![1001, 1002, 4242],
            kind: 100,
            body: vec![1],
            unreliable: false,
        });
        let ctx = TestContext {
            members: Some(vec![1001, 1002]),
            ..TestContext::permissive()
        };
        assert_eq!(
            ledger.validate(&ctx, &BridgeQuotas::default(), &answer),
            Err(BatchRejection::RecipientNotMember { participant: 4242 })
        );
    }

    #[test]
    fn every_script_message_direction_rejects_reserved_authoritative_input_kind() {
        let commands = [
            ScriptCommand::SendTo {
                participant: 1001,
                kind: KIND_AUTHORITATIVE_INPUT,
                body: vec![1],
                unreliable: false,
            },
            ScriptCommand::SendToMany {
                participants: vec![1001],
                kind: KIND_AUTHORITATIVE_INPUT,
                body: vec![1],
                unreliable: false,
            },
            ScriptCommand::BroadcastMatch {
                kind: KIND_AUTHORITATIVE_INPUT,
                body: vec![1],
                unreliable: false,
                exclude: None,
            },
        ];
        for command in commands {
            let mut ledger = PendingBatchLedger::new(1, 5, 9);
            let batch = ledger.issue_with_reserved_kind_mode(
                vec![input_draft(1001, 7)],
                100,
                ReservedKindMode::AuthoritativeInput,
            );
            let mut answer = accept_all(&batch);
            answer.commands.push(command);
            assert_eq!(
                ledger.validate(
                    &TestContext::permissive(),
                    &BridgeQuotas::default(),
                    &answer
                ),
                Err(BatchRejection::ReservedKind {
                    kind: KIND_AUTHORITATIVE_INPUT
                })
            );
        }
    }

    #[test]
    fn reserved_kinds_40_and_41_are_mode_aware() {
        for kind in [40, KIND_AUTHORITATIVE_INPUT] {
            let mut legacy = PendingBatchLedger::new(1, 5, 9);
            let legacy_batch = legacy.issue(vec![input_draft(1001, 7)], 100);
            let mut legacy_answer = accept_all(&legacy_batch);
            legacy_answer.commands.push(ScriptCommand::BroadcastMatch {
                kind,
                body: vec![1],
                unreliable: false,
                exclude: None,
            });
            assert!(
                legacy
                    .validate(
                        &TestContext::permissive(),
                        &BridgeQuotas::default(),
                        &legacy_answer
                    )
                    .is_ok()
            );

            let mut v2 = PendingBatchLedger::new(1, 5, 9);
            let v2_batch = v2.issue_with_reserved_kind_mode(
                vec![input_draft(1001, 7)],
                100,
                ReservedKindMode::AuthoritativeInput,
            );
            let mut v2_answer = accept_all(&v2_batch);
            v2_answer.commands.push(ScriptCommand::BroadcastMatch {
                kind,
                body: vec![1],
                unreliable: false,
                exclude: None,
            });
            assert_eq!(
                v2.validate(
                    &TestContext::permissive(),
                    &BridgeQuotas::default(),
                    &v2_answer
                ),
                Err(BatchRejection::ReservedKind { kind })
            );
        }
    }

    #[test]
    fn reserved_kind_message_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.commands.push(ScriptCommand::BroadcastMatch {
            kind: 9, // KIND_TSYNC_INPUT — reserved
            body: vec![1],
            unreliable: false,
            exclude: None,
        });
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::ReservedKind { kind: 9 })
        );
    }

    // ---- B21 quotas ----
    #[test]
    fn command_count_over_quota_rejects_batch() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        let quotas = BridgeQuotas {
            max_commands: 2,
            ..BridgeQuotas::default()
        };
        for _ in 0..3 {
            answer.commands.push(ScriptCommand::BroadcastMatch {
                kind: 100,
                body: vec![1],
                unreliable: false,
                exclude: None,
            });
        }
        assert_eq!(
            ledger.validate(&TestContext::permissive(), &quotas, &answer),
            Err(BatchRejection::QuotaExceeded {
                quota: Quota::CommandCount
            })
        );
    }

    #[test]
    fn body_bytes_over_quota_rejects_batch() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        let quotas = BridgeQuotas {
            max_command_body_bytes: 8,
            ..BridgeQuotas::default()
        };
        answer.commands.push(ScriptCommand::BroadcastMatch {
            kind: 100,
            body: vec![0u8; 16],
            unreliable: false,
            exclude: None,
        });
        assert_eq!(
            ledger.validate(&TestContext::permissive(), &quotas, &answer),
            Err(BatchRejection::QuotaExceeded {
                quota: Quota::CommandBodyBytes
            })
        );
    }

    #[test]
    fn reply_over_quota_rejects_batch() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = ScriptCommandBatch::answering(&batch);
        answer.input_outcomes.push(InputOutcome {
            event_id: batch.events[0].event_id,
            decision: Decision::Reject { reason_code: 1 },
            reply: Some(vec![0u8; 16]),
        });
        let quotas = BridgeQuotas {
            max_reply_bytes: 8,
            ..BridgeQuotas::default()
        };
        assert_eq!(
            ledger.validate(&TestContext::permissive(), &quotas, &answer),
            Err(BatchRejection::QuotaExceeded {
                quota: Quota::ReplyBytes
            })
        );
    }

    // ---- B22 undeclared capability ----
    #[test]
    fn undeclared_capability_persist_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.commands.push(ScriptCommand::Persist {
            op: crate::runtime::bridge_protocol::PersistOp {
                collection: "scores".into(),
                key: "k".into(),
                value: vec![1],
            },
        });
        let ctx = TestContext {
            capabilities: vec![], // no Persist capability
            ..TestContext::permissive()
        };
        assert_eq!(
            ledger.validate(&ctx, &BridgeQuotas::default(), &answer),
            Err(BatchRejection::MissingCapability {
                capability: Capability::Persist
            })
        );
    }

    // ---- B23 script command out of replicated bounds ----
    #[test]
    fn script_command_out_of_replicated_bounds_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.commands.push(ScriptCommand::SetReplicatedVars {
            object_id: 7,
            fields: vec![BridgeRepField {
                field_id: 3,
                value: BridgeRepValue::Scalar(999.0),
            }],
        });
        let ctx = TestContext {
            out_of_bounds: vec![(7, 3)],
            ..TestContext::permissive()
        };
        assert_eq!(
            ledger.validate(&ctx, &BridgeQuotas::default(), &answer),
            Err(BatchRejection::ReplicatedValueOutOfBounds {
                object_id: 7,
                field_id: 3
            })
        );
    }

    #[test]
    fn state_mutation_on_foreign_object_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.commands.push(ScriptCommand::ApplyTransform {
            object_id: 999,
            transform: transform(),
        });
        let ctx = TestContext {
            objects: Some(vec![7]),
            ..TestContext::permissive()
        };
        assert_eq!(
            ledger.validate(&ctx, &BridgeQuotas::default(), &answer),
            Err(BatchRejection::ObjectNotInMatch { object_id: 999 })
        );
    }

    #[test]
    fn incompatible_correction_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        // A rep-write event corrected with a transform is incompatible.
        let batch = ledger.issue(
            vec![EventDraft::guest(
                1001,
                NormalizedPayload::ReplicatedVarWrite {
                    object_id: 7,
                    class_id: 1,
                    schema_hash: [0; 16],
                    result_id: 1,
                    fields: vec![],
                },
            )],
            100,
        );
        let mut answer = ScriptCommandBatch::answering(&batch);
        answer.input_outcomes.push(InputOutcome {
            event_id: batch.events[0].event_id,
            decision: Decision::Correct {
                correction: Correction::Transform(transform()),
            },
            reply: None,
        });
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::IncompatibleCorrection {
                event_id: batch.events[0].event_id
            })
        );
    }

    #[test]
    fn correct_transform_on_input_accepted() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = ScriptCommandBatch::answering(&batch);
        answer.input_outcomes.push(InputOutcome {
            event_id: batch.events[0].event_id,
            decision: Decision::Correct {
                correction: Correction::Transform(transform()),
            },
            reply: None,
        });
        let validated = ledger
            .validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer,
            )
            .expect("transform correction on input is compatible");
        assert!(matches!(
            validated.outcomes[0].decision,
            Decision::Correct { .. }
        ));
    }

    #[test]
    fn empty_tick_batch_with_zero_events_accepts_zero_outcomes() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![], 100);
        let mut answer = ScriptCommandBatch::answering(&batch);
        answer.commands.push(ScriptCommand::BroadcastMatch {
            kind: 100,
            body: vec![1],
            unreliable: false,
            exclude: None,
        });
        let validated = ledger
            .validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer,
            )
            .expect("a tick-only batch may carry zero outcomes");
        assert!(validated.outcomes.is_empty());
        assert_eq!(validated.commands.len(), 1);
    }

    #[test]
    fn stale_tick_rejected() {
        let mut ledger = PendingBatchLedger::new(1, 5, 9);
        let batch = ledger.issue(vec![input_draft(1001, 7)], 100);
        let mut answer = accept_all(&batch);
        answer.tick = 99;
        assert_eq!(
            ledger.validate(
                &TestContext::permissive(),
                &BridgeQuotas::default(),
                &answer
            ),
            Err(BatchRejection::StaleTick {
                got: 99,
                expected: 100
            })
        );
    }
}
