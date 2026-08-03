//! Durable, fenced authority for authenticated parties.
//!
//! Each party is a system-owned aggregate object.  Membership is a separate
//! system-owned `account -> party` claim projection; every logical change writes
//! both objects in one `StorageRepository::atomic_batch`.  A claim is held for
//! both members and invitees so the global participant-disjointness invariant
//! does not depend on a read of another party aggregate.

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::party::{PartyId, PartySnapshot};
use crate::repository::StorageRepository;
use crate::session::{NodeId, OwnershipGeneration};
use crate::storage::{
    Accessor, AtomicBatchOperation, Collection, Key, ObjectId, Owner, Permissions, Precondition,
    StorageValue, WriteRequest,
};
use crate::time::TimestampMillis;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const PARTY_COLLECTION: &str = "citadel.party.aggregate";
const MEMBER_COLLECTION: &str = "citadel.party.membership";
const CREATE_COLLECTION: &str = "citadel.party.create";
const MIGRATION_COLLECTION: &str = "citadel.party.migration";
const LEGACY_KEY: &str = "directory-v2";
const MAX_RETRIES: usize = 8;
const MAX_REQUEST_ID_BYTES: usize = 128;
/// Results are retained only long enough for normal RPC retry.  Retention is
/// deliberately count-bounded because this state lives with the party
/// aggregate and must not grow with the lifetime of a busy party.
const MAX_DEDUPE: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyOwnerLease {
    pub party_id: PartyId,
    pub owner_node: NodeId,
    pub generation: OwnershipGeneration,
    pub expires_at: TimestampMillis,
}
impl PartyOwnerLease {
    #[must_use]
    pub fn is_current_at(&self, now: TimestampMillis) -> bool {
        self.expires_at > now
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartyOwnerResolution {
    Local(PartyOwnerLease),
    Remote(PartyOwnerLease),
}
/// Opaque, owner-fenced proof of one queue admission.  It must accompany any
/// later release; leader/revision alone are intentionally insufficient.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartyQueueFreeze {
    pub revision: u64,
    pub owner_generation: u64,
    pub admission_generation: u64,
    pub admission_token: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredLease {
    node_id: String,
    generation: u64,
    expires_at: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredResult {
    actor: String,
    request_id: String,
    generation: u64,
    revision: u64,
    /// A compact public replay snapshot.  Do not store `StoredParty` here:
    /// doing so recursively embeds the prior dedupe log in every entry.
    snapshot: StoredReplaySnapshot,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredReplaySnapshot {
    leader_user_id: String,
    members: BTreeSet<String>,
    invitations: BTreeSet<String>,
    revision: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredTicketFreeze {
    revision: u64,
    leader_user_id: String,
    /// Monotonically increasing admission epoch.  It is deliberately kept on
    /// the aggregate (rather than derived from revision) because a timeout can
    /// permit another admission at the same party revision.
    #[serde(default)]
    admission_generation: u64,
    /// Unique within a party for the lifetime of the aggregate.  A delayed
    /// cancellation must present this exact token before it can clear a
    /// reservation made by a later admission (the ABA fence).
    #[serde(default)]
    admission_token: u64,
    /// Owner generation that committed this admission.  A takeover may recover
    /// an expired reservation, but it cannot let an old owner release one.
    #[serde(default)]
    owner_generation: u64,
    /// Missing on an old persisted freeze means expired, so upgrades recover
    /// rather than perpetuating a lock created by a crashed older process.
    #[serde(default)]
    expires_at: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredParty {
    leader_user_id: String,
    members: BTreeSet<String>,
    invitations: BTreeSet<String>,
    revision: u64,
    #[serde(default)]
    lease: Option<StoredLease>,
    #[serde(default)]
    max_generation: u64,
    #[serde(default)]
    results: Vec<StoredResult>,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    ticket_freeze: Option<StoredTicketFreeze>,
    #[serde(default)]
    max_admission_generation: u64,
    /// Highest owner generation for which recovery has published its one
    /// client resync barrier. Keeping this beside the durable owner fence makes
    /// replay/restart of a completed takeover idempotent without retaining a
    /// client payload or member list.
    #[serde(default)]
    last_resync_generation: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MemberProjection {
    party_id: String,
    /// Older membership-only projections deserialize as members.
    #[serde(default)]
    invitation: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreateProjection {
    /// Bounded per-leader durable create replay cache.  The previous layout
    /// used one object per request ID, which was an unbounded projection.
    #[serde(default)]
    results: Vec<StoredCreateResult>,
    /// Read compatibility for the short-lived unbounded projection.  It is
    /// never written by current code.
    #[serde(default)]
    party_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCreateResult {
    request_id: String,
    party_id: String,
    snapshot: StoredReplaySnapshot,
    lease: StoredLease,
}
/// Only used to read the pre-0436 single-object layout during rolling upgrade.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct LegacyState {
    #[serde(default)]
    parties: BTreeMap<String, StoredParty>,
    #[serde(default)]
    membership: BTreeMap<String, String>,
}

/// Durable proof that every reservation in a particular legacy-directory
/// snapshot has been materialized. The digest lets normal create/invite calls
/// do O(1) work after the one-time scan, while a rolling old binary changing
/// the source invalidates the gate and forces a fresh materialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyMigration {
    version: String,
}

struct LegacyFence {
    value: StorageValue,
    expected: Precondition,
    version: String,
}

struct LegacySource {
    state: LegacyState,
    fence: LegacyFence,
}

#[derive(Clone)]
pub struct StoragePartyDirectory {
    storage: Arc<dyn StorageRepository>,
}
#[derive(Debug, Clone, Copy)]
enum Mutation<'a> {
    Invite { target: &'a str },
    Accept,
    Leave,
    Promote { target: &'a str },
    Remove { target: &'a str },
    Close,
}

impl StoragePartyDirectory {
    #[must_use]
    pub fn new(storage: Arc<dyn StorageRepository>) -> Self {
        Self { storage }
    }

    pub async fn create(
        &self,
        party_id: PartyId,
        leader: &str,
        node: NodeId,
        expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> AppResult<(PartySnapshot, PartyOwnerLease)> {
        self.create_with_request(party_id, leader, None, node, expires_at, now)
            .await
    }

    /// Persist a bounded `(leader, request_id)` result with the first create.
    /// This makes a retry (including a concurrent retry on another gateway)
    /// return the original aggregate instead of failing the leader claim.
    pub async fn create_with_request(
        &self,
        party_id: PartyId,
        leader: &str,
        request_id: Option<&str>,
        node: NodeId,
        expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> AppResult<(PartySnapshot, PartyOwnerLease)> {
        if leader.is_empty() || expires_at <= now {
            return Err(AppError::validation("invalid party create request"));
        }
        if let Some(request_id) = request_id {
            if request_id.is_empty() || request_id.len() > MAX_REQUEST_ID_BYTES {
                return Err(AppError::validation("invalid party create request"));
            }
            if let Some(created) = self.create_result(leader, request_id).await? {
                return Ok(created);
            }
        }
        for _ in 0..MAX_RETRIES {
            // The gate does a full materialization only once per legacy
            // snapshot. Its source CAS is carried into this claim batch, so an
            // old-layout write concurrent with this create forces a retry from
            // the changed snapshot rather than losing a reservation.
            let legacy = self.legacy_migration_gate().await?;
            if self.read_party(&party_id).await?.is_some() {
                return Err(AppError::conflict("party id already exists"));
            }
            if self.member(leader).await?.is_some() || self.legacy_member_exists(leader).await? {
                return Err(AppError::conflict("user already belongs to a party"));
            }
            let lease = PartyOwnerLease {
                party_id: party_id.clone(),
                owner_node: node.clone(),
                generation: OwnershipGeneration::new(1),
                expires_at,
            };
            let party = StoredParty {
                leader_user_id: leader.to_owned(),
                members: BTreeSet::from([leader.to_owned()]),
                invitations: BTreeSet::new(),
                revision: 1,
                lease: Some(encode_lease(&lease)),
                max_generation: 1,
                results: vec![],
                closed: false,
                ticket_freeze: None,
                max_admission_generation: 0,
                last_resync_generation: 0,
            };
            let create_replay = if let Some(request_id) = request_id {
                let (mut projection, expected) = self.create_projection(leader).await?;
                if let Some(created) = projection
                    .results
                    .iter()
                    .find(|x| x.request_id == request_id)
                {
                    return Ok(self.created_result(created));
                }
                projection.party_id = None;
                projection.results.push(StoredCreateResult {
                    request_id: request_id.to_owned(),
                    party_id: party_id.as_str().to_owned(),
                    snapshot: replay_snapshot_for(&party),
                    lease: encode_lease(&lease),
                });
                if projection.results.len() > MAX_DEDUPE {
                    projection.results.remove(0);
                }
                Some((projection, expected))
            } else {
                None
            };
            let mut ops = vec![
                write(party_object(&party_id), &party, Precondition::MustNotExist)?,
                write(
                    member_object(leader)?,
                    &MemberProjection {
                        party_id: party_id.as_str().to_owned(),
                        invitation: false,
                    },
                    Precondition::MustNotExist,
                )?,
            ];
            if let Some(source) = legacy.as_ref() {
                ops.push(legacy_touch(source)?);
            }
            if let Some((projection, expected)) = create_replay {
                ops.push(write(create_object(leader)?, &projection, expected)?);
            }
            match self.storage.atomic_batch(ops).await {
                Ok(_) => return Ok((snapshot(&party_id, &party), lease)),
                Err(e) if e.category() == ErrorCategory::Conflict => {
                    if let Some(request_id) = request_id
                        && let Some(created) = self.create_result(leader, request_id).await?
                    {
                        return Ok(created);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(AppError::conflict(
            "party directory changed repeatedly while creating",
        ))
    }

    pub async fn acquire_or_resolve(
        &self,
        party_id: &PartyId,
        node: NodeId,
        expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> AppResult<PartyOwnerResolution> {
        if expires_at <= now {
            return Err(AppError::validation(
                "party lease must expire after acquisition time",
            ));
        }
        self.migrate(party_id).await?;
        for _ in 0..MAX_RETRIES {
            let (mut party, expected) = self.party(party_id).await?;
            if party.closed {
                return Err(AppError::not_found("party not found"));
            }
            if let Some(current) = party
                .lease
                .clone()
                .map(|x| decode_lease(party_id, x))
                .transpose()?
                .filter(|x| x.is_current_at(now))
            {
                if current.owner_node != node {
                    return Ok(PartyOwnerResolution::Remote(current));
                }
                let renewed = PartyOwnerLease {
                    party_id: party_id.clone(),
                    owner_node: node.clone(),
                    generation: current.generation,
                    expires_at,
                };
                party.lease = Some(encode_lease(&renewed));
                match self
                    .batch(vec![write(party_object(party_id), &party, expected)?])
                    .await
                {
                    Ok(()) => return Ok(PartyOwnerResolution::Local(renewed)),
                    Err(e) if e.category() == ErrorCategory::Conflict => continue,
                    Err(e) => return Err(e),
                }
            }
            let generation = party
                .max_generation
                .checked_add(1)
                .ok_or_else(|| AppError::internal("party owner generation overflowed"))?;
            party.max_generation = generation;
            let acquired = PartyOwnerLease {
                party_id: party_id.clone(),
                owner_node: node.clone(),
                generation: OwnershipGeneration::new(generation),
                expires_at,
            };
            party.lease = Some(encode_lease(&acquired));
            match self
                .batch(vec![write(party_object(party_id), &party, expected)?])
                .await
            {
                Ok(()) => return Ok(PartyOwnerResolution::Local(acquired)),
                Err(e) if e.category() == ErrorCategory::Conflict => continue,
                Err(e) => return Err(e),
            }
        }
        Err(AppError::conflict(
            "party ownership changed repeatedly while resolving",
        ))
    }

    /// Claim the sole recoverable client-transition for an already acquired
    /// replacement-owner generation, and return the committed snapshot that
    /// must follow that transition.  The durable marker is intentionally
    /// written before gateway delivery: a restarted gateway can safely reload
    /// the snapshot rather than emitting a duplicate resync for the same
    /// fencing generation.
    ///
    /// Generation one is the initial owner, not a failover, so it has no
    /// recovery transition to publish.
    pub async fn claim_failover_resync(
        &self,
        lease: &PartyOwnerLease,
        now: TimestampMillis,
    ) -> AppResult<Option<PartySnapshot>> {
        if lease.generation.get() <= 1 {
            return Ok(None);
        }
        self.migrate(&lease.party_id).await?;
        for _ in 0..MAX_RETRIES {
            let (mut party, expected) = self.party(&lease.party_id).await?;
            ensure_fence(&party, lease, now)?;
            if party.closed {
                return Err(AppError::not_found("party not found"));
            }
            if party.last_resync_generation >= lease.generation.get() {
                return Ok(None);
            }
            party.last_resync_generation = lease.generation.get();
            let recovered = snapshot(&lease.party_id, &party);
            match self
                .batch(vec![write(
                    party_object(&lease.party_id),
                    &party,
                    expected,
                )?])
                .await
            {
                Ok(()) => return Ok(Some(recovered)),
                Err(error) if error.category() == ErrorCategory::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        Err(AppError::conflict(
            "party ownership changed repeatedly while claiming recovery resync",
        ))
    }

    pub async fn invite(
        &self,
        lease: &PartyOwnerLease,
        actor: &str,
        request_id: &str,
        target: &str,
        expected_revision: u64,
        now: TimestampMillis,
    ) -> AppResult<PartySnapshot> {
        self.mutate(
            lease,
            actor,
            request_id,
            expected_revision,
            now,
            Mutation::Invite { target },
        )
        .await
    }
    pub async fn accept(
        &self,
        lease: &PartyOwnerLease,
        actor: &str,
        request_id: &str,
        expected_revision: u64,
        now: TimestampMillis,
    ) -> AppResult<PartySnapshot> {
        self.mutate(
            lease,
            actor,
            request_id,
            expected_revision,
            now,
            Mutation::Accept,
        )
        .await
    }
    pub async fn leave(
        &self,
        lease: &PartyOwnerLease,
        actor: &str,
        request_id: &str,
        expected_revision: u64,
        now: TimestampMillis,
    ) -> AppResult<PartySnapshot> {
        self.mutate(
            lease,
            actor,
            request_id,
            expected_revision,
            now,
            Mutation::Leave,
        )
        .await
    }
    pub async fn promote(
        &self,
        lease: &PartyOwnerLease,
        actor: &str,
        request_id: &str,
        target: &str,
        expected_revision: u64,
        now: TimestampMillis,
    ) -> AppResult<PartySnapshot> {
        self.mutate(
            lease,
            actor,
            request_id,
            expected_revision,
            now,
            Mutation::Promote { target },
        )
        .await
    }
    pub async fn remove(
        &self,
        lease: &PartyOwnerLease,
        actor: &str,
        request_id: &str,
        target: &str,
        expected_revision: u64,
        now: TimestampMillis,
    ) -> AppResult<PartySnapshot> {
        self.mutate(
            lease,
            actor,
            request_id,
            expected_revision,
            now,
            Mutation::Remove { target },
        )
        .await
    }
    pub async fn close(
        &self,
        lease: &PartyOwnerLease,
        actor: &str,
        request_id: &str,
        expected_revision: u64,
        now: TimestampMillis,
    ) -> AppResult<PartySnapshot> {
        self.mutate(
            lease,
            actor,
            request_id,
            expected_revision,
            now,
            Mutation::Close,
        )
        .await
    }

    async fn mutate(
        &self,
        lease: &PartyOwnerLease,
        actor: &str,
        request_id: &str,
        expected_revision: u64,
        now: TimestampMillis,
        mutation: Mutation<'_>,
    ) -> AppResult<PartySnapshot> {
        if actor.is_empty()
            || request_id.is_empty()
            || request_id.len() > MAX_REQUEST_ID_BYTES
            || matches!(mutation, Mutation::Invite{target}|Mutation::Promote{target}|Mutation::Remove{target} if target.is_empty())
        {
            return Err(AppError::validation("invalid party mutation request"));
        }
        self.migrate(&lease.party_id).await?;
        for _ in 0..MAX_RETRIES {
            // An invite creates a global reservation, so it must fence the
            // old single-object source through the same atomic batch.
            let legacy = if matches!(mutation, Mutation::Invite { .. }) {
                self.legacy_migration_gate().await?
            } else {
                None
            };
            let (mut party, party_expected) = self.party(&lease.party_id).await?;
            ensure_fence(&party, lease, now)?;
            if let Some(r) = party.results.iter().find(|r| {
                r.actor == actor
                    && r.request_id == request_id
                    && r.generation == lease.generation.get()
            }) {
                return Ok(replay_snapshot(&lease.party_id, &r.snapshot));
            }
            if party.closed {
                return Err(AppError::not_found("party not found"));
            }
            if party
                .ticket_freeze
                .as_ref()
                .is_some_and(|freeze| freeze.expires_at > now.unix_millis())
            {
                return Err(AppError::conflict(
                    "party is queued; cancel its matchmaker ticket first",
                ));
            }
            // An expired freeze represents an abandoned owner/shard flow. It
            // is cleared in this same aggregate CAS as the next mutation, so
            // a concurrent recovery cannot unfreeze a current ticket.
            party.ticket_freeze = None;
            if party.revision != expected_revision {
                return Err(AppError::conflict(
                    "party revision changed; retry with a fresh snapshot",
                ));
            }
            let mut ops = Vec::new();
            let mut closing = false;
            match mutation {
                Mutation::Invite { target } => {
                    if party.leader_user_id != actor {
                        return Err(AppError::permission("party leader required"));
                    }
                    if party.invitations.contains(target)
                        || self.member(target).await?.is_some()
                        || self.legacy_member_exists(target).await?
                    {
                        return Err(AppError::conflict(
                            "user already belongs to a party or is invited",
                        ));
                    }
                    if party.members.len() + party.invitations.len() >= 8 {
                        return Err(AppError::conflict("party is full"));
                    }
                    // This MustNotExist precondition is the global atomic
                    // reservation. Two owners of different party aggregates
                    // cannot concurrently invite (or invite/accept) one
                    // account: exactly one batch can create its claim.
                    ops.push(write(
                        member_object(target)?,
                        &MemberProjection {
                            party_id: lease.party_id.as_str().to_owned(),
                            invitation: true,
                        },
                        Precondition::MustNotExist,
                    )?);
                    party.invitations.insert(target.to_owned());
                }
                Mutation::Accept => {
                    let invitation_claim = self.member(actor).await?;
                    if !party.invitations.remove(actor) {
                        return Err(AppError::permission("party invitation required"));
                    }
                    let Some((claim, claim_expected)) = invitation_claim else {
                        return Err(AppError::conflict("party invitation claim changed"));
                    };
                    if !claim.invitation || claim.party_id != lease.party_id.as_str() {
                        return Err(AppError::conflict("user already belongs to a party"));
                    }
                    if party.members.len() >= 8 {
                        return Err(AppError::conflict("party is full"));
                    }
                    party.members.insert(actor.to_owned());
                    ops.push(write(
                        member_object(actor)?,
                        &MemberProjection {
                            party_id: lease.party_id.as_str().to_owned(),
                            invitation: false,
                        },
                        claim_expected,
                    )?);
                }
                Mutation::Leave => {
                    if !party.members.contains(actor) {
                        return Err(AppError::permission("party membership required"));
                    }
                    if party.leader_user_id == actor {
                        closing = true;
                    } else {
                        party.members.remove(actor);
                        ops.push(delete(
                            member_object(actor)?,
                            self.member_expected(actor).await?,
                        ));
                    }
                }
                Mutation::Promote { target } => {
                    if party.leader_user_id != actor {
                        return Err(AppError::permission("party leader required"));
                    }
                    if !party.members.contains(target) {
                        return Err(AppError::permission("party membership required"));
                    }
                    party.leader_user_id = target.to_owned();
                }
                Mutation::Remove { target } => {
                    if party.leader_user_id != actor {
                        return Err(AppError::permission("party leader required"));
                    }
                    if target == actor || !party.members.remove(target) {
                        return Err(AppError::permission("party membership required"));
                    }
                    if party.invitations.remove(target) {
                        // Defensive cleanup for states written by an older
                        // binary; normal states cannot be both invited/member.
                        ops.push(delete(
                            member_object(target)?,
                            self.member_expected(target).await?,
                        ));
                    }
                    ops.push(delete(
                        member_object(target)?,
                        self.member_expected(target).await?,
                    ));
                }
                Mutation::Close => {
                    if party.leader_user_id != actor {
                        return Err(AppError::permission("party leader required"));
                    }
                    closing = true;
                }
            }
            party.revision += 1;
            if closing {
                for member in party.members.clone() {
                    ops.push(delete(
                        member_object(&member)?,
                        self.member_expected(&member).await?,
                    ));
                }
                for invitee in party.invitations.clone() {
                    ops.push(delete(
                        member_object(&invitee)?,
                        self.member_expected(&invitee).await?,
                    ));
                }
                party.invitations.clear();
                party.closed = true;
            }
            let result = snapshot(&lease.party_id, &party);
            let saved = party.clone();
            party.results.push(StoredResult {
                actor: actor.to_owned(),
                request_id: request_id.to_owned(),
                generation: lease.generation.get(),
                revision: party.revision,
                snapshot: replay_snapshot_for(&saved),
            });
            if party.results.len() > MAX_DEDUPE {
                party.results.remove(0);
            }
            ops.insert(
                0,
                write(party_object(&lease.party_id), &party, party_expected)?,
            );
            if let Some(source) = legacy.as_ref() {
                ops.push(legacy_touch(source)?);
            }
            match self.batch(ops).await {
                Ok(()) => return Ok(result),
                Err(e) if e.category() == ErrorCategory::Conflict => continue,
                Err(e) => return Err(e),
            }
        }
        Err(AppError::conflict(
            "party changed repeatedly while mutating",
        ))
    }

    pub async fn snapshot(&self, id: &PartyId) -> AppResult<PartySnapshot> {
        let (p, _) = self.party(id).await?;
        (!p.closed)
            .then(|| snapshot(id, &p))
            .ok_or_else(|| AppError::not_found("party not found"))
    }
    pub async fn snapshot_for(&self, requester: &str, id: &PartyId) -> AppResult<PartySnapshot> {
        if requester.is_empty() {
            return Err(AppError::permission(
                "party membership or invitation required",
            ));
        }
        let s = self.snapshot(id).await?;
        if s.members.iter().any(|x| x == requester) || s.invitations.iter().any(|x| x == requester)
        {
            Ok(s)
        } else {
            Err(AppError::permission(
                "party membership or invitation required",
            ))
        }
    }

    /// Return the current aggregate only when this account is an accepted
    /// member. This is an internal realtime-presence lookup: unlike
    /// [`snapshot_for`](Self::snapshot_for), invitations are intentionally not
    /// sufficient because invitees must learn no member online state.
    pub async fn member_snapshot_for(&self, requester: &str) -> AppResult<Option<PartySnapshot>> {
        let Some((claim, _)) = self.member(requester).await? else {
            return Ok(None);
        };
        if claim.invitation {
            return Ok(None);
        }
        let id = PartyId::parse(&claim.party_id).map_err(|error| {
            AppError::internal(format!("invalid stored party membership: {error}"))
        })?;
        self.snapshot(&id).await.map(Some)
    }
    pub async fn queue_snapshot(
        &self,
        lease: &PartyOwnerLease,
        actor: &str,
        expected_revision: u64,
        ticket_expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> AppResult<(Vec<String>, PartyQueueFreeze)> {
        self.migrate(&lease.party_id).await?;
        for _ in 0..MAX_RETRIES {
            let (mut p, e) = self.party(&lease.party_id).await?;
            ensure_fence(&p, lease, now)?;
            if p.closed {
                return Err(AppError::not_found("party not found"));
            }
            if p.leader_user_id != actor {
                return Err(AppError::permission("party leader required"));
            }
            if p.revision != expected_revision {
                return Err(AppError::conflict(
                    "party changed before queue admission; retry",
                ));
            }
            if p.ticket_freeze
                .as_ref()
                .is_some_and(|freeze| freeze.expires_at > now.unix_millis())
            {
                return Err(AppError::conflict(
                    "party is already queued; cancel its matchmaker ticket first",
                ));
            }
            if ticket_expires_at <= now {
                return Err(AppError::conflict(
                    "party queue ticket expiration is not in the future",
                ));
            }
            // A previous admission owner may have crashed.  The new freeze is
            // written with the party CAS, so only one contender wins recovery.
            p.ticket_freeze = None;
            let admission_generation = p
                .max_admission_generation
                .checked_add(1)
                .ok_or_else(|| AppError::conflict("party admission generation exhausted"))?;
            let out = (
                p.members.iter().cloned().collect(),
                PartyQueueFreeze {
                    revision: p.revision,
                    owner_generation: lease.generation.get(),
                    admission_generation,
                    admission_token: admission_generation,
                },
            );
            p.max_admission_generation = out.1.admission_generation;
            p.ticket_freeze = Some(StoredTicketFreeze {
                revision: p.revision,
                leader_user_id: actor.to_owned(),
                admission_generation: out.1.admission_generation,
                admission_token: out.1.admission_token,
                owner_generation: lease.generation.get(),
                // A freeze represents an actual queued ticket, not an
                // arbitrary recovery lease. Its lifetime is supplied by the
                // matchmaker and must cover that ticket's exact expiration.
                expires_at: ticket_expires_at.unix_millis(),
            });
            match self
                .batch(vec![write(party_object(&lease.party_id), &p, e)?])
                .await
            {
                Ok(()) => return Ok(out),
                Err(x) if x.category() == ErrorCategory::Conflict => continue,
                Err(x) => return Err(x),
            }
        }
        Err(AppError::conflict(
            "party changed repeatedly while freezing queue admission",
        ))
    }
    pub async fn release_queue_freeze(
        &self,
        lease: &PartyOwnerLease,
        leader: &str,
        freeze: &PartyQueueFreeze,
        now: TimestampMillis,
    ) -> AppResult<()> {
        self.migrate(&lease.party_id).await?;
        for _ in 0..MAX_RETRIES {
            let (mut p, e) = self.party(&lease.party_id).await?;
            ensure_fence(&p, lease, now)?;
            if p.closed {
                return Err(AppError::not_found("party not found"));
            }
            let Some(f) = p.ticket_freeze.as_ref() else {
                return Ok(());
            };
            if f.leader_user_id != leader
                || f.revision != freeze.revision
                || f.owner_generation != freeze.owner_generation
                || f.admission_generation != freeze.admission_generation
                || f.admission_token != freeze.admission_token
            {
                return Err(AppError::conflict("party queue freeze changed"));
            }
            p.ticket_freeze = None;
            match self
                .batch(vec![write(party_object(&lease.party_id), &p, e)?])
                .await
            {
                Ok(()) => return Ok(()),
                Err(x) if x.category() == ErrorCategory::Conflict => continue,
                Err(x) => return Err(x),
            }
        }
        Err(AppError::conflict(
            "party changed repeatedly while releasing queue admission",
        ))
    }

    /// Extend an existing admission to the exact expiry of the authoritative
    /// ticket that is about to become live.  A remote shard may request this
    /// only through the current party owner and only with the original
    /// admission's complete ABA fence.
    pub async fn renew_queue_freeze(
        &self,
        lease: &PartyOwnerLease,
        leader: &str,
        freeze: &PartyQueueFreeze,
        ticket_expires_at: TimestampMillis,
        now: TimestampMillis,
    ) -> AppResult<()> {
        self.migrate(&lease.party_id).await?;
        for _ in 0..MAX_RETRIES {
            let (mut p, e) = self.party(&lease.party_id).await?;
            ensure_fence(&p, lease, now)?;
            if p.closed {
                return Err(AppError::not_found("party not found"));
            }
            let Some(f) = p.ticket_freeze.as_ref() else {
                return Err(AppError::conflict("party queue freeze changed"));
            };
            if f.leader_user_id != leader
                || f.revision != freeze.revision
                || f.owner_generation != freeze.owner_generation
                || f.admission_generation != freeze.admission_generation
                || f.admission_token != freeze.admission_token
            {
                return Err(AppError::conflict("party queue freeze changed"));
            }
            // An abandoned/expired admission is recoverable by the next party
            // mutation, not renewable by a delayed shard command. This keeps
            // an old ticket submission from resurrecting a freeze after its
            // ticket lifetime has ended.
            if f.expires_at <= now.unix_millis() {
                return Err(AppError::conflict("party queue freeze expired"));
            }
            if ticket_expires_at <= now {
                return Err(AppError::conflict(
                    "party queue ticket expiration is not in the future",
                ));
            }
            p.ticket_freeze.as_mut().expect("checked above").expires_at =
                ticket_expires_at.unix_millis();
            match self
                .batch(vec![write(party_object(&lease.party_id), &p, e)?])
                .await
            {
                Ok(()) => return Ok(()),
                Err(x) if x.category() == ErrorCategory::Conflict => continue,
                Err(x) => return Err(x),
            }
        }
        Err(AppError::conflict(
            "party changed repeatedly while renewing queue admission",
        ))
    }

    async fn migrate(&self, id: &PartyId) -> AppResult<()> {
        if self.read_party(id).await?.is_some() {
            return Ok(());
        }
        for _ in 0..MAX_RETRIES {
            let Some(legacy) = self.legacy_source().await? else {
                return Ok(());
            };
            let Some(p) = legacy.state.parties.get(id.as_str()).cloned() else {
                return Ok(());
            };
            let mut ops = vec![write(party_object(id), &p, Precondition::MustNotExist)?];
            for user in &p.members {
                ops.push(write(
                    member_object(user)?,
                    &MemberProjection {
                        party_id: id.as_str().to_owned(),
                        invitation: false,
                    },
                    Precondition::MustNotExist,
                )?);
            }
            // Legacy invitations are participant reservations too.  Writing these
            // in the same atomic batch both preserves old invitation acceptance and
            // makes an overlapping invite from another party lose deterministically.
            for user in &p.invitations {
                ops.push(write(
                    member_object(user)?,
                    &MemberProjection {
                        party_id: id.as_str().to_owned(),
                        invitation: true,
                    },
                    Precondition::MustNotExist,
                )?);
            }
            // This no-op write is intentional: the legacy object's version is
            // an atomic-batch precondition, so an old-layout mutation cannot be
            // silently overwritten by a migration based on a stale snapshot.
            ops.push(legacy_touch(&legacy.fence)?);
            match self.batch(ops).await {
                Ok(()) => return Ok(()),
                Err(e)
                    if e.category() == ErrorCategory::Conflict
                        && self.read_party(id).await?.is_some() =>
                {
                    return Ok(());
                }
                Err(e) if e.category() == ErrorCategory::Conflict => continue,
                Err(e) => return Err(e),
            }
        }
        Err(AppError::conflict(
            "legacy party changed repeatedly while migrating",
        ))
    }
    /// Return a fenced, complete legacy source for a new global claim. A
    /// completed marker makes the common path O(1) without deserializing the
    /// directory; a changed source version triggers one bounded catch-up scan.
    async fn legacy_migration_gate(&self) -> AppResult<Option<LegacyFence>> {
        for _ in 0..MAX_RETRIES {
            let Some(fence) = self.legacy_fence().await? else {
                return Ok(None);
            };
            if self
                .migration_marker()
                .await?
                .is_some_and(|marker| marker.version == fence.version)
            {
                return Ok(Some(fence));
            }
            let source = LegacySource {
                state: decode(fence.value.clone())?,
                fence,
            };
            for party_id in source.state.parties.keys() {
                let id = PartyId::parse(party_id.clone())
                    .map_err(|_| AppError::internal("invalid legacy party id"))?;
                self.migrate(&id).await?;
            }
            // Re-read after the scan: an old deployment may have changed the
            // source while individual parties were materialized. Retrying from
            // the new snapshot converges without accepting a stale marker.
            let Some(fresh) = self.legacy_fence().await? else {
                return Ok(None);
            };
            if fresh.version != source.fence.version {
                continue;
            }
            let marker = LegacyMigration {
                version: fresh.version.clone(),
            };
            let expected = self
                .migration_marker_expected()
                .await?
                .unwrap_or(Precondition::MustNotExist);
            match self
                .batch(vec![
                    write(migration_object(), &marker, expected)?,
                    legacy_touch(&fresh)?,
                ])
                .await
            {
                Ok(()) => return self.legacy_fence().await,
                Err(e) if e.category() == ErrorCategory::Conflict => continue,
                Err(e) => return Err(e),
            }
        }
        Err(AppError::conflict(
            "legacy directory changed repeatedly while completing migration",
        ))
    }
    async fn party(&self, id: &PartyId) -> AppResult<(StoredParty, Precondition)> {
        if let Some((p, e)) = self.read_party(id).await? {
            return Ok((p, e));
        }
        let legacy = self
            .legacy()
            .await?
            .and_then(|s| s.parties.get(id.as_str()).cloned())
            .ok_or_else(|| AppError::not_found("party not found"))?;
        Ok((legacy, Precondition::MustNotExist))
    }
    async fn read_party(&self, id: &PartyId) -> AppResult<Option<(StoredParty, Precondition)>> {
        let Some(o) = self
            .storage
            .read(&Accessor::Runtime, &party_object(id))
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((decode(o.value)?, Precondition::Match(o.version))))
    }
    async fn member(&self, user: &str) -> AppResult<Option<(MemberProjection, Precondition)>> {
        let object = member_object(user)?;
        let Some(o) = self.storage.read(&Accessor::Runtime, &object).await? else {
            return Ok(None);
        };
        Ok(Some((decode(o.value)?, Precondition::Match(o.version))))
    }
    async fn create_projection(&self, leader: &str) -> AppResult<(CreateProjection, Precondition)> {
        let object = create_object(leader)?;
        let Some(o) = self.storage.read(&Accessor::Runtime, &object).await? else {
            return Ok((
                CreateProjection {
                    results: vec![],
                    party_id: None,
                },
                Precondition::MustNotExist,
            ));
        };
        Ok((decode(o.value)?, Precondition::Match(o.version)))
    }
    async fn create_result(
        &self,
        leader: &str,
        request_id: &str,
    ) -> AppResult<Option<(PartySnapshot, PartyOwnerLease)>> {
        let (projection, _) = self.create_projection(leader).await?;
        if let Some(created) = projection
            .results
            .iter()
            .find(|x| x.request_id == request_id)
        {
            return Ok(Some(self.created_result(created)));
        }
        // Compatibility with the prior per-request projection.  Current
        // callers never write it, but a rolling retry remains readable.
        if let Some(party_id) = projection.party_id {
            let id = PartyId::parse(party_id)
                .map_err(|_| AppError::internal("invalid persisted party create result"))?;
            let snapshot = self.snapshot(&id).await?;
            let (stored, _) = self.party(&id).await?;
            let lease = stored
                .lease
                .ok_or_else(|| AppError::internal("created party lease absent"))?;
            return Ok(Some((snapshot, decode_lease(&id, lease)?)));
        }
        // Read-only rolling compatibility for the former one-object-per
        // request layout. New writes are consolidated into the bounded
        // per-leader projection above.
        if let Some(o) = self
            .storage
            .read(
                &Accessor::Runtime,
                &legacy_create_object(leader, request_id)?,
            )
            .await?
        {
            let legacy: CreateProjection = decode(o.value)?;
            if let Some(party_id) = legacy.party_id {
                let id = PartyId::parse(party_id)
                    .map_err(|_| AppError::internal("invalid persisted party create result"))?;
                let snapshot = self.snapshot(&id).await?;
                let (stored, _) = self.party(&id).await?;
                let lease = stored
                    .lease
                    .ok_or_else(|| AppError::internal("created party lease absent"))?;
                return Ok(Some((snapshot, decode_lease(&id, lease)?)));
            }
        }
        Ok(None)
    }
    fn created_result(&self, created: &StoredCreateResult) -> (PartySnapshot, PartyOwnerLease) {
        let id = PartyId::parse(created.party_id.clone())
            .expect("stored create party id was validated before persistence");
        (
            replay_snapshot(&id, &created.snapshot),
            decode_lease(&id, created.lease.clone())
                .expect("stored create lease was validated before persistence"),
        )
    }
    async fn member_expected(&self, user: &str) -> AppResult<Precondition> {
        self.member(user)
            .await?
            .map(|x| x.1)
            .ok_or_else(|| AppError::conflict("membership projection changed"))
    }
    async fn legacy_fence(&self) -> AppResult<Option<LegacyFence>> {
        let Some(o) = self
            .storage
            .read(&Accessor::Runtime, &legacy_object())
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(LegacyFence {
            value: o.value,
            version: o.version.as_str().to_owned(),
            expected: Precondition::Match(o.version),
        }))
    }
    async fn legacy_source(&self) -> AppResult<Option<LegacySource>> {
        let Some(fence) = self.legacy_fence().await? else {
            return Ok(None);
        };
        Ok(Some(LegacySource {
            state: decode(fence.value.clone())?,
            fence,
        }))
    }
    async fn legacy(&self) -> AppResult<Option<LegacyState>> {
        Ok(self.legacy_source().await?.map(|source| source.state))
    }
    async fn migration_marker(&self) -> AppResult<Option<LegacyMigration>> {
        let Some(o) = self
            .storage
            .read(&Accessor::Runtime, &migration_object())
            .await?
        else {
            return Ok(None);
        };
        decode(o.value).map(Some)
    }
    async fn migration_marker_expected(&self) -> AppResult<Option<Precondition>> {
        Ok(self
            .storage
            .read(&Accessor::Runtime, &migration_object())
            .await?
            .map(|o| Precondition::Match(o.version)))
    }
    async fn legacy_member_exists(&self, user: &str) -> AppResult<bool> {
        let Some(legacy_party) = self
            .legacy()
            .await?
            .and_then(|state| state.membership.get(user).cloned())
        else {
            return Ok(false);
        };
        // A migrated aggregate owns its projection even though the legacy
        // compatibility object is deliberately retained for other parties.
        let id = PartyId::parse(legacy_party)
            .map_err(|_| AppError::internal("invalid legacy party id"))?;
        Ok(self.read_party(&id).await?.is_none())
    }
    async fn batch(&self, ops: Vec<AtomicBatchOperation>) -> AppResult<()> {
        self.storage.atomic_batch(ops).await.map(|_| ())
    }
}
fn party_object(id: &PartyId) -> ObjectId {
    ObjectId::new(
        Owner::System,
        Collection::new(PARTY_COLLECTION).expect("static"),
        Key::new(id.as_str()).expect("party id validated"),
    )
}
fn member_object(user: &str) -> AppResult<ObjectId> {
    Ok(ObjectId::new(
        Owner::System,
        Collection::new(MEMBER_COLLECTION).expect("static"),
        Key::new(user)?,
    ))
}
fn create_object(leader: &str) -> AppResult<ObjectId> {
    let digest = Sha256::digest(leader.as_bytes());
    Ok(ObjectId::new(
        Owner::System,
        Collection::new(CREATE_COLLECTION).expect("static"),
        Key::new(format!("{digest:x}"))?,
    ))
}
fn legacy_create_object(leader: &str, request_id: &str) -> AppResult<ObjectId> {
    let digest = Sha256::digest(format!("{leader}\0{request_id}").as_bytes());
    Ok(ObjectId::new(
        Owner::System,
        Collection::new(CREATE_COLLECTION).expect("static"),
        Key::new(format!("{digest:x}"))?,
    ))
}
fn legacy_object() -> ObjectId {
    ObjectId::new(
        Owner::System,
        Collection::new(PARTY_COLLECTION).expect("static"),
        Key::new(LEGACY_KEY).expect("static"),
    )
}
fn migration_object() -> ObjectId {
    ObjectId::new(
        Owner::System,
        Collection::new(MIGRATION_COLLECTION).expect("static"),
        Key::new(LEGACY_KEY).expect("static"),
    )
}
fn legacy_touch(fence: &LegacyFence) -> AppResult<AtomicBatchOperation> {
    Ok(AtomicBatchOperation::Write {
        accessor: Accessor::Runtime,
        request: WriteRequest::upsert(
            legacy_object(),
            fence.value.clone(),
            Permissions::runtime_only(),
        )
        .expecting(fence.expected.clone()),
        membership: None,
    })
}
fn write<T: Serialize>(
    id: ObjectId,
    value: &T,
    expected: Precondition,
) -> AppResult<AtomicBatchOperation> {
    Ok(AtomicBatchOperation::Write {
        accessor: Accessor::Runtime,
        request: WriteRequest::upsert(
            id,
            StorageValue::new(
                serde_json::to_value(value)
                    .map_err(|_| AppError::internal("could not serialize party state"))?,
            )?,
            Permissions::runtime_only(),
        )
        .expecting(expected),
        membership: None,
    })
}
fn delete(id: ObjectId, expected: Precondition) -> AtomicBatchOperation {
    AtomicBatchOperation::Delete {
        accessor: Accessor::Runtime,
        id,
        expected,
    }
}
fn decode<T: for<'a> Deserialize<'a>>(v: StorageValue) -> AppResult<T> {
    serde_json::from_value(v.into_json())
        .map_err(|_| AppError::internal("invalid persisted party state"))
}
fn encode_lease(l: &PartyOwnerLease) -> StoredLease {
    StoredLease {
        node_id: l.owner_node.as_str().to_owned(),
        generation: l.generation.get(),
        expires_at: l.expires_at.unix_millis(),
    }
}
fn decode_lease(id: &PartyId, l: StoredLease) -> AppResult<PartyOwnerLease> {
    Ok(PartyOwnerLease {
        party_id: id.clone(),
        owner_node: NodeId::new(l.node_id)?,
        generation: OwnershipGeneration::new(l.generation),
        expires_at: TimestampMillis::from_unix_millis(l.expires_at),
    })
}
fn ensure_fence(p: &StoredParty, l: &PartyOwnerLease, now: TimestampMillis) -> AppResult<()> {
    let Some(current) = p
        .lease
        .clone()
        .map(|x| decode_lease(&l.party_id, x))
        .transpose()?
    else {
        return Err(AppError::conflict("party owner lease is absent"));
    };
    if current.owner_node != l.owner_node
        || current.generation != l.generation
        || !current.is_current_at(now)
    {
        return Err(AppError::conflict("party owner fence is stale"));
    }
    Ok(())
}
fn snapshot(id: &PartyId, p: &StoredParty) -> PartySnapshot {
    PartySnapshot {
        party_id: id.clone(),
        leader_user_id: p.leader_user_id.clone(),
        members: p.members.iter().cloned().collect(),
        invitations: p.invitations.iter().cloned().collect(),
        revision: p.revision,
    }
}
fn replay_snapshot_for(p: &StoredParty) -> StoredReplaySnapshot {
    StoredReplaySnapshot {
        leader_user_id: p.leader_user_id.clone(),
        members: p.members.clone(),
        invitations: p.invitations.clone(),
        revision: p.revision,
    }
}
fn replay_snapshot(id: &PartyId, p: &StoredReplaySnapshot) -> PartySnapshot {
    PartySnapshot {
        party_id: id.clone(),
        leader_user_id: p.leader_user_id.clone(),
        members: p.members.iter().cloned().collect(),
        invitations: p.invitations.iter().cloned().collect(),
        revision: p.revision,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::unwrap_used)]
    use super::*;
    use crate::repository::InMemoryStorageRepository;
    fn now(n: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(n)
    }
    fn node(s: &str) -> NodeId {
        NodeId::new(s).unwrap()
    }
    fn id(s: &str) -> PartyId {
        PartyId::parse(s).unwrap()
    }
    #[tokio::test]
    async fn parties_are_isolated_objects() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let d = StoragePartyDirectory::new(store.clone());
        let (a, la) = d
            .create(id("one"), "alice", node("a"), now(100), now(1))
            .await
            .unwrap();
        let (_, lb) = d
            .create(id("two"), "bob", node("b"), now(100), now(1))
            .await
            .unwrap();
        d.invite(&la, "alice", "i", "carol", a.revision, now(2))
            .await
            .unwrap();
        assert_eq!(d.snapshot(&id("two")).await.unwrap().revision, 1);
        assert!(
            store
                .read(&Accessor::Runtime, &party_object(&id("one")))
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .read(&Accessor::Runtime, &party_object(&id("two")))
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(lb.generation.get(), 1)
    }
    #[tokio::test]
    async fn accept_updates_aggregate_and_projection_atomically() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let d = StoragePartyDirectory::new(store.clone());
        let (p, l) = d
            .create(id("atomic"), "alice", node("a"), now(100), now(1))
            .await
            .unwrap();
        let p = d
            .invite(&l, "alice", "i", "bob", p.revision, now(2))
            .await
            .unwrap();
        d.accept(&l, "bob", "a", p.revision, now(3)).await.unwrap();
        let m: MemberProjection = decode(
            store
                .read(&Accessor::Runtime, &member_object("bob").unwrap())
                .await
                .unwrap()
                .unwrap()
                .value,
        )
        .unwrap();
        assert_eq!(m.party_id, "atomic");
        assert_eq!(
            d.snapshot(&id("atomic")).await.unwrap().members,
            vec!["alice", "bob"]
        )
    }
    #[tokio::test]
    async fn concurrent_cross_party_invites_have_one_global_claim_winner() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let a = StoragePartyDirectory::new(store.clone());
        let b = StoragePartyDirectory::new(store);
        let (p1, l1) = a
            .create(id("left"), "alice", node("a"), now(100), now(1))
            .await
            .unwrap();
        let (p2, l2) = b
            .create(id("right"), "bob", node("b"), now(100), now(1))
            .await
            .unwrap();
        let x = a.invite(&l1, "alice", "invite-left", "carol", p1.revision, now(2));
        let y = b.invite(&l2, "bob", "invite-right", "carol", p2.revision, now(2));
        let (rx, ry) = tokio::join!(x, y);
        assert!(rx.is_ok() ^ ry.is_ok());
        let left = a.snapshot(&id("left")).await.unwrap();
        let right = b.snapshot(&id("right")).await.unwrap();
        assert_eq!(
            usize::from(left.invitations.contains(&"carol".to_owned()))
                + usize::from(right.invitations.contains(&"carol".to_owned())),
            1
        );
    }
    #[tokio::test]
    async fn invite_claim_prevents_concurrent_member_creation() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let a = StoragePartyDirectory::new(store.clone());
        let b = StoragePartyDirectory::new(store);
        let (p1, l1) = a
            .create(id("invite-party"), "alice", node("a"), now(100), now(1))
            .await
            .unwrap();
        let (p2, l2) = b
            .create(id("member-party"), "bob", node("b"), now(100), now(1))
            .await
            .unwrap();
        a.invite(&l1, "alice", "invite", "carol", p1.revision, now(2))
            .await
            .unwrap();
        // The existing invitation claim makes a competing invitation fail
        // deterministically before it could ever be accepted as a member.
        assert!(
            b.invite(&l2, "bob", "other-invite", "carol", p2.revision, now(2))
                .await
                .is_err()
        );
        let accepted = a.accept(&l1, "carol", "accept", 2, now(3)).await.unwrap();
        assert_eq!(accepted.members, vec!["alice", "carol"]);
        assert!(
            b.invite(&l2, "bob", "after-member", "carol", p2.revision, now(4))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn legacy_directory_migrates_on_owner_resolution_and_survives_restart() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let party = id("legacy");
        let p = StoredParty {
            leader_user_id: "alice".into(),
            members: BTreeSet::from(["alice".into()]),
            invitations: BTreeSet::new(),
            revision: 1,
            lease: Some(StoredLease {
                node_id: "a".into(),
                generation: 1,
                expires_at: 10,
            }),
            max_generation: 1,
            results: vec![],
            closed: false,
            ticket_freeze: None,
            max_admission_generation: 0,
            last_resync_generation: 0,
        };
        let state = LegacyState {
            parties: BTreeMap::from([("legacy".into(), p)]),
            membership: BTreeMap::from([("alice".into(), "legacy".into())]),
        };
        store
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(
                    legacy_object(),
                    StorageValue::new(serde_json::to_value(state).unwrap()).unwrap(),
                    Permissions::runtime_only(),
                ),
            )
            .await
            .unwrap();
        let d = StoragePartyDirectory::new(store.clone());
        let PartyOwnerResolution::Local(l) = d
            .acquire_or_resolve(&party, node("b"), now(30), now(11))
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(l.generation.get(), 2);
        let restarted = StoragePartyDirectory::new(store);
        assert_eq!(
            restarted.snapshot(&party).await.unwrap().leader_user_id,
            "alice"
        );
        assert!(restarted.member("alice").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn migration_projects_legacy_invitation_claim_and_accepts_it() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let party = id("legacy-invite");
        let state = LegacyState {
            parties: BTreeMap::from([(
                "legacy-invite".into(),
                StoredParty {
                    leader_user_id: "alice".into(),
                    members: BTreeSet::from(["alice".into()]),
                    invitations: BTreeSet::from(["carol".into()]),
                    revision: 1,
                    lease: Some(StoredLease {
                        node_id: "a".into(),
                        generation: 1,
                        expires_at: 100,
                    }),
                    max_generation: 1,
                    results: vec![],
                    closed: false,
                    ticket_freeze: None,
                    max_admission_generation: 0,
                    last_resync_generation: 0,
                },
            )]),
            membership: BTreeMap::from([("alice".into(), "legacy-invite".into())]),
        };
        store
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(
                    legacy_object(),
                    StorageValue::new(serde_json::to_value(state).unwrap()).unwrap(),
                    Permissions::runtime_only(),
                ),
            )
            .await
            .unwrap();
        let d = StoragePartyDirectory::new(store);
        let PartyOwnerResolution::Local(lease) = d
            .acquire_or_resolve(&party, node("a"), now(100), now(2))
            .await
            .unwrap()
        else {
            panic!()
        };
        let claim = d.member("carol").await.unwrap().unwrap().0;
        assert!(claim.invitation);
        let accepted = d
            .accept(&lease, "carol", "accept-legacy", 1, now(3))
            .await
            .unwrap();
        assert_eq!(accepted.members, vec!["alice", "carol"]);
    }

    #[tokio::test]
    async fn concurrent_new_invite_migrates_legacy_reservation_before_claiming() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let legacy = id("legacy-overlap");
        let existing = StoragePartyDirectory::new(store.clone());
        // This party predates the legacy compatibility object. Once that
        // object exists, its invitee must still win every concurrent new
        // invitation attempt.
        let (other, other_lease) = existing
            .create(id("other-overlap"), "bob", node("b"), now(100), now(1))
            .await
            .unwrap();
        let state = LegacyState {
            parties: BTreeMap::from([(
                "legacy-overlap".into(),
                StoredParty {
                    leader_user_id: "alice".into(),
                    members: BTreeSet::from(["alice".into()]),
                    invitations: BTreeSet::from(["carol".into()]),
                    revision: 1,
                    lease: Some(StoredLease {
                        node_id: "a".into(),
                        generation: 1,
                        expires_at: 100,
                    }),
                    max_generation: 1,
                    results: vec![],
                    closed: false,
                    ticket_freeze: None,
                    max_admission_generation: 0,
                    last_resync_generation: 0,
                },
            )]),
            membership: BTreeMap::from([("alice".into(), "legacy-overlap".into())]),
        };
        store
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(
                    legacy_object(),
                    StorageValue::new(serde_json::to_value(state).unwrap()).unwrap(),
                    Permissions::runtime_only(),
                ),
            )
            .await
            .unwrap();
        let a = StoragePartyDirectory::new(store.clone());
        let b = StoragePartyDirectory::new(store);
        let invite = a.invite(
            &other_lease,
            "bob",
            "other-invite",
            "carol",
            other.revision,
            now(2),
        );
        let resolve = b.acquire_or_resolve(&legacy, node("a"), now(100), now(2));
        let (invite, resolved) = tokio::join!(invite, resolve);
        assert!(invite.is_err());
        let PartyOwnerResolution::Local(lease) = resolved.unwrap() else {
            panic!()
        };
        // A retry after either race order also loses to the durable legacy
        // reservation, while the original invitation remains accept-able.
        assert!(
            a.invite(
                &other_lease,
                "bob",
                "other-invite-retry",
                "carol",
                other.revision,
                now(3),
            )
            .await
            .is_err()
        );
        let accepted = b
            .accept(&lease, "carol", "accept-legacy", 1, now(3))
            .await
            .unwrap();
        assert_eq!(accepted.members, vec!["alice", "carol"]);
    }

    #[tokio::test]
    async fn concurrent_new_create_migrates_legacy_invitee_before_leader_claiming() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let legacy = id("legacy-create-overlap");
        let state = LegacyState {
            parties: BTreeMap::from([(
                "legacy-create-overlap".into(),
                StoredParty {
                    leader_user_id: "alice".into(),
                    members: BTreeSet::from(["alice".into()]),
                    invitations: BTreeSet::from(["carol".into()]),
                    revision: 1,
                    lease: Some(StoredLease {
                        node_id: "a".into(),
                        generation: 1,
                        expires_at: 100,
                    }),
                    max_generation: 1,
                    results: vec![],
                    closed: false,
                    ticket_freeze: None,
                    max_admission_generation: 0,
                    last_resync_generation: 0,
                },
            )]),
            membership: BTreeMap::from([("alice".into(), "legacy-create-overlap".into())]),
        };
        store
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(
                    legacy_object(),
                    StorageValue::new(serde_json::to_value(state).unwrap()).unwrap(),
                    Permissions::runtime_only(),
                ),
            )
            .await
            .unwrap();
        let a = StoragePartyDirectory::new(store.clone());
        let b = StoragePartyDirectory::new(store);
        let create = a.create_with_request(
            id("new-carol-party"),
            "carol",
            Some("create-carol"),
            node("b"),
            now(100),
            now(2),
        );
        let resolve = b.acquire_or_resolve(&legacy, node("a"), now(100), now(2));
        let (created, resolved) = tokio::join!(create, resolve);
        assert!(created.is_err());
        let PartyOwnerResolution::Local(lease) = resolved.unwrap() else {
            panic!()
        };
        // Retrying after a simulated caller restart cannot claim the invitee.
        assert!(
            a.create_with_request(
                id("new-carol-party-retry"),
                "carol",
                Some("create-carol-retry"),
                node("b"),
                now(100),
                now(3),
            )
            .await
            .is_err()
        );
        let accepted = b
            .accept(&lease, "carol", "accept-legacy", 1, now(3))
            .await
            .unwrap();
        assert_eq!(accepted.members, vec!["alice", "carol"]);
    }

    #[tokio::test]
    async fn legacy_mutation_racing_migration_rejects_stale_snapshot_and_retry_converges() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let party = id("legacy-race");
        let original = LegacyState {
            parties: BTreeMap::from([(
                "legacy-race".into(),
                StoredParty {
                    leader_user_id: "alice".into(),
                    members: BTreeSet::from(["alice".into()]),
                    invitations: BTreeSet::new(),
                    revision: 1,
                    lease: Some(StoredLease {
                        node_id: "a".into(),
                        generation: 1,
                        expires_at: 100,
                    }),
                    max_generation: 1,
                    results: vec![],
                    closed: false,
                    ticket_freeze: None,
                    max_admission_generation: 0,
                    last_resync_generation: 0,
                },
            )]),
            membership: BTreeMap::from([("alice".into(), "legacy-race".into())]),
        };
        store
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(
                    legacy_object(),
                    StorageValue::new(serde_json::to_value(&original).unwrap()).unwrap(),
                    Permissions::runtime_only(),
                ),
            )
            .await
            .unwrap();
        let d = StoragePartyDirectory::new(store.clone());
        // Capture the migration source, then let an old-layout node accept an
        // invitation before the migration batch reaches its serialization
        // point. The stale batch must fail its legacy Match precondition.
        let stale = d.legacy_source().await.unwrap().unwrap();
        let mut updated = original.clone();
        let p = updated.parties.get_mut("legacy-race").unwrap();
        p.members.insert("bob".into());
        p.revision = 2;
        updated
            .membership
            .insert("bob".into(), "legacy-race".into());
        store
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(
                    legacy_object(),
                    StorageValue::new(serde_json::to_value(updated).unwrap()).unwrap(),
                    Permissions::runtime_only(),
                )
                .expecting(stale.fence.expected.clone()),
            )
            .await
            .unwrap();
        let stale_party = stale.state.parties["legacy-race"].clone();
        let rejected = store
            .atomic_batch(vec![
                write(
                    party_object(&party),
                    &stale_party,
                    Precondition::MustNotExist,
                )
                .unwrap(),
                legacy_touch(&stale.fence).unwrap(),
            ])
            .await;
        assert!(matches!(
            rejected.unwrap_err().category(),
            ErrorCategory::Conflict
        ));
        // A normal retry re-reads the changed source and preserves Bob's
        // accepted membership rather than overwriting it with the stale party.
        d.migrate(&party).await.unwrap();
        assert_eq!(
            d.snapshot(&party).await.unwrap().members,
            vec!["alice", "bob"]
        );
    }

    #[tokio::test]
    async fn completed_legacy_gate_uses_marker_without_deserializing_each_party() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let mut parties = BTreeMap::new();
        let mut membership = BTreeMap::new();
        for n in 0..32 {
            let user = format!("legacy-user-{n}");
            let party = format!("legacy-party-{n}");
            parties.insert(
                party.clone(),
                StoredParty {
                    leader_user_id: user.clone(),
                    members: BTreeSet::from([user.clone()]),
                    invitations: BTreeSet::new(),
                    revision: 1,
                    lease: None,
                    max_generation: 0,
                    results: vec![],
                    closed: false,
                    ticket_freeze: None,
                    max_admission_generation: 0,
                    last_resync_generation: 0,
                },
            );
            membership.insert(user, party);
        }
        store
            .write(
                &Accessor::Runtime,
                WriteRequest::upsert(
                    legacy_object(),
                    StorageValue::new(
                        serde_json::to_value(LegacyState {
                            parties,
                            membership,
                        })
                        .unwrap(),
                    )
                    .unwrap(),
                    Permissions::runtime_only(),
                ),
            )
            .await
            .unwrap();
        let d = StoragePartyDirectory::new(store.clone());
        // First use performs the finite scan and persists the version marker.
        d.create(id("new-after-scan-a"), "new-a", node("a"), now(100), now(1))
            .await
            .unwrap();
        assert!(d.migration_marker().await.unwrap().is_some());
        // The next create uses the marker plus one source CAS fence; it does
        // not call migrate() for the 32 legacy parties again.
        d.create(id("new-after-scan-b"), "new-b", node("a"), now(100), now(2))
            .await
            .unwrap();
        assert!(
            d.read_party(&id("new-after-scan-b"))
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn create_request_id_replays_original_snapshot_under_concurrency() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let a = StoragePartyDirectory::new(store.clone());
        let b = StoragePartyDirectory::new(store);
        let x = a.create_with_request(
            id("create-a"),
            "alice",
            Some("retry"),
            node("a"),
            now(100),
            now(1),
        );
        let y = b.create_with_request(
            id("create-b"),
            "alice",
            Some("retry"),
            node("b"),
            now(100),
            now(1),
        );
        let (x, y) = tokio::join!(x, y);
        let x = x.unwrap().0;
        let y = y.unwrap().0;
        assert_eq!(x, y);
        assert!(matches!(x.party_id.as_str(), "create-a" | "create-b"));
    }

    #[tokio::test]
    async fn create_replay_is_immutable_across_mutation_takeover_and_restart() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let first = StoragePartyDirectory::new(store.clone());
        let (created, old_lease) = first
            .create_with_request(
                id("create-immutable"),
                "alice",
                Some("create-1"),
                node("a"),
                now(10),
                now(1),
            )
            .await
            .unwrap();
        first
            .invite(
                &old_lease,
                "alice",
                "invite-bob",
                "bob",
                created.revision,
                now(2),
            )
            .await
            .unwrap();
        let restarted = StoragePartyDirectory::new(store);
        let PartyOwnerResolution::Local(new_lease) = restarted
            .acquire_or_resolve(&id("create-immutable"), node("b"), now(100), now(11))
            .await
            .unwrap()
        else {
            panic!("expired owner must be taken over")
        };
        let replay = restarted
            .create_with_request(
                id("ignored-on-retry"),
                "alice",
                Some("create-1"),
                node("b"),
                now(100),
                now(12),
            )
            .await
            .unwrap();
        assert_eq!(
            replay.0, created,
            "replay must not load the mutated aggregate"
        );
        assert_eq!(
            replay.1, old_lease,
            "replay returns the original response fence"
        );
        assert_ne!(replay.1, new_lease);
        let projection = restarted.create_projection("alice").await.unwrap().0;
        assert_eq!(projection.results.len(), 1);
        assert!(projection.party_id.is_none());
    }

    #[tokio::test]
    async fn replacement_owner_claims_one_resync_per_generation_after_expiry() {
        let store: Arc<dyn StorageRepository> = Arc::new(InMemoryStorageRepository::new());
        let initial = StoragePartyDirectory::new(Arc::clone(&store));
        let party_id = id("failover-resync");
        let (created, stale_lease) = initial
            .create(party_id.clone(), "alice", node("a"), now(10), now(1))
            .await
            .unwrap();

        // The replacement may acquire only after the durable lease expires;
        // the resulting fence is strictly newer than the old owner fence.
        let replacement = StoragePartyDirectory::new(Arc::clone(&store));
        let PartyOwnerResolution::Local(recovery_lease) = replacement
            .acquire_or_resolve(&party_id, node("b"), now(30), now(11))
            .await
            .unwrap()
        else {
            panic!("expired owner must be taken over")
        };
        assert!(recovery_lease.generation > stale_lease.generation);

        let recovered = replacement
            .claim_failover_resync(&recovery_lease, now(11))
            .await
            .unwrap()
            .expect("the first replacement generation emits a recovery snapshot");
        assert_eq!(recovered, created);
        assert!(
            replacement
                .claim_failover_resync(&recovery_lease, now(12))
                .await
                .unwrap()
                .is_none(),
            "a retry must not claim a second client transition"
        );
        let next_replacement = StoragePartyDirectory::new(Arc::clone(&store));
        let PartyOwnerResolution::Local(next_lease) = next_replacement
            .acquire_or_resolve(&party_id, node("c"), now(50), now(31))
            .await
            .unwrap()
        else {
            panic!("the replacement lease must also expire before another takeover")
        };
        assert!(next_lease.generation > recovery_lease.generation);
        assert!(
            replacement
                .claim_failover_resync(&recovery_lease, now(31))
                .await
                .is_err(),
            "a stale replacement owner must never claim a later recovery"
        );
        assert!(
            next_replacement
                .claim_failover_resync(&next_lease, now(31))
                .await
                .unwrap()
                .is_some(),
            "each successfully acquired replacement generation claims once"
        );

        // The durable marker survives restart, so replaying the completed
        // takeover cannot duplicate a client resync notification.
        let restarted = StoragePartyDirectory::new(store);
        assert!(
            restarted
                .claim_failover_resync(&next_lease, now(32))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn dedupe_replay_is_bounded_compact_and_survives_restart_and_concurrent_retry() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let first = StoragePartyDirectory::new(store.clone());
        let second = StoragePartyDirectory::new(store.clone());
        let (mut party, lease) = first
            .create(id("dedupe"), "alice", node("a"), now(10_000), now(1))
            .await
            .unwrap();
        party = first
            .invite(&lease, "alice", "invite-bob", "bob", party.revision, now(2))
            .await
            .unwrap();
        party = first
            .accept(&lease, "bob", "accept-bob", party.revision, now(3))
            .await
            .unwrap();

        let mut first_request = None;
        let mut latest = None;
        for number in 0..(MAX_DEDUPE + 4) {
            let actor = party.leader_user_id.clone();
            let target = if actor == "alice" { "bob" } else { "alice" };
            let request_id = format!("promote-{number}");
            let result = first
                .promote(
                    &lease,
                    &actor,
                    &request_id,
                    target,
                    party.revision,
                    now(10 + u64::try_from(number).unwrap()),
                )
                .await
                .unwrap();
            if first_request.is_none() {
                first_request = Some((actor.clone(), request_id.clone(), target.to_owned()));
            }
            latest = Some((actor, request_id, target.to_owned(), result.clone()));
            party = result;
        }
        let stored = first.read_party(&id("dedupe")).await.unwrap().unwrap().0;
        assert_eq!(stored.results.len(), MAX_DEDUPE);
        // `StoredReplaySnapshot` intentionally has no results field; serializing
        // this aggregate therefore cannot recursively retain previous logs.
        let encoded = serde_json::to_value(&stored).unwrap();
        assert_eq!(encoded.to_string().matches("\"results\"").count(), 1);

        let (actor, request_id, target, expected) = latest.unwrap();
        let replay = second
            .promote(&lease, &actor, &request_id, &target, 0, now(500))
            .await
            .unwrap();
        assert_eq!(replay, expected);
        let (old_actor, old_request_id, old_target) = first_request.unwrap();
        assert!(
            second
                .promote(
                    &lease,
                    &old_actor,
                    &old_request_id,
                    &old_target,
                    0,
                    now(501)
                )
                .await
                .is_err()
        );

        let expected_revision = party.revision;
        let x = first.promote(
            &lease,
            &party.leader_user_id,
            "concurrent-retry",
            if party.leader_user_id == "alice" {
                "bob"
            } else {
                "alice"
            },
            expected_revision,
            now(502),
        );
        let y = second.promote(
            &lease,
            &party.leader_user_id,
            "concurrent-retry",
            if party.leader_user_id == "alice" {
                "bob"
            } else {
                "alice"
            },
            expected_revision,
            now(502),
        );
        let (x, y) = tokio::join!(x, y);
        assert_eq!(x.unwrap(), y.unwrap());
        assert_eq!(
            first
                .read_party(&id("dedupe"))
                .await
                .unwrap()
                .unwrap()
                .0
                .results
                .len(),
            MAX_DEDUPE
        );
    }

    #[tokio::test]
    async fn expired_queue_freeze_recovers_after_owner_crash_without_weakening_fences() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let crashed_owner = StoragePartyDirectory::new(store.clone());
        let recovered_owner = StoragePartyDirectory::new(store);
        let (party, initial_lease) = crashed_owner
            .create(id("freeze-recovery"), "alice", node("a"), now(10), now(1))
            .await
            .unwrap();
        let (_, abandoned_freeze) = crashed_owner
            .queue_snapshot(&initial_lease, "alice", party.revision, now(60_002), now(2))
            .await
            .unwrap();
        assert!(
            crashed_owner
                .invite(
                    &initial_lease,
                    "alice",
                    "while-frozen",
                    "bob",
                    party.revision,
                    now(3)
                )
                .await
                .is_err()
        );

        let PartyOwnerResolution::Local(recovered_lease) = recovered_owner
            .acquire_or_resolve(&id("freeze-recovery"), node("b"), now(80_000), now(11))
            .await
            .unwrap()
        else {
            panic!("expired owner must be taken over")
        };
        assert!(
            recovered_owner
                .invite(
                    &initial_lease,
                    "alice",
                    "stale-owner",
                    "bob",
                    party.revision,
                    now(70_001)
                )
                .await
                .is_err()
        );
        assert!(
            recovered_owner
                .renew_queue_freeze(
                    &recovered_lease,
                    "alice",
                    &abandoned_freeze,
                    now(130_000),
                    now(70_001),
                )
                .await
                .is_err(),
            "an expired ticket admission cannot be resurrected by delayed renewal"
        );
        let recovered = recovered_owner
            .invite(
                &recovered_lease,
                "alice",
                "after-expiry",
                "bob",
                party.revision,
                now(70_001),
            )
            .await
            .unwrap();
        assert_eq!(recovered.invitations, vec!["bob"]);
        assert!(
            recovered_owner
                .queue_snapshot(
                    &recovered_lease,
                    "alice",
                    recovered.revision,
                    now(130_002),
                    now(70_002),
                )
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn delayed_old_freeze_release_cannot_aba_clear_new_admission_after_takeover() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let old = StoragePartyDirectory::new(store.clone());
        let current = StoragePartyDirectory::new(store);
        let (party, old_lease) = old
            .create(id("freeze-aba"), "alice", node("a"), now(10), now(1))
            .await
            .unwrap();
        let (_, old_freeze) = old
            .queue_snapshot(&old_lease, "alice", party.revision, now(60_002), now(2))
            .await
            .unwrap();
        let PartyOwnerResolution::Local(current_lease) = current
            .acquire_or_resolve(&id("freeze-aba"), node("b"), now(100_000), now(11))
            .await
            .unwrap()
        else {
            panic!("expired owner must be taken over")
        };
        let (_, new_freeze) = current
            .queue_snapshot(
                &current_lease,
                "alice",
                party.revision,
                now(130_001),
                now(70_001),
            )
            .await
            .unwrap();
        assert_ne!(old_freeze.admission_token, new_freeze.admission_token);
        assert!(
            old.release_queue_freeze(&old_lease, "alice", &old_freeze, now(70_002))
                .await
                .is_err()
        );
        assert!(
            current
                .release_queue_freeze(&current_lease, "alice", &old_freeze, now(70_002))
                .await
                .is_err()
        );
        assert!(
            current
                .queue_snapshot(
                    &current_lease,
                    "alice",
                    party.revision,
                    now(130_003),
                    now(70_003),
                )
                .await
                .is_err()
        );
        current
            .release_queue_freeze(&current_lease, "alice", &new_freeze, now(70_004))
            .await
            .unwrap();
        assert!(
            current
                .queue_snapshot(
                    &current_lease,
                    "alice",
                    party.revision,
                    now(130_005),
                    now(70_005),
                )
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn long_lived_ticket_freeze_survives_takeover_renewal_and_exact_cleanup() {
        let store = Arc::new(InMemoryStorageRepository::new());
        let old = StoragePartyDirectory::new(store.clone());
        let current = StoragePartyDirectory::new(store);
        let (party, old_lease) = old
            .create(
                id("long-ticket-freeze"),
                "alice",
                node("a"),
                now(10),
                now(1),
            )
            .await
            .unwrap();
        // The ticket lives for two minutes, exceeding the obsolete 60-second
        // recovery interval. A party owner takeover must not make it mutable.
        let (_, freeze) = old
            .queue_snapshot(&old_lease, "alice", party.revision, now(120_002), now(2))
            .await
            .unwrap();
        let PartyOwnerResolution::Local(current_lease) = current
            .acquire_or_resolve(&id("long-ticket-freeze"), node("b"), now(200_000), now(11))
            .await
            .unwrap()
        else {
            panic!("expired owner must be taken over")
        };
        assert!(
            old.renew_queue_freeze(&old_lease, "alice", &freeze, now(180_000), now(70_001))
                .await
                .is_err(),
            "stale owner cannot renew a live admission"
        );
        assert!(
            current
                .invite(
                    &current_lease,
                    "alice",
                    "live-ticket-mutation",
                    "bob",
                    party.revision,
                    now(70_001),
                )
                .await
                .is_err(),
            "a >60s live ticket keeps party membership frozen"
        );
        // The current owner may route the authoritative shard's exact-token
        // renewal after takeover; a stale/new admission cannot be targeted.
        current
            .renew_queue_freeze(&current_lease, "alice", &freeze, now(180_000), now(70_002))
            .await
            .unwrap();
        assert!(
            current
                .invite(
                    &current_lease,
                    "alice",
                    "renewed-live-ticket-mutation",
                    "bob",
                    party.revision,
                    now(130_000),
                )
                .await
                .is_err(),
            "renewal extends the live-ticket fence past the original expiry"
        );
        // Both cancellation and match formation use this exact-fenced release.
        current
            .release_queue_freeze(&current_lease, "alice", &freeze, now(130_001))
            .await
            .unwrap();
        let changed = current
            .invite(
                &current_lease,
                "alice",
                "after-cancel-or-match",
                "bob",
                party.revision,
                now(130_002),
            )
            .await
            .unwrap();
        assert_eq!(changed.invitations, vec!["bob"]);
    }
}
