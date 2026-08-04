//! Tournament lifecycle repository contract.
//!
//! A tournament is bound to one leaderboard and settles only from the immutable
//! reset epoch for that board. The epoch identity is persisted with the completed
//! tournament, making a scheduler retry idempotent and preventing results from a
//! later/foreign reset being attached to the tournament.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::leaderboard_scheduler::ResetEpoch;
use crate::time::TimestampMillis;

/// The authoritative lifecycle of a tournament.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TournamentState {
    /// Configured but unavailable to players.
    Draft,
    /// Players can register until the configured close instant.
    RegistrationOpen,
    /// Scores are accumulating on the bound leaderboard.
    Running,
    /// Internal transient state reserved for durable settlement.
    Finalizing,
    /// Settlement snapshot is committed and immutable.
    Completed,
    /// Operator cancelled the tournament before settlement.
    Cancelled,
}

impl TournamentState {
    /// Stable persistence token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::RegistrationOpen => "registration_open",
            Self::Running => "running",
            Self::Finalizing => "finalizing",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Parse a durable state token.
    pub fn from_token(token: &str) -> AppResult<Self> {
        match token {
            "draft" => Ok(Self::Draft),
            "registration_open" => Ok(Self::RegistrationOpen),
            "running" => Ok(Self::Running),
            "finalizing" => Ok(Self::Finalizing),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(AppError::internal("unknown tournament state token")),
        }
    }
}

/// Validate one legal lifecycle edge.
pub fn can_transition(from: TournamentState, to: TournamentState) -> bool {
    matches!(
        (from, to),
        (TournamentState::Draft, TournamentState::RegistrationOpen)
            | (TournamentState::Draft, TournamentState::Cancelled)
            | (TournamentState::RegistrationOpen, TournamentState::Running)
            | (
                TournamentState::RegistrationOpen,
                TournamentState::Cancelled
            )
            | (TournamentState::Running, TournamentState::Finalizing)
            | (TournamentState::Running, TournamentState::Cancelled)
            | (TournamentState::Finalizing, TournamentState::Completed)
    )
}

/// Immutable tournament configuration and current lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Tournament {
    /// Operator-chosen tournament identifier.
    pub id: String,
    /// The leaderboard whose reset epoch settles this tournament.
    pub leaderboard_id: String,
    /// Current lifecycle state.
    pub state: TournamentState,
    /// First instant registration is permitted.
    pub registration_opens_at: TimestampMillis,
    /// First instant registration is no longer permitted.
    pub registration_closes_at: TimestampMillis,
    /// First instant the tournament may run.
    pub starts_at: TimestampMillis,
    /// First instant the tournament may settle.
    pub ends_at: TimestampMillis,
    /// Epoch used for completed settlement.
    pub settled_epoch: Option<ResetEpoch>,
    /// Creation instant.
    pub created_at: TimestampMillis,
    /// Last lifecycle mutation instant.
    pub updated_at: TimestampMillis,
}

/// Parameters for creating a tournament.
#[derive(Debug, Clone)]
pub struct CreateTournamentRequest {
    pub id: String,
    pub leaderboard_id: String,
    pub registration_opens_at: TimestampMillis,
    pub registration_closes_at: TimestampMillis,
    pub starts_at: TimestampMillis,
    pub ends_at: TimestampMillis,
}

/// A player registered to a tournament.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TournamentEntry {
    pub tournament_id: String,
    pub user_id: String,
    pub registered_at: TimestampMillis,
}

/// Immutable ranking copied from the scheduler's pre-reset snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TournamentResult {
    pub tournament_id: String,
    pub user_id: String,
    pub rank: u64,
    pub score: i64,
    pub subscore: i64,
}

/// A durable post-settlement work item.
///
/// Delivery is at-least-once: a process can fail after an external reward side
/// effect and before acknowledgement. Consumers must store [`Self::idempotency_key`]
/// with their own side effect; Citadel deliberately does not claim exactly-once.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TournamentSettlementOutboxRecord {
    /// Tournament whose immutable results were committed.
    pub tournament_id: String,
    /// Reset epoch that deterministically identifies this settlement.
    pub epoch: ResetEpoch,
}

impl TournamentSettlementOutboxRecord {
    /// Stable idempotency key for reward processors and callbacks.
    #[must_use]
    pub fn idempotency_key(&self) -> String {
        format!("{}:{}", self.tournament_id, self.epoch.due_at.unix_millis())
    }
}

/// Persistence boundary for lifecycle, entries, and reset-bound settlement.
#[async_trait]
pub trait TournamentsRepository: Send + Sync {
    async fn create(
        &self,
        request: CreateTournamentRequest,
        now: TimestampMillis,
    ) -> AppResult<Tournament>;
    async fn get(&self, id: &str) -> AppResult<Option<Tournament>>;
    /// List all tournaments in deterministic player-discovery order.
    async fn list(&self) -> AppResult<Vec<Tournament>>;
    async fn transition(
        &self,
        id: &str,
        to: TournamentState,
        now: TimestampMillis,
    ) -> AppResult<Tournament>;
    async fn register(
        &self,
        tournament_id: &str,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<TournamentEntry>;
    async fn entries(&self, tournament_id: &str) -> AppResult<Vec<TournamentEntry>>;
    async fn results(&self, tournament_id: &str) -> AppResult<Vec<TournamentResult>>;

    /// Atomically bind a running tournament to a matching reset epoch and mark it
    /// completed. Replaying the exact committed epoch returns `false`; a foreign
    /// epoch or an incomplete lifecycle is rejected without mutation.
    async fn settle_from_epoch(
        &self,
        id: &str,
        epoch: ResetEpoch,
        now: TimestampMillis,
    ) -> AppResult<bool>;

    /// Read committed post-settlement requests. Backends that cannot stage this
    /// record atomically with settlement must reject it rather than faking a
    /// callback delivery guarantee.
    async fn pending_settlement_outbox(
        &self,
        limit: usize,
    ) -> AppResult<Vec<TournamentSettlementOutboxRecord>> {
        let _ = limit;
        Err(AppError::internal(
            "durable tournament settlement outbox is not supported by this backend",
        ))
    }

    /// Idempotently acknowledge one successful post-settlement callback.
    async fn acknowledge_settlement_outbox(
        &self,
        tournament_id: &str,
        epoch: &ResetEpoch,
    ) -> AppResult<()> {
        let _ = (tournament_id, epoch);
        Err(AppError::internal(
            "durable tournament settlement outbox is not supported by this backend",
        ))
    }
}

/// Consumer for post-settlement callbacks and reward processors.
#[async_trait]
pub trait TournamentSettlementCallback: Send + Sync {
    /// Process one immutable settlement. Reward side effects must deduplicate on
    /// [`TournamentSettlementOutboxRecord::idempotency_key`].
    async fn on_tournament_settled(
        &self,
        settlement: &TournamentSettlementOutboxRecord,
    ) -> AppResult<()>;
}

/// Bounded at-least-once dispatcher for settlement rewards and callbacks.
#[derive(Clone)]
pub struct TournamentSettlementOutboxDispatcher {
    repository: Arc<dyn TournamentsRepository>,
    callback: Arc<dyn TournamentSettlementCallback>,
}

impl TournamentSettlementOutboxDispatcher {
    /// Create a dispatcher over one repository and callback consumer.
    #[must_use]
    pub fn new<R, C>(repository: Arc<R>, callback: Arc<C>) -> Self
    where
        R: TournamentsRepository + 'static,
        C: TournamentSettlementCallback + 'static,
    {
        Self {
            repository,
            callback,
        }
    }

    /// Deliver up to `limit` records and retain failures for retry.
    pub async fn dispatch_pending(&self, limit: usize) -> AppResult<usize> {
        let records = self.repository.pending_settlement_outbox(limit).await?;
        let mut delivered = 0;
        for record in records {
            match self.callback.on_tournament_settled(&record).await {
                Ok(()) => {
                    self.repository
                        .acknowledge_settlement_outbox(&record.tournament_id, &record.epoch)
                        .await?;
                    delivered += 1;
                }
                Err(error) => tracing::warn!(
                    tournament_id = %record.tournament_id,
                    due_at_unix_ms = record.epoch.due_at.unix_millis(),
                    idempotency_key = %record.idempotency_key(),
                    error = %error,
                    "tournament settlement callback failed; retaining outbox record for retry"
                ),
            }
        }
        Ok(delivered)
    }
}

fn not_found(id: &str) -> AppError {
    AppError::not_found(format!("no such tournament '{id}'"))
}

pub(crate) fn validate_schedule(request: &CreateTournamentRequest) -> AppResult<()> {
    if request.id.is_empty() || request.leaderboard_id.is_empty() {
        return Err(AppError::validation(
            "tournament and leaderboard ids must not be empty",
        ));
    }
    if !(request.registration_opens_at <= request.registration_closes_at
        && request.registration_closes_at <= request.starts_at
        && request.starts_at <= request.ends_at)
    {
        return Err(AppError::validation("tournament schedule must be ordered"));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct State {
    tournaments: BTreeMap<String, Tournament>,
    entries: BTreeMap<String, BTreeSet<String>>,
    settlement_outbox: BTreeMap<(String, ResetEpoch), TournamentSettlementOutboxRecord>,
}

/// Single-process reference implementation of the complete lifecycle contract.
#[derive(Default)]
pub struct InMemoryTournamentsRepository {
    state: Mutex<State>,
}

impl std::fmt::Debug for InMemoryTournamentsRepository {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InMemoryTournamentsRepository")
            .finish_non_exhaustive()
    }
}

impl InMemoryTournamentsRepository {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| AppError::internal("tournaments repository mutex poisoned"))
    }
}

#[async_trait]
impl TournamentsRepository for InMemoryTournamentsRepository {
    async fn create(
        &self,
        request: CreateTournamentRequest,
        now: TimestampMillis,
    ) -> AppResult<Tournament> {
        validate_schedule(&request)?;
        let mut state = self.guard()?;
        if state.tournaments.contains_key(&request.id) {
            return Err(AppError::conflict("tournament already exists"));
        }
        let tournament = Tournament {
            id: request.id.clone(),
            leaderboard_id: request.leaderboard_id,
            state: TournamentState::Draft,
            registration_opens_at: request.registration_opens_at,
            registration_closes_at: request.registration_closes_at,
            starts_at: request.starts_at,
            ends_at: request.ends_at,
            settled_epoch: None,
            created_at: now,
            updated_at: now,
        };
        state.tournaments.insert(request.id, tournament.clone());
        Ok(tournament)
    }

    async fn get(&self, id: &str) -> AppResult<Option<Tournament>> {
        Ok(self.guard()?.tournaments.get(id).cloned())
    }

    async fn list(&self) -> AppResult<Vec<Tournament>> {
        let mut tournaments = self
            .guard()?
            .tournaments
            .values()
            .cloned()
            .collect::<Vec<_>>();
        tournaments.sort_by(|left, right| {
            left.starts_at
                .cmp(&right.starts_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(tournaments)
    }

    async fn transition(
        &self,
        id: &str,
        to: TournamentState,
        now: TimestampMillis,
    ) -> AppResult<Tournament> {
        let mut state = self.guard()?;
        let tournament = state.tournaments.get_mut(id).ok_or_else(|| not_found(id))?;
        if !can_transition(tournament.state, to) {
            return Err(AppError::conflict("illegal tournament state transition"));
        }
        tournament.state = to;
        tournament.updated_at = now;
        Ok(tournament.clone())
    }

    async fn register(
        &self,
        tournament_id: &str,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<TournamentEntry> {
        let mut state = self.guard()?;
        let tournament = state
            .tournaments
            .get(tournament_id)
            .ok_or_else(|| not_found(tournament_id))?;
        if tournament.state != TournamentState::RegistrationOpen
            || now < tournament.registration_opens_at
            || now >= tournament.registration_closes_at
        {
            return Err(AppError::conflict("tournament registration is closed"));
        }
        if !state
            .entries
            .entry(tournament_id.to_owned())
            .or_default()
            .insert(user_id.to_owned())
        {
            return Err(AppError::conflict("tournament entry already exists"));
        }
        Ok(TournamentEntry {
            tournament_id: tournament_id.to_owned(),
            user_id: user_id.to_owned(),
            registered_at: now,
        })
    }

    async fn entries(&self, tournament_id: &str) -> AppResult<Vec<TournamentEntry>> {
        if !self.guard()?.tournaments.contains_key(tournament_id) {
            return Err(not_found(tournament_id));
        }
        let users = self
            .guard()?
            .entries
            .get(tournament_id)
            .cloned()
            .unwrap_or_default();
        Ok(users
            .into_iter()
            .map(|user_id| TournamentEntry {
                tournament_id: tournament_id.to_owned(),
                user_id,
                registered_at: TimestampMillis::from_unix_millis(0),
            })
            .collect())
    }

    async fn results(&self, tournament_id: &str) -> AppResult<Vec<TournamentResult>> {
        if !self.guard()?.tournaments.contains_key(tournament_id) {
            return Err(not_found(tournament_id));
        }
        Ok(Vec::new())
    }

    async fn settle_from_epoch(
        &self,
        id: &str,
        epoch: ResetEpoch,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let mut state = self.guard()?;
        let tournament = state.tournaments.get_mut(id).ok_or_else(|| not_found(id))?;
        if tournament.state == TournamentState::Completed
            && tournament.settled_epoch.as_ref() == Some(&epoch)
        {
            return Ok(false);
        }
        if tournament.state != TournamentState::Running
            || tournament.leaderboard_id != epoch.leaderboard_id
        {
            return Err(AppError::conflict(
                "tournament cannot settle from this reset epoch",
            ));
        }
        tournament.state = TournamentState::Completed;
        tournament.settled_epoch = Some(epoch.clone());
        tournament.updated_at = now;
        state.settlement_outbox.insert(
            (id.to_owned(), epoch.clone()),
            TournamentSettlementOutboxRecord {
                tournament_id: id.to_owned(),
                epoch,
            },
        );
        Ok(true)
    }

    async fn pending_settlement_outbox(
        &self,
        limit: usize,
    ) -> AppResult<Vec<TournamentSettlementOutboxRecord>> {
        Ok(self
            .guard()?
            .settlement_outbox
            .values()
            .take(limit)
            .cloned()
            .collect())
    }

    async fn acknowledge_settlement_outbox(
        &self,
        tournament_id: &str,
        epoch: &ResetEpoch,
    ) -> AppResult<()> {
        self.guard()?
            .settlement_outbox
            .remove(&(tournament_id.to_owned(), epoch.clone()));
        Ok(())
    }
}
