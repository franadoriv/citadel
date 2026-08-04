//! Player-facing tournament discovery service.

use std::sync::Arc;

use crate::error::{AppError, AppResult};
use crate::repository::{Tournament, TournamentResult, TournamentState, TournamentsRepository};
use crate::time::TimestampMillis;

/// The registration status visible to one player.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TournamentRegistrationState {
    /// The player is already entered.
    Registered,
    /// The tournament is currently accepting entries from this player.
    Open,
    /// The player cannot register at this time.
    Closed,
}

/// Read-only player view over the selected tournament repository.
#[derive(Clone)]
pub struct TournamentDiscoveryService {
    repo: Arc<dyn TournamentsRepository>,
}

impl TournamentDiscoveryService {
    /// Create the player-facing discovery service.
    #[must_use]
    pub fn new(repo: Arc<dyn TournamentsRepository>) -> Self {
        Self { repo }
    }

    /// List currently active tournaments and scheduled tournaments whose
    /// registration is open, ordered by their start time.
    pub async fn list_active_and_upcoming(&self) -> AppResult<Vec<Tournament>> {
        Ok(self
            .repo
            .list()
            .await?
            .into_iter()
            .filter(|tournament| {
                matches!(
                    tournament.state,
                    TournamentState::RegistrationOpen | TournamentState::Running
                )
            })
            .collect())
    }

    /// Read one player-visible tournament's immutable configuration and state.
    pub async fn get(&self, id: &str) -> AppResult<Tournament> {
        self.repo
            .get(id)
            .await?
            .ok_or_else(|| AppError::not_found(format!("no such tournament '{id}'")))
    }

    /// Read immutable settlement results. Before settlement this is empty.
    pub async fn results(&self, id: &str) -> AppResult<Vec<TournamentResult>> {
        self.get(id).await?;
        self.repo.results(id).await
    }

    /// Resolve the caller's current registration state without exposing other
    /// players' entries.
    pub async fn registration_state(
        &self,
        id: &str,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<TournamentRegistrationState> {
        let tournament = self.get(id).await?;
        if self
            .repo
            .entries(id)
            .await?
            .iter()
            .any(|entry| entry.user_id == user_id)
        {
            return Ok(TournamentRegistrationState::Registered);
        }
        if tournament.state == TournamentState::RegistrationOpen
            && now >= tournament.registration_opens_at
            && now < tournament.registration_closes_at
        {
            Ok(TournamentRegistrationState::Open)
        } else {
            Ok(TournamentRegistrationState::Closed)
        }
    }
}
