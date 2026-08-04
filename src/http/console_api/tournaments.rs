//! Console tournament lifecycle, discovery, and immutable-result operations.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::repository::{
    CreateTournamentRequest, Tournament, TournamentEntry, TournamentResult, TournamentState,
};
use crate::services::{AuditEntry, ConsoleIdentity};
use crate::time::{Clock, SystemClock, TimestampMillis};

pub const TOURNAMENTS_PATH: &str = "/console/v1/tournaments";
pub const TOURNAMENT_PATH: &str = "/console/v1/tournaments/:id";
pub const TOURNAMENT_TRANSITION_PATH: &str = "/console/v1/tournaments/:id/transition";
pub const TOURNAMENT_ENTRIES_PATH: &str = "/console/v1/tournaments/:id/entries";
pub const TOURNAMENT_RESULTS_PATH: &str = "/console/v1/tournaments/:id/results";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateBody {
    pub id: String,
    pub leaderboard_id: String,
    pub registration_opens_at_unix_ms: u64,
    pub registration_closes_at_unix_ms: u64,
    pub starts_at_unix_ms: u64,
    pub ends_at_unix_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionBody {
    pub state: TournamentState,
}

#[derive(Debug, Serialize)]
pub struct TournamentRow {
    pub id: String,
    pub leaderboard_id: String,
    pub state: TournamentState,
    pub registration_opens_at_unix_ms: u64,
    pub registration_closes_at_unix_ms: u64,
    pub starts_at_unix_ms: u64,
    pub ends_at_unix_ms: u64,
    pub settled_epoch_due_at_unix_ms: Option<u64>,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

impl From<Tournament> for TournamentRow {
    fn from(value: Tournament) -> Self {
        Self {
            id: value.id,
            leaderboard_id: value.leaderboard_id,
            state: value.state,
            registration_opens_at_unix_ms: value.registration_opens_at.unix_millis(),
            registration_closes_at_unix_ms: value.registration_closes_at.unix_millis(),
            starts_at_unix_ms: value.starts_at.unix_millis(),
            ends_at_unix_ms: value.ends_at.unix_millis(),
            settled_epoch_due_at_unix_ms: value
                .settled_epoch
                .map(|epoch| epoch.due_at.unix_millis()),
            created_at_unix_ms: value.created_at.unix_millis(),
            updated_at_unix_ms: value.updated_at.unix_millis(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TournamentsResponse {
    pub items: Vec<TournamentRow>,
    pub total: usize,
}

#[derive(Debug, Serialize)]
pub struct EntriesResponse {
    pub items: Vec<TournamentEntry>,
    pub total: usize,
}

#[derive(Debug, Serialize)]
pub struct ResultsResponse {
    pub items: Vec<TournamentResult>,
    pub total: usize,
}

fn body_error(rejection: JsonRejection) -> ApiError {
    AppError::validation("invalid request body")
        .with_detail(rejection.body_text())
        .into()
}

async fn require_tournament(app: &App, id: &str) -> Result<(), ApiError> {
    if app
        .backend()
        .tournaments_repository()
        .get(id)
        .await?
        .is_some()
    {
        Ok(())
    } else {
        Err(AppError::not_found(format!("no such tournament '{id}'")).into())
    }
}

pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
) -> Result<Json<TournamentsResponse>, ApiError> {
    app.metrics().record_http_request();
    let items: Vec<_> = app
        .backend()
        .tournaments_repository()
        .list()
        .await?
        .into_iter()
        .map(TournamentRow::from)
        .collect();
    let total = items.len();
    Ok(Json(TournamentsResponse { items, total }))
}

pub(super) async fn create_handler(
    State(app): State<App>,
    operator: ConsoleIdentity,
    body: Result<Json<CreateBody>, JsonRejection>,
) -> Result<(StatusCode, Json<TournamentRow>), ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let body = body.map_err(body_error)?.0;
    let now = SystemClock.now();
    let tournament = app
        .backend()
        .tournaments_repository()
        .create(
            CreateTournamentRequest {
                id: body.id,
                leaderboard_id: body.leaderboard_id,
                registration_opens_at: TimestampMillis::from_unix_millis(
                    body.registration_opens_at_unix_ms,
                ),
                registration_closes_at: TimestampMillis::from_unix_millis(
                    body.registration_closes_at_unix_ms,
                ),
                starts_at: TimestampMillis::from_unix_millis(body.starts_at_unix_ms),
                ends_at: TimestampMillis::from_unix_millis(body.ends_at_unix_ms),
            },
            now,
        )
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.username,
        operator.role.as_str(),
        "tournaments.create",
        tournament.id.clone(),
        "created tournament",
    ));
    Ok((StatusCode::CREATED, Json(tournament.into())))
}

pub(super) async fn detail_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
    Path(id): Path<String>,
) -> Result<Json<TournamentRow>, ApiError> {
    app.metrics().record_http_request();
    let tournament = app
        .backend()
        .tournaments_repository()
        .get(&id)
        .await?
        .ok_or_else(|| AppError::not_found(format!("no such tournament '{id}'")))?;
    Ok(Json(tournament.into()))
}

pub(super) async fn transition_handler(
    State(app): State<App>,
    operator: ConsoleIdentity,
    Path(id): Path<String>,
    body: Result<Json<TransitionBody>, JsonRejection>,
) -> Result<Json<TournamentRow>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let state = body.map_err(body_error)?.0.state;
    if matches!(
        state,
        TournamentState::Finalizing | TournamentState::Completed
    ) {
        return Err(AppError::conflict("tournament settlement is scheduler-owned").into());
    }
    let now = SystemClock.now();
    let tournament = app
        .backend()
        .tournaments_repository()
        .transition(&id, state, now)
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.username,
        operator.role.as_str(),
        "tournaments.transition",
        id,
        format!("state={}", state.as_str()),
    ));
    Ok(Json(tournament.into()))
}

pub(super) async fn entries_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
    Path(id): Path<String>,
) -> Result<Json<EntriesResponse>, ApiError> {
    app.metrics().record_http_request();
    require_tournament(&app, &id).await?;
    let items = app.backend().tournaments_repository().entries(&id).await?;
    let total = items.len();
    Ok(Json(EntriesResponse { items, total }))
}

pub(super) async fn results_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
    Path(id): Path<String>,
) -> Result<Json<ResultsResponse>, ApiError> {
    app.metrics().record_http_request();
    require_tournament(&app, &id).await?;
    let items = app.backend().tournaments_repository().results(&id).await?;
    let total = items.len();
    Ok(Json(ResultsResponse { items, total }))
}
