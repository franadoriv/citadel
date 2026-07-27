//! Console Leaderboards section.
//!
//! Operator administration over the in-process
//! [`LeaderboardService`](crate::services::LeaderboardService): create/list/
//! delete leaderboard definitions, and submit/list/delete records. The
//! console is also the record producer here (there is no player-facing
//! leaderboard API yet), so ranking is exercisable end-to-end from the
//! console alone.
//!
//! - `GET /console/v1/leaderboards` — every board with its record count.
//! - `POST /console/v1/leaderboards` — create a board (admin, audited).
//! - `DELETE /console/v1/leaderboards/{id}` — delete a board and its records
//!   (admin, audited).
//! - `POST /console/v1/leaderboards/{id}/records` — submit a score (admin,
//!   audited).
//! - `GET /console/v1/leaderboards/{id}/records?limit&offset` — a ranked page.
//! - `DELETE /console/v1/leaderboards/{id}/records/{user_id}` — delete one
//!   record (admin, audited).
//!
//! Leaderboards are persisted behind the repository seam, so boards
//! and records survive a node restart on the Postgres and SQLite backends (the
//! in-memory backend stays non-durable by design). `reset_schedule` is still
//! stored verbatim but never executed; that limitation is recorded in
//! `docs/architecture/technical-debt.md`.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::services::{
    AuditEntry, ConsoleIdentity, CreateLeaderboardRequest, LeaderboardSummary, Operator,
    RankedRecord, SortOrder,
};
use crate::time::{Clock, SystemClock};

/// The Leaderboards section route (list/create).
pub const LEADERBOARDS_PATH: &str = "/console/v1/leaderboards";

/// Single-board route pattern (delete).
pub const LEADERBOARD_PATH: &str = "/console/v1/leaderboards/:id";

/// A board's records route pattern (submit/list).
pub const LEADERBOARD_RECORDS_PATH: &str = "/console/v1/leaderboards/:id/records";

/// A single record route pattern (delete).
pub const LEADERBOARD_RECORD_PATH: &str = "/console/v1/leaderboards/:id/records/:user_id";

/// Default records-page size.
const DEFAULT_LIMIT: usize = 50;
/// Hard ceiling on one records page.
const MAX_LIMIT: usize = 500;

/// One leaderboard row in [`LeaderboardsResponse`].
#[derive(Debug, Clone, Serialize)]
pub struct LeaderboardRow {
    /// The leaderboard's id.
    pub id: String,
    /// Which score value ranks best.
    pub sort: SortOrder,
    /// How a new submission combines with the existing record.
    pub operator: Operator,
    /// Free-form reset schedule string, stored but not executed.
    pub reset_schedule: Option<String>,
    /// Current number of records on the board.
    pub records: usize,
}

impl From<LeaderboardSummary> for LeaderboardRow {
    fn from(summary: LeaderboardSummary) -> Self {
        Self {
            id: summary.definition.id,
            sort: summary.definition.sort,
            operator: summary.definition.operator,
            reset_schedule: summary.definition.reset_schedule,
            records: summary.records,
        }
    }
}

/// The JSON response for `GET /console/v1/leaderboards`.
#[derive(Debug, Clone, Serialize)]
pub struct LeaderboardsResponse {
    /// Every leaderboard, id-ordered.
    pub items: Vec<LeaderboardRow>,
    /// Total leaderboards (equal to `items.len`; there is no pagination
    /// over boards).
    pub total: usize,
}

fn default_sort() -> SortOrder {
    SortOrder::Desc
}

fn default_operator() -> Operator {
    Operator::Best
}

/// The JSON body accepted by `POST /console/v1/leaderboards`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateBody {
    /// Unique, operator-chosen identifier.
    pub id: String,
    /// Which score value ranks best (default `desc`).
    #[serde(default = "default_sort")]
    pub sort: SortOrder,
    /// How a new submission combines with the existing record (default
    /// `best`).
    #[serde(default = "default_operator")]
    pub operator: Operator,
    /// Free-form reset schedule string, stored but not executed.
    #[serde(default)]
    pub reset_schedule: Option<String>,
}

/// The JSON body accepted by `POST /console/v1/leaderboards/{id}/records`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitBody {
    /// The submitting user's id.
    pub user_id: String,
    /// The primary score.
    pub score: i64,
    /// The secondary score (default `0`).
    #[serde(default)]
    pub subscore: i64,
    /// Optional JSON object attached to the record.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

/// Query parameters for the records listing route.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordsQuery {
    /// Page size (default 50, capped at 500).
    pub limit: Option<usize>,
    /// Rank offset (`0` starts at rank `1`).
    pub offset: Option<usize>,
}

/// One ranked record row.
#[derive(Debug, Clone, Serialize)]
pub struct RecordRow {
    /// `1`-based rank; `1` is the best record on the board.
    pub rank: u64,
    /// The record's user id.
    pub user_id: String,
    /// The primary score.
    pub score: i64,
    /// The secondary score.
    pub subscore: i64,
    /// Optional JSON object attached to the record.
    pub metadata: Option<serde_json::Value>,
    /// When this record last changed (unix milliseconds).
    pub updated_at_unix_ms: u64,
    /// How many times this user has submitted to this board.
    pub submissions: u32,
}

impl From<RankedRecord> for RecordRow {
    fn from(record: RankedRecord) -> Self {
        Self {
            rank: record.rank,
            user_id: record.user_id,
            score: record.score,
            subscore: record.subscore,
            metadata: record.metadata,
            updated_at_unix_ms: record.updated_at.unix_millis(),
            submissions: record.submissions,
        }
    }
}

/// The JSON response for `POST /console/v1/leaderboards/{id}/records`.
///
/// A submission does not compute rank (that would require re-ranking the
/// whole board on every write); callers that need the resulting rank read it
/// back with the records listing route.
#[derive(Debug, Clone, Serialize)]
pub struct SubmitResponse {
    /// The record's user id.
    pub user_id: String,
    /// The primary score after applying the board's operator.
    pub score: i64,
    /// The secondary score after applying the board's operator.
    pub subscore: i64,
    /// Optional JSON object attached to the record.
    pub metadata: Option<serde_json::Value>,
    /// When this record last changed (unix milliseconds).
    pub updated_at_unix_ms: u64,
    /// How many times this user has submitted to this board.
    pub submissions: u32,
}

/// The JSON response for the records listing route.
#[derive(Debug, Clone, Serialize)]
pub struct RecordsResponse {
    /// The leaderboard id these records belong to.
    pub board: String,
    /// Ranked records starting at the requested offset.
    pub items: Vec<RecordRow>,
    /// Total records on the board (unaffected by `limit`/`offset`).
    pub total: usize,
}

/// `GET /console/v1/leaderboards`: every board with its record count.
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
) -> Result<Json<LeaderboardsResponse>, ApiError> {
    app.metrics().record_http_request();
    let items: Vec<LeaderboardRow> = app
        .leaderboards()
        .list()
        .await?
        .into_iter()
        .map(LeaderboardRow::from)
        .collect();
    let total = items.len();
    Ok(Json(LeaderboardsResponse { items, total }))
}

/// `POST /console/v1/leaderboards`: create a board (admin).
pub(super) async fn create_handler(
    State(app): State<App>,
    operator: ConsoleIdentity,
    body: Result<Json<CreateBody>, JsonRejection>,
) -> Result<(StatusCode, Json<LeaderboardRow>), ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let body = match body {
        Ok(Json(body)) => body,
        Err(rejection) => {
            return Err(AppError::validation("invalid request body")
                .with_detail(rejection.body_text())
                .into());
        }
    };
    let now = SystemClock.now();
    let definition = app
        .leaderboards()
        .create(
            CreateLeaderboardRequest {
                id: body.id,
                sort: body.sort,
                operator: body.operator,
                reset_schedule: body.reset_schedule,
            },
            now,
        )
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.username,
        operator.role.as_str(),
        "leaderboards.create",
        definition.id.clone(),
        format!(
            "sort={} operator={}",
            definition.sort.as_str(),
            definition.operator.as_str()
        ),
    ));
    Ok((
        StatusCode::CREATED,
        Json(LeaderboardRow {
            id: definition.id,
            sort: definition.sort,
            operator: definition.operator,
            reset_schedule: definition.reset_schedule,
            records: 0,
        }),
    ))
}

/// `DELETE /console/v1/leaderboards/{id}`: delete a board (admin).
pub(super) async fn delete_handler(
    State(app): State<App>,
    operator: ConsoleIdentity,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    app.leaderboards().delete(&id).await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.username,
        operator.role.as_str(),
        "leaderboards.delete",
        id,
        "deleted leaderboard",
    ));
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /console/v1/leaderboards/{id}/records`: submit a score (admin).
pub(super) async fn submit_handler(
    State(app): State<App>,
    operator: ConsoleIdentity,
    Path(id): Path<String>,
    body: Result<Json<SubmitBody>, JsonRejection>,
) -> Result<Json<SubmitResponse>, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    let body = match body {
        Ok(Json(body)) => body,
        Err(rejection) => {
            return Err(AppError::validation("invalid request body")
                .with_detail(rejection.body_text())
                .into());
        }
    };
    let now = SystemClock.now();
    let record = app
        .leaderboards()
        .submit(
            &id,
            &body.user_id,
            body.score,
            body.subscore,
            body.metadata,
            now,
        )
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.username,
        operator.role.as_str(),
        "leaderboards.record.submit",
        format!("{id}/{}", record.user_id),
        format!("score={} subscore={}", record.score, record.subscore),
    ));
    Ok(Json(SubmitResponse {
        user_id: record.user_id,
        score: record.score,
        subscore: record.subscore,
        metadata: record.metadata,
        updated_at_unix_ms: record.updated_at.unix_millis(),
        submissions: record.submissions,
    }))
}

/// `GET /console/v1/leaderboards/{id}/records`: a ranked page.
pub(super) async fn records_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
    Path(id): Path<String>,
    Query(query): Query<RecordsQuery>,
) -> Result<Json<RecordsResponse>, ApiError> {
    app.metrics().record_http_request();
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = query.offset.unwrap_or(0);
    let page = app.leaderboards().records(&id, limit, offset).await?;
    Ok(Json(RecordsResponse {
        board: id,
        items: page.items.into_iter().map(RecordRow::from).collect(),
        total: page.total,
    }))
}

/// `DELETE /console/v1/leaderboards/{id}/records/{user_id}`: delete one record
/// (admin).
pub(super) async fn delete_record_handler(
    State(app): State<App>,
    operator: ConsoleIdentity,
    Path((id, user_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    app.metrics().record_http_request();
    operator.require_admin()?;
    app.leaderboards().delete_record(&id, &user_id).await?;
    app.audit_log().record(AuditEntry::new(
        SystemClock.now(),
        operator.username,
        operator.role.as_str(),
        "leaderboards.record.delete",
        format!("{id}/{user_id}"),
        "deleted record",
    ));
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_body_defaults_sort_desc_and_operator_best() {
        let body: CreateBody = serde_json::from_str(r#"{"id":"race"}"#).expect("parse");
        assert_eq!(body.sort, SortOrder::Desc);
        assert_eq!(body.operator, Operator::Best);
        assert!(body.reset_schedule.is_none());
        // Unknown fields are rejected at the boundary.
        assert!(serde_json::from_str::<CreateBody>(r#"{"id":"x","extra":1}"#).is_err());
    }

    #[test]
    fn submit_body_defaults_subscore_and_metadata() {
        let body: SubmitBody =
            serde_json::from_str(r#"{"user_id":"u1","score":10}"#).expect("parse");
        assert_eq!(body.subscore, 0);
        assert!(body.metadata.is_none());
        assert!(
            serde_json::from_str::<SubmitBody>(r#"{"user_id":"u1","score":1,"extra":1}"#).is_err()
        );
    }

    #[test]
    fn leaderboards_paths_are_registered_and_nested() {
        assert!(super::super::SECTION_PATHS.contains(&LEADERBOARDS_PATH));
        assert!(LEADERBOARD_PATH.starts_with(LEADERBOARDS_PATH));
        assert!(LEADERBOARD_RECORDS_PATH.starts_with(LEADERBOARDS_PATH));
        assert!(LEADERBOARD_RECORD_PATH.starts_with(LEADERBOARD_RECORDS_PATH));
    }
}
