//! Leaderboards service (, persisted in ).
//!
//! `LeaderboardService` is a thin validate-then-delegate layer over a
//! [`LeaderboardsRepository`](crate::repository::LeaderboardsRepository): it keeps
//! the board/user id validation and the metadata-must-be-an-object rejection (the
//! service-level checks the repository never sees) and forwards every operation to
//! the selected persistence backend, so leaderboards and the scores submitted to
//! them now survive a node restart on the Postgres and SQLite backends (the
//! in-memory backend stays non-durable by design).
//!
//! ## Domain
//!
//! A [`LeaderboardDefinition`] fixes a leaderboard's `id`, [`SortOrder`] (which
//! score is "better"), and [`Operator`] (how a new submission combines with the
//! existing record). Each user has at most one [`LeaderboardRecord`] per board.
//!
//! The score-write [`Operator`] semantics (best keeps the better score, set
//! overwrites, incr adds) and the ranking (best-first by `(score, subscore)` per
//! [`SortOrder`], `user_id` tie-break) live in the repository layer
//! (`src/repository/leaderboards.rs`) as pure, unit-tested helpers shared by all
//! three backends. The value types ([`LeaderboardDefinition`],
//! [`LeaderboardRecord`], [`RankedRecord`], …) are re-exported here so existing
//! console/HTTP consumers keep their `crate::services::…` paths.
//!
//! `reset_schedule` is validated as strict UTC five-field CRON and normalized to
//! the scheduler's seconds-first representation, but is not executed yet (see
//! `docs/architecture/technical-debt.md`).

use std::str::FromStr;
use std::sync::Arc;

use cron::Schedule;

use crate::error::{AppError, AppResult};
use crate::repository::LeaderboardsRepository;
use crate::time::TimestampMillis;

// Persistence value types live in the repository module; re-exported so
// `crate::services::LeaderboardDefinition` / `Operator` / … keep resolving for
// console/HTTP consumers.
pub use crate::repository::leaderboards::{
    CreateLeaderboardRequest, LeaderboardDefinition, LeaderboardRecord, LeaderboardSummary,
    Operator, RankedRecord, RecordsPage, SortOrder,
};

/// Maximum byte length of a leaderboard id.
const MAX_BOARD_ID_LEN: usize = 128;

/// Maximum byte length of a record's user id.
const MAX_USER_ID_LEN: usize = 256;

/// Leaderboards service backed by a persistence repository.
///
/// Holds an `Arc<dyn LeaderboardsRepository>` from the selected backend. All
/// methods are `async` and delegate after the service-level validation.
#[derive(Clone)]
pub struct LeaderboardService {
    repo: Arc<dyn LeaderboardsRepository>,
}

impl LeaderboardService {
    /// Create a service over a leaderboards repository (from the selected backend).
    #[must_use]
    pub fn new(repo: Arc<dyn LeaderboardsRepository>) -> Self {
        Self { repo }
    }

    /// Create a leaderboard.
    ///
    /// # Errors
    /// Returns a [`Validation`](crate::error::ErrorCategory::Validation) error if
    /// `id` is empty, exceeds [`MAX_BOARD_ID_LEN`] bytes, or contains a control
    /// character. Returns a [`Conflict`](crate::error::ErrorCategory::Conflict)
    /// error if a board with the same id already exists.
    pub async fn create(
        &self,
        mut request: CreateLeaderboardRequest,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardDefinition> {
        validate_id("leaderboard id", &request.id, MAX_BOARD_ID_LEN)?;
        request.reset_schedule = normalize_reset_schedule(request.reset_schedule)?;
        self.repo.create(request, now).await
    }

    /// List every leaderboard, id-ordered, with its current record count.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    pub async fn list(&self) -> AppResult<Vec<LeaderboardSummary>> {
        self.repo.list().await
    }

    /// Fetch one leaderboard definition by id.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if no board
    /// with `id` exists, or a backend error on failure.
    pub async fn get(&self, id: &str) -> AppResult<LeaderboardDefinition> {
        self.repo.get(id).await?.ok_or_else(|| board_not_found(id))
    }

    /// Delete a leaderboard and every one of its records.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if no board
    /// with `id` exists, or a backend error on failure.
    pub async fn delete(&self, id: &str) -> AppResult<()> {
        if self.repo.delete(id).await? {
            Ok(())
        } else {
            Err(board_not_found(id))
        }
    }

    /// Submit a score for `user_id` on `board`, applying the board's [`Operator`].
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if the board
    /// does not exist, or [`Validation`](crate::error::ErrorCategory::Validation)
    /// if `user_id` is invalid or `metadata` is present but not a JSON object.
    pub async fn submit(
        &self,
        board: &str,
        user_id: &str,
        score: i64,
        subscore: i64,
        metadata: Option<serde_json::Value>,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardRecord> {
        validate_id("user id", user_id, MAX_USER_ID_LEN)?;
        if metadata.as_ref().is_some_and(|value| !value.is_object()) {
            return Err(AppError::validation("metadata must be a JSON object"));
        }
        self.repo
            .submit(board, user_id, score, subscore, metadata, now)
            .await
    }

    /// Read a ranked page of records for `board`.
    ///
    /// `offset` is a rank offset (`0` starts at rank `1`); `limit` bounds how many
    /// ranked records are returned.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if the board
    /// does not exist, or a backend error on failure.
    pub async fn records(
        &self,
        board: &str,
        limit: usize,
        offset: usize,
    ) -> AppResult<RecordsPage> {
        self.repo.records(board, limit, offset).await
    }

    /// Delete one user's record from `board`.
    ///
    /// # Errors
    /// Returns [`NotFound`](crate::error::ErrorCategory::NotFound) if the board
    /// does not exist, or if the user has no record on it. Returns a backend error
    /// on failure.
    pub async fn delete_record(&self, board: &str, user_id: &str) -> AppResult<()> {
        if self.repo.delete_record(board, user_id).await? {
            Ok(())
        } else {
            Err(AppError::not_found(format!(
                "no record for user '{user_id}' on '{board}'"
            )))
        }
    }
}

/// The stable "no such leaderboard" error.
fn board_not_found(id: &str) -> AppError {
    AppError::not_found(format!("no such leaderboard '{id}'"))
}

/// Normalize Citadel's UTC-only five-field CRON dialect to the seconds-first
/// grammar accepted by the `cron` crate.
///
/// `None` remains unscheduled. A schedule must contain exactly five ASCII
/// whitespace-separated fields; timezone prefixes and six/seven-field forms are
/// intentionally rejected instead of silently changing when a reset occurs.
fn normalize_reset_schedule(schedule: Option<String>) -> AppResult<Option<String>> {
    let Some(schedule) = schedule else {
        return Ok(None);
    };
    let fields = schedule.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 || fields.iter().any(|field| field.starts_with("TZ=")) {
        return Err(AppError::validation(
            "reset_schedule must be a UTC five-field CRON expression",
        ));
    }
    let normalized = format!("0 {}", fields.join(" "));
    Schedule::from_str(&normalized).map_err(|_| {
        AppError::validation("reset_schedule must be a valid UTC five-field CRON expression")
    })?;
    Ok(Some(normalized))
}

/// Validate a leaderboard/user id: non-empty, bounded, no control characters.
fn validate_id(kind: &str, value: &str, max_len: usize) -> AppResult<()> {
    if value.is_empty() {
        return Err(AppError::validation(format!("{kind} must not be empty")));
    }
    if value.len() > max_len {
        return Err(AppError::validation(format!(
            "{kind} must not exceed {max_len} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(AppError::validation(format!(
            "{kind} must not contain control characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;
    use crate::repository::InMemoryLeaderboardsRepository;

    fn service() -> LeaderboardService {
        LeaderboardService::new(Arc::new(InMemoryLeaderboardsRepository::new()))
    }

    fn ts(ms: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(ms)
    }

    async fn create(service: &LeaderboardService, id: &str, sort: SortOrder, operator: Operator) {
        service
            .create(
                CreateLeaderboardRequest {
                    id: id.to_string(),
                    sort,
                    operator,
                    reset_schedule: None,
                },
                ts(0),
            )
            .await
            .expect("create board");
    }

    #[tokio::test]
    async fn create_rejects_duplicate_id_with_conflict() {
        let service = service();
        create(&service, "race", SortOrder::Asc, Operator::Best).await;
        let err = service
            .create(
                CreateLeaderboardRequest {
                    id: "race".to_string(),
                    sort: SortOrder::Desc,
                    operator: Operator::Set,
                    reset_schedule: None,
                },
                ts(0),
            )
            .await
            .expect_err("duplicate id must conflict");
        assert_eq!(err.category(), ErrorCategory::Conflict);
    }

    #[tokio::test]
    async fn create_rejects_invalid_ids_before_touching_the_repo() {
        let service = service();
        for id in [
            String::new(),
            "x".repeat(MAX_BOARD_ID_LEN + 1),
            "bad\u{0007}".to_string(),
        ] {
            let err = service
                .create(
                    CreateLeaderboardRequest {
                        id,
                        sort: SortOrder::Asc,
                        operator: Operator::Best,
                        reset_schedule: None,
                    },
                    ts(0),
                )
                .await
                .expect_err("invalid id rejected");
            assert_eq!(err.category(), ErrorCategory::Validation);
        }
    }

    #[tokio::test]
    async fn submit_rejects_non_object_metadata() {
        let service = service();
        create(&service, "board", SortOrder::Desc, Operator::Set).await;
        let err = service
            .submit("board", "u1", 10, 0, Some(serde_json::json!(5)), ts(0))
            .await
            .expect_err("non-object metadata rejected");
        assert_eq!(err.category(), ErrorCategory::Validation);
    }

    #[tokio::test]
    async fn submit_against_unknown_board_is_not_found() {
        let service = service();
        let err = service
            .submit("ghost", "u1", 10, 0, None, ts(0))
            .await
            .expect_err("unknown board");
        assert_eq!(err.category(), ErrorCategory::NotFound);
    }

    #[tokio::test]
    async fn delete_missing_board_and_record_are_not_found() {
        let service = service();
        create(&service, "board", SortOrder::Desc, Operator::Set).await;
        service.delete("board").await.expect("delete board");
        assert_eq!(
            service
                .delete("board")
                .await
                .expect_err("already gone")
                .category(),
            ErrorCategory::NotFound
        );

        create(&service, "board", SortOrder::Desc, Operator::Set).await;
        assert_eq!(
            service
                .delete_record("board", "ghost")
                .await
                .expect_err("no record")
                .category(),
            ErrorCategory::NotFound
        );
    }

    #[tokio::test]
    async fn submit_list_and_records_round_trip() {
        let service = service();
        create(&service, "board", SortOrder::Desc, Operator::Set).await;
        service
            .submit("board", "u1", 10, 0, None, ts(1))
            .await
            .expect("submit");
        service
            .submit("board", "u2", 30, 0, None, ts(1))
            .await
            .expect("submit");

        let summaries = service.list().await.expect("list");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].records, 2);

        let page = service.records("board", 10, 0).await.expect("records");
        assert_eq!(page.total, 2);
        assert_eq!(page.items[0].user_id, "u2", "higher score ranks first");
        assert_eq!(page.items[0].rank, 1);
    }

    #[test]
    fn reset_schedule_accepts_only_utc_five_field_cron_and_normalizes_seconds() {
        assert_eq!(
            normalize_reset_schedule(Some("0 12 * * *".to_string())).expect("five fields valid"),
            Some("0 0 12 * * *".to_string())
        );
        assert_eq!(normalize_reset_schedule(None).expect("none valid"), None);
        for schedule in ["0 0 12 * * *", "TZ=America/New_York 0 12 * * *", "daily"] {
            assert_eq!(
                normalize_reset_schedule(Some(schedule.to_string()))
                    .expect_err("unsupported schedule rejected")
                    .category(),
                ErrorCategory::Validation,
                "{schedule} must be rejected"
            );
        }
    }
}
