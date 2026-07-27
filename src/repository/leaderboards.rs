//! Leaderboards repository contract.
//!
//! Persists leaderboard definitions and per-user records behind the same
//! repository seam as identity/session/storage/friends/groups, so boards and the
//! scores submitted to them survive a node restart. A [`LeaderboardDefinition`]
//! fixes a board's caller-chosen `id`, its [`SortOrder`] (which score ranks best),
//! and its [`Operator`] (how a new submission combines with the existing record).
//! Each user has at most one [`LeaderboardRecord`] per board
//! (`PRIMARY KEY (leaderboard_id, owner_id)`).
//!
//! Following the friends/groups template, the read-modify-write decision — the
//! score-write [`Operator`] semantics and the ranking/pagination — lives in
//! exactly one place: the pure [`apply_submission`] / [`rank_cmp`] / [`rank_page`]
//! functions, unit-tested directly here. Every backend
//! ([`InMemoryLeaderboardsRepository`], the Postgres `PgLeaderboardsRepository`,
//! the SQLite `SqliteLeaderboardsRepository`) only does (lock/transaction) read →
//! apply the pure decision → write, so the three implementations cannot drift on
//! the operator or ranking rules.
//!
//! Ranks are derived on read (order the authoritative records by
//! `(score, subscore)` in the [`SortOrder`] direction, then `user_id` as a stable
//! tie-break); a durable rank cache is intentionally out of scope (see the task's
//! Known Gaps and `docs/architecture/technical-debt.md`).

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::time::TimestampMillis;

/// Which score value ranks best on a leaderboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    /// Lower scores rank better (e.g. race times).
    Asc,
    /// Higher scores rank better (e.g. points).
    Desc,
}

impl SortOrder {
    /// Stable lowercase token used in responses, audit details, and the durable
    /// `sort_order` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }

    /// Parse a stored `sort_order` token back into a [`SortOrder`].
    ///
    /// # Errors
    /// Returns an `Internal` error if the token is not one of the known values —
    /// a corrupt/foreign row rather than a client-visible condition.
    pub fn from_token(token: &str) -> AppResult<Self> {
        match token {
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            other => Err(AppError::internal(format!(
                "unknown leaderboard sort order token `{other}`"
            ))),
        }
    }
}

/// How a new submission combines with a user's existing record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operator {
    /// Keep whichever of the existing and submitted score is better.
    Best,
    /// Unconditionally overwrite the stored score.
    Set,
    /// Add the submission to the existing totals.
    Incr,
}

impl Operator {
    /// Stable lowercase token used in responses, audit details, and the durable
    /// `operator` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Best => "best",
            Self::Set => "set",
            Self::Incr => "incr",
        }
    }

    /// Parse a stored `operator` token back into an [`Operator`].
    ///
    /// # Errors
    /// Returns an `Internal` error if the token is not one of the known values.
    pub fn from_token(token: &str) -> AppResult<Self> {
        match token {
            "best" => Ok(Self::Best),
            "set" => Ok(Self::Set),
            "incr" => Ok(Self::Incr),
            other => Err(AppError::internal(format!(
                "unknown leaderboard operator token `{other}`"
            ))),
        }
    }
}

/// A leaderboard's fixed shape: how it sorts and combines submissions.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LeaderboardDefinition {
    /// Unique, operator-chosen identifier.
    pub id: String,
    /// Which score value ranks best.
    pub sort: SortOrder,
    /// How a new submission combines with the existing record.
    pub operator: Operator,
    /// Free-form reset schedule string (e.g. a cron expression).
    ///
    /// Stored verbatim and returned as-is. Citadel does not parse or execute it
    /// yet; see `docs/architecture/technical-debt.md`.
    pub reset_schedule: Option<String>,
    /// When the leaderboard was created.
    pub created_at: TimestampMillis,
}

/// One user's record on a leaderboard.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LeaderboardRecord {
    /// The submitting user's id.
    pub user_id: String,
    /// The primary score.
    pub score: i64,
    /// The secondary score, used to break ties in ranking and in
    /// [`Operator::Best`].
    pub subscore: i64,
    /// Optional caller-supplied JSON object attached to the record.
    pub metadata: Option<serde_json::Value>,
    /// When this record last changed (operator-applied or not).
    pub updated_at: TimestampMillis,
    /// How many times this user has submitted to this board.
    pub submissions: u32,
}

/// A [`LeaderboardRecord`] with its computed rank.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RankedRecord {
    /// `1`-based rank; `1` is the best record on the board.
    pub rank: u64,
    /// The record's user id.
    pub user_id: String,
    /// The primary score.
    pub score: i64,
    /// The secondary score.
    pub subscore: i64,
    /// Optional caller-supplied JSON object attached to the record.
    pub metadata: Option<serde_json::Value>,
    /// When this record last changed.
    pub updated_at: TimestampMillis,
    /// How many times this user has submitted to this board.
    pub submissions: u32,
}

impl RankedRecord {
    fn from_record(rank: u64, record: &LeaderboardRecord) -> Self {
        Self {
            rank,
            user_id: record.user_id.clone(),
            score: record.score,
            subscore: record.subscore,
            metadata: record.metadata.clone(),
            updated_at: record.updated_at,
            submissions: record.submissions,
        }
    }
}

/// A page of ranked records plus the board's total record count.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RecordsPage {
    /// Ranked records starting at the requested offset.
    pub items: Vec<RankedRecord>,
    /// Total records on the board (unaffected by `limit`/`offset`).
    pub total: usize,
}

/// A leaderboard definition plus its current record count, as returned by
/// [`LeaderboardsRepository::list`].
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LeaderboardSummary {
    /// The leaderboard's definition.
    #[serde(flatten)]
    pub definition: LeaderboardDefinition,
    /// Current number of records on the board.
    pub records: usize,
}

/// Parameters for [`LeaderboardsRepository::create`].
#[derive(Debug, Clone)]
pub struct CreateLeaderboardRequest {
    /// Unique, operator-chosen identifier. The service guarantees it is validated
    /// (non-blank, bounded, no control characters).
    pub id: String,
    /// Which score value ranks best.
    pub sort: SortOrder,
    /// How a new submission combines with the existing record.
    pub operator: Operator,
    /// Free-form reset schedule string, stored but not executed.
    pub reset_schedule: Option<String>,
}

// --- Pure decision helpers (the unit-tested state machine) -------------------

/// Compute the record that results from a submission, given the user's existing
/// record (if any) and the board's [`Operator`]/[`SortOrder`].
///
/// This is the single place the score-write semantics live, so all three backends
/// combine a submission identically:
///
/// - a user's first submission stores the submitted values with `submissions = 1`;
/// - `submissions` always increments on a resubmission;
/// - [`Operator::Set`] overwrites score/subscore/metadata and bumps `updated_at`;
/// - [`Operator::Incr`] adds to the existing totals, replaces metadata, and bumps
///   `updated_at`;
/// - [`Operator::Best`] keeps whichever `(score, subscore)` pair ranks better for
///   the board's [`SortOrder`] (lower wins for `Asc`, higher for `Desc`); a
///   winning submission replaces score/subscore/metadata and bumps `updated_at`,
///   a losing one only counts toward `submissions`.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn apply_submission(
    operator: Operator,
    sort: SortOrder,
    existing: Option<&LeaderboardRecord>,
    user_id: &str,
    score: i64,
    subscore: i64,
    metadata: Option<serde_json::Value>,
    now: TimestampMillis,
) -> LeaderboardRecord {
    let Some(existing) = existing else {
        return LeaderboardRecord {
            user_id: user_id.to_string(),
            score,
            subscore,
            metadata,
            updated_at: now,
            submissions: 1,
        };
    };
    let submissions = existing.submissions.saturating_add(1);
    match operator {
        Operator::Set => LeaderboardRecord {
            user_id: existing.user_id.clone(),
            score,
            subscore,
            metadata,
            updated_at: now,
            submissions,
        },
        Operator::Incr => LeaderboardRecord {
            user_id: existing.user_id.clone(),
            score: existing.score.saturating_add(score),
            subscore: existing.subscore.saturating_add(subscore),
            metadata,
            updated_at: now,
            submissions,
        },
        Operator::Best => {
            let candidate = (score, subscore);
            let current = (existing.score, existing.subscore);
            let wins = match sort {
                SortOrder::Asc => candidate <= current,
                SortOrder::Desc => candidate >= current,
            };
            if wins {
                LeaderboardRecord {
                    user_id: existing.user_id.clone(),
                    score,
                    subscore,
                    metadata,
                    updated_at: now,
                    submissions,
                }
            } else {
                LeaderboardRecord {
                    submissions,
                    ..existing.clone()
                }
            }
        }
    }
}

/// Ordering used for ranking: best-first by `(score, subscore)` per `sort`, then
/// ascending `user_id` as a final, deterministic tie-break so two users with an
/// identical score+subscore still get a stable, reproducible rank order.
#[must_use]
pub fn rank_cmp(
    sort: SortOrder,
    a: &LeaderboardRecord,
    b: &LeaderboardRecord,
) -> std::cmp::Ordering {
    let key_order = match sort {
        SortOrder::Asc => (a.score, a.subscore).cmp(&(b.score, b.subscore)),
        SortOrder::Desc => (b.score, b.subscore).cmp(&(a.score, a.subscore)),
    };
    key_order.then_with(|| a.user_id.cmp(&b.user_id))
}

/// Rank an unordered set of a board's records best-first and return the requested
/// page (rank `offset+1` onward, up to `limit` records). `total` is the board's
/// full record count, unaffected by paging.
///
/// The single place the rank-read semantics live, so all three backends derive
/// identical ranks and pages.
#[must_use]
pub fn rank_page(
    sort: SortOrder,
    mut records: Vec<LeaderboardRecord>,
    limit: usize,
    offset: usize,
) -> RecordsPage {
    records.sort_by(|a, b| rank_cmp(sort, a, b));
    let total = records.len();
    let items = records
        .iter()
        .enumerate()
        .map(|(index, record)| RankedRecord::from_record((index + 1) as u64, record))
        .skip(offset)
        .take(limit)
        .collect();
    RecordsPage { items, total }
}

// --- Repository contract -----------------------------------------------------

/// Persistence boundary for leaderboards and their records.
///
/// The service layer validates the board/user id shape and rejects non-object
/// metadata before delegating, so implementations may assume ids are already
/// well-formed. Id uniqueness (on create), board existence (on submit/records),
/// and the operator + ranking rules are enforced here (via the pure helpers
/// above) so every backend agrees.
#[async_trait]
pub trait LeaderboardsRepository: Send + Sync {
    /// Create a leaderboard.
    ///
    /// # Errors
    /// - `Conflict` if a board with the same id already exists.
    /// - A backend error on failure.
    async fn create(
        &self,
        request: CreateLeaderboardRequest,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardDefinition>;

    /// List every leaderboard, id-ordered, with its current record count.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn list(&self) -> AppResult<Vec<LeaderboardSummary>>;

    /// Fetch one leaderboard definition by id, or `None` if it does not exist.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn get(&self, id: &str) -> AppResult<Option<LeaderboardDefinition>>;

    /// Delete a leaderboard and every one of its records. Returns whether a board
    /// was removed.
    ///
    /// # Errors
    /// Returns a backend error on failure.
    async fn delete(&self, id: &str) -> AppResult<bool>;

    /// Submit a score for `user_id` on `board`, applying the board's [`Operator`].
    ///
    /// # Errors
    /// - `NotFound` if the board does not exist.
    /// - A backend error on failure.
    #[allow(clippy::too_many_arguments)]
    async fn submit(
        &self,
        board: &str,
        user_id: &str,
        score: i64,
        subscore: i64,
        metadata: Option<serde_json::Value>,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardRecord>;

    /// Read a ranked page of records for `board`. `offset` is a rank offset (`0`
    /// starts at rank `1`); `limit` bounds how many ranked records are returned.
    ///
    /// # Errors
    /// - `NotFound` if the board does not exist.
    /// - A backend error on failure.
    async fn records(&self, board: &str, limit: usize, offset: usize) -> AppResult<RecordsPage>;

    /// Delete one user's record from `board`. Returns whether a record was
    /// removed.
    ///
    /// # Errors
    /// - `NotFound` if the board does not exist.
    /// - A backend error on failure.
    async fn delete_record(&self, board: &str, user_id: &str) -> AppResult<bool>;
}

/// The stable "no such leaderboard" error, shared by every backend.
pub(crate) fn board_not_found(id: &str) -> AppError {
    AppError::not_found(format!("no such leaderboard '{id}'"))
}

// --- In-memory reference implementation --------------------------------------

/// A board and its per-user records (keyed by user id for deterministic reads).
#[derive(Debug)]
struct Board {
    definition: LeaderboardDefinition,
    records: BTreeMap<String, LeaderboardRecord>,
}

/// The board store: `id -> Board`. A [`BTreeMap`] keeps [`Self::list`] id-ordered
/// without an extra sort step. A named alias keeps the guard types readable.
type BoardStore = BTreeMap<String, Board>;

/// A contract-faithful, in-memory [`LeaderboardsRepository`] (the reference impl).
///
/// Single-process and not durable, but it enforces the full id-uniqueness +
/// operator + ranking contract through the shared pure helpers, so the contract
/// tests in `tests/leaderboards_repository_contract.rs` can be reused against the
/// durable backends.
#[derive(Debug, Default)]
pub struct InMemoryLeaderboardsRepository {
    boards: Mutex<BoardStore>,
}

impl InMemoryLeaderboardsRepository {
    /// Create an empty repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, BoardStore>> {
        self.boards
            .lock()
            .map_err(|_| AppError::internal("leaderboards repository mutex poisoned"))
    }
}

#[async_trait]
impl LeaderboardsRepository for InMemoryLeaderboardsRepository {
    async fn create(
        &self,
        request: CreateLeaderboardRequest,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardDefinition> {
        let mut boards = self.guard()?;
        if boards.contains_key(&request.id) {
            return Err(AppError::conflict(format!(
                "leaderboard '{}' already exists",
                request.id
            )));
        }
        let definition = LeaderboardDefinition {
            id: request.id.clone(),
            sort: request.sort,
            operator: request.operator,
            reset_schedule: request.reset_schedule,
            created_at: now,
        };
        boards.insert(
            request.id,
            Board {
                definition: definition.clone(),
                records: BTreeMap::new(),
            },
        );
        Ok(definition)
    }

    async fn list(&self) -> AppResult<Vec<LeaderboardSummary>> {
        Ok(self
            .guard()?
            .values()
            .map(|board| LeaderboardSummary {
                definition: board.definition.clone(),
                records: board.records.len(),
            })
            .collect())
    }

    async fn get(&self, id: &str) -> AppResult<Option<LeaderboardDefinition>> {
        Ok(self.guard()?.get(id).map(|board| board.definition.clone()))
    }

    async fn delete(&self, id: &str) -> AppResult<bool> {
        Ok(self.guard()?.remove(id).is_some())
    }

    async fn submit(
        &self,
        board: &str,
        user_id: &str,
        score: i64,
        subscore: i64,
        metadata: Option<serde_json::Value>,
        now: TimestampMillis,
    ) -> AppResult<LeaderboardRecord> {
        let mut boards = self.guard()?;
        let entry = boards
            .get_mut(board)
            .ok_or_else(|| board_not_found(board))?;
        let sort = entry.definition.sort;
        let operator = entry.definition.operator;
        let record = apply_submission(
            operator,
            sort,
            entry.records.get(user_id),
            user_id,
            score,
            subscore,
            metadata,
            now,
        );
        entry.records.insert(user_id.to_string(), record.clone());
        Ok(record)
    }

    async fn records(&self, board: &str, limit: usize, offset: usize) -> AppResult<RecordsPage> {
        let boards = self.guard()?;
        let entry = boards.get(board).ok_or_else(|| board_not_found(board))?;
        let records = entry.records.values().cloned().collect();
        Ok(rank_page(entry.definition.sort, records, limit, offset))
    }

    async fn delete_record(&self, board: &str, user_id: &str) -> AppResult<bool> {
        let mut boards = self.guard()?;
        let entry = boards
            .get_mut(board)
            .ok_or_else(|| board_not_found(board))?;
        Ok(entry.records.remove(user_id).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(ms: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(ms)
    }

    fn record(user: &str, score: i64, subscore: i64) -> LeaderboardRecord {
        LeaderboardRecord {
            user_id: user.to_string(),
            score,
            subscore,
            metadata: None,
            updated_at: ts(0),
            submissions: 1,
        }
    }

    // --- pure helpers -------------------------------------------------------

    #[test]
    fn sort_and_operator_tokens_round_trip() {
        for sort in [SortOrder::Asc, SortOrder::Desc] {
            assert_eq!(SortOrder::from_token(sort.as_str()).expect("parse"), sort);
        }
        assert!(SortOrder::from_token("sideways").is_err());
        for op in [Operator::Best, Operator::Set, Operator::Incr] {
            assert_eq!(Operator::from_token(op.as_str()).expect("parse"), op);
        }
        assert!(Operator::from_token("multiply").is_err());
    }

    #[test]
    fn apply_submission_initializes_first_record() {
        let record = apply_submission(
            Operator::Best,
            SortOrder::Desc,
            None,
            "u1",
            50,
            2,
            Some(serde_json::json!({"a": 1})),
            ts(5),
        );
        assert_eq!(record.user_id, "u1");
        assert_eq!(record.score, 50);
        assert_eq!(record.subscore, 2);
        assert_eq!(record.submissions, 1);
        assert_eq!(record.updated_at, ts(5));
    }

    #[test]
    fn set_operator_always_overwrites_and_counts() {
        let existing = LeaderboardRecord {
            submissions: 3,
            ..record("u1", 100, 1)
        };
        let next = apply_submission(
            Operator::Set,
            SortOrder::Desc,
            Some(&existing),
            "u1",
            10,
            9,
            Some(serde_json::json!({"b": 2})),
            ts(7),
        );
        assert_eq!(next.score, 10, "set overwrites even a worse score");
        assert_eq!(next.subscore, 9);
        assert_eq!(next.metadata, Some(serde_json::json!({"b": 2})));
        assert_eq!(next.submissions, 4);
        assert_eq!(next.updated_at, ts(7));
    }

    #[test]
    fn incr_operator_adds_to_existing_totals() {
        let existing = record("u1", 5, 1);
        let next = apply_submission(
            Operator::Incr,
            SortOrder::Desc,
            Some(&existing),
            "u1",
            3,
            2,
            None,
            ts(2),
        );
        assert_eq!(next.score, 8);
        assert_eq!(next.subscore, 3);
        assert_eq!(next.submissions, 2);
    }

    #[test]
    fn best_operator_desc_keeps_higher_and_breaks_ties_to_higher_subscore() {
        let existing = record("u1", 50, 1);
        // Worse score: unchanged (but counted).
        let worse = apply_submission(
            Operator::Best,
            SortOrder::Desc,
            Some(&existing),
            "u1",
            40,
            99,
            None,
            ts(2),
        );
        assert_eq!(worse.score, 50);
        assert_eq!(worse.subscore, 1);
        assert_eq!(worse.submissions, 2);

        // Tied score, higher subscore wins for Desc.
        let tie = apply_submission(
            Operator::Best,
            SortOrder::Desc,
            Some(&existing),
            "u1",
            50,
            5,
            None,
            ts(3),
        );
        assert_eq!(tie.subscore, 5);
    }

    #[test]
    fn best_operator_asc_keeps_lower_and_breaks_ties_to_lower_subscore() {
        let existing = record("u1", 100, 5);
        // Better (lower) score wins for Asc.
        let better = apply_submission(
            Operator::Best,
            SortOrder::Asc,
            Some(&existing),
            "u1",
            50,
            9,
            None,
            ts(2),
        );
        assert_eq!(better.score, 50);
        assert_eq!(better.subscore, 9);

        // Tied score, higher subscore loses for Asc.
        let existing = record("u1", 50, 2);
        let tie_worse = apply_submission(
            Operator::Best,
            SortOrder::Asc,
            Some(&existing),
            "u1",
            50,
            8,
            None,
            ts(3),
        );
        assert_eq!(tie_worse.subscore, 2, "higher subscore loses for Asc");
    }

    #[test]
    fn rank_page_orders_best_first_with_deterministic_ties() {
        let records = vec![
            record("bravo", 50, 0),
            record("alpha", 50, 0),
            record("charlie", 90, 0),
        ];
        let page = rank_page(SortOrder::Desc, records, 10, 0);
        assert_eq!(page.total, 3);
        let order: Vec<(&str, u64)> = page
            .items
            .iter()
            .map(|r| (r.user_id.as_str(), r.rank))
            .collect();
        assert_eq!(order, vec![("charlie", 1), ("alpha", 2), ("bravo", 3)]);
    }

    #[test]
    fn rank_page_respects_limit_and_offset() {
        let records = vec![
            record("a", 10, 0),
            record("b", 20, 0),
            record("c", 30, 0),
            record("d", 40, 0),
        ];
        let page = rank_page(SortOrder::Desc, records, 2, 1);
        assert_eq!(page.total, 4);
        let users: Vec<&str> = page.items.iter().map(|r| r.user_id.as_str()).collect();
        assert_eq!(users, vec!["c", "b"]);
        assert_eq!(page.items[0].rank, 2);
        assert_eq!(page.items[1].rank, 3);
    }

    // --- InMemoryLeaderboardsRepository (reference impl) --------------------

    fn create_request(id: &str, sort: SortOrder, operator: Operator) -> CreateLeaderboardRequest {
        CreateLeaderboardRequest {
            id: id.to_string(),
            sort,
            operator,
            reset_schedule: None,
        }
    }

    #[tokio::test]
    async fn create_rejects_duplicate_id() {
        let repo = InMemoryLeaderboardsRepository::new();
        repo.create(
            create_request("race", SortOrder::Asc, Operator::Best),
            ts(0),
        )
        .await
        .expect("first");
        assert_eq!(
            repo.create(
                create_request("race", SortOrder::Desc, Operator::Set),
                ts(0)
            )
            .await
            .expect_err("duplicate")
            .category(),
            crate::error::ErrorCategory::Conflict
        );
    }

    #[tokio::test]
    async fn submit_and_rank_round_trip() {
        let repo = InMemoryLeaderboardsRepository::new();
        repo.create(
            create_request("board", SortOrder::Desc, Operator::Set),
            ts(0),
        )
        .await
        .expect("create");
        repo.submit("board", "u1", 10, 0, None, ts(1))
            .await
            .expect("submit u1");
        repo.submit("board", "u2", 30, 0, None, ts(1))
            .await
            .expect("submit u2");
        let page = repo.records("board", 10, 0).await.expect("records");
        assert_eq!(page.total, 2);
        assert_eq!(page.items[0].user_id, "u2", "higher score ranks first");
    }

    #[tokio::test]
    async fn submit_against_unknown_board_is_not_found() {
        let repo = InMemoryLeaderboardsRepository::new();
        assert_eq!(
            repo.submit("ghost", "u1", 1, 0, None, ts(0))
                .await
                .expect_err("unknown")
                .category(),
            crate::error::ErrorCategory::NotFound
        );
    }
}
