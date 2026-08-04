//! SQLite durable tournament adapter.

use async_trait::async_trait;
use sqlx::{Row, SqliteConnection};

use crate::error::{AppError, AppResult};
use crate::leaderboard_scheduler::ResetEpoch;
use crate::repository::tournaments::{
    CreateTournamentRequest, Tournament, TournamentEntry, TournamentResult,
    TournamentSettlementOutboxRecord, TournamentState, TournamentsRepository, can_transition,
    validate_schedule,
};
use crate::time::TimestampMillis;

use super::{SqliteExecutor, db_err, millis_to_ts, ts_to_millis};

const INSERT_RESULTS_FROM_EPOCH_SQL: &str = "\
INSERT INTO tournament_results (tournament_id, user_id, rank, score, subscore) \
SELECT ?, snapshot.owner_id, \
       ROW_NUMBER() OVER (ORDER BY \
           CASE WHEN leaderboard.sort_order = 'asc' THEN snapshot.score END ASC, \
           CASE WHEN leaderboard.sort_order = 'desc' THEN snapshot.score END DESC, \
           CASE WHEN leaderboard.sort_order = 'asc' THEN snapshot.subscore END ASC, \
           CASE WHEN leaderboard.sort_order = 'desc' THEN snapshot.subscore END DESC, \
           snapshot.owner_id ASC \
       ), \
       snapshot.score, snapshot.subscore \
FROM leaderboard_reset_snapshot_records AS snapshot \
JOIN leaderboards AS leaderboard ON leaderboard.id = snapshot.leaderboard_id \
WHERE snapshot.leaderboard_id = ? AND snapshot.due_at_unix_ms = ?";

pub struct SqliteTournamentsRepository {
    executor: SqliteExecutor,
}

impl SqliteTournamentsRepository {
    pub(super) fn new(executor: SqliteExecutor) -> Self {
        Self { executor }
    }

    async fn transaction<T>(
        &self,
        f: impl for<'a> FnOnce(
            &'a mut SqliteConnection,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = AppResult<T>> + Send + 'a>,
        >,
    ) -> AppResult<T> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                let mut tx = pool.begin_with("BEGIN IMMEDIATE;").await.map_err(db_err)?;
                match f(&mut tx).await {
                    Ok(value) => {
                        tx.commit().await.map_err(db_err)?;
                        Ok(value)
                    }
                    Err(error) => {
                        let _ = tx.rollback().await;
                        Err(error)
                    }
                }
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("database transaction is already closed"))?;
                f(tx).await
            }
        }
    }
}

fn parse(row: &sqlx::sqlite::SqliteRow) -> AppResult<Tournament> {
    let state: String = row
        .try_get("state")
        .map_err(|_| AppError::internal("invalid tournament row"))?;
    let leaderboard_id: String = row
        .try_get("leaderboard_id")
        .map_err(|_| AppError::internal("invalid tournament row"))?;
    let settled: Option<i64> = row
        .try_get("settled_due_at_unix_ms")
        .map_err(|_| AppError::internal("invalid tournament row"))?;
    Ok(Tournament {
        id: row
            .try_get("id")
            .map_err(|_| AppError::internal("invalid tournament row"))?,
        leaderboard_id: leaderboard_id.clone(),
        state: TournamentState::from_token(&state)?,
        registration_opens_at: millis_to_ts(
            row.try_get("registration_opens_at_unix_ms")
                .map_err(|_| AppError::internal("invalid tournament row"))?,
        )?,
        registration_closes_at: millis_to_ts(
            row.try_get("registration_closes_at_unix_ms")
                .map_err(|_| AppError::internal("invalid tournament row"))?,
        )?,
        starts_at: millis_to_ts(
            row.try_get("starts_at_unix_ms")
                .map_err(|_| AppError::internal("invalid tournament row"))?,
        )?,
        ends_at: millis_to_ts(
            row.try_get("ends_at_unix_ms")
                .map_err(|_| AppError::internal("invalid tournament row"))?,
        )?,
        settled_epoch: settled
            .map(|due_at| {
                millis_to_ts(due_at).map(|due_at| ResetEpoch::new(leaderboard_id, due_at))
            })
            .transpose()?,
        created_at: millis_to_ts(
            row.try_get("created_at_unix_ms")
                .map_err(|_| AppError::internal("invalid tournament row"))?,
        )?,
        updated_at: millis_to_ts(
            row.try_get("updated_at_unix_ms")
                .map_err(|_| AppError::internal("invalid tournament row"))?,
        )?,
    })
}

#[async_trait]
impl TournamentsRepository for SqliteTournamentsRepository {
    async fn create(
        &self,
        request: CreateTournamentRequest,
        now: TimestampMillis,
    ) -> AppResult<Tournament> {
        validate_schedule(&request)?;
        let now = ts_to_millis(now)?;
        let request_for_result = request.clone();
        self.transaction(|conn| Box::pin(async move {
            sqlx::query("INSERT INTO tournaments (id, leaderboard_id, state, registration_opens_at_unix_ms, registration_closes_at_unix_ms, starts_at_unix_ms, ends_at_unix_ms, created_at_unix_ms, updated_at_unix_ms) VALUES (?, ?, 'draft', ?, ?, ?, ?, ?, ?)")
                .bind(&request.id).bind(&request.leaderboard_id)
                .bind(ts_to_millis(request.registration_opens_at)?).bind(ts_to_millis(request.registration_closes_at)?)
                .bind(ts_to_millis(request.starts_at)?).bind(ts_to_millis(request.ends_at)?)
                .bind(now).bind(now).execute(conn).await.map_err(db_err)?;
            Ok(Tournament { id: request_for_result.id, leaderboard_id: request_for_result.leaderboard_id, state: TournamentState::Draft, registration_opens_at: request_for_result.registration_opens_at, registration_closes_at: request_for_result.registration_closes_at, starts_at: request_for_result.starts_at, ends_at: request_for_result.ends_at, settled_epoch: None, created_at: TimestampMillis::from_unix_millis(now as u64), updated_at: TimestampMillis::from_unix_millis(now as u64) })
        })).await
    }

    async fn get(&self, id: &str) -> AppResult<Option<Tournament>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => sqlx::query("SELECT * FROM tournaments WHERE id = ?")
                .bind(id)
                .fetch_optional(pool)
                .await
                .map_err(db_err)?
                .as_ref()
                .map(parse)
                .transpose(),
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("database transaction is already closed"))?;
                sqlx::query("SELECT * FROM tournaments WHERE id = ?")
                    .bind(id)
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(db_err)?
                    .as_ref()
                    .map(parse)
                    .transpose()
            }
        }
    }

    async fn list(&self) -> AppResult<Vec<Tournament>> {
        match &self.executor {
            SqliteExecutor::Pool(pool) => {
                sqlx::query("SELECT * FROM tournaments ORDER BY starts_at_unix_ms ASC, id ASC")
                    .fetch_all(pool)
                    .await
                    .map_err(db_err)?
                    .iter()
                    .map(parse)
                    .collect()
            }
            SqliteExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard
                    .as_mut()
                    .ok_or_else(|| AppError::internal("database transaction is already closed"))?;
                sqlx::query("SELECT * FROM tournaments ORDER BY starts_at_unix_ms ASC, id ASC")
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(db_err)?
                    .iter()
                    .map(parse)
                    .collect()
            }
        }
    }

    async fn transition(
        &self,
        id: &str,
        to: TournamentState,
        now: TimestampMillis,
    ) -> AppResult<Tournament> {
        let id = id.to_owned();
        self.transaction(|conn| {
            Box::pin(async move {
                let row = sqlx::query("SELECT * FROM tournaments WHERE id = ?")
                    .bind(&id)
                    .fetch_optional(&mut *conn)
                    .await
                    .map_err(db_err)?
                    .ok_or_else(|| AppError::not_found("no such tournament"))?;
                let current = parse(&row)?;
                if !can_transition(current.state, to) {
                    return Err(AppError::conflict("illegal tournament state transition"));
                }
                sqlx::query(
                    "UPDATE tournaments SET state = ?, updated_at_unix_ms = ? WHERE id = ?",
                )
                .bind(to.as_str())
                .bind(ts_to_millis(now)?)
                .bind(&id)
                .execute(conn)
                .await
                .map_err(db_err)?;
                Ok(Tournament {
                    state: to,
                    updated_at: now,
                    ..current
                })
            })
        })
        .await
    }

    async fn register(
        &self,
        tournament_id: &str,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<TournamentEntry> {
        let id = tournament_id.to_owned();
        let user = user_id.to_owned();
        self.transaction(|conn| Box::pin(async move {
            let row = sqlx::query("SELECT * FROM tournaments WHERE id = ?").bind(&id).fetch_optional(&mut *conn).await.map_err(db_err)?.ok_or_else(|| AppError::not_found("no such tournament"))?;
            let tournament = parse(&row)?;
            if tournament.state != TournamentState::RegistrationOpen || now < tournament.registration_opens_at || now >= tournament.registration_closes_at { return Err(AppError::conflict("tournament registration is closed")); }
            sqlx::query("INSERT INTO tournament_entries (tournament_id, user_id, registered_at_unix_ms) VALUES (?, ?, ?)").bind(&id).bind(&user).bind(ts_to_millis(now)?).execute(conn).await.map_err(db_err)?;
            Ok(TournamentEntry { tournament_id: id, user_id: user, registered_at: now })
        })).await
    }

    async fn entries(&self, tournament_id: &str) -> AppResult<Vec<TournamentEntry>> {
        let id = tournament_id.to_owned();
        self.transaction(|conn| Box::pin(async move {
            let rows = sqlx::query("SELECT user_id, registered_at_unix_ms FROM tournament_entries WHERE tournament_id = ? ORDER BY user_id").bind(&id).fetch_all(conn).await.map_err(db_err)?;
            rows.iter().map(|row| Ok(TournamentEntry { tournament_id: id.clone(), user_id: row.try_get("user_id").map_err(|_| AppError::internal("invalid tournament entry row"))?, registered_at: millis_to_ts(row.try_get("registered_at_unix_ms").map_err(|_| AppError::internal("invalid tournament entry row"))?)? })).collect()
        })).await
    }

    async fn results(&self, tournament_id: &str) -> AppResult<Vec<TournamentResult>> {
        let id = tournament_id.to_owned();
        self.transaction(|conn| Box::pin(async move {
            let rows = sqlx::query("SELECT user_id, rank, score, subscore FROM tournament_results WHERE tournament_id = ? ORDER BY rank, user_id").bind(&id).fetch_all(conn).await.map_err(db_err)?;
            rows.iter().map(|row| Ok(TournamentResult { tournament_id: id.clone(), user_id: row.try_get("user_id").map_err(|_| AppError::internal("invalid tournament result row"))?, rank: u64::try_from(row.try_get::<i64, _>("rank").map_err(|_| AppError::internal("invalid tournament result row"))?).map_err(|_| AppError::internal("invalid tournament result rank"))?, score: row.try_get("score").map_err(|_| AppError::internal("invalid tournament result row"))?, subscore: row.try_get("subscore").map_err(|_| AppError::internal("invalid tournament result row"))? })).collect()
        })).await
    }

    async fn settle_from_epoch(
        &self,
        id: &str,
        epoch: ResetEpoch,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let id = id.to_owned();
        self.transaction(|conn| Box::pin(async move {
            let row = sqlx::query("SELECT * FROM tournaments WHERE id = ?").bind(&id).fetch_optional(&mut *conn).await.map_err(db_err)?.ok_or_else(|| AppError::not_found("no such tournament"))?;
            let tournament = parse(&row)?;
            if tournament.state == TournamentState::Completed && tournament.settled_epoch.as_ref() == Some(&epoch) { return Ok(false); }
            if tournament.state != TournamentState::Running || tournament.leaderboard_id != epoch.leaderboard_id { return Err(AppError::conflict("tournament cannot settle from this reset epoch")); }
            let exists = sqlx::query("SELECT 1 FROM leaderboard_reset_epochs WHERE leaderboard_id = ? AND due_at_unix_ms = ?").bind(&epoch.leaderboard_id).bind(ts_to_millis(epoch.due_at)?).fetch_optional(&mut *conn).await.map_err(db_err)?;
            if exists.is_none() { return Err(AppError::conflict("tournament reset epoch is not committed")); }
            let due_at = ts_to_millis(epoch.due_at)?;
            sqlx::query(INSERT_RESULTS_FROM_EPOCH_SQL)
                .bind(&id)
                .bind(&epoch.leaderboard_id)
                .bind(due_at)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
            sqlx::query("INSERT INTO tournament_settlement_outbox (tournament_id, leaderboard_id, due_at_unix_ms, created_at_unix_ms) VALUES (?, ?, ?, ?)")
                .bind(&id)
                .bind(&epoch.leaderboard_id)
                .bind(due_at)
                .bind(ts_to_millis(now)?)
                .execute(&mut *conn)
                .await
                .map_err(db_err)?;
            sqlx::query("UPDATE tournaments SET state = 'completed', settled_due_at_unix_ms = ?, updated_at_unix_ms = ? WHERE id = ?").bind(due_at).bind(ts_to_millis(now)?).bind(&id).execute(conn).await.map_err(db_err)?;
            Ok(true)
        })).await
    }

    async fn pending_settlement_outbox(
        &self,
        limit: usize,
    ) -> AppResult<Vec<TournamentSettlementOutboxRecord>> {
        self.transaction(|conn| Box::pin(async move {
            let rows = sqlx::query("SELECT tournament_id, leaderboard_id, due_at_unix_ms FROM tournament_settlement_outbox ORDER BY created_at_unix_ms, tournament_id, due_at_unix_ms LIMIT ?")
                .bind(i64::try_from(limit).map_err(|_| AppError::validation("outbox limit is too large"))?)
                .fetch_all(conn)
                .await
                .map_err(db_err)?;
            rows.iter().map(|row| Ok(TournamentSettlementOutboxRecord {
                tournament_id: row.try_get("tournament_id").map_err(|_| AppError::internal("invalid tournament settlement outbox row"))?,
                epoch: ResetEpoch::new(
                    row.try_get("leaderboard_id").map_err(|_| AppError::internal("invalid tournament settlement outbox row"))?,
                    millis_to_ts(row.try_get("due_at_unix_ms").map_err(|_| AppError::internal("invalid tournament settlement outbox row"))?)?,
                ),
            })).collect()
        })).await
    }

    async fn acknowledge_settlement_outbox(
        &self,
        tournament_id: &str,
        epoch: &ResetEpoch,
    ) -> AppResult<()> {
        let tournament_id = tournament_id.to_owned();
        let leaderboard_id = epoch.leaderboard_id.clone();
        let due_at = ts_to_millis(epoch.due_at)?;
        self.transaction(|conn| Box::pin(async move {
            sqlx::query("DELETE FROM tournament_settlement_outbox WHERE tournament_id = ? AND leaderboard_id = ? AND due_at_unix_ms = ?")
                .bind(tournament_id)
                .bind(leaderboard_id)
                .bind(due_at)
                .execute(conn)
                .await
                .map_err(db_err)?;
            Ok(())
        })).await
    }
}
