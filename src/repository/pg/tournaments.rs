//! PostgreSQL-wire durable tournament adapter.

use async_trait::async_trait;
use sqlx::postgres::PgRow;

use crate::error::{AppError, AppResult};
use crate::leaderboard_scheduler::ResetEpoch;
use crate::repository::tournaments::{
    CreateTournamentRequest, Tournament, TournamentEntry, TournamentResult,
    TournamentSettlementOutboxRecord, TournamentState, TournamentsRepository, can_transition,
    validate_schedule,
};
use crate::time::TimestampMillis;

use super::{PgExecutor, db_err, get, millis_to_ts, ts_to_millis, tx_closed};

const INSERT_RESULTS_FROM_EPOCH_SQL: &str = "\
INSERT INTO tournament_results (tournament_id, user_id, rank, score, subscore) \
SELECT $1, snapshot.owner_id, \
       ROW_NUMBER() OVER (ORDER BY \
           CASE WHEN leaderboard.sort_order = 'asc' THEN snapshot.score END ASC, \
           CASE WHEN leaderboard.sort_order = 'desc' THEN snapshot.score END DESC, \
           CASE WHEN leaderboard.sort_order = 'asc' THEN snapshot.subscore END ASC, \
           CASE WHEN leaderboard.sort_order = 'desc' THEN snapshot.subscore END DESC, \
           snapshot.owner_id ASC \
       ), snapshot.score, snapshot.subscore \
FROM leaderboard_reset_snapshot_records AS snapshot \
JOIN leaderboards AS leaderboard ON leaderboard.id = snapshot.leaderboard_id \
WHERE snapshot.leaderboard_id = $2 AND snapshot.due_at_unix_ms = $3";

/// PostgreSQL/CockroachDB durable tournament repository.
pub struct PgTournamentsRepository {
    executor: PgExecutor,
}

impl PgTournamentsRepository {
    pub(super) fn new(executor: PgExecutor) -> Self {
        Self { executor }
    }
}

macro_rules! with_write_tx {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            PgExecutor::Pool(pool) => {
                let mut tx = pool.begin().await.map_err(db_err)?;
                let result = {
                    let $conn = &mut *tx;
                    $body
                };
                match result {
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
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

macro_rules! with_conn {
    ($self:ident, $conn:ident => $body:expr) => {
        match &$self.executor {
            PgExecutor::Pool(pool) => {
                let mut conn = pool.acquire().await.map_err(db_err)?;
                let $conn = &mut *conn;
                $body
            }
            PgExecutor::Tx(cell) => {
                let mut guard = cell.lock().await;
                let tx = guard.as_mut().ok_or_else(tx_closed)?;
                let $conn = &mut **tx;
                $body
            }
        }
    };
}

fn parse(row: &PgRow) -> AppResult<Tournament> {
    let state: String = get(row, "state")?;
    let leaderboard_id: String = get(row, "leaderboard_id")?;
    let settled: Option<i64> = get(row, "settled_due_at_unix_ms")?;
    Ok(Tournament {
        id: get(row, "id")?,
        leaderboard_id: leaderboard_id.clone(),
        state: TournamentState::from_token(&state)?,
        registration_opens_at: millis_to_ts(get(row, "registration_opens_at_unix_ms")?)?,
        registration_closes_at: millis_to_ts(get(row, "registration_closes_at_unix_ms")?)?,
        starts_at: millis_to_ts(get(row, "starts_at_unix_ms")?)?,
        ends_at: millis_to_ts(get(row, "ends_at_unix_ms")?)?,
        settled_epoch: settled
            .map(|due_at| {
                millis_to_ts(due_at).map(|due_at| ResetEpoch::new(leaderboard_id, due_at))
            })
            .transpose()?,
        created_at: millis_to_ts(get(row, "created_at_unix_ms")?)?,
        updated_at: millis_to_ts(get(row, "updated_at_unix_ms")?)?,
    })
}

#[async_trait]
impl TournamentsRepository for PgTournamentsRepository {
    async fn create(
        &self,
        request: CreateTournamentRequest,
        now: TimestampMillis,
    ) -> AppResult<Tournament> {
        validate_schedule(&request)?;
        let now_millis = ts_to_millis(now)?;
        let result = request.clone();
        with_write_tx!(self, conn => async {
            sqlx::query("INSERT INTO tournaments (id, leaderboard_id, state, registration_opens_at_unix_ms, registration_closes_at_unix_ms, starts_at_unix_ms, ends_at_unix_ms, created_at_unix_ms, updated_at_unix_ms) VALUES ($1, $2, 'draft', $3, $4, $5, $6, $7, $8)")
                .bind(&request.id).bind(&request.leaderboard_id).bind(ts_to_millis(request.registration_opens_at)?).bind(ts_to_millis(request.registration_closes_at)?).bind(ts_to_millis(request.starts_at)?).bind(ts_to_millis(request.ends_at)?).bind(now_millis).bind(now_millis).execute(conn).await.map_err(db_err)?;
            Ok(Tournament { id: result.id, leaderboard_id: result.leaderboard_id, state: TournamentState::Draft, registration_opens_at: result.registration_opens_at, registration_closes_at: result.registration_closes_at, starts_at: result.starts_at, ends_at: result.ends_at, settled_epoch: None, created_at: now, updated_at: now })
        }.await)
    }

    async fn get(&self, id: &str) -> AppResult<Option<Tournament>> {
        with_conn!(self, conn => async { sqlx::query("SELECT * FROM tournaments WHERE id = $1").bind(id).fetch_optional(conn).await.map_err(db_err)?.as_ref().map(parse).transpose() }.await)
    }

    async fn list(&self) -> AppResult<Vec<Tournament>> {
        with_conn!(self, conn => async {
            sqlx::query("SELECT * FROM tournaments ORDER BY starts_at_unix_ms ASC, id ASC")
                .fetch_all(conn)
                .await
                .map_err(db_err)?
                .iter()
                .map(parse)
                .collect()
        }
        .await)
    }

    async fn transition(
        &self,
        id: &str,
        to: TournamentState,
        now: TimestampMillis,
    ) -> AppResult<Tournament> {
        with_write_tx!(self, conn => async {
            let row = sqlx::query("SELECT * FROM tournaments WHERE id = $1 FOR UPDATE").bind(id).fetch_optional(&mut *conn).await.map_err(db_err)?.ok_or_else(|| AppError::not_found("no such tournament"))?;
            let current = parse(&row)?;
            if !can_transition(current.state, to) { return Err(AppError::conflict("illegal tournament state transition")); }
            sqlx::query("UPDATE tournaments SET state = $1, updated_at_unix_ms = $2 WHERE id = $3").bind(to.as_str()).bind(ts_to_millis(now)?).bind(id).execute(conn).await.map_err(db_err)?;
            Ok(Tournament { state: to, updated_at: now, ..current })
        }.await)
    }

    async fn register(
        &self,
        tournament_id: &str,
        user_id: &str,
        now: TimestampMillis,
    ) -> AppResult<TournamentEntry> {
        with_write_tx!(self, conn => async {
            let row = sqlx::query("SELECT * FROM tournaments WHERE id = $1 FOR UPDATE").bind(tournament_id).fetch_optional(&mut *conn).await.map_err(db_err)?.ok_or_else(|| AppError::not_found("no such tournament"))?;
            let tournament = parse(&row)?;
            if tournament.state != TournamentState::RegistrationOpen || now < tournament.registration_opens_at || now >= tournament.registration_closes_at { return Err(AppError::conflict("tournament registration is closed")); }
            sqlx::query("INSERT INTO tournament_entries (tournament_id, user_id, registered_at_unix_ms) VALUES ($1, $2, $3)").bind(tournament_id).bind(user_id).bind(ts_to_millis(now)?).execute(conn).await.map_err(db_err)?;
            Ok(TournamentEntry { tournament_id: tournament_id.to_owned(), user_id: user_id.to_owned(), registered_at: now })
        }.await)
    }

    async fn entries(&self, tournament_id: &str) -> AppResult<Vec<TournamentEntry>> {
        with_conn!(self, conn => async { sqlx::query("SELECT user_id, registered_at_unix_ms FROM tournament_entries WHERE tournament_id = $1 ORDER BY user_id").bind(tournament_id).fetch_all(conn).await.map_err(db_err)?.iter().map(|row| Ok(TournamentEntry { tournament_id: tournament_id.to_owned(), user_id: get(row, "user_id")?, registered_at: millis_to_ts(get(row, "registered_at_unix_ms")?)? })).collect() }.await)
    }

    async fn results(&self, tournament_id: &str) -> AppResult<Vec<TournamentResult>> {
        with_conn!(self, conn => async { sqlx::query("SELECT user_id, rank, score, subscore FROM tournament_results WHERE tournament_id = $1 ORDER BY rank, user_id").bind(tournament_id).fetch_all(conn).await.map_err(db_err)?.iter().map(|row| Ok(TournamentResult { tournament_id: tournament_id.to_owned(), user_id: get(row, "user_id")?, rank: u64::try_from(get::<i64>(row, "rank")?).map_err(|_| AppError::internal("invalid tournament result rank"))?, score: get(row, "score")?, subscore: get(row, "subscore")? })).collect() }.await)
    }

    async fn settle_from_epoch(
        &self,
        id: &str,
        epoch: ResetEpoch,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        with_write_tx!(self, conn => async {
            let row = sqlx::query("SELECT * FROM tournaments WHERE id = $1 FOR UPDATE").bind(id).fetch_optional(&mut *conn).await.map_err(db_err)?.ok_or_else(|| AppError::not_found("no such tournament"))?;
            let tournament = parse(&row)?;
            if tournament.state == TournamentState::Completed && tournament.settled_epoch.as_ref() == Some(&epoch) { return Ok(false); }
            if tournament.state != TournamentState::Running || tournament.leaderboard_id != epoch.leaderboard_id { return Err(AppError::conflict("tournament cannot settle from this reset epoch")); }
            let due_at = ts_to_millis(epoch.due_at)?;
            if sqlx::query("SELECT 1 FROM leaderboard_reset_epochs WHERE leaderboard_id = $1 AND due_at_unix_ms = $2").bind(&epoch.leaderboard_id).bind(due_at).fetch_optional(&mut *conn).await.map_err(db_err)?.is_none() { return Err(AppError::conflict("tournament reset epoch is not committed")); }
            sqlx::query(INSERT_RESULTS_FROM_EPOCH_SQL).bind(id).bind(&epoch.leaderboard_id).bind(due_at).execute(&mut *conn).await.map_err(db_err)?;
            sqlx::query("INSERT INTO tournament_settlement_outbox (tournament_id, leaderboard_id, due_at_unix_ms, created_at_unix_ms) VALUES ($1, $2, $3, $4)").bind(id).bind(&epoch.leaderboard_id).bind(due_at).bind(ts_to_millis(now)?).execute(&mut *conn).await.map_err(db_err)?;
            sqlx::query("UPDATE tournaments SET state = 'completed', settled_due_at_unix_ms = $1, updated_at_unix_ms = $2 WHERE id = $3").bind(due_at).bind(ts_to_millis(now)?).bind(id).execute(conn).await.map_err(db_err)?;
            Ok(true)
        }.await)
    }

    async fn pending_settlement_outbox(
        &self,
        limit: usize,
    ) -> AppResult<Vec<TournamentSettlementOutboxRecord>> {
        with_conn!(self, conn => async {
            let rows = sqlx::query("SELECT tournament_id, leaderboard_id, due_at_unix_ms FROM tournament_settlement_outbox ORDER BY created_at_unix_ms, tournament_id, due_at_unix_ms LIMIT $1")
                .bind(i64::try_from(limit).map_err(|_| AppError::validation("outbox limit is too large"))?)
                .fetch_all(conn)
                .await
                .map_err(db_err)?;
            rows.iter().map(|row| Ok(TournamentSettlementOutboxRecord {
                tournament_id: get(row, "tournament_id")?,
                epoch: ResetEpoch::new(get(row, "leaderboard_id")?, millis_to_ts(get(row, "due_at_unix_ms")?)?),
            })).collect()
        }.await)
    }

    async fn acknowledge_settlement_outbox(
        &self,
        tournament_id: &str,
        epoch: &ResetEpoch,
    ) -> AppResult<()> {
        let due_at = ts_to_millis(epoch.due_at)?;
        with_write_tx!(self, conn => async {
            sqlx::query("DELETE FROM tournament_settlement_outbox WHERE tournament_id = $1 AND leaderboard_id = $2 AND due_at_unix_ms = $3")
                .bind(tournament_id)
                .bind(&epoch.leaderboard_id)
                .bind(due_at)
                .execute(conn)
                .await
                .map_err(db_err)?;
            Ok(())
        }.await)
    }
}
