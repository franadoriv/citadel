use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use crate::error::{AppError, AppResult};
use crate::repository::api_keys::{decode_scopes_json, validate_stored_key};
use crate::repository::{ApiKeyId, ApiKeyRepository, ApiKeyScope, ApiKeyVerifier, StoredApiKey};
use crate::time::TimestampMillis;

use super::{db_err, millis_to_ts, ts_to_millis};

pub struct SqliteApiKeyRepository {
    pool: SqlitePool,
}

impl SqliteApiKeyRepository {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn scopes_json(scopes: &[ApiKeyScope]) -> AppResult<String> {
    serde_json::to_string(scopes).map_err(|_| AppError::internal("failed to encode API key scopes"))
}

fn decode(row: &sqlx::sqlite::SqliteRow) -> AppResult<StoredApiKey> {
    let scopes = decode_scopes_json(&row.try_get::<String, _>("scopes_json").map_err(db_err)?)?;
    let optional_ts = |column| -> AppResult<Option<TimestampMillis>> {
        row.try_get::<Option<i64>, _>(column)
            .map_err(db_err)?
            .map(millis_to_ts)
            .transpose()
    };
    let key = StoredApiKey {
        id: ApiKeyId::new(row.try_get::<String, _>("id").map_err(db_err)?)?,
        name: row.try_get("name").map_err(db_err)?,
        scopes,
        verifier: ApiKeyVerifier::from_slice(
            &row.try_get::<Vec<u8>, _>("secret_verifier")
                .map_err(db_err)?,
        )?,
        generation: u64::try_from(row.try_get::<i64, _>("generation").map_err(db_err)?)
            .map_err(|_| AppError::internal("invalid API key generation"))?,
        created_at: millis_to_ts(row.try_get("created_at_ms").map_err(db_err)?)?,
        expires_at: optional_ts("expires_at_ms")?,
        revoked_at: optional_ts("revoked_at_ms")?,
        last_used_at: optional_ts("last_used_at_ms")?,
    };
    validate_stored_key(&key)?;
    Ok(key)
}

const COLUMNS: &str = "id,name,scopes_json,secret_verifier,generation,created_at_ms,expires_at_ms,revoked_at_ms,last_used_at_ms";

#[async_trait]
impl ApiKeyRepository for SqliteApiKeyRepository {
    async fn create(&self, key: StoredApiKey) -> AppResult<()> {
        validate_stored_key(&key)?;
        sqlx::query("INSERT INTO api_keys(id,name,scopes_json,secret_verifier,generation,created_at_ms,expires_at_ms,revoked_at_ms,last_used_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
            .bind(key.id.as_str())
            .bind(key.name)
            .bind(scopes_json(&key.scopes)?)
            .bind(key.verifier.as_bytes().as_slice())
            .bind(i64::try_from(key.generation).map_err(|_| AppError::internal("API key generation out of range"))?)
            .bind(ts_to_millis(key.created_at)?)
            .bind(key.expires_at.map(ts_to_millis).transpose()?)
            .bind(key.revoked_at.map(ts_to_millis).transpose()?)
            .bind(key.last_used_at.map(ts_to_millis).transpose()?)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn get(&self, id: &ApiKeyId) -> AppResult<Option<StoredApiKey>> {
        let sql = format!("SELECT {COLUMNS} FROM api_keys WHERE id=$1");
        sqlx::query(&sql)
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?
            .as_ref()
            .map(decode)
            .transpose()
    }

    async fn list(&self) -> AppResult<Vec<StoredApiKey>> {
        let sql = format!("SELECT {COLUMNS} FROM api_keys ORDER BY created_at_ms DESC,id");
        sqlx::query(&sql)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?
            .iter()
            .map(decode)
            .collect()
    }

    async fn rotate(
        &self,
        id: &ApiKeyId,
        expected_generation: u64,
        verifier: ApiKeyVerifier,
        at: TimestampMillis,
    ) -> AppResult<StoredApiKey> {
        let sql = format!(
            "UPDATE api_keys SET secret_verifier=$1,generation=generation+1 \
             WHERE id=$2 AND generation=$3 AND revoked_at_ms IS NULL \
             AND created_at_ms<=$4 AND (expires_at_ms IS NULL OR expires_at_ms>$4) \
             RETURNING {COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(verifier.as_bytes().as_slice())
            .bind(id.as_str())
            .bind(
                i64::try_from(expected_generation)
                    .map_err(|_| AppError::conflict("API key state changed"))?,
            )
            .bind(ts_to_millis(at)?)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::conflict("API key state changed"))?;
        decode(&row)
    }

    async fn revoke(
        &self,
        id: &ApiKeyId,
        expected_generation: u64,
        at: TimestampMillis,
    ) -> AppResult<StoredApiKey> {
        let generation = i64::try_from(expected_generation)
            .map_err(|_| AppError::conflict("API key state changed"))?;
        let sql = format!(
            "UPDATE api_keys SET revoked_at_ms=$1 WHERE id=$2 AND generation=$3 \
             AND created_at_ms<=$1 AND revoked_at_ms IS NULL RETURNING {COLUMNS}"
        );
        if let Some(row) = sqlx::query(&sql)
            .bind(ts_to_millis(at)?)
            .bind(id.as_str())
            .bind(generation)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?
        {
            return decode(&row);
        }
        let sql = format!(
            "SELECT {COLUMNS} FROM api_keys WHERE id=$1 AND generation=$2 \
             AND created_at_ms<=$3 AND revoked_at_ms IS NOT NULL"
        );
        let row = sqlx::query(&sql)
            .bind(id.as_str())
            .bind(generation)
            .bind(ts_to_millis(at)?)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?
            .ok_or_else(|| AppError::conflict("API key state changed"))?;
        decode(&row)
    }

    async fn update_last_used(
        &self,
        id: &ApiKeyId,
        expected_generation: u64,
        at: TimestampMillis,
    ) -> AppResult<()> {
        sqlx::query(
            "UPDATE api_keys SET last_used_at_ms=MAX(COALESCE(last_used_at_ms,0),$1) \
             WHERE id=$2 AND generation=$3 AND revoked_at_ms IS NULL \
             AND created_at_ms<=$1 AND (expires_at_ms IS NULL OR expires_at_ms>$1)",
        )
        .bind(ts_to_millis(at)?)
        .bind(id.as_str())
        .bind(
            i64::try_from(expected_generation)
                .map_err(|_| AppError::internal("invalid API key generation"))?,
        )
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }
}
