//! Console Purchases & Subscriptions sections.
//!
//! Read surfaces over the validated-purchase store, plus the console-side
//! producer that runs a receipt through the node's
//! [`ReceiptValidator`](crate::services::ReceiptValidator) (the deterministic
//! dev validator today; real store validators are follow-up work):
//!
//! - `GET /console/v1/purchases?user_id&limit` — newest-first purchases.
//! - `POST /console/v1/purchases` — validate + record a receipt (admin,
//!   audited `purchases.validate`).
//! - `GET /console/v1/purchases/{transaction_id}` — one purchase.
//! - `GET /console/v1/subscriptions?user_id&limit` — subscription rows with
//!   `active`/`expired` derived against the read-time clock.

use axum::Json;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::services::{AuditEntry, ConsoleIdentity, Purchase, PurchaseStore, SubscriptionRow};
use crate::time::{Clock, SystemClock};

/// The Purchases section route.
pub const PURCHASES_PATH: &str = "/console/v1/purchases";

/// Single-purchase route pattern.
pub const PURCHASE_DETAIL_PATH: &str = "/console/v1/purchases/:transaction_id";

/// The Subscriptions section route.
pub const SUBSCRIPTIONS_PATH: &str = "/console/v1/subscriptions";

/// Default page size.
const DEFAULT_LIMIT: usize = 50;
/// Hard ceiling on one page.
const MAX_LIMIT: usize = 200;

/// Accepted query parameters for both listing routes.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListParams {
    /// Restrict to one buying account.
    pub user_id: Option<String>,
    /// Page size (default 50, capped at 200).
    pub limit: Option<usize>,
}

/// The JSON response for the purchases listing.
#[derive(Debug, Clone, Serialize)]
pub struct PurchasesPage {
    /// Newest-first validated purchases.
    pub items: Vec<Purchase>,
}

/// The JSON response for the subscriptions listing.
#[derive(Debug, Clone, Serialize)]
pub struct SubscriptionsPage {
    /// Newest-first subscription rows with derived status.
    pub items: Vec<SubscriptionRow>,
}

/// The JSON body accepted by the validation producer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidateBody {
    /// The buying account.
    pub user_id: String,
    /// Originating store: `apple`, `google`, `huawei`, or `custom`.
    pub store: PurchaseStore,
    /// The raw receipt document (never stored; only its SHA-256 digest is).
    pub receipt: String,
}

/// `GET /console/v1/purchases`: newest-first validated purchases.
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
    Query(params): Query<ListParams>,
) -> Result<Json<PurchasesPage>, ApiError> {
    app.metrics().record_http_request();
    let items = app
        .purchases()
        .purchases(
            params.user_id.as_deref(),
            params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
        )
        .await?;
    Ok(Json(PurchasesPage { items }))
}

/// `POST /console/v1/purchases`: validate + record a receipt (admin).
pub(super) async fn validate_handler(
    State(app): State<App>,
    operator: ConsoleIdentity,
    body: Result<Json<ValidateBody>, JsonRejection>,
) -> Result<(StatusCode, Json<Purchase>), ApiError> {
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
    let purchase = app
        .purchases()
        .validate_and_record(&body.user_id, body.store, &body.receipt, now)
        .await?;
    app.audit_log().record(AuditEntry::new(
        now,
        operator.username,
        operator.role.as_str(),
        "purchases.validate",
        format!("transaction {}", purchase.transaction_id),
        format!(
            "recorded {} purchase of {} for {}",
            purchase.store.as_str(),
            purchase.product_id,
            purchase.user_id
        ),
    ));
    Ok((StatusCode::CREATED, Json(purchase)))
}

/// `GET /console/v1/purchases/{transaction_id}`: one purchase.
pub(super) async fn detail_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
    Path(transaction_id): Path<String>,
) -> Result<Json<Purchase>, ApiError> {
    app.metrics().record_http_request();
    app.purchases()
        .get(&transaction_id)
        .await?
        .map(Json)
        .ok_or_else(|| AppError::not_found("purchase not found").into())
}

/// `GET /console/v1/subscriptions`: subscription rows with derived status.
pub(super) async fn subscriptions_handler(
    State(app): State<App>,
    _operator: ConsoleIdentity,
    Query(params): Query<ListParams>,
) -> Result<Json<SubscriptionsPage>, ApiError> {
    app.metrics().record_http_request();
    let items = app
        .purchases()
        .subscriptions(
            params.user_id.as_deref(),
            params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
            SystemClock.now(),
        )
        .await?;
    Ok(Json(SubscriptionsPage { items }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn purchases_paths_are_registered_sections() {
        assert!(super::super::SECTION_PATHS.contains(&PURCHASES_PATH));
        assert!(super::super::SECTION_PATHS.contains(&SUBSCRIPTIONS_PATH));
        assert!(PURCHASE_DETAIL_PATH.starts_with(PURCHASES_PATH));
    }

    #[test]
    fn validate_body_rejects_unknown_fields_and_stores() {
        assert!(
            serde_json::from_str::<ValidateBody>(
                r#"{"user_id":"u","store":"custom","receipt":"{}","extra":1}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ValidateBody>(
                r#"{"user_id":"u","store":"steam","receipt":"{}"}"#
            )
            .is_err(),
            "unknown store rejected"
        );
        let ok: ValidateBody =
            serde_json::from_str(r#"{"user_id":"u","store":"apple","receipt":"{}"}"#)
                .expect("parse");
        assert_eq!(ok.store, PurchaseStore::Apple);
    }
}
