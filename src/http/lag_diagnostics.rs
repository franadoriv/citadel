//! State-gated HTTP ingestion for private lag-capture artifacts.
//!
//! The route is deliberately permanent: opening and closing public paths is
//! race-prone and makes deployment configuration hard to audit. Instead every
//! request must carry a signed, one-use FLUSH capability whose capture is in
//! the server-owned `Flushing` state.

use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use futures_util::StreamExt as _;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::app::App;
use crate::lag_analysis::{AnalysisOptions, ArtifactAnalysisRequest};
use crate::lag_diagnostics::LagDiagnosticsError;
use crate::time::{Clock, SystemClock};

pub use citadel_wire::diagnostics::DIAGNOSTICS_UPLOAD_PATH as LAG_CAPTURE_UPLOAD_PATH;

pub(super) fn routes() -> Router<App> {
    Router::new()
        .route(
            LAG_CAPTURE_UPLOAD_PATH,
            post(upload_handler).options(preflight_handler),
        )
        // This endpoint applies its own token-bound streaming byte cap. Axum's
        // global JSON-oriented default must not buffer or impose a smaller,
        // inconsistent size limit before that boundary sees a chunk.
        .layer(DefaultBodyLimit::disable())
}

async fn preflight_handler(State(app): State<App>, headers: HeaderMap) -> Response {
    app.metrics().record_http_request();
    let Some(origin) = header_value(&headers, header::ORIGIN) else {
        return rejected();
    };
    if !app.lag_diagnostics().cors_origin_allowed(origin)
        || header_value(&headers, header::ACCESS_CONTROL_REQUEST_METHOD) != Some("POST")
    {
        return rejected();
    }
    cors_response(StatusCode::NO_CONTENT, origin)
}

async fn upload_handler(State(app): State<App>, headers: HeaderMap, body: Body) -> Response {
    app.metrics().record_http_request();
    let origin = header_value(&headers, header::ORIGIN);
    let content_length = match header_value(&headers, header::CONTENT_LENGTH) {
        Some(value) => match value.parse::<u64>() {
            Ok(value) => Some(value),
            Err(_) => return rejected(),
        },
        None => None,
    };
    let now = SystemClock.now();
    // Retention is a two-stage operation: first durably project the exact
    // source artifact as unavailable, then remove its private bytes. Run the
    // reusable reconciliation before accepting another upload so a failed
    // filesystem deletion can never leave a report advertising raw evidence
    // that has already expired.
    if app.reconcile_lag_raw_retention(now).await.is_err() {
        return rejected();
    }
    let service = app.lag_diagnostics();
    if service.expire_deadlines(now).is_err() {
        return rejected();
    }
    let lease = match service.begin_upload(
        header_value(&headers, header::AUTHORIZATION),
        header_value(&headers, header::CONTENT_TYPE),
        header_value(&headers, header::CONTENT_ENCODING),
        content_length,
        origin,
        now,
    ) {
        Ok(lease) => lease,
        Err(_) => return rejected(),
    };

    let mut file = match tokio::fs::OpenOptions::new()
        .write(true)
        .open(lease.staging_path())
        .await
    {
        Ok(file) => file,
        Err(_) => {
            service.reject_upload(&lease, now);
            return rejected();
        }
    };
    let mut bytes = 0_u64;
    let mut digest = Sha256::new();
    let mut stream = body.into_data_stream();
    let mut stream_error = false;
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            stream_error = true;
            break;
        };
        let next = bytes.saturating_add(chunk.len() as u64);
        if next > lease.compressed_cap() || file.write_all(&chunk).await.is_err() {
            stream_error = true;
            break;
        }
        bytes = next;
        digest.update(&chunk);
    }
    if file.flush().await.is_err() {
        stream_error = true;
    }
    drop(file);
    if stream_error {
        service.reject_upload(&lease, now);
        return rejected();
    }
    let digest: [u8; 32] = digest.finalize().into();
    let service = std::sync::Arc::clone(service);
    let result = tokio::task::spawn_blocking(move || {
        service.validate_and_publish(lease, bytes, digest, now)
    })
    .await;
    match result {
        Ok(Ok(receipt)) => {
            if !receipt.analysis_artifact_ids.is_empty() {
                queue_optional_analysis(app.clone(), receipt.analysis_artifact_ids, now);
            }
            origin.map_or_else(
                || StatusCode::NO_CONTENT.into_response(),
                |value| cors_response(StatusCode::NO_CONTENT, value),
            )
        }
        Ok(Err(LagDiagnosticsError::Storage)) | Err(_) => rejected(),
        Ok(Err(_)) => rejected(),
    }
}

/// Queue only the developer-requested report work after the private artifact
/// has been made durable. Upload acknowledgement never waits on decoding or
/// database I/O, and a failed report job leaves the raw retention lifecycle
/// untouched for an operator-controlled retry.
fn queue_optional_analysis(app: App, artifact_ids: Vec<String>, now: crate::time::TimestampMillis) {
    tokio::spawn(async move {
        for artifact_id in artifact_ids {
            // The worker intentionally has no implicit waiting queue. Retrying
            // here is explicit, bounded, and does not retain raw bytes; it
            // prevents a normal completion burst from silently losing every
            // report merely because all decoder slots were occupied at the
            // instant the upload sealed.
            let mut delay = std::time::Duration::from_millis(25);
            let mut final_outcome = crate::lag_analysis::AnalysisWorkResult::Busy;
            for attempt in 0..8_u8 {
                let outcome = app
                    .lag_analysis_worker()
                    .analyze_artifact_async(
                        std::sync::Arc::clone(app.lag_diagnostics()),
                        ArtifactAnalysisRequest {
                            artifact_id: artifact_id.clone(),
                            analyze: true,
                            options: AnalysisOptions::default(),
                        },
                        now,
                    )
                    .await;
                if matches!(outcome, crate::lag_analysis::AnalysisWorkResult::Busy) && attempt < 7 {
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                    continue;
                }
                final_outcome = app.persist_lag_analysis(outcome).await;
                break;
            }
            if matches!(
                final_outcome,
                crate::lag_analysis::AnalysisWorkResult::Busy
                    | crate::lag_analysis::AnalysisWorkResult::Failed
                    | crate::lag_analysis::AnalysisWorkResult::RawUnavailable
            ) {
                tracing::warn!(
                    "lag diagnostics report analysis was not completed; raw artifact remains eligible for an admin retry"
                );
            }
        }
    });
}

fn header_value(headers: &HeaderMap, name: header::HeaderName) -> Option<&str> {
    headers.get(name)?.to_str().ok()
}

fn rejected() -> Response {
    // One response shape for disabled, malformed, expired, foreign, replayed,
    // wrong-state, body-limit, and parser failures. This is intentionally not a
    // JSON API error because an attacker should not learn capture lifecycle.
    StatusCode::NOT_FOUND.into_response()
}

fn cors_response(status: StatusCode, origin: &str) -> Response {
    let mut response = status.into_response();
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(origin) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, value);
    }
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("POST"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type, content-encoding"),
    );
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    response
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write as _;
    use std::path::PathBuf;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::Request;
    use base64::Engine as _;
    use citadel_wire::diagnostics::{UploadContentEncoding, UploadContentType};
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use tower::ServiceExt as _;
    use uuid::Uuid;

    use super::*;
    use crate::config::{Config, LagDiagnosticsConfig};
    use crate::lag_analysis::LagReportRepository;
    use crate::lag_diagnostics::{CaptureFlushPlan, CaptureParticipant};
    use crate::time::{Clock, TimestampMillis};

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("citadel-http-lag-{}", Uuid::new_v4())))
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn enabled_app(root: &TestRoot) -> App {
        let mut config = Config::default();
        let mut keys = BTreeMap::new();
        keys.insert(
            "current".to_string(),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([4_u8; 32]),
        );
        config.lag_diagnostics = LagDiagnosticsConfig {
            enabled: true,
            raw_root: Some(root.0.display().to_string()),
            active_key_id: Some("current".to_string()),
            upload_hmac_keys: keys,
            allowed_origins: vec!["https://game.example".to_string()],
            max_compressed_bytes: 1024 * 1024,
            max_decompressed_bytes: 1024 * 1024,
            max_decompression_ratio: 32,
            max_concurrent_uploads: 2,
            max_raw_bytes: 4 * 1024 * 1024,
            retention_hours: 1,
            shared_raw_store: false,
        };
        App::new(config)
    }

    fn gzip_clag(capture: citadel_wire::diagnostics::CaptureId) -> Vec<u8> {
        // A zero-record capture is a valid `no_data` analysis input. It keeps
        // this transport test focused on the upload-to-report hand-off rather
        // than inventing a malformed movement observation.
        let mut plain = vec![0_u8; 128];
        plain[0..4].copy_from_slice(b"CLAG");
        plain[4..6].copy_from_slice(&1_u16.to_be_bytes());
        plain[6..8].copy_from_slice(&128_u16.to_be_bytes());
        plain[8..10].copy_from_slice(&48_u16.to_be_bytes());
        plain[10..12].copy_from_slice(&0x0005_u16.to_be_bytes());
        plain[48..64].copy_from_slice(&capture.bytes());
        plain[64..68].copy_from_slice(&1_u32.to_be_bytes());
        plain[72..80].copy_from_slice(&1_u64.to_be_bytes());
        let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
        gzip.write_all(&plain).expect("gzip input");
        gzip.finish().expect("gzip finish")
    }

    #[tokio::test]
    async fn permanent_route_is_state_gated_and_streams_a_valid_chunked_upload() {
        let disabled = super::super::router(App::new(Config::default()));
        let disabled_response = disabled
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(LAG_CAPTURE_UPLOAD_PATH)
                    .body(Body::from("ignored"))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(disabled_response.status(), StatusCode::NOT_FOUND);

        let root = TestRoot::new();
        let app = enabled_app(&root);
        let capture = citadel_wire::diagnostics::CaptureId::new([8; 16]).expect("capture");
        let now = SystemClock.now().unix_millis();
        app.lag_diagnostics()
            .register_recording(capture, 1, now + 10_000)
            .expect("recording");
        let grant = app
            .lag_diagnostics()
            .open_flush(
                CaptureFlushPlan {
                    capture_id: capture,
                    generation: 1,
                    attempt_id: 1,
                    upload_deadline_server_utc_ms: now + 5_000,
                    max_compressed_bytes: 1024 * 1024,
                    required_uploads: 1,
                    analyze: true,
                    participants: vec![CaptureParticipant {
                        participant_id: 1,
                        session_id: "session-1".to_string(),
                        tenant_id: "tenant-a".to_string(),
                        match_id: "match-a".to_string(),
                    }],
                },
                TimestampMillis::from_unix_millis(now),
            )
            .expect("flush")
            .pop()
            .expect("grant");
        let payload = gzip_clag(capture);
        let response = super::super::router(app.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(LAG_CAPTURE_UPLOAD_PATH)
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", grant.flush.upload_token),
                    )
                    .header(
                        header::CONTENT_TYPE,
                        UploadContentType::CitadelLagCapture.as_str(),
                    )
                    .header(
                        header::CONTENT_ENCODING,
                        UploadContentEncoding::Gzip.as_str(),
                    )
                    .header(header::ORIGIN, "https://game.example")
                    // No Content-Length: this exercises the per-chunk cap rather
                    // than relying on a fixed buffered request extractor.
                    .body(Body::from(payload))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("https://game.example"))
        );
        assert!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .is_none()
        );
        // Analysis is queued after acknowledging the stream upload. The
        // in-memory test store lets this exercise the same bounded worker
        // path; durable backends persist the resulting immutable row through
        // `App::persist_lag_analysis`.
        for _ in 0..200 {
            if !app.lag_reports().list(None, 1).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(app.lag_reports().list(None, 2).len(), 1);
    }

    #[tokio::test]
    async fn replayed_http_grant_is_indistinguishable_from_an_unavailable_route() {
        let root = TestRoot::new();
        let app = enabled_app(&root);
        let capture = citadel_wire::diagnostics::CaptureId::new([9; 16]).expect("capture");
        let now = SystemClock.now().unix_millis();
        app.lag_diagnostics()
            .register_recording(capture, 1, now + 10_000)
            .expect("recording");
        let grant = app
            .lag_diagnostics()
            .open_flush(
                CaptureFlushPlan {
                    capture_id: capture,
                    generation: 1,
                    attempt_id: 1,
                    upload_deadline_server_utc_ms: now + 5_000,
                    max_compressed_bytes: 1024 * 1024,
                    required_uploads: 1,
                    analyze: false,
                    participants: vec![CaptureParticipant {
                        participant_id: 1,
                        session_id: "session-1".to_string(),
                        tenant_id: "tenant-a".to_string(),
                        match_id: "match-a".to_string(),
                    }],
                },
                TimestampMillis::from_unix_millis(now),
            )
            .expect("flush")
            .pop()
            .expect("grant");
        let payload = gzip_clag(capture);
        let router = super::super::router(app.clone());
        for expected in [StatusCode::NO_CONTENT, StatusCode::NOT_FOUND] {
            let response = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(LAG_CAPTURE_UPLOAD_PATH)
                        .header(
                            header::AUTHORIZATION,
                            format!("Bearer {}", grant.flush.upload_token),
                        )
                        .header(
                            header::CONTENT_TYPE,
                            UploadContentType::CitadelLagCapture.as_str(),
                        )
                        .header(
                            header::CONTENT_ENCODING,
                            UploadContentEncoding::Gzip.as_str(),
                        )
                        .body(Body::from(payload.clone()))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), expected);
        }
        // `analyze=false` preserves the accepted raw artifact but must never
        // enqueue or create a derived report row.
        assert!(app.lag_reports().list(None, 1).is_empty());
    }

    #[tokio::test]
    async fn preflight_never_uses_wildcard_cors() {
        let root = TestRoot::new();
        let app = enabled_app(&root);
        let response = super::super::router(app)
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri(LAG_CAPTURE_UPLOAD_PATH)
                    .header(header::ORIGIN, "https://game.example")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_ne!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("*"))
        );
    }
}
