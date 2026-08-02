//! End-to-end contract tests for the player account/session lifecycle routes
//! //!. The suite uses the real HTTP server and token service.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::http;
use citadel::realtime::{Gateway, Outbound, ParticipantIdentity, SessionHandle};
use citadel::services::{CreateSessionRequest, ValidateSessionRequest};
use citadel::session::{NodeId, SessionTokenSecret, SessionValidation};
use citadel::time::{Clock, DurationMillis, SystemClock};
use citadel::transport::{Envelope, TransportKind};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

struct Response {
    status: u16,
    body: Option<Value>,
}

async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&str>,
) -> Response {
    let body = body.unwrap_or("");
    let authorization = bearer
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let content = if body.is_empty() {
        String::new()
    } else {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        )
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let raw_request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{authorization}{content}Connection: close\r\n\r\n{body}"
    );
    stream
        .write_all(raw_request.as_bytes())
        .await
        .expect("write request");
    let mut raw = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
            Ok(Ok(read)) => raw.extend_from_slice(&chunk[..read]),
        }
    }
    let raw = String::from_utf8_lossy(&raw);
    let status = raw
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .expect("HTTP status");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.trim())
        .filter(|body| !body.is_empty())
        .and_then(|body| serde_json::from_str(body).ok());
    Response { status, body }
}

async fn spawn(app: App) -> (SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let (shutdown, receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let _ = http::serve(listener, app, async move {
            let _ = receiver.await;
        })
        .await;
    });
    (address, shutdown, server)
}

async fn authenticate(addr: SocketAddr, device: &str, username: &str) -> Value {
    let response = request(
        addr,
        "POST",
        http::DEVICE_AUTH_PATH,
        None,
        Some(&format!(
            r#"{{"id":"{device}","create":true,"username":"{username}"}}"#
        )),
    )
    .await;
    assert_eq!(response.status, 201);
    response.body.expect("auth response")
}

async fn run_player_lifecycle_scenario(app: App, device_prefix: &str) {
    let (addr, shutdown, server) = spawn(app).await;
    let first = authenticate(
        addr,
        &format!("{device_prefix}-1"),
        &format!("{device_prefix}-one"),
    )
    .await;
    let second = authenticate(
        addr,
        &format!("{device_prefix}-2"),
        &format!("{device_prefix}-two"),
    )
    .await;
    let access = first["token"].as_str().expect("access");
    let refresh = first["refresh_token"].as_str().expect("refresh");

    let own = request(addr, "GET", http::ACCOUNT_PATH, Some(access), None).await;
    assert_eq!(own.status, 200);
    let own = own.body.expect("profile");
    assert_eq!(own["username"], format!("{device_prefix}-one"));
    assert!(own.get("metadata").is_none());
    assert!(own.get("state").is_none());

    let updated = request(
        addr,
        "PATCH",
        http::ACCOUNT_PATH,
        Some(access),
        Some(r#"{"display_name":"A Player"}"#),
    )
    .await;
    assert_eq!(updated.status, 200);
    assert_eq!(updated.body.expect("updated")["display_name"], "A Player");

    let lookup = request(
        addr,
        "POST",
        http::PLAYER_LOOKUP_PATH,
        Some(access),
        Some(&format!(
            r#"{{"user_ids":["{}","does-not-exist"]}}"#,
            second["user_id"].as_str().expect("second user id")
        )),
    )
    .await;
    assert_eq!(lookup.status, 200);
    let users = lookup.body.expect("lookup")["users"]
        .as_array()
        .expect("users")
        .len();
    assert_eq!(users, 1, "unknown ids are omitted, not distinguished");

    let replacement = request(
        addr,
        "POST",
        http::SESSION_REFRESH_PATH,
        None,
        Some(&format!(r#"{{"refresh_token":"{refresh}"}}"#)),
    )
    .await;
    assert_eq!(replacement.status, 200);
    let replacement = replacement.body.expect("replacement");
    let replacement_access = replacement["token"].as_str().expect("replacement access");
    let replacement_refresh = replacement["refresh_token"]
        .as_str()
        .expect("replacement refresh");

    let replay = request(
        addr,
        "POST",
        http::SESSION_REFRESH_PATH,
        None,
        Some(&format!(r#"{{"refresh_token":"{refresh}"}}"#)),
    )
    .await;
    assert_eq!(replay.status, 401, "rotated refresh tokens cannot replay");

    let logout = request(
        addr,
        "POST",
        http::SESSION_LOGOUT_PATH,
        Some(replacement_access),
        Some(&format!(r#"{{"refresh_token":"{replacement_refresh}"}}"#)),
    )
    .await;
    assert_eq!(logout.status, 204);
    let retry = request(
        addr,
        "POST",
        http::SESSION_LOGOUT_PATH,
        Some(replacement_access),
        None,
    )
    .await;
    assert_eq!(retry.status, 204, "logout is idempotent");

    let revoked = request(
        addr,
        "GET",
        http::ACCOUNT_PATH,
        Some(replacement_access),
        None,
    )
    .await;
    assert_eq!(revoked.status, 401);
    let other_still_works = request(
        addr,
        "GET",
        http::ACCOUNT_PATH,
        Some(second["token"].as_str().expect("other access")),
        None,
    )
    .await;
    assert_eq!(
        other_still_works.status, 200,
        "logout cannot affect another player"
    );

    shutdown.send(()).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server stopped")
        .expect("server task");
}

#[tokio::test]
async fn player_account_lookup_refresh_and_logout_are_private_and_safe_in_memory() {
    run_player_lifecycle_scenario(App::new(Config::default()), "player-lifecycle").await;
}

#[tokio::test]
async fn logout_revokes_and_fences_only_the_exact_live_session() {
    let app = App::new(Config::default());
    let gateway = Arc::new(Gateway::new());
    app.attach_realtime_gateway(Arc::clone(&gateway));
    let (addr, shutdown, server) = spawn(app.clone()).await;
    let auth = authenticate(addr, "live-revocation-device", "live-revocation-user").await;
    let access = auth["token"].as_str().expect("access");
    let validation = app
        .session_service()
        .validate_session(ValidateSessionRequest {
            access_token: SessionTokenSecret::new(access).expect("token"),
            now: SystemClock.now(),
        })
        .await
        .expect("validate");
    assert!(validation.is_valid(), "fresh session is valid");
    let SessionValidation::Valid(validated) = validation else {
        return;
    };
    let (target_tx, mut target_rx) = tokio::sync::mpsc::channel(4);
    let target = gateway.next_participant_id();
    gateway.register_session(SessionHandle {
        id: target,
        kind: TransportKind::WebSocket,
        outbound: target_tx,
        identity: Some(ParticipantIdentity {
            user_id: validated.user_id.clone(),
            session_id: validated.session_id.clone(),
            expires_at: validated.expires_at,
        }),
    });
    assert!(gateway.registry().send_to(
        target,
        &Outbound::reliable(Envelope::new(700, b"queued".to_vec()))
    ));

    // A second session for the same account must remain live.
    let sibling = app
        .session_service()
        .create_session(CreateSessionRequest {
            user_id: validated.user_id.clone(),
            owner_node: NodeId::new(app.node_id().to_owned()).expect("node"),
            now: SystemClock.now(),
            session_ttl: DurationMillis::from_millis(60_000),
            refresh_ttl: Some(DurationMillis::from_millis(60_000)),
        })
        .await
        .expect("sibling session");
    let (sibling_tx, mut sibling_rx) = tokio::sync::mpsc::channel(4);
    let sibling_id = gateway.next_participant_id();
    gateway.register_session(SessionHandle {
        id: sibling_id,
        kind: TransportKind::WebSocket,
        outbound: sibling_tx,
        identity: Some(ParticipantIdentity {
            user_id: validated.user_id,
            session_id: sibling.session.id,
            expires_at: sibling.session.expires_at,
        }),
    });

    let logout = request(addr, "POST", http::SESSION_LOGOUT_PATH, Some(access), None).await;
    assert_eq!(logout.status, 204);
    let queued = target_rx
        .try_recv()
        .expect("queued envelope remains observable");
    assert!(
        !queued.is_deliverable(),
        "close fence discards queued delivery"
    );
    assert_eq!(
        gateway.handle_inbound(target, &Envelope::new(701, b"late".to_vec())),
        0
    );
    assert!(!gateway.registry().send_to(
        target,
        &Outbound::reliable(Envelope::new(702, b"late".to_vec()))
    ));
    assert!(gateway.registry().send_to(
        sibling_id,
        &Outbound::reliable(Envelope::new(703, b"sibling".to_vec()))
    ));
    assert_eq!(
        sibling_rx
            .recv()
            .await
            .expect("sibling outbound")
            .envelope
            .body
            .as_ref(),
        b"sibling"
    );

    shutdown.send(()).expect("shutdown");
    server.await.expect("server task");
}

#[tokio::test]
async fn player_account_lifecycle_uses_the_sqlite_backend() {
    use citadel::config::DatabaseConfig;
    use citadel::repository::{Backend, SqliteDatabase};

    let config = DatabaseConfig {
        url: Some("sqlite::memory:".to_string()),
        ..DatabaseConfig::default()
    };
    let backend: Arc<dyn Backend> = Arc::new(
        SqliteDatabase::connect(&config)
            .await
            .expect("connect + migrate SQLite"),
    );
    run_player_lifecycle_scenario(
        App::with_backend(Config::default(), backend),
        "sqlite-lifecycle",
    )
    .await;
}

mod postgres {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::{Backend, PgDatabase};

    #[tokio::test]
    async fn player_account_lifecycle_uses_the_postgres_backend_when_configured() {
        let Some(url) = std::env::var("DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
            .filter(|url| !url.trim().is_empty())
        else {
            eprintln!("skipping Postgres player lifecycle: set DATABASE_URL");
            return;
        };
        let config = DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        };
        let db = PgDatabase::connect(&config)
            .await
            .expect("connect + migrate");
        db.reset_storage_for_tests().await.expect("reset");
        let backend: Arc<dyn Backend> = Arc::new(db);
        run_player_lifecycle_scenario(
            App::with_backend(Config::default(), backend),
            "pg-lifecycle",
        )
        .await;
    }
}

mod mongodb {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::{Backend, MongoDatabase};

    #[tokio::test]
    async fn player_account_lifecycle_uses_the_mongodb_backend_when_configured() {
        let Some(url) = std::env::var("CITADEL_TEST_MONGODB_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
        else {
            eprintln!("skipping MongoDB player lifecycle: set CITADEL_TEST_MONGODB_URL");
            return;
        };
        let config = DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        };
        let db = MongoDatabase::connect(&config)
            .await
            .expect("connect + reconcile");
        db.clear_identity_session_data_for_tests()
            .await
            .expect("reset identity/session projections");
        let backend: Arc<dyn Backend> = Arc::new(db);
        run_player_lifecycle_scenario(
            App::with_backend(Config::default(), backend),
            "mongodb-lifecycle",
        )
        .await;
    }
}
