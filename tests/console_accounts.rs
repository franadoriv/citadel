//! Integration tests for console Accounts administration.
//!
//! Drives the full operator flow over the real server: create, list/search,
//! detail with linked identities, ban (auth rejection + session revocation),
//! unban, edit, export, and logical delete. The same scenario runs against the
//! in-memory backend and the SQLite backend, so the `list_users` repository
//! extension is exercised on both.

use std::net::SocketAddr;
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::http;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

struct Response {
    status: u16,
    body: Option<Value>,
}

async fn read_http_response(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) | Ok(Err(_)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn parse_response(raw: &str) -> Response {
    let status = raw
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .expect("http status");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .filter(|b| !b.trim().is_empty())
        .and_then(|b| serde_json::from_str::<Value>(b.trim()).ok());
    Response { status, body }
}

/// Raw HTTP request with optional bearer + optional JSON body.
async fn send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: Option<&str>,
    body: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let auth = bearer
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let body_text = body.unwrap_or("");
    let content = if body.is_some() {
        format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body_text.len()
        )
    } else {
        String::new()
    };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{auth}{content}Connection: close\r\n\r\n{body_text}"
    );
    stream.write_all(request.as_bytes()).await.expect("write");
    let raw = read_http_response(&mut stream).await;
    parse_response(&raw)
}

async fn spawn_server(app: App) -> (SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let _ = http::serve(listener, app, async move {
            let _ = rx.await;
        })
        .await;
    });
    (addr, tx, server)
}

fn console_config() -> Config {
    let mut config = Config::default();
    config.console.username = "ops".to_string();
    config.console.password = "operator-secret".to_string();
    config.console.viewer_password = Some("viewer-secret".to_string());
    config.validate().expect("valid test config");
    config
}

async fn admin_token(addr: SocketAddr) -> String {
    let response = send(
        addr,
        "POST",
        http::LOGIN_PATH,
        None,
        Some(r#"{"username":"ops","password":"operator-secret"}"#),
    )
    .await;
    response.body.expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string()
}

/// The full accounts-administration scenario against an assembled `App`.
async fn run_accounts_scenario(app: App, device_id: &str) {
    let (addr, tx, server) = spawn_server(app).await;
    let token = admin_token(addr).await;

    // 1. Console-created account appears in the listing and search.
    let created = send(
        addr,
        "POST",
        "/console/v1/accounts",
        Some(&token),
        Some(r#"{"username":"console-born","display_name":"Console Born"}"#),
    )
    .await;
    assert_eq!(created.status, 201);
    let created = created.body.expect("created");
    let console_user_id = created["id"].as_str().expect("id").to_string();
    assert_eq!(created["state"], "active");

    // 2. A player registers via device auth (creates account + device link).
    let registered = send(
        addr,
        "POST",
        http::DEVICE_AUTH_PATH,
        None,
        Some(&format!(
            r#"{{"id":"{device_id}","create":true,"username":"player-one"}}"#
        )),
    )
    .await;
    assert_eq!(registered.status, 201);
    let player_id = registered.body.expect("auth")["user_id"]
        .as_str()
        .expect("user_id")
        .to_string();

    // 3. Listing shows both; substring search narrows.
    let listed = send(addr, "GET", "/console/v1/accounts", Some(&token), None).await;
    assert_eq!(listed.status, 200);
    let listed = listed.body.expect("list");
    assert_eq!(listed["total"], 2);
    let filtered = send(
        addr,
        "GET",
        "/console/v1/accounts?filter=player",
        Some(&token),
        None,
    )
    .await;
    let filtered = filtered.body.expect("filtered");
    assert_eq!(filtered["total"], 1);
    assert_eq!(filtered["items"][0]["username"], "player-one");

    // 4. Detail shows the linked device identity.
    let detail = send(
        addr,
        "GET",
        &format!("/console/v1/accounts/{player_id}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(detail.status, 200);
    let detail = detail.body.expect("detail");
    let identities = detail["identities"].as_array().expect("identities");
    assert_eq!(identities.len(), 1);
    assert_eq!(identities[0]["provider"], "device");
    assert_eq!(identities[0]["external_id"], device_id);

    // 5. Ban: the account state flips and device auth is rejected uniformly.
    let banned = send(
        addr,
        "POST",
        &format!("/console/v1/accounts/{player_id}/ban"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(banned.status, 200);
    assert_eq!(banned.body.expect("banned")["state"], "disabled");
    let rejected = send(
        addr,
        "POST",
        http::DEVICE_AUTH_PATH,
        None,
        Some(&format!(r#"{{"id":"{device_id}"}}"#)),
    )
    .await;
    assert_eq!(rejected.status, 401, "banned account cannot authenticate");

    // 6. Unban restores login.
    let unbanned = send(
        addr,
        "POST",
        &format!("/console/v1/accounts/{player_id}/unban"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(unbanned.body.expect("unbanned")["state"], "active");
    let back = send(
        addr,
        "POST",
        http::DEVICE_AUTH_PATH,
        None,
        Some(&format!(r#"{{"id":"{device_id}"}}"#)),
    )
    .await;
    assert_eq!(back.status, 200, "unbanned account logs in again");

    // 7. Edit profile fields.
    let edited = send(
        addr,
        "PUT",
        &format!("/console/v1/accounts/{console_user_id}"),
        Some(&token),
        Some(r#"{"username":"console-renamed","metadata":{"vip":true}}"#),
    )
    .await;
    assert_eq!(edited.status, 200);
    assert_eq!(edited.body.expect("edited")["username"], "console-renamed");

    // 8. Export carries profile + metadata + identities.
    let export = send(
        addr,
        "GET",
        &format!("/console/v1/accounts/{console_user_id}/export"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(export.status, 200);
    let export = export.body.expect("export");
    assert_eq!(export["metadata"]["vip"], true);

    // 9. Logical delete: tombstoned, credentials unlinked, auth dead.
    let deleted = send(
        addr,
        "DELETE",
        &format!("/console/v1/accounts/{player_id}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(deleted.status, 204);
    let gone = send(
        addr,
        "GET",
        &format!("/console/v1/accounts/{player_id}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(gone.body.expect("gone")["state"], "tombstoned");
    let dead = send(
        addr,
        "POST",
        http::DEVICE_AUTH_PATH,
        None,
        Some(&format!(r#"{{"id":"{device_id}"}}"#)),
    )
    .await;
    assert_eq!(dead.status, 401, "tombstoned account cannot authenticate");

    // 10. Viewer role cannot mutate; audit trail carries the actions.
    let viewer = send(
        addr,
        "POST",
        http::LOGIN_PATH,
        None,
        Some(r#"{"username":"ops","password":"viewer-secret"}"#),
    )
    .await;
    let viewer_token = viewer.body.expect("viewer")["token"]
        .as_str()
        .expect("token")
        .to_string();
    let forbidden = send(
        addr,
        "POST",
        "/console/v1/accounts",
        Some(&viewer_token),
        Some(r#"{"username":"nope"}"#),
    )
    .await;
    assert_eq!(forbidden.status, 403);

    let audit = send(
        addr,
        "GET",
        "/console/v1/audit?action=accounts",
        Some(&token),
        None,
    )
    .await;
    let actions: Vec<String> = audit.body.expect("audit")["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| e["action"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        actions,
        vec![
            "accounts.delete",
            "accounts.update",
            "accounts.unban",
            "accounts.ban",
            "accounts.create",
        ],
        "newest-first accounts trail"
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn wallet_and_friends_panels() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;
    let token = admin_token(addr).await;

    // Two console-created accounts to relate.
    let mut ids = Vec::new();
    for name in ["wallet-owner", "wallet-buddy"] {
        let created = send(
            addr,
            "POST",
            "/console/v1/accounts",
            Some(&token),
            Some(&format!(r#"{{"username":"{name}"}}"#)),
        )
        .await;
        assert_eq!(created.status, 201);
        ids.push(
            created.body.expect("created")["id"]
                .as_str()
                .expect("id")
                .to_string(),
        );
    }
    let (owner, buddy) = (&ids[0], &ids[1]);

    // Wallet: credit, debit, overdraft rejection, ledger.
    let credited = send(
        addr,
        "POST",
        &format!("/console/v1/accounts/{owner}/wallet"),
        Some(&token),
        Some(r#"{"currency":"coins","delta":100,"reason":"grant"}"#),
    )
    .await;
    assert_eq!(credited.status, 200);
    assert_eq!(credited.body.expect("wallet")["balances"]["coins"], 100);
    let debited = send(
        addr,
        "POST",
        &format!("/console/v1/accounts/{owner}/wallet"),
        Some(&token),
        Some(r#"{"currency":"coins","delta":-40}"#),
    )
    .await;
    let debited = debited.body.expect("wallet");
    assert_eq!(debited["balances"]["coins"], 60);
    assert_eq!(debited["ledger"][0]["delta"], -40, "newest first");
    let overdraft = send(
        addr,
        "POST",
        &format!("/console/v1/accounts/{owner}/wallet"),
        Some(&token),
        Some(r#"{"currency":"coins","delta":-100}"#),
    )
    .await;
    assert_eq!(overdraft.status, 409, "overdraft rejected");
    let read_back = send(
        addr,
        "GET",
        &format!("/console/v1/accounts/{owner}/wallet"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(read_back.body.expect("wallet")["balances"]["coins"], 60);

    // Friends: add from one side (invite), complete from the other, remove.
    let invited = send(
        addr,
        "POST",
        &format!("/console/v1/accounts/{owner}/friends"),
        Some(&token),
        Some(&format!(r#"{{"user_id":"{buddy}"}}"#)),
    )
    .await;
    assert_eq!(invited.status, 200);
    assert_eq!(invited.body.expect("friends")[0]["state"], "invited_sent");
    let accepted = send(
        addr,
        "POST",
        &format!("/console/v1/accounts/{buddy}/friends"),
        Some(&token),
        Some(&format!(r#"{{"user_id":"{owner}"}}"#)),
    )
    .await;
    assert_eq!(accepted.body.expect("friends")[0]["state"], "friend");
    let removed = send(
        addr,
        "DELETE",
        &format!("/console/v1/accounts/{owner}/friends/{buddy}"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(removed.status, 204);
    let empty = send(
        addr,
        "GET",
        &format!("/console/v1/accounts/{owner}/friends"),
        Some(&token),
        None,
    )
    .await;
    assert_eq!(
        empty.body.expect("friends").as_array().expect("rows").len(),
        0
    );

    // Panels 404 for unknown accounts; friends with unknown other 404s too.
    assert_eq!(
        send(
            addr,
            "GET",
            "/console/v1/accounts/u-nope/wallet",
            Some(&token),
            None
        )
        .await
        .status,
        404
    );

    // Viewer cannot adjust wallets.
    let viewer = send(
        addr,
        "POST",
        http::LOGIN_PATH,
        None,
        Some(r#"{"username":"ops","password":"viewer-secret"}"#),
    )
    .await;
    let viewer_token = viewer.body.expect("viewer")["token"]
        .as_str()
        .expect("token")
        .to_string();
    let forbidden = send(
        addr,
        "POST",
        &format!("/console/v1/accounts/{owner}/wallet"),
        Some(&viewer_token),
        Some(r#"{"currency":"coins","delta":1}"#),
    )
    .await;
    assert_eq!(forbidden.status, 403);

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn accounts_admin_over_in_memory_backend() {
    run_accounts_scenario(App::new(console_config()), "console-dev-mem").await;
}

#[tokio::test]
async fn accounts_admin_over_sqlite_backend() {
    let mut config = console_config();
    config.database.url = Some("sqlite::memory:".to_string());
    let app = App::bootstrap(config).await.expect("bootstrap sqlite");
    assert_eq!(
        app.backend_kind(),
        citadel::repository::BackendKind::Sqlite,
        "scenario must exercise the SQLite repositories"
    );
    run_accounts_scenario(app, "console-dev-sqlite").await;
}
