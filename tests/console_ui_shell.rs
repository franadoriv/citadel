//! Integration tests for the live console SPA shell.
//!
//! Binds an ephemeral port, runs the real server, and verifies the served
//! `/dashboard` document is the fully live console (login wired to
//! `/console/v1/login`, no placeholder affordances left), then smoke-tests
//! that the endpoints the SPA drives (accounts, audit) answer an
//! authenticated operator — proving the UI's data sources exist end to end.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::error::AppError;
use citadel::error_journal::{ErrorJournal, JournalIncident};
use citadel::http;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

static NEXT_JOURNAL_PATH: AtomicU64 = AtomicU64::new(0);

/// A parsed HTTP response: status code and the raw (decoded) body text.
struct RawResponse {
    status: u16,
    body: String,
}

async fn read_http_response(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk)).await {
            Ok(Ok(0)) | Err(_) | Ok(Err(_)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn parse_raw(raw: &str) -> RawResponse {
    let status = raw
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .expect("response should carry an HTTP status code");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    RawResponse { status, body }
}

/// Issue a raw HTTP/1.1 `GET`, optionally with a bearer token.
async fn get(addr: SocketAddr, path: &str, bearer: Option<&str>) -> RawResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to server");
    let auth_header = bearer
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n{auth_header}Connection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let raw = read_http_response(&mut stream).await;
    parse_raw(&raw)
}

/// Issue a raw HTTP/1.1 `POST` with a JSON body.
async fn post_json(addr: SocketAddr, path: &str, body: &str) -> RawResponse {
    let mut stream = TcpStream::connect(addr).await.expect("connect to server");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let raw = read_http_response(&mut stream).await;
    parse_raw(&raw)
}

/// Spawn the real server on an ephemeral loopback port for `app`.
async fn spawn_server(app: App) -> (SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    let (tx, rx) = oneshot::channel::<()>();
    let shutdown = async move {
        let _ = rx.await;
    };
    let server = tokio::spawn(async move {
        let _ = http::serve(listener, app, shutdown).await;
    });
    (addr, tx, server)
}

/// A config with non-default operator credentials.
fn console_config() -> Config {
    let mut config = Config::default();
    config.console.username = "ops".to_string();
    config.console.password = "operator-secret".to_string();
    config.validate().expect("test config must validate");
    config
}

fn test_journal_path() -> PathBuf {
    let nonce = NEXT_JOURNAL_PATH.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "citadel-console-errors-{}-{nonce}.jsonl",
        std::process::id()
    ))
}

fn function_source<'a>(script: &'a str, name: &str, next_name: &str) -> &'a str {
    let start = script
        .find(&format!("function {name}("))
        .expect("missing JavaScript function");
    let end = script[start..]
        .find(&format!("function {next_name}("))
        .map(|offset| start + offset)
        .expect("missing following JavaScript function");
    &script[start..end]
}

#[tokio::test]
async fn dashboard_serves_the_fully_live_console_spa() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let dashboard = get(addr, http::DASHBOARD_PATH, None).await;
    assert_eq!(dashboard.status, 200);
    // The SPA wires its login view to the console API...
    assert!(
        dashboard.body.contains("/console/v1/login"),
        "console SPA must reference the login endpoint"
    );
    // ...and ships no placeholder affordance anywhere.
    assert!(
        !dashboard.body.contains("Not yet implemented"),
        "console SPA must not contain placeholder sections"
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn dashboard_api_keys_section_is_admin_only_and_covers_the_management_lifecycle() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let dashboard = get(addr, http::DASHBOARD_PATH, None).await;
    assert_eq!(dashboard.status, 200);
    for required in [
        "id: 'api-keys'",
        "adminOnly: true",
        "state.user.role === 'admin'",
        "if (it.adminOnly && !isAdmin()) return;",
        "(!ROUTES[h].adminOnly || isAdmin())",
        "'/console/v1/api-keys'",
        "'/rotate'",
        "data-act=\"api-key-rotate\"",
        "'\" data-generation=\"' + esc(key.generation) + '\" data-confirm'",
        "'/revoke'",
        "data-confirm",
        "active",
        "expired",
        "revoked",
        "last_used_at",
        "expires_at",
        "generation",
        "grid-template-areas: \"brand\" \"topbar\" \"sidebar\" \"content\"",
    ] {
        assert!(
            dashboard.body.contains(required),
            "console API-key lifecycle is missing {required:?}"
        );
    }
    assert_eq!(
        dashboard
            .body
            .matches("'\" data-generation=\"' + esc(key.generation) + '\" data-confirm'")
            .count(),
        2,
        "rotate and revoke must both use the shared inline confirmation gate"
    );
    for scope in [
        "telemetry:read",
        "config:read",
        "audit:read",
        "errors:read",
        "accounts:read",
        "groups:read",
        "runtime:read",
        "matches:read",
        "storage:read",
        "database:read",
        "chat:read",
        "notifications:read",
        "leaderboards:read",
        "tournaments:read",
        "purchases:read",
        "subscriptions:read",
    ] {
        assert!(
            dashboard.body.contains(scope),
            "API-key form is missing supported scope {scope}"
        );
    }

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn dashboard_keeps_one_time_api_key_secrets_only_in_an_accessible_modal() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let dashboard = get(addr, http::DASHBOARD_PATH, None).await;
    assert_eq!(dashboard.status, 200);
    for required in [
        "role=\"dialog\"",
        "aria-modal=\"true\"",
        "role=\"alert\"",
        "aria-live=\"polite\"",
        "id=\"api-key-secret\"",
        "Copy secret",
        "shown only once",
        "navigator.clipboard.writeText(secretNode.textContent)",
        "qs('modal-root').replaceChildren()",
    ] {
        assert!(
            dashboard.body.contains(required),
            "one-time secret UI is missing {required:?}"
        );
    }
    assert!(
        !dashboard.body.contains("localStorage"),
        "the console must never persist an API-key secret in localStorage"
    );
    assert!(
        !dashboard.body.contains("console.log"),
        "the console must never log API-key responses or secrets"
    );
    assert_eq!(
        dashboard.body.matches("issued.secret").count(),
        1,
        "the one-time response secret must only be transferred into the modal DOM"
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn dashboard_navigation_closes_the_one_time_secret_modal() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let dashboard = get(addr, http::DASHBOARD_PATH, None).await;
    assert_eq!(dashboard.status, 200);
    let render_start = dashboard
        .body
        .find("function render() {")
        .expect("render function");
    let render_end = dashboard.body[render_start..]
        .find("\n  }")
        .map(|offset| render_start + offset)
        .expect("render function end");
    let render_body = &dashboard.body[render_start..render_end];
    assert!(
        render_body.contains("closeModal();"),
        "every dashboard render must remove one-time secret DOM"
    );
    assert!(
        dashboard.body.contains(
            "window.addEventListener('hashchange', function () { if (state.user) render(); });"
        ),
        "hash navigation must route through render(), which closes the modal"
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn dashboard_discards_delayed_api_key_secrets_after_context_changes() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let dashboard = get(addr, http::DASHBOARD_PATH, None).await;
    assert_eq!(dashboard.status, 200);
    let create = function_source(&dashboard.body, "openApiKeyCreate", "showApiKeySecret");
    let rotate = function_source(&dashboard.body, "rotateApiKey", "revokeApiKey");
    for (operation, source) in [("create", create), ("rotate", rotate)] {
        assert!(
            source.contains("var requestContext = captureApiKeyRequestContext();"),
            "{operation} must snapshot the route and authenticated session before issuing"
        );
        assert!(
            source.contains("if (!apiKeyRequestContextIsCurrent(requestContext)) return;"),
            "{operation} must discard a delayed one-time secret before rendering it"
        );
        let guard = source
            .find("apiKeyRequestContextIsCurrent(requestContext)")
            .expect("context guard");
        let reveal = source.find("showApiKeySecret(").expect("secret reveal");
        assert!(
            guard < reveal,
            "{operation} must guard before revealing the secret"
        );
    }

    let capture = function_source(
        &dashboard.body,
        "captureApiKeyRequestContext",
        "apiKeyRequestContextIsCurrent",
    );
    let is_current = function_source(&dashboard.body, "apiKeyRequestContextIsCurrent", "toast");
    let render = function_source(&dashboard.body, "render", "statusClass");
    assert!(
        render.contains("state.viewGeneration += 1;"),
        "every render/hash navigation must invalidate pending one-time-secret responses"
    );
    let harness = format!(
        r#"
var state = {{ user: {{ role: 'admin' }}, sessionGeneration: 7, viewGeneration: 11 }};
var location = {{ hash: '#/api-keys' }};
var activeToken = 'token-a';
function token() {{ return activeToken; }}
function currentRoute() {{ return location.hash.replace(/^#\/?/, '') || 'status'; }}
{capture}
{is_current}
var original = captureApiKeyRequestContext();
if (!apiKeyRequestContextIsCurrent(original)) throw new Error('fresh context rejected');
location.hash = '#/status';
state.viewGeneration += 1;
if (apiKeyRequestContextIsCurrent(original)) throw new Error('route change accepted');
location.hash = '#/api-keys';
state.viewGeneration += 1;
if (apiKeyRequestContextIsCurrent(original)) throw new Error('away-and-back navigation accepted');
state.sessionGeneration += 1;
if (apiKeyRequestContextIsCurrent(original)) throw new Error('session generation change accepted');
state.sessionGeneration = 7;
activeToken = 'token-b';
if (apiKeyRequestContextIsCurrent(original)) throw new Error('token change accepted');
activeToken = 'token-a';
state.user = null;
if (apiKeyRequestContextIsCurrent(original)) throw new Error('logout accepted');
"#
    );
    let output = Command::new("node")
        .args(["--input-type=commonjs", "-e", &harness])
        .output()
        .expect("execute request-context regression with Node.js");
    assert!(
        output.status.success(),
        "request-context regression failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn dashboard_modals_manage_keyboard_focus_accessibly() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let dashboard = get(addr, http::DASHBOARD_PATH, None).await;
    assert_eq!(dashboard.status, 200);
    let open = function_source(&dashboard.body, "openModal", "closeModal");
    let close = function_source(&dashboard.body, "closeModal", "armConfirm");
    let confirm = function_source(&dashboard.body, "armConfirm", "bindDelegation");
    for required in [
        "document.activeElement",
        "modalFocusTarget.focus()",
        "addEventListener('keydown'",
        "ev.key === 'Escape'",
        "ev.key !== 'Tab'",
        "ev.shiftKey",
    ] {
        assert!(
            open.contains(required),
            "modal open behavior is missing {required:?}"
        );
    }
    assert!(
        close.contains("modalReturnFocus.focus()"),
        "closing a modal must restore the previously focused control"
    );
    assert!(
        close.contains("qs('modal-root').replaceChildren()"),
        "closing must still remove one-time secret DOM"
    );
    assert!(confirm.contains("document.activeElement"));
    assert!(confirm.contains("yes.focus()"));
    assert!(confirm.contains("no.focus()"));
    assert!(
        confirm.contains("confirmReturnFocus.focus()"),
        "cancelling an inline confirmation must restore its trigger focus"
    );

    let _ = tx.send(());
    let _ = server.await;
}

#[test]
fn api_key_documentation_covers_the_v1_security_contract() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let page_path = root.join("website/src/content/docs/reference/admin-api/api-keys.md");
    let page = std::fs::read_to_string(&page_path).expect("read API-key documentation page");
    let index = std::fs::read_to_string(
        root.join("website/src/content/docs/reference/admin-api/index.mdx"),
    )
    .expect("read admin API index");

    for required in [
        "opaque",
        "not JWT",
        "Authorization: Bearer <CITADEL_API_KEY>",
        "query parameter",
        "ctdl_k1_",
        "human `admin`",
        "exactly once",
        "hash",
        "last_used_at",
        "expiration",
        "revocation",
        "rotation",
        "read-only",
        "fail closed",
        "/console/v1/api-keys",
        "telemetry:read",
        "subscriptions:read",
        "curl",
    ] {
        assert!(
            page.contains(required),
            "API-key documentation is missing {required:?}"
        );
    }
    assert!(
        index.contains("/reference/admin-api/api-keys/"),
        "Admin API module navigation must link to API keys"
    );
}

#[test]
fn database_explorer_documentation_covers_principal_sensitive_api_key_visibility() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let page = std::fs::read_to_string(
        root.join("website/src/content/docs/reference/admin-api/database-explorer.md"),
    )
    .expect("read Database Explorer documentation page");
    let normalized = page.split_whitespace().collect::<Vec<_>>().join(" ");

    for required in [
        "Both human console roles",
        "human `admin`",
        "human `viewer`",
        "`api_keys`",
        "hidden",
        "`403`",
        "API key principals",
        "verifier",
        "redacted",
        "`database:read`",
        "normal application relations",
        "exact semantic `POST` reads",
    ] {
        assert!(
            normalized.contains(required),
            "Database Explorer documentation is missing {required:?}"
        );
    }
}

#[tokio::test]
async fn console_spa_data_sources_answer_an_authenticated_operator() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    // Login exactly like the SPA does.
    let login = post_json(
        addr,
        http::LOGIN_PATH,
        r#"{"username":"ops","password":"operator-secret"}"#,
    )
    .await;
    assert_eq!(login.status, 200);
    let login_body: Value = serde_json::from_str(login.body.trim()).expect("login JSON");
    let token = login_body["token"].as_str().expect("token").to_string();
    assert_eq!(login_body["role"], "admin");

    // A representative subset of the sections the UI renders on load.
    let accounts = get(addr, "/console/v1/accounts?limit=5", Some(&token)).await;
    assert_eq!(accounts.status, 200);
    let accounts_body: Value = serde_json::from_str(accounts.body.trim()).expect("accounts JSON");
    assert!(accounts_body["items"].is_array());

    let audit = get(addr, "/console/v1/audit?limit=10", Some(&token)).await;
    assert_eq!(audit.status, 200);
    let audit_body: Value = serde_json::from_str(audit.body.trim()).expect("audit JSON");
    // The login above is already in the trail the UI renders.
    assert!(
        audit_body["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .any(|e| e["action"] == "console.login"),
        "audit trail should record the SPA-style login"
    );

    let errors = get(addr, "/console/v1/errors?limit=10", Some(&token)).await;
    assert_eq!(errors.status, 200);
    let errors_body: Value = serde_json::from_str(errors.body.trim()).expect("errors JSON");
    assert!(errors_body["entries"].is_array());
    assert!(errors_body["total"].is_u64());

    let _ = tx.send(());
    let _ = server.await;
}

#[tokio::test]
async fn error_journal_is_redacted_and_viewer_readable_end_to_end() {
    let mut config = console_config();
    config.console.viewer_password = Some("viewer-secret".to_string());
    let journal_path = test_journal_path();
    let journal = Arc::new(ErrorJournal::new(&journal_path));
    assert!(
        journal
            .append(JournalIncident::from_app_error(
                "repository.pg",
                &AppError::database("postgres://operator:very-secret@db.example/citadel")
                    .with_detail("token=very-secret"),
            ))
            .is_written()
    );
    let app = App::new(config).with_error_journal(Arc::clone(&journal));
    let (addr, tx, server) = spawn_server(app).await;

    let login = post_json(
        addr,
        http::LOGIN_PATH,
        r#"{"username":"ops","password":"viewer-secret"}"#,
    )
    .await;
    assert_eq!(login.status, 200);
    let token =
        serde_json::from_str::<Value>(login.body.trim()).expect("viewer login JSON")["token"]
            .as_str()
            .expect("viewer token")
            .to_string();

    let response = get(addr, "/console/v1/errors?offset=0&limit=9999", Some(&token)).await;
    assert_eq!(response.status, 200);
    assert!(!response.body.contains("very-secret"));
    assert!(!response.body.contains("postgres://"));
    let body: Value = serde_json::from_str(response.body.trim()).expect("errors JSON");
    let entry = body["entries"]
        .as_array()
        .and_then(|entries| entries.first())
        .expect("one redacted incident");
    assert_eq!(entry["category"], "database");
    assert_eq!(entry["component"], "repository.pg");
    assert_eq!(entry["message"], "database failure");

    let _ = tx.send(());
    let _ = server.await;
    let _ = std::fs::remove_file(journal_path);
}
