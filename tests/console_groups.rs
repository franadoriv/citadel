//! Integration tests for the console Groups section.
//!
//! Drives the real server with raw HTTP/1.1 requests over `TcpStream`
//! (helpers copied from `tests/console_api_auth.rs`, which is not edited by
//! this task): login as admin and viewer, then create -> list -> detail
//! (members) -> add member -> promote -> reject demoting the last superadmin
//! -> kick -> update -> delete, plus a viewer-mutation 403 and the audit
//! trail showing the dotted `groups.*` actions.

use std::net::SocketAddr;
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::http;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// A parsed HTTP response: status code and (optional) JSON body.
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
    let status_line = raw.lines().next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .expect("response should carry an HTTP status code");
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .filter(|b| !b.trim().is_empty())
        .and_then(|b| serde_json::from_str::<Value>(b.trim()).ok());
    Response { status, body }
}

/// Issue a raw HTTP/1.1 `POST` with a JSON body (unauthenticated; used only
/// for login).
async fn post_json(addr: SocketAddr, path: &str, body: &str) -> Response {
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
    parse_response(&raw)
}

/// Issue a raw HTTP/1.1 `GET`, optionally with a bearer token.
async fn get(addr: SocketAddr, path: &str, bearer: Option<&str>) -> Response {
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
    parse_response(&raw)
}

/// Issue a raw HTTP/1.1 request with a method, bearer token, and JSON body.
async fn send_json(
    addr: SocketAddr,
    method: &str,
    path: &str,
    bearer: &str,
    body: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(addr).await.expect("connect to server");
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
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {bearer}\r\n{content}Connection: close\r\n\r\n{body_text}"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let raw = read_http_response(&mut stream).await;
    parse_response(&raw)
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

/// A config with non-default operator credentials and a viewer role enabled.
fn console_config() -> Config {
    let mut config = Config::default();
    config.console.username = "ops".to_string();
    config.console.password = "operator-secret".to_string();
    config.console.viewer_password = Some("viewer-secret".to_string());
    config.validate().expect("test config must validate");
    config
}

async fn login(addr: SocketAddr, username: &str, password: &str) -> Response {
    post_json(
        addr,
        http::LOGIN_PATH,
        &format!(r#"{{"username":"{username}","password":"{password}"}}"#),
    )
    .await
}

#[tokio::test]
async fn groups_section_supports_the_full_membership_lifecycle() {
    let (addr, tx, server) = spawn_server(App::new(console_config())).await;

    let admin_token = login(addr, "ops", "operator-secret")
        .await
        .body
        .expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();
    let viewer_token = login(addr, "ops", "viewer-secret")
        .await
        .body
        .expect("login")["token"]
        .as_str()
        .expect("token")
        .to_string();

    // 1. Listing with no query params returns 200 on an empty node.
    let empty = get(addr, "/console/v1/groups", Some(&admin_token)).await;
    assert_eq!(empty.status, 200);
    let empty_body = empty.body.expect("body");
    assert_eq!(empty_body["total"], 0);
    assert_eq!(empty_body["items"].as_array().expect("items").len(), 0);

    // 2. Create a group; the creator defaults to the operator's username and
    //    becomes its founding superadmin.
    let created = send_json(
        addr,
        "POST",
        "/console/v1/groups",
        &admin_token,
        Some(r#"{"name":"raiders","description":"a raiding guild","max_size":3}"#),
    )
    .await;
    assert_eq!(created.status, 201);
    let created_body = created.body.expect("create body");
    assert_eq!(created_body["name"], "raiders");
    assert_eq!(created_body["member_count"], 1);
    assert_eq!(created_body["members"][0]["user_id"], "ops");
    assert_eq!(created_body["members"][0]["role"], "superadmin");
    let group_id = created_body["id"].as_u64().expect("id");

    // A viewer cannot create a group.
    let forbidden_create = send_json(
        addr,
        "POST",
        "/console/v1/groups",
        &viewer_token,
        Some(r#"{"name":"forbidden"}"#),
    )
    .await;
    assert_eq!(forbidden_create.status, 403);
    assert_eq!(forbidden_create.body.expect("body")["code"], "forbidden");

    // Creating a second group with the same name conflicts.
    let duplicate = send_json(
        addr,
        "POST",
        "/console/v1/groups",
        &admin_token,
        Some(r#"{"name":"raiders"}"#),
    )
    .await;
    assert_eq!(duplicate.status, 409);

    // 3. List reflects the created group (any role may read).
    let listed = get(addr, "/console/v1/groups", Some(&viewer_token)).await;
    assert_eq!(listed.status, 200);
    let listed_body = listed.body.expect("listed body");
    assert_eq!(listed_body["total"], 1);
    assert_eq!(listed_body["items"][0]["name"], "raiders");

    // Substring filter narrows the listing.
    let filtered = get(addr, "/console/v1/groups?filter=raid", Some(&admin_token)).await;
    assert_eq!(
        filtered.body.expect("body")["items"]
            .as_array()
            .expect("items")
            .len(),
        1
    );
    let filtered_out = get(
        addr,
        "/console/v1/groups?filter=nomatch",
        Some(&admin_token),
    )
    .await;
    assert_eq!(
        filtered_out.body.expect("body")["items"]
            .as_array()
            .expect("items")
            .len(),
        0
    );

    // 4. Detail includes the member roll.
    let detail = get(
        addr,
        &format!("/console/v1/groups/{group_id}"),
        Some(&admin_token),
    )
    .await;
    assert_eq!(detail.status, 200);
    let detail_body = detail.body.expect("detail body");
    assert_eq!(detail_body["members"].as_array().expect("members").len(), 1);

    // Unknown id is a 404.
    assert_eq!(
        get(addr, "/console/v1/groups/999999", Some(&admin_token))
            .await
            .status,
        404
    );

    // 5. Add a member (admin, audited).
    let added = send_json(
        addr,
        "POST",
        &format!("/console/v1/groups/{group_id}/members"),
        &admin_token,
        Some(r#"{"user_id":"player-1"}"#),
    )
    .await;
    assert_eq!(added.status, 200);
    let added_body = added.body.expect("added body");
    assert_eq!(added_body["member_count"], 2);

    // A viewer cannot add a member.
    let forbidden_add = send_json(
        addr,
        "POST",
        &format!("/console/v1/groups/{group_id}/members"),
        &viewer_token,
        Some(r#"{"user_id":"player-2"}"#),
    )
    .await;
    assert_eq!(forbidden_add.status, 403);

    // Adding the same user again conflicts.
    let dup_member = send_json(
        addr,
        "POST",
        &format!("/console/v1/groups/{group_id}/members"),
        &admin_token,
        Some(r#"{"user_id":"player-1"}"#),
    )
    .await;
    assert_eq!(dup_member.status, 409);

    // 6. Promote the new member: member -> admin.
    let promoted = send_json(
        addr,
        "POST",
        &format!("/console/v1/groups/{group_id}/members/player-1/promote"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(promoted.status, 200);
    let promoted_body = promoted.body.expect("promoted body");
    let player_one = promoted_body["members"]
        .as_array()
        .expect("members")
        .iter()
        .find(|m| m["user_id"] == "player-1")
        .expect("player-1 present");
    assert_eq!(player_one["role"], "admin");

    // 7. Demoting the *last superadmin* ("ops") is rejected with 409.
    let demote_last_superadmin = send_json(
        addr,
        "POST",
        &format!("/console/v1/groups/{group_id}/members/ops/demote"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(demote_last_superadmin.status, 409);

    // Demoting player-1 (an admin, not the last superadmin) succeeds.
    let demoted = send_json(
        addr,
        "POST",
        &format!("/console/v1/groups/{group_id}/members/player-1/demote"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(demoted.status, 200);
    let demoted_body = demoted.body.expect("demoted body");
    let player_one = demoted_body["members"]
        .as_array()
        .expect("members")
        .iter()
        .find(|m| m["user_id"] == "player-1")
        .expect("player-1 present");
    assert_eq!(player_one["role"], "member");

    // 8. Kicking the last superadmin is also rejected with 409.
    let kick_last_superadmin = send_json(
        addr,
        "POST",
        &format!("/console/v1/groups/{group_id}/members/ops/kick"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(kick_last_superadmin.status, 409);

    // Kicking player-1 succeeds.
    let kicked = send_json(
        addr,
        "POST",
        &format!("/console/v1/groups/{group_id}/members/player-1/kick"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(kicked.status, 200);
    assert_eq!(kicked.body.expect("body")["member_count"], 1);

    // A viewer cannot kick either.
    let forbidden_kick = send_json(
        addr,
        "POST",
        &format!("/console/v1/groups/{group_id}/members/ops/kick"),
        &viewer_token,
        None,
    )
    .await;
    assert_eq!(forbidden_kick.status, 403);

    // 9. Update the group's settings (admin, audited).
    let updated = send_json(
        addr,
        "PUT",
        &format!("/console/v1/groups/{group_id}"),
        &admin_token,
        Some(r#"{"description":"updated guild","open":false}"#),
    )
    .await;
    assert_eq!(updated.status, 200);
    let updated_body = updated.body.expect("updated body");
    assert_eq!(updated_body["description"], "updated guild");
    assert_eq!(updated_body["open"], false);
    assert_eq!(updated_body["max_size"], 3, "untouched field unchanged");

    // A viewer cannot update.
    let forbidden_update = send_json(
        addr,
        "PUT",
        &format!("/console/v1/groups/{group_id}"),
        &viewer_token,
        Some(r#"{"open":true}"#),
    )
    .await;
    assert_eq!(forbidden_update.status, 403);

    // 10. Delete the group (admin, audited).
    let deleted = send_json(
        addr,
        "DELETE",
        &format!("/console/v1/groups/{group_id}"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(deleted.status, 204);
    let missing = get(
        addr,
        &format!("/console/v1/groups/{group_id}"),
        Some(&admin_token),
    )
    .await;
    assert_eq!(missing.status, 404);

    // A viewer cannot delete either.
    let recreated = send_json(
        addr,
        "POST",
        "/console/v1/groups",
        &admin_token,
        Some(r#"{"name":"second-guild"}"#),
    )
    .await;
    let second_id = recreated.body.expect("body")["id"].as_u64().expect("id");
    let forbidden_delete = send_json(
        addr,
        "DELETE",
        &format!("/console/v1/groups/{second_id}"),
        &viewer_token,
        None,
    )
    .await;
    assert_eq!(forbidden_delete.status, 403);

    // 11. Every mutation left a dotted `groups.*` trail, newest first.
    let audit = get(addr, "/console/v1/audit?action=groups", Some(&admin_token)).await;
    assert_eq!(audit.status, 200);
    let audit_body = audit.body.expect("audit body");
    let actions: Vec<String> = audit_body["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| e["action"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        actions,
        vec![
            "groups.create",
            "groups.delete",
            "groups.update",
            "groups.member.kick",
            "groups.member.demote",
            "groups.member.promote",
            "groups.member.add",
            "groups.create",
        ],
        "newest-first mutation trail"
    );

    let _ = tx.send(());
    let _ = server.await;
}
