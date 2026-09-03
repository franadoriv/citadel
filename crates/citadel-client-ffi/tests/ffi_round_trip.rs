//! Tests for the C ABI: exercise the `extern "C"` entrypoints directly (as a C
//! consumer would, with valid pointers) against an in-process Citadel server.
//!
//! Two FFI clients connect over WebSocket; client A sends a position and client
//! B polls and receives it relayed by the gateway (tagged with A's session id).
//! Also covers error translation for invalid arguments.

use std::ffi::{CStr, CString, c_char};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use citadel::App;
use citadel::config::Config;
use citadel::identity::{DeviceId, Username};
use citadel::lifecycle::Supervisor;
use citadel::realtime::{Authenticator, Gateway};
use citadel::services::{AuthenticationOptions, DeviceAuthenticationRequest};
use citadel::session::NodeId;
use citadel::time::{Clock, DurationMillis, SystemClock};
use citadel::transport::quic::{QuicServer, SelfSignedCert};
use citadel::transport::websocket::WebSocketServer;
use citadel_client_ffi::{
    CITADEL_FFI_ABI_VERSION, CitadelAuthStatus, CitadelClient, CitadelStatus,
    citadel_client_abi_version, citadel_client_authenticate, citadel_client_connect_quic,
    citadel_client_connect_websocket, citadel_client_free, citadel_client_poll,
    citadel_client_send,
};
use citadel_wire::protocol::{
    AUTH_REASON_AUTH_REQUIRED, KIND_PEER_POSITION, KIND_POSITION, split_sender,
};

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

/// Connect an FFI client over WebSocket to `addr`, returning the raw handle.
fn connect_ws(addr: SocketAddr) -> *mut CitadelClient {
    let url = CString::new(format!("ws://{addr}/")).expect("cstring");
    let mut handle: *mut CitadelClient = ptr::null_mut();
    // SAFETY: `url` is a valid NUL-terminated C string and `&mut handle` is a
    // valid writable out-pointer.
    let status = unsafe { citadel_client_connect_websocket(url.as_ptr(), &mut handle) };
    assert_eq!(status, CitadelStatus::Ok, "connect should succeed");
    assert!(!handle.is_null());
    handle
}

/// Connect an FFI client over QUIC to `addr` (insecure dev TLS), returning the
/// raw handle.
fn connect_quic(addr: SocketAddr) -> *mut CitadelClient {
    let addr_c = CString::new(addr.to_string()).expect("cstring");
    let name_c = CString::new("localhost").expect("cstring");
    let mut handle: *mut CitadelClient = ptr::null_mut();
    // SAFETY: `addr_c`/`name_c` are valid NUL-terminated C strings and
    // `&mut handle` is a valid writable out-pointer.
    let status =
        unsafe { citadel_client_connect_quic(addr_c.as_ptr(), name_c.as_ptr(), true, &mut handle) };
    assert_eq!(status, CitadelStatus::Ok, "quic connect should succeed");
    assert!(!handle.is_null());
    handle
}

/// Register a device account through the app's auth service and return
/// `(access_token, user_id)`.
async fn mint_token(app: &App) -> (String, String) {
    let outcome = app
        .authentication_service()
        .authenticate_device(DeviceAuthenticationRequest {
            device_id: DeviceId::new("ffi-handshake-device").expect("device id"),
            options: AuthenticationOptions {
                create_account: true,
                username: Some(Username::new("ffi-handshake-player").expect("username")),
                display_name: None,
                metadata: None,
                now: SystemClock.now(),
                owner_node: NodeId::new(app.node_id()).expect("node id"),
                session_ttl: DurationMillis::from_millis(60 * 60 * 1_000),
                refresh_ttl: Some(DurationMillis::from_millis(24 * 60 * 60 * 1_000)),
            },
        })
        .await
        .expect("device auth succeeds");
    (
        outcome.tokens.access.expose_secret().to_string(),
        outcome.user.id.as_str().to_string(),
    )
}

async fn serve_ws_auth(
    app: &App,
    require_auth: bool,
    allow_guests: bool,
) -> (SocketAddr, Supervisor) {
    let authenticator = Authenticator::new(
        Some(Arc::clone(app.session_service())),
        require_auth,
        allow_guests,
    );
    let gateway = Arc::new(Gateway::with_metrics_runtime_auth(
        Arc::clone(app.metrics()),
        None,
        authenticator,
    ));
    let server = WebSocketServer::bind_with_gateway(loopback_any(), gateway)
        .await
        .expect("bind ws");
    let addr = server.local_addr();
    let mut sup = Supervisor::new();
    sup.spawn(server);
    (addr, sup)
}

#[test]
fn abi_version_is_exposed() {
    assert_eq!(citadel_client_abi_version(), CITADEL_FFI_ABI_VERSION);
}

#[test]
fn connect_rejects_null_arguments() {
    // SAFETY: deliberately passing null to confirm the InvalidArgument path.
    let status = unsafe { citadel_client_connect_websocket(ptr::null(), ptr::null_mut()) };
    assert_eq!(status, CitadelStatus::InvalidArgument);
}

#[test]
fn authenticate_rejects_null_arguments() {
    // SAFETY: deliberately passing nulls to confirm the InvalidArgument path.
    let status = unsafe {
        citadel_client_authenticate(
            ptr::null_mut(),
            ptr::null(),
            0,
            ptr::null_mut(),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    };
    assert_eq!(status, CitadelStatus::InvalidArgument);
}

#[test]
fn ffi_authenticate_with_valid_token_returns_user_id() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let app = App::new(Config::default());
    let (token, expected_user_id) = rt.block_on(mint_token(&app));
    let (addr, _supervisor_guard) = rt.block_on(serve_ws_auth(&app, true, false));

    let handle = connect_ws(addr);
    let token_bytes = token.as_bytes();
    let mut auth_status = CitadelAuthStatus::Rejected;
    let mut user_buf = [0 as c_char; 256];
    let mut user_len = 0usize;
    let mut reason = u8::MAX;

    // SAFETY: `handle` is live; token bytes and all output buffers are valid.
    let status = unsafe {
        citadel_client_authenticate(
            handle,
            token_bytes.as_ptr(),
            token_bytes.len(),
            &mut auth_status,
            user_buf.as_mut_ptr(),
            user_buf.len(),
            &mut user_len,
            &mut reason,
        )
    };

    assert_eq!(status, CitadelStatus::Ok);
    assert_eq!(auth_status, CitadelAuthStatus::Authenticated);
    assert_eq!(user_len, expected_user_id.len());
    // SAFETY: the FFI helper always NUL-terminates `user_buf` when cap > 0.
    let actual_user_id = unsafe { CStr::from_ptr(user_buf.as_ptr()) }
        .to_str()
        .expect("user id utf8");
    assert_eq!(actual_user_id, expected_user_id);

    // SAFETY: handle is live and not yet freed.
    unsafe { citadel_client_free(handle) };
}

#[test]
fn ffi_authenticate_guest_is_rejected_when_auth_required() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let app = App::new(Config::default());
    let (addr, _supervisor_guard) = rt.block_on(serve_ws_auth(&app, true, true));

    let handle = connect_ws(addr);
    let mut auth_status = CitadelAuthStatus::Guest;
    let mut user_buf = [0 as c_char; 16];
    let mut user_len = usize::MAX;
    let mut reason = u8::MAX;

    // SAFETY: `handle` is live; null token + len 0 requests guest auth; outputs
    // are valid/writable.
    let status = unsafe {
        citadel_client_authenticate(
            handle,
            ptr::null(),
            0,
            &mut auth_status,
            user_buf.as_mut_ptr(),
            user_buf.len(),
            &mut user_len,
            &mut reason,
        )
    };

    assert_eq!(status, CitadelStatus::Ok);
    assert_eq!(auth_status, CitadelAuthStatus::Rejected);
    assert_eq!(reason, AUTH_REASON_AUTH_REQUIRED);
    assert_eq!(user_len, 0);

    // SAFETY: handle is live and not yet freed.
    unsafe { citadel_client_free(handle) };
}

#[test]
fn ffi_send_poll_relay_round_trip() {
    // Multi-thread runtime so the in-process server and the FFI clients' own
    // background runtimes coexist.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (addr, _supervisor_guard) = rt.block_on(async {
        let server = WebSocketServer::bind(loopback_any())
            .await
            .expect("bind server");
        let addr = server.local_addr();
        let mut sup = Supervisor::new();
        sup.spawn(server);
        (addr, sup)
    });

    // Two FFI clients in the same gateway room. Each presents the guest
    // handshake (empty KIND_AUTH) and drains the ack so it registers.
    let a = connect_ws(addr);
    let b = connect_ws(addr);
    ffi_guest_handshake(a);
    ffi_guest_handshake(b);

    // A sends a position. The body is opaque to the server.
    let payload = [3u8, 1, 4, 1, 5];
    // SAFETY: `a` is a live handle; `payload` points to `payload.len` bytes.
    let send_status = unsafe {
        citadel_client_send(
            a,
            KIND_POSITION,
            payload.as_ptr(),
            payload.len(),
            true, // reliable (WebSocket is always reliable)
        )
    };
    assert_eq!(send_status, CitadelStatus::Ok);

    // B polls until it receives the relayed peer position (or times out).
    let mut got = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let mut out_kind: u16 = 0;
        let mut buf = [0u8; 256];
        let mut out_len: usize = 0;
        let mut truncated = false;
        // SAFETY: `b` is live; all out-pointers and `buf` are valid/writable.
        let status = unsafe {
            citadel_client_poll(
                b,
                &mut out_kind,
                buf.as_mut_ptr(),
                buf.len(),
                &mut out_len,
                &mut truncated,
            )
        };
        if status == CitadelStatus::Ok {
            assert!(!truncated, "256 bytes is plenty for the test payload");
            // Diagnostics control frames may arrive independently of relay
            // traffic; keep polling until the peer-position relay arrives.
            if out_kind == KIND_PEER_POSITION {
                got = Some((out_kind, buf[..out_len].to_vec()));
                break;
            }
            continue;
        }
        assert_eq!(
            status,
            CitadelStatus::Again,
            "poll should be Ok or Again, not an error"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let (kind, body) = got.expect("B should receive a relayed envelope");
    assert_eq!(kind, KIND_PEER_POSITION);
    let (_sender, rest) = split_sender(&body).expect("tagged body");
    assert_eq!(rest, &payload[..], "original payload relayed intact");

    // SAFETY: `a` and `b` are live handles not yet freed.
    unsafe {
        citadel_client_free(a);
        citadel_client_free(b);
    }
}

#[test]
fn ffi_quic_receives_reliable_uni_stream_frames() {
    // Regression: the QUIC receiver must drain server-opened
    // unidirectional streams, not only datagrams. Over QUIC the gateway delivers
    // every reliable frame on a uni stream — the guest handshake ack
    // (`KIND_AUTH_RESULT`) and a reliably relayed peer position both ride one.
    // Before the fix `spawn_quic_receiver` looped on `recv_datagram` alone, so an
    // FFI QUIC client never saw either and (in the field) the transform-sync
    // codec reply was dropped, freezing every synced actor.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let (addr, _supervisor_guard) = rt.block_on(async {
        let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("cert");
        let server = QuicServer::bind(loopback_any(), &cert).expect("bind quic server");
        let addr = server.local_addr();
        let mut sup = Supervisor::new();
        sup.spawn(server);
        (addr, sup)
    });

    let a = connect_quic(addr);
    let b = connect_quic(addr);
    // The handshake ack rides a uni stream — completing it at all exercises the
    // fix (this call times out against the pre-fix receiver).
    ffi_guest_handshake(a);
    ffi_guest_handshake(b);

    // A sends a reliable position; the gateway relays it reliably to B on a uni
    // stream. Confirms end-to-end reliable server->client delivery over QUIC.
    let payload = [9u8, 2, 6, 5, 3, 5];
    // SAFETY: `a` is a live handle; `payload` points to `payload.len` bytes.
    let send_status =
        unsafe { citadel_client_send(a, KIND_POSITION, payload.as_ptr(), payload.len(), true) };
    assert_eq!(send_status, CitadelStatus::Ok);

    let mut got = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let mut out_kind: u16 = 0;
        let mut buf = [0u8; 256];
        let mut out_len: usize = 0;
        let mut truncated = false;
        // SAFETY: `b` is live; all out-pointers and `buf` are valid/writable.
        let status = unsafe {
            citadel_client_poll(
                b,
                &mut out_kind,
                buf.as_mut_ptr(),
                buf.len(),
                &mut out_len,
                &mut truncated,
            )
        };
        if status == CitadelStatus::Ok {
            // Diagnostics control frames may arrive independently of relay
            // traffic; keep polling until the peer-position relay arrives.
            if out_kind == KIND_PEER_POSITION {
                got = Some((out_kind, buf[..out_len].to_vec()));
                break;
            }
            continue;
        }
        assert_eq!(status, CitadelStatus::Again, "poll should be Ok or Again");
        std::thread::sleep(Duration::from_millis(20));
    }

    let (kind, body) = got.expect("B should receive the reliably relayed envelope over QUIC");
    assert_eq!(kind, KIND_PEER_POSITION);
    let (_sender, rest) = split_sender(&body).expect("tagged body");
    assert_eq!(rest, &payload[..], "original payload relayed intact");

    // SAFETY: `a` and `b` are live handles not yet freed.
    unsafe {
        citadel_client_free(a);
        citadel_client_free(b);
    }
}

/// Present the guest handshake over the dedicated FFI auth helper.
fn ffi_guest_handshake(handle: *mut CitadelClient) {
    let mut auth_status = CitadelAuthStatus::Rejected;
    let mut user_buf = [0 as c_char; 64];
    let mut user_len = usize::MAX;
    let mut reason = u8::MAX;
    // SAFETY: `handle` is live; null token + len 0 requests a guest session; all
    // out-pointers and `user_buf` are valid/writable.
    let status = unsafe {
        citadel_client_authenticate(
            handle,
            ptr::null(),
            0,
            &mut auth_status,
            user_buf.as_mut_ptr(),
            user_buf.len(),
            &mut user_len,
            &mut reason,
        )
    };
    assert_eq!(status, CitadelStatus::Ok, "guest auth succeeds");
    assert_eq!(auth_status, CitadelAuthStatus::Guest);
    assert_eq!(user_len, 0);
}

#[test]
fn free_null_is_safe() {
    // SAFETY: passing null is a documented no-op.
    unsafe { citadel_client_free(ptr::null_mut()) };
}
