//! C ABI over `citadel-client` for native game engines (Unity/Unreal/Godot).
//!
//! This is the ONLY crate in the workspace permitted to use `unsafe`: a C ABI
//! requires raw pointers and `extern "C"`. The rest of the workspace keeps
//! `unsafe_code = forbid`. Every `unsafe` block here carries a `// SAFETY:`
//! comment, and every `extern "C"` entrypoint wraps its body in
//! [`std::panic::catch_unwind`] so no Rust panic can unwind across the boundary
//! (which would be undefined behavior).
//!
//! # Model
//!
//! - Opaque handle [`CitadelClient`] (heap `Box`), created by a `connect`
//!   function and destroyed by [`citadel_client_free`]. The caller owns it.
//! - A background tokio runtime drives the async [`citadel_client`] SDK; inbound
//!   envelopes are pushed into a bounded queue.
//! - Receive is **poll-based** ([`citadel_client_poll`]); no callbacks cross the
//!   boundary. Bytes/strings cross as pointer + length into CALLER-provided
//!   buffers, so the FFI never hands out Rust-allocated memory the caller must
//!   free.
//!
//! The documented unsafe exception is isolated to this crate.

use std::ffi::{CStr, c_char};
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::time::Duration;

use citadel_client::quic::ClientTls;
use citadel_client::{Envelope, QuicClient, WsClient};
use citadel_wire::protocol::{
    AUTH_REASON_AUTH_FAILED, AUTH_STATUS_REJECTED, KIND_AUTH, KIND_AUTH_RESULT, decode_auth_result,
};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

/// C ABI over the shared `citadel-wire` quantized codecs.
pub mod codec_ffi;
/// C ABI wrapper around the tested transform snapshot runtime. Native engines
/// use this rather than independently decoding the bit-packed snapshot format.
pub mod transform_ffi;

/// Stable ABI version. Bump on any breaking change to the C surface.
pub const CITADEL_FFI_ABI_VERSION: u32 = 2;

/// Capacity of the inbound envelope queue (envelopes buffered before poll).
const INBOUND_CAPACITY: usize = 4096;

/// Bound the blocking FFI realtime auth helper so a game thread cannot hang
/// forever if a server accepts the transport but never answers the first frame.
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

/// Status codes returned by the C ABI. Stable, `#[repr(C)]`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitadelStatus {
    /// Operation succeeded (and, for poll, an envelope was written).
    Ok = 0,
    /// Non-fatal: nothing was available to poll right now; try again later.
    Again = 1,
    /// The connection is closed; no more envelopes will arrive.
    Disconnected = 2,
    /// A pointer was null or an argument was invalid.
    InvalidArgument = 3,
    /// Connecting or handshaking failed.
    Connect = 4,
    /// Sending failed.
    Send = 5,
    /// Receiving/decoding failed.
    Receive = 6,
    /// An unexpected internal error (including a caught panic).
    Internal = 7,
}

/// Realtime auth outcome status returned by [`citadel_client_authenticate`].
///
/// Values mirror the wire-level `AUTH_STATUS_*` constants exactly so engine
/// bindings can compare them with their generated/declaration-checked protocol
/// constants.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitadelAuthStatus {
    /// The token validated and `user_buf` contains the resolved user id.
    Authenticated = 0,
    /// The connection was admitted as an anonymous guest.
    Guest = 1,
    /// The server refused the handshake; `out_reason` contains an `AUTH_REASON_*`.
    Rejected = 2,
}

/// A reliability-tagged outbound envelope queued to a transport driver.
struct OutboundCmd {
    reliable: bool,
    envelope: Envelope,
}

/// The transport backing a connected client.
enum Transport {
    /// QUIC: shared via `Arc` so the receive task and the FFI send path both use
    /// the client (its send methods take `&self`).
    Quic(std::sync::Arc<QuicClient>),
    /// WebSocket: a background driver task owns the `WsClient` (its `recv`/`send`
    /// take `&mut self`); the FFI `send` pushes commands over this channel.
    WebSocket(mpsc::Sender<OutboundCmd>),
}

/// The opaque client handle exposed to C as `CitadelClient *`.
pub struct CitadelClient {
    runtime: Runtime,
    transport: Transport,
    inbound: Mutex<mpsc::Receiver<Envelope>>,
    last_error: Mutex<String>,
}

impl CitadelClient {
    fn set_error(&self, msg: impl Into<String>) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = msg.into;
        }
    }
}

/// Return the ABI version this library was built with.
#[unsafe(no_mangle)]
pub extern "C" fn citadel_client_abi_version() -> u32 {
    CITADEL_FFI_ABI_VERSION
}

/// Connect to a Citadel QUIC endpoint.
///
/// `addr` and `server_name` are NUL-terminated C strings. `insecure` selects dev
/// TLS that does not verify the server certificate (for the dev self-signed
/// cert). On success, writes a heap-allocated handle to `*out_handle`; the
/// caller owns it and must call [`citadel_client_free`].
///
/// # Safety
/// `addr` and `server_name` must be valid NUL-terminated C strings.
/// `out_handle` must be a valid, writable `*mut *mut CitadelClient`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_client_connect_quic(
    addr: *const c_char,
    server_name: *const c_char,
    insecure: bool,
    out_handle: *mut *mut CitadelClient,
) -> CitadelStatus {
    guard(|| {
        if addr.is_null() || server_name.is_null() || out_handle.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        // SAFETY: caller guarantees `addr`/`server_name` are valid NUL-terminated
        // C strings; we only read them here and copy into owned Strings.
        let addr_str = match unsafe { CStr::from_ptr(addr) }.to_str() {
            Ok(s) => s,
            Err(_) => return CitadelStatus::InvalidArgument,
        };
        // SAFETY: same contract as `addr` above.
        let name = match unsafe { CStr::from_ptr(server_name) }.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return CitadelStatus::InvalidArgument,
        };
        let socket: SocketAddr = match addr_str.parse() {
            Ok(a) => a,
            Err(_) => return CitadelStatus::InvalidArgument,
        };
        if !insecure {
            // Only the dev insecure path is wired for now; a pinned-cert path is
            // a follow-up. Be explicit rather than silently insecure.
            return CitadelStatus::InvalidArgument;
        }

        let runtime = match build_runtime() {
            Ok(rt) => rt,
            Err(()) => return CitadelStatus::Internal,
        };
        let connect = runtime.block_on(QuicClient::connect(
            socket,
            &name,
            ClientTls::insecure_skip_verification(),
        ));
        let client = match connect {
            Ok(c) => c,
            Err(_) => return CitadelStatus::Connect,
        };

        let client = std::sync::Arc::new(client);
        let (tx, rx) = mpsc::channel::<Envelope>(INBOUND_CAPACITY);
        // Receive task: relayed peer messages arrive as datagrams.
        spawn_quic_receiver(&runtime, std::sync::Arc::clone(&client), tx);

        let handle = Box::new(CitadelClient {
            runtime,
            transport: Transport::Quic(client),
            inbound: Mutex::new(rx),
            last_error: Mutex::new(String::new()),
        });
        // SAFETY: `out_handle` is non-null (checked) and the caller guarantees it
        // is writable. We transfer ownership of the box to the caller.
        unsafe { *out_handle = Box::into_raw(handle) };
        CitadelStatus::Ok
    })
}

/// Connect to a Citadel WebSocket endpoint (e.g. `ws://127.0.0.1:7352/`).
///
/// # Safety
/// `url` must be a valid NUL-terminated C string. `out_handle` must be a valid,
/// writable `*mut *mut CitadelClient`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_client_connect_websocket(
    url: *const c_char,
    out_handle: *mut *mut CitadelClient,
) -> CitadelStatus {
    guard(|| {
        if url.is_null() || out_handle.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        // SAFETY: caller guarantees `url` is a valid NUL-terminated C string.
        let url_str = match unsafe { CStr::from_ptr(url) }.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return CitadelStatus::InvalidArgument,
        };

        let runtime = match build_runtime() {
            Ok(rt) => rt,
            Err(()) => return CitadelStatus::Internal,
        };
        let client = match runtime.block_on(WsClient::connect(&url_str)) {
            Ok(c) => c,
            Err(_) => return CitadelStatus::Connect,
        };

        let (inbound_tx, inbound_rx) = mpsc::channel::<Envelope>(INBOUND_CAPACITY);
        let (cmd_tx, cmd_rx) = mpsc::channel::<OutboundCmd>(INBOUND_CAPACITY);
        spawn_ws_driver(&runtime, client, cmd_rx, inbound_tx);

        let handle = Box::new(CitadelClient {
            runtime,
            transport: Transport::WebSocket(cmd_tx),
            inbound: Mutex::new(inbound_rx),
            last_error: Mutex::new(String::new()),
        });
        // SAFETY: `out_handle` is non-null (checked) and caller-writable; we
        // transfer ownership of the box to the caller.
        unsafe { *out_handle = Box::into_raw(handle) };
        CitadelStatus::Ok
    })
}

/// Send an envelope. `reliable` chooses a reliable stream vs an unreliable
/// datagram on QUIC; WebSocket is always reliable. The `data`/`len` bytes are
/// copied; the caller keeps ownership of its buffer.
///
/// # Safety
/// `handle` must be a valid pointer returned by a `connect` function and not yet
/// freed. `data` must point to at least `len` readable bytes (or be null iff
/// `len == 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_client_send(
    handle: *mut CitadelClient,
    kind: u16,
    data: *const u8,
    len: usize,
    reliable: bool,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `handle` is a live handle from connect.
        let Some(client) = (unsafe { handle.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if data.is_null() && len != 0 {
            return CitadelStatus::InvalidArgument;
        }
        let bytes = if len == 0 {
            Vec::new()
        } else {
            // SAFETY: caller guarantees `data` points to `len` readable bytes;
            // we copy them into an owned Vec and do not retain the pointer.
            unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
        };
        let env = Envelope::new(kind, bytes);
        send_envelope(client, env, reliable)
    })
}

/// Perform the realtime auth handshake as the next frame on a freshly connected
/// transport.
///
/// `token`/`len` is copied into a `KIND_AUTH` body; pass null + 0 for an explicit
/// guest session. On [`CitadelStatus::Ok`], `*out_status` is set to the resolved
/// auth status. For authenticated sessions, the user id is copied into
/// `user_buf` as a NUL-terminated UTF-8 string when `user_cap > 0`; `*out_user_len`
/// receives the full user-id length even when truncated. For rejected sessions,
/// `*out_reason` receives the coarse `AUTH_REASON_*` class.
///
/// # Safety
/// `handle` must be a valid pointer returned by a `connect` function and not yet
/// freed. `token` must point to at least `len` readable bytes (or be null iff
/// `len == 0`). `out_status`, `out_user_len`, and `out_reason` must be writable.
/// `user_buf` must point to at least `user_cap` writable bytes (or be null iff
/// `user_cap == 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_client_authenticate(
    handle: *mut CitadelClient,
    token: *const u8,
    len: usize,
    out_status: *mut CitadelAuthStatus,
    user_buf: *mut c_char,
    user_cap: usize,
    out_user_len: *mut usize,
    out_reason: *mut u8,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `handle` is a live handle from connect.
        let Some(client) = (unsafe { handle.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if token.is_null() && len != 0 {
            return CitadelStatus::InvalidArgument;
        }
        if out_status.is_null() || out_user_len.is_null() || out_reason.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        if user_buf.is_null() && user_cap != 0 {
            return CitadelStatus::InvalidArgument;
        }

        let body = if len == 0 {
            Vec::new()
        } else {
            // SAFETY: caller guarantees `token` points to `len` readable bytes.
            unsafe { std::slice::from_raw_parts(token, len) }.to_vec()
        };
        let send_status = send_envelope(client, Envelope::new(KIND_AUTH, body), true);
        if send_status != CitadelStatus::Ok {
            return send_status;
        }

        let env = {
            let mut rx = match client.inbound.lock() {
                Ok(g) => g,
                Err(_) => return CitadelStatus::Internal,
            };
            match client
                .runtime
                .block_on(async { tokio::time::timeout(AUTH_TIMEOUT, rx.recv()).await })
            {
                Ok(Some(env)) => env,
                Ok(None) => {
                    client.set_error("connection closed during auth handshake");
                    return CitadelStatus::Disconnected;
                }
                Err(_) => {
                    client.set_error("auth handshake timed out");
                    return CitadelStatus::Receive;
                }
            }
        };

        if env.kind != KIND_AUTH_RESULT {
            client.set_error(format!(
                "expected KIND_AUTH_RESULT ({KIND_AUTH_RESULT}) as auth reply, got kind {}",
                env.kind
            ));
            return CitadelStatus::Receive;
        }
        let Some(result) = decode_auth_result(&env.body) else {
            client.set_error("server sent malformed KIND_AUTH_RESULT");
            return CitadelStatus::Receive;
        };

        // SAFETY: out pointers are non-null (checked) and caller-writable.
        unsafe {
            *out_user_len = result.user_id.len;
            *out_reason = result.reason_class;
        }
        if result.is_authenticated() {
            copy_c_string(result.user_id.as_bytes(), user_buf, user_cap);
            // SAFETY: non-null and writable (checked above).
            unsafe { *out_status = CitadelAuthStatus::Authenticated };
        } else if result.is_guest() {
            copy_c_string(&[], user_buf, user_cap);
            // SAFETY: non-null and writable (checked above).
            unsafe { *out_status = CitadelAuthStatus::Guest };
        } else {
            copy_c_string(&[], user_buf, user_cap);
            // SAFETY: non-null and writable (checked above).
            unsafe {
                *out_status = CitadelAuthStatus::Rejected;
                if result.status != AUTH_STATUS_REJECTED {
                    *out_reason = AUTH_REASON_AUTH_FAILED;
                }
            }
        }
        CitadelStatus::Ok
    })
}

/// Poll for the next inbound envelope (non-blocking).
///
/// On [`CitadelStatus::Ok`], writes the envelope kind to `*out_kind`, copies the
/// payload into the caller's `buf` (capacity `cap`), writes the payload length to
/// `*out_len`, and sets `*out_truncated` to true if the payload did not fit
/// (in which case only `cap` bytes were written). Returns [`CitadelStatus::Again`]
/// if nothing is ready, or [`CitadelStatus::Disconnected`] if the connection
/// closed and the queue is drained.
///
/// # Safety
/// `handle` must be a live handle. `out_kind`, `out_len`, and `out_truncated`
/// must be valid writable pointers. `buf` must point to at least `cap` writable
/// bytes (or be null iff `cap == 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_client_poll(
    handle: *mut CitadelClient,
    out_kind: *mut u16,
    buf: *mut u8,
    cap: usize,
    out_len: *mut usize,
    out_truncated: *mut bool,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `handle` is a live handle from connect.
        let Some(client) = (unsafe { handle.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if out_kind.is_null() || out_len.is_null() || out_truncated.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        if buf.is_null() && cap != 0 {
            return CitadelStatus::InvalidArgument;
        }
        let mut rx = match client.inbound.lock() {
            Ok(g) => g,
            Err(_) => return CitadelStatus::Internal,
        };
        match rx.try_recv() {
            Ok(env) => {
                let payload = &env.body;
                let copy = payload.len().min(cap);
                let truncated = payload.len() > cap;
                if copy > 0 {
                    // SAFETY: `buf` points to at least `cap >= copy` writable
                    // bytes (checked above); source and dest do not overlap.
                    unsafe { std::ptr::copy_nonoverlapping(payload.as_ptr(), buf, copy) };
                }
                // SAFETY: out_* pointers are non-null (checked) and caller-writable.
                unsafe {
                    *out_kind = env.kind;
                    *out_len = payload.len;
                    *out_truncated = truncated;
                }
                CitadelStatus::Ok
            }
            Err(mpsc::error::TryRecvError::Empty) => CitadelStatus::Again,
            Err(mpsc::error::TryRecvError::Disconnected) => CitadelStatus::Disconnected,
        }
    })
}

/// Copy the last error message for `handle` into `buf` as a NUL-terminated
/// string (truncated to `cap`). Returns the number of bytes written including
/// the NUL, or 0 on invalid arguments.
///
/// # Safety
/// `handle` must be a live handle. `buf` must point to at least `cap` writable
/// bytes (or be null iff `cap == 0`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_client_last_error(
    handle: *mut CitadelClient,
    buf: *mut c_char,
    cap: usize,
) -> usize {
    guard_usize(|| {
        // SAFETY: caller guarantees `handle` is a live handle from connect.
        let Some(client) = (unsafe { handle.as_ref() }) else {
            return 0;
        };
        if buf.is_null() || cap == 0 {
            return 0;
        }
        let msg = match client.last_error.lock() {
            Ok(g) => g.clone(),
            Err(_) => return 0,
        };
        let bytes = msg.as_bytes();
        // Leave room for the trailing NUL.
        let copy = bytes.len().min(cap - 1);
        if copy > 0 {
            // SAFETY: `buf` has at least `cap` writable bytes; `copy < cap`.
            unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf.cast::<u8>(), copy) };
        }
        // SAFETY: writing the NUL terminator within `cap` bounds (`copy < cap`).
        unsafe { *buf.add(copy) = 0 };
        copy + 1
    })
}

/// Free a client handle. After this call the pointer is invalid and must not be
/// used again. Passing null is a no-op.
///
/// # Safety
/// `handle` must be a pointer returned by a `connect` function that has not
/// already been freed, or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_client_free(handle: *mut CitadelClient) {
    let _ = guard(|| {
        if handle.is_null() {
            return CitadelStatus::Ok;
        }
        // SAFETY: caller guarantees `handle` was returned by connect and not yet
        // freed; reconstituting the Box drops it. Dropping the runtime stops the
        // background tasks, and dropping the command sender lets the WebSocket
        // driver task exit, closing the socket.
        let client = unsafe { Box::from_raw(handle) };
        drop(client);
        CitadelStatus::Ok
    });
}

// --- internal helpers (safe) ----------------------------------------------

/// Run an `extern "C"` body, catching any panic and mapping it to
/// [`CitadelStatus::Internal`] so no panic crosses the FFI boundary.
fn guard<F: FnOnce() -> CitadelStatus>(f: F) -> CitadelStatus {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(status) => status,
        Err(_) => CitadelStatus::Internal,
    }
}

/// Like [`guard`] but for functions returning a length (`usize`); a panic maps
/// to 0.
fn guard_usize<F: FnOnce() -> usize>(f: F) -> usize {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or(0)
}

fn build_runtime() -> Result<Runtime, ()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|_| ())
}

fn send_envelope(client: &CitadelClient, env: Envelope, reliable: bool) -> CitadelStatus {
    match &client.transport {
        Transport::Quic(q) => {
            let result = if reliable {
                client.runtime.block_on(q.send_reliable(&env))
            } else {
                q.send_unreliable(&env)
            };
            match result {
                Ok(()) => CitadelStatus::Ok,
                Err(e) => {
                    client.set_error(e.to_string());
                    CitadelStatus::Send
                }
            }
        }
        Transport::WebSocket(cmd_tx) => {
            // Hand off to the driver task. `try_send` is non-blocking; a full
            // queue or a closed driver maps to a Send error.
            match cmd_tx.try_send(OutboundCmd {
                reliable,
                envelope: env,
            }) {
                Ok(()) => CitadelStatus::Ok,
                Err(e) => {
                    client.set_error(e.to_string());
                    CitadelStatus::Send
                }
            }
        }
    }
}

fn copy_c_string(bytes: &[u8], buf: *mut c_char, cap: usize) {
    if buf.is_null() || cap == 0 {
        return;
    }
    let copy = bytes.len().min(cap - 1);
    if copy > 0 {
        // SAFETY: caller already validated `buf` has at least `cap` writable
        // bytes and `copy < cap`; source and destination do not overlap.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf.cast::<u8>(), copy) };
    }
    // SAFETY: `copy < cap`, so the NUL terminator is in-bounds.
    unsafe { *buf.add(copy) = 0 };
}

fn spawn_quic_receiver(
    runtime: &Runtime,
    client: std::sync::Arc<QuicClient>,
    tx: mpsc::Sender<Envelope>,
) {
    // Unreliable path: server->client datagrams (e.g. transform-sync snapshots).
    let dgram_client = std::sync::Arc::clone(&client);
    let dgram_tx = tx.clone();
    runtime.spawn(async move {
        // Datagrams arrive until the connection closes (Err).
        while let Ok(env) = dgram_client.recv_datagram().await {
            if dgram_tx.send(env).await.is_err() {
                break; // handle freed: receiver dropped
            }
        }
    });

    // Reliable path: server-opened unidirectional streams. The gateway delivers
    // every `Delivery::Reliable` frame this way — the transform-sync HELLO/codec
    // reply, role frames, rewind results, NetworkPeer deltas, and relayed
    // reliable peer messages. Without draining these the client only ever sees
    // datagrams, so (for example) the transform codec never arrives and snapshot
    // datagrams cannot be decoded. Each accepted stream carries >=1 framed
    // envelope.
    runtime.spawn(async move {
        // Streams arrive until the connection closes (Err ends the loop).
        while let Ok(envelopes) = client.recv_uni().await {
            for env in envelopes {
                if tx.send(env).await.is_err() {
                    return; // handle freed: receiver dropped
                }
            }
        }
    });
}

/// Spawn the WebSocket driver task: it OWNS the `WsClient` and concurrently
/// reads inbound frames (pushing envelopes to `inbound`) and outbound commands
/// (sending them on the socket). Owning the client avoids sharing `&mut self`
/// across tasks.
fn spawn_ws_driver(
    runtime: &Runtime,
    mut client: WsClient,
    mut cmd_rx: mpsc::Receiver<OutboundCmd>,
    inbound: mpsc::Sender<Envelope>,
) {
    runtime.spawn(async move {
        loop {
            tokio::select! {
                // Outbound: a send command from the FFI thread.
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else { break }; // handle freed
                    // WebSocket is always reliable; `cmd.reliable` is ignored.
                    let _ = cmd.reliable;
                    if client.send(&cmd.envelope).await.is_err() {
                        break;
                    }
                }
                // Inbound: a frame from the server.
                recv = client.recv() => {
                    match recv {
                        Ok(Some(env)) => {
                            if inbound.send(env).await.is_err() {
                                break;
                            }
                        }
                        Ok(None) | Err(_) => break, // closed
                    }
                }
            }
        }
    });
}
