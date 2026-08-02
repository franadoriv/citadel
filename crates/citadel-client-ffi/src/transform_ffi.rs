//! C ABI for the shared transform-sync client runtime.
//!
//! The Rust [`RemoteWorldView`](citadel::realtime::transform::RemoteWorldView)
//! remains the sole snapshot decoder, adaptive-buffer implementation, and source
//! of reconciliation targets. Unity/Godot own only engine-facing transform
//! application and local input glue.

use std::sync::Mutex;

use citadel::realtime::transform::{RemoteWorldView, TransformState};
use citadel_wire::tsync::{Hello, InputBundle, InputFrame, TransformCodec};

use crate::{CitadelStatus, guard};

/// Opaque shared transform runtime handle exposed as `CitadelTransformView *`.
pub struct CitadelTransformView {
    view: Mutex<RemoteWorldView>,
}

/// A transform returned by the shared runtime, in Citadel world units (cm).
#[repr(C)]
pub struct CitadelTransformState {
    /// Position `[x, y, z]`.
    pub position: [f32; 3],
    /// Quaternion `[x, y, z, w]`.
    pub rotation: [f32; 4],
    /// Linear velocity `[x, y, z]`.
    pub velocity: [f32; 3],
}

impl From<TransformState> for CitadelTransformState {
    fn from(value: TransformState) -> Self {
        Self {
            position: value.position,
            rotation: value.rotation,
            velocity: value.velocity,
        }
    }
}

/// Build a transform runtime from the reliable `KIND_TSYNC_HELLO` body.
///
/// # Safety
/// `hello_body` must reference `hello_len` readable bytes (or be null only when
/// `hello_len == 0`), and `out_view` must be a writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_transform_view_new(
    hello_body: *const u8,
    hello_len: usize,
    out_view: *mut *mut CitadelTransformView,
) -> CitadelStatus {
    guard(|| {
        if out_view.is_null() || (hello_body.is_null() && hello_len != 0) {
            return CitadelStatus::InvalidArgument;
        }
        let body = if hello_len == 0 {
            &[]
        } else {
            // SAFETY: caller guarantees the readable body range; it is used only
            // during this call and decoded into owned runtime state.
            unsafe { std::slice::from_raw_parts(hello_body, hello_len) }
        };
        let Ok(hello) = Hello::decode(body) else {
            return CitadelStatus::InvalidArgument;
        };
        let Ok(codec) = TransformCodec::from_hello(&hello) else {
            return CitadelStatus::InvalidArgument;
        };
        let view = Box::new(CitadelTransformView {
            view: Mutex::new(RemoteWorldView::new(
                codec,
                hello.sim_rate_hz,
                hello.send_rate_hz,
            )),
        });
        // SAFETY: out_view was checked non-null and the caller owns the returned
        // handle until it passes it to citadel_transform_view_free.
        unsafe { *out_view = Box::into_raw(view) };
        CitadelStatus::Ok
    })
}

/// Decode and apply one `KIND_TSYNC_SNAPSHOT` body. `out_applied` reports false
/// for malformed, stale, or missing-base snapshots; those are normal datagram
/// conditions, not transport errors.
///
/// # Safety
/// `view` must be live, `body` must reference `body_len` readable bytes (or be
/// null when zero), and `out_applied` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_transform_view_apply_datagram(
    view: *mut CitadelTransformView,
    body: *const u8,
    body_len: usize,
    out_applied: *mut bool,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `view` is live for this call; this borrows it only.
        let Some(view) = (unsafe { view.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if out_applied.is_null() || (body.is_null() && body_len != 0) {
            return CitadelStatus::InvalidArgument;
        }
        let body = if body_len == 0 {
            &[]
        } else {
            // SAFETY: caller guarantees this readable range for the duration of the call.
            unsafe { std::slice::from_raw_parts(body, body_len) }
        };
        let Ok(mut runtime) = view.view.lock() else {
            return CitadelStatus::Internal;
        };
        let applied = runtime.apply_datagram(body);
        // SAFETY: the non-null output pointer was validated above and is caller-writable.
        unsafe { *out_applied = applied };
        CitadelStatus::Ok
    })
}

/// Decode and apply one negotiated `KIND_TSYNC_V2_SNAPSHOT` body. The v2
/// clock epoch is fenced by the view: stale or mixed epochs return
/// `out_applied = false` and never contaminate v1/v2 baseline state.
///
/// # Safety
/// Same pointer requirements as [`citadel_transform_view_apply_datagram`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_transform_view_apply_v2_datagram(
    view: *mut CitadelTransformView,
    body: *const u8,
    body_len: usize,
    out_applied: *mut bool,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `view` is live for this call; this borrows it only.
        let Some(view) = (unsafe { view.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if out_applied.is_null() || (body.is_null() && body_len != 0) {
            return CitadelStatus::InvalidArgument;
        }
        let body = if body_len == 0 {
            &[]
        } else {
            // SAFETY: caller guarantees this readable range for the duration of the call.
            unsafe { std::slice::from_raw_parts(body, body_len) }
        };
        let Ok(mut runtime) = view.view.lock() else {
            return CitadelStatus::Internal;
        };
        let applied = runtime.apply_v2_datagram(body);
        // SAFETY: the non-null output pointer was validated above and is caller-writable.
        unsafe { *out_applied = applied };
        CitadelStatus::Ok
    })
}

/// Fence this transform view for a negotiated reconnect/reset epoch. This
/// clears all snapshot baselines, samples and acknowledgements so no stale
/// state can cross a match lifetime.
///
/// # Safety
/// `view` must be live and `epoch` must be nonzero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_transform_view_reset_v2_epoch(
    view: *mut CitadelTransformView,
    epoch: u64,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `view` is live for this call; this borrows it only.
        let Some(view) = (unsafe { view.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        let Ok(mut runtime) = view.view.lock() else {
            return CitadelStatus::Internal;
        };
        if runtime.reset_v2_epoch(epoch) {
            CitadelStatus::Ok
        } else {
            CitadelStatus::InvalidArgument
        }
    })
}

/// Return the runtime's current adaptive-buffer sample for an object.
///
/// # Safety
/// `view` must be live and `out_state`/`out_found` must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_transform_view_sample_now(
    view: *mut CitadelTransformView,
    object_id: u32,
    out_state: *mut CitadelTransformState,
    out_found: *mut bool,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `view` is live for this call; this borrows it only.
        let Some(view) = (unsafe { view.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if out_state.is_null() || out_found.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        let Ok(runtime) = view.view.lock() else {
            return CitadelStatus::Internal;
        };
        let state = runtime
            .render_tick()
            .and_then(|tick| runtime.sample(object_id, tick));
        // SAFETY: both output pointers were checked non-null and are
        // caller-writable. A zero transform is deterministic when not found.
        unsafe {
            *out_found = state.is_some();
            *out_state = state.unwrap_or_default().into();
        }
        CitadelStatus::Ok
    })
}

/// Return an owned object's present authoritative state and highest contiguous
/// input acknowledgement, the required reconciliation target.
///
/// # Safety
/// `view` must be live and all output pointers must be writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_transform_view_authoritative_state(
    view: *mut CitadelTransformView,
    object_id: u32,
    out_state: *mut CitadelTransformState,
    out_input_seq: *mut u32,
    out_found: *mut bool,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `view` is live for this call; this borrows it only.
        let Some(view) = (unsafe { view.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if out_state.is_null() || out_input_seq.is_null() || out_found.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        let Ok(runtime) = view.view.lock() else {
            return CitadelStatus::Internal;
        };
        let state = runtime.authoritative_state(object_id);
        // SAFETY: output pointers were validated and are caller-writable.
        unsafe {
            *out_found = state.is_some();
            *out_state = state.unwrap_or_default().into();
            *out_input_seq = runtime.owner_ack(object_id).unwrap_or(0);
        }
        CitadelStatus::Ok
    })
}

/// Encode the snapshot acknowledgement into `out_ack[0..8]`.
///
/// # Safety
/// `view` must be live and `out_ack` must reference at least eight writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_transform_view_ack(
    view: *mut CitadelTransformView,
    out_ack: *mut u8,
) -> CitadelStatus {
    guard(|| {
        // SAFETY: caller guarantees `view` is live for this call; this borrows it only.
        let Some(view) = (unsafe { view.as_ref() }) else {
            return CitadelStatus::InvalidArgument;
        };
        if out_ack.is_null() {
            return CitadelStatus::InvalidArgument;
        }
        let Ok(runtime) = view.view.lock() else {
            return CitadelStatus::Internal;
        };
        let encoded = runtime.ack().encode();
        // SAFETY: caller guarantees eight writable output bytes; encode is a
        // fixed-size KIND_TSYNC_ACK body.
        unsafe { std::ptr::copy_nonoverlapping(encoded.as_ptr(), out_ack, encoded.len()) };
        CitadelStatus::Ok
    })
}

/// Encode one sequenced owner-input frame as `KIND_TSYNC_INPUT`'s body. Engine
/// components retain and resend recent frames as their own bounded redundancy
/// ring; this function keeps the wire layout itself in Rust.
///
/// # Safety
/// `out`, `out_len`, and `out_truncated` must be writable. `out` may be null
/// only when `cap == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_transform_encode_input(
    input_seq: u32,
    sim_tick: u32,
    dt: f32,
    object_id: u32,
    ownership_epoch: u32,
    velocity_x: f32,
    velocity_y: f32,
    velocity_z: f32,
    out: *mut u8,
    cap: usize,
    out_len: *mut usize,
    out_truncated: *mut bool,
) -> CitadelStatus {
    guard(|| {
        if out_len.is_null() || out_truncated.is_null() || (out.is_null() && cap != 0) {
            return CitadelStatus::InvalidArgument;
        }
        let encoded = InputBundle {
            acked_snapshot_id: 0,
            last_seen_snapshot_id: 0,
            frames: vec![InputFrame {
                input_seq,
                sim_tick,
                dt,
                object_id,
                ownership_epoch,
                move_velocity: [velocity_x, velocity_y, velocity_z],
                payload: Vec::new(),
                fire: None,
            }],
        }
        .encode();
        let copied = encoded.len().min(cap);
        if copied != 0 {
            // SAFETY: caller guarantees cap writable bytes; copied <= cap and the
            // encoded Vec does not overlap the caller's output buffer.
            unsafe { std::ptr::copy_nonoverlapping(encoded.as_ptr(), out, copied) };
        }
        // SAFETY: pointers were checked non-null and caller-writable.
        unsafe {
            *out_len = encoded.len();
            *out_truncated = encoded.len() > cap;
        }
        CitadelStatus::Ok
    })
}

/// Free a transform runtime handle. Passing null is a no-op.
///
/// # Safety
/// `view` must be a live handle returned by citadel_transform_view_new or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn citadel_transform_view_free(view: *mut CitadelTransformView) {
    let _ = guard(|| {
        if !view.is_null() {
            // SAFETY: caller transfers back the one live allocation returned by
            // citadel_transform_view_new; dropping it releases the mutex/runtime.
            drop(unsafe { Box::from_raw(view) });
        }
        CitadelStatus::Ok
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use citadel_wire::tsync::{GameplayClockMetadata, Snapshot, SnapshotV2};

    #[test]
    fn state_layout_is_c_compatible() {
        assert_eq!(std::mem::size_of::<CitadelTransformState>(), 40);
    }

    #[test]
    fn input_encoder_emits_the_canonical_owner_frame() {
        let mut out = [0u8; 128];
        let mut len = 0usize;
        let mut truncated = true;
        // SAFETY: all output pointers reference the local writable buffers; the
        // scalar inputs are plain values and the output capacity is exact.
        let status = unsafe {
            citadel_transform_encode_input(
                9,
                11,
                0.016,
                42,
                3,
                100.0,
                0.0,
                -25.0,
                out.as_mut_ptr(),
                out.len(),
                &mut len,
                &mut truncated,
            )
        };
        assert_eq!(status, CitadelStatus::Ok);
        assert!(!truncated);
        let decoded = InputBundle::decode(&out[..len]).expect("canonical frame decodes");
        assert_eq!(decoded.frames.len(), 1);
        assert_eq!(decoded.frames[0].input_seq, 9);
        assert_eq!(decoded.frames[0].object_id, 42);
        assert_eq!(decoded.frames[0].move_velocity, [100.0, 0.0, -25.0]);
    }

    #[test]
    fn v2_ffi_entrypoints_reject_invalid_handles_without_mutating_state() {
        let mut applied = true;
        // SAFETY: null is deliberately supplied to verify the documented invalid-handle path.
        let status = unsafe {
            citadel_transform_view_apply_v2_datagram(
                std::ptr::null_mut(),
                std::ptr::null(),
                0,
                &mut applied,
            )
        };
        assert_eq!(status, CitadelStatus::InvalidArgument);
        assert!(applied, "invalid calls do not write the caller output");
        // SAFETY: null is deliberately supplied to verify the documented invalid-handle path.
        let status = unsafe { citadel_transform_view_reset_v2_epoch(std::ptr::null_mut(), 9) };
        assert_eq!(status, CitadelStatus::InvalidArgument);
    }

    #[test]
    fn v2_ffi_applies_fences_stale_epochs_and_resets() {
        let hello = Hello::default();
        let codec = TransformCodec::from_hello(&hello).expect("default codec");
        let hello_body = hello.encode();
        let mut view = std::ptr::null_mut();
        // SAFETY: hello_body and view are live writable/readable local buffers.
        let status =
            unsafe { citadel_transform_view_new(hello_body.as_ptr(), hello_body.len(), &mut view) };
        assert_eq!(status, CitadelStatus::Ok);
        let wire = |epoch, id| {
            SnapshotV2 {
                clock: GameplayClockMetadata {
                    epoch,
                    tick: u64::from(id),
                    tick_hz: 60,
                },
                snapshot: Snapshot {
                    server_tick: id,
                    snapshot_id: id,
                    base_snapshot_id: 0,
                    send_rate_hz: 20,
                    removed: vec![],
                    updates: vec![],
                },
            }
            .encode(&codec)
            .expect("v2 snapshot")
        };
        let first = wire(7, 1);
        let mut applied = false;
        // SAFETY: view is a live FFI handle and all body/output pointers reference locals.
        let status = unsafe {
            citadel_transform_view_apply_v2_datagram(
                view,
                first.as_ptr(),
                first.len(),
                &mut applied,
            )
        };
        assert_eq!(status, CitadelStatus::Ok);
        assert!(applied);
        let stale = wire(6, 2);
        applied = true;
        // SAFETY: view is live and the body/output pointers reference local storage.
        let status = unsafe {
            citadel_transform_view_apply_v2_datagram(
                view,
                stale.as_ptr(),
                stale.len(),
                &mut applied,
            )
        };
        assert_eq!(status, CitadelStatus::Ok);
        assert!(!applied, "a stale epoch is rejected through the C ABI");
        // SAFETY: view is live and epoch eight is a nonzero reconnect epoch.
        let status = unsafe { citadel_transform_view_reset_v2_epoch(view, 8) };
        assert_eq!(status, CitadelStatus::Ok);
        let next = wire(8, 1);
        // SAFETY: view is live and all body/output pointers reference locals.
        let status = unsafe {
            citadel_transform_view_apply_v2_datagram(view, next.as_ptr(), next.len(), &mut applied)
        };
        assert_eq!(status, CitadelStatus::Ok);
        assert!(
            applied,
            "reset permits a fresh snapshot-id sequence for the new epoch"
        );
        // SAFETY: view is the one live allocation returned above.
        unsafe { citadel_transform_view_free(view) };
    }
}
