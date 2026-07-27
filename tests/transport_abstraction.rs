//! Integration tests for the transport abstraction public contract.
//!
//! These exercise the wire-agnostic envelope codec and the per-connection
//! outbound queue exactly as a concrete transport (QUIC/WebSocket) will use
//! them: encode an envelope, simulate a fragmented stream read, decode it, and
//! exercise the backpressure overflow policies for reliable vs unreliable
//! delivery.

use bytes::BytesMut;
use citadel::transport::codec::{Envelope, decode_datagram, decode_framed};
use citadel::transport::queue::{OutboundQueue, PushOutcome};
use citadel::transport::{Delivery, OverflowPolicy, TransportKind};

#[test]
fn framed_envelope_survives_fragmented_stream_reads() {
    let env = Envelope::new(11, &b"realtime-control-message"[..]);
    let frame = env.encode_framed();

    // Simulate a stream transport delivering the frame in two reads.
    let split = frame.len() / 2;
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&frame[..split]);
    // Not enough bytes yet.
    assert!(decode_framed(&mut buf).expect("decode ok").is_none());

    buf.extend_from_slice(&frame[split..]);
    let decoded = decode_framed(&mut buf)
        .expect("decode ok")
        .expect("complete frame after second read");
    assert_eq!(decoded, env);
    assert!(buf.is_empty());
}

#[test]
fn datagram_envelope_round_trips_for_unreliable_transport() {
    let env = Envelope::new(200, &b"hot-path-state"[..]);
    let datagram = env.encode_datagram();
    let decoded = decode_datagram(&datagram).expect("decode ok");
    assert_eq!(decoded, env);
}

#[test]
fn reliable_delivery_uses_close_on_full_backpressure() {
    let policy = Delivery::Reliable.overflow_policy();
    assert_eq!(policy, OverflowPolicy::CloseOnFull);

    let mut q: OutboundQueue<Envelope> = OutboundQueue::new(1, policy);
    assert_eq!(q.push(Envelope::new(1, &b"a"[..])), PushOutcome::Enqueued);
    // Full now: reliable traffic must be rejected, signalling connection close.
    let outcome = q.push(Envelope::new(2, &b"b"[..]));
    assert_eq!(
        outcome,
        PushOutcome::Rejected(Envelope::new(2, &b"b"[..])),
        "reliable overflow must reject the new item"
    );
}

#[test]
fn unreliable_delivery_uses_drop_oldest_backpressure() {
    let policy = Delivery::Unreliable.overflow_policy();
    assert_eq!(policy, OverflowPolicy::DropOldest);

    let mut q: OutboundQueue<Envelope> = OutboundQueue::new(2, policy);
    q.push(Envelope::new(1, &b"old"[..]));
    q.push(Envelope::new(2, &b"mid"[..]));
    // Full: unreliable traffic drops the oldest to admit the newest.
    let outcome = q.push(Envelope::new(3, &b"new"[..]));
    assert_eq!(
        outcome,
        PushOutcome::DroppedOldest(Envelope::new(1, &b"old"[..])),
        "unreliable overflow must drop the oldest item"
    );
    assert_eq!(q.len(), 2);
}

#[test]
fn transport_kind_capabilities_are_exposed() {
    assert!(TransportKind::Quic.supports_unreliable());
    assert!(!TransportKind::WebSocket.supports_unreliable());
}
