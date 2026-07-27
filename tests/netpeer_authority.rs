//! End-to-end integration coverage for the NetworkPeer server authority pipeline
//! (, design §7): untrusted client delta -> validate -> apply -> the
//! server re-derives and rebroadcasts its OWN authoritative delta.
//!
//! Two complementary tests:
//!
//! 1. [`palpable_health_clamp_rebroadcasts_authoritative_value`] drives the real
//!    [`Gateway`] + [`RepAuthority`] through the registry's outbound seam with the
//!    exact encoded `DeltaBunch` bytes: client A proposes an out-of-range Health,
//!    the server clamps it authoritatively, and client B sees the clamped value in
//!    a **server-stamped** bunch (never A's bytes).
//! 2. [`rep_delta_traverses_real_quic_and_peer_sees_authoritative_value`] proves the
//!    same slice over a real QUIC socket: A's `KIND_REP_DELTA` rides a reliable
//!    stream to the server, and B reads the server's authoritative rebroadcast off
//!    the wire.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use citadel::realtime::Gateway;
use citadel::realtime::netpeer::{
    FieldAuthority, FieldBounds, RepAuthority, RepCondition, RepLayout, RepLayoutBuilder,
    RepSnapshot, TypeTag,
};
use citadel::realtime::registry::{Outbound, SessionHandle};
use citadel::transport::TransportKind;
use citadel_wire::codec::{ScalarQuant, codec_id};
use citadel_wire::netpeer::{
    DeltaBunch, FieldDelta, MAX_ENVELOPE_ALLOC, RepFieldCodec, RepSchema, RepValue,
};
use citadel_wire::protocol::KIND_REP_DELTA;
use tokio::sync::mpsc;

const CLASS: u32 = 42;
const OBJ: u32 = 500;
const MATCH: u64 = 1;
const F_HEALTH: u16 = 0;

fn layout() -> &'static RepLayout {
    static L: OnceLock<RepLayout> = OnceLock::new();
    L.get_or_init(|| {
        RepLayoutBuilder::new(CLASS, 1)
            .field(
                "health",
                TypeTag::Int,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ClientOwned,
                FieldBounds::IntRange { min: 0, max: 100 },
                true,
            )
            .field(
                "emote",
                TypeTag::Scalar,
                codec_id::SCALAR_QUANT,
                RepCondition::None,
                FieldAuthority::ClientOwned,
                FieldBounds::ScalarRange {
                    min: 0.0,
                    max: 1.0,
                    values_per_unit: 1024,
                },
                true,
            )
            .build()
            .expect("layout builds")
    })
}

fn schema() -> RepSchema {
    RepSchema::new(
        *layout().schema_hash(),
        vec![
            RepFieldCodec::IntRange { min: 0, max: 100 },
            RepFieldCodec::Scalar(ScalarQuant::new(0.0, 1.0, 1024).expect("quant")),
        ],
    )
    .expect("schema builds")
}

/// A standalone client `DeltaBunch` blob proposing `health` (full snapshot).
fn client_health_bunch(result_id: u64, health: i64) -> Vec<u8> {
    let mut b = DeltaBunch::new(OBJ, true, result_id, 0);
    b.set(F_HEALTH, FieldDelta::Value(RepValue::Int(health)));
    b.encode(&schema()).expect("client encodes")
}

fn decode(body: &[u8]) -> DeltaBunch {
    let mut budget = MAX_ENVELOPE_ALLOC;
    DeltaBunch::decode(body, &schema(), &mut budget).expect("server bunch decodes")
}

#[tokio::test]
async fn palpable_health_clamp_rebroadcasts_authoritative_value() {
    // The authority owns the object; A is the owner, B a peer receiver.
    let rep = Arc::new(RepAuthority::new(
        citadel::realtime::netpeer::RateLimits::default(),
    ));
    rep.register_class(CLASS, layout(), schema())
        .expect("class");
    let gw = Gateway::new().with_rep_authority(Arc::clone(&rep));

    let a = gw.next_participant_id();
    let b = gw.next_participant_id();
    let (tx_a, mut _ra) = mpsc::channel::<Outbound>(16);
    let (tx_b, mut rb) = mpsc::channel::<Outbound>(16);
    gw.registry().register(SessionHandle {
        id: a,
        kind: TransportKind::Quic,
        outbound: tx_a,
        identity: None,
    });
    gw.registry().register(SessionHandle {
        id: b,
        kind: TransportKind::Quic,
        outbound: tx_b,
        identity: None,
    });

    rep.spawn_object(OBJ, MATCH, CLASS, Some(a.get()), false, RepSnapshot::new())
        .expect("spawn");
    rep.join_match(a.get(), MATCH, false);
    rep.join_match(b.get(), MATCH, false);

    // A proposes Health = 150 (out of the 0..=100 bound). The client-side codec
    // saturates to 100 on encode; the server validates + applies the clamp.
    let client_body = client_health_bunch(10, 150);
    let delivered = gw.handle_inbound(
        a,
        &citadel::transport::Envelope::new(KIND_REP_DELTA, client_body.clone()),
    );
    assert_eq!(delivered, 1, "rebroadcast reached the one peer");

    // B receives a SERVER-STAMPED bunch (not A's bytes) carrying the clamped value.
    let out = rb.recv().await.expect("peer receives rebroadcast");
    assert_eq!(out.envelope.kind, KIND_REP_DELTA);
    assert_ne!(
        out.envelope.body, client_body,
        "server must re-encode its own delta, never relay client bytes"
    );
    let bunch = decode(&out.envelope.body);
    assert_eq!(
        bunch.changes.get(&F_HEALTH),
        Some(&FieldDelta::Value(RepValue::Int(100))),
        "peer sees the authoritative clamped Health"
    );
    // The authoritative state holds the clamp too.
    assert_eq!(
        rep.authoritative_scalar(OBJ, F_HEALTH),
        Some(RepValue::Int(100))
    );
}

#[tokio::test]
async fn non_owner_delta_delivers_nothing() {
    let rep = Arc::new(RepAuthority::new(
        citadel::realtime::netpeer::RateLimits::default(),
    ));
    rep.register_class(CLASS, layout(), schema())
        .expect("class");
    let gw = Gateway::new().with_rep_authority(Arc::clone(&rep));

    let a = gw.next_participant_id();
    let b = gw.next_participant_id();
    let (tx_a, _ra) = mpsc::channel::<Outbound>(16);
    let (tx_b, mut rb) = mpsc::channel::<Outbound>(16);
    gw.registry().register(SessionHandle {
        id: a,
        kind: TransportKind::Quic,
        outbound: tx_a,
        identity: None,
    });
    gw.registry().register(SessionHandle {
        id: b,
        kind: TransportKind::Quic,
        outbound: tx_b,
        identity: None,
    });
    rep.spawn_object(OBJ, MATCH, CLASS, Some(a.get()), false, RepSnapshot::new())
        .expect("spawn");
    rep.join_match(a.get(), MATCH, false);
    rep.join_match(b.get(), MATCH, false);

    // B (not the owner) tries to change A's object: rejected, nothing rebroadcast.
    let delivered = gw.handle_inbound(
        b,
        &citadel::transport::Envelope::new(KIND_REP_DELTA, client_health_bunch(10, 30)),
    );
    assert_eq!(delivered, 0, "a non-owner delta is dropped, no oracle");
    assert!(rb.try_recv().is_err());
    assert_eq!(rep.authoritative_scalar(OBJ, F_HEALTH), None);
}

// --- real QUIC end-to-end ------------------------------------------------------

mod common;
use common::quic_guest_handshake;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use citadel::transport::codec::Envelope as TEnvelope;
use citadel::transport::quic::{QuicServer, SelfSignedCert};

fn loopback_any() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

async fn connect(server_addr: SocketAddr, cert: &SelfSignedCert) -> quinn::Connection {
    use citadel::transport::quic::tls;
    let client_config = tls::client_config_trusting(cert).expect("client config");
    let mut endpoint = quinn::Endpoint::client(loopback_any()).expect("client endpoint");
    endpoint.set_default_client_config(client_config);
    tokio::time::timeout(
        Duration::from_secs(5),
        endpoint
            .connect(server_addr, "localhost")
            .expect("connect call"),
    )
    .await
    .expect("connect did not time out")
    .expect("connection established")
}

#[tokio::test]
async fn rep_delta_traverses_real_quic_and_peer_sees_authoritative_value() {
    use citadel::lifecycle::Supervisor;

    let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("cert");
    let rep = Arc::new(RepAuthority::new(
        citadel::realtime::netpeer::RateLimits::default(),
    ));
    rep.register_class(CLASS, layout(), schema())
        .expect("class");

    // The QUIC server assigns participant ids sequentially from 1. A connects and
    // handshakes first (id 1), then B (id 2); set the authority up for those ids.
    let owner_id = 1u64;
    let peer_id = 2u64;
    rep.spawn_object(OBJ, MATCH, CLASS, Some(owner_id), false, RepSnapshot::new())
        .expect("spawn");
    rep.join_match(owner_id, MATCH, false);
    rep.join_match(peer_id, MATCH, false);

    let gateway = Arc::new(Gateway::new().with_rep_authority(Arc::clone(&rep)));
    let server = QuicServer::bind_with_gateway(loopback_any(), &cert, Arc::clone(&gateway))
        .expect("bind server");
    let server_addr = server.local_addr();

    let mut supervisor = Supervisor::new();
    supervisor.spawn(server);

    // A connects first -> participant 1 (the owner).
    let conn_a = connect(server_addr, &cert).await;
    quic_guest_handshake(&conn_a).await;
    // B connects second -> participant 2 (the peer receiver).
    let conn_b = connect(server_addr, &cert).await;
    quic_guest_handshake(&conn_b).await;

    // A proposes Health = 150 over a reliable uni stream (out of bound -> clamped).
    let delta = TEnvelope::new(KIND_REP_DELTA, client_health_bunch(10, 150));
    let mut send = conn_a.open_uni().await.expect("open uni for delta");
    send.write_all(&delta.encode_framed())
        .await
        .expect("write delta");
    send.finish().expect("finish delta stream");

    // B reads the server's authoritative rebroadcast off a reliable uni stream.
    let mut recv = tokio::time::timeout(Duration::from_secs(5), conn_b.accept_uni())
        .await
        .expect("rebroadcast did not time out")
        .expect("rebroadcast stream");
    let data = recv.read_to_end(64 * 1024).await.expect("read rebroadcast");
    let mut buf = bytes::BytesMut::from(&data[..]);
    let env = citadel::transport::codec::decode_framed(&mut buf)
        .expect("decode framed")
        .expect("one frame");
    assert_eq!(env.kind, KIND_REP_DELTA);
    let bunch = decode(&env.body);
    assert_eq!(
        bunch.changes.get(&F_HEALTH),
        Some(&FieldDelta::Value(RepValue::Int(100))),
        "peer sees the authoritative clamped Health over real QUIC"
    );

    conn_a.close(0u32.into(), b"done");
    conn_b.close(0u32.into(), b"done");
    let result = tokio::time::timeout(Duration::from_secs(5), supervisor.shutdown())
        .await
        .expect("shutdown completes");
    result.expect("clean shutdown");
}
