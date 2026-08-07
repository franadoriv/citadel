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
use citadel::realtime::registry::{Outbound, ParticipantId, SessionHandle};
use citadel::transport::TransportKind;
use citadel_wire::codec::{ScalarQuant, codec_id};
use citadel_wire::netpeer::{
    DeltaBunch, FieldDelta, MAX_ENVELOPE_ALLOC, RepAck, RepAckEntry, RepFieldCodec, RepSchema,
    RepSchemaTable, RepValue,
};
use citadel_wire::protocol::{
    KIND_REP_ACK, KIND_REP_DELTA, KIND_REP_SCHEMA, KIND_ROOM_CREATE, KIND_ROOM_JOIN,
    KIND_ROOM_JOINED,
};
use citadel_wire::room::{RoomCreate, RoomJoin, RoomJoined};
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
async fn gateway_lifecycle_bootstraps_schema_full_then_authoritative_delta() {
    let rep = Arc::new(RepAuthority::new(
        citadel::realtime::netpeer::RateLimits::default(),
    ));
    let gw = Gateway::new().with_rep_authority(Arc::clone(&rep));
    // Registration and spawning are server-only Gateway lifecycle APIs; no
    // NetworkPeer frame exposes either operation to a client.
    gw.register_rep_class(CLASS, layout(), schema())
        .expect("approved class registers");

    let owner = gw.next_participant_id();
    let receiver = gw.next_participant_id();
    let (owner_tx, mut owner_rx) = mpsc::channel::<Outbound>(16);
    let (receiver_tx, mut receiver_rx) = mpsc::channel::<Outbound>(16);
    let mut initial = RepSnapshot::new();
    initial.set_scalar(F_HEALTH, RepValue::Int(10));
    gw.spawn_rep_object(OBJ, 0, CLASS, Some(owner), false, initial)
        .expect("trusted lifecycle spawns object");

    gw.register_session(SessionHandle {
        id: owner,
        kind: TransportKind::Quic,
        outbound: owner_tx,
        identity: None,
    });
    // The owner also gets a bootstrap; draining it keeps this test focused on
    // the joining receiver's exact order.
    owner_rx.recv().await.expect("owner schema");
    owner_rx.recv().await.expect("owner full");
    gw.register_session(SessionHandle {
        id: receiver,
        kind: TransportKind::Quic,
        outbound: receiver_tx,
        identity: None,
    });

    let schema_out = receiver_rx.recv().await.expect("schema bootstrap");
    assert_eq!(schema_out.envelope.kind, KIND_REP_SCHEMA);
    let table = RepSchemaTable::decode(&schema_out.envelope.body).expect("schema table");
    assert_eq!(table.entries.len(), 1);
    assert_eq!(table.entries[0].class_id, CLASS);
    assert_eq!(table.entries[0].schema_hash, layout().schema_hash().bytes);

    let full_out = receiver_rx
        .recv()
        .await
        .expect("full baseline after schema");
    assert_eq!(full_out.envelope.kind, KIND_REP_DELTA);
    let full = decode(&full_out.envelope.body);
    assert!(full.is_full);
    assert_eq!(
        full.changes.get(&F_HEALTH),
        Some(&FieldDelta::Value(RepValue::Int(10)))
    );

    let ack = RepAck {
        entries: vec![RepAckEntry {
            object_id: OBJ,
            acked_result_id: full.result_id,
            history: 0,
        }],
    }
    .encode()
    .expect("ack encodes");
    assert_eq!(
        gw.handle_inbound(
            receiver,
            &citadel::transport::Envelope::new(KIND_REP_ACK, ack)
        ),
        0
    );
    assert_eq!(
        gw.handle_inbound(
            owner,
            &citadel::transport::Envelope::new(KIND_REP_DELTA, client_health_bunch(1, 37)),
        ),
        1
    );
    let delta_out = receiver_rx.recv().await.expect("authoritative delta");
    let delta = decode(&delta_out.envelope.body);
    assert!(!delta.is_full, "acked bootstrap permits a delta");
    assert_eq!(
        delta.changes.get(&F_HEALTH),
        Some(&FieldDelta::Value(RepValue::Int(37)))
    );
}

#[tokio::test]
async fn reconnect_gets_fresh_schema_and_full_bootstrap() {
    let rep = Arc::new(RepAuthority::new(
        citadel::realtime::netpeer::RateLimits::default(),
    ));
    let gw = Gateway::new().with_rep_authority(Arc::clone(&rep));
    gw.register_rep_class(CLASS, layout(), schema())
        .expect("class registers");
    gw.spawn_rep_object(OBJ, 0, CLASS, None, false, RepSnapshot::new())
        .expect("object spawns");
    let peer = gw.next_participant_id();
    for _ in 0..2 {
        let (tx, mut rx) = mpsc::channel::<Outbound>(8);
        gw.register_session(SessionHandle {
            id: peer,
            kind: TransportKind::Quic,
            outbound: tx,
            identity: None,
        });
        assert_eq!(
            rx.recv().await.expect("schema").envelope.kind,
            KIND_REP_SCHEMA
        );
        assert!(decode(&rx.recv().await.expect("full").envelope.body).is_full);
        gw.unregister_session(peer);
    }
}

#[test]
fn disabled_gateway_drops_rep_and_invalid_server_schema_is_rejected() {
    let disabled = Gateway::new();
    let peer = disabled.next_participant_id();
    assert_eq!(
        disabled.handle_inbound(
            peer,
            &citadel::transport::Envelope::new(KIND_REP_DELTA, vec![0])
        ),
        0
    );
    let enabled = Gateway::new().with_rep_authority(Arc::new(RepAuthority::new(
        citadel::realtime::netpeer::RateLimits::default(),
    )));
    let bad = RepSchema::new(
        citadel_wire::schema::schema_hash(99, &[]).expect("hash"),
        vec![RepFieldCodec::IntRange { min: 0, max: 100 }],
    )
    .expect("well-formed but mismatched schema");
    assert_eq!(
        enabled.register_rep_class(CLASS, layout(), bad),
        Err(citadel::realtime::netpeer::RepReject::SchemaBinding)
    );
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

async fn send_quic_reliable(conn: &quinn::Connection, env: TEnvelope) {
    let mut send = conn.open_uni().await.expect("open reliable uni stream");
    send.write_all(&env.encode_framed())
        .await
        .expect("write reliable frame");
    send.finish().expect("finish reliable stream");
}

async fn recv_quic_reliable_kind(
    conn: &quinn::Connection,
    expected_kind: u16,
) -> Option<TEnvelope> {
    for _ in 0..8 {
        let mut recv = tokio::time::timeout(Duration::from_secs(5), conn.accept_uni())
            .await
            .expect("outbound frame did not time out")
            .expect("outbound stream");
        let data = recv
            .read_to_end(64 * 1024)
            .await
            .expect("read outbound frame");
        let mut buf = bytes::BytesMut::from(&data[..]);
        let env = citadel::transport::codec::decode_framed(&mut buf)
            .expect("decode framed")
            .expect("one frame");
        if env.kind == expected_kind {
            return Some(env);
        }
    }
    None
}

#[tokio::test]
async fn rep_delta_traverses_real_quic_and_peer_sees_authoritative_value() {
    use citadel::lifecycle::Supervisor;

    let cert = SelfSignedCert::generate(&["localhost".to_string()]).expect("cert");
    let rep = Arc::new(RepAuthority::new(
        citadel::realtime::netpeer::RateLimits::default(),
    ));
    let gateway = Arc::new(Gateway::new().with_rep_authority(Arc::clone(&rep)));
    gateway
        .register_rep_class(CLASS, layout(), schema())
        .expect("class");

    // The QUIC server assigns participant ids sequentially from 1. A connects and
    // handshakes first (id 1), then B (id 2).
    let owner_id = 1u64;
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

    // Bind both live QUIC sessions through the production room create/join
    // handlers. The room transitions atomically establish the RepRoomBindings
    // connection scope; direct RepAuthority::join_match calls would bypass it.
    send_quic_reliable(
        &conn_a,
        TEnvelope::new(
            KIND_ROOM_CREATE,
            RoomCreate {
                params: b"netpeer-authority".to_vec(),
            }
            .encode(),
        ),
    )
    .await;
    let room_id = RoomJoined::decode(
        &recv_quic_reliable_kind(&conn_a, KIND_ROOM_JOINED)
            .await
            .expect("owner room join response")
            .body,
    )
    .expect("owner room join decodes")
    .room_id;
    send_quic_reliable(
        &conn_b,
        TEnvelope::new(KIND_ROOM_JOIN, RoomJoin { room_id }.encode()),
    )
    .await;
    let peer_room_id = RoomJoined::decode(
        &recv_quic_reliable_kind(&conn_b, KIND_ROOM_JOINED)
            .await
            .expect("peer room join response")
            .body,
    )
    .expect("peer room join decodes")
    .room_id;
    assert_eq!(peer_room_id, room_id, "both peers join the same room");

    // A RepAuthority match is an internal index, never the room identity. Use a
    // distinct value so this real-QUIC test proves delivery follows the trusted
    // room binding rather than accidentally relying on equal numeric ids.
    const QUIC_MATCH: u64 = 77;
    assert_ne!(QUIC_MATCH, room_id);
    gateway
        .spawn_rep_object(
            OBJ,
            QUIC_MATCH,
            CLASS,
            Some(ParticipantId::from_raw(owner_id)),
            false,
            RepSnapshot::new(),
        )
        .expect("object spawns in the room-bound replication scope");

    // A proposes Health = 150 over a reliable uni stream (out of bound -> clamped).
    let delta = TEnvelope::new(KIND_REP_DELTA, client_health_bunch(10, 150));
    let mut send = conn_a.open_uni().await.expect("open uni for delta");
    send.write_all(&delta.encode_framed())
        .await
        .expect("write delta");
    send.finish().expect("finish delta stream");

    // B reads the server's authoritative rebroadcast off a reliable uni stream.
    let mut bunch = None;
    for _ in 0..3 {
        let mut recv = tokio::time::timeout(Duration::from_secs(5), conn_b.accept_uni())
            .await
            .expect("outbound frame did not time out")
            .expect("outbound stream");
        let data = recv.read_to_end(64 * 1024).await.expect("read outbound");
        let mut buf = bytes::BytesMut::from(&data[..]);
        let env = citadel::transport::codec::decode_framed(&mut buf)
            .expect("decode framed")
            .expect("one frame");
        if env.kind == KIND_REP_DELTA {
            let decoded = decode(&env.body);
            if decoded.changes.get(&F_HEALTH) == Some(&FieldDelta::Value(RepValue::Int(100))) {
                bunch = Some(decoded);
                break;
            }
        }
    }
    let bunch = bunch.expect("authoritative rebroadcast after bootstrap");
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
