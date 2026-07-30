use citadel_client::{NetworkPeerAuthor, NetworkPeerReceive, NetworkPeerSession};
use citadel_wire::codec::{DEFAULT_WORLD_BOUNDS, QuatMode, ScalarQuant, VectorQuant, WorldBounds};
use citadel_wire::netpeer::{
    CollItem, CollectionDelta, DeltaBunch, FieldDelta, RepFieldCodec, RepId, RepSchema, RepValue,
    RepWireError,
};
use citadel_wire::schema::SchemaHash;
use citadel_wire::{Envelope, protocol::KIND_REP_DELTA};

fn fixture_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn fixture_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("fixture hex is valid"))
        .collect()
}

fn golden_schema() -> RepSchema {
    RepSchema::new(
        SchemaHash {
            bytes: [7; 16],
            layout_version: 3,
        },
        vec![
            RepFieldCodec::Bool,
            RepFieldCodec::IntRange { min: -10, max: 10 },
            RepFieldCodec::Scalar(ScalarQuant::new(-100.0, 100.0, 10).expect("valid scalar")),
            RepFieldCodec::Vector3(
                VectorQuant::new(WorldBounds {
                    min: [-100.0; 3],
                    max: [100.0; 3],
                    values_per_unit: 10,
                })
                .expect("valid vector"),
            ),
            RepFieldCodec::Quat(QuatMode::Bits10),
            RepFieldCodec::Bytes { max_len: 32 },
            RepFieldCodec::Collection {
                item: Box::new(RepFieldCodec::IntRange { min: 0, max: 100 }),
                max_items: 4,
            },
        ],
    )
    .expect("valid golden schema")
}

fn schema() -> (SchemaHash, Vec<RepFieldCodec>) {
    (
        SchemaHash {
            bytes: [7; 16],
            layout_version: 3,
        },
        vec![
            RepFieldCodec::Vector3(
                VectorQuant::new(DEFAULT_WORLD_BOUNDS).expect("test fixture is valid"),
            ),
            RepFieldCodec::Quat(QuatMode::Bits10),
            RepFieldCodec::Collection {
                item: Box::new(RepFieldCodec::Bool),
                max_items: 4,
            },
        ],
    )
}

#[test]
fn authors_vector_quat_with_shared_codec() {
    let (id, fields) = schema();
    let author = NetworkPeerAuthor::new(id, fields.clone()).expect("test fixture is valid");
    let mut draft = author.full(9, 1).expect("test fixture is valid");
    draft
        .vector3(0, [1.0, -2.0, 3.0])
        .quat(1, [0.0, 0.0, 0.0, 1.0]);
    let bytes = draft.finish().expect("test fixture is valid");
    let mut budget = citadel_wire::netpeer::MAX_ENVELOPE_ALLOC;
    let decoded = citadel_wire::netpeer::DeltaBunch::decode(
        &bytes,
        &RepSchema::new(id, fields).expect("test fixture is valid"),
        &mut budget,
    )
    .expect("test fixture is valid");
    assert!(matches!(
        decoded.changes[&0],
        citadel_wire::netpeer::FieldDelta::Value(RepValue::Vector3(_))
    ));
    assert!(matches!(
        decoded.changes[&1],
        citadel_wire::netpeer::FieldDelta::Value(RepValue::Quat(_))
    ));
}

#[test]
fn bad_type_fails_the_entire_draft() {
    let (id, fields) = schema();
    let author = NetworkPeerAuthor::new(id, fields).expect("test fixture is valid");
    let mut draft = author.delta(9, 2, 1).expect("test fixture is valid");
    draft
        .scalar(0, 1.0)
        .collection(2, CollectionDelta::default());
    assert!(draft.finish().is_err());
}

#[test]
fn canonical_rust_golden_vectors_pin_cross_engine_semantics_and_u32_rep_ids() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/networkpeer-cross-engine-v1.json"
    ))
    .expect("fixture is valid JSON");
    let vectors = fixture["golden_vectors"]
        .as_array()
        .expect("golden vectors array");
    let vector = |id: &str| {
        vectors
            .iter()
            .find(|v| v["id"] == id)
            .expect("golden vector exists")
    };
    let schema = golden_schema();

    let mut full = DeltaBunch::new(9, true, 3, 0);
    full.set(0, FieldDelta::Value(RepValue::Bool(true)));
    full.set(1, FieldDelta::Value(RepValue::Int(4)));
    full.set(2, FieldDelta::Value(RepValue::Scalar(1.2)));
    full.set(3, FieldDelta::Value(RepValue::Vector3([1.0, -2.0, 3.0])));
    full.set(4, FieldDelta::Value(RepValue::Quat([0.0, 0.0, 0.0, 1.0])));
    full.set(5, FieldDelta::Value(RepValue::Bytes(b"citadel".to_vec())));
    full.set(
        6,
        FieldDelta::Collection(CollectionDelta {
            removed: vec![RepId {
                index: 1,
                generation: 2,
            }],
            added: vec![CollItem {
                id: RepId {
                    index: 3,
                    generation: 1,
                },
                key: 8,
                value: RepValue::Int(7),
            }],
            changed: vec![CollItem {
                id: RepId {
                    index: 4,
                    generation: 1,
                },
                key: 9,
                value: RepValue::Int(8),
            }],
        }),
    );
    assert_eq!(
        fixture_hex(&full.encode(&schema).expect("full encodes")),
        vector("canonical_full_all_value_families")["encoded_hex"]
            .as_str()
            .expect("full vector has encoded hex")
    );

    let mut delta = DeltaBunch::new(9, false, 5, 3);
    delta.set(0, FieldDelta::Value(RepValue::Bool(false)));
    delta.set(
        6,
        FieldDelta::Collection(CollectionDelta {
            removed: vec![RepId {
                index: u32::MAX,
                generation: u32::MAX,
            }],
            added: vec![],
            changed: vec![],
        }),
    );
    let delta_bytes = delta.encode(&schema).expect("delta encodes");
    assert_eq!(
        fixture_hex(&delta_bytes),
        vector("canonical_delta_u32_rep_id_boundaries")["encoded_hex"]
            .as_str()
            .expect("boundary vector has encoded hex")
    );
    let mut budget = citadel_wire::netpeer::MAX_ENVELOPE_ALLOC;
    let decoded = DeltaBunch::decode(&delta_bytes, &schema, &mut budget).expect("delta decodes");
    assert!(matches!(
        &decoded.changes[&6],
        FieldDelta::Collection(CollectionDelta { removed, added, changed })
            if removed == &vec![RepId { index: u32::MAX, generation: u32::MAX }]
                && added.is_empty() && changed.is_empty()
    ));

    let overflow = vector("reject_rep_id_index_above_u32")["encoded_hex"]
        .as_str()
        .expect("overflow vector has encoded hex");
    let mut budget = citadel_wire::netpeer::MAX_ENVELOPE_ALLOC;
    assert_eq!(
        DeltaBunch::decode(&fixture_bytes(overflow), &schema, &mut budget),
        Err(RepWireError::VarintOverflow)
    );

    let generation_overflow = vector("reject_rep_id_generation_above_u32")["encoded_hex"]
        .as_str()
        .expect("generation overflow vector has encoded hex");
    let mut budget = citadel_wire::netpeer::MAX_ENVELOPE_ALLOC;
    assert_eq!(
        DeltaBunch::decode(&fixture_bytes(generation_overflow), &schema, &mut budget),
        Err(RepWireError::VarintOverflow)
    );
}

#[test]
fn session_acks_applied_bunches_rejects_stale_and_requests_full_for_a_missing_base() {
    let (id, fields) = schema();
    let author = NetworkPeerAuthor::new(id, fields.clone()).expect("valid author schema");
    let mut full = author.full(9, 3).expect("nonzero full token");
    full.vector3(0, [1.0, 2.0, 3.0]);
    let full = full.finish().expect("valid full packet");
    let mut session = NetworkPeerSession::new(RepSchema::new(id, fields).expect("valid schema"));
    assert!(matches!(
        session.apply_body(&full).expect("valid full packet"),
        NetworkPeerReceive::Applied(_)
    ));
    assert_eq!(session.baseline(9), Some(3));
    assert_eq!(
        session.apply_body(&full).expect("valid stale packet"),
        NetworkPeerReceive::Stale
    );

    let mut missing_base = author.delta(9, 5, 4).expect("nonzero delta tokens");
    missing_base.vector3(0, [4.0, 5.0, 6.0]);
    let missing_base = missing_base.finish().expect("valid delta packet");
    assert_eq!(
        session
            .apply_envelope(&Envelope::new(KIND_REP_DELTA, missing_base))
            .expect("valid delta envelope"),
        NetworkPeerReceive::NeedsFull {
            object_id: 9,
            expected_base: Some(3)
        }
    );
    let ack = session.ack_envelope().expect("ack fits cap");
    assert_eq!(ack.kind, citadel_wire::protocol::KIND_REP_ACK);
    let decoded = citadel_wire::netpeer::RepAck::decode(&ack.body).expect("valid ack body");
    assert_eq!(decoded.entries[0].object_id, 9);
    assert_eq!(decoded.entries[0].acked_result_id, 3);
}
