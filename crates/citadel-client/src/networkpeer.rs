//! Typed NetworkPeer authoring facade backed exclusively by `citadel-wire`.
//!
//! This owns a bound schema and creates canonical `DeltaBunch` values. It does
//! not reimplement quantization or collection framing: validation and encoding
//! remain in the shared wire crate used by the authoritative server.

use std::collections::BTreeMap;

use citadel_wire::baseline::AckField;
use citadel_wire::netpeer::{
    CollectionDelta, DeltaBunch, FieldDelta, RepAck, RepAckEntry, RepFieldCodec, RepSchema,
    RepValue,
};
use citadel_wire::protocol::{KIND_REP_ACK, KIND_REP_DELTA};
use citadel_wire::schema::SchemaHash;

/// A schema-bound authoring surface for full and delta NetworkPeer packets.
#[derive(Debug, Clone)]
pub struct NetworkPeerAuthor {
    schema: RepSchema,
}

impl NetworkPeerAuthor {
    /// Bind a canonical schema. Invalid ranges and nested collections are
    /// rejected by the same validator as the server.
    pub fn new(
        schema_hash: SchemaHash,
        fields: Vec<RepFieldCodec>,
    ) -> Result<Self, citadel_wire::netpeer::RepWireError> {
        Ok(Self {
            schema: RepSchema::new(schema_hash, fields)?,
        })
    }

    /// The immutable schema used for every authored packet.
    pub fn schema(&self) -> &RepSchema {
        &self.schema
    }

    /// Start a full snapshot. `result_id` must be nonzero; full packets use base 0.
    pub fn full(&self, object_id: u32, result_id: u64) -> Option<NetworkPeerDraft<'_>> {
        (result_id != 0).then(|| NetworkPeerDraft {
            schema: &self.schema,
            bunch: DeltaBunch::new(object_id, true, result_id, 0),
            failed: false,
        })
    }

    /// Start a delta. Both server-issued result and base tokens must be nonzero.
    pub fn delta(
        &self,
        object_id: u32,
        result_id: u64,
        base_id: u64,
    ) -> Option<NetworkPeerDraft<'_>> {
        (result_id != 0 && base_id != 0).then(|| NetworkPeerDraft {
            schema: &self.schema,
            bunch: DeltaBunch::new(object_id, false, result_id, base_id),
            failed: false,
        })
    }
}

/// A one-packet typed authoring transaction. Any invalid field/type makes the
/// entire draft fail; callers cannot accidentally emit a partial bunch.
pub struct NetworkPeerDraft<'a> {
    schema: &'a RepSchema,
    bunch: DeltaBunch,
    failed: bool,
}

impl NetworkPeerDraft<'_> {
    fn value(&mut self, field_id: u16, value: RepValue) -> &mut Self {
        if self
            .schema
            .field(field_id)
            .is_none_or(|codec| codec.is_collection() || !codec_accepts(codec, &value))
        {
            self.failed = true;
        } else {
            self.bunch.set(field_id, FieldDelta::Value(value));
        }
        self
    }
    pub fn bool(&mut self, field_id: u16, value: bool) -> &mut Self {
        self.value(field_id, RepValue::Bool(value))
    }
    pub fn int(&mut self, field_id: u16, value: i64) -> &mut Self {
        self.value(field_id, RepValue::Int(value))
    }
    pub fn scalar(&mut self, field_id: u16, value: f32) -> &mut Self {
        self.value(field_id, RepValue::Scalar(value))
    }
    pub fn vector3(&mut self, field_id: u16, value: [f32; 3]) -> &mut Self {
        self.value(field_id, RepValue::Vector3(value))
    }
    pub fn quat(&mut self, field_id: u16, value: [f32; 4]) -> &mut Self {
        self.value(field_id, RepValue::Quat(value))
    }
    pub fn bytes(&mut self, field_id: u16, value: impl AsRef<[u8]>) -> &mut Self {
        self.value(field_id, RepValue::Bytes(value.as_ref().to_vec()))
    }

    /// Set an already-keyed collection delta. The shared encoder enforces caps,
    /// duplicate IDs, add/remove/change precedence, `rep_key`, and all-or-nothing
    /// serialization semantics.
    pub fn collection(&mut self, field_id: u16, value: CollectionDelta) -> &mut Self {
        if !self
            .schema
            .field(field_id)
            .is_some_and(RepFieldCodec::is_collection)
        {
            self.failed = true;
        } else {
            self.bunch.set(field_id, FieldDelta::Collection(value));
        }
        self
    }

    /// Encode through the canonical server wire codec, or reject the whole draft.
    pub fn finish(self) -> Result<Vec<u8>, citadel_wire::netpeer::RepWireError> {
        if self.failed {
            return Err(citadel_wire::netpeer::RepWireError::InvalidSchema(
                "field value does not match bound schema",
            ));
        }
        self.bunch.encode(self.schema)
    }
    /// Return the validated logical packet for callers retaining lower-level wire access.
    pub fn into_bunch(self) -> Result<DeltaBunch, citadel_wire::netpeer::RepWireError> {
        if self.failed {
            return Err(citadel_wire::netpeer::RepWireError::InvalidSchema(
                "field value does not match bound schema",
            ));
        }
        Ok(self.bunch)
    }
}

fn codec_accepts(codec: &RepFieldCodec, value: &RepValue) -> bool {
    matches!(
        (codec, value),
        (RepFieldCodec::Bool, RepValue::Bool(_))
            | (RepFieldCodec::IntRange { .. }, RepValue::Int(_))
            | (RepFieldCodec::Scalar(_), RepValue::Scalar(_))
            | (RepFieldCodec::Vector3(_), RepValue::Vector3(_))
            | (RepFieldCodec::Quat(_), RepValue::Quat(_))
            | (RepFieldCodec::Bytes { .. }, RepValue::Bytes(_))
    )
}

/// Outcome of routing one authoritative NetworkPeer delta envelope.
#[derive(Debug, Clone, PartialEq)]
pub enum NetworkPeerReceive {
    /// The packet was accepted and establishes the returned result token.
    Applied(DeltaBunch),
    /// The packet is older than (or equal to) a packet already applied.
    Stale,
    /// The delta names a base the client does not retain; request/resend a full
    /// snapshot rather than applying it against an invented state.
    NeedsFull {
        object_id: u32,
        expected_base: Option<u64>,
    },
}

/// Schema-bound NetworkPeer receive/ack state.
///
/// This facade deliberately owns only client receive baselines: result tokens
/// are issued by the server and never minted client-side. Accepted packets are
/// accumulated into the shared 32-bit ack window, and callers can route the
/// resulting `KIND_REP_ACK` envelope through any transport.
#[derive(Debug, Clone)]
pub struct NetworkPeerSession {
    schema: RepSchema,
    baselines: BTreeMap<u32, u64>,
    acks: BTreeMap<u32, AckField>,
}

impl NetworkPeerSession {
    /// Bind the class schema used to decode every routed bunch.
    pub fn new(schema: RepSchema) -> Self {
        Self {
            schema,
            baselines: BTreeMap::new(),
            acks: BTreeMap::new(),
        }
    }

    /// Decode and apply one `KIND_REP_DELTA` body using full/delta baseline
    /// rules. Malformed bodies remain wire errors; stale and missing-base
    /// packets are normal datagram outcomes represented by [`NetworkPeerReceive`].
    pub fn apply_body(
        &mut self,
        body: &[u8],
    ) -> Result<NetworkPeerReceive, citadel_wire::netpeer::RepWireError> {
        let mut budget = citadel_wire::netpeer::MAX_ENVELOPE_ALLOC;
        let bunch = DeltaBunch::decode(body, &self.schema, &mut budget)?;
        let current = self.baselines.get(&bunch.object_id).copied();
        if current.is_some_and(|id| bunch.result_id <= id) {
            return Ok(NetworkPeerReceive::Stale);
        }
        if !bunch.is_full && current != Some(bunch.base_id) {
            return Ok(NetworkPeerReceive::NeedsFull {
                object_id: bunch.object_id,
                expected_base: current,
            });
        }
        self.baselines.insert(bunch.object_id, bunch.result_id);
        self.acks
            .entry(bunch.object_id)
            .or_default()
            .ack(bunch.result_id);
        Ok(NetworkPeerReceive::Applied(bunch))
    }

    /// Route one client-facing envelope. Other kinds are rejected so a caller
    /// cannot accidentally feed a transform or application payload to this
    /// replication session.
    pub fn apply_envelope(
        &mut self,
        envelope: &citadel_wire::Envelope,
    ) -> Result<NetworkPeerReceive, citadel_wire::netpeer::RepWireError> {
        if envelope.kind != KIND_REP_DELTA {
            return Err(citadel_wire::netpeer::RepWireError::InvalidSchema(
                "expected KIND_REP_DELTA",
            ));
        }
        self.apply_body(&envelope.body)
    }

    /// Build the canonical `KIND_REP_ACK` envelope for every object accepted
    /// by this session. Repeated calls are safe: the ack window is intentionally
    /// resent until the server observes it.
    pub fn ack_envelope(
        &self,
    ) -> Result<citadel_wire::Envelope, citadel_wire::netpeer::RepWireError> {
        let entries = self
            .acks
            .iter()
            .map(|(&object_id, ack)| {
                let (acked_result_id, history) = ack.to_wire();
                RepAckEntry {
                    object_id,
                    acked_result_id,
                    history,
                }
            })
            .collect();
        Ok(citadel_wire::Envelope::new(
            KIND_REP_ACK,
            RepAck { entries }.encode()?,
        ))
    }

    /// Last accepted result token for an object, if any.
    pub fn baseline(&self, object_id: u32) -> Option<u64> {
        self.baselines.get(&object_id).copied()
    }
}
