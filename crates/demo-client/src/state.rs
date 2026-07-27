//! Testable network/state logic for the demo, independent of rendering.
//!
//! The render loop (macroquad) and the network task both operate on these
//! types, but nothing here depends on macroquad, so it is unit-tested.
//!
//! Protocol (shared via `citadel-wire::protocol`):
//! - We send our position as `KIND_POSITION` (body: two LE f32).
//! - The gateway relays it to peers as `KIND_PEER_POSITION` (body: 8-byte BE
//!   sender id + the position payload). We render each peer by sender id.

use std::collections::HashMap;

use citadel_wire::Envelope;
use citadel_wire::protocol::{self, KIND_PEER_POSITION, KIND_POSITION};

/// A 2D position in world units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pos {
    /// Horizontal coordinate.
    pub x: f32,
    /// Vertical coordinate.
    pub y: f32,
}

impl Pos {
    /// Construct a position.
    #[must_use]
    pub fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }

    /// Encode this position as a payload of two little-endian `f32`.
    #[must_use]
    pub fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8);
        buf.extend_from_slice(&self.x.to_le_bytes());
        buf.extend_from_slice(&self.y.to_le_bytes());
        buf
    }

    /// Decode a position from a two-`f32` little-endian payload.
    ///
    /// Returns `None` if the payload is not exactly 8 bytes.
    #[must_use]
    pub fn decode(payload: &[u8]) -> Option<Self> {
        if payload.len() != 8 {
            return None;
        }
        let x = f32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let y = f32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
        Some(Self { x, y })
    }

    /// Clamp the position into the inclusive square `[-limit, limit]`.
    #[must_use]
    pub fn clamped(self, limit: f32) -> Self {
        Self {
            x: self.x.clamp(-limit, limit),
            y: self.y.clamp(-limit, limit),
        }
    }
}

impl Default for Pos {
    fn default() -> Self {
        Self::new(0.0, 0.0)
    }
}

/// Build the envelope that reports our own position to the server.
#[must_use]
pub fn position_envelope(pos: Pos) -> Envelope {
    Envelope::new(KIND_POSITION, pos.encode())
}

/// The demo's view of the world: our position and the last known position of
/// every other player, keyed by their session id.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WorldState {
    /// The locally controlled position.
    pub local: Pos,
    /// Last position of each peer, keyed by relayed sender session id.
    pub peers: HashMap<u64, Pos>,
}

impl WorldState {
    /// Apply a movement delta to the local position, clamped to `limit`.
    pub fn move_local(&mut self, dx: f32, dy: f32, limit: f32) {
        self.local = Pos::new(self.local.x + dx, self.local.y + dy).clamped(limit);
    }

    /// Apply a relayed envelope from the server, updating the matching peer.
    ///
    /// Unknown kinds and malformed payloads are ignored. Returns `true` if a
    /// peer position was updated.
    pub fn apply_relayed(&mut self, env: &Envelope) -> bool {
        if env.kind != KIND_PEER_POSITION {
            return false;
        }
        let Some((sender_id, payload)) = protocol::split_sender(&env.body) else {
            return false;
        };
        let Some(pos) = Pos::decode(payload) else {
            return false;
        };
        self.peers.insert(sender_id, pos);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_round_trips_through_payload() {
        let p = Pos::new(1.5, -2.25);
        let decoded = Pos::decode(&p.encode()).expect("decode");
        assert_eq!(decoded, p);
    }

    #[test]
    fn decode_rejects_wrong_length() {
        assert!(Pos::decode(&[0u8; 4]).is_none());
        assert!(Pos::decode(&[0u8; 8]).is_some());
    }

    #[test]
    fn clamp_bounds_position() {
        let p = Pos::new(100.0, -100.0).clamped(9.0);
        assert_eq!(p, Pos::new(9.0, -9.0));
    }

    #[test]
    fn apply_relayed_updates_peer_by_sender_id() {
        // Build a relayed envelope as the gateway would: tagged peer position.
        let payload = Pos::new(3.0, 4.0).encode();
        let body = protocol::tag_with_sender(77, &payload);
        let env = Envelope::new(KIND_PEER_POSITION, body);

        let mut world = WorldState::default();
        assert!(world.apply_relayed(&env));
        assert_eq!(world.peers.get(&77), Some(&Pos::new(3.0, 4.0)));
    }

    #[test]
    fn apply_relayed_ignores_wrong_kind_and_bad_body() {
        let mut world = WorldState::default();
        // Wrong kind.
        assert!(!world.apply_relayed(&Envelope::new(KIND_POSITION, &b"xxxxxxxx"[..])));
        // Right kind, body too short to contain a sender id.
        assert!(!world.apply_relayed(&Envelope::new(KIND_PEER_POSITION, &b"abc"[..])));
        assert!(world.peers.is_empty());
    }

    #[test]
    fn move_local_accumulates_and_clamps() {
        let mut world = WorldState::default();
        world.move_local(5.0, 5.0, 9.0);
        world.move_local(10.0, 10.0, 9.0);
        assert_eq!(world.local, Pos::new(9.0, 9.0));
    }

    #[test]
    fn position_envelope_uses_position_kind() {
        let env = position_envelope(Pos::new(1.0, 2.0));
        assert_eq!(env.kind, KIND_POSITION);
        assert_eq!(Pos::decode(&env.body), Some(Pos::new(1.0, 2.0)));
    }
}
