//! Room (match/lobby) wire messages (kinds 21-25, plus the server-driven
//! match-closed notification on kind 32).
//!
//! Rooms are server-owned, admission-gated groupings of participants. A client
//! creates a room ([`RoomCreate`]) or joins an existing one ([`RoomJoin`]); the
//! server runs the game's Lua admission logic, assigns membership, and replies with
//! [`RoomJoined`], which carries the room's **map** name — the "you are in room R,
//! load this map" signal. The map/mode come from the room's Lua-set label, so the
//! game controls what each room loads. [`RoomLeave`] leaves (or notifies of removal)
//! and [`RoomMapReady`] acks that the client has the map/level open.
//!
//! All integers are big-endian; strings are `u16` length-prefixed UTF-8, matching
//! the rest of the wire.

use crate::tsync::TsyncError;

/// `KIND_ROOM_CREATE` (C→S, reliable): request to create a room. `params` are opaque
/// bytes the game's `on_room_create` Lua hook interprets (e.g. a desired map name or
/// a small encoded config); the server assigns the room id and label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomCreate {
    /// Opaque, game-defined creation params (may be empty).
    pub params: Vec<u8>,
}

impl RoomCreate {
    /// Encode: `u16` length + that many param bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let len = self.params.len().min(u16::MAX as usize);
        let mut buf = Vec::with_capacity(2 + len);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
        buf.extend_from_slice(&self.params[..len]);
        buf
    }

    /// Decode a create body, rejecting a length that overruns the bytes.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, 2)?;
        let mut off = 0usize;
        let len = read_u16(body, &mut off) as usize;
        need(body, 2 + len)?;
        let params = body[off..off + len].to_vec();
        Ok(Self { params })
    }
}

/// `KIND_ROOM_JOIN` (C→S, reliable): request to join an existing room by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomJoin {
    /// The room to join.
    pub room_id: u64,
}

/// Bytes in a serialized [`RoomJoin`].
pub const ROOM_JOIN_BYTES: usize = 8;

impl RoomJoin {
    /// Encode the join body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.room_id.to_be_bytes().to_vec()
    }

    /// Decode a join body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, ROOM_JOIN_BYTES)?;
        let mut off = 0usize;
        Ok(Self {
            room_id: read_u64(body, &mut off),
        })
    }
}

/// `KIND_ROOM_JOINED` (S→C, reliable): confirmation that the participant is in the
/// room, carrying the map/mode from the room's label so the client can load the level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomJoined {
    /// The room the participant joined.
    pub room_id: u64,
    /// The map/level name the client must have open (from the room label).
    pub map: String,
    /// The room's game mode (free-form, game-defined; may be empty).
    pub mode: String,
}

impl RoomJoined {
    /// Encode: `room_id u64`, then `map` and `mode` as `u16`-length UTF-8.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 2 + self.map.len() + 2 + self.mode.len());
        buf.extend_from_slice(&self.room_id.to_be_bytes());
        write_str(&mut buf, &self.map);
        write_str(&mut buf, &self.mode);
        buf
    }

    /// Decode a joined body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, 8)?;
        let mut off = 0usize;
        let room_id = read_u64(body, &mut off);
        let map = read_str(body, &mut off)?;
        let mode = read_str(body, &mut off)?;
        Ok(Self { room_id, map, mode })
    }
}

/// `KIND_ROOM_LEAVE` (C→S request or S→C notify, reliable): leave/removed from a room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomLeave {
    /// The room being left.
    pub room_id: u64,
}

/// Bytes in a serialized [`RoomLeave`].
pub const ROOM_LEAVE_BYTES: usize = 8;

impl RoomLeave {
    /// Encode the leave body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.room_id.to_be_bytes().to_vec()
    }

    /// Decode a leave body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, ROOM_LEAVE_BYTES)?;
        let mut off = 0usize;
        Ok(Self {
            room_id: read_u64(body, &mut off),
        })
    }
}

/// `KIND_ROOM_MAP_READY` (C→S, reliable): the client has the room's map/level open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomMapReady {
    /// The room whose map is now loaded on the client.
    pub room_id: u64,
}

/// Bytes in a serialized [`RoomMapReady`].
pub const ROOM_MAP_READY_BYTES: usize = 8;

impl RoomMapReady {
    /// Encode the map-ready body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.room_id.to_be_bytes().to_vec()
    }

    /// Decode a map-ready body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, ROOM_MAP_READY_BYTES)?;
        let mut off = 0usize;
        Ok(Self {
            room_id: read_u64(body, &mut off),
        })
    }
}

/// [`MatchClosed`] reason: the match's server-side execution failed (script
/// fault, blown budget, or a worker crash). Deliberately coarse: the client
/// learns the match is gone, never why at a level that leaks server internals.
pub const MATCH_CLOSE_REASON_SERVER_ERROR: u8 = 0;

/// `KIND_MATCH_CLOSED` (S→C, reliable): the participant's authoritative match
/// was closed by the server and its room no longer exists.
///
/// Carries a coarse `reason` and a `requeue_hint`. The requeue model is
/// client-prompted: when `requeue_hint` is set the client should prompt (or
/// auto-drive) its own fresh matchmaker ticket submission — the server retains
/// no ticket on the member's behalf, so a raw reconnect can never land back in
/// the dead match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchClosed {
    /// The room (match) that was closed.
    pub room_id: u64,
    /// Coarse close reason ([`MATCH_CLOSE_REASON_SERVER_ERROR`]).
    pub reason: u8,
    /// Whether the client should re-submit its own matchmaker ticket.
    pub requeue_hint: bool,
}

/// Bytes in a serialized [`MatchClosed`]: `room_id` (8) + `reason` (1) +
/// `requeue_hint` (1).
pub const MATCH_CLOSED_BYTES: usize = 10;

impl MatchClosed {
    /// Encode the match-closed body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(MATCH_CLOSED_BYTES);
        buf.extend_from_slice(&self.room_id.to_be_bytes());
        buf.push(self.reason);
        buf.push(u8::from(self.requeue_hint));
        buf
    }

    /// Decode a match-closed body. Any nonzero requeue byte reads as a hint,
    /// so a future hint refinement stays decodable by this version.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, MATCH_CLOSED_BYTES)?;
        let mut off = 0usize;
        let room_id = read_u64(body, &mut off);
        let reason = body[off];
        let requeue_hint = body[off + 1] != 0;
        Ok(Self {
            room_id,
            reason,
            requeue_hint,
        })
    }
}

// ------------------------------- byte helpers ------------------------------- //

fn need(body: &[u8], needed: usize) -> Result<(), TsyncError> {
    if body.len() < needed {
        Err(TsyncError::TooShort {
            needed,
            got: body.len(),
        })
    } else {
        Ok(())
    }
}

fn read_u16(body: &[u8], off: &mut usize) -> u16 {
    let v = u16::from_be_bytes([body[*off], body[*off + 1]]);
    *off += 2;
    v
}

fn read_u64(body: &[u8], off: &mut usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&body[*off..*off + 8]);
    *off += 8;
    u64::from_be_bytes(b)
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len().min(u16::MAX as usize);
    buf.extend_from_slice(&(len as u16).to_be_bytes());
    buf.extend_from_slice(&bytes[..len]);
}

fn read_str(body: &[u8], off: &mut usize) -> Result<String, TsyncError> {
    need(body, *off + 2)?;
    let len = read_u16(body, off) as usize;
    need(body, *off + len)?;
    let s = std::str::from_utf8(&body[*off..*off + len])
        .map_err(|_| TsyncError::OutOfRange("room string is not valid UTF-8"))?
        .to_owned();
    *off += len;
    Ok(s)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn room_create_round_trips_including_empty() {
        for params in [vec![], vec![1u8, 2, 3, 4]] {
            let m = RoomCreate {
                params: params.clone(),
            };
            assert_eq!(RoomCreate::decode(&m.encode()).unwrap(), m);
        }
    }

    #[test]
    fn room_create_rejects_overrun_length() {
        // Claims 10 param bytes but carries none.
        let mut body = 10u16.to_be_bytes().to_vec();
        body.truncate(2);
        assert!(matches!(
            RoomCreate::decode(&body),
            Err(TsyncError::TooShort { .. })
        ));
    }

    #[test]
    fn room_join_leave_map_ready_round_trip() {
        assert_eq!(
            RoomJoin::decode(&RoomJoin { room_id: 42 }.encode()).unwrap(),
            RoomJoin { room_id: 42 }
        );
        assert_eq!(
            RoomLeave::decode(&RoomLeave { room_id: 7 }.encode()).unwrap(),
            RoomLeave { room_id: 7 }
        );
        assert_eq!(
            RoomMapReady::decode(&RoomMapReady { room_id: 9 }.encode()).unwrap(),
            RoomMapReady { room_id: 9 }
        );
    }

    #[test]
    fn room_joined_round_trips_with_strings() {
        let m = RoomJoined {
            room_id: 1234,
            map: "ForestArena".to_owned(),
            mode: "ffa".to_owned(),
        };
        assert_eq!(RoomJoined::decode(&m.encode()).unwrap(), m);
        // Empty mode is valid.
        let m2 = RoomJoined {
            room_id: 1,
            map: "Lobby".to_owned(),
            mode: String::new(),
        };
        assert_eq!(RoomJoined::decode(&m2.encode()).unwrap(), m2);
    }

    #[test]
    fn room_joined_rejects_truncated_string() {
        let mut body = 5u64.to_be_bytes().to_vec();
        body.extend_from_slice(&20u16.to_be_bytes()); // claims a 20-byte map name
        body.extend_from_slice(b"short"); // but only 5 bytes follow
        assert!(matches!(
            RoomJoined::decode(&body),
            Err(TsyncError::TooShort { .. })
        ));
    }

    #[test]
    fn match_closed_round_trips_reason_and_requeue_hint() {
        let closed = MatchClosed {
            room_id: 77,
            reason: MATCH_CLOSE_REASON_SERVER_ERROR,
            requeue_hint: true,
        };
        let body = closed.encode();
        assert_eq!(body.len(), MATCH_CLOSED_BYTES);
        assert_eq!(MatchClosed::decode(&body).unwrap(), closed);
        let quiet = MatchClosed {
            room_id: 1,
            reason: MATCH_CLOSE_REASON_SERVER_ERROR,
            requeue_hint: false,
        };
        assert_eq!(MatchClosed::decode(&quiet.encode()).unwrap(), quiet);
    }

    #[test]
    fn match_closed_rejects_truncated_body() {
        let mut body = MatchClosed {
            room_id: 3,
            reason: MATCH_CLOSE_REASON_SERVER_ERROR,
            requeue_hint: true,
        }
        .encode();
        body.pop();
        assert!(matches!(
            MatchClosed::decode(&body),
            Err(TsyncError::TooShort { .. })
        ));
    }

    #[test]
    fn room_joined_rejects_invalid_utf8() {
        let mut body = 1u64.to_be_bytes().to_vec();
        body.extend_from_slice(&2u16.to_be_bytes());
        body.extend_from_slice(&[0xff, 0xfe]); // invalid UTF-8
        assert!(matches!(
            RoomJoined::decode(&body),
            Err(TsyncError::OutOfRange(_))
        ));
    }
}
