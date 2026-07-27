//! Networked-Actors wire messages: presence + replicated spawn (kinds 16-20).
//!
//! A thin layer **above** transform-sync that makes player/actor replication work
//! out of the box: a client announces its avatar on connect ([`NaPresence`]), the
//! server tells every other client to spawn it ([`NaSpawn`]) and hands the newcomer
//! everyone already present ([`NaSpawnBatch`]), and despawns on disconnect
//! ([`NaDespawn`]). In relay mode the owner reports its own transform each tick
//! ([`NaState`]); the server applies it and the normal transform-sync snapshots
//! replicate it to observers, so this layer never runs its own interpolation.
//!
//! Transforms here are **raw** (`f32` position + quaternion + velocity), not the
//! quantized snapshot codec: presence/spawn are rare and `NaState` is one small
//! message per owner per tick, while the bandwidth-sensitive observer path is the
//! already-quantized snapshot. All integers are big-endian, matching the rest of
//! the wire.

use crate::tsync::TsyncError;

/// A raw (unquantized) transform carried by the Networked-Actors messages.
///
/// `rotation` is a quaternion in `xyzw` order. `velocity` lets the observer path
/// use Hermite interpolation once the server republishes it in snapshots.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NaTransform {
    /// World position (cm), `[x, y, z]`.
    pub position: [f32; 3],
    /// Rotation quaternion, `[x, y, z, w]`.
    pub rotation: [f32; 4],
    /// Linear velocity (cm/s), `[x, y, z]`.
    pub velocity: [f32; 3],
}

/// Bytes in a serialized [`NaTransform`]: 3+4+3 `f32`.
pub const NA_TRANSFORM_BYTES: usize = (3 + 4 + 3) * 4;

impl NaTransform {
    /// A transform at the origin, identity rotation, zero velocity.
    #[must_use]
    pub fn identity() -> Self {
        Self {
            position: [0.0; 3],
            rotation: [0.0, 0.0, 0.0, 1.0],
            velocity: [0.0; 3],
        }
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        for v in self.position {
            buf.extend_from_slice(&v.to_be_bytes());
        }
        for v in self.rotation {
            buf.extend_from_slice(&v.to_be_bytes());
        }
        for v in self.velocity {
            buf.extend_from_slice(&v.to_be_bytes());
        }
    }

    /// Decode a transform at `off` (caller guarantees `>= NA_TRANSFORM_BYTES`
    /// remain), advancing `off`.
    fn decode_at(body: &[u8], off: &mut usize) -> Self {
        let mut position = [0f32; 3];
        for p in &mut position {
            *p = read_f32(body, off);
        }
        let mut rotation = [0f32; 4];
        for r in &mut rotation {
            *r = read_f32(body, off);
        }
        let mut velocity = [0f32; 3];
        for v in &mut velocity {
            *v = read_f32(body, off);
        }
        Self {
            position,
            rotation,
            velocity,
        }
    }
}

/// `KIND_NA_PRESENCE` (C→S, reliable): a connecting client announces its avatar.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NaPresence {
    /// Which registered actor archetype represents this client.
    pub archetype_id: u16,
    /// The client's initial transform (spawn pose the peers should see).
    pub transform: NaTransform,
}

/// Bytes in a serialized [`NaPresence`].
pub const NA_PRESENCE_BYTES: usize = 2 + NA_TRANSFORM_BYTES;

impl NaPresence {
    /// Encode the presence body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(NA_PRESENCE_BYTES);
        buf.extend_from_slice(&self.archetype_id.to_be_bytes());
        self.transform.encode_into(&mut buf);
        buf
    }

    /// Decode a presence body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, NA_PRESENCE_BYTES)?;
        let mut off = 0usize;
        let archetype_id = read_u16(body, &mut off);
        let transform = NaTransform::decode_at(body, &mut off);
        Ok(Self {
            archetype_id,
            transform,
        })
    }
}

/// `KIND_NA_SPAWN` (S→C, reliable): instantiate one networked actor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NaSpawn {
    /// Transform-sync object id this actor binds to.
    pub object_id: u32,
    /// Which registered archetype to instantiate.
    pub archetype_id: u16,
    /// Owning participant id (`0` = server-owned).
    pub owner: u64,
    /// Spawn transform.
    pub transform: NaTransform,
}

/// Bytes in a serialized [`NaSpawn`].
pub const NA_SPAWN_BYTES: usize = 4 + 2 + 8 + NA_TRANSFORM_BYTES;

impl NaSpawn {
    /// Encode the spawn body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(NA_SPAWN_BYTES);
        self.encode_into(&mut buf);
        buf
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.object_id.to_be_bytes());
        buf.extend_from_slice(&self.archetype_id.to_be_bytes());
        buf.extend_from_slice(&self.owner.to_be_bytes());
        self.transform.encode_into(buf);
    }

    /// Decode a spawn body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, NA_SPAWN_BYTES)?;
        let mut off = 0usize;
        Ok(Self::decode_at(body, &mut off))
    }

    fn decode_at(body: &[u8], off: &mut usize) -> Self {
        let object_id = read_u32(body, off);
        let archetype_id = read_u16(body, off);
        let owner = read_u64(body, off);
        let transform = NaTransform::decode_at(body, off);
        Self {
            object_id,
            archetype_id,
            owner,
            transform,
        }
    }
}

/// `KIND_NA_SPAWN_BATCH` (S→C, reliable): every actor already present, sent to a
/// newly-joined client so it sees the whole world at once.
#[derive(Debug, Clone, PartialEq)]
pub struct NaSpawnBatch {
    /// The present actors (may be empty when the newcomer is the first in).
    pub spawns: Vec<NaSpawn>,
}

impl NaSpawnBatch {
    /// Encode the batch body: `u16` count, then that many [`NaSpawn`]s.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let count = self.spawns.len().min(u16::MAX as usize);
        let mut buf = Vec::with_capacity(2 + count * NA_SPAWN_BYTES);
        buf.extend_from_slice(&(count as u16).to_be_bytes());
        for spawn in self.spawns.iter().take(count) {
            spawn.encode_into(&mut buf);
        }
        buf
    }

    /// Decode a batch body, rejecting a count that does not match the bytes.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, 2)?;
        let mut off = 0usize;
        let count = read_u16(body, &mut off) as usize;
        let expected = 2 + count * NA_SPAWN_BYTES;
        if body.len() < expected {
            return Err(TsyncError::TooShort {
                needed: expected,
                got: body.len(),
            });
        }
        let mut spawns = Vec::with_capacity(count);
        for _ in 0..count {
            spawns.push(NaSpawn::decode_at(body, &mut off));
        }
        Ok(Self { spawns })
    }
}

/// `KIND_NA_DESPAWN` (S→C, reliable): destroy the actor bound to `object_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NaDespawn {
    /// Transform-sync object id to remove.
    pub object_id: u32,
}

/// Bytes in a serialized [`NaDespawn`].
pub const NA_DESPAWN_BYTES: usize = 4;

impl NaDespawn {
    /// Encode the despawn body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.object_id.to_be_bytes().to_vec()
    }

    /// Decode a despawn body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, NA_DESPAWN_BYTES)?;
        let mut off = 0usize;
        Ok(Self {
            object_id: read_u32(body, &mut off),
        })
    }
}

/// `KIND_NA_STATE` (C→S, unreliable): the owner's authoritative transform in relay
/// mode. The server applies it (after checking ownership) and republishes it in
/// the normal transform-sync snapshots.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NaState {
    /// The owned object being updated.
    pub object_id: u32,
    /// The owner's current transform.
    pub transform: NaTransform,
}

/// Bytes in a serialized [`NaState`].
pub const NA_STATE_BYTES: usize = 4 + NA_TRANSFORM_BYTES;

impl NaState {
    /// Encode the state body.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(NA_STATE_BYTES);
        buf.extend_from_slice(&self.object_id.to_be_bytes());
        self.transform.encode_into(&mut buf);
        buf
    }

    /// Decode a state body.
    pub fn decode(body: &[u8]) -> Result<Self, TsyncError> {
        need(body, NA_STATE_BYTES)?;
        let mut off = 0usize;
        let object_id = read_u32(body, &mut off);
        let transform = NaTransform::decode_at(body, &mut off);
        Ok(Self {
            object_id,
            transform,
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

fn read_u32(body: &[u8], off: &mut usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&body[*off..*off + 4]);
    *off += 4;
    u32::from_be_bytes(b)
}

fn read_u64(body: &[u8], off: &mut usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&body[*off..*off + 8]);
    *off += 8;
    u64::from_be_bytes(b)
}

fn read_f32(body: &[u8], off: &mut usize) -> f32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&body[*off..*off + 4]);
    *off += 4;
    f32::from_be_bytes(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_transform() -> NaTransform {
        NaTransform {
            position: [1.5, -2.0, 300.25],
            rotation: [0.25, 0.5, -0.5, 0.75],
            velocity: [0.0, 120.0, -3.5],
        }
    }

    #[test]
    fn presence_round_trips() {
        let p = NaPresence {
            archetype_id: 3,
            transform: sample_transform(),
        };
        let bytes = p.encode();
        assert_eq!(bytes.len(), NA_PRESENCE_BYTES);
        assert_eq!(NaPresence::decode(&bytes).expect("decode"), p);
    }

    #[test]
    fn spawn_round_trips() {
        let s = NaSpawn {
            object_id: 7,
            archetype_id: 1,
            owner: 0xDEAD_BEEF,
            transform: sample_transform(),
        };
        let bytes = s.encode();
        assert_eq!(bytes.len(), NA_SPAWN_BYTES);
        assert_eq!(NaSpawn::decode(&bytes).expect("decode"), s);
    }

    #[test]
    fn spawn_batch_round_trips_including_empty() {
        for n in [0usize, 1, 3] {
            let spawns: Vec<NaSpawn> = (0..n)
                .map(|i| NaSpawn {
                    object_id: i as u32 + 1,
                    archetype_id: (i % 2) as u16,
                    owner: 100 + i as u64,
                    transform: sample_transform(),
                })
                .collect();
            let batch = NaSpawnBatch { spawns };
            let bytes = batch.encode();
            let decoded = NaSpawnBatch::decode(&bytes).expect("decode");
            assert_eq!(decoded, batch, "batch of {n} round-trips");
        }
    }

    #[test]
    fn despawn_round_trips() {
        let d = NaDespawn { object_id: 42 };
        assert_eq!(NaDespawn::decode(&d.encode()).expect("decode"), d);
    }

    #[test]
    fn state_round_trips() {
        let s = NaState {
            object_id: 9,
            transform: sample_transform(),
        };
        let bytes = s.encode();
        assert_eq!(bytes.len(), NA_STATE_BYTES);
        assert_eq!(NaState::decode(&bytes).expect("decode"), s);
    }

    #[test]
    fn decoders_reject_truncated_bodies() {
        assert!(matches!(
            NaPresence::decode(&[0u8; NA_PRESENCE_BYTES - 1]),
            Err(TsyncError::TooShort { .. })
        ));
        assert!(matches!(
            NaSpawn::decode(&[0u8; NA_SPAWN_BYTES - 1]),
            Err(TsyncError::TooShort { .. })
        ));
        assert!(matches!(
            NaState::decode(&[0u8; NA_STATE_BYTES - 1]),
            Err(TsyncError::TooShort { .. })
        ));
        assert!(matches!(
            NaDespawn::decode(&[0u8; 3]),
            Err(TsyncError::TooShort { .. })
        ));
    }

    #[test]
    fn spawn_batch_rejects_count_larger_than_body() {
        // Claim 5 spawns but provide bytes for none.
        let mut body = Vec::new();
        body.extend_from_slice(&5u16.to_be_bytes());
        assert!(matches!(
            NaSpawnBatch::decode(&body),
            Err(TsyncError::TooShort { .. })
        ));
    }
}
