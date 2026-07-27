//! Citadel `.map` (CMAP) level-geometry file format.
//!
//! A `.map` file carries a level's static geometry so the server can later bake a
//! navmesh and perform authoritative collision. It is a small, versioned,
//! **section-based** big-endian binary format with a deliberately forward-
//! compatible layout: a reader skips sections it does not understand, so a future
//! phase can add baked navigation data without breaking older readers.
//!
//! # Byte layout
//!
//! All integers are big-endian; floats are IEEE-754 (`f32::to_be_bytes`); strings
//! are `u16`-length-prefixed UTF-8 (matching `citadel-wire`'s `room` codec).
//!
//! ```text
//! Header:
//!   magic          : 4 bytes  = b"CMAP"
//!   format_version : u32      = 1 (CURRENT)
//!
//! Then zero or more length-prefixed sections until EOF:
//!   section_id     : u32      (tag; see SectionId)
//!   section_len    : u32      (payload byte count)
//!   payload        : section_len bytes
//! ```
//!
//! The `section_len` frame is what makes the format extensible: a reader that
//! encounters an unknown `section_id` advances by `section_len` and continues, so
//! new sections are transparently ignored by older code.
//!
//! ## `METADATA` section (id 1)
//!
//! ```text
//!   name       : u16-len UTF-8   (level name)
//!   bounds_min : f32 * 3         (world-space AABB minimum, x/y/z)
//!   bounds_max : f32 * 3         (world-space AABB maximum, x/y/z)
//! ```
//!
//! ## `COLLISION` section (id 2)
//!
//! An indexed triangle mesh in **world** space — the input a navmesh baker
//! (Phase C) will consume.
//!
//! ```text
//!   vertex_count   : u32
//!   vertices       : (f32 * 3) * vertex_count   (x/y/z per vertex)
//!   triangle_count : u32
//!   triangles      : (u32 * 3) * triangle_count (3 vertex indices per triangle)
//! ```
//!
//! On decode every triangle index is validated against `vertex_count`; an
//! out-of-range index is rejected with [`MapError::TriangleIndexOutOfRange`].
//!
//! ## `NAVMESH` section (id 3)
//!
//! An optional server-baked Detour tile: `detour_version:u32`,
//! `poly_ref_bits:u8`, `tile_data_len:u32`, and opaque tile bytes.

#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt;

/// Magic bytes at the start of every CMAP file.
pub const MAGIC: &[u8; 4] = b"CMAP";

/// Current CMAP format version. Bump only on a breaking header/section change;
/// additive sections do not require a version bump thanks to section skipping.
pub const CURRENT_VERSION: u32 = 1;

/// Stable section tags. Encoded as a `u32` at the head of each section frame.
///
/// Tags are an open set: a reader must skip any tag it does not recognize using
/// the section's length prefix rather than treating it as an error.
pub mod section_id {
    /// Level metadata (name + world-space AABB).
    pub const METADATA: u32 = 1;
    /// World-space indexed collision triangle mesh.
    pub const COLLISION: u32 = 2;
    /// Server-baked navigation tile data.
    pub const NAVMESH: u32 = 3;
}

/// An error reading or writing a CMAP file.
///
/// Mirrors the shape of `citadel-wire`'s `TsyncError`: every fallible decode path
/// returns a typed, contextual error and never panics on truncated or corrupt
/// input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MapError {
    /// The leading 4 bytes were not `b"CMAP"`.
    BadMagic,
    /// The header declared a `format_version` this build cannot read.
    UnsupportedVersion(u32),
    /// A read ran past the end of the buffer (truncated or corrupt input).
    Truncated {
        /// Bytes the layout required from the current offset.
        needed: usize,
        /// Bytes actually remaining.
        got: usize,
    },
    /// A length-prefixed string was not valid UTF-8.
    InvalidUtf8,
    /// A triangle referenced a vertex index `>= vertex_count`.
    TriangleIndexOutOfRange {
        /// The offending vertex index.
        index: u32,
        /// Number of vertices present in the mesh.
        vertex_count: u32,
    },
    /// The optional NAVMESH payload was structurally invalid.
    InvalidNavMesh,
    /// A required section was absent from the file.
    MissingSection(&'static str),
}

impl fmt::Display for MapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MapError::BadMagic => write!(f, "bad CMAP magic (expected b\"CMAP\")"),
            MapError::UnsupportedVersion(v) => write!(f, "unsupported CMAP version: {v}"),
            MapError::Truncated { needed, got } => {
                write!(f, "truncated CMAP input: needed {needed}, got {got}")
            }
            MapError::InvalidUtf8 => write!(f, "CMAP string is not valid UTF-8"),
            MapError::TriangleIndexOutOfRange {
                index,
                vertex_count,
            } => write!(
                f,
                "triangle vertex index {index} out of range (vertex_count = {vertex_count})"
            ),
            MapError::InvalidNavMesh => write!(f, "CMAP NAVMESH section is invalid"),
            MapError::MissingSection(name) => {
                write!(f, "CMAP file is missing required {name} section")
            }
        }
    }
}

impl Error for MapError {}

/// Level metadata: the human-readable name and the world-space bounding box.
#[derive(Debug, Clone, PartialEq)]
pub struct MapMetadata {
    /// The level name.
    pub name: String,
    /// World-space AABB minimum corner (`x`, `y`, `z`).
    pub bounds_min: [f32; 3],
    /// World-space AABB maximum corner (`x`, `y`, `z`).
    pub bounds_max: [f32; 3],
}

impl MapMetadata {
    /// Serialize the metadata payload (no section framing).
    fn encode_payload(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.name.len() + 6 * 4);
        write_str(&mut buf, &self.name);
        write_vec3(&mut buf, self.bounds_min);
        write_vec3(&mut buf, self.bounds_max);
        buf
    }

    /// Parse a metadata payload from an exact section slice.
    fn decode_payload(payload: &[u8]) -> Result<Self, MapError> {
        let mut off = 0usize;
        let name = read_str(payload, &mut off)?;
        let bounds_min = read_vec3(payload, &mut off)?;
        let bounds_max = read_vec3(payload, &mut off)?;
        Ok(Self {
            name,
            bounds_min,
            bounds_max,
        })
    }
}

/// An indexed triangle mesh in world space.
///
/// Each entry of `triangles` holds three indices into `vertices`. Decoding
/// validates that every index is `< vertices.len`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CollisionMesh {
    /// World-space vertex positions (`x`, `y`, `z`).
    pub vertices: Vec<[f32; 3]>,
    /// Triangles as triples of indices into `vertices`.
    pub triangles: Vec<[u32; 3]>,
}

impl CollisionMesh {
    /// Serialize the collision payload (no section framing).
    fn encode_payload(&self) -> Vec<u8> {
        let vcount = self.vertices.len().min(u32::MAX as usize);
        let tcount = self.triangles.len().min(u32::MAX as usize);
        let mut buf = Vec::with_capacity(4 + vcount * 12 + 4 + tcount * 12);
        buf.extend_from_slice(&(vcount as u32).to_be_bytes());
        for v in &self.vertices[..vcount] {
            write_vec3(&mut buf, *v);
        }
        buf.extend_from_slice(&(tcount as u32).to_be_bytes());
        for t in &self.triangles[..tcount] {
            buf.extend_from_slice(&t[0].to_be_bytes());
            buf.extend_from_slice(&t[1].to_be_bytes());
            buf.extend_from_slice(&t[2].to_be_bytes());
        }
        buf
    }

    /// Parse a collision payload from an exact section slice, validating that
    /// every triangle index is in range.
    fn decode_payload(payload: &[u8]) -> Result<Self, MapError> {
        let mut off = 0usize;
        let vertex_count = read_u32(payload, &mut off)?;
        let mut vertices = Vec::with_capacity(vertex_count as usize);
        for _ in 0..vertex_count {
            vertices.push(read_vec3(payload, &mut off)?);
        }
        let triangle_count = read_u32(payload, &mut off)?;
        let mut triangles = Vec::with_capacity(triangle_count as usize);
        for _ in 0..triangle_count {
            let a = read_u32(payload, &mut off)?;
            let b = read_u32(payload, &mut off)?;
            let c = read_u32(payload, &mut off)?;
            for &index in &[a, b, c] {
                if index >= vertex_count {
                    return Err(MapError::TriangleIndexOutOfRange {
                        index,
                        vertex_count,
                    });
                }
            }
            triangles.push([a, b, c]);
        }
        Ok(Self {
            vertices,
            triangles,
        })
    }
}

/// Opaque, server-owned Detour navigation tile with ABI guards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BakedNavMesh {
    /// Detour's `DT_NAVMESH_VERSION` used by the baker.
    pub detour_version: u32,
    /// Width of `dtPolyRef` in the baker (normally 32).
    pub poly_ref_bits: u8,
    /// `dtCreateNavMeshData` output consumed by `dtNavMesh::init`.
    pub tile_data: Vec<u8>,
}

impl BakedNavMesh {
    fn encode_payload(&self) -> Vec<u8> {
        let len = self.tile_data.len().min(u32::MAX as usize);
        let mut buf = Vec::with_capacity(9 + len);
        buf.extend_from_slice(&self.detour_version.to_be_bytes());
        buf.push(self.poly_ref_bits);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
        buf.extend_from_slice(&self.tile_data[..len]);
        buf
    }

    fn decode_payload(payload: &[u8]) -> Result<Self, MapError> {
        let mut off = 0;
        let detour_version = read_u32(payload, &mut off)?;
        need(payload, off, 1)?;
        let poly_ref_bits = payload[off];
        off += 1;
        let len = read_u32(payload, &mut off)? as usize;
        need(payload, off, len)?;
        if off + len != payload.len() {
            return Err(MapError::InvalidNavMesh);
        }
        Ok(Self {
            detour_version,
            poly_ref_bits,
            tile_data: payload[off..].to_vec(),
        })
    }
}

/// A parsed CMAP file: level metadata plus a world-space collision mesh.
#[derive(Debug, Clone, PartialEq)]
pub struct MapFile {
    /// Level metadata.
    pub metadata: MapMetadata,
    /// World-space collision mesh (navmesh-baker input).
    pub collision: CollisionMesh,
    /// Optional server-baked navigation tile.
    pub navmesh: Option<BakedNavMesh>,
}

impl MapFile {
    /// Encode this map into a self-describing CMAP byte buffer.
    ///
    /// Emits metadata, collision, and an optional baked NAVMESH section.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.extend_from_slice(&CURRENT_VERSION.to_be_bytes());
        write_section(
            &mut buf,
            section_id::METADATA,
            &self.metadata.encode_payload(),
        );
        write_section(
            &mut buf,
            section_id::COLLISION,
            &self.collision.encode_payload(),
        );
        if let Some(navmesh) = &self.navmesh {
            write_section(&mut buf, section_id::NAVMESH, &navmesh.encode_payload());
        }
        buf
    }

    /// Decode a CMAP byte buffer.
    ///
    /// Validates the magic and version, then walks the section frames. Unknown
    /// section ids are skipped (forward compatibility). Returns a [`MapError`] on
    /// any truncation, bad magic, unsupported version, invalid UTF-8, or
    /// out-of-range triangle index — never panics on hostile input.
    pub fn decode(bytes: &[u8]) -> Result<Self, MapError> {
        let mut off = 0usize;
        // Header: magic + version.
        need(bytes, off, 4)?;
        if &bytes[..4] != MAGIC.as_slice() {
            return Err(MapError::BadMagic);
        }
        off += 4;
        let version = read_u32(bytes, &mut off)?;
        if version != CURRENT_VERSION {
            return Err(MapError::UnsupportedVersion(version));
        }

        let mut metadata: Option<MapMetadata> = None;
        let mut collision: Option<CollisionMesh> = None;
        let mut navmesh: Option<BakedNavMesh> = None;

        // Section loop: id + length-prefixed payload until EOF.
        while off < bytes.len() {
            let id = read_u32(bytes, &mut off)?;
            let len = read_u32(bytes, &mut off)? as usize;
            need(bytes, off, len)?;
            let payload = &bytes[off..off + len];
            off += len;
            match id {
                section_id::METADATA => metadata = Some(MapMetadata::decode_payload(payload)?),
                section_id::COLLISION => collision = Some(CollisionMesh::decode_payload(payload)?),
                section_id::NAVMESH => navmesh = Some(BakedNavMesh::decode_payload(payload)?),
                // Unknown / reserved (e.g. NAVMESH) sections are skipped so older
                // readers stay compatible with files written by newer writers.
                _ => {}
            }
        }

        Ok(Self {
            metadata: metadata.ok_or(MapError::MissingSection("METADATA"))?,
            collision: collision.ok_or(MapError::MissingSection("COLLISION"))?,
            navmesh,
        })
    }
}

// ------------------------------- byte helpers ------------------------------- //

/// Write one section frame: `id u32`, `len u32`, then the payload bytes.
fn write_section(buf: &mut Vec<u8>, id: u32, payload: &[u8]) {
    let len = payload.len().min(u32::MAX as usize);
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(&(len as u32).to_be_bytes());
    buf.extend_from_slice(&payload[..len]);
}

fn write_vec3(buf: &mut Vec<u8>, v: [f32; 3]) {
    buf.extend_from_slice(&v[0].to_be_bytes());
    buf.extend_from_slice(&v[1].to_be_bytes());
    buf.extend_from_slice(&v[2].to_be_bytes());
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len().min(u16::MAX as usize);
    buf.extend_from_slice(&(len as u16).to_be_bytes());
    buf.extend_from_slice(&bytes[..len]);
}

/// Ensure `bytes[off..off + needed]` is in bounds.
fn need(bytes: &[u8], off: usize, needed: usize) -> Result<(), MapError> {
    let remaining = bytes.len().saturating_sub(off);
    if remaining < needed {
        Err(MapError::Truncated {
            needed,
            got: remaining,
        })
    } else {
        Ok(())
    }
}

fn read_u16(bytes: &[u8], off: &mut usize) -> Result<u16, MapError> {
    need(bytes, *off, 2)?;
    let v = u16::from_be_bytes([bytes[*off], bytes[*off + 1]]);
    *off += 2;
    Ok(v)
}

fn read_u32(bytes: &[u8], off: &mut usize) -> Result<u32, MapError> {
    need(bytes, *off, 4)?;
    let mut b = [0u8; 4];
    b.copy_from_slice(&bytes[*off..*off + 4]);
    *off += 4;
    Ok(u32::from_be_bytes(b))
}

fn read_f32(bytes: &[u8], off: &mut usize) -> Result<f32, MapError> {
    need(bytes, *off, 4)?;
    let mut b = [0u8; 4];
    b.copy_from_slice(&bytes[*off..*off + 4]);
    *off += 4;
    Ok(f32::from_be_bytes(b))
}

fn read_vec3(bytes: &[u8], off: &mut usize) -> Result<[f32; 3], MapError> {
    Ok([
        read_f32(bytes, off)?,
        read_f32(bytes, off)?,
        read_f32(bytes, off)?,
    ])
}

fn read_str(bytes: &[u8], off: &mut usize) -> Result<String, MapError> {
    let len = read_u16(bytes, off)? as usize;
    need(bytes, *off, len)?;
    let s = std::str::from_utf8(&bytes[*off..*off + len])
        .map_err(|_| MapError::InvalidUtf8)?
        .to_owned();
    *off += len;
    Ok(s)
}

#[cfg(test)]
mod tests {
    // `unwrap`/`expect` are intentionally allowed in tests; the
    // crate-level `unwrap_used = "warn"` lint only guards production paths.
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn quad_map() -> MapFile {
        // A unit quad on the XZ plane: 4 verts, 2 triangles.
        MapFile {
            metadata: MapMetadata {
                name: "TestArena".to_owned(),
                bounds_min: [-1.0, 0.0, -1.0],
                bounds_max: [1.0, 2.5, 1.0],
            },
            collision: CollisionMesh {
                vertices: vec![
                    [0.0, 0.0, 0.0],
                    [1.0, 0.0, 0.0],
                    [1.0, 0.0, 1.0],
                    [0.0, 0.0, 1.0],
                ],
                triangles: vec![[0, 1, 2], [0, 2, 3]],
            },
            navmesh: None,
        }
    }

    fn empty_map() -> MapFile {
        MapFile {
            metadata: MapMetadata {
                name: String::new(),
                bounds_min: [0.0, 0.0, 0.0],
                bounds_max: [0.0, 0.0, 0.0],
            },
            collision: CollisionMesh::default(),
            navmesh: None,
        }
    }

    #[test]
    fn round_trips_non_trivial_mesh() {
        let m = quad_map();
        let decoded = MapFile::decode(&m.encode()).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn round_trips_empty_mesh() {
        let m = empty_map();
        let decoded = MapFile::decode(&m.encode()).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn header_is_magic_then_version() {
        let bytes = empty_map().encode();
        assert_eq!(&bytes[..4], MAGIC.as_slice());
        assert_eq!(
            u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            CURRENT_VERSION
        );
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = quad_map().encode();
        bytes[0] = b'X';
        assert_eq!(MapFile::decode(&bytes), Err(MapError::BadMagic));
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut bytes = quad_map().encode();
        // Overwrite the version dword with 999.
        bytes[4..8].copy_from_slice(&999u32.to_be_bytes());
        assert_eq!(
            MapFile::decode(&bytes),
            Err(MapError::UnsupportedVersion(999))
        );
    }

    #[test]
    fn rejects_truncated_input() {
        let bytes = quad_map().encode();
        // Every proper-prefix truncation must be rejected without panicking.
        for cut in 0..bytes.len() {
            let res = MapFile::decode(&bytes[..cut]);
            assert!(res.is_err(), "truncation at {cut} should fail");
        }
    }

    #[test]
    fn rejects_out_of_range_triangle_index() {
        let bad = MapFile {
            metadata: empty_map().metadata,
            collision: CollisionMesh {
                vertices: vec![[0.0, 0.0, 0.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
                // Index 3 does not exist (only 0..=2 are valid).
                triangles: vec![[0, 1, 3]],
            },
            navmesh: None,
        };
        assert_eq!(
            MapFile::decode(&bad.encode()),
            Err(MapError::TriangleIndexOutOfRange {
                index: 3,
                vertex_count: 3,
            })
        );
    }

    #[test]
    fn rejects_invalid_utf8_name() {
        // Hand-build a header + a METADATA section whose name bytes are invalid.
        let mut payload = Vec::new();
        payload.extend_from_slice(&2u16.to_be_bytes()); // name len = 2
        payload.extend_from_slice(&[0xff, 0xfe]); // invalid UTF-8
        payload.extend_from_slice(&[0u8; 24]); // bounds_min + bounds_max (6 f32)

        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&CURRENT_VERSION.to_be_bytes());
        write_section(&mut bytes, section_id::METADATA, &payload);
        write_section(
            &mut bytes,
            section_id::COLLISION,
            &empty_map().collision.encode_payload(),
        );

        assert_eq!(MapFile::decode(&bytes), Err(MapError::InvalidUtf8));
    }

    #[test]
    fn missing_required_section_is_rejected() {
        // A well-formed header with no sections at all.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&CURRENT_VERSION.to_be_bytes());
        assert_eq!(
            MapFile::decode(&bytes),
            Err(MapError::MissingSection("METADATA"))
        );
    }

    #[test]
    fn skips_unknown_trailing_section() {
        // Forward compatibility: a newer writer appends an unknown section
        // after the ones we understand. Today's reader must ignore it and still
        // decode successfully. NAVMESH is intentionally not used as a stand-in:
        // it is now a known section and invalid NAVMESH data must be rejected.
        let base = quad_map();
        let mut bytes = base.encode();
        write_section(&mut bytes, 0xDEAD_BEEF, &[1, 2, 3, 4, 5]);

        let decoded = MapFile::decode(&bytes).unwrap();
        assert_eq!(decoded, base);
    }

    #[test]
    fn unknown_leading_section_does_not_shadow_known_ones() {
        // Section order independence: an unknown section before the known ones is
        // skipped and the known sections still parse.
        let base = quad_map();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&CURRENT_VERSION.to_be_bytes());
        write_section(&mut bytes, 0x1234_5678, b"unknown-first");
        write_section(
            &mut bytes,
            section_id::METADATA,
            &base.metadata.encode_payload(),
        );
        write_section(
            &mut bytes,
            section_id::COLLISION,
            &base.collision.encode_payload(),
        );

        assert_eq!(MapFile::decode(&bytes).unwrap(), base);
    }

    #[test]
    fn section_length_lie_is_rejected() {
        // A section that claims more payload than the buffer holds must fail, not
        // panic (hostile-input bounds check on the section frame itself).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&CURRENT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&section_id::METADATA.to_be_bytes());
        bytes.extend_from_slice(&9999u32.to_be_bytes()); // claims 9999 payload bytes
        bytes.extend_from_slice(b"short"); // but only 5 follow
        assert!(matches!(
            MapFile::decode(&bytes),
            Err(MapError::Truncated { .. })
        ));
    }

    #[test]
    fn decodes_bytes_built_like_the_unreal_cook_tool() {
        // Golden cross-check for the Unreal cook tool's CMAP writer
        // (clients/unreal/.../CitadelCmapWriter.h). That writer is a hand port of
        // the WRITE half of this crate; if either side's byte layout drifts, a map
        // cooked in-editor stops decoding here. We rebuild the exact bytes it emits
        // — big-endian, section-framed, NO use of `MapFile::encode` — and assert the
        // reader recovers the intended mesh. Keep this in lockstep with the header.
        let mut bytes = Vec::new();
        // Header: magic + version u32 BE.
        bytes.extend_from_slice(b"CMAP");
        bytes.extend_from_slice(&1u32.to_be_bytes());

        // METADATA section (id 1): u16-len utf8 name + bounds_min f32*3 + bounds_max f32*3.
        let mut meta = Vec::new();
        let name = b"Lvl_ThirdPerson";
        meta.extend_from_slice(&(name.len() as u16).to_be_bytes());
        meta.extend_from_slice(name);
        for f in [-100.0f32, 0.0, -100.0, 100.0, 250.0, 100.0] {
            meta.extend_from_slice(&f.to_be_bytes());
        }
        bytes.extend_from_slice(&1u32.to_be_bytes()); // section id
        bytes.extend_from_slice(&(meta.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&meta);

        // COLLISION section (id 2): vcount u32 + verts + tcount u32 + tris (u32*3).
        let mut col = Vec::new();
        let verts = [
            [0.0f32, 0.0, 0.0],
            [100.0, 0.0, 0.0],
            [100.0, 0.0, 100.0],
            [0.0, 0.0, 100.0],
        ];
        col.extend_from_slice(&(verts.len() as u32).to_be_bytes());
        for v in verts {
            for c in v {
                col.extend_from_slice(&c.to_be_bytes());
            }
        }
        let tris = [[0u32, 1, 2], [0, 2, 3]];
        col.extend_from_slice(&(tris.len() as u32).to_be_bytes());
        for t in tris {
            for i in t {
                col.extend_from_slice(&i.to_be_bytes());
            }
        }
        bytes.extend_from_slice(&2u32.to_be_bytes()); // section id
        bytes.extend_from_slice(&(col.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&col);

        let decoded = MapFile::decode(&bytes).unwrap();
        assert_eq!(decoded.metadata.name, "Lvl_ThirdPerson");
        assert_eq!(decoded.metadata.bounds_min, [-100.0, 0.0, -100.0]);
        assert_eq!(decoded.metadata.bounds_max, [100.0, 250.0, 100.0]);
        assert_eq!(decoded.collision.vertices.len(), 4);
        assert_eq!(decoded.collision.triangles, vec![[0, 1, 2], [0, 2, 3]]);
    }
}
