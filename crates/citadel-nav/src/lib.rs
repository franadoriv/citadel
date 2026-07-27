//! Citadel's safe, deliberately small Detour seam.
//!
//! This is the only crate allowed to call `recastnavigation-sys`. It builds one
//! navigation tile from Citadel collision triangles and returns a sequence of
//! polygon centroids suitable for server-authoritative bot movement. The unsafe
//! calls are contained in `ffi`; all public inputs are bounds-checked first.

#![allow(unsafe_code)]

use citadel_map::{BakedNavMesh, CollisionMesh};

/// Detour's serialized navmesh ABI expected by this build.
pub const DETOUR_VERSION: u32 = ffi::DT_NAVMESH_VERSION as u32;
/// Citadel intentionally uses upstream Detour's 32-bit polygon references.
pub const POLY_REF_BITS: u8 = 32;

/// Navigation bake/query failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NavError {
    /// A collision mesh cannot be represented by Detour's u16 vertex indices.
    TooManyVertices,
    /// No collision triangles were supplied.
    EmptyGeometry,
    /// Native Detour allocation or initialization failed.
    NativeFailure,
    /// A persisted NAVMESH section targets another Detour ABI.
    IncompatibleAbi,
    /// The query point lies outside the baked navigation tile.
    PointOutsideNavMesh,
}

/// Bake one Detour tile from Citadel collision geometry.
pub fn bake(mesh: &CollisionMesh) -> Result<BakedNavMesh, NavError> {
    ffi::bake(mesh)
}

/// Find a path from `start` to `goal` over a freshly baked collision tile.
///
/// The returned points include a centroid for each crossed polygon and the final
/// goal, so driving an actor through the list follows the navigation corridor.
pub fn find_path(
    mesh: &CollisionMesh,
    start: [f32; 3],
    goal: [f32; 3],
) -> Result<Vec<[f32; 3]>, NavError> {
    ffi::find_path(mesh, start, goal)
}

/// Validate a persisted tile's ABI before a server attempts to use it.
pub fn validate_abi(navmesh: &BakedNavMesh) -> Result<(), NavError> {
    if navmesh.detour_version != DETOUR_VERSION || navmesh.poly_ref_bits != POLY_REF_BITS {
        return Err(NavError::IncompatibleAbi);
    }
    Ok(())
}

mod ffi {
    use std::collections::HashMap;
    use std::ptr;

    use citadel_map::{BakedNavMesh, CollisionMesh};
    use recastnavigation_sys::*;

    use super::{NavError, POLY_REF_BITS};

    pub(super) const DT_NAVMESH_VERSION: i32 = recastnavigation_sys::DT_NAVMESH_VERSION;

    pub(super) fn bake(mesh: &CollisionMesh) -> Result<BakedNavMesh, NavError> {
        let prepared = prepare(mesh)?;
        // SAFETY: `prepare` owns all arrays until dtCreateNavMeshData has copied
        // them. The params contain only valid pointers/counts and native output
        // is copied into Rust ownership before Detour frees it through init1.
        unsafe {
            let mut params = params(&prepared);
            let mut data = ptr::null_mut();
            let mut size = 0;
            if !dtCreateNavMeshData(&mut params, &mut data, &mut size)
                || data.is_null()
                || size <= 0
            {
                return Err(NavError::NativeFailure);
            }
            let bytes = std::slice::from_raw_parts(data, size as usize).to_vec();
            dtFree(data.cast());
            Ok(BakedNavMesh {
                detour_version: DT_NAVMESH_VERSION as u32,
                poly_ref_bits: POLY_REF_BITS,
                tile_data: bytes,
            })
        }
    }

    pub(super) fn find_path(
        mesh: &CollisionMesh,
        start: [f32; 3],
        goal: [f32; 3],
    ) -> Result<Vec<[f32; 3]>, NavError> {
        let prepared = prepare(mesh)?;
        // SAFETY: native objects are allocated/freed in this scope; all pointers
        // remain valid for each call, status is checked before outputs are read.
        unsafe {
            let mut params = params(&prepared);
            let mut data = ptr::null_mut();
            let mut size = 0;
            if !dtCreateNavMeshData(&mut params, &mut data, &mut size)
                || data.is_null()
                || size <= 0
            {
                return Err(NavError::NativeFailure);
            }
            let nav = dtAllocNavMesh();
            if nav.is_null() {
                dtFree(data.cast());
                return Err(NavError::NativeFailure);
            }
            // bindgen exposes `dtTileFlags` as an unsigned type on macOS but
            // Detour's `init1` parameter is a C `int` on every supported ABI.
            // FREE_DATA is the fixed positive one-bit value, so converting at
            // the FFI boundary is lossless and keeps the wrapper portable.
            let free_data_flag: std::os::raw::c_int = dtTileFlags_DT_TILE_FREE_DATA as _;
            if (*nav).init1(data, size, free_data_flag) != DT_SUCCESS {
                dtFreeNavMesh(nav);
                return Err(NavError::NativeFailure);
            }
            let query = dtAllocNavMeshQuery();
            if query.is_null() || (*query).init(nav, 2048) != DT_SUCCESS {
                if !query.is_null() {
                    dtFreeNavMeshQuery(query);
                }
                dtFreeNavMesh(nav);
                return Err(NavError::NativeFailure);
            }
            let filter = dtQueryFilter {
                m_areaCost: [1.0; 64],
                m_includeFlags: 0xffff,
                m_excludeFlags: 0,
            };
            let extents = [2.0, 500.0, 2.0];
            let start_ref = nearest(query, &start, &extents, &filter)?;
            let goal_ref = nearest(query, &goal, &extents, &filter)?;
            let mut refs = vec![0; mesh.triangles.len().max(1)];
            let mut count = 0;
            if (*query).findPath(
                start_ref,
                goal_ref,
                start.as_ptr(),
                goal.as_ptr(),
                &filter,
                refs.as_mut_ptr(),
                &mut count,
                refs.len() as i32,
            ) != DT_SUCCESS
            {
                dtFreeNavMeshQuery(query);
                dtFreeNavMesh(nav);
                return Err(NavError::NativeFailure);
            }
            let mut out = Vec::new();
            for poly_ref in refs.into_iter().take(count as usize) {
                let mut tile = ptr::null();
                let mut poly = ptr::null();
                if (*nav).getTileAndPolyByRef(poly_ref, &mut tile, &mut poly) != DT_SUCCESS {
                    continue;
                }
                let p = &*poly;
                let t = &*tile;
                let mut c = [0.0; 3];
                for i in 0..p.vertCount as usize {
                    let v = *p.verts.as_ptr().add(i) as usize;
                    c[0] += *t.verts.add(v * 3);
                    c[1] += *t.verts.add(v * 3 + 1);
                    c[2] += *t.verts.add(v * 3 + 2);
                }
                let n = p.vertCount as f32;
                out.push([c[0] / n, c[1] / n, c[2] / n]);
            }
            out.push(goal);
            dtFreeNavMeshQuery(query);
            dtFreeNavMesh(nav);
            Ok(out)
        }
    }

    unsafe fn nearest(
        query: *mut dtNavMeshQuery,
        point: &[f32; 3],
        extents: &[f32; 3],
        filter: &dtQueryFilter,
    ) -> Result<dtPolyRef, NavError> {
        let mut poly_ref = 0;
        if unsafe {
            (*query).findNearestPoly(
                point.as_ptr(),
                extents.as_ptr(),
                filter,
                &mut poly_ref,
                ptr::null_mut(),
            )
        } != DT_SUCCESS
            || poly_ref == 0
        {
            Err(NavError::PointOutsideNavMesh)
        } else {
            Ok(poly_ref)
        }
    }

    struct PreparedMesh {
        verts: Vec<u16>,
        polys: Vec<u16>,
        flags: Vec<u16>,
        areas: Vec<u8>,
        bmin: [f32; 3],
        bmax: [f32; 3],
    }

    fn prepare(mesh: &CollisionMesh) -> Result<PreparedMesh, NavError> {
        if mesh.triangles.is_empty() || mesh.vertices.is_empty() {
            return Err(NavError::EmptyGeometry);
        }
        if mesh.vertices.len() > u16::MAX as usize {
            return Err(NavError::TooManyVertices);
        }
        let mut verts = Vec::with_capacity(mesh.vertices.len() * 3);
        let mut bmin = [f32::INFINITY; 3];
        let mut bmax = [f32::NEG_INFINITY; 3];
        for v in &mesh.vertices {
            for axis in 0..3 {
                bmin[axis] = bmin[axis].min(v[axis]);
                bmax[axis] = bmax[axis].max(v[axis]);
            }
        }
        for v in &mesh.vertices {
            for axis in 0..3 {
                let q = (v[axis] - bmin[axis]).round();
                if !(0.0..=u16::MAX as f32).contains(&q) {
                    return Err(NavError::TooManyVertices);
                }
                verts.push(q as u16);
            }
        }
        let mut neighbors: HashMap<(u16, u16), (usize, usize)> = HashMap::new();
        let mut polys = vec![u16::MAX; mesh.triangles.len() * 6];
        for (i, tri) in mesh.triangles.iter().enumerate() {
            let vs = [tri[0] as u16, tri[1] as u16, tri[2] as u16];
            polys[i * 6..i * 6 + 3].copy_from_slice(&vs);
            for edge in 0..3 {
                let a = vs[edge];
                let b = vs[(edge + 1) % 3];
                if let Some((other, other_edge)) = neighbors.remove(&(b, a)) {
                    polys[i * 6 + 3 + edge] = other as u16;
                    polys[other * 6 + 3 + other_edge] = i as u16;
                } else {
                    neighbors.insert((a, b), (i, edge));
                }
            }
        }
        Ok(PreparedMesh {
            verts,
            polys,
            flags: vec![1; mesh.triangles.len()],
            areas: vec![0; mesh.triangles.len()],
            bmin,
            bmax,
        })
    }

    fn params(mesh: &PreparedMesh) -> dtNavMeshCreateParams {
        dtNavMeshCreateParams {
            verts: mesh.verts.as_ptr(),
            vertCount: (mesh.verts.len() / 3) as i32,
            polys: mesh.polys.as_ptr(),
            polyFlags: mesh.flags.as_ptr(),
            polyAreas: mesh.areas.as_ptr(),
            polyCount: mesh.flags.len() as i32,
            nvp: 3,
            detailMeshes: ptr::null(),
            detailVerts: ptr::null(),
            detailVertsCount: 0,
            detailTris: ptr::null(),
            detailTriCount: 0,
            offMeshConVerts: ptr::null(),
            offMeshConRad: ptr::null(),
            offMeshConFlags: ptr::null(),
            offMeshConAreas: ptr::null(),
            offMeshConDir: ptr::null(),
            offMeshConUserID: ptr::null(),
            offMeshConCount: 0,
            userId: 0,
            tileX: 0,
            tileY: 0,
            tileLayer: 0,
            bmin: mesh.bmin,
            bmax: mesh.bmax,
            walkableHeight: 1.0,
            walkableRadius: 0.0,
            walkableClimb: 1.0,
            cs: 1.0,
            ch: 1.0,
            buildBvTree: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn floor() -> CollisionMesh {
        CollisionMesh {
            vertices: vec![
                [0.0, 0.0, 0.0],
                [10.0, 0.0, 0.0],
                [10.0, 0.0, 10.0],
                [0.0, 0.0, 10.0],
            ],
            triangles: vec![[0, 1, 2], [0, 2, 3]],
        }
    }

    #[test]
    fn bakes_and_queries_a_known_mesh() {
        let baked = bake(&floor()).expect("simple walkable floor must bake");
        validate_abi(&baked).expect("fresh bake has this build's ABI");
        let path = find_path(&floor(), [1.0, 0.0, 1.0], [9.0, 0.0, 9.0])
            .expect("points on the floor must connect");
        assert!(path.len() >= 2);
        assert_eq!(path.last(), Some(&[9.0, 0.0, 9.0]));
    }

    #[test]
    fn rejects_an_incompatible_persisted_abi() {
        let nav = BakedNavMesh {
            detour_version: DETOUR_VERSION + 1,
            poly_ref_bits: POLY_REF_BITS,
            tile_data: vec![],
        };
        assert_eq!(validate_abi(&nav), Err(NavError::IncompatibleAbi));
    }

    #[test]
    fn path_routes_around_a_collision_hole() {
        // Three connected walkable strips form a U around the missing centre.
        // A direct route from the upper left to upper right would cross the hole;
        // the Detour corridor must instead visit the lower bridge (z <= 3).
        let mesh = CollisionMesh {
            vertices: vec![
                [0., 0., 0.],
                [3., 0., 0.],
                [3., 0., 3.],
                [0., 0., 3.],
                [0., 0., 10.],
                [3., 0., 10.],
                [7., 0., 0.],
                [7., 0., 3.],
                [10., 0., 0.],
                [10., 0., 3.],
                [7., 0., 10.],
                [10., 0., 10.],
            ],
            triangles: vec![
                [0, 1, 2],
                [0, 2, 3],
                [3, 2, 5],
                [3, 5, 4],
                [1, 6, 7],
                [1, 7, 2],
                [6, 8, 9],
                [6, 9, 7],
                [7, 9, 11],
                [7, 11, 10],
            ],
        };
        let path = find_path(&mesh, [1., 0., 8.], [9., 0., 8.]).expect("route around the hole");
        assert!(
            path.iter().any(|point| point[2] <= 3.0),
            "path must use the lower bridge: {path:?}"
        );
    }
}
