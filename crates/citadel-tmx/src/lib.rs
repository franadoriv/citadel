//! Tiled TMX collision import for Citadel's authoritative CMAP geometry.
//!
//! The supported subset is deliberately narrow: finite orthogonal maps, tile
//! collision selected by a `citadel_collision=true` tile property, and rectangle
//! objects in an object layer named `collision` (or with that layer property).

#![forbid(unsafe_code)]

use std::fmt;
use std::path::Path;

use citadel_map::{CollisionMesh, MapFile, MapMetadata};
use tiled::{LayerType, ObjectShape, Orientation, PropertyValue, TileLayer};

#[derive(Debug)]
pub enum TmxError {
    Parse(tiled::Error),
    Unsupported(&'static str),
    Invalid(&'static str),
}

impl fmt::Display for TmxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(error) => write!(f, "TMX parse error: {error}"),
            Self::Unsupported(feature) => write!(f, "unsupported TMX feature: {feature}"),
            Self::Invalid(message) => write!(f, "invalid TMX collision map: {message}"),
        }
    }
}

impl std::error::Error for TmxError {}

impl From<tiled::Error> for TmxError {
    fn from(error: tiled::Error) -> Self {
        Self::Parse(error)
    }
}

fn enabled(properties: &tiled::Properties) -> bool {
    matches!(
        properties.get("citadel_collision"),
        Some(PropertyValue::BoolValue(true))
    )
}

fn add_prism(mesh: &mut CollisionMesh, x: f32, z: f32, width: f32, depth: f32, height: f32) {
    let base = mesh.vertices.len() as u32;
    mesh.vertices.extend([
        [x, 0.0, z],
        [x + width, 0.0, z],
        [x + width, 0.0, z + depth],
        [x, 0.0, z + depth],
        [x, height, z],
        [x + width, height, z],
        [x + width, height, z + depth],
        [x, height, z + depth],
    ]);
    for triangle in [
        [0, 2, 1],
        [0, 3, 2],
        [4, 5, 6],
        [4, 6, 7],
        [0, 1, 5],
        [0, 5, 4],
        [1, 2, 6],
        [1, 6, 5],
        [2, 3, 7],
        [2, 7, 6],
        [3, 0, 4],
        [3, 4, 7],
    ] {
        mesh.triangles
            .push([base + triangle[0], base + triangle[1], base + triangle[2]]);
    }
}

fn add_polygon_prism(
    mesh: &mut CollisionMesh,
    points: &[(f32, f32)],
    height: f32,
) -> Result<(), TmxError> {
    if points.len() < 3 {
        return Err(TmxError::Invalid(
            "collision polygons need at least three points",
        ));
    }
    let mut winding = 0.0f32;
    for index in 0..points.len() {
        let a = points[index];
        let b = points[(index + 1) % points.len()];
        let c = points[(index + 2) % points.len()];
        let cross = (b.0 - a.0) * (c.1 - b.1) - (b.1 - a.1) * (c.0 - b.0);
        if cross != 0.0 {
            if winding != 0.0 && cross.signum() != winding.signum() {
                return Err(TmxError::Unsupported("non-convex collision polygons"));
            }
            winding = cross;
        }
    }
    if winding == 0.0 {
        return Err(TmxError::Invalid("degenerate collision polygon"));
    }
    let base = mesh.vertices.len() as u32;
    for &(x, z) in points {
        mesh.vertices.push([x, 0.0, z]);
    }
    for &(x, z) in points {
        mesh.vertices.push([x, height, z]);
    }
    let n = points.len() as u32;
    for i in 1..n - 1 {
        mesh.triangles.push([base, base + i + 1, base + i]);
        mesh.triangles
            .push([base + n, base + n + i, base + n + i + 1]);
    }
    for i in 0..n {
        let next = (i + 1) % n;
        mesh.triangles.extend([
            [base + i, base + next, base + n + next],
            [base + i, base + n + next, base + n + i],
        ]);
    }
    Ok(())
}

fn add_object(
    mesh: &mut CollisionMesh,
    object: &tiled::ObjectData,
    x: f32,
    z: f32,
    scale: f32,
    height: f32,
) -> Result<(), TmxError> {
    if !object.visible {
        return Ok(());
    }
    if object.rotation != 0.0 {
        return Err(TmxError::Unsupported("rotated collision objects"));
    }
    match &object.shape {
        ObjectShape::Rect {
            width,
            height: depth,
        } if *width > 0.0 && *depth > 0.0 => {
            add_prism(mesh, x, z, *width * scale, *depth * scale, height);
            Ok(())
        }
        ObjectShape::Rect { .. } => Err(TmxError::Invalid(
            "collision rectangles must have positive dimensions",
        )),
        ObjectShape::Polygon { points } => {
            let points = points
                .iter()
                .map(|(px, pz)| (x + px * scale, z + pz * scale))
                .collect::<Vec<_>>();
            add_polygon_prism(mesh, &points, height)
        }
        _ => Err(TmxError::Unsupported("unsupported collision object shape")),
    }
}

/// Load a supported TMX map, resolving external TSX files relative to `path`.
pub fn load(path: &Path) -> Result<MapFile, TmxError> {
    let map = tiled::Loader::new().load_tmx_map(path)?;
    if map.orientation != Orientation::Orthogonal {
        return Err(TmxError::Unsupported(
            "map orientation (only orthogonal is accepted)",
        ));
    }
    if map.infinite() {
        return Err(TmxError::Unsupported("infinite maps"));
    }
    if map.tile_width == 0 || map.tile_height == 0 {
        return Err(TmxError::Invalid("tile dimensions must be non-zero"));
    }
    let scale = match map.properties.get("citadel_units_per_pixel") {
        Some(PropertyValue::FloatValue(value)) if value.is_finite() && *value > 0.0 => *value,
        Some(_) => {
            return Err(TmxError::Invalid(
                "citadel_units_per_pixel must be a positive float",
            ));
        }
        None => 1.0,
    };
    let wall_height = map.tile_height as f32 * scale;
    let mut collision = CollisionMesh::default();
    for layer in map.layers() {
        let layer_enabled = enabled(&layer.properties) || layer.name == "collision";
        match layer.layer_type() {
            LayerType::Tiles(TileLayer::Finite(tiles)) => {
                for y in 0..map.height as i32 {
                    for x in 0..map.width as i32 {
                        if let Some(instance) = tiles.get_tile(x, y) {
                            if instance.flip_h || instance.flip_v || instance.flip_d {
                                return Err(TmxError::Unsupported("flipped collision tiles"));
                            }
                            if let Some(tile) = instance.get_tile() {
                                if enabled(&tile.properties) {
                                    add_prism(
                                        &mut collision,
                                        x as f32 * map.tile_width as f32 * scale,
                                        y as f32 * map.tile_height as f32 * scale,
                                        map.tile_width as f32 * scale,
                                        map.tile_height as f32 * scale,
                                        wall_height,
                                    );
                                }
                                if let Some(objects) = &tile.collision {
                                    for object in objects.object_data() {
                                        add_object(
                                            &mut collision,
                                            object,
                                            x as f32 * map.tile_width as f32 * scale
                                                + object.x * scale,
                                            y as f32 * map.tile_height as f32 * scale
                                                + object.y * scale,
                                            scale,
                                            wall_height,
                                        )?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            LayerType::Objects(objects) if layer_enabled => {
                for object in objects.objects() {
                    add_object(
                        &mut collision,
                        &object,
                        object.x * scale,
                        object.y * scale,
                        scale,
                        wall_height,
                    )?;
                }
            }
            _ => {}
        }
    }
    let name = path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or(TmxError::Invalid("map path has no UTF-8 stem"))?
        .to_owned();
    let (mut min, mut max) = ([f32::INFINITY; 3], [f32::NEG_INFINITY; 3]);
    for vertex in &collision.vertices {
        for axis in 0..3 {
            min[axis] = min[axis].min(vertex[axis]);
            max[axis] = max[axis].max(vertex[axis]);
        }
    }
    if collision.vertices.is_empty() {
        min = [0.0; 3];
        max = [0.0; 3];
    }
    Ok(MapFile {
        metadata: MapMetadata {
            name,
            bounds_min: min,
            bounds_max: max,
        },
        collision,
        navmesh: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str, body: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("citadel-tmx-{name}-{}.tmx", std::process::id()));
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn imports_collision_tile_property() {
        let path = fixture(
            "tile",
            r#"<?xml version="1.0"?><map version="1.10" orientation="orthogonal" width="1" height="1" tilewidth="16" tileheight="16" infinite="0"><tileset firstgid="1" name="a" tilewidth="16" tileheight="16" tilecount="1" columns="1"><tile id="0"><properties><property name="citadel_collision" type="bool" value="true"/></properties></tile></tileset><layer name="terrain" width="1" height="1"><data encoding="csv">1</data></layer></map>"#,
        );
        let map = load(&path).unwrap();
        assert_eq!(map.collision.vertices.len(), 8);
        assert_eq!(map.collision.triangles.len(), 12);
    }

    #[test]
    fn rejects_non_orthogonal_maps() {
        let path = fixture(
            "iso",
            r#"<?xml version="1.0"?><map version="1.10" orientation="isometric" width="1" height="1" tilewidth="16" tileheight="16" infinite="0"></map>"#,
        );
        assert!(matches!(load(&path), Err(TmxError::Unsupported(_))));
    }
}
