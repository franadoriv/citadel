//! Cross-engine golden test for the hand-written Godot CMAP exporter.
//!
//! The checked-in fixture is the exact big-endian byte sequence documented by
//! `clients/godot/addons/citadel_map_exporter/cmap_exporter.gd`. Keeping it out
//! of `MapFile::encode` is intentional: this catches a layout drift in the
//! foreign writer instead of merely retesting the Rust encoder.

use citadel_map::MapFile;

fn fixture_bytes() -> Vec<u8> {
    include_str!("../clients/godot/addons/citadel_map_exporter/tests/godot_fixture_cmap_v1.hex")
        .lines()
        .filter(|line| !line.starts_with('#'))
        .flat_map(str::chars)
        .filter(|character| !character.is_whitespace())
        .collect::<String>()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let hex = std::str::from_utf8(pair).expect("fixture hex is UTF-8");
            u8::from_str_radix(hex, 16).expect("fixture contains valid hexadecimal")
        })
        .collect()
}

#[test]
fn godot_exported_cmap_decodes_and_bakes() {
    let map = MapFile::decode(&fixture_bytes()).expect("Godot CMAP fixture must decode");

    assert_eq!(map.metadata.name, "GodotFixture");
    assert_eq!(map.metadata.bounds_min, [-2.0, 0.0, -2.0]);
    assert_eq!(map.metadata.bounds_max, [2.0, 0.0, 2.0]);
    assert_eq!(map.collision.vertices.len(), 4);
    assert_eq!(map.collision.triangles, vec![[0, 1, 2], [0, 2, 3]]);

    citadel_nav::bake(&map.collision).expect("Godot floor geometry must bake a Detour tile");
}
