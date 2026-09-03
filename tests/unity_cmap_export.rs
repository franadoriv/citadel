//! Golden regression fixture for the Unity editor-only CMAP writer.
//!
//! The bytes deliberately do not come from `MapFile::encode`: this verifies
//! the big-endian writer in `clients/unity/Editor/CitadelCmapExporter.cs`.

use citadel_map::MapFile;

fn fixture_bytes() -> Vec<u8> {
    include_str!("../clients/unity/Editor/tests/unity_terrain_fixture_cmap_v1.hex")
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
fn unity_terrain_cmap_decodes_and_bakes() {
    let map = MapFile::decode(&fixture_bytes()).expect("Unity CMAP fixture must decode");

    assert_eq!(map.metadata.name, "UnityTerrainFixture");
    assert_eq!(map.metadata.bounds_min, [0.0, 0.0, 0.0]);
    assert_eq!(map.metadata.bounds_max, [1.0, 0.0, 1.0]);
    assert_eq!(map.collision.vertices.len(), 4);
    assert_eq!(map.collision.triangles, vec![[0, 1, 2], [2, 1, 3]]);

    citadel_nav::bake(&map.collision).expect("Unity terrain floor must bake a Detour tile");
}
