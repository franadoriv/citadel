// Editor-only CMAP v1 collision exporter. Import under Assets/Citadel/Editor.
// ; terrain normalization rules: docs/architecture/cmap-terrain-export.md.
#if UNITY_EDITOR
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using UnityEditor;
using UnityEngine;

namespace Citadel.Editor
{
    public static class CitadelCmapExporter
    {
        const float WeldEpsilon = 0.001f;
        // A bad terrain resolution should fail the export rather than leave a
        // partially written level file. Projects with deliberately larger maps
        // can raise this constant in their vendored editor script.
        const int MaximumTriangles = 10_000_000;

        [MenuItem("Tools/Citadel/Export CMAP Map...")]
        public static void ExportActiveScene()
        {
            var sceneName = UnityEngine.SceneManagement.SceneManager.GetActiveScene().name;
            var path = EditorUtility.SaveFilePanel("Export Citadel CMAP", Application.dataPath, sceneName, "map");
            if (string.IsNullOrEmpty(path)) return;
            try
            {
                var mesh = new Builder();
                var colliders = new List<MeshCollider>(UnityEngine.Object.FindObjectsByType<MeshCollider>(FindObjectsInactive.Exclude, FindObjectsSortMode.None));
                var terrains = new List<Terrain>(UnityEngine.Object.FindObjectsByType<Terrain>(FindObjectsInactive.Exclude, FindObjectsSortMode.None));
                colliders.Sort((left, right) => string.CompareOrdinal(SceneKey(left.transform), SceneKey(right.transform)));
                terrains.Sort((left, right) => string.CompareOrdinal(SceneKey(left.transform), SceneKey(right.transform)));
                foreach (var collider in colliders)
                    if (collider.enabled && collider.sharedMesh != null && collider.gameObject.isStatic)
                        mesh.AppendMesh(collider.sharedMesh, collider.transform.localToWorldMatrix, SceneKey(collider.transform));
                foreach (var terrain in terrains)
                    if (terrain.enabled && terrain.terrainData != null && terrain.gameObject.isStatic)
                        mesh.AppendTerrain(terrain, SceneKey(terrain.transform));
                if (mesh.Triangles.Count == 0) { EditorUtility.DisplayDialog("Citadel CMAP", "No static MeshCollider or Terrain geometry found.", "OK"); return; }
                mesh.Write(path, Path.GetFileNameWithoutExtension(path));
                Debug.Log($"Citadel CMAP exported: {path} ({mesh.Sources} sources; {mesh.InputVertices} input vertices, {mesh.Vertices.Count} output vertices; {mesh.InputTriangles} input triangles, {mesh.Triangles.Count} output triangles; welded {mesh.Welded}, degenerate {mesh.Degenerate})");
            }
            catch (Exception error)
            {
                Debug.LogError($"Citadel CMAP export failed: {error.Message}");
                EditorUtility.DisplayDialog("Citadel CMAP", error.Message, "OK");
            }
        }

        static string SceneKey(Transform transform)
        {
            var parts = new Stack<string>();
            for (var current = transform; current != null; current = current.parent)
                parts.Push($"{current.GetSiblingIndex():D8}:{current.name}");
            return $"{transform.gameObject.scene.path}/{string.Join("/", parts)}";
        }

        sealed class Builder
        {
            public readonly List<Vector3> Vertices = new();
            public readonly List<int[]> Triangles = new();
            readonly Dictionary<Key, int> index = new();
            public int Welded { get; private set; }
            public int Degenerate { get; private set; }
            public int Sources { get; private set; }
            public int InputVertices { get; private set; }
            public int InputTriangles { get; private set; }
            public void AppendMesh(Mesh mesh, Matrix4x4 transform, string sourceName)
            {
                ValidateTransform(transform, sourceName);
                var source = mesh.vertices; var ids = mesh.triangles; var mapped = new int[source.Length];
                Sources++; InputVertices += source.Length;
                for (var i = 0; i < source.Length; i++) mapped[i] = Vertex(transform.MultiplyPoint3x4(source[i]));
                for (var i = 0; i + 2 < ids.Length; i += 3) { InputTriangles++; Triangle(mapped[ids[i]], mapped[ids[i + 1]], mapped[ids[i + 2]]); }
            }
            public void AppendTerrain(Terrain terrain, string sourceName)
            {
                var data = terrain.terrainData; var resolution = data.heightmapResolution;
                var heights = data.GetHeights(0, 0, resolution, resolution); var holes = data.GetHoles(0, 0, data.holesResolution, data.holesResolution);
                var grid = new int[resolution, resolution]; var size = data.size; var matrix = terrain.transform.localToWorldMatrix;
                ValidateTransform(matrix, sourceName); Sources++; InputVertices += resolution * resolution;
                for (var z = 0; z < resolution; z++) for (var x = 0; x < resolution; x++)
                    grid[z, x] = Vertex(matrix.MultiplyPoint3x4(new Vector3(size.x * x / (resolution - 1), heights[z, x] * size.y, size.z * z / (resolution - 1))));
                for (var z = 0; z < resolution - 1; z++) for (var x = 0; x < resolution - 1; x++)
                {
                    var hx = Math.Min(x * holes.GetLength(1) / (resolution - 1), holes.GetLength(1) - 1);
                    var hz = Math.Min(z * holes.GetLength(0) / (resolution - 1), holes.GetLength(0) - 1);
                    // TerrainData.GetHoles marks an absent terrain cell as true.
                    if (holes[hz, hx]) continue;
                    InputTriangles += 2;
                    Triangle(grid[z, x], grid[z + 1, x], grid[z, x + 1]); Triangle(grid[z, x + 1], grid[z + 1, x], grid[z + 1, x + 1]);
                }
            }
            int Vertex(Vector3 point) { if (float.IsNaN(point.x) || float.IsNaN(point.y) || float.IsNaN(point.z) || float.IsInfinity(point.x) || float.IsInfinity(point.y) || float.IsInfinity(point.z)) throw new InvalidOperationException("A collision source contains a non-finite world-space vertex."); var key = new Key(point); if (index.TryGetValue(key, out var existing)) { Welded++; return existing; } var next = Vertices.Count; index[key] = next; Vertices.Add(point); return next; }
            void Triangle(int a, int b, int c) { if (a == b || b == c || a == c || Vector3.Cross(Vertices[b] - Vertices[a], Vertices[c] - Vertices[a]).sqrMagnitude <= WeldEpsilon * WeldEpsilon) { Degenerate++; return; } if (Triangles.Count == MaximumTriangles) throw new InvalidOperationException($"CMAP export exceeds the {MaximumTriangles:N0}-triangle safety limit."); Triangles.Add(new[] { a, b, c }); }
            static void ValidateTransform(Matrix4x4 transform, string sourceName) { if (Mathf.Abs(transform.determinant) <= float.Epsilon) throw new InvalidOperationException($"Collision source '{sourceName}' has a singular world transform."); }
            public void Write(string path, string name)
            {
                var nameBytes = Encoding.UTF8.GetByteCount(name);
                if (nameBytes > ushort.MaxValue) throw new InvalidOperationException("CMAP level name exceeds the u16 UTF-8 length limit.");
                using var file = File.Create(path); using var writer = new BinaryWriter(file);
                writer.Write(Encoding.ASCII.GetBytes("CMAP")); U32(writer, 1);
                using var metadata = new MemoryStream(); using (var outp = new BinaryWriter(metadata, Encoding.UTF8, true)) { var utf8 = Encoding.UTF8.GetBytes(name); U16(outp, (ushort)utf8.Length); outp.Write(utf8); var min = Vertices[0]; var max = min; foreach (var v in Vertices) { min = Vector3.Min(min, v); max = Vector3.Max(max, v); } Vec(outp, min); Vec(outp, max); } Section(writer, 1, metadata.ToArray());
                using var collision = new MemoryStream(); using (var outp = new BinaryWriter(collision, Encoding.UTF8, true)) { U32(outp, (uint)Vertices.Count); foreach (var v in Vertices) Vec(outp, v); U32(outp, (uint)Triangles.Count); foreach (var t in Triangles) { U32(outp, (uint)t[0]); U32(outp, (uint)t[1]); U32(outp, (uint)t[2]); } } Section(writer, 2, collision.ToArray());
            }
            static void Section(BinaryWriter w, uint id, byte[] payload) { U32(w, id); U32(w, (uint)payload.Length); w.Write(payload); }
            static void Vec(BinaryWriter w, Vector3 v) { F32(w, v.x); F32(w, v.y); F32(w, v.z); }
            static void U16(BinaryWriter w, ushort value) { w.Write((byte)(value >> 8)); w.Write((byte)value); }
            static void U32(BinaryWriter w, uint value) { w.Write((byte)(value >> 24)); w.Write((byte)(value >> 16)); w.Write((byte)(value >> 8)); w.Write((byte)value); }
            static void F32(BinaryWriter w, float value) { U32(w, BitConverter.ToUInt32(BitConverter.GetBytes(value), 0)); }
            readonly struct Key : IEquatable<Key> { readonly int x, y, z; public Key(Vector3 p) { x = Mathf.RoundToInt(p.x / WeldEpsilon); y = Mathf.RoundToInt(p.y / WeldEpsilon); z = Mathf.RoundToInt(p.z / WeldEpsilon); } public bool Equals(Key other) => x == other.x && y == other.y && z == other.z; public override int GetHashCode() => HashCode.Combine(x, y, z); }
        }
    }
}
#endif
