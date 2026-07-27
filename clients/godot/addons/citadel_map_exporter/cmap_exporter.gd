@tool
class_name CitadelCmapExporter
extends RefCounted

## Writes Citadel CMAP v1 files. Keep this layout in lockstep with
## crates/citadel-map/src/lib.rs: every integer and float is big-endian.
const MAGIC := "CMAP"
const VERSION := 1
const SECTION_METADATA := 1
const SECTION_COLLISION := 2
const WELD_EPSILON := 0.001
const MAXIMUM_TRIANGLES := 10_000_000


## Export `root`'s static MeshInstance3D geometry into `output_path`.
##
## A mesh is included when it is below a StaticBody3D, or it carries the
## `citadel_export_collision` metadata set to true. Set that metadata to false
## to exclude an otherwise eligible mesh. The output uses Godot world units;
## keep the server and game scene in the same unit convention.
static func export_scene(root: Node, output_path: String, level_name: String = "") -> Dictionary:
	var vertices: Array[Vector3] = []
	var triangles: Array[PackedInt32Array] = []
	var collection_error := _collect(root, false, vertices, triangles)
	if not collection_error.is_empty():
		return {"ok": false, "error": collection_error}
	if triangles.is_empty():
		return {"ok": false, "error": "No static collision meshes were found."}
	var normalized := _normalize(vertices, triangles)
	if normalized.get("error", "") != "":
		return {"ok": false, "error": normalized.get("error")}
	vertices = normalized.get("vertices")
	triangles = normalized.get("triangles")

	var resolved_name := level_name if not level_name.is_empty() else output_path.get_file().get_basename()
	var payload := _encode(resolved_name, vertices, triangles)
	var file := FileAccess.open(output_path, FileAccess.WRITE)
	if file == null:
		return {"ok": false, "error": "Could not open %s for writing: %s" % [output_path, FileAccess.get_open_error()]}
	file.store_buffer(payload)
	file.close()
	return {"ok": true, "path": output_path, "vertices": vertices.size(), "triangles": triangles.size(), "welded": normalized.get("welded"), "degenerate": normalized.get("degenerate")}


static func _collect(node: Node, beneath_static_body: bool, vertices: Array[Vector3], triangles: Array[PackedInt32Array]) -> String:
	var static_here := beneath_static_body or node is StaticBody3D
	if node is MeshInstance3D and _should_export(node, static_here):
		_append_mesh(node, vertices, triangles)
	# Godot intentionally has no built-in Terrain node. A terrain addon opts in by
	# implementing `citadel_cmap_terrain` on its node; see the README for the
	# exact heightfield dictionary. This keeps the map plugin independent of any
	# one terrain addon while exporting collision rather than render geometry.
	if node.has_method("citadel_cmap_terrain"):
		var terrain_error := _append_terrain(node, vertices, triangles)
		if not terrain_error.is_empty():
			return terrain_error
	for child in node.get_children():
		var child_error := _collect(child, static_here, vertices, triangles)
		if not child_error.is_empty():
			return child_error
	return ""


static func _should_export(mesh_instance: MeshInstance3D, beneath_static_body: bool) -> bool:
	if mesh_instance.has_meta("citadel_export_collision"):
		return bool(mesh_instance.get_meta("citadel_export_collision"))
	return beneath_static_body


static func _append_mesh(instance: MeshInstance3D, vertices: Array[Vector3], triangles: Array[PackedInt32Array]) -> void:
	if instance.mesh == null:
		return
	for surface_index in instance.mesh.get_surface_count():
		var arrays := instance.mesh.surface_get_arrays(surface_index)
		if arrays.is_empty():
			continue
		var source_vertices: PackedVector3Array = arrays[Mesh.ARRAY_VERTEX]
		var source_indices: PackedInt32Array = arrays[Mesh.ARRAY_INDEX]
		if source_vertices.is_empty():
			continue
		var start := vertices.size()
		for vertex in source_vertices:
			vertices.append(instance.global_transform * vertex)
		if source_indices.is_empty():
			for index in range(0, source_vertices.size() - 2, 3):
				triangles.append(PackedInt32Array([start + index, start + index + 1, start + index + 2]))
		else:
			for index in range(0, source_indices.size() - 2, 3):
				triangles.append(PackedInt32Array([start + source_indices[index], start + source_indices[index + 1], start + source_indices[index + 2]]))


static func _append_terrain(provider: Node, vertices: Array[Vector3], triangles: Array[PackedInt32Array]) -> String:
	var terrain: Dictionary = provider.call("citadel_cmap_terrain")
	var width := int(terrain.get("width", 0))
	var depth := int(terrain.get("depth", 0))
	var heights: PackedFloat32Array = terrain.get("heights", PackedFloat32Array())
	var holes: PackedByteArray = terrain.get("holes", PackedByteArray())
	var size: Vector3 = terrain.get("size", Vector3.ZERO)
	if width < 2 or depth < 2 or heights.size() != width * depth:
		return "Citadel terrain provider '%s' returned an invalid heightfield." % provider.get_path()
	if not holes.is_empty() and holes.size() != (width - 1) * (depth - 1):
		return "Citadel terrain provider '%s' returned invalid hole cells." % provider.get_path()
	var start := vertices.size()
	for z in depth:
		for x in width:
			var local := Vector3(size.x * float(x) / float(width - 1), heights[z * width + x], size.z * float(z) / float(depth - 1))
			vertices.append(provider.global_transform * local)
	for z in range(depth - 1):
		for x in range(width - 1):
			if not holes.is_empty() and holes[z * (width - 1) + x] != 0:
				continue
			var a := start + z * width + x
			var b := start + (z + 1) * width + x
			var c := start + z * width + x + 1
			var d := start + (z + 1) * width + x + 1
			triangles.append(PackedInt32Array([a, b, c]))
			triangles.append(PackedInt32Array([c, b, d]))
	return ""


static func _normalize(vertices: Array[Vector3], triangles: Array[PackedInt32Array]) -> Dictionary:
	var output_vertices: Array[Vector3] = []
	var output_triangles: Array[PackedInt32Array] = []
	var lookup := {}
	var remap: Array[int] = []
	var welded := 0
	for vertex in vertices:
		if not vertex.is_finite():
			return {"error": "A collision source contains a non-finite world-space vertex."}
		var key := "%s:%s:%s" % [roundi(vertex.x / WELD_EPSILON), roundi(vertex.y / WELD_EPSILON), roundi(vertex.z / WELD_EPSILON)]
		if lookup.has(key):
			remap.append(lookup[key])
			welded += 1
		else:
			var index := output_vertices.size()
			lookup[key] = index
			remap.append(index)
			output_vertices.append(vertex)
	var degenerate := 0
	for triangle in triangles:
		var a := remap[triangle[0]]
		var b := remap[triangle[1]]
		var c := remap[triangle[2]]
		if a == b or b == c or a == c or output_vertices[b].distance_squared_to(output_vertices[a]) == 0.0 or (output_vertices[b] - output_vertices[a]).cross(output_vertices[c] - output_vertices[a]).length_squared() <= WELD_EPSILON * WELD_EPSILON:
			degenerate += 1
			continue
		if output_triangles.size() >= MAXIMUM_TRIANGLES:
			return {"error": "CMAP export exceeds the %s-triangle safety limit." % MAXIMUM_TRIANGLES}
		output_triangles.append(PackedInt32Array([a, b, c]))
	return {"vertices": output_vertices, "triangles": output_triangles, "welded": welded, "degenerate": degenerate}


static func _encode(level_name: String, vertices: Array[Vector3], triangles: Array[PackedInt32Array]) -> PackedByteArray:
	var metadata := _metadata_payload(level_name, vertices)
	var collision := _collision_payload(vertices, triangles)
	var out := PackedByteArray()
	out.append_array(MAGIC.to_ascii_buffer())
	out.append_array(_u32(VERSION))
	out.append_array(_section(SECTION_METADATA, metadata))
	out.append_array(_section(SECTION_COLLISION, collision))
	return out


static func _metadata_payload(level_name: String, vertices: Array[Vector3]) -> PackedByteArray:
	var utf8 := level_name.to_utf8_buffer()
	if utf8.size() > 65535:
		push_error("Citadel map name must fit in a u16 UTF-8 length")
		return PackedByteArray()
	var min_corner := vertices[0]
	var max_corner := vertices[0]
	for vertex in vertices:
		min_corner = min_corner.min(vertex)
		max_corner = max_corner.max(vertex)
	var out := PackedByteArray()
	out.append_array(_u16(utf8.size()))
	out.append_array(utf8)
	out.append_array(_vec3(min_corner))
	out.append_array(_vec3(max_corner))
	return out


static func _collision_payload(vertices: Array[Vector3], triangles: Array[PackedInt32Array]) -> PackedByteArray:
	var out := PackedByteArray()
	out.append_array(_u32(vertices.size()))
	for vertex in vertices:
		out.append_array(_vec3(vertex))
	out.append_array(_u32(triangles.size()))
	for triangle in triangles:
		out.append_array(_u32(triangle[0]))
		out.append_array(_u32(triangle[1]))
		out.append_array(_u32(triangle[2]))
	return out


static func _section(id: int, payload: PackedByteArray) -> PackedByteArray:
	var out := PackedByteArray()
	out.append_array(_u32(id))
	out.append_array(_u32(payload.size()))
	out.append_array(payload)
	return out


static func _u16(value: int) -> PackedByteArray:
	var peer := StreamPeerBuffer.new()
	peer.big_endian = true
	peer.put_u16(value)
	return peer.data_array


static func _u32(value: int) -> PackedByteArray:
	var peer := StreamPeerBuffer.new()
	peer.big_endian = true
	peer.put_u32(value)
	return peer.data_array


static func _f32(value: float) -> PackedByteArray:
	var peer := StreamPeerBuffer.new()
	peer.big_endian = true
	peer.put_float(value)
	return peer.data_array


static func _vec3(value: Vector3) -> PackedByteArray:
	var out := PackedByteArray()
	out.append_array(_f32(value.x))
	out.append_array(_f32(value.y))
	out.append_array(_f32(value.z))
	return out
