@tool
extends EditorPlugin

const CmapExporter = preload("res://addons/citadel_map_exporter/cmap_exporter.gd")

var _dialog: EditorFileDialog


func _enter_tree() -> void:
	add_tool_menu_item("Citadel/Export CMAP Map…", _show_export_dialog)
	_dialog = EditorFileDialog.new()
	_dialog.file_mode = EditorFileDialog.FILE_MODE_SAVE_FILE
	_dialog.access = EditorFileDialog.ACCESS_FILESYSTEM
	_dialog.filters = PackedStringArray(["*.map ; Citadel map (CMAP)"])
	_dialog.file_selected.connect(_export_selected_scene)
	get_editor_interface().get_base_control().add_child(_dialog)


func _exit_tree() -> void:
	remove_tool_menu_item("Citadel/Export CMAP Map…")
	if is_instance_valid(_dialog):
		_dialog.queue_free()


func _show_export_dialog() -> void:
	var root := get_editor_interface().get_edited_scene_root()
	if root == null:
		push_warning("Open and save a 3D scene before exporting a Citadel map.")
		return
	_dialog.current_file = "%s.map" % root.name
	_dialog.popup_centered_ratio(0.65)


func _export_selected_scene(path: String) -> void:
	var root := get_editor_interface().get_edited_scene_root()
	if root == null:
		return
	var result: Dictionary = CmapExporter.export_scene(root, path, path.get_file().get_basename())
	if result.get("ok", false):
		print("Citadel CMAP exported: %s (%s vertices, %s triangles; welded %s, degenerate %s)" % [result.path, result.vertices, result.triangles, result.welded, result.degenerate])
	else:
		push_error("Citadel CMAP export failed: %s" % result.get("error", "unknown error"))
