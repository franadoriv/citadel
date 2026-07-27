// CitadelEditor.Build.cs — UnrealBuildTool rules for the Citadel editor-only
// tooling module (, Phase B).
//
// This module exists to host in-editor authoring tools that must NOT ship in a
// packaged game. Its first (and today only) tool is the map cooker:
//   Tools -> Citadel -> Cook Map Data
// which exports the current level's static collision geometry to a Citadel
// `.map` (CMAP) file the server consumes for navmesh baking + authoritative
// collision (see crates/citadel-map + src/maps/).
//
// It is a plain Editor module (Type "Editor" in Citadel.uplugin), so it is only
// compiled for editor targets and never linked into a shipping client. It does
// NOT depend on the runtime CitadelClient module or the native FFI: cooking map
// geometry is a pure editor-world -> bytes transform with no networking.
using UnrealBuildTool;

public class CitadelEditor : ModuleRules
{
	public CitadelEditor(ReadOnlyTargetRules Target) : base(Target)
	{
		PCHUsage = PCHUsageMode.UseExplicitOrSharedPCHs;

		PublicDependencyModuleNames.AddRange(new string[]
		{
			"Core",
			"CoreUObject",
			"Engine",
			// Landscape exposes the collision-heightfield API used by the CMAP
			// cooker. This is editor-only with the whole CitadelEditor module.
			"Landscape",
		});

		PrivateDependencyModuleNames.AddRange(new string[]
		{
			// Editor scaffolding: menu extension, notifications, and the level
			// editor world we cook geometry from.
			"UnrealEd",
			"ToolMenus",
			"Slate",
			"SlateCore",
			// Native "Save As..." file dialog for choosing the .map output path.
			"DesktopPlatform",
			// FTriMeshCollisionData / GetPhysicsTriMeshData live in the physics
			// interface headers; this is the authoritative collision geometry the
			// cooker exports.
			"PhysicsCore",
		});
	}
}
