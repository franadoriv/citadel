// CitadelMapCooker.h — level-geometry -> Citadel `.map` cook tool (, Phase B).
//
// FCitadelMapCooker gathers the current editor level's static collision geometry
// into one world-space triangle mesh and writes it as a CMAP `.map` file (see
// CitadelCmapWriter.h / crates/citadel-map). The server loads that file by its
// file stem when a room selects the matching map name (src/maps/), then bakes a
// navmesh from the collision (Phase C).
//
// The entry point is CookCurrentLevelInteractive: it prompts the user with a
// native "Save As..." dialog, cooks, writes, and reports the result. It is wired
// to Tools -> Citadel -> Cook Map Data by FCitadelEditorModule.
#pragma once

#include "CoreMinimal.h"

class UWorld;

namespace CitadelCmap { struct FCookedMesh; }

/** Editor-only map cooker. Pure geometry -> bytes; no networking, no game runtime. */
class FCitadelMapCooker
{
public:
	/** Prompt for an output path, cook the current level, write the `.map`, and
	 *  surface a success/failure notification. Safe to call with no world (no-op
	 *  with a warning). */
	static void CookCurrentLevelInteractive();

	/** Gather every static, collidable StaticMesh (including instanced) in `World`
	 *  into one world-space indexed triangle mesh + AABB. `OutMesh.Name` is left to
	 *  the caller. Returns false only when `World` is null. An empty level yields a
	 *  valid mesh with zero triangles (true). */
	static bool CookWorld(UWorld* World, CitadelCmap::FCookedMesh& OutMesh);
};
