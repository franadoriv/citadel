// CitadelMapCooker.cpp — see CitadelMapCooker.h.

#include "CitadelMapCooker.h"

#include "CitadelCmapWriter.h"

#include "Components/InstancedStaticMeshComponent.h"
#include "Components/StaticMeshComponent.h"
#include "Editor.h"
#include "Engine/StaticMesh.h"
#include "Engine/World.h"
#include "EngineUtils.h"
#include "Framework/Application/SlateApplication.h"
#include "Framework/Notifications/NotificationManager.h"
#include "GameFramework/Actor.h"
#include "Interfaces/Interface_CollisionDataProvider.h"
#include "LandscapeHeightfieldCollisionComponent.h"
#include "Misc/FileHelper.h"
#include "Misc/PackageName.h"
#include "Misc/Paths.h"
#include "StaticMeshResources.h"
#include "Widgets/Notifications/SNotificationList.h"

#include "DesktopPlatformModule.h"
#include "IDesktopPlatform.h"

DEFINE_LOG_CATEGORY_STATIC(LogCitadelEditor, Log, All);

namespace
{
	// Append one static mesh's collision triangles (world space) to the cook.
	//
	// Prefers the authoritative complex-collision trimesh (GetPhysicsTriMeshData);
	// falls back to LOD0 render geometry when a mesh has no cooked trimesh (e.g.
	// simple-collision-only primitives). Either way the result is real triangles a
	// navmesh baker can walk.
	void AppendMeshCollision(const UStaticMesh* Mesh, const FTransform& WorldXform,
	                         CitadelCmap::FCookedMesh& Out, FBox& Bounds)
	{
		if (!Mesh)
		{
			return;
		}

		// --- Primary: complex collision trimesh -------------------------------
		FTriMeshCollisionData Tri;
		const bool bGot = const_cast<UStaticMesh*>(Mesh)->GetPhysicsTriMeshData(&Tri, /*InUseAllTriData=*/true);
		if (bGot && Tri.Vertices.Num() > 0 && Tri.Indices.Num() > 0)
		{
			const int32 Base = Out.Vertices.Num();
			for (const FVector3f& LocalV : Tri.Vertices)
			{
				const FVector WorldV = WorldXform.TransformPosition(FVector(LocalV));
				Out.Vertices.Add(FVector3f(WorldV));
				Bounds += WorldV;
			}
			for (const FTriIndices& T : Tri.Indices)
			{
				Out.Triangles.Add(FIntVector(Base + T.v0, Base + T.v1, Base + T.v2));
			}
			return;
		}

		// --- Fallback: LOD0 render geometry -----------------------------------
		const FStaticMeshRenderData* RD = Mesh->GetRenderData();
		if (!RD || RD->LODResources.Num() == 0)
		{
			return;
		}
		const FStaticMeshLODResources& LOD = RD->LODResources[0];
		const FPositionVertexBuffer& PVB = LOD.VertexBuffers.PositionVertexBuffer;
		const FIndexArrayView Indices = LOD.IndexBuffer.GetArrayView();
		const uint32 NumVerts = PVB.GetNumVertices();
		if (NumVerts == 0 || Indices.Num() < 3)
		{
			return;
		}
		const int32 Base = Out.Vertices.Num();
		for (uint32 i = 0; i < NumVerts; ++i)
		{
			const FVector WorldV = WorldXform.TransformPosition(FVector(PVB.VertexPosition(i)));
			Out.Vertices.Add(FVector3f(WorldV));
			Bounds += WorldV;
		}
		const int32 TriCount = Indices.Num() / 3;
		for (int32 t = 0; t < TriCount; ++t)
		{
			Out.Triangles.Add(FIntVector(
				Base + int32(Indices[t * 3 + 0]),
				Base + int32(Indices[t * 3 + 1]),
				Base + int32(Indices[t * 3 + 2])));
		}
	}

	// Append the same collision-height grid Unreal uses for a Landscape component.
	// We deliberately do not read the render heightmap or invoke Merge Actors:
	// render LOD/material displacement is not authoritative collision geometry.
	void AppendLandscapeCollision(const ULandscapeHeightfieldCollisionComponent* Collision,
	                             CitadelCmap::FCookedMesh& Out, FBox& Bounds)
	{
		if (!Collision || Collision->GetCollisionEnabled() == ECollisionEnabled::NoCollision)
		{
			return;
		}

		const int32 Rows = Collision->HeightfieldRowsCount;
		const int32 Columns = Collision->HeightfieldColumnsCount;
		if (Rows < 2 || Columns < 2 || !Collision->CachedLocalBox.IsValid)
		{
			return;
		}

		TArray<float> Heights;
		Heights.SetNumUninitialized(Rows * Columns);
		if (!Collision->FillHeightTile(Heights, /*Offset=*/0, /*Stride=*/Columns))
		{
			UE_LOG(LogCitadelEditor, Warning,
		       TEXT("Citadel: unable to read Landscape collision heightfield '%s'."),
		       *Collision->GetPathName());
			return;
		}

		const FBox& LocalBounds = Collision->CachedLocalBox;
		const FTransform WorldXform = Collision->GetComponentTransform();
		const int32 Base = Out.Vertices.Num();
		for (int32 Row = 0; Row < Rows; ++Row)
		{
			for (int32 Column = 0; Column < Columns; ++Column)
			{
				const float X = FMath::Lerp(LocalBounds.Min.X, LocalBounds.Max.X,
				                            float(Column) / float(Columns - 1));
				const float Y = FMath::Lerp(LocalBounds.Min.Y, LocalBounds.Max.Y,
				                            float(Row) / float(Rows - 1));
				const float Z = Heights[Row * Columns + Column];
				if (!FMath::IsFinite(Z))
				{
					UE_LOG(LogCitadelEditor, Warning,
					       TEXT("Citadel: Landscape collision '%s' has a non-finite height."),
					       *Collision->GetPathName());
					return;
				}
				const FVector WorldV = WorldXform.TransformPosition(FVector(X, Y, Z));
				Out.Vertices.Add(FVector3f(WorldV));
				Bounds += WorldV;
			}
		}

		// These are the stable serialized Landscape collision flags: bit 7 is a
		// visibility hole/no-collision cell and bit 6 selects the alternate quad
		// diagonal. Lower bits select physical material and do not affect CMAP.
		constexpr uint8 LandscapeNoCollision = 128;
		constexpr uint8 LandscapeEdgeTurned = 64;
		for (int32 Row = 0; Row < Rows - 1; ++Row)
		{
			for (int32 Column = 0; Column < Columns - 1; ++Column)
			{
				const int32 QuadIndex = Row * (Columns - 1) + Column;
				const uint8 Flags = Collision->CollisionQuadFlags.IsValidIndex(QuadIndex)
				                        ? Collision->CollisionQuadFlags[QuadIndex]
				                        : 0;
				if ((Flags & LandscapeNoCollision) != 0)
				{
					continue;
				}
				const int32 A = Base + Row * Columns + Column;
				const int32 B = Base + (Row + 1) * Columns + Column;
				const int32 C = Base + Row * Columns + Column + 1;
				const int32 D = Base + (Row + 1) * Columns + Column + 1;
				if ((Flags & LandscapeEdgeTurned) != 0)
				{
					Out.Triangles.Add(FIntVector(A, B, D));
					Out.Triangles.Add(FIntVector(A, D, C));
				}
				else
				{
					Out.Triangles.Add(FIntVector(A, B, C));
					Out.Triangles.Add(FIntVector(C, B, D));
				}
			}
		}
	}

	// Match the cross-engine CMAP normalization contract. This is intentionally a
	// logical union: it welds a shared vertex/index and drops degenerates, but it
	// never mutates source assets or performs an implicit boolean operation.
	bool NormalizeMesh(CitadelCmap::FCookedMesh& Mesh)
	{
		constexpr float WeldEpsilon = 0.001f;
		constexpr int32 MaximumTriangles = 10 * 1000 * 1000;
		TMap<FIntVector, int32> WeldedIndices;
		TArray<FVector3f> WeldedVertices;
		TArray<int32> Remap;
		Remap.SetNumUninitialized(Mesh.Vertices.Num());

		for (int32 SourceIndex = 0; SourceIndex < Mesh.Vertices.Num(); ++SourceIndex)
		{
			const FVector3f& Vertex = Mesh.Vertices[SourceIndex];
			if (!FMath::IsFinite(Vertex.X) || !FMath::IsFinite(Vertex.Y) || !FMath::IsFinite(Vertex.Z))
			{
				UE_LOG(LogCitadelEditor, Warning, TEXT("Citadel: collision geometry contains a non-finite vertex."));
				return false;
			}
			const FIntVector Key(
				FMath::RoundToInt(Vertex.X / WeldEpsilon),
				FMath::RoundToInt(Vertex.Y / WeldEpsilon),
				FMath::RoundToInt(Vertex.Z / WeldEpsilon));
			if (const int32* Existing = WeldedIndices.Find(Key))
			{
				Remap[SourceIndex] = *Existing;
			}
			else
			{
				const int32 NewIndex = WeldedVertices.Num();
				WeldedIndices.Add(Key, NewIndex);
				Remap[SourceIndex] = NewIndex;
				WeldedVertices.Add(Vertex);
			}
		}

		TArray<FIntVector> WeldedTriangles;
		WeldedTriangles.Reserve(Mesh.Triangles.Num());
		for (const FIntVector& SourceTriangle : Mesh.Triangles)
		{
			if (!Remap.IsValidIndex(SourceTriangle.X) || !Remap.IsValidIndex(SourceTriangle.Y) || !Remap.IsValidIndex(SourceTriangle.Z))
			{
				UE_LOG(LogCitadelEditor, Warning, TEXT("Citadel: collision geometry has an invalid triangle index."));
				return false;
			}
			const FIntVector Triangle(Remap[SourceTriangle.X], Remap[SourceTriangle.Y], Remap[SourceTriangle.Z]);
			if (Triangle.X == Triangle.Y || Triangle.Y == Triangle.Z || Triangle.X == Triangle.Z)
			{
				continue;
			}
			const FVector3f Area = FVector3f::CrossProduct(
				WeldedVertices[Triangle.Y] - WeldedVertices[Triangle.X],
				WeldedVertices[Triangle.Z] - WeldedVertices[Triangle.X]);
			if (Area.SizeSquared() <= WeldEpsilon * WeldEpsilon)
			{
				continue;
			}
			if (WeldedTriangles.Num() >= MaximumTriangles)
			{
				UE_LOG(LogCitadelEditor, Warning, TEXT("Citadel: CMAP export exceeds the 10-million-triangle safety limit."));
				return false;
			}
			WeldedTriangles.Add(Triangle);
		}

		if (WeldedTriangles.IsEmpty())
		{
			return false;
		}
		Mesh.Vertices = MoveTemp(WeldedVertices);
		Mesh.Triangles = MoveTemp(WeldedTriangles);
		FBox Bounds(ForceInit);
		for (const FVector3f& Vertex : Mesh.Vertices)
		{
			Bounds += FVector(Vertex);
		}
		Mesh.BoundsMin = FVector3f(Bounds.Min);
		Mesh.BoundsMax = FVector3f(Bounds.Max);
		return true;
	}

	void Notify(const FString& Message, bool bSuccess)
	{
		FNotificationInfo Info(FText::FromString(Message));
		Info.ExpireDuration = bSuccess ? 6.0f : 8.0f;
		Info.bUseSuccessFailIcons = true;
		const TSharedPtr<SNotificationItem> Item = FSlateNotificationManager::Get().AddNotification(Info);
		if (Item.IsValid())
		{
			Item->SetCompletionState(bSuccess ? SNotificationItem::CS_Success : SNotificationItem::CS_Fail);
		}
		if (bSuccess)
		{
			UE_LOG(LogCitadelEditor, Log, TEXT("%s"), *Message);
		}
		else
		{
			UE_LOG(LogCitadelEditor, Warning, TEXT("%s"), *Message);
		}
	}
}

bool FCitadelMapCooker::CookWorld(UWorld* World, CitadelCmap::FCookedMesh& OutMesh)
{
	if (!World)
	{
		return false;
	}

	FBox Bounds(ForceInit);
	for (TActorIterator<AActor> It(World); It; ++It)
	{
		AActor* Actor = *It;
		if (!Actor)
		{
			continue;
		}
		TArray<UStaticMeshComponent*> Comps;
		Actor->GetComponents(Comps);
		for (UStaticMeshComponent* Comp : Comps)
		{
			// Only static geometry that actually collides feeds the navmesh baker.
			if (!Comp || Comp->GetCollisionEnabled() == ECollisionEnabled::NoCollision)
			{
				continue;
			}
			const UStaticMesh* Mesh = Comp->GetStaticMesh();
			if (!Mesh)
			{
				continue;
			}

			// Instanced meshes carry one transform per instance; regular components
			// carry a single world transform.
			if (const UInstancedStaticMeshComponent* ISM = Cast<UInstancedStaticMeshComponent>(Comp))
			{
				const int32 Count = ISM->GetInstanceCount();
				for (int32 i = 0; i < Count; ++i)
				{
					FTransform InstXform;
					if (ISM->GetInstanceTransform(i, InstXform, /*bWorldSpace=*/true))
					{
						AppendMeshCollision(Mesh, InstXform, OutMesh, Bounds);
					}
				}
			}
			else
			{
				AppendMeshCollision(Mesh, Comp->GetComponentTransform(), OutMesh, Bounds);
			}
		}

		// Landscape actors own one collision-heightfield component per tile. Read
		// those components rather than the Landscape render mesh so the server gets
		// the same collision resolution configured in Unreal.
		TArray<ULandscapeHeightfieldCollisionComponent*> LandscapeComps;
		Actor->GetComponents(LandscapeComps);
		LandscapeComps.Sort([](const ULandscapeHeightfieldCollisionComponent& Left,
		                         const ULandscapeHeightfieldCollisionComponent& Right)
		{
			return Left.GetPathName() < Right.GetPathName();
		});
		for (ULandscapeHeightfieldCollisionComponent* Landscape : LandscapeComps)
		{
			AppendLandscapeCollision(Landscape, OutMesh, Bounds);
		}
	}

	if (Bounds.IsValid && OutMesh.Vertices.Num() > 0 && NormalizeMesh(OutMesh))
	{
		return true;
	}
	OutMesh.Vertices.Reset();
	OutMesh.Triangles.Reset();
	return false;
}

void FCitadelMapCooker::CookCurrentLevelInteractive()
{
	UWorld* World = GEditor ? GEditor->GetEditorWorldContext().World() : nullptr;
	if (!World)
	{
		Notify(TEXT("Citadel: Cook Map Data — no editor world is open."), /*bSuccess=*/false);
		return;
	}

	const FString LevelName = FPackageName::GetShortName(World->GetOutermost()->GetName());

	CitadelCmap::FCookedMesh Mesh;
	CookWorld(World, Mesh);
	Mesh.Name = LevelName;

	if (Mesh.Triangles.Num() == 0)
	{
		Notify(FText::Format(
			       NSLOCTEXT("Citadel", "CookEmpty",
			                 "Citadel: Cook Map Data — level '{0}' has no static collision geometry to export."),
			       FText::FromString(LevelName))
		           .ToString(),
		       /*bSuccess=*/false);
		return;
	}

	// --- Choose the output path ("Save As...") --------------------------------
	IDesktopPlatform* DesktopPlatform = FDesktopPlatformModule::Get();
	if (!DesktopPlatform)
	{
		Notify(TEXT("Citadel: Cook Map Data — desktop platform unavailable; cannot open the save dialog."),
		       /*bSuccess=*/false);
		return;
	}
	const void* ParentWindowHandle =
		FSlateApplication::Get().FindBestParentWindowHandleForDialogs(nullptr);
	const FString DefaultPath = FPaths::ProjectSavedDir();
	const FString DefaultFile = LevelName + TEXT(".map");
	TArray<FString> OutFiles;
	const bool bPicked = DesktopPlatform->SaveFileDialog(
		ParentWindowHandle,
		TEXT("Cook Citadel Map Data"),
		DefaultPath,
		DefaultFile,
		TEXT("Citadel Map (*.map)|*.map"),
		EFileDialogFlags::None,
		OutFiles);
	if (!bPicked || OutFiles.Num() == 0)
	{
		return; // user cancelled — no notification needed.
	}
	FString OutPath = OutFiles[0];
	if (!OutPath.EndsWith(TEXT(".map")))
	{
		OutPath += TEXT(".map");
	}

	// --- Encode + write -------------------------------------------------------
	const TArray<uint8> Bytes = CitadelCmap::Encode(Mesh);
	if (!FFileHelper::SaveArrayToFile(Bytes, *OutPath))
	{
		Notify(FText::Format(
			       NSLOCTEXT("Citadel", "CookWriteFail",
			                 "Citadel: Cook Map Data — failed to write '{0}'."),
			       FText::FromString(OutPath))
		           .ToString(),
		       /*bSuccess=*/false);
		return;
	}

	Notify(FText::Format(
		       NSLOCTEXT("Citadel", "CookOk",
		                 "Citadel: cooked '{0}' — {1} verts, {2} tris -> {3}"),
		       FText::FromString(LevelName),
		       FText::AsNumber(Mesh.Vertices.Num()),
		       FText::AsNumber(Mesh.Triangles.Num()),
		       FText::FromString(OutPath))
	           .ToString(),
	       /*bSuccess=*/true);
}
