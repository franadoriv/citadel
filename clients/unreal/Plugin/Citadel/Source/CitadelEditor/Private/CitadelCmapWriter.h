// CitadelCmapWriter.h — big-endian CMAP encoder used by the map cook tool.
//
// This is a hand port of the WRITE half of crates/citadel-map/src/lib.rs. It MUST
// stay byte-for-byte compatible with that crate, because the Citadel server decodes
// the `.map` files this produces with `citadel_map::MapFile::decode`. The format is:
//
//   Header:  magic b"CMAP" (4) + format_version u32 (=1)
//   Section: id u32 + len u32 + payload[len]   (repeated)
//     METADATA (id 1): name (u16-len utf8) + bounds_min f32*3 + bounds_max f32*3
//     COLLISION (id 2): vcount u32 + verts (f32*3)*vcount + tcount u32 + tris (u32*3)*tcount
//
// All integers are big-endian; floats are IEEE-754 big-endian (matches Rust's
// `f32::to_be_bytes`); strings are u16-length-prefixed UTF-8. Keeping this a small
// private header (rather than reusing the runtime CitadelWire helpers) keeps the
// editor module free of any dependency on the client runtime module.
#pragma once

#include "CoreMinimal.h"

namespace CitadelCmap
{
	// Mirrors crates/citadel-map constants. Bump only alongside the Rust crate.
	constexpr uint32 FormatVersion = 1;
	namespace SectionId
	{
		constexpr uint32 Metadata = 1;
		constexpr uint32 Collision = 2;
		// Navmesh (3) is reserved and not written here (Phase C bakes it).
	}

	inline void PutU16(TArray<uint8>& Out, uint16 V)
	{
		Out.Add(uint8(V >> 8));
		Out.Add(uint8(V & 0xFF));
	}

	inline void PutU32(TArray<uint8>& Out, uint32 V)
	{
		Out.Add(uint8((V >> 24) & 0xFF));
		Out.Add(uint8((V >> 16) & 0xFF));
		Out.Add(uint8((V >> 8) & 0xFF));
		Out.Add(uint8(V & 0xFF));
	}

	inline void PutF32(TArray<uint8>& Out, float F)
	{
		uint32 U;
		FMemory::Memcpy(&U, &F, 4);
		PutU32(Out, U);
	}

	inline void PutVec3(TArray<uint8>& Out, const FVector3f& V)
	{
		PutF32(Out, V.X);
		PutF32(Out, V.Y);
		PutF32(Out, V.Z);
	}

	// u16-length-prefixed UTF-8, matching Rust's `write_str` (truncates at u16::MAX).
	inline void PutStr(TArray<uint8>& Out, const FString& S)
	{
		const FTCHARToUTF8 Utf8(*S);
		const int32 Len = FMath::Min(Utf8.Length(), int32(MAX_uint16));
		PutU16(Out, uint16(Len));
		Out.Append(reinterpret_cast<const uint8*>(Utf8.Get()), Len);
	}

	// Frame one section: id u32 + len u32 + payload (mirrors Rust `write_section`).
	inline void PutSection(TArray<uint8>& Out, uint32 Id, const TArray<uint8>& Payload)
	{
		PutU32(Out, Id);
		PutU32(Out, uint32(Payload.Num()));
		Out.Append(Payload);
	}

	// A world-space indexed triangle mesh plus its AABB — the cooker's output shape.
	struct FCookedMesh
	{
		FString Name;
		FVector3f BoundsMin = FVector3f::ZeroVector;
		FVector3f BoundsMax = FVector3f::ZeroVector;
		TArray<FVector3f> Vertices;
		TArray<FIntVector> Triangles; // indices into Vertices
	};

	// Encode a full CMAP file from a cooked mesh. Byte-compatible with
	// citadel_map::MapFile::decode.
	inline TArray<uint8> Encode(const FCookedMesh& Mesh)
	{
		TArray<uint8> Out;

		// Header.
		Out.Add('C');
		Out.Add('M');
		Out.Add('A');
		Out.Add('P');
		PutU32(Out, FormatVersion);

		// METADATA section.
		{
			TArray<uint8> Payload;
			PutStr(Payload, Mesh.Name);
			PutVec3(Payload, Mesh.BoundsMin);
			PutVec3(Payload, Mesh.BoundsMax);
			PutSection(Out, SectionId::Metadata, Payload);
		}

		// COLLISION section.
		{
			TArray<uint8> Payload;
			PutU32(Payload, uint32(Mesh.Vertices.Num()));
			for (const FVector3f& V : Mesh.Vertices)
			{
				PutVec3(Payload, V);
			}
			PutU32(Payload, uint32(Mesh.Triangles.Num()));
			for (const FIntVector& T : Mesh.Triangles)
			{
				PutU32(Payload, uint32(T.X));
				PutU32(Payload, uint32(T.Y));
				PutU32(Payload, uint32(T.Z));
			}
			PutSection(Out, SectionId::Collision, Payload);
		}

		return Out;
	}
}
