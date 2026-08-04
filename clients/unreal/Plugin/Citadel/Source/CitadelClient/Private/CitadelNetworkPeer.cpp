// CitadelNetworkPeer.cpp — implementation of the NetworkPeer Phase-1 property
// table + push/shadow dirty tracking on the Unreal client.
//
// SKELETON / NOT CI-COMPILED (see the header). Verified by the manual in-editor
// step in website/src/content/docs/reference/client-sdk/networkpeer-replication.md. Keep in structural parity
// with src/realtime/netpeer/*.rs.

#include "CitadelNetworkPeer.h"
#include "CitadelClientSubsystem.h"
#include "CitadelWire.h"
#include "Engine/World.h"

// The canonical, cbindgen-generated C ABI: citadel_schema_hash +
// citadel_rep_encoder_*. Included verbatim (never re-declared).
#include "citadel_client.h"

// Enable the pre-encode audit in editor/development builds; a shipping build
// relies on the enforced accessors + mandatory shadow net (design section 3.2).
#ifndef WITH_CITADEL_REP_AUDIT
#if UE_BUILD_SHIPPING
#define WITH_CITADEL_REP_AUDIT 0
#else
#define WITH_CITADEL_REP_AUDIT 1
#endif
#endif

// --- FNV-1a fold: MUST match src/realtime/netpeer/layout.rs::fnv1a exactly -----
namespace
{
    TArray<TWeakObjectPtr<UCitadelNetworkPeer>>& BoundPeers()
    {
        static TArray<TWeakObjectPtr<UCitadelNetworkPeer>> Peers;
        return Peers;
    }

    // KIND_REP_ACK uses the shared MSB-first bit layout: one entry, object id,
    // bit-varint result token, and a zero history window.
    void WriteBits(TArray<uint8>& Out, uint64 Value, uint32 Bits, uint8& BitInByte)
    {
        while (Bits--)
        {
            if (BitInByte == 0) { Out.Add(0); }
            const uint8 Bit = static_cast<uint8>((Value >> Bits) & 1);
            Out.Last() |= Bit << (7 - BitInByte);
            BitInByte = static_cast<uint8>((BitInByte + 1) % 8);
        }
    }

    void WriteBitVarint(TArray<uint8>& Out, uint64 Value, uint8& BitInByte)
    {
        do {
            const uint64 Chunk = Value & 0x7f;
            Value >>= 7;
            // Wire groups are [continuation:1][data:7], MSB-first in the
            // bitstream; this is deliberately not byte-aligned LEB128.
            WriteBits(Out, Value != 0 ? 1 : 0, 1, BitInByte);
            WriteBits(Out, Chunk, 7, BitInByte);
        } while (Value != 0);
    }

    uint64 CollectionIdentity(const CitadelRepDecodedCollectionOp& Op)
    {
        // The wire codec has already validated this tuple; fold only for the
        // local lookup key and retain the complete tuple in the operation.
        return (static_cast<uint64>(Op.rep_index) << 32) | Op.rep_generation;
    }

    constexpr uint64 FNV_OFFSET = 0xcbf29ce484222325ULL;
    constexpr uint64 FNV_PRIME = 0x00000100000001b3ULL;

    uint64 Fnv1a(const uint8* Bytes, int32 Len)
    {
        uint64 Hash = FNV_OFFSET;
        for (int32 i = 0; i < Len; ++i)
        {
            Hash ^= static_cast<uint64>(Bytes[i]);
            Hash *= FNV_PRIME; // wrapping (unsigned overflow is defined)
        }
        return Hash;
    }

    // Append a little-endian integer to a byte buffer (matches Rust to_le_bytes).
    template <typename T>
    void AppendLE(TArray<uint8>& Out, T Value)
    {
        for (uint32 i = 0; i < sizeof(T); ++i)
        {
            Out.Add(static_cast<uint8>((Value >> (i * 8)) & 0xFF));
        }
    }

}

// Stable per-field key (FNV-1a over the UTF-8 property name). MUST match the Rust
// `stable_key_from_name`.
uint64 CitadelStableKeyFromName(const FString& Name)
{
    const FTCHARToUTF8 Utf8(*Name);
    return Fnv1a(reinterpret_cast<const uint8*>(Utf8.Get()), Utf8.Length());
}

// Full 64-bit fold of a base bounds shape with the stable key. MUST match the
// Rust `combined_bounds_shape` (16-byte LE preimage: bounds_shape || stable_key).
uint64 CitadelCombinedBoundsShape(uint64 BoundsShape, uint64 StableKey)
{
    uint8 Buf[16];
    for (uint32 i = 0; i < 8; ++i)
    {
        Buf[i] = static_cast<uint8>((BoundsShape >> (i * 8)) & 0xFF);
        Buf[8 + i] = static_cast<uint8>((StableKey >> (i * 8)) & 0xFF);
    }
    return Fnv1a(Buf, 16);
}

uint64 FCitadelFieldBounds::Shape() const
{
    if (Kind == ECitadelBoundsKind::None)
    {
        return 0; // reserved all-zero shape (matches Rust None => 0)
    }

    TArray<uint8> Payload;
    uint8 Disc = 0;
    switch (Kind)
    {
    case ECitadelBoundsKind::IntRange:
        Disc = 1;
        AppendLE(Payload, static_cast<uint64>(IntMin)); // i64 two's-complement LE
        AppendLE(Payload, static_cast<uint64>(IntMax));
        break;
    case ECitadelBoundsKind::ScalarRange:
    {
        Disc = 2;
        uint32 MinBits, MaxBits;
        FMemory::Memcpy(&MinBits, &ScalarMin, sizeof(float));
        FMemory::Memcpy(&MaxBits, &ScalarMax, sizeof(float));
        AppendLE(Payload, MinBits);
        AppendLE(Payload, MaxBits);
        AppendLE(Payload, ValuesPerUnit);
        break;
    }
    case ECitadelBoundsKind::MaxLen:
        Disc = 3;
        AppendLE(Payload, MaxLenOrItems);
        break;
    case ECitadelBoundsKind::MaxCardinality:
        Disc = 4;
        AppendLE(Payload, MaxLenOrItems);
        break;
    default:
        return 0;
    }

    const uint64 Fold = Fnv1a(Payload.GetData(), Payload.Num()) & 0x00FFFFFFFFFFFFFFULL;
    return (static_cast<uint64>(Disc) << 56) | Fold;
}

// --- Layout registration: the CPF_Net reflection walk (ONCE per UClass) --------
//
// The DOREP_CITADEL_* macros feed per-class metadata into a transient registrar
// map before/at layout build; GetOrBuild walks CPF_Net properties in reflection
// order, joins the metadata, assigns FieldId = registration index, and caches the
// immutable layout. This walk never runs per frame (design section 2.1).

namespace
{
    // Per-class Citadel metadata collected by the DOREP_CITADEL_* macros, keyed by
    // property name. Populated at module/class registration, read once at build.
    struct FFieldMeta
    {
        ECitadelFieldAuthority Authority = ECitadelFieldAuthority::ServerOnly;
        ECitadelRepCondition Cond = ECitadelRepCondition::None;
        FCitadelFieldBounds Bounds;
    };

    TMap<UClass*, TMap<FString, FFieldMeta>>& MetaRegistry()
    {
        static TMap<UClass*, TMap<FString, FFieldMeta>> Registry;
        return Registry;
    }

    TMap<UClass*, TSharedPtr<FCitadelRepLayout>>& LayoutCache()
    {
        static TMap<UClass*, TSharedPtr<FCitadelRepLayout>> Cache;
        return Cache;
    }

    // Classify a reflected property into (TypeTag, default codec_id). The codec
    // ids match crates/citadel-wire/src/codec.rs::codec_id. Value encode itself is
    // ; this only records the type/codec for the schema tuple.
    bool ClassifyProperty(FProperty* P, ECitadelFieldType& OutType, uint16& OutCodecId,
        bool& bOutPush)
    {
        bOutPush = true;
        if (P->IsA<FBoolProperty>())
        {
            OutType = ECitadelFieldType::Bool;
            OutCodecId = 1; // codec_id::BOOL
            return true;
        }
        if (P->IsA<FIntProperty>() || P->IsA<FInt64Property>() || P->IsA<FInt16Property>())
        {
            OutType = ECitadelFieldType::Int;
            OutCodecId = 2; // codec_id::SCALAR_QUANT (integer range codec, )
            return true;
        }
        if (P->IsA<FUInt32Property>() || P->IsA<FUInt64Property>() || P->IsA<FByteProperty>())
        {
            OutType = ECitadelFieldType::Uint;
            OutCodecId = 2;
            return true;
        }
        if (P->IsA<FFloatProperty>())
        {
            OutType = ECitadelFieldType::Scalar;
            OutCodecId = 2; // SCALAR_QUANT
            return true;
        }
        if (const FStructProperty* Struct = CastField<FStructProperty>(P))
        {
            if (Struct->Struct == TBaseStructure<FVector>::Get())
            {
                OutType = ECitadelFieldType::Vector3;
                OutCodecId = 3; // VECTOR3_QUANT
                return true;
            }
            if (Struct->Struct == TBaseStructure<FQuat>::Get())
            {
                OutType = ECitadelFieldType::Quat;
                OutCodecId = 5; // QUAT_SMALLEST3_10
                return true;
            }
        }
        if (P->IsA<FStrProperty>() || P->IsA<FNameProperty>())
        {
            OutType = ECitadelFieldType::Bytes;
            OutCodecId = 2;
            // Strings mutate by direct access; they cannot be auto-marked, so they
            // fall to the mandatory shadow net (design section 3.2).
            bOutPush = false;
            return true;
        }
        if (P->IsA<FArrayProperty>() || P->IsA<FSetProperty>() || P->IsA<FMapProperty>())
        {
            // Collection contents are never serialized as an opaque Unreal
            // container. QueueCollection supplies the explicit ABI-v3 item
            // descriptor and generation-keyed operations; codec id zero marks
            // this descriptor-owned field in the schema tuple.
            OutType = ECitadelFieldType::Collection;
            OutCodecId = 0;
            bOutPush = false;
            return true;
        }
        return false; // unsupported property type: not replicated by Citadel
    }

    // The ABI v3 collection descriptor must describe the reflected TArray
    // element. A zeroed descriptor is Bool, so defaulting it would corrupt any
    // non-bool keyed collection.
    bool PopulateCollectionItemCodec(const FArrayProperty* Array,
        const FCitadelFieldBounds& Bounds, CitadelRepCodecV3& Out)
    {
        if (!Array || !Array->Inner) { return false; }
        Out = {};
        if (Array->Inner->IsA<FBoolProperty>()) { Out.kind = 0; return true; }
        if (Array->Inner->IsA<FFloatProperty>())
        {
            if (Bounds.ValuesPerUnit == 0 || Bounds.ScalarMin >= Bounds.ScalarMax) { return false; }
            Out.kind = 2; Out.scalar_min = Bounds.ScalarMin; Out.scalar_max = Bounds.ScalarMax;
            Out.values_per_unit = Bounds.ValuesPerUnit;
            return true;
        }
        if (Array->Inner->IsA<FIntProperty>() || Array->Inner->IsA<FInt64Property>()
            || Array->Inner->IsA<FInt16Property>())
        {
            if (Bounds.IntMin > Bounds.IntMax) { return false; }
            Out.kind = 1; Out.int_min = Bounds.IntMin; Out.int_max = Bounds.IntMax;
            return true;
        }
        return false; // receive/apply has no typed setter for other TArray inners.
    }
}

void FCitadelRepLayoutRegistrar::Register(UClass* Class, const TCHAR* PropertyName,
    ECitadelFieldAuthority Authority, const FCitadelFieldBounds& Bounds)
{
    FFieldMeta& Meta = MetaRegistry().FindOrAdd(Class).FindOrAdd(PropertyName);
    Meta.Authority = Authority;
    Meta.Bounds = Bounds;
}

void FCitadelRepLayoutRegistrar::RegisterClampedInt(UClass* Class, const TCHAR* PropertyName,
    int64 Min, int64 Max, ECitadelFieldAuthority Authority)
{
    FFieldMeta& Meta = MetaRegistry().FindOrAdd(Class).FindOrAdd(PropertyName);
    Meta.Authority = Authority;
    Meta.Bounds.Kind = ECitadelBoundsKind::IntRange;
    Meta.Bounds.IntMin = Min;
    Meta.Bounds.IntMax = Max;
}

void FCitadelRepLayoutRegistrar::RegisterCond(UClass* Class, const TCHAR* PropertyName,
    ECitadelRepCondition Cond, ECitadelFieldAuthority Authority)
{
    FFieldMeta& Meta = MetaRegistry().FindOrAdd(Class).FindOrAdd(PropertyName);
    Meta.Authority = Authority;
    Meta.Cond = Cond;
}

const FCitadelRepLayout& FCitadelRepLayout::GetOrBuild(UClass* Class)
{
    if (const TSharedPtr<FCitadelRepLayout>* Cached = LayoutCache().Find(Class))
    {
        return *Cached->Get();
    }

    TSharedPtr<FCitadelRepLayout> Layout = MakeShared<FCitadelRepLayout>();
    Layout->ClassId = GetTypeHash(Class->GetPathName()); // stable per class path
    Layout->LayoutVersion = 1;

    const TMap<FString, FFieldMeta>* ClassMeta = MetaRegistry().Find(Class);

    // The reflection walk: once, in reflection order, over CPF_Net properties
    // only (design section 2.1). FieldId = registration index.
    uint16 NextId = 0;
    for (TFieldIterator<FProperty> It(Class, EFieldIterationFlags::IncludeSuper); It; ++It)
    {
        FProperty* P = *It;
        if ((P->PropertyFlags & CPF_Net) == 0)
        {
            continue; // not a replicated property
        }

        FCitadelFieldDesc Desc;
        if (!ClassifyProperty(P, Desc.TypeTag, Desc.CodecId, Desc.bPushBased))
        {
            continue; // unsupported type
        }
        Desc.FieldId = NextId++;
        Desc.Property = P;

        if (ClassMeta)
        {
            if (const FFieldMeta* Meta = ClassMeta->Find(P->GetName()))
            {
                Desc.Authority = Meta->Authority;
                Desc.Cond = Meta->Cond;
                if (Meta->Bounds.Kind != ECitadelBoundsKind::None)
                {
                    Desc.Bounds = Meta->Bounds;
                }
            }
        }

        // Bind the field's identity (its property name) into the schema-hash
        // bounds_shape slot, matching the Rust mirror. The name is never sent on
        // the wire; only this fold participates in the hash.
        const uint64 StableKey = CitadelStableKeyFromName(P->GetName());
        Desc.BoundsShapeSlot = CitadelCombinedBoundsShape(Desc.Bounds.Shape(), StableKey);

        Layout->Fields.Add(Desc);
    }

    // The ordered (field_id,type_tag,codec_id,cond,authority,bounds_shape) tuples
    // are now assembled identically to the Rust mirror. Compute the real BLAKE3-128
    // digest through the shared C ABI so the client's SchemaHash matches
    // the server's for the same class — this closes the Phase-1 gap where it was
    // left zeroed.
    {
        TArray<CitadelSchemaField> Tuples;
        Tuples.Reserve(Layout->Fields.Num());
        for (const FCitadelFieldDesc& F : Layout->Fields)
        {
            CitadelSchemaField T;
            T.field_id = F.FieldId;
            T.type_tag = static_cast<uint16>(F.TypeTag);
            T.codec_id = F.CodecId;
            T.cond = static_cast<uint8>(F.Cond);
            T.authority = static_cast<uint8>(F.Authority);
            T.bounds_shape = F.BoundsShapeSlot;
            Tuples.Add(T);
        }
        const CitadelStatus St = citadel_schema_hash(Layout->LayoutVersion,
            Tuples.GetData(), static_cast<uintptr_t>(Tuples.Num()), Layout->SchemaHash);
        if (St != CITADEL_STATUS_OK)
        {
            // Fail closed: leave the hash zeroed so a mismatch is detected rather
            // than silently trusting a bad layout (matches the Rust reject posture).
            FMemory::Memzero(Layout->SchemaHash, sizeof(Layout->SchemaHash));
        }
    }

    const FCitadelRepLayout& Ref = *Layout;
    LayoutCache().Add(Class, MoveTemp(Layout));
    return Ref;
}

// --- TCitadelReplicated auto-mark ---------------------------------------------
template <typename T>
void TCitadelReplicated<T>::MarkDirty()
{
    if (Owner)
    {
        Owner->MarkDirty(FieldId);
    }
}

// --- UCitadelNetworkPeer -------------------------------------------------------
void UCitadelNetworkPeer::InitializeComponent()
{
    Super::InitializeComponent();
    if (AActor* Owner = GetOwner())
    {
        CachedLayout = &FCitadelRepLayout::GetOrBuild(Owner->GetClass());
        DirtyMask.Init(false, CachedLayout->Num());
        SnapshotShadow();
        BoundPeers().AddUnique(this);
    }
}

void UCitadelNetworkPeer::OnComponentDestroyed(bool bDestroyingHierarchy)
{
    BoundPeers().RemoveAll([this](const TWeakObjectPtr<UCitadelNetworkPeer>& Peer)
    {
        return !Peer.IsValid() || Peer.Get() == this;
    });
    CollectionIndices.Reset();
    Super::OnComponentDestroyed(bDestroyingHierarchy);
}

void UCitadelNetworkPeer::SnapshotShadow()
{
    ShadowValues.Reset();
    if (!CachedLayout)
    {
        return;
    }
    ShadowValues.Reserve(CachedLayout->Num());
    for (const FCitadelFieldDesc& Field : CachedLayout->Fields)
    {
        ShadowValues.Add(ReadFieldValue(Field));
    }
}

TArray<uint8> UCitadelNetworkPeer::ReadFieldValue(const FCitadelFieldDesc& Field) const
{
    // A comparable, opaque byte snapshot of the current property value. Phase 1
    // only needs equality for change detection; the quantized wire encode is
    // . ExportText gives a stable textual form across property types.
    TArray<uint8> Out;
    const AActor* Owner = GetOwner();
    if (!Owner || !Field.Property)
    {
        return Out;
    }
    const void* ValuePtr = Field.Property->ContainerPtrToValuePtr<void>(Owner);
    FString Text;
    Field.Property->ExportTextItem_Direct(Text, ValuePtr, nullptr, nullptr, PPF_None);
    const FTCHARToUTF8 Utf8(*Text);
    Out.Append(reinterpret_cast<const uint8*>(Utf8.Get()), Utf8.Length());
    return Out;
}

void UCitadelNetworkPeer::MarkDirty(uint16 FieldId)
{
    if (CachedLayout && FieldId < CachedLayout->Num())
    {
        DirtyMask[FieldId] = true;
    }
}

bool UCitadelNetworkPeer::IsDirty(uint16 FieldId) const
{
    return CachedLayout && FieldId < CachedLayout->Num() && DirtyMask[FieldId];
}

int32 UCitadelNetworkPeer::DirtyCount() const
{
    int32 Count = 0;
    for (TConstSetBitIterator<> It(DirtyMask); It; ++It)
    {
        ++Count;
    }
    return Count;
}

void UCitadelNetworkPeer::DetectShadowChanges()
{
    if (!CachedLayout)
    {
        return;
    }
    for (int32 i = 0; i < CachedLayout->Num(); ++i)
    {
        const FCitadelFieldDesc& Field = CachedLayout->Fields[i];
        if (Field.bPushBased)
        {
            continue; // safety net covers non-push fields only
        }
        TArray<uint8> Current = ReadFieldValue(Field);
        if (Current != ShadowValues[i])
        {
            DirtyMask[i] = true;
            ShadowValues[i] = MoveTemp(Current);
        }
    }
}

bool UCitadelNetworkPeer::AuditUnmarkedChanges(TArray<uint16>& OutOffenders) const
{
    OutOffenders.Reset();
#if WITH_CITADEL_REP_AUDIT
    if (!CachedLayout)
    {
        return true;
    }
    for (int32 i = 0; i < CachedLayout->Num(); ++i)
    {
        const FCitadelFieldDesc& Field = CachedLayout->Fields[i];
        const TArray<uint8> Current = ReadFieldValue(Field);
        const bool bChanged = Current != ShadowValues[i];
        if (bChanged && !DirtyMask[i])
        {
            OutOffenders.Add(Field.FieldId);
        }
    }
#endif
    return OutOffenders.Num() == 0;
}

void UCitadelNetworkPeer::AdvanceAfterEncode()
{
    DirtyMask.Init(false, CachedLayout ? CachedLayout->Num() : 0);
    PendingCollections.Reset();
    PendingCollectionCodecs.Reset();
    SnapshotShadow();
}

bool UCitadelNetworkPeer::AcceptBaseline(uint64 ResultId)
{
    if (ResultId == 0 || ResultId <= AcceptedBaselineId)
    {
        return false; // stale acknowledgements never regress the base token
    }
    AcceptedBaselineId = ResultId;
    return true;
}

bool UCitadelNetworkPeer::QueueCollection(uint16 FieldId,
    const FCitadelCollectionCodec& Codec, const TArray<FCitadelCollectionOp>& Operations)
{
    if (!CachedLayout || FieldId >= CachedLayout->Num() || Codec.MaxItems == 0)
    {
        return false;
    }
    const FCitadelFieldDesc& Field = CachedLayout->Fields[FieldId];
    if (Field.Authority != ECitadelFieldAuthority::ClientOwned
        || Field.TypeTag != ECitadelFieldType::Collection)
    {
        return false; // ownership is enforced before data reaches the C ABI
    }
    PendingCollectionCodecs.Add(FieldId, Codec);
    PendingCollections.Add(FieldId, Operations); // deep-copy bytes and operation identities
    MarkDirty(FieldId);
    return true;
}

// --- DeltaBunch encode via the shared C ABI ------------------------

bool UCitadelNetworkPeer::AppendFieldToEncoder(CitadelRepEncoder* Encoder,
    const FCitadelFieldDesc& Field) const
{
    const AActor* Owner = GetOwner();
    if (!Owner || !Field.Property || !Encoder)
    {
        return false;
    }
    const void* ValuePtr = Field.Property->ContainerPtrToValuePtr<void>(Owner);

    switch (Field.TypeTag)
    {
    case ECitadelFieldType::Bool:
    {
        if (const FBoolProperty* P = CastField<FBoolProperty>(Field.Property))
        {
            const bool bValue = P->GetPropertyValue(ValuePtr);
            return citadel_rep_encoder_add_bool(Encoder, Field.FieldId, bValue) == CITADEL_STATUS_OK;
        }
        return false;
    }
    case ECitadelFieldType::Int:
    case ECitadelFieldType::Uint:
    case ECitadelFieldType::Enum:
    {
        // A bounded integer needs its declared range; without it the server codec
        // width is unknown, so the field is skipped (not guessed).
        if (Field.Bounds.Kind != ECitadelBoundsKind::IntRange)
        {
            return false;
        }
        int64 Value = 0;
        if (const FNumericProperty* P = CastField<FNumericProperty>(Field.Property))
        {
            Value = P->GetSignedIntPropertyValue(ValuePtr);
        }
        else
        {
            return false;
        }
        return citadel_rep_encoder_add_int(Encoder, Field.FieldId,
            Field.Bounds.IntMin, Field.Bounds.IntMax, Value) == CITADEL_STATUS_OK;
    }
    case ECitadelFieldType::Scalar:
    {
        if (Field.Bounds.Kind != ECitadelBoundsKind::ScalarRange)
        {
            return false;
        }
        float Value = 0.f;
        if (const FNumericProperty* P = CastField<FNumericProperty>(Field.Property))
        {
            Value = static_cast<float>(P->GetFloatingPointPropertyValue(ValuePtr));
        }
        else
        {
            return false;
        }
        return citadel_rep_encoder_add_scalar(Encoder, Field.FieldId,
            Field.Bounds.ScalarMin, Field.Bounds.ScalarMax, Field.Bounds.ValuesPerUnit,
            Value) == CITADEL_STATUS_OK;
    }
    case ECitadelFieldType::Vector3:
    {
        const FStructProperty* P = CastField<FStructProperty>(Field.Property);
        if (!P || P->Struct != TBaseStructure<FVector>::Get())
        {
            return false;
        }
        const FVector& Value = *static_cast<const FVector*>(ValuePtr);
        const float Components[3] = { static_cast<float>(Value.X), static_cast<float>(Value.Y), static_cast<float>(Value.Z) };
        const float Bounds = Field.Bounds.Kind == ECitadelBoundsKind::ScalarRange
            ? Field.Bounds.ScalarMax : 0.f;
        return citadel_rep_encoder_add_vector3(Encoder, Field.FieldId, Bounds, Components)
            == CITADEL_STATUS_OK;
    }
    case ECitadelFieldType::Quat:
    {
        const FStructProperty* P = CastField<FStructProperty>(Field.Property);
        if (!P || P->Struct != TBaseStructure<FQuat>::Get())
        {
            return false;
        }
        const FQuat& Value = *static_cast<const FQuat*>(ValuePtr);
        const float Components[4] = { Value.X, Value.Y, Value.Z, Value.W };
        const uint32 Bits = Field.CodecId == 4 ? 9u : Field.CodecId == 6 ? 15u : 10u;
        return citadel_rep_encoder_add_quat(Encoder, Field.FieldId, Bits, Components)
            == CITADEL_STATUS_OK;
    }
    case ECitadelFieldType::Bytes:
    {
        FString Text;
        if (const FStrProperty* P = CastField<FStrProperty>(Field.Property))
        {
            Text = P->GetPropertyValue(ValuePtr);
        }
        else if (const FNameProperty* NP = CastField<FNameProperty>(Field.Property))
        {
            Text = NP->GetPropertyValue(ValuePtr).ToString();
        }
        else
        {
            return false;
        }
        const uint32 MaxLen = (Field.Bounds.Kind == ECitadelBoundsKind::MaxLen)
            ? Field.Bounds.MaxLenOrItems : 0xFFFFu;
        const FTCHARToUTF8 Utf8(*Text);
        return citadel_rep_encoder_add_bytes(Encoder, Field.FieldId, MaxLen,
            reinterpret_cast<const uint8*>(Utf8.Get()), Utf8.Length()) == CITADEL_STATUS_OK;
    }
    default:
        // Vector3 / Quat / Collection are not encoded on the client fast path here.
        return false;
    }
}

TArray<uint8> UCitadelNetworkPeer::BuildDeltaBunch(bool bIsFull, uint64 ResultId, uint64 BaseId) const
{
    TArray<uint8> Out;
    if (!CachedLayout)
    {
        return Out;
    }

    // Baseline ownership/control is local and fail-closed: a full cannot carry
    // a base, and a delta must be rooted at the last server-accepted token.
    if (ResultId == 0 || (bIsFull && BaseId != 0)
        || (!bIsFull && (BaseId == 0 || BaseId != AcceptedBaselineId)))
    {
        return Out;
    }

    const uint32 BoundId = BoundObjectId();
    if (BoundId == 0)
    {
        return Out; // object_id=0 is never a valid actor binding.
    }
    CitadelRepEncoder* Encoder = citadel_rep_encoder_new(
        BoundId, bIsFull, ResultId, BaseId,
        static_cast<uintptr_t>(CachedLayout->Num()));
    if (!Encoder)
    {
        return Out; // invalid args (zero result_id, or non-full with zero base_id)
    }

    bool bOk = true;
    if (bIsFull)
    {
        // Embed the real, C-ABI-computed SchemaHash so the server can gate decode
        // on a matching layout (closes the Phase-1 zeroed-hash gap).
        bOk = citadel_rep_encoder_set_schema(Encoder, CachedLayout->SchemaHash,
            CachedLayout->LayoutVersion) == CITADEL_STATUS_OK;
    }

    if (bOk)
    {
        for (int32 i = 0; i < CachedLayout->Num(); ++i)
        {
            const FCitadelFieldDesc& Field = CachedLayout->Fields[i];
            // Only CLIENT-OWNED fields may be proposed upstream (design section 7.2);
            // ServerOnly fields are never encoded by the client.
            if (Field.Authority != ECitadelFieldAuthority::ClientOwned)
            {
                continue;
            }
            // On a delta, include only currently-dirty fields; on a full snapshot,
            // include every client-owned scalar field.
            if (!bIsFull && !(i < DirtyMask.Num() && DirtyMask[i]))
            {
                continue;
            }
            if (const TArray<FCitadelCollectionOp>* Operations = PendingCollections.Find(Field.FieldId))
            {
                const FCitadelCollectionCodec* Codec = PendingCollectionCodecs.Find(Field.FieldId);
                if (!Codec)
                {
                    bOk = false;
                    break;
                }
                TArray<CitadelRepCollectionOp> NativeOps;
                NativeOps.Reserve(Operations->Num());
                for (const FCitadelCollectionOp& Op : *Operations)
                {
                    CitadelRepCollectionOp Native{};
                    Native.op = Op.Op;
                    Native.value_kind = Op.ValueKind;
                    Native.rep_index = Op.RepIndex;
                    Native.rep_generation = Op.RepGeneration;
                    Native.rep_key = Op.RepKey;
                    Native.int_value = Op.IntValue;
                    FMemory::Memcpy(Native.floats, Op.Floats, sizeof(Native.floats));
                    Native.bytes = Op.Bytes.Num() ? Op.Bytes.GetData() : nullptr;
                    Native.bytes_len = static_cast<uintptr_t>(Op.Bytes.Num());
                    NativeOps.Add(Native);
                }
                CitadelRepCodecV3 Item{};
                Item.kind = Codec->ItemKind;
                Item.int_min = Codec->IntMin;
                Item.int_max = Codec->IntMax;
                Item.scalar_min = Codec->ScalarMin;
                Item.scalar_max = Codec->ScalarMax;
                Item.values_per_unit = Codec->ValuesPerUnit;
                Item.max_len = Codec->MaxLen;
                Item.vector_bounds = Codec->VectorBounds;
                Item.quat_bits = Codec->QuatBits;
                bOk = citadel_rep_encoder_add_collection(Encoder, Field.FieldId, Item,
                    Codec->MaxItems, NativeOps.GetData(), static_cast<uintptr_t>(NativeOps.Num()))
                    == CITADEL_STATUS_OK;
            }
            else
            {
                bOk = AppendFieldToEncoder(Encoder, Field);
            }
            if (!bOk)
            {
                break; // whole-bunch failure; never silently omit a dirty field
            }
        }

        // First pass sizes the buffer, second pass fills it (the encoder reports the
        // needed length via out_truncated).
        uintptr_t Needed = 0;
        bool bTruncated = false;
        const CitadelStatus SizeSt = citadel_rep_encoder_finish(Encoder, nullptr, 0, &Needed, &bTruncated);
        if (SizeSt == CITADEL_STATUS_OK && Needed > 0)
        {
            Out.SetNumUninitialized(static_cast<int32>(Needed));
            uintptr_t Written = 0;
            const CitadelStatus St = citadel_rep_encoder_finish(Encoder, Out.GetData(),
                static_cast<uintptr_t>(Out.Num()), &Written, &bTruncated);
            if (St != CITADEL_STATUS_OK || bTruncated)
            {
                Out.Reset();
            }
        }
    }

    citadel_rep_encoder_free(Encoder);
    return Out;
}

void UCitadelNetworkPeer::RouteRepDelta(UGameInstance* GameInstance, const TArray<uint8>& Body)
{
    // Header parsing and schema validation happen in ReceiveDeltaBunch.  Route
    // only to a component in this game instance; a matching numeric id in a
    // stale world is never an object identity match.
    for (const TWeakObjectPtr<UCitadelNetworkPeer>& Peer : BoundPeers())
    {
        if (Peer.IsValid() && Peer->GetWorld()
            && Peer->GetWorld()->GetGameInstance() == GameInstance && Peer->ReceiveDeltaBunch(Body))
        {
            return;
        }
    }
}

bool UCitadelNetworkPeer::ReceiveDeltaBunch(const TArray<uint8>& Body)
{
    if (!CachedLayout || BoundObjectId() == 0 || Body.Num() == 0) { return false; }
    TArray<CitadelRepDecodeFieldCodecV3> Codecs;
    Codecs.SetNumZeroed(CachedLayout->Num());
    for (const FCitadelFieldDesc& Field : CachedLayout->Fields)
    {
        CitadelRepDecodeFieldCodecV3& Codec = Codecs[Field.FieldId];
        Codec.codec.kind = static_cast<uint8>(Field.TypeTag) - 1;
        Codec.codec.int_min = Field.Bounds.IntMin; Codec.codec.int_max = Field.Bounds.IntMax;
        Codec.codec.scalar_min = Field.Bounds.ScalarMin; Codec.codec.scalar_max = Field.Bounds.ScalarMax;
        Codec.codec.values_per_unit = Field.Bounds.ValuesPerUnit; Codec.codec.max_len = Field.Bounds.MaxLenOrItems;
        Codec.codec.vector_bounds = Field.Bounds.ScalarMax;
        Codec.codec.quat_bits = Field.CodecId == 4 ? 9u : Field.CodecId == 6 ? 15u : 10u;
        Codec.is_collection = Field.TypeTag == ECitadelFieldType::Collection;
        if (Codec.is_collection)
        {
            const FArrayProperty* Array = CastField<FArrayProperty>(Field.Property);
            if (!PopulateCollectionItemCodec(Array, Field.Bounds, Codec.collection_item_codec)
                || Field.Bounds.MaxLenOrItems == 0)
            {
                return false; // never guess/default a keyed collection item codec.
            }
            Codec.collection_max_items = Field.Bounds.MaxLenOrItems;
        }
    }
    CitadelRepDecoded* Decoded = nullptr;
    if (citadel_rep_decode_with_collections(Body.GetData(), static_cast<uintptr_t>(Body.Num()),
        CachedLayout->SchemaHash, CachedLayout->LayoutVersion, Codecs.GetData(),
        static_cast<uintptr_t>(Codecs.Num()), &Decoded) != CITADEL_STATUS_OK || !Decoded) { return false; }
    uint32 Object = 0; bool bFull = false; uint64 Result = 0, Base = 0;
    const bool bHeader = citadel_rep_decoded_header(Decoded, &Object, &bFull, &Result, &Base) == CITADEL_STATUS_OK;
    const bool bStale = Result == 0 || (!bFull && (bReceiveNeedsFullRecovery || Base != AcceptedBaselineId));
    const bool bIdentity = Object == BoundObjectId();
    const bool bApplied = bHeader && bIdentity && !bStale && ApplyDecodedFields(Decoded, bFull);
    citadel_rep_decoded_free(Decoded);
    if (!bApplied) { if (bHeader && bIdentity && !bFull) { RequestFullRecovery(); } return false; }
    AcceptedBaselineId = Result;
    bReceiveNeedsFullRecovery = false;
    SendRepAck(Result);
    return true;
}

bool UCitadelNetworkPeer::ValidateDecodedFields(const CitadelRepDecoded* Decoded, bool bIsFull) const
{
    if (!Decoded || !CachedLayout || !GetOwner()) { return false; }
    const uintptr_t Count = citadel_rep_decoded_field_count(Decoded);
    TSet<uint16> FullCollectionFields;
    for (uintptr_t Changed = 0; Changed < Count; ++Changed)
    {
        uint16 CollectionFieldId = 0;
        if (citadel_rep_decoded_collection_field_id(Decoded, Changed, &CollectionFieldId) == CITADEL_STATUS_OK)
        {
            if (!ValidateCollectionField(Decoded, Changed, CollectionFieldId, bIsFull)) { return false; }
            FullCollectionFields.Add(CollectionFieldId);
            continue;
        }
        CitadelRepFieldValue Value{};
        if (citadel_rep_decoded_field_at(Decoded, Changed, &Value) != CITADEL_STATUS_OK
            || Value.field_id >= CachedLayout->Num()) { return false; }
        const FProperty* Property = CachedLayout->Fields[Value.field_id].Property;
        if ((CastField<FBoolProperty>(Property) && Value.kind == 0)
            || (CastField<FFloatProperty>(Property) && Value.kind == 2)
            || (CastField<FNumericProperty>(Property) && Value.kind == 1)) { continue; }
        return false;
    }
    // A full snapshot is authoritative for every schema keyed collection. Do
    // not clear local state from a legacy/defective full that omitted one.
    if (bIsFull)
    {
        for (const FCitadelFieldDesc& Field : CachedLayout->Fields)
        {
            if (Field.TypeTag == ECitadelFieldType::Collection && !FullCollectionFields.Contains(Field.FieldId))
            {
                return false;
            }
        }
    }
    return true;
}

bool UCitadelNetworkPeer::ValidateCollectionField(const CitadelRepDecoded* Decoded,
    uintptr_t ChangedIndex, uint16 SourceFieldId, bool bIsFull) const
{
    if (SourceFieldId >= CachedLayout->Num()) { return false; }
    const FCitadelFieldDesc& Field = CachedLayout->Fields[SourceFieldId];
    const FArrayProperty* Array = CastField<FArrayProperty>(Field.Property);
    if (!Array || Field.TypeTag != ECitadelFieldType::Collection) { return false; }
    FScriptArrayHelper Values(Array, Array->ContainerPtrToValuePtr<void>(GetOwner()));
    TMap<uint64, int32> Indices = bIsFull ? TMap<uint64, int32>() : CollectionIndices.FindRef(SourceFieldId);
    int32 SimulatedNum = bIsFull ? 0 : Values.Num();
    uintptr_t OpCount = 0;
    if (citadel_rep_decoded_collection_count(Decoded, ChangedIndex, &OpCount) != CITADEL_STATUS_OK) { return false; }
    for (uintptr_t OpIndex = 0; OpIndex < OpCount; ++OpIndex)
    {
        CitadelRepDecodedCollectionOp Op{};
        if (citadel_rep_decoded_collection_at(Decoded, ChangedIndex, OpIndex, &Op) != CITADEL_STATUS_OK
            || Op.op > 2) { return false; }
        const uint64 Key = CollectionIdentity(Op);
        if (Op.op == 0)
        {
            const int32* Existing = Indices.Find(Key);
            if (!Existing) { return false; }
            const int32 Removed = *Existing;
            Indices.Remove(Key);
            --SimulatedNum;
            for (TPair<uint64, int32>& Pair : Indices) { if (Pair.Value > Removed) { --Pair.Value; } }
            continue;
        }
        const bool bSupported = (CastField<FBoolProperty>(Array->Inner) && Op.value_kind == 0)
            || (CastField<FFloatProperty>(Array->Inner) && Op.value_kind == 2)
            || (CastField<FNumericProperty>(Array->Inner) && Op.value_kind == 1);
        if (!bSupported) { return false; }
        if (!Indices.Contains(Key)) { Indices.Add(Key, SimulatedNum++); }
    }
    return true;
}

bool UCitadelNetworkPeer::ApplyDecodedFields(const CitadelRepDecoded* Decoded, bool bIsFull)
{
    // Validate all later fields before touching the actor. This prevents a bad
    // trailing delta from leaving an earlier scalar/collection change behind.
    if (!ValidateDecodedFields(Decoded, bIsFull)) { return false; }

    struct FPropertySnapshot { FProperty* Property = nullptr; TArray<uint8> Value; };
    TArray<FPropertySnapshot> Snapshots;
    Snapshots.Reserve(CachedLayout->Num());
    for (const FCitadelFieldDesc& Field : CachedLayout->Fields)
    {
        if (!Field.Property) { continue; }
        FPropertySnapshot& Snapshot = Snapshots.AddDefaulted_GetRef();
        Snapshot.Property = Field.Property;
        Snapshot.Value.SetNumUninitialized(Field.Property->GetSize());
        Field.Property->InitializeValue(Snapshot.Value.GetData());
        Field.Property->CopyCompleteValue(Snapshot.Value.GetData(),
            Field.Property->ContainerPtrToValuePtr<void>(GetOwner()));
    }
    const TMap<uint16, TMap<uint64, int32>> OriginalCollectionIndices = CollectionIndices;
    const auto Restore = [&]()
    {
        for (FPropertySnapshot& Snapshot : Snapshots)
        {
            Snapshot.Property->CopyCompleteValue(
                Snapshot.Property->ContainerPtrToValuePtr<void>(GetOwner()), Snapshot.Value.GetData());
        }
        CollectionIndices = OriginalCollectionIndices;
    };
    const auto DestroySnapshots = [&]()
    {
        for (FPropertySnapshot& Snapshot : Snapshots) { Snapshot.Property->DestroyValue(Snapshot.Value.GetData()); }
    };

    // Server fulls are encoded as diff(empty, current). Reset every reflected
    // keyed collection before adding those authoritative entries, including a
    // zero-op collection delta. Incremental deltas retain their keyed state.
    if (bIsFull)
    {
        for (const FCitadelFieldDesc& Field : CachedLayout->Fields)
        {
            if (Field.TypeTag != ECitadelFieldType::Collection) { continue; }
            const FArrayProperty* Array = CastField<FArrayProperty>(Field.Property);
            if (!Array) { Restore(); DestroySnapshots(); return false; }
            FScriptArrayHelper Values(Array, Array->ContainerPtrToValuePtr<void>(GetOwner()));
            Values.EmptyValues();
        }
        CollectionIndices.Reset();
    }

    const uintptr_t Count = citadel_rep_decoded_field_count(Decoded);
    for (uintptr_t Changed = 0; Changed < Count; ++Changed)
    {
        uint16 CollectionFieldId = 0;
        if (citadel_rep_decoded_collection_field_id(Decoded, Changed, &CollectionFieldId) == CITADEL_STATUS_OK)
        {
            if (!ApplyCollectionField(Decoded, Changed, CollectionFieldId)) { Restore(); DestroySnapshots(); return false; }
            continue;
        }
        CitadelRepFieldValue Value{};
        if (citadel_rep_decoded_field_at(Decoded, Changed, &Value) != CITADEL_STATUS_OK
            || Value.field_id >= CachedLayout->Num()) { Restore(); DestroySnapshots(); return false; }
        const FCitadelFieldDesc& Field = CachedLayout->Fields[Value.field_id];
        void* Ptr = Field.Property->ContainerPtrToValuePtr<void>(GetOwner());
        if (const FBoolProperty* P = CastField<FBoolProperty>(Field.Property)) { P->SetPropertyValue(Ptr, Value.bool_value); }
        else if (const FFloatProperty* P = CastField<FFloatProperty>(Field.Property)) { P->SetPropertyValue(Ptr, Value.scalar_value); }
        else if (const FNumericProperty* P = CastField<FNumericProperty>(Field.Property)) { P->SetIntPropertyValue(Ptr, Value.int_value); }
        else { Restore(); DestroySnapshots(); return false; }
    }
    DestroySnapshots();
    return true;
}

bool UCitadelNetworkPeer::ApplyCollectionField(const CitadelRepDecoded* Decoded, uintptr_t ChangedIndex, uint16 SourceFieldId)
{
    if (!ValidateCollectionField(Decoded, ChangedIndex, SourceFieldId, false)) { return false; }
    const FCitadelFieldDesc& Field = CachedLayout->Fields[SourceFieldId];
    const FArrayProperty* Array = CastField<FArrayProperty>(Field.Property);
    FScriptArrayHelper Values(Array, Array->ContainerPtrToValuePtr<void>(GetOwner()));
    TMap<uint64, int32>& Indices = CollectionIndices.FindOrAdd(SourceFieldId);
    uintptr_t OpCount = 0;
    if (citadel_rep_decoded_collection_count(Decoded, ChangedIndex, &OpCount) != CITADEL_STATUS_OK) { return false; }
    for (uintptr_t OpIndex = 0; OpIndex < OpCount; ++OpIndex)
    {
        CitadelRepDecodedCollectionOp Op{};
        if (citadel_rep_decoded_collection_at(Decoded, ChangedIndex, OpIndex, &Op) != CITADEL_STATUS_OK) { return false; }
        const uint64 Key = CollectionIdentity(Op);
        if (Op.op == 0) { const int32 Removed = *Indices.Find(Key); Values.RemoveValues(Removed, 1); Indices.Remove(Key); for (TPair<uint64, int32>& Pair : Indices) if (Pair.Value > Removed) --Pair.Value; continue; }
        int32* Existing = Indices.Find(Key);
        const int32 Slot = Existing ? *Existing : Values.AddValue();
        if (FBoolProperty* P = CastField<FBoolProperty>(Array->Inner)) { P->SetPropertyValue(Values.GetRawPtr(Slot), Op.int_value != 0); }
        else if (FFloatProperty* P = CastField<FFloatProperty>(Array->Inner)) { P->SetPropertyValue(Values.GetRawPtr(Slot), Op.floats[0]); }
        else if (FNumericProperty* P = CastField<FNumericProperty>(Array->Inner)) { P->SetIntPropertyValue(Values.GetRawPtr(Slot), Op.int_value); }
        else { return false; }
        Indices.Add(Key, Slot);
    }
    return true;
}

void UCitadelNetworkPeer::SendRepAck(uint64 ResultId)
{
    UWorld* World = GetWorld();
    UGameInstance* GI = World ? World->GetGameInstance() : nullptr;
    if (!GI || ResultId == 0) { return; }
    UCitadelClientSubsystem* Client = GI->GetSubsystem<UCitadelClientSubsystem>(); if (!Client) { return; }
    TArray<uint8> Body; uint8 Bit = 0;
    WriteBitVarint(Body, 1, Bit); WriteBits(Body, BoundObjectId(), 32, Bit);
    WriteBitVarint(Body, ResultId, Bit); WriteBits(Body, 0, 32, Bit);
    Client->Send(CitadelWire::KIND_REP_ACK, Body, /*bReliable=*/true);
}

void UCitadelNetworkPeer::RequestFullRecovery()
{
    // KIND_REP_DELTA/ACK/SCHEMA define no client->server full-recovery request.
    // This is therefore receive-local fail-closed state, not an undocumented
    // resend request: reject without mutation or ACK and wait for the server's
    // existing full-baseline/timeout policy. A prompt request requires a new
    // protocol kind plus gateway handling before it can be claimed.
    AcceptedBaselineId = 0;
    bReceiveNeedsFullRecovery = true;
}
