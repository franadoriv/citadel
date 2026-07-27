// CitadelNetworkPeer.cpp — implementation of the NetworkPeer Phase-1 property
// table + push/shadow dirty tracking on the Unreal client.
//
// SKELETON / NOT CI-COMPILED (see the header). Verified by the manual in-editor
// step in docs/features/network-peer-replication.md. Keep in structural parity
// with src/realtime/netpeer/*.rs.

#include "CitadelNetworkPeer.h"

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
            OutType = ECitadelFieldType::Bytes; // keyed-collection delta is
            OutCodecId = 2;
            bOutPush = false;
            return true;
        }
        return false; // unsupported property type: not replicated by Citadel
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
    }
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
    SnapshotShadow();
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

    CitadelRepEncoder* Encoder = citadel_rep_encoder_new(
        /*object_id*/ 0, bIsFull, ResultId, BaseId,
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
            AppendFieldToEncoder(Encoder, Field);
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
