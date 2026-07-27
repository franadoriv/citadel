// CitadelNetworkPeer.h — Unreal client half of NetworkPeer property replication,
// Phase 1: the property table + push/shadow dirty tracking.
//
// This mirrors the Rust `citadel::realtime::netpeer` module field-for-field so
// both ends derive the same layout identity. Design:
// docs/architecture/network-peer-property-replication.md sections 2-3.
//
// SKELETON / NOT CI-COMPILED. Unreal C++ (UObject reflection) cannot be built in
// Citadel CI, so this file is verified by the MANUAL in-editor step documented in
// docs/features/network-peer-replication.md ("Manual Unreal verification"). Keep
// it in exact structural parity with the Rust mirror; the schema_hash / wire
// encode of a DeltaBunch is .
//
// The single biggest performance rule (design section 2.1): the CPF_Net
// reflection walk runs ONCE at class registration and is cached per UClass. It
// must NEVER run per frame per actor.
#pragma once

#include "CoreMinimal.h"
#include "Components/ActorComponent.h"
#include "UObject/UnrealType.h" // TFieldIterator<FProperty>, CPF_Net

#include "CitadelNetworkPeer.generated.h"

// --- Enum parity with the Rust mirror (src/realtime/netpeer/layout.rs) ---------
// The numeric values are stable schema/contract identities and MUST match the
// Rust `TypeTag` / `RepCondition` / `FieldAuthority` discriminants exactly, since
// they feed the shared schema_hash preimage.

/** Field type discriminant; matches Rust `TypeTag`. */
UENUM()
enum class ECitadelFieldType : uint16
{
    Bool = 1,
    Int = 2,
    Uint = 3,
    Scalar = 4,
    Vector3 = 5,
    Quat = 6,
    Bytes = 7,
    Enum = 8,
};

/** Replication condition (COND_* analogue); matches Rust `RepCondition`. */
UENUM()
enum class ECitadelRepCondition : uint8
{
    None = 0,
    OwnerOnly = 1,
    SkipOwner = 2,
    InitialOnly = 3,
    SimulatedOnly = 4,
    AutonomousOnly = 5,
    Custom = 6,
    Never = 7,
};

/** Field authority; matches Rust `FieldAuthority`. Default ServerOnly. */
UENUM()
enum class ECitadelFieldAuthority : uint8
{
    ServerOnly = 0,
    ClientOwned = 1,
};

/** Bounds discriminant; matches Rust `FieldBounds` (the top byte of `shape`). */
UENUM()
enum class ECitadelBoundsKind : uint8
{
    None = 0,
    IntRange = 1,
    ScalarRange = 2,
    MaxLen = 3,
    MaxCardinality = 4,
};

/**
 * The server-side validation envelope for a field. Phase 1 only needs its
 * `Shape` (the fixed-width fold that feeds the schema hash); enforcement is
 * server-side.
 */
struct FCitadelFieldBounds
{
    ECitadelBoundsKind Kind = ECitadelBoundsKind::None;
    int64 IntMin = 0;
    int64 IntMax = 0;
    float ScalarMin = 0.f;
    float ScalarMax = 0.f;
    uint32 ValuesPerUnit = 0;
    uint32 MaxLenOrItems = 0;

    /**
     * The canonical `bounds_shape`: discriminant in the top byte, a 56-bit FNV-1a
     * fold of the little-endian parameters in the low bits. MUST reproduce
     * `FieldBounds::shape` in the Rust mirror bit-for-bit (see
     * CitadelNetworkPeer.cpp). `None` is the reserved all-zero shape.
     */
    uint64 Shape() const;
};

/**
 * One replicated field's descriptor. Immutable after registration. Mirrors the
 * Rust `FieldDesc`; `FieldId` is the registration-order index.
 */
struct FCitadelFieldDesc
{
    uint16 FieldId = 0;
    ECitadelFieldType TypeTag = ECitadelFieldType::Bool;
    uint16 CodecId = 0; // citadel_wire codec_id; encode lands in
    ECitadelRepCondition Cond = ECitadelRepCondition::None;
    ECitadelFieldAuthority Authority = ECitadelFieldAuthority::ServerOnly;
    FCitadelFieldBounds Bounds;
    bool bPushBased = true;

    // The value placed in the schema-hash `bounds_shape` slot: the full 64-bit
    // fold of Bounds.Shape with this field's stable name key (see
    // CitadelNetworkPeer.cpp CombinedBoundsShape). Binds field identity into the
    // hash so a same-shaped reorder still changes it (matches the Rust
    // `combined_bounds_shape`). Computed once at registration.
    uint64 BoundsShapeSlot = 0;

    // The reflected property this descriptor maps to (resolved once at
    // registration; never dereferenced via reflection per frame for the value —
    // the auto-marking accessor / shadow buffer own the value path).
    FProperty* Property = nullptr;
};

// Stable per-field key from a property name (FNV-1a over UTF-8). Never sent on
// the wire; folded into the schema-hash bounds_shape slot. MUST match the Rust
// `stable_key_from_name`.
uint64 CitadelStableKeyFromName(const FString& Name);

// The full 64-bit fold of a base bounds shape with a stable key. MUST match the
// Rust `combined_bounds_shape`.
uint64 CitadelCombinedBoundsShape(uint64 BoundsShape, uint64 StableKey);

/**
 * The immutable, per-UClass replicated-field table (the UE `FRepLayout`
 * analogue). Built ONCE by walking CPF_Net reflected properties at registration
 * and cached in a per-UClass map (see FCitadelRepLayout::GetOrBuild). Mirrors the
 * Rust `RepLayout`.
 */
struct FCitadelRepLayout
{
    uint32 ClassId = 0;
    uint32 LayoutVersion = 1;
    TArray<FCitadelFieldDesc> Fields; // ordered; Fields[i].FieldId == i

    // 128-bit wide canonical schema hash over the ordered tuples. Computed at
    // registration through the shared C ABI (`citadel_schema_hash`, ) so
    // it matches the server's digest for the same class; a full-snapshot
    // DeltaBunch embeds it and the server gates decode on a match. Left zeroed
    // (fail-closed) only if the C ABI rejects the layout.
    uint8 SchemaHash[16] = {0};

    /**
     * Return the cached layout for `Class`, building it once on first use via the
     * CPF_Net reflection walk (design section 2.1). Thread-safe; the walk never
     * runs per frame.
     */
    static const FCitadelRepLayout& GetOrBuild(UClass* Class);

    int32 Num() const { return Fields.Num(); }
};

/**
 * DOREPLIFETIME analogue macros: declare a replicated field's codec/bounds/
 * authority next to the property so the schema and validation rules travel
 * together (design section 2.3). These register the field into the reflection
 * pass; they do not themselves send anything.
 *
 * NOTE: the property must still carry UPROPERTY(Replicated ...) so CPF_Net is
 * set; these macros attach the Citadel codec/bounds/authority metadata.
 */
#define DOREP_CITADEL(ClassName, PropertyName)                                    \
    FCitadelRepLayoutRegistrar::Register(ClassName::StaticClass(), TEXT(#PropertyName), \
        ECitadelFieldAuthority::ServerOnly, FCitadelFieldBounds{})

#define DOREP_CITADEL_CLAMPED(ClassName, PropertyName, MinV, MaxV, AuthorityV)    \
    FCitadelRepLayoutRegistrar::RegisterClampedInt(ClassName::StaticClass(),      \
        TEXT(#PropertyName), (MinV), (MaxV), (AuthorityV))

#define DOREP_CITADEL_COND(ClassName, PropertyName, CondV, AuthorityV)            \
    FCitadelRepLayoutRegistrar::RegisterCond(ClassName::StaticClass(),            \
        TEXT(#PropertyName), (CondV), (AuthorityV))

#define DOREP_CITADEL_CLIENTOWNED(ClassName, PropertyName)                        \
    FCitadelRepLayoutRegistrar::Register(ClassName::StaticClass(), TEXT(#PropertyName), \
        ECitadelFieldAuthority::ClientOwned, FCitadelFieldBounds{})

/** Registration-time collector the DOREP_CITADEL_* macros feed. */
struct FCitadelRepLayoutRegistrar
{
    static void Register(UClass* Class, const TCHAR* PropertyName,
        ECitadelFieldAuthority Authority, const FCitadelFieldBounds& Bounds);
    static void RegisterClampedInt(UClass* Class, const TCHAR* PropertyName,
        int64 Min, int64 Max, ECitadelFieldAuthority Authority);
    static void RegisterCond(UClass* Class, const TCHAR* PropertyName,
        ECitadelRepCondition Cond, ECitadelFieldAuthority Authority);
};

/**
 * Auto-marking replicated accessor (design section 3.1). The raw value is
 * PRIVATE and the ONLY mutator is operator=, which marks the owning peer's field
 * dirty. This makes "forgot to mark dirty" structurally impossible for push
 * fields — a write cannot bypass the mark. Fields that cannot be wrapped this way
 * (direct member access, nested/collection mutation) MUST be declared
 * non-push-based and fall to the mandatory shadow net.
 */
template <typename T>
class TCitadelReplicated
{
public:
    TCitadelReplicated() = default;
    explicit TCitadelReplicated(const T& InValue) : Value(InValue) {}

    // Bind to the owning peer + this field's id once, at construction/registration.
    void Bind(class UCitadelNetworkPeer* InOwner, uint16 InFieldId)
    {
        Owner = InOwner;
        FieldId = InFieldId;
    }

    // Read-only access; never mutates, never marks.
    const T& Get() const { return Value; }
    operator const T&() const { return Value; }

    // The single mutation path: assigning marks the field dirty.
    TCitadelReplicated& operator=(const T& NewValue)
    {
        if (!(Value == NewValue))
        {
            Value = NewValue;
            MarkDirty();
        }
        return *this;
    }

private:
    void MarkDirty(); // defined in the .cpp (needs UCitadelNetworkPeer)

    T Value{};
    UCitadelNetworkPeer* Owner = nullptr;
    uint16 FieldId = 0;
};

/**
 * The opt-in component you add to an actor to auto-sync its replication-flagged
 * properties to Citadel. Phase 1 provides the change-detection layer only:
 * dirty mask + shadow net + the dev pre-encode audit. It does NOT yet build or
 * send a DeltaBunch.
 */
UCLASS(ClassGroup = (Citadel), meta = (BlueprintSpawnableComponent))
class UCitadelNetworkPeer : public UActorComponent
{
    GENERATED_BODY()

public:
    // Resolve + cache the owner's layout (once) at registration.
    virtual void InitializeComponent() override;

    /** Push-model mark (design section 3.1). O(1). */
    void MarkDirty(uint16 FieldId);

    /** Whether a field is currently marked dirty. */
    bool IsDirty(uint16 FieldId) const;

    /** Number of dirty fields. */
    int32 DirtyCount() const;

    /**
     * Shadow-diff safety net (design section 3.2): for every NON-push field,
     * compare the actor's current value against the shadow; on a difference set
     * the dirty bit and advance the shadow. Mandatory for non-enforceable fields.
     * Run once per net tick BEFORE the audit and encode. O(non-push fields).
     */
    void DetectShadowChanges();

    /**
     * Dev/CI pre-encode audit (design section 3.2). Over ALL registered fields,
     * any field whose current value differs from the shadow but has no dirty bit
     * is a bug (a write bypassed both the accessor and the net). Returns false and
     * fills OutOffenders; the caller MUST treat this as a HARD failure before
     * encode (checkf / test failure), never a warning. Compiled only in dev/CI
     * (see WITH_CITADEL_REP_AUDIT).
     */
    bool AuditUnmarkedChanges(TArray<uint16>& OutOffenders) const;

    /** Per-tick reset after the delta is encoded ( drives this). */
    void AdvanceAfterEncode();

    const FCitadelRepLayout* Layout() const { return CachedLayout; }

    /**
     * Encode a client->server DeltaBunch of this actor's changed, CLIENT-OWNED
     * scalar fields through the shared C ABI encoder, so the bits are
     * byte-identical to the server's `citadel_wire::netpeer` encoder — the client
     * never reimplements the BitWriter or codecs.
     *
     * `bIsFull` selects a full snapshot (embeds the real SchemaHash computed via
     * the C ABI, closing the Phase-1 zeroed-hash gap); `ResultId` is the nonzero
     * token this bunch establishes; `BaseId` is the token it is diffed against
     * (ignored / must be 0 for a full snapshot). On a delta (`!bIsFull`) only
     * currently-dirty fields are included; on a full snapshot every client-owned
     * scalar field is included. Returns the encoded bytes to send under
     * `CitadelWire::KIND_REP_DELTA`, or an empty array on failure.
     *
     * Collections are not encoded from the client here (client-owned collections
     * are rare); the server retains full keyed-collection support. See docs.
     */
    TArray<uint8> BuildDeltaBunch(bool bIsFull, uint64 ResultId, uint64 BaseId) const;

private:
    // Snapshot every registered field's current value into ShadowValues.
    void SnapshotShadow();
    // Read one registered field's current value as an opaque comparable blob.
    TArray<uint8> ReadFieldValue(const FCitadelFieldDesc& Field) const;
    // Add one field's current value to the C ABI encoder (returns false on an
    // unsupported/unbounded field, which is skipped).
    bool AppendFieldToEncoder(struct CitadelRepEncoder* Encoder,
        const FCitadelFieldDesc& Field) const;

    const FCitadelRepLayout* CachedLayout = nullptr; // built once, per UClass
    TBitArray<> DirtyMask;                            // one bit per field_id
    TArray<TArray<uint8>> ShadowValues;               // registered fields only
};
