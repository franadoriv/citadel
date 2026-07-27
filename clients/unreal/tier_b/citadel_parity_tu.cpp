// Tier-B compile-against-header parity translation unit for the Citadel Unreal
// SDK (docs/architecture/client-sdk-sync.md section 2 / task ).
//
// This is the ONLY automatic native-signature check across the three engines.
// It is deliberately Unreal-free so it compiles on any bare C++ compiler in CI
// (or skips cleanly when none is present — see parity-hook.sh). It:
//
//   1. `#include`s the canonical, cbindgen-generated `citadel_client.h` and the
//      SDK's `CitadelWire.h` (which also includes the header).
//   2. Binds every exported C ABI function to a function pointer of the exact
//      signature this SDK expects. If a parameter or return type changes in the
//      header, these initializations FAIL to compile — exactly the drift Tier-B
//      exists to catch.
//   3. `static_assert`s the SDK's declared ABI version and a few enum values
//      against the header.
//
// It is compiled with `-c` / `/c` (object only, no link), so the native library
// does not need to be built or present.
//
// NOTE: this TU does NOT include the UE subsystem (CitadelClientSubsystem.h)
// because that pulls in the Unreal engine headers, which are not available in
// CI. The subsystem is still header-driven and calls these same prototypes
// directly, so its in-UE compile enforces the same parity; here we reproduce the
// binding standalone so CI can verify it without the engine.

#include "citadel_client.h"
#include "CitadelWire.h"

namespace
{
    // (1) Signature parity. A changed signature in citadel_client.h breaks the
    // corresponding initialization below (incompatible function-pointer types).
    uint32_t (*const kAbiVersion)(void) = &citadel_client_abi_version;

    CitadelStatus (*const kConnectQuic)(const char*, const char*, bool, CitadelClient**) =
        &citadel_client_connect_quic;

    CitadelStatus (*const kConnectWebSocket)(const char*, CitadelClient**) =
        &citadel_client_connect_websocket;

    CitadelStatus (*const kAuthenticate)(
        CitadelClient*, const uint8_t*, uintptr_t, CitadelAuthStatus*, char*, uintptr_t,
        uintptr_t*, uint8_t*) = &citadel_client_authenticate;

    CitadelStatus (*const kSend)(CitadelClient*, uint16_t, const uint8_t*, uintptr_t, bool) =
        &citadel_client_send;

    CitadelStatus (*const kPoll)(CitadelClient*, uint16_t*, uint8_t*, uintptr_t, uintptr_t*, bool*) =
        &citadel_client_poll;

    uintptr_t (*const kLastError)(CitadelClient*, char*, uintptr_t) =
        &citadel_client_last_error;

    void (*const kFree)(CitadelClient*) = &citadel_client_free;

    // NetworkPeer schema_hash + DeltaBunch encoder C ABI.
    CitadelStatus (*const kSchemaHash)(uint32_t, const CitadelSchemaField*, uintptr_t, uint8_t*) =
        &citadel_schema_hash;
    CitadelRepEncoder* (*const kRepNew)(uint32_t, bool, uint64_t, uint64_t, uintptr_t) =
        &citadel_rep_encoder_new;
    CitadelStatus (*const kRepSetSchema)(CitadelRepEncoder*, const uint8_t*, uint32_t) =
        &citadel_rep_encoder_set_schema;
    CitadelStatus (*const kRepAddBool)(CitadelRepEncoder*, uint16_t, bool) =
        &citadel_rep_encoder_add_bool;
    CitadelStatus (*const kRepAddInt)(CitadelRepEncoder*, uint16_t, int64_t, int64_t, int64_t) =
        &citadel_rep_encoder_add_int;
    CitadelStatus (*const kRepAddScalar)(CitadelRepEncoder*, uint16_t, float, float, uint32_t, float) =
        &citadel_rep_encoder_add_scalar;
    CitadelStatus (*const kRepAddBytes)(CitadelRepEncoder*, uint16_t, uint32_t, const uint8_t*, uintptr_t) =
        &citadel_rep_encoder_add_bytes;
    CitadelStatus (*const kRepFinish)(CitadelRepEncoder*, uint8_t*, uintptr_t, uintptr_t*, bool*) =
        &citadel_rep_encoder_finish;
    void (*const kRepFree)(CitadelRepEncoder*) = &citadel_rep_encoder_free;

    // Silence "unused variable" diagnostics without needing -Wno flags: touch
    // every pointer. `reinterpret_cast` to a common type keeps this a pure
    // compile-time reference (no call, no link dependency).
    const void* const kBindings[] = {
        reinterpret_cast<const void*>(kAbiVersion),
        reinterpret_cast<const void*>(kConnectQuic),
        reinterpret_cast<const void*>(kConnectWebSocket),
        reinterpret_cast<const void*>(kAuthenticate),
        reinterpret_cast<const void*>(kSend),
        reinterpret_cast<const void*>(kPoll),
        reinterpret_cast<const void*>(kLastError),
        reinterpret_cast<const void*>(kFree),
        reinterpret_cast<const void*>(kSchemaHash),
        reinterpret_cast<const void*>(kRepNew),
        reinterpret_cast<const void*>(kRepSetSchema),
        reinterpret_cast<const void*>(kRepAddBool),
        reinterpret_cast<const void*>(kRepAddInt),
        reinterpret_cast<const void*>(kRepAddScalar),
        reinterpret_cast<const void*>(kRepAddBytes),
        reinterpret_cast<const void*>(kRepFinish),
        reinterpret_cast<const void*>(kRepFree),
    };
    const void* const* const kBindingsRef = kBindings;
}

// (2) ABI version parity against the header #define (also asserted inside
// CitadelWire.h; repeated here to keep the guarantee self-evident in the TU).
static_assert(CitadelWire::ABI_VERSION == CITADEL_FFI_ABI_VERSION,
              "Tier-B: Citadel Unreal SDK ABI version drifted from citadel_client.h");

// (3) Spot-check the status enum values the SDK maps onto ECitadelStatus.
static_assert(CITADEL_STATUS_OK == 0, "Tier-B: CITADEL_STATUS_OK changed");
static_assert(CITADEL_STATUS_DISCONNECTED == 2, "Tier-B: CITADEL_STATUS_DISCONNECTED changed");

// A definition so the TU is a complete object even if a compiler wants one; the
// hook compiles with -c so this is never linked.
extern "C" int citadel_unreal_tier_b_parity_marker(void)
{
    return static_cast<int>(reinterpret_cast<uintptr_t>(kBindingsRef) & 1);
}
