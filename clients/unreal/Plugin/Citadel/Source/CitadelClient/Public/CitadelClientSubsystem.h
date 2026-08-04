// CitadelClientSubsystem.h — thin, UE-idiomatic wrapper over the Citadel C ABI.
//
// Design: Unreal is
// header-driven. This wrapper `#include`s the canonical, cbindgen-generated
// `citadel_client.h` VERBATIM and calls its functions directly. It NEVER
// re-declares the C prototypes — the compiler binding against the real header is
// the drift check (Tier-B). Only the ergonomic UE surface (a subsystem, UE
// types, lifetime management) is hand-written here.
//
// Blueprint: the connection surface is exposed to Blueprint so a
// designer can connect, authenticate, and drive the gameplay components
// (`UCitadelTransformSync`, `UCitadelNetworkPeer`) with no C++. Connect/send/poll
// go over the native C ABI; authenticate is an HTTP request to the node's
// `/v1/auth/device|custom` route (the C ABI carries no auth) that yields a
// session token, exposed via async Blueprint delegates. The original C++ methods
// are unchanged; the Blueprint entry points are thin wrappers.
#pragma once

#include "CoreMinimal.h"
#include "Subsystems/GameInstanceSubsystem.h"
#include "Interfaces/IHttpRequest.h"

// Canonical C ABI, included verbatim. Do not re-declare these prototypes.
#include "citadel_client.h"

// Wire-protocol constants (envelope kinds / RPC statuses / byte counts) that
// live in citadel-wire rather than the C header.
#include "CitadelWire.h"

#include "CitadelClientSubsystem.generated.h"

/** Outcome of a Citadel client call, mirroring CitadelStatus for Blueprint use. */
UENUM(BlueprintType)
enum class ECitadelStatus : uint8
{
    Ok = 0,
    Again = 1,
    Disconnected = 2,
    InvalidArgument = 3,
    Connect = 4,
    Send = 5,
    Receive = 6,
    Internal = 7,
};

/** Realtime auth handshake outcome, mirroring AUTH_STATUS_* / CitadelAuthStatus. */
UENUM(BlueprintType)
enum class ECitadelRealtimeAuthStatus : uint8
{
    Authenticated = 0,
    Guest = 1,
    Rejected = 2,
};

/**
 * Fired when an authenticate call succeeds. `SessionToken` is the access token to
 * present to authenticated routes; `UserId`/`Username` identify the account.
 */
DECLARE_DYNAMIC_MULTICAST_DELEGATE_ThreeParams(
    FCitadelAuthSucceeded,
    const FString&, SessionToken,
    const FString&, UserId,
    const FString&, Username);

/** Fired when an authenticate call fails (network, non-2xx, or malformed body). */
DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(
    FCitadelAuthFailed,
    const FString&, ErrorMessage);

/**
 * Fired for each reliable `KIND_NOTIFICATION` envelope. `NotificationJson` is
 * the persisted notification object encoded as UTF-8 JSON. Deliveries are
 * at-least-once; consumers should deduplicate by `id` and reconcile with
 * `notifications.list` after reconnecting.
 */
DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(
    FCitadelNotificationReceived,
    const FString&, NotificationJson);

/** Privacy-preserving player profile returned by the player lifecycle API. */
USTRUCT(BlueprintType)
struct FCitadelPublicProfile
{
    GENERATED_BODY()

    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Player") FString UserId;
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Player") FString Username;
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Player") FString DisplayName;
};

/** Replacement credentials returned by refresh. Store both values atomically. */
USTRUCT(BlueprintType)
struct FCitadelSessionTokenPair
{
    GENERATED_BODY()

    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Player") FString Token;
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Player") FString RefreshToken;
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Player") FString UserId;
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Player") FString Username;
};

DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(FCitadelProfileReceived, const FCitadelPublicProfile&, Profile);
DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(FCitadelUsersLookupReceived, const TArray<FCitadelPublicProfile>&, Users);
DECLARE_DYNAMIC_MULTICAST_DELEGATE_OneParam(FCitadelSessionRefreshed, const FCitadelSessionTokenPair&, Tokens);
DECLARE_DYNAMIC_MULTICAST_DELEGATE_ThreeParams(FCitadelPlayerRequestFailed, int32, StatusCode, const FString&, Code, const FString&, Message);

/**
 * Minimal UE GameInstance subsystem that owns a single native CitadelClient
 * handle and forwards to the C ABI. Header-driven: every native call below is a
 * direct call into `citadel_client.h`, so a C ABI signature change breaks this
 * translation unit's compile inside UE (and the Tier-B TU in CI).
 *
 * The connection surface (connect QUIC/WS, disconnect, status, last-error) and
 * the device/custom authenticate flow are `BlueprintCallable`, so the plugin is
 * usable no-code. `Send`/`Poll` stay C++-only for now: their `uint16` envelope
 * kind is not a Blueprint-representable type, and designers drive traffic through
 * the higher-level components rather than raw envelopes.
 */
UCLASS()
class UCitadelClientSubsystem : public UGameInstanceSubsystem
{
    GENERATED_BODY()

public:
    /** Connect over QUIC. `bInsecure` selects the dev self-signed TLS path. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Connection")
    ECitadelStatus ConnectQuic(const FString& Address, const FString& ServerName, bool bInsecure);

    /** Connect over WebSocket (e.g. `ws://127.0.0.1:7352/`). */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Connection")
    ECitadelStatus ConnectWebSocket(const FString& Url);

    /**
     * Close the connection and free the native handle. Safe to call when not
     * connected (no-op). After this, `IsConnected` is false.
     */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Connection")
    void Disconnect();

    /** True while a native client handle is held (post-connect, pre-disconnect). */
    UFUNCTION(BlueprintPure, Category = "Citadel|Connection")
    bool IsConnected() const;

    /** The status of the most recent connect/send/poll call. */
    UFUNCTION(BlueprintPure, Category = "Citadel|Connection")
    ECitadelStatus GetLastStatus() const;

    /** The last native error message for the current handle, or empty. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Connection")
    FString GetLastError();

    /**
     * Authenticate (or register) with a device id against the node's
     * `POST {BaseUrl}/v1/auth/device`. `BaseUrl` is the HTTP origin, e.g.
     * `http://127.0.0.1:7350`. On success, `SessionToken` is set and
     * `OnAuthenticated` fires; on failure `OnAuthenticationFailed` fires. Async.
     */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Connection")
    void AuthenticateDevice(const FString& BaseUrl, const FString& DeviceId, bool bCreate, const FString& Username);

    /** As `AuthenticateDevice`, using a custom id against `/v1/auth/custom`. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Connection")
    void AuthenticateCustom(const FString& BaseUrl, const FString& CustomId, bool bCreate, const FString& Username);

    /** Asynchronously register or sign in with `/v1/auth/email`. Password is
     * sent only in this HTTPS-capable HTTP request and is never retained by the
     * subsystem after serialization. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Connection")
    void AuthenticateEmail(const FString& BaseUrl, const FString& Email, const FString& Password, bool bCreate, const FString& Username);

    /**
     * Perform the realtime auth handshake as an explicit guest. Call immediately
     * after ConnectQuic/ConnectWebSocket and before sending gameplay envelopes.
     */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Connection")
    ECitadelStatus AuthenticateRealtimeGuest(
        ECitadelRealtimeAuthStatus& OutAuthStatus,
        FString& OutUserId,
        uint8& OutReason);

    /**
     * Perform the realtime auth handshake with a session token obtained from
     * AuthenticateDevice/AuthenticateCustom or another identity flow.
     */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Connection")
    ECitadelStatus AuthenticateRealtimeWithSessionToken(
        const FString& Token,
        ECitadelRealtimeAuthStatus& OutAuthStatus,
        FString& OutUserId,
        uint8& OutReason);

    /** Broadcast when an authenticate call succeeds. */
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Connection")
    FCitadelAuthSucceeded OnAuthenticated;

    /** Broadcast when an authenticate call fails. */
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Connection")
    FCitadelAuthFailed OnAuthenticationFailed;

    /** Broadcast when the server delivers a persisted player notification. */
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Notifications")
    FCitadelNotificationReceived OnNotificationReceived;

    /** The session (access) token from the last successful authenticate, or empty. */
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Connection")
    FString SessionToken;

    /** The account id from the last successful authenticate, or empty. */
    UPROPERTY(BlueprintReadOnly, Category = "Citadel|Connection")
    FString UserId;

    /** Fetch the authenticated player's sanitized public profile. Async. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Player")
    void GetAccount(const FString& BaseUrl, const FString& AccessToken);

    /** Update username and/or display name. Set bClearDisplayName to send null. Async. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Player")
    void UpdateAccount(const FString& BaseUrl, const FString& AccessToken, const FString& Username, const FString& DisplayName, bool bClearDisplayName);

    /** Exact known-player lookup; this is never a public directory search. Async. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Player")
    void LookupUsers(const FString& BaseUrl, const FString& AccessToken, const TArray<FString>& UserIds, const TArray<FString>& Usernames);

    /** Rotate a refresh secret. This intentionally sends no bearer header. Async. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Player")
    void RefreshSession(const FString& BaseUrl, const FString& RefreshToken);

    /** Revoke one session. Supplying no credentials is rejected by the server. Async. */
    UFUNCTION(BlueprintCallable, Category = "Citadel|Player")
    void LogoutSession(const FString& BaseUrl, const FString& AccessToken, const FString& RefreshToken);

    UPROPERTY(BlueprintAssignable, Category = "Citadel|Player") FCitadelProfileReceived OnAccountReceived;
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Player") FCitadelProfileReceived OnAccountUpdated;
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Player") FCitadelUsersLookupReceived OnUsersLookupReceived;
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Player") FCitadelSessionRefreshed OnSessionRefreshed;
    UPROPERTY(BlueprintAssignable, Category = "Citadel|Player") FCitadelPlayerRequestFailed OnPlayerRequestFailed;

    /** Send an envelope of `Kind` with `Payload` bytes. (C++ API.) */
    ECitadelStatus Send(uint16 Kind, const TArray<uint8>& Payload, bool bReliable);

    /**
     * Poll the next inbound envelope (non-blocking). On Ok, fills `OutKind` and
     * `OutPayload`. Returns Again when nothing is ready, Disconnected when the
     * connection closed and the queue is drained. (C++ API.)
     */
    ECitadelStatus Poll(uint16& OutKind, TArray<uint8>& OutPayload);

    /** The last native error message for the current handle, or empty. (C++ API.) */
    FString LastError();

    // USubsystem lifecycle: free the native handle on teardown.
    virtual void Deinitialize() override;

private:
    // Shared device/custom auth: POST {BaseUrl}{Path} with the id-based body and
    // route the JSON response into the delegates.
    void Authenticate(const FString& BaseUrl, const FString& Path, const FString& Id, bool bCreate, const FString& Username);

    // HTTP completion handler for an authenticate request.
    void OnAuthResponse(FHttpRequestPtr Request, FHttpResponsePtr Response, bool bConnectedOk);

    enum class EPlayerRequest : uint8 { GetAccount, UpdateAccount, LookupUsers, Refresh, Logout };
    void StartPlayerRequest(const FString& BaseUrl, const FString& Path, const FString& Verb, const FString& AccessToken, const FString& JsonBody, EPlayerRequest Kind);
    void OnPlayerResponse(FHttpRequestPtr Request, FHttpResponsePtr Response, bool bConnectedOk, EPlayerRequest Kind);
    static bool ParseProfile(const TSharedPtr<FJsonObject>& Json, FCitadelPublicProfile& OutProfile);

    // The opaque native handle from the C ABI. Owned by this subsystem.
    CitadelClient* Handle = nullptr;

    // Status of the most recent connect/send/poll call.
    ECitadelStatus LastStatus = ECitadelStatus::Disconnected;
};
