// CitadelWire.h — canonical Citadel wire/ABI constants for the Unreal C++ SDK.
//
// This is the ONE file Tier-A parity parses for this engine. It declares each
// wire/ABI constant as `constexpr <type> NAME = N;` so
// `scripts/check_sdk_parity.py` (the `cpp` format) can diff the literals against
// `crates/citadel-wire/contract.json`. Change a value here and Tier-A fails.
//
// The Unreal SDK is HEADER-DRIVEN (see crates/citadel-wire/contract.json
// section 3): it `#include`s the canonical, cbindgen-generated
// `citadel_client.h` verbatim and NEVER re-declares the C ABI prototypes. These
// constants are the wire-protocol layer (envelope kinds, RPC statuses, body byte
// counts) that lives in Rust (`citadel-wire::protocol`) rather than in the C
// header, plus the ABI version, which we cross-check against the header below.
//
// Endianness (documented, NOT auto-checked — see Tier-B limits): position floats
// are little-endian; the relayed sender id and all RPC integer fields are
// big-endian. Marshaling correctness is covered by per-SDK tests + the manual
// in-editor smoke test, not by this header.
#pragma once

#include <array>
#include <utility>
#include <vector>

// Canonical C ABI header (cbindgen-generated). Included verbatim so this SDK
// tracks the exact C surface; also provides CITADEL_FFI_ABI_VERSION for the
// compile-time ABI cross-check below.
#include "citadel_client.h"

// Unreal's platform headers define the sized-integer aliases (uint8/uint16/
// uint32) globally. When CitadelWire.h is compiled OUTSIDE Unreal — e.g. by the
// Tier-B parity translation unit on a bare compiler — fall back to <cstdint> so
// the same source compiles standalone. A UE .Build.cs should define
// CITADEL_WITH_UNREAL to use Unreal's own type aliases instead.
#if !defined(CITADEL_WITH_UNREAL)
#include <cstdint>
#include <cstddef>
using uint8 = std::uint8_t;
using uint16 = std::uint16_t;
using uint32 = std::uint32_t;
using uint64 = std::uint64_t;
#endif

namespace CitadelWire
{
    // --- Envelope kinds (u16 on the wire) ---
    // Client -> server: "my position" (body: two little-endian f32 x, y).
    constexpr uint16 KIND_POSITION = 1;
    // Server -> client: a relayed peer position (body: 8-byte big-endian sender
    // id + the two-f32 position payload).
    constexpr uint16 KIND_PEER_POSITION = 2;
    // Client -> server: invoke a server-side RPC (request/response).
    constexpr uint16 KIND_RPC_REQUEST = 3;
    // Server -> client: the correlated reply to a KIND_RPC_REQUEST.
    constexpr uint16 KIND_RPC_RESPONSE = 4;

    // --- Auth handshake (; bodies in citadel_wire::protocol) ---
    // Client -> server: the auth handshake. MUST be the first frame on a new
    // connection. Body: the session token bytes, or empty for an explicit guest.
    constexpr uint16 KIND_AUTH = 5;
    // Server -> client: the reply to a KIND_AUTH handshake. Body: a status byte
    // (AUTH_STATUS_*) plus, on the authenticated path, the resolved user_id (utf8).
    constexpr uint16 KIND_AUTH_RESULT = 6;

    // --- Transform-sync kinds (; bodies in citadel_wire::tsync) ---
    // Client<->server: negotiate world bounds / precision / rates (reliable).
    constexpr uint16 KIND_TSYNC_HELLO = 7;
    // Server -> client: per-client delta snapshot (unreliable datagram hot path).
    constexpr uint16 KIND_TSYNC_SNAPSHOT = 8;
    // Client -> server: owner input bundle (redundant frames + fire, unreliable;
    //  P2). Carries a piggybacked snapshot ack + last-seen-id hint.
    constexpr uint16 KIND_TSYNC_INPUT = 9;
    // Client -> server: snapshot ack (absolute id + 32-bit bitfield, unreliable).
    constexpr uint16 KIND_TSYNC_ACK = 10;
    // Server -> client: ownership/role/relevancy transition (reliable, idempotent).
    constexpr uint16 KIND_TSYNC_ROLE = 11;
    // Server -> client: authoritative lag-compensated hit result for a fire that
    // rode a KIND_TSYNC_INPUT bundle (reliable;  P2). The client never
    // resolves hits itself.
    constexpr uint16 KIND_TSYNC_REWIND = 12;
    // Dedicated negotiated v2 clock metadata kinds; v1 bodies stay unchanged.
    constexpr uint16 KIND_TSYNC_V2_HELLO = 29;
    constexpr uint16 KIND_TSYNC_V2_SNAPSHOT = 30;
    constexpr uint16 KIND_TSYNC_V2_INPUT = 31;

    // --- NetworkPeer replication kinds (; bodies in citadel_wire::netpeer) ---
    // Client<->server: property DeltaBunch (reliable by default). Bit-packed:
    // object_id + is_full + result_id + base_id + schema_hash(full) + changed_mask
    // + length-delimited values + keyed collection add/remove/change.
    constexpr uint16 KIND_REP_DELTA = 13;
    // Client<->server: baseline ack ([(object_id, result_id, history)...]).
    constexpr uint16 KIND_REP_ACK = 14;
    // Server -> client: schema table (class_id -> schema_hash) on join.
    constexpr uint16 KIND_REP_SCHEMA = 15;

    // --- Networked-Actors kinds (; bodies in citadel_wire::na) ---
    // The out-of-the-box presence + replicated-spawn layer above transform-sync.
    // Client -> server: announce this client's avatar {archetype_id, transform}.
    constexpr uint16 KIND_NA_PRESENCE = 16;
    // Server -> client: spawn one networked actor {object_id, archetype_id, owner,
    // transform} (reliable).
    constexpr uint16 KIND_NA_SPAWN = 17;
    // Server -> client: batch spawn — every actor already present, sent to a newly
    // joined client (reliable).
    constexpr uint16 KIND_NA_SPAWN_BATCH = 18;
    // Server -> client: despawn the actor bound to {object_id} (reliable).
    constexpr uint16 KIND_NA_DESPAWN = 19;
    // Client -> server: the owner's relay transform report {object_id, transform}
    // (unreliable hot path).
    constexpr uint16 KIND_NA_STATE = 20;

    // --- Rooms (match/lobby membership + map load), kinds 21-25 (reliable) ---
    // Client -> server: create a room; {params} bytes the game's on_room_create Lua
    // hook interprets (Citadel sends the desired map name).
    constexpr uint16 KIND_ROOM_CREATE = 21;
    // Client -> server: join an existing room {room_id}.
    constexpr uint16 KIND_ROOM_JOIN = 22;
    // Server -> client: you are in the room {room_id, map, mode} — load this map.
    constexpr uint16 KIND_ROOM_JOINED = 23;
    // Client -> server request / server -> client notify: leave/removed {room_id}.
    constexpr uint16 KIND_ROOM_LEAVE = 24;
    // Client -> server: the room's map/level is now open {room_id}.
    constexpr uint16 KIND_ROOM_MAP_READY = 25;
    // Server -> client: JSON matchmaker handoff {ticket_id, match_id, join_token, expires_at}.
    constexpr uint16 KIND_MATCHMAKER_MATCHED = 26;
    // Server -> client: a durable player-notification live delivery. The body is
    // UTF-8 JSON for the persisted notification. Delivery is at-least-once: use
    // the notification id to deduplicate and notifications.list to reconcile.
    constexpr uint16 KIND_NOTIFICATION = 27;
    // Server -> client: authorized chat presence or durable mutation event (UTF-8 JSON).
    constexpr uint16 KIND_CHAT_EVENT = 28;
    // Server -> client reliable authoritative input-stream lease control.
    constexpr uint16 KIND_INPUT_STREAM_CONTROL = 40;
    constexpr uint8 INPUT_STREAM_CONTROL_VERSION = 1;
    constexpr uint8 INPUT_STREAM_CONTROL_ADVERTISE = 1;
    constexpr uint8 INPUT_STREAM_CONTROL_REVOKE = 2;
    constexpr uint32 INPUT_STREAM_TOKEN_BYTES = 16;
    // Client -> server canonical stream-bound custom input (legacy generic input is unchanged).
    constexpr uint16 KIND_AUTHORITATIVE_INPUT = 41;
    constexpr uint8 AUTHORITATIVE_INPUT_VERSION = 1;
    constexpr uint16 KIND_CAPABILITY_OFFER = 42;
    constexpr uint16 KIND_CAPABILITY_ACCEPTANCE = 43;
    constexpr uint8 CAPABILITY_NEGOTIATION_VERSION = 1;
    constexpr uint8 CAPABILITY_AUTHORITATIVE_INPUT = 1;
    constexpr uint32 CAPABILITY_CHALLENGE_BYTES = 16;
    constexpr uint32 MAX_SEQUENCED_INPUT_BODY_BYTES = 64 * 1024;

    inline bool DecodeCapabilityOffer(const uint8* Body, uint32 Length, std::array<uint8, CAPABILITY_CHALLENGE_BYTES>& Challenge)
    {
        if (Body == nullptr || Length != 2 + CAPABILITY_CHALLENGE_BYTES
            || Body[0] != CAPABILITY_NEGOTIATION_VERSION || Body[1] != CAPABILITY_AUTHORITATIVE_INPUT) return false;
        bool Nonzero = false;
        for (uint32 I = 0; I < CAPABILITY_CHALLENGE_BYTES; ++I) { Challenge[I] = Body[2 + I]; Nonzero |= Challenge[I] != 0; }
        return Nonzero;
    }

    inline bool EncodeCapabilityAcceptance(const uint8* Offer, uint32 Length, std::vector<uint8>& Out)
    {
        std::array<uint8, CAPABILITY_CHALLENGE_BYTES> Challenge{};
        if (!DecodeCapabilityOffer(Offer, Length, Challenge)) return false;
        Out.assign(Offer, Offer + Length); return true;
    }

    // Exact typed, non-owning view of a server-issued input-stream control body.
    struct InputStreamControlView
    {
        uint8 Opcode = 0;
        uint64 MatchId = 0;
        uint64 StreamId = 0;
        const uint8* Token = nullptr; // exactly INPUT_STREAM_TOKEN_BYTES for Advertise only
    };

    inline bool DecodeInputStreamControl(const uint8* Body, uint32 Length, InputStreamControlView& Out)
    {
        if (Body == nullptr || Length < 18 || Body[0] != INPUT_STREAM_CONTROL_VERSION) return false;
        const auto ReadBe64 = [](const uint8* P) { uint64 V = 0; for (uint32 I = 0; I < 8; ++I) V = (V << 8) | P[I]; return V; };
        Out.Opcode = Body[1]; Out.MatchId = ReadBe64(Body + 2); Out.StreamId = ReadBe64(Body + 10); Out.Token = nullptr;
        if (Out.Opcode == INPUT_STREAM_CONTROL_REVOKE) return Length == 18;
        if (Out.Opcode != INPUT_STREAM_CONTROL_ADVERTISE || Length != 18 + INPUT_STREAM_TOKEN_BYTES) return false;
        bool Nonzero = false; for (uint32 I = 0; I < INPUT_STREAM_TOKEN_BYTES; ++I) Nonzero |= Body[18 + I] != 0;
        if (!Nonzero) return false; Out.Token = Body + 18; return true;
    }

    // Canonical error classification for local authoritative input/receipt codec validation.
    // No error includes bearer material or opaque payload bytes.
    enum class EAuthoritativeInputCodecError : uint8
    {
        None,
        NullBody,
        Truncated,
        UnsupportedVersion,
        AllZeroStreamToken,
        ZeroSequence,
        BodyTooLarge,
        TrailingBytes,
        InvalidDisposition,
        InvalidCorrectionPresence,
    };

    inline uint16 ReadBe16(const uint8* Data)
    {
        return (uint16(Data[0]) << 8) | uint16(Data[1]);
    }

    inline uint32 ReadBe32(const uint8* Data)
    {
        uint32 Value = 0;
        for (uint32 Index = 0; Index < 4; ++Index) Value = (Value << 8) | Data[Index];
        return Value;
    }

    inline uint64 ReadBe64(const uint8* Data)
    {
        uint64 Value = 0;
        for (uint32 Index = 0; Index < 8; ++Index) Value = (Value << 8) | Data[Index];
        return Value;
    }

    inline void AppendBe16(std::vector<uint8>& Out, uint16 Value)
    {
        Out.push_back(uint8(Value >> 8));
        Out.push_back(uint8(Value));
    }

    inline void AppendBe32(std::vector<uint8>& Out, uint32 Value)
    {
        for (int Shift = 24; Shift >= 0; Shift -= 8) Out.push_back(uint8(Value >> Shift));
    }

    inline void AppendBe64(std::vector<uint8>& Out, uint64 Value)
    {
        for (int Shift = 56; Shift >= 0; Shift -= 8) Out.push_back(uint8(Value >> Shift));
    }

    inline bool IsNonzeroStreamToken(const std::array<uint8, INPUT_STREAM_TOKEN_BYTES>& Token)
    {
        for (uint8 Byte : Token) if (Byte != 0) return true;
        return false;
    }

    /// Canonical `SequencedInput` body. Match and stream identities are not
    /// client fields: Gateway derives them from the server-owned token lease.
    struct FSequencedInput
    {
        std::array<uint8, INPUT_STREAM_TOKEN_BYTES> StreamToken{};
        uint64 Sequence = 0;
        uint16 OriginalCustomKind = 0;
        std::vector<uint8> Body;

        static constexpr size_t PrefixBytes = 1 + INPUT_STREAM_TOKEN_BYTES + 8 + 2 + 4;

        bool Encode(std::vector<uint8>& Out, EAuthoritativeInputCodecError& OutError) const
        {
            Out.clear();
            if (!IsNonzeroStreamToken(StreamToken)) { OutError = EAuthoritativeInputCodecError::AllZeroStreamToken; return false; }
            if (Sequence == 0) { OutError = EAuthoritativeInputCodecError::ZeroSequence; return false; }
            if (Body.size() > MAX_SEQUENCED_INPUT_BODY_BYTES) { OutError = EAuthoritativeInputCodecError::BodyTooLarge; return false; }
            Out.reserve(PrefixBytes + Body.size());
            Out.push_back(AUTHORITATIVE_INPUT_VERSION);
            Out.insert(Out.end(), StreamToken.begin(), StreamToken.end());
            AppendBe64(Out, Sequence);
            AppendBe16(Out, OriginalCustomKind);
            AppendBe32(Out, static_cast<uint32>(Body.size()));
            Out.insert(Out.end(), Body.begin(), Body.end());
            OutError = EAuthoritativeInputCodecError::None;
            return true;
        }

        static bool Decode(const uint8* Data, size_t Length, FSequencedInput& Out,
            EAuthoritativeInputCodecError& OutError)
        {
            if (Data == nullptr) { OutError = EAuthoritativeInputCodecError::NullBody; return false; }
            if (Length < PrefixBytes) { OutError = EAuthoritativeInputCodecError::Truncated; return false; }
            if (Data[0] != AUTHORITATIVE_INPUT_VERSION) { OutError = EAuthoritativeInputCodecError::UnsupportedVersion; return false; }
            FSequencedInput Candidate;
            for (uint32 Index = 0; Index < INPUT_STREAM_TOKEN_BYTES; ++Index) Candidate.StreamToken[Index] = Data[1 + Index];
            if (!IsNonzeroStreamToken(Candidate.StreamToken)) { OutError = EAuthoritativeInputCodecError::AllZeroStreamToken; return false; }
            Candidate.Sequence = ReadBe64(Data + 1 + INPUT_STREAM_TOKEN_BYTES);
            if (Candidate.Sequence == 0) { OutError = EAuthoritativeInputCodecError::ZeroSequence; return false; }
            Candidate.OriginalCustomKind = ReadBe16(Data + 1 + INPUT_STREAM_TOKEN_BYTES + 8);
            const uint32 BodyLength = ReadBe32(Data + 1 + INPUT_STREAM_TOKEN_BYTES + 8 + 2);
            if (BodyLength > MAX_SEQUENCED_INPUT_BODY_BYTES) { OutError = EAuthoritativeInputCodecError::BodyTooLarge; return false; }
            const size_t Expected = PrefixBytes + static_cast<size_t>(BodyLength);
            if (Length < Expected) { OutError = EAuthoritativeInputCodecError::Truncated; return false; }
            if (Length > Expected) { OutError = EAuthoritativeInputCodecError::TrailingBytes; return false; }
            Candidate.Body.assign(Data + PrefixBytes, Data + Expected);
            Out = std::move(Candidate);
            OutError = EAuthoritativeInputCodecError::None;
            return true;
        }
    };

    /// Canonical `InputReceipt` body. Its `(MatchId, StreamId, StreamToken,
    /// DecidedSequence)` tuple is the server-owned receipt correlation key.
    struct FInputReceipt
    {
        uint64 MatchId = 0;
        uint64 StreamId = 0;
        std::array<uint8, INPUT_STREAM_TOKEN_BYTES> StreamToken{};
        uint64 AcknowledgedSequence = 0;
        uint64 DecidedSequence = 0;
        bool bAccepted = false;
        uint64 AuthoritativeTick = 0;
        bool bCorrectionPresent = false;
        std::vector<uint8> Correction;

        static constexpr size_t PrefixBytes = 1 + 8 + 8 + INPUT_STREAM_TOKEN_BYTES + 8 + 8 + 1 + 8 + 1 + 4;

        bool Encode(std::vector<uint8>& Out, EAuthoritativeInputCodecError& OutError) const
        {
            Out.clear();
            if (!IsNonzeroStreamToken(StreamToken)) { OutError = EAuthoritativeInputCodecError::AllZeroStreamToken; return false; }
            if (DecidedSequence == 0) { OutError = EAuthoritativeInputCodecError::ZeroSequence; return false; }
            if (!bCorrectionPresent && !Correction.empty()) { OutError = EAuthoritativeInputCodecError::InvalidCorrectionPresence; return false; }
            if (Correction.size() > MAX_SEQUENCED_INPUT_BODY_BYTES) { OutError = EAuthoritativeInputCodecError::BodyTooLarge; return false; }
            Out.reserve(PrefixBytes + Correction.size());
            Out.push_back(AUTHORITATIVE_INPUT_VERSION);
            AppendBe64(Out, MatchId);
            AppendBe64(Out, StreamId);
            Out.insert(Out.end(), StreamToken.begin(), StreamToken.end());
            AppendBe64(Out, AcknowledgedSequence);
            AppendBe64(Out, DecidedSequence);
            Out.push_back(bAccepted ? 0 : 1);
            AppendBe64(Out, AuthoritativeTick);
            Out.push_back(bCorrectionPresent ? 1 : 0);
            AppendBe32(Out, static_cast<uint32>(Correction.size()));
            Out.insert(Out.end(), Correction.begin(), Correction.end());
            OutError = EAuthoritativeInputCodecError::None;
            return true;
        }

        static bool Decode(const uint8* Data, size_t Length, FInputReceipt& Out,
            EAuthoritativeInputCodecError& OutError)
        {
            if (Data == nullptr) { OutError = EAuthoritativeInputCodecError::NullBody; return false; }
            if (Length < PrefixBytes) { OutError = EAuthoritativeInputCodecError::Truncated; return false; }
            if (Data[0] != AUTHORITATIVE_INPUT_VERSION) { OutError = EAuthoritativeInputCodecError::UnsupportedVersion; return false; }
            FInputReceipt Candidate;
            Candidate.MatchId = ReadBe64(Data + 1);
            Candidate.StreamId = ReadBe64(Data + 9);
            for (uint32 Index = 0; Index < INPUT_STREAM_TOKEN_BYTES; ++Index) Candidate.StreamToken[Index] = Data[17 + Index];
            if (!IsNonzeroStreamToken(Candidate.StreamToken)) { OutError = EAuthoritativeInputCodecError::AllZeroStreamToken; return false; }
            Candidate.AcknowledgedSequence = ReadBe64(Data + 33);
            Candidate.DecidedSequence = ReadBe64(Data + 41);
            if (Candidate.DecidedSequence == 0) { OutError = EAuthoritativeInputCodecError::ZeroSequence; return false; }
            if (Data[49] > 1) { OutError = EAuthoritativeInputCodecError::InvalidDisposition; return false; }
            Candidate.bAccepted = Data[49] == 0;
            Candidate.AuthoritativeTick = ReadBe64(Data + 50);
            if (Data[58] > 1) { OutError = EAuthoritativeInputCodecError::InvalidCorrectionPresence; return false; }
            Candidate.bCorrectionPresent = Data[58] == 1;
            const uint32 CorrectionLength = ReadBe32(Data + 59);
            if (CorrectionLength > MAX_SEQUENCED_INPUT_BODY_BYTES) { OutError = EAuthoritativeInputCodecError::BodyTooLarge; return false; }
            if (!Candidate.bCorrectionPresent && CorrectionLength != 0) { OutError = EAuthoritativeInputCodecError::InvalidCorrectionPresence; return false; }
            const size_t Expected = PrefixBytes + static_cast<size_t>(CorrectionLength);
            if (Length < Expected) { OutError = EAuthoritativeInputCodecError::Truncated; return false; }
            if (Length > Expected) { OutError = EAuthoritativeInputCodecError::TrailingBytes; return false; }
            Candidate.Correction.assign(Data + PrefixBytes, Data + Expected);
            Out = std::move(Candidate);
            OutError = EAuthoritativeInputCodecError::None;
            return true;
        }
    };

    // --- RPC response status (u8) ---
    // The handler ran and the payload is its reply.
    constexpr uint8 RPC_STATUS_OK = 0;
    // The call failed; the payload is a utf8 error message.
    constexpr uint8 RPC_STATUS_ERROR = 1;

    // --- Auth result status (u8; the first byte of a KIND_AUTH_RESULT body) ---
    // The token validated; the connection is bound to the user_id that follows.
    constexpr uint8 AUTH_STATUS_AUTHENTICATED = 0;
    // Accepted as an anonymous guest (no account bound). Only when guests allowed.
    constexpr uint8 AUTH_STATUS_GUEST = 1;
    // The handshake was refused; the body carries a coarse AUTH_REASON_* class and
    // the connection closes immediately after.
    constexpr uint8 AUTH_STATUS_REJECTED = 2;

    // --- Auth rejected reason class (u8; coarse by design, never enumeration-aiding) ---
    // Authentication failed (bad/expired/revoked token).
    constexpr uint8 AUTH_REASON_AUTH_FAILED = 0;
    // A token was required but none was presented (guests disallowed here).
    constexpr uint8 AUTH_REASON_AUTH_REQUIRED = 1;
    // The handshake broke protocol (first frame not KIND_AUTH, a duplicate auth,
    // an oversized token, or auth on an unreliable path).
    constexpr uint8 AUTH_REASON_PROTOCOL = 2;

    // --- Body byte counts ---
    // Bytes prefixing a relayed message with the big-endian sender session id.
    constexpr uint32 SENDER_ID_BYTES = 8;
    // Bytes of the two little-endian f32 (x, y) position payload.
    constexpr uint32 POSITION_BYTES = 8;
    // Bytes of the big-endian RPC request id (u64).
    constexpr uint32 RPC_REQUEST_ID_BYTES = 8;
    // Bytes of the big-endian RPC method-length prefix (u16).
    constexpr uint32 RPC_METHOD_LEN_BYTES = 2;

    // --- Native C ABI version ---
    // The ABI version this SDK is written against. Cross-checked against the
    // canonical header's CITADEL_FFI_ABI_VERSION below so a header bump that this
    // SDK has not caught up to is a compile error (belt) and a Tier-A drift
    // against contract.json (braces).
    constexpr uint32 ABI_VERSION = 3;
}

// Compile-time ABI cross-check: fires wherever CitadelWire.h is compiled,
// including the Tier-B TU.
static_assert(CitadelWire::ABI_VERSION == CITADEL_FFI_ABI_VERSION,
              "Citadel Unreal SDK ABI_VERSION drifted from citadel_client.h "
              "CITADEL_FFI_ABI_VERSION; re-sync the SDK.");
