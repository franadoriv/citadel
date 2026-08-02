// CitadelTransformWire.h — self-contained decoder for the transform-sync wire
//, a faithful C++ port of `citadel_wire::{bits,codec,tsync}`.
//
// This header decodes the KIND_TSYNC_SNAPSHOT / KIND_TSYNC_HELLO bodies and
// encodes KIND_TSYNC_ACK, matching the canonical Rust bit layout **exactly**:
//
//   * MSB-first bit packing within each byte (citadel_wire::bits);
//   * inclusive fixed-point scalar quantization, `code in 0..=steps`,
//     `bits = ceil_log2(steps + 1)`, `value = min + code / values_per_unit`
//     (citadel_wire::codec::ScalarQuant);
//   * smallest-three quaternion decode: 2-bit dropped index + three n-bit
//     components over [-1/sqrt2, +1/sqrt2], reconstruct the dropped component as
//     sqrt(max(0, 1 - a^2 - b^2 - c^2)), renormalize, identity fallback
//     (citadel_wire::codec::decode_quat).
//
// It is header-only and depends only on <cstdint>/<cmath>/<vector>, so it also
// compiles outside Unreal (used by the manual smoke test). The wire contract is
// pinned by crates/citadel-wire/tests/wire_vectors.json; if that changes, this
// port must be re-synced. A later task replaces this hand-port with the shared
// citadel-client-ffi C ABI so every engine inherits one implementation.
#pragma once

#include <cstdint>
#include <cmath>
#include <cstddef>
#include <cstring> // std::memcpy
#include <vector>

namespace CitadelTransform
{
    /** Exact v2 capability manifest; unknown values never select a guessed layout. */
    struct FV2Manifest
    {
        static bool IsClock(const uint8_t* Body, size_t Len)
        {
            return Body && Len == 2 && Body[0] == 2 && Body[1] == 1;
        }
    };

    /** Fixed v2 gameplay-clock wrapper preceding byte-for-byte v1 snapshot bytes. */
    struct FClockMetadata
    {
        uint64_t Epoch = 0;
        uint64_t Tick = 0;
        uint16_t TickHz = 0;

        static bool Decode(const uint8_t* Body, size_t Len, FClockMetadata& Out)
        {
            if (!Body || Len < 18) { return false; }
            const auto U64 = [](const uint8_t* B) {
                uint64_t V = 0;
                for (int I = 0; I < 8; ++I) { V = (V << 8) | uint64_t(B[I]); }
                return V;
            };
            Out.Epoch = U64(Body);
            Out.Tick = U64(Body + 8);
            Out.TickHz = (uint16_t(Body[16]) << 8) | uint16_t(Body[17]);
            return Out.Epoch != 0 && Out.TickHz != 0;
        }
    };

    /**
     * The v2 owner-input prefix. It is diagnostic-only: the authority does not
     * use either value to select simulation work or authorize the input.
     * Layout: epoch:u64 BE | last_observed_tick:u64 BE | flags:u8(0) | v1 bundle.
     */
    struct FInputV2Metadata
    {
        static bool Encode(uint64_t Epoch, uint64_t LastObservedTick,
                           const std::vector<uint8_t>& V1Bundle,
                           std::vector<uint8_t>& Out)
        {
            if (Epoch == 0 || V1Bundle.empty()) { return false; }
            const auto PutU64 = [&Out](uint64_t Value) {
                for (int Shift = 56; Shift >= 0; Shift -= 8)
                {
                    Out.push_back(static_cast<uint8_t>(Value >> Shift));
                }
            };
            Out.clear();
            Out.reserve(17 + V1Bundle.size());
            PutU64(Epoch);
            PutU64(LastObservedTick);
            Out.push_back(0); // only zero flags are currently valid.
            Out.insert(Out.end(), V1Bundle.begin(), V1Bundle.end());
            return true;
        }
    };

    // ---- MSB-first bit reader (mirror of citadel_wire::bits::BitReader) ----
    class FBitReader
    {
    public:
        FBitReader(const uint8_t* InData, size_t InLen)
            : Data(InData), BitLen(InLen * 8), BitPos(0) {}

        bool ReadBits(uint32_t N, uint64_t& Out)
        {
            if (N > 64) { return false; }
            if (N == 0) { Out = 0; return true; }
            if (BitLen - BitPos < N) { return false; } // bound-before-consume
            uint64_t Result = 0;
            uint32_t Left = N;
            while (Left > 0)
            {
                const size_t ByteIndex = BitPos / 8;
                const uint32_t BitInByte = static_cast<uint32_t>(BitPos % 8);
                const uint32_t Free = 8 - BitInByte;
                const uint32_t Take = Free < Left ? Free : Left;
                const uint32_t SrcShift = Free - Take;
                const uint8_t Mask = static_cast<uint8_t>((1u << Take) - 1);
                const uint8_t Chunk = static_cast<uint8_t>((Data[ByteIndex] >> SrcShift) & Mask);
                Result = (Result << Take) | Chunk;
                BitPos += Take;
                Left -= Take;
            }
            Out = Result;
            return true;
        }

        bool AtByteBoundaryEnd() const { return BitLen - BitPos < 8; }

    private:
        const uint8_t* Data;
        size_t BitLen;
        size_t BitPos;
    };

    // Integer ceil(log2(count)); matches citadel_wire::codec::ceil_log2.
    inline uint32_t CeilLog2(uint64_t Count)
    {
        if (Count <= 1) { return 0; }
        uint32_t Bits = 0;
        uint64_t V = Count - 1;
        while (V > 0) { V >>= 1; ++Bits; }
        return Bits;
    }

    // A single bounded fixed-point axis (inclusive endpoints).
    struct FScalarQuant
    {
        double Min = 0.0;
        double Max = 0.0;
        uint32_t ValuesPerUnit = 1;
        uint64_t Steps = 1;
        uint32_t Bits = 1;

        void Init(float InMin, float InMax, uint32_t InVpu)
        {
            Min = static_cast<double>(InMin);
            Max = static_cast<double>(InMax);
            ValuesPerUnit = InVpu < 1 ? 1 : InVpu;
            double S = std::floor((Max - Min) * static_cast<double>(ValuesPerUnit) + 0.5);
            Steps = S < 1.0 ? 1 : static_cast<uint64_t>(S);
            Bits = CeilLog2(Steps + 1);
        }

        bool Read(FBitReader& R, float& Out) const
        {
            uint64_t Code = 0;
            if (!R.ReadBits(Bits, Code)) { return false; }
            if (Code > Steps) { return false; } // decode rejects out-of-range
            Out = static_cast<float>(Min + static_cast<double>(Code) / static_cast<double>(ValuesPerUnit));
            return true;
        }
    };

    struct FVectorQuant
    {
        FScalarQuant Axis[3];
        void Init(const float InMin[3], const float InMax[3], uint32_t Vpu)
        {
            for (int i = 0; i < 3; ++i) { Axis[i].Init(InMin[i], InMax[i], Vpu); }
        }
        bool Read(FBitReader& R, float Out[3]) const
        {
            return Axis[0].Read(R, Out[0]) && Axis[1].Read(R, Out[1]) && Axis[2].Read(R, Out[2]);
        }
    };

    // Smallest-three quaternion decode; QuatBits is 9/10/15.
    inline bool ReadQuat(FBitReader& R, uint32_t QuatBits, float OutXyzw[4])
    {
        uint64_t Index = 0;
        if (!R.ReadBits(2, Index)) { return false; }
        const double SqrtHalf = 0.70710678118654752440;
        const double Span = 2.0 * SqrtHalf;
        const double Levels = static_cast<double>(1ull << QuatBits);
        double Kept[3];
        for (int i = 0; i < 3; ++i)
        {
            uint64_t Code = 0;
            if (!R.ReadBits(QuatBits, Code)) { return false; }
            Kept[i] = (static_cast<double>(Code) / (Levels - 1.0)) * Span - SqrtHalf;
        }
        const double SumSq = Kept[0] * Kept[0] + Kept[1] * Kept[1] + Kept[2] * Kept[2];
        double Largest = 1.0 - SumSq;
        Largest = Largest > 0.0 ? std::sqrt(Largest) : 0.0;
        double Q[4] = {0, 0, 0, 0};
        const int Idx = static_cast<int>(Index & 0x3);
        Q[Idx] = Largest;
        int K = 0;
        for (int i = 0; i < 4; ++i)
        {
            if (i == Idx) { continue; }
            Q[i] = Kept[K++];
        }
        const double Norm = std::sqrt(Q[0]*Q[0] + Q[1]*Q[1] + Q[2]*Q[2] + Q[3]*Q[3]);
        if (!(Norm > 1e-6) || !std::isfinite(Norm))
        {
            OutXyzw[0] = 0; OutXyzw[1] = 0; OutXyzw[2] = 0; OutXyzw[3] = 1; // identity
            return true;
        }
        for (int i = 0; i < 4; ++i) { OutXyzw[i] = static_cast<float>(Q[i] / Norm); }
        return true;
    }

    // The negotiated codec parameters, from a decoded KIND_TSYNC_HELLO body.
    struct FCodecParams
    {
        FVectorQuant Position;
        FVectorQuant Velocity;
        uint32_t QuatBits = 10;
        uint8_t SendRateHz = 20;
        uint8_t SimRateHz = 60;

        // Decode a HELLO body: min[3]f32 max[3]f32 vpu u32 (position), same for
        // velocity, quat_mode u8, send_rate u8, sim_rate u8. Big-endian.
        bool DecodeHello(const uint8_t* B, size_t Len)
        {
            const size_t BoundsBytes = 3*4 + 3*4 + 4;
            if (Len < BoundsBytes*2 + 3) { return false; }
            size_t Off = 0;
            float PMin[3], PMax[3]; uint32_t PVpu;
            float VMin[3], VMax[3]; uint32_t VVpu;
            ReadBounds(B, Off, PMin, PMax, PVpu);
            ReadBounds(B, Off, VMin, VMax, VVpu);
            Position.Init(PMin, PMax, PVpu);
            Velocity.Init(VMin, VMax, VVpu);
            QuatBits = B[Off]; ++Off;
            SendRateHz = B[Off]; ++Off;
            SimRateHz = B[Off]; ++Off;
            return QuatBits == 9 || QuatBits == 10 || QuatBits == 15;
        }

    private:
        static float BeF32(const uint8_t* B, size_t& Off)
        {
            uint32_t U = (uint32_t(B[Off]) << 24) | (uint32_t(B[Off+1]) << 16)
                       | (uint32_t(B[Off+2]) << 8) | uint32_t(B[Off+3]);
            Off += 4;
            float F; std::memcpy(&F, &U, 4); return F;
        }
        static uint32_t BeU32(const uint8_t* B, size_t& Off)
        {
            uint32_t U = (uint32_t(B[Off]) << 24) | (uint32_t(B[Off+1]) << 16)
                       | (uint32_t(B[Off+2]) << 8) | uint32_t(B[Off+3]);
            Off += 4; return U;
        }
        static void ReadBounds(const uint8_t* B, size_t& Off, float Min[3], float Max[3], uint32_t& Vpu)
        {
            for (int i = 0; i < 3; ++i) { Min[i] = BeF32(B, Off); }
            for (int i = 0; i < 3; ++i) { Max[i] = BeF32(B, Off); }
            Vpu = BeU32(B, Off);
        }
    };

    // One decoded object update (present fields only; absent => unchanged).
    struct FObjectUpdate
    {
        uint32_t ObjectId = 0;
        uint16_t GenEpoch = 0;
        bool bHasPosition = false;
        bool bHasRotation = false;
        bool bHasVelocity = false;
        // For an owned object, the highest CONTIGUOUS input seq the server applied
        // ( P2 §5.1) — the reconciliation ack. Present only when set.
        bool bHasInputSeq = false;
        float Position[3] = {0, 0, 0};
        float Rotation[4] = {0, 0, 0, 1};
        float Velocity[3] = {0, 0, 0};
        uint32_t LastInputSeq = 0;
    };

    // A decoded KIND_TSYNC_SNAPSHOT body.
    struct FSnapshot
    {
        uint32_t ServerTick = 0;
        uint32_t SnapshotId = 0;
        uint32_t BaseSnapshotId = 0;
        uint8_t SendRateHz = 20;
        std::vector<uint32_t> Removed;
        std::vector<FObjectUpdate> Updates;

        bool Decode(const uint8_t* Body, size_t Len, const FCodecParams& C)
        {
            FBitReader R(Body, Len);
            uint64_t V;
            if (!R.ReadBits(32, V)) return false; ServerTick = static_cast<uint32_t>(V);
            if (!R.ReadBits(32, V)) return false; SnapshotId = static_cast<uint32_t>(V);
            if (!R.ReadBits(32, V)) return false; BaseSnapshotId = static_cast<uint32_t>(V);
            if (!R.ReadBits(8, V)) return false; SendRateHz = static_cast<uint8_t>(V);
            uint64_t RemovedCount, UpdateCount;
            if (!R.ReadBits(16, RemovedCount)) return false;
            if (!R.ReadBits(16, UpdateCount)) return false;
            Removed.clear();
            for (uint64_t i = 0; i < RemovedCount; ++i)
            {
                if (!R.ReadBits(32, V)) return false;
                Removed.push_back(static_cast<uint32_t>(V));
            }
            Updates.clear();
            for (uint64_t i = 0; i < UpdateCount; ++i)
            {
                FObjectUpdate U;
                if (!R.ReadBits(32, V)) return false; U.ObjectId = static_cast<uint32_t>(V);
                if (!R.ReadBits(16, V)) return false; U.GenEpoch = static_cast<uint16_t>(V);
                uint64_t Changed;
                // 4 changed bits: pos(4) | rot(2) | vel(1) | last_input_seq(8).
                if (!R.ReadBits(4, Changed)) return false;
                if (Changed & 0x4) { if (!C.Position.Read(R, U.Position)) return false; U.bHasPosition = true; }
                if (Changed & 0x2) { if (!ReadQuat(R, C.QuatBits, U.Rotation)) return false; U.bHasRotation = true; }
                if (Changed & 0x1) { if (!C.Velocity.Read(R, U.Velocity)) return false; U.bHasVelocity = true; }
                if (Changed & 0x8) { if (!R.ReadBits(32, V)) return false; U.LastInputSeq = static_cast<uint32_t>(V); U.bHasInputSeq = true; }
                Updates.push_back(U);
            }
            return true;
        }
    };

    // Encode a KIND_TSYNC_ACK body: acked_snapshot_id u32 + history u32 (BE).
    inline void EncodeAck(uint32_t AckedId, uint32_t History, uint8_t Out[8])
    {
        Out[0] = uint8_t(AckedId >> 24); Out[1] = uint8_t(AckedId >> 16);
        Out[2] = uint8_t(AckedId >> 8);  Out[3] = uint8_t(AckedId);
        Out[4] = uint8_t(History >> 24); Out[5] = uint8_t(History >> 16);
        Out[6] = uint8_t(History >> 8);  Out[7] = uint8_t(History);
    }

    // ---- Owner input bundle (KIND_TSYNC_INPUT) — mirror of tsync::InputBundle --

    // A lag-compensated fire command carried inside an input frame (§5.2).
    struct FFireCommand
    {
        float Origin[3] = {0, 0, 0};
        float Direction[3] = {0, 0, 0};
    };

    // One individually-sequenced owner input frame (§5.1).
    struct FInputFrame
    {
        uint32_t InputSeq = 0;
        uint32_t SimTick = 0;
        float Dt = 0.0f;
        uint32_t ObjectId = 0;
        uint32_t OwnershipEpoch = 0;
        float MoveVelocity[3] = {0, 0, 0}; // cm/s kinematic intent
        std::vector<uint8_t> Payload;      // opaque game data
        bool bHasFire = false;
        FFireCommand Fire;
    };

    inline void PutBeU32(std::vector<uint8_t>& B, uint32_t V)
    {
        B.push_back(uint8_t(V >> 24)); B.push_back(uint8_t(V >> 16));
        B.push_back(uint8_t(V >> 8));  B.push_back(uint8_t(V));
    }
    inline void PutBeU16(std::vector<uint8_t>& B, uint16_t V)
    {
        B.push_back(uint8_t(V >> 8)); B.push_back(uint8_t(V));
    }
    inline void PutBeF32(std::vector<uint8_t>& B, float F)
    {
        uint32_t U; std::memcpy(&U, &F, 4); PutBeU32(B, U);
    }

    // Encode a KIND_TSYNC_INPUT body from the last N frames (oldest first) plus
    // the piggybacked acked/last-seen snapshot ids. Matches tsync::InputBundle so
    // the server decodes it bit-for-bit.
    inline std::vector<uint8_t> EncodeInputBundle(
        uint32_t AckedSnapshotId, uint32_t LastSeenSnapshotId,
        const std::vector<FInputFrame>& Frames)
    {
        static const size_t MaxFrames = 32;
        std::vector<uint8_t> B;
        PutBeU32(B, AckedSnapshotId);
        PutBeU32(B, LastSeenSnapshotId);
        const size_t Count = Frames.size() < MaxFrames ? Frames.size() : MaxFrames;
        B.push_back(static_cast<uint8_t>(Count));
        for (size_t i = 0; i < Count; ++i)
        {
            const FInputFrame& F = Frames[i];
            PutBeU32(B, F.InputSeq);
            PutBeU32(B, F.SimTick);
            PutBeF32(B, F.Dt);
            PutBeU32(B, F.ObjectId);
            PutBeU32(B, F.OwnershipEpoch);
            for (int a = 0; a < 3; ++a) { PutBeF32(B, F.MoveVelocity[a]); }
            const uint8_t Flags = F.bHasFire ? 0x01 : 0x00;
            B.push_back(Flags);
            const uint16_t PayloadLen = F.Payload.size() > 0xFFFF ? 0xFFFF : static_cast<uint16_t>(F.Payload.size());
            PutBeU16(B, PayloadLen);
            B.insert(B.end(), F.Payload.begin(), F.Payload.begin() + PayloadLen);
            if (F.bHasFire)
            {
                for (int a = 0; a < 3; ++a) { PutBeF32(B, F.Fire.Origin[a]); }
                for (int a = 0; a < 3; ++a) { PutBeF32(B, F.Fire.Direction[a]); }
            }
        }
        return B;
    }

    // ---- Role/ownership frame (KIND_TSYNC_ROLE) — mirror of tsync::Role -------

    struct FRoleFrame
    {
        uint32_t ObjectId = 0;
        uint8_t Role = 1;          // 0=OwnerPredicted 1=RemoteInterpolated ...
        uint64_t Owner = 0;        // participant id (0 = none)
        uint32_t OwnershipEpoch = 0;
        uint16_t GenEpoch = 0;
        uint8_t Event = 0;

        // Layout (big-endian): object_id u32 · role u8 · owner u64 ·
        // ownership_epoch u32 · gen_epoch u16 · event u8 = 20 bytes.
        bool Decode(const uint8_t* B, size_t Len)
        {
            if (Len < 4 + 1 + 8 + 4 + 2 + 1) { return false; }
            size_t Off = 0;
            auto BeU32 = [&](void) -> uint32_t {
                uint32_t U = (uint32_t(B[Off]) << 24) | (uint32_t(B[Off+1]) << 16)
                           | (uint32_t(B[Off+2]) << 8) | uint32_t(B[Off+3]);
                Off += 4; return U;
            };
            ObjectId = BeU32();
            Role = B[Off]; Off += 1;
            uint64_t O = 0;
            for (int i = 0; i < 8; ++i) { O = (O << 8) | uint64_t(B[Off + i]); }
            Owner = O; Off += 8;
            OwnershipEpoch = BeU32();
            GenEpoch = (uint16_t(B[Off]) << 8) | uint16_t(B[Off + 1]); Off += 2;
            Event = B[Off]; Off += 1;
            return true;
        }
    };

    // ---- Rewind result (KIND_TSYNC_REWIND) — mirror of tsync::RewindResult ----

    struct FRewindResult
    {
        uint32_t InputSeq = 0;
        bool bHit = false;
        uint32_t ObjectId = 0;
        float HitPoint[3] = {0, 0, 0};
        uint32_t RewindTick = 0;

        // Decode: input_seq u32 · flags u8 · object_id u32 · hit_point[3] f32 ·
        // rewind_tick u32 (all big-endian). 25 bytes.
        bool Decode(const uint8_t* B, size_t Len)
        {
            if (Len < 4 + 1 + 4 + 12 + 4) { return false; }
            size_t Off = 0;
            auto BeU32 = [&](void) -> uint32_t {
                uint32_t U = (uint32_t(B[Off]) << 24) | (uint32_t(B[Off+1]) << 16)
                           | (uint32_t(B[Off+2]) << 8) | uint32_t(B[Off+3]);
                Off += 4; return U;
            };
            auto BeF32 = [&](void) -> float {
                uint32_t U = (uint32_t(B[Off]) << 24) | (uint32_t(B[Off+1]) << 16)
                           | (uint32_t(B[Off+2]) << 8) | uint32_t(B[Off+3]);
                Off += 4; float F; std::memcpy(&F, &U, 4); return F;
            };
            InputSeq = BeU32();
            bHit = (B[Off] & 0x01) != 0; Off += 1;
            ObjectId = BeU32();
            for (int a = 0; a < 3; ++a) { HitPoint[a] = BeF32(); }
            RewindTick = BeU32();
            return true;
        }
    };
}
