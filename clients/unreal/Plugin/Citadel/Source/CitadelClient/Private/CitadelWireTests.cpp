#if WITH_DEV_AUTOMATION_TESTS

#include "CitadelWire.h"
#include "Misc/AutomationTest.h"

IMPLEMENT_SIMPLE_AUTOMATION_TEST(
    FCitadelAuthoritativeInputCodecTest,
    "Citadel.AuthoritativeInput.V1.Codec",
    EAutomationTestFlags::EditorContext | EAutomationTestFlags::EngineFilter)

bool FCitadelAuthoritativeInputCodecTest::RunTest(const FString& Parameters)
{
    using namespace CitadelWire;
    EAuthoritativeInputCodecError Error = EAuthoritativeInputCodecError::None;
    FSequencedInput Input;
    for (uint32 Index = 0; Index < INPUT_STREAM_TOKEN_BYTES; ++Index)
    {
        Input.StreamToken[Index] = static_cast<uint8>(Index + 1);
    }
    Input.Sequence = 42;
    Input.OriginalCustomKind = 900;
    Input.Body = { 0xaa, 0xbb, 0xcc };

    std::vector<uint8> Encoded;
    TestTrue(TEXT("canonical SequencedInput encodes"), Input.Encode(Encoded, Error));
    const std::vector<uint8> Expected = {
        AUTHORITATIVE_INPUT_VERSION,
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,
        0,0,0,0,0,0,0,42, 3,132, 0,0,0,3, 0xaa,0xbb,0xcc,
    };
    TestTrue(TEXT("SequencedInput is canonical big-endian bytes"), Encoded == Expected);

    FSequencedInput Decoded;
    TestTrue(TEXT("canonical SequencedInput decodes"),
        FSequencedInput::Decode(Encoded.data(), Encoded.size(), Decoded, Error));
    TestTrue(TEXT("decoded stream token is exact"), Decoded.StreamToken == Input.StreamToken);
    TestEqual(TEXT("decoded nonzero sequence is exact"), Decoded.Sequence, uint64(42));
    TestEqual(TEXT("decoded opaque kind is exact"), Decoded.OriginalCustomKind, uint16(900));
    TestTrue(TEXT("decoded opaque body is exact"), Decoded.Body == Input.Body);

    std::vector<uint8> Malformed = Encoded;
    Malformed[0] = 2;
    TestFalse(TEXT("wrong version is rejected"),
        FSequencedInput::Decode(Malformed.data(), Malformed.size(), Decoded, Error));
    TestEqual(TEXT("wrong version has exact validation error"), Error,
        EAuthoritativeInputCodecError::UnsupportedVersion);
    Malformed = Encoded;
    for (uint32 Index = 1; Index <= INPUT_STREAM_TOKEN_BYTES; ++Index) Malformed[Index] = 0;
    TestFalse(TEXT("all-zero bearer token is rejected"),
        FSequencedInput::Decode(Malformed.data(), Malformed.size(), Decoded, Error));
    TestEqual(TEXT("all-zero token has exact validation error"), Error,
        EAuthoritativeInputCodecError::AllZeroStreamToken);
    Malformed = Encoded;
    for (uint32 Index = 17; Index < 25; ++Index) Malformed[Index] = 0;
    TestFalse(TEXT("zero sequence is rejected"),
        FSequencedInput::Decode(Malformed.data(), Malformed.size(), Decoded, Error));
    TestEqual(TEXT("zero sequence has exact validation error"), Error,
        EAuthoritativeInputCodecError::ZeroSequence);
    Malformed = Encoded;
    Malformed.resize(Malformed.size() - 1);
    TestFalse(TEXT("declared opaque body truncation is rejected"),
        FSequencedInput::Decode(Malformed.data(), Malformed.size(), Decoded, Error));
    TestEqual(TEXT("truncation has exact validation error"), Error,
        EAuthoritativeInputCodecError::Truncated);
    Malformed = Encoded;
    Malformed.push_back(0);
    TestFalse(TEXT("trailing bytes are rejected"),
        FSequencedInput::Decode(Malformed.data(), Malformed.size(), Decoded, Error));
    TestEqual(TEXT("trailing bytes have exact validation error"), Error,
        EAuthoritativeInputCodecError::TrailingBytes);
    Malformed = Encoded;
    Malformed[27] = 0;
    Malformed[28] = 1;
    Malformed[29] = 0;
    Malformed[30] = 1;
    TestFalse(TEXT("body length above the canonical bound is rejected"),
        FSequencedInput::Decode(Malformed.data(), Malformed.size(), Decoded, Error));
    TestEqual(TEXT("oversize body has exact validation error"), Error,
        EAuthoritativeInputCodecError::BodyTooLarge);

    FInputReceipt Receipt;
    Receipt.MatchId = 7;
    Receipt.StreamId = 9;
    for (uint32 Index = 0; Index < INPUT_STREAM_TOKEN_BYTES; ++Index)
    {
        Receipt.StreamToken[Index] = static_cast<uint8>(INPUT_STREAM_TOKEN_BYTES - Index);
    }
    Receipt.AcknowledgedSequence = 41;
    Receipt.DecidedSequence = 42;
    Receipt.bAccepted = false;
    Receipt.AuthoritativeTick = 99;
    Receipt.bCorrectionPresent = true;
    Receipt.Correction = { 0xde, 0xad };
    TestTrue(TEXT("canonical InputReceipt encodes"), Receipt.Encode(Encoded, Error));
    FInputReceipt ReceiptDecoded;
    TestTrue(TEXT("canonical InputReceipt decodes"),
        FInputReceipt::Decode(Encoded.data(), Encoded.size(), ReceiptDecoded, Error));
    TestEqual(TEXT("receipt preserves server-owned match and stream correlations"),
        ReceiptDecoded.MatchId, uint64(7));
    TestEqual(TEXT("receipt preserves server-owned stream correlation"),
        ReceiptDecoded.StreamId, uint64(9));
    TestTrue(TEXT("receipt preserves exact opaque token"), ReceiptDecoded.StreamToken == Receipt.StreamToken);
    TestEqual(TEXT("receipt preserves acknowledged sequence"), ReceiptDecoded.AcknowledgedSequence, uint64(41));
    TestEqual(TEXT("receipt preserves decided sequence"), ReceiptDecoded.DecidedSequence, uint64(42));
    TestFalse(TEXT("receipt preserves rejected disposition"), ReceiptDecoded.bAccepted);
    TestEqual(TEXT("receipt preserves authoritative tick"), ReceiptDecoded.AuthoritativeTick, uint64(99));
    TestTrue(TEXT("receipt preserves correction presence and bytes"),
        ReceiptDecoded.bCorrectionPresent && ReceiptDecoded.Correction == Receipt.Correction);
    Malformed = Encoded;
    Malformed[49] = 2;
    TestFalse(TEXT("unknown receipt disposition is rejected"),
        FInputReceipt::Decode(Malformed.data(), Malformed.size(), ReceiptDecoded, Error));
    TestEqual(TEXT("unknown receipt disposition has exact validation error"), Error,
        EAuthoritativeInputCodecError::InvalidDisposition);
    Malformed = Encoded;
    Malformed[58] = 2;
    TestFalse(TEXT("unknown correction presence is rejected"),
        FInputReceipt::Decode(Malformed.data(), Malformed.size(), ReceiptDecoded, Error));
    TestEqual(TEXT("unknown correction presence has exact validation error"), Error,
        EAuthoritativeInputCodecError::InvalidCorrectionPresence);
    return true;
}

#endif
