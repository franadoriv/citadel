#!/usr/bin/env python3
"""Deterministic executable and UE-source contract for authoritative input codecs."""
from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURE = json.loads(
    (ROOT / "clients/authoritative-input-fixtures.json").read_text(encoding="utf-8")
)
SEQUENCED_INPUT = FIXTURE["sequenced_input"]
INPUT_RECEIPT = FIXTURE["input_receipt"]
PUBLIC = ROOT / "clients/unreal/Plugin/Citadel/Source/CitadelClient/Public"
PRIVATE = ROOT / "clients/unreal/Plugin/Citadel/Source/CitadelClient/Private/CitadelWireTests.cpp"
FFI = ROOT / "crates/citadel-client-ffi/include"
COMPILER = next((shutil.which(name) for name in ("c++", "g++", "clang++")), None)

if COMPILER is None:
    print("unreal authoritative-input codec: SKIP — no C++ compiler")
    raise SystemExit(0)


def cpp_bytes(value: str) -> str:
    return ", ".join(f"0x{value[index:index + 2]}" for index in range(0, len(value), 2))

source = r'''
#include "CitadelWire.h"
#include <cassert>
#include <vector>

int main() {
  using namespace CitadelWire;
  EAuthoritativeInputCodecError error = EAuthoritativeInputCodecError::None;
  FSequencedInput input;
  input.StreamToken = { __INPUT_TOKEN_BYTES__ };
  input.Sequence = UINT64_MAX;
  input.OriginalCustomKind = __INPUT_ORIGINAL_CUSTOM_KIND__;
  input.Body = { __INPUT_BODY_BYTES__ };
  std::vector<uint8> encoded;
  assert(input.Encode(encoded, error));
  const std::vector<uint8> expected = {
    __SEQUENCED_INPUT_BYTES__
  };
  assert(encoded == expected);
  FSequencedInput decoded;
  assert(FSequencedInput::Decode(encoded.data(), encoded.size(), decoded, error));
  assert(decoded.StreamToken == input.StreamToken && decoded.Sequence == input.Sequence);
  assert(decoded.OriginalCustomKind == input.OriginalCustomKind && decoded.Body == input.Body);
  auto malformed = encoded;
  malformed[0] = 2;
  assert(!FSequencedInput::Decode(malformed.data(), malformed.size(), decoded, error));
  assert(error == EAuthoritativeInputCodecError::UnsupportedVersion);
  malformed = encoded;
  for (uint32 i = 1; i <= INPUT_STREAM_TOKEN_BYTES; ++i) malformed[i] = 0;
  assert(!FSequencedInput::Decode(malformed.data(), malformed.size(), decoded, error));
  assert(error == EAuthoritativeInputCodecError::AllZeroStreamToken);
  malformed = encoded;
  for (uint32 i = 17; i < 25; ++i) malformed[i] = 0;
  assert(!FSequencedInput::Decode(malformed.data(), malformed.size(), decoded, error));
  assert(error == EAuthoritativeInputCodecError::ZeroSequence);
  malformed = encoded;
  malformed.pop_back();
  assert(!FSequencedInput::Decode(malformed.data(), malformed.size(), decoded, error));
  assert(error == EAuthoritativeInputCodecError::Truncated);
  malformed = encoded;
  malformed.push_back(0);
  assert(!FSequencedInput::Decode(malformed.data(), malformed.size(), decoded, error));
  assert(error == EAuthoritativeInputCodecError::TrailingBytes);
  input.Body.assign(MAX_SEQUENCED_INPUT_BODY_BYTES + 1, 0);
  assert(!input.Encode(encoded, error));
  assert(error == EAuthoritativeInputCodecError::BodyTooLarge);

  FInputReceipt receipt;
  receipt.MatchId = 7;
  receipt.StreamId = 9;
  for (uint32 i = 0; i < INPUT_STREAM_TOKEN_BYTES; ++i) receipt.StreamToken[i] = uint8(16 - i);
  receipt.AcknowledgedSequence = 41;
  receipt.DecidedSequence = 42;
  receipt.bAccepted = false;
  receipt.AuthoritativeTick = 99;
  receipt.bCorrectionPresent = true;
  receipt.Correction = {0xde, 0xad};
  assert(receipt.Encode(encoded, error));
  FInputReceipt receipt_decoded;
  assert(FInputReceipt::Decode(encoded.data(), encoded.size(), receipt_decoded, error));
  assert(receipt_decoded.MatchId == 7 && receipt_decoded.StreamId == 9);
  assert(receipt_decoded.StreamToken == receipt.StreamToken);
  assert(receipt_decoded.AcknowledgedSequence == 41 && receipt_decoded.DecidedSequence == 42);
  assert(!receipt_decoded.bAccepted && receipt_decoded.AuthoritativeTick == 99);
  assert(receipt_decoded.bCorrectionPresent && receipt_decoded.Correction == receipt.Correction);
  malformed = encoded;
  malformed[49] = 2; // disposition after version/match/stream/token/sequence fields
  assert(!FInputReceipt::Decode(malformed.data(), malformed.size(), receipt_decoded, error));
  assert(error == EAuthoritativeInputCodecError::InvalidDisposition);
  malformed = encoded;
  malformed[58] = 2; // correction-present discriminator
  assert(!FInputReceipt::Decode(malformed.data(), malformed.size(), receipt_decoded, error));
  assert(error == EAuthoritativeInputCodecError::InvalidCorrectionPresence);

  const std::vector<uint8> fixture_receipt = { __INPUT_RECEIPT_BYTES__ };
  assert(FInputReceipt::Decode(fixture_receipt.data(), fixture_receipt.size(), receipt_decoded, error));
  assert(receipt_decoded.MatchId == UINT64_MAX && receipt_decoded.DecidedSequence == UINT64_MAX);
}
'''
source = (
    source.replace("__INPUT_TOKEN_BYTES__", cpp_bytes(SEQUENCED_INPUT["token_hex"]))
    .replace("__INPUT_ORIGINAL_CUSTOM_KIND__", str(SEQUENCED_INPUT["original_custom_kind"]))
    .replace("__INPUT_BODY_BYTES__", cpp_bytes(SEQUENCED_INPUT["opaque_body_hex"]))
    .replace("__SEQUENCED_INPUT_BYTES__", cpp_bytes(SEQUENCED_INPUT["hex"]))
    .replace("__INPUT_RECEIPT_BYTES__", cpp_bytes(INPUT_RECEIPT["hex"]))
)
with tempfile.TemporaryDirectory(prefix="citadel-unreal-input-") as tmp:
    src = pathlib.Path(tmp) / "input.cpp"
    exe = pathlib.Path(tmp) / "input"
    src.write_text(source)
    subprocess.run(
        [COMPILER, "-std=c++17", "-I", str(PUBLIC), "-I", str(FFI), str(src), "-o", str(exe)],
        check=True,
    )
    subprocess.run([str(exe)], check=True)
print("unreal authoritative-input codec: passed")

if not PRIVATE.is_file():
    raise SystemExit(f"unreal authoritative-input UE automation source missing: {PRIVATE}")
ue_test = PRIVATE.read_text()
required = (
    "FCitadelAuthoritativeInputCodecTest",
    "FSequencedInput::Decode",
    "FInputReceipt::Decode",
    "all-zero bearer token is rejected",
    "trailing bytes are rejected",
    "receipt preserves server-owned match and stream correlations",
)
missing = [needle for needle in required if needle not in ue_test]
if missing:
    raise SystemExit("unreal authoritative-input UE automation coverage missing: " + ", ".join(missing))
print("unreal authoritative-input UE automation coverage: present (run with CITADEL_UE_BUILD=1)")
