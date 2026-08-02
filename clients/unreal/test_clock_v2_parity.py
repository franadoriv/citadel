#!/usr/bin/env python3
"""Deterministic parity guard for Unreal's real v2 wrapper automation test."""
from __future__ import annotations

import pathlib
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
PUBLIC = ROOT / "clients/unreal/Plugin/Citadel/Source/CitadelClient/Public"
PRIVATE = ROOT / "clients/unreal/Plugin/Citadel/Source/CitadelClient/Private/CitadelTransformSync.cpp"
FFI = ROOT / "crates/citadel-client-ffi/include"
COMPILER = next((shutil.which(name) for name in ("c++", "g++", "clang++")), None)

if COMPILER is None:
    print("unreal v2 clock parity: SKIP — no C++ compiler")
    raise SystemExit(0)

source = r'''
#include "CitadelTransformWire.h"
#include <cassert>
#include <vector>
int main() {
  const uint8_t body[] = {
    0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,99, 0,60, 0xaa
  };
  CitadelTransform::FClockMetadata clock;
  assert(CitadelTransform::FClockMetadata::Decode(body, sizeof(body), clock));
  assert(clock.Epoch == 7 && clock.Tick == 99 && clock.TickHz == 60);
  assert(!CitadelTransform::FClockMetadata::Decode(body, 17, clock));
  const uint8_t zero_epoch[] = {0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,1, 0,60};
  assert(!CitadelTransform::FClockMetadata::Decode(zero_epoch, sizeof(zero_epoch), clock));
  const uint8_t accepted[] = {2, 1};
  const uint8_t unknown_capability[] = {2, 2};
  assert(CitadelTransform::FV2Manifest::IsClock(accepted, sizeof(accepted)));
  assert(!CitadelTransform::FV2Manifest::IsClock(unknown_capability, sizeof(unknown_capability)));
  const std::vector<uint8_t> v1 = {0xaa, 0xbb};
  std::vector<uint8_t> v2;
  assert(CitadelTransform::FInputV2Metadata::Encode(7, 99, v1, v2));
  const std::vector<uint8_t> expected = {
    0,0,0,0,0,0,0,7, 0,0,0,0,0,0,0,99, 0, 0xaa,0xbb
  };
  assert(v2 == expected);
  assert(!CitadelTransform::FInputV2Metadata::Encode(0, 99, v1, v2));
}
'''
with tempfile.TemporaryDirectory(prefix="citadel-unreal-v2-") as tmp:
    src = pathlib.Path(tmp) / "clock.cpp"
    exe = pathlib.Path(tmp) / "clock"
    src.write_text(source)
    subprocess.run([COMPILER, "-std=c++17", "-I", str(PUBLIC), "-I", str(FFI), str(src), "-o", str(exe)], check=True)
    subprocess.run([str(exe)], check=True)
print("unreal v2 clock parity: passed")

# The actual wrapper-level behavioral test is compiled and run by Unreal's
# automation framework when CITADEL_UE_BUILD=1. The normal Tier-B environment
# has no UE headers/runtime, so keep a deterministic source-contract guard here
# rather than pretending the prefix decoder alone proves wrapper behavior.
wrapper = PRIVATE.read_text()
required = (
    "FCitadelTransformV2WrapperParityTest",
    "View.ApplyV2Datagram(V2Epoch7",
    "View.ApplyV2Datagram(V2Epoch8",
    "View.ResetV2Epoch(8)",
    "View.ApplyDatagram(V1Snapshot",
    "reset clears managed acknowledgement state",
    "FInputV2Metadata::Encode(8, 100",
    "bV2Negotiated && WorldView.ApplyV2Datagram",
    "FV2Manifest::IsClock(Payload.GetData(), Payload.Num())",
    "Sub->SendFrame(CitadelWire::KIND_TSYNC_V2_INPUT",
    "Sub->SendFrame(CitadelWire::KIND_TSYNC_INPUT",
)
missing = [needle for needle in required if needle not in wrapper]
if missing:
    raise SystemExit("unreal v2 wrapper parity: missing UE automation coverage: " + ", ".join(missing))
print("unreal v2 wrapper automation coverage: present (run with CITADEL_UE_BUILD=1)")
