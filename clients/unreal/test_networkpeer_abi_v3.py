#!/usr/bin/env python3
"""Static ABI-v3 contract assertions for the Unreal NetworkPeer bridge.

This deliberately needs neither Unreal nor a native shared library.  UE/UHT and
PIE gameplay remain an external gate; this test pins the code paths that must
bind the canonical typed C ABI when those environments are available.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
HEADER = (ROOT / "crates/citadel-client-ffi/include/citadel_client.h").read_text()
WIRE = (ROOT / "clients/unreal/Plugin/Citadel/Source/CitadelClient/Public/CitadelWire.h").read_text()
PEER_H = (ROOT / "clients/unreal/Plugin/Citadel/Source/CitadelClient/Public/CitadelNetworkPeer.h").read_text()
PEER_CPP = (ROOT / "clients/unreal/Plugin/Citadel/Source/CitadelClient/Private/CitadelNetworkPeer.cpp").read_text()
TIER_B = (ROOT / "clients/unreal/tier_b/citadel_parity_tu.cpp").read_text()
BUILD = (ROOT / "clients/unreal/ue-plugin-build.sh").read_text()


def require(text: str, needle: str, source: str) -> None:
    if needle not in text:
        raise AssertionError(f"{source}: missing {needle!r}")


try:
    require(HEADER, "#define CITADEL_FFI_ABI_VERSION 3", "canonical header")
    require(WIRE, "constexpr uint32 ABI_VERSION = 3;", "CitadelWire")
    for symbol in (
        "citadel_rep_encoder_add_vector3",
        "citadel_rep_encoder_add_quat",
        "citadel_rep_encoder_add_collection",
        "citadel_rep_decode",
        "citadel_rep_decoded_header",
        "citadel_rep_decoded_field_floats",
        "citadel_rep_decoded_collection_field_id",
    ):
        require(HEADER, symbol, "canonical header")
        require(TIER_B, f"&{symbol}", "Tier-B TU")
    for branch in ("ECitadelFieldType::Vector3", "ECitadelFieldType::Quat", "ECitadelFieldType::Collection"):
        require(PEER_CPP, branch, "NetworkPeer authoring")
    for call in (
        "citadel_rep_encoder_add_vector3",
        "citadel_rep_encoder_add_quat",
        "citadel_rep_encoder_add_collection",
    ):
        require(PEER_CPP, call, "NetworkPeer authoring")
    require(PEER_H, "QueueCollection", "NetworkPeer collection API")
    require(PEER_H, "Collection = 9", "NetworkPeer collection type tag")
    require(PEER_CPP, "OutType = ECitadelFieldType::Collection", "NetworkPeer typed collection reflection")
    require(PEER_H, "AcceptBaseline", "NetworkPeer token control")
    require(PEER_H, "int64 ObjectId = 0", "NetworkPeer authoritative actor binding")
    require(PEER_H, "BoundObjectId", "NetworkPeer authoritative actor binding")
    require(PEER_CPP, "const uint32 BoundId = BoundObjectId();", "NetworkPeer authoritative actor binding")
    if "/*object_id*/ 0" in PEER_CPP:
        raise AssertionError("NetworkPeer authoring: object_id=0 placeholder remains")
    require(PEER_CPP, "BaseId != AcceptedBaselineId", "NetworkPeer stale-base rejection")
    require(PEER_CPP, "Field.Authority != ECitadelFieldAuthority::ClientOwned", "NetworkPeer ownership gate")
    # Collection decode must derive its ABI-v3 item codec from the actual
    # reflected TArray inner; a zeroed item codec would decode every item as Bool.
    for needle in (
        "PopulateCollectionItemCodec",
        "Codec.collection_item_codec",
        "Array->Inner->IsA<FFloatProperty>()",
        "Out.kind = 2",
    ):
        require(PEER_CPP, needle, "NetworkPeer typed collection decode")
    require(PEER_H, "ObjectId <= static_cast<int64>(TNumericLimits<uint32>::Max())",
            "NetworkPeer object id wire-range rejection")
    # A later malformed field must not leave an earlier reflected mutation or
    # keyed identity update behind: preflight plus snapshot/restore is atomic.
    for needle in (
        "ValidateDecodedFields",
        "OriginalCollectionIndices",
        "const auto Restore",
        "Restore(); DestroySnapshots(); return false;",
    ):
        require(PEER_CPP, needle, "NetworkPeer transactional delta apply")
    # A server full is diff(empty, current), so all keyed reflected collections
    # must clear before add operations. The server now emits every collection
    # field (including empty) and the receiver rejects a full that omits one.
    for needle in (
        "ValidateDecodedFields(Decoded, bIsFull)",
        "TSet<uint16> FullCollectionFields",
        "FullCollectionFields.Contains(Field.FieldId)",
        "if (bIsFull)",
        "Values.EmptyValues()",
        "CollectionIndices.Reset()",
        "bIsFull ? TMap<uint64, int32>()",
    ):
        require(PEER_CPP, needle, "NetworkPeer authoritative full collection reset")
    # Receive must route by the decoded object identity and collection updates
    # must use the ABI-v3 source field_id, never the sparse changed ordinal.
    for needle in (
        "ReceiveDeltaBunch",
        "RouteRepDelta",
        "Object == BoundObjectId()",
        "citadel_rep_decode_with_collections",
        "citadel_rep_decoded_collection_field_id(Decoded, Changed, &CollectionFieldId)",
        "ApplyCollectionField(Decoded, Changed, CollectionFieldId)",
        "CollectionIndices.FindOrAdd(SourceFieldId)",
        "Client->Send(CitadelWire::KIND_REP_ACK",
        "RequestFullRecovery",
    ):
        require(PEER_CPP, needle, "NetworkPeer receive/apply lifecycle")
    require(PEER_H, "OnComponentDestroyed", "NetworkPeer lifecycle cleanup")
    require(PEER_H, "TMap<uint16, TMap<uint64, int32>> CollectionIndices", "keyed collection identity")
    require(ROOT.joinpath("clients/unreal/Plugin/Citadel/Source/CitadelClient/Private/CitadelTransformSync.cpp").read_text(),
            "UCitadelNetworkPeer::RouteRepDelta", "transport REP_DELTA routing")
    require(BUILD, "CITADEL_FFI_STUB=0", "real-link UE build")
    require(BUILD, "bundle-ffi.sh", "real-link UE build")
except AssertionError as error:
    print(f"unreal ABI v3 static contract: FAIL — {error}", file=sys.stderr)
    sys.exit(1)

print("unreal ABI v3 static contract: OK")
