/* Direct C consumer test for the produced citadel-client-ffi static archive.
 *
 * This is intentionally outside Unreal: it proves the canonical C header and
 * actual Rust archive link and execute the Vector3/quat NetworkPeer ABI. UE
 * compile/PIE remains a separate external-environment gate.
 */
#include "citadel_client.h"

#include <math.h>
#include <stdio.h>
#include <string.h>

#define REQUIRE(expr) do { \
    if (!(expr)) { fprintf(stderr, "real FFI parity: failed: %s (%s:%d)\n", #expr, __FILE__, __LINE__); return 1; } \
} while (0)

static CitadelRepCodecV3 codec_v3(unsigned char kind, float bounds, unsigned int quat_bits) {
    CitadelRepCodecV3 value;
    memset(&value, 0, sizeof(value));
    value.kind = kind;
    value.vector_bounds = bounds;
    value.quat_bits = quat_bits;
    return value;
}

int main(void) {
    REQUIRE(citadel_client_abi_version() == 3);

    const float vector[3] = { 12.25f, -7.5f, 0.125f };
    const float quat[4] = { 0.5f, 0.5f, 0.5f, 0.5f };
    CitadelRepEncoder *encoder = citadel_rep_encoder_new(91, false, 12, 8, 2);
    REQUIRE(encoder != NULL);
    REQUIRE(citadel_rep_encoder_add_vector3(encoder, 0, 100.0f, vector) == CITADEL_STATUS_OK);
    REQUIRE(citadel_rep_encoder_add_quat(encoder, 1, 15, quat) == CITADEL_STATUS_OK);

    unsigned char body[512];
    uintptr_t body_len = 0;
    bool truncated = true;
    REQUIRE(citadel_rep_encoder_finish(encoder, body, sizeof(body), &body_len, &truncated) == CITADEL_STATUS_OK);
    REQUIRE(!truncated && body_len != 0);
    citadel_rep_encoder_free(encoder);

    CitadelRepDecodeFieldCodecV3 codecs[2];
    memset(codecs, 0, sizeof(codecs));
    codecs[0].codec = codec_v3(4, 100.0f, 0);
    codecs[1].codec = codec_v3(5, 0.0f, 15);
    const unsigned char schema_hash[16] = { 0 };
    CitadelRepDecoded *decoded = NULL;
    REQUIRE(citadel_rep_decode_with_collections(body, body_len, schema_hash, 0, codecs, 2, &decoded) == CITADEL_STATUS_OK);
    REQUIRE(decoded != NULL);
    REQUIRE(citadel_rep_decoded_field_count(decoded) == 2);

    float decoded_vector[4] = { 0, 0, 0, 99.0f };
    float decoded_quat[4] = { 0, 0, 0, 0 };
    REQUIRE(citadel_rep_decoded_field_floats(decoded, 0, decoded_vector) == CITADEL_STATUS_OK);
    REQUIRE(citadel_rep_decoded_field_floats(decoded, 1, decoded_quat) == CITADEL_STATUS_OK);
    REQUIRE(fabsf(decoded_vector[0] - vector[0]) < 0.01f);
    REQUIRE(fabsf(decoded_vector[1] - vector[1]) < 0.01f);
    REQUIRE(fabsf(decoded_vector[2] - vector[2]) < 0.01f);
    REQUIRE(decoded_vector[3] == 99.0f);
    REQUIRE(fabsf(decoded_quat[0] - quat[0]) < 0.001f);
    REQUIRE(fabsf(decoded_quat[1] - quat[1]) < 0.001f);
    REQUIRE(fabsf(decoded_quat[2] - quat[2]) < 0.001f);
    REQUIRE(fabsf(decoded_quat[3] - quat[3]) < 0.001f);
    citadel_rep_decoded_free(decoded);

    const unsigned char added[] = { 'o', 'k' };
    CitadelRepCodecV3 bytes_codec = codec_v3(3, 0.0f, 0);
    bytes_codec.max_len = 16;
    const CitadelRepCollectionOp operations[2] = {
        { .op = 0, .rep_index = 1, .rep_generation = 2 },
        { .op = 1, .value_kind = 3, .rep_index = 2, .rep_generation = 3,
          .rep_key = 4, .bytes = added, .bytes_len = sizeof(added) },
    };
    encoder = citadel_rep_encoder_new(93, false, 14, 8, 1);
    REQUIRE(encoder != NULL);
    REQUIRE(citadel_rep_encoder_add_collection(encoder, 0, bytes_codec, 8,
        operations, 2) == CITADEL_STATUS_OK);
    REQUIRE(citadel_rep_encoder_finish(encoder, body, sizeof(body), &body_len, &truncated) == CITADEL_STATUS_OK);
    citadel_rep_encoder_free(encoder);

    CitadelRepDecodeFieldCodecV3 collection_codec;
    memset(&collection_codec, 0, sizeof(collection_codec));
    collection_codec.collection_item_codec = bytes_codec;
    collection_codec.collection_max_items = 8;
    collection_codec.is_collection = true;
    decoded = NULL;
    REQUIRE(citadel_rep_decode_with_collections(body, body_len, schema_hash, 0,
        &collection_codec, 1, &decoded) == CITADEL_STATUS_OK);
    uintptr_t operation_count = 0;
    REQUIRE(citadel_rep_decoded_collection_count(decoded, 0, &operation_count) == CITADEL_STATUS_OK);
    REQUIRE(operation_count == 2);
    CitadelRepDecodedCollectionOp collection_op;
    REQUIRE(citadel_rep_decoded_collection_at(decoded, 0, 0, &collection_op) == CITADEL_STATUS_OK);
    REQUIRE(collection_op.op == 0 && collection_op.rep_index == 1 && collection_op.rep_generation == 2);
    REQUIRE(citadel_rep_decoded_collection_at(decoded, 0, 1, &collection_op) == CITADEL_STATUS_OK);
    REQUIRE(collection_op.op == 1 && collection_op.value_kind == 3 && collection_op.bytes_len == sizeof(added));
    unsigned char copied[2] = { 0 };
    uintptr_t copied_len = 0;
    REQUIRE(citadel_rep_decoded_collection_op_bytes(decoded, 0, 1, copied, sizeof(copied), &copied_len) == CITADEL_STATUS_OK);
    REQUIRE(copied_len == sizeof(added) && memcmp(copied, added, sizeof(added)) == 0);
    REQUIRE(citadel_rep_decoded_collection_at(decoded, 0, 2, &collection_op) == CITADEL_STATUS_INVALID_ARGUMENT);
    citadel_rep_decoded_free(decoded);

    /* Regression: a non-bool keyed collection must decode through its supplied
     * scalar item codec. A zeroed collection_item_codec would instead be Bool. */
    CitadelRepCodecV3 scalar_item = codec_v3(2, 0.0f, 0);
    scalar_item.scalar_min = -10.0f;
    scalar_item.scalar_max = 10.0f;
    scalar_item.values_per_unit = 100;
    const CitadelRepCollectionOp scalar_operation = {
        .op = 1, .value_kind = 2, .rep_index = 7, .rep_generation = 1,
        .rep_key = 9, .floats = { 3.25f, 0.0f, 0.0f, 0.0f },
    };
    encoder = citadel_rep_encoder_new(95, false, 16, 8, 1);
    REQUIRE(encoder != NULL);
    REQUIRE(citadel_rep_encoder_add_collection(encoder, 0, scalar_item, 8,
        &scalar_operation, 1) == CITADEL_STATUS_OK);
    REQUIRE(citadel_rep_encoder_finish(encoder, body, sizeof(body), &body_len, &truncated) == CITADEL_STATUS_OK);
    citadel_rep_encoder_free(encoder);
    memset(&collection_codec, 0, sizeof(collection_codec));
    collection_codec.collection_item_codec = scalar_item;
    collection_codec.collection_max_items = 8;
    collection_codec.is_collection = true;
    decoded = NULL;
    REQUIRE(citadel_rep_decode_with_collections(body, body_len, schema_hash, 0,
        &collection_codec, 1, &decoded) == CITADEL_STATUS_OK);
    REQUIRE(citadel_rep_decoded_collection_at(decoded, 0, 0, &collection_op) == CITADEL_STATUS_OK);
    REQUIRE(collection_op.op == 1 && collection_op.value_kind == 2);
    REQUIRE(fabsf(collection_op.floats[0] - 3.25f) < 0.01f);
    citadel_rep_decoded_free(decoded);

    /* Changed-field index is intentionally sparse: the collection is schema
     * field 3 but the second changed field. The accessor must expose 3 rather
     * than inviting an engine binding to use ordinal 1 as a property id. */
    CitadelRepCodecV3 int_codec = codec_v3(1, 0.0f, 0);
    int_codec.int_max = 100;
    encoder = citadel_rep_encoder_new(94, false, 15, 8, 4);
    REQUIRE(encoder != NULL);
    REQUIRE(citadel_rep_encoder_add_int(encoder, 0, 0, 100, 42) == CITADEL_STATUS_OK);
    REQUIRE(citadel_rep_encoder_add_collection(encoder, 3, bytes_codec, 8,
        operations, 2) == CITADEL_STATUS_OK);
    REQUIRE(citadel_rep_encoder_finish(encoder, body, sizeof(body), &body_len, &truncated) == CITADEL_STATUS_OK);
    citadel_rep_encoder_free(encoder);

    CitadelRepDecodeFieldCodecV3 sparse_codecs[4];
    memset(sparse_codecs, 0, sizeof(sparse_codecs));
    for (size_t index = 0; index < 3; ++index) {
        sparse_codecs[index].codec = int_codec;
    }
    sparse_codecs[3].codec = bytes_codec;
    sparse_codecs[3].collection_item_codec = bytes_codec;
    sparse_codecs[3].collection_max_items = 8;
    sparse_codecs[3].is_collection = true;
    decoded = NULL;
    REQUIRE(citadel_rep_decode_with_collections(body, body_len, schema_hash, 0,
        sparse_codecs, 4, &decoded) == CITADEL_STATUS_OK);
    uint16_t collection_field_id = 0;
    REQUIRE(citadel_rep_decoded_collection_field_id(decoded, 1, &collection_field_id) == CITADEL_STATUS_OK);
    REQUIRE(collection_field_id == 3);
    REQUIRE(citadel_rep_decoded_collection_field_id(decoded, 0, &collection_field_id) == CITADEL_STATUS_INVALID_ARGUMENT);
    REQUIRE(citadel_rep_decoded_collection_field_id(decoded, 2, &collection_field_id) == CITADEL_STATUS_INVALID_ARGUMENT);
    citadel_rep_decoded_free(decoded);

    encoder = citadel_rep_encoder_new(92, false, 13, 8, 1);
    REQUIRE(encoder != NULL);
    REQUIRE(citadel_rep_encoder_add_vector3(encoder, 0, 100.0f, NULL) == CITADEL_STATUS_INVALID_ARGUMENT);
    REQUIRE(citadel_rep_encoder_add_quat(encoder, 0, 7, quat) == CITADEL_STATUS_INVALID_ARGUMENT);
    citadel_rep_encoder_free(encoder);
    puts("unreal real FFI Vector3/quat/keyed-collection parity: OK");
    return 0;
}
