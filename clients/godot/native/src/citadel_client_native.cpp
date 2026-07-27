#include "citadel_client_native.hpp"

#include <algorithm>
#include <array>
#include <vector>

#include <godot_cpp/core/class_db.hpp>
#include <godot_cpp/variant/char_string.hpp>

namespace godot {
namespace {

constexpr size_t kInitialPollCapacity = 64 * 1024;
constexpr size_t kMaximumPollCapacity = 4 * 1024 * 1024;

int64_t as_status(CitadelStatus status) {
    return static_cast<int64_t>(status);
}

} // namespace

CitadelClientNative::~CitadelClientNative() {
    close_handle();
}

void CitadelClientNative::_bind_methods() {
    ClassDB::bind_method(D_METHOD("abi_version"), &CitadelClientNative::abi_version);
    ClassDB::bind_method(D_METHOD("connect_quic", "addr", "server_name", "insecure"),
                         &CitadelClientNative::connect_quic);
    ClassDB::bind_method(D_METHOD("connect_websocket", "url"),
                         &CitadelClientNative::connect_websocket);
    ClassDB::bind_method(D_METHOD("authenticate", "token"), &CitadelClientNative::authenticate);
    ClassDB::bind_method(D_METHOD("send", "kind", "data", "reliable"), &CitadelClientNative::send);
    ClassDB::bind_method(D_METHOD("poll"), &CitadelClientNative::poll);
    ClassDB::bind_method(D_METHOD("decode_rep", "body", "schema_hash", "layout_version", "codecs"),
                         &CitadelClientNative::decode_rep);
    ClassDB::bind_method(D_METHOD("encode_rep", "object_id", "is_full", "result_id", "base_id", "field_count", "schema_hash", "layout_version", "fields"), &CitadelClientNative::encode_rep);
    ClassDB::bind_method(D_METHOD("last_error"), &CitadelClientNative::last_error);
    ClassDB::bind_method(D_METHOD("free_handle"), &CitadelClientNative::free_handle);
}

void CitadelClientNative::close_handle() {
    if (handle_ != nullptr) {
        citadel_client_free(handle_);
        handle_ = nullptr;
    }
}

String CitadelClientNative::last_error_string() const {
    if (handle_ == nullptr) {
        return "Citadel client is not connected";
    }
    std::array<char, 1024> buffer{};
    const uintptr_t written = citadel_client_last_error(handle_, buffer.data(), buffer.size());
    return written == 0 ? String() : String::utf8(buffer.data());
}

int64_t CitadelClientNative::abi_version() const {
    return static_cast<int64_t>(citadel_client_abi_version());
}

int64_t CitadelClientNative::connect_quic(const String &addr, const String &server_name, bool insecure) {
    close_handle();
    const CharString addr_utf8 = addr.utf8();
    const CharString name_utf8 = server_name.utf8();
    CitadelClient *next = nullptr;
    const CitadelStatus status = citadel_client_connect_quic(
        addr_utf8.get_data(), name_utf8.get_data(), insecure, &next);
    if (status == CITADEL_STATUS_OK) {
        handle_ = next;
    }
    return as_status(status);
}

int64_t CitadelClientNative::connect_websocket(const String &url) {
    close_handle();
    const CharString url_utf8 = url.utf8();
    CitadelClient *next = nullptr;
    const CitadelStatus status = citadel_client_connect_websocket(url_utf8.get_data(), &next);
    if (status == CITADEL_STATUS_OK) {
        handle_ = next;
    }
    return as_status(status);
}

Dictionary CitadelClientNative::authenticate(const PackedByteArray &token) {
    Dictionary result;
    if (handle_ == nullptr) {
        result["transport_status"] = static_cast<int64_t>(CITADEL_STATUS_INVALID_ARGUMENT);
        result["reason"] = 0;
        return result;
    }

    std::array<char, 1024> user{};
    CitadelAuthStatus auth_status = CITADEL_AUTH_STATUS_REJECTED;
    uintptr_t user_len = 0;
    uint8_t reason = 0;
    const uint8_t *token_data = token.is_empty() ? nullptr : token.ptr();
    const CitadelStatus transport_status = citadel_client_authenticate(
        handle_, token_data, token.size(), &auth_status, user.data(), user.size(), &user_len, &reason);
    result["transport_status"] = as_status(transport_status);
    result["status"] = static_cast<int64_t>(auth_status);
    result["reason"] = static_cast<int64_t>(reason);
    if (transport_status == CITADEL_STATUS_OK && auth_status == CITADEL_AUTH_STATUS_AUTHENTICATED) {
        result["user_id"] = String::utf8(user.data(), static_cast<int>(std::min(user_len, user.size() - 1)));
    }
    return result;
}

int64_t CitadelClientNative::send(int64_t kind, const PackedByteArray &data, bool reliable) {
    if (handle_ == nullptr || kind < 0 || kind > UINT16_MAX) {
        return static_cast<int64_t>(CITADEL_STATUS_INVALID_ARGUMENT);
    }
    const uint8_t *body = data.is_empty() ? nullptr : data.ptr();
    return as_status(citadel_client_send(handle_, static_cast<uint16_t>(kind), body, data.size(), reliable));
}

Dictionary CitadelClientNative::poll() {
    Dictionary result;
    if (handle_ == nullptr) {
        result["transport_status"] = static_cast<int64_t>(CITADEL_STATUS_INVALID_ARGUMENT);
        return result;
    }

    std::vector<uint8_t> buffer(kInitialPollCapacity);
    uint16_t kind = 0;
    uintptr_t payload_len = 0;
    bool truncated = false;
    CitadelStatus status = citadel_client_poll(
        handle_, &kind, buffer.data(), buffer.size(), &payload_len, &truncated);
    if (status == CITADEL_STATUS_OK && truncated && payload_len <= kMaximumPollCapacity) {
        buffer.resize(payload_len);
        status = citadel_client_poll(handle_, &kind, buffer.data(), buffer.size(), &payload_len, &truncated);
    }

    result["transport_status"] = as_status(status);
    if (status == CITADEL_STATUS_OK && !truncated) {
        PackedByteArray payload;
        payload.resize(static_cast<int64_t>(payload_len));
        if (payload_len > 0) {
            std::copy_n(buffer.data(), payload_len, payload.ptrw());
        }
        result["kind"] = static_cast<int64_t>(kind);
        result["payload"] = payload;
    }
    return result;
}

Dictionary CitadelClientNative::decode_rep(const PackedByteArray &body, const PackedByteArray &schema_hash,
                                           int64_t layout_version, const Array &codecs) const {
    Dictionary result;
    if (schema_hash.size() != 16 || layout_version < 0 || codecs.is_empty()) {
        result["transport_status"] = static_cast<int64_t>(CITADEL_STATUS_INVALID_ARGUMENT);
        return result;
    }
    std::vector<CitadelRepCodec> native_codecs;
    native_codecs.reserve(codecs.size());
    for (int i = 0; i < codecs.size(); ++i) {
        const Dictionary spec = codecs[i];
        CitadelRepCodec codec{};
        codec.kind = static_cast<uint8_t>(int64_t(spec.get("kind", -1)));
        codec.int_min = int64_t(spec.get("int_min", 0));
        codec.int_max = int64_t(spec.get("int_max", 0));
        codec.scalar_min = float(spec.get("scalar_min", 0.0));
        codec.scalar_max = float(spec.get("scalar_max", 0.0));
        codec.values_per_unit = static_cast<uint32_t>(int64_t(spec.get("values_per_unit", 0)));
        codec.max_len = static_cast<uint32_t>(int64_t(spec.get("max_len", 0)));
        native_codecs.push_back(codec);
    }
    CitadelRepDecoded *decoded = nullptr;
    const uint8_t *body_data = body.is_empty() ? nullptr : body.ptr();
    const CitadelStatus status = citadel_rep_decode(body_data, body.size(), schema_hash.ptr(),
        static_cast<uint32_t>(layout_version), native_codecs.data(), native_codecs.size(), &decoded);
    result["transport_status"] = as_status(status);
    if (status != CITADEL_STATUS_OK) return result;
    uint32_t object_id = 0;
    bool is_full = false;
    uint64_t result_id = 0;
    uint64_t base_id = 0;
    if (citadel_rep_decoded_header(decoded, &object_id, &is_full, &result_id, &base_id) != CITADEL_STATUS_OK) {
        citadel_rep_decoded_free(decoded);
        result["transport_status"] = static_cast<int64_t>(CITADEL_STATUS_RECEIVE);
        return result;
    }
    Array fields;
    const uintptr_t count = citadel_rep_decoded_field_count(decoded);
    for (uintptr_t i = 0; i < count; ++i) {
        CitadelRepFieldValue value{};
        if (citadel_rep_decoded_field_at(decoded, i, &value) != CITADEL_STATUS_OK) continue;
        Dictionary field;
        field["field_id"] = static_cast<int64_t>(value.field_id);
        field["kind"] = static_cast<int64_t>(value.kind);
        field["bool"] = value.bool_value;
        field["int"] = value.int_value;
        field["scalar"] = value.scalar_value;
        fields.append(field);
    }
    citadel_rep_decoded_free(decoded);
    result["object_id"] = static_cast<int64_t>(object_id);
    result["is_full"] = is_full;
    result["result_id"] = static_cast<int64_t>(result_id);
    result["base_id"] = static_cast<int64_t>(base_id);
    result["fields"] = fields;
    return result;
}

Dictionary CitadelClientNative::encode_rep(int64_t object_id, bool is_full, int64_t result_id,
                                           int64_t base_id, int64_t field_count,
                                           const PackedByteArray &schema_hash, int64_t layout_version,
                                           const Array &fields) const {
    Dictionary result;
    if (object_id < 0 || result_id <= 0 || base_id < 0 || field_count < 0 ||
        (is_full && schema_hash.size() != 16) || (!is_full && base_id == 0)) {
        result["transport_status"] = static_cast<int64_t>(CITADEL_STATUS_INVALID_ARGUMENT);
        return result;
    }
    CitadelRepEncoder *encoder = citadel_rep_encoder_new(static_cast<uint32_t>(object_id), is_full,
        static_cast<uint64_t>(result_id), static_cast<uint64_t>(base_id), static_cast<uintptr_t>(field_count));
    if (encoder == nullptr) { result["transport_status"] = static_cast<int64_t>(CITADEL_STATUS_INVALID_ARGUMENT); return result; }
    CitadelStatus status = CITADEL_STATUS_OK;
    if (is_full) status = citadel_rep_encoder_set_schema(encoder, schema_hash.ptr(), static_cast<uint32_t>(layout_version));
    for (int i = 0; status == CITADEL_STATUS_OK && i < fields.size(); ++i) {
        const Dictionary field = fields[i];
        const uint16_t id = static_cast<uint16_t>(int64_t(field.get("field_id", -1)));
        switch (int64_t(field.get("kind", -1))) {
            case 0: status = citadel_rep_encoder_add_bool(encoder, id, bool(field.get("value", false))); break;
            case 1: status = citadel_rep_encoder_add_int(encoder, id, int64_t(field.get("min", 0)), int64_t(field.get("max", 0)), int64_t(field.get("value", 0))); break;
            case 2: status = citadel_rep_encoder_add_scalar(encoder, id, float(field.get("min", 0.0)), float(field.get("max", 0.0)), static_cast<uint32_t>(int64_t(field.get("values_per_unit", 0))), float(field.get("value", 0.0))); break;
            case 3: { const PackedByteArray bytes = field.get("value", PackedByteArray()); status = citadel_rep_encoder_add_bytes(encoder, id, static_cast<uint32_t>(int64_t(field.get("max_len", 0))), bytes.is_empty() ? nullptr : bytes.ptr(), bytes.size()); break; }
            default: status = CITADEL_STATUS_INVALID_ARGUMENT; break;
        }
    }
    std::vector<uint8_t> output(64 * 1024); uintptr_t len = 0; bool truncated = false;
    if (status == CITADEL_STATUS_OK) status = citadel_rep_encoder_finish(encoder, output.data(), output.size(), &len, &truncated);
    if (status == CITADEL_STATUS_OK && truncated) { output.resize(len); status = citadel_rep_encoder_finish(encoder, output.data(), output.size(), &len, &truncated); }
    citadel_rep_encoder_free(encoder); result["transport_status"] = as_status(status);
    if (status == CITADEL_STATUS_OK && !truncated) { PackedByteArray body; body.resize(static_cast<int64_t>(len)); if (len > 0) std::copy_n(output.data(), len, body.ptrw()); result["body"] = body; }
    return result;
}

String CitadelClientNative::last_error() const {
    return last_error_string();
}

void CitadelClientNative::free_handle() {
    close_handle();
}

} // namespace godot
