#pragma once

#include <cstdint>

#include <godot_cpp/classes/ref_counted.hpp>
#include <godot_cpp/variant/dictionary.hpp>
#include <godot_cpp/variant/array.hpp>
#include <godot_cpp/variant/packed_byte_array.hpp>
#include <godot_cpp/variant/string.hpp>

extern "C" {
#include "citadel_client.h"
}

namespace godot {

/// Godot-facing owner for one `CitadelClient` C-ABI handle.
///
/// The GDScript SDK deliberately delegates all transport and auth framing here:
/// it never duplicates the realtime protocol or blocks the Godot main thread.
class CitadelClientNative final : public RefCounted {
    GDCLASS(CitadelClientNative, RefCounted)

    CitadelClient *handle_ = nullptr;

    void close_handle();
    String last_error_string() const;

protected:
    static void _bind_methods();

public:
    CitadelClientNative() = default;
    ~CitadelClientNative() override;

    int64_t abi_version() const;
    int64_t connect_quic(const String &addr, const String &server_name, bool insecure);
    int64_t connect_websocket(const String &url);
    Dictionary authenticate(const PackedByteArray &token);
    int64_t send(int64_t kind, const PackedByteArray &data, bool reliable);
    Dictionary poll();
    Dictionary decode_rep(const PackedByteArray &body, const PackedByteArray &schema_hash,
                          int64_t layout_version, const Array &codecs) const;
    Dictionary encode_rep(int64_t object_id, bool is_full, int64_t result_id, int64_t base_id,
                          int64_t field_count, const PackedByteArray &schema_hash,
                          int64_t layout_version, const Array &fields) const;
    String last_error() const;
    void free_handle();
};

} // namespace godot
