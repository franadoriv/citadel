#pragma once

#include <cstdint>

#include <godot_cpp/classes/ref_counted.hpp>
#include <godot_cpp/variant/dictionary.hpp>
#include <godot_cpp/variant/array.hpp>
#include <godot_cpp/variant/packed_byte_array.hpp>
#include <godot_cpp/variant/string.hpp>
#include <godot_cpp/variant/variant.hpp>

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

    // Shared transform-sync runtime. The opaque `CitadelTransformView *` is
    // carried across the GDScript boundary as an int64 pointer value, exactly as
    // the Unity binding holds it in an IntPtr; a null/zero handle returns the
    // safe empty value for each call.
    Variant transform_view_new(const PackedByteArray &hello) const;
    bool transform_view_apply_datagram(int64_t view, const PackedByteArray &snapshot) const;
    PackedByteArray transform_view_ack(int64_t view) const;
    Dictionary transform_view_sample_now(int64_t view, int64_t object_id) const;
    Dictionary transform_view_authoritative(int64_t view, int64_t object_id) const;
    void transform_view_free(int64_t view) const;
    PackedByteArray transform_encode_input(int64_t input_seq, int64_t sim_tick, double dt,
                                           int64_t object_id, int64_t ownership_epoch,
                                           double velocity_x, double velocity_y,
                                           double velocity_z) const;

    String last_error() const;
    void free_handle();
};

} // namespace godot
