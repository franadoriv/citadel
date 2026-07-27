// CitadelFfiStub.cpp — a link-only stub of the Citadel client C ABI, compiled
// ONLY when CITADEL_FFI_STUB=1 (set by clients/unreal/ue-plugin-build.sh for the
// gated UE compile-verification build). See CitadelClient.Build.cs.
//
// Purpose: let the plugin module compile AND link against real UE 5.8 headers
// without the real citadel-client-ffi native library present. It defines every
// exported C ABI symbol the SDK calls, with inert bodies, using the SAME
// signatures as the canonical header (which it includes) — so a signature drift
// would break THIS translation unit too.
//
// This is NEVER compiled into a real game build: a real project links the built
// citadel-client-ffi lib (CITADEL_FFI_LIB) and leaves CITADEL_FFI_STUB unset, so
// this whole TU is #if'd out and emits no symbols. Real behavior is out of scope
// for the compile check (in-editor verification stays manual).
#if defined(CITADEL_FFI_STUB) && CITADEL_FFI_STUB

#include "citadel_client.h"

extern "C" {

uint32_t citadel_client_abi_version(void)
{
	return CITADEL_FFI_ABI_VERSION;
}

CitadelStatus citadel_client_connect_quic(const char* /*addr*/,
	const char* /*server_name*/, bool /*insecure*/, CitadelClient** out_handle)
{
	if (out_handle != nullptr)
	{
		*out_handle = nullptr;
	}
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_client_connect_websocket(const char* /*url*/, CitadelClient** out_handle)
{
	if (out_handle != nullptr)
	{
		*out_handle = nullptr;
	}
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_client_authenticate(CitadelClient* /*handle*/,
	const uint8_t* /*token*/, uintptr_t /*len*/, CitadelAuthStatus* out_status,
	char* user_buf, uintptr_t user_cap, uintptr_t* out_user_len, uint8_t* out_reason)
{
	if (out_status != nullptr)
	{
		*out_status = CITADEL_AUTH_STATUS_REJECTED;
	}
	if (user_buf != nullptr && user_cap > 0)
	{
		user_buf[0] = '\0';
	}
	if (out_user_len != nullptr)
	{
		*out_user_len = 0;
	}
	if (out_reason != nullptr)
	{
		*out_reason = 0;
	}
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_client_send(CitadelClient* /*handle*/, uint16_t /*kind*/,
	const uint8_t* /*data*/, uintptr_t /*len*/, bool /*reliable*/)
{
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_client_poll(CitadelClient* /*handle*/, uint16_t* /*out_kind*/,
	uint8_t* /*buf*/, uintptr_t /*cap*/, uintptr_t* out_len, bool* out_truncated)
{
	if (out_len != nullptr)
	{
		*out_len = 0;
	}
	if (out_truncated != nullptr)
	{
		*out_truncated = false;
	}
	return CITADEL_STATUS_AGAIN;
}

uintptr_t citadel_client_last_error(CitadelClient* /*handle*/, char* buf, uintptr_t cap)
{
	if (buf != nullptr && cap > 0)
	{
		buf[0] = '\0';
	}
	return 0;
}

void citadel_client_free(CitadelClient* /*handle*/)
{
}

CitadelStatus citadel_quantize_scalar(float /*min*/, float /*max*/,
	uint32_t /*values_per_unit*/, float /*value*/, uint64_t* out_code)
{
	if (out_code != nullptr)
	{
		*out_code = 0;
	}
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_dequantize_scalar(float /*min*/, float /*max*/,
	uint32_t /*values_per_unit*/, uint64_t /*code*/, float* out_value)
{
	if (out_value != nullptr)
	{
		*out_value = 0.0f;
	}
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_quat_encode_components(const float* /*quat*/,
	uint32_t /*bits_per_component*/, uint8_t* out_index, uint64_t* out_codes)
{
	if (out_index != nullptr)
	{
		*out_index = 0;
	}
	if (out_codes != nullptr)
	{
		out_codes[0] = 0;
		out_codes[1] = 0;
		out_codes[2] = 0;
	}
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_quat_decode_components(uint8_t /*index*/,
	const uint64_t* /*codes*/, uint32_t /*bits_per_component*/, float* out_quat)
{
	if (out_quat != nullptr)
	{
		out_quat[0] = 0.0f;
		out_quat[1] = 0.0f;
		out_quat[2] = 0.0f;
		out_quat[3] = 1.0f;
	}
	return CITADEL_STATUS_INTERNAL;
}

// --- NetworkPeer schema_hash + DeltaBunch encoder stubs ---

CitadelStatus citadel_schema_hash(uint32_t /*layout_version*/,
	const CitadelSchemaField* /*fields*/, uintptr_t /*count*/, uint8_t* out_hash)
{
	if (out_hash != nullptr)
	{
		for (int i = 0; i < 16; ++i)
		{
			out_hash[i] = 0;
		}
	}
	return CITADEL_STATUS_INTERNAL;
}

CitadelRepEncoder* citadel_rep_encoder_new(uint32_t /*object_id*/, bool /*is_full*/,
	uint64_t /*result_id*/, uint64_t /*base_id*/, uintptr_t /*num_fields*/)
{
	return nullptr;
}

CitadelStatus citadel_rep_encoder_set_schema(CitadelRepEncoder* /*enc*/,
	const uint8_t* /*hash*/, uint32_t /*layout_version*/)
{
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_rep_encoder_add_bool(CitadelRepEncoder* /*enc*/,
	uint16_t /*field_id*/, bool /*value*/)
{
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_rep_encoder_add_int(CitadelRepEncoder* /*enc*/,
	uint16_t /*field_id*/, int64_t /*min*/, int64_t /*max*/, int64_t /*value*/)
{
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_rep_encoder_add_scalar(CitadelRepEncoder* /*enc*/,
	uint16_t /*field_id*/, float /*min*/, float /*max*/, uint32_t /*values_per_unit*/,
	float /*value*/)
{
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_rep_encoder_add_bytes(CitadelRepEncoder* /*enc*/,
	uint16_t /*field_id*/, uint32_t /*max_len*/, const uint8_t* /*data*/, uintptr_t /*len*/)
{
	return CITADEL_STATUS_INTERNAL;
}

CitadelStatus citadel_rep_encoder_finish(CitadelRepEncoder* /*enc*/,
	uint8_t* /*buf*/, uintptr_t /*cap*/, uintptr_t* out_len, bool* out_truncated)
{
	if (out_len != nullptr)
	{
		*out_len = 0;
	}
	if (out_truncated != nullptr)
	{
		*out_truncated = false;
	}
	return CITADEL_STATUS_INTERNAL;
}

void citadel_rep_encoder_free(CitadelRepEncoder* /*enc*/)
{
}

} // extern "C"

#endif // CITADEL_FFI_STUB
