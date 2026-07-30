// Low-level P/Invoke bindings for the Citadel client C ABI.
//
// These bindings map 1:1 onto `crates/citadel-client-ffi/include/citadel_client.h`
// (ABI version 1). They are intentionally thin: raw handles are `IntPtr`, C
// `uintptr_t` is `UIntPtr`, and C `bool` (1 byte) is marshaled with
// `UnmanagedType.I1`. Prefer the managed `CitadelClient` wrapper for app code;
// use these entry points only when you need the exact C surface.
//
// The extern "C" functions use the C calling convention (cdecl); every import
// sets `CallingConvention.Cdecl` so the binding is correct on x86 as well as
// x64 (on x64 there is a single native convention, but being explicit is free).

using System;
using System.Runtime.InteropServices;

namespace Citadel
{
    /// <summary>
    /// Status codes returned by the Citadel client C ABI. Mirrors the
    /// <c>CitadelStatus</c> enum in <c>citadel_client.h</c> (stable, repr(C)).
    /// </summary>
    public enum CitadelStatus
    {
        /// <summary>Operation succeeded (and, for poll, an envelope was written).</summary>
        Ok = 0,

        /// <summary>Non-fatal: nothing was available to poll right now; try again later.</summary>
        Again = 1,

        /// <summary>The connection is closed; no more envelopes will arrive.</summary>
        Disconnected = 2,

        /// <summary>A pointer was null or an argument was invalid.</summary>
        InvalidArgument = 3,

        /// <summary>Connecting or handshaking failed.</summary>
        Connect = 4,

        /// <summary>Sending failed.</summary>
        Send = 5,

        /// <summary>Receiving/decoding failed.</summary>
        Receive = 6,

        /// <summary>An unexpected internal error (including a caught panic).</summary>
        Internal = 7,
    }

    /// <summary>
    /// Realtime auth handshake outcome. Mirrors the wire <c>AUTH_STATUS_*</c>
    /// constants and <c>CitadelAuthStatus</c> in the C ABI.
    /// </summary>
    public enum CitadelAuthStatus
    {
        /// <summary>The token validated and the connection is bound to a user id.</summary>
        Authenticated = 0,

        /// <summary>The connection was admitted as an anonymous guest.</summary>
        Guest = 1,

        /// <summary>The server refused the handshake.</summary>
        Rejected = 2,
    }

    /// <summary>
    /// Raw P/Invoke entry points into the native <c>citadel_client_ffi</c>
    /// library. Unity resolves the platform-specific file from its matching
    /// plugin directory: <c>Assets/Plugins/x86_64/</c> on Windows and
    /// <c>Assets/Plugins/macOS/</c> on macOS.
    /// </summary>
    public static class CitadelNative
    {
        /// <summary>
        /// The native library name without extension. Unity/.NET appends the
        /// platform-specific suffix (<c>.dll</c> / <c>.so</c> / <c>.dylib</c>).
        /// </summary>
        public const string Library = "citadel_client_ffi";

        /// <summary>
        /// The ABI version this binding was written against
        /// (<c>CITADEL_FFI_ABI_VERSION</c>). Checked against
        /// <see cref="citadel_client_abi_version"/> at startup.
        /// </summary>
        public const uint ExpectedAbiVersion = 3;

        /// <summary>Return the ABI version the native library was built with.</summary>
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern uint citadel_client_abi_version();

        /// <summary>
        /// Connect to a Citadel QUIC endpoint. <paramref name="addr"/> and
        /// <paramref name="serverName"/> are NUL-terminated UTF-8 C strings;
        /// <paramref name="insecure"/> selects dev TLS that skips certificate
        /// verification (for the self-signed dev cert). On success a heap handle
        /// is written to <paramref name="outHandle"/>; free it with
        /// <see cref="citadel_client_free"/>.
        /// </summary>
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_client_connect_quic(
            [In] byte[] addr,
            [In] byte[] serverName,
            [MarshalAs(UnmanagedType.I1)] bool insecure,
            out IntPtr outHandle);

        /// <summary>
        /// Connect to a Citadel WebSocket endpoint (e.g.
        /// <c>ws://127.0.0.1:7352/</c>). <paramref name="url"/> is a
        /// NUL-terminated UTF-8 C string.
        /// </summary>
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_client_connect_websocket(
            [In] byte[] url,
            out IntPtr outHandle);

        /// <summary>
        /// Perform the realtime auth handshake on a freshly connected transport.
        /// Pass an empty token for an explicit guest session. On success,
        /// <paramref name="outStatus"/> receives the handshake status,
        /// <paramref name="userBuf"/> receives the authenticated user id when
        /// applicable, <paramref name="outUserLen"/> receives the full user-id
        /// length, and <paramref name="outReason"/> receives an
        /// <c>AUTH_REASON_*</c> for rejected handshakes.
        /// </summary>
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_client_authenticate(
            IntPtr handle,
            [In] byte[] token,
            UIntPtr len,
            out CitadelAuthStatus outStatus,
            [Out] byte[] userBuf,
            UIntPtr userCap,
            out UIntPtr outUserLen,
            out byte outReason);

        /// <summary>
        /// Send an envelope. <paramref name="reliable"/> chooses a reliable
        /// stream vs an unreliable datagram on QUIC (WebSocket is always
        /// reliable). The <paramref name="data"/>/<paramref name="len"/> bytes
        /// are copied by the native side; the caller keeps ownership.
        /// </summary>
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_client_send(
            IntPtr handle,
            ushort kind,
            [In] byte[] data,
            UIntPtr len,
            [MarshalAs(UnmanagedType.I1)] bool reliable);

        /// <summary>
        /// Poll for the next inbound envelope (non-blocking). On
        /// <see cref="CitadelStatus.Ok"/> writes the kind to
        /// <paramref name="outKind"/>, copies up to <paramref name="cap"/>
        /// payload bytes into <paramref name="buf"/>, writes the payload length
        /// to <paramref name="outLen"/>, and sets <paramref name="outTruncated"/>
        /// if the payload did not fit.
        /// </summary>
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_client_poll(
            IntPtr handle,
            out ushort outKind,
            [Out] byte[] buf,
            UIntPtr cap,
            out UIntPtr outLen,
            [MarshalAs(UnmanagedType.I1)] out bool outTruncated);

        /// <summary>
        /// Copy the last error message for <paramref name="handle"/> into
        /// <paramref name="buf"/> as a NUL-terminated string (truncated to
        /// <paramref name="cap"/>). Returns the number of bytes written
        /// including the NUL, or 0 on invalid arguments.
        /// </summary>
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr citadel_client_last_error(
            IntPtr handle,
            [Out] byte[] buf,
            UIntPtr cap);

        /// <summary>
        /// Free a client handle. After this call the pointer is invalid.
        /// Passing <see cref="IntPtr.Zero"/> is a no-op.
        /// </summary>
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern void citadel_client_free(IntPtr handle);

        [StructLayout(LayoutKind.Sequential)]
        public struct TransformState
        {
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 3)] public float[] position;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 4)] public float[] rotation;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 3)] public float[] velocity;
        }

        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_transform_view_new(
            [In] byte[] helloBody, UIntPtr helloLen, out IntPtr outView);

        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_transform_view_apply_datagram(
            IntPtr view, [In] byte[] body, UIntPtr bodyLen,
            [MarshalAs(UnmanagedType.I1)] out bool applied);

        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_transform_view_sample_now(
            IntPtr view, uint objectId, out TransformState state,
            [MarshalAs(UnmanagedType.I1)] out bool found);

        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_transform_view_authoritative_state(
            IntPtr view, uint objectId, out TransformState state, out uint inputSeq,
            [MarshalAs(UnmanagedType.I1)] out bool found);

        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_transform_view_ack(IntPtr view, [Out] byte[] ack);

        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_transform_encode_input(
            uint inputSeq, uint simTick, float dt, uint objectId, uint ownershipEpoch,
            float velocityX, float velocityY, float velocityZ,
            [Out] byte[] output, UIntPtr capacity, out UIntPtr outputLength,
            [MarshalAs(UnmanagedType.I1)] out bool truncated);

        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern void citadel_transform_view_free(IntPtr view);

        [StructLayout(LayoutKind.Sequential)]
        public struct RepCodec
        {
            public byte kind;
            public long int_min;
            public long int_max;
            public float scalar_min;
            public float scalar_max;
            public uint values_per_unit;
            public uint max_len;
            public float vector_bounds;
            public uint quat_bits;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct RepCollectionOp
        {
            public byte op;
            public byte value_kind;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 6)] public byte[] reserved;
            public uint rep_index;
            public uint rep_generation;
            public ulong rep_key;
            public long int_value;
            [MarshalAs(UnmanagedType.ByValArray, SizeConst = 4)] public float[] floats;
            public IntPtr bytes;
            public UIntPtr bytes_len;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct RepFieldValue
        {
            public ushort field_id;
            public byte kind;
            [MarshalAs(UnmanagedType.I1)] public bool bool_value;
            public long int_value;
            public float scalar_value;
            public UIntPtr bytes_len;
        }

        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_decode(
            [In] byte[] body, UIntPtr bodyLen, [In] byte[] schemaHash, uint layoutVersion,
            [In] RepCodec[] codecs, UIntPtr codecCount, out IntPtr decoded);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_decoded_header(
            IntPtr decoded, out uint objectId, [MarshalAs(UnmanagedType.I1)] out bool isFull,
            out ulong resultId, out ulong baseId);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern UIntPtr citadel_rep_decoded_field_count(IntPtr decoded);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_decoded_field_at(IntPtr decoded, UIntPtr index, out RepFieldValue value);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern void citadel_rep_decoded_free(IntPtr decoded);

        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr citadel_rep_encoder_new(uint objectId,
            [MarshalAs(UnmanagedType.I1)] bool isFull, ulong resultId, ulong baseId, UIntPtr fieldCount);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_encoder_set_schema(IntPtr encoder, [In] byte[] hash, uint layoutVersion);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_encoder_add_bool(IntPtr encoder, ushort fieldId, [MarshalAs(UnmanagedType.I1)] bool value);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_encoder_add_int(IntPtr encoder, ushort fieldId, long min, long max, long value);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_encoder_add_scalar(IntPtr encoder, ushort fieldId, float min, float max, uint valuesPerUnit, float value);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_encoder_add_bytes(IntPtr encoder, ushort fieldId, uint maxLen, [In] byte[] data, UIntPtr len);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_encoder_add_vector3(IntPtr encoder, ushort fieldId, float bounds, [In] float[] value);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_encoder_add_quat(IntPtr encoder, ushort fieldId, uint bitsPerComponent, [In] float[] value);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_encoder_add_collection(IntPtr encoder, ushort fieldId, RepCodec itemCodec, uint maxItems, [In] RepCollectionOp[] ops, UIntPtr opCount);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern CitadelStatus citadel_rep_encoder_finish(IntPtr encoder, [Out] byte[] output, UIntPtr capacity, out UIntPtr outputLength, [MarshalAs(UnmanagedType.I1)] out bool truncated);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
        public static extern void citadel_rep_encoder_free(IntPtr encoder);
    }
}
