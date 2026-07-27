// Managed, safe-ish wrapper over the Citadel client C ABI.
//
// Owns the native handle and frees it on Dispose. All entry points go through
// `CitadelNative`. Strings are marshaled explicitly as NUL-terminated UTF-8 so
// the binding does not depend on `LPUTF8Str` marshaling support across Unity's
// Mono/IL2CPP backends.

using System;
using System.Text;

namespace Citadel
{
    /// <summary>
    /// The outcome of a <see cref="CitadelClient.Poll"/> call.
    /// </summary>
    public enum PollResult
    {
        /// <summary>An envelope was written to the caller's buffer.</summary>
        Message,

        /// <summary>Nothing is ready right now; poll again next frame.</summary>
        Again,

        /// <summary>The connection is closed and the queue is drained.</summary>
        Disconnected,
    }

    /// <summary>
    /// Realtime auth handshake status.
    /// </summary>
    public enum AuthHandshakeStatus
    {
        /// <summary>The token validated and the connection is bound to a user id.</summary>
        Authenticated = 0,

        /// <summary>The connection was admitted as an anonymous guest.</summary>
        Guest = 1,

        /// <summary>The server refused the handshake.</summary>
        Rejected = 2,
    }

    /// <summary>
    /// Result returned by <see cref="CitadelClient.AuthenticateGuest"/> and
    /// <see cref="CitadelClient.AuthenticateWithToken"/>.
    /// </summary>
    public readonly struct AuthHandshakeResult
    {
        /// <summary>The resolved handshake status.</summary>
        public AuthHandshakeStatus Status { get; }

        /// <summary>The authenticated user id, or empty for guest/rejected.</summary>
        public string UserId { get; }

        /// <summary>The coarse <c>AUTH_REASON_*</c> class for rejected handshakes.</summary>
        public byte Reason { get; }

        /// <summary>True when the server admitted the connection.</summary>
        public bool IsAccepted => Status == AuthHandshakeStatus.Authenticated || Status == AuthHandshakeStatus.Guest;

        public AuthHandshakeResult(AuthHandshakeStatus status, string userId, byte reason)
        {
            Status = status;
            UserId = userId ?? string.Empty;
            Reason = reason;
        }
    }

    /// <summary>
    /// Thrown when a Citadel connect call fails. Carries the native
    /// <see cref="CitadelStatus"/> for diagnostics.
    /// </summary>
    public sealed class CitadelException : Exception
    {
        /// <summary>The native status code that triggered the failure.</summary>
        public CitadelStatus Status { get; }

        /// <summary>Create a Citadel exception from a status and message.</summary>
        public CitadelException(CitadelStatus status, string message)
            : base(message)
        {
            Status = status;
        }
    }

    /// <summary>
    /// A managed handle to a connected Citadel client. Not thread-safe: call
    /// <see cref="Send"/>/<see cref="Poll"/> from a single thread (in the Unity
    /// sample, the main thread's <c>Update</c>).
    /// </summary>
    public sealed class CitadelClient : IDisposable
    {
        private IntPtr _handle;

        private CitadelClient(IntPtr handle)
        {
            _handle = handle;
        }

        /// <summary>Whether the underlying native handle is still live.</summary>
        public bool IsValid => _handle != IntPtr.Zero;

        /// <summary>
        /// Verify the loaded native library reports the ABI version this
        /// binding was built against (<see cref="CitadelNative.ExpectedAbiVersion"/>).
        /// Call once at startup before connecting.
        /// </summary>
        /// <exception cref="CitadelException">If the versions differ.</exception>
        public static uint CheckAbiVersion()
        {
            uint actual = CitadelNative.citadel_client_abi_version();
            if (actual != CitadelNative.ExpectedAbiVersion)
            {
                throw new CitadelException(
                    CitadelStatus.Internal,
                    $"Citadel ABI mismatch: binding expects {CitadelNative.ExpectedAbiVersion}, " +
                    $"native library reports {actual}. Rebuild the native plugin.");
            }

            return actual;
        }

        /// <summary>
        /// Connect to a Citadel QUIC endpoint (e.g. <c>127.0.0.1:7351</c>).
        /// <paramref name="insecure"/> selects dev TLS that skips certificate
        /// verification, matching the native demo and the self-signed dev cert.
        /// </summary>
        /// <exception cref="CitadelException">If the connect fails.</exception>
        public static CitadelClient ConnectQuic(string addr, string serverName, bool insecure)
        {
            if (string.IsNullOrEmpty(addr))
            {
                throw new ArgumentException("addr must not be empty", nameof(addr));
            }

            if (string.IsNullOrEmpty(serverName))
            {
                throw new ArgumentException("serverName must not be empty", nameof(serverName));
            }

            CitadelStatus status = CitadelNative.citadel_client_connect_quic(
                ToCString(addr),
                ToCString(serverName),
                insecure,
                out IntPtr handle);

            if (status != CitadelStatus.Ok || handle == IntPtr.Zero)
            {
                throw new CitadelException(status, $"connect_quic({addr}) failed: {status}");
            }

            return new CitadelClient(handle);
        }

        /// <summary>
        /// Connect to a Citadel WebSocket endpoint (e.g.
        /// <c>ws://127.0.0.1:7352/</c>).
        /// </summary>
        /// <exception cref="CitadelException">If the connect fails.</exception>
        public static CitadelClient ConnectWebSocket(string url)
        {
            if (string.IsNullOrEmpty(url))
            {
                throw new ArgumentException("url must not be empty", nameof(url));
            }

            CitadelStatus status = CitadelNative.citadel_client_connect_websocket(
                ToCString(url),
                out IntPtr handle);

            if (status != CitadelStatus.Ok || handle == IntPtr.Zero)
            {
                throw new CitadelException(status, $"connect_websocket({url}) failed: {status}");
            }

            return new CitadelClient(handle);
        }

        /// <summary>
        /// Perform the realtime auth handshake as an explicit guest. Call this
        /// immediately after connecting and before any gameplay send.
        /// </summary>
        /// <exception cref="CitadelException">If the native handshake call fails.</exception>
        public AuthHandshakeResult AuthenticateGuest()
        {
            return Authenticate(Array.Empty<byte>());
        }

        /// <summary>
        /// Perform the realtime auth handshake with a session token. Call this
        /// immediately after connecting and before any gameplay send.
        /// </summary>
        /// <exception cref="CitadelException">If the native handshake call fails.</exception>
        public AuthHandshakeResult AuthenticateWithToken(string sessionToken)
        {
            if (string.IsNullOrEmpty(sessionToken))
            {
                throw new ArgumentException("sessionToken must not be empty", nameof(sessionToken));
            }

            return Authenticate(Encoding.UTF8.GetBytes(sessionToken));
        }

        private AuthHandshakeResult Authenticate(byte[] token)
        {
            ThrowIfDisposed();
            byte[] tokenBytes = token ?? Array.Empty<byte>();
            var userBuf = new byte[256];
            CitadelStatus status = CitadelNative.citadel_client_authenticate(
                _handle,
                tokenBytes,
                (UIntPtr)tokenBytes.Length,
                out CitadelAuthStatus authStatus,
                userBuf,
                (UIntPtr)userBuf.Length,
                out UIntPtr outUserLen,
                out byte reason);

            if (status != CitadelStatus.Ok)
            {
                throw new CitadelException(status, $"authenticate failed: {status} ({LastError()})");
            }

            int fullLen = (int)outUserLen;
            int copiedLen = Math.Min(fullLen, Math.Max(0, userBuf.Length - 1));
            string userId = copiedLen > 0 ? Encoding.UTF8.GetString(userBuf, 0, copiedLen) : string.Empty;
            return new AuthHandshakeResult((AuthHandshakeStatus)authStatus, userId, reason);
        }

        /// <summary>
        /// Send an envelope of the given <paramref name="kind"/> carrying
        /// <paramref name="body"/>. <paramref name="reliable"/> picks a reliable
        /// stream vs an unreliable datagram on QUIC. The bytes are copied by the
        /// native side.
        /// </summary>
        /// <returns>The native status (<see cref="CitadelStatus.Ok"/> on success).</returns>
        public CitadelStatus Send(ushort kind, byte[] body, bool reliable)
        {
            ThrowIfDisposed();
            byte[] data = body ?? Array.Empty<byte>();
            return CitadelNative.citadel_client_send(
                _handle,
                kind,
                data,
                (UIntPtr)data.Length,
                reliable);
        }

        /// <summary>
        /// Poll for the next inbound envelope into the caller-owned
        /// <paramref name="buffer"/> (non-blocking). On
        /// <see cref="PollResult.Message"/>, <paramref name="kind"/> and
        /// <paramref name="length"/> describe the payload written to
        /// <paramref name="buffer"/>; <paramref name="truncated"/> is true if the
        /// payload was larger than the buffer (only <c>buffer.Length</c> bytes
        /// were written).
        /// </summary>
        public PollResult Poll(byte[] buffer, out ushort kind, out int length, out bool truncated)
        {
            ThrowIfDisposed();
            byte[] buf = buffer ?? Array.Empty<byte>();

            CitadelStatus status = CitadelNative.citadel_client_poll(
                _handle,
                out kind,
                buf,
                (UIntPtr)buf.Length,
                out UIntPtr outLen,
                out truncated);

            length = (int)outLen;

            switch (status)
            {
                case CitadelStatus.Ok:
                    return PollResult.Message;
                case CitadelStatus.Disconnected:
                    return PollResult.Disconnected;
                case CitadelStatus.Again:
                default:
                    // Treat unexpected receive/decode errors as "nothing this
                    // frame" so the render loop keeps running; details are in
                    // LastError for diagnostics.
                    return PollResult.Again;
            }
        }

        /// <summary>
        /// The last error message recorded for this handle, or an empty string.
        /// </summary>
        public string LastError()
        {
            if (!IsValid)
            {
                return string.Empty;
            }

            var buf = new byte[256];
            UIntPtr written = CitadelNative.citadel_client_last_error(
                _handle,
                buf,
                (UIntPtr)buf.Length);

            int n = (int)written;
            if (n <= 1)
            {
                return string.Empty;
            }

            // `written` includes the trailing NUL; drop it.
            return Encoding.UTF8.GetString(buf, 0, n - 1);
        }

        /// <summary>Free the native handle. Safe to call more than once.</summary>
        public void Dispose()
        {
            if (_handle != IntPtr.Zero)
            {
                CitadelNative.citadel_client_free(_handle);
                _handle = IntPtr.Zero;
            }

            GC.SuppressFinalize(this);
        }

        ~CitadelClient()
        {
            // Backstop: free the native handle if Dispose was missed. Native
            // free is safe off the main thread (it does not touch Unity APIs).
            if (_handle != IntPtr.Zero)
            {
                CitadelNative.citadel_client_free(_handle);
                _handle = IntPtr.Zero;
            }
        }

        private void ThrowIfDisposed()
        {
            if (_handle == IntPtr.Zero)
            {
                throw new ObjectDisposedException(nameof(CitadelClient));
            }
        }

        /// <summary>Encode a string as NUL-terminated UTF-8 for the C ABI.</summary>
        private static byte[] ToCString(string value)
        {
            int byteCount = Encoding.UTF8.GetByteCount(value);
            var bytes = new byte[byteCount + 1]; // trailing NUL (already zero)
            Encoding.UTF8.GetBytes(value, 0, value.Length, bytes, 0);
            return bytes;
        }
    }
}
