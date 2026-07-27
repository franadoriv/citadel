using System;

namespace Citadel
{
    /// <summary>Decoded authoritative NetworkPeer delta backed by the Rust C ABI.</summary>
    public sealed class CitadelNetworkPeerDelta : IDisposable
    {
        private IntPtr _handle;
        public uint ObjectId { get; }
        public bool IsFull { get; }
        public ulong ResultId { get; }
        public ulong BaseId { get; }

        private CitadelNetworkPeerDelta(IntPtr handle, uint objectId, bool isFull, ulong resultId, ulong baseId)
        { _handle = handle; ObjectId = objectId; IsFull = isFull; ResultId = resultId; BaseId = baseId; }

        public static CitadelNetworkPeerDelta Decode(byte[] body, byte[] schemaHash, uint layoutVersion, CitadelNative.RepCodec[] codecs)
        {
            if (body == null || schemaHash == null || schemaHash.Length != 16 || codecs == null)
                throw new ArgumentException("NetworkPeer decode requires body, 16-byte schema hash, and codecs.");
            var status = CitadelNative.citadel_rep_decode(body, (UIntPtr)body.Length, schemaHash, layoutVersion, codecs, (UIntPtr)codecs.Length, out var handle);
            if (status != CitadelStatus.Ok || handle == IntPtr.Zero) throw new CitadelException(status, "NetworkPeer delta decode failed.");
            status = CitadelNative.citadel_rep_decoded_header(handle, out var id, out var full, out var result, out var basis);
            if (status != CitadelStatus.Ok) { CitadelNative.citadel_rep_decoded_free(handle); throw new CitadelException(status, "NetworkPeer delta header failed."); }
            return new CitadelNetworkPeerDelta(handle, id, full, result, basis);
        }

        public int FieldCount => checked((int)CitadelNative.citadel_rep_decoded_field_count(_handle));
        public CitadelNative.RepFieldValue FieldAt(int index)
        { if (_handle == IntPtr.Zero || index < 0) throw new ObjectDisposedException(nameof(CitadelNetworkPeerDelta)); var s = CitadelNative.citadel_rep_decoded_field_at(_handle, (UIntPtr)index, out var value); if (s != CitadelStatus.Ok) throw new CitadelException(s, "NetworkPeer field decode failed."); return value; }
        public void Dispose() { if (_handle != IntPtr.Zero) { CitadelNative.citadel_rep_decoded_free(_handle); _handle = IntPtr.Zero; } GC.SuppressFinalize(this); }
    }
}
