using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using UnityEngine;

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
            // citadel_rep_decode consumes the legacy 7-field scalar descriptor; map
            // the public v3-shaped RepCodec (whose vector_bounds/quat_bits the legacy
            // decode ignores) into it so a multi-codec array does not mis-stride.
            var legacy = new CitadelNative.RepCodecLegacy[codecs.Length];
            for (var i = 0; i < codecs.Length; i++)
                legacy[i] = new CitadelNative.RepCodecLegacy
                {
                    kind = codecs[i].kind, int_min = codecs[i].int_min, int_max = codecs[i].int_max,
                    scalar_min = codecs[i].scalar_min, scalar_max = codecs[i].scalar_max,
                    values_per_unit = codecs[i].values_per_unit, max_len = codecs[i].max_len,
                };
            var status = CitadelNative.citadel_rep_decode(body, (UIntPtr)body.Length, schemaHash, layoutVersion, legacy, (UIntPtr)legacy.Length, out var handle);
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

    /// <summary>Managed authoring data for the ABI v3 NetworkPeer encoder.</summary>
    public sealed class CitadelNetworkPeerField
    {
        public ushort FieldId;
        public byte Kind;
        public bool BoolValue;
        public long IntValue;
        public float ScalarValue;
        public byte[] Bytes = Array.Empty<byte>();
        public Vector3 Vector3Value;
        public Quaternion QuaternionValue = Quaternion.identity;
        public long IntMin, IntMax;
        public float ScalarMin, ScalarMax, VectorBounds;
        public uint ValuesPerUnit, MaxLen, QuaternionBits;
        public CitadelNative.RepCodec ItemCodec;
        public uint MaxItems;
        public CitadelNetworkPeerCollectionOperation[] CollectionOperations = Array.Empty<CitadelNetworkPeerCollectionOperation>();
    }

    public sealed class CitadelNetworkPeerCollectionOperation
    {
        public byte Operation, ValueKind;
        public uint RepIndex, RepGeneration;
        public ulong RepKey;
        public long IntValue;
        public float[] Floats = new float[4];
        public byte[] Bytes = Array.Empty<byte>();
    }

    /// <summary>Client-owned DeltaBunch authoring; native input is copied before this returns.</summary>
    public static class CitadelNetworkPeerAuthor
    {
        public static byte[] Encode(uint objectId, bool isFull, ulong resultId, ulong baseId,
            byte[] schemaHash, uint layoutVersion, IReadOnlyList<CitadelNetworkPeerField> fields)
        {
            if (fields == null || (isFull && (schemaHash == null || schemaHash.Length != 16)) || (!isFull && baseId == 0))
                throw new ArgumentException("Invalid NetworkPeer schema or baseline.");
            IntPtr encoder = CitadelNative.citadel_rep_encoder_new(objectId, isFull, resultId, baseId, (UIntPtr)fields.Count);
            if (encoder == IntPtr.Zero) throw new CitadelException(CitadelStatus.InvalidArgument, "NetworkPeer encoder creation failed.");
            try
            {
                Check(isFull ? CitadelNative.citadel_rep_encoder_set_schema(encoder, schemaHash, layoutVersion) : CitadelStatus.Ok);
                foreach (var field in fields) Add(encoder, field);
                var output = new byte[64 * 1024];
                Check(CitadelNative.citadel_rep_encoder_finish(encoder, output, (UIntPtr)output.Length, out var length, out var truncated));
                if (truncated) { output = new byte[checked((int)length)]; Check(CitadelNative.citadel_rep_encoder_finish(encoder, output, (UIntPtr)output.Length, out length, out truncated)); }
                if (truncated) throw new CitadelException(CitadelStatus.Internal, "NetworkPeer encoder output remained truncated.");
                Array.Resize(ref output, checked((int)length)); return output;
            }
            finally { CitadelNative.citadel_rep_encoder_free(encoder); }
        }

        private static void Add(IntPtr encoder, CitadelNetworkPeerField field)
        {
            if (field == null) throw new ArgumentNullException(nameof(field));
            CitadelStatus status;
            switch (field.Kind) {
                case 0: status = CitadelNative.citadel_rep_encoder_add_bool(encoder, field.FieldId, field.BoolValue); break;
                case 1: status = CitadelNative.citadel_rep_encoder_add_int(encoder, field.FieldId, field.IntMin, field.IntMax, field.IntValue); break;
                case 2: status = CitadelNative.citadel_rep_encoder_add_scalar(encoder, field.FieldId, field.ScalarMin, field.ScalarMax, field.ValuesPerUnit, field.ScalarValue); break;
                case 3: status = CitadelNative.citadel_rep_encoder_add_bytes(encoder, field.FieldId, field.MaxLen, field.Bytes ?? Array.Empty<byte>(), (UIntPtr)(field.Bytes?.Length ?? 0)); break;
                case 4: status = CitadelNative.citadel_rep_encoder_add_vector3(encoder, field.FieldId, field.VectorBounds, new[] { field.Vector3Value.x, field.Vector3Value.y, field.Vector3Value.z }); break;
                case 5: status = CitadelNative.citadel_rep_encoder_add_quat(encoder, field.FieldId, field.QuaternionBits, new[] { field.QuaternionValue.x, field.QuaternionValue.y, field.QuaternionValue.z, field.QuaternionValue.w }); break;
                case 6: status = AddCollection(encoder, field); break;
                default: throw new ArgumentOutOfRangeException(nameof(field.Kind), "Unknown NetworkPeer field kind.");
            }
            Check(status);
        }

        private static CitadelStatus AddCollection(IntPtr encoder, CitadelNetworkPeerField field)
        {
            var source = field.CollectionOperations ?? Array.Empty<CitadelNetworkPeerCollectionOperation>();
            var pins = new GCHandle[source.Length]; var ops = new CitadelNative.RepCollectionOp[source.Length];
            try {
                for (var i = 0; i < source.Length; i++) { var op = source[i] ?? throw new ArgumentException("Collection operation is null."); var bytes = op.Bytes ?? Array.Empty<byte>(); var floats = op.Floats ?? new float[4]; if (floats.Length != 4) throw new ArgumentException("Collection values must carry exactly four float slots."); if (bytes.Length != 0) pins[i] = GCHandle.Alloc(bytes, GCHandleType.Pinned); ops[i] = new CitadelNative.RepCollectionOp { op = op.Operation, value_kind = op.ValueKind, reserved = new byte[6], rep_index = op.RepIndex, rep_generation = op.RepGeneration, rep_key = op.RepKey, int_value = op.IntValue, floats = floats, bytes = bytes.Length == 0 ? IntPtr.Zero : pins[i].AddrOfPinnedObject(), bytes_len = (UIntPtr)bytes.Length }; }
                return CitadelNative.citadel_rep_encoder_add_collection(encoder, field.FieldId, field.ItemCodec, field.MaxItems, ops, (UIntPtr)ops.Length);
            } finally { foreach (var pin in pins) if (pin.IsAllocated) pin.Free(); }
        }
        private static void Check(CitadelStatus status) { if (status != CitadelStatus.Ok) throw new CitadelException(status, "NetworkPeer authoring failed validation."); }
    }

    /// <summary>Tracks authoritative full/delta baselines for one Unity client.</summary>
    public sealed class CitadelNetworkPeerState
    {
        private readonly Dictionary<uint, ulong> _resultTokens = new Dictionary<uint, ulong>();
        public CitadelNetworkPeerDelta Apply(byte[] body, byte[] schemaHash, uint layoutVersion, CitadelNative.RepCodec[] codecs)
        {
            var delta = CitadelNetworkPeerDelta.Decode(body, schemaHash, layoutVersion, codecs);
            if (!delta.IsFull && (!_resultTokens.TryGetValue(delta.ObjectId, out var baseToken) || baseToken != delta.BaseId))
            { delta.Dispose(); throw new CitadelException(CitadelStatus.Receive, "Stale NetworkPeer delta baseline."); }
            _resultTokens[delta.ObjectId] = delta.ResultId;
            return delta;
        }
        /// <summary>Result token to put in the next acknowledgement; zero means no accepted state.</summary>
        public ulong AckToken(uint objectId) => _resultTokens.TryGetValue(objectId, out var token) ? token : 0;
    }
}
