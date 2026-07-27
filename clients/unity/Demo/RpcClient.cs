// Client-side RPC helper for the Citadel demo.
//
// Sends KIND_RPC_REQUEST envelopes with a monotonically increasing request_id
// and correlates the server's KIND_RPC_RESPONSE back to the caller's callback by
// that id. Because the native poll queue is shared by every kind, exactly ONE
// component owns the poll loop (here, PeerManager): it dispatches peer positions
// itself and forwards RPC responses to this component's HandleResponse. This
// keeps RPC replies from being mistaken for peer positions, and vice versa.
//
// Sample: press R to call the server `add` RPC (two big-endian int32) and the
// `ping` RPC, logging the results. `game/main.lua` defines both handlers.

using System;
using System.Collections.Generic;
using UnityEngine;

namespace Citadel.Demo
{
    /// <summary>
    /// The outcome of a <see cref="RpcClient.CallRpc"/> call, delivered to the
    /// caller's callback when the correlated response arrives.
    /// </summary>
    public readonly struct CitadelRpcResult
    {
        /// <summary>The correlation id of the originating request.</summary>
        public ulong RequestId { get; }

        /// <summary>True if the server handler succeeded (status == ok).</summary>
        public bool Ok { get; }

        /// <summary>
        /// On success, the handler's reply bytes; on failure, a short utf8 error
        /// message. Never null (may be empty).
        /// </summary>
        public byte[] Payload { get; }

        /// <summary>Create a result.</summary>
        public CitadelRpcResult(ulong requestId, bool ok, byte[] payload)
        {
            RequestId = requestId;
            Ok = ok;
            Payload = payload ?? Array.Empty<byte>();
        }

        /// <summary>Interpret the payload as a big-endian int32 (e.g. an `add` sum).</summary>
        public bool TryReadBeInt32(out int value)
        {
            value = 0;
            if (Payload.Length != 4)
            {
                return false;
            }

            value = (Payload[0] << 24) | (Payload[1] << 16) | (Payload[2] << 8) | Payload[3];
            return true;
        }

        /// <summary>Interpret the payload as UTF-8 text (e.g. `ping` -&gt; "pong").</summary>
        public string PayloadAsText()
        {
            return System.Text.Encoding.UTF8.GetString(Payload);
        }
    }

    /// <summary>
    /// Issues request/response RPCs over the shared Citadel connection and
    /// correlates replies by request_id. Does not poll: <see cref="PeerManager"/>
    /// owns the single poll loop and calls <see cref="HandleResponse"/> for each
    /// <see cref="CitadelProtocol.KindRpcResponse"/> envelope it drains.
    /// </summary>
    public sealed class RpcClient : MonoBehaviour
    {
        [Tooltip("The scene connection that owns the Citadel client.")]
        public CitadelConnection connection;

        [Tooltip("Key that fires the sample add/ping RPCs.")]
        public KeyCode sampleKey = KeyCode.R;

        [Tooltip("First operand for the sample `add` RPC.")]
        public int sampleAddA = 7;

        [Tooltip("Second operand for the sample `add` RPC.")]
        public int sampleAddB = 35;

        // request_id -> callback awaiting that correlated response. Ids are
        // monotonic, so an id is only ever pending once; a response for an id not
        // in the map is unknown or a duplicate and is dropped.
        private readonly Dictionary<ulong, Action<CitadelRpcResult>> _pending =
            new Dictionary<ulong, Action<CitadelRpcResult>>();

        private ulong _nextRequestId = 1;

        /// <summary>
        /// Send an RPC <paramref name="method"/> with <paramref name="payload"/>
        /// (reliable) and register <paramref name="onReply"/> to be invoked when
        /// the correlated response arrives. Returns false (without registering)
        /// if the connection is down or the send failed.
        /// </summary>
        public bool CallRpc(string method, byte[] payload, Action<CitadelRpcResult> onReply)
        {
            if (connection == null || !connection.IsConnected)
            {
                Debug.LogWarning("[Citadel] CallRpc skipped: not connected.");
                return false;
            }

            ulong requestId = _nextRequestId;
            byte[] body;
            try
            {
                body = CitadelProtocol.EncodeRpcRequest(requestId, method, payload);
            }
            catch (ArgumentException e)
            {
                Debug.LogWarning($"[Citadel] CallRpc({method}) not sent: {e.Message}");
                return false;
            }

            CitadelStatus status = connection.Client.Send(
                CitadelProtocol.KindRpcRequest,
                body,
                reliable: true);

            if (status != CitadelStatus.Ok && status != CitadelStatus.Again)
            {
                Debug.LogWarning(
                    $"[Citadel] CallRpc({method}) send failed: {status} " +
                    $"({connection.Client.LastError()})");
                return false;
            }

            // Only claim the id and register once the send actually went out.
            _nextRequestId++;
            _pending[requestId] = onReply;
            return true;
        }

        /// <summary>
        /// Decode a polled <see cref="CitadelProtocol.KindRpcResponse"/> body,
        /// find its pending callback by request_id, and invoke it. Called by the
        /// single poll owner (<see cref="PeerManager"/>). Unknown or duplicate
        /// request_ids are dropped with a warning.
        /// </summary>
        public void HandleResponse(byte[] body, int length)
        {
            if (!CitadelProtocol.TryDecodeRpcResponse(
                    body, length, out ulong requestId, out byte status, out byte[] payload))
            {
                Debug.LogWarning("[Citadel] dropped a malformed RPC response.");
                return;
            }

            if (!_pending.TryGetValue(requestId, out Action<CitadelRpcResult> onReply))
            {
                // Unknown/duplicate correlation id: nothing is awaiting it.
                Debug.LogWarning($"[Citadel] RPC response for unknown request_id {requestId}; dropped.");
                return;
            }

            _pending.Remove(requestId);

            var result = new CitadelRpcResult(requestId, status == CitadelProtocol.RpcStatusOk, payload);
            try
            {
                onReply?.Invoke(result);
            }
            catch (Exception e)
            {
                // A throwing callback must not wedge the poll loop.
                Debug.LogError($"[Citadel] RPC callback for request_id {requestId} threw: {e}");
            }
        }

        private void Update()
        {
            if (!Input.GetKeyDown(sampleKey))
            {
                return;
            }

            // `add`: two big-endian int32 operands; reply is their int32 sum.
            byte[] addPayload = EncodeTwoBeInt32(sampleAddA, sampleAddB);
            CallRpc("add", addPayload, result =>
            {
                if (!result.Ok)
                {
                    Debug.LogWarning($"[Citadel] add RPC error: {result.PayloadAsText()}");
                    return;
                }

                if (result.TryReadBeInt32(out int sum))
                {
                    Debug.Log($"[Citadel] add({sampleAddA}, {sampleAddB}) = {sum}");
                }
                else
                {
                    Debug.LogWarning("[Citadel] add RPC reply was not a 4-byte int32.");
                }
            });

            // `ping`: liveness check; reply is the text "pong".
            CallRpc("ping", Array.Empty<byte>(), result =>
            {
                if (result.Ok)
                {
                    Debug.Log($"[Citadel] ping RPC -> {result.PayloadAsText()}");
                }
                else
                {
                    Debug.LogWarning($"[Citadel] ping RPC error: {result.PayloadAsText()}");
                }
            });
        }

        private static byte[] EncodeTwoBeInt32(int a, int b)
        {
            var buf = new byte[8];
            WriteBeInt32(buf, 0, a);
            WriteBeInt32(buf, 4, b);
            return buf;
        }

        private static void WriteBeInt32(byte[] buf, int offset, int value)
        {
            buf[offset] = (byte)(value >> 24);
            buf[offset + 1] = (byte)(value >> 16);
            buf[offset + 2] = (byte)(value >> 8);
            buf[offset + 3] = (byte)value;
        }
    }
}
