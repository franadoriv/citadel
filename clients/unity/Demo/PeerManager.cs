// Polls the Citadel client each frame, decodes relayed peer positions
// (KIND_PEER_POSITION), and spawns/moves one cube per remote sender id.
//
// Poll happens on the main thread in Update — the C ABI poll is non-blocking,
// so this is safe and simple (no background thread required for the demo).
//
// This is the SINGLE owner of the poll loop: the native queue is shared across
// kinds, so one place must drain it and dispatch by kind. Peer positions are
// handled here; RPC responses (KIND_RPC_RESPONSE) are forwarded to the optional
// RpcClient so a reply is never mistaken for a peer position (or vice versa).

using System.Collections.Generic;
using UnityEngine;

namespace Citadel.Demo
{
    /// <summary>
    /// Renders other players. Each distinct relayed sender id gets its own cube,
    /// created on first sight and moved on every subsequent update.
    /// </summary>
    public sealed class PeerManager : MonoBehaviour
    {
        [Tooltip("The scene connection that owns the Citadel client.")]
        public CitadelConnection connection;

        [Tooltip("Optional RPC helper; polled responses are dispatched to it by kind.")]
        public RpcClient rpcClient;

        [Tooltip("Optional room helper; room frames are forwarded to it by the single poll loop.")]
        public CitadelRooms rooms;

        /// <summary>Durable chat helper fed by this component's one poll loop.</summary>
        public CitadelChatLive chatLive;

        [Tooltip("Optional prefab for a peer. If null, a primitive cube is created.")]
        public GameObject peerPrefab;

        [Tooltip("Max inbound envelopes to drain per frame (bounds work per frame).")]
        public int maxPollsPerFrame = 64;

        private readonly Dictionary<ulong, Transform> _peers = new Dictionary<ulong, Transform>();

        // The C ABI consumes one envelope per poll even when the destination is
        // too small. Match the protocol's bounded native/Unreal envelope cap so
        // valid history responses are never consumed as truncated 8 KiB frames.
        private readonly byte[] _buffer = new byte[8 * 1024 * 1024];

        private void Awake()
        {
            if (chatLive == null && rpcClient != null)
            {
                chatLive = new CitadelChatLive((method, payload, callback) =>
                    rpcClient.CallRpc(method, payload, result =>
                        callback?.Invoke(result.Ok ? result.Payload : System.Array.Empty<byte>())), 64);
            }
        }

        private void Update()
        {
            if (connection == null || !connection.IsConnected)
            {
                return;
            }

            CitadelClient client = connection.Client;

            for (int i = 0; i < maxPollsPerFrame; i++)
            {
                PollResult result = client.Poll(_buffer, out ushort kind, out int length, out bool truncated);
                if (result == PollResult.Again)
                {
                    break;
                }

                if (result == PollResult.Disconnected)
                {
                    rpcClient?.FailAllPending("transport disconnected");
                    chatLive?.OnDisconnected();
                    Debug.LogWarning("[Citadel] server disconnected");
                    break;
                }

                if (truncated)
                {
                    // Poll already consumed this envelope, so correlation cannot
                    // be recovered. Fail all RPCs and consume chat admission.
                    rpcClient?.FailAllPending($"consumed oversized RPC envelope ({length} bytes)");
                    chatLive?.OnDisconnected();
                    Debug.LogError($"[Citadel] consumed oversized envelope ({length} bytes); pending RPCs failed closed");
                    continue;
                }

                if (kind == CitadelProtocol.KindRpcResponse)
                {
                    // Correlated RPC reply: hand it to the RPC helper, if present.
                    rpcClient?.HandleResponse(_buffer, length);
                    continue;
                }

                if (chatLive != null)
                {
                    var payload = new byte[length];
                    System.Buffer.BlockCopy(_buffer, 0, payload, 0, length);
                    if (chatLive.HandleEnvelope(kind, payload))
                    {
                        continue;
                    }
                }

                if (rooms != null && rooms.HandleEnvelope(kind, _buffer, length))
                {
                    continue;
                }

                if (kind != CitadelProtocol.KindPeerPosition)
                {
                    continue;
                }

                if (CitadelProtocol.TryDecodePeerPosition(_buffer, length, out ulong senderId, out float x, out float y))
                {
                    UpdatePeer(senderId, x, y);
                }
            }
        }

        private void UpdatePeer(ulong senderId, float x, float y)
        {
            if (!_peers.TryGetValue(senderId, out Transform peer) || peer == null)
            {
                peer = CreatePeer(senderId);
                _peers[senderId] = peer;
            }

            // World (x, y) -> Unity (x, 0, y), same mapping the local player uses.
            peer.position = new Vector3(x, 0f, y);
        }

        private Transform CreatePeer(ulong senderId)
        {
            GameObject go;
            if (peerPrefab != null)
            {
                go = Instantiate(peerPrefab);
            }
            else
            {
                go = GameObject.CreatePrimitive(PrimitiveType.Cube);
                Renderer r = go.GetComponent<Renderer>();
                if (r != null)
                {
                    // A distinct color so peers read differently from the local cube.
                    r.material.color = new Color(0.36f, 0.84f, 0.44f);
                }
            }

            go.name = $"Peer {senderId}";
            go.transform.SetParent(transform, worldPositionStays: false);
            return go.transform;
        }
    }
}
