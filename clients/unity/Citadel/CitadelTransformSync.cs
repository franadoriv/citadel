// Unity engine surface for the shared transform-sync runtime.
//
// Snapshot decoding, delta-baseline recovery, Hermite+slerp interpolation, and
// the adaptive buffer all stay inside citadel-client-ffi. This component only
// routes envelopes, sends the acknowledgement, and applies the returned native
// transform to Unity's Transform; it never forks the wire codec.

using System;
using System.Collections.Generic;
using UnityEngine;

namespace Citadel
{
    public sealed class CitadelTransformSync : MonoBehaviour, IDisposable
    {
        [Tooltip("The connection whose single poll dispatcher routes transform envelopes here.")]
        public CitadelClient Client;

        [Tooltip("The replicated transform object id assigned by the server.")]
        public uint ObjectId;

        [Tooltip("Local owners reconcile against the present authoritative state; remote actors render the adaptive-buffer sample.")]
        public bool IsLocalOwner;

        [Tooltip("Ownership epoch from the reliable transform role assignment.")]
        public uint OwnershipEpoch;

        [Tooltip("Hard-snap threshold in Citadel centimetres for owner corrections.")]
        public float HardSnapCentimetres = 100f;

        private IntPtr _view;
        private uint _lastAck;
        private uint _nextInputSeq = 1;
        private readonly List<PendingInput> _pendingInputs = new List<PendingInput>();

        private struct PendingInput
        {
            public uint Sequence;
            public Vector3 Velocity;
            public float Dt;
        }

        /// <summary>
        /// Predict one local movement input immediately and send its sequenced
        /// owner-input frame. Call from the game's input controller only when this
        /// object owns the transform role.
        /// </summary>
        public void SubmitInput(Vector3 velocityMetresPerSecond, float dt)
        {
            if (!IsLocalOwner || Client == null || ObjectId == 0 || dt < 0f)
            {
                return;
            }
            uint sequence = _nextInputSeq++;
            _pendingInputs.Add(new PendingInput { Sequence = sequence, Velocity = velocityMetresPerSecond, Dt = dt });
            transform.position += velocityMetresPerSecond * dt;

            var buffer = new byte[96];
            CitadelStatus result = CitadelNative.citadel_transform_encode_input(
                sequence, sequence, dt, ObjectId, OwnershipEpoch,
                velocityMetresPerSecond.x * 100f, velocityMetresPerSecond.y * 100f, velocityMetresPerSecond.z * 100f,
                buffer, (UIntPtr)buffer.Length, out UIntPtr length, out bool truncated);
            if (result == CitadelStatus.Ok && !truncated)
            {
                Array.Resize(ref buffer, (int)length);
                Client.Send(CitadelProtocol.KindTsyncInput, buffer, reliable: false);
            }
        }

        /// <summary>Route a transform envelope from the connection's one poll loop.</summary>
        public void HandleEnvelope(ushort kind, byte[] body, int length)
        {
            if (body == null || length < 0 || length > body.Length)
            {
                return;
            }

            if (kind == CitadelProtocol.KindTsyncHello)
            {
                DisposeView();
                byte[] hello = Slice(body, length);
                if (CitadelNative.citadel_transform_view_new(hello, (UIntPtr)hello.Length, out _view) != CitadelStatus.Ok)
                {
                    _view = IntPtr.Zero;
                }
                else if (Client != null)
                {
                    // Dedicated negotiation means a v1 server ignores/rejects
                    // this without changing its v1 snapshot layout.
                    Client.Send(CitadelProtocol.KindTsyncV2Hello, new byte[] { 2, 1 }, reliable: true);
                }
                return;
            }

            if ((kind != CitadelProtocol.KindTsyncSnapshot && kind != CitadelProtocol.KindTsyncV2Snapshot) || _view == IntPtr.Zero)
            {
                return;
            }

            byte[] snapshot = Slice(body, length);
            bool applied;
            CitadelStatus status = kind == CitadelProtocol.KindTsyncV2Snapshot
                ? CitadelNative.citadel_transform_view_apply_v2_datagram(_view, snapshot, (UIntPtr)snapshot.Length, out applied)
                : CitadelNative.citadel_transform_view_apply_datagram(_view, snapshot, (UIntPtr)snapshot.Length, out applied);
            if (status != CitadelStatus.Ok || !applied)
            {
                return;
            }

            var ack = new byte[8];
            if (Client != null && CitadelNative.citadel_transform_view_ack(_view, ack) == CitadelStatus.Ok)
            {
                Client.Send(CitadelProtocol.KindTsyncAck, ack, reliable: false);
            }
        }

        private void Update()
        {
            if (_view == IntPtr.Zero || ObjectId == 0)
            {
                return;
            }

            if (IsLocalOwner)
            {
                ApplyOwnerCorrection();
            }
            else
            {
                ApplyRemoteSample();
            }
        }

        private void ApplyRemoteSample()
        {
            if (CitadelNative.citadel_transform_view_sample_now(_view, ObjectId, out var state, out bool found) == CitadelStatus.Ok && found)
            {
                Apply(state);
            }
        }

        private void ApplyOwnerCorrection()
        {
            if (CitadelNative.citadel_transform_view_authoritative_state(
                    _view, ObjectId, out var state, out uint ack, out bool found) != CitadelStatus.Ok || !found || ack <= _lastAck)
            {
                return;
            }
            _lastAck = ack;
            Vector3 authoritative = ToUnityPosition(state);
            _pendingInputs.RemoveAll(input => input.Sequence <= ack);
            Vector3 replayed = authoritative;
            foreach (PendingInput input in _pendingInputs)
            {
                replayed += input.Velocity * input.Dt;
            }
            if (Vector3.Distance(transform.position, authoritative) > HardSnapCentimetres / 100f)
            {
                transform.position = replayed;
            }
            else
            {
                transform.position = Vector3.Lerp(transform.position, replayed, 0.15f);
            }
        }

        private void Apply(CitadelNative.TransformState state)
        {
            transform.SetPositionAndRotation(ToUnityPosition(state), new Quaternion(
                state.rotation[0], state.rotation[1], state.rotation[2], state.rotation[3]));
        }

        // Citadel uses centimetres; Unity world units conventionally use metres.
        private static Vector3 ToUnityPosition(CitadelNative.TransformState state) =>
            new Vector3(state.position[0], state.position[1], state.position[2]) / 100f;

        private static byte[] Slice(byte[] source, int length)
        {
            var destination = new byte[length];
            Buffer.BlockCopy(source, 0, destination, 0, length);
            return destination;
        }

        private void OnDestroy() => Dispose();
        public void Dispose() => DisposeView();

        private void DisposeView()
        {
            if (_view != IntPtr.Zero)
            {
                CitadelNative.citadel_transform_view_free(_view);
                _view = IntPtr.Zero;
            }
        }
    }
}
