// The locally controlled cube: WASD/arrow input moves it and reports its
// position to the server as KIND_POSITION. The server (via game/main.lua or the
// built-in relay) rebroadcasts it to peers as KIND_PEER_POSITION.
//
// The wire is 2D (x, y). This maps world X -> Unity X and world Y -> Unity Z so
// the cube slides on the ground plane; peers use the same mapping.

using UnityEngine;

namespace Citadel.Demo
{
    /// <summary>
    /// Drives the local player cube from input and streams its position to the
    /// server each network tick. Send is unreliable (positions are hot-path
    /// state), mirroring the native demo.
    /// </summary>
    [RequireComponent(typeof(Transform))]
    public sealed class LocalPlayer : MonoBehaviour
    {
        [Tooltip("The scene connection that owns the Citadel client.")]
        public CitadelConnection connection;

        [Tooltip("Movement speed in world units per second.")]
        public float speed = 6f;

        [Tooltip("Half-extent of the play area; position is clamped to [-limit, limit].")]
        public float limit = 9f;

        [Tooltip("How often to send the position, in seconds.")]
        public float sendInterval = 0.05f;

        private float _sendTimer;

        private void Update()
        {
            if (connection == null)
            {
                return;
            }

            float dt = Time.deltaTime;

            // Input -> local movement on the X/Z ground plane.
            float dx = 0f;
            float dy = 0f;
            if (Input.GetKey(KeyCode.W) || Input.GetKey(KeyCode.UpArrow))
            {
                dy += 1f;
            }

            if (Input.GetKey(KeyCode.S) || Input.GetKey(KeyCode.DownArrow))
            {
                dy -= 1f;
            }

            if (Input.GetKey(KeyCode.A) || Input.GetKey(KeyCode.LeftArrow))
            {
                dx -= 1f;
            }

            if (Input.GetKey(KeyCode.D) || Input.GetKey(KeyCode.RightArrow))
            {
                dx += 1f;
            }

            Vector3 p = transform.position;
            p.x = Mathf.Clamp(p.x + dx * speed * dt, -limit, limit);
            p.z = Mathf.Clamp(p.z + dy * speed * dt, -limit, limit);
            transform.position = p;

            // Send at a fixed cadence once connected.
            if (!connection.IsConnected)
            {
                return;
            }

            _sendTimer += dt;
            if (_sendTimer < sendInterval)
            {
                return;
            }

            _sendTimer = 0f;
            byte[] body = CitadelProtocol.EncodePosition(p.x, p.z);
            CitadelStatus status = connection.Client.Send(
                CitadelProtocol.KindPosition,
                body,
                reliable: false);

            if (status != CitadelStatus.Ok && status != CitadelStatus.Again)
            {
                Debug.LogWarning($"[Citadel] send failed: {status} ({connection.Client.LastError()})");
            }
        }
    }
}
