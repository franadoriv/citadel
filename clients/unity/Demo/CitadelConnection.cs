// Owns the single Citadel client connection for the demo scene.
//
// Attach this to one GameObject (e.g. "Citadel"). It verifies the native ABI
// version, connects over QUIC on Start, and disposes the handle on destroy.
// The local player and the peer manager reference it to send and poll.

using UnityEngine;

namespace Citadel.Demo
{
    /// <summary>
    /// Scene-level owner of the <see cref="CitadelClient"/>. Connects on
    /// <c>Start</c> using the insecure dev cert (matching the native demo) and
    /// exposes the live client to other behaviours.
    /// </summary>
    public sealed class CitadelConnection : MonoBehaviour
    {
        [Header("Server")]
        [Tooltip("QUIC endpoint, host:port. Matches examples/configs/demo.toml.")]
        public string serverAddress = "127.0.0.1:7351";

        [Tooltip("TLS SNI server name. The dev server uses 'localhost'.")]
        public string serverName = "localhost";

        [Tooltip("Skip certificate verification (required for the self-signed dev cert).")]
        public bool insecure = true;

        /// <summary>The connected client, or null until <c>Start</c> succeeds.</summary>
        public CitadelClient Client { get; private set; }

        /// <summary>True once a connection has been established.</summary>
        public bool IsConnected => Client != null && Client.IsValid;

        private void Start()
        {
            try
            {
                uint abi = CitadelClient.CheckAbiVersion();
                Debug.Log($"[Citadel] native ABI version {abi} OK");
            }
            catch (CitadelException e)
            {
                Debug.LogError($"[Citadel] {e.Message}");
                enabled = false;
                return;
            }
            catch (System.DllNotFoundException)
            {
                Debug.LogError(
                    "[Citadel] native plugin 'citadel_client_ffi' not found. Build it with " +
                    "'make unity-plugin' (from cmd or '.\\make unity-plugin' from PowerShell) " +
                    "so the library lands in the platform-specific Assets/Plugins directory.");
                enabled = false;
                return;
            }

            try
            {
                Client = CitadelClient.ConnectQuic(serverAddress, serverName, insecure);
                AuthHandshakeResult auth = Client.AuthenticateGuest();
                Debug.Log($"[Citadel] connected to {serverAddress} (QUIC), auth={auth.Status}");
            }
            catch (CitadelException e)
            {
                Debug.LogError($"[Citadel] connect failed: {e.Message}");
                enabled = false;
            }
        }

        private void OnDestroy()
        {
            Client?.Dispose();
            Client = null;
        }
    }
}
