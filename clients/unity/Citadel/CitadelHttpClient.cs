// Typed player account/session HTTP client for Unity.
//
// Kept separate from CitadelClient's realtime native transport. Token strings
// are explicit inputs/outputs so the game can commit a refreshed pair to its
// platform secure storage atomically.
using System;
using System.Text;
using System.Threading.Tasks;
using UnityEngine;
using UnityEngine.Networking;

namespace Citadel
{
    [Serializable]
    public sealed class PublicProfile
    {
        public string user_id;
        public string username;
        public string display_name;
    }

    [Serializable]
    public sealed class LookupUsersResponse
    {
        public PublicProfile[] users;
    }

    [Serializable]
    public sealed class SessionTokenPair
    {
        public string token;
        public string refresh_token;
        public string user_id;
        public string username;
        public bool created;
    }

    /// <summary>Email/password credentials for registration or sign-in. Do not log this object.</summary>
    public sealed class EmailAuthenticationRequest
    {
        public string Email { get; set; }
        public string Password { get; set; }
        public bool Create { get; set; }
        public string Username { get; set; }

        internal string ToJson()
        {
            if (Email == null) throw new ArgumentNullException(nameof(Email));
            if (Password == null) throw new ArgumentNullException(nameof(Password));
            var fields = new System.Collections.Generic.List<string> {
                "\"email\":" + Json(Email), "\"password\":" + Json(Password)
            };
            if (Create) fields.Add("\"create\":true");
            if (Username != null) fields.Add("\"username\":" + Json(Username));
            return "{" + string.Join(",", fields) + "}";
        }

        public override string ToString() => "EmailAuthenticationRequest { Email = [redacted], Password = [redacted] }";

        private static string Json(string value)
        {
            string wrapped = JsonUtility.ToJson(new JsonString { value = value });
            const int prefixLength = 9; // {"value":
            return wrapped.Substring(prefixLength, wrapped.Length - prefixLength - 1);
        }
        [Serializable] private sealed class JsonString { public string value; }
    }

    /// <summary>Sanitized error returned by a Citadel player HTTP endpoint.</summary>
    public sealed class CitadelHttpException : Exception
    {
        public long StatusCode { get; }
        public string Code { get; }

        public CitadelHttpException(long statusCode, string code, string message)
            : base(message)
        {
            StatusCode = statusCode;
            Code = code;
        }
    }

    /// <summary>
    /// An optional account-profile update. Set <see cref="ClearDisplayName"/>
    /// to send JSON <c>null</c>; otherwise null properties are omitted.
    /// </summary>
    public sealed class UpdateAccountRequest
    {
        public string Username { get; set; }
        public string DisplayName { get; set; }
        public bool ClearDisplayName { get; set; }

        internal string ToJson()
        {
            var fields = new System.Collections.Generic.List<string>();
            if (Username != null) fields.Add("\"username\":" + Json(Username));
            if (ClearDisplayName) fields.Add("\"display_name\":null");
            else if (DisplayName != null) fields.Add("\"display_name\":" + Json(DisplayName));
            return "{" + string.Join(",", fields) + "}";
        }

        private static string Json(string value)
        {
            string wrapped = JsonUtility.ToJson(new JsonString { value = value });
            const int prefixLength = 9; // {"value":
            return wrapped.Substring(prefixLength, wrapped.Length - prefixLength - 1);
        }
        [Serializable] private sealed class JsonString { public string value; }
    }

    /// <summary>Exact lookup keys; this is never a public directory search.</summary>
    public sealed class LookupUsersRequest
    {
        public string[] UserIds { get; set; }
        public string[] Usernames { get; set; }

        internal string ToJson()
        {
            var fields = new System.Collections.Generic.List<string>();
            if (UserIds != null && UserIds.Length > 0) fields.Add("\"user_ids\":" + JsonUtility.ToJson(new Strings { values = UserIds }).Substring(10).TrimEnd('}'));
            if (Usernames != null && Usernames.Length > 0) fields.Add("\"usernames\":" + JsonUtility.ToJson(new Strings { values = Usernames }).Substring(10).TrimEnd('}'));
            return "{" + string.Join(",", fields) + "}";
        }
        [Serializable] private sealed class Strings { public string[] values; }
    }

    /// <summary>First-class account and session calls over UnityWebRequest.</summary>
    public sealed class CitadelHttpClient
    {
        private readonly string _baseUrl;

        public CitadelHttpClient(string baseUrl)
        {
            if (string.IsNullOrEmpty(baseUrl)) throw new ArgumentException("baseUrl must not be empty", nameof(baseUrl));
            _baseUrl = baseUrl.TrimEnd('/');
        }

        public Task<PublicProfile> GetAccountAsync(string accessToken) => SendJson<PublicProfile>("GET", "/v1/account", accessToken, null);
        public Task<PublicProfile> UpdateAccountAsync(string accessToken, UpdateAccountRequest patch) => SendJson<PublicProfile>("PATCH", "/v1/account", accessToken, patch?.ToJson() ?? throw new ArgumentNullException(nameof(patch)));
        public Task<LookupUsersResponse> LookupUsersAsync(string accessToken, LookupUsersRequest query) => SendJson<LookupUsersResponse>("POST", "/v1/users/lookup", accessToken, query?.ToJson() ?? throw new ArgumentNullException(nameof(query)));
        /// <summary>Register (<c>Create=true</c>) or sign in with an email/password account.</summary>
        public Task<SessionTokenPair> AuthenticateEmailAsync(EmailAuthenticationRequest request) => SendJson<SessionTokenPair>("POST", "/v1/auth/email", null, request?.ToJson() ?? throw new ArgumentNullException(nameof(request)));
        public Task<SessionTokenPair> RefreshSessionAsync(string refreshToken) => SendJson<SessionTokenPair>("POST", "/v1/session/refresh", null, "{\"refresh_token\":" + Quote(refreshToken) + "}");
        public Task LogoutSessionAsync(string accessToken = null, string refreshToken = null) => SendEmpty("POST", "/v1/session/logout", accessToken, refreshToken == null ? null : "{\"refresh_token\":" + Quote(refreshToken) + "}");

        private async Task<T> SendJson<T>(string method, string path, string accessToken, string body)
        {
            using (var request = Create(method, path, accessToken, body))
            {
                await WaitFor(request.SendWebRequest());
                ThrowForError(request);
                try {
                    T result = JsonUtility.FromJson<T>(request.downloadHandler.text);
                    if (result == null) throw new ArgumentException();
                    return result;
                }
                catch { throw new CitadelHttpException(request.responseCode, "invalid_response", "server returned an invalid response"); }
            }
        }

        private async Task SendEmpty(string method, string path, string accessToken, string body)
        {
            using (var request = Create(method, path, accessToken, body))
            {
                await WaitFor(request.SendWebRequest());
                ThrowForError(request);
            }
        }

        private UnityWebRequest Create(string method, string path, string accessToken, string body)
        {
            var request = new UnityWebRequest(_baseUrl + path, method) { downloadHandler = new DownloadHandlerBuffer() };
            request.SetRequestHeader("Accept", "application/json");
            if (!string.IsNullOrEmpty(accessToken)) request.SetRequestHeader("Authorization", "Bearer " + accessToken);
            if (body != null) { request.uploadHandler = new UploadHandlerRaw(Encoding.UTF8.GetBytes(body)); request.SetRequestHeader("Content-Type", "application/json"); }
            return request;
        }

        private static void ThrowForError(UnityWebRequest request)
        {
            if (request.result == UnityWebRequest.Result.Success) return;
            if (request.responseCode == 0)
                throw new CitadelHttpException(0, "transport_error", "request failed");
            try
            {
                ErrorPayload error = JsonUtility.FromJson<ErrorPayload>(request.downloadHandler.text);
                if (error != null && !string.IsNullOrEmpty(error.code) && !string.IsNullOrEmpty(error.message))
                    throw new CitadelHttpException(request.responseCode, error.code, error.message);
            }
            catch (CitadelHttpException) { throw; }
            catch { }
            throw new CitadelHttpException(request.responseCode, "http_error", "request failed");
        }

        private static async Task WaitFor(UnityWebRequestAsyncOperation operation)
        {
            while (!operation.isDone) await Task.Yield();
        }

        private static string Quote(string value)
        {
            string wrapped = JsonUtility.ToJson(new JsonString { value = value });
            const int prefixLength = 9; // {"value":
            return wrapped.Substring(prefixLength, wrapped.Length - prefixLength - 1);
        }

        [Serializable] private sealed class JsonString { public string value; }
        [Serializable] private sealed class ErrorPayload { public string code; public string message; }
    }
}
