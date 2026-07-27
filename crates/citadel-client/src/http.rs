//! Typed player account and session HTTP client.
//!
//! [`CitadelHttpClient`] is intentionally separate from the realtime transport
//! clients. It never stores a bearer or refresh secret: applications pass
//! credentials to each call and must atomically replace their stored token pair
//! after [`CitadelHttpClient::refresh_session`] succeeds.

use reqwest::{Client, RequestBuilder, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{ClientError, ClientResult};

const ACCOUNT_PATH: &str = "v1/account";
const EMAIL_AUTH_PATH: &str = "v1/auth/email";
const LOOKUP_PATH: &str = "v1/users/lookup";
const REFRESH_PATH: &str = "v1/session/refresh";
const LOGOUT_PATH: &str = "v1/session/logout";

/// The privacy-preserving public player profile returned by the player API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicProfile {
    pub user_id: String,
    pub username: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

/// A request to change the current account's mutable public fields.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct UpdateAccountRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// `Some(None)` serializes as `null`, which clears the display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<Option<String>>,
}

/// Exact known-player keys. This is not a user-directory search request.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct LookupUsersRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub user_ids: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub usernames: Vec<String>,
}

/// The exact known-player lookup result.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LookupUsersResponse {
    pub users: Vec<PublicProfile>,
}

/// Access/refresh credentials issued by authentication or session refresh.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SessionTokenPair {
    pub token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    pub user_id: String,
    pub username: String,
    pub created: bool,
}

/// Email/password credentials for account registration or sign-in.
///
/// The password is intentionally omitted from [`Debug`] output. Keep this
/// value short-lived and never serialize it outside the HTTPS request.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct EmailAuthenticationRequest {
    pub email: String,
    pub password: String,
    #[serde(skip_serializing_if = "is_false")]
    pub create: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

fn is_false(value: &bool) -> bool {
    !value
}

impl std::fmt::Debug for EmailAuthenticationRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmailAuthenticationRequest")
            .field("email", &"[redacted]")
            .field("password", &"[redacted]")
            .field("create", &self.create)
            .field("username", &self.username)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
    message: String,
}

/// Typed wrapper for Citadel's player account and session HTTP API.
#[derive(Clone, Debug)]
pub struct CitadelHttpClient {
    base_url: Url,
    client: Client,
}

impl CitadelHttpClient {
    /// Create a client for an HTTP origin such as `https://game.example/`.
    pub fn new(base_url: &str) -> ClientResult<Self> {
        let mut base_url = Url::parse(base_url)
            .map_err(|error| ClientError::Config(format!("invalid HTTP base URL: {error}")))?;
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Ok(Self {
            base_url,
            client: Client::new(),
        })
    }

    /// Fetch the authenticated caller's public profile.
    pub async fn get_account(&self, access_token: &str) -> ClientResult<PublicProfile> {
        self.decode_json(self.authorized(self.client.get(self.url(ACCOUNT_PATH)?), access_token))
            .await
    }

    /// Update the authenticated caller's mutable public profile fields.
    pub async fn update_account(
        &self,
        access_token: &str,
        patch: &UpdateAccountRequest,
    ) -> ClientResult<PublicProfile> {
        self.decode_json(
            self.authorized(self.client.patch(self.url(ACCOUNT_PATH)?), access_token)
                .json(patch),
        )
        .await
    }

    /// Resolve explicit known ids/usernames without exposing a player directory.
    pub async fn lookup_users(
        &self,
        access_token: &str,
        query: &LookupUsersRequest,
    ) -> ClientResult<LookupUsersResponse> {
        self.decode_json(
            self.authorized(self.client.post(self.url(LOOKUP_PATH)?), access_token)
                .json(query),
        )
        .await
    }

    /// Register (`create: true`) or sign in with an email and password.
    /// The result's tokens are caller-owned and should be stored securely.
    pub async fn authenticate_email(
        &self,
        request: &EmailAuthenticationRequest,
    ) -> ClientResult<SessionTokenPair> {
        self.decode_json(self.client.post(self.url(EMAIL_AUTH_PATH)?).json(request))
            .await
    }

    /// Rotate a refresh secret into a replacement token pair. No bearer header
    /// is sent with this request.
    pub async fn refresh_session(&self, refresh_token: &str) -> ClientResult<SessionTokenPair> {
        self.decode_json(
            self.client
                .post(self.url(REFRESH_PATH)?)
                .json(&serde_json::json!({
                    "refresh_token": refresh_token,
                })),
        )
        .await
    }

    /// Revoke one session using its access token, refresh token, or both. The
    /// server's `204 No Content` response makes a successful retry idempotent.
    pub async fn logout_session(
        &self,
        access_token: Option<&str>,
        refresh_token: Option<&str>,
    ) -> ClientResult<()> {
        let request = self.client.post(self.url(LOGOUT_PATH)?);
        let request = match access_token {
            Some(token) => self.authorized(request, token),
            None => request,
        };
        let request = match refresh_token {
            Some(token) => request.json(&serde_json::json!({ "refresh_token": token })),
            None => request,
        };
        self.decode_empty(request).await
    }

    fn url(&self, path: &str) -> ClientResult<Url> {
        self.base_url.join(path).map_err(|error| {
            ClientError::Config(format!("could not construct player API URL: {error}"))
        })
    }

    fn authorized(&self, request: RequestBuilder, access_token: &str) -> RequestBuilder {
        request.bearer_auth(access_token)
    }

    async fn decode_json<T: DeserializeOwned>(&self, request: RequestBuilder) -> ClientResult<T> {
        let response = self.send(request).await?;
        response.json::<T>().await.map_err(|_| ClientError::Http {
            status: Some(200),
            code: "invalid_response".to_string(),
            message: "server returned an invalid response".to_string(),
        })
    }

    async fn decode_empty(&self, request: RequestBuilder) -> ClientResult<()> {
        self.send(request).await.map(|_| ())
    }

    async fn send(&self, request: RequestBuilder) -> ClientResult<reqwest::Response> {
        let response = request.send().await.map_err(|_| ClientError::Http {
            status: None,
            code: "transport_error".to_string(),
            message: "request failed".to_string(),
        })?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let error = response.json::<ErrorBody>().await.ok();
        Err(ClientError::Http {
            status: Some(status),
            code: error
                .as_ref()
                .map_or_else(|| "http_error".to_string(), |body| body.code.clone()),
            message: error.map_or_else(|| "request failed".to_string(), |body| body.message),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_and_paths_preserve_a_deployment_prefix() {
        let client = CitadelHttpClient::new("https://citadel.example/api").expect("valid URL");
        assert_eq!(
            client.url(ACCOUNT_PATH).expect("account URL").as_str(),
            "https://citadel.example/api/v1/account"
        );
    }

    #[test]
    fn update_request_distinguishes_absent_and_cleared_display_name() {
        let absent =
            serde_json::to_value(UpdateAccountRequest::default()).expect("serializable request");
        let cleared = serde_json::to_value(UpdateAccountRequest {
            username: None,
            display_name: Some(None),
        })
        .expect("serializable request");
        assert_eq!(absent, serde_json::json!({}));
        assert_eq!(cleared, serde_json::json!({ "display_name": null }));
    }

    #[test]
    fn email_auth_request_redacts_secrets_and_omits_false_create() {
        let request = EmailAuthenticationRequest {
            email: "ada@example.com".to_string(),
            password: "not-logged".to_string(),
            create: false,
            username: None,
        };
        assert!(!format!("{request:?}").contains("not-logged"));
        assert_eq!(
            serde_json::to_value(request).expect("serializable request"),
            serde_json::json!({ "email": "ada@example.com", "password": "not-logged" })
        );
    }
}
