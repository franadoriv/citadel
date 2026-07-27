// Player HTTP lifecycle client for Citadel's browser/Node SDK.
//
// This deliberately stays separate from CitadelClient, which owns the realtime
// WebSocket transport. It accepts a fetch implementation so it works in a
// browser, Node >= 22, and test environments without a global fetch.

/** Error returned by a Citadel player HTTP endpoint. */
export class HttpApiError extends Error {
  /**
   * @param {number} status
   * @param {string} code
   * @param {string} message
   */
  constructor(status, code, message) {
    super(message);
    this.name = "HttpApiError";
    this.status = status;
    this.code = code;
  }
}

/**
 * Typed wrapper for Citadel's player account and session HTTP endpoints.
 * Tokens are inputs and results rather than mutable client state: callers must
 * persist a refreshed token pair atomically in their platform's secure store.
 */
export class CitadelHttpClient {
  /**
   * @param {string} baseUrl HTTP origin, for example "https://game.example".
   * @param {{ fetch?: typeof fetch }} [opts]
   */
  constructor(baseUrl, opts = {}) {
    this._baseUrl = baseUrl.replace(/\/$/, "");
    this._fetch = opts.fetch || globalThis.fetch;
    if (!this._fetch) throw new Error("no fetch implementation; pass opts.fetch");
  }

  /**
   * Register (`create: true`) or sign in with an email and password.
   * The returned tokens are caller-owned; do not log or persist the password.
   * @param {{ email: string, password: string, create?: boolean, username?: string }} request
   * @returns {Promise<SessionTokenPair>}
   */
  authenticateEmail(request) {
    return this._request("/v1/auth/email", { method: "POST", body: request });
  }

  /** @param {string} accessToken @returns {Promise<PublicProfile>} */
  getAccount(accessToken) {
    return this._request("/v1/account", { accessToken });
  }

  /** @param {string} accessToken @param {{ username?: string, display_name?: string | null }} patch @returns {Promise<PublicProfile>} */
  updateAccount(accessToken, patch) {
    return this._request("/v1/account", { method: "PATCH", accessToken, body: patch });
  }

  /** @param {string} accessToken @param {{ user_ids?: string[], usernames?: string[] }} query @returns {Promise<{ users: PublicProfile[] }>} */
  lookupUsers(accessToken, query) {
    return this._request("/v1/users/lookup", { method: "POST", accessToken, body: query });
  }

  /** @param {string} refreshToken @returns {Promise<SessionTokenPair>} */
  refreshSession(refreshToken) {
    // Refresh credentials deliberately never share an Authorization header.
    return this._request("/v1/session/refresh", {
      method: "POST",
      body: { refresh_token: refreshToken },
    });
  }

  /**
   * Revoke exactly one session. Supply its access token, refresh token, or
   * both. A successful retry resolves to undefined because the route is 204.
   * @param {{ accessToken?: string, refreshToken?: string }} tokens
   * @returns {Promise<void>}
   */
  async logoutSession(tokens = {}) {
    await this._request("/v1/session/logout", {
      method: "POST",
      accessToken: tokens.accessToken,
      body: tokens.refreshToken ? { refresh_token: tokens.refreshToken } : undefined,
    });
  }

  async _request(path, { method = "GET", accessToken, body } = {}) {
    const headers = { accept: "application/json" };
    if (accessToken) headers.authorization = `Bearer ${accessToken}`;
    if (body !== undefined) headers["content-type"] = "application/json";

    let response;
    try {
      response = await this._fetch(`${this._baseUrl}${path}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch {
      throw new HttpApiError(0, "transport_error", "request failed");
    }

    if (response.status === 204) return undefined;
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      const code = typeof payload?.code === "string" ? payload.code : "http_error";
      const message = typeof payload?.message === "string" ? payload.message : "request failed";
      throw new HttpApiError(response.status, code, message);
    }
    return payload;
  }
}

/** @typedef {{ user_id: string, username: string, display_name?: string }} PublicProfile */
/** @typedef {{ token: string, refresh_token?: string, user_id: string, username: string, created: boolean }} SessionTokenPair */
