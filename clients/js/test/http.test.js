import { test } from "node:test";
import assert from "node:assert/strict";

import { CitadelHttpClient, HttpApiError } from "../src/http.js";

function response(status, body) {
  return { status, ok: status >= 200 && status < 300, json: async () => body };
}

test("lifecycle methods use the documented paths, bodies, and bearer rules", async () => {
  const requests = [];
  const client = new CitadelHttpClient("https://citadel.test/", {
    fetch: async (url, init) => {
      requests.push({ url, ...init });
      if (url.endsWith("/v1/session/logout")) return response(204);
      return response(200, { user_id: "u-1", username: "ada", token: "new", refresh_token: "new-r", created: false });
    },
  });

  await client.getAccount("access");
  await client.updateAccount("access", { display_name: null });
  await client.lookupUsers("access", { usernames: ["ada"] });
  await client.authenticateEmail({ email: "ada@example.com", password: "not-logged", create: true, username: "ada" });
  await client.refreshSession("refresh");
  await client.logoutSession({ accessToken: "access", refreshToken: "refresh" });

  assert.deepEqual(requests.map((request) => request.url), [
    "https://citadel.test/v1/account",
    "https://citadel.test/v1/account",
    "https://citadel.test/v1/users/lookup",
    "https://citadel.test/v1/auth/email",
    "https://citadel.test/v1/session/refresh",
    "https://citadel.test/v1/session/logout",
  ]);
  assert.equal(requests[0].headers.authorization, "Bearer access");
  assert.equal(requests[1].method, "PATCH");
  assert.equal(requests[2].body, '{"usernames":["ada"]}');
  assert.equal(requests[3].headers.authorization, undefined);
  assert.equal(requests[3].body, '{"email":"ada@example.com","password":"not-logged","create":true,"username":"ada"}');
  assert.equal(requests[4].headers.authorization, undefined);
  assert.equal(requests[4].body, '{"refresh_token":"refresh"}');
  assert.equal(requests[5].headers.authorization, "Bearer access");
  assert.equal(requests[5].body, '{"refresh_token":"refresh"}');
});

test("sanitized server errors remain inspectable without exposing request data", async () => {
  const client = new CitadelHttpClient("https://citadel.test", {
    fetch: async () => response(401, { code: "authentication_failed", message: "authentication failed" }),
  });
  await assert.rejects(
    client.getAccount("secret-token"),
    (error) => error instanceof HttpApiError
      && error.status === 401
      && error.code === "authentication_failed"
      && error.message === "authentication failed",
  );
});
