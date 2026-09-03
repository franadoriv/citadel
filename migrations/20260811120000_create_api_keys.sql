CREATE TABLE IF NOT EXISTS api_keys (
    id text PRIMARY KEY CHECK (id ~ '^[0-9a-f]{32}$'),
    name text NOT NULL CHECK (
        octet_length(name) BETWEEN 1 AND 128
        AND name = btrim(name)
    ),
    scopes_json text NOT NULL CHECK (octet_length(scopes_json) > 2),
    secret_verifier bytea NOT NULL CHECK (octet_length(secret_verifier) = 32),
    generation bigint NOT NULL CHECK (generation > 0),
    created_at_ms bigint NOT NULL CHECK (created_at_ms >= 0),
    expires_at_ms bigint CHECK (expires_at_ms > created_at_ms),
    revoked_at_ms bigint CHECK (revoked_at_ms >= created_at_ms),
    last_used_at_ms bigint CHECK (last_used_at_ms >= created_at_ms)
);
CREATE INDEX IF NOT EXISTS api_keys_created_at_idx
    ON api_keys (created_at_ms DESC, id);
CREATE INDEX IF NOT EXISTS api_keys_active_idx
    ON api_keys (revoked_at_ms, expires_at_ms);
