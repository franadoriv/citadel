CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY CHECK (
        length(id) = 32
        AND id NOT GLOB '*[^0-9a-f]*'
    ),
    name TEXT NOT NULL CHECK (
        length(CAST(name AS BLOB)) BETWEEN 1 AND 128
        AND name = trim(name)
    ),
    scopes_json TEXT NOT NULL CHECK (
        json_valid(scopes_json)
        AND json_type(scopes_json) = 'array'
        AND json_array_length(scopes_json) > 0
    ),
    secret_verifier BLOB NOT NULL CHECK (
        typeof(secret_verifier) = 'blob'
        AND length(secret_verifier) = 32
    ),
    generation INTEGER NOT NULL CHECK (generation > 0),
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    expires_at_ms INTEGER CHECK (expires_at_ms > created_at_ms),
    revoked_at_ms INTEGER CHECK (revoked_at_ms >= created_at_ms),
    last_used_at_ms INTEGER CHECK (last_used_at_ms >= created_at_ms)
);
CREATE INDEX IF NOT EXISTS api_keys_created_at_idx
    ON api_keys (created_at_ms DESC, id);
CREATE INDEX IF NOT EXISTS api_keys_active_idx
    ON api_keys (revoked_at_ms, expires_at_ms);
