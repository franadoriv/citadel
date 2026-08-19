-- Current raw availability is a separate projection from an immutable report.
-- This table stores no raw bytes, paths, handles, client identifiers, or
-- tokens. It closes the race where retention/deletion precedes report insert.
CREATE TABLE IF NOT EXISTS lag_diagnostic_raw_tombstones (
    capture_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    artifact_digest_sha256 TEXT NOT NULL,
    PRIMARY KEY (capture_id, generation, artifact_digest_sha256)
);
