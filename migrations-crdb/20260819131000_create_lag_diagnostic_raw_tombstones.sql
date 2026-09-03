-- Current raw availability is a separate projection from an immutable report.
-- This table stores no raw bytes, paths, handles, client identifiers, or
-- tokens. It closes the race where retention/deletion precedes report insert.
CREATE TABLE IF NOT EXISTS lag_diagnostic_raw_tombstones (
    capture_id STRING NOT NULL,
    generation INT8 NOT NULL,
    artifact_digest_sha256 STRING NOT NULL,
    PRIMARY KEY (capture_id, generation, artifact_digest_sha256)
);
