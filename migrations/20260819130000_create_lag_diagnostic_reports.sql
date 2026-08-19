-- Report-only persistence: no raw bytes/chunks/rows, storage paths, MIME,
-- filenames, upload grants, JTI, payloads, IP addresses, or user agents.
CREATE TABLE IF NOT EXISTS lag_diagnostic_reports (
    report_id TEXT PRIMARY KEY,
    capture_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    artifact_digest_sha256 TEXT NOT NULL,
    decoder_version INTEGER NOT NULL,
    analyzer_version INTEGER NOT NULL,
    options_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    raw_available BOOLEAN NOT NULL,
    report_json JSONB NOT NULL,
    UNIQUE (capture_id, generation, artifact_digest_sha256, analyzer_version, options_hash)
);

CREATE INDEX IF NOT EXISTS lag_diagnostic_reports_capture_idx
ON lag_diagnostic_reports (capture_id, generation, report_id);
