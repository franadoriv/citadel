-- Compact derived diagnostics only. Raw CLAG bytes and every upload/storage
-- locator stay in the private filesystem service, never in this database.
CREATE TABLE IF NOT EXISTS lag_diagnostic_reports (
    report_id TEXT PRIMARY KEY,
    capture_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    artifact_digest_sha256 TEXT NOT NULL,
    decoder_version INTEGER NOT NULL,
    analyzer_version INTEGER NOT NULL,
    options_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    raw_available INTEGER NOT NULL,
    report_json TEXT NOT NULL,
    UNIQUE (capture_id, generation, artifact_digest_sha256, analyzer_version, options_hash)
);

CREATE INDEX IF NOT EXISTS lag_diagnostic_reports_capture_idx
ON lag_diagnostic_reports (capture_id, generation, report_id);
