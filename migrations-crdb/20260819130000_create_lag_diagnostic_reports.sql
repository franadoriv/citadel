-- Cockroach-compatible report-only persistence. Raw artifact storage remains
-- outside SQL and is referenced only by a digest identity in derived reports.
CREATE TABLE IF NOT EXISTS lag_diagnostic_reports (
    report_id STRING PRIMARY KEY,
    capture_id STRING NOT NULL,
    generation INT8 NOT NULL,
    artifact_digest_sha256 STRING NOT NULL,
    decoder_version INT4 NOT NULL,
    analyzer_version INT4 NOT NULL,
    options_hash STRING NOT NULL,
    status STRING NOT NULL,
    raw_available BOOL NOT NULL,
    report_json JSONB NOT NULL,
    UNIQUE (capture_id, generation, artifact_digest_sha256, analyzer_version, options_hash)
);

CREATE INDEX IF NOT EXISTS lag_diagnostic_reports_capture_idx
ON lag_diagnostic_reports (capture_id, generation, report_id);
