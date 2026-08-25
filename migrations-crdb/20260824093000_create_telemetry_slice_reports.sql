-- Cockroach-compatible aggregate-only authoritative telemetry. Never stores
-- marker text, event payloads, participant or account identity, replies,
-- script commands, corrected values, or the recorder's process-local decision
-- sequence. `match_id` is the durable server-minted match identity, never the
-- raw process-local room correlation.
CREATE TABLE IF NOT EXISTS telemetry_slice_reports (
    report_id       STRING PRIMARY KEY,
    node_id         STRING NOT NULL,
    match_id        STRING,
    context_kind    STRING NOT NULL,
    close_reason    STRING NOT NULL
        CHECK (close_reason IN ('restarted','active_cap','marker_cap','ttl','finished')),
    closed_at_ms    INT8 NOT NULL,
    duration_ms     INT8 NOT NULL,
    marker_total    INT4 NOT NULL,
    truncated       BOOL NOT NULL,
    accepted_total  INT8 NOT NULL,
    rejected_total  INT8 NOT NULL,
    corrected_total INT8 NOT NULL
);

CREATE INDEX IF NOT EXISTS telemetry_slice_reports_match_idx     ON telemetry_slice_reports (match_id, report_id);
CREATE INDEX IF NOT EXISTS telemetry_slice_reports_retention_idx ON telemetry_slice_reports (closed_at_ms);
