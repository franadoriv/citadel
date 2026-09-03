-- Aggregate-only authoritative telemetry. Never stores marker text, event
-- payloads, participant or account identity, replies, script commands,
-- corrected values, or the recorder's process-local decision sequence.
-- `match_id` is the durable server-minted match identity, never the raw
-- process-local room correlation.
CREATE TABLE IF NOT EXISTS telemetry_slice_reports (
    report_id       TEXT COLLATE "C" PRIMARY KEY,
    node_id         TEXT COLLATE "C" NOT NULL,
    match_id        TEXT COLLATE "C",
    context_kind    TEXT COLLATE "C" NOT NULL,
    close_reason    TEXT COLLATE "C" NOT NULL
        CHECK (close_reason IN ('restarted','active_cap','marker_cap','ttl','finished')),
    closed_at_ms    BIGINT NOT NULL,
    duration_ms     BIGINT NOT NULL,
    marker_total    INTEGER NOT NULL,
    truncated       BOOLEAN NOT NULL,
    accepted_total  BIGINT NOT NULL,
    rejected_total  BIGINT NOT NULL,
    corrected_total BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS telemetry_slice_reports_match_idx     ON telemetry_slice_reports (match_id, report_id);
CREATE INDEX IF NOT EXISTS telemetry_slice_reports_retention_idx ON telemetry_slice_reports (closed_at_ms);
