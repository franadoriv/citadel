-- Aggregate-only authoritative telemetry. Never stores marker text, event
-- payloads, participant or account identity, replies, script commands,
-- corrected values, or the recorder's process-local decision sequence.
-- `match_id` is the durable server-minted match identity, never the raw
-- process-local room correlation, and carries no foreign key (see the `matches`
-- migration for why).
CREATE TABLE IF NOT EXISTS telemetry_slice_reports (
    report_id       TEXT PRIMARY KEY,
    node_id         TEXT NOT NULL,
    match_id        TEXT,
    context_kind    TEXT NOT NULL,
    close_reason    TEXT NOT NULL
        CHECK (close_reason IN ('restarted','active_cap','marker_cap','ttl','finished')),
    closed_at_ms    INTEGER NOT NULL,
    duration_ms     INTEGER NOT NULL,
    marker_total    INTEGER NOT NULL,
    truncated       INTEGER NOT NULL,
    accepted_total  INTEGER NOT NULL,
    rejected_total  INTEGER NOT NULL,
    corrected_total INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS telemetry_slice_reports_match_idx     ON telemetry_slice_reports (match_id, report_id);
CREATE INDEX IF NOT EXISTS telemetry_slice_reports_retention_idx ON telemetry_slice_reports (closed_at_ms);
