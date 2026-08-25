-- Optional durable match reference for a derived lag report. Nullable: reports
-- produced before match plumbing, or outside a match, carry NULL.
ALTER TABLE lag_diagnostic_reports ADD COLUMN IF NOT EXISTS match_id TEXT COLLATE "C";
CREATE INDEX IF NOT EXISTS lag_diagnostic_reports_match_idx
ON lag_diagnostic_reports (match_id, report_id);
