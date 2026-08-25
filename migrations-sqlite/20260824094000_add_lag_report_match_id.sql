-- Optional durable match reference for a derived lag report. Nullable: reports
-- produced before match plumbing, or outside a match, carry NULL.
--
-- SQLite has no `ADD COLUMN IF NOT EXISTS`; the migrator records this version
-- once applied, so the plain form runs exactly once.
ALTER TABLE lag_diagnostic_reports ADD COLUMN match_id TEXT;
CREATE INDEX IF NOT EXISTS lag_diagnostic_reports_match_idx
ON lag_diagnostic_reports (match_id, report_id);
