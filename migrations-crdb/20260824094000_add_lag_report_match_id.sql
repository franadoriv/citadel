-- Optional durable match reference for a derived lag report. Nullable: reports
-- produced before match plumbing, or outside a match, carry NULL.
--
-- A single-statement column addition with no dependent DDL: the migrator runs
-- with locking disabled and retries while Cockroach backfills the column.
ALTER TABLE lag_diagnostic_reports ADD COLUMN IF NOT EXISTS match_id STRING;
CREATE INDEX IF NOT EXISTS lag_diagnostic_reports_match_idx
ON lag_diagnostic_reports (match_id, report_id);
