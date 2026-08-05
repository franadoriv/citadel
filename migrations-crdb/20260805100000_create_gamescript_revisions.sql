-- Immutable GameScript revision store: drafts, hash-addressed revisions,
-- diagnostics, activation generations, redacted audit, and rollout outbox.
-- The revision id IS the content hash, so the primary key both deduplicates
-- identical content and closes the concurrent-submission race. Additive only.

CREATE TABLE IF NOT EXISTS gamescript_drafts (
    draft_id STRING PRIMARY KEY,
    language STRING NOT NULL,
    entrypoint STRING NOT NULL,
    content STRING NOT NULL,
    created_by STRING NOT NULL,
    created_at_unix_ms INT8 NOT NULL,
    updated_at_unix_ms INT8 NOT NULL
);
CREATE INDEX IF NOT EXISTS gamescript_drafts_retention_idx
    ON gamescript_drafts (updated_at_unix_ms, draft_id);

-- Immutable: no statement in the codebase updates a row of this table.
CREATE TABLE IF NOT EXISTS gamescript_revisions (
    revision_id STRING PRIMARY KEY,
    language STRING NOT NULL,
    entrypoint STRING NOT NULL,
    content STRING NOT NULL,
    size_bytes INT8 NOT NULL,
    created_by STRING NOT NULL,
    created_at_unix_ms INT8 NOT NULL
);
CREATE INDEX IF NOT EXISTS gamescript_revisions_retention_idx
    ON gamescript_revisions (created_at_unix_ms, revision_id);

-- Retention metadata; the revision row itself stays byte-identical.
CREATE TABLE IF NOT EXISTS gamescript_revision_pins (
    revision_id STRING PRIMARY KEY
        REFERENCES gamescript_revisions(revision_id) ON DELETE CASCADE,
    pinned_by STRING NOT NULL,
    pinned_at_unix_ms INT8 NOT NULL
);

-- Appendable validation output; never mutates revision content and dies with
-- its revision.
CREATE TABLE IF NOT EXISTS gamescript_revision_diagnostics (
    revision_id STRING NOT NULL
        REFERENCES gamescript_revisions(revision_id) ON DELETE CASCADE,
    seq INT8 NOT NULL,
    severity STRING NOT NULL,
    source STRING NOT NULL,
    message STRING NOT NULL,
    created_at_unix_ms INT8 NOT NULL,
    PRIMARY KEY (revision_id, seq)
);

-- Strictly monotonic fencing counter per scope. Stored in the shared backend,
-- so it is cluster-scoped whenever nodes share a database.
CREATE TABLE IF NOT EXISTS gamescript_activation_generations (
    scope STRING PRIMARY KEY,
    current_generation INT8 NOT NULL
);

-- The RESTRICT foreign key (no ON DELETE action) is the database-level
-- backstop for "an activation-referenced revision is never pruned".
CREATE TABLE IF NOT EXISTS gamescript_activations (
    scope STRING NOT NULL,
    generation INT8 NOT NULL,
    revision_id STRING NOT NULL
        REFERENCES gamescript_revisions(revision_id),
    activated_by STRING NOT NULL,
    activated_at_unix_ms INT8 NOT NULL,
    PRIMARY KEY (scope, generation)
);
CREATE INDEX IF NOT EXISTS gamescript_activations_revision_idx
    ON gamescript_activations (revision_id);

-- Details are redacted BEFORE insertion; raw secrets never reach this table.
-- `unique_rowid()` ids are unique but only approximately time-ordered, so all
-- reads order by (created_at_unix_ms, audit_id), never by id alone.
CREATE TABLE IF NOT EXISTS gamescript_audit (
    audit_id INT8 PRIMARY KEY DEFAULT unique_rowid(),
    actor STRING NOT NULL,
    action STRING NOT NULL,
    target STRING NOT NULL,
    details STRING NOT NULL,
    created_at_unix_ms INT8 NOT NULL
);

-- Written in the same transaction as the state change that produced the
-- entry. Delivery is at-least-once; acknowledgement deletes the row.
CREATE TABLE IF NOT EXISTS gamescript_outbox (
    outbox_id INT8 PRIMARY KEY DEFAULT unique_rowid(),
    kind STRING NOT NULL,
    scope STRING,
    revision_id STRING NOT NULL,
    generation INT8,
    created_at_unix_ms INT8 NOT NULL
);
CREATE INDEX IF NOT EXISTS gamescript_outbox_pending_idx
    ON gamescript_outbox (created_at_unix_ms, outbox_id);
