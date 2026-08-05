-- Immutable GameScript revision store: drafts, hash-addressed revisions,
-- diagnostics, activation generations, redacted audit, and rollout outbox.
-- The revision id IS the content hash, so the primary key both deduplicates
-- identical content and closes the concurrent-submission race. Additive only.

CREATE TABLE gamescript_drafts (
    draft_id TEXT PRIMARY KEY,
    language TEXT NOT NULL,
    entrypoint TEXT NOT NULL,
    content TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL
);
CREATE INDEX gamescript_drafts_retention_idx
    ON gamescript_drafts (updated_at_unix_ms, draft_id);

-- Immutable: no statement in the codebase updates a row of this table.
CREATE TABLE gamescript_revisions (
    revision_id TEXT PRIMARY KEY,
    language TEXT NOT NULL,
    entrypoint TEXT NOT NULL,
    content TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    created_by TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL
);
CREATE INDEX gamescript_revisions_retention_idx
    ON gamescript_revisions (created_at_unix_ms, revision_id);

-- Retention metadata; the revision row itself stays byte-identical.
CREATE TABLE gamescript_revision_pins (
    revision_id TEXT PRIMARY KEY
        REFERENCES gamescript_revisions(revision_id) ON DELETE CASCADE,
    pinned_by TEXT NOT NULL,
    pinned_at_unix_ms INTEGER NOT NULL
);

-- Appendable validation output; never mutates revision content and dies with
-- its revision.
CREATE TABLE gamescript_revision_diagnostics (
    revision_id TEXT NOT NULL
        REFERENCES gamescript_revisions(revision_id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    severity TEXT NOT NULL,
    source TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (revision_id, seq)
);

-- Strictly monotonic fencing counter per scope. Stored in the shared backend,
-- so it is cluster-scoped whenever nodes share a database.
CREATE TABLE gamescript_activation_generations (
    scope TEXT PRIMARY KEY,
    current_generation INTEGER NOT NULL
);

-- The RESTRICT foreign key (no ON DELETE action) is the database-level
-- backstop for "an activation-referenced revision is never pruned".
CREATE TABLE gamescript_activations (
    scope TEXT NOT NULL,
    generation INTEGER NOT NULL,
    revision_id TEXT NOT NULL REFERENCES gamescript_revisions(revision_id),
    activated_by TEXT NOT NULL,
    activated_at_unix_ms INTEGER NOT NULL,
    PRIMARY KEY (scope, generation)
);
CREATE INDEX gamescript_activations_revision_idx
    ON gamescript_activations (revision_id);

-- Details are redacted BEFORE insertion; raw secrets never reach this table.
CREATE TABLE gamescript_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at_unix_ms INTEGER NOT NULL
);

-- Written in the same transaction as the state change that produced the
-- entry. Delivery is at-least-once; acknowledgement deletes the row.
CREATE TABLE gamescript_outbox (
    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    scope TEXT,
    revision_id TEXT NOT NULL,
    generation INTEGER,
    created_at_unix_ms INTEGER NOT NULL
);
CREATE INDEX gamescript_outbox_pending_idx
    ON gamescript_outbox (created_at_unix_ms, outbox_id);
