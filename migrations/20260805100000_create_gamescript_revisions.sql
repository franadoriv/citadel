-- Immutable GameScript revision store: drafts, hash-addressed revisions,
-- diagnostics, activation generations, redacted audit, and rollout outbox.
-- The revision id IS the content hash, so the primary key both deduplicates
-- identical content and closes the concurrent-submission race. Additive only.

CREATE TABLE IF NOT EXISTS gamescript_drafts (
    draft_id text COLLATE "C" PRIMARY KEY,
    language text COLLATE "C" NOT NULL,
    entrypoint text COLLATE "C" NOT NULL,
    content text NOT NULL,
    created_by text COLLATE "C" NOT NULL,
    created_at_unix_ms bigint NOT NULL,
    updated_at_unix_ms bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS gamescript_drafts_retention_idx
    ON gamescript_drafts (updated_at_unix_ms, draft_id);

-- Immutable: no statement in the codebase updates a row of this table.
CREATE TABLE IF NOT EXISTS gamescript_revisions (
    revision_id text COLLATE "C" PRIMARY KEY,
    language text COLLATE "C" NOT NULL,
    entrypoint text COLLATE "C" NOT NULL,
    content text NOT NULL,
    size_bytes bigint NOT NULL,
    created_by text COLLATE "C" NOT NULL,
    created_at_unix_ms bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS gamescript_revisions_retention_idx
    ON gamescript_revisions (created_at_unix_ms, revision_id);

-- Retention metadata; the revision row itself stays byte-identical.
CREATE TABLE IF NOT EXISTS gamescript_revision_pins (
    revision_id text COLLATE "C" PRIMARY KEY
        REFERENCES gamescript_revisions(revision_id) ON DELETE CASCADE,
    pinned_by text COLLATE "C" NOT NULL,
    pinned_at_unix_ms bigint NOT NULL
);

-- Appendable validation output; never mutates revision content and dies with
-- its revision.
CREATE TABLE IF NOT EXISTS gamescript_revision_diagnostics (
    revision_id text COLLATE "C" NOT NULL
        REFERENCES gamescript_revisions(revision_id) ON DELETE CASCADE,
    seq bigint NOT NULL,
    severity text COLLATE "C" NOT NULL,
    source text NOT NULL,
    message text NOT NULL,
    created_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (revision_id, seq)
);

-- Strictly monotonic fencing counter per scope. Stored in the shared backend,
-- so it is cluster-scoped whenever nodes share a database.
CREATE TABLE IF NOT EXISTS gamescript_activation_generations (
    scope text COLLATE "C" PRIMARY KEY,
    current_generation bigint NOT NULL
);

-- The RESTRICT foreign key (no ON DELETE action) is the database-level
-- backstop for "an activation-referenced revision is never pruned".
CREATE TABLE IF NOT EXISTS gamescript_activations (
    scope text COLLATE "C" NOT NULL,
    generation bigint NOT NULL,
    revision_id text COLLATE "C" NOT NULL
        REFERENCES gamescript_revisions(revision_id),
    activated_by text COLLATE "C" NOT NULL,
    activated_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (scope, generation)
);
CREATE INDEX IF NOT EXISTS gamescript_activations_revision_idx
    ON gamescript_activations (revision_id);

-- Details are redacted BEFORE insertion; raw secrets never reach this table.
CREATE TABLE IF NOT EXISTS gamescript_audit (
    audit_id bigserial PRIMARY KEY,
    actor text COLLATE "C" NOT NULL,
    action text COLLATE "C" NOT NULL,
    target text COLLATE "C" NOT NULL,
    details text NOT NULL,
    created_at_unix_ms bigint NOT NULL
);

-- Written in the same transaction as the state change that produced the
-- entry. Delivery is at-least-once; acknowledgement deletes the row.
CREATE TABLE IF NOT EXISTS gamescript_outbox (
    outbox_id bigserial PRIMARY KEY,
    kind text COLLATE "C" NOT NULL,
    scope text COLLATE "C",
    revision_id text COLLATE "C" NOT NULL,
    generation bigint,
    created_at_unix_ms bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS gamescript_outbox_pending_idx
    ON gamescript_outbox (created_at_unix_ms, outbox_id);
