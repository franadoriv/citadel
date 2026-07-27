-- : CockroachDB variant of durable group admission state.
CREATE TABLE IF NOT EXISTS group_admissions (
    group_id bigint NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    user_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('request', 'invitation')),
    inviter_user_id text,
    created_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (group_id, user_id),
    CHECK ((kind = 'request' AND inviter_user_id IS NULL) OR
           (kind = 'invitation' AND inviter_user_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS group_admissions_user_idx
    ON group_admissions (user_id, created_at_unix_ms);
