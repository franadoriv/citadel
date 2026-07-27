-- : durable closed-group requests and administrator invitations.
CREATE TABLE IF NOT EXISTS group_admissions (
    group_id bigint NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    user_id text COLLATE "C" NOT NULL,
    kind text NOT NULL CHECK (kind IN ('request', 'invitation')),
    inviter_user_id text COLLATE "C",
    created_at_unix_ms bigint NOT NULL,
    PRIMARY KEY (group_id, user_id),
    CHECK ((kind = 'request' AND inviter_user_id IS NULL) OR
           (kind = 'invitation' AND inviter_user_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS group_admissions_user_idx
    ON group_admissions (user_id, created_at_unix_ms);
