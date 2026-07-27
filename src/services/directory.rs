//! In-memory session directory reference implementation.
//!
//! [`InMemorySessionDirectory`] realizes the [`SessionDirectory`] contract with
//! the generation/lease preconditions deferred by :
//!
//! - Every session remembers the **highest generation ever seen**, so a
//!   lower-generation `bind` can never roll ownership back even after an
//!   `unbind` or lease expiry.
//! - `bind` accepts a brand-new session, an exact idempotent re-bind, or a
//!   strictly higher generation; everything else (lower/equal generation with a
//!   different owner or a different expiry) conflicts and requires `renew`.
//! - `renew` applies only for the current owner and a generation `>=` the
//!   current one; a different owner or a lower generation conflicts.
//! - `unbind` clears ownership only for the current owner and is idempotent when
//!   there is nothing (live) to release; a different owner conflicts.
//! - `resolve` returns `Unknown` for a missing/expired lease, `Stale` when the
//!   caller's `expected` lease no longer matches, and otherwise `Local`/`Remote`
//!   relative to this directory's node.
//!
//! The directory is single-process (`Mutex`-guarded); a distributed substrate is
//! a later task, but it must preserve these invariants.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::{AppError, AppResult};
use crate::session::{
    NodeId, OwnershipGeneration, ResolveSessionOwnerRequest, SessionDirectoryEntry, SessionId,
    SessionOwnerLease, SessionOwnership,
};

use super::SessionDirectory;

/// Per-session directory state: the current lease (if any) and the high-water
/// generation mark that guards against rollback.
#[derive(Debug, Clone)]
struct DirectoryState {
    current: Option<SessionOwnerLease>,
    max_generation: OwnershipGeneration,
}

/// An in-memory [`SessionDirectory`] bound to one local node.
#[derive(Debug)]
pub struct InMemorySessionDirectory {
    local_node: NodeId,
    entries: Mutex<HashMap<SessionId, DirectoryState>>,
}

impl InMemorySessionDirectory {
    /// Create a directory that treats `local_node` as "this node" for routing.
    #[must_use]
    pub fn new(local_node: NodeId) -> Self {
        Self {
            local_node,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, HashMap<SessionId, DirectoryState>>> {
        self.entries
            .lock()
            .map_err(|_| AppError::internal("session directory mutex poisoned"))
    }
}

#[async_trait]
impl SessionDirectory for InMemorySessionDirectory {
    async fn resolve_session_owner(
        &self,
        request: &ResolveSessionOwnerRequest,
    ) -> AppResult<SessionOwnership> {
        let entries = self.guard()?;
        let Some(state) = entries.get(&request.session_id) else {
            return Ok(SessionOwnership::Unknown);
        };
        let Some(lease) = &state.current else {
            return Ok(SessionOwnership::Unknown);
        };
        // An expired lease is treated as no live ownership.
        if !lease.is_current_at(request.now) {
            return Ok(SessionOwnership::Unknown);
        }
        // A caller whose expected view no longer matches the live lease is stale.
        if let Some(expected) = &request.expected
            && expected != lease
        {
            return Ok(SessionOwnership::Stale);
        }
        if lease.node_id == self.local_node {
            Ok(SessionOwnership::Local)
        } else {
            Ok(SessionOwnership::Remote(lease.node_id.clone()))
        }
    }

    async fn bind_session_owner(&self, entry: SessionDirectoryEntry) -> AppResult<()> {
        let mut entries = self.guard()?;
        match entries.get_mut(&entry.session_id) {
            None => {
                entries.insert(
                    entry.session_id,
                    DirectoryState {
                        max_generation: entry.owner.generation,
                        current: Some(entry.owner),
                    },
                );
                Ok(())
            }
            Some(state) => {
                // Exact idempotent re-bind of the live lease.
                if state.current.as_ref() == Some(&entry.owner) {
                    return Ok(());
                }
                // Only a strictly higher generation may (re)claim ownership.
                if entry.owner.generation > state.max_generation {
                    state.max_generation = entry.owner.generation;
                    state.current = Some(entry.owner);
                    Ok(())
                } else {
                    Err(AppError::conflict(
                        "session ownership bind rejected: stale or conflicting generation",
                    ))
                }
            }
        }
    }

    async fn unbind_session_owner(
        &self,
        session_id: &SessionId,
        owner_node: &NodeId,
    ) -> AppResult<()> {
        let mut entries = self.guard()?;
        let Some(state) = entries.get_mut(session_id) else {
            // Nothing recorded: idempotent.
            return Ok(());
        };
        match &state.current {
            // Idempotent when there is no live lease (history/max_generation kept).
            None => Ok(()),
            Some(lease) if &lease.node_id == owner_node => {
                state.current = None;
                Ok(())
            }
            Some(_) => Err(AppError::conflict(
                "session ownership unbind rejected: not the current owner",
            )),
        }
    }

    async fn renew_session_owner(&self, entry: SessionDirectoryEntry) -> AppResult<()> {
        let mut entries = self.guard()?;
        let Some(state) = entries.get_mut(&entry.session_id) else {
            return Err(AppError::conflict(
                "session ownership renew rejected: no lease to renew",
            ));
        };
        match &state.current {
            None => Err(AppError::conflict(
                "session ownership renew rejected: no live lease",
            )),
            Some(current) => {
                if current.node_id != entry.owner.node_id {
                    return Err(AppError::conflict(
                        "session ownership renew rejected: different owner",
                    ));
                }
                if entry.owner.generation < current.generation {
                    return Err(AppError::conflict(
                        "session ownership renew rejected: stale generation",
                    ));
                }
                if entry.owner.generation > state.max_generation {
                    state.max_generation = entry.owner.generation;
                }
                state.current = Some(entry.owner);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCategory;
    use crate::time::TimestampMillis;

    fn node(id: &str) -> NodeId {
        NodeId::new(id).expect("node")
    }

    fn sid(id: &str) -> SessionId {
        SessionId::new(id).expect("sid")
    }

    fn ts(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    fn lease(node_id: &str, generation: u64, expires: u64) -> SessionOwnerLease {
        SessionOwnerLease {
            node_id: node(node_id),
            generation: OwnershipGeneration::new(generation),
            expires_at: ts(expires),
        }
    }

    fn entry(session: &str, node_id: &str, generation: u64, expires: u64) -> SessionDirectoryEntry {
        SessionDirectoryEntry {
            session_id: sid(session),
            owner: lease(node_id, generation, expires),
        }
    }

    fn resolve(
        now: u64,
        session: &str,
        expected: Option<SessionOwnerLease>,
    ) -> ResolveSessionOwnerRequest {
        ResolveSessionOwnerRequest {
            session_id: sid(session),
            expected,
            now: ts(now),
        }
    }

    #[tokio::test]
    async fn resolve_local_remote_unknown_and_stale() {
        let dir = InMemorySessionDirectory::new(node("node-a"));
        // Unknown before any bind.
        assert_eq!(
            dir.resolve_session_owner(&resolve(50, "s-1", None))
                .await
                .expect("test value"),
            SessionOwnership::Unknown
        );

        dir.bind_session_owner(entry("s-1", "node-a", 1, 100))
            .await
            .expect("bind local");
        assert_eq!(
            dir.resolve_session_owner(&resolve(50, "s-1", None))
                .await
                .expect("test value"),
            SessionOwnership::Local
        );

        dir.bind_session_owner(entry("s-2", "node-b", 1, 100))
            .await
            .expect("bind remote");
        assert_eq!(
            dir.resolve_session_owner(&resolve(50, "s-2", None))
                .await
                .expect("test value"),
            SessionOwnership::Remote(node("node-b"))
        );

        // Expired lease resolves as Unknown.
        assert_eq!(
            dir.resolve_session_owner(&resolve(100, "s-1", None))
                .await
                .expect("test value"),
            SessionOwnership::Unknown
        );

        // A mismatched expected lease resolves as Stale.
        let stale_expectation = lease("node-a", 0, 100);
        assert_eq!(
            dir.resolve_session_owner(&resolve(50, "s-1", Some(stale_expectation)))
                .await
                .expect("test value"),
            SessionOwnership::Stale
        );
    }

    #[tokio::test]
    async fn bind_generation_rules() {
        let dir = InMemorySessionDirectory::new(node("node-a"));
        dir.bind_session_owner(entry("s-1", "node-a", 2, 100))
            .await
            .expect("initial bind");

        // Exact idempotent re-bind.
        dir.bind_session_owner(entry("s-1", "node-a", 2, 100))
            .await
            .expect("idempotent");

        // Same generation, different owner: conflict.
        assert_eq!(
            dir.bind_session_owner(entry("s-1", "node-b", 2, 100))
                .await
                .expect_err("same gen diff owner")
                .category(),
            ErrorCategory::Conflict
        );

        // Same generation/owner, different expiry: conflict (must renew).
        assert_eq!(
            dir.bind_session_owner(entry("s-1", "node-a", 2, 200))
                .await
                .expect_err("same gen new expiry")
                .category(),
            ErrorCategory::Conflict
        );

        // Lower generation: conflict (rollback guard).
        assert_eq!(
            dir.bind_session_owner(entry("s-1", "node-a", 1, 100))
                .await
                .expect_err("lower gen")
                .category(),
            ErrorCategory::Conflict
        );

        // Higher generation always wins (ownership transfer).
        dir.bind_session_owner(entry("s-1", "node-b", 3, 100))
            .await
            .expect("higher gen wins");
        assert_eq!(
            dir.resolve_session_owner(&resolve(50, "s-1", None))
                .await
                .expect("test value"),
            SessionOwnership::Remote(node("node-b"))
        );
    }

    #[tokio::test]
    async fn rollback_after_unbind_is_blocked() {
        let dir = InMemorySessionDirectory::new(node("node-a"));
        dir.bind_session_owner(entry("s-1", "node-a", 5, 100))
            .await
            .expect("bind");
        dir.unbind_session_owner(&sid("s-1"), &node("node-a"))
            .await
            .expect("unbind");
        // After unbind there is no live owner.
        assert_eq!(
            dir.resolve_session_owner(&resolve(50, "s-1", None))
                .await
                .expect("test value"),
            SessionOwnership::Unknown
        );
        // A bind at or below the high-water generation is rejected.
        assert_eq!(
            dir.bind_session_owner(entry("s-1", "node-b", 5, 100))
                .await
                .expect_err("no rollback")
                .category(),
            ErrorCategory::Conflict
        );
        // A higher generation re-binds cleanly.
        dir.bind_session_owner(entry("s-1", "node-b", 6, 100))
            .await
            .expect("higher gen rebinds");
    }

    #[tokio::test]
    async fn unbind_rejects_non_owner_and_is_idempotent() {
        let dir = InMemorySessionDirectory::new(node("node-a"));
        dir.bind_session_owner(entry("s-1", "node-a", 1, 100))
            .await
            .expect("bind");
        // Different owner cannot unbind.
        assert_eq!(
            dir.unbind_session_owner(&sid("s-1"), &node("node-b"))
                .await
                .expect_err("not owner")
                .category(),
            ErrorCategory::Conflict
        );
        // Owner unbinds; repeating is idempotent.
        dir.unbind_session_owner(&sid("s-1"), &node("node-a"))
            .await
            .expect("unbind");
        dir.unbind_session_owner(&sid("s-1"), &node("node-a"))
            .await
            .expect("idempotent unbind");
        // Unbinding an unknown session is a no-op.
        dir.unbind_session_owner(&sid("missing"), &node("node-a"))
            .await
            .expect("idempotent missing");
    }

    #[tokio::test]
    async fn renew_owner_generation_rules() {
        let dir = InMemorySessionDirectory::new(node("node-a"));
        // Renew before any bind: conflict.
        assert_eq!(
            dir.renew_session_owner(entry("s-1", "node-a", 1, 200))
                .await
                .expect_err("no lease")
                .category(),
            ErrorCategory::Conflict
        );

        dir.bind_session_owner(entry("s-1", "node-a", 1, 100))
            .await
            .expect("bind");

        // Same owner/generation extends the expiry.
        dir.renew_session_owner(entry("s-1", "node-a", 1, 300))
            .await
            .expect("extend");
        assert_eq!(
            dir.resolve_session_owner(&resolve(250, "s-1", None))
                .await
                .expect("test value"),
            SessionOwnership::Local
        );

        // Different owner cannot renew.
        assert_eq!(
            dir.renew_session_owner(entry("s-1", "node-b", 1, 300))
                .await
                .expect_err("diff owner")
                .category(),
            ErrorCategory::Conflict
        );

        // Lower generation cannot renew.
        assert_eq!(
            dir.renew_session_owner(entry("s-1", "node-a", 0, 300))
                .await
                .expect_err("stale gen")
                .category(),
            ErrorCategory::Conflict
        );

        // Same owner, higher generation advances the lease.
        dir.renew_session_owner(entry("s-1", "node-a", 2, 400))
            .await
            .expect("advance");
    }
}
