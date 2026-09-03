//! Internal composition of durable session state and local realtime control.

use std::sync::Arc;

use crate::error::AppResult;
use crate::realtime::gateway::Gateway;
use crate::session::{RevocationReason, SessionId};
use crate::time::TimestampMillis;

use super::{RevokeSessionRequest, SessionService};

/// Typed, routeable close intent. It has no token, actor, or private reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRevocationCommand {
    pub session_id: SessionId,
    pub revocation_id: String,
    pub expected_generation: Option<u64>,
}

/// Result of the local half of a durable session revocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RevocationDispatch {
    pub disconnected: usize,
}

/// The narrow orchestration seam: commit durable revocation first, then issue a
/// deduplicable close command. A future outbox/router owns remote delivery;
/// callers must never roll back the durable result if local delivery fails.
pub struct SessionRevocationCoordinator {
    sessions: Arc<dyn SessionService + Send + Sync>,
    gateway: Option<Arc<Gateway>>,
}

impl SessionRevocationCoordinator {
    /// Assemble the coordinator for this node. HTTP-only nodes have no live
    /// gateway to fence, but still use this same durable-revocation boundary.
    #[must_use]
    pub fn new(
        sessions: Arc<dyn SessionService + Send + Sync>,
        gateway: Option<Arc<Gateway>>,
    ) -> Self {
        Self { sessions, gateway }
    }

    pub async fn revoke_local(
        &self,
        command: SessionRevocationCommand,
        revoked_at: TimestampMillis,
        reason: RevocationReason,
    ) -> AppResult<RevocationDispatch> {
        let revoked = self
            .sessions
            .revoke_session(RevokeSessionRequest {
                session_id: command.session_id.clone(),
                revoked_at,
                reason,
            })
            .await?;
        let disconnected = match &self.gateway {
            Some(gateway) => {
                gateway
                    .disconnect_session(
                        &command.session_id,
                        &command.revocation_id,
                        command.expected_generation,
                        revoked.expires_at,
                        revoked_at,
                    )
                    .await
            }
            None => 0,
        };
        Ok(RevocationDispatch { disconnected })
    }
}
