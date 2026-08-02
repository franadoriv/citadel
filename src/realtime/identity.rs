//! Session-scoped identity, resume, and privacy-safe presence lifecycle.
//!
//! This deliberately has no transport or wire dependency. Transports must first
//! authenticate through `Authenticator`; only then may they call this registry
//! with the resolved `ParticipantIdentity`. Resume secrets are opaque, supplied
//! by an injected random source, single-use, and never identify a user/session.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::session::SessionId;
use crate::storage::UserId;
use crate::time::TimestampMillis;

use super::registry::ParticipantId;

/// Opaque server-minted resume material. It is intentionally not `Display` or
/// `Debug`, and callers must not persist it by default.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ResumeSecret(Vec<u8>);

impl ResumeSecret {
    #[must_use]
    pub fn from_server_bytes(bytes: Vec<u8>) -> Option<Self> {
        (bytes.len() >= 16).then_some(Self(bytes))
    }
}

impl std::fmt::Debug for ResumeSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ResumeSecret([REDACTED])")
    }
}

/// A privacy-safe presence projection. It is scoped to a room by its caller;
/// this lifecycle owns no global account-online directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresenceState {
    Online,
    Suspect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Presence {
    pub participant: ParticipantId,
    pub generation: u64,
    pub state: PresenceState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeResult {
    Accepted { generation: u64 },
    ReauthRequired,
    Rejected,
}

#[derive(Debug, Clone)]
struct Live {
    user: UserId,
    participant: ParticipantId,
    generation: u64,
    owner_generation: u64,
    suspect_until: Option<TimestampMillis>,
}

#[derive(Debug, Clone)]
struct Ticket {
    session: SessionId,
    generation: u64,
    owner_generation: u64,
    expires_at: TimestampMillis,
}

#[derive(Debug, Default)]
struct State {
    next_generation: u64,
    live: HashMap<SessionId, Live>,
    tickets: HashMap<ResumeSecret, Ticket>,
    revoked: HashSet<SessionId>,
}

/// Atomic, single-node lifecycle state. `now` and secret generation are supplied
/// by the caller so all transitions are deterministic in tests.
#[derive(Debug, Clone, Default)]
pub struct IdentityLifecycle {
    state: Arc<Mutex<State>>,
}

impl IdentityLifecycle {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Authenticate/activate a session. A second activation of the exact same
    /// session replaces (fences) the earlier generation; another session for the
    /// same user remains independent.
    pub fn activate(
        &self,
        user: UserId,
        session: SessionId,
        participant: ParticipantId,
        owner_generation: u64,
    ) -> Option<u64> {
        let mut state = self.state.lock().ok()?;
        if state.revoked.contains(&session) {
            return None;
        }
        state.next_generation = state.next_generation.checked_add(1)?;
        let generation = state.next_generation;
        state.live.insert(
            session,
            Live {
                user,
                participant,
                generation,
                owner_generation,
                suspect_until: None,
            },
        );
        Some(generation)
    }

    /// Mint a resume ticket only for the current generation. Old tickets for the
    /// session are removed, preventing a stale connection from retaining a path
    /// back to ownership.
    pub fn issue_resume(
        &self,
        session: &SessionId,
        generation: u64,
        secret: ResumeSecret,
        expires_at: TimestampMillis,
    ) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        let Some(owner_generation) = state.live.get(session).map(|live| live.owner_generation)
        else {
            return false;
        };
        if state
            .live
            .get(session)
            .is_none_or(|live| live.generation != generation)
            || state.revoked.contains(session)
        {
            return false;
        }
        state.tickets.retain(|_, ticket| &ticket.session != session);
        state.tickets.insert(
            secret,
            Ticket {
                session: session.clone(),
                generation,
                owner_generation,
                expires_at,
            },
        );
        true
    }

    /// Mark only the matching generation suspect. Stale transport teardown is a
    /// no-op and cannot withdraw a replacement presence.
    pub fn mark_suspect(
        &self,
        session: &SessionId,
        generation: u64,
        until: TimestampMillis,
    ) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.revoked.contains(session) {
            return false;
        }
        let Some(live) = state.live.get_mut(session) else {
            return false;
        };
        if live.generation != generation {
            return false;
        }
        live.suspect_until = Some(until);
        true
    }

    /// Resume is reauthentication-backed: callers must pass `reauthenticated`
    /// only after current access-token/session validation. Consumption and the
    /// generation CAS happen under one lock, so a replay or revoke race fails
    /// closed. A fresh transport participant replaces the suspect connection.
    pub fn resume(
        &self,
        secret: ResumeSecret,
        reauthenticated_session: Option<&SessionId>,
        participant: ParticipantId,
        owner_generation: u64,
        now: TimestampMillis,
    ) -> ResumeResult {
        // A bare `true` is insufficient: a valid credential for a different
        // device session must not redeem this session's resume ticket.
        let Some(reauthenticated_session) = reauthenticated_session else {
            return ResumeResult::ReauthRequired;
        };
        let Ok(mut state) = self.state.lock() else {
            return ResumeResult::Rejected;
        };
        let Some(ticket) = state.tickets.remove(&secret) else {
            return ResumeResult::Rejected;
        };
        if ticket.expires_at <= now
            || ticket.owner_generation != owner_generation
            || ticket.session != *reauthenticated_session
            || state.revoked.contains(&ticket.session)
        {
            return ResumeResult::Rejected;
        }
        let Some((live_generation, live_owner_generation, user)) = state
            .live
            .get(&ticket.session)
            .map(|live| (live.generation, live.owner_generation, live.user.clone()))
        else {
            return ResumeResult::Rejected;
        };
        if live_generation != ticket.generation || live_owner_generation != owner_generation {
            return ResumeResult::Rejected;
        }
        state.next_generation = match state.next_generation.checked_add(1) {
            Some(value) => value,
            None => return ResumeResult::Rejected,
        };
        let generation = state.next_generation;
        state.live.insert(
            ticket.session,
            Live {
                user,
                participant,
                generation,
                owner_generation,
                suspect_until: None,
            },
        );
        ResumeResult::Accepted { generation }
    }

    /// Revocation wins all future activations/resumes and terminally removes the
    /// exact session only; same-user sibling devices are intentionally untouched.
    pub fn revoke(&self, session: &SessionId) {
        if let Ok(mut state) = self.state.lock() {
            state.revoked.insert(session.clone());
            state.live.remove(session);
            state.tickets.retain(|_, ticket| &ticket.session != session);
        }
    }

    /// Deterministic grace sweep. Returns session ids that became terminal; only
    /// matching suspect records expire, never a global user presence.
    pub fn expire_grace(&self, now: TimestampMillis) -> Vec<SessionId> {
        let Ok(mut state) = self.state.lock() else {
            return Vec::new();
        };
        let expired: Vec<_> = state
            .live
            .iter()
            .filter_map(|(session, live)| {
                (live.suspect_until.is_some_and(|until| until <= now)).then_some(session.clone())
            })
            .collect();
        for session in &expired {
            state.live.remove(session);
            state.tickets.retain(|_, ticket| &ticket.session != session);
        }
        expired
    }

    #[must_use]
    pub fn presence(&self, session: &SessionId) -> Option<Presence> {
        let state = self.state.lock().ok()?;
        let live = state.live.get(session)?;
        Some(Presence {
            participant: live.participant,
            generation: live.generation,
            state: if live.suspect_until.is_some() {
                PresenceState::Suspect
            } else {
                PresenceState::Online
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn user(v: &str) -> UserId {
        UserId::new(v).expect("test fixture")
    }
    fn session(v: &str) -> SessionId {
        SessionId::new(v).expect("test fixture")
    }
    fn secret(v: u8) -> ResumeSecret {
        ResumeSecret::from_server_bytes(vec![v; 16]).expect("test fixture")
    }
    fn at(v: u64) -> TimestampMillis {
        TimestampMillis::from_unix_millis(v)
    }

    #[test]
    fn normal_resume_is_reauthenticated_single_use_and_replaces_generation() {
        let lifecycle = IdentityLifecycle::new();
        let s = session("s");
        let g = lifecycle
            .activate(user("u"), s.clone(), ParticipantId::from_raw(1), 7)
            .expect("test fixture");
        assert!(lifecycle.issue_resume(&s, g, secret(1), at(20)));
        assert_eq!(
            lifecycle.resume(secret(1), Some(&s), ParticipantId::from_raw(2), 7, at(10)),
            ResumeResult::Accepted { generation: g + 1 }
        );
        assert_eq!(
            lifecycle.resume(secret(1), Some(&s), ParticipantId::from_raw(3), 7, at(10)),
            ResumeResult::Rejected
        );
        assert_eq!(
            lifecycle.presence(&s).expect("test fixture").participant,
            ParticipantId::from_raw(2)
        );
    }

    #[test]
    fn expiry_reauth_owner_and_revocation_all_fail_closed() {
        let lifecycle = IdentityLifecycle::new();
        let s = session("s");
        let other = session("other");
        let g = lifecycle
            .activate(user("u"), s.clone(), ParticipantId::from_raw(1), 7)
            .expect("test fixture");
        assert!(lifecycle.issue_resume(&s, g, secret(1), at(10)));
        assert_eq!(
            lifecycle.resume(secret(1), Some(&s), ParticipantId::from_raw(2), 7, at(10)),
            ResumeResult::Rejected
        );
        assert!(lifecycle.issue_resume(&s, g, secret(2), at(20)));
        assert_eq!(
            lifecycle.resume(secret(2), None, ParticipantId::from_raw(2), 7, at(11)),
            ResumeResult::ReauthRequired
        );
        assert_eq!(
            lifecycle.resume(secret(2), Some(&s), ParticipantId::from_raw(2), 8, at(11)),
            ResumeResult::Rejected
        );
        assert!(lifecycle.issue_resume(&s, g, secret(3), at(20)));
        assert_eq!(
            lifecycle.resume(
                secret(3),
                Some(&other),
                ParticipantId::from_raw(2),
                7,
                at(11)
            ),
            ResumeResult::Rejected,
            "a currently valid credential for a sibling session cannot resume this session"
        );
        lifecycle.revoke(&s);
        assert!(
            lifecycle
                .activate(user("u"), s.clone(), ParticipantId::from_raw(2), 7)
                .is_none()
        );
    }

    #[test]
    fn stale_loss_cannot_withdraw_replacement_and_grace_is_bounded() {
        let lifecycle = IdentityLifecycle::new();
        let s = session("s");
        let g = lifecycle
            .activate(user("u"), s.clone(), ParticipantId::from_raw(1), 1)
            .expect("test fixture");
        assert!(lifecycle.mark_suspect(&s, g, at(10)));
        assert_eq!(
            lifecycle.presence(&s).expect("test fixture").state,
            PresenceState::Suspect
        );
        assert!(lifecycle.expire_grace(at(9)).is_empty());
        assert_eq!(lifecycle.expire_grace(at(10)), vec![s]);
    }

    #[test]
    fn revocation_is_exact_session_not_same_user_sibling() {
        let lifecycle = IdentityLifecycle::new();
        let a = session("a");
        let b = session("b");
        lifecycle.activate(user("u"), a.clone(), ParticipantId::from_raw(1), 1);
        lifecycle.activate(user("u"), b.clone(), ParticipantId::from_raw(2), 1);
        lifecycle.revoke(&a);
        assert!(lifecycle.presence(&a).is_none());
        assert!(lifecycle.presence(&b).is_some());
    }
}
