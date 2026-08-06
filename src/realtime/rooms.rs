//! Room registry (Phase A): server-owned, admission-gated participant groupings.
//!
//! A **room** groups participants and carries a [`RoomLabel`] whose `map` names the
//! level clients in the room should have open. This is Citadel's analog of Nakama's
//! authoritative match + label: the gateway creates/joins rooms, tracks membership,
//! and replies with the label so a joining client knows which map to load. The game's
//! Lua logic sets the label and gates admission (wired in Phase A2); this module owns
//! the state and the fan-out primitives.
//!
//! MVP scope: **one room per participant** (a `membership` index), a single global
//! `TransformWorld` with the room as a filter dimension (applied to snapshots/spawns
//! in Phase A4, not here). Object-id space stays global.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::realtime::registry::ParticipantId;
use crate::runtime::ScriptBinding;
use crate::session::NodeId;

/// Server-assigned room identifier (monotonic, starts at 1; `0` is never a room).
pub type RoomId = u64;

/// A room's game-defined metadata. The `map` is the load-bearing field: it is what
/// the server sends a joining client in `KIND_ROOM_JOINED`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomLabel {
    /// The map/level name clients in this room must have open.
    pub map: String,
    /// Free-form game mode tag (may be empty).
    pub mode: String,
    /// Member cap (`0` = unlimited).
    pub max_players: u16,
    /// Whether new joins are currently accepted.
    pub open: bool,
}

impl RoomLabel {
    /// A label with just a map name: unlimited, open, no mode.
    #[must_use]
    pub fn with_map(map: impl Into<String>) -> Self {
        Self {
            map: map.into(),
            mode: String::new(),
            max_players: 0,
            open: true,
        }
    }
}

/// A point-in-time copy of one live room (see [`RoomRegistry::snapshot`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomSnapshot {
    /// Server-assigned room id.
    pub id: RoomId,
    /// The matchmaking name the room was created under, if any.
    pub name: Option<String>,
    /// The room's game-defined label (map, mode, cap, open).
    pub label: RoomLabel,
    /// Current members, ascending by participant id.
    pub members: Vec<ParticipantId>,
    /// Members proxied by another node. Their local participant ids are not
    /// meaningful on this node, so the console exposes only the count.
    pub remote_member_count: usize,
    /// The GameScript `(revision, generation)` this room was born bound to.
    /// `None` on ungated nodes (`runtime.require_script` off).
    pub script_binding: Option<ScriptBinding>,
}

/// Globally scoped identity for a member proxied by another node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteRoomMember {
    pub node_id: NodeId,
    pub user_id: String,
}

/// Why a join was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinError {
    /// No room with that id exists.
    NoSuchRoom,
    /// The room is at its `max_players` cap.
    Full,
    /// The room's label has `open = false`.
    Closed,
    /// The room is bound to a GameScript revision that is no longer the
    /// loaded one (or carries no binding on a gated node). Admission into a
    /// superseded match fails closed.
    StaleScript,
}

#[derive(Debug)]
struct Room {
    label: RoomLabel,
    members: HashSet<ParticipantId>,
    remote_members: HashSet<RemoteRoomMember>,
    /// The matchmaking name this room was created under, if any (used to evict the
    /// name from the `names` index when the room is pruned). `None` for id-only
    /// rooms created via [`RoomRegistry::create`].
    name: Option<String>,
    /// The GameScript `(revision, generation)` captured from the readiness
    /// snapshot that admitted this room's creation. `None` on ungated nodes.
    binding: Option<ScriptBinding>,
}

#[derive(Debug)]
struct Inner {
    rooms: HashMap<RoomId, Room>,
    /// One room per participant (MVP). Absent = not in any room.
    membership: HashMap<ParticipantId, RoomId>,
    remote_membership: HashMap<RemoteRoomMember, RoomId>,
    /// Matchmaking name -> room id, so `join_or_create` lands everyone asking for
    /// the same name in the same room. Cleared when the room is pruned.
    names: HashMap<String, RoomId>,
    next_id: RoomId,
}

/// The gateway's room state. Interior-mutable (like the session registry) so the
/// shared `Gateway` can drive it behind `&self`.
#[derive(Debug)]
pub struct RoomRegistry {
    inner: Mutex<Inner>,
}

impl RoomRegistry {
    /// An empty registry (no rooms).
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                rooms: HashMap::new(),
                membership: HashMap::new(),
                remote_membership: HashMap::new(),
                names: HashMap::new(),
                next_id: 1,
            }),
        }
    }

    /// Create a new, empty room with `label`; returns its id. Does not add members.
    pub fn create(&self, label: RoomLabel) -> RoomId {
        self.create_bound(label, None)
    }

    /// Create a new, empty room born bound to `binding` (the readiness
    /// snapshot that admitted its creation). `None` preserves the ungated
    /// behavior.
    pub fn create_bound(&self, label: RoomLabel, binding: Option<ScriptBinding>) -> RoomId {
        let mut g = self.lock();
        let id = g.next_id;
        g.next_id += 1;
        g.rooms.insert(
            id,
            Room {
                label,
                members: HashSet::new(),
                remote_members: HashSet::new(),
                name: None,
                binding,
            },
        );
        id
    }

    /// Join the room registered under `name`, or create it (labelled by `make_label`)
    /// and join it. This is the matchmaking primitive: everyone asking for the same
    /// name lands in the **same** room — the first caller creates it, the rest join.
    /// `make_label` is invoked only on creation (so an existing room keeps its label
    /// and the game's `on_room_create` hook does not re-run on a plain join). Returns
    /// the room id and its label, or a [`JoinError`] if an existing named room refuses
    /// the join (full/closed).
    pub fn join_or_create(
        &self,
        participant: ParticipantId,
        name: &str,
        make_label: impl FnOnce() -> RoomLabel,
    ) -> Result<(RoomId, RoomLabel), JoinError> {
        self.join_or_create_bound(participant, name, None, make_label)
    }

    /// [`Self::join_or_create`] under the readiness gate: a newly created room
    /// is born bound to `binding`, and joining an existing named room
    /// requires its binding to match (see [`Self::join_bound`]). `None`
    /// preserves the ungated behavior.
    pub fn join_or_create_bound(
        &self,
        participant: ParticipantId,
        name: &str,
        binding: Option<ScriptBinding>,
        make_label: impl FnOnce() -> RoomLabel,
    ) -> Result<(RoomId, RoomLabel), JoinError> {
        let mut g = self.lock();
        if let Some(&existing) = g.names.get(name) {
            return Self::join_locked(&mut g, participant, existing, false, binding.as_ref());
        }
        // Create the named room, then join the creator into it.
        let id = g.next_id;
        g.next_id += 1;
        g.rooms.insert(
            id,
            Room {
                label: make_label(),
                members: HashSet::new(),
                remote_members: HashSet::new(),
                name: Some(name.to_owned()),
                binding: binding.clone(),
            },
        );
        g.names.insert(name.to_owned(), id);
        Self::join_locked(&mut g, participant, id, false, binding.as_ref())
    }

    /// Add `participant` to `room_id`, first removing it from any prior room (MVP:
    /// one room each). Returns the room's label on success. Empties left behind by
    /// the move are pruned.
    pub fn join(
        &self,
        participant: ParticipantId,
        room_id: RoomId,
    ) -> Result<RoomLabel, JoinError> {
        self.join_bound(participant, room_id, None)
    }

    /// [`Self::join`] under the readiness gate: with `expected` set, the room
    /// must be bound to exactly that `(revision, generation)` or the join is
    /// refused as [`JoinError::StaleScript`]. `None` preserves the ungated
    /// behavior.
    pub fn join_bound(
        &self,
        participant: ParticipantId,
        room_id: RoomId,
        expected: Option<&ScriptBinding>,
    ) -> Result<RoomLabel, JoinError> {
        let mut g = self.lock();
        Self::join_locked(&mut g, participant, room_id, false, expected).map(|(_, label)| label)
    }

    /// Admit a participant selected by the trusted matchmaker into a closed
    /// match room. This is deliberately separate from [`Self::join_bound`]: a
    /// raw `ROOM_JOIN` frame cannot bypass a room's `open = false` policy.
    /// See [`Self::join_bound`] for the `expected` binding contract
    /// (`None` = ungated node).
    pub(crate) fn admit_match_bound(
        &self,
        participant: ParticipantId,
        room_id: RoomId,
        expected: Option<&ScriptBinding>,
    ) -> Result<RoomLabel, JoinError> {
        let mut g = self.lock();
        Self::join_locked(&mut g, participant, room_id, true, expected).map(|(_, label)| label)
    }

    /// Join logic operating on an already-locked `Inner` (shared by [`Self::join`]
    /// and [`Self::join_or_create`]). Validates the script binding and open/cap
    /// first so a failed join never disturbs current state, moves the participant
    /// out of any prior room (pruning an emptied one), then inserts and returns
    /// `(room_id, label)`.
    fn join_locked(
        g: &mut Inner,
        participant: ParticipantId,
        room_id: RoomId,
        bypass_closed: bool,
        expected: Option<&ScriptBinding>,
    ) -> Result<(RoomId, RoomLabel), JoinError> {
        {
            let room = g.rooms.get(&room_id).ok_or(JoinError::NoSuchRoom)?;
            Self::check_binding(room, expected)?;
            if !bypass_closed && !room.label.open {
                return Err(JoinError::Closed);
            }
            let cap = room.label.max_players;
            let already_here = room.members.contains(&participant);
            if cap != 0
                && !already_here
                && room.members.len() + room.remote_members.len() >= usize::from(cap)
            {
                return Err(JoinError::Full);
            }
        }
        if let Some(prev) = g.membership.get(&participant).copied()
            && prev != room_id
        {
            Self::remove_member(g, participant, prev);
        }
        let label = {
            let room = g
                .rooms
                .get_mut(&room_id)
                .expect("room existence checked above");
            room.members.insert(participant);
            room.label.clone()
        };
        g.membership.insert(participant, room_id);
        Ok((room_id, label))
    }

    /// Admit a member whose transport is owned by another node. This trusted
    /// boundary preserves room capacity and one-room membership without treating
    /// the remote node's local participant id as globally unique.
    /// See [`Self::join_bound`] for the `expected` binding contract
    /// (`None` = ungated node).
    pub(crate) fn admit_remote_match_bound(
        &self,
        member: RemoteRoomMember,
        room_id: RoomId,
        expected: Option<&ScriptBinding>,
    ) -> Result<RoomLabel, JoinError> {
        let mut g = self.lock();
        {
            let room = g.rooms.get(&room_id).ok_or(JoinError::NoSuchRoom)?;
            Self::check_binding(room, expected)?;
            let already_here = room.remote_members.contains(&member);
            if room.label.max_players != 0
                && !already_here
                && room.members.len() + room.remote_members.len()
                    >= usize::from(room.label.max_players)
            {
                return Err(JoinError::Full);
            }
        }
        if let Some(previous) = g.remote_membership.get(&member).copied()
            && previous != room_id
        {
            Self::remove_remote_member(&mut g, &member, previous);
        }
        let label = g.rooms.get_mut(&room_id).ok_or(JoinError::NoSuchRoom)?;
        label.remote_members.insert(member.clone());
        let label = label.label.clone();
        g.remote_membership.insert(member, room_id);
        Ok(label)
    }

    /// Remove `participant` from whatever room it is in. Returns the room it left and
    /// the members that remain (for leave notifications), or `None` if it was in no
    /// room. A room that empties is pruned.
    pub fn leave(&self, participant: ParticipantId) -> Option<(RoomId, Vec<ParticipantId>)> {
        let mut g = self.lock();
        let room_id = g.membership.remove(&participant)?;
        Self::remove_member(&mut g, participant, room_id);
        let remaining = g
            .rooms
            .get(&room_id)
            .map(|r| r.members.iter().copied().collect())
            .unwrap_or_default();
        Some((room_id, remaining))
    }

    /// The room a participant is currently in, if any.
    #[must_use]
    pub fn room_of(&self, participant: ParticipantId) -> Option<RoomId> {
        self.lock().membership.get(&participant).copied()
    }

    /// Run `f` only while `participant` still belongs to `expected_room`.
    ///
    /// The membership comparison and `f` share the registry lock, giving
    /// client-delivery paths a linearization point: a room move cannot land
    /// between the check and a queued outbound frame.
    pub fn while_member_in<R>(
        &self,
        participant: ParticipantId,
        expected_room: Option<RoomId>,
        f: impl FnOnce() -> R,
    ) -> Option<R> {
        let g = self.lock();
        (g.membership.get(&participant).copied() == expected_room).then(f)
    }

    /// Run `f` only while a snapshot recipient and every owning participant
    /// captured for that snapshot all remain in `expected_room`. The membership
    /// checks and enqueue share one lock, so neither a recipient nor a source
    /// owner can move between validation and client delivery.
    pub fn while_member_and_owners_in<R>(
        &self,
        participant: ParticipantId,
        expected_room: Option<RoomId>,
        owners: &[u64],
        f: impl FnOnce() -> R,
    ) -> Option<R> {
        let g = self.lock();
        (g.membership.get(&participant).copied() == expected_room
            && owners.iter().all(|owner| {
                g.membership.get(&ParticipantId::from_raw(*owner)).copied() == expected_room
            }))
        .then(f)
    }

    /// Run `f` only while both participants remain in the same room scope.
    /// Roomless participants share the legacy relay-compatible scope. The check
    /// and `f` are one membership-lock critical section, so a move cannot turn
    /// a validated cross-participant delivery into a stale-room enqueue.
    pub fn while_same_room<R>(
        &self,
        first: ParticipantId,
        second: ParticipantId,
        f: impl FnOnce() -> R,
    ) -> Option<R> {
        let g = self.lock();
        (g.membership.get(&first) == g.membership.get(&second)).then(f)
    }

    /// Run `f` while `room_id` remains live and expose its current local members.
    /// This holds the membership lock through nonblocking session queue writes,
    /// making a room-scoped fan-out atomic with respect to member moves.
    pub fn while_members_in<R>(
        &self,
        room_id: RoomId,
        f: impl FnOnce(&HashSet<ParticipantId>) -> R,
    ) -> Option<R> {
        let g = self.lock();
        g.rooms.get(&room_id).map(|room| f(&room.members))
    }

    /// The current members of a room (empty if it does not exist).
    #[must_use]
    pub fn members(&self, room_id: RoomId) -> Vec<ParticipantId> {
        self.lock()
            .rooms
            .get(&room_id)
            .map(|r| r.members.iter().copied().collect())
            .unwrap_or_default()
    }

    /// A room's label, if it exists.
    #[must_use]
    pub fn label(&self, room_id: RoomId) -> Option<RoomLabel> {
        self.lock().rooms.get(&room_id).map(|r| r.label.clone())
    }

    /// The script binding a room was born with, if the room exists and was
    /// created under the readiness gate.
    #[must_use]
    pub fn binding(&self, room_id: RoomId) -> Option<ScriptBinding> {
        self.lock()
            .rooms
            .get(&room_id)
            .and_then(|r| r.binding.clone())
    }

    /// Fail-closed script-binding check shared by every admission path.
    ///
    /// With `expected` set (a gated node's current Ready snapshot), the room
    /// must carry exactly that binding: a room from a superseded load — or a
    /// room that somehow has no binding at all on a gated node — refuses
    /// admission as [`JoinError::StaleScript`]. `expected = None` (ungated
    /// node) checks nothing.
    fn check_binding(room: &Room, expected: Option<&ScriptBinding>) -> Result<(), JoinError> {
        match expected {
            None => Ok(()),
            Some(expected) if room.binding.as_ref() == Some(expected) => Ok(()),
            Some(_) => Err(JoinError::StaleScript),
        }
    }

    /// Number of live rooms (tests/metrics).
    #[must_use]
    pub fn room_count(&self) -> usize {
        self.lock().rooms.len()
    }

    /// A point-in-time copy of every live room, id-ordered.
    ///
    /// Used by the console's Matches section. This clones under the same lock
    /// the hot paths use, so it is intended for operator-frequency reads (a
    /// dashboard poll), not per-tick game logic.
    #[must_use]
    pub fn snapshot(&self) -> Vec<RoomSnapshot> {
        let g = self.lock();
        let mut rooms: Vec<RoomSnapshot> = g
            .rooms
            .iter()
            .map(|(&id, room)| {
                let mut members: Vec<ParticipantId> = room.members.iter().copied().collect();
                members.sort_unstable();
                RoomSnapshot {
                    id,
                    name: room.name.clone(),
                    label: room.label.clone(),
                    members,
                    remote_member_count: room.remote_members.len(),
                    script_binding: room.binding.clone(),
                }
            })
            .collect();
        rooms.sort_unstable_by_key(|room| room.id);
        rooms
    }

    /// Remove an empty room that has not accepted any member. Used when every
    /// matchmaker handoff for a newly allocated room expires before acceptance.
    /// Named rooms retain the existing name-index cleanup behavior.
    pub(crate) fn discard_empty(&self, room_id: RoomId) -> bool {
        let mut g = self.lock();
        let Some(room) = g.rooms.get(&room_id) else {
            return false;
        };
        if !room.members.is_empty() || !room.remote_members.is_empty() {
            return false;
        }
        let name = room.name.clone();
        g.rooms.remove(&room_id);
        if let Some(name) = name {
            g.names.remove(&name);
        }
        true
    }

    /// Close a whole room server-side: remove the room (and its matchmaking
    /// name) and clear every member's membership in one step. Returns the
    /// local members that were in the room, ascending, or `None` if no such
    /// room exists. Used by the match-closure flow: the gateway informs the
    /// members and returns them to matchmaking after the prune.
    pub(crate) fn close(&self, room_id: RoomId) -> Option<Vec<ParticipantId>> {
        let mut g = self.lock();
        let room = g.rooms.remove(&room_id)?;
        if let Some(name) = &room.name {
            g.names.remove(name);
        }
        for member in &room.members {
            g.membership.remove(member);
        }
        for remote in &room.remote_members {
            g.remote_membership.remove(remote);
        }
        let mut members: Vec<ParticipantId> = room.members.into_iter().collect();
        members.sort_unstable();
        Some(members)
    }

    fn remove_member(g: &mut Inner, participant: ParticipantId, room_id: RoomId) {
        let (empty, name) = if let Some(room) = g.rooms.get_mut(&room_id) {
            room.members.remove(&participant);
            (
                room.members.is_empty() && room.remote_members.is_empty(),
                room.name.clone(),
            )
        } else {
            (false, None)
        };
        if empty {
            g.rooms.remove(&room_id);
            if let Some(name) = name {
                g.names.remove(&name);
            }
        }
    }

    fn remove_remote_member(g: &mut Inner, member: &RemoteRoomMember, room_id: RoomId) {
        let (empty, name) = if let Some(room) = g.rooms.get_mut(&room_id) {
            room.remote_members.remove(member);
            (
                room.members.is_empty() && room.remote_members.is_empty(),
                room.name.clone(),
            )
        } else {
            (false, None)
        };
        if empty {
            g.rooms.remove(&room_id);
            if let Some(name) = name {
                g.names.remove(&name);
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Default for RoomRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use super::*;

    fn pid(n: u64) -> ParticipantId {
        ParticipantId::from_raw(n)
    }

    #[test]
    fn join_or_create_shares_one_room_by_name() {
        let r = RoomRegistry::new();
        let (id1, l1) = r
            .join_or_create(pid(1), "lobby", || RoomLabel::with_map("MapA"))
            .unwrap();
        // Second participant, same name -> SAME room; make_label must NOT run again.
        let (id2, l2) = r
            .join_or_create(pid(2), "lobby", || {
                panic!("existing room must not re-label")
            })
            .unwrap();
        assert_eq!(id1, id2, "same name lands both in one room");
        assert_eq!(l1.map, "MapA");
        assert_eq!(l2.map, "MapA", "joiner gets the creator's label");
        assert_eq!(r.room_count(), 1);
        assert_eq!(r.members(id1).len(), 2);
        // A different name is a different room.
        let (id3, _) = r
            .join_or_create(pid(3), "arena", || RoomLabel::with_map("MapB"))
            .unwrap();
        assert_ne!(id3, id1);
        assert_eq!(r.room_count(), 2);
    }

    #[test]
    fn join_or_create_frees_the_name_when_room_prunes() {
        let r = RoomRegistry::new();
        let (id1, _) = r
            .join_or_create(pid(1), "lobby", || RoomLabel::with_map("M"))
            .unwrap();
        r.leave(pid(1)); // last member leaves -> room pruned, name freed
        assert_eq!(r.room_count(), 0);
        // Re-creating "lobby" now makes a fresh room, not a dangling id.
        let (id2, _) = r
            .join_or_create(pid(2), "lobby", || RoomLabel::with_map("M2"))
            .unwrap();
        assert_ne!(id1, id2);
        assert_eq!(r.room_count(), 1);
    }

    #[test]
    fn create_join_delivers_label_and_tracks_membership() {
        let r = RoomRegistry::new();
        let id = r.create(RoomLabel::with_map("ForestArena"));
        let label = r.join(pid(1), id).unwrap();
        assert_eq!(label.map, "ForestArena");
        assert_eq!(r.room_of(pid(1)), Some(id));
        assert_eq!(r.members(id), vec![pid(1)]);
    }

    #[test]
    fn join_nonexistent_room_is_rejected() {
        let r = RoomRegistry::new();
        assert_eq!(r.join(pid(1), 999), Err(JoinError::NoSuchRoom));
        assert_eq!(r.room_of(pid(1)), None);
    }

    #[test]
    fn join_respects_capacity_and_closed() {
        let r = RoomRegistry::new();
        let full = r.create(RoomLabel {
            map: "M".into(),
            mode: String::new(),
            max_players: 1,
            open: true,
        });
        r.join(pid(1), full).unwrap();
        assert_eq!(r.join(pid(2), full), Err(JoinError::Full));
        // Re-joining as an existing member is allowed (idempotent).
        assert!(r.join(pid(1), full).is_ok());

        let closed = r.create(RoomLabel {
            map: "M".into(),
            mode: String::new(),
            max_players: 0,
            open: false,
        });
        assert_eq!(r.join(pid(3), closed), Err(JoinError::Closed));
        assert!(r.admit_match_bound(pid(3), closed, None).is_ok());
        assert_eq!(r.room_of(pid(3)), Some(closed));
    }

    #[test]
    fn remote_match_admission_counts_toward_capacity_without_local_id_collision() {
        let r = RoomRegistry::new();
        let room = r.create(RoomLabel {
            map: "M".into(),
            mode: "matchmaker".into(),
            max_players: 2,
            open: false,
        });
        let remote = RemoteRoomMember {
            node_id: NodeId::new("node-b").expect("node id"),
            user_id: "bob".to_owned(),
        };
        assert!(r.admit_remote_match_bound(remote, room, None).is_ok());
        assert!(r.admit_match_bound(pid(1), room, None).is_ok());
        assert_eq!(
            r.admit_match_bound(pid(2), room, None),
            Err(JoinError::Full)
        );
        let snapshot = r.snapshot();
        assert_eq!(snapshot[0].members, vec![pid(1)]);
        assert_eq!(snapshot[0].remote_member_count, 1);
    }

    #[test]
    fn joining_a_second_room_moves_the_participant() {
        let r = RoomRegistry::new();
        let a = r.create(RoomLabel::with_map("A"));
        let b = r.create(RoomLabel::with_map("B"));
        r.join(pid(1), a).unwrap();
        r.join(pid(1), b).unwrap();
        assert_eq!(r.room_of(pid(1)), Some(b));
        // Room A emptied and was pruned.
        assert_eq!(r.members(a), Vec::<ParticipantId>::new());
        assert_eq!(r.room_count(), 1);
    }

    #[test]
    fn snapshot_reports_rooms_id_ordered_with_sorted_members() {
        let r = RoomRegistry::new();
        assert!(r.snapshot().is_empty());
        let (named, _) = r
            .join_or_create(pid(2), "lobby", || RoomLabel::with_map("MapA"))
            .unwrap();
        r.join(pid(1), named).unwrap();
        let solo = r.create(RoomLabel::with_map("MapB"));
        r.join(pid(3), solo).unwrap();

        let snapshot = r.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot[0].id < snapshot[1].id, "id-ordered");
        let lobby = snapshot.iter().find(|s| s.id == named).unwrap();
        assert_eq!(lobby.name.as_deref(), Some("lobby"));
        assert_eq!(lobby.label.map, "MapA");
        assert_eq!(lobby.members, vec![pid(1), pid(2)], "member-sorted");
        let other = snapshot.iter().find(|s| s.id == solo).unwrap();
        assert_eq!(other.name, None);
        assert_eq!(other.members, vec![pid(3)]);
    }

    fn binding(revision: &str, generation: u64) -> ScriptBinding {
        ScriptBinding {
            revision_id: revision.to_owned(),
            generation,
        }
    }

    #[test]
    fn rooms_are_born_bound_and_report_their_binding() {
        let r = RoomRegistry::new();
        let bound = r.create_bound(RoomLabel::with_map("M"), Some(binding("sha256:v1", 1)));
        assert_eq!(r.binding(bound), Some(binding("sha256:v1", 1)));
        let (named, _) = r
            .join_or_create_bound(pid(1), "lobby", Some(binding("sha256:v1", 1)), || {
                RoomLabel::with_map("M")
            })
            .expect("bound create joins");
        assert_eq!(r.binding(named), Some(binding("sha256:v1", 1)));
        let snapshot = r.snapshot();
        assert!(
            snapshot
                .iter()
                .all(|room| room.script_binding == Some(binding("sha256:v1", 1))),
            "snapshots expose the birth binding"
        );
        // Ungated rooms carry no binding.
        let unbound = r.create(RoomLabel::with_map("M"));
        assert_eq!(r.binding(unbound), None);
    }

    #[test]
    fn admission_refuses_a_stale_or_missing_binding() {
        let r = RoomRegistry::new();
        let v1 = binding("sha256:v1", 1);
        let v2 = binding("sha256:v1", 2); // same content, superseded load
        let room = r.create_bound(
            RoomLabel {
                map: "M".into(),
                mode: "matchmaker".into(),
                max_players: 0,
                open: false,
            },
            Some(v1.clone()),
        );

        // The bound revision is still loaded: admission proceeds.
        assert!(r.admit_match_bound(pid(1), room, Some(&v1)).is_ok());
        // A newer generation supersedes the room: fail closed.
        assert_eq!(
            r.admit_match_bound(pid(2), room, Some(&v2)),
            Err(JoinError::StaleScript)
        );
        assert_eq!(
            r.join_bound(pid(2), room, Some(&v2)),
            Err(JoinError::StaleScript)
        );
        let remote = RemoteRoomMember {
            node_id: NodeId::new("node-b").expect("node id"),
            user_id: "bob".to_owned(),
        };
        assert_eq!(
            r.admit_remote_match_bound(remote.clone(), room, Some(&v2)),
            Err(JoinError::StaleScript)
        );
        assert_eq!(
            r.admit_remote_match_bound(remote, room, Some(&v1))
                .map(|_| ()),
            Ok(())
        );
        // A failed stale admission never disturbed membership.
        assert_eq!(r.members(room), vec![pid(1)]);

        // An unbound room on a gated node is structurally impossible to admit
        // into: no placeholder matches.
        let placeholder = r.create(RoomLabel::with_map("M"));
        assert_eq!(
            r.join_bound(pid(3), placeholder, Some(&v1)),
            Err(JoinError::StaleScript)
        );
    }

    #[test]
    fn close_removes_room_membership_and_name() {
        let r = RoomRegistry::new();
        let (id, _) = r
            .join_or_create(pid(1), "arena", || RoomLabel::with_map("M"))
            .unwrap();
        r.join(pid(2), id).unwrap();
        let members = r.close(id).expect("room exists");
        assert_eq!(members, vec![pid(1), pid(2)], "members reported, sorted");
        assert_eq!(r.room_count(), 0);
        assert_eq!(r.room_of(pid(1)), None);
        assert_eq!(r.room_of(pid(2)), None);
        // The matchmaking name is freed: re-creating "arena" makes a fresh room.
        let (id2, _) = r
            .join_or_create(pid(3), "arena", || RoomLabel::with_map("M2"))
            .unwrap();
        assert_ne!(id, id2);
        // Closing a nonexistent room is a no-op.
        assert_eq!(r.close(id), None);
    }

    #[test]
    fn leave_returns_remaining_and_prunes_empty() {
        let r = RoomRegistry::new();
        let id = r.create(RoomLabel::with_map("M"));
        r.join(pid(1), id).unwrap();
        r.join(pid(2), id).unwrap();
        let (left, remaining) = r.leave(pid(1)).unwrap();
        assert_eq!(left, id);
        assert_eq!(remaining, vec![pid(2)]);
        // Last member leaves -> room pruned.
        assert!(r.leave(pid(2)).is_some());
        assert_eq!(r.room_count(), 0);
        // Leaving when in no room is a no-op.
        assert_eq!(r.leave(pid(3)), None);
    }
}
