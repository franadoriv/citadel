//! Console Matches section (live rooms introspection, ).
//!
//! Reads point-in-time snapshots of the realtime gateway's
//! [`RoomRegistry`](crate::realtime::RoomRegistry):
//!
//! - `GET /console/v1/matches` — every live room (id, name, map, mode, player
//!   counts, open flag), id-ordered, with an optional map/name/mode filter.
//! - `GET /console/v1/matches/{id}` — one room plus its member roll
//!   (participant ids, and the authenticated user id behind each when bound).
//!
//! Before the transports start (or on an HTTP-only node) there is no gateway;
//! the listing then reports `realtime_attached: false` with zero rooms rather
//! than erroring — an empty node and a transportless node look the same to an
//! operator, deliberately.

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::error::AppError;
use crate::http::error::ApiError;
use crate::matchmaker::MatchmakerStats;
use crate::realtime::{RoomId, RoomSnapshot};
use crate::services::ConsolePrincipal;

/// The Matches section route.
pub const MATCHES_PATH: &str = "/console/v1/matches";

/// Single-match detail route pattern.
pub const MATCH_DETAIL_PATH: &str = "/console/v1/matches/:id";

/// Default listing page size.
const DEFAULT_LIMIT: usize = 100;
/// Hard ceiling on one listing page.
const MAX_LIMIT: usize = 500;

/// Accepted query parameters for [`MATCHES_PATH`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchesQuery {
    /// Case-sensitive substring filter over the room's name, map, and mode.
    pub filter: Option<String>,
    /// Maximum rooms returned (default 100, capped at 500).
    pub limit: Option<usize>,
}

/// One room row in the listing.
#[derive(Debug, Clone, Serialize)]
pub struct MatchRow {
    /// Server-assigned room id.
    pub id: RoomId,
    /// Matchmaking name the room was created under, if any.
    pub name: Option<String>,
    /// Map/level the room's clients load.
    pub map: String,
    /// Free-form game mode tag (may be empty).
    pub mode: String,
    /// Current member count.
    pub players: usize,
    /// Member cap (`0` = unlimited).
    pub max_players: u16,
    /// Whether new joins are currently accepted.
    pub open: bool,
    /// GameScript revision the room was born bound to (`require_script`
    /// nodes; `None` on ungated nodes).
    pub script_revision: Option<String>,
    /// Load generation of that revision at the room's birth.
    pub script_generation: Option<u64>,
}

impl MatchRow {
    fn from_snapshot(snapshot: &RoomSnapshot) -> Self {
        Self {
            id: snapshot.id,
            name: snapshot.name.clone(),
            map: snapshot.label.map.clone(),
            mode: snapshot.label.mode.clone(),
            players: snapshot.members.len() + snapshot.remote_member_count,
            max_players: snapshot.label.max_players,
            open: snapshot.label.open,
            script_revision: snapshot
                .script_binding
                .as_ref()
                .map(|binding| binding.revision_id.clone()),
            script_generation: snapshot
                .script_binding
                .as_ref()
                .map(|binding| binding.generation),
        }
    }
}

/// The JSON response for [`MATCHES_PATH`].
#[derive(Debug, Clone, Serialize)]
pub struct MatchesResponse {
    /// Whether a realtime gateway is attached (transports running).
    pub realtime_attached: bool,
    /// Total live rooms before filtering/limiting.
    pub total: usize,
    /// Non-sensitive local ticket-queue telemetry. `None` when no realtime
    /// gateway is attached.
    pub matchmaker: Option<MatchmakerStats>,
    /// Matching rooms, id-ordered.
    pub items: Vec<MatchRow>,
}

/// One member row in the match detail.
#[derive(Debug, Clone, Serialize)]
pub struct MemberRow {
    /// Transport-level participant id.
    pub participant: u64,
    /// The authenticated account behind the participant, when bound.
    pub user_id: Option<String>,
}

/// The JSON response for [`MATCH_DETAIL_PATH`].
#[derive(Debug, Clone, Serialize)]
pub struct MatchDetail {
    /// The room row.
    #[serde(flatten)]
    pub row: MatchRow,
    /// Member roll, ascending by participant id.
    pub members: Vec<MemberRow>,
}

/// `GET /console/v1/matches`: list live rooms.
///
/// On a `runtime.require_script` node the listing is an enforcement surface:
/// while the readiness gate is not `Ready` no matches are advertised — the
/// endpoint fails closed with the stable `503 runtime_unavailable` error
/// (the console's readiness surface on `GET /console/v1/runtime` explains
/// why to the operator).
pub(super) async fn list_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Query(query): Query<MatchesQuery>,
) -> Result<Json<MatchesResponse>, ApiError> {
    app.metrics().record_http_request();
    let Some(gateway) = app.realtime_gateway() else {
        return Ok(Json(MatchesResponse {
            realtime_attached: false,
            total: 0,
            matchmaker: None,
            items: Vec::new(),
        }));
    };
    console_script_gate(&app, &gateway)?;
    let snapshot = gateway.room_snapshot();
    let total = snapshot.len();
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let items = snapshot
        .iter()
        .filter(|room| matches_filter(room, query.filter.as_deref()))
        .take(limit)
        .map(MatchRow::from_snapshot)
        .collect();
    Ok(Json(MatchesResponse {
        realtime_attached: true,
        total,
        matchmaker: Some(gateway.matchmaker_stats()),
        items,
    }))
}

/// `GET /console/v1/matches/{id}`: one room with its member roll. Fails
/// closed like the listing on a gated, not-ready node.
pub(super) async fn detail_handler(
    State(app): State<App>,
    _operator: ConsolePrincipal,
    Path(id): Path<RoomId>,
) -> Result<Json<MatchDetail>, ApiError> {
    app.metrics().record_http_request();
    let gateway = app
        .realtime_gateway()
        .ok_or_else(|| AppError::not_found("no realtime gateway is running"))?;
    console_script_gate(&app, &gateway)?;
    let snapshot = gateway
        .room_snapshot()
        .into_iter()
        .find(|room| room.id == id)
        .ok_or_else(|| AppError::not_found("no such match"))?;
    let members = snapshot
        .members
        .iter()
        .map(|&participant| MemberRow {
            participant: participant.get(),
            user_id: gateway.registry().user_id_of(participant),
        })
        .collect();
    Ok(Json(MatchDetail {
        row: MatchRow::from_snapshot(&snapshot),
        members,
    }))
}

/// The listing/detail enforcement surface of the GameScript readiness gate.
///
/// `Ok` when the node is ungated or the gate is `Ready`; otherwise counts the
/// rejection and fails closed with the stable client-safe `503`.
fn console_script_gate(app: &App, gateway: &crate::realtime::Gateway) -> Result<(), ApiError> {
    let Some(readiness) = gateway.script_readiness() else {
        return Ok(());
    };
    if readiness.gate().is_err() {
        app.metrics()
            .record_script_gate_rejection(crate::observability::ScriptGateSurface::ConsoleList);
        return Err(ApiError::script_unavailable());
    }
    Ok(())
}

/// Whether a room matches the listing filter (name, map, or mode substring).
fn matches_filter(room: &RoomSnapshot, filter: Option<&str>) -> bool {
    let Some(filter) = filter else { return true };
    room.name
        .as_deref()
        .is_some_and(|name| name.contains(filter))
        || room.label.map.contains(filter)
        || room.label.mode.contains(filter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::realtime::RoomLabel;

    fn snapshot(name: Option<&str>, map: &str, mode: &str) -> RoomSnapshot {
        RoomSnapshot {
            id: 1,
            match_id: "mt1-0000000000001000100000000000a".to_string(),
            name: name.map(str::to_string),
            label: RoomLabel {
                map: map.to_string(),
                mode: mode.to_string(),
                max_players: 0,
                open: true,
            },
            members: Vec::new(),
            remote_member_count: 0,
            script_binding: None,
        }
    }

    #[test]
    fn filter_matches_name_map_or_mode_substring() {
        let room = snapshot(Some("lobby-eu"), "ForestArena", "ctf");
        assert!(matches_filter(&room, None));
        assert!(matches_filter(&room, Some("lobby")));
        assert!(matches_filter(&room, Some("Forest")));
        assert!(matches_filter(&room, Some("ctf")));
        assert!(!matches_filter(&room, Some("desert")));
        // Unnamed rooms still match on map.
        assert!(matches_filter(&snapshot(None, "MapA", ""), Some("MapA")));
    }

    #[test]
    fn matches_path_is_a_registered_section() {
        assert!(super::super::SECTION_PATHS.contains(&MATCHES_PATH));
        assert!(MATCH_DETAIL_PATH.starts_with(MATCHES_PATH));
    }
}
