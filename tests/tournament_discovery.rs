//! Player-facing tournament discovery contract.

use std::sync::Arc;

use citadel::repository::{
    CreateTournamentRequest, InMemoryTournamentsRepository, TournamentState, TournamentsRepository,
};
use citadel::services::{TournamentDiscoveryService, TournamentRegistrationState};
use citadel::time::TimestampMillis;

fn ts(value: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(value)
}

fn request(id: &str, leaderboard_id: &str, starts_at: u64) -> CreateTournamentRequest {
    CreateTournamentRequest {
        id: id.to_owned(),
        leaderboard_id: leaderboard_id.to_owned(),
        registration_opens_at: ts(starts_at - 20),
        registration_closes_at: ts(starts_at - 10),
        starts_at: ts(starts_at),
        ends_at: ts(starts_at + 10),
    }
}

#[tokio::test]
async fn discovery_lists_player_visible_upcoming_and_active_tournaments_by_start_time() {
    let repo = Arc::new(InMemoryTournamentsRepository::new());
    let service = TournamentDiscoveryService::new(repo.clone());

    for (id, starts_at) in [("later", 50), ("earlier", 30), ("live", 20), ("draft", 40)] {
        repo.create(request(id, "scores", starts_at), ts(0))
            .await
            .expect("create tournament");
    }
    for id in ["later", "earlier", "live"] {
        repo.transition(id, TournamentState::RegistrationOpen, ts(10))
            .await
            .expect("open registration");
    }
    repo.transition("live", TournamentState::Running, ts(20))
        .await
        .expect("start live tournament");

    let tournaments = service
        .list_active_and_upcoming()
        .await
        .expect("discover tournaments");

    assert_eq!(
        tournaments
            .iter()
            .map(|tournament| tournament.id.as_str())
            .collect::<Vec<_>>(),
        ["live", "earlier", "later"]
    );
    assert!(
        tournaments
            .iter()
            .all(|tournament| tournament.id != "draft")
    );
}

#[tokio::test]
async fn details_results_and_registration_state_are_player_scoped() {
    let repo = Arc::new(InMemoryTournamentsRepository::new());
    let service = TournamentDiscoveryService::new(repo.clone());
    repo.create(request("weekly", "scores", 30), ts(0))
        .await
        .expect("create tournament");
    repo.transition("weekly", TournamentState::RegistrationOpen, ts(10))
        .await
        .expect("open registration");
    repo.register("weekly", "alice", ts(11))
        .await
        .expect("register alice");

    let detail = service.get("weekly").await.expect("player detail");
    assert_eq!(detail.id, "weekly");
    assert_eq!(detail.leaderboard_id, "scores");
    assert!(
        service
            .results("weekly")
            .await
            .expect("empty pre-settlement results")
            .is_empty()
    );

    let alice = service
        .registration_state("weekly", "alice", ts(12))
        .await
        .expect("alice registration state");
    assert_eq!(alice, TournamentRegistrationState::Registered);

    let bob = service
        .registration_state("weekly", "bob", ts(12))
        .await
        .expect("bob registration state");
    assert_eq!(bob, TournamentRegistrationState::Open);

    let closed = service
        .registration_state("weekly", "bob", ts(20))
        .await
        .expect("closed registration state");
    assert_eq!(closed, TournamentRegistrationState::Closed);
}
