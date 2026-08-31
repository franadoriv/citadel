#![allow(clippy::panic)]

use std::time::Duration;

use citadel::runtime::{
    LifecycleHook, OutboundCommand, PhysicsOptions, RoomBridgeMode, RoomSpec, RpcOutcome, Runtime,
};
use citadel_physics::{PhysicsConfig, Shape};

const NPC_ID_BASE: u32 = 0x4000_0000;

pub fn assert_host_api_smoke_contract<R: Runtime>(runtime: &R) {
    assert_eq!(
        Runtime::dispatch(runtime, 9, Some("user-1"), 1, b"body"),
        vec![OutboundCommand::Broadcast {
            kind: 2,
            body: b"hello:body".to_vec(),
            unreliable: true,
        }]
    );

    let join = Runtime::dispatch_lifecycle(runtime, LifecycleHook::Join, 9, Some("user-1"));
    assert_eq!(join.len(), 2);
    let actor_id = match &join[0] {
        OutboundCommand::SpawnActor {
            object_id,
            archetype,
            position,
        } => {
            assert!(*object_id >= NPC_ID_BASE);
            assert_eq!(*archetype, 7);
            assert_eq!(*position, [1.0, 2.0, 3.0]);
            *object_id
        }
        other => panic!("expected spawn_actor command, got {other:?}"),
    };
    assert_eq!(
        join[1],
        OutboundCommand::Send {
            session: 9,
            kind: 3,
            body: b"joined".to_vec(),
            unreliable: false,
        }
    );

    let tick = Runtime::tick(
        runtime,
        Duration::from_millis(16),
        Duration::from_millis(100),
    );
    assert_eq!(
        tick,
        vec![
            OutboundCommand::MoveActor {
                object_id: actor_id,
                position: [4.0, 5.0, 6.0],
                rotation: [0.0, 0.0, 0.0, 1.0],
                velocity: [7.0, 8.0, 9.0],
            },
            OutboundCommand::SetPhysics {
                object_id: actor_id,
                opts: Some(PhysicsOptions {
                    enabled: true,
                    config: PhysicsConfig {
                        shape: Shape::Capsule {
                            radius: 30.0,
                            height: 90.0,
                        },
                        gravity: 900.0,
                        buoyancy: 300.0,
                        drag: 0.5,
                        max_speed: 400.0,
                    },
                }),
            },
            OutboundCommand::ApplyImpulse {
                object_id: actor_id,
                impulse: [1.0, 2.0, 3.0],
            },
            OutboundCommand::SetMoveIntent {
                object_id: actor_id,
                intent: [4.0, 0.0, -5.0],
            },
        ]
    );

    assert_eq!(
        Runtime::dispatch_lifecycle(runtime, LifecycleHook::Leave, 9, Some("user-1")),
        vec![OutboundCommand::DespawnActor {
            object_id: actor_id,
        }]
    );

    assert_eq!(
        Runtime::call_rpc(runtime, 9, Some("user-1"), "ping", b""),
        RpcOutcome::Ok(b"pong".to_vec())
    );

    assert_eq!(
        Runtime::call_room_create(runtime, 9, Some("user-1"), b""),
        Some(RoomSpec {
            map: "Arena".to_string(),
            mode: "duel".to_string(),
            max_players: 2,
            open: true,
            bridge_mode: RoomBridgeMode::Relay,
        })
    );
    assert!(Runtime::call_room_join(runtime, 9, Some("user-1"), 7));
    assert!(!Runtime::call_room_join(runtime, 9, Some("user-1"), 8));
}
