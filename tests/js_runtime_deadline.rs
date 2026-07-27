#![allow(clippy::unwrap_used, clippy::panic)]

#[cfg(feature = "runtime-js")]
mod js_deadline {
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    use citadel::runtime::{JsRuntime, OutboundCommand, RpcOutcome, Runtime};

    fn fixture_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/js_deadline")
    }

    #[test]
    fn hung_handlers_return_control_and_leave_runtime_usable() {
        let runtime = JsRuntime::load(&fixture_dir(), 50)
            .expect("javascript fixture load succeeds")
            .expect("javascript deadline fixture is present");

        let start = Instant::now();
        let commands = Runtime::dispatch(&runtime, 7, Some("user-7"), 1, b"lost");
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "hung message handler should return within the deadline guard, elapsed: {elapsed:?}"
        );
        assert!(
            commands.is_empty(),
            "timed-out handler side effects should be discarded"
        );

        assert_eq!(
            Runtime::dispatch(&runtime, 7, Some("user-7"), 2, b"ok"),
            vec![OutboundCommand::Broadcast {
                kind: 3,
                body: b"alive:ok".to_vec(),
                unreliable: false,
            }]
        );

        let start = Instant::now();
        let rpc = Runtime::call_rpc(&runtime, 7, Some("user-7"), "hang", b"");
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "hung RPC handler should return within the deadline guard, elapsed: {elapsed:?}"
        );
        assert_eq!(
            rpc,
            RpcOutcome::Err("RPC handler timed out".to_string()),
            "RPC timeout classification should remain observable"
        );
        assert_eq!(
            Runtime::call_rpc(&runtime, 7, Some("user-7"), "ping", b""),
            RpcOutcome::Ok(b"pong".to_vec())
        );
    }
}

#[cfg(not(feature = "runtime-js"))]
#[test]
fn js_runtime_deadline_skips_without_feature() {
    eprintln!("javascript runtime deadline test skipped: build lacks runtime-js feature");
}
