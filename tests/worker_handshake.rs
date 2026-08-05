#![cfg(unix)]

use std::{path::Path, time::Duration};

use citadel::runtime::worker_supervisor::{
    RestartController, SupervisedWorker, WorkerLifecycleService, WorkerResourceLimits,
    WorkerSupervisionPolicy,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_lifecycle_supervises_and_stops_the_external_worker() {
    use std::sync::atomic::Ordering;

    // The same service the transport supervisor spawns for
    // `runtime.adapter = "external-worker"`, driven through the real
    // Supervisor lifecycle: boot, periodic health, cancel, orderly stop.
    let service = WorkerLifecycleService::new(
        std::path::PathBuf::from(env!("CARGO_BIN_EXE_citadel")),
        std::env::temp_dir(),
        WorkerSupervisionPolicy::default()
            .with_health_cadence(Duration::from_millis(50))
            .with_liveness_deadline(Duration::from_secs(2)),
    );
    let healthy_cycles = service.healthy_cycles();
    let mut supervisor = citadel::lifecycle::Supervisor::new();
    supervisor.spawn(service);
    let until = std::time::Instant::now() + Duration::from_secs(5);
    while healthy_cycles.load(Ordering::SeqCst) < 3 && std::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        healthy_cycles.load(Ordering::SeqCst) >= 3,
        "the supervised worker must complete periodic health cycles from the serve lifecycle"
    );
    supervisor
        .shutdown()
        .await
        .expect("the worker service must stop cleanly with the server");
}

#[test]
fn restarted_worker_completes_a_fresh_authenticated_handshake() {
    let executable = Path::new(env!("CARGO_BIN_EXE_citadel")).to_path_buf();
    let mut controller = RestartController::new(
        executable,
        std::env::temp_dir(),
        WorkerSupervisionPolicy::default().with_restart_limit(3),
    );
    let mut worker = controller
        .restart_after_failure()
        .expect("restart must boot a replacement worker")
        .expect("restart permitted below the breaker limit");
    // A restarted worker must have completed the same fresh-secret handshake
    // as a first boot; an unauthenticated replacement has no control stream
    // and can neither report health nor be shut down.
    worker
        .health_check(Duration::from_secs(2))
        .expect("a restarted worker must be authenticated and reporting health");
}

#[test]
fn same_binary_worker_completes_authenticated_bootstrap() {
    let executable = Path::new(env!("CARGO_BIN_EXE_citadel"));
    let secret = [9; 32];
    let nonce = vec![4; 32];
    let mut worker = SupervisedWorker::spawn(
        executable,
        &std::env::temp_dir(),
        &secret,
        &WorkerSupervisionPolicy::default(),
    )
    .expect("worker process starts");

    worker
        .authenticate(&secret, nonce, Duration::from_secs(2))
        .expect("worker must complete the authenticated bootstrap");
    worker
        .health_check(Duration::from_secs(2))
        .expect("worker must report health after readiness");
    worker
        .shutdown(Duration::from_secs(2))
        .expect("worker must acknowledge orderly shutdown");
}

#[test]
fn worker_reports_health_periodically_until_shutdown() {
    let executable = Path::new(env!("CARGO_BIN_EXE_citadel"));
    let secret = [13; 32];
    let mut worker = SupervisedWorker::spawn(
        executable,
        &std::env::temp_dir(),
        &secret,
        &WorkerSupervisionPolicy::default(),
    )
    .expect("worker process starts");
    worker
        .authenticate(&secret, vec![6; 32], Duration::from_secs(2))
        .expect("worker must complete the authenticated bootstrap");
    // Health is a continuous signal, not a one-shot readiness echo: each
    // check must observe a fresh report within the liveness deadline.
    for cycle in 0..3 {
        worker
            .health_check(Duration::from_secs(2))
            .unwrap_or_else(|error| {
                panic!("worker must keep reporting health (cycle {cycle}): {error}")
            });
    }
    worker
        .shutdown(Duration::from_secs(2))
        .expect("worker must acknowledge orderly shutdown");
}

#[test]
fn worker_process_applies_supervised_resource_limits() {
    let executable = Path::new(env!("CARGO_BIN_EXE_citadel"));
    let secret = [11; 32];
    let policy = WorkerSupervisionPolicy::default()
        .with_resource_limits(WorkerResourceLimits::new(64).expect("valid limits"));
    let mut worker = SupervisedWorker::spawn(executable, &std::env::temp_dir(), &secret, &policy)
        .expect("worker process starts");
    worker
        .authenticate(&secret, vec![5; 32], Duration::from_secs(2))
        .expect("worker must complete the authenticated bootstrap");
    // The worker applies the supervisor's policy before reading the bootstrap
    // secret, so after authentication the kernel-visible limit must match it.
    let limits = std::fs::read_to_string(format!("/proc/{}/limits", worker.id()))
        .expect("worker limits are readable");
    let open_files = limits
        .lines()
        .find(|line| line.starts_with("Max open files"))
        .expect("open-file limit row");
    let soft_limit = open_files
        .split_whitespace()
        .nth(3)
        .expect("soft limit column");
    assert_eq!(
        soft_limit, "64",
        "worker must run under the supervisor's open-file policy, got: {open_files}"
    );
    worker
        .health_check(Duration::from_secs(2))
        .expect("worker must report health after readiness");
    worker
        .shutdown(Duration::from_secs(2))
        .expect("worker must acknowledge orderly shutdown");
}
