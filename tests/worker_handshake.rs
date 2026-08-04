#![cfg(unix)]

use std::{path::Path, time::Duration};

use citadel::runtime::worker_supervisor::SupervisedWorker;

#[test]
fn same_binary_worker_completes_authenticated_bootstrap() {
    let executable = Path::new(env!("CARGO_BIN_EXE_citadel"));
    let secret = [9; 32];
    let nonce = vec![4; 32];
    let mut worker = SupervisedWorker::spawn(executable, &std::env::temp_dir(), &secret)
        .expect("worker process starts");

    worker
        .authenticate(&secret, nonce, Duration::from_secs(2))
        .expect("worker must complete the authenticated bootstrap");
}
