//! Script hot-reload watcher service.
//!
//! When `runtime.hot_reload` is enabled and a reloadable script is loaded, the
//! bootstrap layer spawns a [`LuaReloadService`] on the transport [`Supervisor`].
//! It polls the script file's modification time and size on a small interval and,
//! when either changes, drives one [`Runtime::reload`] so a developer can edit
//! the selected entrypoint and see new handlers take effect without restarting
//! the node.
//!
//! Detection approach (poll, not `notify`):
//!
//! - A dependency-free mtime+size poll reuses the exact `AsyncService` +
//!   `spawn_blocking` pattern already proven by [`LuaTickService`](crate::realtime::LuaTickService).
//!   A file-watch crate (`notify`) would add a dependency, a background thread,
//!   and platform-specific event semantics for a dev-only convenience whose
//!   latency budget is "a human just hit save" — a sub-second poll is ample. If a
//!   future need (large script trees, sub-100ms latency) justifies it, `notify`
//!   can replace the poll behind this same service boundary.
//!
//! Safety model (reviewed for the swap-under-lock and failure-safe paths):
//!
//! - The actual VM swap happens inside [`Runtime::reload`], under the same
//!   `Mutex` that serializes dispatch/lifecycle/tick, so a reload never
//!   interleaves with an in-flight handler. This service only decides *when* to
//!   call it.
//! - `reload` is failure-safe: it builds the fresh VM off-lock and only swaps on
//!   success, so a broken edit is logged and rejected while the previous script
//!   keeps serving. The watcher therefore treats every fire as best-effort and
//!   never stops on a rejected reload.
//! - The blocking reload (file read + VM build + lock) runs on `spawn_blocking`,
//!   off the core async workers, and a panic there surfaces as a `JoinError` that
//!   is logged so the watcher loop keeps running.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::time::MissedTickBehavior;

use crate::error::AppResult;
use crate::lifecycle::{AsyncService, CancellationToken};
use crate::runtime::Runtime;

/// A cheap change fingerprint for the watched script file.
///
/// Combining modification time with length catches edits even when a coarse
/// filesystem mtime resolution would otherwise collapse two quick saves, and
/// treats a missing/unreadable file as a distinct signature (so a delete then
/// re-create is seen as a change).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileSig {
    modified: Option<SystemTime>,
    len: u64,
    exists: bool,
}

impl FileSig {
    /// Stat `path` into a signature; any I/O error becomes the "absent" signature.
    fn probe(path: &std::path::Path) -> Self {
        match std::fs::metadata(path) {
            Ok(meta) => Self {
                modified: meta.modified().ok(),
                len: meta.len(),
                exists: true,
            },
            Err(_) => Self {
                modified: None,
                len: 0,
                exists: false,
            },
        }
    }
}

/// Combine the runtime's initialization-time dependencies with the selected
/// entrypoint. The latter remains an unconditional fallback for adapters that
/// have not opted into dependency reporting yet.
fn watched_paths(runtime: &dyn Runtime, entrypoint: &Path) -> Vec<PathBuf> {
    let mut paths = runtime.reload_watch_paths();
    if !paths.iter().any(|path| path == entrypoint) {
        paths.push(entrypoint.to_path_buf());
    }
    paths.sort();
    paths.dedup();
    paths
}

fn probe_paths(paths: Vec<PathBuf>) -> BTreeMap<PathBuf, FileSig> {
    paths
        .into_iter()
        .map(|path| {
            let signature = FileSig::probe(&path);
            (path, signature)
        })
        .collect()
}

/// A periodic task that reloads the game script when its file changes on disk.
pub struct LuaReloadService {
    runtime: Arc<dyn Runtime>,
    /// Path to the watched script entrypoint.
    path: PathBuf,
    /// Poll interval between change checks.
    interval: Duration,
}

impl LuaReloadService {
    /// Build a watcher that reloads `runtime` from `path` every `interval`.
    #[must_use]
    pub fn new(runtime: Arc<dyn Runtime>, path: PathBuf, interval: Duration) -> Self {
        Self {
            runtime,
            path,
            interval,
        }
    }
}

impl AsyncService for LuaReloadService {
    fn name(&self) -> &str {
        "lua-reload"
    }

    async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
        let mut interval = tokio::time::interval(self.interval);
        // Missed polls need not pile up: one catch-up check is enough.
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Baseline: the signature at startup. The script was just loaded, so we
        // only reload on a *subsequent* change and never re-run the initial load.
        let mut last = probe_paths(watched_paths(self.runtime.as_ref(), &self.path));
        tracing::info!(
            script = %self.path.display(),
            poll_ms = self.interval.as_millis() as u64,
            "watching game script for hot-reload"
        );
        loop {
            tokio::select! {
                // Cooperative shutdown: stop promptly when the supervisor cancels.
                () = cancel.cancelled() => break,
                _ = interval.tick() => {
                    let current = probe_paths(watched_paths(self.runtime.as_ref(), &self.path));
                    // New dependencies discovered by a successful reload begin
                    // with a baseline on the next poll. Existing watched files
                    // changing (including disappearance) request a replacement
                    // VM/catalog atomically.
                    let changed = current.iter().any(|(path, signature)| {
                        last.get(path).is_some_and(|previous| previous != signature)
                    });
                    if !changed {
                        last = current;
                        continue;
                    }
                    last = current;
                    let runtime = Arc::clone(&self.runtime);
                    // Run the blocking reload (file read + VM build + lock swap)
                    // off the async workers. `reload` is failure-safe internally;
                    // a panic here becomes a JoinError, logged, loop continues.
                    match tokio::task::spawn_blocking(move || runtime.reload()).await {
                        Ok(_outcome) => {}
                        Err(join_err) => {
                            tracing::error!(
                                error = %join_err,
                                "lua reload task panicked; isolated, watcher continues"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::Supervisor;
    use crate::runtime::{LuaRuntime, OutboundCommand, ReloadOutcome, Runtime};

    /// A throwaway temp directory (no `tempfile` dependency), removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let dir =
                std::env::temp_dir().join(format!("citadel-reload-svc-{}-{n}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            Self(dir)
        }

        fn main_lua(&self) -> PathBuf {
            self.0.join("main.lua")
        }

        fn write(&self, src: &str) {
            std::fs::write(self.main_lua(), src).expect("write main.lua");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn file_sig_changes_with_content_and_absence() {
        let dir = TempDir::new();
        dir.write("a = 1");
        let path = dir.main_lua();
        let a = FileSig::probe(&path);
        assert!(a.exists);
        // A longer file changes the length even if mtime resolution is coarse.
        dir.write("a = 1 -- longer now");
        let b = FileSig::probe(&path);
        assert_ne!(a, b, "content change alters the signature");
        std::fs::remove_file(&path).expect("remove");
        let c = FileSig::probe(&path);
        assert!(!c.exists);
        assert_ne!(b, c, "deletion alters the signature");
    }

    #[tokio::test]
    async fn watcher_reloads_on_edit_and_stops_on_shutdown() {
        let dir = TempDir::new();
        dir.write(
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, "v1", false)
            end)
        "#,
        );
        let runtime: Arc<dyn Runtime> = Arc::new(
            LuaRuntime::load(&dir.0, 100)
                .expect("loads")
                .expect("present"),
        );
        // Sanity: the initial handler serves v1.
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: b"v1".to_vec(),
                unreliable: false,
            }]
        );

        // Fast poll so the test is quick.
        let mut supervisor = Supervisor::new();
        supervisor.spawn(LuaReloadService::new(
            Arc::clone(&runtime),
            dir.main_lua(),
            Duration::from_millis(20),
        ));

        // Let the watcher run and establish its baseline signature (v1) before we
        // edit, so the change is observed as a *subsequent* modification.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Edit the script (a distinctly longer body so the change is visible even
        // on a coarse mtime resolution): the watcher should swap in v2.
        dir.write(
            r#"
            citadel.on_message(1, function(ctx, body)
                -- edited handler, longer than the original to shift the file size
                citadel.broadcast(2, "v2", false)
            end)
        "#,
        );

        // Poll the runtime until the new handler is live (bounded wait).
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let out = runtime.dispatch(1, None, 1, b"");
            if let [OutboundCommand::Broadcast { body, .. }] = out.as_slice()
                && body == b"v2"
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "watcher did not reload the edited script in time"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Cancellation stops the watcher cleanly.
        tokio::time::timeout(Duration::from_secs(2), supervisor.shutdown())
            .await
            .expect("shutdown completes")
            .expect("clean shutdown");
    }

    #[tokio::test]
    async fn watcher_keeps_serving_after_a_broken_edit() {
        let dir = TempDir::new();
        dir.write(
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, "good", false)
            end)
        "#,
        );
        let runtime: Arc<dyn Runtime> = Arc::new(
            LuaRuntime::load(&dir.0, 100)
                .expect("loads")
                .expect("present"),
        );

        let mut supervisor = Supervisor::new();
        supervisor.spawn(LuaReloadService::new(
            Arc::clone(&runtime),
            dir.main_lua(),
            Duration::from_millis(20),
        ));

        // Let the watcher establish its baseline (the good script) before editing.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // A broken edit: the watcher fires a reload that is rejected internally.
        dir.write("this is not lua ==");
        // Give the watcher time to observe and reject the change.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // The previous good handler still serves.
        assert_eq!(
            runtime.dispatch(1, None, 1, b""),
            vec![OutboundCommand::Broadcast {
                kind: 2,
                body: b"good".to_vec(),
                unreliable: false,
            }],
            "broken edit rejected; previous script keeps serving"
        );

        // A subsequent good edit still reloads (watcher not wedged by the reject).
        dir.write(
            r#"
            citadel.on_message(1, function(ctx, body)
                citadel.broadcast(2, "fixed", false)
            end)
        "#,
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let out = runtime.dispatch(1, None, 1, b"");
            if let [OutboundCommand::Broadcast { body, .. }] = out.as_slice()
                && body == b"fixed"
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "watcher did not recover after a rejected reload"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        tokio::time::timeout(Duration::from_secs(2), supervisor.shutdown())
            .await
            .expect("shutdown completes")
            .expect("clean shutdown");

        // Sanity that the reject path was actually exercised end to end.
        assert_eq!(runtime.reload(), ReloadOutcome::Reloaded);
    }
}
