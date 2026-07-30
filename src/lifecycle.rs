//! Async service lifecycle for long-running components.
//!
//! The synchronous [`ServiceLifecycle`](crate::services::ServiceLifecycle) trait
//! describes a service's name and health for aggregation. Transport listeners
//! and other long-running tasks additionally need to be *started* and
//! *gracefully stopped*. This module adds that async dimension without
//! overloading the sync trait:
//!
//! - [`CancellationToken`] is a cheap, clonable shutdown signal built on
//!   `tokio::sync::watch`, so we do not pull in an extra dependency for one
//!   small concept.
//! - [`AsyncService`] is the contract a long-running component implements: a
//!   `run` future that serves until the token is cancelled, then returns.
//! - [`Supervisor`] starts a set of services on the tokio runtime, exposes a
//!   single `shutdown` that cancels them all, and joins their tasks.
//!
//! Services own their accept/serve loops; the supervisor owns ordered startup,
//! the shared cancellation signal, and join-on-shutdown. This keeps cancellation
//! cooperative and explicit.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::{AppError, AppResult, ErrorCategory};
use crate::error_reporting;

/// A clonable cooperative-cancellation signal.
///
/// Clone it freely; all clones observe the same cancellation. Call
/// [`CancellationToken::cancel`] once to signal shutdown, and
/// [`CancellationToken::cancelled`] in a service loop (typically inside
/// `tokio::select!`) to await the signal.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    rx: watch::Receiver<bool>,
    tx: Arc<watch::Sender<bool>>,
}

impl CancellationToken {
    /// Create a fresh, un-cancelled token.
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self {
            rx,
            tx: Arc::new(tx),
        }
    }

    /// Signal cancellation to all clones. Idempotent.
    pub fn cancel(&self) {
        // Ignore the error: a send error only means there are no receivers,
        // which is fine for a shutdown signal.
        let _ = self.tx.send(true);
    }

    /// Whether cancellation has already been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    /// Resolve once cancellation has been requested.
    ///
    /// Returns immediately if already cancelled. Safe to call repeatedly and
    /// from multiple tasks.
    pub async fn cancelled(&self) {
        // Take a local clone so we can wait on changes without &mut self.
        let mut rx = self.rx.clone();
        if *rx.borrow() {
            return;
        }
        // Wait until the value becomes true. If the sender is dropped, treat it
        // as cancelled so services never hang.
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                return;
            }
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// A long-running async component managed by the [`Supervisor`].
///
/// Implementors run until the provided [`CancellationToken`] is cancelled, then
/// perform any cleanup and return. Returning `Ok()` indicates a clean stop;
/// returning an error indicates the service failed.
pub trait AsyncService: Send + 'static {
    /// Stable, human-readable name for logs and diagnostics.
    fn name(&self) -> &str;

    /// Serve until `cancel` is triggered, then return.
    fn run(
        self: Box<Self>,
        cancel: CancellationToken,
    ) -> impl Future<Output = AppResult<()>> + Send;
}

/// A spawned service task with its name, awaiting completion on shutdown.
struct Supervised {
    name: String,
    handle: JoinHandle<AppResult<()>>,
}

/// Starts and gracefully stops a set of [`AsyncService`] values.
///
/// The supervisor holds the shared [`CancellationToken`]. `shutdown` cancels it
/// and joins every spawned task, surfacing the first error encountered.
pub struct Supervisor {
    cancel: CancellationToken,
    services: Vec<Supervised>,
}

impl Supervisor {
    /// Create a supervisor with a fresh cancellation token.
    #[must_use]
    pub fn new() -> Self {
        Self::with_token(CancellationToken::new())
    }

    /// Create a supervisor that shares an existing cancellation token.
    ///
    /// Use this to tie transport shutdown to an external trigger (e.g. the same
    /// token used by the HTTP graceful-shutdown path).
    #[must_use]
    pub fn with_token(cancel: CancellationToken) -> Self {
        Self {
            cancel,
            services: Vec::new(),
        }
    }

    /// The shared cancellation token; clone it to wire external shutdown
    /// triggers (e.g. Ctrl-C).
    #[must_use]
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Spawn `service` on the current tokio runtime.
    ///
    /// The service receives a clone of the supervisor's cancellation token.
    pub fn spawn<S: AsyncService>(&mut self, service: S) {
        let name = service.name().to_string();
        let cancel = self.cancel.clone();
        let boxed = Box::new(service);
        let handle = tokio::spawn(async move { boxed.run(cancel).await });
        self.services.push(Supervised { name, handle });
    }

    /// Number of supervised services.
    #[must_use]
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// Whether no services are supervised.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }

    /// Cancel all services and join their tasks.
    ///
    /// Returns the first service error encountered (services are still all
    /// joined). A panicked task is reported as an
    /// [`Internal`](ErrorCategory::Internal) error.
    pub async fn shutdown(self) -> AppResult<()> {
        self.cancel.cancel();
        let mut first_err: Option<AppError> = None;
        for Supervised { name, handle } in self.services {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!(service = %name, error = %e, "service stopped with error");
                    error_reporting::report_app_error("lifecycle.supervisor", &e);
                    first_err.get_or_insert(e);
                }
                Err(join_err) => {
                    let e = AppError::new(
                        ErrorCategory::Internal,
                        format!("service '{name}' task failed to join"),
                    )
                    .with_detail(join_err.to_string());
                    tracing::error!(service = %name, "service task panicked or was cancelled");
                    error_reporting::report_app_error("lifecycle.supervisor", &e);
                    first_err.get_or_insert(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl Default for Supervisor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[tokio::test]
    async fn token_resolves_when_cancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        let waiter = token.clone();
        let task = tokio::spawn(async move {
            waiter.cancelled().await;
            true
        });
        token.cancel();
        assert!(token.is_cancelled());
        assert!(task.await.expect("join"));
    }

    #[tokio::test]
    async fn cancelled_returns_immediately_if_already_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        // Should not hang.
        token.cancelled().await;
    }

    struct CountingService {
        name: String,
        started: Arc<AtomicBool>,
        stopped: Arc<AtomicBool>,
    }

    impl AsyncService for CountingService {
        fn name(&self) -> &str {
            &self.name
        }

        async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
            self.started.store(true, Ordering::SeqCst);
            cancel.cancelled().await;
            self.stopped.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn supervisor_starts_and_gracefully_stops_a_service() {
        let started = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let mut sup = Supervisor::new();
        sup.spawn(CountingService {
            name: "noop".to_string(),
            started: started.clone(),
            stopped: stopped.clone(),
        });
        assert_eq!(sup.len(), 1);
        assert!(!sup.is_empty());

        // Give the task a moment to start.
        tokio::task::yield_now().await;

        sup.shutdown().await.expect("clean shutdown");
        assert!(started.load(Ordering::SeqCst), "service ran");
        assert!(stopped.load(Ordering::SeqCst), "service stopped on cancel");
    }

    struct FailingService;

    impl AsyncService for FailingService {
        fn name(&self) -> &str {
            "failing"
        }
        async fn run(self: Box<Self>, _cancel: CancellationToken) -> AppResult<()> {
            Err(AppError::internal("boom"))
        }
    }

    #[tokio::test]
    async fn supervisor_surfaces_service_error() {
        let mut sup = Supervisor::new();
        sup.spawn(FailingService);
        let err = sup.shutdown().await.expect_err("service error surfaces");
        assert_eq!(err.category(), ErrorCategory::Internal);
    }

    struct CounterService {
        counter: Arc<AtomicUsize>,
    }

    impl AsyncService for CounterService {
        fn name(&self) -> &str {
            "counter"
        }
        async fn run(self: Box<Self>, cancel: CancellationToken) -> AppResult<()> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            cancel.cancelled().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn supervisor_starts_multiple_services() {
        let counter = Arc::new(AtomicUsize::new(0));
        let mut sup = Supervisor::new();
        sup.spawn(CounterService {
            counter: counter.clone(),
        });
        sup.spawn(CounterService {
            counter: counter.clone(),
        });
        tokio::task::yield_now().await;
        sup.shutdown().await.expect("clean shutdown");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn empty_supervisor_shuts_down_cleanly() {
        let sup = Supervisor::new();
        assert!(sup.is_empty());
        sup.shutdown().await.expect("empty shutdown");
    }
}
