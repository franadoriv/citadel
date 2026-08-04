//! Native TLS termination for the HTTP surface.
//!
//! The HTTP listener carries the operator console password, the console bearer
//! tokens issued from it, and every player session token, so it must not be
//! served in cleartext on a reachable address. A deployment can put a
//! TLS-terminating reverse proxy in front and declare that with
//! `http.behind_tls_proxy`; this module is the other option, so a node is
//! deployable over `https://` without any additional infrastructure.
//!
//! axum 0.7's [`axum::serve`] takes a concrete `TcpListener` and cannot wrap a
//! TLS stream, so the https path drives hyper directly. That costs the graceful
//! connection drain `axum::serve` provides; the accept loop compensates by
//! bounding how long it waits for in-flight connections after shutdown.

use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ConnectInfo;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use rustls::pki_types::PrivateKeyDer;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower::Service;

use crate::app::App;
use crate::error::{AppError, AppResult, ErrorCategory};

/// How long shutdown waits for in-flight TLS connections to finish.
const DRAIN_GRACE: Duration = Duration::from_secs(10);

fn tls_error(message: &str, error: impl std::fmt::Display) -> AppError {
    AppError::new(ErrorCategory::Config, message).with_detail(error.to_string())
}

/// Build the rustls server configuration from operator-supplied PEM material.
///
/// The key may be PKCS#8, RSA/PKCS#1 or SEC1. File contents are never included
/// in errors, since the key file is a secret. ALPN advertises HTTP/2 and
/// HTTP/1.1 in that order, matching what the connection builder can serve.
///
/// # Errors
/// Returns a [`Config`](ErrorCategory::Config) error when either file cannot be
/// read, contains no usable material, or the pair is not a valid identity.
pub fn server_config(
    certificate_file: &Path,
    private_key_file: &Path,
) -> AppResult<Arc<rustls::ServerConfig>> {
    let cert_file = File::open(certificate_file)
        .map_err(|e| tls_error("failed to open http.tls.certificate_file", e))?;
    let cert_chain = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| tls_error("failed to parse http.tls.certificate_file PEM", e))?;
    if cert_chain.is_empty() {
        return Err(AppError::new(
            ErrorCategory::Config,
            "http.tls.certificate_file contains no certificates",
        ));
    }

    let key_file = File::open(private_key_file)
        .map_err(|e| tls_error("failed to open http.tls.private_key_file", e))?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|e| tls_error("failed to parse http.tls.private_key_file PEM", e))?
        .ok_or_else(|| {
            AppError::new(
                ErrorCategory::Config,
                "http.tls.private_key_file contains no private key",
            )
        })?;
    let key = PrivateKeyDer::try_from(key.secret_der().to_vec())
        .map_err(|e| tls_error("invalid http.tls private key", e))?;

    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .map_err(|e| {
            tls_error(
                "http.tls certificate and private key do not form an identity",
                e,
            )
        })?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// Serve HTTPS on `listener` until `shutdown` resolves.
///
/// A failed handshake affects only its own connection: it is logged at debug
/// and the loop keeps accepting, so a port scanner or a client with the wrong
/// trust store cannot take the listener down.
///
/// # Errors
/// Returns a [`Transport`](ErrorCategory::Transport) error only if the listener
/// itself becomes unusable.
pub async fn serve<F>(
    listener: TcpListener,
    app: App,
    config: Arc<rustls::ServerConfig>,
    shutdown: F,
) -> AppResult<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let acceptor = TlsAcceptor::from(config);
    let router = super::router(app);
    let mut connections = tokio::task::JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);

    loop {
        let (stream, peer) = tokio::select! {
            // Bias the shutdown branch so a busy listener still stops promptly.
            biased;
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::warn!(%error, "HTTPS listener failed to accept a connection");
                    continue;
                }
            },
            // Reap finished connections so the set does not grow without bound.
            Some(_) = connections.join_next(), if !connections.is_empty() => continue,
        };

        let acceptor = acceptor.clone();
        let router = router.clone();
        connections.spawn(async move {
            let stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => {
                    // Wrong trust store, plain HTTP to an https port, a scanner.
                    tracing::debug!(%peer, %error, "TLS handshake failed");
                    return;
                }
            };
            // `ConnectInfo` is inserted here because driving hyper directly
            // bypasses `into_make_service_with_connect_info`. The auth and
            // console rate limiters key on this peer address.
            let service = TowerToHyperService::new(tower::service_fn(
                move |mut request: hyper::Request<Incoming>| {
                    request.extensions_mut().insert(ConnectInfo(peer));
                    let mut router = router.clone();
                    router.call(request)
                },
            ));
            if let Err(error) = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(%peer, %error, "HTTPS connection ended with an error");
            }
        });
    }

    // Stop accepting, then give in-flight requests a bounded window to finish.
    if !connections.is_empty() {
        tracing::info!(
            in_flight = connections.len(),
            "HTTPS listener draining in-flight connections"
        );
        if tokio::time::timeout(DRAIN_GRACE, async {
            while connections.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            tracing::warn!(
                in_flight = connections.len(),
                "HTTPS drain grace period expired; aborting remaining connections"
            );
            connections.shutdown().await;
        }
    }
    Ok(())
}

/// The socket address a bound listener is actually serving on.
///
/// # Errors
/// Returns a [`Transport`](ErrorCategory::Transport) error if the address
/// cannot be read back from the socket.
pub fn local_addr(listener: &TcpListener) -> AppResult<SocketAddr> {
    listener.local_addr().map_err(|e| {
        AppError::new(
            ErrorCategory::Transport,
            "failed to read the HTTPS listener address",
        )
        .with_detail(e.to_string())
    })
}
