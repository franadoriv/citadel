//! Security response headers for the whole HTTP surface.
//!
//! These are defence in depth rather than the primary control: the console is
//! bearer-authenticated and the API is session-authenticated. They matter most
//! for the operator console, which is a single self-contained page with
//! substantial inline JavaScript, served from the same origin that performs
//! privileged mutations.
//!
//! The content-security policy is built from the **hashes of the page's own
//! inline blocks** rather than `'unsafe-inline'`. That is possible because the
//! console has no external resources, no inline event handlers, no
//! `javascript:` URLs and no inline `style` attributes, which
//! [`console_csp_matches_the_served_document`](tests) keeps true: if the SPA
//! grows an inline handler or its script changes, the test fails rather than
//! the console silently breaking in a browser.

use std::sync::OnceLock;

use axum::extract::Request;
use axum::http::HeaderValue;
use axum::http::header::{
    CONTENT_SECURITY_POLICY, REFERRER_POLICY, STRICT_TRANSPORT_SECURITY, X_CONTENT_TYPE_OPTIONS,
    X_FRAME_OPTIONS,
};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use sha2::{Digest, Sha256};

use crate::app::App;

/// One year, the usual floor for a host to be eligible for HSTS preloading.
const HSTS_VALUE: &str = "max-age=31536000; includeSubDomains";

/// Extract the body of the first `<tag>...</tag>` block, if present.
fn inline_block(html: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = html.find(&open)? + open.len();
    let end = html[start..].find(&close)? + start;
    Some(html[start..end].to_owned())
}

/// CSP source expression for one inline block: `'sha256-<base64>'`.
fn hash_source(body: &str) -> String {
    let digest = Sha256::digest(body.as_bytes());
    format!("'sha256-{}'", BASE64.encode(digest))
}

/// The policy served with every response.
///
/// `default-src 'none'` is the baseline: the console loads nothing from the
/// network, so every fetch other than same-origin XHR is denied outright. That
/// closes the exfiltration paths an injected string would otherwise have, and
/// `frame-ancestors 'none'` closes clickjacking against console mutations.
fn content_security_policy() -> &'static str {
    static POLICY: OnceLock<String> = OnceLock::new();
    POLICY.get_or_init(|| {
        let html = super::console::CONSOLE_HTML;
        let mut script = String::from("script-src");
        if let Some(body) = inline_block(html, "script") {
            script.push(' ');
            script.push_str(&hash_source(&body));
        }
        let mut style = String::from("style-src");
        if let Some(body) = inline_block(html, "style") {
            style.push(' ');
            style.push_str(&hash_source(&body));
        }
        [
            "default-src 'none'",
            script.as_str(),
            style.as_str(),
            // The dashboard renders the Citadel logo as a data URI.
            "img-src 'self' data:",
            // The console talks only to the node that served it.
            "connect-src 'self'",
            "base-uri 'none'",
            "form-action 'none'",
            "frame-ancestors 'none'",
        ]
        .join("; ")
    })
}

/// Apply the security headers to every response.
///
/// Existing headers are never overwritten, so a handler that sets a deliberate
/// policy of its own keeps it.
pub async fn apply(
    axum::extract::State(app): axum::extract::State<App>,
    request: Request,
    next: Next,
) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();

    headers
        .entry(CONTENT_SECURITY_POLICY)
        .or_insert_with(|| HeaderValue::from_static(content_security_policy()));
    headers
        .entry(X_CONTENT_TYPE_OPTIONS)
        .or_insert_with(|| HeaderValue::from_static("nosniff"));
    headers
        .entry(X_FRAME_OPTIONS)
        .or_insert_with(|| HeaderValue::from_static("DENY"));
    headers
        .entry(REFERRER_POLICY)
        .or_insert_with(|| HeaderValue::from_static("no-referrer"));

    // HSTS is only meaningful, and only safe, once the surface is actually
    // reachable over TLS. Sending it from a plaintext loopback demo would
    // pin a browser to https for a host that does not serve it.
    let http = &app.config().http;
    if http.tls.is_configured() || http.behind_tls_proxy {
        headers
            .entry(STRICT_TRANSPORT_SECURITY)
            .or_insert_with(|| HeaderValue::from_static(HSTS_VALUE));
    }

    response
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The hash policy is only correct while the page keeps the shape it was
    /// derived from. If the console grows a second script block, an inline
    /// event handler, or a `javascript:` URL, a hash policy stops covering it
    /// and the console breaks in a browser rather than in CI. Fail here first.
    #[test]
    fn console_csp_matches_the_served_document() {
        let html = crate::http::console::CONSOLE_HTML;

        assert_eq!(
            html.matches("<script").count(),
            1,
            "the CSP hashes exactly one inline script block"
        );
        assert_eq!(
            html.matches("<style").count(),
            1,
            "the CSP hashes exactly one inline style block"
        );
        assert!(
            !html.contains("javascript:"),
            "a javascript: URL is not covered by a hash policy"
        );
        for handler in [
            "onclick=",
            "onchange=",
            "oninput=",
            "onsubmit=",
            "onload=",
            "onerror=",
        ] {
            assert!(
                !html.contains(handler),
                "inline handler {handler} is not covered by a hash policy"
            );
        }
        assert!(
            !html.contains("//cdn.") && !html.contains("https://"),
            "the console must stay self-contained for default-src 'none'"
        );

        // The advertised hash must be the hash of what is actually served.
        let policy = content_security_policy();
        let script = inline_block(html, "script").expect("inline script block");
        assert!(
            policy.contains(&hash_source(&script)),
            "the script-src hash must match the served document"
        );
        let style = inline_block(html, "style").expect("inline style block");
        assert!(policy.contains(&hash_source(&style)));
        assert!(policy.starts_with("default-src 'none'"));
        assert!(policy.contains("frame-ancestors 'none'"));
    }

    #[test]
    fn inline_block_extracts_only_the_first_block() {
        let html = "<style>a{}</style><body><script>let x = 1;</script></body>";
        assert_eq!(inline_block(html, "style").unwrap(), "a{}");
        assert_eq!(inline_block(html, "script").unwrap(), "let x = 1;");
        assert_eq!(inline_block(html, "template"), None);
    }
}
