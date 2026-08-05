//! Docs gate: the cooperative-yield warning must be present in the runtime
//! reference page of every engine.
//!
//! Scripts execute on scheduler threads the platform cannot safely reclaim
//! from non-cooperative code, so the server-SDK docs for each engine must
//! carry the honest warning: handlers must yield by returning; a
//! non-cooperative match is closed, and a thread that cannot be reclaimed
//! costs the worker process. This gate pins the shared marker sentence so an
//! edit cannot silently drop the warning from one engine's page.

use std::path::PathBuf;

/// The load-bearing sentence every engine page must carry verbatim.
const WARNING_MARKER: &str = "cannot safely terminate non-cooperative code in-thread";

/// The aside title shared by the three pages.
const WARNING_TITLE: &str = "Cooperative yielding required";

fn runtime_docs_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("website")
        .join("src")
        .join("content")
        .join("docs")
        .join("reference")
        .join("server-sdk")
}

#[test]
fn cooperative_yield_warning_is_present_for_all_three_engines() {
    for page in ["lua-runtime.md", "js-runtime.mdx", "python-runtime.mdx"] {
        let path = runtime_docs_dir().join(page);
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| unreachable!("read {}: {error}", path.display()));
        // Markdown hard-wraps prose; compare against whitespace-normalized
        // text so a re-wrap cannot break the gate.
        let content = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            content.contains(WARNING_TITLE),
            "{page} must carry the '{WARNING_TITLE}' aside"
        );
        assert!(
            content.contains(WARNING_MARKER),
            "{page} must state that the platform {WARNING_MARKER}"
        );
        assert!(
            content.contains("worker process is restarted"),
            "{page} must state the worker-restart consequence of a non-reclaimable thread"
        );
    }
}
