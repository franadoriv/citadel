//! Contract tests for the immutable GameScript revision repository.
//!
//! These assert the draft → immutable revision → diagnostics → activation
//! generation semantics that *any* [`GameScriptRepository`] implementation must
//! honor. Each scenario is written against `&dyn GameScriptRepository` and is
//! run against every backend:
//!
//! - always against [`InMemoryGameScriptRepository`] (the reference impl),
//! - always against a real embedded SQLite backend (un-gated; no server),
//! - against a real Postgres backend when `DATABASE_URL` (or
//!   `CITADEL_TEST_DATABASE_URL`) is set,
//! - against a real CockroachDB instance when `CITADEL_TEST_COCKROACH_URL` is
//!   set (URL scheme rewritten to `cockroach://` so the backend selects the
//!   CockroachDB migrations), and
//! - against a real MongoDB replica set when `CITADEL_TEST_MONGODB_URL` is set.
//!
//! The gated runs skip cleanly when their variable is unset, so
//! `bash scripts/check.sh` stays green with no external database.

use std::collections::BTreeMap;
use std::sync::Arc;

use citadel::config::RuntimeLanguage;
use citadel::error::ErrorCategory;
use citadel::repository::{
    CreateGameScriptDraftRequest, GameScriptAuditContext, GameScriptDiagnosticSeverity,
    GameScriptOutboxKind, GameScriptRepository, InMemoryGameScriptRepository,
    UpdateGameScriptDraftRequest, gamescript_revision_content_hash,
};
use citadel::time::TimestampMillis;

fn ts(v: u64) -> TimestampMillis {
    TimestampMillis::from_unix_millis(v)
}

fn draft(id: &str, content: &str) -> CreateGameScriptDraftRequest {
    CreateGameScriptDraftRequest {
        draft_id: id.to_owned(),
        language: RuntimeLanguage::Lua,
        entrypoint: "main.lua".to_owned(),
        content: content.to_owned(),
        created_by: "op-author".to_owned(),
    }
}

fn no_context() -> GameScriptAuditContext {
    BTreeMap::new()
}

async fn submit(
    repo: &dyn GameScriptRepository,
    draft_id: &str,
    content: &str,
    now: TimestampMillis,
) -> citadel::repository::GameScriptSubmission {
    repo.create_draft(draft(draft_id, content), now)
        .await
        .expect("create draft");
    repo.submit_draft(draft_id, "op-submitter", &no_context(), now)
        .await
        .expect("submit draft")
}

// --- Scenarios (backend-agnostic) -------------------------------------------

async fn scenario_draft_lifecycle_round_trips(repo: &dyn GameScriptRepository) {
    let created = repo
        .create_draft(draft("d-1", "return 1"), ts(10))
        .await
        .expect("create");
    assert_eq!(created.draft_id, "d-1");
    assert_eq!(created.language, RuntimeLanguage::Lua);
    assert_eq!(created.entrypoint, "main.lua");
    assert_eq!(created.content, "return 1");
    assert_eq!(created.created_by, "op-author");
    assert_eq!(created.created_at, ts(10));
    assert_eq!(created.updated_at, ts(10));

    // Durability: a fresh read returns the same draft.
    let fetched = repo
        .get_draft("d-1")
        .await
        .expect("get")
        .expect("draft present");
    assert_eq!(fetched, created);

    // Drafts stay mutable until submitted.
    let updated = repo
        .update_draft(
            "d-1",
            UpdateGameScriptDraftRequest {
                language: RuntimeLanguage::Lua,
                entrypoint: "main.lua".to_owned(),
                content: "return 2".to_owned(),
            },
            ts(20),
        )
        .await
        .expect("update");
    assert_eq!(updated.content, "return 2");
    assert_eq!(updated.created_at, ts(10), "creation instant is preserved");
    assert_eq!(updated.updated_at, ts(20));

    let listed = repo.list_drafts(10).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0], updated);

    assert!(repo.delete_draft("d-1").await.expect("delete"));
    assert!(repo.get_draft("d-1").await.expect("get").is_none());
    assert!(
        !repo.delete_draft("d-1").await.expect("idempotent delete"),
        "second delete removes nothing"
    );

    // Absent drafts: read is None, update is NotFound.
    assert!(repo.get_draft("ghost").await.expect("get").is_none());
    assert_eq!(
        repo.update_draft(
            "ghost",
            UpdateGameScriptDraftRequest {
                language: RuntimeLanguage::Lua,
                entrypoint: "main.lua".to_owned(),
                content: "return 3".to_owned(),
            },
            ts(30),
        )
        .await
        .expect_err("update missing draft")
        .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_draft_creation_is_validated_and_unique(repo: &dyn GameScriptRepository) {
    repo.create_draft(draft("d-1", "return 1"), ts(1))
        .await
        .expect("create");
    assert_eq!(
        repo.create_draft(draft("d-1", "return 9"), ts(2))
            .await
            .expect_err("duplicate draft id")
            .category(),
        ErrorCategory::Conflict
    );

    for broken in [
        CreateGameScriptDraftRequest {
            draft_id: String::new(),
            ..draft("d-2", "return 1")
        },
        CreateGameScriptDraftRequest {
            entrypoint: String::new(),
            ..draft("d-2", "return 1")
        },
        CreateGameScriptDraftRequest {
            content: String::new(),
            ..draft("d-2", "return 1")
        },
        CreateGameScriptDraftRequest {
            created_by: String::new(),
            ..draft("d-2", "return 1")
        },
    ] {
        assert_eq!(
            repo.create_draft(broken, ts(3))
                .await
                .expect_err("invalid draft request")
                .category(),
            ErrorCategory::Validation
        );
    }

    // The provisional source-size cap bounds every backend identically.
    let oversized = "x".repeat(
        citadel::repository::GameScriptLimits::default()
            .max_source_bytes
            .saturating_add(1),
    );
    assert_eq!(
        repo.create_draft(draft("d-3", &oversized), ts(4))
            .await
            .expect_err("oversized draft content")
            .category(),
        ErrorCategory::Validation
    );
}

async fn scenario_submit_creates_immutable_hash_addressed_revision(
    repo: &dyn GameScriptRepository,
) {
    repo.create_draft(draft("d-1", "return 42"), ts(10))
        .await
        .expect("create");
    let mut context = BTreeMap::new();
    context.insert("reason".to_owned(), "initial rollout".to_owned());
    let submission = repo
        .submit_draft("d-1", "op-submitter", &context, ts(20))
        .await
        .expect("submit");
    assert!(!submission.deduplicated);

    let revision = &submission.revision;
    let expected_id =
        gamescript_revision_content_hash(RuntimeLanguage::Lua, "main.lua", "return 42");
    assert_eq!(revision.revision_id, expected_id, "id is the content hash");
    assert_eq!(revision.content_hash(), expected_id.as_str());
    assert_eq!(revision.language, RuntimeLanguage::Lua);
    assert_eq!(revision.entrypoint, "main.lua");
    assert_eq!(revision.content, "return 42");
    assert_eq!(revision.size_bytes, "return 42".len() as u64);
    assert_eq!(revision.created_by, "op-submitter");
    assert_eq!(revision.created_at, ts(20));

    // The draft is consumed by submission.
    assert!(repo.get_draft("d-1").await.expect("get").is_none());

    // Durability: a fresh read returns the identical revision.
    let fetched = repo
        .get_revision(&revision.revision_id)
        .await
        .expect("get revision")
        .expect("revision present");
    assert_eq!(&fetched, revision);
    assert_eq!(repo.list_revisions(10).await.expect("list").len(), 1);

    // The audit record and outbox entry were committed with the revision.
    let audit = repo.audit_log(10).await.expect("audit");
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, "gamescript.draft.submit");
    assert_eq!(audit[0].actor, "op-submitter");
    assert_eq!(audit[0].target, revision.revision_id);
    assert_eq!(audit[0].created_at, ts(20));
    assert_eq!(
        audit[0].details.get("draft_id").map(String::as_str),
        Some("d-1")
    );
    assert_eq!(
        audit[0].details.get("reason").map(String::as_str),
        Some("initial rollout")
    );

    let outbox = repo.pending_outbox(10).await.expect("outbox");
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].kind, GameScriptOutboxKind::RevisionCreated);
    assert_eq!(outbox[0].revision_id, revision.revision_id);
    assert_eq!(outbox[0].scope, None);
    assert_eq!(outbox[0].generation, None);
    assert_eq!(outbox[0].created_at, ts(20));
}

async fn scenario_identical_content_dedupes_to_one_revision(repo: &dyn GameScriptRepository) {
    let first = submit(repo, "d-1", "return 7", ts(10)).await;
    assert!(!first.deduplicated);

    let second = submit(repo, "d-2", "return 7", ts(20)).await;
    assert!(second.deduplicated, "identical content is deduplicated");
    assert_eq!(second.revision.revision_id, first.revision.revision_id);
    assert_eq!(
        second.revision.created_at,
        ts(10),
        "the original revision is returned unchanged"
    );
    assert_eq!(repo.list_revisions(10).await.expect("list").len(), 1);

    // Both operator submissions are audited; only the creating one rolls out.
    assert_eq!(repo.audit_log(10).await.expect("audit").len(), 2);
    let outbox = repo.pending_outbox(10).await.expect("outbox");
    assert_eq!(
        outbox.len(),
        1,
        "a deduplicated submit creates no second outbox entry"
    );

    // Different content is a different immutable revision.
    let third = submit(repo, "d-3", "return 8", ts(30)).await;
    assert!(!third.deduplicated);
    assert_ne!(third.revision.revision_id, first.revision.revision_id);
    assert_eq!(repo.list_revisions(10).await.expect("list").len(), 2);
}

async fn scenario_revision_is_immutable_under_related_mutations(repo: &dyn GameScriptRepository) {
    let submitted = submit(repo, "d-1", "return 1", ts(10)).await.revision;

    repo.append_diagnostic(
        &submitted.revision_id,
        GameScriptDiagnosticSeverity::Warning,
        "validator:lua",
        "unused variable `x`",
        ts(20),
    )
    .await
    .expect("append diagnostic");
    repo.pin_revision(&submitted.revision_id, "op-pinner", ts(30))
        .await
        .expect("pin");
    repo.allocate_activation_generation(
        "cluster",
        &submitted.revision_id,
        "op-activator",
        &no_context(),
        ts(40),
    )
    .await
    .expect("activate");

    // No related mutation may alter one byte of the revision record.
    let fetched = repo
        .get_revision(&submitted.revision_id)
        .await
        .expect("get")
        .expect("present");
    assert_eq!(fetched, submitted, "revision record is immutable");
}

async fn scenario_failed_submit_leaves_no_audit_or_outbox(repo: &dyn GameScriptRepository) {
    assert_eq!(
        repo.submit_draft("ghost", "op-submitter", &no_context(), ts(10))
            .await
            .expect_err("submit of a missing draft")
            .category(),
        ErrorCategory::NotFound
    );
    assert!(repo.audit_log(10).await.expect("audit").is_empty());
    assert!(repo.pending_outbox(10).await.expect("outbox").is_empty());
    assert!(repo.list_revisions(10).await.expect("revisions").is_empty());
}

async fn scenario_diagnostics_append_in_order_without_mutation(repo: &dyn GameScriptRepository) {
    let revision = submit(repo, "d-1", "return 1", ts(10)).await.revision;

    let first = repo
        .append_diagnostic(
            &revision.revision_id,
            GameScriptDiagnosticSeverity::Info,
            "validator:lua",
            "parsed",
            ts(20),
        )
        .await
        .expect("first diagnostic");
    assert_eq!(first.seq, 1);
    let second = repo
        .append_diagnostic(
            &revision.revision_id,
            GameScriptDiagnosticSeverity::Error,
            "validator:lua",
            "missing handler",
            ts(21),
        )
        .await
        .expect("second diagnostic");
    assert_eq!(second.seq, 2);

    let listed = repo
        .diagnostics(&revision.revision_id)
        .await
        .expect("diagnostics");
    assert_eq!(listed, vec![first, second], "append order is preserved");
    assert_eq!(listed[0].severity, GameScriptDiagnosticSeverity::Info);
    assert_eq!(listed[1].severity, GameScriptDiagnosticSeverity::Error);
    assert_eq!(listed[1].message, "missing handler");
    assert_eq!(listed[1].source, "validator:lua");

    // Diagnostics attach only to existing revisions.
    assert_eq!(
        repo.append_diagnostic(
            "ghost",
            GameScriptDiagnosticSeverity::Info,
            "validator:lua",
            "orphan",
            ts(22),
        )
        .await
        .expect_err("diagnostic for a missing revision")
        .category(),
        ErrorCategory::NotFound
    );
    assert_eq!(
        repo.diagnostics("ghost")
            .await
            .expect_err("diagnostics of a missing revision")
            .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_activation_generations_are_monotonic_per_scope(repo: &dyn GameScriptRepository) {
    let a = submit(repo, "d-1", "return 1", ts(10)).await.revision;
    let b = submit(repo, "d-2", "return 2", ts(11)).await.revision;

    let first = repo
        .allocate_activation_generation("cluster", &a.revision_id, "op-1", &no_context(), ts(20))
        .await
        .expect("first activation");
    assert_eq!(first.generation, 1);
    assert_eq!(first.scope, "cluster");
    assert_eq!(first.revision_id, a.revision_id);
    assert_eq!(first.activated_by, "op-1");
    assert_eq!(first.activated_at, ts(20));

    let second = repo
        .allocate_activation_generation("cluster", &b.revision_id, "op-1", &no_context(), ts(21))
        .await
        .expect("second activation");
    assert_eq!(second.generation, 2, "generation is strictly monotonic");

    // A rollback is a *new* generation targeting a prior revision — the fencing
    // counter never moves backwards.
    let rollback = repo
        .allocate_activation_generation("cluster", &a.revision_id, "op-2", &no_context(), ts(22))
        .await
        .expect("rollback activation");
    assert_eq!(rollback.generation, 3);
    assert_eq!(rollback.revision_id, a.revision_id);

    // Scopes advance independently.
    let other_scope = repo
        .allocate_activation_generation("node:eu-1", &a.revision_id, "op-1", &no_context(), ts(23))
        .await
        .expect("other scope activation");
    assert_eq!(other_scope.generation, 1);

    let current = repo
        .current_activation("cluster")
        .await
        .expect("current")
        .expect("activated scope");
    assert_eq!(current, rollback);
    assert!(
        repo.current_activation("node:ap-1")
            .await
            .expect("current of untouched scope")
            .is_none()
    );

    let history = repo.list_activations("cluster", 10).await.expect("history");
    assert_eq!(
        history
            .iter()
            .map(|entry| entry.generation)
            .collect::<Vec<_>>(),
        vec![3, 2, 1],
        "history is newest-first"
    );

    // Every activation committed an audit record and an outbox entry.
    let audit = repo.audit_log(10).await.expect("audit");
    assert_eq!(
        audit
            .iter()
            .filter(|entry| entry.action == "gamescript.activation.commit")
            .count(),
        4
    );
    let outbox = repo.pending_outbox(10).await.expect("outbox");
    let activations: Vec<_> = outbox
        .iter()
        .filter(|entry| entry.kind == GameScriptOutboxKind::ActivationCommitted)
        .collect();
    assert_eq!(activations.len(), 4);
    assert!(
        activations
            .iter()
            .all(|entry| entry.generation.is_some() && entry.scope.is_some())
    );

    // Scope and actor are mandatory.
    assert_eq!(
        repo.allocate_activation_generation("", &a.revision_id, "op-1", &no_context(), ts(24))
            .await
            .expect_err("empty scope")
            .category(),
        ErrorCategory::Validation
    );
    assert_eq!(
        repo.allocate_activation_generation("cluster", &a.revision_id, "", &no_context(), ts(24))
            .await
            .expect_err("empty actor")
            .category(),
        ErrorCategory::Validation
    );
}

async fn scenario_activation_rejects_unknown_or_pruned_revision(repo: &dyn GameScriptRepository) {
    // A rollback/activation target must reference an existing revision.
    assert_eq!(
        repo.allocate_activation_generation(
            "cluster",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "op-1",
            &no_context(),
            ts(10),
        )
        .await
        .expect_err("unknown revision target")
        .category(),
        ErrorCategory::NotFound
    );
    assert!(repo.audit_log(10).await.expect("audit").is_empty());
    assert!(repo.pending_outbox(10).await.expect("outbox").is_empty());
    assert!(
        repo.current_activation("cluster")
            .await
            .expect("current")
            .is_none(),
        "a rejected activation consumes no generation"
    );

    // A pruned revision is no longer a valid rollback target.
    let pruned = submit(repo, "d-1", "return 1", ts(20)).await.revision;
    assert_eq!(
        repo.prune_revisions(ts(100), 10).await.expect("prune"),
        1,
        "unpinned, inactive revision is prunable"
    );
    assert_eq!(
        repo.allocate_activation_generation(
            "cluster",
            &pruned.revision_id,
            "op-1",
            &no_context(),
            ts(30),
        )
        .await
        .expect_err("pruned revision target")
        .category(),
        ErrorCategory::NotFound
    );
}

async fn scenario_retention_never_prunes_active_or_pinned_revisions(
    repo: &dyn GameScriptRepository,
) {
    let active = submit(repo, "d-1", "return 1", ts(10)).await.revision;
    let pinned = submit(repo, "d-2", "return 2", ts(20)).await.revision;
    let stale = submit(repo, "d-3", "return 3", ts(30)).await.revision;
    repo.append_diagnostic(
        &stale.revision_id,
        GameScriptDiagnosticSeverity::Info,
        "validator:lua",
        "parsed",
        ts(31),
    )
    .await
    .expect("diagnostic on prunable revision");

    repo.allocate_activation_generation(
        "cluster",
        &active.revision_id,
        "op-1",
        &no_context(),
        ts(40),
    )
    .await
    .expect("activate");
    assert!(
        repo.pin_revision(&pinned.revision_id, "op-1", ts(41))
            .await
            .expect("pin")
    );
    assert!(
        !repo
            .pin_revision(&pinned.revision_id, "op-1", ts(42))
            .await
            .expect("idempotent pin"),
        "second pin changes nothing"
    );

    assert_eq!(
        repo.prune_revisions(ts(100), 10).await.expect("prune"),
        1,
        "only the unprotected revision is pruned"
    );
    assert!(
        repo.get_revision(&active.revision_id)
            .await
            .expect("get")
            .is_some(),
        "activation-referenced revisions are never pruned"
    );
    assert!(
        repo.get_revision(&pinned.revision_id)
            .await
            .expect("get")
            .is_some(),
        "pinned revisions are never pruned"
    );
    assert!(
        repo.get_revision(&stale.revision_id)
            .await
            .expect("get")
            .is_none()
    );
    assert_eq!(
        repo.diagnostics(&stale.revision_id)
            .await
            .expect_err("diagnostics die with their revision")
            .category(),
        ErrorCategory::NotFound
    );

    // Unpinning releases the retention hold.
    assert!(
        repo.unpin_revision(&pinned.revision_id, "op-1", ts(50))
            .await
            .expect("unpin")
    );
    assert!(
        !repo
            .unpin_revision(&pinned.revision_id, "op-1", ts(51))
            .await
            .expect("idempotent unpin")
    );
    assert_eq!(repo.prune_revisions(ts(100), 10).await.expect("prune"), 1);
    assert!(
        repo.get_revision(&pinned.revision_id)
            .await
            .expect("get")
            .is_none()
    );

    // Pruning is bounded by its limit, oldest first.
    let old_a = submit(repo, "d-4", "return 4", ts(60)).await.revision;
    let old_b = submit(repo, "d-5", "return 5", ts(61)).await.revision;
    assert_eq!(repo.prune_revisions(ts(100), 1).await.expect("prune"), 1);
    assert!(
        repo.get_revision(&old_a.revision_id)
            .await
            .expect("get")
            .is_none(),
        "the oldest prunable revision goes first"
    );
    assert!(
        repo.get_revision(&old_b.revision_id)
            .await
            .expect("get")
            .is_some()
    );
}

async fn scenario_draft_retention_is_bounded_and_cutoff_scoped(repo: &dyn GameScriptRepository) {
    repo.create_draft(draft("d-old", "return 1"), ts(10))
        .await
        .expect("create");
    repo.create_draft(draft("d-mid", "return 2"), ts(20))
        .await
        .expect("create");
    repo.create_draft(draft("d-new", "return 3"), ts(30))
        .await
        .expect("create");

    assert_eq!(
        repo.prune_drafts(ts(25), 1).await.expect("bounded prune"),
        1
    );
    assert!(
        repo.get_draft("d-old").await.expect("get").is_none(),
        "the oldest stale draft goes first"
    );
    assert!(repo.get_draft("d-mid").await.expect("get").is_some());

    assert_eq!(repo.prune_drafts(ts(25), 10).await.expect("prune"), 1);
    assert!(repo.get_draft("d-mid").await.expect("get").is_none());
    assert_eq!(
        repo.prune_drafts(ts(25), 10)
            .await
            .expect("prune of nothing"),
        0
    );
    assert!(
        repo.get_draft("d-new").await.expect("get").is_some(),
        "drafts newer than the cutoff are retained"
    );
}

async fn scenario_outbox_lifecycle_is_at_least_once(repo: &dyn GameScriptRepository) {
    let revision = submit(repo, "d-1", "return 1", ts(10)).await.revision;
    repo.allocate_activation_generation(
        "cluster",
        &revision.revision_id,
        "op-1",
        &no_context(),
        ts(20),
    )
    .await
    .expect("activate");

    let pending = repo.pending_outbox(10).await.expect("outbox");
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].kind, GameScriptOutboxKind::RevisionCreated);
    assert_eq!(pending[1].kind, GameScriptOutboxKind::ActivationCommitted);
    assert_eq!(
        repo.pending_outbox(1).await.expect("bounded outbox").len(),
        1
    );

    assert!(
        repo.acknowledge_outbox(pending[0].outbox_id)
            .await
            .expect("acknowledge")
    );
    assert!(
        !repo
            .acknowledge_outbox(pending[0].outbox_id)
            .await
            .expect("idempotent acknowledge"),
        "second acknowledgement removes nothing"
    );
    let remaining = repo.pending_outbox(10).await.expect("outbox");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].kind, GameScriptOutboxKind::ActivationCommitted);
    assert_eq!(remaining[0].generation, Some(1));
    assert_eq!(remaining[0].scope.as_deref(), Some("cluster"));
}

async fn scenario_audit_details_are_redacted(repo: &dyn GameScriptRepository) {
    repo.create_draft(draft("d-1", "return 1"), ts(10))
        .await
        .expect("create");
    let mut context = BTreeMap::new();
    context.insert("api_token".to_owned(), "sk-live-actual-secret".to_owned());
    context.insert("reason".to_owned(), "scheduled deploy".to_owned());
    let revision = repo
        .submit_draft("d-1", "op-1", &context, ts(20))
        .await
        .expect("submit")
        .revision;

    let mut activation_context = BTreeMap::new();
    activation_context.insert("webhook_secret".to_owned(), "shhh".to_owned());
    activation_context.insert("ticket".to_owned(), "OPS-1234".to_owned());
    repo.allocate_activation_generation(
        "cluster",
        &revision.revision_id,
        "op-1",
        &activation_context,
        ts(30),
    )
    .await
    .expect("activate");

    let audit = repo.audit_log(10).await.expect("audit");
    assert_eq!(audit.len(), 2);
    // Newest first: the activation, then the submit.
    assert_eq!(
        audit[0].details.get("webhook_secret").map(String::as_str),
        Some("[redacted]"),
        "secret-bearing detail values never persist"
    );
    assert_eq!(
        audit[0].details.get("ticket").map(String::as_str),
        Some("OPS-1234")
    );
    assert_eq!(
        audit[1].details.get("api_token").map(String::as_str),
        Some("[redacted]")
    );
    assert_eq!(
        audit[1].details.get("reason").map(String::as_str),
        Some("scheduled deploy")
    );
    assert!(
        !format!("{audit:?}").contains("sk-live-actual-secret"),
        "the raw secret is nowhere in the persisted audit log"
    );
}

async fn scenario_zero_limits_are_rejected(repo: &dyn GameScriptRepository) {
    for error in [
        repo.list_drafts(0).await.expect_err("drafts limit"),
        repo.list_revisions(0).await.expect_err("revisions limit"),
        repo.list_activations("cluster", 0)
            .await
            .expect_err("activations limit"),
        repo.audit_log(0).await.expect_err("audit limit"),
        repo.pending_outbox(0).await.expect_err("outbox limit"),
        repo.prune_drafts(ts(1), 0)
            .await
            .expect_err("draft prune limit"),
        repo.prune_revisions(ts(1), 0)
            .await
            .expect_err("revision prune limit"),
    ] {
        assert_eq!(error.category(), ErrorCategory::Validation);
    }
}

// --- Concurrency scenarios (Arc-based; run per backend below) ----------------

async fn concurrent_identical_submissions_converge(repo: Arc<dyn GameScriptRepository>) {
    const TASKS: usize = 8;
    for index in 0..TASKS {
        repo.create_draft(draft(&format!("d-{index}"), "return 99"), ts(10))
            .await
            .expect("create draft");
    }
    let mut handles = Vec::new();
    for index in 0..TASKS {
        let repo = Arc::clone(&repo);
        handles.push(tokio::spawn(async move {
            repo.submit_draft(&format!("d-{index}"), "op-race", &BTreeMap::new(), ts(20))
                .await
        }));
    }
    let mut created = 0;
    let mut ids = std::collections::BTreeSet::new();
    for handle in handles {
        let submission = handle
            .await
            .expect("join")
            .expect("every racing submit succeeds");
        if !submission.deduplicated {
            created += 1;
        }
        ids.insert(submission.revision.revision_id.clone());
    }
    assert_eq!(created, 1, "exactly one racer creates the revision");
    assert_eq!(ids.len(), 1, "every racer observes the same revision id");
    assert_eq!(repo.list_revisions(10).await.expect("list").len(), 1);
    assert_eq!(
        repo.pending_outbox(10)
            .await
            .expect("outbox")
            .iter()
            .filter(|entry| entry.kind == GameScriptOutboxKind::RevisionCreated)
            .count(),
        1,
        "one revision creation rolls out exactly once"
    );
}

async fn concurrent_generation_allocations_stay_monotonic(repo: Arc<dyn GameScriptRepository>) {
    const TASKS: u64 = 8;
    repo.create_draft(draft("d-gen", "return 1"), ts(10))
        .await
        .expect("create draft");
    let revision = repo
        .submit_draft("d-gen", "op-1", &BTreeMap::new(), ts(11))
        .await
        .expect("submit")
        .revision;

    let mut handles = Vec::new();
    for _ in 0..TASKS {
        let repo = Arc::clone(&repo);
        let revision_id = revision.revision_id.clone();
        handles.push(tokio::spawn(async move {
            repo.allocate_activation_generation(
                "cluster",
                &revision_id,
                "op-race",
                &BTreeMap::new(),
                ts(20),
            )
            .await
        }));
    }
    let mut generations = std::collections::BTreeSet::new();
    for handle in handles {
        let activation = handle
            .await
            .expect("join")
            .expect("every racing allocation succeeds");
        generations.insert(activation.generation);
    }
    assert_eq!(
        generations,
        (1..=TASKS).collect::<std::collections::BTreeSet<_>>(),
        "concurrent allocations produce distinct consecutive generations"
    );
    assert_eq!(
        repo.current_activation("cluster")
            .await
            .expect("current")
            .expect("present")
            .generation,
        TASKS
    );
}

/// Race retention pruning against activation of the same (prunable) revision:
/// whatever interleaving occurs, a committed activation must never reference a
/// pruned revision.
///
/// A deterministic interleaving — pausing one repository transaction between
/// its read and write phases — is not expressible through the repository API
/// without server-side failpoints, so this bounded stress loop is the
/// strongest expressible guard. Either racing operation may fail (the SQL
/// foreign-key backstop surfaces as an error on the losing side; a pruned
/// target is NotFound); the invariant below is what must always hold. Before
/// the MongoDB retention-fence fix this loop reproduced the write-skew
/// (a stranded activation referencing a deleted revision) reliably.
async fn concurrent_prune_never_strands_an_activation(repo: Arc<dyn GameScriptRepository>) {
    const ROUNDS: u64 = 16;
    for round in 0..ROUNDS {
        let draft_id = format!("d-race-{round}");
        let scope = format!("scope-{round}");
        repo.create_draft(draft(&draft_id, &format!("return {round}")), ts(10))
            .await
            .expect("create draft");
        let revision = repo
            .submit_draft(&draft_id, "op-1", &BTreeMap::new(), ts(10 + round))
            .await
            .expect("submit")
            .revision;

        let activate = {
            let repo = Arc::clone(&repo);
            let revision_id = revision.revision_id.clone();
            let scope = scope.clone();
            tokio::spawn(async move {
                repo.allocate_activation_generation(
                    &scope,
                    &revision_id,
                    "op-race",
                    &BTreeMap::new(),
                    ts(20),
                )
                .await
            })
        };
        let prune = {
            let repo = Arc::clone(&repo);
            tokio::spawn(async move { repo.prune_revisions(ts(1_000_000), 10).await })
        };
        let _ = activate.await.expect("join activation");
        let _ = prune.await.expect("join prune");

        for activation in repo
            .list_activations(&scope, 10)
            .await
            .expect("list activations")
        {
            assert!(
                repo.get_revision(&activation.revision_id)
                    .await
                    .expect("get revision")
                    .is_some(),
                "round {round}: a committed activation must reference an existing revision"
            );
        }
    }
}

// --- Scenario table ---------------------------------------------------------

type ScenarioFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>;
type Scenario = (
    &'static str,
    fn(&dyn GameScriptRepository) -> ScenarioFuture<'_>,
);

macro_rules! scenarios {
    ($($name:ident),* $(,)?) => {
        vec![$((
            stringify!($name),
            (|repo| -> ScenarioFuture<'_> { Box::pin($name(repo)) })
                as fn(&dyn GameScriptRepository) -> ScenarioFuture<'_>,
        )),*]
    };
}

fn all_scenarios() -> Vec<Scenario> {
    scenarios![
        scenario_draft_lifecycle_round_trips,
        scenario_draft_creation_is_validated_and_unique,
        scenario_submit_creates_immutable_hash_addressed_revision,
        scenario_identical_content_dedupes_to_one_revision,
        scenario_revision_is_immutable_under_related_mutations,
        scenario_failed_submit_leaves_no_audit_or_outbox,
        scenario_diagnostics_append_in_order_without_mutation,
        scenario_activation_generations_are_monotonic_per_scope,
        scenario_activation_rejects_unknown_or_pruned_revision,
        scenario_retention_never_prunes_active_or_pinned_revisions,
        scenario_draft_retention_is_bounded_and_cutoff_scoped,
        scenario_outbox_lifecycle_is_at_least_once,
        scenario_audit_details_are_redacted,
        scenario_zero_limits_are_rejected,
    ]
}

// --- In-memory runs (always) ------------------------------------------------

#[tokio::test]
async fn in_memory_backend_satisfies_the_contract() {
    for (name, run) in all_scenarios() {
        let repo = InMemoryGameScriptRepository::new();
        eprintln!("in-memory scenario: {name}");
        run(&repo).await;
    }
}

#[tokio::test]
async fn in_memory_concurrent_identical_submissions_converge() {
    concurrent_identical_submissions_converge(Arc::new(InMemoryGameScriptRepository::new())).await;
}

#[tokio::test]
async fn in_memory_concurrent_generation_allocations_stay_monotonic() {
    concurrent_generation_allocations_stay_monotonic(Arc::new(InMemoryGameScriptRepository::new()))
        .await;
}

#[tokio::test]
async fn in_memory_concurrent_prune_never_strands_an_activation() {
    concurrent_prune_never_strands_an_activation(Arc::new(InMemoryGameScriptRepository::new()))
        .await;
}

// --- SQLite runs (always; embedded, no server) -------------------------------

mod sqlite {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::SqliteDatabase;

    async fn fresh_database() -> SqliteDatabase {
        SqliteDatabase::connect(&DatabaseConfig {
            url: Some("sqlite::memory:".to_owned()),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect + migrate an in-memory SQLite database")
    }

    #[tokio::test]
    async fn sqlite_backend_satisfies_the_contract() {
        for (name, run) in all_scenarios() {
            let db = fresh_database().await;
            let repo = db.gamescript_repository();
            eprintln!("sqlite scenario: {name}");
            run(repo.as_ref()).await;
        }
    }

    #[tokio::test]
    async fn sqlite_concurrent_identical_submissions_converge() {
        let db = fresh_database().await;
        concurrent_identical_submissions_converge(db.gamescript_repository()).await;
    }

    #[tokio::test]
    async fn sqlite_concurrent_generation_allocations_stay_monotonic() {
        let db = fresh_database().await;
        concurrent_generation_allocations_stay_monotonic(db.gamescript_repository()).await;
    }

    #[tokio::test]
    async fn sqlite_concurrent_prune_never_strands_an_activation() {
        let db = fresh_database().await;
        concurrent_prune_never_strands_an_activation(db.gamescript_repository()).await;
    }
}

// --- Postgres runs (opt-in via DATABASE_URL) ---------------------------------

mod postgres {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::PgDatabase;

    fn test_database_url() -> Option<String> {
        std::env::var("DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("CITADEL_TEST_DATABASE_URL").ok())
            .filter(|url| !url.trim().is_empty())
    }

    async fn connect(url: String) -> PgDatabase {
        PgDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect + migrate against the test Postgres")
    }

    /// One gated test per backend, like the sibling wallet/friends/groups
    /// suites: the gated runs share one external database, so scenario and
    /// concurrency coverage must execute inside a single `#[tokio::test]`
    /// rather than racing each other under the default parallel harness.
    #[tokio::test]
    async fn postgres_backend_satisfies_the_contract() {
        let Some(url) = test_database_url() else {
            eprintln!(
                "skipping Postgres GameScript contract: set DATABASE_URL or \
                 CITADEL_TEST_DATABASE_URL to run it"
            );
            return;
        };
        let db = connect(url).await;
        let repo = db.gamescript_repository();
        for (name, run) in all_scenarios() {
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("postgres scenario: {name}");
            run(repo.as_ref()).await;
        }
        db.reset_storage_for_tests().await.expect("reset");
        concurrent_identical_submissions_converge(db.gamescript_repository()).await;
        db.reset_storage_for_tests().await.expect("reset");
        concurrent_generation_allocations_stay_monotonic(db.gamescript_repository()).await;
        db.reset_storage_for_tests().await.expect("reset");
        concurrent_prune_never_strands_an_activation(db.gamescript_repository()).await;
    }
}

// --- CockroachDB runs (opt-in via CITADEL_TEST_COCKROACH_URL) ----------------

mod cockroach {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::PgDatabase;

    /// Rewrite the provided URL to the `cockroach://` scheme so the backend
    /// selects the CockroachDB flavor (its migrations + advisory-lock skip).
    fn test_cockroach_url() -> Option<String> {
        let url = std::env::var("CITADEL_TEST_COCKROACH_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        Some(if let Some(rest) = url.strip_prefix("postgresql://") {
            format!("cockroach://{rest}")
        } else if let Some(rest) = url.strip_prefix("postgres://") {
            format!("cockroach://{rest}")
        } else {
            url
        })
    }

    #[tokio::test]
    async fn cockroach_backend_satisfies_the_contract() {
        let Some(url) = test_cockroach_url() else {
            eprintln!("skipping CockroachDB GameScript contract: set CITADEL_TEST_COCKROACH_URL");
            return;
        };
        let db = PgDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect + migrate against the test CockroachDB");
        let repo = db.gamescript_repository();
        for (name, run) in all_scenarios() {
            db.reset_storage_for_tests()
                .await
                .expect("reset storage between scenarios");
            eprintln!("cockroach scenario: {name}");
            run(repo.as_ref()).await;
        }
        db.reset_storage_for_tests().await.expect("reset");
        concurrent_identical_submissions_converge(db.gamescript_repository()).await;
        db.reset_storage_for_tests().await.expect("reset");
        concurrent_generation_allocations_stay_monotonic(db.gamescript_repository()).await;
        db.reset_storage_for_tests().await.expect("reset");
        concurrent_prune_never_strands_an_activation(db.gamescript_repository()).await;
    }
}

// --- MongoDB runs (opt-in via CITADEL_TEST_MONGODB_URL) ----------------------

mod mongodb {
    use super::*;
    use citadel::config::DatabaseConfig;
    use citadel::repository::MongoDatabase;

    fn test_database_url() -> Option<String> {
        std::env::var("CITADEL_TEST_MONGODB_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())
    }

    #[tokio::test]
    async fn mongodb_backend_satisfies_the_contract() {
        let Some(url) = test_database_url() else {
            eprintln!("skipping MongoDB GameScript contract: set CITADEL_TEST_MONGODB_URL");
            return;
        };
        let db = MongoDatabase::connect(&DatabaseConfig {
            url: Some(url),
            ..DatabaseConfig::default()
        })
        .await
        .expect("connect + reconcile against MongoDB replica set");
        let repo = db.gamescript_repository();
        for (name, run) in all_scenarios() {
            db.clear_gamescript_data_for_tests()
                .await
                .expect("reset gamescript collections between scenarios");
            eprintln!("mongodb scenario: {name}");
            run(repo.as_ref()).await;
        }
        db.clear_gamescript_data_for_tests().await.expect("reset");
        concurrent_identical_submissions_converge(db.gamescript_repository()).await;
        db.clear_gamescript_data_for_tests().await.expect("reset");
        concurrent_generation_allocations_stay_monotonic(db.gamescript_repository()).await;
        db.clear_gamescript_data_for_tests().await.expect("reset");
        concurrent_prune_never_strands_an_activation(db.gamescript_repository()).await;
    }
}
