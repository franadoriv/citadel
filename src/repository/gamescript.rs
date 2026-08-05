//! Immutable GameScript revision repository contract.
//!
//! This is the storage foundation for the GameScript
//! revision → validation → activation → rollout pipeline:
//!
//! - **Drafts** are the only mutable stage. An operator edits a draft until it
//!   is submitted; submission consumes the draft.
//! - **Revisions** are immutable and hash-addressed: the revision id *is* the
//!   lowercase-hex SHA-256 of the canonicalized payload (language, entrypoint,
//!   content — see [`gamescript_revision_content_hash`]). Submitting content
//!   identical to an existing revision deduplicates to that revision; no
//!   contract method can mutate a stored revision, and concurrent submissions
//!   of identical content race safely to one row because the hash is the
//!   primary key in every durable backend.
//! - **Diagnostics** are appendable validation output attached to a revision.
//!   They never alter revision content and die with their revision.
//! - **Activation generations** are a per-scope, strictly monotonic fencing
//!   counter. Every activation (including a rollback, which is simply a new
//!   generation that targets a prior revision) allocates the next generation
//!   for its scope. Scope note: the counter lives in the node's selected
//!   backend, so — exactly like chat authority epochs and the leaderboard
//!   scheduler's fencing tokens — it is **cluster-scoped whenever nodes share
//!   a durable database** and node-local only on the non-durable in-memory
//!   backend. The free-form `scope` key (e.g. `"cluster"`, `"node:eu-1"`)
//!   leaves room for narrower fencing domains without a schema change.
//! - **Audit** records capture the operator action that produced each state
//!   change. Detail values under secret-looking keys are redacted *before*
//!   they reach any backend (see [`redact_gamescript_audit_details`]).
//! - **Outbox** entries are written in the same atomic scope as the state
//!   change that produced them and feed cluster rollout notification.
//!   Delivery is at-least-once: consumers acknowledge entries after acting on
//!   them and must deduplicate on `(kind, revision, scope, generation)`.
//!
//! Per-backend atomicity of the audit/outbox write, honestly stated:
//! in-memory mutates all state under one lock; SQLite/PostgreSQL/CockroachDB
//! run the state change, audit row, and outbox row in one database
//! transaction; MongoDB uses a replayable replica-set transaction (a
//! standalone `mongod` without a replica set cannot start transactions, and
//! the Mongo adapter then surfaces `Database` errors rather than faking
//! atomicity).

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::RuntimeLanguage;
use crate::error::{AppError, AppResult};
use crate::time::TimestampMillis;

/// Free-form operator context persisted (redacted) with an audit record.
pub type GameScriptAuditContext = BTreeMap<String, String>;

/// Validation caps every backend applies to draft/revision source payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GameScriptLimits {
    /// Maximum size in bytes of one script source payload.
    pub max_source_bytes: usize,
}

/// PROVISIONAL cap on one script source payload.
///
/// 1 MiB is a placeholder pending a real measurement: the p95 size of shipped
/// Lua/Python/JS entrypoint bundles across the sample games and early adopter
/// projects. Operators get a config knob for this when the GameScript console
/// API lands; until then the constant exists so no backend accepts an
/// unbounded payload.
pub const PROVISIONAL_MAX_GAMESCRIPT_SOURCE_BYTES: usize = 1024 * 1024;

impl Default for GameScriptLimits {
    fn default() -> Self {
        Self {
            max_source_bytes: PROVISIONAL_MAX_GAMESCRIPT_SOURCE_BYTES,
        }
    }
}

/// A mutable, not-yet-submitted script draft.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GameScriptDraft {
    /// Operator-chosen draft identifier.
    pub draft_id: String,
    /// Script language the draft targets.
    pub language: RuntimeLanguage,
    /// Entrypoint filename metadata (e.g. `main.lua`).
    pub entrypoint: String,
    /// Full script source.
    pub content: String,
    /// Operator that created the draft.
    pub created_by: String,
    /// Creation instant.
    pub created_at: TimestampMillis,
    /// Last mutation instant (drives draft retention).
    pub updated_at: TimestampMillis,
}

/// Parameters for creating a draft.
#[derive(Debug, Clone)]
pub struct CreateGameScriptDraftRequest {
    pub draft_id: String,
    pub language: RuntimeLanguage,
    pub entrypoint: String,
    pub content: String,
    pub created_by: String,
}

/// Replacement payload for an existing draft.
#[derive(Debug, Clone)]
pub struct UpdateGameScriptDraftRequest {
    pub language: RuntimeLanguage,
    pub entrypoint: String,
    pub content: String,
}

/// One immutable, hash-addressed script revision.
///
/// No repository method mutates a stored revision; every field is fixed at
/// submission. `revision_id` is the content hash (see
/// [`gamescript_revision_content_hash`]), which is what makes deduplication
/// and the concurrent-submission race safe in every backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GameScriptRevision {
    /// Lowercase-hex SHA-256 of the canonicalized payload; also the identity.
    pub revision_id: String,
    /// Script language.
    pub language: RuntimeLanguage,
    /// Entrypoint filename metadata.
    pub entrypoint: String,
    /// Full immutable script source.
    pub content: String,
    /// Source size in bytes (denormalized for listings and caps).
    pub size_bytes: u64,
    /// Operator whose submission created the revision.
    pub created_by: String,
    /// Creation instant.
    pub created_at: TimestampMillis,
}

impl GameScriptRevision {
    /// The content hash — identical to [`Self::revision_id`] by construction.
    #[must_use]
    pub fn content_hash(&self) -> &str {
        &self.revision_id
    }
}

/// Outcome of submitting a draft.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GameScriptSubmission {
    /// The revision the submitted content resolves to.
    pub revision: GameScriptRevision,
    /// `true` when identical content already existed and no new revision row
    /// (and no new outbox entry) was created.
    pub deduplicated: bool,
}

/// Severity of one validation diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GameScriptDiagnosticSeverity {
    Info,
    Warning,
    Error,
}

impl GameScriptDiagnosticSeverity {
    /// Stable persistence token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }

    /// Parse a durable severity token.
    pub fn from_token(token: &str) -> AppResult<Self> {
        match token {
            "info" => Ok(Self::Info),
            "warning" => Ok(Self::Warning),
            "error" => Ok(Self::Error),
            _ => Err(AppError::internal(
                "unknown gamescript diagnostic severity token",
            )),
        }
    }
}

/// One appendable validation-output record attached to a revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GameScriptDiagnostic {
    /// The revision this diagnostic describes.
    pub revision_id: String,
    /// 1-based, per-revision append order.
    pub seq: u64,
    /// Diagnostic severity.
    pub severity: GameScriptDiagnosticSeverity,
    /// Emitting component (e.g. `validator:lua`).
    pub source: String,
    /// Human-readable diagnostic text.
    pub message: String,
    /// Append instant.
    pub created_at: TimestampMillis,
}

/// One committed activation: a revision bound to a fencing generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GameScriptActivation {
    /// Fencing scope the generation counter belongs to.
    pub scope: String,
    /// Strictly monotonic (per scope) fencing generation.
    pub generation: u64,
    /// The activated revision.
    pub revision_id: String,
    /// Operator that committed the activation.
    pub activated_by: String,
    /// Commit instant.
    pub activated_at: TimestampMillis,
}

/// One redacted operator-action audit record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GameScriptAuditRecord {
    /// Backend-assigned identifier (unique; newest-first ordering key).
    pub audit_id: u64,
    /// Operator that performed the action.
    pub actor: String,
    /// Stable action token (e.g. `gamescript.draft.submit`).
    pub action: String,
    /// Primary target of the action (revision id or scope).
    pub target: String,
    /// Redacted key/value context. Values under secret-looking keys are
    /// replaced with `[redacted]` before any backend sees them.
    pub details: GameScriptAuditContext,
    /// Action instant.
    pub created_at: TimestampMillis,
}

/// What a pending outbox entry announces to the rollout pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GameScriptOutboxKind {
    /// A new immutable revision exists (deduplicated submits emit nothing).
    RevisionCreated,
    /// A revision was bound to a new activation generation.
    ActivationCommitted,
}

impl GameScriptOutboxKind {
    /// Stable persistence token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RevisionCreated => "revision_created",
            Self::ActivationCommitted => "activation_committed",
        }
    }

    /// Parse a durable kind token.
    pub fn from_token(token: &str) -> AppResult<Self> {
        match token {
            "revision_created" => Ok(Self::RevisionCreated),
            "activation_committed" => Ok(Self::ActivationCommitted),
            _ => Err(AppError::internal("unknown gamescript outbox kind token")),
        }
    }
}

/// One durable rollout-notification work item.
///
/// Delivery is at-least-once: a consumer can crash between acting on an entry
/// and acknowledging it, so downstream side effects must deduplicate on the
/// entry's `(kind, revision_id, scope, generation)` identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GameScriptOutboxRecord {
    /// Backend-assigned identifier used to acknowledge the entry.
    pub outbox_id: u64,
    /// What happened.
    pub kind: GameScriptOutboxKind,
    /// Fencing scope for activations; `None` for revision creation.
    pub scope: Option<String>,
    /// The revision the entry is about.
    pub revision_id: String,
    /// The committed generation for activations; `None` for revision creation.
    pub generation: Option<u64>,
    /// Commit instant of the producing state change.
    pub created_at: TimestampMillis,
}

/// Persistence boundary for drafts, immutable revisions, diagnostics,
/// activation generations, redacted audit, and the rollout outbox.
///
/// Deliberately absent: any method that mutates a stored revision. The
/// downstream validation/activation orchestration tasks consume this contract;
/// they never get a wider one.
#[async_trait]
pub trait GameScriptRepository: Send + Sync {
    /// Create a new draft. Fails with `Conflict` if the id is taken.
    async fn create_draft(
        &self,
        request: CreateGameScriptDraftRequest,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDraft>;

    /// Replace a draft's payload. Fails with `NotFound` for a missing draft.
    async fn update_draft(
        &self,
        draft_id: &str,
        update: UpdateGameScriptDraftRequest,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDraft>;

    /// Read one draft.
    async fn get_draft(&self, draft_id: &str) -> AppResult<Option<GameScriptDraft>>;

    /// List drafts in ascending draft-id order, bounded by `limit`.
    async fn list_drafts(&self, limit: usize) -> AppResult<Vec<GameScriptDraft>>;

    /// Delete one draft. Returns whether a draft was removed.
    async fn delete_draft(&self, draft_id: &str) -> AppResult<bool>;

    /// Atomically convert a draft into its immutable, hash-addressed revision.
    ///
    /// In one atomic scope this: resolves the draft's content hash, creates
    /// the revision row unless identical content already exists (then the
    /// existing revision is returned with `deduplicated = true`), writes the
    /// redacted audit record, stages a [`GameScriptOutboxKind::RevisionCreated`]
    /// entry only when a revision was actually created, and deletes the draft.
    async fn submit_draft(
        &self,
        draft_id: &str,
        actor: &str,
        context: &GameScriptAuditContext,
        now: TimestampMillis,
    ) -> AppResult<GameScriptSubmission>;

    /// Read one immutable revision.
    async fn get_revision(&self, revision_id: &str) -> AppResult<Option<GameScriptRevision>>;

    /// List revisions oldest-first (creation instant, then id), bounded by
    /// `limit`.
    async fn list_revisions(&self, limit: usize) -> AppResult<Vec<GameScriptRevision>>;

    /// Append one validation diagnostic to an existing revision.
    ///
    /// Fails with `NotFound` when the revision does not exist (or was pruned).
    async fn append_diagnostic(
        &self,
        revision_id: &str,
        severity: GameScriptDiagnosticSeverity,
        source: &str,
        message: &str,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDiagnostic>;

    /// All diagnostics for a revision in append order.
    ///
    /// Fails with `NotFound` when the revision does not exist (or was pruned).
    async fn diagnostics(&self, revision_id: &str) -> AppResult<Vec<GameScriptDiagnostic>>;

    /// Pin a revision, exempting it from retention pruning.
    ///
    /// Returns `false` when the revision was already pinned. Fails with
    /// `NotFound` for a missing revision. The pin is retention metadata in its
    /// own table — the revision record itself stays byte-identical.
    async fn pin_revision(
        &self,
        revision_id: &str,
        actor: &str,
        now: TimestampMillis,
    ) -> AppResult<bool>;

    /// Remove a revision's retention pin. Returns whether a pin was removed.
    async fn unpin_revision(
        &self,
        revision_id: &str,
        actor: &str,
        now: TimestampMillis,
    ) -> AppResult<bool>;

    /// Atomically allocate the next activation generation for `scope` and bind
    /// it to an existing revision.
    ///
    /// This is the single gate for both roll-forward and rollback: a rollback
    /// target must reference an existing, non-pruned revision or the
    /// allocation fails with `NotFound` **without consuming a generation** and
    /// without writing audit or outbox rows. On success the generation
    /// counter, activation row, redacted audit record, and
    /// [`GameScriptOutboxKind::ActivationCommitted`] outbox entry commit in
    /// one atomic scope.
    async fn allocate_activation_generation(
        &self,
        scope: &str,
        revision_id: &str,
        actor: &str,
        context: &GameScriptAuditContext,
        now: TimestampMillis,
    ) -> AppResult<GameScriptActivation>;

    /// The highest-generation activation for `scope`, if any.
    async fn current_activation(&self, scope: &str) -> AppResult<Option<GameScriptActivation>>;

    /// Activation history for `scope`, newest-first, bounded by `limit`.
    async fn list_activations(
        &self,
        scope: &str,
        limit: usize,
    ) -> AppResult<Vec<GameScriptActivation>>;

    /// Delete up to `limit` drafts last updated strictly before
    /// `updated_before`, oldest first. Returns how many were removed.
    async fn prune_drafts(&self, updated_before: TimestampMillis, limit: usize)
    -> AppResult<usize>;

    /// Delete up to `limit` revisions created strictly before
    /// `created_before`, oldest first, together with their diagnostics.
    ///
    /// Revisions referenced by any activation and pinned revisions are never
    /// pruned (the activation foreign key is a database-level backstop for the
    /// same rule). Returns how many revisions were removed.
    async fn prune_revisions(
        &self,
        created_before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize>;

    /// Audit records, newest-first, bounded by `limit`.
    async fn audit_log(&self, limit: usize) -> AppResult<Vec<GameScriptAuditRecord>>;

    /// Unacknowledged outbox entries in commit order, bounded by `limit`.
    async fn pending_outbox(&self, limit: usize) -> AppResult<Vec<GameScriptOutboxRecord>>;

    /// Acknowledge (remove) one delivered outbox entry. Idempotent: returns
    /// whether an entry was removed.
    async fn acknowledge_outbox(&self, outbox_id: u64) -> AppResult<bool>;
}

// --- Shared helpers (used by every backend) ----------------------------------

/// Domain-separated canonical content hash of one script payload.
///
/// The canonical payload is version-tagged and length-prefixed
/// (`citadel.gamescript.revision.v1`, then `len || bytes` for language token,
/// entrypoint, and content), so no field boundary ambiguity can make two
/// different payloads hash equal. The lowercase-hex SHA-256 of that byte
/// stream is the revision id.
#[must_use]
pub fn gamescript_revision_content_hash(
    language: RuntimeLanguage,
    entrypoint: &str,
    content: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"citadel.gamescript.revision.v1\0");
    for field in [language.as_str(), entrypoint, content] {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    let digest = hasher.finalize();
    format!("{digest:x}")
}

/// Detail keys whose values are always redacted in persisted audit records.
///
/// Matching is by case-insensitive substring, so `api_token`,
/// `WEBHOOK_SECRET`, `sessionKey`, and `db_password` are all masked. This is
/// deliberately over-broad: losing a benign detail value is acceptable;
/// persisting a secret is not.
const SENSITIVE_DETAIL_KEY_MARKERS: &[&str] = &[
    "secret",
    "token",
    "password",
    "passphrase",
    "credential",
    "authorization",
    "cookie",
    "key",
];

/// Mask secret-looking values in operator-supplied audit context.
///
/// Applied by every backend *before* persisting, so raw secrets never reach a
/// durable store or its logs.
#[must_use]
pub fn redact_gamescript_audit_details(details: &GameScriptAuditContext) -> GameScriptAuditContext {
    details
        .iter()
        .map(|(key, value)| {
            let lowered = key.to_ascii_lowercase();
            if SENSITIVE_DETAIL_KEY_MARKERS
                .iter()
                .any(|marker| lowered.contains(marker))
            {
                (key.clone(), crate::error::redact(value).to_owned())
            } else {
                (key.clone(), value.clone())
            }
        })
        .collect()
}

/// Parse a durable language token back to [`RuntimeLanguage`].
pub(crate) fn language_from_token(token: &str) -> AppResult<RuntimeLanguage> {
    match token {
        "lua" => Ok(RuntimeLanguage::Lua),
        "python" => Ok(RuntimeLanguage::Python),
        "js" => Ok(RuntimeLanguage::Js),
        _ => Err(AppError::internal("unknown gamescript language token")),
    }
}

/// Validate one draft/revision source payload against the shared caps.
pub(crate) fn validate_source(
    entrypoint: &str,
    content: &str,
    limits: &GameScriptLimits,
) -> AppResult<()> {
    if entrypoint.is_empty() {
        return Err(AppError::validation(
            "gamescript entrypoint must not be empty",
        ));
    }
    if content.is_empty() {
        return Err(AppError::validation("gamescript content must not be empty"));
    }
    if content.len() > limits.max_source_bytes {
        return Err(AppError::validation(
            "gamescript content exceeds the maximum source size",
        ));
    }
    Ok(())
}

/// Validate a full draft-creation request.
pub(crate) fn validate_create_draft(
    request: &CreateGameScriptDraftRequest,
    limits: &GameScriptLimits,
) -> AppResult<()> {
    if request.draft_id.is_empty() {
        return Err(AppError::validation(
            "gamescript draft id must not be empty",
        ));
    }
    if request.created_by.is_empty() {
        return Err(AppError::validation(
            "gamescript draft author must not be empty",
        ));
    }
    validate_source(&request.entrypoint, &request.content, limits)
}

/// Reject the zero limit every listing/pruning method treats as a bug.
pub(crate) fn validate_limit(limit: usize) -> AppResult<()> {
    if limit == 0 {
        return Err(AppError::validation("limit must be greater than zero"));
    }
    Ok(())
}

pub(crate) fn draft_not_found(draft_id: &str) -> AppError {
    AppError::not_found(format!("no such gamescript draft '{draft_id}'"))
}

pub(crate) fn revision_not_found(revision_id: &str) -> AppError {
    AppError::not_found(format!("no such gamescript revision '{revision_id}'"))
}

/// Stable audit action tokens.
pub(crate) const AUDIT_ACTION_SUBMIT: &str = "gamescript.draft.submit";
pub(crate) const AUDIT_ACTION_ACTIVATE: &str = "gamescript.activation.commit";
pub(crate) const AUDIT_ACTION_PIN: &str = "gamescript.revision.pin";
pub(crate) const AUDIT_ACTION_UNPIN: &str = "gamescript.revision.unpin";

/// Compose the (already redacted) audit details for one submission.
pub(crate) fn submit_audit_details(
    draft_id: &str,
    revision: &GameScriptRevision,
    deduplicated: bool,
    context: &GameScriptAuditContext,
) -> GameScriptAuditContext {
    let mut details = redact_gamescript_audit_details(context);
    details.insert("draft_id".to_owned(), draft_id.to_owned());
    details.insert("language".to_owned(), revision.language.as_str().to_owned());
    details.insert("entrypoint".to_owned(), revision.entrypoint.clone());
    details.insert("size_bytes".to_owned(), revision.size_bytes.to_string());
    details.insert("deduplicated".to_owned(), deduplicated.to_string());
    details
}

/// Compose the (already redacted) audit details for one activation.
pub(crate) fn activation_audit_details(
    activation: &GameScriptActivation,
    context: &GameScriptAuditContext,
) -> GameScriptAuditContext {
    let mut details = redact_gamescript_audit_details(context);
    details.insert("scope".to_owned(), activation.scope.clone());
    details.insert("generation".to_owned(), activation.generation.to_string());
    details.insert("revision_id".to_owned(), activation.revision_id.clone());
    details
}

// --- In-memory reference implementation --------------------------------------

/// All mutable state guarded together, so a state change and its audit/outbox
/// rows can never be observed half-committed — the in-memory equivalent of the
/// durable backends' single transaction.
#[derive(Debug, Default)]
struct State {
    drafts: BTreeMap<String, GameScriptDraft>,
    revisions: BTreeMap<String, GameScriptRevision>,
    pins: BTreeMap<String, (String, TimestampMillis)>,
    diagnostics: BTreeMap<String, Vec<GameScriptDiagnostic>>,
    generations: BTreeMap<String, u64>,
    activations: BTreeMap<(String, u64), GameScriptActivation>,
    audit: Vec<GameScriptAuditRecord>,
    outbox: BTreeMap<u64, GameScriptOutboxRecord>,
    next_audit_id: u64,
    next_outbox_id: u64,
}

impl State {
    fn push_audit(
        &mut self,
        actor: &str,
        action: &str,
        target: &str,
        details: GameScriptAuditContext,
        now: TimestampMillis,
    ) {
        self.next_audit_id += 1;
        self.audit.push(GameScriptAuditRecord {
            audit_id: self.next_audit_id,
            actor: actor.to_owned(),
            action: action.to_owned(),
            target: target.to_owned(),
            details,
            created_at: now,
        });
    }

    fn push_outbox(
        &mut self,
        kind: GameScriptOutboxKind,
        scope: Option<&str>,
        revision_id: &str,
        generation: Option<u64>,
        now: TimestampMillis,
    ) {
        self.next_outbox_id += 1;
        self.outbox.insert(
            self.next_outbox_id,
            GameScriptOutboxRecord {
                outbox_id: self.next_outbox_id,
                kind,
                scope: scope.map(str::to_owned),
                revision_id: revision_id.to_owned(),
                generation,
                created_at: now,
            },
        );
    }

    fn revision_is_protected(&self, revision_id: &str) -> bool {
        self.pins.contains_key(revision_id)
            || self
                .activations
                .values()
                .any(|activation| activation.revision_id == revision_id)
    }
}

/// Single-process, contract-faithful reference implementation.
#[derive(Default)]
pub struct InMemoryGameScriptRepository {
    limits: GameScriptLimits,
    state: Mutex<State>,
}

impl std::fmt::Debug for InMemoryGameScriptRepository {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InMemoryGameScriptRepository")
            .finish_non_exhaustive()
    }
}

impl InMemoryGameScriptRepository {
    /// Create an empty repository with the default (provisional) limits.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn guard(&self) -> AppResult<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| AppError::internal("gamescript repository mutex poisoned"))
    }
}

#[async_trait]
impl GameScriptRepository for InMemoryGameScriptRepository {
    async fn create_draft(
        &self,
        request: CreateGameScriptDraftRequest,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDraft> {
        validate_create_draft(&request, &self.limits)?;
        let mut state = self.guard()?;
        if state.drafts.contains_key(&request.draft_id) {
            return Err(AppError::conflict("gamescript draft already exists"));
        }
        let draft = GameScriptDraft {
            draft_id: request.draft_id.clone(),
            language: request.language,
            entrypoint: request.entrypoint,
            content: request.content,
            created_by: request.created_by,
            created_at: now,
            updated_at: now,
        };
        state.drafts.insert(request.draft_id, draft.clone());
        Ok(draft)
    }

    async fn update_draft(
        &self,
        draft_id: &str,
        update: UpdateGameScriptDraftRequest,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDraft> {
        validate_source(&update.entrypoint, &update.content, &self.limits)?;
        let mut state = self.guard()?;
        let draft = state
            .drafts
            .get_mut(draft_id)
            .ok_or_else(|| draft_not_found(draft_id))?;
        draft.language = update.language;
        draft.entrypoint = update.entrypoint;
        draft.content = update.content;
        draft.updated_at = now;
        Ok(draft.clone())
    }

    async fn get_draft(&self, draft_id: &str) -> AppResult<Option<GameScriptDraft>> {
        Ok(self.guard()?.drafts.get(draft_id).cloned())
    }

    async fn list_drafts(&self, limit: usize) -> AppResult<Vec<GameScriptDraft>> {
        validate_limit(limit)?;
        Ok(self.guard()?.drafts.values().take(limit).cloned().collect())
    }

    async fn delete_draft(&self, draft_id: &str) -> AppResult<bool> {
        Ok(self.guard()?.drafts.remove(draft_id).is_some())
    }

    async fn submit_draft(
        &self,
        draft_id: &str,
        actor: &str,
        context: &GameScriptAuditContext,
        now: TimestampMillis,
    ) -> AppResult<GameScriptSubmission> {
        if actor.is_empty() {
            return Err(AppError::validation("gamescript actor must not be empty"));
        }
        let mut state = self.guard()?;
        let draft = state
            .drafts
            .get(draft_id)
            .ok_or_else(|| draft_not_found(draft_id))?
            .clone();
        let revision_id =
            gamescript_revision_content_hash(draft.language, &draft.entrypoint, &draft.content);
        let (revision, deduplicated) = match state.revisions.get(&revision_id) {
            Some(existing) => (existing.clone(), true),
            None => {
                let revision = GameScriptRevision {
                    revision_id: revision_id.clone(),
                    language: draft.language,
                    entrypoint: draft.entrypoint.clone(),
                    content: draft.content.clone(),
                    size_bytes: draft.content.len() as u64,
                    created_by: actor.to_owned(),
                    created_at: now,
                };
                state
                    .revisions
                    .insert(revision_id.clone(), revision.clone());
                (revision, false)
            }
        };
        let details = submit_audit_details(draft_id, &revision, deduplicated, context);
        state.push_audit(actor, AUDIT_ACTION_SUBMIT, &revision_id, details, now);
        if !deduplicated {
            state.push_outbox(
                GameScriptOutboxKind::RevisionCreated,
                None,
                &revision_id,
                None,
                now,
            );
        }
        state.drafts.remove(draft_id);
        Ok(GameScriptSubmission {
            revision,
            deduplicated,
        })
    }

    async fn get_revision(&self, revision_id: &str) -> AppResult<Option<GameScriptRevision>> {
        Ok(self.guard()?.revisions.get(revision_id).cloned())
    }

    async fn list_revisions(&self, limit: usize) -> AppResult<Vec<GameScriptRevision>> {
        validate_limit(limit)?;
        let state = self.guard()?;
        let mut revisions: Vec<_> = state.revisions.values().cloned().collect();
        revisions.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.revision_id.cmp(&right.revision_id))
        });
        revisions.truncate(limit);
        Ok(revisions)
    }

    async fn append_diagnostic(
        &self,
        revision_id: &str,
        severity: GameScriptDiagnosticSeverity,
        source: &str,
        message: &str,
        now: TimestampMillis,
    ) -> AppResult<GameScriptDiagnostic> {
        let mut state = self.guard()?;
        if !state.revisions.contains_key(revision_id) {
            return Err(revision_not_found(revision_id));
        }
        let entries = state.diagnostics.entry(revision_id.to_owned()).or_default();
        let diagnostic = GameScriptDiagnostic {
            revision_id: revision_id.to_owned(),
            seq: entries.len() as u64 + 1,
            severity,
            source: source.to_owned(),
            message: message.to_owned(),
            created_at: now,
        };
        entries.push(diagnostic.clone());
        Ok(diagnostic)
    }

    async fn diagnostics(&self, revision_id: &str) -> AppResult<Vec<GameScriptDiagnostic>> {
        let state = self.guard()?;
        if !state.revisions.contains_key(revision_id) {
            return Err(revision_not_found(revision_id));
        }
        Ok(state
            .diagnostics
            .get(revision_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn pin_revision(
        &self,
        revision_id: &str,
        actor: &str,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let mut state = self.guard()?;
        if !state.revisions.contains_key(revision_id) {
            return Err(revision_not_found(revision_id));
        }
        if state.pins.contains_key(revision_id) {
            return Ok(false);
        }
        state
            .pins
            .insert(revision_id.to_owned(), (actor.to_owned(), now));
        state.push_audit(
            actor,
            AUDIT_ACTION_PIN,
            revision_id,
            GameScriptAuditContext::new(),
            now,
        );
        Ok(true)
    }

    async fn unpin_revision(
        &self,
        revision_id: &str,
        actor: &str,
        now: TimestampMillis,
    ) -> AppResult<bool> {
        let mut state = self.guard()?;
        if state.pins.remove(revision_id).is_none() {
            return Ok(false);
        }
        state.push_audit(
            actor,
            AUDIT_ACTION_UNPIN,
            revision_id,
            GameScriptAuditContext::new(),
            now,
        );
        Ok(true)
    }

    async fn allocate_activation_generation(
        &self,
        scope: &str,
        revision_id: &str,
        actor: &str,
        context: &GameScriptAuditContext,
        now: TimestampMillis,
    ) -> AppResult<GameScriptActivation> {
        if scope.is_empty() {
            return Err(AppError::validation(
                "gamescript activation scope must not be empty",
            ));
        }
        if actor.is_empty() {
            return Err(AppError::validation("gamescript actor must not be empty"));
        }
        let mut state = self.guard()?;
        if !state.revisions.contains_key(revision_id) {
            return Err(revision_not_found(revision_id));
        }
        let generation = state
            .generations
            .entry(scope.to_owned())
            .and_modify(|current| *current += 1)
            .or_insert(1);
        let activation = GameScriptActivation {
            scope: scope.to_owned(),
            generation: *generation,
            revision_id: revision_id.to_owned(),
            activated_by: actor.to_owned(),
            activated_at: now,
        };
        state.activations.insert(
            (scope.to_owned(), activation.generation),
            activation.clone(),
        );
        let details = activation_audit_details(&activation, context);
        state.push_audit(actor, AUDIT_ACTION_ACTIVATE, revision_id, details, now);
        state.push_outbox(
            GameScriptOutboxKind::ActivationCommitted,
            Some(scope),
            revision_id,
            Some(activation.generation),
            now,
        );
        Ok(activation)
    }

    async fn current_activation(&self, scope: &str) -> AppResult<Option<GameScriptActivation>> {
        let state = self.guard()?;
        Ok(state
            .activations
            .range((scope.to_owned(), 0)..=(scope.to_owned(), u64::MAX))
            .next_back()
            .map(|(_, activation)| activation.clone()))
    }

    async fn list_activations(
        &self,
        scope: &str,
        limit: usize,
    ) -> AppResult<Vec<GameScriptActivation>> {
        validate_limit(limit)?;
        let state = self.guard()?;
        Ok(state
            .activations
            .range((scope.to_owned(), 0)..=(scope.to_owned(), u64::MAX))
            .rev()
            .take(limit)
            .map(|(_, activation)| activation.clone())
            .collect())
    }

    async fn prune_drafts(
        &self,
        updated_before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        validate_limit(limit)?;
        let mut state = self.guard()?;
        let mut stale: Vec<(TimestampMillis, String)> = state
            .drafts
            .values()
            .filter(|draft| draft.updated_at < updated_before)
            .map(|draft| (draft.updated_at, draft.draft_id.clone()))
            .collect();
        stale.sort();
        stale.truncate(limit);
        for (_, draft_id) in &stale {
            state.drafts.remove(draft_id);
        }
        Ok(stale.len())
    }

    async fn prune_revisions(
        &self,
        created_before: TimestampMillis,
        limit: usize,
    ) -> AppResult<usize> {
        validate_limit(limit)?;
        let mut state = self.guard()?;
        let mut stale: Vec<(TimestampMillis, String)> = state
            .revisions
            .values()
            .filter(|revision| revision.created_at < created_before)
            .filter(|revision| !state.revision_is_protected(&revision.revision_id))
            .map(|revision| (revision.created_at, revision.revision_id.clone()))
            .collect();
        stale.sort();
        stale.truncate(limit);
        for (_, revision_id) in &stale {
            state.revisions.remove(revision_id);
            state.diagnostics.remove(revision_id);
        }
        Ok(stale.len())
    }

    async fn audit_log(&self, limit: usize) -> AppResult<Vec<GameScriptAuditRecord>> {
        validate_limit(limit)?;
        Ok(self
            .guard()?
            .audit
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect())
    }

    async fn pending_outbox(&self, limit: usize) -> AppResult<Vec<GameScriptOutboxRecord>> {
        validate_limit(limit)?;
        Ok(self.guard()?.outbox.values().take(limit).cloned().collect())
    }

    async fn acknowledge_outbox(&self, outbox_id: u64) -> AppResult<bool> {
        Ok(self.guard()?.outbox.remove(&outbox_id).is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_hash_is_stable_and_field_separated() {
        let hash = gamescript_revision_content_hash(RuntimeLanguage::Lua, "main.lua", "return 1");
        assert_eq!(hash.len(), 64, "lowercase hex sha-256");
        assert_eq!(
            hash,
            gamescript_revision_content_hash(RuntimeLanguage::Lua, "main.lua", "return 1"),
            "deterministic"
        );
        // Moving a boundary byte between fields must change the hash.
        assert_ne!(
            gamescript_revision_content_hash(RuntimeLanguage::Lua, "main.luar", "eturn 1"),
            hash
        );
        assert_ne!(
            gamescript_revision_content_hash(RuntimeLanguage::Js, "main.lua", "return 1"),
            hash
        );
    }

    #[test]
    fn audit_redaction_masks_secretish_keys_only() {
        let mut details = GameScriptAuditContext::new();
        details.insert("api_token".to_owned(), "raw".to_owned());
        details.insert("WEBHOOK_SECRET".to_owned(), "raw".to_owned());
        details.insert("sessionKey".to_owned(), "raw".to_owned());
        details.insert("reason".to_owned(), "deploy".to_owned());
        let redacted = redact_gamescript_audit_details(&details);
        assert_eq!(
            redacted.get("api_token").map(String::as_str),
            Some("[redacted]")
        );
        assert_eq!(
            redacted.get("WEBHOOK_SECRET").map(String::as_str),
            Some("[redacted]")
        );
        assert_eq!(
            redacted.get("sessionKey").map(String::as_str),
            Some("[redacted]")
        );
        assert_eq!(redacted.get("reason").map(String::as_str), Some("deploy"));
    }

    #[test]
    fn severity_and_outbox_tokens_round_trip() {
        for severity in [
            GameScriptDiagnosticSeverity::Info,
            GameScriptDiagnosticSeverity::Warning,
            GameScriptDiagnosticSeverity::Error,
        ] {
            assert_eq!(
                GameScriptDiagnosticSeverity::from_token(severity.as_str()).expect("round trip"),
                severity
            );
        }
        for kind in [
            GameScriptOutboxKind::RevisionCreated,
            GameScriptOutboxKind::ActivationCommitted,
        ] {
            assert_eq!(
                GameScriptOutboxKind::from_token(kind.as_str()).expect("round trip"),
                kind
            );
        }
        assert!(GameScriptDiagnosticSeverity::from_token("fatal").is_err());
        assert!(GameScriptOutboxKind::from_token("mystery").is_err());
    }
}
