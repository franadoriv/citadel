//! Deterministic, fail-closed text-content policy evaluation.
//!
//! Phase 1 matching is case-insensitive for ASCII letters only. It does not
//! normalize Unicode text.

use std::collections::{BTreeMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};
use thiserror::Error;

use crate::runtime::static_data::StaticDataCatalog;

/// A validated text policy.
#[derive(Debug, Clone)]
pub struct TextPolicy {
    rules: Vec<TextPolicyRule>,
    default_action: TextPolicyAction,
}

/// Supported per-rule and policy-default actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TextPolicyAction {
    Allow,
    Flag,
    Mask,
    Replace,
    Reject,
}

impl TextPolicyAction {
    /// The script-facing action label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Flag => "flag",
            Self::Mask => "mask",
            Self::Replace => "replace",
            Self::Reject => "reject",
        }
    }

    /// The decision associated with this action.
    pub const fn decision(self) -> TextPolicyDecision {
        match self {
            Self::Allow => TextPolicyDecision::Allow,
            Self::Flag => TextPolicyDecision::Flag,
            Self::Mask => TextPolicyDecision::Mask,
            Self::Replace => TextPolicyDecision::Replace,
            Self::Reject => TextPolicyDecision::Reject,
        }
    }
}

/// Aggregate decision from scanning text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TextPolicyDecision {
    Allow,
    Flag,
    Mask,
    Replace,
    Reject,
}

impl TextPolicyDecision {
    /// The script-facing aggregate-decision label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Flag => "flag",
            Self::Mask => "mask",
            Self::Replace => "replace",
            Self::Reject => "reject",
        }
    }
}

/// One matching rule and its byte span in the input text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextPolicyMatch {
    pub rule_id: String,
    pub category: String,
    pub severity: Option<String>,
    pub span: Range<usize>,
    pub action: TextPolicyAction,
}

/// The results of a policy scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextPolicyScan {
    pub matches: Vec<TextPolicyMatch>,
    pub decision: TextPolicyDecision,
}

/// The result of scanning and applying the applicable text transformations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextPolicySanitize {
    pub text: String,
    pub matches: Vec<TextPolicyMatch>,
    pub decision: TextPolicyDecision,
}

/// A policy parse or validation failure. All failures reject the policy.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TextPolicyError {
    #[error("policy is not valid JSON: {0}")]
    InvalidJson(String),
    #[error("policy must be a JSON object")]
    PolicyMustBeObject,
    #[error("policy contains unsupported field {0:?}")]
    UnsupportedPolicyField(String),
    #[error("policy schema_version must be the supported integer version")]
    InvalidSchemaVersion,
    #[error("unsupported policy schema_version {0}")]
    UnsupportedSchemaVersion(u64),
    #[error("policy rules must be a nonempty array")]
    MissingRules,
    #[error("rule {rule} must be a JSON object")]
    RuleMustBeObject { rule: usize },
    #[error("rule {rule} contains unsupported field {field:?}")]
    UnsupportedRuleField { rule: usize, field: String },
    #[error("rule {rule} has invalid or empty {field}")]
    InvalidRuleField { rule: usize, field: &'static str },
    #[error("rule {rule} has duplicate id {id:?}")]
    DuplicateRuleId { rule: usize, id: String },
    #[error("rule {rule} has unsupported match mode {mode:?}")]
    UnsupportedMatchMode { rule: usize, mode: String },
    #[error("rule {rule} has unsupported action {action:?}")]
    UnsupportedAction { rule: usize, action: String },
    #[error("policy has unsupported default action {0:?}")]
    UnsupportedDefaultAction(String),
    #[error("rule {rule} action replace requires a nonempty replacement")]
    ReplacementRequired { rule: usize },
}

#[derive(Debug, Default)]
struct TextPolicyCatalogState {
    entries: BTreeMap<String, Arc<TextPolicy>>,
    sealed: bool,
}

/// Rust-owned compiled-policy cache scoped to a [`StaticDataCatalog`].
///
/// Policies can be loaded only during script initialization. Once sealed, an
/// existing reference remains reusable while an unknown path is denied before
/// consulting the static-data catalog, so handler and tick paths cannot cause
/// filesystem I/O.
#[derive(Debug, Clone)]
pub(crate) struct TextPolicyCatalog {
    static_data: StaticDataCatalog,
    state: Arc<Mutex<TextPolicyCatalogState>>,
}

impl TextPolicyCatalog {
    pub(crate) fn new(static_data: StaticDataCatalog) -> Self {
        Self {
            static_data,
            state: Arc::new(Mutex::new(TextPolicyCatalogState::default())),
        }
    }

    /// Compile and cache a JSON policy, returning a reusable opaque reference.
    pub(crate) fn load_json(&self, relative_path: &str) -> Result<String, TextPolicyCatalogError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.entries.contains_key(relative_path) {
            return Ok(policy_reference(relative_path));
        }
        if state.sealed {
            return Err(TextPolicyCatalogError::NotLoaded(relative_path.to_owned()));
        }
        let value = self
            .static_data
            .load_json(relative_path)
            .map_err(TextPolicyCatalogError::StaticData)?;
        let policy = TextPolicy::from_json(&value.to_string())
            .map_err(TextPolicyCatalogError::InvalidPolicy)?;
        state
            .entries
            .insert(relative_path.to_owned(), Arc::new(policy));
        Ok(policy_reference(relative_path))
    }

    pub(crate) fn scan(
        &self,
        reference: &str,
        text: &str,
    ) -> Result<TextPolicyScan, TextPolicyCatalogError> {
        Ok(self.resolve(reference)?.scan(text))
    }

    pub(crate) fn sanitize(
        &self,
        reference: &str,
        text: &str,
    ) -> Result<TextPolicySanitize, TextPolicyCatalogError> {
        Ok(self.resolve(reference)?.sanitize(text))
    }

    pub(crate) fn scan_value(
        &self,
        reference: &str,
        text: &str,
    ) -> Result<Value, TextPolicyCatalogError> {
        let scan = self.scan(reference, text)?;
        Ok(policy_result_value(text, scan.decision, scan.matches))
    }

    pub(crate) fn sanitize_value(
        &self,
        reference: &str,
        text: &str,
    ) -> Result<Value, TextPolicyCatalogError> {
        let sanitized = self.sanitize(reference, text)?;
        Ok(policy_result_value(
            &sanitized.text,
            sanitized.decision,
            sanitized.matches,
        ))
    }

    pub(crate) fn seal(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .sealed = true;
    }

    fn resolve(&self, reference: &str) -> Result<Arc<TextPolicy>, TextPolicyCatalogError> {
        let key = reference
            .strip_prefix("text-policy:")
            .ok_or_else(|| TextPolicyCatalogError::UnknownReference(reference.to_owned()))?;
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .get(key)
            .cloned()
            .ok_or_else(|| TextPolicyCatalogError::UnknownReference(reference.to_owned()))
    }
}

fn policy_reference(relative_path: &str) -> String {
    format!("text-policy:{relative_path}")
}

fn policy_result_value(
    text: &str,
    decision: TextPolicyDecision,
    matches: Vec<TextPolicyMatch>,
) -> Value {
    let matches = matches
        .into_iter()
        .map(|matched| {
            json!({
                "rule_id": matched.rule_id,
                "category": matched.category,
                "severity": matched.severity,
                "span": { "start": matched.span.start, "end": matched.span.end },
                "action": matched.action.as_str(),
            })
        })
        .collect::<Vec<_>>();
    json!({ "decision": decision.as_str(), "matches": matches, "text": text })
}

/// Script-safe errors from the compiled policy catalog.
#[derive(Debug, Error)]
pub(crate) enum TextPolicyCatalogError {
    #[error("text policy static data error: {0}")]
    StaticData(#[source] crate::runtime::static_data::StaticDataError),
    #[error("text policy is invalid: {0}")]
    InvalidPolicy(#[source] TextPolicyError),
    #[error("text policy was not loaded during script initialization: {0}")]
    NotLoaded(String),
    #[error("unknown text policy reference")]
    UnknownReference(String),
}

impl TextPolicy {
    /// Parses and validates a schema-version 1 policy, rejecting invalid input.
    pub fn from_json(json: &str) -> Result<Self, TextPolicyError> {
        let value: Value = serde_json::from_str(json)
            .map_err(|error| TextPolicyError::InvalidJson(error.to_string()))?;
        let object = value
            .as_object()
            .ok_or(TextPolicyError::PolicyMustBeObject)?;
        reject_unknown_policy_fields(object)?;

        let schema_version = object
            .get("schema_version")
            .and_then(Value::as_u64)
            .ok_or(TextPolicyError::InvalidSchemaVersion)?;
        if schema_version != 1 {
            return Err(TextPolicyError::UnsupportedSchemaVersion(schema_version));
        }

        let default_action = match object.get("default_action") {
            Some(value) => parse_action(value, None)?,
            None => TextPolicyAction::Allow,
        };
        let rules = object
            .get("rules")
            .and_then(Value::as_array)
            .filter(|rules| !rules.is_empty())
            .ok_or(TextPolicyError::MissingRules)?;

        let mut ids = HashSet::new();
        let mut parsed_rules = Vec::with_capacity(rules.len());
        for (rule_index, value) in rules.iter().enumerate() {
            let rule = parse_rule(value, rule_index, default_action)?;
            if !ids.insert(rule.id.clone()) {
                return Err(TextPolicyError::DuplicateRuleId {
                    rule: rule_index,
                    id: rule.id,
                });
            }
            parsed_rules.push(rule);
        }

        Ok(Self {
            rules: parsed_rules,
            default_action,
        })
    }

    /// Scans text in policy-rule and term order using ASCII-only case folding.
    pub fn scan(&self, text: &str) -> TextPolicyScan {
        let folded_text = text.to_ascii_lowercase();
        let mut matches = Vec::new();
        let mut decision = self.default_action.decision();

        for rule in &self.rules {
            for term in &rule.terms {
                let folded_term = term.to_ascii_lowercase();
                for span in find_spans(&folded_text, &folded_term, rule.match_mode) {
                    decision = decision.max(rule.action.decision());
                    matches.push(TextPolicyMatch {
                        rule_id: rule.id.clone(),
                        category: rule.category.clone(),
                        severity: rule.severity.clone(),
                        span,
                        action: rule.action,
                    });
                }
            }
        }

        matches.sort_by(|left, right| {
            left.span
                .start
                .cmp(&right.span.start)
                .then(left.span.end.cmp(&right.span.end))
        });
        TextPolicyScan { matches, decision }
    }

    /// Applies mask and replace actions left-to-right. Overlapping matches use
    /// the earliest span, then the shortest span, as the deterministic winner.
    pub fn sanitize(&self, text: &str) -> TextPolicySanitize {
        let scan = self.scan(text);
        let mut output = String::with_capacity(text.len());
        let mut cursor = 0;

        for matched in &scan.matches {
            if !matches!(
                matched.action,
                TextPolicyAction::Mask | TextPolicyAction::Replace
            ) {
                continue;
            }
            if matched.span.start < cursor {
                continue;
            }
            output.push_str(&text[cursor..matched.span.start]);
            match matched.action {
                TextPolicyAction::Mask => {
                    output.extend(std::iter::repeat_n(
                        '*',
                        text[matched.span.clone()].chars().count(),
                    ));
                }
                TextPolicyAction::Replace => {
                    if let Some(replacement) = self.replacement_for(&matched.rule_id) {
                        output.push_str(replacement);
                    }
                }
                TextPolicyAction::Allow | TextPolicyAction::Flag | TextPolicyAction::Reject => {
                    unreachable!("non-transforming actions are skipped before sanitization")
                }
            }
            cursor = matched.span.end;
        }
        output.push_str(&text[cursor..]);

        TextPolicySanitize {
            text: output,
            matches: scan.matches,
            decision: scan.decision,
        }
    }

    fn replacement_for(&self, rule_id: &str) -> Option<&str> {
        self.rules
            .iter()
            .find(|rule| rule.id == rule_id)
            .and_then(|rule| rule.replacement.as_deref())
    }
}

#[derive(Debug, Clone)]
struct TextPolicyRule {
    id: String,
    category: String,
    severity: Option<String>,
    terms: Vec<String>,
    match_mode: TextPolicyMatchMode,
    action: TextPolicyAction,
    replacement: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum TextPolicyMatchMode {
    WholeWord,
    Phrase,
}

fn reject_unknown_policy_fields(object: &Map<String, Value>) -> Result<(), TextPolicyError> {
    for field in object.keys() {
        if !matches!(
            field.as_str(),
            "schema_version" | "rules" | "default_action"
        ) {
            return Err(TextPolicyError::UnsupportedPolicyField(field.clone()));
        }
    }
    Ok(())
}

fn parse_rule(
    value: &Value,
    rule_index: usize,
    default_action: TextPolicyAction,
) -> Result<TextPolicyRule, TextPolicyError> {
    let object = value
        .as_object()
        .ok_or(TextPolicyError::RuleMustBeObject { rule: rule_index })?;
    for field in object.keys() {
        if !matches!(
            field.as_str(),
            "id" | "category"
                | "severity"
                | "terms"
                | "match"
                | "match_mode"
                | "action"
                | "replacement"
        ) {
            return Err(TextPolicyError::UnsupportedRuleField {
                rule: rule_index,
                field: field.clone(),
            });
        }
    }

    let id = required_string(object, rule_index, "id")?;
    let category = required_string(object, rule_index, "category")?;
    let severity = optional_string(object, rule_index, "severity")?;
    let terms = object
        .get("terms")
        .and_then(Value::as_array)
        .filter(|terms| !terms.is_empty())
        .ok_or(TextPolicyError::InvalidRuleField {
            rule: rule_index,
            field: "terms",
        })?
        .iter()
        .map(|term| {
            term.as_str()
                .filter(|term| !term.trim().is_empty())
                .map(str::to_owned)
                .ok_or(TextPolicyError::InvalidRuleField {
                    rule: rule_index,
                    field: "terms",
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let match_value = match (object.get("match"), object.get("match_mode")) {
        (Some(_), Some(_)) => {
            return Err(TextPolicyError::InvalidRuleField {
                rule: rule_index,
                field: "match",
            });
        }
        (Some(value), None) | (None, Some(value)) => value,
        (None, None) => {
            return Err(TextPolicyError::InvalidRuleField {
                rule: rule_index,
                field: "match",
            });
        }
    };
    let match_mode = match match_value.as_str() {
        Some("whole_word") => TextPolicyMatchMode::WholeWord,
        Some("phrase") => TextPolicyMatchMode::Phrase,
        Some(mode) => {
            return Err(TextPolicyError::UnsupportedMatchMode {
                rule: rule_index,
                mode: mode.to_owned(),
            });
        }
        None => {
            return Err(TextPolicyError::InvalidRuleField {
                rule: rule_index,
                field: "match",
            });
        }
    };

    let action = match object.get("action") {
        Some(value) => parse_action(value, Some(rule_index))?,
        None => default_action,
    };
    let replacement = optional_string(object, rule_index, "replacement")?;
    if action == TextPolicyAction::Replace && replacement.is_none() {
        return Err(TextPolicyError::ReplacementRequired { rule: rule_index });
    }

    Ok(TextPolicyRule {
        id,
        category,
        severity,
        terms,
        match_mode,
        action,
        replacement,
    })
}

fn required_string(
    object: &Map<String, Value>,
    rule: usize,
    field: &'static str,
) -> Result<String, TextPolicyError> {
    optional_string(object, rule, field)?.ok_or(TextPolicyError::InvalidRuleField { rule, field })
}

fn optional_string(
    object: &Map<String, Value>,
    rule: usize,
    field: &'static str,
) -> Result<Option<String>, TextPolicyError> {
    match object.get(field) {
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| Some(value.to_owned()))
            .ok_or(TextPolicyError::InvalidRuleField { rule, field }),
        None => Ok(None),
    }
}

fn parse_action(value: &Value, rule: Option<usize>) -> Result<TextPolicyAction, TextPolicyError> {
    let action = value.as_str().ok_or_else(|| match rule {
        Some(rule) => TextPolicyError::InvalidRuleField {
            rule,
            field: "action",
        },
        None => TextPolicyError::UnsupportedDefaultAction(value.to_string()),
    })?;
    match action {
        "allow" => Ok(TextPolicyAction::Allow),
        "mask" => Ok(TextPolicyAction::Mask),
        "replace" => Ok(TextPolicyAction::Replace),
        "reject" => Ok(TextPolicyAction::Reject),
        "flag" => Ok(TextPolicyAction::Flag),
        _ => match rule {
            Some(rule) => Err(TextPolicyError::UnsupportedAction {
                rule,
                action: action.to_owned(),
            }),
            None => Err(TextPolicyError::UnsupportedDefaultAction(action.to_owned())),
        },
    }
}

fn find_spans(text: &str, term: &str, mode: TextPolicyMatchMode) -> Vec<Range<usize>> {
    let mut spans = Vec::new();
    let mut search_from = 0;
    while let Some(offset) = text[search_from..].find(term) {
        let start = search_from + offset;
        let end = start + term.len();
        let is_match = match mode {
            TextPolicyMatchMode::Phrase => true,
            TextPolicyMatchMode::WholeWord => is_whole_word(text, start, end),
        };
        if is_match {
            spans.push(start..end);
        }
        search_from = end;
    }
    spans
}

fn is_whole_word(text: &str, start: usize, end: usize) -> bool {
    let before_is_word = text[..start]
        .chars()
        .next_back()
        .is_some_and(is_ascii_word_char);
    let after_is_word = text[end..].chars().next().is_some_and(is_ascii_word_char);
    !before_is_word && !after_is_word
}

fn is_ascii_word_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

#[cfg(test)]
mod tests {
    use super::{TextPolicy, TextPolicyAction, TextPolicyDecision};

    #[test]
    fn scan_and_sanitize_apply_case_insensitive_whole_word_masking_to_unicode_scalars() {
        let policy = TextPolicy::from_json(
            r#"{
                "schema_version": 1,
                "rules": [{
                    "id": "slur-1",
                    "category": "abuse",
                    "severity": "high",
                    "terms": ["bad"],
                    "match": "whole_word",
                    "action": "mask"
                }]
            }"#,
        )
        .expect("valid policy");

        let scan = policy.scan("😺 BAD actor");
        assert_eq!(scan.decision, TextPolicyDecision::Mask);
        assert_eq!(scan.matches.len(), 1);
        assert_eq!(scan.matches[0].rule_id, "slur-1");
        assert_eq!(scan.matches[0].category, "abuse");
        assert_eq!(scan.matches[0].severity.as_deref(), Some("high"));
        assert_eq!(scan.matches[0].span, 5..8);

        let sanitized = policy.sanitize("😺 BAD actor");
        assert_eq!(sanitized.decision, TextPolicyDecision::Mask);
        assert_eq!(sanitized.text, "😺 *** actor");
        assert_eq!(sanitized.matches.len(), 1);
        assert_eq!(TextPolicyAction::Mask.decision(), TextPolicyDecision::Mask);
    }

    #[test]
    fn phrase_replacement_is_deterministic_and_whole_word_does_not_match_inside_word() {
        let policy = TextPolicy::from_json(
            r#"{
                "schema_version": 1,
                "rules": [
                    {
                        "id": "word",
                        "category": "abuse",
                        "terms": ["bad"],
                        "match_mode": "whole_word",
                        "action": "mask"
                    },
                    {
                        "id": "phrase",
                        "category": "privacy",
                        "terms": ["top secret"],
                        "match": "phrase",
                        "action": "replace",
                        "replacement": "[redacted]"
                    }
                ]
            }"#,
        )
        .expect("valid policy");

        let scan = policy.scan("badly bad: TOP SECRET");
        assert_eq!(scan.matches.len(), 2);
        assert_eq!(scan.matches[0].rule_id, "word");
        assert_eq!(scan.matches[0].span, 6..9);
        assert_eq!(scan.matches[1].rule_id, "phrase");
        assert_eq!(scan.matches[1].span, 11..21);
        assert_eq!(scan.decision, TextPolicyDecision::Replace);
        assert_eq!(
            policy.sanitize("badly bad: TOP SECRET").text,
            "badly ***: [redacted]"
        );
    }

    #[test]
    fn malformed_and_unsupported_policies_return_typed_errors() {
        use super::TextPolicyError;

        assert!(matches!(
            TextPolicy::from_json("[]"),
            Err(TextPolicyError::PolicyMustBeObject)
        ));
        assert!(matches!(
            TextPolicy::from_json(r#"{"schema_version": 2, "rules": []}"#),
            Err(TextPolicyError::UnsupportedSchemaVersion(2))
        ));
        assert!(matches!(
            TextPolicy::from_json(r#"{"schema_version": 1, "rules": []}"#),
            Err(TextPolicyError::MissingRules)
        ));
        assert!(matches!(
            TextPolicy::from_json(
                r#"{"schema_version": 1, "rules": [{"id":"r", "category":"c", "terms":["x"], "match":"regex", "action":"mask"}]}"#
            ),
            Err(TextPolicyError::UnsupportedMatchMode { .. })
        ));
        assert!(matches!(
            TextPolicy::from_json(
                r#"{"schema_version": 1, "rules": [{"id":"r", "category":"c", "terms":["x"], "match":"phrase", "action":"replace"}]}"#
            ),
            Err(TextPolicyError::ReplacementRequired { .. })
        ));
    }

    #[test]
    fn sanitize_does_not_allow_a_non_transforming_overlap_to_suppress_a_mask() {
        let policy = TextPolicy::from_json(
            r#"{
                "schema_version": 1,
                "rules": [
                    {"id":"allow-first","category":"exception","terms":["bad"],"match":"phrase","action":"allow"},
                    {"id":"mask-second","category":"abuse","terms":["bad"],"match":"phrase","action":"mask"}
                ]
            }"#,
        )
        .expect("valid policy");

        let sanitized = policy.sanitize("bad");
        assert_eq!(sanitized.decision, TextPolicyDecision::Mask);
        assert_eq!(sanitized.text, "***");
    }
}
