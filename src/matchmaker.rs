//! Deterministic, local ticket matchmaking.
//!
//! This module owns the queue semantics, not realtime delivery. The gateway
//! supplies connected participants and turns a formed [`Match`] into a
//! server-owned room. Keeping those boundaries separate makes the index
//! testable without sockets and leaves room for a future leader/shard adapter.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::Instant;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::realtime::ParticipantId;
use crate::time::TimestampMillis;

/// Opaque ticket handle returned to a client. It is deliberately not a match id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TicketId(String);

impl TicketId {
    /// Parse a ticket handle returned by the server for a later cancel/status
    /// request. This performs shape validation only; ownership is checked by the
    /// matchmaker before the handle has any effect.
    pub fn parse(value: impl Into<String>) -> Result<Self, MatchmakerError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(MatchmakerError::Validation("invalid ticket_id".to_owned()));
        }
        Ok(Self(value))
    }

    /// The opaque value for a wire response.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One value exposed to a ticket query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PropertyValue {
    /// An exact string property.
    String(String),
    /// An ordered finite numeric property.
    Number(f64),
}

/// Input accepted when a participant enters the queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TicketRequest {
    /// Query evaluated against every other candidate's properties. Empty means
    /// every candidate is acceptable.
    #[serde(default)]
    pub query: String,
    /// Game-defined public matching properties.
    #[serde(default)]
    pub properties: BTreeMap<String, PropertyValue>,
    /// Smallest cohort the player accepts; must be at least two.
    pub min_count: u8,
    /// Largest cohort the player accepts.
    pub max_count: u8,
    /// Final cohort size must be a multiple of this value (one by default).
    #[serde(default = "one")]
    pub count_multiple: u8,
    /// Optional opaque party id. The gateway resolves it to a leader-authorized,
    /// indivisible member list before entering the local index.
    #[serde(default)]
    pub party_id: Option<String>,
    /// Positive time-to-live in milliseconds.
    pub ttl_ms: u64,
}

const fn one() -> u8 {
    1
}

/// A queued ticket visible to the evaluator and status endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct Ticket {
    /// Opaque client handle.
    pub id: TicketId,
    /// Local realtime participant that owns the ticket.
    pub owner: ParticipantId,
    /// All connected members represented by this ticket. A party is one ticket
    /// but occupies this many final cohort slots and is never split.
    pub members: Vec<ParticipantId>,
    /// Parsed, typed predicate; never reparsed during evaluation.
    pub query: MatchQuery,
    /// Properties compared by other tickets.
    pub properties: BTreeMap<String, PropertyValue>,
    /// Inclusive size bounds.
    pub min_count: u8,
    /// Inclusive size bounds.
    pub max_count: u8,
    /// Required final-size multiple.
    pub count_multiple: u8,
    /// Stable fairness sequence, strictly increasing while process lives.
    pub sequence: u64,
    /// Queue entry time.
    pub created_at: TimestampMillis,
    /// Queue expiry time.
    pub expires_at: TimestampMillis,
}

/// A cohort atomically removed from the queue by one evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    /// Tickets in deterministic age order.
    pub tickets: Vec<TicketId>,
    /// Participants represented by each entry in [`Self::tickets`], in the same
    /// deterministic ticket order.
    pub ticket_members: Vec<Vec<ParticipantId>>,
    /// Connected participants that must enter the same match.
    pub participants: Vec<ParticipantId>,
}

/// Observable state of a known ticket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TicketState {
    /// Still eligible for a future cohort.
    Queued,
    /// Formed and removed from the queue.
    Matched,
    /// Removed by its owner or expiry.
    Removed,
}

/// Non-sensitive local queue telemetry. The snapshot intentionally contains no
/// ticket id, player identity, query, or property value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct MatchmakerStats {
    /// Tickets currently eligible for matching.
    pub queued_tickets: u64,
    /// Completed evaluator passes since process start.
    pub evaluations_total: u64,
    /// Cumulative wall-clock evaluator time in microseconds.
    pub evaluation_duration_us_total: u64,
    /// Cohorts formed since process start.
    pub matches_formed_total: u64,
    /// Tickets consumed by formed cohorts since process start.
    pub tickets_formed_total: u64,
    /// Tickets explicitly cancelled, including disconnect cleanup.
    pub tickets_cancelled_total: u64,
    /// Tickets removed after their TTL elapsed.
    pub tickets_expired_total: u64,
}

/// A parsed predicate over one candidate's property map.
#[derive(Debug, Clone, PartialEq)]
pub enum MatchQuery {
    /// No predicate: every candidate satisfies it.
    Any,
    /// Both predicates must match.
    And(Box<Self>, Box<Self>),
    /// Either predicate may match.
    Or(Box<Self>, Box<Self>),
    /// One property comparison.
    Compare {
        /// Property name.
        field: String,
        /// Comparison operation.
        op: Comparison,
        /// Literal in the original ticket query.
        value: PropertyValue,
    },
}

/// Supported typed comparison operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    /// Equal to.
    Equal,
    /// Not equal to.
    NotEqual,
    /// Numeric lower-than comparison.
    Less,
    /// Numeric lower-than-or-equal comparison.
    LessOrEqual,
    /// Numeric greater-than comparison.
    Greater,
    /// Numeric greater-than-or-equal comparison.
    GreaterOrEqual,
}

/// Validation or parsing failure at the queue boundary.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MatchmakerError {
    /// A count or TTL is invalid.
    #[error("{0}")]
    Validation(String),
    /// The query cannot be parsed.
    #[error("invalid match query: {0}")]
    Query(String),
    /// OS entropy could not mint an opaque ticket id.
    #[error("could not generate ticket id")]
    Entropy,
}

#[derive(Debug, Default)]
struct Inner {
    next_sequence: u64,
    queued: BTreeMap<u64, Ticket>,
    owner_ticket: HashMap<ParticipantId, TicketId>,
    states: HashMap<TicketId, TicketState>,
    stats: MatchmakerStats,
}

/// Interior-mutable local index. Every mutation evaluates/removes tickets under
/// one lock, preventing a ticket from entering two formed cohorts.
#[derive(Debug, Default)]
pub struct Matchmaker {
    inner: Mutex<Inner>,
}

impl Matchmaker {
    /// Construct an empty local matchmaker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate and enqueue a ticket. A participant can own at most one queued
    /// ticket; submitting again replaces nothing and returns a validation error.
    pub fn add(
        &self,
        owner: ParticipantId,
        request: TicketRequest,
        now: TimestampMillis,
    ) -> Result<TicketId, MatchmakerError> {
        self.add_party(owner, vec![owner], request, now)
    }

    /// Validate and enqueue an indivisible party ticket. `owner` is the party
    /// leader and every member is made ineligible for another queued ticket.
    pub fn add_party(
        &self,
        owner: ParticipantId,
        mut members: Vec<ParticipantId>,
        request: TicketRequest,
        now: TimestampMillis,
    ) -> Result<TicketId, MatchmakerError> {
        validate_request(&request)?;
        members.sort_unstable();
        members.dedup();
        if members.is_empty() || !members.contains(&owner) {
            return Err(MatchmakerError::Validation(
                "party ticket must contain its owner".to_owned(),
            ));
        }
        let query = MatchQuery::parse(&request.query)?;
        let expires_at = now
            .unix_millis()
            .checked_add(request.ttl_ms)
            .map(TimestampMillis::from_unix_millis)
            .ok_or_else(|| MatchmakerError::Validation("ttl_ms overflows timestamp".to_owned()))?;
        let id = fresh_ticket_id()?;
        let mut inner = self.lock();
        self.expire_locked(&mut inner, now);
        if members
            .iter()
            .any(|member| inner.owner_ticket.contains_key(member))
        {
            return Err(MatchmakerError::Validation(
                "party member already has a queued ticket".to_owned(),
            ));
        }
        let sequence = inner.next_sequence;
        inner.next_sequence = inner.next_sequence.saturating_add(1);
        inner.queued.insert(
            sequence,
            Ticket {
                id: id.clone(),
                owner,
                members: members.clone(),
                query,
                properties: request.properties,
                min_count: request.min_count,
                max_count: request.max_count,
                count_multiple: request.count_multiple,
                sequence,
                created_at: now,
                expires_at,
            },
        );
        for member in members {
            inner.owner_ticket.insert(member, id.clone());
        }
        inner.states.insert(id.clone(), TicketState::Queued);
        Ok(id)
    }

    /// Cancel a ticket when `owner` owns it. It is idempotent for a ticket that
    /// has already been removed, and never lets one participant cancel another's.
    pub fn cancel(&self, owner: ParticipantId, id: &TicketId, now: TimestampMillis) -> bool {
        let mut inner = self.lock();
        self.expire_locked(&mut inner, now);
        if inner.owner_ticket.get(&owner) != Some(id) {
            return false;
        }
        let removed = self.remove_ticket_locked(&mut inner, id, TicketState::Removed);
        if removed {
            inner.stats.tickets_cancelled_total =
                inner.stats.tickets_cancelled_total.saturating_add(1);
        }
        removed
    }

    /// Remove the one queued ticket for a disconnected participant, if present.
    /// This is deliberately idempotent because transport teardown may race with
    /// an explicit cancellation request.
    pub fn cancel_owner(&self, owner: ParticipantId, now: TimestampMillis) -> bool {
        let mut inner = self.lock();
        self.expire_locked(&mut inner, now);
        let Some(id) = inner.owner_ticket.get(&owner).cloned() else {
            return false;
        };
        let removed = self.remove_ticket_locked(&mut inner, &id, TicketState::Removed);
        if removed {
            inner.stats.tickets_cancelled_total =
                inner.stats.tickets_cancelled_total.saturating_add(1);
        }
        removed
    }

    /// Read a ticket state without exposing other players' properties.
    #[must_use]
    pub fn state(
        &self,
        owner: ParticipantId,
        id: &TicketId,
        now: TimestampMillis,
    ) -> Option<TicketState> {
        let mut inner = self.lock();
        self.expire_locked(&mut inner, now);
        // While a ticket is queued the owner must currently own it, so a live
        // ticket's state never leaks to another participant. Once it reaches a
        // terminal state (matched, or removed by cancellation/expiry) the
        // owner->ticket mapping is cleared so the participant can queue again;
        // the terminal state stays inspectable to a caller holding the ticket id.
        if inner.owner_ticket.get(&owner) == Some(id) {
            return inner.states.get(id).cloned();
        }
        match inner.states.get(id) {
            Some(state) if matches!(state, TicketState::Matched | TicketState::Removed) => {
                Some(state.clone())
            }
            _ => None,
        }
    }

    /// Remove expired tickets and form zero or more oldest-first cohorts.
    pub fn evaluate(&self, now: TimestampMillis) -> Vec<Match> {
        let started = Instant::now();
        let matches = self.preview(now);
        let committed = self.commit_formations(&matches, now);
        let matches = if committed { matches } else { Vec::new() };
        self.record_evaluation(&matches, started.elapsed());
        matches
    }

    /// Preview the oldest-first cohorts that could form at `now` without
    /// removing their tickets. A distributed shard owner uses this before its
    /// durable formation claim, then calls [`Self::commit_formations`] only
    /// after that fenced claim succeeds.
    #[must_use]
    pub fn preview(&self, now: TimestampMillis) -> Vec<Match> {
        let mut inner = self.lock();
        self.expire_locked(&mut inner, now);
        let ordered: Vec<Ticket> = inner.queued.values().cloned().collect();
        let mut consumed = Vec::new();
        let mut matches = Vec::new();
        for seed in &ordered {
            if consumed.contains(&seed.id) {
                continue;
            }
            let mut cohort = vec![seed.clone()];
            for candidate in &ordered {
                if candidate.id == seed.id || consumed.contains(&candidate.id) {
                    continue;
                }
                if cohort
                    .iter()
                    .all(|member| mutually_compatible(member, candidate))
                {
                    cohort.push(candidate.clone());
                    if valid_cohort(&cohort) {
                        break;
                    }
                }
            }
            if valid_cohort(&cohort) {
                consumed.extend(cohort.iter().map(|ticket| ticket.id.clone()));
                matches.push(Match {
                    tickets: cohort.iter().map(|ticket| ticket.id.clone()).collect(),
                    ticket_members: cohort.iter().map(|ticket| ticket.members.clone()).collect(),
                    participants: cohort
                        .iter()
                        .flat_map(|ticket| ticket.members.iter().copied())
                        .collect(),
                });
            }
        }
        matches
    }

    /// Commit previously previewed cohorts. The call fails closed if a ticket
    /// was cancelled, expired, or otherwise changed after the preview; callers
    /// must then discard the external formation claim rather than silently
    /// admitting a partial cohort.
    pub fn commit_formations(&self, matches: &[Match], now: TimestampMillis) -> bool {
        let mut inner = self.lock();
        self.expire_locked(&mut inner, now);
        let mut tickets = HashSet::new();
        for matching in matches {
            for ticket in &matching.tickets {
                if !tickets.insert(ticket.clone())
                    || !inner.queued.values().any(|queued| queued.id == *ticket)
                {
                    return false;
                }
            }
        }
        for ticket in tickets {
            let _ = self.remove_ticket_locked(&mut inner, &ticket, TicketState::Matched);
        }
        true
    }

    fn record_evaluation(&self, matches: &[Match], elapsed: std::time::Duration) {
        let formed_tickets = matches
            .iter()
            .map(|matching| matching.tickets.len())
            .sum::<usize>();
        let formed_matches = matches.len() as u64;
        let mut inner = self.lock();
        inner.stats.evaluations_total = inner.stats.evaluations_total.saturating_add(1);
        inner.stats.evaluation_duration_us_total = inner
            .stats
            .evaluation_duration_us_total
            .saturating_add(u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX));
        inner.stats.matches_formed_total = inner
            .stats
            .matches_formed_total
            .saturating_add(formed_matches);
        inner.stats.tickets_formed_total = inner
            .stats
            .tickets_formed_total
            .saturating_add(formed_tickets as u64);
    }

    /// Snapshot queue telemetry without exposing ticket contents.
    #[must_use]
    pub fn stats(&self) -> MatchmakerStats {
        let inner = self.lock();
        MatchmakerStats {
            queued_tickets: inner.queued.len() as u64,
            ..inner.stats
        }
    }

    /// Number of queued tickets after removing expired entries.
    pub fn queued_len(&self, now: TimestampMillis) -> usize {
        let mut inner = self.lock();
        self.expire_locked(&mut inner, now);
        inner.queued.len()
    }

    /// Whether this participant is represented by a queued ticket. Party
    /// management uses this to freeze membership while a ticket is eligible, so
    /// a formed cohort cannot observe a partial party after a late mutation.
    pub fn has_queued_ticket(&self, owner: ParticipantId, now: TimestampMillis) -> bool {
        let mut inner = self.lock();
        self.expire_locked(&mut inner, now);
        inner.owner_ticket.contains_key(&owner)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn expire_locked(&self, inner: &mut Inner, now: TimestampMillis) {
        let expired: Vec<TicketId> = inner
            .queued
            .values()
            .filter(|ticket| ticket.expires_at <= now)
            .map(|ticket| ticket.id.clone())
            .collect();
        for id in expired {
            if self.remove_ticket_locked(inner, &id, TicketState::Removed) {
                inner.stats.tickets_expired_total =
                    inner.stats.tickets_expired_total.saturating_add(1);
            }
        }
    }

    fn remove_ticket_locked(&self, inner: &mut Inner, id: &TicketId, state: TicketState) -> bool {
        let Some(sequence) = inner
            .queued
            .iter()
            .find_map(|(sequence, ticket)| (ticket.id == *id).then_some(*sequence))
        else {
            return false;
        };
        if let Some(ticket) = inner.queued.remove(&sequence) {
            for member in ticket.members {
                inner.owner_ticket.remove(&member);
            }
            inner.states.insert(ticket.id, state);
            true
        } else {
            false
        }
    }
}

fn validate_request(request: &TicketRequest) -> Result<(), MatchmakerError> {
    if request.min_count < 2 {
        return Err(MatchmakerError::Validation(
            "min_count must be at least 2".to_owned(),
        ));
    }
    if request.max_count < request.min_count {
        return Err(MatchmakerError::Validation(
            "max_count must be greater than or equal to min_count".to_owned(),
        ));
    }
    if request.count_multiple == 0 {
        return Err(MatchmakerError::Validation(
            "count_multiple must be greater than zero".to_owned(),
        ));
    }
    if request.ttl_ms == 0 {
        return Err(MatchmakerError::Validation(
            "ttl_ms must be greater than zero".to_owned(),
        ));
    }
    for (key, value) in &request.properties {
        if key.is_empty()
            || key.len() > 64
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.')
        {
            return Err(MatchmakerError::Validation(format!(
                "invalid property name: {key}"
            )));
        }
        if let PropertyValue::Number(number) = value
            && !number.is_finite()
        {
            return Err(MatchmakerError::Validation(format!(
                "property {key} must be finite"
            )));
        }
    }
    Ok(())
}

fn fresh_ticket_id() -> Result<TicketId, MatchmakerError> {
    let mut bytes = [0_u8; 18];
    getrandom::fill(&mut bytes).map_err(|_| MatchmakerError::Entropy)?;
    Ok(TicketId(
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes),
    ))
}

fn mutually_compatible(left: &Ticket, right: &Ticket) -> bool {
    left.query.matches(&right.properties) && right.query.matches(&left.properties)
}

fn valid_cohort(cohort: &[Ticket]) -> bool {
    let count: usize = cohort.iter().map(|ticket| ticket.members.len()).sum();
    count >= 2
        && cohort.iter().all(|ticket| {
            count >= usize::from(ticket.min_count)
                && count <= usize::from(ticket.max_count)
                && count.is_multiple_of(usize::from(ticket.count_multiple))
        })
}

impl MatchQuery {
    /// Parse the deliberately-small match expression grammar.
    pub fn parse(source: &str) -> Result<Self, MatchmakerError> {
        if source.trim().is_empty() {
            return Ok(Self::Any);
        }
        Parser::new(source)?.parse()
    }

    /// Evaluate this predicate against a single candidate's properties.
    #[must_use]
    pub fn matches(&self, properties: &BTreeMap<String, PropertyValue>) -> bool {
        match self {
            Self::Any => true,
            Self::And(left, right) => left.matches(properties) && right.matches(properties),
            Self::Or(left, right) => left.matches(properties) || right.matches(properties),
            Self::Compare { field, op, value } => properties
                .get(field)
                .is_some_and(|candidate| compare(candidate, *op, value)),
        }
    }
}

fn compare(candidate: &PropertyValue, op: Comparison, wanted: &PropertyValue) -> bool {
    match (candidate, wanted) {
        (PropertyValue::String(candidate), PropertyValue::String(wanted)) => match op {
            Comparison::Equal => candidate == wanted,
            Comparison::NotEqual => candidate != wanted,
            _ => false,
        },
        (PropertyValue::Number(candidate), PropertyValue::Number(wanted)) => match op {
            Comparison::Equal => candidate == wanted,
            Comparison::NotEqual => candidate != wanted,
            Comparison::Less => candidate < wanted,
            Comparison::LessOrEqual => candidate <= wanted,
            Comparison::Greater => candidate > wanted,
            Comparison::GreaterOrEqual => candidate >= wanted,
        },
        _ => false,
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    String(String),
    Number(f64),
    And,
    Or,
    LeftParen,
    RightParen,
    Op(Comparison),
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn new(source: &str) -> Result<Self, MatchmakerError> {
        Ok(Self {
            tokens: lex(source)?,
            index: 0,
        })
    }

    fn parse(mut self) -> Result<MatchQuery, MatchmakerError> {
        let query = self.or()?;
        if self.peek().is_some() {
            return Err(MatchmakerError::Query(
                "unexpected trailing token".to_owned(),
            ));
        }
        Ok(query)
    }

    fn or(&mut self) -> Result<MatchQuery, MatchmakerError> {
        let mut query = self.and()?;
        while matches!(self.peek(), Some(Token::Or)) {
            self.index += 1;
            query = MatchQuery::Or(Box::new(query), Box::new(self.and()?));
        }
        Ok(query)
    }

    fn and(&mut self) -> Result<MatchQuery, MatchmakerError> {
        let mut query = self.primary()?;
        while matches!(self.peek(), Some(Token::And)) {
            self.index += 1;
            query = MatchQuery::And(Box::new(query), Box::new(self.primary()?));
        }
        Ok(query)
    }

    fn primary(&mut self) -> Result<MatchQuery, MatchmakerError> {
        if matches!(self.peek(), Some(Token::LeftParen)) {
            self.index += 1;
            let query = self.or()?;
            if !matches!(self.next(), Some(Token::RightParen)) {
                return Err(MatchmakerError::Query("missing closing ')'".to_owned()));
            }
            return Ok(query);
        }
        let Some(Token::Ident(field)) = self.next() else {
            return Err(MatchmakerError::Query("expected property name".to_owned()));
        };
        let Some(Token::Op(op)) = self.next() else {
            return Err(MatchmakerError::Query(
                "expected comparison operator".to_owned(),
            ));
        };
        let value = match self.next() {
            Some(Token::String(value)) => PropertyValue::String(value),
            Some(Token::Number(value)) => PropertyValue::Number(value),
            _ => {
                return Err(MatchmakerError::Query(
                    "expected string or number literal".to_owned(),
                ));
            }
        };
        if matches!(value, PropertyValue::String(_))
            && !matches!(op, Comparison::Equal | Comparison::NotEqual)
        {
            return Err(MatchmakerError::Query(
                "strings only support == and != comparisons".to_owned(),
            ));
        }
        Ok(MatchQuery::Compare { field, op, value })
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.index).cloned();
        self.index += usize::from(token.is_some());
        token
    }
}

fn lex(source: &str) -> Result<Vec<Token>, MatchmakerError> {
    let bytes = source.as_bytes();
    let mut index = 0;
    let mut tokens = Vec::new();
    while index < bytes.len() {
        if bytes[index].is_ascii_whitespace() {
            index += 1;
            continue;
        }
        match bytes[index] {
            b'(' => {
                tokens.push(Token::LeftParen);
                index += 1;
            }
            b')' => {
                tokens.push(Token::RightParen);
                index += 1;
            }
            b'=' if bytes.get(index + 1) == Some(&b'=') => {
                tokens.push(Token::Op(Comparison::Equal));
                index += 2;
            }
            b'!' if bytes.get(index + 1) == Some(&b'=') => {
                tokens.push(Token::Op(Comparison::NotEqual));
                index += 2;
            }
            b'<' => {
                let equal = bytes.get(index + 1) == Some(&b'=');
                tokens.push(Token::Op(if equal {
                    Comparison::LessOrEqual
                } else {
                    Comparison::Less
                }));
                index += if equal { 2 } else { 1 };
            }
            b'>' => {
                let equal = bytes.get(index + 1) == Some(&b'=');
                tokens.push(Token::Op(if equal {
                    Comparison::GreaterOrEqual
                } else {
                    Comparison::Greater
                }));
                index += if equal { 2 } else { 1 };
            }
            b'"' => {
                index += 1;
                let start = index;
                while index < bytes.len() && bytes[index] != b'"' {
                    if bytes[index] == b'\\' {
                        return Err(MatchmakerError::Query(
                            "string escapes are not supported".to_owned(),
                        ));
                    }
                    index += 1;
                }
                if index == bytes.len() {
                    return Err(MatchmakerError::Query(
                        "unterminated string literal".to_owned(),
                    ));
                }
                let value = std::str::from_utf8(&bytes[start..index])
                    .map_err(|_| MatchmakerError::Query("string is not UTF-8".to_owned()))?;
                tokens.push(Token::String(value.to_owned()));
                index += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = index;
                index += 1;
                while index < bytes.len() && (bytes[index].is_ascii_digit() || bytes[index] == b'.')
                {
                    index += 1;
                }
                let raw = std::str::from_utf8(&bytes[start..index])
                    .map_err(|_| MatchmakerError::Query("invalid number".to_owned()))?;
                let number = raw
                    .parse::<f64>()
                    .map_err(|_| MatchmakerError::Query(format!("invalid number: {raw}")))?;
                if !number.is_finite() {
                    return Err(MatchmakerError::Query("number must be finite".to_owned()));
                }
                tokens.push(Token::Number(number));
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || matches!(bytes[index], b'_' | b'.'))
                {
                    index += 1;
                }
                let word = std::str::from_utf8(&bytes[start..index])
                    .map_err(|_| MatchmakerError::Query("invalid identifier".to_owned()))?;
                match word.to_ascii_uppercase().as_str() {
                    "AND" => tokens.push(Token::And),
                    "OR" => tokens.push(Token::Or),
                    _ => tokens.push(Token::Ident(word.to_owned())),
                }
            }
            _ => {
                return Err(MatchmakerError::Query(format!(
                    "unexpected character at byte {index}"
                )));
            }
        }
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: TimestampMillis = TimestampMillis::from_unix_millis(1_000);

    fn request(query: &str, properties: &[(&str, PropertyValue)]) -> TicketRequest {
        TicketRequest {
            query: query.to_owned(),
            properties: properties
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
            min_count: 2,
            max_count: 4,
            count_multiple: 1,
            party_id: None,
            ttl_ms: 1_000,
        }
    }

    #[test]
    fn parses_and_evaluates_typed_boolean_queries() {
        let query = MatchQuery::parse("mode == \"ranked\" AND (skill >= 10 OR region == \"jp\")")
            .expect("query parses");
        let props = BTreeMap::from([
            (
                "mode".to_owned(),
                PropertyValue::String("ranked".to_owned()),
            ),
            ("skill".to_owned(), PropertyValue::Number(11.0)),
        ]);
        assert!(query.matches(&props));
        assert!(
            !MatchQuery::parse("mode > \"ranked\"")
                .expect_err("string range rejected")
                .to_string()
                .is_empty()
        );
    }

    #[test]
    fn oldest_compatible_tickets_form_the_smallest_cohort() {
        let matchmaker = Matchmaker::new();
        let first = matchmaker
            .add(
                ParticipantId::from_raw(1),
                request(
                    "mode == \"duo\"",
                    &[("mode", PropertyValue::String("duo".to_owned()))],
                ),
                NOW,
            )
            .expect("first");
        let second = matchmaker
            .add(
                ParticipantId::from_raw(2),
                request(
                    "mode == \"duo\"",
                    &[("mode", PropertyValue::String("duo".to_owned()))],
                ),
                NOW,
            )
            .expect("second");
        let third = matchmaker
            .add(ParticipantId::from_raw(3), request("", &[]), NOW)
            .expect("third");
        let formed = matchmaker.evaluate(NOW);
        assert_eq!(formed.len(), 1);
        assert_eq!(
            formed[0].participants,
            vec![ParticipantId::from_raw(1), ParticipantId::from_raw(2)]
        );
        assert_eq!(
            matchmaker.state(ParticipantId::from_raw(1), &first, NOW),
            Some(TicketState::Matched)
        );
        assert_eq!(
            matchmaker.state(ParticipantId::from_raw(2), &second, NOW),
            Some(TicketState::Matched)
        );
        assert_eq!(
            matchmaker.state(ParticipantId::from_raw(3), &third, NOW),
            Some(TicketState::Queued)
        );
    }

    #[test]
    fn mutual_queries_and_count_constraints_are_required() {
        let matchmaker = Matchmaker::new();
        let mut strict = request("skill >= 100", &[("skill", PropertyValue::Number(100.0))]);
        strict.min_count = 3;
        strict.count_multiple = 3;
        matchmaker
            .add(ParticipantId::from_raw(1), strict, NOW)
            .expect("first");
        matchmaker
            .add(
                ParticipantId::from_raw(2),
                request("skill >= 100", &[("skill", PropertyValue::Number(100.0))]),
                NOW,
            )
            .expect("second");
        assert!(
            matchmaker.evaluate(NOW).is_empty(),
            "two is incompatible with seed count requirement"
        );
        matchmaker
            .add(
                ParticipantId::from_raw(3),
                request("skill >= 100", &[("skill", PropertyValue::Number(100.0))]),
                NOW,
            )
            .expect("third");
        assert_eq!(matchmaker.evaluate(NOW)[0].participants.len(), 3);
    }

    #[test]
    fn party_ticket_is_indivisible_and_counts_every_member() {
        let matchmaker = Matchmaker::new();
        let mut party_request = request("", &[]);
        party_request.min_count = 3;
        party_request.max_count = 3;
        let party_ticket = matchmaker
            .add_party(
                ParticipantId::from_raw(1),
                vec![ParticipantId::from_raw(1), ParticipantId::from_raw(2)],
                party_request,
                NOW,
            )
            .expect("party ticket");
        assert!(
            matchmaker
                .add(ParticipantId::from_raw(2), request("", &[]), NOW)
                .is_err(),
            "a party member cannot enter a second queue"
        );
        matchmaker
            .add(ParticipantId::from_raw(3), request("", &[]), NOW)
            .expect("solo ticket");

        let formed = matchmaker.evaluate(NOW);
        assert_eq!(formed.len(), 1);
        assert_eq!(
            formed[0].ticket_members,
            vec![vec![pid(1), pid(2)], vec![pid(3)]]
        );
        assert_eq!(formed[0].participants, vec![pid(1), pid(2), pid(3)]);
        assert_eq!(
            matchmaker.state(pid(2), &party_ticket, NOW),
            Some(TicketState::Matched),
            "each party member can inspect the shared ticket"
        );
    }

    #[test]
    fn cancellation_is_owner_bound_and_expiry_removes_ticket() {
        let matchmaker = Matchmaker::new();
        let mut ticket_request = request("", &[]);
        ticket_request.ttl_ms = 10;
        let id = matchmaker
            .add(ParticipantId::from_raw(1), ticket_request, NOW)
            .expect("added");
        assert!(!matchmaker.cancel(ParticipantId::from_raw(2), &id, NOW));
        assert!(matchmaker.cancel(ParticipantId::from_raw(1), &id, NOW));
        assert!(!matchmaker.cancel(ParticipantId::from_raw(1), &id, NOW));
        let id = matchmaker
            .add(ParticipantId::from_raw(1), request("", &[]), NOW)
            .expect("can queue again");
        assert_eq!(
            matchmaker.queued_len(TimestampMillis::from_unix_millis(2_001)),
            0
        );
        assert_eq!(
            matchmaker.state(
                ParticipantId::from_raw(1),
                &id,
                TimestampMillis::from_unix_millis(2_001)
            ),
            Some(TicketState::Removed)
        );
        let stats = matchmaker.stats();
        assert_eq!(stats.queued_tickets, 0);
        assert_eq!(stats.tickets_cancelled_total, 1);
        assert_eq!(stats.tickets_expired_total, 1);
    }

    #[test]
    fn evaluation_stats_count_formed_cohorts_without_ticket_details() {
        let matchmaker = Matchmaker::new();
        matchmaker
            .add(ParticipantId::from_raw(1), request("", &[]), NOW)
            .expect("first");
        matchmaker
            .add(ParticipantId::from_raw(2), request("", &[]), NOW)
            .expect("second");
        assert_eq!(matchmaker.evaluate(NOW).len(), 1);
        let stats = matchmaker.stats();
        assert_eq!(stats.queued_tickets, 0);
        assert_eq!(stats.evaluations_total, 1);
        assert_eq!(stats.matches_formed_total, 1);
        assert_eq!(stats.tickets_formed_total, 2);
        let value = serde_json::to_value(stats).expect("stats serialize");
        assert!(value.get("ticket_id").is_none());
        assert!(value.get("properties").is_none());
    }

    #[test]
    fn invalid_boundary_input_never_enters_queue() {
        let matchmaker = Matchmaker::new();
        let mut invalid = request("bad @@", &[]);
        invalid.min_count = 1;
        assert!(
            matchmaker
                .add(ParticipantId::from_raw(1), invalid, NOW)
                .is_err()
        );
        assert_eq!(matchmaker.queued_len(NOW), 0);
    }

    const fn pid(id: u64) -> ParticipantId {
        ParticipantId::from_raw(id)
    }
}
