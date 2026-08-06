//! Startup UX for the standalone Citadel server.
//!
//! This module owns the first-contact experience of `citadel serve`:
//!
//! - [`build_banner`] renders a tidy, boxed ASCII banner (a `CITADEL` wordmark
//!   plus version, node id, selected database backend, and an aligned list of
//!   dashboard/status/health and enabled-transport links). It is a pure function
//!   over the resolved [`Config`], the selected [`BackendKind`], and the version
//!   string, so it is unit-testable without capturing stdout. The server prints
//!   it once, right before it starts accepting connections, so it is the
//!   prominent, readable thing on a normal run.
//! - The first-run wizard ([`run_first_run_wizard`]) offers to create a starter
//!   gameplay script and to choose a database when neither exists yet. It is
//!   driven through a [`Prompt`] seam so the interaction logic is unit-testable
//!   with scripted answers and no real terminal. On a non-interactive stdin
//!   (CI/headless), with an explicit `--config`, or with `--yes`, the wizard is
//!   skipped entirely and the existing silent auto-defaults apply.
//!
//! The wizard deliberately does no work when a script or database already
//! exists, so re-running the server never re-prompts; the SQLite choice is
//! persisted into `citadel.toml` so the *next* run is non-interactive too.

use std::io::{self, BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};

use crate::config::{Config, DEFAULT_CONFIG_FILE};
use crate::error::{AppError, AppResult};
use crate::repository::BackendKind;

/// The default single-file SQLite URL the wizard writes when SQLite is chosen.
///
/// A `sqlite://` URL selects the embedded backend; the file is created next to
/// the working directory on the next run by the persistence bootstrap.
pub const DEFAULT_SQLITE_URL: &str = "sqlite://data.sqlite";

/// The default PostgreSQL URL suggested by the wizard.
///
/// Used only as the pre-filled answer for the Postgres prompt; the operator can
/// edit it. It is never assumed silently.
pub const DEFAULT_POSTGRES_URL: &str = "postgres://localhost:5432/citadel";

/// Gameplay-script languages the wizard can scaffold.
///
/// Lua is always available. Python and JavaScript are offered only in builds
/// that compile their runtimes, so the default executable never scaffolds a
/// script it cannot run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptLanguage {
    /// The embedded Lua runtime (`game/main.lua`).
    Lua,
    /// The embedded CPython runtime (`game/main.py`).
    Python,
    /// The embedded capped QuickJS runtime (`game/main.js`).
    Js,
}

impl ScriptLanguage {
    /// The languages offered by the wizard, in menu order.
    #[must_use]
    pub const fn all() -> &'static [ScriptLanguage] {
        #[cfg(all(feature = "runtime-python", feature = "runtime-js"))]
        {
            &[
                ScriptLanguage::Lua,
                ScriptLanguage::Python,
                ScriptLanguage::Js,
            ]
        }
        #[cfg(all(feature = "runtime-python", not(feature = "runtime-js")))]
        {
            &[ScriptLanguage::Lua, ScriptLanguage::Python]
        }
        #[cfg(all(not(feature = "runtime-python"), feature = "runtime-js"))]
        {
            &[ScriptLanguage::Lua, ScriptLanguage::Js]
        }
        #[cfg(all(not(feature = "runtime-python"), not(feature = "runtime-js")))]
        {
            &[ScriptLanguage::Lua]
        }
    }

    /// Entrypoints that count as an existing gameplay script.
    #[must_use]
    pub const fn known_entry_files() -> &'static [&'static str] {
        &["main.lua", "main.py", "main.js"]
    }

    /// Human-readable menu label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            ScriptLanguage::Lua => "Lua",
            ScriptLanguage::Python => "Python",
            ScriptLanguage::Js => "JavaScript (QuickJS capped mode)",
        }
    }

    /// The entrypoint filename created inside the scripts directory.
    #[must_use]
    pub const fn entry_file(self) -> &'static str {
        match self {
            ScriptLanguage::Lua => "main.lua",
            ScriptLanguage::Python => "main.py",
            ScriptLanguage::Js => "main.js",
        }
    }

    /// The starter script contents for this language.
    #[must_use]
    pub const fn starter(self) -> &'static str {
        match self {
            ScriptLanguage::Lua => STARTER_LUA,
            ScriptLanguage::Python => STARTER_PYTHON,
            ScriptLanguage::Js => STARTER_JS,
        }
    }
}

/// The database backends the wizard offers, in menu order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseChoice {
    /// Embedded single-file SQLite (the zero-infra default).
    Sqlite,
    /// A networked PostgreSQL server (operator supplies the URL).
    Postgres,
    /// A transaction-capable MongoDB replica set or sharded cluster.
    MongoDb,
}

impl DatabaseChoice {
    /// The choices offered by the wizard, in menu order (SQLite first/default).
    #[must_use]
    pub const fn all() -> &'static [DatabaseChoice] {
        &[
            DatabaseChoice::Sqlite,
            DatabaseChoice::Postgres,
            DatabaseChoice::MongoDb,
        ]
    }

    /// Human-readable menu label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            DatabaseChoice::Sqlite => "SQLite (embedded, recommended)",
            DatabaseChoice::Postgres => "PostgreSQL (external server)",
            DatabaseChoice::MongoDb => "MongoDB (replica set or sharded cluster)",
        }
    }
}

/// A minimal, working starter Lua script: the position relay plus a `ping` RPC.
///
/// Kept intentionally small so a first-time operator can read the whole thing.
/// The full-featured sample lives in the repo's `game/main.lua`; this is what the
/// wizard drops for someone starting from nothing.
const STARTER_LUA: &str = r#"-- Citadel starter gameplay script (created by the first-run wizard).
--
-- This is a minimal, working example. It relays player positions to peers and
-- answers a "ping" RPC. Edit it freely, then restart the server (or enable
-- runtime.hot_reload in citadel.toml to reload on save). Delete this file to
-- fall back to the built-in relay. See website/src/content/docs/reference/server-sdk/lua-runtime.md
-- for the full host API.

local KIND_POSITION = 1      -- client -> server: "my position update"
local KIND_PEER_POSITION = 2 -- server -> client: a peer's position, sender-tagged

-- Relay: tag the sender's id onto the body and broadcast to everyone else.
citadel.on_message(KIND_POSITION, function(ctx, body)
  local tagged = string.pack(">I8", ctx.sender) .. body
  citadel.broadcast(KIND_PEER_POSITION, tagged, true)
end)

-- A request/response RPC: reply "pong" to any "ping". Great for a first
-- round-trip test from a client SDK.
citadel.on_rpc("ping", function(ctx, body)
  return "pong"
end)
"#;

/// A minimal, working starter Python script matching [`STARTER_LUA`].
const STARTER_PYTHON: &str = r#"# Citadel starter gameplay script (created by the first-run wizard).
#
# This is a minimal, working example. It relays player positions to peers and
# answers a "ping" RPC. Run Citadel with --features runtime-python to use it.
# Edit it freely, then restart the server (or enable runtime.hot_reload in
# citadel.toml to reload on save). Delete this file to fall back to the built-in
# relay. See website/src/content/docs/reference/server-sdk/python-runtime.mdx for
# the full host API.

import citadel

KIND_POSITION = 1      # client -> server: "my position update"
KIND_PEER_POSITION = 2 # server -> client: a peer's position, sender-tagged

@citadel.on_message(KIND_POSITION)
def relay_position(ctx, body):
    tagged = int(ctx.sender).to_bytes(8, "big") + body
    citadel.broadcast(KIND_PEER_POSITION, tagged, True)

@citadel.on_rpc("ping")
def ping(ctx, body):
    return citadel.Reply.ok(b"pong")
"#;

/// A minimal, working starter JavaScript script matching [`STARTER_LUA`].
const STARTER_JS: &str = r#"// Citadel starter gameplay script (created by the first-run wizard).
//
// This is a minimal, working example. It relays player positions to peers and
// answers a "ping" RPC. Run Citadel with --features runtime-js to use it.
// Capped QuickJS mode has no npm, Node APIs, threads, or TypeScript transpiler.
// Edit it freely, then restart the server (or enable runtime.hot_reload in
// citadel.toml to reload on save). Delete this file to fall back to the built-in
// relay. See website/src/content/docs/reference/server-sdk/js-runtime.mdx for
// the full host API.

const KIND_POSITION = 1;      // client -> server: "my position update"
const KIND_PEER_POSITION = 2; // server -> client: a peer's position, sender-tagged

function u64be(value) {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, BigInt(value), false);
  return out;
}

function concat(a, b) {
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

citadel.on_message(KIND_POSITION, (ctx, body) => {
  const tagged = concat(u64be(ctx.sender), body);
  citadel.broadcast(KIND_PEER_POSITION, tagged, true);
});

citadel.on_rpc("ping", () => citadel.Reply.ok("pong"));
"#;

/// A prompt seam so the wizard's interaction logic is testable without a TTY.
///
/// Production uses [`StdioPrompt`] over the process stdin/stdout; tests inject a
/// scripted implementation that returns canned answers and records the prompts
/// it was asked, with no real terminal involved.
pub trait Prompt {
    /// Ask a yes/no question, returning the operator's choice. An empty answer
    /// takes `default_yes`.
    fn confirm(&mut self, question: &str, default_yes: bool) -> io::Result<bool>;

    /// Ask the operator to pick one of `options`, returning the chosen index.
    /// An empty answer takes `default`.
    fn choose(&mut self, question: &str, options: &[&str], default: usize) -> io::Result<usize>;

    /// Ask for a free-text value, returning it trimmed. An empty answer takes
    /// `default`.
    fn ask(&mut self, question: &str, default: &str) -> io::Result<String>;
}

/// A [`Prompt`] over a buffered reader and a writer (the real stdin/stdout in
/// production, or in-memory buffers in tests).
pub struct StdioPrompt<R, W> {
    reader: R,
    writer: W,
}

impl<R: BufRead, W: Write> StdioPrompt<R, W> {
    /// Build a prompt over an explicit reader and writer.
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    /// Read a single trimmed line, or an empty string at EOF.
    fn read_line(&mut self) -> io::Result<String> {
        let mut line = String::new();
        let read = self.reader.read_line(&mut line)?;
        if read == 0 {
            // EOF: behave as if the operator accepted the default.
            return Ok(String::new());
        }
        Ok(line.trim().to_string())
    }
}

impl<R: BufRead, W: Write> Prompt for StdioPrompt<R, W> {
    fn confirm(&mut self, question: &str, default_yes: bool) -> io::Result<bool> {
        let hint = if default_yes { "[Y/n]" } else { "[y/N]" };
        loop {
            write!(self.writer, "{question} {hint} ")?;
            self.writer.flush()?;
            let answer = self.read_line()?.to_ascii_lowercase();
            match answer.as_str() {
                "" => return Ok(default_yes),
                "y" | "yes" => return Ok(true),
                "n" | "no" => return Ok(false),
                _ => {
                    writeln!(self.writer, "Please answer 'y' or 'n'.")?;
                }
            }
        }
    }

    fn choose(&mut self, question: &str, options: &[&str], default: usize) -> io::Result<usize> {
        loop {
            writeln!(self.writer, "{question}")?;
            for (idx, option) in options.iter().enumerate() {
                let marker = if idx == default { " (default)" } else { "" };
                writeln!(self.writer, "  {}) {option}{marker}", idx + 1)?;
            }
            write!(self.writer, "Choose [1-{}]: ", options.len())?;
            self.writer.flush()?;
            let answer = self.read_line()?;
            if answer.is_empty() {
                return Ok(default);
            }
            if let Ok(n) = answer.parse::<usize>()
                && n >= 1
                && n <= options.len()
            {
                return Ok(n - 1);
            }
            writeln!(
                self.writer,
                "Please enter a number between 1 and {}.",
                options.len()
            )?;
        }
    }

    fn ask(&mut self, question: &str, default: &str) -> io::Result<String> {
        write!(self.writer, "{question} [{default}]: ")?;
        self.writer.flush()?;
        let answer = self.read_line()?;
        if answer.is_empty() {
            Ok(default.to_string())
        } else {
            Ok(answer)
        }
    }
}

/// The filesystem locations the wizard reads and writes.
#[derive(Debug, Clone)]
pub struct WizardPaths {
    /// Where to persist `citadel.toml` when the operator makes a choice.
    pub config_path: PathBuf,
    /// The runtime scripts directory (from `runtime.scripts_dir`).
    pub scripts_dir: PathBuf,
}

impl WizardPaths {
    /// Derive the wizard's paths from a resolved config and an optional explicit
    /// `--config` path.
    ///
    /// When `--config` is given, that file is the persistence target; otherwise
    /// choices are written to `./citadel.toml`.
    #[must_use]
    pub fn from_config(config: &Config, explicit_config: Option<&Path>) -> Self {
        let config_path = explicit_config
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_FILE));
        Self {
            config_path,
            scripts_dir: PathBuf::from(&config.runtime.scripts_dir),
        }
    }
}

/// What the wizard did, for a concise closing summary.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WizardReport {
    /// The starter script that was written, if any.
    pub created_script: Option<PathBuf>,
    /// The database choice that was made, if any.
    pub selected_database: Option<DatabaseChoice>,
    /// Whether `citadel.toml` was (re)written to persist a choice.
    pub persisted_config: bool,
}

impl WizardReport {
    /// Whether the wizard changed anything on disk or in config.
    #[must_use]
    pub fn made_changes(&self) -> bool {
        self.created_script.is_some() || self.selected_database.is_some() || self.persisted_config
    }
}

/// Decide whether the interactive wizard should run.
///
/// It runs only on a real terminal, with no explicit `--config`, and when the
/// operator did not pass `--yes`/`--non-interactive`. Every other case falls
/// back to the silent auto-defaults, so CI and headless deploys never block.
#[must_use]
pub fn should_run_wizard(explicit_config: bool, assume_yes: bool) -> bool {
    !explicit_config && !assume_yes && io::stdin().is_terminal()
}

/// Run the first-run wizard against a resolved `config`, prompting through
/// `prompt` and reading/writing the locations in `paths`.
///
/// The wizard is idempotent by construction: it offers to create a gameplay
/// script only when none exists, and to choose a database only when the config
/// has none. When it makes a database choice it mutates `config` in place and
/// persists the config file so the next run is non-interactive.
///
/// # Errors
/// Returns a [`Config`](crate::error::ErrorCategory::Config) error if the starter
/// script or the persisted config file cannot be written.
pub fn run_first_run_wizard<P: Prompt>(
    config: &mut Config,
    paths: &WizardPaths,
    prompt: &mut P,
) -> AppResult<WizardReport> {
    let mut report = WizardReport::default();

    // 1) Gameplay script.
    if config.runtime.enabled {
        let has_script = ScriptLanguage::known_entry_files()
            .iter()
            .any(|entry| paths.scripts_dir.join(entry).exists());
        if !has_script
            && prompt
                .confirm("No gameplay script found. Initialize one?", true)
                .map_err(prompt_err)?
        {
            let languages = ScriptLanguage::all();
            let labels: Vec<&str> = languages.iter().map(|l| l.label()).collect();
            let idx = prompt
                .choose("Select a scripting language:", &labels, 0)
                .map_err(prompt_err)?;
            let language = languages.get(idx).copied().unwrap_or(ScriptLanguage::Lua);
            let written = write_starter_script(&paths.scripts_dir, language)?;
            report.created_script = Some(written);
            // A scripted project is GameScript-dependent: matches must not
            // exist without the script, so the strict readiness gate is
            // enabled and persisted alongside the scaffold.
            config.runtime.require_script = true;
            config.write_to(&paths.config_path)?;
            report.persisted_config = true;
        }
    }

    // 2) Database.
    if !config.database.is_enabled()
        && prompt
            .confirm("No database configured. Initialize one?", true)
            .map_err(prompt_err)?
    {
        let choices = DatabaseChoice::all();
        let labels: Vec<&str> = choices.iter().map(|c| c.label()).collect();
        let idx = prompt
            .choose("Select a database backend:", &labels, 0)
            .map_err(prompt_err)?;
        let choice = choices.get(idx).copied().unwrap_or(DatabaseChoice::Sqlite);
        match choice {
            DatabaseChoice::Sqlite => {
                config.database.url = Some(DEFAULT_SQLITE_URL.to_string());
            }
            DatabaseChoice::Postgres => {
                let url = prompt
                    .ask("Enter the PostgreSQL connection URL", DEFAULT_POSTGRES_URL)
                    .map_err(prompt_err)?;
                config.database.url = Some(url);
            }
            DatabaseChoice::MongoDb => {
                let url = prompt
                    .ask(
                        "Enter the MongoDB connection URL (transaction-capable replica set or sharded cluster)",
                        "mongodb://localhost:27017/citadel?replicaSet=rs0",
                    )
                    .map_err(prompt_err)?;
                config.database.url = Some(url);
            }
        }
        report.selected_database = Some(choice);
        config.write_to(&paths.config_path)?;
        report.persisted_config = true;
    }

    Ok(report)
}

/// Map a prompt I/O failure (e.g. a broken stdin) to a config-category error.
fn prompt_err(e: io::Error) -> AppError {
    AppError::config("failed to read interactive input").with_detail(e.to_string())
}

/// Write `language`'s starter script into `scripts_dir`, creating the directory
/// if needed. Returns the path written.
fn write_starter_script(scripts_dir: &Path, language: ScriptLanguage) -> AppResult<PathBuf> {
    std::fs::create_dir_all(scripts_dir).map_err(|e| {
        AppError::config(format!(
            "failed to create scripts directory: {}",
            scripts_dir.display()
        ))
        .with_detail(e.to_string())
    })?;
    let path = scripts_dir.join(language.entry_file());
    std::fs::write(&path, language.starter()).map_err(|e| {
        AppError::config(format!(
            "failed to write starter script: {}",
            path.display()
        ))
        .with_detail(e.to_string())
    })?;
    Ok(path)
}

// -----------------------------------------------------------------------------
// Banner
// -----------------------------------------------------------------------------

/// The `CITADEL` ASCII wordmark, one entry per line (no trailing padding).
const WORDMARK: &[&str] = &[
    r"  ____ ___ _____  _    ____  _____ _     ",
    r" / ___|_ _|_   _|/ \  |  _ \| ____| |    ",
    r"| |    | |  | | / _ \ | | | |  _| | |    ",
    r"| |___ | |  | |/ ___ \| |_| | |___| |___ ",
    r" \____|___| |_/_/   \_\____/|_____|_____|",
];

/// Build the boxed startup banner for a resolved config.
///
/// The banner shows the wordmark, a summary line (version / node id / selected
/// database backend), and an aligned list of links: the HTTP dashboard, status,
/// and health endpoints, followed by every enabled realtime transport with its
/// bind address. It is a pure function of its inputs so tests assert on the
/// rendered string directly.
#[must_use]
pub fn build_banner(config: &Config, backend: BackendKind, version: &str) -> String {
    let mut content: Vec<String> = WORDMARK.iter().map(|l| (*l).to_string()).collect();
    content.push(String::new());
    content.push(format!(
        "version {version}   node {}   db {}",
        config.server.node_id,
        backend.as_str(),
    ));
    content.push(String::new());

    for line in banner_link_lines(config) {
        content.push(line);
    }

    // Surface the insecure out-of-the-box console credentials until an operator
    // changes them; values are never echoed beyond the well-known defaults.
    if config.console.uses_default_credentials() {
        content.push(String::new());
        content.push(
            "WARNING  console login uses the default credentials (admin/password)".to_string(),
        );
        content.push(
            "         set [console] password in citadel.toml before exposing this node".to_string(),
        );
    }

    render_box(&content)
}

/// The aligned `label   value` link lines for the banner, in display order.
fn banner_link_lines(config: &Config) -> Vec<String> {
    let http = &config.http.bind;
    let mut links: Vec<(&str, String)> = vec![
        (
            "Dashboard",
            format!("http://{http}{}", crate::http::DASHBOARD_PATH),
        ),
        (
            "Status",
            format!("http://{http}{}", crate::http::STATUS_PATH),
        ),
        (
            "Health",
            format!("http://{http}{}", crate::http::HEALTH_PATH),
        ),
    ];
    if config.transport.quic.enabled {
        links.push(("QUIC", format!("udp://{}", config.transport.quic.bind)));
    }
    if config.transport.websocket.enabled {
        links.push((
            "WebSocket",
            format!("ws://{}", config.transport.websocket.bind),
        ));
    }
    if config.transport.webtransport.enabled {
        links.push((
            "WebTransport",
            format!("https://{}", config.transport.webtransport.bind),
        ));
    }

    let label_width = links
        .iter()
        .map(|(label, _)| label.len())
        .max()
        .unwrap_or(0);
    links
        .into_iter()
        .map(|(label, value)| format!("{label:<label_width$}   {value}"))
        .collect()
}

/// Draw `lines` inside an ASCII box, padding every line to the widest one.
fn render_box(lines: &[String]) -> String {
    let width = lines.iter().map(|l| l.chars().count()).max().unwrap_or(0);
    let border = format!("+{}+", "-".repeat(width + 2));
    let mut out = String::with_capacity((width + 4) * (lines.len() + 2));
    out.push_str(&border);
    out.push('\n');
    for line in lines {
        let pad = width - line.chars().count();
        out.push_str("| ");
        out.push_str(line);
        for _ in 0..pad {
            out.push(' ');
        }
        out.push_str(" |\n");
    }
    out.push_str(&border);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!(
            "citadel-startup-{tag}-{}-{nanos}",
            std::process::id()
        ))
    }

    // --- Banner -------------------------------------------------------------

    #[test]
    fn banner_renders_a_closed_box_with_wordmark() {
        let banner = build_banner(&Config::default(), BackendKind::InMemory, "9.9.9");
        let lines: Vec<&str> = banner.lines().collect();
        assert!(
            lines
                .first()
                .is_some_and(|l| l.starts_with('+') && l.ends_with('+'))
        );
        assert!(
            lines
                .last()
                .is_some_and(|l| l.starts_with('+') && l.ends_with('+'))
        );
        // Every content row is a full-width, closed box row.
        let width = lines[0].chars().count();
        for line in &lines {
            assert_eq!(
                line.chars().count(),
                width,
                "row not padded to box width: {line}"
            );
        }
        assert!(banner.contains("CITADEL") || banner.contains("____"));
    }

    #[test]
    fn banner_includes_core_links_and_backend() {
        let banner = build_banner(&Config::default(), BackendKind::Sqlite, "1.2.3");
        assert!(banner.contains("version 1.2.3"));
        assert!(banner.contains("db sqlite"));
        assert!(banner.contains("http://127.0.0.1:7350/dashboard"));
        assert!(banner.contains("http://127.0.0.1:7350/status"));
        assert!(banner.contains(&format!(
            "http://127.0.0.1:7350{}",
            crate::http::HEALTH_PATH
        )));
    }

    #[test]
    fn banner_lists_only_enabled_transports() {
        let mut config = Config::default();
        // All transports off by default: no transport rows.
        let banner = build_banner(&config, BackendKind::InMemory, "0");
        assert!(!banner.contains("QUIC"));
        assert!(!banner.contains("WebSocket"));

        config.transport.quic.enabled = true;
        config.transport.quic.bind = "0.0.0.0:7351".to_string();
        config.transport.webtransport.enabled = true;
        config.transport.webtransport.bind = "0.0.0.0:7353".to_string();
        let banner = build_banner(&config, BackendKind::InMemory, "0");
        assert!(banner.contains("udp://0.0.0.0:7351"), "QUIC bind shown");
        assert!(
            banner.contains("https://0.0.0.0:7353"),
            "WebTransport bind shown"
        );
        // WebSocket stays off, so it must not appear.
        assert!(!banner.contains("ws://"));
    }

    // --- Prompt seam --------------------------------------------------------

    /// A prompt that replays scripted answers and records the questions asked.
    struct ScriptedPrompt {
        confirms: Vec<bool>,
        chooses: Vec<usize>,
        asks: Vec<String>,
        pub asked: Vec<String>,
    }

    impl ScriptedPrompt {
        fn new(confirms: Vec<bool>, chooses: Vec<usize>, asks: Vec<String>) -> Self {
            Self {
                confirms,
                chooses,
                asks,
                asked: Vec::new(),
            }
        }
    }

    impl Prompt for ScriptedPrompt {
        fn confirm(&mut self, question: &str, _default_yes: bool) -> io::Result<bool> {
            self.asked.push(question.to_string());
            Ok(if self.confirms.is_empty() {
                false
            } else {
                self.confirms.remove(0)
            })
        }
        fn choose(
            &mut self,
            question: &str,
            _options: &[&str],
            default: usize,
        ) -> io::Result<usize> {
            self.asked.push(question.to_string());
            Ok(if self.chooses.is_empty() {
                default
            } else {
                self.chooses.remove(0)
            })
        }
        fn ask(&mut self, question: &str, default: &str) -> io::Result<String> {
            self.asked.push(question.to_string());
            Ok(if self.asks.is_empty() {
                default.to_string()
            } else {
                self.asks.remove(0)
            })
        }
    }

    /// A prompt that fails the test if it is ever consulted.
    struct NeverPrompt;
    #[allow(clippy::panic)]
    impl Prompt for NeverPrompt {
        fn confirm(&mut self, q: &str, _d: bool) -> io::Result<bool> {
            panic!("prompted unexpectedly: {q}")
        }
        fn choose(&mut self, q: &str, _o: &[&str], _d: usize) -> io::Result<usize> {
            panic!("prompted unexpectedly: {q}")
        }
        fn ask(&mut self, q: &str, _d: &str) -> io::Result<String> {
            panic!("prompted unexpectedly: {q}")
        }
    }

    // --- StdioPrompt parsing ------------------------------------------------

    #[test]
    fn stdio_confirm_parses_yes_no_and_default() {
        let input = b"\ny\nno\n";
        let mut out = Vec::new();
        let mut prompt = StdioPrompt::new(&input[..], &mut out);
        assert!(
            prompt
                .confirm("q", true)
                .expect("empty line takes the default (yes)"),
            "empty => default yes"
        );
        // The above consumed one line; the next two lines are y then no.
        assert!(prompt.confirm("q", false).expect("yes"));
        assert!(!prompt.confirm("q", true).expect("no"));
    }

    #[test]
    fn stdio_choose_parses_index_and_default() {
        let input = b"2\n\n";
        let mut out = Vec::new();
        let mut prompt = StdioPrompt::new(&input[..], &mut out);
        assert_eq!(
            prompt.choose("q", &["a", "b", "c"], 0).expect("2 => idx 1"),
            1
        );
        assert_eq!(
            prompt
                .choose("q", &["a", "b"], 1)
                .expect("empty => default"),
            1
        );
    }

    // --- Wizard logic -------------------------------------------------------

    #[test]
    fn wizard_creates_lua_script_when_missing_and_confirmed() {
        let base = unique_temp_dir("script");
        let scripts = base.join("game");
        let mut config = Config::default();
        // Pretend a database is already configured so only the script step runs.
        config.database.url = Some(DEFAULT_SQLITE_URL.to_string());
        let paths = WizardPaths {
            config_path: base.join("citadel.toml"),
            scripts_dir: scripts.clone(),
        };
        // confirm: yes (create script); choose: index 0 (Lua).
        let mut prompt = ScriptedPrompt::new(vec![true], vec![0], vec![]);
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("wizard runs");
        let expected = scripts.join("main.lua");
        assert_eq!(report.created_script.as_deref(), Some(expected.as_path()));
        assert!(expected.is_file(), "starter script written");
        let body = std::fs::read_to_string(&expected).expect("read script");
        assert!(
            body.contains("citadel.on_rpc(\"ping\""),
            "starter has a ping rpc"
        );
        assert!(body.contains("citadel.on_message"), "starter has the relay");
        // Database was already set, so no DB prompt was asked.
        assert!(report.selected_database.is_none());
        // A scripted project is GameScript-dependent: the readiness gate is
        // enabled and persisted so the next run enforces it non-interactively.
        assert!(config.runtime.require_script, "wizard enables the gate");
        assert!(report.persisted_config, "gate choice is persisted");
        let persisted = Config::from_file(&paths.config_path).expect("read persisted config");
        assert!(persisted.runtime.require_script);
        std::fs::remove_dir_all(&base).ok();
    }

    #[cfg(feature = "runtime-python")]
    #[test]
    fn wizard_creates_python_script_when_selected() {
        let base = unique_temp_dir("python-script");
        let scripts = base.join("game");
        let mut config = Config::default();
        config.database.url = Some(DEFAULT_SQLITE_URL.to_string());
        let paths = WizardPaths {
            config_path: base.join("citadel.toml"),
            scripts_dir: scripts.clone(),
        };
        let mut prompt = ScriptedPrompt::new(vec![true], vec![1], vec![]);
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("wizard runs");
        let expected = scripts.join("main.py");
        assert_eq!(report.created_script.as_deref(), Some(expected.as_path()));
        let body = std::fs::read_to_string(&expected).expect("read script");
        assert!(body.contains("import citadel"));
        assert!(body.contains("@citadel.on_message"));
        assert!(body.contains("@citadel.on_rpc(\"ping\")"));
        assert!(report.selected_database.is_none());
        std::fs::remove_dir_all(&base).ok();
    }

    #[cfg(feature = "runtime-js")]
    #[test]
    fn wizard_creates_javascript_script_when_selected() {
        let base = unique_temp_dir("js-script");
        let scripts = base.join("game");
        let mut config = Config::default();
        config.database.url = Some(DEFAULT_SQLITE_URL.to_string());
        let paths = WizardPaths {
            config_path: base.join("citadel.toml"),
            scripts_dir: scripts.clone(),
        };
        let js_index = ScriptLanguage::all()
            .iter()
            .position(|language| *language == ScriptLanguage::Js)
            .expect("js option is offered in runtime-js builds");
        let mut prompt = ScriptedPrompt::new(vec![true], vec![js_index], vec![]);
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("wizard runs");
        let expected = scripts.join("main.js");
        assert_eq!(report.created_script.as_deref(), Some(expected.as_path()));
        let body = std::fs::read_to_string(&expected).expect("read script");
        assert!(body.contains("citadel.on_message"));
        assert!(body.contains("citadel.on_rpc(\"ping\""));
        assert!(body.contains("QuickJS"));
        assert!(report.selected_database.is_none());
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn wizard_treats_existing_python_entrypoint_as_script() {
        let base = unique_temp_dir("existing-python-script");
        let scripts = base.join("game");
        std::fs::create_dir_all(&scripts).expect("scripts");
        std::fs::write(scripts.join("main.py"), "# present").expect("script");
        let mut config = Config::default();
        config.database.url = Some(DEFAULT_SQLITE_URL.to_string());
        let paths = WizardPaths {
            config_path: base.join("citadel.toml"),
            scripts_dir: scripts,
        };
        let mut prompt = ScriptedPrompt::new(vec![], vec![], vec![]);
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("wizard runs");
        assert!(report.created_script.is_none());
        assert!(report.selected_database.is_none());
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn wizard_treats_existing_javascript_entrypoint_as_script() {
        let base = unique_temp_dir("existing-js-script");
        let scripts = base.join("game");
        std::fs::create_dir_all(&scripts).expect("scripts");
        std::fs::write(scripts.join("main.js"), "// present").expect("script");
        let mut config = Config::default();
        config.database.url = Some(DEFAULT_SQLITE_URL.to_string());
        let paths = WizardPaths {
            config_path: base.join("citadel.toml"),
            scripts_dir: scripts,
        };
        let mut prompt = ScriptedPrompt::new(vec![], vec![], vec![]);
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("wizard runs");
        assert!(report.created_script.is_none());
        assert!(report.selected_database.is_none());
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn wizard_selects_sqlite_and_persists_config() {
        let base = unique_temp_dir("sqlite");
        std::fs::create_dir_all(&base).expect("base");
        let scripts = base.join("game");
        // Script already present so only the DB step runs.
        std::fs::create_dir_all(&scripts).expect("scripts");
        std::fs::write(scripts.join("main.lua"), "-- present").expect("script");
        let mut config = Config::default();
        assert!(!config.database.is_enabled());
        let config_path = base.join("citadel.toml");
        let paths = WizardPaths {
            config_path: config_path.clone(),
            scripts_dir: scripts,
        };
        // confirm: yes (init db); choose: index 0 (SQLite).
        let mut prompt = ScriptedPrompt::new(vec![true], vec![0], vec![]);
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("wizard runs");
        assert_eq!(report.selected_database, Some(DatabaseChoice::Sqlite));
        assert!(report.persisted_config);
        assert_eq!(config.database.url.as_deref(), Some(DEFAULT_SQLITE_URL));
        // The persisted file round-trips to a config with the SQLite URL.
        let reloaded = Config::from_file(&config_path).expect("reload persisted config");
        assert_eq!(reloaded.database.url.as_deref(), Some(DEFAULT_SQLITE_URL));
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn wizard_captures_postgres_url() {
        let base = unique_temp_dir("pg");
        std::fs::create_dir_all(&base).expect("base");
        let scripts = base.join("game");
        std::fs::create_dir_all(&scripts).expect("scripts");
        std::fs::write(scripts.join("main.lua"), "-- present").expect("script");
        let mut config = Config::default();
        let paths = WizardPaths {
            config_path: base.join("citadel.toml"),
            scripts_dir: scripts,
        };
        let custom = "postgres://user:pw@db.example:5432/prod".to_string();
        // confirm: yes; choose: index 1 (Postgres); ask: the custom URL.
        let mut prompt = ScriptedPrompt::new(vec![true], vec![1], vec![custom.clone()]);
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("wizard runs");
        assert_eq!(report.selected_database, Some(DatabaseChoice::Postgres));
        assert_eq!(config.database.url.as_deref(), Some(custom.as_str()));
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn wizard_captures_mongodb_url() {
        let base = unique_temp_dir("mongodb");
        std::fs::create_dir_all(&base).expect("base");
        let scripts = base.join("game");
        std::fs::create_dir_all(&scripts).expect("scripts");
        std::fs::write(scripts.join("main.lua"), "-- present").expect("script");
        let mut config = Config::default();
        let paths = WizardPaths {
            config_path: base.join("citadel.toml"),
            scripts_dir: scripts,
        };
        let custom = "mongodb://db-1,db-2/citadel?replicaSet=rs0".to_string();
        // confirm: yes; choose: index 2 (MongoDB); ask: the custom URL.
        let mut prompt = ScriptedPrompt::new(vec![true], vec![2], vec![custom.clone()]);
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("wizard runs");
        assert_eq!(report.selected_database, Some(DatabaseChoice::MongoDb));
        assert_eq!(config.database.url.as_deref(), Some(custom.as_str()));
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn wizard_declining_both_prompts_changes_nothing() {
        let base = unique_temp_dir("decline");
        let scripts = base.join("game");
        let mut config = Config::default();
        let paths = WizardPaths {
            config_path: base.join("citadel.toml"),
            scripts_dir: scripts.clone(),
        };
        // Both confirms are "no".
        let mut prompt = ScriptedPrompt::new(vec![false, false], vec![], vec![]);
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("wizard runs");
        assert!(!report.made_changes());
        assert!(
            !scripts.exists(),
            "declining must not create the scripts dir"
        );
        assert!(
            !paths.config_path.exists(),
            "declining must not persist config"
        );
        assert!(config.database.url.is_none());
    }

    #[test]
    fn wizard_skips_prompts_when_script_and_db_exist() {
        let base = unique_temp_dir("exists");
        std::fs::create_dir_all(&base).expect("base");
        let scripts = base.join("game");
        std::fs::create_dir_all(&scripts).expect("scripts");
        std::fs::write(scripts.join("main.lua"), "-- present").expect("script");
        let mut config = Config::default();
        config.database.url = Some(DEFAULT_SQLITE_URL.to_string());
        let paths = WizardPaths {
            config_path: base.join("citadel.toml"),
            scripts_dir: scripts,
        };
        // NeverPrompt panics if consulted; nothing should be asked.
        let mut prompt = NeverPrompt;
        let report = run_first_run_wizard(&mut config, &paths, &mut prompt).expect("no prompts");
        assert!(!report.made_changes());
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn should_run_wizard_is_false_for_explicit_config_or_yes() {
        // Regardless of the terminal, an explicit --config or --yes disables it.
        assert!(!should_run_wizard(true, false));
        assert!(!should_run_wizard(false, true));
    }
}
