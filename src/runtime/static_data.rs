//! Bounded, read-only static gameplay data for embedded runtimes.
//!
//! A [`StaticDataCatalog`] is deliberately narrower than a filesystem API:
//! callers name a relative JSON or CSV file below one canonical operator-owned
//! root, the catalog validates and parses it once during runtime initialization,
//! and later reads are in-memory cache hits. It never writes the root.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Take};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use csv::{ReaderBuilder, StringRecord, Trim};
use serde_json::{Map, Number, Value};
use thiserror::Error;

use crate::error::{AppError, AppResult};

/// Default maximum bytes read for one static-data file (1 MiB).
pub const DEFAULT_STATIC_DATA_MAX_FILE_BYTES: usize = 1024 * 1024;

/// The format the caller expects for a static-data file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaticDataFormat {
    /// A JSON object or array.
    Json,
    /// A header-keyed CSV table.
    Csv,
}

impl StaticDataFormat {
    const fn extension(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Csv => "csv",
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Json => "JSON",
            Self::Csv => "CSV",
        }
    }
}

/// A clear, script-safe static-data failure. Messages name only the relative
/// requested file; they never disclose the host's configured root.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StaticDataError {
    /// The runtime was not configured with a static-data root.
    #[error("static data access denied: runtime.static_data_dir is not configured")]
    NotConfigured,
    /// A requested path is not an allowed relative data path.
    #[error("static data access denied: {0}")]
    AccessDenied(String),
    /// The requested data file does not exist.
    #[error("static data file not found: {0}")]
    Missing(String),
    /// The configured size limit would be exceeded.
    #[error(
        "static data file exceeds configured size limit: {path} ({size} bytes > {limit} bytes)"
    )]
    TooLarge {
        /// Relative requested path.
        path: String,
        /// Observed file size.
        size: u64,
        /// Configured maximum.
        limit: usize,
    },
    /// JSON could not be decoded.
    #[error("invalid JSON static data in {path}: {detail}")]
    InvalidJson {
        /// Relative requested path.
        path: String,
        /// Parser summary.
        detail: String,
    },
    /// CSV could not be decoded as UTF-8/RFC 4180 data.
    #[error("invalid CSV static data in {path}: {detail}")]
    InvalidCsv {
        /// Relative requested path.
        path: String,
        /// Parser summary.
        detail: String,
    },
    /// The decoded file cannot represent the stable static-data schema.
    #[error("static data schema invalid in {path}: {detail}")]
    SchemaInvalid {
        /// Relative requested path.
        path: String,
        /// Schema rule that failed.
        detail: String,
    },
}

#[derive(Debug, Clone)]
struct CachedData {
    canonical_path: PathBuf,
    value: Arc<Value>,
}

#[derive(Debug, Default)]
struct CatalogState {
    entries: BTreeMap<String, CachedData>,
    sealed: bool,
}

/// A cache scoped to one configured, canonical data root.
///
/// A disabled catalog still exists for every Lua VM so the host API can return a
/// useful denial rather than exposing a conditional/nil global. A configured
/// catalog is sealed after script initialization; a cache miss after sealing is
/// refused, guaranteeing that message and tick paths cannot trigger disk I/O.
#[derive(Debug, Clone)]
pub struct StaticDataCatalog {
    root: Option<PathBuf>,
    max_file_bytes: usize,
    state: Arc<Mutex<CatalogState>>,
}

impl StaticDataCatalog {
    /// Construct a catalog from an optional configuration root.
    ///
    /// A configured root must already be a directory. Citadel deliberately does
    /// not create it because static data is operator-owned, read-only content.
    pub fn new(root: Option<&Path>, max_file_bytes: usize) -> AppResult<Self> {
        if root.is_some() && max_file_bytes == 0 {
            return Err(AppError::config(
                "runtime.static_data_max_file_bytes must be >= 1 when static data is configured",
            ));
        }
        let root = match root {
            None => None,
            Some(root) => Some(root.canonicalize().map_err(|error| {
                AppError::config("cannot access runtime.static_data_dir")
                    .with_detail(error.to_string())
            })?),
        };
        if root.as_ref().is_some_and(|root| !root.is_dir()) {
            return Err(AppError::config(
                "runtime.static_data_dir must resolve to a directory",
            ));
        }
        Ok(Self {
            root,
            max_file_bytes,
            state: Arc::new(Mutex::new(CatalogState::default())),
        })
    }

    /// Load one JSON object/array, returning a cached parsed value.
    pub fn load_json(&self, relative_path: &str) -> Result<Arc<Value>, StaticDataError> {
        self.load(relative_path, StaticDataFormat::Json)
    }

    /// Load one CSV table as an array of header-keyed JSON objects.
    pub fn load_csv(&self, relative_path: &str) -> Result<Arc<Value>, StaticDataError> {
        self.load(relative_path, StaticDataFormat::Csv)
    }

    /// Return canonical paths successfully loaded by the initialization script.
    /// Used solely by the development hot-reload watcher.
    pub fn loaded_paths(&self) -> Vec<PathBuf> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .entries
            .values()
            .map(|entry| entry.canonical_path.clone())
            .collect()
    }

    /// Prevent cache misses after the runtime initialization body completes.
    pub fn seal(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.sealed = true;
    }

    fn load(
        &self,
        requested_path: &str,
        format: StaticDataFormat,
    ) -> Result<Arc<Value>, StaticDataError> {
        let root = self.root.as_deref().ok_or(StaticDataError::NotConfigured)?;
        let relative = validate_relative_path(requested_path, format)?;
        let key = relative.to_string_lossy().replace('\\', "/");
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = state.entries.get(&key) {
            return Ok(Arc::clone(&entry.value));
        }
        if state.sealed {
            return Err(StaticDataError::AccessDenied(format!(
                "{key} was not loaded during script initialization"
            )));
        }

        let canonical_path = resolve_under_root(root, &relative, &key)?;
        let bytes = read_bounded(&canonical_path, &key, self.max_file_bytes)?;
        let value = match format {
            StaticDataFormat::Json => parse_json(&bytes, &key)?,
            StaticDataFormat::Csv => parse_csv(&bytes, &key)?,
        };
        let value = Arc::new(value);
        state.entries.insert(
            key,
            CachedData {
                canonical_path,
                value: Arc::clone(&value),
            },
        );
        Ok(value)
    }
}

fn validate_relative_path(
    requested_path: &str,
    format: StaticDataFormat,
) -> Result<PathBuf, StaticDataError> {
    if requested_path.is_empty() {
        return Err(StaticDataError::AccessDenied(
            "path must not be empty".to_string(),
        ));
    }
    // Lua uses `/` as the one portable separator. Reject Windows separators and
    // drive-qualified forms even while Citadel happens to run on Unix: accepting
    // them there as literal file-name characters would make the security
    // contract platform-dependent and could turn into traversal after a move.
    if requested_path.contains('\\') {
        return Err(StaticDataError::AccessDenied(
            "path must use '/' separators".to_string(),
        ));
    }
    if requested_path
        .as_bytes()
        .get(1)
        .is_some_and(|character| *character == b':')
        && requested_path
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
    {
        return Err(StaticDataError::AccessDenied(
            "path must be relative to runtime.static_data_dir".to_string(),
        ));
    }
    let input = Path::new(requested_path);
    if input.is_absolute() {
        return Err(StaticDataError::AccessDenied(
            "path must be relative to runtime.static_data_dir".to_string(),
        ));
    }
    let mut relative = PathBuf::new();
    for component in input.components() {
        match component {
            Component::Normal(part) if !part.is_empty() => relative.push(part),
            Component::Normal(_) => {
                return Err(StaticDataError::AccessDenied(
                    "path must name a data file".to_string(),
                ));
            }
            Component::CurDir => {
                return Err(StaticDataError::AccessDenied(
                    "path must not contain '.' components".to_string(),
                ));
            }
            Component::ParentDir => {
                return Err(StaticDataError::AccessDenied(
                    "path traversal ('..') is not allowed".to_string(),
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(StaticDataError::AccessDenied(
                    "path must be relative to runtime.static_data_dir".to_string(),
                ));
            }
        }
    }
    if relative.as_os_str().is_empty() {
        return Err(StaticDataError::AccessDenied(
            "path must name a data file".to_string(),
        ));
    }
    let matches_format = relative
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case(format.extension()));
    if !matches_format {
        return Err(StaticDataError::AccessDenied(format!(
            "{} loader accepts only .{} files",
            format.label(),
            format.extension()
        )));
    }
    Ok(relative)
}

fn resolve_under_root(
    canonical_root: &Path,
    relative: &Path,
    display_path: &str,
) -> Result<PathBuf, StaticDataError> {
    let candidate = canonical_root.join(relative);
    let canonical = candidate.canonicalize().map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            StaticDataError::Missing(display_path.to_string())
        } else {
            StaticDataError::AccessDenied(format!("cannot open {display_path}"))
        }
    })?;
    if !canonical.starts_with(canonical_root) {
        return Err(StaticDataError::AccessDenied(format!(
            "{display_path} resolves outside runtime.static_data_dir"
        )));
    }
    let metadata = std::fs::metadata(&canonical)
        .map_err(|_| StaticDataError::AccessDenied(format!("cannot inspect {display_path}")))?;
    if !metadata.is_file() {
        return Err(StaticDataError::AccessDenied(format!(
            "{display_path} must be a regular file"
        )));
    }
    Ok(canonical)
}

fn read_bounded(
    canonical_path: &Path,
    display_path: &str,
    max_file_bytes: usize,
) -> Result<Vec<u8>, StaticDataError> {
    let metadata = std::fs::metadata(canonical_path)
        .map_err(|_| StaticDataError::AccessDenied(format!("cannot inspect {display_path}")))?;
    let limit = u64::try_from(max_file_bytes).unwrap_or(u64::MAX);
    if metadata.len() > limit {
        return Err(StaticDataError::TooLarge {
            path: display_path.to_string(),
            size: metadata.len(),
            limit: max_file_bytes,
        });
    }
    let file = File::open(canonical_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            StaticDataError::Missing(display_path.to_string())
        } else {
            StaticDataError::AccessDenied(format!("cannot read {display_path}"))
        }
    })?;
    let mut reader: Take<File> = file.take(limit.saturating_add(1));
    let capacity = usize::try_from(metadata.len())
        .unwrap_or(max_file_bytes)
        .min(max_file_bytes);
    let mut bytes = Vec::with_capacity(capacity);
    reader
        .read_to_end(&mut bytes)
        .map_err(|_| StaticDataError::AccessDenied(format!("cannot read {display_path}")))?;
    if bytes.len() > max_file_bytes {
        return Err(StaticDataError::TooLarge {
            path: display_path.to_string(),
            size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            limit: max_file_bytes,
        });
    }
    Ok(bytes)
}

fn parse_json(bytes: &[u8], path: &str) -> Result<Value, StaticDataError> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|error| StaticDataError::InvalidJson {
            path: path.to_string(),
            detail: error.to_string(),
        })?;
    if !value.is_object() && !value.is_array() {
        return Err(StaticDataError::SchemaInvalid {
            path: path.to_string(),
            detail: "JSON root must be an object or array".to_string(),
        });
    }
    Ok(value)
}

fn parse_csv(bytes: &[u8], path: &str) -> Result<Value, StaticDataError> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .trim(Trim::All)
        .from_reader(bytes);
    let headers = reader
        .headers()
        .map_err(|error| StaticDataError::InvalidCsv {
            path: path.to_string(),
            detail: error.to_string(),
        })?;
    validate_headers(headers, path)?;
    let headers: Vec<String> = headers.iter().map(ToOwned::to_owned).collect();
    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record.map_err(|error| StaticDataError::InvalidCsv {
            path: path.to_string(),
            detail: error.to_string(),
        })?;
        let mut row = Map::new();
        for (header, cell) in headers.iter().zip(record.iter()) {
            row.insert(header.clone(), infer_csv_value(cell));
        }
        rows.push(Value::Object(row));
    }
    Ok(Value::Array(rows))
}

fn validate_headers(headers: &StringRecord, path: &str) -> Result<(), StaticDataError> {
    if headers.is_empty() {
        return Err(StaticDataError::SchemaInvalid {
            path: path.to_string(),
            detail: "CSV must have a non-empty header row".to_string(),
        });
    }
    let mut seen = BTreeMap::new();
    for header in headers.iter() {
        if header.is_empty() {
            return Err(StaticDataError::SchemaInvalid {
                path: path.to_string(),
                detail: "CSV header names must not be empty".to_string(),
            });
        }
        if seen.insert(header, ()).is_some() {
            return Err(StaticDataError::SchemaInvalid {
                path: path.to_string(),
                detail: format!("CSV header '{header}' is duplicated"),
            });
        }
    }
    Ok(())
}

fn infer_csv_value(cell: &str) -> Value {
    match cell {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => cell
            .parse::<i64>()
            .map(|number| Value::Number(Number::from(number)))
            .or_else(|_| {
                cell.parse::<u64>()
                    .map(|number| Value::Number(Number::from(number)))
            })
            .or_else(|_| {
                cell.parse::<f64>()
                    .ok()
                    .filter(|number| number.is_finite())
                    .and_then(Number::from_f64)
                    .map(Value::Number)
                    .ok_or(())
            })
            .unwrap_or_else(|_| Value::String(cell.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "citadel-static-data-{label}-{}-{n}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn write(&self, name: &str, data: &[u8]) {
            let path = self.0.join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("create parent");
            }
            std::fs::write(path, data).expect("write fixture");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn json_and_csv_are_parsed_and_cached_before_sealing() {
        let dir = TempDir::new("parse-cache");
        dir.write("gameplay/collision.json", br#"{"hitbox":{"radius":4}}"#);
        dir.write(
            "gameplay/balance.csv",
            b"id,damage,enabled\nslash,12,true\n",
        );
        let catalog = StaticDataCatalog::new(Some(&dir.0), 1024).expect("catalog");

        let json = catalog
            .load_json("gameplay/collision.json")
            .expect("valid JSON");
        assert_eq!(json["hitbox"]["radius"], 4);
        let csv = catalog.load_csv("gameplay/balance.csv").expect("valid CSV");
        assert_eq!(csv[0]["damage"], 12);
        assert_eq!(csv[0]["enabled"], true);

        dir.write("gameplay/collision.json", br#"{"hitbox":{"radius":99}}"#);
        assert_eq!(
            catalog
                .load_json("gameplay/collision.json")
                .expect("cache hit")["hitbox"]["radius"],
            4,
            "a loaded file is kept in memory and never re-read by this catalog"
        );
        catalog.seal();
        let error = catalog
            .load_json("gameplay/new.json")
            .expect_err("post-init cache misses must not perform I/O");
        assert!(error.to_string().contains("initialization"));
    }

    #[test]
    fn invalid_content_schema_and_size_have_clear_errors() {
        let dir = TempDir::new("invalid");
        dir.write("bad.json", b"{");
        dir.write("scalar.json", b"42");
        dir.write("bad.csv", b"id,id\na,b\n");
        dir.write("wide.csv", b"id,damage\na,1,extra\n");
        dir.write("large.json", br#"{"value":"too large"}"#);
        let catalog = StaticDataCatalog::new(Some(&dir.0), 1024).expect("catalog");

        assert!(matches!(
            catalog.load_json("bad.json"),
            Err(StaticDataError::InvalidJson { .. })
        ));
        assert!(matches!(
            catalog.load_json("scalar.json"),
            Err(StaticDataError::SchemaInvalid { .. })
        ));
        assert!(matches!(
            catalog.load_csv("bad.csv"),
            Err(StaticDataError::SchemaInvalid { .. })
        ));
        assert!(matches!(
            catalog.load_csv("wide.csv"),
            Err(StaticDataError::InvalidCsv { .. })
        ));
        let bounded = StaticDataCatalog::new(Some(&dir.0), 16).expect("bounded catalog");
        assert!(matches!(
            bounded.load_json("large.json"),
            Err(StaticDataError::TooLarge { .. })
        ));
    }

    #[test]
    fn malicious_paths_and_wrong_extensions_are_denied() {
        let dir = TempDir::new("paths");
        dir.write("safe.json", b"{}");
        let catalog = StaticDataCatalog::new(Some(&dir.0), 1024).expect("catalog");
        for path in [
            "",
            "../outside.json",
            "..\\outside.json",
            "./safe.json",
            "safe.csv",
            "C:\\temp\\x.json",
            "C:/temp/x.json",
        ] {
            let error = catalog.load_json(path).expect_err("path must be denied");
            assert!(matches!(error, StaticDataError::AccessDenied(_)), "{path}");
        }
        assert!(matches!(
            catalog.load_json("missing.json"),
            Err(StaticDataError::Missing(_))
        ));
    }

    #[test]
    fn disabled_catalog_allows_an_unused_zero_limit() {
        let catalog = StaticDataCatalog::new(None, 0).expect("disabled catalog");
        assert!(matches!(
            catalog.load_json("gameplay/collision.json"),
            Err(StaticDataError::NotConfigured)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_that_escapes_the_root_is_denied() {
        use std::os::unix::fs::symlink;

        let root = TempDir::new("symlink-root");
        let outside = TempDir::new("symlink-outside");
        outside.write("outside.json", b"{}");
        symlink(outside.0.join("outside.json"), root.0.join("escape.json"))
            .expect("create fixture symlink");
        let catalog = StaticDataCatalog::new(Some(&root.0), 1024).expect("catalog");
        assert!(matches!(
            catalog.load_json("escape.json"),
            Err(StaticDataError::AccessDenied(_))
        ));
    }

    #[cfg(windows)]
    #[test]
    fn windows_symlink_that_escapes_the_root_is_denied() {
        use std::os::windows::fs::symlink_file;

        let root = TempDir::new("symlink-root");
        let outside = TempDir::new("symlink-outside");
        outside.write("outside.json", b"{}");
        if let Err(error) = symlink_file(outside.0.join("outside.json"), root.0.join("escape.json"))
        {
            // Windows machines without Developer Mode or an elevated token cannot
            // create a symlink. The identical containment implementation is
            // exercised on Unix CI; do not turn a platform policy into a false
            // product failure on this host.
            // Windows reports ERROR_PRIVILEGE_NOT_HELD (1314) as
            // `Uncategorized` on some Rust/std combinations rather than
            // `PermissionDenied`. In either representation, the host cannot
            // create the fixture without Developer Mode or an elevated token.
            if error.kind() == std::io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(1314)
            {
                return;
            }
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::PermissionDenied,
                "create fixture symlink"
            );
            return;
        }
        let catalog = StaticDataCatalog::new(Some(&root.0), 1024).expect("catalog");
        assert!(matches!(
            catalog.load_json("escape.json"),
            Err(StaticDataError::AccessDenied(_))
        ));
    }
}
