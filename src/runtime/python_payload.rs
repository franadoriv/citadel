//! Verified, cacheable Python resource payloads.
//!
//! This module deliberately does **not** discover, extract, or load `libpython`.
//! It provides the portable payload/cache seam that a later approved launcher may
//! use for resources which are not needed before `main`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// The fixed payload magic. The next byte is the wire-format version.
pub const PAYLOAD_MAGIC: &[u8; 8] = b"CITPYLD1";
pub const PAYLOAD_VERSION: u8 = 1;
pub const READY_FILE: &str = "READY";
const MAX_MANIFEST_BYTES: usize = 1 << 20;
const MAX_PATH_BYTES: usize = 4096;
/// This seam deliberately retains verified bytes until `publish`. Keep the
/// authenticated upper bound small enough that a valid-but-hostile payload
/// cannot exhaust the process; a future streaming-to-staging implementation
/// may raise this independently of process memory.
pub const MAX_IN_MEMORY_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
const LOCK_STALE_AFTER: Duration = Duration::from_secs(15 * 60);

/// Release signing key. Its private counterpart is intentionally not present at runtime.
/// This is a stable test/release placeholder until the release pipeline supplies its key.
pub const EMBEDDED_MANIFEST_PUBLIC_KEY: [u8; 32] = [
    0xd7, 0x5a, 0x98, 0x01, 0x82, 0xb1, 0x0a, 0xb7, 0xd5, 0x4b, 0xfe, 0xd3, 0xc9, 0x64, 0x07, 0x3a,
    0x0e, 0xe1, 0x72, 0xf3, 0xda, 0xa6, 0x23, 0x25, 0xaf, 0x02, 0x1a, 0x68, 0xf7, 0x07, 0x51, 0x1a,
];

#[derive(Debug, Error)]
pub enum PythonPayloadError {
    #[error("payload is not a supported CITPYLD1 document")]
    InvalidMagic,
    #[error("payload manifest is invalid: {0}")]
    Manifest(String),
    #[error("payload manifest signature is invalid")]
    BadSignature,
    #[error("payload entry {0:?} is invalid")]
    InvalidPath(PathBuf),
    #[error("payload entry set is not exactly the signed manifest")]
    EntrySetMismatch,
    #[error("payload entry {path:?} exceeds a declared limit or has the wrong size/hash")]
    Integrity { path: PathBuf },
    #[error("payload is truncated or has trailing data")]
    Framing,
    #[error("payload compression {0:?} is unsupported")]
    UnsupportedCompression(String),
    #[error("libpython is out of scope for Python payload resources")]
    LibpythonOutOfScope,
    #[error("cache operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("cache lock for {0} is already held")]
    Busy(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PayloadFile {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

/// Signed payload metadata. The signature is over [`Self::canonical_bytes`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PayloadManifest {
    pub schema: String,
    pub version: u32,
    pub target: String,
    pub abi: String,
    pub compression: String,
    pub max_file_bytes: u64,
    pub max_total_bytes: u64,
    pub files: Vec<PayloadFile>,
    pub signature: String,
}

impl PayloadManifest {
    fn canonical_bytes(&self) -> Result<Vec<u8>, PythonPayloadError> {
        #[derive(Serialize)]
        struct Signed<'a> {
            schema: &'a str,
            version: u32,
            target: &'a str,
            abi: &'a str,
            compression: &'a str,
            max_file_bytes: u64,
            max_total_bytes: u64,
            files: &'a [PayloadFile],
        }
        serde_json::to_vec(&Signed {
            schema: &self.schema,
            version: self.version,
            target: &self.target,
            abi: &self.abi,
            compression: &self.compression,
            max_file_bytes: self.max_file_bytes,
            max_total_bytes: self.max_total_bytes,
            files: &self.files,
        })
        .map_err(|e| PythonPayloadError::Manifest(e.to_string()))
    }

    pub fn verify(&self) -> Result<(), PythonPayloadError> {
        if self.schema != "citadel.python.payload"
            || self.version != 1
            || self.target.is_empty()
            || self.abi.is_empty()
            || self.compression != "none"
            || self.files.is_empty()
            || self.max_file_bytes == 0
            || self.max_total_bytes == 0
            || self.max_total_bytes > MAX_IN_MEMORY_PAYLOAD_BYTES
        {
            return Err(PythonPayloadError::Manifest(
                "unsupported schema/version/fields".into(),
            ));
        }
        let key = VerifyingKey::from_bytes(&EMBEDDED_MANIFEST_PUBLIC_KEY)
            .map_err(|_| PythonPayloadError::BadSignature)?;
        let bytes = STANDARD_NO_PAD
            .decode(&self.signature)
            .map_err(|_| PythonPayloadError::BadSignature)?;
        let signature =
            Signature::from_slice(&bytes).map_err(|_| PythonPayloadError::BadSignature)?;
        key.verify(&self.canonical_bytes()?, &signature)
            .map_err(|_| PythonPayloadError::BadSignature)?;
        let mut paths = BTreeSet::new();
        let mut total = 0_u64;
        for file in &self.files {
            validate_relative_path(&file.path)?;
            if is_libpython_candidate(Path::new(&file.path)) {
                return Err(PythonPayloadError::LibpythonOutOfScope);
            }
            if !paths.insert(&file.path) || file.size > self.max_file_bytes {
                return Err(PythonPayloadError::EntrySetMismatch);
            }
            total = total
                .checked_add(file.size)
                .ok_or_else(|| PythonPayloadError::Manifest("size overflow".into()))?;
            if total > self.max_total_bytes || !is_sha256(&file.sha256) {
                return Err(PythonPayloadError::EntrySetMismatch);
            }
        }
        if !self
            .files
            .windows(2)
            .all(|pair| pair[0].path.as_bytes() < pair[1].path.as_bytes())
        {
            return Err(PythonPayloadError::Manifest(
                "file entries are not in canonical path order".into(),
            ));
        }
        Ok(())
    }

    pub fn digest(&self) -> Result<String, PythonPayloadError> {
        Ok(hex_digest(&self.canonical_bytes()?))
    }
}

/// A fully verified in-memory payload. No filesystem input is trusted by this type.
#[derive(Debug, Clone)]
pub struct VerifiedPayload {
    pub manifest: PayloadManifest,
    files: BTreeMap<PathBuf, Vec<u8>>,
}

impl VerifiedPayload {
    pub fn parse(bytes: &[u8]) -> Result<Self, PythonPayloadError> {
        Self::parse_reader(Cursor::new(bytes))
    }

    /// Parse from an arbitrary stream. Each entry is read and hashed in bounded
    /// chunks before it can be cached; the signed aggregate has a strict 64 MiB
    /// process-memory ceiling (`MAX_IN_MEMORY_PAYLOAD_BYTES`).
    pub fn parse_reader(mut input: impl Read) -> Result<Self, PythonPayloadError> {
        let mut magic = [0; 8];
        input
            .read_exact(&mut magic)
            .map_err(|_| PythonPayloadError::Framing)?;
        if &magic != PAYLOAD_MAGIC {
            return Err(PythonPayloadError::InvalidMagic);
        }
        let manifest_len = read_u32(&mut input)? as usize;
        if manifest_len > MAX_MANIFEST_BYTES {
            return Err(PythonPayloadError::Framing);
        }
        let mut raw_manifest = vec![0; manifest_len];
        input
            .read_exact(&mut raw_manifest)
            .map_err(|_| PythonPayloadError::Framing)?;
        let manifest: PayloadManifest = serde_json::from_slice(&raw_manifest)
            .map_err(|e| PythonPayloadError::Manifest(e.to_string()))?;
        manifest.verify()?;
        let expected: BTreeMap<_, _> = manifest
            .files
            .iter()
            .map(|f| (PathBuf::from(&f.path), f))
            .collect();
        let mut files = BTreeMap::new();
        while let Some(path_len) = read_optional_u16(&mut input)? {
            let path_len = path_len as usize;
            if path_len == 0 || path_len > MAX_PATH_BYTES {
                return Err(PythonPayloadError::Framing);
            }
            let mut raw_path = vec![0; path_len];
            input
                .read_exact(&mut raw_path)
                .map_err(|_| PythonPayloadError::Framing)?;
            let path = String::from_utf8(raw_path).map_err(|_| PythonPayloadError::Framing)?;
            validate_relative_path(&path)?;
            let size = read_u64(&mut input)?;
            let file = expected
                .get(Path::new(&path))
                .ok_or(PythonPayloadError::EntrySetMismatch)?;
            if size != file.size || size > manifest.max_file_bytes || size > usize::MAX as u64 {
                return Err(PythonPayloadError::Integrity { path: path.into() });
            }
            let (data, digest) = read_and_hash(&mut input, size)?;
            if digest != file.sha256 || files.insert(PathBuf::from(path), data).is_some() {
                return Err(PythonPayloadError::Integrity {
                    path: file.path.clone().into(),
                });
            }
        }
        if files.len() != expected.len() || files.keys().any(|p| !expected.contains_key(p)) {
            return Err(PythonPayloadError::EntrySetMismatch);
        }
        tracing::debug!(event = "python_payload.verify", status = "accepted");
        Ok(Self { manifest, files })
    }

    /// Materialize only verified regular files through same-filesystem staging and atomic rename.
    pub fn publish(&self, cache: &PayloadCache) -> Result<PathBuf, PythonPayloadError> {
        let digest = self.manifest.digest()?;
        let final_dir = cache.path_for(&self.manifest.target, &self.manifest.abi, &digest)?;
        let parent = final_dir
            .parent()
            .ok_or_else(|| PythonPayloadError::Manifest("cache root has no parent".into()))?;
        create_private_dir_tree(parent)?;
        ensure_private_cache_path(&cache.root, parent)?;
        if ready_tree_is_valid(&final_dir, &self.manifest, &digest) {
            return Ok(final_dir);
        }
        let _lock = CacheLock::acquire(&final_dir.with_extension("lock"))?;
        ensure_private_cache_path(&cache.root, parent)?;
        if ready_tree_is_valid(&final_dir, &self.manifest, &digest) {
            return Ok(final_dir);
        }
        recover_staging(parent, &digest)?;
        let staging = parent.join(format!(".staging-{digest}-{}", std::process::id()));
        fs::create_dir(&staging)?;
        set_private_dir(&staging)?;
        sync_dir(parent)?;
        let result = (|| {
            for (relative, data) in &self.files {
                let destination = secure_destination(&staging, relative)?;
                if let Some(dir) = destination.parent() {
                    create_private_dir_tree(dir)?;
                }
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&destination)?;
                file.write_all(data)?;
                file.sync_all()?;
                set_private_file(&destination)?;
                let destination_parent = destination.parent().ok_or_else(|| {
                    PythonPayloadError::Manifest("staging destination has no parent".into())
                })?;
                sync_dir(destination_parent)?;
            }
            let mut ready = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(staging.join(READY_FILE))?;
            ready.write_all(digest.as_bytes())?;
            ready.sync_all()?;
            // Windows refuses to rename the staging directory while the READY
            // file still has an open handle. Close it before atomic activation.
            drop(ready);
            set_private_file(&staging.join(READY_FILE))?;
            // Persist the READY directory entry before the atomic activation.
            sync_dir(&staging)?;
            fs::rename(&staging, &final_dir)?;
            // Persist the rename itself: a crash after this point must recover
            // either the old tree or this complete READY tree, never a partial.
            sync_dir(parent)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = remove_staging(&staging);
        }
        result.map(|()| {
            tracing::debug!(event = "python_payload.activate", status = "published");
            final_dir
        })
    }
}

#[derive(Debug, Clone)]
pub struct PayloadCache {
    root: PathBuf,
}
impl PayloadCache {
    /// Per-user cache root, never a system/global Python directory.
    pub fn for_current_user() -> Result<Self, PythonPayloadError> {
        let base = if cfg!(windows) {
            std::env::var_os("LOCALAPPDATA")
        } else {
            std::env::var_os("XDG_CACHE_HOME").or_else(|| {
                std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache").into_os_string())
            })
        }
        .ok_or_else(|| {
            PythonPayloadError::Manifest("cannot determine user cache directory".into())
        })?;
        Ok(Self {
            root: PathBuf::from(base).join("citadel").join("python-payloads"),
        })
    }
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }
    pub fn path_for(
        &self,
        target: &str,
        abi: &str,
        digest: &str,
    ) -> Result<PathBuf, PythonPayloadError> {
        validate_segment(target)?;
        validate_segment(abi)?;
        if !is_sha256(digest) {
            return Err(PythonPayloadError::Manifest("invalid cache digest".into()));
        }
        Ok(self.root.join(target).join(abi).join(digest))
    }
    /// Conservative cleanup only removes our own stale staging directories.
    pub fn cleanup_orphaned_staging(&self) -> Result<usize, PythonPayloadError> {
        let mut removed = 0;
        if !self.root.exists() {
            return Ok(0);
        }
        for entry in walk_dirs(&self.root)? {
            if entry
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|n| n.starts_with(".staging-"))
                && is_stale(&entry)?
            {
                remove_staging(&entry)?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

fn validate_relative_path(value: &str) -> Result<(), PythonPayloadError> {
    if value.is_empty()
        || value.len() > MAX_PATH_BYTES
        || value.contains('\\')
        || value.contains(':')
        || value.bytes().any(|byte| byte.is_ascii_control())
        || Path::new(value).is_absolute()
    {
        return Err(PythonPayloadError::InvalidPath(value.into()));
    }
    // Signed payload paths are canonical slash-delimited wire values. Building
    // a PathBuf from their components rewrites '/' to '\\' on Windows, so use
    // the wire separator to validate instead of comparing platform paths.
    if value
        .split('/')
        .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(PythonPayloadError::InvalidPath(value.into()));
    }
    Ok(())
}
fn validate_segment(value: &str) -> Result<(), PythonPayloadError> {
    if value.is_empty()
        || value.len() > 255
        || value.contains(['/', '\\'])
        || value.bytes().any(|byte| byte.is_ascii_control())
        || Path::new(value).components().count() != 1
        || !matches!(
            Path::new(value).components().next(),
            Some(Component::Normal(_))
        )
    {
        Err(PythonPayloadError::InvalidPath(value.into()))
    } else {
        Ok(())
    }
}
fn is_libpython_candidate(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return true;
    };
    let name = name.to_ascii_lowercase();
    name.starts_with("libpython") || (name.starts_with("python") && name.ends_with(".dll"))
}
fn secure_destination(root: &Path, relative: &Path) -> Result<PathBuf, PythonPayloadError> {
    validate_relative_path(&relative.to_string_lossy())?;
    Ok(root.join(relative))
}
fn read_optional_u16(r: &mut impl Read) -> Result<Option<u16>, PythonPayloadError> {
    let mut first = [0; 1];
    match r.read(&mut first).map_err(PythonPayloadError::Io)? {
        0 => return Ok(None),
        1 => {}
        _ => unreachable!("single-byte read buffer cannot be filled beyond one byte"),
    }
    let mut second = [0; 1];
    r.read_exact(&mut second)
        .map_err(|_| PythonPayloadError::Framing)?;
    Ok(Some(u16::from_be_bytes([first[0], second[0]])))
}
fn read_u32(r: &mut impl Read) -> Result<u32, PythonPayloadError> {
    let mut b = [0; 4];
    r.read_exact(&mut b)
        .map_err(|_| PythonPayloadError::Framing)?;
    Ok(u32::from_be_bytes(b))
}
fn read_u64(r: &mut impl Read) -> Result<u64, PythonPayloadError> {
    let mut b = [0; 8];
    r.read_exact(&mut b)
        .map_err(|_| PythonPayloadError::Framing)?;
    Ok(u64::from_be_bytes(b))
}
fn read_and_hash(r: &mut impl Read, size: u64) -> Result<(Vec<u8>, String), PythonPayloadError> {
    let capacity = usize::try_from(size).map_err(|_| PythonPayloadError::Framing)?;
    let mut data = Vec::with_capacity(capacity);
    let mut remaining = size;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    while remaining != 0 {
        let chunk_len = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| PythonPayloadError::Framing)?;
        r.read_exact(&mut buffer[..chunk_len])
            .map_err(|_| PythonPayloadError::Framing)?;
        hasher.update(&buffer[..chunk_len]);
        data.extend_from_slice(&buffer[..chunk_len]);
        remaining -= chunk_len as u64;
    }
    let digest = hasher.finalize();
    Ok((
        data,
        digest.iter().map(|byte| format!("{byte:02x}")).collect(),
    ))
}
fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}
fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
}
fn ready_tree_is_valid(dir: &Path, manifest: &PayloadManifest, digest: &str) -> bool {
    fs::symlink_metadata(dir)
        .ok()
        .is_some_and(|m| private_directory_metadata(&m))
        && regular_private_file(&dir.join(READY_FILE))
        && fs::read_to_string(dir.join(READY_FILE))
            .ok()
            .is_some_and(|v| v == digest)
        && published_files_are_valid(dir, manifest)
}
fn regular_private_file(path: &Path) -> bool {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return false;
    };
    if !meta.file_type().is_file() || meta.file_type().is_symlink() {
        return false;
    }
    #[cfg(unix)]
    if std::os::unix::fs::MetadataExt::nlink(&meta) != 1 {
        return false;
    }
    true
}
fn published_files_are_valid(dir: &Path, manifest: &PayloadManifest) -> bool {
    let expected: BTreeMap<_, _> = manifest
        .files
        .iter()
        .map(|file| (PathBuf::from(&file.path), file))
        .collect();
    let expected_dirs: BTreeSet<_> = manifest
        .files
        .iter()
        .filter_map(|file| Path::new(&file.path).parent().map(Path::to_path_buf))
        .filter(|path| !path.as_os_str().is_empty())
        .collect();
    let mut found = BTreeSet::new();
    let mut pending = vec![PathBuf::new()];
    while let Some(relative_dir) = pending.pop() {
        let current = dir.join(&relative_dir);
        let entries = match fs::read_dir(&current) {
            Ok(entries) => entries,
            Err(_) => return false,
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let relative = relative_dir.join(name);
            if relative == Path::new(READY_FILE) {
                continue;
            }
            let meta = match fs::symlink_metadata(entry.path()) {
                Ok(meta) => meta,
                Err(_) => return false,
            };
            if meta.file_type().is_dir() && !meta.file_type().is_symlink() {
                if !private_directory_metadata(&meta) || !expected_dirs.contains(&relative) {
                    return false;
                }
                pending.push(relative);
                continue;
            }
            if !regular_private_file(&entry.path()) {
                return false;
            }
            let Some(file) = expected.get(&relative) else {
                return false;
            };
            if meta.len() != file.size
                || fs::read(entry.path())
                    .ok()
                    .is_none_or(|data| hex_digest(&data) != file.sha256)
            {
                return false;
            }
            found.insert(relative);
        }
    }
    found == expected.keys().cloned().collect()
}
fn remove_staging(path: &Path) -> Result<(), PythonPayloadError> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.file_type().is_dir() || meta.file_type().is_symlink() {
        return Err(PythonPayloadError::InvalidPath(path.into()));
    }
    fs::remove_dir_all(path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}
/// Under the digest lock, an incomplete staging directory cannot belong to a
/// live publisher. Remove it regardless of age so an interrupted activation is
/// recovered on the very next attempt rather than after a timeout.
fn recover_staging(parent: &Path, digest: &str) -> Result<(), PythonPayloadError> {
    let prefix = format!(".staging-{digest}-");
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(&prefix))
        {
            remove_staging(&entry.path())?;
        }
    }
    Ok(())
}
fn is_stale(path: &Path) -> Result<bool, PythonPayloadError> {
    Ok(SystemTime::now()
        .duration_since(fs::symlink_metadata(path)?.modified()?)
        .unwrap_or_default()
        > LOCK_STALE_AFTER)
}
fn walk_dirs(root: &Path) -> Result<Vec<PathBuf>, PythonPayloadError> {
    let mut out = Vec::new();
    for e in fs::read_dir(root)? {
        let p = e?.path();
        if fs::symlink_metadata(&p)?.file_type().is_dir() {
            out.push(p.clone());
            out.extend(walk_dirs(&p)?);
        }
    }
    Ok(out)
}
struct CacheLock {
    path: PathBuf,
}
impl CacheLock {
    fn acquire(path: &Path) -> Result<Self, PythonPayloadError> {
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut f) => {
                f.write_all(b"lock")?;
                f.sync_all()?;
                if let Some(parent) = path.parent() {
                    sync_dir(parent)?;
                }
                Ok(Self { path: path.into() })
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && is_stale(path)? => {
                fs::remove_file(path)?;
                Self::acquire(path)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(PythonPayloadError::Busy(path.display().to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }
}
impl Drop for CacheLock {
    fn drop(&mut self) {
        if fs::remove_file(&self.path).is_ok()
            && let Some(parent) = self.path.parent()
        {
            let _ = sync_dir(parent);
        }
    }
}
#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<(), PythonPayloadError> {
    OpenOptions::new().read(true).open(path)?.sync_all()?;
    Ok(())
}
#[cfg(not(unix))]
fn sync_dir(_: &Path) -> Result<(), PythonPayloadError> {
    // Windows rename durability is delegated to the platform's atomic rename
    // semantics; the regular-file syncs above still make payload data durable.
    Ok(())
}
fn create_private_dir_tree(path: &Path) -> Result<(), PythonPayloadError> {
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match fs::symlink_metadata(current) {
            Ok(metadata) => {
                let valid = if current == path {
                    private_directory_metadata(&metadata)
                } else {
                    metadata.file_type().is_dir() && !metadata.file_type().is_symlink()
                };
                if !valid {
                    return Err(PythonPayloadError::InvalidPath(current.into()));
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(current.to_path_buf());
                current = current.parent().ok_or_else(|| {
                    PythonPayloadError::Manifest("cache root has no existing ancestor".into())
                })?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    for directory in missing.iter().rev() {
        fs::create_dir(directory)?;
        set_private_dir(directory)?;
    }
    Ok(())
}
fn ensure_private_dir(path: &Path) -> Result<(), PythonPayloadError> {
    let metadata = fs::symlink_metadata(path)?;
    if private_directory_metadata(&metadata) {
        Ok(())
    } else {
        Err(PythonPayloadError::InvalidPath(path.into()))
    }
}
fn ensure_private_cache_path(root: &Path, path: &Path) -> Result<(), PythonPayloadError> {
    ensure_private_dir(root)?;
    let relative = path
        .strip_prefix(root)
        .map_err(|_| PythonPayloadError::InvalidPath(path.into()))?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(PythonPayloadError::InvalidPath(path.into()));
        }
        current.push(component.as_os_str());
        ensure_private_dir(&current)?;
    }
    Ok(())
}
fn private_directory_metadata(metadata: &fs::Metadata) -> bool {
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o077 == 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}
#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<(), PythonPayloadError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}
#[cfg(not(unix))]
fn set_private_dir(_: &Path) -> Result<(), PythonPayloadError> {
    Ok(())
}
#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<(), PythonPayloadError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}
#[cfg(not(unix))]
fn set_private_file(_: &Path) -> Result<(), PythonPayloadError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    fn resign(m: &mut PayloadManifest) {
        let key = SigningKey::from_bytes(&[
            0x9d, 0x61, 0xb1, 0x9d, 0xef, 0xfd, 0x5a, 0x60, 0xba, 0x84, 0x4a, 0xf4, 0x92, 0xec,
            0x2c, 0xc4, 0x44, 0x49, 0xc5, 0x69, 0x7b, 0x32, 0x69, 0x19, 0x70, 0x3b, 0xac, 0x03,
            0x1c, 0xae, 0x7f, 0x60,
        ]);
        assert_eq!(key.verifying_key().to_bytes(), EMBEDDED_MANIFEST_PUBLIC_KEY);
        m.signature = STANDARD_NO_PAD.encode(key.sign(&m.canonical_bytes().unwrap()).to_bytes());
    }
    fn manifest(files: &[(&str, &[u8])]) -> PayloadManifest {
        let mut m = PayloadManifest {
            schema: "citadel.python.payload".into(),
            version: 1,
            target: "x86_64-unknown-linux-gnu".into(),
            abi: "cp313".into(),
            compression: "none".into(),
            max_file_bytes: 1024,
            max_total_bytes: 2048,
            files: files
                .iter()
                .map(|(p, b)| PayloadFile {
                    path: (*p).into(),
                    size: b.len() as u64,
                    sha256: hex_digest(b),
                })
                .collect(),
            signature: String::new(),
        };
        resign(&mut m);
        m
    }
    fn payload(m: &PayloadManifest, files: &[(&str, &[u8])]) -> Vec<u8> {
        let raw = serde_json::to_vec(m).unwrap();
        let mut b = b"CITPYLD1".to_vec();
        b.extend((raw.len() as u32).to_be_bytes());
        b.extend(raw);
        for (p, d) in files {
            b.extend((p.len() as u16).to_be_bytes());
            b.extend(p.as_bytes());
            b.extend((d.len() as u64).to_be_bytes());
            b.extend(*d);
        }
        b
    }
    #[test]
    fn parses_signed_payload_and_publishes_atomically() {
        let files = [("Lib/site.py", b"ok" as &[u8])];
        let m = manifest(&files);
        let p = VerifiedPayload::parse(&payload(&m, &files)).unwrap();
        let root = std::env::temp_dir().join(format!("citadel-payload-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let cache = PayloadCache::new(root.clone());
        let published = p.publish(&cache).unwrap();
        assert_eq!(fs::read(published.join("Lib/site.py")).unwrap(), b"ok");
        assert!(published.join(READY_FILE).is_file());
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn rejects_mutation_zip_slip_duplicates_and_extra_files() {
        let files = [("Lib/site.py", b"ok" as &[u8])];
        let m = manifest(&files);
        let mut altered = payload(&m, &files);
        *altered.last_mut().unwrap() = b'!';
        assert!(matches!(
            VerifiedPayload::parse(&altered),
            Err(PythonPayloadError::Integrity { .. })
        ));
        let bad = manifest(&[("../escape", b"x")]);
        assert!(bad.verify().is_err());
        let noncanonical = manifest(&[("a//b", b"x")]);
        assert!(noncanonical.verify().is_err());
        let unordered = manifest(&[("z", b"x"), ("a", b"y")]);
        assert!(unordered.verify().is_err());
        let extra = payload(&m, &[("Lib/site.py", b"ok"), ("other", b"x")]);
        assert!(matches!(
            VerifiedPayload::parse(&extra),
            Err(PythonPayloadError::EntrySetMismatch)
        ));
    }
    #[test]
    fn concurrent_publish_never_exposes_a_partial_tree() {
        use std::sync::{Arc, Barrier};
        let files = [("Lib/site.py", b"ok" as &[u8])];
        let manifest = manifest(&files);
        let payload = Arc::new(VerifiedPayload::parse(&payload(&manifest, &files)).unwrap());
        let root =
            std::env::temp_dir().join(format!("citadel-payload-race-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let cache = Arc::new(PayloadCache::new(root.clone()));
        let start = Arc::new(Barrier::new(2));
        let joins: Vec<_> = (0..2)
            .map(|_| {
                let payload = Arc::clone(&payload);
                let cache = Arc::clone(&cache);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    payload.publish(&cache)
                })
            })
            .collect();
        let results: Vec<_> = joins.into_iter().map(|j| j.join().unwrap()).collect();
        assert!(results.iter().any(Result::is_ok));
        let final_dir = payload.publish(&cache).unwrap();
        assert!(final_dir.join(READY_FILE).is_file());
        assert_eq!(fs::read(final_dir.join("Lib/site.py")).unwrap(), b"ok");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn payload_does_not_mention_or_materialize_libpython() {
        let files = [("Lib/site.py", b"ok" as &[u8])];
        let m = manifest(&files);
        let p = VerifiedPayload::parse(&payload(&m, &files)).unwrap();
        assert!(
            p.files
                .keys()
                .all(|p| !p.to_string_lossy().contains("libpython"))
        );
        assert!(matches!(
            manifest(&[("libpython3.13.so", b"not here")]).verify(),
            Err(PythonPayloadError::LibpythonOutOfScope)
        ));
    }

    #[test]
    fn compromised_ready_tree_is_never_reused() {
        let files = [("Lib/site.py", b"ok" as &[u8])];
        let m = manifest(&files);
        let p = VerifiedPayload::parse_reader(Cursor::new(payload(&m, &files))).unwrap();
        let root =
            std::env::temp_dir().join(format!("citadel-payload-reverify-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let cache = PayloadCache::new(root.clone());
        let published = p.publish(&cache).unwrap();
        fs::write(published.join("unexpected"), b"tamper").unwrap();
        assert!(
            p.publish(&cache).is_err(),
            "a corrupt existing cache must fail closed"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn cache_root_symlink_is_rejected_before_any_write() {
        use std::os::unix::fs::symlink;

        let files = [("Lib/site.py", b"ok" as &[u8])];
        let m = manifest(&files);
        let p = VerifiedPayload::parse(&payload(&m, &files)).unwrap();
        let base =
            std::env::temp_dir().join(format!("citadel-payload-link-{}", std::process::id()));
        let root = base.join("cache");
        let destination = base.join("destination");
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&destination).unwrap();
        symlink(&destination, &root).unwrap();
        assert!(matches!(
            p.publish(&PayloadCache::new(root)),
            Err(PythonPayloadError::InvalidPath(_))
        ));
        assert!(fs::read_dir(destination).unwrap().next().is_none());
        let _ = fs::remove_dir_all(base);
    }

    #[cfg(unix)]
    #[test]
    fn hard_linked_cache_file_is_not_reused() {
        let files = [("Lib/site.py", b"ok" as &[u8])];
        let m = manifest(&files);
        let p = VerifiedPayload::parse(&payload(&m, &files)).unwrap();
        let root =
            std::env::temp_dir().join(format!("citadel-payload-hardlink-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let cache = PayloadCache::new(root.clone());
        let published = p.publish(&cache).unwrap();
        let original = published.join("Lib/site.py");
        let outside = root.join("outside-copy");
        fs::hard_link(&original, &outside).unwrap();
        assert!(p.publish(&cache).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn signed_memory_ceiling_rejects_large_payload_before_reading_entries() {
        let mut m = manifest(&[("Lib/site.py", b"ok")]);
        m.max_total_bytes = MAX_IN_MEMORY_PAYLOAD_BYTES + 1;
        resign(&mut m);
        let wire = payload(&m, &[]);
        assert!(matches!(
            VerifiedPayload::parse_reader(Cursor::new(wire)),
            Err(PythonPayloadError::Manifest(_))
        ));
    }

    #[test]
    fn path_validation_property_rejects_generated_escape_forms() {
        // Property-style coverage: no separator/parent-component composition
        // may become an accepted cache-relative path.
        for segment in [
            "",
            ".",
            "..",
            "a/..",
            "../a",
            "a//b",
            "/a",
            "//server/share",
            "C:/a",
            "C:a",
            "a:b",
            "a\\b",
        ] {
            assert!(validate_relative_path(segment).is_err(), "{segment:?}");
        }
        for name in ["site.py", "Lib/site.py", "Lib/encodings/utf_8.py"] {
            assert!(validate_relative_path(name).is_ok(), "{name:?}");
        }
    }

    #[test]
    fn interrupted_staging_is_recovered_under_the_digest_lock() {
        let files = [("Lib/site.py", b"ok" as &[u8])];
        let m = manifest(&files);
        let p = VerifiedPayload::parse(&payload(&m, &files)).unwrap();
        let root =
            std::env::temp_dir().join(format!("citadel-payload-recover-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let cache = PayloadCache::new(root.clone());
        let digest = p.manifest.digest().unwrap();
        let parent = cache
            .path_for(&p.manifest.target, &p.manifest.abi, &digest)
            .unwrap();
        let parent = parent.parent().unwrap().to_path_buf();
        create_private_dir_tree(&parent).unwrap();
        let orphan = parent.join(format!(".staging-{digest}-interrupted"));
        fs::create_dir(&orphan).unwrap();
        set_private_dir(&orphan).unwrap();
        fs::write(orphan.join("partial"), b"partial").unwrap();
        assert!(p.publish(&cache).unwrap().join(READY_FILE).is_file());
        assert!(!orphan.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn active_digest_lock_excludes_another_publisher() {
        let files = [("Lib/site.py", b"ok" as &[u8])];
        let m = manifest(&files);
        let p = VerifiedPayload::parse(&payload(&m, &files)).unwrap();
        let root =
            std::env::temp_dir().join(format!("citadel-payload-lock-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let cache = PayloadCache::new(root.clone());
        let digest = p.manifest.digest().unwrap();
        let final_dir = cache
            .path_for(&p.manifest.target, &p.manifest.abi, &digest)
            .unwrap();
        let parent = final_dir.parent().unwrap();
        create_private_dir_tree(parent).unwrap();
        let lock = CacheLock::acquire(&final_dir.with_extension("lock")).unwrap();
        assert!(matches!(
            p.publish(&cache),
            Err(PythonPayloadError::Busy(_))
        ));
        drop(lock);
        assert!(p.publish(&cache).is_ok());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn insecure_cache_permissions_fail_closed_before_write() {
        use std::os::unix::fs::PermissionsExt;

        let files = [("Lib/site.py", b"ok" as &[u8])];
        let m = manifest(&files);
        let p = VerifiedPayload::parse(&payload(&m, &files)).unwrap();
        let root =
            std::env::temp_dir().join(format!("citadel-payload-perms-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            p.publish(&PayloadCache::new(root.clone())),
            Err(PythonPayloadError::InvalidPath(_))
        ));
        let _ = fs::remove_dir_all(root);
    }
}
