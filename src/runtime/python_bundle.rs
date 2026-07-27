//! CPython bundle discovery for `runtime-python` release artifacts.
//!
//! PyO3's `auto-initialize` feature initializes CPython on first
//! `Python::attach`. A Python-enabled Citadel release needs to point CPython at
//! the bundled standard library before that happens. The binary calls
//! [`configure_bundled_python_runtime`] at process start, before any threads are
//! spawned.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

/// Environment values used to initialize bundled CPython.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundledPythonEnv {
    /// Directory containing `Lib/` and `DLLs/`.
    pub python_home: PathBuf,
    /// Search path limited to the bundled standard-library directories.
    pub python_path: OsString,
    /// Dynamic CPython library found beside the executable.
    pub dynamic_library: PathBuf,
}

/// Detect and configure a CPython bundle next to the running executable.
///
/// Returns `None` when no bundle is present, preserving PyO3's normal local
/// development behavior. When a bundle is present, this overwrites
/// `PYTHONHOME`/`PYTHONPATH` so CPython prefers the staged standard library over
/// any globally installed Python.
#[must_use]
pub fn configure_bundled_python_runtime() -> Option<BundledPythonEnv> {
    let env = detect_bundled_python_runtime()?;
    apply_to_process(&env);
    Some(env)
}

/// Detect a bundle next to the running executable without mutating the process.
#[must_use]
pub fn detect_bundled_python_runtime() -> Option<BundledPythonEnv> {
    let exe = std::env::current_exe().ok()?;
    detect_bundled_python_runtime_next_to(&exe)
}

fn detect_bundled_python_runtime_next_to(exe: &Path) -> Option<BundledPythonEnv> {
    let exe_dir = exe.parent()?;
    detect_bundled_python_runtime_in(exe_dir)
}

fn detect_bundled_python_runtime_in(exe_dir: &Path) -> Option<BundledPythonEnv> {
    let dynamic_library = find_cpython_dynamic_library(exe_dir)?;
    let python_home = exe_dir.join("python");
    let lib_dir = python_home.join("Lib");
    let dlls_dir = python_home.join("DLLs");

    if !lib_dir.join("os.py").is_file() {
        return None;
    }
    if cfg!(windows) && !dlls_dir.is_dir() {
        return None;
    }

    let mut paths = vec![lib_dir];
    if dlls_dir.is_dir() {
        paths.push(dlls_dir);
    }
    let python_path = match std::env::join_paths(paths) {
        Ok(joined) => joined,
        Err(_) => python_home.join("Lib").into_os_string(),
    };

    Some(BundledPythonEnv {
        python_home,
        python_path,
        dynamic_library,
    })
}

fn find_cpython_dynamic_library(exe_dir: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(exe_dir).ok()?;
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let lower = name.to_ascii_lowercase();
        if is_cpython_dynamic_library_name(&lower) {
            return Some(path);
        }
    }
    None
}

#[cfg(windows)]
fn is_cpython_dynamic_library_name(name: &str) -> bool {
    name.starts_with("python3") && name.ends_with(".dll")
}

#[cfg(target_os = "macos")]
fn is_cpython_dynamic_library_name(name: &str) -> bool {
    name.starts_with("libpython3") && name.ends_with(".dylib")
}

#[cfg(all(not(windows), not(target_os = "macos")))]
fn is_cpython_dynamic_library_name(name: &str) -> bool {
    name.starts_with("libpython3") && name.ends_with(".so")
}

fn apply_to_process(env: &BundledPythonEnv) {
    apply_with(env, |key, value| {
        citadel_process_env::set_var(key, value);
    });
}

fn apply_with<F>(env: &BundledPythonEnv, mut set_var: F)
where
    F: FnMut(&str, &std::ffi::OsStr),
{
    set_var("PYTHONHOME", env.python_home.as_os_str());
    set_var("PYTHONPATH", env.python_path.as_os_str());
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn detects_windows_bundle_layout_and_env_values() {
        let root = unique_temp_dir("citadel-python-bundle");
        let dynamic_library = test_dynamic_library_name();
        std::fs::create_dir_all(root.join("python").join("Lib")).expect("Lib dir");
        std::fs::create_dir_all(root.join("python").join("DLLs")).expect("DLLs dir");
        std::fs::write(root.join("citadel.exe"), b"exe").expect("exe");
        std::fs::write(root.join(dynamic_library), b"dll").expect("dll");
        std::fs::write(root.join("python").join("Lib").join("os.py"), b"# os").expect("os.py");

        let env = detect_bundled_python_runtime_in(&root).expect("bundle detected");

        assert_eq!(env.python_home, root.join("python"));
        assert_eq!(env.dynamic_library, root.join(dynamic_library));

        let mut vars = HashMap::new();
        apply_with(&env, |key, value| {
            vars.insert(key.to_string(), value.to_os_string());
        });

        assert_eq!(
            vars.get("PYTHONHOME").expect("PYTHONHOME"),
            root.join("python").as_os_str()
        );
        assert!(vars.contains_key("PYTHONPATH"));

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn ignores_incomplete_bundle_layout() {
        let root = unique_temp_dir("citadel-python-no-bundle");
        std::fs::create_dir_all(&root).expect("root dir");
        std::fs::write(root.join(test_dynamic_library_name()), b"dll").expect("dll");

        assert_eq!(detect_bundled_python_runtime_in(&root), None);

        std::fs::remove_dir_all(root).ok();
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{now}", std::process::id()))
    }

    #[cfg(windows)]
    fn test_dynamic_library_name() -> &'static str {
        "python313.dll"
    }

    #[cfg(target_os = "macos")]
    fn test_dynamic_library_name() -> &'static str {
        "libpython3.13.dylib"
    }

    #[cfg(all(not(windows), not(target_os = "macos")))]
    fn test_dynamic_library_name() -> &'static str {
        "libpython3.13.so"
    }
}
