#![forbid(unsafe_code)]

//! Small process-environment helper for the Citadel binary.
//!
//! The main `citadel` crate is Rust 2024 and forbids unsafe code. On current
//! Rust, mutating the process environment is a safe API in Rust 2021 but an
//! unsafe API in Rust 2024. Citadel only uses this helper during single-threaded
//! binary startup, before the Tokio runtime and before PyO3 initializes CPython.

use std::ffi::OsStr;

/// Set a process environment variable during early, single-threaded startup.
pub fn set_var<K, V>(key: K, value: V)
where
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    std::env::set_var(key, value);
}
