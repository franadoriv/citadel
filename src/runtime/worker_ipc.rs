//! Parent-owned private IPC endpoints for supervised runtime workers.

#[cfg(unix)]
use uuid::Uuid;

#[cfg(unix)]
use std::{
    fs, io,
    os::unix::{ffi::OsStrExt, fs::PermissionsExt, net::UnixListener},
    path::{Path, PathBuf},
};

pub const MAX_UNIX_SOCKET_PATH_BYTES: usize = 100;

/// Parent-created private named-pipe endpoint for the runtime worker.
///
/// The Windows analog of [`PrivateUnixEndpoint`]'s 0700 directory + 0600
/// socket: the name carries 32 bytes of CSPRNG entropy (unguessable), the
/// first bound instance is created with a protected DACL restricted to the
/// current user's SID (no Everyone/World access), and first-instance-only
/// plus a single-instance cap keep the name from being squatted or shadowed.
/// Peer-pid validation on connect is the supervisor's job on top of this.
#[cfg(windows)]
pub struct PrivateNamedPipeEndpoint {
    name: String,
}

#[cfg(windows)]
impl PrivateNamedPipeEndpoint {
    /// Generate a fresh unguessable endpoint name.
    pub fn create() -> std::io::Result<Self> {
        let mut entropy = [0u8; 32];
        getrandom::fill(&mut entropy)
            .map_err(|_| std::io::Error::other("worker endpoint entropy unavailable"))?;
        let mut suffix = String::with_capacity(entropy.len() * 2);
        for byte in entropy {
            use std::fmt::Write;
            let _ = write!(suffix, "{byte:02x}");
        }
        Ok(Self {
            name: format!(r"\\.\pipe\citadel-worker-{suffix}"),
        })
    }

    /// Create the single, DACL-restricted server instance for this endpoint.
    ///
    /// Must be called from within a tokio runtime context (the named-pipe
    /// server registers with the runtime's reactor). Unlike the unix
    /// endpoint there is nothing on disk to clean up: the pipe object
    /// disappears when the returned handle closes.
    pub fn bind(&self) -> std::io::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
        let mut options = tokio::net::windows::named_pipe::ServerOptions::new();
        options
            .first_pipe_instance(true)
            .max_instances(1)
            .reject_remote_clients(true);
        citadel_win_proc::create_restricted_pipe_server(&options, &self.name)
    }

    /// The full pipe name handed to the worker on its command line.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(unix)]
pub struct PrivateUnixEndpoint {
    path: PathBuf,
    dir: PathBuf,
}

#[cfg(unix)]
impl PrivateUnixEndpoint {
    pub fn create(parent: &Path) -> io::Result<Self> {
        let dir = parent.join(format!("citadel-worker-{}", Uuid::new_v4()));
        let path = dir.join("control.sock");
        if path.as_os_str().as_bytes().len() > MAX_UNIX_SOCKET_PATH_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker socket path is too long",
            ));
        }
        fs::create_dir(&dir)?;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
        Ok(Self { path, dir })
    }

    pub fn bind(&self) -> io::Result<UnixListener> {
        let listener = UnixListener::bind(&self.path)?;
        fs::set_permissions(&self.path, fs::Permissions::from_mode(0o600))?;
        Ok(listener)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(unix)]
impl Drop for PrivateUnixEndpoint {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;

    #[test]
    fn endpoint_names_are_unguessable_and_unique() {
        let first = PrivateNamedPipeEndpoint::create().expect("first endpoint");
        let second = PrivateNamedPipeEndpoint::create().expect("second endpoint");
        assert_ne!(first.name(), second.name());
        let suffix = first
            .name()
            .strip_prefix(r"\\.\pipe\citadel-worker-")
            .expect("worker pipe namespace prefix");
        // 32 bytes of CSPRNG entropy, hex-encoded: an unpredictable name is
        // the first layer against pipe squatting.
        assert_eq!(suffix.len(), 64);
        assert!(suffix.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    fn bound_endpoint_is_restricted_to_the_current_user() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("runtime");
        let _guard = runtime.enter();
        let endpoint = PrivateNamedPipeEndpoint::create().expect("endpoint");
        let server = endpoint.bind().expect("bind");
        // The DACL is the Windows analog of the unix 0700 dir + 0600 socket:
        // exactly one allow ACE, and it names the current user.
        let entries = citadel_win_proc::handle_dacl_entries(&server).expect("read DACL");
        let me = citadel_win_proc::current_user_sid_string().expect("current user sid");
        assert_eq!(entries.len(), 1, "exactly one ACE may exist: {entries:?}");
        assert!(entries[0].allows);
        assert_eq!(entries[0].sid, me);
    }

    #[test]
    fn bound_endpoint_refuses_a_second_instance() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("runtime");
        let _guard = runtime.enter();
        let endpoint = PrivateNamedPipeEndpoint::create().expect("endpoint");
        let _server = endpoint.bind().expect("bind");
        // First-instance-only plus a single-instance cap: even the same user
        // cannot stand up a second instance to intercept the worker connect.
        endpoint
            .bind()
            .expect_err("a second instance on the endpoint name must be refused");
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    #[test]
    fn rejects_a_parent_that_would_exceed_socket_path_limit() {
        let parent = std::env::temp_dir().join("x".repeat(MAX_UNIX_SOCKET_PATH_BYTES));
        let result = PrivateUnixEndpoint::create(&parent);
        assert_eq!(
            result.as_ref().err().map(io::Error::kind),
            Some(io::ErrorKind::InvalidInput)
        );
    }

    #[test]
    fn dropping_endpoint_removes_its_private_directory() {
        let path = {
            let endpoint = PrivateUnixEndpoint::create(&std::env::temp_dir()).expect("endpoint");
            endpoint.path().parent().expect("parent").to_path_buf()
        };
        assert!(!path.exists());
    }

    #[test]
    fn bound_endpoint_is_owner_only() {
        let endpoint = PrivateUnixEndpoint::create(&std::env::temp_dir()).expect("endpoint");
        let _listener = endpoint.bind().expect("bind");
        assert_eq!(
            fs::metadata(endpoint.path())
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
