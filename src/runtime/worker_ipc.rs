//! Parent-owned private IPC endpoints for supervised runtime workers.

use uuid::Uuid;

#[cfg(unix)]
use std::{
    fs, io,
    os::unix::{ffi::OsStrExt, fs::PermissionsExt, net::UnixListener},
    path::{Path, PathBuf},
};

pub const MAX_UNIX_SOCKET_PATH_BYTES: usize = 100;

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
