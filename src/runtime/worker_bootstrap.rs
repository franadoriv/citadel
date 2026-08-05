//! One-shot bootstrap secret delivery over an inherited Unix descriptor.

use nix::fcntl::{FcntlArg, FdFlag, fcntl};

#[cfg(unix)]
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, OwnedFd, RawFd};

#[cfg(unix)]
pub fn read_secret_from_fd(fd: RawFd) -> io::Result<[u8; 32]> {
    if fd < 0 {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    let mut secret = [0; 32];
    let mut offset = 0;
    while offset < secret.len() {
        let read = nix::unistd::read(fd, &mut secret[offset..]).map_err(io::Error::other)?;
        if read == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        offset += read;
    }
    Ok(secret)
}

#[cfg(unix)]
pub struct BootstrapPipe {
    reader: OwnedFd,
    writer: OwnedFd,
}

#[cfg(unix)]
pub struct BootstrapWriter(OwnedFd);

#[cfg(unix)]
impl BootstrapPipe {
    pub fn create() -> io::Result<Self> {
        let (reader, writer) = nix::unistd::pipe().map_err(io::Error::other)?;
        Ok(Self { reader, writer })
    }

    pub fn reader_fd(&self) -> i32 {
        self.reader.as_raw_fd()
    }

    pub fn make_reader_inheritable(&self) -> io::Result<()> {
        let flags = FdFlag::from_bits_retain(
            fcntl(self.reader.as_raw_fd(), FcntlArg::F_GETFD).map_err(io::Error::other)?,
        );
        fcntl(
            self.reader.as_raw_fd(),
            FcntlArg::F_SETFD(flags & !FdFlag::FD_CLOEXEC),
        )
        .map_err(io::Error::other)?;
        Ok(())
    }

    pub fn into_reader_and_writer(self) -> (OwnedFd, BootstrapWriter) {
        (self.reader, BootstrapWriter(self.writer))
    }

    pub fn write_secret(self, secret: &[u8; 32]) -> io::Result<()> {
        let mut writer = std::fs::File::from(self.writer);
        writer.write_all(secret)
    }

    pub fn read_secret(self) -> io::Result<[u8; 32]> {
        let mut reader = std::fs::File::from(self.reader);
        let mut secret = [0; 32];
        reader.read_exact(&mut secret)?;
        Ok(secret)
    }
}

#[cfg(unix)]
impl BootstrapWriter {
    pub fn write_secret(self, secret: &[u8; 32]) -> io::Result<()> {
        let mut writer = std::fs::File::from(self.0);
        writer.write_all(secret)
    }
}
