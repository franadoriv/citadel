//! Safe wrappers over the Win32 calls the worker supervisor needs.

use std::ffi::c_void;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::ptr::{null, null_mut};

use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
use windows_sys::Win32::Foundation::{
    ERROR_ACCESS_DENIED, ERROR_INVALID_PARAMETER, GetHandleInformation, HANDLE,
    HANDLE_FLAG_INHERIT, LocalFree, SetHandleInformation, WAIT_TIMEOUT,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SDDL_REVISION_1, SE_KERNEL_OBJECT,
};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
    DACL_SECURITY_INFORMATION, GetAce, GetAclInformation, GetTokenInformation, PSID,
    SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Pipes::{
    CreatePipe, GetNamedPipeClientProcessId, GetNamedPipeServerProcessId, PeekNamedPipe,
};
use windows_sys::Win32::System::SystemServices::{ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, OpenProcess, OpenProcessToken, PROCESS_SYNCHRONIZE, WaitForSingleObject,
};

/// The string form (`S-1-5-21-...`) of the current process token's user SID.
///
/// This is the identity the worker pipe's DACL is restricted to.
pub fn current_user_sid_string() -> io::Result<String> {
    let mut token: HANDLE = null_mut();
    // SAFETY: `GetCurrentProcess` returns the process pseudo-handle, which
    // needs no closing; on success `OpenProcessToken` writes a real, owned
    // token handle into `token`.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: on success `token` is a valid token handle owned by this call.
    let token = unsafe { OwnedHandle::from_raw_handle(token as RawHandle) };
    let mut needed = 0u32;
    // SAFETY: a null buffer with length 0 is the documented sizing call; it
    // fails with ERROR_INSUFFICIENT_BUFFER and reports the needed size.
    unsafe {
        GetTokenInformation(
            token.as_raw_handle() as HANDLE,
            TokenUser,
            null_mut(),
            0,
            &mut needed,
        )
    };
    if needed == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buffer = vec![0u8; needed as usize];
    // SAFETY: `buffer` is writable for `needed` bytes, exactly the size the
    // kernel reported for the TOKEN_USER payload.
    if unsafe {
        GetTokenInformation(
            token.as_raw_handle() as HANDLE,
            TokenUser,
            buffer.as_mut_ptr().cast(),
            needed,
            &mut needed,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: on success the buffer starts with a TOKEN_USER structure whose
    // SID pointer targets memory inside the same buffer, still alive here.
    let sid = unsafe { (*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid };
    sid_to_string(sid)
}

/// Convert a SID to its string form, freeing the intermediate allocation.
fn sid_to_string(sid: PSID) -> io::Result<String> {
    let mut wide: *mut u16 = null_mut();
    // SAFETY: `sid` is a valid SID supplied by the kernel; on success the
    // function allocates a NUL-terminated wide string into `wide`.
    if unsafe { ConvertSidToStringSidW(sid, &mut wide) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut length = 0usize;
    // SAFETY: `wide` is NUL-terminated, so the scan stops inside the
    // allocation.
    while unsafe { *wide.add(length) } != 0 {
        length += 1;
    }
    // SAFETY: `length` counts the in-bounds non-NUL characters just scanned.
    let text = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(wide, length) });
    // SAFETY: `wide` was allocated by ConvertSidToStringSidW and is
    // documented to be released with LocalFree.
    unsafe { LocalFree(wide.cast()) };
    Ok(text)
}

/// Create a named-pipe server instance restricted to the current user.
///
/// The security descriptor is a protected DACL with a single ACE granting
/// generic-all to the current user's SID: no Everyone/World access, so no
/// other local user can open the pipe or create further instances. Flags such
/// as `first_pipe_instance` are the caller's responsibility on `options`.
/// Must be called from within a tokio runtime context.
pub fn create_restricted_pipe_server(
    options: &ServerOptions,
    name: &str,
) -> io::Result<NamedPipeServer> {
    let sid = current_user_sid_string()?;
    // Protected DACL (`P`): nothing is inherited from a parent object, so the
    // single explicit ACE below is the complete access policy.
    let sddl = format!("D:P(A;;GA;;;{sid})");
    let sddl_wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
    let mut descriptor: *mut c_void = null_mut();
    // SAFETY: `sddl_wide` is NUL-terminated and outlives the call; on success
    // a self-relative security descriptor is allocated into `descriptor`.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl_wide.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor,
        bInheritHandle: 0,
    };
    // SAFETY: `attributes` and the descriptor it points to stay alive across
    // the call; CreateNamedPipeW copies what it needs before returning.
    let server = unsafe {
        options.create_with_security_attributes_raw(name, (&raw mut attributes).cast::<c_void>())
    };
    // SAFETY: the descriptor was allocated by the conversion above and is
    // documented to be released with LocalFree.
    unsafe { LocalFree(descriptor.cast()) };
    server
}

/// One entry of a kernel object's DACL, in SID string form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaclEntry {
    /// Trustee SID in string form (`S-1-...`).
    pub sid: String,
    /// Raw access mask of the ACE.
    pub access_mask: u32,
    /// `true` for an access-allowed ACE, `false` for access-denied.
    pub allows: bool,
}

/// Read back the DACL of a kernel object (for example a named-pipe handle).
///
/// Used by security tests to prove the worker pipe is restricted to the
/// current user. A NULL DACL — which would grant everyone full access — and
/// any ACE type other than plain allow/deny fail closed with an error.
pub fn handle_dacl_entries<H: AsRawHandle>(object: &H) -> io::Result<Vec<DaclEntry>> {
    let mut dacl: *mut ACL = null_mut();
    let mut descriptor: *mut c_void = null_mut();
    // SAFETY: the handle is kept alive by the borrowed `object`; out
    // pointers receive the DACL (into the returned descriptor) and the
    // descriptor allocation itself.
    let status = unsafe {
        GetSecurityInfo(
            object.as_raw_handle() as HANDLE,
            SE_KERNEL_OBJECT,
            DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            &mut dacl,
            null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    let entries = read_dacl_entries(dacl);
    // SAFETY: the descriptor was allocated by GetSecurityInfo and is
    // documented to be released with LocalFree; `dacl` points into it and is
    // not used past this point.
    unsafe { LocalFree(descriptor.cast()) };
    entries
}

fn read_dacl_entries(dacl: *mut ACL) -> io::Result<Vec<DaclEntry>> {
    if dacl.is_null() {
        // A NULL DACL grants everyone full access; for a worker transport
        // that is a security failure, never a state to report as entries.
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "object has a NULL DACL (unrestricted access)",
        ));
    }
    let mut size = ACL_SIZE_INFORMATION {
        AceCount: 0,
        AclBytesInUse: 0,
        AclBytesFree: 0,
    };
    // SAFETY: `dacl` is a valid ACL from GetSecurityInfo and the out struct
    // matches the requested information class.
    if unsafe {
        GetAclInformation(
            dacl,
            (&raw mut size).cast::<c_void>(),
            std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
            AclSizeInformation,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let mut entries = Vec::with_capacity(size.AceCount as usize);
    for index in 0..size.AceCount {
        let mut ace: *mut c_void = null_mut();
        // SAFETY: `index` is below the ACE count reported for this ACL.
        if unsafe { GetAce(dacl, index, &mut ace) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: every ACE begins with an ACE_HEADER.
        let header = unsafe { *ace.cast::<ACE_HEADER>() };
        let allows = match u32::from(header.AceType) {
            ACCESS_ALLOWED_ACE_TYPE => true,
            ACCESS_DENIED_ACE_TYPE => false,
            other => {
                // Callback/object ACEs are never produced by the SDDL this
                // crate writes; treat an unexpected type as a hard error
                // rather than mis-reporting the policy.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported ACE type {other} in DACL"),
                ));
            }
        };
        // SAFETY: allow and deny ACEs share the {header, mask, SidStart}
        // layout, and `SidStart` is the first u32 of the trustee SID stored
        // inline in the ACE.
        let allowed = ace.cast::<ACCESS_ALLOWED_ACE>();
        let access_mask = unsafe { (*allowed).Mask };
        let sid = unsafe { (&raw mut (*allowed).SidStart).cast::<c_void>() };
        entries.push(DaclEntry {
            sid: sid_to_string(sid)?,
            access_mask,
            allows,
        });
    }
    Ok(entries)
}

/// Process id of the client connected to a named-pipe server handle.
///
/// The supervisor compares this against the pid of the child it spawned
/// before any protocol byte is exchanged, so a foreign same-user process that
/// wins the connect race is rejected outright.
pub fn named_pipe_client_process_id<H: AsRawHandle>(pipe: &H) -> io::Result<u32> {
    let mut pid = 0u32;
    // SAFETY: the pipe handle is kept alive by the borrowed `pipe`.
    if unsafe { GetNamedPipeClientProcessId(pipe.as_raw_handle() as HANDLE, &mut pid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(pid)
}

/// Process id of the server side of a named-pipe client handle.
///
/// The worker compares this against `--parent-pid` before speaking the
/// protocol, so it never hands a proof to a squatted endpoint.
pub fn named_pipe_server_process_id<H: AsRawHandle>(pipe: &H) -> io::Result<u32> {
    let mut pid = 0u32;
    // SAFETY: the pipe handle is kept alive by the borrowed `pipe`.
    if unsafe { GetNamedPipeServerProcessId(pipe.as_raw_handle() as HANDLE, &mut pid) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(pid)
}

/// Bytes currently readable from a pipe without blocking.
///
/// The worker's health loop uses this to pace itself: it polls for a parent
/// frame instead of blocking forever in a read that has no timeout on
/// synchronous pipe handles.
pub fn named_pipe_bytes_available<H: AsRawHandle>(pipe: &H) -> io::Result<u32> {
    let mut available = 0u32;
    // SAFETY: null buffers with size 0 are the documented "peek sizes only"
    // form; the handle is kept alive by the borrowed `pipe`.
    if unsafe {
        PeekNamedPipe(
            pipe.as_raw_handle() as HANDLE,
            null_mut(),
            0,
            null_mut(),
            &mut available,
            null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(available)
}

/// Inheritance flags of a [`SecretPipe`]'s two ends (test observability).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretPipeInheritance {
    /// Whether the read end is marked inheritable for child processes.
    pub reader_inheritable: bool,
    /// Whether the write end is marked inheritable for child processes.
    pub writer_inheritable: bool,
}

/// Anonymous pipe delivering the one-shot bootstrap secret to the worker.
///
/// The Windows equivalent of the Unix inherited-fd bootstrap: the read end is
/// marked inheritable so the spawned child receives it at `CreateProcess`
/// time and can address it by the numeric handle value passed on the command
/// line; the write end stays parent-exclusive.
pub struct SecretPipe {
    reader: OwnedHandle,
    writer: OwnedHandle,
}

/// Write end of a [`SecretPipe`], consumed by the single secret write.
pub struct SecretWriter(OwnedHandle);

impl SecretPipe {
    /// Create the pipe with an inheritable read end and a private write end.
    pub fn create_with_inheritable_reader() -> io::Result<Self> {
        let mut reader: HANDLE = null_mut();
        let mut writer: HANDLE = null_mut();
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: null_mut(),
            bInheritHandle: 1,
        };
        // SAFETY: both out pointers are valid; the attributes struct lives
        // across the call. On success both handles are owned by this call.
        if unsafe { CreatePipe(&mut reader, &mut writer, &attributes, 0) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: on success both handles are valid and unowned elsewhere.
        let reader = unsafe { OwnedHandle::from_raw_handle(reader as RawHandle) };
        // SAFETY: as above.
        let writer = unsafe { OwnedHandle::from_raw_handle(writer as RawHandle) };
        // CreatePipe marks both ends inheritable; only the read end may leak
        // into the child, so clear the flag on the write end. A worker that
        // inherited the write end could feed itself its own "secret".
        // SAFETY: `writer` is a valid handle owned above.
        if unsafe { SetHandleInformation(writer.as_raw_handle() as HANDLE, HANDLE_FLAG_INHERIT, 0) }
            == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { reader, writer })
    }

    /// Numeric value of the inheritable read handle, for the command line.
    #[must_use]
    pub fn reader_handle_value(&self) -> usize {
        self.reader.as_raw_handle() as usize
    }

    /// Report the inheritance flags of both ends (test observability).
    pub fn inheritance_flags(&self) -> io::Result<SecretPipeInheritance> {
        let flag_of = |handle: &OwnedHandle| -> io::Result<bool> {
            let mut flags = 0u32;
            // SAFETY: the handle is kept alive by the borrow.
            if unsafe { GetHandleInformation(handle.as_raw_handle() as HANDLE, &mut flags) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(flags & HANDLE_FLAG_INHERIT != 0)
        };
        Ok(SecretPipeInheritance {
            reader_inheritable: flag_of(&self.reader)?,
            writer_inheritable: flag_of(&self.writer)?,
        })
    }

    /// Split into the parent-held read end and the one-shot secret writer.
    ///
    /// The parent keeps the read end alive until the child is spawned (the
    /// numeric value must stay valid through `CreateProcess`) and drops it
    /// with the worker.
    #[must_use]
    pub fn into_reader_and_writer(self) -> (OwnedHandle, SecretWriter) {
        (self.reader, SecretWriter(self.writer))
    }
}

impl SecretWriter {
    /// Write the 32-byte bootstrap secret and close the write end.
    pub fn write_secret(self, secret: &[u8; 32]) -> io::Result<()> {
        let mut file = File::from(self.0);
        file.write_all(secret)
    }
}

/// Read the one-shot bootstrap secret from an inherited handle value.
///
/// Takes ownership of the handle — it is closed when the read completes — so
/// the secret transport cannot be reused. The value is the untrusted command
/// line's claim; an invalid handle fails the read, it cannot fault.
pub fn read_secret_from_handle(handle_value: usize) -> io::Result<[u8; 32]> {
    if handle_value == 0 || handle_value == usize::MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bootstrap handle value is not a real handle",
        ));
    }
    // SAFETY: the value names a handle in this process's handle table (the
    // inherited pipe read end). Wrapping transfers ownership so the one-shot
    // transport is closed after the read; a stale value yields a read error
    // on a handle this process still owns nothing else through.
    let owned = unsafe { OwnedHandle::from_raw_handle(handle_value as RawHandle) };
    let mut reader = File::from(owned);
    let mut secret = [0; 32];
    reader.read_exact(&mut secret)?;
    Ok(secret)
}

/// A kill-on-close Job Object owning the worker process tree.
///
/// The Windows equivalent of the Unix process group plus `PDEATHSIG`: every
/// process assigned to the job (and every descendant it spawns) is terminated
/// when [`JobObject::terminate`] is called or when the last job handle closes
/// — including implicitly when the supervisor itself dies.
pub struct JobObject(OwnedHandle);

impl JobObject {
    /// Create an anonymous job with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
    pub fn create_kill_on_close() -> io::Result<Self> {
        // SAFETY: null attributes and name request a default anonymous job.
        let handle = unsafe { CreateJobObjectW(null(), null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: on success the handle is valid and owned by this call.
        let job = Self(unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) });
        // SAFETY: the all-zero bit pattern is valid for this plain C struct;
        // only the limit flag field is meaningful for this configuration.
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: the job handle is valid; the info pointer/length pair
        // matches the requested information class.
        if unsafe {
            SetInformationJobObject(
                job.0.as_raw_handle() as HANDLE,
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast::<c_void>(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }

    /// Assign a spawned process (for example a `std::process::Child`) to the
    /// job. Descendants it spawns afterwards are job members automatically.
    pub fn assign<H: AsRawHandle>(&self, process: &H) -> io::Result<()> {
        // SAFETY: both handles are kept alive by their borrows.
        if unsafe {
            AssignProcessToJobObject(
                self.0.as_raw_handle() as HANDLE,
                process.as_raw_handle() as HANDLE,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Whether the process is a member of this job (test observability).
    pub fn contains<H: AsRawHandle>(&self, process: &H) -> io::Result<bool> {
        let mut result = 0i32;
        // SAFETY: both handles are kept alive by their borrows.
        if unsafe {
            IsProcessInJob(
                process.as_raw_handle() as HANDLE,
                self.0.as_raw_handle() as HANDLE,
                &mut result,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(result != 0)
    }

    /// Terminate every process in the job, the group-kill primitive used on
    /// authentication failure, health failure, shutdown, and hung workers.
    pub fn terminate(&self) -> io::Result<()> {
        // SAFETY: the job handle is kept alive by the borrow; exit code 1
        // marks the involuntary termination.
        if unsafe { TerminateJobObject(self.0.as_raw_handle() as HANDLE, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// Whether a process with this pid is currently running.
///
/// Best-effort by nature (pids recycle); used by the worker's parent-alive
/// pre-check and by tests asserting descendants died. A pid owned by another
/// user reads as alive (access denied still proves existence).
pub fn process_is_alive(pid: u32) -> io::Result<bool> {
    // SAFETY: OpenProcess with any pid is safe; a null return is handled.
    let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    if handle.is_null() {
        let error = io::Error::last_os_error();
        return match error.raw_os_error().map(|code| code as u32) {
            Some(ERROR_INVALID_PARAMETER) => Ok(false),
            Some(ERROR_ACCESS_DENIED) => Ok(true),
            _ => Err(error),
        };
    }
    // SAFETY: on success the handle is valid and owned by this call.
    let handle = unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) };
    // SAFETY: a zero-timeout wait only samples the signaled state.
    let wait = unsafe { WaitForSingleObject(handle.as_raw_handle() as HANDLE, 0) };
    Ok(wait == WAIT_TIMEOUT)
}
