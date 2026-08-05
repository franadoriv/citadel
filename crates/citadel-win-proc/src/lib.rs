//! Scoped Win32 process/IPC primitives for the supervised runtime worker.
//!
//! The main `citadel` crate forbids `unsafe` code. The Windows equivalents of
//! its Unix worker-supervision primitives — restrictive named-pipe DACLs,
//! pipe peer validation, inherited-handle secret bootstrap, and Job Object
//! lifecycle containment — all require raw Win32 calls, so those calls live
//! here behind safe, documented wrappers. Every `unsafe` block carries its
//! invariant; nothing in this crate spawns threads or holds global state.

#[cfg(windows)]
mod windows_impl;
#[cfg(windows)]
pub use windows_impl::*;

#[cfg(all(test, windows))]
mod tests {
    use std::io::Read;
    use std::os::windows::io::IntoRawHandle;
    use std::process::Command;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    use super::*;

    fn unique_token() -> String {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        format!(
            "{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        )
    }

    #[test]
    fn secret_pipe_round_trips_through_the_reader_handle() {
        let pipe = SecretPipe::create_with_inheritable_reader().expect("create pipe");
        let (reader, writer) = pipe.into_reader_and_writer();
        writer.write_secret(&[7; 32]).expect("write secret");
        // Releasing the reader into its raw value models the child's view: it
        // receives only the numeric handle value on the command line.
        let value = reader.into_raw_handle() as usize;
        assert_eq!(
            read_secret_from_handle(value).expect("read secret"),
            [7; 32]
        );
    }

    #[test]
    fn secret_reader_is_inheritable_and_writer_is_not() {
        let pipe = SecretPipe::create_with_inheritable_reader().expect("create pipe");
        let flags = pipe.inheritance_flags().expect("handle flags");
        assert!(flags.reader_inheritable, "child must inherit the read end");
        assert!(
            !flags.writer_inheritable,
            "the secret writer must stay parent-exclusive"
        );
    }

    #[test]
    fn read_secret_from_handle_rejects_obviously_invalid_values() {
        for value in [0usize, usize::MAX] {
            let error = read_secret_from_handle(value)
                .expect_err("an invalid handle value must fail closed");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn restricted_pipe_server_grants_access_only_to_the_current_user() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("runtime");
        let _guard = runtime.enter();
        let name = format!(r"\\.\pipe\citadel-win-proc-dacl-{}", unique_token());
        let mut options = tokio::net::windows::named_pipe::ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true);
        let server = create_restricted_pipe_server(&options, &name).expect("create server");
        let entries = handle_dacl_entries(&server).expect("read DACL");
        let me = current_user_sid_string().expect("current user sid");
        assert_eq!(
            entries.len(),
            1,
            "exactly one ACE may exist, got: {entries:?}"
        );
        assert!(entries[0].allows, "the single ACE is an allow ACE");
        assert_eq!(
            entries[0].sid, me,
            "the pipe must be restricted to the current user"
        );
    }

    #[test]
    fn first_instance_flag_blocks_pipe_name_squatting() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .expect("runtime");
        let _guard = runtime.enter();
        let name = format!(r"\\.\pipe\citadel-win-proc-squat-{}", unique_token());
        let mut options = tokio::net::windows::named_pipe::ServerOptions::new();
        options
            .first_pipe_instance(true)
            .reject_remote_clients(true);
        let _server = create_restricted_pipe_server(&options, &name).expect("create server");
        // A second first-instance create on the same name models a squatter
        // racing the supervisor; the kernel must refuse it.
        create_restricted_pipe_server(&options, &name)
            .expect_err("a second first-instance create must be refused");
    }

    #[test]
    fn named_pipe_reports_local_peer_process_ids() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("runtime");
        let name = format!(r"\\.\pipe\citadel-win-proc-peer-{}", unique_token());
        runtime.block_on(async {
            let mut options = tokio::net::windows::named_pipe::ServerOptions::new();
            options
                .first_pipe_instance(true)
                .reject_remote_clients(true);
            let server = create_restricted_pipe_server(&options, &name).expect("create server");
            let client = tokio::net::windows::named_pipe::ClientOptions::new()
                .open(&name)
                .expect("client connects");
            server.connect().await.expect("server accepts");
            assert_eq!(
                named_pipe_client_process_id(&server).expect("client pid"),
                std::process::id()
            );
            assert_eq!(
                named_pipe_server_process_id(&client).expect("server pid"),
                std::process::id()
            );
        });
    }

    #[test]
    fn job_object_terminate_kills_descendant_processes() {
        let pid_file =
            std::env::temp_dir().join(format!("citadel-win-proc-job-{}.pid", unique_token()));
        let script = format!(
            "$p = Start-Process ping -ArgumentList '-n','60','127.0.0.1' -WindowStyle Hidden -PassThru; \
             [IO.File]::WriteAllText('{}', [string]$p.Id); \
             Wait-Process -Id $p.Id",
            pid_file.display()
        );
        let mut child = Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                &script,
            ])
            .spawn()
            .expect("spawn fixture");
        let job = JobObject::create_kill_on_close().expect("create job");
        job.assign(&child).expect("assign child to job");
        assert!(job.contains(&child).expect("membership query"));
        let until = Instant::now() + Duration::from_secs(10);
        while !pid_file.exists() && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(20));
        }
        let descendant: u32 = std::fs::read_to_string(&pid_file)
            .expect("descendant pid file")
            .trim()
            .parse()
            .expect("numeric pid");
        assert!(process_is_alive(descendant).expect("descendant liveness"));
        job.terminate().expect("terminate job");
        assert!(
            !child.wait().expect("reap leader").success(),
            "job termination reports a non-zero worker exit"
        );
        let until = Instant::now() + Duration::from_secs(2);
        let gone = loop {
            if !process_is_alive(descendant).expect("descendant liveness") {
                break true;
            }
            if Instant::now() >= until {
                break false;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        let _ = std::fs::remove_file(&pid_file);
        assert!(gone, "descendant survived job termination");
    }

    #[test]
    fn closing_the_job_handle_kills_the_assigned_process() {
        let mut child = Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                "Start-Sleep -Seconds 60",
            ])
            .spawn()
            .expect("spawn fixture");
        let job = JobObject::create_kill_on_close().expect("create job");
        job.assign(&child).expect("assign child to job");
        // Dropping the only job handle is the supervisor-death model: the
        // kernel must kill every job member without any parent cooperation.
        drop(job);
        let until = Instant::now() + Duration::from_secs(5);
        let exited = loop {
            if child.try_wait().expect("child status").is_some() {
                break true;
            }
            if Instant::now() >= until {
                break false;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(exited, "kill-on-close must terminate the job member");
    }

    #[test]
    fn process_is_alive_reflects_process_liveness() {
        assert!(process_is_alive(std::process::id()).expect("self liveness"));
        let mut child = Command::new("where.exe")
            .arg("citadel-win-proc-no-such-binary")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn fixture");
        let pid = child.id();
        child.wait().expect("fixture exits");
        drop(child);
        let until = Instant::now() + Duration::from_secs(2);
        let gone = loop {
            if !process_is_alive(pid).expect("liveness") {
                break true;
            }
            if Instant::now() >= until {
                break false;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(gone, "an exited, reaped process must read as not alive");
    }

    #[test]
    fn secret_writer_write_is_one_shot_and_consumes_the_pipe() {
        let pipe = SecretPipe::create_with_inheritable_reader().expect("create pipe");
        let (reader, writer) = pipe.into_reader_and_writer();
        writer.write_secret(&[9; 32]).expect("write secret");
        // After the writer is consumed the read end sees exactly 32 bytes and
        // then end-of-stream: no second secret can ever be written.
        let mut file = std::fs::File::from(reader);
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).expect("drain pipe");
        assert_eq!(buffer, vec![9; 32]);
    }
}
