//! Bounded process primitive for the controlled wrapper/fake-agent harness.
//! This is not an adapter sandbox or proof against an escaping hostile process.
use std::{collections::BTreeMap, path::PathBuf, process::Stdio, time::Duration};

use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use thiserror::Error;
use tokio::{io::AsyncReadExt, process::Command, sync::watch};

#[derive(Clone)]
pub struct ProcessSpec {
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub directory: PathBuf,
    /// Explicit environment only. The parent environment is never inherited.
    pub environment: BTreeMap<String, String>,
    pub timeout: Duration,
    pub output_limit: usize,
}

impl std::fmt::Debug for ProcessSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessSpec")
            .field("executable", &self.executable)
            .field("argument_count", &self.arguments.len())
            .field(
                "environment_keys",
                &self.environment.keys().collect::<Vec<_>>(),
            )
            .field("timeout", &self.timeout)
            .field("output_limit", &self.output_limit)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ProcessOutput {
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("invalid process bounds or paths")]
    InvalidSpec,
    #[error("process I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("process output exceeded its byte bound")]
    OutputLimit,
    #[error("process deadline elapsed")]
    Timeout,
    #[error("process cancelled or supervisor channel closed")]
    Cancelled,
    #[error("process group could not be fenced: {0}")]
    Fence(Errno),
}

/// Supervises a controlled process with bounded streams, timeout and cancellation.
/// A caller must independently enforce filesystem/network permissions before use.
/// Cancellation fences the process group and reaps the child before returning.
/// # Errors
/// Fails on invalid input, output overflow, timeout, cancellation or process errors.
pub async fn run_process(
    spec: &ProcessSpec,
    cancellation: watch::Receiver<bool>,
) -> Result<ProcessOutput, ProcessError> {
    run_process_captured(spec, cancellation, &mut Vec::new()).await
}

/// Retains bounded stdout even when a controlled process is cancelled. This is
/// observation data, not proof of capability confinement or task acceptance.
/// # Errors
/// Same supervision/fencing errors as `run_process`; callers must inspect the
/// error and independently validate an adapter envelope before using stop data.
pub async fn run_process_captured(
    spec: &ProcessSpec,
    mut cancellation: watch::Receiver<bool>,
    captured_stdout: &mut Vec<u8>,
) -> Result<ProcessOutput, ProcessError> {
    use std::os::unix::process::CommandExt;
    captured_stdout.clear();
    if !spec.executable.is_absolute()
        || !spec.directory.is_absolute()
        || spec.timeout.is_zero()
        || spec.timeout > Duration::from_secs(86_400)
        || spec.output_limit == 0
        || spec.output_limit > 16 * 1024 * 1024
    {
        return Err(ProcessError::InvalidSpec);
    }
    if *cancellation.borrow() || cancellation.has_changed().is_err() {
        return Err(ProcessError::Cancelled);
    }
    let deadline = tokio::time::Instant::now() + spec.timeout;
    let mut command = Command::new(&spec.executable);
    command
        .args(&spec.arguments)
        .current_dir(&spec.directory)
        .env_clear()
        .envs(&spec.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut child = command.spawn()?;
    let pid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .ok_or(ProcessError::InvalidSpec)?;
    // Retain the pipe handles until fencing finishes. Dropping a full stdout
    // pipe first can race SIGPIPE exit with group signalling on macOS.
    let mut stdout = child.stdout.take().ok_or(ProcessError::InvalidSpec)?;
    let mut stderr = child.stderr.take().ok_or(ProcessError::InvalidSpec)?;
    // The guard also fences on future cancellation/unwind while the child handle
    // is still owned. No PID is persisted or used to kill a recovered process.
    let mut group = ProcessGroup {
        pid: Pid::from_raw(pid),
        armed: true,
    };
    let mut captured_stderr = Vec::new();
    let outcome = {
        let collect = async {
            tokio::try_join!(
                bounded_read(&mut stdout, spec.output_limit, captured_stdout),
                bounded_read(&mut stderr, spec.output_limit, &mut captured_stderr),
            )?;
            Ok::<_, ProcessError>(())
        };
        tokio::select! {
            result = collect => result,
            () = tokio::time::sleep_until(deadline) => Err(ProcessError::Timeout),
            _ = cancellation.changed() => Err(ProcessError::Cancelled),
        }
    };
    // Keep the leader unreaped until the group is fenced so its numeric identity
    // cannot be recycled into an unrelated process group during cancellation.
    // Closed streams are not proof that the child exited: it may close them early.
    match outcome {
        Ok(()) => {}
        Err(error) => {
            group.fence()?;
            child.wait().await?;
            return Err(error);
        }
    }
    let status = tokio::select! {
        result = child.wait() => result?,
        () = tokio::time::sleep_until(deadline) => {
            group.fence()?;
            child.wait().await?;
            return Err(ProcessError::Timeout);
        },
        _ = cancellation.changed() => {
            group.fence()?;
            child.wait().await?;
            return Err(ProcessError::Cancelled);
        },
    };
    // The leader has been reaped. Do not signal this numeric group identity again.
    group.armed = false;
    Ok(ProcessOutput {
        exit_code: status.code(),
        stdout: std::mem::take(captured_stdout),
        stderr: captured_stderr,
    })
}

struct ProcessGroup {
    pid: Pid,
    armed: bool,
}
impl ProcessGroup {
    fn fence(&mut self) -> Result<(), ProcessError> {
        match killpg(self.pid, Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => {
                self.armed = false;
                Ok(())
            }
            Err(error) => Err(ProcessError::Fence(error)),
        }
    }
}
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.fence();
        }
    }
}

async fn bounded_read(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    limit: usize,
    bytes: &mut Vec<u8>,
) -> Result<(), ProcessError> {
    reader
        .take(u64::try_from(limit).map_err(|_| ProcessError::InvalidSpec)? + 1)
        .read_to_end(bytes)
        .await?;
    if bytes.len() > limit {
        return Err(ProcessError::OutputLimit);
    }
    Ok(())
}
