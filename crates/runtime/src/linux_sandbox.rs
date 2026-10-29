//! Linux check isolation. Full-capability worker launches do not use this module.
use crate::{ProcessSpec, SandboxError};
use nix::{
    fcntl::{FcntlArg, FdFlag, SealFlag, fcntl},
    sys::memfd::{MemFdCreateFlag, memfd_create},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{Seek, Write},
    os::{fd::AsRawFd, unix::process::CommandExt},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinuxCheckRequest {
    pub workspace: PathBuf,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub read_roots: Vec<PathBuf>,
}

fn directory(path: &Path) -> Result<PathBuf, SandboxError> {
    let canonical = path.canonicalize().map_err(|_| SandboxError::InvalidRoot)?;
    if !path.is_absolute()
        || canonical != path
        || !canonical.is_dir()
        || canonical.parent().is_none()
        || ["/proc", "/sys", "/dev"]
            .iter()
            .any(|root| canonical.starts_with(root))
    {
        return Err(SandboxError::InvalidRoot);
    }
    Ok(canonical)
}

impl LinuxCheckRequest {
    fn validate(&self) -> Result<(), SandboxError> {
        directory(&self.workspace)?;
        if !self.executable.is_absolute()
            || self.read_roots.len() > 128
            || self.arguments.len() > 4096
        {
            return Err(SandboxError::InvalidRoot);
        }
        for root in &self.read_roots {
            directory(root)?;
        }
        Ok(())
    }
}

pub(crate) fn command(
    workspace: &Path,
    executable: &Path,
    arguments: &[String],
    read_roots: &[PathBuf],
    timeout: Duration,
) -> Result<ProcessSpec, SandboxError> {
    let request = LinuxCheckRequest {
        workspace: workspace.into(),
        executable: executable.into(),
        arguments: arguments.into(),
        read_roots: read_roots.into(),
    };
    request.validate()?;
    let current = std::env::current_exe().map_err(|_| SandboxError::InvalidRoot)?;
    let mut parent = current.parent().ok_or(SandboxError::InvalidRoot)?;
    if parent.file_name().is_some_and(|name| name == "deps") {
        parent = parent.parent().ok_or(SandboxError::InvalidRoot)?;
    }
    let helper = parent.join("conductor-check-sandbox");
    if !helper.is_file() {
        return Err(SandboxError::Unsupported);
    }
    let encoded = serde_json::to_string(&request).map_err(|_| SandboxError::InvalidRoot)?;
    if encoded.len() > 64 * 1024 {
        return Err(SandboxError::InvalidRoot);
    }
    Ok(ProcessSpec {
        executable: helper,
        arguments: vec![encoded],
        directory: workspace.into(),
        environment: BTreeMap::from([
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("HOME".into(), workspace.to_string_lossy().into_owned()),
            ("TMPDIR".into(), workspace.to_string_lossy().into_owned()),
        ]),
        timeout,
        output_limit: 1024 * 1024,
    })
}

// Classic BPF encoding from linux/filter.h and linux/seccomp.h. Native endian
// fields are consumed by this host's kernel; incompatible syscall ABIs are killed.
fn instruction(out: &mut Vec<u8>, code: u16, jt: u8, jf: u8, value: u32) {
    out.extend(code.to_ne_bytes());
    out.extend([jt, jf]);
    out.extend(value.to_ne_bytes());
}

fn filter() -> Result<Vec<u8>, SandboxError> {
    let arch = match std::env::consts::ARCH {
        "x86_64" => 0xc000_003e,
        "aarch64" => 0xc000_00b7,
        _ => return Err(SandboxError::Unsupported),
    };
    let mut out = Vec::new();
    instruction(&mut out, 0x20, 0, 0, 4); // seccomp_data.arch
    instruction(&mut out, 0x15, 1, 0, arch);
    instruction(&mut out, 0x06, 0, 0, 0x8000_0000); // KILL_PROCESS
    instruction(&mut out, 0x20, 0, 0, 0); // seccomp_data.nr
    instruction(&mut out, 0x35, 0, 1, 0x4000_0000); // reject x32/negative numbers
    instruction(&mut out, 0x06, 0, 0, 0x8000_0000);
    // Deny new sockets and network operations, including pathname Unix sockets.
    // socketpair remains available for local child IPC. io_uring must not provide
    // an alternative socket/connect path around these syscall checks.
    for syscall in [
        nix::libc::SYS_socket,
        nix::libc::SYS_connect,
        nix::libc::SYS_bind,
        nix::libc::SYS_listen,
        nix::libc::SYS_accept,
        nix::libc::SYS_accept4,
        nix::libc::SYS_sendto,
        nix::libc::SYS_sendmsg,
        nix::libc::SYS_sendmmsg,
        nix::libc::SYS_recvfrom,
        nix::libc::SYS_recvmsg,
        nix::libc::SYS_recvmmsg,
        nix::libc::SYS_io_uring_setup,
        nix::libc::SYS_io_uring_enter,
        nix::libc::SYS_io_uring_register,
    ] {
        let nr = u32::try_from(syscall).map_err(|_| SandboxError::Unsupported)?;
        instruction(&mut out, 0x15, 0, 1, nr);
        instruction(&mut out, 0x06, 0, 0, 0x0005_0001); // ERRNO(EPERM)
    }
    instruction(&mut out, 0x06, 0, 0, 0x7fff_0000); // ALLOW
    Ok(out)
}

/// Replace this single-threaded helper with bubblewrap and its isolated check.
/// # Errors
/// Refuses invalid roots, unsupported architectures and missing kernel/tool support.
/// There is no unrestricted execution fallback.
pub fn exec_linux_check(request: &LinuxCheckRequest) -> Result<(), Box<dyn std::error::Error>> {
    request.validate()?;
    let fd = memfd_create(
        c"conductor-check-seccomp",
        MemFdCreateFlag::MFD_ALLOW_SEALING,
    )?;
    let mut program = File::from(fd);
    program.write_all(&filter()?)?;
    program.rewind()?;
    fcntl(
        &program,
        FcntlArg::F_ADD_SEALS(
            SealFlag::F_SEAL_WRITE
                | SealFlag::F_SEAL_GROW
                | SealFlag::F_SEAL_SHRINK
                | SealFlag::F_SEAL_SEAL,
        ),
    )?;
    fcntl(&program, FcntlArg::F_SETFD(FdFlag::empty()))?;
    let mut cmd = Command::new("/usr/bin/bwrap");
    cmd.args([
        "--unshare-user",
        "--unshare-ipc",
        "--unshare-pid",
        "--unshare-net",
        "--unshare-uts",
        "--disable-userns",
        "--die-with-parent",
        "--new-session",
        "--cap-drop",
        "ALL",
    ]);
    for root in ["/usr/bin", "/usr/lib", "/bin", "/lib", "/lib64"] {
        if Path::new(root).is_dir() {
            cmd.args(["--ro-bind", root, root]);
        }
    }
    cmd.args(["--proc", "/proc", "--dev", "/dev"]);
    for root in &request.read_roots {
        cmd.arg("--ro-bind").arg(root).arg(root);
    }
    // The exact check copy is the only writable host mount, even when a selected
    // read root contains its parent. No host /tmp or home is implicitly mounted.
    cmd.arg("--bind")
        .arg(&request.workspace)
        .arg(&request.workspace);
    cmd.arg("--chdir").arg(&request.workspace);
    cmd.arg("--seccomp").arg(program.as_raw_fd().to_string());
    cmd.arg("--")
        .arg(&request.executable)
        .args(&request.arguments);
    // bubblewrap passes caller-owned FDs through to its child. This helper is
    // single-threaded: enumerate first, then close inherited descriptors except
    // stdio and the sealed filter before replacing ourselves.
    let descriptors: Vec<i32> = std::fs::read_dir("/proc/self/fd")?
        .map(|entry| {
            entry.and_then(|entry| {
                entry.file_name().to_string_lossy().parse().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid descriptor")
                })
            })
        })
        .collect::<Result<_, _>>()?;
    for fd in descriptors {
        if fd > 2 && fd != program.as_raw_fd() {
            match nix::unistd::close(fd) {
                Ok(()) | Err(nix::errno::Errno::EBADF) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    Err(cmd.exec().into())
}
