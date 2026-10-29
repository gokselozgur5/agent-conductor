#![cfg(unix)]
use conductor_runtime::{NetworkPolicy, SandboxError, sandboxed_command};
use std::time::Duration;

#[cfg(target_os = "linux")]
#[test]
fn linux_helper_closes_inherited_host_file_descriptors() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let workspace = root.join("work");
    std::fs::create_dir(&workspace).unwrap();
    let private = root.join("private");
    std::fs::write(&private, "private-fd-sentinel").unwrap();
    let script = r"import os
for name in os.listdir('/proc/self/fd'):
    if int(name) <= 2:
        continue
    try:
        value = os.readlink('/proc/self/fd/' + name)
    except FileNotFoundError:
        continue
    assert not value.endswith('/private'), value
print('fd-boundary-ok')
";
    let spec = sandboxed_command(
        &workspace,
        std::path::Path::new("/usr/bin/python3"),
        &["-I".into(), "-S".into(), "-c".into(), script.into()],
        &[],
        NetworkPolicy::Denied,
        Duration::from_secs(5),
    )
    .unwrap();
    // Open the inheritable descriptor only in a disposable single-threaded child,
    // never in the concurrently running Rust test harness.
    let launcher = "import os,sys; fd=os.open(sys.argv[1],os.O_RDONLY); os.set_inheritable(fd,True); os.execv(sys.argv[2],sys.argv[2:])";
    let output = std::process::Command::new("/usr/bin/python3")
        .args(["-I", "-S", "-c", launcher])
        .arg(private)
        .arg(spec.executable)
        .args(spec.arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"fd-boundary-ok\n");
}

#[cfg(target_os = "linux")]
#[test]
fn linux_rejects_host_kernel_filesystems_as_read_roots() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    for virtual_root in ["/proc", "/sys", "/dev"] {
        assert!(matches!(
            sandboxed_command(
                &root,
                std::path::Path::new("/bin/true"),
                &[],
                &[virtual_root.into()],
                NetworkPolicy::Denied,
                Duration::from_secs(2)
            ),
            Err(SandboxError::InvalidRoot)
        ));
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn linux_filter_denies_io_uring_and_incompatible_syscall_abi() {
    use conductor_runtime::run_process;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    // These attempted syscalls run only inside the actual isolated process.
    // io_uring receives EPERM before its pointers are evaluated. An incompatible
    // syscall ABI kills that child instead of reaching an unfiltered syscall.
    let script = r"import ctypes, errno, os, signal, resource
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
libc = ctypes.CDLL(None, use_errno=True)
for nr in (425, 426, 427):
    assert libc.syscall(nr, 0, 0, 0, 0, 0, 0) == -1
    assert ctypes.get_errno() == errno.EPERM
pid = os.fork()
if pid == 0:
    libc.syscall(0x40000000, 0, 0, 0, 0, 0, 0)
    os._exit(91)
_, status = os.waitpid(pid, 0)
assert os.WIFSIGNALED(status) and os.WTERMSIG(status) == signal.SIGSYS
print('syscall-boundary-ok')
";
    let spec = sandboxed_command(
        &root,
        std::path::Path::new("/usr/bin/python3"),
        &["-I".into(), "-S".into(), "-c".into(), script.into()],
        &[],
        NetworkPolicy::Denied,
        Duration::from_secs(5),
    )
    .unwrap();
    let (_sender, receiver) = tokio::sync::watch::channel(false);
    let output = run_process(&spec, receiver).await.unwrap();
    assert_eq!(
        output.exit_code,
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"syscall-boundary-ok\n");
}

#[test]
fn unsupported_network_authority_is_never_silently_broadened() {
    let directory = tempfile::tempdir().expect("directory");
    let root = directory.path().canonicalize().expect("canonical");
    assert!(matches!(
        sandboxed_command(
            &root,
            std::path::Path::new("/bin/sh"),
            &[],
            &[],
            NetworkPolicy::InferenceOnly,
            Duration::from_secs(2)
        ),
        Err(SandboxError::Unsupported)
    ));
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[tokio::test]
async fn sandbox_allows_workspace_and_denies_external_read_write_and_symlink_escape() {
    use conductor_runtime::run_process;
    use std::{fs, os::unix::fs::symlink};
    let directory = tempfile::tempdir().expect("directory");
    let root = directory.path().canonicalize().expect("canonical");
    let workspace = root.join("work \" quoted");
    fs::create_dir(&workspace).expect("workspace");
    fs::write(root.join("private"), b"fixture sentinel").expect("outside");
    symlink(root.join("private"), workspace.join("escape")).expect("symlink");
    let script = "printf inside > allowed; test -f allowed || exit 10; if /bin/cat ../private >/dev/null 2>&1; then exit 11; fi; if (printf outside > ../forbidden) 2>/dev/null; then exit 12; fi; if /bin/cat escape >/dev/null 2>&1; then exit 13; fi; /bin/sh -c 'if (printf child > ../child-forbidden) 2>/dev/null; then exit 14; fi' || exit 14; printf boundary-ok";
    let spec = sandboxed_command(
        &workspace,
        std::path::Path::new("/bin/sh"),
        &["-c".into(), script.into()],
        &[],
        NetworkPolicy::Denied,
        Duration::from_secs(5),
    )
    .expect("spec");
    let (_sender, receiver) = tokio::sync::watch::channel(false);
    let output = run_process(&spec, receiver).await.expect("sandbox process");
    assert_eq!(
        output.exit_code,
        Some(0),
        "sandbox stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"boundary-ok");
    assert_eq!(
        fs::read(workspace.join("allowed")).expect("allowed"),
        b"inside"
    );
    assert!(!root.join("forbidden").exists());
    assert!(!root.join("child-forbidden").exists());
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[test]
fn sandbox_network_probe() {
    let Ok(address) = std::env::var("CONDUCTOR_SANDBOX_PROBE_ADDR") else {
        return;
    };
    let tcp = std::net::TcpStream::connect(address).expect_err("network must be denied");
    assert_eq!(tcp.kind(), std::io::ErrorKind::PermissionDenied);
    let socket = std::env::var("CONDUCTOR_SANDBOX_PROBE_SOCKET").expect("socket fixture");
    let unix =
        std::os::unix::net::UnixStream::connect(socket).expect_err("control socket must be denied");
    assert_eq!(unix.kind(), std::io::ErrorKind::PermissionDenied);
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
#[tokio::test]
async fn sandbox_denies_live_tcp_and_control_socket_connections() {
    use conductor_runtime::run_process;
    let directory = tempfile::tempdir().expect("directory");
    let root = directory.path().canonicalize().expect("canonical");
    let workspace = root.join("work");
    std::fs::create_dir(&workspace).expect("workspace");
    let tcp = std::net::TcpListener::bind("127.0.0.1:0").expect("live TCP fixture");
    let socket = root.join("control.sock");
    let _unix = std::os::unix::net::UnixListener::bind(&socket).expect("live Unix fixture");
    let executable = std::env::current_exe()
        .expect("probe executable")
        .canonicalize()
        .expect("canonical executable");
    let mut spec = sandboxed_command(
        &workspace,
        &executable,
        &[
            "--exact".into(),
            "sandbox_network_probe".into(),
            "--nocapture".into(),
        ],
        &[
            executable.parent().expect("parent").to_path_buf(),
            root.clone(),
        ],
        NetworkPolicy::Denied,
        Duration::from_secs(5),
    )
    .expect("spec");
    spec.environment.insert(
        "CONDUCTOR_SANDBOX_PROBE_ADDR".into(),
        tcp.local_addr().expect("address").to_string(),
    );
    spec.environment.insert(
        "CONDUCTOR_SANDBOX_PROBE_SOCKET".into(),
        socket.to_str().expect("path").into(),
    );
    let (_sender, receiver) = tokio::sync::watch::channel(false);
    let output = run_process(&spec, receiver).await.expect("probe");
    assert_eq!(
        output.exit_code,
        Some(0),
        "probe stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
