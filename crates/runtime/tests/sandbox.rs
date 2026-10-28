#![cfg(unix)]
use conductor_runtime::{NetworkPolicy, SandboxError, sandboxed_command};
use std::time::Duration;

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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn seatbelt_allows_workspace_and_denies_external_read_write_and_symlink_escape() {
    use conductor_runtime::run_process;
    use std::{fs, os::unix::fs::symlink};
    let directory = tempfile::tempdir().expect("directory");
    let root = directory.path().canonicalize().expect("canonical");
    let workspace = root.join("work \" quoted");
    fs::create_dir(&workspace).expect("workspace");
    fs::write(root.join("private"), b"fixture sentinel").expect("outside");
    symlink(root.join("private"), workspace.join("escape")).expect("symlink");
    let script = "printf inside > allowed; test -f allowed || exit 10; if /bin/cat ../private >/dev/null 2>&1; then exit 11; fi; if (printf outside > ../forbidden) 2>/dev/null; then exit 12; fi; if /bin/cat escape >/dev/null 2>&1; then exit 13; fi; /bin/sh -c 'if (printf child > ../child-forbidden) 2>/dev/null; then exit 14; fi'; printf boundary-ok";
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

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
#[tokio::test]
async fn seatbelt_denies_live_tcp_and_control_socket_connections() {
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
        &[executable.parent().expect("parent").to_path_buf()],
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
