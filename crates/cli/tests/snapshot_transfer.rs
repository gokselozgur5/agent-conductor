#![cfg(unix)]
use std::{
    fs,
    process::{Command, Stdio},
};

#[test]
fn cli_streams_snapshot_through_real_pipe_without_a_daemon() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    fs::create_dir(root.join("source")).unwrap();
    let content = vec![42_u8; 128 * 1024];
    fs::write(root.join("source/file"), &content).unwrap();
    let manifest = conductor_runtime::snapshot_paths(
        &root.join("source"),
        &root.join("snapshot"),
        &["file".into()],
    )
    .unwrap();
    let hash = manifest.sha256().unwrap();
    let cli = env!("CARGO_BIN_EXE_conductorctl");
    let mut exporter = Command::new(cli)
        .args(["transfer-snapshot", "export", "--source"])
        .arg(root.join("snapshot/tree"))
        .arg("--manifest")
        .arg(root.join("snapshot/manifest.json"))
        .args(["--expected-sha256", &hash])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let received = Command::new(cli)
        .args(["transfer-snapshot", "import", "--destination"])
        .arg(root.join("received"))
        .args(["--expected-sha256", &hash])
        .stdin(exporter.stdout.take().unwrap())
        .output()
        .unwrap();
    assert!(exporter.wait().unwrap().success());
    assert!(
        received.status.success(),
        "{}",
        String::from_utf8_lossy(&received.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&received.stdout).unwrap()["snapshot_sha256"],
        hash
    );
    assert_eq!(fs::read(root.join("received/tree/file")).unwrap(), content);
    let refused = Command::new(cli)
        .args(["transfer-snapshot", "export", "--source"])
        .arg(root.join("snapshot/tree"))
        .arg("--manifest")
        .arg(root.join("snapshot/manifest.json"))
        .args(["--expected-sha256", &"a".repeat(64)])
        .output()
        .unwrap();
    assert_eq!(refused.status.code(), Some(2));
    assert!(refused.stdout.is_empty());
}
