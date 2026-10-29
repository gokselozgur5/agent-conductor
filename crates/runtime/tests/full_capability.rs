#![cfg(unix)]
use conductor_runtime::{
    AdapterKind, ExecutionAccount, VendorPin, binary_sha256, full_capability_command, run_process,
};
use nix::unistd::{User, getuid};
use std::{fs, os::unix::fs::PermissionsExt, time::Duration};

#[tokio::test]
async fn unrestricted_launch_retains_account_home_and_literal_prompt() {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("vendor");
    fs::write(
        &executable,
        "#!/bin/sh\nprintf '%s\\n' \"$HOME\" \"$USER\" \"$PATH\" \"$@\"\nprintf ok > result\n",
    )
    .unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    let user = User::from_uid(getuid()).unwrap().unwrap();
    let account = ExecutionAccount::resolve(&user.name).unwrap();
    for kind in [AdapterKind::Codex, AdapterKind::Claude] {
        let pin = VendorPin {
            kind,
            version: "fixture".into(),
            executable: executable.clone(),
            executable_sha256: binary_sha256(&executable).unwrap(),
            conformance_sha256: "fixture-only".into(),
        };
        let spec = full_capability_command(
            &account,
            &pin,
            directory.path().to_path_buf(),
            "/usr/bin:/bin",
            "--literal $(touch injected)".into(),
            Duration::from_secs(5),
        )
        .unwrap();
        let (_sender, receiver) = tokio::sync::watch::channel(false);
        let output = run_process(&spec, receiver).await.unwrap();
        assert_eq!(output.exit_code, Some(0));
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.starts_with(&format!("{}\n{}\n", account.home.display(), account.name)));
        assert!(text.contains(match kind {
            AdapterKind::Codex => "--yolo\nexec\n",
            AdapterKind::Claude => "--dangerously-skip-permissions\n",
            AdapterKind::Fake => unreachable!(),
        }));
        assert!(text.ends_with("--\n--literal $(touch injected)\n"));
        assert_eq!(fs::read(directory.path().join("result")).unwrap(), b"ok");
        assert!(!directory.path().join("injected").exists());
    }
}

#[test]
fn frozen_launch_checks_vendor_identity_and_retains_packet() {
    let user = User::from_uid(getuid()).unwrap().unwrap();
    let account = ExecutionAccount::resolve(&user.name).unwrap();
    let executable = std::path::PathBuf::from("/bin/echo");
    let mut pin = VendorPin {
        kind: AdapterKind::Codex,
        version: "fixture".into(),
        executable_sha256: binary_sha256(&executable).unwrap(),
        executable,
        conformance_sha256: "b".repeat(64),
    };
    let directory = tempfile::tempdir().unwrap();
    let packet: conductor_runtime::ResumePacket = serde_json::from_value(serde_json::json!({
        "contract": {
            "version":1, "task_id":"task", "attempt_id":"attempt", "lease_epoch":1,
            "worker_id":"worker", "adapter": {
                "kind":"codex", "version":pin.version,
                "executable_sha256":pin.executable_sha256, "conformance_sha256":pin.conformance_sha256
            },
            "workspace":directory.path(), "starting_head":"c".repeat(40),
            "authority":{"read_roots":["/"],"write_roots":["/"],"network":"unrestricted","allow_workspace_commands":true},
            "full_capability":{"account":account,"path":"/usr/bin:/bin"},
            "instructions":[],"checks":[],"max_runtime_ms":1000
        },
        "objective":"Run a build", "checkpoint":null,"untrusted_context":[]
    })).unwrap();
    let launch = conductor_runtime::full_capability_launch(&pin, &packet).unwrap();
    assert_eq!(launch.packet, packet);
    assert_eq!(launch.arguments[0], "--yolo");
    pin.version = "changed".into();
    assert!(conductor_runtime::full_capability_launch(&pin, &packet).is_err());
}
