#![cfg(unix)]
use conductor_runtime::{
    AuxiliaryControl, AuxiliaryEvent, AuxiliaryLaunch, AuxiliaryObservation, AuxiliaryReceipt,
    ChildIdentity, read_auxiliary_receipt,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

fn fixture(script: &str) -> (tempfile::TempDir, AuxiliaryLaunch) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    for name in ["work", "receipts"] {
        fs::create_dir(root.join(name)).unwrap();
        fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o700)).unwrap();
    }
    let executable = PathBuf::from("/bin/sh").canonicalize().unwrap();
    let executable_sha256 = format!("{:x}", Sha256::digest(fs::read(&executable).unwrap()));
    let launch = AuxiliaryLaunch {
        version: 1,
        invocation_id: uuid::Uuid::now_v7().to_string(),
        authority_id: "a".repeat(64),
        boot_generation: 9,
        receipt_root: root.join("receipts"),
        executable,
        executable_sha256,
        arguments: vec!["-c".into(), script.into()],
        directory: root.join("work"),
        environment: BTreeMap::new(),
        timeout_ms: 4000,
        heartbeat_timeout_ms: 1500,
        output_limit: 1024,
    };
    (dir, launch)
}
async fn send(writer: &mut ChildStdin, value: &impl serde::Serialize) {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    writer.write_all(&bytes).await.unwrap();
    writer.flush().await.unwrap();
}
async fn event(reader: &mut BufReader<ChildStdout>) -> AuxiliaryEvent {
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), reader.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    serde_json::from_str(&line).unwrap()
}
async fn spawn(launch: &AuxiliaryLaunch) -> (Child, ChildStdin, BufReader<ChildStdout>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_conductor-runner"))
        .args(["--auxiliary", &launch.invocation_id])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut writer = child.stdin.take().unwrap();
    let reader = BufReader::new(child.stdout.take().unwrap());
    send(&mut writer, launch).await;
    (child, writer, reader)
}
async fn ready(
    launch: &AuxiliaryLaunch,
    child: &Child,
    reader: &mut BufReader<ChildStdout>,
) -> AuxiliaryReceipt {
    let AuxiliaryEvent::Ready {
        launch_sha256,
        process,
    } = event(reader).await
    else {
        panic!("expected readiness")
    };
    assert_eq!(launch_sha256, launch.sha256().unwrap());
    assert_eq!(
        process,
        ChildIdentity::observe(child.id().unwrap()).await.unwrap()
    );
    AuxiliaryReceipt {
        version: 1,
        invocation_id: launch.invocation_id.clone(),
        authority_id: launch.authority_id.clone(),
        boot_generation: launch.boot_generation,
        launch_sha256,
        process,
        observation: AuxiliaryObservation::NeverReleased,
    }
}
async fn finish(child: &mut Child) {
    tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
}
fn receipt_path(launch: &AuxiliaryLaunch) -> PathBuf {
    launch
        .receipt_root
        .join(format!("auxiliary-{}.json", launch.invocation_id))
}

#[tokio::test]
async fn release_is_required_and_parent_loss_publishes_no_spawn_proof() {
    let (_dir, launch) = fixture("echo forbidden > output");
    let (mut child, writer, mut reader) = spawn(&launch).await;
    let receipt = ready(&launch, &child, &mut reader).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!launch.directory.join("output").exists());
    drop(writer);
    finish(&mut child).await;
    assert_eq!(
        read_auxiliary_receipt(&launch.receipt_root, &receipt).unwrap(),
        receipt
    );
    assert!(!launch.directory.join("output").exists());
}

#[tokio::test]
async fn wrong_release_is_never_executed_and_killed_wrapper_cannot_claim_no_spawn() {
    for kill in [false, true] {
        let (_dir, launch) = fixture("echo forbidden > output");
        let (mut child, mut writer, mut reader) = spawn(&launch).await;
        let receipt = ready(&launch, &child, &mut reader).await;
        if kill {
            child.start_kill().unwrap();
        } else {
            send(
                &mut writer,
                &AuxiliaryControl::Release {
                    launch_sha256: "0".repeat(64),
                },
            )
            .await;
        }
        finish(&mut child).await;
        assert!(!launch.directory.join("output").exists());
        assert_eq!(
            read_auxiliary_receipt(&launch.receipt_root, &receipt).is_err(),
            kill
        );
    }
}

#[tokio::test]
async fn output_follows_settlement_and_receipt_is_bound_private_and_immutable() {
    let (_dir, mut launch) = fixture("printf '%s' \"$TEST_PRIVATE_VALUE\"; echo done > output");
    launch.environment.insert(
        "TEST_PRIVATE_VALUE".into(),
        "SYNTHETIC_PRIVATE_CANARY".into(),
    );
    let (mut child, mut writer, mut reader) = spawn(&launch).await;
    let mut receipt = ready(&launch, &child, &mut reader).await;
    send(
        &mut writer,
        &AuxiliaryControl::Release {
            launch_sha256: receipt.launch_sha256.clone(),
        },
    )
    .await;
    let AuxiliaryEvent::Finished {
        exit_code, stdout, ..
    } = event(&mut reader).await
    else {
        panic!("expected result")
    };
    assert_eq!(exit_code, Some(0));
    assert_eq!(stdout, b"SYNTHETIC_PRIVATE_CANARY");
    finish(&mut child).await;
    receipt.observation = AuxiliaryObservation::OwnedGroupSettled;
    assert_eq!(
        read_auxiliary_receipt(&launch.receipt_root, &receipt).unwrap(),
        receipt
    );
    let path = receipt_path(&launch);
    let bytes = fs::read(&path).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("SYNTHETIC_PRIVATE_CANARY"));
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let (mut replay, _writer, _reader) = spawn(&launch).await;
    finish(&mut replay).await;
    assert!(!replay.wait().await.unwrap().success());
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let mut wrong = receipt.clone();
    wrong.boot_generation += 1;
    assert!(read_auxiliary_receipt(&launch.receipt_root, &wrong).is_err());
    wrong = receipt.clone();
    wrong.process.start_marker.push('0');
    assert!(read_auxiliary_receipt(&launch.receipt_root, &wrong).is_err());
    let alias = path.with_extension("alias");
    fs::hard_link(&path, &alias).unwrap();
    assert!(read_auxiliary_receipt(&launch.receipt_root, &receipt).is_err());
    fs::remove_file(alias).unwrap();
    let retained = path.with_extension("retained");
    fs::rename(&path, &retained).unwrap();
    symlink(&retained, &path).unwrap();
    assert!(read_auxiliary_receipt(&launch.receipt_root, &receipt).is_err());
}

#[tokio::test]
async fn eof_and_heartbeat_expiry_settle_started_children_without_late_writes() {
    for eof in [true, false] {
        let (_dir, mut launch) = fixture("echo started > started; sleep 2; echo late > late");
        launch.heartbeat_timeout_ms = 300;
        let (mut child, writer, mut reader) = spawn(&launch).await;
        let mut writer = Some(writer);
        let mut receipt = ready(&launch, &child, &mut reader).await;
        send(
            writer.as_mut().unwrap(),
            &AuxiliaryControl::Release {
                launch_sha256: receipt.launch_sha256.clone(),
            },
        )
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !launch.directory.join("started").exists() {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if eof {
            drop(writer.take());
        }
        assert!(matches!(
            event(&mut reader).await,
            AuxiliaryEvent::Stopped { .. }
        ));
        finish(&mut child).await;
        receipt.observation = AuxiliaryObservation::OwnedGroupSettled;
        read_auxiliary_receipt(&launch.receipt_root, &receipt).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!launch.directory.join("late").exists());
    }
}

#[tokio::test]
async fn surviving_group_member_has_no_settlement_receipt() {
    let (_dir, launch) =
        fixture("(exec >/dev/null 2>&1; sleep 1; echo settled > descendant) & exit 0");
    let (mut child, mut writer, mut reader) = spawn(&launch).await;
    let receipt = ready(&launch, &child, &mut reader).await;
    send(
        &mut writer,
        &AuxiliaryControl::Release {
            launch_sha256: receipt.launch_sha256,
        },
    )
    .await;
    finish(&mut child).await;
    assert!(!receipt_path(&launch).exists());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !launch.directory.join("descendant").exists() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn duplicate_live_wrapper_and_changed_executable_do_not_launch() {
    let (dir, mut launch) = fixture("echo forbidden > output");
    let executable = dir.path().canonicalize().unwrap().join("shell");
    fs::copy(&launch.executable, &executable).unwrap();
    launch.executable = executable;
    let (mut child, mut writer, mut reader) = spawn(&launch).await;
    let receipt = ready(&launch, &child, &mut reader).await;
    let (mut duplicate, _duplicate_writer, _duplicate_reader) = spawn(&launch).await;
    finish(&mut duplicate).await;
    assert!(!duplicate.wait().await.unwrap().success());
    fs::write(&launch.executable, b"changed").unwrap();
    send(
        &mut writer,
        &AuxiliaryControl::Release {
            launch_sha256: receipt.launch_sha256.clone(),
        },
    )
    .await;
    finish(&mut child).await;
    read_auxiliary_receipt(&launch.receipt_root, &receipt).unwrap();
    assert!(!launch.directory.join("output").exists());
}

#[tokio::test]
async fn renewals_keep_work_alive_and_lost_terminal_pipe_retains_settlement() {
    let (_dir, mut launch) = fixture("sleep 1.5; echo done > output");
    launch.heartbeat_timeout_ms = 1000;
    let (mut child, mut writer, mut reader) = spawn(&launch).await;
    let mut receipt = ready(&launch, &child, &mut reader).await;
    send(
        &mut writer,
        &AuxiliaryControl::Release {
            launch_sha256: receipt.launch_sha256.clone(),
        },
    )
    .await;
    for _ in 0..2 {
        tokio::time::sleep(Duration::from_millis(600)).await;
        send(
            &mut writer,
            &AuxiliaryControl::Renew {
                launch_sha256: receipt.launch_sha256.clone(),
            },
        )
        .await;
    }
    // The terminal evidence must survive even if delivery to the parent fails.
    drop(reader);
    finish(&mut child).await;
    assert!(launch.directory.join("output").exists());
    receipt.observation = AuxiliaryObservation::OwnedGroupSettled;
    read_auxiliary_receipt(&launch.receipt_root, &receipt).unwrap();
}

#[tokio::test]
async fn receipt_permission_schema_size_and_scope_mutations_are_refused() {
    let (_dir, launch) = fixture("exit 0");
    let (mut child, writer, mut reader) = spawn(&launch).await;
    let receipt = ready(&launch, &child, &mut reader).await;
    drop(writer);
    finish(&mut child).await;
    let path = receipt_path(&launch);
    let original = fs::read(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_auxiliary_receipt(&launch.receipt_root, &receipt).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    for bytes in [
        b"{".to_vec(),
        vec![b' '; 4097],
        serde_json::to_vec(&serde_json::json!({"version":1,"unrecognized":"field"})).unwrap(),
    ] {
        fs::write(&path, bytes).unwrap();
        assert!(read_auxiliary_receipt(&launch.receipt_root, &receipt).is_err());
    }
    fs::write(&path, original).unwrap();
    for field in 0..4 {
        let mut changed = receipt.clone();
        match field {
            0 => changed.authority_id = "b".repeat(64),
            1 => changed.launch_sha256 = "0".repeat(64),
            2 => changed.process.host.boot_id = "0".repeat(64),
            _ => changed.observation = AuxiliaryObservation::OwnedGroupSettled,
        }
        assert!(read_auxiliary_receipt(&launch.receipt_root, &changed).is_err());
    }
    read_auxiliary_receipt(&launch.receipt_root, &receipt).unwrap();
}

#[tokio::test]
async fn receipt_root_inside_child_workspace_is_refused_before_readiness() {
    let (_dir, mut launch) = fixture("echo forbidden > output");
    launch.receipt_root = launch.directory.join("receipts");
    fs::create_dir(&launch.receipt_root).unwrap();
    fs::set_permissions(&launch.receipt_root, fs::Permissions::from_mode(0o700)).unwrap();
    let (mut child, _writer, _reader) = spawn(&launch).await;
    finish(&mut child).await;
    assert!(!child.wait().await.unwrap().success());
    assert!(!launch.directory.join("output").exists());
    assert_eq!(fs::read_dir(&launch.receipt_root).unwrap().count(), 0);
}

#[tokio::test]
async fn unreleased_proof_needs_launch_scope_but_never_an_invented_observation() {
    use conductor_runtime::read_auxiliary_unreleased_receipt;
    let (_dir, launch) = fixture("echo forbidden > output");
    let hash = launch.sha256().unwrap();
    let (mut child, writer, reader) = spawn(&launch).await;
    // Simulate loss before Ready is consumed: no independently observed identity.
    drop(writer);
    drop(reader);
    finish(&mut child).await;
    let read = || {
        read_auxiliary_unreleased_receipt(
            &launch.receipt_root,
            &launch.invocation_id,
            &launch.authority_id,
            launch.boot_generation,
            &hash,
        )
    };
    let receipt = read().unwrap();
    assert_eq!(receipt.observation, AuxiliaryObservation::NeverReleased);
    assert!(!launch.directory.join("output").exists());
    assert!(
        read_auxiliary_unreleased_receipt(
            &launch.receipt_root,
            &launch.invocation_id,
            &"b".repeat(64),
            launch.boot_generation,
            &hash
        )
        .is_err()
    );
    assert!(
        read_auxiliary_unreleased_receipt(
            &launch.receipt_root,
            &launch.invocation_id,
            &launch.authority_id,
            launch.boot_generation + 1,
            &hash
        )
        .is_err()
    );
    assert!(
        read_auxiliary_unreleased_receipt(
            &launch.receipt_root,
            &launch.invocation_id,
            &launch.authority_id,
            launch.boot_generation,
            &"c".repeat(64)
        )
        .is_err()
    );
    let original = fs::read(receipt_path(&launch)).unwrap();
    for change in 0..6 {
        let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
        match change {
            0 => value["observation"] = "owned_group_settled".into(),
            1 => value["process"]["pid"] = 0.into(),
            2 => value["process"]["host"]["boot_id"] = "invalid".into(),
            3 => value["unexpected"] = true.into(),
            4 => value["invocation_id"] = uuid::Uuid::now_v7().to_string().into(),
            _ => value["version"] = 2.into(),
        }
        fs::write(receipt_path(&launch), serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(read().is_err());
    }
    fs::write(receipt_path(&launch), original).unwrap();
    assert_eq!(read().unwrap(), receipt);
}
