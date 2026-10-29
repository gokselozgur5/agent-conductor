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

#[tokio::test]
async fn frozen_launch_checks_vendor_identity_and_retains_packet() {
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
    let launch =
        conductor_runtime::native_writer_launch(&pin, &packet, std::collections::BTreeMap::new())
            .unwrap();
    assert_eq!(launch.packet, packet);
    assert_eq!(launch.arguments[0], "--yolo");
    let mut connection = conductor_runtime::RunnerConnection::launch(
        std::path::Path::new(env!("CARGO_BIN_EXE_conductor-runner")),
        &launch,
        None,
        |_, _| async { Ok(conductor_runtime::continuous_millis().unwrap() + 1000) },
    )
    .await
    .unwrap();
    assert!(matches!(
        connection.event().await.unwrap(),
        conductor_runtime::RunnerEvent::Finished {
            exit_code: Some(0),
            ..
        }
    ));
    connection.wait().await.unwrap();
    assert_eq!(
        conductor_runtime::read_terminal_receipt(&packet)
            .unwrap()
            .observation,
        "full_group_exit"
    );
    assert_full_group_stop_receipt(launch.clone()).await;
    assert_full_profile_never_starts_without_ack(launch).await;
    assert_account_native_entry(&pin, &packet).await;
    assert_account_socket_entry(&pin, &packet, "refused").await;
    assert_account_socket_entry(&pin, &packet, "accepted").await;
    assert_account_socket_entry(&pin, &packet, "expired").await;
    assert_account_socket_entry(&pin, &packet, "wrong-pin").await;
    assert_account_socket_entry(&pin, &packet, "wrong-receiver").await;
    assert_persistent_account_worker(&pin, &packet).await;
    pin.version = "changed".into();
    assert!(conductor_runtime::full_capability_launch(&pin, &packet).is_err());
}

async fn assert_account_socket_entry(
    pin: &VendorPin,
    packet: &conductor_runtime::ResumePacket,
    decision: &str,
) {
    use conductor_runtime::{
        AccountChannelAddress, AccountNativeLaunch, AccountRunnerConnection, RunnerConnectionError,
        RunnerEvent, RunnerProcessIdentity,
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("runner.sock");
    let mut request = AccountNativeLaunch {
        pin: pin.clone(),
        packet: packet.clone(),
    };
    request.packet.contract.attempt_id = format!("account-socket-{decision}");
    if decision == "wrong-pin" {
        request.pin.executable_sha256 = "a".repeat(64);
    }
    let digest = request.packet.sha256().unwrap();
    let identity = RunnerProcessIdentity {
        authority_id: "a".repeat(64),
        packet_sha256: digest.clone(),
        wrapper_identity: "12345678-1234-1234-1234-123456789abc".into(),
    };
    let uid = nix::unistd::geteuid().as_raw();
    let mut child = start_account_receiver(&path, &identity).await;
    let acknowledged = std::sync::atomic::AtomicBool::new(false);
    let mut expected = identity.clone();
    if decision == "wrong-receiver" {
        expected.wrapper_identity = "12345678-1234-1234-1234-123456789abd".into();
    }
    let connection = AccountRunnerConnection::connect(
        &AccountChannelAddress {
            path,
            worker_uid: uid,
            coordinator_uid: uid,
            packet_sha256: digest,
        },
        &request,
        &expected,
        |ack| {
            request.packet.verify_ack(&ack).unwrap();
            acknowledged.store(true, std::sync::atomic::Ordering::SeqCst);
            async move {
                match decision {
                    "accepted" => Ok(conductor_runtime::continuous_millis().unwrap() + 1000),
                    "expired" => Ok(conductor_runtime::continuous_millis().unwrap() - 1),
                    _ => Err(RunnerConnectionError::Persistence),
                }
            }
        },
    )
    .await;
    assert_eq!(
        acknowledged.load(std::sync::atomic::Ordering::SeqCst),
        !matches!(decision, "wrong-pin" | "wrong-receiver")
    );
    let release = decision == "accepted";
    if release {
        let mut connection = connection.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(3), connection.event())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            RunnerEvent::Finished {
                exit_code: Some(0),
                ..
            }
        ));
        drop(connection);
    } else if matches!(decision, "wrong-pin" | "wrong-receiver") {
        assert!(connection.is_err());
    } else {
        assert!(matches!(
            connection,
            Err(RunnerConnectionError::Persistence)
        ));
    }
    let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.success(), release);
    if !release && !matches!(decision, "wrong-pin" | "wrong-receiver") {
        let receipt = conductor_runtime::read_unstarted_receipt(&request.packet, &identity);
        assert!(receipt.is_ok());
    }
}

async fn start_account_receiver(
    path: &std::path::Path,
    identity: &conductor_runtime::RunnerProcessIdentity,
) -> tokio::process::Child {
    let uid = nix::unistd::geteuid().as_raw();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_conductor-runner"))
        .args([
            "--account-listen",
            path.to_str().unwrap(),
            &uid.to_string(),
            &identity.argument().unwrap(),
        ])
        .env_clear()
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !path.exists() {
        assert!(child.try_wait().unwrap().is_none());
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child
}

async fn assert_account_native_entry(pin: &VendorPin, packet: &conductor_runtime::ResumePacket) {
    use conductor_runtime::{AccountNativeLaunch, RunnerConnection, RunnerEvent};
    let wrapper = std::path::Path::new(env!("CARGO_BIN_EXE_conductor-runner"));
    let mut request = AccountNativeLaunch {
        pin: pin.clone(),
        packet: packet.clone(),
    };
    request.packet.contract.attempt_id = "account-native".into();
    let mut connection =
        RunnerConnection::launch_account_native(wrapper, &request, None, |_, _| async {
            Ok(conductor_runtime::continuous_millis().unwrap() + 1000)
        })
        .await
        .unwrap();
    assert!(matches!(
        connection.event().await.unwrap(),
        RunnerEvent::Finished {
            exit_code: Some(0),
            ..
        }
    ));
    connection.wait().await.unwrap();
    let mut wrong_account = request.clone();
    let profile = wrong_account
        .packet
        .contract
        .full_capability
        .as_mut()
        .unwrap();
    profile.account.uid = profile.account.uid.checked_add(1).unwrap();
    let mut wrong_pin = request;
    wrong_pin.pin.executable_sha256 = "a".repeat(64);
    for invalid in [wrong_account, wrong_pin] {
        let acknowledged = std::sync::atomic::AtomicBool::new(false);
        let result =
            RunnerConnection::launch_account_native(wrapper, &invalid, None, |_, _| async {
                acknowledged.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(conductor_runtime::RunnerConnectionError::Persistence)
            })
            .await;
        assert!(result.is_err());
        assert!(!acknowledged.load(std::sync::atomic::Ordering::SeqCst));
    }
}

async fn assert_full_profile_never_starts_without_ack(mut launch: conductor_runtime::RunnerLaunch) {
    launch.packet.contract.attempt_id = "unstarted-attempt".into();
    launch.executable = "/bin/sh".into();
    launch.arguments = vec!["-c".into(), "touch must-not-start".into()];
    let identity = conductor_runtime::RunnerProcessIdentity {
        authority_id: "a".repeat(64),
        packet_sha256: launch.packet.sha256().unwrap(),
        wrapper_identity: uuid::Uuid::now_v7().to_string(),
    };
    let result = conductor_runtime::RunnerConnection::launch(
        std::path::Path::new(env!("CARGO_BIN_EXE_conductor-runner")),
        &launch,
        Some(&identity),
        |_, _| async { Err(conductor_runtime::RunnerConnectionError::Persistence) },
    )
    .await;
    assert!(result.is_err());
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if conductor_runtime::read_unstarted_receipt(&launch.packet, &identity).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !launch
            .packet
            .contract
            .workspace
            .join("must-not-start")
            .exists()
    );
}

async fn assert_full_group_stop_receipt(mut launch: conductor_runtime::RunnerLaunch) {
    launch.packet.contract.attempt_id = "stopped-attempt".into();
    launch.packet.contract.max_runtime_ms = 5000;
    launch.executable = "/bin/sh".into();
    launch.arguments = vec![
        "-c".into(),
        "printf started > started; exec sleep 10".into(),
    ];
    let mut connection = conductor_runtime::RunnerConnection::launch(
        std::path::Path::new(env!("CARGO_BIN_EXE_conductor-runner")),
        &launch,
        None,
        |_, _| async { Ok(conductor_runtime::continuous_millis().unwrap() + 5000) },
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !launch.packet.contract.workspace.join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    connection.cancel().await.unwrap();
    assert!(matches!(
        connection.event().await.unwrap(),
        conductor_runtime::RunnerEvent::Stopped { .. }
    ));
    connection.wait().await.unwrap();
    let receipt = conductor_runtime::read_terminal_receipt(&launch.packet).unwrap();
    assert_eq!(receipt.observation, "full_group_stop");
    let path = launch
        .packet
        .contract
        .workspace
        .parent()
        .unwrap()
        .join(format!("terminal-{}.json", launch.packet.sha256().unwrap(),));
    let mut forged: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    forged["observation"] = "restricted_stop".into();
    fs::write(path, serde_json::to_vec(&forged).unwrap()).unwrap();
    assert!(conductor_runtime::read_terminal_receipt(&launch.packet).is_err());
}

async fn assert_persistent_account_worker(
    pin: &VendorPin,
    packet: &conductor_runtime::ResumePacket,
) {
    use conductor_runtime::{
        AccountChannelAddress, AccountNativeLaunch, AccountRunnerConnection, RunnerConnectionError,
        RunnerEvent, RunnerProcessIdentity,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("worker.sock");
    let uid = nix::unistd::geteuid().as_raw();
    let mut worker = tokio::process::Command::new(env!("CARGO_BIN_EXE_conductor-runner"))
        .args(["--account-worker", path.to_str().unwrap(), &uid.to_string()])
        .env_clear()
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !path.exists() {
        assert!(worker.try_wait().unwrap().is_none());
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (staged_pin, staged_packet) = prepare_staged_worker(&path, root.path(), pin, packet).await;
    let (pin, packet) = (&staged_pin, &staged_packet);
    assert_worker_digest_binding(&path, pin, packet).await;
    for decision in ["wrong-pin", "refused", "first", "second"] {
        let mut request = AccountNativeLaunch {
            pin: pin.clone(),
            packet: packet.clone(),
        };
        request.packet.contract.attempt_id = format!("persistent-{decision}");
        if decision == "wrong-pin" {
            request.pin.executable_sha256 = "a".repeat(64);
        }
        let identity = RunnerProcessIdentity {
            authority_id: "a".repeat(64),
            packet_sha256: request.packet.sha256().unwrap(),
            wrapper_identity: uuid::Uuid::now_v7().to_string(),
        };
        let address = AccountChannelAddress {
            path: path.clone(),
            worker_uid: uid,
            coordinator_uid: uid,
            packet_sha256: identity.packet_sha256.clone(),
        };
        let ack = std::sync::atomic::AtomicBool::new(false);
        let result = AccountRunnerConnection::connect(&address, &request, &identity, |_| {
            ack.store(true, std::sync::atomic::Ordering::SeqCst);
            async move {
                if decision == "refused" {
                    Err(RunnerConnectionError::Persistence)
                } else {
                    Ok(conductor_runtime::continuous_millis().unwrap() + 1000)
                }
            }
        })
        .await;
        assert_eq!(
            ack.load(std::sync::atomic::Ordering::SeqCst),
            decision != "wrong-pin"
        );
        if matches!(decision, "wrong-pin" | "refused") {
            assert!(result.is_err());
        } else {
            let mut connection = result.unwrap();
            let event = tokio::time::timeout(Duration::from_secs(3), connection.event())
                .await
                .unwrap()
                .unwrap();
            let RunnerEvent::Finished {
                exit_code: Some(0),
                stdout,
                ..
            } = event
            else {
                panic!("staged worker did not finish successfully");
            };
            assert_eq!(stdout, b"selected source bytes\n");
        }
        assert!(worker.try_wait().unwrap().is_none());
        assert!(path.exists());
    }
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(i32::try_from(worker.id().unwrap()).unwrap()),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), worker.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(!path.exists());
}

async fn assert_worker_digest_binding(
    path: &std::path::Path,
    pin: &VendorPin,
    packet: &conductor_runtime::ResumePacket,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let uid = nix::unistd::geteuid().as_raw();
    let address = conductor_runtime::AccountChannelAddress {
        path: path.into(),
        worker_uid: uid,
        coordinator_uid: uid,
        packet_sha256: "0".repeat(64),
    };
    let mut stream = conductor_runtime::connect_account_channel(&address, Duration::from_secs(2))
        .await
        .unwrap();
    let request = conductor_runtime::AccountRunnerRequest {
        launch: conductor_runtime::AccountNativeLaunch {
            pin: pin.clone(),
            packet: packet.clone(),
        },
        identity: conductor_runtime::RunnerProcessIdentity {
            authority_id: "a".repeat(64),
            packet_sha256: packet.sha256().unwrap(),
            wrapper_identity: uuid::Uuid::now_v7().to_string(),
        },
    };
    let mut frame = serde_json::to_vec(&request).unwrap();
    frame.push(b'\n');
    stream.write_all(&frame).await.unwrap();
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

async fn stage_worker_baseline(
    socket: &std::path::Path,
    root: &std::path::Path,
) -> std::path::PathBuf {
    let source = root.join("baseline-source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("baseline"), b"selected source bytes\n").unwrap();
    let snapshot = root.join("baseline-snapshot");
    let manifest =
        conductor_runtime::snapshot_paths(&source, &snapshot, &["baseline".into()]).unwrap();
    let uid = nix::unistd::geteuid().as_raw();
    let address = conductor_runtime::AccountChannelAddress {
        path: socket.into(),
        worker_uid: uid,
        coordinator_uid: uid,
        packet_sha256: manifest.sha256().unwrap(),
    };
    let destination = root.join("worker-baseline");
    conductor_runtime::stage_account_snapshot(
        &address,
        &snapshot.join("tree"),
        &manifest,
        &destination,
    )
    .await
    .unwrap();
    assert_eq!(
        fs::read(destination.join("tree/baseline")).unwrap(),
        b"selected source bytes\n"
    );
    let published = fs::read(destination.join("manifest.json")).unwrap();
    let received: conductor_runtime::SnapshotManifest = serde_json::from_slice(&published).unwrap();
    assert_eq!(received, manifest);
    assert!(
        conductor_runtime::stage_account_snapshot(
            &address,
            &snapshot.join("tree"),
            &manifest,
            &destination
        )
        .await
        .is_err()
    );
    assert_eq!(
        fs::read(destination.join("manifest.json")).unwrap(),
        published
    );
    assert_truncated_staging(&address, &root.join("truncated-baseline")).await;
    destination.join("tree")
}

async fn assert_truncated_staging(
    address: &conductor_runtime::AccountChannelAddress,
    destination: &std::path::Path,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = conductor_runtime::connect_account_channel(address, Duration::from_secs(2))
        .await
        .unwrap();
    let request = conductor_runtime::AccountSnapshotRequest {
        snapshot_sha256: address.packet_sha256.clone(),
        destination: destination.into(),
    };
    // Send header and truncated archive together to exercise buffered payload.
    let mut bytes = serde_json::to_vec(&request).unwrap();
    bytes.extend_from_slice(b"\nACSNAP01");
    stream.write_all(&bytes).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert!(reply.is_empty());
    assert!(!destination.join("manifest.json").exists());
}

async fn prepare_staged_worker(
    socket: &std::path::Path,
    root: &std::path::Path,
    pin: &VendorPin,
    packet: &conductor_runtime::ResumePacket,
) -> (VendorPin, conductor_runtime::ResumePacket) {
    let mut staged_packet = packet.clone();
    staged_packet.contract.workspace = stage_worker_baseline(socket, root).await;
    let mut staged_pin = pin.clone();
    staged_pin.executable = root.join("staged-vendor");
    fs::write(&staged_pin.executable, "#!/bin/sh\ncat baseline\n").unwrap();
    fs::set_permissions(&staged_pin.executable, fs::Permissions::from_mode(0o700)).unwrap();
    staged_pin.executable_sha256 = binary_sha256(&staged_pin.executable).unwrap();
    staged_packet
        .contract
        .adapter
        .executable_sha256
        .clone_from(&staged_pin.executable_sha256);
    (staged_pin, staged_packet)
}
