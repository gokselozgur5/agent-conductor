#![cfg(unix)]
use conductor_runtime::{ResumePacket, RunnerControl, RunnerEvent, RunnerLaunch};
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

fn launch(directory: &std::path::Path, script: &str, heartbeat: u64) -> RunnerLaunch {
    let packet: ResumePacket = serde_json::from_value(serde_json::json!({
        "contract": {
            "version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"worker_id":"fake",
            "adapter":{"kind":"fake","version":"v1","executable_sha256":"a".repeat(64),"conformance_sha256":"b".repeat(64)},
            "workspace":directory,"starting_head":"c".repeat(40),
            "authority":{"read_roots":[directory],"write_roots":[directory],"network":"denied","allow_workspace_commands":true},
            "instructions":[],"checks":[],"max_runtime_ms":5000
        },"objective":"Controlled wrapper fixture","checkpoint":null,"untrusted_context":[]
    })).expect("packet fixture");
    RunnerLaunch {
        packet,
        executable: "/bin/sh".into(),
        arguments: vec!["-c".into(), script.into()],
        environment: std::collections::BTreeMap::new(),
        output_limit: 1024,
        heartbeat_timeout_ms: heartbeat,
    }
}
async fn wrapper(input: &RunnerLaunch) -> (Child, ChildStdin, BufReader<ChildStdout>) {
    tagged_wrapper(input, &[]).await
}
async fn tagged_wrapper(
    input: &RunnerLaunch,
    arguments: &[String],
) -> (Child, ChildStdin, BufReader<ChildStdout>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_conductor-runner"))
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("wrapper");
    let mut writer = child.stdin.take().expect("stdin");
    let reader = BufReader::new(child.stdout.take().expect("stdout"));
    send(&mut writer, input).await;
    (child, writer, reader)
}
async fn send(writer: &mut ChildStdin, value: &impl serde::Serialize) {
    let mut bytes = serde_json::to_vec(value).expect("encode");
    bytes.push(b'\n');
    writer.write_all(&bytes).await.expect("write frame");
    writer.flush().await.expect("flush");
}
async fn event(reader: &mut BufReader<ChildStdout>) -> RunnerEvent {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), reader.read_until(b'\n', &mut bytes))
        .await
        .expect("event timeout")
        .expect("read");
    serde_json::from_slice(&bytes).expect("event")
}
async fn exited(child: &mut Child) -> std::process::ExitStatus {
    tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .expect("wrapper exits promptly")
        .expect("wait")
}

// Readiness is not a missed-heartbeat test. Maintain the lease until the
// descendant has demonstrably progressed, then deliberately exercise stop.
async fn wait_for_progress(
    writer: &mut ChildStdin,
    hash: &str,
    path: &std::path::Path,
    prior: usize,
) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            send(
                writer,
                &RunnerControl::Renew {
                    packet_sha256: hash.into(),
                    continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock")
                        + 5000,
                },
            )
            .await;
            if std::fs::read(path).unwrap_or_default().len() > prior {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker progress while lease is renewed");
}

#[tokio::test]
async fn no_worker_side_effect_before_matching_recorded_release() {
    let directory = tempfile::tempdir().expect("directory");
    let input = launch(
        directory.path(),
        "printf result > changed; printf done",
        1000,
    );
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    let RunnerEvent::Ready { acknowledgement } = event(&mut reader).await else {
        panic!("ready")
    };
    input
        .packet
        .verify_ack(&acknowledgement)
        .expect("exact packet ack");
    assert!(!directory.path().join("changed").exists());
    send(
        &mut writer,
        &RunnerControl::Release {
            continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock") + 5000,
            packet_sha256: acknowledgement.packet_sha256,
        },
    )
    .await;
    let RunnerEvent::Finished {
        exit_code, stdout, ..
    } = event(&mut reader).await
    else {
        panic!("finished")
    };
    assert_eq!(exit_code, Some(0));
    assert_eq!(stdout, b"done");
    assert_eq!(
        std::fs::read(directory.path().join("changed")).expect("change"),
        b"result"
    );
    assert!(
        exited(&mut child).await.success(),
        "open parent stdin must not prevent wrapper exit"
    );
}

#[tokio::test]
async fn wrong_release_or_parent_loss_before_release_never_launches_worker() {
    for wrong_release in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let input = launch(directory.path(), "printf bad > changed", 1000);
        let (mut child, mut writer, mut reader) = wrapper(&input).await;
        assert!(matches!(
            event(&mut reader).await,
            RunnerEvent::Ready { .. }
        ));
        if wrong_release {
            send(
                &mut writer,
                &RunnerControl::Release {
                    continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock")
                        + 5000,
                    packet_sha256: "wrong".into(),
                },
            )
            .await;
        } else {
            drop(writer);
        }
        assert!(!exited(&mut child).await.success());
        assert!(!directory.path().join("changed").exists());
    }
}

#[tokio::test]
async fn parent_loss_and_missed_renewal_stop_the_worker_group() {
    for parent_loss in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let input = launch(
            directory.path(),
            "/bin/sleep 0.3; ( while [ ! -e probe ]; do /bin/sleep 0.02; done; printf escaped > escaped ) & printf ready > running; wait",
            150,
        );
        let hash = input.packet.sha256().expect("hash");
        let (mut child, mut writer, mut reader) = wrapper(&input).await;
        assert!(matches!(
            event(&mut reader).await,
            RunnerEvent::Ready { .. }
        ));
        send(
            &mut writer,
            &RunnerControl::Release {
                continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock")
                    + 5000,
                packet_sha256: hash.clone(),
            },
        )
        .await;
        wait_for_progress(&mut writer, &hash, &directory.path().join("running"), 0).await;
        if parent_loss {
            drop(writer);
        }
        let RunnerEvent::Stopped { reason, .. } = event(&mut reader).await else {
            panic!("expected stop");
        };
        assert_eq!(
            reason,
            if parent_loss {
                "parent_lost_or_invalid_control"
            } else {
                "lease_or_runtime_expired"
            }
        );
        assert!(exited(&mut child).await.success());
        std::fs::write(directory.path().join("probe"), "probe").unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!directory.path().join("escaped").exists());
    }
}

#[tokio::test]
async fn timely_renewals_keep_attempt_alive_but_wrong_hash_stops_it() {
    let directory = tempfile::tempdir().expect("directory");
    let input = launch(
        directory.path(),
        "printf ready > running; /bin/sleep 5",
        250,
    );
    let hash = input.packet.sha256().expect("hash");
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    send(
        &mut writer,
        &RunnerControl::Release {
            continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock") + 5000,
            packet_sha256: hash.clone(),
        },
    )
    .await;
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(75)).await;
        send(
            &mut writer,
            &RunnerControl::Renew {
                continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock")
                    + 5000,
                packet_sha256: hash.clone(),
            },
        )
        .await;
        assert!(child.try_wait().expect("poll").is_none());
    }
    send(
        &mut writer,
        &RunnerControl::Renew {
            continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock") + 5000,
            packet_sha256: "wrong".into(),
        },
    )
    .await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Stopped { .. }
    ));
    assert!(exited(&mut child).await.success());
}

#[tokio::test]
async fn spawn_failure_is_not_reported_as_successfully_stopped() {
    let directory = tempfile::tempdir().expect("directory");
    let mut input = launch(directory.path(), "", 1000);
    input.executable = directory.path().join("missing-executable");
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    send(
        &mut writer,
        &RunnerControl::Release {
            continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock") + 5000,
            packet_sha256: input.packet.sha256().expect("hash"),
        },
    )
    .await;
    assert!(!exited(&mut child).await.success());
    let mut bytes = Vec::new();
    assert_eq!(reader.read_until(b'\n', &mut bytes).await.expect("EOF"), 0);
}

#[tokio::test]
async fn controller_persists_ack_before_release_and_refusal_has_no_side_effect() {
    use conductor_runtime::{RunnerConnection, RunnerConnectionError};
    for accept in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let input = launch(directory.path(), "printf result > changed", 1000);
        let path = directory.path().join("changed");
        let result = RunnerConnection::launch(
            std::path::Path::new(env!("CARGO_BIN_EXE_conductor-runner")),
            &input,
            None,
            |_, _| async {
                assert!(!path.exists(), "persistence precedes worker execution");
                if accept {
                    Ok(conductor_runtime::continuous_millis().expect("clock") + 1000)
                } else {
                    Err(RunnerConnectionError::Persistence)
                }
            },
        )
        .await;
        if accept {
            let mut connection = result.expect("released");
            assert!(matches!(
                connection.event().await.expect("result"),
                RunnerEvent::Finished {
                    exit_code: Some(0),
                    ..
                }
            ));
            connection.wait().await.expect("settled");
            assert!(path.exists());
        } else {
            assert!(matches!(result, Err(RunnerConnectionError::Persistence)));
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(!path.exists());
        }
    }
}

#[tokio::test]
async fn expired_absolute_release_never_starts_worker() {
    let directory = tempfile::tempdir().expect("directory");
    let input = launch(directory.path(), "printf bad > changed", 1000);
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    send(
        &mut writer,
        &RunnerControl::Release {
            packet_sha256: input.packet.sha256().expect("hash"),
            continuous_deadline_ms: conductor_runtime::continuous_millis()
                .expect("clock")
                .saturating_sub(1),
        },
    )
    .await;
    assert!(!exited(&mut child).await.success());
    assert!(!directory.path().join("changed").exists());
}

#[tokio::test]
async fn absolute_lease_expires_even_when_heartbeats_are_fresh() {
    let directory = tempfile::tempdir().expect("directory");
    let input = launch(directory.path(), "/bin/sleep 1; printf bad > changed", 1000);
    let hash = input.packet.sha256().expect("hash");
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    let deadline = conductor_runtime::continuous_millis().expect("clock") + 250;
    send(
        &mut writer,
        &RunnerControl::Release {
            packet_sha256: hash.clone(),
            continuous_deadline_ms: deadline,
        },
    )
    .await;
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(40)).await;
        send(
            &mut writer,
            &RunnerControl::Renew {
                packet_sha256: hash.clone(),
                continuous_deadline_ms: deadline,
            },
        )
        .await;
    }
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Stopped { .. }
    ));
    assert!(exited(&mut child).await.success());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(!directory.path().join("changed").exists());
}

#[tokio::test]
async fn valid_grant_extends_absolute_lease_before_it_expires() {
    let directory = tempfile::tempdir().expect("directory");
    let input = launch(directory.path(), "/bin/sleep 5", 1000);
    let hash = input.packet.sha256().expect("hash");
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    send(
        &mut writer,
        &RunnerControl::Release {
            packet_sha256: hash.clone(),
            continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock") + 300,
        },
    )
    .await;
    send(
        &mut writer,
        &RunnerControl::Renew {
            packet_sha256: hash,
            continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock") + 1500,
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert!(child.try_wait().expect("poll").is_none());
    send(&mut writer, &RunnerControl::Cancel).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Stopped { .. }
    ));
    assert!(exited(&mut child).await.success());
}

#[tokio::test]
async fn exit_cancel_race_never_claims_a_fence_for_a_surviving_descendant() {
    for delay in [0, 1, 2, 5, 10, 400] {
        let directory = tempfile::tempdir().expect("directory");
        let input = launch(
            directory.path(),
            "( i=0; while [ \"$i\" -lt 40 ]; do printf tick >> escaped; i=$((i+1)); /bin/sleep 0.05; done ) >/dev/null 2>&1 & exit 0",
            1000,
        );
        let (mut child, mut writer, mut reader) = wrapper(&input).await;
        assert!(matches!(
            event(&mut reader).await,
            RunnerEvent::Ready { .. }
        ));
        send(
            &mut writer,
            &RunnerControl::Release {
                packet_sha256: input.packet.sha256().expect("hash"),
                continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock")
                    + 1000,
            },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(delay)).await;
        // A completed wrapper may already have closed its input; that is an
        // observation to inspect, not a reason to respawn it.
        let _ = writer.write_all(b"{\"type\":\"cancel\"}\n").await;
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), reader.read_until(b'\n', &mut bytes))
            .await
            .expect("bounded observation")
            .expect("read");
        let status = exited(&mut child).await;
        let observation = serde_json::from_slice::<RunnerEvent>(&bytes).ok();
        let at_observation = std::fs::read(directory.path().join("escaped")).unwrap_or_default();
        tokio::time::sleep(Duration::from_millis(350)).await;
        match observation {
            Some(RunnerEvent::Stopped { .. }) => assert_eq!(
                std::fs::read(directory.path().join("escaped")).unwrap_or_default(),
                at_observation,
                "a stop observation must not hide continued descendant writes"
            ),
            Some(RunnerEvent::Finished { .. }) => {}
            None => assert!(!status.success(), "uncertain fencing must fail closed"),
            Some(RunnerEvent::Ready { .. }) => panic!("duplicate ready"),
            Some(RunnerEvent::Checkpoint { .. }) => panic!("unexpected fake-worker checkpoint"),
        }
    }
}

#[tokio::test]
async fn stopped_observation_allows_prior_writes_but_no_later_writes() {
    let directory = tempfile::tempdir().expect("directory");
    let input = launch(
        directory.path(),
        "/bin/sleep 0.3; ( while :; do printf tick >> writes; /bin/sleep 0.02; done ) >/dev/null 2>&1 & /bin/sleep 5",
        2000,
    );
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    send(
        &mut writer,
        &RunnerControl::Release {
            packet_sha256: input.packet.sha256().expect("hash"),
            continuous_deadline_ms: conductor_runtime::continuous_millis().expect("clock") + 2000,
        },
    )
    .await;
    let path = directory.path().join("writes");
    let hash = input.packet.sha256().expect("hash");
    wait_for_progress(&mut writer, &hash, &path, 0).await;
    let before = std::fs::read(&path).unwrap();
    wait_for_progress(&mut writer, &hash, &path, before.len()).await;
    send(&mut writer, &RunnerControl::Cancel).await;
    let RunnerEvent::Stopped { reason, .. } = event(&mut reader).await else {
        panic!("expected stop");
    };
    assert_eq!(reason, "cancelled");
    let stopped = std::fs::read(&path).unwrap();
    assert!(exited(&mut child).await.success());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(std::fs::read(&path).unwrap(), stopped);
}

#[tokio::test]
async fn instruction_change_before_ack_or_release_never_starts_worker() {
    use conductor_runtime::InstructionPin;
    use sha2::{Digest, Sha256};
    for before_ack in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let instruction = root.join("policy.md");
        std::fs::write(&instruction, "approved instruction").unwrap();
        let mut input = launch(&root, "printf ran > worker-ran", 1000);
        input.packet.contract.instructions = vec![InstructionPin {
            path: instruction.clone(),
            sha256: format!("{:x}", Sha256::digest(b"approved instruction")),
        }];
        if before_ack {
            std::fs::write(&instruction, "changed instruction").unwrap();
        }
        let (mut child, mut writer, mut reader) = wrapper(&input).await;
        if !before_ack {
            assert!(matches!(
                event(&mut reader).await,
                RunnerEvent::Ready { .. }
            ));
            std::fs::write(&instruction, "changed instruction").unwrap();
            send(
                &mut writer,
                &RunnerControl::Release {
                    packet_sha256: input.packet.sha256().unwrap(),
                    continuous_deadline_ms: conductor_runtime::continuous_millis().unwrap() + 5000,
                },
            )
            .await;
        }
        assert!(!exited(&mut child).await.success());
        let mut extra = Vec::new();
        reader.read_until(b'\n', &mut extra).await.unwrap();
        assert!(
            extra.is_empty(),
            "invalid instruction must not produce a ready or terminal-success frame"
        );
        assert!(!directory.path().join("worker-ran").exists());
    }
}

#[tokio::test]
async fn changed_instruction_refuses_renewal_and_stops_live_worker() {
    use conductor_runtime::InstructionPin;
    use sha2::{Digest, Sha256};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let instruction = root.join("policy.md");
    std::fs::write(&instruction, "approved").unwrap();
    let mut input = launch(
        &root,
        "while :; do printf tick >> writes; /bin/sleep 0.02; done",
        2000,
    );
    input.packet.contract.instructions = vec![InstructionPin {
        path: instruction.clone(),
        sha256: format!("{:x}", Sha256::digest(b"approved")),
    }];
    let hash = input.packet.sha256().unwrap();
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    send(
        &mut writer,
        &RunnerControl::Release {
            packet_sha256: hash.clone(),
            continuous_deadline_ms: conductor_runtime::continuous_millis().unwrap() + 5000,
        },
    )
    .await;
    let output_path = directory.path().join("writes");
    wait_for_progress(&mut writer, &hash, &output_path, 0).await;
    std::fs::write(instruction, "changed").unwrap();
    send(
        &mut writer,
        &RunnerControl::Renew {
            packet_sha256: hash,
            continuous_deadline_ms: conductor_runtime::continuous_millis().unwrap() + 5000,
        },
    )
    .await;
    let RunnerEvent::Stopped { reason, .. } = event(&mut reader).await else {
        panic!("expected stop")
    };
    assert_eq!(reason, "instruction_changed_or_unavailable");
    assert!(exited(&mut child).await.success());
    let stopped = std::fs::read(&output_path).unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(std::fs::read(output_path).unwrap(), stopped);
}

#[tokio::test]
async fn wrapper_streams_claim_before_exit_without_creating_project_files() {
    let directory = tempfile::tempdir().unwrap();
    let claims = serde_json::json!({"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"sequence":1,"completed_work":"Nothing yet","current_state":"Planning","next_action":"Inspect source","open_decisions":[],"running_commands":[],"changed_artifacts":[]});
    let text = serde_json::json!({"conductor_checkpoint":claims}).to_string();
    let line=serde_json::json!({"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":text}]}}).to_string();
    let mut input = launch(
        directory.path(),
        &format!("printf '%s\\n' '{line}'; sleep 3"),
        4000,
    );
    // Controlled stream fixture, not an accepted native vendor pin.
    input.packet.contract.adapter.kind = conductor_runtime::AdapterKind::Claude;
    input.output_limit = 8192;
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    send(
        &mut writer,
        &RunnerControl::Release {
            packet_sha256: input.packet.sha256().unwrap(),
            continuous_deadline_ms: conductor_runtime::continuous_millis().unwrap() + 5000,
        },
    )
    .await;
    let RunnerEvent::Checkpoint { claims } =
        tokio::time::timeout(Duration::from_secs(2), event(&mut reader))
            .await
            .unwrap()
    else {
        panic!("checkpoint before terminal")
    };
    assert_eq!(claims.current_state, "Planning");
    assert!(child.try_wait().unwrap().is_none());
    send(&mut writer, &RunnerControl::Cancel).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Stopped { .. }
    ));
    assert!(exited(&mut child).await.success());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn checkpoint_backpressure_does_not_delay_the_worker_watchdog() {
    use std::fmt::Write as _;
    let directory = tempfile::tempdir().unwrap();
    let mut script = String::new();
    for sequence in 1..=32 {
        let claims = serde_json::json!({"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"sequence":sequence,"completed_work":"x".repeat(500),"current_state":"x".repeat(500),"next_action":"x".repeat(500),"open_decisions":[],"running_commands":[],"changed_artifacts":[]});
        let text = serde_json::json!({"conductor_checkpoint":claims}).to_string();
        let line=serde_json::json!({"type":"assistant","message":{"content":[{"type":"text","text":text}]}}).to_string();
        write!(script, "printf '%s\\n' '{line}'; ").unwrap();
    }
    script.push_str("while :; do printf x >> writes; sleep 0.02; done");
    let mut input = launch(directory.path(), &script, 500);
    input.packet.contract.adapter.kind = conductor_runtime::AdapterKind::Claude;
    input.output_limit = 1024 * 1024;
    let (mut child, mut writer, mut reader) = wrapper(&input).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    send(
        &mut writer,
        &RunnerControl::Release {
            packet_sha256: input.packet.sha256().unwrap(),
            continuous_deadline_ms: conductor_runtime::continuous_millis().unwrap() + 5000,
        },
    )
    .await;
    // Deliberately stop reading progress. The watchdog must still fence writes.
    tokio::time::sleep(Duration::from_millis(900)).await;
    let stopped = std::fs::read(directory.path().join("writes")).unwrap();
    assert!(!stopped.is_empty());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        std::fs::read(directory.path().join("writes")).unwrap(),
        stopped
    );
    loop {
        match event(&mut reader).await {
            RunnerEvent::Checkpoint { .. } => {}
            RunnerEvent::Stopped { .. } => break,
            _ => panic!("expected watchdog stop"),
        }
    }
    assert!(exited(&mut child).await.success());
}

#[tokio::test]
async fn observable_process_identity_rejects_wrong_packet_before_release() {
    for matches in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let input = launch(directory.path(), "printf result > tagged-result", 1000);
        let hash = input.packet.sha256().unwrap();
        let identity = conductor_runtime::RunnerProcessIdentity {
            authority_id: "a".repeat(64),
            packet_sha256: if matches {
                hash.clone()
            } else {
                "0".repeat(64)
            },
            wrapper_identity: "12345678-1234-1234-1234-123456789abc".into(),
        };
        let arg = identity.argument().unwrap();
        let (mut child, mut input_pipe, mut output) =
            tagged_wrapper(&input, std::slice::from_ref(&arg)).await;
        if matches {
            assert!(matches!(
                event(&mut output).await,
                RunnerEvent::Ready { .. }
            ));
            let observed = Command::new("/bin/ps")
                .arg("-ww")
                .arg("-p")
                .arg(child.id().unwrap().to_string())
                .args(["-o", "command="])
                .output()
                .await
                .unwrap();
            assert!(observed.status.success());
            assert!(String::from_utf8(observed.stdout).unwrap().contains(&arg));
            assert!(!directory.path().join("tagged-result").exists());
            send(
                &mut input_pipe,
                &RunnerControl::Release {
                    packet_sha256: hash,
                    continuous_deadline_ms: conductor_runtime::continuous_millis().unwrap() + 1000,
                },
            )
            .await;
            assert!(matches!(
                event(&mut output).await,
                RunnerEvent::Finished {
                    exit_code: Some(0),
                    ..
                }
            ));
            assert!(exited(&mut child).await.success());
            assert!(directory.path().join("tagged-result").exists());
        } else {
            assert!(!exited(&mut child).await.success());
            let mut bytes = Vec::new();
            assert_eq!(output.read_until(b'\n', &mut bytes).await.unwrap(), 0);
            assert!(!directory.path().join("tagged-result").exists());
        }
    }
}

#[tokio::test]
async fn child_exit_after_acknowledgement_refuses_release() {
    use conductor_runtime::RunnerConnection;
    let directory = tempfile::tempdir().unwrap();
    let input = launch(directory.path(), "printf result > changed", 1000);
    let result = RunnerConnection::launch(
        std::path::Path::new(env!("CARGO_BIN_EXE_conductor-runner")),
        &input,
        None,
        |_, birth| async move {
            birth.host.validate().unwrap();
            birth.revalidate().await.unwrap();
            // This is the parent's still-owned fixture child, before any worker release.
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(i32::try_from(birth.pid).unwrap()),
                nix::sys::signal::Signal::SIGKILL,
            )
            .unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(conductor_runtime::continuous_millis().unwrap() + 1000)
        },
    )
    .await;
    assert!(result.is_err());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!directory.path().join("changed").exists());
}

fn unstarted_identity(input: &RunnerLaunch) -> conductor_runtime::RunnerProcessIdentity {
    conductor_runtime::RunnerProcessIdentity {
        authority_id: "a".repeat(64),
        packet_sha256: input.packet.sha256().unwrap(),
        wrapper_identity: "12345678-1234-1234-1234-123456789abc".into(),
    }
}

#[tokio::test]
async fn trusted_wrapper_proves_no_spawn_on_lost_invalid_or_expired_release() {
    use conductor_runtime::read_unstarted_receipt;
    for mode in ["lost", "wrong", "expired", "released"] {
        let root = tempfile::tempdir().unwrap();
        let tree = root.path().join("tree");
        std::fs::create_dir(&tree).unwrap();
        let input = launch(
            &tree.canonicalize().unwrap(),
            "printf changed > changed",
            1000,
        );
        let identity = unstarted_identity(&input);
        let (mut child, mut writer, mut reader) =
            tagged_wrapper(&input, &[identity.argument().unwrap()]).await;
        assert!(matches!(
            event(&mut reader).await,
            RunnerEvent::Ready { .. }
        ));
        assert!(read_unstarted_receipt(&input.packet, &identity).is_err());
        if mode == "lost" {
            drop(writer);
        } else {
            send(
                &mut writer,
                &RunnerControl::Release {
                    packet_sha256: if mode == "wrong" {
                        "0".repeat(64)
                    } else {
                        identity.packet_sha256.clone()
                    },
                    continuous_deadline_ms: if mode == "expired" {
                        0
                    } else {
                        conductor_runtime::continuous_millis().unwrap() + 5000
                    },
                },
            )
            .await;
            if mode == "released" {
                assert!(matches!(
                    event(&mut reader).await,
                    RunnerEvent::Finished { .. }
                ));
            }
        }
        let status = exited(&mut child).await;
        if mode == "released" {
            assert!(status.success());
            assert!(tree.join("changed").exists());
            assert!(read_unstarted_receipt(&input.packet, &identity).is_err());
        } else {
            assert!(!status.success());
            assert!(!tree.join("changed").exists());
            read_unstarted_receipt(&input.packet, &identity).expect("trusted no-spawn proof");
        }
    }
}

#[test]
fn unstarted_receipts_refuse_forged_partial_linked_and_replaced_evidence() {
    use conductor_runtime::{read_unstarted_receipt, write_unstarted_receipt};
    use std::{
        fs,
        os::unix::fs::{PermissionsExt, symlink},
    };
    let root = tempfile::tempdir().unwrap();
    let tree = root.path().join("tree");
    fs::create_dir(&tree).unwrap();
    let input = launch(&tree.canonicalize().unwrap(), "exit 0", 1000);
    let identity = unstarted_identity(&input);
    let path = root
        .path()
        .join(format!("unstarted-{}.json", input.packet.sha256().unwrap()));
    let pending = path.with_extension("pending");
    fs::write(&pending, b"{").unwrap();
    assert!(read_unstarted_receipt(&input.packet, &identity).is_err());
    assert!(write_unstarted_receipt(&input.packet, &identity).is_err());
    fs::remove_file(&pending).unwrap();
    write_unstarted_receipt(&input.packet, &identity).unwrap();
    let bytes = fs::read(&path).unwrap();
    read_unstarted_receipt(&input.packet, &identity).unwrap();
    assert!(write_unstarted_receipt(&input.packet, &identity).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let mut wrong = identity.clone();
    wrong.authority_id = "b".repeat(64);
    assert!(read_unstarted_receipt(&input.packet, &wrong).is_err());
    wrong = identity.clone();
    wrong.wrapper_identity = "22345678-1234-1234-1234-123456789abc".into();
    assert!(read_unstarted_receipt(&input.packet, &wrong).is_err());
    wrong = identity.clone();
    wrong.packet_sha256 = "b".repeat(64);
    assert!(read_unstarted_receipt(&input.packet, &wrong).is_err());
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for (field, value) in [
        ("version", serde_json::json!(2)),
        ("attempt_id", serde_json::json!("stale")),
        ("observation", serde_json::json!("finished")),
        ("extra", serde_json::json!(true)),
    ] {
        let mut bad = original.clone();
        bad[field] = value;
        fs::write(&path, serde_json::to_vec(&bad).unwrap()).unwrap();
        assert!(read_unstarted_receipt(&input.packet, &identity).is_err());
    }
    for bad in [b"{".to_vec(), vec![b'x'; 4097]] {
        fs::write(&path, bad).unwrap();
        assert!(read_unstarted_receipt(&input.packet, &identity).is_err());
    }
    fs::write(&path, &bytes).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read_unstarted_receipt(&input.packet, &identity).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let alias = root.path().join("alias");
    fs::hard_link(&path, &alias).unwrap();
    assert!(read_unstarted_receipt(&input.packet, &identity).is_err());
    fs::remove_file(&alias).unwrap();
    read_unstarted_receipt(&input.packet, &identity).unwrap();
    fs::rename(&path, &alias).unwrap();
    symlink(&alias, &path).unwrap();
    assert!(read_unstarted_receipt(&input.packet, &identity).is_err());
}

#[tokio::test]
async fn killed_wrapper_before_release_cannot_supply_no_spawn_proof() {
    let root = tempfile::tempdir().unwrap();
    let tree = root.path().join("tree");
    std::fs::create_dir(&tree).unwrap();
    let input = launch(
        &tree.canonicalize().unwrap(),
        "printf changed > changed",
        1000,
    );
    let identity = unstarted_identity(&input);
    let (mut child, _writer, mut reader) =
        tagged_wrapper(&input, &[identity.argument().unwrap()]).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Ready { .. }
    ));
    child.start_kill().unwrap();
    assert!(!exited(&mut child).await.success());
    assert!(!tree.join("changed").exists());
    assert!(conductor_runtime::read_unstarted_receipt(&input.packet, &identity).is_err());
}
