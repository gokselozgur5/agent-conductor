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
    let mut child = Command::new(env!("CARGO_BIN_EXE_conductor-runner"))
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
            "( /bin/sleep 1; printf escaped > escaped ) & printf ready > running; wait",
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
                packet_sha256: hash,
            },
        )
        .await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !directory.path().join("running").exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker started");
        if parent_loss {
            drop(writer);
        }
        assert!(matches!(
            event(&mut reader).await,
            RunnerEvent::Stopped { .. }
        ));
        assert!(exited(&mut child).await.success());
        tokio::time::sleep(Duration::from_millis(1100)).await;
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
            |_| async {
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
        }
    }
}

#[tokio::test]
async fn stopped_observation_allows_prior_writes_but_no_later_writes() {
    let directory = tempfile::tempdir().expect("directory");
    let input = launch(
        directory.path(),
        "( while :; do printf tick >> writes; /bin/sleep 0.02; done ) >/dev/null 2>&1 & /bin/sleep 5",
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
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if std::fs::read(&path).unwrap_or_default().len() >= 8 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("descendant must have already written");
    let before = std::fs::read(&path).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        std::fs::read(&path).unwrap().len() > before.len(),
        "live positive control"
    );
    send(&mut writer, &RunnerControl::Cancel).await;
    assert!(matches!(
        event(&mut reader).await,
        RunnerEvent::Stopped { .. }
    ));
    let stopped = std::fs::read(&path).unwrap();
    assert!(exited(&mut child).await.success());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(std::fs::read(&path).unwrap(), stopped);
}
