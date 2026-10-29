use std::time::Duration;

async fn assert_account_receipt_is_untrusted(
    f: &AccountServiceFixture,
    record: &agent_conductor_store::ExecutionRecord,
) {
    use agent_conductor_store::{EffectAudit, ExecutionState, ExecutionUpdate};
    let identity = conductor_runtime::RunnerProcessIdentity {
        authority_id: "a".repeat(64),
        packet_sha256: record.preparation.packet_sha256.clone(),
        wrapper_identity: record
            .preparation
            .expected_wrapper_identity
            .clone()
            .unwrap(),
    };
    conductor_runtime::write_unstarted_receipt(&f.packet, &identity).unwrap();
    f.store
        .update_execution(
            &f.token,
            record.revision,
            "account-unknown",
            &ExecutionUpdate::Blocked {
                reason: "account outcome unknown".into(),
            },
            EffectAudit {
                id: "account-unknown",
                actor_id: "runtime",
                correlation_id: &f.token,
                now_ms: 100,
            },
        )
        .await
        .unwrap();
    f.service.recover_fenced_receipts().await.unwrap();
    assert_eq!(
        f.store.execution(&f.token).await.unwrap().unwrap().state,
        ExecutionState::Blocked
    );
}

async fn assert_account_ack_refusals(
    f: &AccountServiceFixture,
    record: &agent_conductor_store::ExecutionRecord,
) {
    use agent_conductor_store::{AccountExecutionPeer, EffectAudit, ExecutionUpdate};
    let wrapper = record
        .preparation
        .expected_wrapper_identity
        .clone()
        .unwrap();
    let mut updates = vec![ExecutionUpdate::Acknowledge {
        wrapper_identity: wrapper.clone(),
        packet_sha256: record.preparation.packet_sha256.clone(),
        process: None,
    }];
    // Even structurally valid local process metadata cannot change transport kind.
    updates.push(ExecutionUpdate::Acknowledge {
        wrapper_identity: wrapper.clone(),
        packet_sha256: record.preparation.packet_sha256.clone(),
        process: Some(agent_conductor_store::ExecutionProcess {
            platform: "linux".into(),
            host_id: "a".repeat(64),
            boot_id: "b".repeat(64),
            pid: 1,
            start_marker: "linux-start-ticks:1".into(),
        }),
    });
    let base = AccountExecutionPeer {
        worker_uid: nix::unistd::geteuid().as_raw(),
        coordinator_uid: nix::unistd::geteuid().as_raw(),
        authority_id: "a".repeat(64),
        boot_generation: 1,
    };
    for variant in 0..3 {
        let mut peer = base.clone();
        match variant {
            0 => peer.worker_uid += 1,
            1 => peer.authority_id = "b".repeat(64),
            _ => peer.boot_generation += 1,
        }
        updates.push(ExecutionUpdate::AcknowledgeAccount {
            wrapper_identity: wrapper.clone(),
            packet_sha256: record.preparation.packet_sha256.clone(),
            peer,
        });
    }
    for (index, update) in updates.into_iter().enumerate() {
        let id = format!("invalid-account-ack-{index}");
        assert!(
            f.store
                .update_execution(
                    &f.token,
                    record.revision,
                    &id,
                    &update,
                    EffectAudit {
                        id: &id,
                        actor_id: "runtime",
                        correlation_id: &f.token,
                        now_ms: 100
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(
            f.store.execution(&f.token).await.unwrap().as_ref(),
            Some(record)
        );
    }
}

async fn assert_account_exit_refusal(
    f: &AccountServiceFixture,
    record: &agent_conductor_store::ExecutionRecord,
) {
    use agent_conductor_store::{EffectAudit, ExecutionUpdate};
    assert!(
        f.store
            .update_execution(
                &f.token,
                record.revision,
                "false-local-exit",
                &ExecutionUpdate::Exited {
                    wrapper_identity: record.wrapper_identity.clone().unwrap(),
                    exit_code: Some(0)
                },
                EffectAudit {
                    id: "false-local-exit",
                    actor_id: "runtime",
                    correlation_id: &f.token,
                    now_ms: 100
                }
            )
            .await
            .is_err()
    );
    let mut connection = SqliteConnection::connect(&format!(
        "sqlite://{}",
        f.directory.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    let error =
        sqlx::query("UPDATE executions SET state='exited',exit_code=0 WHERE effect_token=?")
            .bind(&f.token)
            .execute(&mut connection)
            .await
            .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("account channel is not local process exit evidence")
    );
    assert_eq!(
        f.store.execution(&f.token).await.unwrap().as_ref(),
        Some(record)
    );
}

struct AccountServiceFixture {
    directory: tempfile::TempDir,
    store: Store,
    service: PersistentControlService<FixedClock>,
    packet: conductor_runtime::ResumePacket,
    pin: conductor_runtime::VendorPin,
    token: String,
}

async fn account_service_fixture() -> AccountServiceFixture {
    use std::os::unix::fs::PermissionsExt;
    let (directory, store) = lifecycle_store().await;
    let service = PersistentControlService::start_with_reservation(
        store.clone(),
        100,
        FixedClock::new(0),
        Some((&"a".repeat(64), 1)),
    )
    .await
    .unwrap();
    let (task, token) = claim_task_with_lease(&service, 5000).await;
    let row = store.task("task").await.unwrap().unwrap();
    service
        .apply_row(
            row,
            lifecycle_command(
                &task,
                Actor {
                    id: ActorId("worker".into()),
                    role: ActorRole::Worker,
                },
                CommandKind::Start {
                    attempt_id: AttemptId("attempt".into()),
                    lease_epoch: 1,
                },
                "account-start",
            ),
        )
        .await
        .unwrap();
    store
        .claim_effect(
            &token,
            "runner",
            10000,
            agent_conductor_store::EffectAudit {
                id: "account-claim",
                actor_id: "runtime",
                correlation_id: &token,
                now_ms: 100,
            },
        )
        .await
        .unwrap();
    let workspace = directory.path().join("work");
    fs::create_dir(&workspace).unwrap();
    let executable = directory.path().join("vendor");
    // The actual child reads committed SQLite state before making its first effect.
    fs::write(&executable, "#!/usr/bin/python3\nimport sqlite3,pathlib,time\ndb=sqlite3.connect('file:../state.sqlite3?mode=ro',uri=True)\nstate=db.execute('select state from executions').fetchone()[0]\npathlib.Path('ack-state').write_text(state)\ndeadline=time.monotonic()+2\nwhile not pathlib.Path('finish').exists():\n if time.monotonic()>deadline: raise SystemExit(4)\n time.sleep(.01)\nprint('done')\n").unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    let mut packet = unrestricted_capture_launch(&workspace).packet;
    packet.contract.adapter.kind = conductor_runtime::AdapterKind::Codex;
    packet.contract.adapter.executable_sha256 =
        conductor_runtime::binary_sha256(&executable).unwrap();
    let pin = conductor_runtime::VendorPin {
        kind: packet.contract.adapter.kind,
        version: packet.contract.adapter.version.clone(),
        executable,
        executable_sha256: packet.contract.adapter.executable_sha256.clone(),
        conformance_sha256: packet.contract.adapter.conformance_sha256.clone(),
    };
    AccountServiceFixture {
        directory,
        store,
        service,
        packet,
        pin,
        token,
    }
}

async fn checked_account_intent(f: &AccountServiceFixture) -> AccountExecutionIntent {
    use agent_conductor_store::ExecutionState;
    let intent = f
        .service
        .prepare_account_execution(&f.packet, &f.token, "runner")
        .await
        .unwrap();
    let prepared = f.store.execution(&f.token).await.unwrap().unwrap();
    assert_eq!(prepared.state, ExecutionState::Prepared);
    assert!(prepared.account_channel);
    assert!(
        f.store
            .prepare_execution_once(
                &prepared.preparation,
                agent_conductor_store::EffectAudit {
                    id: "wrong-transport",
                    actor_id: "runtime",
                    correlation_id: &f.token,
                    now_ms: 100
                }
            )
            .await
            .is_err()
    );
    assert_eq!(
        prepared.preparation.expected_wrapper_identity.as_deref(),
        Some(intent.identity().wrapper_identity.as_str())
    );
    assert!(
        f.service
            .prepare_account_execution(&f.packet, &f.token, "runner")
            .await
            .is_err()
    );
    assert_eq!(
        f.store.execution(&f.token).await.unwrap().unwrap(),
        prepared
    );
    assert_account_ack_refusals(f, &prepared).await;
    intent
}

#[tokio::test]
async fn account_service_commits_ack_before_effect_and_preserves_unknown_process() {
    use agent_conductor_store::ExecutionState;
    let f = account_service_fixture().await;
    let intent = checked_account_intent(&f).await;
    let socket = f.directory.path().join("worker.sock");
    let uid = nix::unistd::geteuid().as_raw();
    let mut child = account_service_receiver(&socket, intent.identity()).await;
    let address = conductor_runtime::AccountChannelAddress {
        path: socket,
        worker_uid: uid,
        coordinator_uid: uid,
        packet_sha256: f.packet.sha256().unwrap(),
    };
    let (record, mut connection) = f
        .service
        .release_account_execution(intent, &address, &f.pin)
        .await
        .unwrap();
    assert_eq!(record.state, ExecutionState::Started);
    assert!(record.process.is_none());
    assert_eq!(record.account_peer.as_ref().unwrap().worker_uid, uid);
    assert!(
        f.service
            .renew_account_execution(&f.token, "wrong-wrapper", &mut connection)
            .await
            .is_err()
    );
    f.service
        .renew_account_execution(
            &f.token,
            record.wrapper_identity.as_deref().unwrap(),
            &mut connection,
        )
        .await
        .unwrap();
    fs::write(f.packet.contract.workspace.join("finish"), b"ok").unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), connection.event())
            .await
            .unwrap()
            .unwrap(),
        conductor_runtime::RunnerEvent::Finished {
            exit_code: Some(0),
            ..
        }
    ));
    assert!(
        tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert_eq!(
        fs::read_to_string(f.packet.contract.workspace.join("ack-state")).unwrap(),
        "started"
    );
    let stored = f.store.execution(&f.token).await.unwrap().unwrap();
    assert_eq!(
        stored, record,
        "a channel terminal alone must not publish local process exit"
    );
    assert_account_exit_refusal(&f, &stored).await;
    assert!(f.store.has_unfenced_execution("task").await.unwrap());
    assert!(
        f.service
            .validate_capture_eligibility(&stored, &f.packet)
            .await
            .is_err()
    );
    assert_account_receipt_is_untrusted(&f, &stored).await;
}

async fn account_service_receiver(
    socket: &Path,
    identity: &conductor_runtime::RunnerProcessIdentity,
) -> tokio::process::Child {
    let uid = nix::unistd::geteuid().as_raw();
    let wrapper =
        PathBuf::from(std::env::var("CONDUCTOR_TEST_BIN_DIR").unwrap()).join("conductor-runner");
    let mut child = tokio::process::Command::new(wrapper)
        .args([
            "--account-listen",
            socket.to_str().unwrap(),
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
    while !socket.exists() {
        assert!(child.try_wait().unwrap().is_none());
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child
}

#[tokio::test]
async fn account_service_expiry_before_connect_preserves_intent_without_ack() {
    let f = account_service_fixture().await;
    let intent = f
        .service
        .prepare_account_execution(&f.packet, &f.token, "runner")
        .await
        .unwrap();
    let prepared = f.store.execution(&f.token).await.unwrap().unwrap();
    f.service.clock.set(5000);
    let uid = nix::unistd::geteuid().as_raw();
    let address = conductor_runtime::AccountChannelAddress {
        path: f.directory.path().join("absent.sock"),
        worker_uid: uid,
        coordinator_uid: uid,
        packet_sha256: f.packet.sha256().unwrap(),
    };
    assert!(
        f.service
            .release_account_execution(intent, &address, &f.pin)
            .await
            .is_err()
    );
    assert_eq!(
        f.store.execution(&f.token).await.unwrap().unwrap(),
        prepared
    );
    assert!(!f.packet.contract.workspace.join("ack-state").exists());
    assert_account_receipt_is_untrusted(&f, &prepared).await;
}
