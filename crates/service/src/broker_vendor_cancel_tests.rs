use super::*;

#[tokio::test]
async fn cancelling_public_vendor_future_revokes_broker_without_claiming_settlement() {
    let setup = native_launch_setup(false).await;
    let prepared = prepare_cancellation_vendor(&setup).await;
    let acknowledged = crate::prepare_native_broker(
        &setup.store,
        &setup.binding,
        &setup.launch,
        &setup.wrapper,
        &conductor_runtime::binary_sha256(&setup.wrapper).unwrap(),
        setup.endpoint,
        101,
    )
    .await
    .unwrap();
    let mut expected = acknowledged.runner.identity().clone();
    let deadline = continuous_millis().unwrap() + 4000;
    let session = acknowledged
        .release(
            setup.store.clone(),
            setup.binding.clone(),
            deadline,
            Duration::from_secs(5),
            broker_owner_audit("cancel-vendor-owner"),
        )
        .await
        .unwrap();
    let (peer, _) =
        conductor_runtime::connect_broker_endpoint(session.proxy_handoff(), Duration::from_secs(2))
            .await
            .unwrap();
    let (_authority, lease) = tokio::sync::watch::channel(Some(deadline));
    let operation = tokio::spawn(session.run_vendor(
        prepared,
        lease,
        broker_owner_audit("cancel-vendor-close"),
    ));
    let pid = read_started_vendor(&setup.launch.root).await;
    let identity = conductor_runtime::ChildIdentity::observe(pid)
        .await
        .unwrap();
    operation.abort();
    assert!(matches!(operation.await, Err(error) if error.is_cancelled()));
    tokio::time::timeout(Duration::from_secs(3), async {
        while identity.revalidate().await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    expected.observation = conductor_runtime::BrokerShutdownObservation::Revoked {
        completed_sequence: 0,
    };
    let receipts =
        conductor_runtime::BrokerReceiptDirectory::open(&setup.launch.receipt_root).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while receipts.read(&expected).is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_first_write_refused(peer, &setup.binding).await;
    assert_progress(&setup.store, &setup.binding, 0, 0).await;
    let reopened = Store::open(setup.root.path().join("state.sqlite3"))
        .await
        .unwrap();
    assert!(
        !reopened
            .file_broker_status(&setup.binding)
            .await
            .unwrap()
            .closed
    );
    assert!(
        reopened
            .execution_has_unresolved_file_broker(&setup.binding.execution_token)
            .await
            .unwrap()
    );
    assert!(
        crate::close_broker_from_receipt(
            &reopened,
            &setup.binding,
            &expected,
            &setup.launch.receipt_root,
            broker_owner_audit("cancel-vendor-recovered")
        )
        .await
        .unwrap()
    );
}

async fn read_started_vendor(root: &Path) -> u32 {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(text) = fs::read_to_string(root.join("vendor-pid"))
                && let Ok(pid) = text.parse()
            {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}

async fn prepare_cancellation_vendor(
    setup: &NativeLaunchSetup,
) -> conductor_runtime::PreparedBrokerVendor {
    prepare_controlled_candidate_vendor(
        setup,
        "#!/bin/sh\nprintf '%s' \"$$\" > vendor-pid\nexec /bin/sleep 30\n",
    )
    .await
}

async fn prepare_controlled_candidate_vendor(
    setup: &NativeLaunchSetup,
    script: &str,
) -> conductor_runtime::PreparedBrokerVendor {
    let executable = setup.root.path().join("controlled-vendor");
    fs::write(&executable, script).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    let pin = conductor_runtime::VendorPin {
        kind: conductor_runtime::AdapterKind::Codex,
        version: "controlled".into(),
        executable_sha256: conductor_runtime::binary_sha256(&executable).unwrap(),
        executable,
        conformance_sha256: "b".repeat(64),
    };
    let packet = persist_packet_for_vendor(setup, &pin).await;
    conductor_runtime::PreparedBrokerVendor::new(
        &pin,
        &packet,
        std::collections::BTreeMap::new(),
        &setup.wrapper,
        &conductor_runtime::binary_sha256(&setup.wrapper).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn service_pause_rejects_candidate_and_preserves_owned_evidence() {
    let (setup, service) = native_launch_setup_with_service(false).await;
    extend_service_fixture_lease(&setup, &service).await;
    let prepared = prepare_cancellation_vendor(&setup).await;
    let acknowledged = crate::prepare_native_broker(
        &setup.store,
        &setup.binding,
        &setup.launch,
        &setup.wrapper,
        &conductor_runtime::binary_sha256(&setup.wrapper).unwrap(),
        setup.endpoint,
        101,
    )
    .await
    .unwrap();
    let deadline = service
        .validate_runtime_owner(prepared.packet())
        .await
        .unwrap();
    let session = acknowledged
        .release(
            setup.store.clone(),
            setup.binding.clone(),
            deadline,
            Duration::from_secs(5),
            broker_owner_audit("service-pause-owner"),
        )
        .await
        .unwrap();
    let (peer, _) =
        conductor_runtime::connect_broker_endpoint(session.proxy_handoff(), Duration::from_secs(2))
            .await
            .unwrap();
    let supervisor = service.clone();
    let operation = tokio::spawn(async move {
        supervisor
            .run_broker_candidate(session, prepared, broker_owner_audit("service-pause-close"))
            .await
    });
    let pid = read_started_vendor(&setup.launch.root).await;
    let identity = conductor_runtime::ChildIdentity::observe(pid)
        .await
        .unwrap();
    let row = setup.store.task("task").await.unwrap().unwrap();
    let running: Task = serde_json::from_str(&row.aggregate_json).unwrap();
    service
        .apply_row(
            row,
            lifecycle_command(
                &running,
                Actor {
                    id: ActorId("operator".into()),
                    role: ActorRole::Operator,
                },
                CommandKind::Pause,
                "broker-service-pause",
            ),
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), operation)
        .await
        .unwrap()
        .unwrap();
    let failure = result.err().expect("paused run must not yield a candidate");
    let outcome = failure
        .outcome()
        .expect("completed supervision evidence retained");
    assert!(matches!(
        outcome.process,
        Err(conductor_runtime::ProcessError::Cancelled)
    ));
    assert!(outcome.broker.is_ok());
    assert!(identity.revalidate().await.is_err());
    assert_first_write_refused(peer, &setup.binding).await;
    let reopened = Store::open(setup.root.path().join("state.sqlite3"))
        .await
        .unwrap();
    let status = reopened.file_broker_status(&setup.binding).await.unwrap();
    assert!(status.closed);
    assert_eq!((status.admitted, status.completed), (0, 0));
    assert_eq!(
        service.workflow_task("task").await.unwrap().state,
        TaskState::Paused
    );
}

async fn extend_service_fixture_lease(
    setup: &NativeLaunchSetup,
    service: &PersistentControlService<FixedClock>,
) {
    let row = setup.store.task("task").await.unwrap().unwrap();
    let claimed: Task = serde_json::from_str(&row.aggregate_json).unwrap();
    service
        .apply_row(
            row,
            lifecycle_command(
                &claimed,
                Actor {
                    id: ActorId("worker".into()),
                    role: ActorRole::Worker,
                },
                CommandKind::Heartbeat {
                    attempt_id: AttemptId("attempt".into()),
                    lease_epoch: 1,
                    lease_duration_ms: 10000,
                    worker_margin_ms: 0,
                },
                "broker-service-heartbeat",
            ),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn malformed_candidate_preserves_successful_process_and_broker_receipt() {
    let (setup, service) = native_launch_setup_with_service(false).await;
    extend_service_fixture_lease(&setup, &service).await;
    let prepared = prepare_controlled_candidate_vendor(
        &setup,
        "#!/bin/sh\nprintf private-invalid-json\nprintf private-stderr >&2\n",
    )
    .await;
    let acknowledged = crate::prepare_native_broker(
        &setup.store,
        &setup.binding,
        &setup.launch,
        &setup.wrapper,
        &conductor_runtime::binary_sha256(&setup.wrapper).unwrap(),
        setup.endpoint,
        101,
    )
    .await
    .unwrap();
    let deadline = service
        .validate_runtime_owner(prepared.packet())
        .await
        .unwrap();
    let session = acknowledged
        .release(
            setup.store.clone(),
            setup.binding.clone(),
            deadline,
            Duration::from_secs(5),
            broker_owner_audit("malformed-candidate-owner"),
        )
        .await
        .unwrap();
    let result = service
        .run_broker_candidate(
            session,
            prepared,
            broker_owner_audit("malformed-candidate-close"),
        )
        .await;
    let failure = result
        .err()
        .expect("invalid protocol cannot yield candidate");
    let outcome = failure.outcome().unwrap();
    let output = outcome.process.as_ref().unwrap();
    assert_eq!(output.exit_code, Some(0));
    assert_eq!(output.stdout, b"private-invalid-json");
    assert_eq!(output.stderr, b"private-stderr");
    assert!(matches!(
        outcome.broker.as_ref().unwrap().observation,
        conductor_runtime::BrokerShutdownObservation::Revoked {
            completed_sequence: 0
        }
    ));
    assert!(!format!("{failure:?}").contains("private-"));
    let reopened = Store::open(setup.root.path().join("state.sqlite3"))
        .await
        .unwrap();
    assert!(
        reopened
            .file_broker_status(&setup.binding)
            .await
            .unwrap()
            .closed
    );
    assert_ne!(
        service.workflow_task("task").await.unwrap().state,
        TaskState::Completed
    );
}

#[tokio::test]
async fn candidate_revalidation_rejects_changed_binding_receipt_and_expired_owner() {
    let (_setup, service, packet, candidate) = Box::pin(completed_candidate_fixture()).await;
    assert!(
        service
            .check_broker_candidate_fixture(
                &packet,
                candidate.binding(),
                candidate.output(),
                candidate.receipt(),
            )
            .await
            .is_ok()
    );
    for mode in 0..5 {
        let mut binding = candidate.binding().clone();
        match mode {
            0 => binding.authority_id = "f".repeat(64),
            1 => binding.boot_generation += 1,
            2 => binding.invocation_id = Uuid::now_v7().to_string(),
            3 => binding.execution_token = "missing".into(),
            4 => binding.lease_epoch += 1,
            _ => unreachable!(),
        }
        assert!(
            service
                .check_broker_candidate_fixture(
                    &packet,
                    &binding,
                    candidate.output(),
                    candidate.receipt(),
                )
                .await
                .is_err(),
            "changed binding {mode}"
        );
    }
    for observation in [
        conductor_runtime::BrokerShutdownObservation::NeverReleased,
        conductor_runtime::BrokerShutdownObservation::Revoked {
            completed_sequence: 1,
        },
    ] {
        let mut receipt = candidate.receipt().clone();
        receipt.observation = observation;
        assert!(
            service
                .check_broker_candidate_fixture(
                    &packet,
                    candidate.binding(),
                    candidate.output(),
                    &receipt,
                )
                .await
                .is_err()
        );
    }
    service.clock.set(10001);
    assert!(
        service
            .check_broker_candidate_fixture(
                &packet,
                candidate.binding(),
                candidate.output(),
                candidate.receipt(),
            )
            .await
            .is_err()
    );
    assert_ne!(
        service.workflow_task("task").await.unwrap().state,
        TaskState::Completed
    );
}

async fn completed_candidate_fixture() -> (
    tempfile::TempDir,
    PersistentControlService<FixedClock>,
    conductor_runtime::ResumePacket,
    crate::BrokerCandidate,
) {
    let (setup, service) = native_launch_setup_with_service(false).await;
    extend_service_fixture_lease(&setup, &service).await;
    let script = r#"#!/bin/sh
printf '%s\n' '{"type":"thread.started","thread_id":"fixture"}' '{"type":"turn.started"}' '{"type":"item.completed","item":{"type":"agent_message","text":"candidate"}}' '{"type":"turn.completed","usage":{}}'
"#;
    let prepared = prepare_controlled_candidate_vendor(&setup, script).await;
    let packet = prepared.packet().clone();
    let acknowledged = crate::prepare_native_broker(
        &setup.store,
        &setup.binding,
        &setup.launch,
        &setup.wrapper,
        &conductor_runtime::binary_sha256(&setup.wrapper).unwrap(),
        setup.endpoint,
        101,
    )
    .await
    .unwrap();
    let deadline = service.validate_runtime_owner(&packet).await.unwrap();
    let session = acknowledged
        .release(
            setup.store.clone(),
            setup.binding.clone(),
            deadline,
            Duration::from_secs(5),
            broker_owner_audit("revalidate-candidate-owner"),
        )
        .await
        .unwrap();
    let candidate = service
        .run_broker_candidate(
            session,
            prepared,
            broker_owner_audit("revalidate-candidate-close"),
        )
        .await
        .unwrap();
    (setup.root, service, packet, candidate)
}
