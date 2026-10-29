use super::*;
use conductor_runtime::PreparedBrokerRunner;
use conductor_runtime::{BrokerEndpoint, BrokerRunnerLaunch, broker_root_identity};
use sha2::{Digest, Sha256};
use sqlx::Executor;
use std::os::unix::fs::PermissionsExt;

struct NativeLaunchSetup {
    root: tempfile::TempDir,
    store: Store,
    binding: FileBrokerBinding,
    launch: BrokerRunnerLaunch,
    endpoint: BrokerEndpoint,
    wrapper: PathBuf,
}
async fn native_launch_setup(persist: bool) -> NativeLaunchSetup {
    let (root, store, mut binding, mut db) = fixture().await;
    db.execute("UPDATE executions SET state='prepared',wrapper_identity=NULL,process_json=NULL")
        .await
        .unwrap();
    store
        .claim_effect(
            &binding.execution_token,
            "runner",
            10000,
            broker_owner_audit("native-claim"),
        )
        .await
        .unwrap();
    let base = root.path().canonicalize().unwrap();
    for name in ["files", "j", "p", "r"] {
        let path = base.join(name);
        fs::create_dir(&path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    binding.root_identity_sha256 = broker_root_identity(&base.join("files")).unwrap();
    let endpoint = BrokerEndpoint::bind(&base.join("j"), request(&binding).scope).unwrap();
    let launch = BrokerRunnerLaunch {
        scope: request(&binding).scope,
        root: base.join("files"),
        root_identity_sha256: binding.root_identity_sha256.clone(),
        endpoint_directory: base.join("p"),
        receipt_root: base.join("r"),
        journal_handoff: endpoint.environment_value().unwrap(),
        maximum_deadline_ms: continuous_millis().unwrap() + 60000,
        heartbeat_timeout_ms: 5000,
    };
    let wrapper = PathBuf::from(std::env::var_os("CONDUCTOR_TEST_BIN_DIR").unwrap())
        .join("conductor-runner")
        .canonicalize()
        .unwrap();
    let hash = format!("{:x}", Sha256::digest(fs::read(&wrapper).unwrap()));
    if persist {
        crate::record_broker_launch(
            &store,
            &binding,
            &launch,
            &wrapper,
            &hash,
            broker_owner_audit("native-prepare"),
        )
        .await
        .unwrap();
        assert!(
            crate::record_broker_launch(
                &store,
                &binding,
                &launch,
                &wrapper,
                &hash,
                broker_owner_audit("native-replay")
            )
            .await
            .is_err()
        );
        assert!(
            RegisteredBrokerJournal::register(store.clone(), binding.clone())
                .await
                .is_err()
        );
    }
    NativeLaunchSetup {
        root,
        store,
        binding,
        launch,
        endpoint,
        wrapper,
    }
}

#[tokio::test]
async fn service_prelaunch_persists_exact_intent_without_registering() {
    let NativeLaunchSetup {
        root,
        store,
        binding,
        launch,
        endpoint,
        wrapper,
    } = native_launch_setup(true).await;
    let saved = store.file_broker_launch(&binding).await.unwrap();
    assert_eq!(saved.launch_sha256, launch.sha256().unwrap());
    assert_eq!(saved.wrapper_path, wrapper.to_str().unwrap());
    assert_eq!(saved.receipt_root, launch.receipt_root.to_str().unwrap());
    assert_eq!(
        endpoint.environment_value().unwrap(),
        launch.journal_handoff
    );
    assert!(root.path().exists());
    assert!(store.file_broker_status(&binding).await.is_err());
    assert!(
        store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn native_ready_is_acknowledged_before_broker_registration_and_release() {
    let NativeLaunchSetup {
        root: _root,
        store,
        binding,
        launch,
        endpoint,
        wrapper,
    } = native_launch_setup(false).await;
    let hash = format!("{:x}", Sha256::digest(fs::read(&wrapper).unwrap()));
    let acknowledged =
        crate::prepare_native_broker(&store, &binding, &launch, &wrapper, &hash, endpoint, 101)
            .await
            .unwrap();
    let prepared = acknowledged.runner;
    let stream = acknowledged.journal_stream;
    let saved = store
        .execution(&binding.execution_token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.process.unwrap().pid, prepared.identity().process.pid);
    release_write_and_close(&store, &binding, &launch, prepared, stream).await;
}

async fn release_write_and_close(
    store: &Store,
    binding: &FileBrokerBinding,
    launch: &BrokerRunnerLaunch,
    prepared: PreparedBrokerRunner,
    stream: tokio::net::UnixStream,
) {
    let mut session = crate::AcknowledgedBroker {
        runner: prepared,
        journal_stream: stream,
    }
    .release(
        store.clone(),
        binding.clone(),
        continuous_millis().unwrap() + 4000,
        Duration::from_secs(5),
        broker_owner_audit("native-owner"),
    )
    .await
    .unwrap();
    let (mut peer, _) =
        conductor_runtime::connect_broker_endpoint(session.proxy_handoff(), Duration::from_secs(2))
            .await
            .unwrap();
    let mut bytes = serde_json::to_vec(&request(binding)).unwrap();
    bytes.push(b'\n');
    peer.write_all(&bytes).await.unwrap();
    let mut peer = BufReader::new(peer);
    let mut reply = String::new();
    tokio::time::timeout(Duration::from_secs(5), peer.read_line(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_progress(store, binding, 1, 1).await;
    assert_eq!(
        fs::read_to_string(launch.root.join("answer")).unwrap(),
        "durable answer\n"
    );
    session.cancel().await.unwrap();
    let receipt = session
        .finish(broker_owner_audit("native-close"))
        .await
        .unwrap();
    assert_late_write_refused(peer, binding, &launch.root).await;
    assert!(
        !store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .file_broker_launch(binding)
            .await
            .unwrap()
            .launch_sha256,
        receipt.launch_sha256
    );
}

#[tokio::test]
async fn native_launcher_refuses_consumed_intent_without_ack_or_receipt_claim() {
    let NativeLaunchSetup {
        root: _root,
        store,
        binding,
        launch,
        endpoint,
        wrapper,
    } = native_launch_setup(true).await;
    let hash = format!("{:x}", Sha256::digest(fs::read(&wrapper).unwrap()));
    let before = store
        .execution(&binding.execution_token)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::prepare_native_broker(&store, &binding, &launch, &wrapper, &hash, endpoint, 101)
            .await
            .is_err()
    );
    let after = store
        .execution(&binding.execution_token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before, after);
    assert!(after.process.is_none());
    assert_eq!(fs::read_dir(&launch.receipt_root).unwrap().count(), 0);
    assert_eq!(fs::read_dir(&launch.root).unwrap().count(), 0);
    assert!(
        store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn native_ack_audit_failure_retains_intent_and_never_releases_writer() {
    let NativeLaunchSetup {
        root,
        store,
        binding,
        launch,
        endpoint,
        wrapper,
    } = native_launch_setup(false).await;
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        root.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    db.execute("CREATE TRIGGER reject_native_ack BEFORE INSERT ON audit_events WHEN NEW.kind='execution.started' BEGIN SELECT RAISE(FAIL,'fixture'); END").await.unwrap();
    let before = store
        .execution(&binding.execution_token)
        .await
        .unwrap()
        .unwrap();
    let hash = format!("{:x}", Sha256::digest(fs::read(&wrapper).unwrap()));
    assert!(
        crate::prepare_native_broker(&store, &binding, &launch, &wrapper, &hash, endpoint, 101)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .execution(&binding.execution_token)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert!(store.file_broker_owner(&binding).await.is_err());
    assert!(store.file_broker_status(&binding).await.is_err());
    assert!(
        store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
    let receipt_path = launch
        .receipt_root
        .join(format!("broker-{}.json", binding.invocation_id));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !receipt_path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Fixture inspection only: receipt-provided identity is not recovery authority.
    let receipt: conductor_runtime::BrokerShutdownReceipt =
        serde_json::from_slice(&fs::read(receipt_path).unwrap()).unwrap();
    assert!(matches!(
        receipt.observation,
        conductor_runtime::BrokerShutdownObservation::NeverReleased
    ));
    assert_eq!(receipt.launch_sha256, launch.sha256().unwrap());
    assert_eq!(receipt.scope.invocation_id, binding.invocation_id);
    assert_eq!(fs::read_dir(&launch.root).unwrap().count(), 0);
    let audits: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE kind='execution.started'")
            .fetch_one(&mut db)
            .await
            .unwrap();
    assert_eq!(audits, 0);
    assert!(store.file_broker_launch(&binding).await.is_ok());
    assert!(
        crate::recover_unregistered_broker_from_receipt(
            &store,
            &binding,
            binding.boot_generation,
            broker_owner_audit("preowner-no-ack")
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn dropping_acknowledged_broker_never_releases_or_discards_prelaunch() {
    preowner_drop_control(false).await;
}

#[tokio::test]
async fn registered_broker_recovers_after_drop_before_release() {
    preowner_drop_control(true).await;
}

async fn preowner_drop_control(registered: bool) {
    let NativeLaunchSetup {
        root,
        store,
        binding,
        launch,
        endpoint,
        wrapper,
    } = native_launch_setup(false).await;
    let hash = format!("{:x}", Sha256::digest(fs::read(&wrapper).unwrap()));
    let acknowledged =
        crate::prepare_native_broker(&store, &binding, &launch, &wrapper, &hash, endpoint, 101)
            .await
            .unwrap();
    let expected = acknowledged.runner.identity().clone();
    let before = store
        .execution(&binding.execution_token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.process.as_ref().unwrap().pid, expected.process.pid);
    assert!(
        crate::recover_unregistered_broker_from_receipt(
            &store,
            &binding,
            binding.boot_generation,
            broker_owner_audit("preowner-missing-receipt")
        )
        .await
        .is_err()
    );
    if registered {
        drop(
            RegisteredBrokerJournal::register(store.clone(), binding.clone())
                .await
                .unwrap(),
        );
    }
    drop(acknowledged);
    let directory = conductor_runtime::BrokerReceiptDirectory::open(&launch.receipt_root).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if directory.read(&expected).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let receipt = directory.read(&expected).unwrap();
    assert!(matches!(
        receipt.observation,
        conductor_runtime::BrokerShutdownObservation::NeverReleased
    ));
    assert_eq!(
        store
            .execution(&binding.execution_token)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert!(store.file_broker_owner(&binding).await.is_err());
    assert_eq!(store.file_broker_status(&binding).await.is_ok(), registered);
    assert!(
        store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
    assert_eq!(fs::read_dir(&launch.root).unwrap().count(), 0);
    assert_preowner_restart(&root, &store, &binding).await;
    if registered {
        assert!(store.file_broker_status(&binding).await.unwrap().closed);
        assert!(
            crate::recover_unregistered_broker_from_receipt(
                &store,
                &binding,
                binding.boot_generation + 1,
                broker_owner_audit("registered-strict-replay")
            )
            .await
            .is_err()
        );
    }
}

async fn assert_preowner_restart(
    root: &tempfile::TempDir,
    store: &Store,
    binding: &FileBrokerBinding,
) {
    prepare_broker_restart_packet(root, binding).await;
    seed_invalid_preowner_page(root, binding).await;
    let reopened = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    let generation = binding.boot_generation + 1;
    let service = PersistentControlService::start_with_reservation(
        reopened.clone(),
        200,
        FixedClock::new(0),
        Some((&binding.authority_id, generation.try_into().unwrap())),
    )
    .await
    .unwrap();
    assert_eq!(service.recover_interrupted_workflows().await.unwrap(), 0);
    assert!(service.broker_preowner_cursor.lock().await.is_some());
    assert!(
        store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
    assert_eq!(
        service
            .clone()
            .recover_interrupted_workflows()
            .await
            .unwrap(),
        0
    );

    assert!(service.broker_preowner_cursor.lock().await.is_none());
    assert!(std::sync::Arc::ptr_eq(
        &service.broker_preowner_cursor,
        &service.clone().broker_preowner_cursor
    ));
    assert!(
        !crate::recover_unowned_broker_from_receipt(
            store,
            binding,
            generation,
            broker_owner_audit("preowner-replay")
        )
        .await
        .unwrap()
    );
    assert!(
        !store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
    let first = store
        .pending_unregistered_broker_launch_page(None)
        .await
        .unwrap();
    assert_eq!(first.len(), 128);
    assert!(first.iter().all(|candidate| candidate.binding.is_err()));
    let remaining = store
        .pending_unregistered_broker_launch_page(Some(&first.last().unwrap().invocation_id))
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].binding.is_err());
}

async fn seed_invalid_preowner_page(root: &tempfile::TempDir, binding: &FileBrokerBinding) {
    seed_invalid_broker_page(root, binding).await;
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        root.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    let mut tx = db.begin().await.unwrap();
    sqlx::query("UPDATE executions SET process_json=(SELECT process_json FROM executions WHERE effect_token=?) WHERE effect_token LIKE 'invalid-broker-%'")
        .bind(&binding.execution_token).execute(&mut *tx).await.unwrap();
    tx.execute("INSERT INTO file_broker_launches(invocation_id,execution_token,binding_json,launch_json,state,created_at_ms) SELECT invocation_id,execution_token,binding_json,'{}','open',0 FROM file_brokers WHERE execution_token LIKE 'invalid-broker-%'").await.unwrap();
    tx.execute("DELETE FROM file_broker_owners WHERE invocation_id IN (SELECT invocation_id FROM file_brokers WHERE execution_token LIKE 'invalid-broker-%')").await.unwrap();
    tx.execute("DELETE FROM file_brokers WHERE execution_token LIKE 'invalid-broker-%'")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        db.fetch_optional("PRAGMA foreign_key_check")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn owned_session_refuses_release_when_owner_audit_fails() {
    let NativeLaunchSetup {
        root,
        store,
        binding,
        launch,
        endpoint,
        wrapper,
    } = native_launch_setup(false).await;
    let hash = format!("{:x}", Sha256::digest(fs::read(&wrapper).unwrap()));
    let acknowledged =
        crate::prepare_native_broker(&store, &binding, &launch, &wrapper, &hash, endpoint, 101)
            .await
            .unwrap();
    let expected = acknowledged.runner.identity().clone();
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        root.path().join("state.sqlite3").display(),
    ))
    .await
    .unwrap();
    db.execute("CREATE TRIGGER reject_session_owner BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.release_authorized' BEGIN SELECT RAISE(FAIL,'fixture'); END").await.unwrap();
    assert!(
        acknowledged
            .release(
                store.clone(),
                binding.clone(),
                continuous_millis().unwrap() + 4000,
                Duration::from_secs(5),
                broker_owner_audit("session-owner-fail"),
            )
            .await
            .is_err()
    );
    let directory = conductor_runtime::BrokerReceiptDirectory::open(&launch.receipt_root).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if directory.read(&expected).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        directory.read(&expected).unwrap().observation,
        conductor_runtime::BrokerShutdownObservation::NeverReleased
    ));
    assert!(store.file_broker_owner(&binding).await.is_err());
    assert_progress(&store, &binding, 0, 0).await;
    assert_eq!(fs::read_dir(&launch.root).unwrap().count(), 0);
    assert!(
        store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
    assert!(
        crate::recover_unowned_broker_from_receipt(
            &store,
            &binding,
            binding.boot_generation,
            broker_owner_audit("session-owner-recover"),
        )
        .await
        .unwrap()
    );
    assert!(store.file_broker_status(&binding).await.unwrap().closed);
}

#[tokio::test]
async fn owned_session_drop_and_cancelled_finish_retain_durable_ownership() {
    use std::{future::Future, task::Poll};
    for cancel_finish in [false, true] {
        let NativeLaunchSetup {
            root: _root,
            store,
            binding,
            launch,
            endpoint,
            wrapper,
        } = native_launch_setup(false).await;
        let hash = format!("{:x}", Sha256::digest(fs::read(&wrapper).unwrap()));
        let acknowledged =
            crate::prepare_native_broker(&store, &binding, &launch, &wrapper, &hash, endpoint, 101)
                .await
                .unwrap();
        let mut expected = acknowledged.runner.identity().clone();
        let mut session = acknowledged
            .release(
                store.clone(),
                binding.clone(),
                continuous_millis().unwrap() + 4000,
                Duration::from_secs(5),
                broker_owner_audit("session-drop-owner"),
            )
            .await
            .unwrap();
        session
            .renew(continuous_millis().unwrap() + 4000)
            .await
            .unwrap();
        assert!(session.renew(0).await.is_err());
        let (peer, _) = conductor_runtime::connect_broker_endpoint(
            session.proxy_handoff(),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        if cancel_finish {
            let mut finish = Box::pin(session.finish(broker_owner_audit("cancelled-close")));
            std::future::poll_fn(|cx| {
                assert!(finish.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(finish);
        } else {
            drop(session);
        }
        expected.observation = conductor_runtime::BrokerShutdownObservation::Revoked {
            completed_sequence: 0,
        };
        let directory =
            conductor_runtime::BrokerReceiptDirectory::open(&launch.receipt_root).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if directory.read(&expected).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_progress(&store, &binding, 0, 0).await;
        assert!(store.file_broker_owner(&binding).await.is_ok());
        assert_first_write_refused(peer, &binding).await;
        assert_eq!(fs::read_dir(&launch.root).unwrap().count(), 0);
        assert!(
            store
                .execution_has_unresolved_file_broker(&binding.execution_token)
                .await
                .unwrap()
        );
        assert!(
            crate::close_broker_from_receipt(
                &store,
                &binding,
                &expected,
                &launch.receipt_root,
                broker_owner_audit("session-drop-independent-close"),
            )
            .await
            .unwrap()
        );
    }
}

async fn assert_first_write_refused(peer: tokio::net::UnixStream, binding: &FileBrokerBinding) {
    let mut peer = BufReader::new(peer);
    let mut bytes = serde_json::to_vec(&request(binding)).unwrap();
    bytes.push(b'\n');
    let _ = peer.write_all(&bytes).await;
    let mut reply = String::new();
    let read = tokio::time::timeout(Duration::from_secs(2), peer.read_line(&mut reply))
        .await
        .unwrap();
    assert!(matches!(read, Ok(0) | Err(_)));
}

#[tokio::test]
async fn owned_session_failed_close_retains_receipt_for_independent_recovery() {
    let NativeLaunchSetup {
        root,
        store,
        binding,
        launch,
        endpoint,
        wrapper,
    } = native_launch_setup(false).await;
    let hash = format!("{:x}", Sha256::digest(fs::read(&wrapper).unwrap()));
    let acknowledged =
        crate::prepare_native_broker(&store, &binding, &launch, &wrapper, &hash, endpoint, 101)
            .await
            .unwrap();
    let mut expected = acknowledged.runner.identity().clone();
    let mut session = acknowledged
        .release(
            store.clone(),
            binding.clone(),
            continuous_millis().unwrap() + 4000,
            Duration::from_secs(5),
            broker_owner_audit("session-close-owner"),
        )
        .await
        .unwrap();
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        root.path().join("state.sqlite3").display(),
    ))
    .await
    .unwrap();
    db.execute("CREATE TRIGGER reject_session_close BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.closed' BEGIN SELECT RAISE(FAIL,'fixture'); END").await.unwrap();
    session.cancel().await.unwrap();
    assert!(
        session
            .finish(broker_owner_audit("session-close-fail"))
            .await
            .is_err()
    );
    expected.observation = conductor_runtime::BrokerShutdownObservation::Revoked {
        completed_sequence: 0,
    };
    conductor_runtime::BrokerReceiptDirectory::open(&launch.receipt_root)
        .unwrap()
        .read(&expected)
        .unwrap();
    assert_progress(&store, &binding, 0, 0).await;
    assert!(
        store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
    let closed: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE kind='file_broker.closed'")
            .fetch_one(&mut db)
            .await
            .unwrap();
    assert_eq!(closed, 0);
    db.execute("DROP TRIGGER reject_session_close")
        .await
        .unwrap();
    assert!(
        crate::close_broker_from_receipt(
            &store,
            &binding,
            &expected,
            &launch.receipt_root,
            broker_owner_audit("session-close-recovered"),
        )
        .await
        .unwrap()
    );
    assert!(
        !store
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap()
    );
}

async fn persist_vendor_packet(
    setup: &NativeLaunchSetup,
) -> (
    conductor_runtime::VendorPin,
    conductor_runtime::ResumePacket,
) {
    use conductor_runtime::{AdapterKind, VendorPin};
    let digest = conductor_runtime::binary_sha256(&setup.wrapper).unwrap();
    let pin = VendorPin {
        kind: AdapterKind::Codex,
        version: "fixture".into(),
        executable: setup.wrapper.clone(),
        executable_sha256: digest.clone(),
        conformance_sha256: "b".repeat(64),
    };
    let packet = persist_packet_for_vendor(setup, &pin).await;
    (pin, packet)
}

async fn persist_packet_for_vendor(
    setup: &NativeLaunchSetup,
    pin: &conductor_runtime::VendorPin,
) -> conductor_runtime::ResumePacket {
    use conductor_runtime::ResumePacket;
    let packet: ResumePacket = serde_json::from_value(serde_json::json!({
        "contract":{"version":1,"task_id":setup.binding.task_id,"attempt_id":setup.binding.attempt_id,
            "lease_epoch":setup.binding.lease_epoch,"worker_id":"runner",
            "adapter":{"kind":pin.kind,"version":pin.version,"executable_sha256":pin.executable_sha256,"conformance_sha256":pin.conformance_sha256},
            "workspace":setup.launch.root,"starting_head":"c".repeat(40),
            "authority":{"read_roots":[setup.launch.root],"write_roots":[setup.launch.root],"network":"inference_only","allow_workspace_commands":false},
            "instructions":[],"checks":[],"max_runtime_ms":10000},
        "objective":"Synthetic vendor handoff test","checkpoint":null,"untrusted_context":[]
    })).unwrap();
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        setup.root.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    sqlx::query("UPDATE executions SET packet_json=?,packet_sha256=? WHERE effect_token=?")
        .bind(serde_json::to_string(&packet).unwrap())
        .bind(packet.sha256().unwrap())
        .bind(&setup.binding.execution_token)
        .execute(&mut db)
        .await
        .unwrap();
    packet
}

#[tokio::test]
async fn owned_session_vendor_handoff_requires_exact_packet_and_single_issuance() {
    for cancel_before in [false, true] {
        let setup = native_launch_setup(false).await;
        let (pin, packet) = persist_vendor_packet(&setup).await;
        let acknowledged = crate::prepare_native_broker(
            &setup.store,
            &setup.binding,
            &setup.launch,
            &setup.wrapper,
            &pin.executable_sha256,
            setup.endpoint,
            101,
        )
        .await
        .unwrap();
        let mut session = acknowledged
            .release(
                setup.store.clone(),
                setup.binding.clone(),
                continuous_millis().unwrap() + 4000,
                Duration::from_secs(5),
                broker_owner_audit("vendor-owner"),
            )
            .await
            .unwrap();
        let mut changed = packet.clone();
        changed.objective.push_str(" substituted");
        assert!(
            session
                .vendor_command(prepared_test_vendor(&pin, &changed, &setup.wrapper))
                .await
                .is_err()
        );
        changed = packet.clone();
        changed.contract.task_id = "another-task".into();
        assert!(
            session
                .vendor_command(prepared_test_vendor(&pin, &changed, &setup.wrapper))
                .await
                .is_err()
        );
        if cancel_before {
            session.cancel().await.unwrap();
        }
        let result = session
            .vendor_command(prepared_test_vendor(&pin, &packet, &setup.wrapper))
            .await;
        if cancel_before {
            assert!(result.is_err());
        } else {
            let spec = result.unwrap();
            let secret = session.proxy_handoff();
            assert!(
                spec.environment
                    .get(conductor_runtime::BROKER_HANDOFF_ENV)
                    .is_some_and(|value| value == secret)
            );
            assert!(!spec.arguments.iter().any(|value| value.contains(secret)));
            assert!(!format!("{spec:?}").contains(secret));
            assert!(
                session
                    .vendor_command(prepared_test_vendor(&pin, &packet, &setup.wrapper))
                    .await
                    .is_err()
            );
            session.cancel().await.unwrap();
        }
        session
            .finish(broker_owner_audit("vendor-close"))
            .await
            .unwrap();
        assert_eq!(fs::read_dir(&setup.launch.root).unwrap().count(), 0);
    }
}

fn prepared_test_vendor(
    pin: &conductor_runtime::VendorPin,
    packet: &conductor_runtime::ResumePacket,
    proxy: &Path,
) -> conductor_runtime::PreparedBrokerVendor {
    conductor_runtime::PreparedBrokerVendor::new(
        pin,
        packet,
        std::collections::BTreeMap::new(),
        proxy,
        &pin.executable_sha256,
    )
    .unwrap()
}
