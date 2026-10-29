async fn account_result_fixture() -> AccountServiceFixture {
    let mut f = account_service_fixture().await;
    fs::write(&f.pin.executable, "#!/usr/bin/python3\nimport json,pathlib,sqlite3\ndb=sqlite3.connect('file:../state.sqlite3?mode=ro',uri=True)\nassert db.execute('select state from executions').fetchone()[0]=='started'\npathlib.Path('result').write_text('account candidate')\nprint(json.dumps({'type':'result','subtype':'success','is_error':False,'result':'candidate'}))\n").unwrap();
    f.pin.kind = conductor_runtime::AdapterKind::Claude;
    f.pin.executable_sha256 = conductor_runtime::binary_sha256(&f.pin.executable).unwrap();
    f.packet.contract.adapter.kind = f.pin.kind;
    f.packet
        .contract
        .adapter
        .executable_sha256
        .clone_from(&f.pin.executable_sha256);
    f
}

async fn account_result_worker(path: &Path) -> tokio::process::Child {
    let wrapper =
        PathBuf::from(std::env::var("CONDUCTOR_TEST_BIN_DIR").unwrap()).join("conductor-runner");
    let mut child = tokio::process::Command::new(wrapper)
        .args([
            "--account-worker",
            path.to_str().unwrap(),
            &nix::unistd::geteuid().as_raw().to_string(),
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

async fn account_result_release(
    f: &AccountServiceFixture,
    address: &conductor_runtime::AccountChannelAddress,
) -> conductor_runtime::AccountRunnerConnection {
    let intent = f
        .service
        .prepare_account_execution(&f.packet, &f.token, "runner")
        .await
        .unwrap();
    f.service
        .release_account_execution(intent, address, &f.pin)
        .await
        .unwrap()
        .1
}

fn account_result_address(f: &AccountServiceFixture) -> conductor_runtime::AccountChannelAddress {
    let uid = nix::unistd::geteuid().as_raw();
    conductor_runtime::AccountChannelAddress {
        path: f.directory.path().join("account.sock"),
        worker_uid: uid,
        coordinator_uid: uid,
        packet_sha256: f.packet.sha256().unwrap(),
    }
}

#[tokio::test]
async fn account_result_binds_imported_source_without_local_exit_or_fence() {
    use agent_conductor_store::{EffectAudit, ExecutionState, ExecutionUpdate};
    let f = account_result_fixture().await;
    let address = account_result_address(&f);
    let mut worker = account_result_worker(&address.path).await;
    let connection = account_result_release(&f, &address).await;
    let outcome = f
        .service
        .await_account_execution(&f.token, connection)
        .await
        .unwrap();
    let reported = f.store.execution(&f.token).await.unwrap().unwrap();
    assert_eq!(reported.state, ExecutionState::Started);
    assert!(reported.process.is_none());
    assert_account_exit_refusal(&f, &reported).await;
    assert!(
        reported
            .account_result
            .as_ref()
            .unwrap()
            .snapshot_sha256
            .is_none()
    );
    assert!(
        f.service
            .validate_capture_eligibility(&reported, &f.packet)
            .await
            .is_err()
    );
    assert!(
        f.store
            .update_execution(
                &f.token,
                reported.revision,
                "wrong-output",
                &ExecutionUpdate::CaptureAccount {
                    wrapper_identity: reported.wrapper_identity.clone().unwrap(),
                    output_sha256: "0".repeat(64),
                    snapshot_sha256: "a".repeat(64),
                },
                EffectAudit {
                    id: "wrong-output",
                    actor_id: "runtime",
                    correlation_id: &f.token,
                    now_ms: 100
                }
            )
            .await
            .is_err()
    );
    let imported = f.directory.path().join("source");
    let manifest = f
        .service
        .finalize_account_source(
            outcome,
            &address,
            &f.directory.path().join("peer-source"),
            &imported,
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        fs::read_to_string(imported.join("tree/result")).unwrap(),
        "account candidate"
    );
    let record = f.store.execution(&f.token).await.unwrap().unwrap();
    assert_eq!(record.state, ExecutionState::Started);
    assert!(record.process.is_none());
    assert!(f.store.has_unfenced_execution("task").await.unwrap());
    assert_eq!(
        record
            .account_result
            .as_ref()
            .unwrap()
            .snapshot_sha256
            .as_deref(),
        Some(manifest.sha256().unwrap().as_str())
    );
    f.service
        .validate_capture_eligibility(&record, &f.packet)
        .await
        .unwrap();
    let task = f.service.workflow_task("task").await.unwrap();
    assert_eq!(task.state, TaskState::Verifying);
    assert_eq!(
        task.verification.unwrap().source.unwrap().0,
        format!("snapshot:{}", manifest.sha256().unwrap())
    );
    assert_account_import_binding(&f, &record).await;
    assert_capture_projection_revokes_blocked(&f.service, &f.token).await;
    worker.kill().await.unwrap();
    worker.wait().await.unwrap();
}

#[tokio::test]
async fn account_result_expired_before_capture_cannot_publish_source() {
    let f = account_result_fixture().await;
    let address = account_result_address(&f);
    let mut worker = account_result_worker(&address.path).await;
    let connection = account_result_release(&f, &address).await;
    let outcome = f
        .service
        .await_account_execution(&f.token, connection)
        .await
        .unwrap();
    f.service.clock.set(5000);
    let remote = f.directory.path().join("peer-source");
    let local = f.directory.path().join("source");
    assert!(
        f.service
            .finalize_account_source(outcome, &address, &remote, &local, &[])
            .await
            .is_err()
    );
    assert!(!remote.exists());
    assert!(!local.exists());
    let record = f.store.execution(&f.token).await.unwrap().unwrap();
    assert!(record.account_result.unwrap().snapshot_sha256.is_none());
    worker.kill().await.unwrap();
    worker.wait().await.unwrap();
}

#[tokio::test]
async fn account_result_malformed_terminal_preserves_unresolved_execution() {
    let f = account_service_fixture().await;
    fs::write(f.packet.contract.workspace.join("finish"), b"done").unwrap();
    let address = account_result_address(&f);
    let mut worker = account_result_worker(&address.path).await;
    let connection = account_result_release(&f, &address).await;
    assert!(
        f.service
            .await_account_execution(&f.token, connection)
            .await
            .is_err()
    );
    let record = f.store.execution(&f.token).await.unwrap().unwrap();
    assert_eq!(record.state, agent_conductor_store::ExecutionState::Blocked);
    assert!(record.account_result.is_none());
    assert!(record.process.is_none());
    assert!(f.store.has_unfenced_execution("task").await.unwrap());
    worker.kill().await.unwrap();
    worker.wait().await.unwrap();
}

async fn assert_account_import_binding(
    f: &AccountServiceFixture,
    record: &agent_conductor_store::ExecutionRecord,
) {
    let mut connection = SqliteConnection::connect(&format!(
        "sqlite://{}",
        f.directory.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    let original: String = sqlx::query_scalar("SELECT aggregate_json FROM tasks WHERE id='task'")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    sqlx::query("UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.verification.source',?) WHERE id='task'")
        .bind(format!("snapshot:{}", "0".repeat(64))).execute(&mut connection).await.unwrap();
    let eligible: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM capture_eligible_executions WHERE effect_token=?")
            .bind(&f.token)
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(eligible, 0);
    assert!(
        f.service
            .validate_capture_eligibility(record, &f.packet)
            .await
            .is_err()
    );
    sqlx::query("UPDATE tasks SET aggregate_json=? WHERE id='task'")
        .bind(original)
        .execute(&mut connection)
        .await
        .unwrap();
}
