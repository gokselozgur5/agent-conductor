use super::*;
use std::collections::BTreeMap;
use tokio::io::AsyncBufReadExt;

#[tokio::test]
#[ignore = "requires pinned Codex0.163.0; service owns actual vendor process"]
async fn pinned_codex_runs_through_service_vendor_supervision() {
    run_owned_codex(0).await;
}

#[tokio::test]
#[ignore = "requires pinned Codex0.163.0; real model idle exceeds 30 seconds"]
async fn pinned_codex_survives_service_idle_beyond_frame_budget() {
    run_owned_codex(35).await;
}

async fn run_owned_codex(delay: u64) {
    let (mut setup, service) = native_launch_setup_with_service(false).await;
    grant_owned_codex_lease(&setup, &service).await;
    let evidence = PathBuf::from(std::env::var_os("CONDUCTOR_CODEX_EVIDENCE").unwrap());
    fs::create_dir(&evidence).unwrap();
    let home = evidence.join("home");
    let mut provider = start_owned_provider(&home, &evidence, delay).await;
    let prepared = prepare_owned_codex(&setup, &home).await;
    let packet = prepared.packet().clone();
    // Establish the launch ceiling after expensive executable validation.
    setup.launch.maximum_deadline_ms = continuous_millis().unwrap() + 60000;
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
            Duration::from_secs(30),
            broker_owner_audit("owned-codex-owner"),
        )
        .await
        .unwrap();
    let handoff: serde_json::Value = serde_json::from_str(session.proxy_handoff()).unwrap();
    let token = handoff["token"].as_str().unwrap().as_bytes();
    let outcome = drive_owned_codex(&service, session, prepared).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    provider.start_kill().unwrap();
    provider.wait().await.unwrap();
    fs::write(evidence.join("outcome.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "process": outcome.process.as_ref().map(|p| p.exit_code).map_err(ToString::to_string),
        "broker": outcome.broker.as_ref().map(|_| "verified_closed").map_err(ToString::to_string),
    })).unwrap()).unwrap();
    let output = outcome.process.expect("actual vendor process failed");
    fs::write(evidence.join("stdout.log"), &output.stdout).unwrap();
    fs::write(evidence.join("stderr.log"), &output.stderr).unwrap();
    assert_eq!(output.exit_code, Some(0));
    let candidate = conductor_runtime::interpret_vendor_output(&packet, &output)
        .expect("successful broker vendor must yield a bounded candidate");
    assert_eq!(candidate.text, "Fixture done.");
    assert!(
        !candidate.file_only_tools,
        "broker output is not a Claude fence"
    );
    assert!(outcome.broker.is_ok());
    assert_owned_process_observations(&home);
    if delay != 0 {
        let proof: serde_json::Value =
            serde_json::from_slice(&fs::read(home.join("idle-delay.json")).unwrap()).unwrap();
        assert!(proof["elapsed_seconds"].as_f64().unwrap() >= 35.0);
    }
    for bytes in [&output.stdout, &output.stderr] {
        assert!(!bytes.windows(token.len()).any(|w| w == token));
    }
    for step in 0..3 {
        let raw = fs::read(home.join(format!("request-{step}.json"))).unwrap();
        assert!(!raw.windows(token.len()).any(|w| w == token));
    }
    assert_eq!(
        fs::read(setup.launch.root.join("owned-answer")).unwrap(),
        b"native owned codex\n"
    );
    let reopened = Store::open(setup.root.path().join("state.sqlite3"))
        .await
        .unwrap();
    let status = reopened.file_broker_status(&setup.binding).await.unwrap();
    assert!(status.closed);
    assert_eq!((status.admitted, status.completed), (2, 2));
    fs::write(
        evidence.join("receipt.json"),
        b"{\"actual_owned_codex\":true,\"operations\":2,\"closed_after_reopen\":true}\n",
    )
    .unwrap();
}

async fn start_owned_provider(home: &Path, evidence: &Path, delay: u64) -> tokio::process::Child {
    let mut child = tokio::process::Command::new("/usr/bin/python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../runtime/tests/fixtures/codex_owned_provider.py"
        ))
        .arg(home)
        .arg(std::env::var_os("CONDUCTOR_CODEX_BIN").unwrap())
        .arg(delay.to_string())
        .env_clear()
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(fs::File::create(evidence.join("provider-stderr.log")).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdout = tokio::io::BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(5), stdout.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(line, "ready\n");
    child
}

async fn prepare_owned_codex(
    setup: &NativeLaunchSetup,
    home: &Path,
) -> conductor_runtime::PreparedBrokerVendor {
    let executable = PathBuf::from(std::env::var_os("CONDUCTOR_CODEX_BIN").unwrap());
    let digest = conductor_runtime::binary_sha256(&executable).unwrap();
    assert_eq!(
        digest,
        "8813367285524ac798dd96d1b071aea30d58708c7226fd236cd275cf1cfdee31"
    );
    // The test-only exec launcher changes provider options, never broker policy.
    let executable = home.join("launch-codex");
    let digest = conductor_runtime::binary_sha256(&executable).unwrap();
    let pin = conductor_runtime::VendorPin {
        kind: conductor_runtime::AdapterKind::Codex,
        version: "0.163.0".into(),
        executable,
        executable_sha256: digest,
        conformance_sha256: "b".repeat(64),
    };
    let mut packet = persist_packet_for_vendor(setup, &pin).await;
    packet.contract.max_runtime_ms = 60000;
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
    let proxy = setup.wrapper.with_file_name("conductor-file-proxy");
    let environment = BTreeMap::from([
        ("HOME".into(), home.to_str().unwrap().into()),
        ("CODEX_HOME".into(), home.to_str().unwrap().into()),
        ("LANG".into(), "synthetic-local-only".into()),
        ("TMPDIR".into(), std::env::var("TMPDIR").unwrap()),
    ]);
    conductor_runtime::PreparedBrokerVendor::new(
        &pin,
        &packet,
        environment,
        &proxy,
        &conductor_runtime::binary_sha256(&proxy).unwrap(),
    )
    .unwrap()
}

fn assert_owned_process_observations(home: &Path) {
    let records: Vec<serde_json::Value> = fs::read_to_string(home.join("process-group.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(!records.is_empty());
    assert_eq!(records.last().unwrap()["members"], serde_json::json!([]));
    assert!(
        records
            .iter()
            .any(|record| !record["members"].as_array().unwrap().is_empty())
    );
    for record in records {
        for member in record["members"].as_array().unwrap() {
            let command = Path::new(member[4].as_str().unwrap());
            assert!(
                !command
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("git")
            );
        }
    }
}

async fn grant_owned_codex_lease(
    setup: &NativeLaunchSetup,
    service: &PersistentControlService<FixedClock>,
) {
    let row = setup.store.task("task").await.unwrap().unwrap();
    let task: Task = serde_json::from_str(&row.aggregate_json).unwrap();
    service
        .apply_row(
            row,
            lifecycle_command(
                &task,
                Actor {
                    id: ActorId("worker".into()),
                    role: ActorRole::Worker,
                },
                CommandKind::Heartbeat {
                    attempt_id: AttemptId("attempt".into()),
                    lease_epoch: 1,
                    lease_duration_ms: 55000,
                    worker_margin_ms: 0,
                },
                "codex-service-heartbeat",
            ),
        )
        .await
        .unwrap();
}

async fn drive_owned_codex(
    service: &PersistentControlService<FixedClock>,
    session: crate::NativeBrokerSession,
    prepared: conductor_runtime::PreparedBrokerVendor,
) -> crate::BrokerVendorOutcome {
    // Advance the injected service clock from real elapsed host time. A frozen
    // test clock would manufacture lease extensions on every authority refresh.
    let started = continuous_millis().unwrap();
    let operation = tokio::time::timeout(
        Duration::from_secs(65),
        Box::pin(service.run_broker_vendor(
            session,
            prepared,
            broker_owner_audit("owned-codex-close"),
        )),
    );
    tokio::pin!(operation);
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            result = &mut operation => return result.unwrap().unwrap(),
            _ = tick.tick() => service.clock.set(continuous_millis().unwrap() - started),
        }
    }
}
