use super::*;
use std::collections::BTreeMap;
use tokio::io::AsyncBufReadExt;

#[tokio::test]
#[ignore = "requires pinned Codex0.163.0; service owns actual vendor process"]
async fn pinned_codex_runs_through_owned_vendor_supervision() {
    let mut setup = native_launch_setup(false).await;
    let evidence = PathBuf::from(std::env::var_os("CONDUCTOR_CODEX_EVIDENCE").unwrap());
    fs::create_dir(&evidence).unwrap();
    let home = evidence.join("home");
    let mut provider = start_owned_provider(&home, &evidence).await;
    let prepared = prepare_owned_codex(&setup, &home).await;
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
    let deadline = continuous_millis().unwrap() + 45000;
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
    let (_authority, lease) = tokio::sync::watch::channel(Some(deadline));
    let outcome = tokio::time::timeout(
        Duration::from_secs(65),
        Box::pin(session.run_vendor(prepared, lease, broker_owner_audit("owned-codex-close"))),
    )
    .await
    .unwrap()
    .unwrap();
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
    assert!(outcome.broker.is_ok());
    assert_owned_process_observations(&home);
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

async fn start_owned_provider(home: &Path, evidence: &Path) -> tokio::process::Child {
    let mut child = tokio::process::Command::new("/usr/bin/python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../runtime/tests/fixtures/codex_owned_provider.py"
        ))
        .arg(home)
        .arg(std::env::var_os("CONDUCTOR_CODEX_BIN").unwrap())
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
