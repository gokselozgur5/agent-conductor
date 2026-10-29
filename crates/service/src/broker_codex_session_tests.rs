use super::*;

#[tokio::test]
#[ignore = "requires pinned Codex0.163.0; deterministic local provider, no inference"]
async fn pinned_codex_uses_owned_session_and_durable_sqlite_journal() {
    let setup = native_launch_setup(false).await;
    let executable = PathBuf::from(std::env::var_os("CONDUCTOR_CODEX_BIN").unwrap());
    let pin = conductor_runtime::VendorPin {
        kind: conductor_runtime::AdapterKind::Codex,
        version: "0.163.0".into(),
        executable_sha256: conductor_runtime::binary_sha256(&executable).unwrap(),
        executable,
        conformance_sha256: "b".repeat(64),
    };
    let packet = persist_packet_for_vendor(&setup, &pin).await;
    let proxy = setup.wrapper.with_file_name("conductor-file-proxy");
    let prepared_vendor = conductor_runtime::PreparedBrokerVendor::new(
        &pin,
        &packet,
        std::collections::BTreeMap::new(),
        &proxy,
        &conductor_runtime::binary_sha256(&proxy).unwrap(),
    )
    .unwrap();
    let wrapper_hash = conductor_runtime::binary_sha256(&setup.wrapper).unwrap();
    let acknowledged = crate::prepare_native_broker(
        &setup.store,
        &setup.binding,
        &setup.launch,
        &setup.wrapper,
        &wrapper_hash,
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
            Duration::from_secs(30),
            broker_owner_audit("codex-owner"),
        )
        .await
        .unwrap();
    let spec = session.vendor_command(prepared_vendor).await.unwrap();
    let evidence = std::env::var_os("CONDUCTOR_CODEX_EVIDENCE")
        .map_or_else(|| setup.root.path().join("codex-evidence"), PathBuf::from);
    run_owned_codex_fixture(&mut session, &spec, &proxy, &evidence).await;
    assert_progress(&setup.store, &setup.binding, 2, 2).await;
    assert_eq!(
        fs::read(setup.launch.root.join("codex-answer")).unwrap(),
        b"native codex proxy\n"
    );
    // Proxy EOF may already have stopped the runner; only finish proves closure.
    let _ = session.cancel().await;
    session
        .finish(broker_owner_audit("codex-close"))
        .await
        .unwrap();
    let reopened = Store::open(setup.root.path().join("state.sqlite3"))
        .await
        .unwrap();
    let status = reopened.file_broker_status(&setup.binding).await.unwrap();
    assert!(status.closed);
    assert_eq!((status.admitted, status.completed), (2, 2));
    assert!(
        !reopened
            .execution_has_unresolved_file_broker(&setup.binding.execution_token)
            .await
            .unwrap()
    );
}

async fn run_owned_codex_fixture(
    session: &mut crate::NativeBrokerSession,
    spec: &conductor_runtime::ProcessSpec,
    proxy: &Path,
    evidence: &Path,
) {
    let mut command = tokio::process::Command::new("/usr/bin/python3");
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../runtime/tests/fixtures/codex_proxy_inventory.py"
        ))
        .arg(&spec.executable)
        .arg(proxy)
        .arg(evidence)
        .env_clear()
        .envs(&spec.environment)
        .env("TMPDIR", std::env::var_os("TMPDIR").unwrap())
        .env("CONDUCTOR_PROXY_CALLS", "approved")
        .env(
            "CONDUCTOR_CODEX_BROKER_ARGUMENTS",
            serde_json::to_string(&spec.arguments).unwrap(),
        )
        .kill_on_drop(true);
    let output = command.output();
    tokio::pin!(output);
    let mut renewal = tokio::time::interval(Duration::from_millis(500));
    let mut renewing = true;
    let result = tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            tokio::select! {
                result = &mut output => break result.unwrap(),
                _ = renewal.tick(), if renewing => {
                    // A completed proxy may close the broker before Codex exits.
                    // Stop renewal on failure; session.finish must still prove settlement.
                    renewing = session.renew(continuous_millis().unwrap()+4000).await.is_ok();
                },
            }
        }
    })
    .await
    .unwrap();
    assert!(result.status.success(), "controlled Codex fixture failed");
}
