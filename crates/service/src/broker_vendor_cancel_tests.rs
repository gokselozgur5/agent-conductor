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
    let executable = setup.root.path().join("controlled-vendor");
    fs::write(
        &executable,
        "#!/bin/sh\nprintf '%s' \"$$\" > vendor-pid\nexec /bin/sleep 30\n",
    )
    .unwrap();
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
