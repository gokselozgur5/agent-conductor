use super::*;

const SUPERVISION_SCENARIOS: [&str; 12] = [
    "success",
    "nonzero",
    "revoked",
    "sender-lost",
    "expires",
    "live-revoked",
    "live-lost",
    "live-invalid-deadline",
    "close-fails",
    "proxy-eof",
    "proxy-eof-revoked",
    "proxy-eof-close-fails",
];

#[tokio::test]
async fn owned_vendor_supervision_separates_process_exit_and_broker_closure() {
    for mode in SUPERVISION_SCENARIOS {
        let setup = native_launch_setup(false).await;
        let hash = conductor_runtime::binary_sha256(&setup.wrapper).unwrap();
        let acknowledged = crate::prepare_native_broker(
            &setup.store,
            &setup.binding,
            &setup.launch,
            &setup.wrapper,
            &hash,
            setup.endpoint,
            101,
        )
        .await
        .unwrap();
        let deadline = continuous_millis().unwrap() + 4000;
        let session = acknowledged
            .release(
                setup.store.clone(),
                setup.binding.clone(),
                deadline,
                Duration::from_secs(5),
                broker_owner_audit("supervised-owner"),
            )
            .await
            .unwrap();
        let initial = if mode == "revoked" {
            None
        } else {
            Some(deadline)
        };
        let (authority, lease) = tokio::sync::watch::channel(initial);
        let mut authority = if mode == "sender-lost" {
            drop(authority);
            None
        } else {
            Some(authority)
        };
        let spec = vendor_process_fixture(
            mode,
            &setup.launch.root,
            &setup.wrapper,
            session.proxy_handoff(),
        );
        inject_vendor_close_failure(mode, setup.root.path()).await;
        let operation = tokio::spawn(session.supervise_vendor_spec(
            spec,
            lease,
            broker_owner_audit("supervised-close"),
        ));
        if mode.starts_with("live-") {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !setup.launch.root.join("vendor-started").exists() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            if mode == "live-revoked" {
                authority.as_ref().unwrap().send(None).unwrap();
            } else if mode == "live-invalid-deadline" {
                authority
                    .as_ref()
                    .unwrap()
                    .send(Some(setup.launch.maximum_deadline_ms + 1))
                    .unwrap();
            } else {
                drop(authority.take());
            }
        }
        if mode == "proxy-eof-revoked" {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !setup
                    .store
                    .file_broker_status(&setup.binding)
                    .await
                    .unwrap()
                    .closed
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            authority.as_ref().unwrap().send(None).unwrap();
        }
        let outcome = tokio::time::timeout(Duration::from_secs(15), operation)
            .await
            .unwrap()
            .unwrap();
        assert_vendor_outcome(mode, setup.root.path(), &setup.binding, outcome).await;
        drop(authority);
    }
}

async fn assert_vendor_outcome(
    mode: &str,
    root: &Path,
    binding: &FileBrokerBinding,
    outcome: crate::BrokerVendorOutcome,
) {
    assert_eq!(
        outcome.broker.is_ok(),
        !mode.ends_with("close-fails"),
        "{mode}"
    );
    match mode {
        "success" | "close-fails" | "proxy-eof" => {
            let output = outcome.process.unwrap();
            assert_eq!(output.exit_code, Some(0));
            assert_eq!(output.stdout, b"finished");
        }
        "nonzero" => assert_eq!(outcome.process.unwrap().exit_code, Some(7)),
        _ => assert!(matches!(
            outcome.process,
            Err(conductor_runtime::ProcessError::Cancelled)
        )),
    }
    assert_eq!(
        root.join("files/vendor-started").exists(),
        !matches!(mode, "revoked" | "sender-lost")
    );
    let reopened = Store::open(root.join("state.sqlite3")).await.unwrap();
    let status = reopened.file_broker_status(binding).await.unwrap();
    assert_eq!(status.closed, !mode.ends_with("close-fails"));
    assert_eq!(
        reopened
            .execution_has_unresolved_file_broker(&binding.execution_token)
            .await
            .unwrap(),
        mode.ends_with("close-fails")
    );
    assert_eq!((status.admitted, status.completed), (0, 0));
}

fn vendor_process_fixture(
    mode: &str,
    root: &Path,
    wrapper: &Path,
    handoff: &str,
) -> conductor_runtime::ProcessSpec {
    let code = match mode {
        "success" | "close-fails" => "printf started > vendor-started; printf finished",
        "nonzero" => "printf started > vendor-started; exit 7",
        _ => "printf started > vendor-started; exec /bin/sleep 30",
    };
    let mut spec = conductor_runtime::ProcessSpec {
        executable: PathBuf::from("/bin/sh"),
        arguments: vec!["-c".into(), code.into()],
        directory: root.to_path_buf(),
        environment: std::collections::BTreeMap::new(),
        timeout: Duration::from_secs(10),
        output_limit: 1024,
    };
    if mode.starts_with("proxy-eof") {
        spec.executable = PathBuf::from("/usr/bin/python3");
        spec.arguments = vec!["-c".into(),
                "import os, pathlib, subprocess, sys, time; pathlib.Path('vendor-started').write_text('started'); subprocess.run([sys.argv[1]], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, check=True); time.sleep(2); print('finished', end='')".into(),
                wrapper.with_file_name("conductor-file-proxy").to_str().unwrap().into()];
        spec.environment
            .insert(conductor_runtime::BROKER_HANDOFF_ENV.into(), handoff.into());
    }
    spec
}

async fn inject_vendor_close_failure(mode: &str, root: &Path) {
    if mode.ends_with("close-fails") {
        let mut db = sqlx::SqliteConnection::connect(&format!(
            "sqlite:{}",
            root.join("state.sqlite3").display(),
        ))
        .await
        .unwrap();
        db.execute("CREATE TRIGGER reject_vendor_close BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.closed' BEGIN SELECT RAISE(FAIL,'fixture'); END").await.unwrap();
    }
}
