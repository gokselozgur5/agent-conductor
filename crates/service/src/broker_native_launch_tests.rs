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
    let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
        .await
        .unwrap();
    let (_hold, rx) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(journal.serve_remote(stream, rx, Duration::from_secs(5)));
    let (store_ref, binding_ref, receipt_root) = (store, binding, &launch.receipt_root);
    let mut session = prepared
        .release(|identity| async move {
            crate::record_broker_owner(
                store_ref,
                binding_ref,
                &identity,
                receipt_root,
                broker_owner_audit("native-owner"),
            )
            .await?;
            Ok(continuous_millis().unwrap() + 4000)
        })
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
    let receipt = session.finish().await.unwrap();
    assert!(server.await.unwrap().is_ok());
    assert_late_write_refused(peer, binding, &launch.root).await;
    crate::close_broker_from_receipt(
        store,
        binding,
        &receipt,
        &launch.receipt_root,
        broker_owner_audit("native-close"),
    )
    .await
    .unwrap();
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
