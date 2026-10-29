use super::*;
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
async fn native_launch_setup() -> NativeLaunchSetup {
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
    } = native_launch_setup().await;
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
