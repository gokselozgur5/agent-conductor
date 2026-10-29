#![cfg(unix)]
use conductor_runtime::{
    BrokerFilesystem, BrokerOperation, BrokerRequest, BrokerScope, broker_root_identity,
    continuous_millis,
};
use std::time::Duration;
fn request() -> BrokerRequest {
    BrokerRequest {
        version: 1,
        sequence: 1,
        scope: BrokerScope {
            authority_id: "a".repeat(64),
            boot_generation: 1,
            invocation_id: uuid::Uuid::now_v7().to_string(),
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_epoch: 1,
        },
        operation: BrokerOperation::WriteText {
            path: "answer".into(),
            expected_sha256: None,
            contents: "value".into(),
        },
    }
}
#[test]
fn lease_expiry_after_admission_prevents_file_effect_and_cannot_revive() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap();
    let req = request();
    let broker = BrokerFilesystem::new(
        &path,
        req.scope.clone(),
        continuous_millis().unwrap() + 60000,
    )
    .unwrap();
    let control = broker.lease_control();
    control.renew(continuous_millis().unwrap() + 100).unwrap();
    let result = broker.execute(&req, |_| {
        std::thread::sleep(Duration::from_millis(150));
        Ok(())
    });
    assert!(result.is_err());
    assert!(!path.join("answer").exists());
    assert!(broker.revoke().is_err());
    assert!(control.renew(continuous_millis().unwrap() + 10000).is_err());
}
#[test]
fn timely_renewal_extends_only_live_lease_and_revocation_is_permanent() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap();
    let req = request();
    let broker = BrokerFilesystem::new(
        &path,
        req.scope.clone(),
        continuous_millis().unwrap() + 60000,
    )
    .unwrap();
    let control = broker.lease_control();
    control.renew(continuous_millis().unwrap() + 1000).unwrap();
    control.renew(continuous_millis().unwrap() + 10000).unwrap();
    broker.execute(&req, |_| Ok(())).unwrap();
    control.revoke();
    assert!(control.renew(continuous_millis().unwrap() + 20000).is_err());
    assert_eq!(broker.revoke(), Ok(1));
}
#[test]
fn hard_deadline_cannot_be_extended() {
    let root = tempfile::tempdir().unwrap();
    let req = request();
    let maximum = continuous_millis().unwrap() + 60000;
    let broker = BrokerFilesystem::new(
        &root.path().canonicalize().unwrap(),
        req.scope.clone(),
        maximum,
    )
    .unwrap();
    assert!(broker.lease_control().renew(maximum + 1).is_err());
    assert!(broker.execute(&req, |_| Ok(())).is_err());
    assert!(!root.path().join("answer").exists());
}
#[test]
fn root_digest_binds_open_descriptor_and_path_replacement_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap().join("files");
    std::fs::create_dir(&path).unwrap();
    let req = request();
    let expected = broker_root_identity(&path).unwrap();
    let broker =
        BrokerFilesystem::new(&path, req.scope, continuous_millis().unwrap() + 60000).unwrap();
    assert_eq!(broker.root_identity_sha256().unwrap(), expected);
    std::fs::rename(&path, path.with_file_name("old")).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert_ne!(broker_root_identity(&path).unwrap(), expected);
    assert!(broker.root_identity_sha256().is_err());
}

#[test]
fn late_renewal_without_an_intervening_operation_cannot_revive_lease() {
    let root = tempfile::tempdir().unwrap();
    let req = request();
    let broker = BrokerFilesystem::new(
        &root.path().canonicalize().unwrap(),
        req.scope.clone(),
        continuous_millis().unwrap() + 60000,
    )
    .unwrap();
    let control = broker.lease_control();
    control.renew(continuous_millis().unwrap() + 100).unwrap();
    std::thread::sleep(Duration::from_millis(150));
    assert!(control.renew(continuous_millis().unwrap() + 10000).is_err());
    assert!(broker.execute(&req, |_| Ok(())).is_err());
    assert!(!root.path().join("answer").exists());
}
