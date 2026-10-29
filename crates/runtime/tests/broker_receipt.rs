#![cfg(unix)]
use conductor_runtime::{
    BrokerReceiptDirectory, BrokerScope, BrokerShutdownObservation, BrokerShutdownReceipt,
    ChildIdentity,
};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::PathBuf,
};
async fn fixture() -> (
    tempfile::TempDir,
    PathBuf,
    BrokerReceiptDirectory,
    BrokerShutdownReceipt,
) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap().join("receipts");
    fs::create_dir(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    let directory = BrokerReceiptDirectory::open(&path).unwrap();
    let receipt = BrokerShutdownReceipt {
        version: 1,
        scope: BrokerScope {
            authority_id: "a".repeat(64),
            boot_generation: 1,
            invocation_id: uuid::Uuid::now_v7().to_string(),
            task_id: "task".into(),
            attempt_id: "attempt".into(),
            lease_epoch: 1,
        },
        launch_sha256: "b".repeat(64),
        root_identity_sha256: "c".repeat(64),
        process: ChildIdentity::observe(std::process::id()).await.unwrap(),
        observation: BrokerShutdownObservation::Revoked {
            completed_sequence: 2,
        },
    };
    (root, path, directory, receipt)
}
#[tokio::test]
async fn receipt_requires_claim_and_preserves_immutable_publication() {
    let (_root, path, directory, receipt) = fixture().await;
    assert!(directory.publish(&receipt).is_err());
    assert!(directory.read(&receipt).is_err());
    directory
        .claim(&receipt.scope, &receipt.launch_sha256)
        .unwrap();
    assert!(
        directory
            .claim(&receipt.scope, &receipt.launch_sha256)
            .is_err()
    );
    directory.publish(&receipt).unwrap();
    assert!(directory.read(&receipt).unwrap() == receipt);
    let destination = path.join(format!("broker-{}.json", receipt.scope.invocation_id));
    let original = fs::read(&destination).unwrap();
    assert!(directory.publish(&receipt).is_err());
    assert_eq!(fs::read(destination).unwrap(), original);
    let reopened = BrokerReceiptDirectory::open(&path).unwrap();
    assert!(reopened.read(&receipt).unwrap() == receipt);
}
#[tokio::test]
async fn mutated_linked_nonprivate_oversized_and_rebound_receipts_are_refused() {
    for mode in [
        "permissions",
        "hardlink",
        "symlink",
        "extra",
        "sequence",
        "scope",
        "process",
        "oversize",
        "claim",
        "moved_root",
    ] {
        let (_root, path, directory, receipt) = fixture().await;
        directory
            .claim(&receipt.scope, &receipt.launch_sha256)
            .unwrap();
        directory.publish(&receipt).unwrap();
        let destination = path.join(format!("broker-{}.json", receipt.scope.invocation_id));
        match mode {
            "permissions" => {
                fs::set_permissions(&destination, fs::Permissions::from_mode(0o644)).unwrap();
            }
            "hardlink" => fs::hard_link(&destination, path.join("linked")).unwrap(),
            "symlink" => {
                fs::rename(&destination, path.join("old")).unwrap();
                symlink(path.join("old"), &destination).unwrap();
            }
            "oversize" => fs::write(&destination, vec![b'x'; 4097]).unwrap(),
            "claim" => fs::write(
                path.join(format!("broker-{}.claimed", receipt.scope.invocation_id)),
                "e".repeat(64),
            )
            .unwrap(),
            "moved_root" => {
                fs::rename(&path, path.with_file_name("old-root")).unwrap();
                fs::create_dir(&path).unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            }
            _ => {
                let mut value = serde_json::to_value(&receipt).unwrap();
                match mode {
                    "extra" => value["extra"] = serde_json::json!(true),
                    "sequence" => value["observation"]["completed_sequence"] = serde_json::json!(3),
                    "scope" => value["scope"]["lease_epoch"] = serde_json::json!(2),
                    _ => value["process"]["pid"] = serde_json::json!(1),
                }
                fs::write(&destination, serde_json::to_vec(&value).unwrap()).unwrap();
            }
        }
        assert!(directory.read(&receipt).is_err(), "{mode}");
    }
}
#[tokio::test]
async fn pending_only_and_nonprivate_root_never_become_receipts() {
    let (_root, path, directory, receipt) = fixture().await;
    directory
        .claim(&receipt.scope, &receipt.launch_sha256)
        .unwrap();
    fs::write(
        path.join(format!("broker-{}.pending", receipt.scope.invocation_id)),
        serde_json::to_vec(&receipt).unwrap(),
    )
    .unwrap();
    assert!(directory.read(&receipt).is_err());
    assert!(directory.publish(&receipt).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(BrokerReceiptDirectory::open(&path).is_err());
}
