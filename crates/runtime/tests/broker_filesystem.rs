#![cfg(unix)]
use conductor_runtime::{
    BrokerError, BrokerFileResult, BrokerFilesystem, BrokerOperation, BrokerRequest, BrokerScope,
    continuous_millis,
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
};

fn request(operation: BrokerOperation) -> BrokerRequest {
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
        operation,
    }
}
fn broker(root: &Path, request: &BrokerRequest) -> BrokerFilesystem {
    BrokerFilesystem::new(
        &root.canonicalize().unwrap(),
        request.scope.clone(),
        continuous_millis().unwrap() + 60_000,
    )
    .unwrap()
}
fn write(path: &str, contents: &str, expected: Option<String>) -> BrokerOperation {
    BrokerOperation::WriteText {
        path: path.into(),
        contents: contents.into(),
        expected_sha256: expected,
    }
}
fn digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

#[test]
fn real_create_read_and_digest_replacement_preserve_mode_and_revoke() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("src")).unwrap();
    let mut request = request(write("src/main.rs", "first", None));
    let broker = broker(root.path(), &request);
    let result = broker
        .execute(&request, |intent| {
            assert!(!root.path().join("src/main.rs").exists());
            assert_eq!(intent.sequence, 1);
            Ok(())
        })
        .unwrap();
    assert!(matches!(result, BrokerFileResult::Written { sha256 } if sha256 == digest("first")));
    fs::set_permissions(
        root.path().join("src/main.rs"),
        fs::Permissions::from_mode(0o750),
    )
    .unwrap();
    request.sequence = 2;
    request.operation = write("src/main.rs", "replacement", Some(digest("first")));
    broker.execute(&request, |_| Ok(())).unwrap();
    assert_eq!(
        fs::metadata(root.path().join("src/main.rs"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o750
    );
    request.sequence = 3;
    request.operation = BrokerOperation::ReadText {
        path: "src/main.rs".into(),
    };
    assert!(
        matches!(broker.execute(&request, |_| Ok(())).unwrap(), BrokerFileResult::Text { contents, sha256 } if contents == "replacement" && sha256 == digest("replacement"))
    );
    assert_eq!(fs::read_dir(root.path().join("src")).unwrap().count(), 1);
    assert_eq!(broker.revoke(), Ok(3));
    request.sequence = 4;
    assert!(matches!(
        broker.execute(&request, |_| panic!("revoked")),
        Err(BrokerError::Revoked)
    ));
}

#[test]
fn wrong_digest_and_create_only_never_replace_existing_bytes() {
    for expected in [None, Some(digest("wrong"))] {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file"), "original").unwrap();
        let request = request(write("file", "changed", expected));
        let broker = broker(root.path(), &request);
        assert!(matches!(
            broker.execute(&request, |_| Ok(())),
            Err(BrokerError::Uncertain)
        ));
        assert_eq!(
            fs::read_to_string(root.path().join("file")).unwrap(),
            "original"
        );
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
        assert_eq!(broker.revoke(), Err(BrokerError::Uncertain));
    }
}

#[test]
fn links_and_special_files_never_touch_outside_canary() {
    for kind in ["file-symlink", "directory-symlink", "hardlink", "fifo"] {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("canary"), "outside").unwrap();
        let path = match kind {
            "file-symlink" => {
                symlink(outside.path().join("canary"), root.path().join("target")).unwrap();
                "target"
            }
            "directory-symlink" => {
                symlink(outside.path(), root.path().join("target")).unwrap();
                "target/canary"
            }
            "hardlink" => {
                fs::hard_link(outside.path().join("canary"), root.path().join("target")).unwrap();
                "target"
            }
            "fifo" => {
                nix::unistd::mkfifo(&root.path().join("target"), nix::sys::stat::Mode::S_IRWXU)
                    .unwrap();
                "target"
            }
            _ => unreachable!(),
        };
        for operation in [
            BrokerOperation::ReadText { path: path.into() },
            write(path, "changed", Some(digest("outside"))),
        ] {
            let request = request(operation);
            let broker = broker(root.path(), &request);
            assert!(broker.execute(&request, |_| Ok(())).is_err(), "{kind}");
            assert_eq!(broker.revoke(), Err(BrokerError::Uncertain));
            assert_eq!(
                fs::read_to_string(outside.path().join("canary")).unwrap(),
                "outside"
            );
        }
    }
}

#[test]
fn replaced_root_and_parent_links_are_refused_after_admission() {
    let holder = tempfile::tempdir().unwrap();
    let root = holder.path().join("root");
    fs::create_dir(&root).unwrap();
    let request = request(write("file", "changed", None));
    let broker = broker(&root, &request);
    assert!(
        broker
            .execute(&request, |_| {
                fs::rename(&root, holder.path().join("retained")).unwrap();
                fs::create_dir(&root).unwrap();
                Ok(())
            })
            .is_err()
    );
    assert!(!root.join("file").exists());
    assert!(!holder.path().join("retained/file").exists());
    assert_eq!(broker.revoke(), Err(BrokerError::Uncertain));
}

#[test]
fn private_staging_git_aliases_oversized_and_non_utf8_are_refused() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join("large"), vec![b'a'; 256 * 1024 + 1]).unwrap();
    fs::write(root.path().join("binary"), [255]).unwrap();
    fs::write(
        root.path().join(".conductor-broker-retained"),
        "partial evidence",
    )
    .unwrap();
    fs::create_dir(root.path().join(".GIT")).unwrap();
    fs::write(root.path().join(".GIT/config"), "private").unwrap();
    for path in [
        "large",
        "binary",
        ".conductor-broker-retained",
        ".GIT/config",
    ] {
        let request = request(BrokerOperation::ReadText { path: path.into() });
        let broker = broker(root.path(), &request);
        assert!(broker.execute(&request, |_| Ok(())).is_err());
    }
    assert_eq!(
        fs::read_to_string(root.path().join(".conductor-broker-retained")).unwrap(),
        "partial evidence"
    );
}

#[test]
fn failed_admission_leaves_no_staging_or_file_effect() {
    let root = tempfile::tempdir().unwrap();
    let request = request(write("file", "changed", None));
    let broker = broker(root.path(), &request);
    assert!(matches!(
        broker.execute(&request, |_| Err(BrokerError::Admission)),
        Err(BrokerError::Admission)
    ));
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    assert_eq!(broker.revoke(), Err(BrokerError::Uncertain));
}

#[test]
fn retained_staging_is_never_overwritten_by_a_repeated_invocation() {
    let root = tempfile::tempdir().unwrap();
    let request = request(write("file", "new", None));
    let retained = root.path().join(format!(
        ".conductor-broker-{}-1",
        request.scope.invocation_id
    ));
    fs::write(&retained, "interrupted evidence").unwrap();
    let broker = broker(root.path(), &request);
    assert!(broker.execute(&request, |_| Ok(())).is_err());
    assert_eq!(
        fs::read_to_string(&retained).unwrap(),
        "interrupted evidence"
    );
    assert!(!root.path().join("file").exists());
    assert_eq!(broker.revoke(), Err(BrokerError::Uncertain));
}
