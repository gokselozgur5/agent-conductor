#![cfg(unix)]
use conductor_runtime::{SnapshotManifest, export_snapshot, import_snapshot, snapshot_paths};
use std::{fs, os::unix::fs::PermissionsExt};

#[test]
fn deleted_file_cannot_also_be_a_parent_directory_in_received_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let manifest = SnapshotManifest {
        version: 1,
        entries: vec![
            conductor_runtime::SnapshotEntry {
                path: "parent".into(),
                sha256: None,
                bytes: 0,
                executable: false,
            },
            conductor_runtime::SnapshotEntry {
                path: "parent/empty".into(),
                sha256: Some(
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
                ),
                bytes: 0,
                executable: false,
            },
        ],
    };
    let header = serde_json::to_vec(&manifest).unwrap();
    let mut bytes = b"ACSNAP01".to_vec();
    bytes.extend_from_slice(&(header.len() as u64).to_be_bytes());
    bytes.extend_from_slice(&header);
    let destination = root.join("received");
    assert!(
        import_snapshot(
            &mut bytes.as_slice(),
            &destination,
            &manifest.sha256().unwrap()
        )
        .is_err()
    );
    assert!(!destination.exists());
}

#[test]
fn transfer_preserves_executables_deletions_and_source_identity() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let source = root.join("source");
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::write(source.join("nested/run"), b"#!/bin/sh\nprintf 42\\n\n").unwrap();
    fs::set_permissions(source.join("nested/run"), fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(source.join("empty"), b"").unwrap();
    let manifest = snapshot_paths(
        &source,
        &root.join("snapshot"),
        &["nested/run".into(), "empty".into(), "deleted".into()],
    )
    .unwrap();
    let expected = manifest.sha256().unwrap();
    let mut stream = Vec::new();
    export_snapshot(
        &root.join("snapshot/tree"),
        &manifest,
        &expected,
        &mut stream,
    )
    .unwrap();
    let target = root.join("received");
    assert_eq!(
        import_snapshot(&mut stream.as_slice(), &target, &expected).unwrap(),
        manifest
    );
    let restored: SnapshotManifest =
        serde_json::from_slice(&fs::read(target.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(restored.sha256().unwrap(), expected);
    assert_eq!(
        fs::read(target.join("tree/nested/run")).unwrap(),
        fs::read(source.join("nested/run")).unwrap()
    );
    assert_eq!(
        fs::metadata(target.join("tree/nested/run"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert!(!target.join("tree/deleted").exists());
    assert_eq!(fs::metadata(target.join("tree/empty")).unwrap().len(), 0);
    assert!(import_snapshot(&mut stream.as_slice(), &target, &expected).is_err());
    assert_eq!(restored, manifest);
}

#[test]
fn partial_corrupt_and_appended_streams_never_publish_a_manifest() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    fs::create_dir(root.join("source")).unwrap();
    fs::write(root.join("source/file"), b"content").unwrap();
    let manifest = snapshot_paths(
        &root.join("source"),
        &root.join("snapshot"),
        &["file".into()],
    )
    .unwrap();
    let expected = manifest.sha256().unwrap();
    let mut stream = Vec::new();
    export_snapshot(
        &root.join("snapshot/tree"),
        &manifest,
        &expected,
        &mut stream,
    )
    .unwrap();
    let mut corrupted = stream.clone();
    *corrupted.last_mut().unwrap() ^= 1;
    let mut appended = stream.clone();
    appended.push(0);
    for (name, bytes) in [
        ("truncated", &stream[..stream.len() - 1]),
        ("corrupt", corrupted.as_slice()),
        ("appended", appended.as_slice()),
    ] {
        let target = root.join(name);
        assert!(import_snapshot(&mut &*bytes, &target, &expected).is_err());
        assert!(!target.join("manifest.json").exists());
    }
}

#[test]
fn unsafe_paths_and_wrong_expected_identity_are_rejected_before_creation() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    for path in ["../escape", "/absolute", ".git/config", "a//b"] {
        let manifest = SnapshotManifest {
            version: 1,
            entries: vec![conductor_runtime::SnapshotEntry {
                path: path.into(),
                sha256: None,
                bytes: 0,
                executable: false,
            }],
        };
        let header = serde_json::to_vec(&manifest).unwrap();
        let mut bytes = b"ACSNAP01".to_vec();
        bytes.extend_from_slice(&(header.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&header);
        assert!(
            import_snapshot(
                &mut bytes.as_slice(),
                &root.join("received"),
                &manifest.sha256().unwrap()
            )
            .is_err()
        );
        assert!(!root.join("received").exists());
    }
    let manifest = SnapshotManifest {
        version: 1,
        entries: vec![],
    };
    let mut bytes = Vec::new();
    export_snapshot(&root, &manifest, &manifest.sha256().unwrap(), &mut bytes).unwrap();
    assert!(import_snapshot(&mut bytes.as_slice(), &root.join("wrong"), &"a".repeat(64)).is_err());
    assert!(!root.join("wrong").exists());
}
