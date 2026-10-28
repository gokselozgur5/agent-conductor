#![cfg(unix)]
use conductor_runtime::{SnapshotError, snapshot_paths};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
};

#[test]
fn private_copy_preserves_dirty_bytes_deletions_modes_and_original() {
    let directory = tempfile::tempdir().expect("directory");
    let source = directory.path().join("source");
    fs::create_dir(&source).expect("source");
    fs::write(source.join("tracked"), b"dirty contents").expect("dirty");
    fs::write(source.join("untracked"), b"new contents").expect("new");
    fs::set_permissions(source.join("untracked"), fs::Permissions::from_mode(0o755)).expect("mode");
    let paths = vec!["untracked".into(), "deleted".into(), "tracked".into()];
    let destination = directory.path().join("snapshot");
    let manifest = snapshot_paths(&source, &destination, &paths).expect("snapshot");
    assert_eq!(manifest.entries.len(), 3);
    assert_eq!(manifest.entries[0].path, "deleted");
    assert!(manifest.entries[0].sha256.is_none());
    assert!(manifest.entries[2].executable);
    assert_eq!(
        fs::read(destination.join("tree/tracked")).expect("copy"),
        b"dirty contents"
    );
    fs::write(destination.join("tree/tracked"), b"worker changed copy").expect("modify copy");
    assert_eq!(
        fs::read(source.join("tracked")).expect("original"),
        b"dirty contents"
    );
    let second = snapshot_paths(&source, &directory.path().join("second"), &paths).expect("repeat");
    assert_eq!(
        manifest.sha256().expect("hash"),
        second.sha256().expect("hash")
    );
    assert!(
        snapshot_paths(&source, &destination, &paths).is_err(),
        "never overwrite an existing snapshot"
    );
    assert_eq!(
        fs::metadata(&destination)
            .expect("mode")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
}

#[test]
fn escapes_git_metadata_duplicates_and_nested_destinations_are_refused() {
    let directory = tempfile::tempdir().expect("directory");
    let source = directory.path().join("source");
    fs::create_dir(&source).expect("source");
    for (index, paths) in [
        vec!["../outside"],
        vec!["/absolute"],
        vec![".git/config"],
        vec!["a/.GIT/config"],
        vec!["a", "a"],
        vec!["a//b"],
        vec!["a/./b"],
    ]
    .iter()
    .enumerate()
    {
        let target = directory.path().join(format!("rejected-{index}"));
        let paths: Vec<_> = paths.iter().map(ToString::to_string).collect();
        assert!(matches!(
            snapshot_paths(&source, &target, &paths),
            Err(SnapshotError::Invalid)
        ));
        assert!(
            !target.exists(),
            "invalid inventory must be refused before destination creation"
        );
    }
    assert!(matches!(
        snapshot_paths(&source, &source.join("nested"), &[]),
        Err(SnapshotError::Invalid)
    ));
}

#[test]
fn final_and_ancestor_symlinks_and_hardlinks_are_never_copied() {
    let directory = tempfile::tempdir().expect("directory");
    let source = directory.path().join("source");
    let outside = directory.path().join("outside");
    fs::create_dir(&source).expect("source");
    fs::create_dir(&outside).expect("outside");
    fs::write(outside.join("private"), b"fixture sentinel").expect("fixture");
    symlink(outside.join("private"), source.join("link")).expect("symlink");
    symlink(&outside, source.join("ancestor")).expect("ancestor");
    fs::hard_link(outside.join("private"), source.join("hardlink")).expect("hardlink");
    for (index, path) in ["link", "ancestor/private", "hardlink"].iter().enumerate() {
        let target = directory.path().join(format!("rejected-{index}"));
        assert!(matches!(
            snapshot_paths(&source, &target, &[(*path).into()]),
            Err(SnapshotError::Unsupported)
        ));
        assert!(!target.join("manifest.json").exists());
    }
}

#[test]
fn sparse_oversized_files_and_special_files_are_refused_without_blocking() {
    use nix::{sys::stat::Mode, unistd::mkfifo};
    let directory = tempfile::tempdir().expect("directory");
    let source = directory.path().join("source");
    fs::create_dir(&source).expect("source");
    fs::File::create(source.join("large"))
        .expect("file")
        .set_len(33 * 1024 * 1024)
        .expect("sparse");
    mkfifo(&source.join("pipe"), Mode::S_IRUSR | Mode::S_IWUSR).expect("fifo");
    assert!(matches!(
        snapshot_paths(
            &source,
            &directory.path().join("large-copy"),
            &["large".into()]
        ),
        Err(SnapshotError::Bounds)
    ));
    assert!(matches!(
        snapshot_paths(
            &source,
            &directory.path().join("pipe-copy"),
            &["pipe".into()]
        ),
        Err(SnapshotError::Unsupported)
    ));
}
