#![cfg(unix)]
use conductor_runtime::{SnapshotError, snapshot_git};
use std::{fs, path::Path, process::Command};

fn git(root: &Path, args: &[&str]) -> Vec<u8> {
    let output = Command::new("/usr/bin/git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(root)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .output()
        .expect("git fixture");
    assert!(output.status.success(), "git fixture command failed");
    output.stdout
}
fn repository(root: &Path) {
    fs::create_dir(root).expect("root");
    git(root, &["init", "-q"]);
    fs::write(root.join("tracked"), b"committed").expect("tracked");
    fs::write(root.join("deleted"), b"committed deletion candidate").expect("deleted");
    fs::write(root.join(".gitignore"), b"ignored\n").expect("ignore");
    git(root, &["add", "."]);
    git(root, &["commit", "-qm", "fixture"]);
}

#[tokio::test]
async fn git_snapshot_preserves_index_head_dirty_tracked_and_untracked_files() {
    let directory = tempfile::tempdir().expect("directory");
    let source = directory.path().join("source");
    repository(&source);
    fs::write(source.join("tracked"), b"staged").expect("stage change");
    git(&source, &["add", "tracked"]);
    fs::write(source.join("tracked"), b"unstaged dirty bytes").expect("dirty");
    fs::remove_file(source.join("deleted")).expect("fixture deletion");
    fs::write(source.join("untracked"), b"new work").expect("new");
    fs::write(source.join("ignored"), b"excluded fixture").expect("ignored");
    let index = fs::read(source.join(".git/index")).expect("index");
    let head = git(&source, &["rev-parse", "HEAD"]);
    let target = directory.path().join("snapshot");
    let snapshot = snapshot_git(&source, &target).await.expect("snapshot");
    assert_eq!(
        snapshot.starting_head.as_bytes(),
        head.strip_suffix(b"\n").expect("newline")
    );
    assert_eq!(fs::read(source.join(".git/index")).expect("index"), index);
    assert_eq!(git(&source, &["rev-parse", "HEAD"]), head);
    assert_eq!(
        fs::read(target.join("tree/tracked")).expect("copy"),
        b"unstaged dirty bytes"
    );
    assert_eq!(
        fs::read(target.join("tree/untracked")).expect("copy"),
        b"new work"
    );
    assert!(!target.join("tree/ignored").exists());
    assert!(!target.join("tree/.git").exists());
    assert!(
        snapshot
            .manifest
            .entries
            .iter()
            .any(|entry| entry.path == "deleted" && entry.sha256.is_none())
    );
    assert!(target.join("git-snapshot.json").exists());
}

#[tokio::test]
async fn sparse_checkout_refuses_instead_of_recording_omitted_files_as_deletions() {
    let directory = tempfile::tempdir().expect("directory");
    let source = directory.path().join("source");
    repository(&source);
    git(&source, &["config", "core.sparseCheckout", "true"]);
    let target = directory.path().join("snapshot");
    assert!(matches!(
        snapshot_git(&source, &target).await,
        Err(SnapshotError::Unsupported)
    ));
    assert!(!target.exists());
}

#[tokio::test]
async fn observation_detects_same_status_edits_and_preserves_source() {
    use conductor_runtime::observe_git;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    repository(&source);
    let index = fs::read(source.join(".git/index")).unwrap();
    fs::write(source.join("tracked"), "first dirty version").unwrap();
    let first = observe_git(&source).await.unwrap();
    fs::write(source.join("tracked"), "other dirty version").unwrap();
    let second = observe_git(&source).await.unwrap();
    assert_eq!(first.status_sha256, second.status_sha256);
    assert_ne!(
        first.manifest.sha256().unwrap(),
        second.manifest.sha256().unwrap()
    );
    assert_eq!(fs::read(source.join(".git/index")).unwrap(), index);
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    fs::remove_file(source.join("deleted")).unwrap();
    fs::write(source.join("ignored"), "not included").unwrap();
    let third = observe_git(&source).await.unwrap();
    assert!(
        third
            .manifest
            .entries
            .iter()
            .any(|e| e.path == "deleted" && e.sha256.is_none())
    );
    assert!(!third.manifest.entries.iter().any(|e| e.path == "ignored"));
    std::os::unix::fs::symlink("tracked", source.join("linked")).unwrap();
    assert!(matches!(
        observe_git(&source).await,
        Err(SnapshotError::Unsupported)
    ));
}

#[tokio::test]
async fn notification_roots_include_linked_worktree_metadata_without_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    repository(&source);
    let source = source.canonicalize().unwrap();
    let linked = directory.path().join("linked");
    git(
        &source,
        &[
            "worktree",
            "add",
            "--detach",
            linked.to_str().unwrap(),
            "HEAD",
        ],
    );
    let linked = linked.canonicalize().unwrap();
    let index = fs::read(source.join(".git/index")).unwrap();
    let head = git(&linked, &["rev-parse", "HEAD"]);
    let roots = conductor_runtime::git_observation_roots(&linked)
        .await
        .unwrap();
    assert!(roots.contains(&linked));
    assert!(roots.contains(&source.join(".git")));
    assert!(
        roots
            .iter()
            .any(|p| p.starts_with(source.join(".git/worktrees")))
    );
    assert_eq!(fs::read(source.join(".git/index")).unwrap(), index);
    assert_eq!(git(&linked, &["rev-parse", "HEAD"]), head);
}

#[tokio::test]
async fn observation_distinguishes_same_commit_branches_and_detached_head() {
    use conductor_runtime::{discover_advisory, observe_git};
    let directory = tempfile::tempdir().expect("directory");
    let source = directory.path().join("source");
    repository(&source);
    let before = observe_git(&source).await.unwrap();
    git(&source, &["checkout", "-qb", "external-branch"]);
    let switched = observe_git(&source).await.unwrap();
    assert_eq!(before.starting_head, switched.starting_head);
    assert_eq!(before.manifest, switched.manifest);
    assert_eq!(before.status_sha256, switched.status_sha256);
    assert_ne!(before.starting_ref, switched.starting_ref);
    assert_eq!(
        switched.starting_ref.as_deref(),
        Some("refs/heads/external-branch")
    );
    assert_eq!(
        discover_advisory(&switched).unwrap().starting_ref,
        switched.starting_ref
    );
    git(&source, &["checkout", "--detach", "-q"]);
    let detached = observe_git(&source).await.unwrap();
    assert_eq!(detached.starting_head, before.starting_head);
    assert_eq!(detached.starting_ref.as_deref(), Some("HEAD"));
    let mut legacy = serde_json::to_value(&before).unwrap();
    legacy.as_object_mut().unwrap().remove("starting_ref");
    let decoded: conductor_runtime::GitSnapshot = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(decoded.starting_ref, None);
    assert_eq!(serde_json::to_value(decoded).unwrap(), legacy);
}

#[tokio::test]
async fn source_identity_tracks_reference_commit_and_directory_replacement() {
    use conductor_runtime::observe_git_source;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().canonicalize().unwrap().join("source");
    repository(&source);
    let before = observe_git_source(&source).await.unwrap();
    fs::write(source.join("tracked"), b"dirty work").unwrap();
    assert_eq!(observe_git_source(&source).await.unwrap(), before);
    git(&source, &["checkout", "-qb", "identity-branch"]);
    let branch = observe_git_source(&source).await.unwrap();
    assert_eq!(branch.head, before.head);
    assert_ne!(branch.reference, before.reference);
    git(
        &source,
        &["commit", "--allow-empty", "-qm", "external commit"],
    );
    assert_ne!(observe_git_source(&source).await.unwrap().head, before.head);
    fs::rename(&source, directory.path().join("retained")).unwrap();
    repository(&source);
    assert_ne!(
        observe_git_source(&source).await.unwrap().inode,
        before.inode
    );
    assert!(observe_git_source(&source.join(".git")).await.is_err());
}
