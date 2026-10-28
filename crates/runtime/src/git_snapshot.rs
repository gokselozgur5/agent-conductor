//! Read-only Git inventory around explicit private snapshot construction.
use crate::{ProcessSpec, SnapshotError, SnapshotManifest, run_process, snapshot_paths};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::watch;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GitSnapshot {
    pub starting_head: String,
    pub status_sha256: String,
    pub manifest: SnapshotManifest,
}
#[derive(Eq, PartialEq)]
struct Inventory {
    head: String,
    status_sha256: String,
    paths: Vec<String>,
}

/// Captures tracked and unignored untracked working-tree bytes without changing
/// the source HEAD, index or files. Ignored files are deliberately excluded;
/// this is not a backup of secrets, build outputs or repository object history.
/// Sparse checkouts, submodules, non-UTF8 paths and links fail closed.
/// `git-snapshot.json` is written only after both inventory observations agree.
/// # Errors
/// Refuses unsupported Git layouts, changed inventory, bounds and snapshot errors.
pub async fn snapshot_git(source: &Path, destination: &Path) -> Result<GitSnapshot, SnapshotError> {
    let source = source.canonicalize().map_err(|_| SnapshotError::Io)?;
    let before = inventory(&source).await?;
    let root = source.clone();
    let target = destination.to_path_buf();
    let paths = before.paths.clone();
    let manifest = tokio::task::spawn_blocking(move || snapshot_paths(&root, &target, &paths))
        .await
        .map_err(|_| SnapshotError::Io)??;
    if inventory(&source).await? != before {
        return Err(SnapshotError::Changed);
    }
    let snapshot = GitSnapshot {
        starting_head: before.head,
        status_sha256: before.status_sha256,
        manifest,
    };
    let bytes = serde_json::to_vec(&snapshot).map_err(|_| SnapshotError::Invalid)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(destination.join("git-snapshot.json"))
        .map_err(|_| SnapshotError::Io)?;
    file.write_all(&bytes).map_err(|_| SnapshotError::Io)?;
    file.sync_all().map_err(|_| SnapshotError::Io)?;
    Ok(snapshot)
}
/// Observes repository content identities without copying files or changing Git.
/// This is a bounded, double-checked observation, not an atomic filesystem freeze.
/// Ignored files stay excluded; later equal status text does not hide byte changes.
/// # Errors
/// Refuses unsupported layouts/links, changing inventory and snapshot bounds.
pub async fn observe_git(source: &Path) -> Result<GitSnapshot, SnapshotError> {
    let source = source.canonicalize().map_err(|_| SnapshotError::Io)?;
    let before = inventory(&source).await?;
    let root = source.clone();
    let paths = before.paths.clone();
    let manifest =
        tokio::task::spawn_blocking(move || crate::snapshot::observe_paths(&root, &paths))
            .await
            .map_err(|_| SnapshotError::Io)??;
    if inventory(&source).await? != before {
        return Err(SnapshotError::Changed);
    }
    Ok(GitSnapshot {
        starting_head: before.head,
        status_sha256: before.status_sha256,
        manifest,
    })
}

async fn inventory(root: &Path) -> Result<Inventory, SnapshotError> {
    let toplevel = git(root, &["rev-parse", "--show-toplevel"]).await?;
    let top = PathBuf::from(
        std::str::from_utf8(&toplevel)
            .map_err(|_| SnapshotError::Unsupported)?
            .trim_end_matches('\n'),
    );
    if top.canonicalize().map_err(|_| SnapshotError::Io)? != root {
        return Err(SnapshotError::Invalid);
    }
    // ls-files --sparse emits directory entries for sparse indexes, but ordinary
    // sparse checkout can omit tracked files too; reject the configuration.
    let sparse = git_status(root, &["config", "--bool", "core.sparseCheckout"]).await?;
    if sparse.exit_code == Some(0) && sparse.stdout != b"false\n" {
        return Err(SnapshotError::Unsupported);
    }
    if !matches!(sparse.exit_code, Some(0 | 1)) {
        return Err(SnapshotError::Io);
    }
    let head = String::from_utf8(git(root, &["rev-parse", "--verify", "HEAD"]).await?)
        .map_err(|_| SnapshotError::Invalid)?;
    let head = head.trim_end().to_string();
    if ![40, 64].contains(&head.len()) || !head.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(SnapshotError::Invalid);
    }
    let raw = git(
        root,
        &[
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
    )
    .await?;
    let mut paths = Vec::new();
    for path in raw.split(|byte| *byte == 0).filter(|path| !path.is_empty()) {
        paths.push(
            std::str::from_utf8(path)
                .map_err(|_| SnapshotError::Unsupported)?
                .to_string(),
        );
    }
    // An unmerged index can repeat a path across stages. A snapshot cannot
    // represent unresolved stage ownership; reject instead of deduplicating it.
    paths.sort();
    let status = git(
        root,
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )
    .await?;
    Ok(Inventory {
        head,
        status_sha256: format!("{:x}", Sha256::digest(status)),
        paths,
    })
}
async fn git(root: &Path, args: &[&str]) -> Result<Vec<u8>, SnapshotError> {
    let result = git_status(root, args).await?;
    if result.exit_code != Some(0) {
        return Err(SnapshotError::Io);
    }
    Ok(result.stdout)
}
async fn git_status(root: &Path, args: &[&str]) -> Result<crate::ProcessOutput, SnapshotError> {
    let mut arguments: Vec<String> = [
        "--no-optional-locks",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.untrackedCache=false",
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    arguments.extend(args.iter().map(ToString::to_string));
    let environment = BTreeMap::from([
        ("PATH".into(), "/usr/bin:/bin".into()),
        ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
        ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
        ("GIT_TERMINAL_PROMPT".into(), "0".into()),
        ("LC_ALL".into(), "C".into()),
    ]);
    let (_sender, receiver) = watch::channel(false);
    run_process(
        &ProcessSpec {
            executable: "/usr/bin/git".into(),
            arguments,
            directory: root.into(),
            environment,
            timeout: Duration::from_secs(10),
            output_limit: 16 * 1024 * 1024,
        },
        receiver,
    )
    .await
    .map_err(|_| SnapshotError::Io)
}
