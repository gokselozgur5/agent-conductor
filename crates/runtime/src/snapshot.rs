//! Bounded private snapshots of an explicit source inventory. Git inventory and
//! quiescence are separate requirements; this module never resets a checkout.
use nix::{
    errno::Errno,
    fcntl::{OFlag, openat},
    sys::stat::Mode,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Component, Path},
};

const FILE_LIMIT: u64 = 32 * 1024 * 1024;
const TOTAL_LIMIT: u64 = 512 * 1024 * 1024;
const PATH_LIMIT: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotEntry {
    pub path: String,
    /// Missing tracked files remain explicit deletions in the manifest.
    pub sha256: Option<String>,
    pub bytes: u64,
    pub executable: bool,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotManifest {
    pub version: u16,
    pub entries: Vec<SnapshotEntry>,
}
impl SnapshotManifest {
    /// Hashes the ordered manifest for binding into an authoritative source record.
    /// # Errors
    /// Returns serialization failure without printing file contents.
    pub fn sha256(&self) -> Result<String, SnapshotError> {
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(self).map_err(|_| SnapshotError::Invalid)?)
        ))
    }
}
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("invalid source inventory or overlapping snapshot destination")]
    Invalid,
    #[error("snapshot exceeds file, byte or path bounds")]
    Bounds,
    #[error("source changed while snapshot was being prepared")]
    Changed,
    #[error("source contains a symlink, hardlink or unsupported filesystem object")]
    Unsupported,
    #[error("snapshot filesystem operation failed")]
    Io,
}

/// Copies exactly the supplied tracked/untracked inventory into a new private
/// directory. Existing destinations are refused. Source files are opened relative
/// to directory descriptors with no symlink traversal, including ancestors.
/// A second read checks source stability; this is not an atomic filesystem freeze.
/// On failure the partial destination remains for inspection, without a completed
/// manifest. The caller must not publish or launch from a failed snapshot.
/// # Errors
/// Refuses escapes, links, special files, oversized inputs and concurrent changes.
pub fn snapshot_paths(
    source: &Path,
    destination: &Path,
    paths: &[String],
) -> Result<SnapshotManifest, SnapshotError> {
    let inventory = validate_paths(paths)?;
    let source = source.canonicalize().map_err(|_| SnapshotError::Io)?;
    let parent = destination
        .parent()
        .ok_or(SnapshotError::Invalid)?
        .canonicalize()
        .map_err(|_| SnapshotError::Io)?;
    let name = destination.file_name().ok_or(SnapshotError::Invalid)?;
    let destination = parent.join(name);
    if destination.starts_with(&source) || source.starts_with(&destination) {
        return Err(SnapshotError::Invalid);
    }
    let root = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW)
        .open(&source)
        .map_err(|_| SnapshotError::Io)?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&destination)
        .map_err(|_| SnapshotError::Io)?;
    let tree = destination.join("tree");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&tree)
        .map_err(|_| SnapshotError::Io)?;
    let mut manifest = SnapshotManifest {
        version: 1,
        entries: Vec::new(),
    };
    let mut total = 0_u64;
    for path in inventory {
        let content = read_source(&root, &path)?;
        let entry = entry(&path, content.as_ref());
        total = total
            .checked_add(entry.bytes)
            .filter(|total| *total <= TOTAL_LIMIT)
            .ok_or(SnapshotError::Bounds)?;
        if let Some((bytes, executable)) = content {
            let output = tree.join(&path);
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(output.parent().ok_or(SnapshotError::Invalid)?)
                .map_err(|_| SnapshotError::Io)?;
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&output)
                .map_err(|_| SnapshotError::Io)?;
            file.write_all(&bytes).map_err(|_| SnapshotError::Io)?;
            file.set_permissions(fs::Permissions::from_mode(if executable {
                0o700
            } else {
                0o600
            }))
            .map_err(|_| SnapshotError::Io)?;
            file.sync_all().map_err(|_| SnapshotError::Io)?;
        }
        manifest.entries.push(entry);
    }
    for expected in &manifest.entries {
        if entry(&expected.path, read_source(&root, &expected.path)?.as_ref()) != *expected {
            return Err(SnapshotError::Changed);
        }
    }
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(destination.join("manifest.json"))
        .map_err(|_| SnapshotError::Io)?;
    output
        .write_all(&serde_json::to_vec(&manifest).map_err(|_| SnapshotError::Invalid)?)
        .map_err(|_| SnapshotError::Io)?;
    output.sync_all().map_err(|_| SnapshotError::Io)?;
    Ok(manifest)
}

fn validate_paths(paths: &[String]) -> Result<BTreeSet<String>, SnapshotError> {
    if paths.len() > PATH_LIMIT {
        return Err(SnapshotError::Bounds);
    }
    let mut unique = BTreeSet::new();
    for path in paths {
        if path.is_empty()
            || path.len() > 4096
            || path.contains('\0')
            || Path::new(path)
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
            || path.split('/').any(|part| {
                part.is_empty() || part == "." || part == ".." || part.eq_ignore_ascii_case(".git")
            })
            || !unique.insert(path.clone())
        {
            return Err(SnapshotError::Invalid);
        }
    }
    Ok(unique)
}
fn entry(path: &str, content: Option<&(Vec<u8>, bool)>) -> SnapshotEntry {
    SnapshotEntry {
        path: path.into(),
        sha256: content.map(|(bytes, _)| format!("{:x}", Sha256::digest(bytes))),
        bytes: content.map_or(0, |(bytes, _)| bytes.len() as u64),
        executable: content.is_some_and(|(_, executable)| *executable),
    }
}
fn read_source(root: &File, path: &str) -> Result<Option<(Vec<u8>, bool)>, SnapshotError> {
    let mut directory = root.try_clone().map_err(|_| SnapshotError::Io)?;
    let components: Vec<_> = path.split('/').collect();
    for (index, component) in components.iter().enumerate() {
        let final_component = index + 1 == components.len();
        let flags = OFlag::O_RDONLY
            | OFlag::O_NOFOLLOW
            | OFlag::O_CLOEXEC
            | if final_component {
                OFlag::O_NONBLOCK
            } else {
                OFlag::O_DIRECTORY
            };
        let fd = match openat(&directory, *component, flags, Mode::empty()) {
            Ok(fd) => fd,
            Err(Errno::ENOENT) => return Ok(None),
            Err(Errno::ELOOP | Errno::ENOTDIR) => return Err(SnapshotError::Unsupported),
            Err(_) => return Err(SnapshotError::Io),
        };
        let file = File::from(fd);
        if !final_component {
            directory = file;
            continue;
        }
        let before = file.metadata().map_err(|_| SnapshotError::Io)?;
        if !before.is_file() || before.nlink() != 1 {
            return Err(SnapshotError::Unsupported);
        }
        if before.len() > FILE_LIMIT {
            return Err(SnapshotError::Bounds);
        }
        let mut bytes = Vec::new();
        (&file)
            .take(FILE_LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SnapshotError::Io)?;
        if bytes.len() as u64 > FILE_LIMIT {
            return Err(SnapshotError::Bounds);
        }
        let after = file.metadata().map_err(|_| SnapshotError::Io)?;
        if before.len() != after.len()
            || before.mtime() != after.mtime()
            || before.mtime_nsec() != after.mtime_nsec()
            || before.mode() != after.mode()
        {
            return Err(SnapshotError::Changed);
        }
        return Ok(Some((bytes, before.mode() & 0o111 != 0)));
    }
    Err(SnapshotError::Invalid)
}
