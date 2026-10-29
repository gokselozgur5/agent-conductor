//! Descriptor-relative file operations for an exclusively broker-owned task tree.
//! External writers must be excluded by the host sandbox before granting access.
use crate::{
    BrokerError, BrokerIntent, BrokerOperation, BrokerRequest, BrokerScope, FileBrokerGate,
};
use nix::{
    errno::Errno,
    fcntl::{AtFlags, OFlag, openat, renameat},
    sys::stat::Mode,
    unistd::{UnlinkatFlags, linkat, unlinkat},
};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

const LIMIT: u64 = 256 * 1024;
const WRITE_BUDGET: u64 = 64 * 1024 * 1024;

/// Private contents are deliberately not Debug-formatted.
#[derive(serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BrokerFileResult {
    Text { contents: String, sha256: String },
    Written { sha256: String },
}

/// Owns the only file-operation path and serializes it through its capability.
/// Construction does not establish OS isolation or authorize the supplied root.
/// The trusted launcher must supply a private non-Git task copy, deny all other
/// writers (including directory moves), and persist intents in `admit`.
pub struct BrokerFilesystem {
    root: File,
    path: PathBuf,
    gate: FileBrokerGate,
    written_bytes: AtomicU64,
}

impl BrokerFilesystem {
    /// Pin an already-authorized canonical root and immutable invocation scope.
    /// # Errors
    /// Refuses noncanonical, linked, missing roots and invalid/expired scope.
    pub fn new(root: &Path, scope: BrokerScope, deadline_ms: u64) -> Result<Self, BrokerError> {
        if !root.is_absolute() || root.canonicalize().map_err(|_| BrokerError::Refused)? != root {
            return Err(BrokerError::Refused);
        }
        let handle = OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(root)
            .map_err(|_| BrokerError::Refused)?;
        let broker = Self {
            root: handle,
            path: root.to_owned(),
            gate: FileBrokerGate::new(scope, deadline_ms)?,
            written_bytes: AtomicU64::new(0),
        };
        broker.check_root()?;
        Ok(broker)
    }

    /// Execute one bounded operation after trusted durable intent admission.
    /// # Errors
    /// Any admitted file failure closes the capability with an uncertain result.
    /// Failed publication never triggers automatic deletion of staging evidence.
    pub fn execute(
        &self,
        request: &BrokerRequest,
        admit: impl FnOnce(&BrokerIntent) -> Result<(), BrokerError>,
    ) -> Result<BrokerFileResult, BrokerError> {
        self.execute_recorded(request, admit, |_, _| Ok(()))
    }

    /// Execute and persist the result before acknowledging or permitting closure.
    /// # Errors
    /// Admission, file or completion failures preserve uncertainty and close access.
    pub fn execute_recorded(
        &self,
        request: &BrokerRequest,
        admit: impl FnOnce(&BrokerIntent) -> Result<(), BrokerError>,
        complete: impl FnOnce(&BrokerIntent, &BrokerFileResult) -> Result<(), BrokerError>,
    ) -> Result<BrokerFileResult, BrokerError> {
        self.gate
            .execute_recorded(request, admit, |request| self.apply(request), complete)
    }

    fn apply(&self, request: &BrokerRequest) -> Result<BrokerFileResult, BrokerError> {
        self.check_root()?;
        match &request.operation {
            BrokerOperation::ReadText { path } => {
                let (parent, name) = self.parent(path)?;
                let (contents, _) = read(&parent, name)?.ok_or(BrokerError::Refused)?;
                let sha256 = hash(contents.as_bytes());
                Ok(BrokerFileResult::Text { contents, sha256 })
            }
            BrokerOperation::WriteText {
                path,
                expected_sha256,
                contents,
            } => {
                let (parent, name) = self.parent(path)?;
                let previous = read(&parent, name)?;
                let mode = match (&previous, expected_sha256) {
                    (None, None) => 0o600,
                    (Some((bytes, mode)), Some(expected))
                        if hash(bytes.as_bytes()) == *expected =>
                    {
                        *mode
                    }
                    _ => return Err(BrokerError::Refused),
                };
                self.written_bytes
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |total| {
                        total
                            .checked_add(contents.len() as u64)
                            .filter(|next| *next <= WRITE_BUDGET)
                    })
                    .map_err(|_| BrokerError::Refused)?;
                let staging = format!(
                    ".conductor-broker-{}-{}",
                    request.scope.invocation_id, request.sequence
                );
                let fd = openat(
                    &parent,
                    staging.as_str(),
                    OFlag::O_WRONLY
                        | OFlag::O_CREAT
                        | OFlag::O_EXCL
                        | OFlag::O_NOFOLLOW
                        | OFlag::O_CLOEXEC,
                    Mode::S_IRUSR | Mode::S_IWUSR,
                )
                .map_err(|_| BrokerError::Uncertain)?;
                let mut file = File::from(fd);
                file.write_all(contents.as_bytes())
                    .map_err(|_| BrokerError::Uncertain)?;
                file.set_permissions(std::fs::Permissions::from_mode(mode))
                    .map_err(|_| BrokerError::Uncertain)?;
                file.sync_all().map_err(|_| BrokerError::Uncertain)?;
                // Recheck observed preconditions. This detects changes; it is
                // not a compare-and-swap against hostile external writers.
                if read(&parent, name)? != previous {
                    return Err(BrokerError::Uncertain);
                }
                self.check_root()?;
                if expected_sha256.is_none() {
                    // Atomic create-only publication cannot replace a racer.
                    linkat(&parent, staging.as_str(), &parent, name, AtFlags::empty())
                        .map_err(|_| BrokerError::Uncertain)?;
                    unlinkat(&parent, staging.as_str(), UnlinkatFlags::NoRemoveDir)
                        .map_err(|_| BrokerError::Uncertain)?;
                } else {
                    renameat(&parent, staging.as_str(), &parent, name)
                        .map_err(|_| BrokerError::Uncertain)?;
                }
                parent.sync_all().map_err(|_| BrokerError::Uncertain)?;
                Ok(BrokerFileResult::Written {
                    sha256: hash(contents.as_bytes()),
                })
            }
        }
    }

    /// Close admission immediately; does not acknowledge completion of writes.
    pub fn request_revocation(&self) {
        self.gate.request_revocation();
    }

    /// Wait for active file work and acknowledge only an unambiguous gate state.
    /// # Errors
    /// Uncertain operations or poisoned state do not produce a clean result.
    pub fn revoke(&self) -> Result<u64, BrokerError> {
        self.gate.revoke()
    }

    fn check_root(&self) -> Result<(), BrokerError> {
        let pinned = self.root.metadata().map_err(|_| BrokerError::Refused)?;
        let current = self
            .path
            .symlink_metadata()
            .map_err(|_| BrokerError::Refused)?;
        if !current.is_dir()
            || current.dev() != pinned.dev()
            || current.ino() != pinned.ino()
            || self.path.canonicalize().map_err(|_| BrokerError::Refused)? != self.path
        {
            return Err(BrokerError::Refused);
        }
        Ok(())
    }

    fn parent<'a>(&self, path: &'a str) -> Result<(File, &'a str), BrokerError> {
        let mut parts = path.split('/').peekable();
        let mut parent = self.root.try_clone().map_err(|_| BrokerError::Refused)?;
        let device = parent.metadata().map_err(|_| BrokerError::Refused)?.dev();
        while let Some(part) = parts.next() {
            if part.starts_with(".conductor-broker-") || part.eq_ignore_ascii_case(".git") {
                return Err(BrokerError::Refused);
            }
            if parts.peek().is_none() {
                return Ok((parent, part));
            }
            let fd = openat(
                &parent,
                part,
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| BrokerError::Refused)?;
            parent = File::from(fd);
            if parent.metadata().map_err(|_| BrokerError::Refused)?.dev() != device {
                return Err(BrokerError::Refused);
            }
        }
        Err(BrokerError::Invalid)
    }
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read(parent: &File, name: &str) -> Result<Option<(String, u32)>, BrokerError> {
    let fd = match openat(
        parent,
        name,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::ENOENT) => return Ok(None),
        Err(_) => return Err(BrokerError::Refused),
    };
    let file = File::from(fd);
    let before = file.metadata().map_err(|_| BrokerError::Refused)?;
    if !before.is_file()
        || before.nlink() != 1
        || before.len() > LIMIT
        || before.dev() != parent.metadata().map_err(|_| BrokerError::Refused)?.dev()
    {
        return Err(BrokerError::Refused);
    }
    let mut bytes = Vec::new();
    (&file)
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| BrokerError::Refused)?;
    let after = file.metadata().map_err(|_| BrokerError::Refused)?;
    if bytes.len() as u64 > LIMIT
        || after.nlink() != 1
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
        || before.ctime() != after.ctime()
        || before.ctime_nsec() != after.ctime_nsec()
        || before.mode() != after.mode()
    {
        return Err(BrokerError::Refused);
    }
    let text = String::from_utf8(bytes).map_err(|_| BrokerError::Refused)?;
    // Preserve ordinary executable bits, never setuid/setgid/sticky privileges.
    Ok(Some((text, before.mode() & 0o777)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exhausted_cumulative_budget_refuses_before_staging() {
        let root = tempfile::tempdir().unwrap();
        let request = BrokerRequest {
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
                path: "file".into(),
                contents: "ab".into(),
                expected_sha256: None,
            },
        };
        let broker = BrokerFilesystem::new(
            &root.path().canonicalize().unwrap(),
            request.scope.clone(),
            crate::continuous_millis().unwrap() + 60_000,
        )
        .unwrap();
        broker
            .written_bytes
            .store(WRITE_BUDGET - 1, Ordering::SeqCst);
        assert!(broker.execute(&request, |_| Ok(())).is_err());
        assert_eq!(fs_count(root.path()), 0);
        assert_eq!(broker.revoke(), Err(BrokerError::Uncertain));
    }
    fn fs_count(root: &Path) -> usize {
        std::fs::read_dir(root).unwrap().count()
    }
}
