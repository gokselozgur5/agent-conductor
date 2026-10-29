//! Private immutable broker claim/receipt files. Host policy must exclude all
//! untrusted same-UID writers and movers from this directory and its ancestors.
use crate::{BrokerError, BrokerScope, ChildIdentity};
use nix::{
    fcntl::{AtFlags, OFlag, openat},
    sys::stat::Mode,
    unistd::{UnlinkatFlags, linkat, unlinkat},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrokerShutdownObservation {
    NeverReleased,
    Revoked { completed_sequence: u64 },
}
/// No bearer capability, file contents, vendor output or raw launch is retained.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerShutdownReceipt {
    pub version: u16,
    pub scope: BrokerScope,
    pub launch_sha256: String,
    pub root_identity_sha256: String,
    pub process: ChildIdentity,
    pub observation: BrokerShutdownObservation,
}
fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl BrokerShutdownReceipt {
    fn validate(&self) -> Result<(), BrokerError> {
        if self.version != 1
            || !self.scope.valid()
            || !valid_hash(&self.launch_sha256)
            || !valid_hash(&self.root_identity_sha256)
            || self.process.pid == 0
            || self.process.pid > i32::MAX.unsigned_abs()
            || self.process.start_marker.is_empty()
            || self.process.start_marker.len() > 128
            || self.process.start_marker.chars().any(char::is_control)
            || matches!(self.observation,BrokerShutdownObservation::Revoked{completed_sequence} if completed_sequence>100_000)
        {
            return Err(BrokerError::Invalid);
        }
        self.process
            .host
            .validate()
            .map_err(|_| BrokerError::Invalid)
    }
}
/// Pinned private directory; descriptor-relative operations never follow a
/// replacement entry. Root/path revalidation detects moves but cannot establish
/// hostile same-UID exclusion by itself.
pub struct BrokerReceiptDirectory {
    directory: File,
    path: PathBuf,
}
impl BrokerReceiptDirectory {
    /// # Errors
    /// Refuses noncanonical, linked, nonprivate or foreign-owned directories.
    pub fn open(path: &Path) -> Result<Self, BrokerError> {
        if !path.is_absolute() || path.canonicalize().map_err(|_| BrokerError::Refused)? != path {
            return Err(BrokerError::Refused);
        }
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(path)
            .map_err(|_| BrokerError::Refused)?;
        let root = Self {
            directory,
            path: path.to_owned(),
        };
        root.check()?;
        Ok(root)
    }
    fn check(&self) -> Result<(), BrokerError> {
        let pinned = self
            .directory
            .metadata()
            .map_err(|_| BrokerError::Refused)?;
        let current = self
            .path
            .symlink_metadata()
            .map_err(|_| BrokerError::Refused)?;
        if !current.is_dir()
            || current.dev() != pinned.dev()
            || current.ino() != pinned.ino()
            || pinned.uid() != nix::unistd::geteuid().as_raw()
            || pinned.mode() & 0o777 != 0o700
            || self.path.canonicalize().map_err(|_| BrokerError::Refused)? != self.path
        {
            return Err(BrokerError::Refused);
        }
        Ok(())
    }
    fn name(scope: &BrokerScope, suffix: &str) -> Result<String, BrokerError> {
        if !scope.valid() {
            return Err(BrokerError::Invalid);
        }
        Ok(format!("broker-{}.{suffix}", scope.invocation_id))
    }
    fn create(&self, name: &str) -> Result<File, BrokerError> {
        self.check()?;
        openat(
            &self.directory,
            name,
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .map(File::from)
        .map_err(|_| BrokerError::Refused)
    }
    /// Claim exactly once before Ready. Never delete a failed or old claim.
    /// # Errors
    /// Refuses replay, changed directory, invalid binding and persistence failure.
    pub fn claim(&self, scope: &BrokerScope, launch_sha256: &str) -> Result<(), BrokerError> {
        if !valid_hash(launch_sha256) {
            return Err(BrokerError::Invalid);
        }
        let mut file = self.create(&Self::name(scope, "claimed")?)?;
        file.write_all(launch_sha256.as_bytes())
            .and_then(|()| file.sync_all())
            .map_err(|_| BrokerError::Uncertain)?;
        self.directory
            .sync_all()
            .map_err(|_| BrokerError::Uncertain)?;
        self.check()
    }
    /// Publish only `NeverReleased` or a proved quiescent, revoked watermark.
    /// Unknown outcomes must not call this method. This artifact is evidence,
    /// never task acceptance or authority to replay a file operation.
    /// # Errors
    /// Refuses existing evidence, changed directory and any incomplete publication.
    pub fn publish(&self, receipt: &BrokerShutdownReceipt) -> Result<(), BrokerError> {
        receipt.validate()?;
        self.verify_claim(&receipt.scope, &receipt.launch_sha256)?;
        let bytes = serde_json::to_vec(receipt).map_err(|_| BrokerError::Invalid)?;
        if bytes.len() > 4096 {
            return Err(BrokerError::Invalid);
        }
        let pending = Self::name(&receipt.scope, "pending")?;
        let destination = Self::name(&receipt.scope, "json")?;
        let mut file = self.create(&pending)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| BrokerError::Uncertain)?;
        self.check()?;
        linkat(
            &self.directory,
            pending.as_str(),
            &self.directory,
            destination.as_str(),
            AtFlags::empty(),
        )
        .map_err(|_| BrokerError::Uncertain)?;
        unlinkat(
            &self.directory,
            pending.as_str(),
            UnlinkatFlags::NoRemoveDir,
        )
        .map_err(|_| BrokerError::Uncertain)?;
        self.directory
            .sync_all()
            .map_err(|_| BrokerError::Uncertain)?;
        self.check()
    }
    /// Read and compare every field to independently persisted expectations.
    /// Merely finding a receipt or observing PID absence is never enough.
    /// # Errors
    /// Refuses missing, linked, mutable, oversized or mismatched evidence.
    pub fn read(
        &self,
        expected: &BrokerShutdownReceipt,
    ) -> Result<BrokerShutdownReceipt, BrokerError> {
        expected.validate()?;
        self.verify_claim(&expected.scope, &expected.launch_sha256)?;
        let bytes = self.read_file(&Self::name(&expected.scope, "json")?)?;
        let receipt: BrokerShutdownReceipt =
            serde_json::from_slice(&bytes).map_err(|_| BrokerError::Invalid)?;
        receipt.validate()?;
        if &receipt != expected {
            return Err(BrokerError::Refused);
        }
        Ok(receipt)
    }
    fn verify_claim(&self, scope: &BrokerScope, hash: &str) -> Result<(), BrokerError> {
        if self.read_file(&Self::name(scope, "claimed")?)? != hash.as_bytes() {
            return Err(BrokerError::Refused);
        }
        Ok(())
    }
    fn read_file(&self, name: &str) -> Result<Vec<u8>, BrokerError> {
        self.check()?;
        let file = File::from(
            openat(
                &self.directory,
                name,
                OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| BrokerError::Refused)?,
        );
        let before = file.metadata().map_err(|_| BrokerError::Refused)?;
        if !before.is_file()
            || before.nlink() != 1
            || before.uid() != nix::unistd::geteuid().as_raw()
            || before.mode() & 0o777 != 0o600
            || before.len() > 4096
        {
            return Err(BrokerError::Refused);
        }
        let mut bytes = Vec::new();
        (&file)
            .take(4097)
            .read_to_end(&mut bytes)
            .map_err(|_| BrokerError::Uncertain)?;
        let after = file.metadata().map_err(|_| BrokerError::Refused)?;
        if bytes.len() > 4096
            || before.len() != after.len()
            || before.mtime() != after.mtime()
            || before.mtime_nsec() != after.mtime_nsec()
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
            || after.nlink() != 1
        {
            return Err(BrokerError::Refused);
        }
        self.check()?;
        Ok(bytes)
    }
}
