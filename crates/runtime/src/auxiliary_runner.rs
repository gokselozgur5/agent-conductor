//! Auxiliary supervisor protocol. Receipts describe owned groups, not all descendants.
use crate::{ChildIdentity, ProcessError, ProcessOutput, ProcessSpec, run_process};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::watch,
};

const INPUT_LIMIT: usize = 4 * 1024 * 1024;
const OUTPUT_LIMIT: usize = 32 * 1024 * 1024;

/// Sent on the private stdin pipe, never saved or formatted with Debug.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliaryLaunch {
    pub version: u8,
    pub invocation_id: String,
    pub authority_id: String,
    pub boot_generation: u64,
    pub receipt_root: PathBuf,
    pub executable: PathBuf,
    pub executable_sha256: String,
    pub arguments: Vec<String>,
    pub directory: PathBuf,
    pub environment: BTreeMap<String, String>,
    pub timeout_ms: u64,
    pub heartbeat_timeout_ms: u64,
    pub output_limit: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuxiliaryControl {
    Release { launch_sha256: String },
    Renew { launch_sha256: String },
    Cancel { launch_sha256: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuxiliaryStopReason {
    Cancelled,
    TimedOut,
    OutputLimit,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuxiliaryEvent {
    Ready {
        launch_sha256: String,
        process: ChildIdentity,
    },
    Finished {
        exit_code: Option<i32>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    Stopped {
        reason: AuxiliaryStopReason,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuxiliaryObservation {
    NeverReleased,
    OwnedGroupSettled,
}

/// No raw output or credentials. This is settlement evidence, never check success.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliaryReceipt {
    pub version: u8,
    pub invocation_id: String,
    pub authority_id: String,
    pub boot_generation: u64,
    pub launch_sha256: String,
    pub process: ChildIdentity,
    pub observation: AuxiliaryObservation,
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "auxiliary protocol or evidence invalid",
    )
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|v| !v.is_nil() && v.to_string() == value)
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn private_root(root: &Path) -> io::Result<()> {
    let meta = fs::symlink_metadata(root)?;
    if root.canonicalize()? != root
        || !meta.is_dir()
        || meta.uid() != nix::unistd::getuid().as_raw()
        || meta.permissions().mode() & 0o777 != 0o700
    {
        return Err(invalid());
    }
    Ok(())
}

impl AuxiliaryLaunch {
    /// Validates bounded transport input. The trusted caller separately establishes
    /// the sandbox and ensures `receipt_root` is outside every child write root.
    /// # Errors
    /// Rejects malformed identities, paths, budgets and unsafe receipt directories.
    pub fn validate(&self) -> io::Result<()> {
        if self.version != 1
            || !canonical_uuid(&self.invocation_id)
            || !valid_digest(&self.authority_id)
            || self.boot_generation == 0
            || !self.executable.is_absolute()
            || !self.directory.is_absolute()
            || self.directory.canonicalize()? != self.directory
            || self.receipt_root.starts_with(&self.directory)
            || self.arguments.len() > 128
            || self
                .arguments
                .iter()
                .any(|a| a.len() > 65536 || a.contains('\0'))
            || self.environment.len() > 128
            || self.environment.iter().any(|(k, v)| {
                k.is_empty()
                    || k.len() > 256
                    || k.contains(['\0', '='])
                    || v.len() > 65536
                    || v.contains('\0')
            })
            || self.timeout_ms == 0
            || self.timeout_ms > 86_400_000
            || self.heartbeat_timeout_ms == 0
            || self.heartbeat_timeout_ms > 30_000
            || self.output_limit == 0
            || self.output_limit > 2 * 1024 * 1024
        {
            return Err(invalid());
        }
        private_root(&self.receipt_root)?;
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
            .open(&self.executable)?;
        if !file.metadata()?.is_file() {
            return Err(invalid());
        }
        let mut bounded = file.take(512 * 1024 * 1024 + 1);
        let mut hasher = Sha256::new();
        let size = io::copy(&mut bounded, &mut hasher)?;
        if size > 512 * 1024 * 1024 || format!("{:x}", hasher.finalize()) != self.executable_sha256
        {
            return Err(invalid());
        }
        Ok(())
    }

    /// Hashes the exact approved launch including its private environment; only
    /// the digest may be persisted. Never log the serialized input.
    /// # Errors
    /// Rejects oversized or invalid inputs.
    pub fn sha256(&self) -> io::Result<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid())?;
        if bytes.len() > INPUT_LIMIT {
            return Err(invalid());
        }
        Ok(digest(&bytes))
    }

    fn specification(&self) -> ProcessSpec {
        ProcessSpec {
            executable: self.executable.clone(),
            arguments: self.arguments.clone(),
            directory: self.directory.clone(),
            environment: self.environment.clone(),
            timeout: Duration::from_millis(self.timeout_ms),
            output_limit: self.output_limit,
        }
    }
}

fn receipt_path(root: &Path, invocation: &str) -> io::Result<PathBuf> {
    if !canonical_uuid(invocation) {
        return Err(invalid());
    }
    private_root(root)?;
    Ok(root.join(format!("auxiliary-{invocation}.json")))
}

fn publish_receipt(root: &Path, receipt: &AuxiliaryReceipt) -> io::Result<()> {
    let destination = receipt_path(root, &receipt.invocation_id)?;
    let pending = destination.with_extension("pending");
    let bytes = serde_json::to_vec(receipt).map_err(|_| invalid())?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::hard_link(&pending, &destination)?;
    fs::remove_file(&pending)?;
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

/// Reads one private, complete receipt and compares every field with trusted
/// persisted expectations, including wrapper birth/host identity. PID absence
/// never substitutes for this evidence. No process is signaled.
/// # Errors
/// Refuses missing, changed, linked, malformed, oversized or nonprivate evidence.
pub fn read_auxiliary_receipt(
    root: &Path,
    expected: &AuxiliaryReceipt,
) -> io::Result<AuxiliaryReceipt> {
    validate_receipt(expected)?;
    let receipt = read_private_receipt(root, &expected.invocation_id)?;
    if &receipt != expected {
        return Err(invalid());
    }
    Ok(receipt)
}

/// Reads pre-release evidence when no independently observed process was saved.
/// The receipt's process is evidence only: it must never become an ACK, live
/// process observation or signaling target. All durable launch scope must match.
/// # Errors
/// Refuses released outcomes, mismatched scope and invalid/private-file evidence.
pub fn read_auxiliary_unreleased_receipt(
    root: &Path,
    invocation: &str,
    authority: &str,
    boot_generation: u64,
    launch_sha256: &str,
) -> io::Result<AuxiliaryReceipt> {
    let receipt = read_private_receipt(root, invocation)?;
    if receipt.invocation_id != invocation
        || receipt.authority_id != authority
        || receipt.boot_generation != boot_generation
        || receipt.launch_sha256 != launch_sha256
        || receipt.observation != AuxiliaryObservation::NeverReleased
    {
        return Err(invalid());
    }
    Ok(receipt)
}

fn validate_receipt(receipt: &AuxiliaryReceipt) -> io::Result<()> {
    receipt.process.host.validate()?;
    if receipt.version != 1
        || receipt.boot_generation == 0
        || !valid_digest(&receipt.authority_id)
        || receipt.process.pid == 0
        || receipt.process.start_marker.is_empty()
        || receipt.process.start_marker.len() > 128
        || receipt.launch_sha256.len() != 64
        || !receipt.launch_sha256.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid());
    }
    if !canonical_uuid(&receipt.invocation_id) {
        return Err(invalid());
    }
    Ok(())
}

fn read_private_receipt(root: &Path, invocation: &str) -> io::Result<AuxiliaryReceipt> {
    let path = receipt_path(root, invocation)?;
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.nlink() != 1
        || meta.uid() != nix::unistd::getuid().as_raw()
        || meta.permissions().mode() & 0o777 != 0o600
        || meta.len() > 4096
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    (&file).take(4097).read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    if meta.len() != after.len()
        || meta.mtime() != after.mtime()
        || meta.mtime_nsec() != after.mtime_nsec()
        || meta.ctime() != after.ctime()
        || meta.ctime_nsec() != after.ctime_nsec()
        || after.nlink() != 1
    {
        return Err(invalid());
    }
    if bytes.len() > 4096 {
        return Err(invalid());
    }
    let receipt: AuxiliaryReceipt = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    validate_receipt(&receipt)?;
    Ok(receipt)
}

async fn frame(reader: &mut (impl AsyncBufRead + Unpin), limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(u64::try_from(limit).map_err(|_| invalid())? + 1)
        .read_until(b'\n', &mut bytes)
        .await?;
    if bytes.is_empty() || bytes.len() > limit || bytes.last() != Some(&b'\n') {
        return Err(invalid());
    }
    Ok(bytes)
}
async fn emit(event: &AuxiliaryEvent) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(event).map_err(|_| invalid())?;
    if bytes.len() > OUTPUT_LIMIT {
        return Err(invalid());
    }
    bytes.push(b'\n');
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut out = tokio::io::stdout();
        out.write_all(&bytes).await?;
        out.flush().await
    })
    .await
    .map_err(|_| invalid())?
}

async fn await_release(
    launch: &AuxiliaryLaunch,
    hash: &str,
    identity: &ChildIdentity,
    input: &mut (impl AsyncBufRead + Unpin),
) -> io::Result<()> {
    emit(&AuxiliaryEvent::Ready {
        launch_sha256: hash.into(),
        process: identity.clone(),
    })
    .await?;
    let bytes = tokio::time::timeout(Duration::from_secs(10), frame(input, 4096))
        .await
        .map_err(|_| invalid())??;
    match serde_json::from_slice::<AuxiliaryControl>(&bytes).map_err(|_| invalid())? {
        AuxiliaryControl::Release { launch_sha256 } if launch_sha256 == hash => {
            launch.validate()?;
            identity.revalidate().await
        }
        _ => Err(invalid()),
    }
}

async fn supervise(
    launch: &AuxiliaryLaunch,
    hash: &str,
    input: &mut (impl AsyncBufRead + Unpin),
) -> Result<ProcessOutput, ProcessError> {
    let specification = launch.specification();
    let (cancel, receiver) = watch::channel(false);
    let operation = run_process(&specification, receiver);
    tokio::pin!(operation);
    let mut deadline =
        tokio::time::Instant::now() + Duration::from_millis(launch.heartbeat_timeout_ms);
    loop {
        tokio::select! {
            result=&mut operation=>return result,
            ()=tokio::time::sleep_until(deadline)=>break,
            message=frame(input,4096)=>{
                match message.ok().and_then(|v|serde_json::from_slice::<AuxiliaryControl>(&v).ok()) {
                    Some(AuxiliaryControl::Renew{launch_sha256}) if launch_sha256==hash=>deadline=tokio::time::Instant::now()+Duration::from_millis(launch.heartbeat_timeout_ms),
                    _=>break,
                }
            }
        }
    }
    let _ = cancel.send(true);
    operation.await
}

/// Entrypoint for the existing trusted runner's `--auxiliary UUID` mode.
/// The parent commits the launch digest before spawn and identity before Release.
/// # Errors
/// Protocol/identity/receipt failures remain unknown; private input is not logged.
pub async fn serve_auxiliary(invocation: &str) -> io::Result<()> {
    if !canonical_uuid(invocation) {
        return Err(invalid());
    }
    let mut input = BufReader::new(tokio::io::stdin());
    let bytes = tokio::time::timeout(Duration::from_secs(10), frame(&mut input, INPUT_LIMIT))
        .await
        .map_err(|_| invalid())??;
    let launch: AuxiliaryLaunch = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if launch.invocation_id != invocation {
        return Err(invalid());
    }
    let hash = launch.sha256()?;
    // Durable exclusive wrapper claim: even a caller replay cannot release two
    // wrappers with the same invocation identity. A killed wrapper stays unknown.
    let marker = launch
        .receipt_root
        .join(format!("auxiliary-{invocation}.launched"));
    let mut claim = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(marker)?;
    claim.write_all(hash.as_bytes())?;
    claim.sync_all()?;
    fs::File::open(&launch.receipt_root)?.sync_all()?;
    let process = ChildIdentity::observe(std::process::id()).await?;
    let mut receipt = AuxiliaryReceipt {
        version: 1,
        invocation_id: invocation.into(),
        authority_id: launch.authority_id.clone(),
        boot_generation: launch.boot_generation,
        launch_sha256: hash.clone(),
        process: process.clone(),
        observation: AuxiliaryObservation::NeverReleased,
    };
    if await_release(&launch, &hash, &process, &mut input)
        .await
        .is_err()
    {
        publish_receipt(&launch.receipt_root, &receipt)?;
        return Err(invalid());
    }
    let result = supervise(&launch, &hash, &mut input).await;
    let event = match result {
        Ok(output) => AuxiliaryEvent::Finished {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
        },
        Err(ProcessError::Cancelled) => AuxiliaryEvent::Stopped {
            reason: AuxiliaryStopReason::Cancelled,
        },
        Err(ProcessError::Timeout) => AuxiliaryEvent::Stopped {
            reason: AuxiliaryStopReason::TimedOut,
        },
        Err(ProcessError::OutputLimit) => AuxiliaryEvent::Stopped {
            reason: AuxiliaryStopReason::OutputLimit,
        },
        Err(_) => return Err(invalid()),
    };
    receipt.observation = AuxiliaryObservation::OwnedGroupSettled;
    publish_receipt(&launch.receipt_root, &receipt)?;
    emit(&event).await
}
