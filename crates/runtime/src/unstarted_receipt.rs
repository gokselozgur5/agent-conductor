//! Trusted proof that the wrapper abandoned launch before spawning any worker.
use crate::{ResumePacket, RunnerProcessIdentity};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnstartedReceipt {
    version: u8,
    identity: String,
    attempt_id: String,
    observation: String,
}
fn invalid() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid unstarted receipt")
}
fn path(packet: &ResumePacket) -> std::io::Result<PathBuf> {
    let terminal = crate::terminal_receipt::path(packet)?;
    Ok(terminal.with_file_name(format!(
        "unstarted-{}.json",
        packet.sha256().map_err(|_| invalid())?
    )))
}
fn binding(packet: &ResumePacket, identity: &RunnerProcessIdentity) -> std::io::Result<String> {
    if identity.packet_sha256 != packet.sha256().map_err(|_| invalid())? {
        return Err(invalid());
    }
    identity.argument().map_err(|_| invalid())
}
/// Called only by the trusted wrapper on an irrevocable return before worker spawn.
/// The caller must never subsequently launch a worker for this packet/identity.
/// A tag or expected identity alone is not evidence; the private publication is.
/// # Errors
/// Refuses invalid binding, paths and publication failures; never replaces evidence.
pub fn write_unstarted_receipt(
    packet: &ResumePacket,
    identity: &RunnerProcessIdentity,
) -> std::io::Result<()> {
    let receipt = UnstartedReceipt {
        version: 1,
        identity: binding(packet, identity)?,
        attempt_id: packet.contract.attempt_id.clone(),
        observation: "worker_not_started".into(),
    };
    let destination = path(packet)?;
    let pending = destination.with_extension("pending");
    let bytes = serde_json::to_vec(&receipt).map_err(|_| invalid())?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(&pending)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::hard_link(&pending, &destination)?;
    fs::remove_file(&pending)?;
    fs::File::open(destination.parent().ok_or_else(invalid)?)?.sync_all()?;
    Ok(())
}
/// Requires the durable expected wrapper identity and authority, never OS argv.
/// # Errors
/// Missing, partial, linked, public, oversized or mismatched evidence refuses.
pub fn read_unstarted_receipt(
    packet: &ResumePacket,
    identity: &RunnerProcessIdentity,
) -> std::io::Result<UnstartedReceipt> {
    let expected = binding(packet, identity)?;
    let destination = path(packet)?;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(&destination)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.len() > 4096
        || metadata.uid() != fs::metadata(destination.parent().ok_or_else(invalid)?)?.uid()
    {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(invalid());
    }
    let receipt: UnstartedReceipt = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if receipt.version != 1
        || receipt.identity != expected
        || receipt.attempt_id != packet.contract.attempt_id
        || receipt.observation != "worker_not_started"
    {
        return Err(invalid());
    }
    Ok(receipt)
}
