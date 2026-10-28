//! Private trusted-wrapper observations, outside the native writer's allowed root.
use crate::{
    ProcessOutput, ResumePacket, RunnerEvent, accepts_file_writer_exit, accepts_file_writer_stop,
    interpret_vendor_output,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalReceipt {
    version: u8,
    packet_sha256: String,
    attempt_id: String,
    pub observation: String,
    pub output_sha256: String,
}
fn path(packet: &ResumePacket) -> Result<PathBuf, std::io::Error> {
    packet.validate().map_err(|_| invalid())?;
    let root = packet.contract.workspace.canonicalize()?;
    if root != packet.contract.workspace
        || packet
            .contract
            .authority
            .write_roots
            .iter()
            .any(|p| p != &root)
    {
        return Err(invalid());
    }
    let digest = packet.sha256().map_err(|_| invalid())?;
    Ok(root
        .parent()
        .ok_or_else(invalid)?
        .join(format!("terminal-{digest}.json")))
}
fn invalid() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid terminal receipt")
}
/// Publishes only accepted restricted-writer observations, without raw output.
/// Files are created privately and never replace prior receipt bytes.
/// # Errors
/// Returns path, permission, publication or durability errors.
pub fn write_terminal_receipt(packet: &ResumePacket, event: &RunnerEvent) -> std::io::Result<()> {
    let (observation, stdout) = match event {
        RunnerEvent::Stopped { stdout, .. } if accepts_file_writer_stop(packet, stdout) => {
            ("restricted_stop", stdout)
        }
        RunnerEvent::Finished {
            exit_code,
            stdout,
            stderr,
        } if interpret_vendor_output(
            packet,
            &ProcessOutput {
                exit_code: *exit_code,
                stdout: stdout.clone(),
                stderr: stderr.clone(),
            },
        )
        .is_ok_and(|c| accepts_file_writer_exit(packet, &c)) =>
        {
            ("restricted_exit", stdout)
        }
        _ => return Ok(()),
    };
    let destination = path(packet)?;
    let pending = destination.with_extension("pending");
    let receipt = TerminalReceipt {
        version: 1,
        packet_sha256: packet.sha256().map_err(|_| invalid())?,
        attempt_id: packet.contract.attempt_id.clone(),
        observation: observation.into(),
        output_sha256: format!("{:x}", Sha256::digest(stdout)),
    };
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
/// Reads a complete private receipt for exactly this stored attempt packet.
/// # Errors
/// Missing/malformed/mismatched/linked or oversized files are not fence evidence.
pub fn read_terminal_receipt(packet: &ResumePacket) -> std::io::Result<TerminalReceipt> {
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
    let receipt: TerminalReceipt = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if receipt.version != 1
        || receipt.packet_sha256 != packet.sha256().map_err(|_| invalid())?
        || receipt.attempt_id != packet.contract.attempt_id
        || !matches!(
            receipt.observation.as_str(),
            "restricted_stop" | "restricted_exit"
        )
        || receipt.output_sha256.len() != 64
        || !receipt.output_sha256.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid());
    }
    Ok(receipt)
}
