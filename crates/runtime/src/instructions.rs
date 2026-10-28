//! Operator-pinned instruction bytes; repository filenames alone grant no authority.
use crate::{ResumePacket, SnapshotError};
use sha2::{Digest, Sha256};
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt, path::Component};

/// Reads bounded UTF-8 instruction files in frozen order and verifies every hash.
/// Sources must be authorized by the frozen packet read capabilities.
/// Reads use directory descriptors and reject symlink/hardlink/special-file escapes.
/// # Errors
/// Refuses missing, changed, oversized, invalid UTF-8 or unauthorized sources.
pub fn read_pinned_instructions(packet: &ResumePacket) -> Result<Vec<String>, SnapshotError> {
    packet.validate().map_err(|_| SnapshotError::Invalid)?;
    read_approved_instruction_sources(&packet.contract.instructions)
}

/// Reads exactly the source files explicitly pinned by a trusted operator plan.
/// This broker read does not add directories to native model-tool permissions.
/// # Errors
/// Refuses non-absolute/non-clean/duplicate paths and bounded-content violations.
pub fn read_approved_instruction_sources(
    pins: &[crate::InstructionPin],
) -> Result<Vec<String>, SnapshotError> {
    if pins.is_empty() {
        return Ok(Vec::new());
    }
    if pins.len() > 128 {
        return Err(SnapshotError::Bounds);
    }
    let root = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW)
        .open("/")
        .map_err(|_| SnapshotError::Io)?;
    let mut unique = std::collections::BTreeSet::new();
    let mut contents = Vec::new();
    let mut total = 0usize;
    for pin in pins {
        if !unique.insert(&pin.path) {
            return Err(SnapshotError::Invalid);
        }
        let relative = pin
            .path
            .strip_prefix("/")
            .map_err(|_| SnapshotError::Invalid)?;
        if relative
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(SnapshotError::Invalid);
        }
        let path = relative
            .to_str()
            .filter(|s| !s.is_empty())
            .ok_or(SnapshotError::Invalid)?;
        let (bytes, _) = crate::snapshot::read_source_bounded(&root, path, 64 * 1024)?
            .ok_or(SnapshotError::Changed)?;
        total = total
            .checked_add(bytes.len())
            .filter(|n| *n <= 256 * 1024)
            .ok_or(SnapshotError::Bounds)?;
        if format!("{:x}", Sha256::digest(&bytes)) != pin.sha256 {
            return Err(SnapshotError::Changed);
        }
        contents.push(String::from_utf8(bytes).map_err(|_| SnapshotError::Invalid)?);
    }
    Ok(contents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InstructionPin;
    use std::fs;
    #[test]
    fn pins_require_exact_bounded_regular_utf8_content() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let file = root.join("policy.md");
        let mut packet:ResumePacket=serde_json::from_value(serde_json::json!({
            "contract":{"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"worker_id":"worker",
            "adapter":{"kind":"fake","version":"fixture","executable_sha256":"a".repeat(64),"conformance_sha256":"b".repeat(64)},
            "workspace":root,"starting_head":"c".repeat(40),"authority":{"read_roots":[root],"write_roots":[root],"network":"denied","allow_workspace_commands":false},"instructions":[],"checks":[],"max_runtime_ms":1000},
            "objective":"fixture","checkpoint":null,"untrusted_context":[]
        })).unwrap();
        fs::write(&file, "approved").unwrap();
        packet.contract.instructions = vec![InstructionPin {
            path: file.clone(),
            sha256: format!("{:x}", Sha256::digest(b"approved")),
        }];
        assert_eq!(read_pinned_instructions(&packet).unwrap(), vec!["approved"]);
        fs::write(&file, "different").unwrap();
        assert!(matches!(
            read_pinned_instructions(&packet),
            Err(SnapshotError::Changed)
        ));
        fs::write(&file, vec![b'a'; 65_537]).unwrap();
        assert!(matches!(
            read_pinned_instructions(&packet),
            Err(SnapshotError::Bounds)
        ));
        fs::write(&file, [255]).unwrap();
        packet.contract.instructions[0].sha256 = format!("{:x}", Sha256::digest([255]));
        assert!(matches!(
            read_pinned_instructions(&packet),
            Err(SnapshotError::Invalid)
        ));
        fs::hard_link(&file, root.join("alias")).unwrap();
        assert!(matches!(
            read_pinned_instructions(&packet),
            Err(SnapshotError::Unsupported)
        ));
        fs::remove_file(&file).unwrap();
        std::os::unix::fs::symlink(root.join("alias"), &file).unwrap();
        assert!(read_pinned_instructions(&packet).is_err());
        fs::remove_file(&file).unwrap();
        assert!(read_pinned_instructions(&packet).is_err());
    }
}
