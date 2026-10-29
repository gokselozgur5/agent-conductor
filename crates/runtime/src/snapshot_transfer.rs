//! Transport of an exact snapshot between accounts without sharing a directory.
//! This binds bytes, not worker authority, write fencing, or task acceptance.
use crate::{SnapshotError, SnapshotManifest, snapshot};
use sha2::{Digest, Sha256};
use std::{fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt, path::Path};

const MAGIC: &[u8; 8] = b"ACSNAP01";
const MANIFEST_LIMIT: u64 = 64 * 1024 * 1024;
mod receive;
pub use receive::import_snapshot;

fn validate(manifest: &SnapshotManifest, expected: &str) -> Result<(), SnapshotError> {
    let paths = manifest
        .entries
        .iter()
        .map(|e| e.path.clone())
        .collect::<Vec<_>>();
    let ordered = snapshot::validate_paths(&paths)?;
    if manifest.version != 1 || !ordered.iter().eq(paths.iter()) {
        return Err(SnapshotError::Invalid);
    }
    let mut total = 0_u64;
    for entry in &manifest.entries {
        if entry.bytes > snapshot::FILE_LIMIT {
            return Err(SnapshotError::Bounds);
        }
        total = total
            .checked_add(entry.bytes)
            .filter(|v| *v <= snapshot::TOTAL_LIMIT)
            .ok_or(SnapshotError::Bounds)?;
        match &entry.sha256 {
            None if entry.bytes == 0 && !entry.executable => {}
            Some(hash)
                if hash.len() == 64
                    && hash
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) => {}
            _ => return Err(SnapshotError::Invalid),
        }
    }
    if manifest.sha256()? != expected {
        return Err(SnapshotError::Changed);
    }
    Ok(())
}

/// Writes a versioned manifest followed by its exact file bytes in manifest order.
/// The expected identity must come from the caller's selected source, not a peer.
/// A failed or interrupted stream is incomplete and must never be published.
/// # Errors
/// Rejects invalid inventories, changed bytes/modes, links and transport failures.
pub fn export_snapshot(
    source: &Path,
    manifest: &SnapshotManifest,
    expected_sha256: &str,
    output: &mut impl Write,
) -> Result<(), SnapshotError> {
    validate(manifest, expected_sha256)?;
    if source.canonicalize().map_err(|_| SnapshotError::Io)? != source {
        return Err(SnapshotError::Invalid);
    }
    let root = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW)
        .open(source)
        .map_err(|_| SnapshotError::Io)?;
    let header = serde_json::to_vec(manifest).map_err(|_| SnapshotError::Invalid)?;
    let size = u64::try_from(header.len()).map_err(|_| SnapshotError::Bounds)?;
    if size > MANIFEST_LIMIT {
        return Err(SnapshotError::Bounds);
    }
    output
        .write_all(MAGIC)
        .and_then(|()| output.write_all(&size.to_be_bytes()))
        .and_then(|()| output.write_all(&header))
        .map_err(|_| SnapshotError::Io)?;
    for entry in &manifest.entries {
        let content = snapshot::read_source_bounded(&root, &entry.path, snapshot::FILE_LIMIT)?;
        match (content, &entry.sha256) {
            (None, None) => {}
            (Some((bytes, executable)), Some(hash))
                if bytes.len() as u64 == entry.bytes
                    && executable == entry.executable
                    && format!("{:x}", Sha256::digest(&bytes)) == *hash =>
            {
                output.write_all(&bytes).map_err(|_| SnapshotError::Io)?;
            }
            _ => return Err(SnapshotError::Changed),
        }
    }
    output.flush().map_err(|_| SnapshotError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn export_binds_source_bytes_and_rejects_changed_identity() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let source = root.join("source");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("answer"), b"42\n").unwrap();
        let snapshot = root.join("snapshot");
        let manifest =
            crate::snapshot_paths(&source, &snapshot, &["answer".into(), "deleted".into()])
                .unwrap();
        let identity = manifest.sha256().unwrap();
        let mut bytes = Vec::new();
        export_snapshot(&snapshot.join("tree"), &manifest, &identity, &mut bytes).unwrap();
        assert!(bytes.starts_with(MAGIC));
        assert!(bytes.ends_with(b"42\n"));
        assert!(
            export_snapshot(
                &snapshot.join("tree"),
                &manifest,
                &"a".repeat(64),
                &mut Vec::new()
            )
            .is_err()
        );
        std::fs::write(snapshot.join("tree/answer"), b"43\n").unwrap();
        assert!(matches!(
            export_snapshot(
                &snapshot.join("tree"),
                &manifest,
                &identity,
                &mut Vec::new()
            ),
            Err(SnapshotError::Changed)
        ));
    }
}
