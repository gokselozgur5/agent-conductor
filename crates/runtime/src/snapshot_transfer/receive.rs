use super::{MAGIC, MANIFEST_LIMIT, validate};
use crate::{SnapshotError, SnapshotManifest};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::Path,
};

/// Receives exact source bytes into a new private snapshot directory.
/// The caller must obtain `expected_sha256` independently of this stream.
/// `manifest.json` is published only after all bytes and stream EOF are checked.
/// Errors leave an unpublished partial directory for inspection; never reuse it.
/// # Errors
/// Rejects changed content, unsafe paths, bounds, trailing bytes and existing targets.
pub fn import_snapshot(
    input: &mut impl Read,
    destination: &Path,
    expected_sha256: &str,
) -> Result<SnapshotManifest, SnapshotError> {
    let manifest = read_manifest(input)?;
    validate(&manifest, expected_sha256)?;
    let parent = destination.parent().ok_or(SnapshotError::Invalid)?;
    if !destination.is_absolute() || parent.canonicalize().map_err(|_| SnapshotError::Io)? != parent
    {
        return Err(SnapshotError::Invalid);
    }
    fs::DirBuilder::new()
        .mode(0o700)
        .create(destination)
        .map_err(|_| SnapshotError::Io)?;
    let tree = destination.join("tree");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&tree)
        .map_err(|_| SnapshotError::Io)?;
    for entry in &manifest.entries {
        let Some(expected) = &entry.sha256 else {
            continue;
        };
        let mut bytes = Vec::new();
        (&mut *input)
            .take(entry.bytes)
            .read_to_end(&mut bytes)
            .map_err(|_| SnapshotError::Io)?;
        if bytes.len() as u64 != entry.bytes || format!("{:x}", Sha256::digest(&bytes)) != *expected
        {
            return Err(SnapshotError::Changed);
        }
        let path = tree.join(&entry.path);
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path.parent().ok_or(SnapshotError::Invalid)?)
            .map_err(|_| SnapshotError::Io)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(if entry.executable { 0o700 } else { 0o600 })
            .custom_flags(nix::libc::O_NOFOLLOW)
            .open(path)
            .map_err(|_| SnapshotError::Io)?;
        file.set_permissions(fs::Permissions::from_mode(if entry.executable {
            0o700
        } else {
            0o600
        }))
        .map_err(|_| SnapshotError::Io)?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|_| SnapshotError::Io)?;
    }
    let mut trailing = [0];
    if input.read(&mut trailing).map_err(|_| SnapshotError::Io)? != 0 {
        return Err(SnapshotError::Invalid);
    }
    publish(destination, &manifest)?;
    Ok(manifest)
}

fn read_manifest(input: &mut impl Read) -> Result<SnapshotManifest, SnapshotError> {
    let mut magic = [0; 8];
    let mut length = [0; 8];
    input
        .read_exact(&mut magic)
        .and_then(|()| input.read_exact(&mut length))
        .map_err(|_| SnapshotError::Io)?;
    if &magic != MAGIC {
        return Err(SnapshotError::Invalid);
    }
    let length = u64::from_be_bytes(length);
    if length > MANIFEST_LIMIT {
        return Err(SnapshotError::Bounds);
    }
    let mut bytes = Vec::new();
    input
        .take(length)
        .read_to_end(&mut bytes)
        .map_err(|_| SnapshotError::Io)?;
    if bytes.len() as u64 != length {
        return Err(SnapshotError::Invalid);
    }
    serde_json::from_slice(&bytes).map_err(|_| SnapshotError::Invalid)
}

fn publish(destination: &Path, manifest: &SnapshotManifest) -> Result<(), SnapshotError> {
    let bytes = serde_json::to_vec(manifest).map_err(|_| SnapshotError::Invalid)?;
    let pending = destination.join("manifest.pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW)
        .open(&pending)
        .map_err(|_| SnapshotError::Io)?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| SnapshotError::Io)?;
    fs::hard_link(&pending, destination.join("manifest.json")).map_err(|_| SnapshotError::Io)?;
    fs::remove_file(pending).map_err(|_| SnapshotError::Io)?;
    fs::File::open(destination)
        .and_then(|f| f.sync_all())
        .map_err(|_| SnapshotError::Io)
}
