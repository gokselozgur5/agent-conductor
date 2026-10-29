//! Candidate byte capture and return. Peer claims do not establish task acceptance.
const MANIFEST_LIMIT: u64 = 64 * 1024 * 1024;
use crate::{
    AccountChannelAddress, AccountSnapshotError as Error, SnapshotManifest,
    account_snapshot::DeadlineStream, connect_account_channel, export_snapshot, import_snapshot,
    snapshot_attempt_tree,
};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountCaptureRequest {
    pub packet_sha256: String,
    pub source: PathBuf,
    pub destination: PathBuf,
    pub baseline_paths: Vec<String>,
}

/// Authenticated peer's capture selection, not proof of execution or acceptance.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountCaptureReceipt {
    pub packet_sha256: String,
    pub snapshot_sha256: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountSnapshotFetchRequest {
    pub snapshot_sha256: String,
    pub snapshot_directory: PathBuf,
}

/// Request a new snapshot under the receiving account's own permissions. The
/// caller must separately establish terminal/lease eligibility before publication.
/// # Errors
/// Refuses packet mismatch, invalid paths, existing destinations or capture failure.
pub async fn capture_account_candidate(
    address: &AccountChannelAddress,
    request: &AccountCaptureRequest,
) -> Result<AccountCaptureReceipt, Error> {
    if request.packet_sha256 != address.packet_sha256 {
        return Err(Error);
    }
    let stream = connect_account_channel(address, Duration::from_secs(30))
        .await
        .map_err(|_| Error)?;
    let request = request.clone();
    tokio::task::spawn_blocking(move || {
        let mut wire = DeadlineStream::new(stream, Vec::new())?;
        send_request(&mut wire, &request)?;
        let mut bytes = Vec::new();
        (&mut wire)
            .take(1025)
            .read_to_end(&mut bytes)
            .map_err(|_| Error)?;
        if bytes.len() > 1024 {
            return Err(Error);
        }
        let receipt: AccountCaptureReceipt = serde_json::from_slice(&bytes).map_err(|_| Error)?;
        if receipt.packet_sha256 != request.packet_sha256 || !digest_valid(&receipt.snapshot_sha256)
        {
            return Err(Error);
        }
        Ok(receipt)
    })
    .await
    .map_err(|_| Error)?
}

/// Import bytes for a separately selected peer capture digest into a new local
/// snapshot. Independent checks/review must assess this untrusted candidate.
/// # Errors
/// Refuses changed source bytes, digest mismatch, unsafe paths or incomplete I/O.
pub async fn fetch_account_snapshot(
    address: &AccountChannelAddress,
    snapshot_directory: &Path,
    destination: &Path,
) -> Result<SnapshotManifest, Error> {
    let stream = connect_account_channel(address, Duration::from_secs(30))
        .await
        .map_err(|_| Error)?;
    let request = AccountSnapshotFetchRequest {
        snapshot_sha256: address.packet_sha256.clone(),
        snapshot_directory: snapshot_directory.into(),
    };
    let destination = destination.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut wire = DeadlineStream::new(stream, Vec::new())?;
        send_request(&mut wire, &request)?;
        import_snapshot(&mut wire, &destination, &request.snapshot_sha256).map_err(|_| Error)
    })
    .await
    .map_err(|_| Error)?
}

/// Serve capture only after the selected coordinator UID has been authenticated.
/// The admitted packet binds the request; this does not assert that it has exited.
/// # Errors
/// Refuses mismatched request identity, failed capture or response transport.
pub async fn serve_account_capture(
    stream: tokio::net::UnixStream,
    request: AccountCaptureRequest,
    admitted_digest: &str,
) -> Result<(), Error> {
    if request.packet_sha256 != admitted_digest {
        return Err(Error);
    }
    tokio::task::spawn_blocking(move || {
        let mut wire = DeadlineStream::new(stream, Vec::new())?;
        let manifest = snapshot_attempt_tree(
            &request.source,
            &request.destination,
            &request.baseline_paths,
        )
        .map_err(|_| Error)?;
        let receipt = AccountCaptureReceipt {
            packet_sha256: request.packet_sha256,
            snapshot_sha256: manifest.sha256().map_err(|_| Error)?,
        };
        wire.write_all(&serde_json::to_vec(&receipt).map_err(|_| Error)?)
            .map_err(|_| Error)
    })
    .await
    .map_err(|_| Error)?
}

/// Export a selected snapshot to an already authenticated coordinator. Source
/// content is revalidated against the selected digest while sending it.
/// # Errors
/// Refuses changed manifests/bytes, invalid paths and failed transport.
pub async fn serve_account_snapshot_fetch(
    stream: tokio::net::UnixStream,
    request: AccountSnapshotFetchRequest,
    admitted_digest: &str,
) -> Result<(), Error> {
    if request.snapshot_sha256 != admitted_digest {
        return Err(Error);
    }
    tokio::task::spawn_blocking(move || {
        let mut wire = DeadlineStream::new(stream, Vec::new())?;
        let mut bytes = Vec::new();
        std::fs::File::open(request.snapshot_directory.join("manifest.json"))
            .map_err(|_| Error)?
            .take(MANIFEST_LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error)?;
        if bytes.len() as u64 > MANIFEST_LIMIT {
            return Err(Error);
        }
        let manifest: SnapshotManifest = serde_json::from_slice(&bytes).map_err(|_| Error)?;
        export_snapshot(
            &request.snapshot_directory.join("tree"),
            &manifest,
            &request.snapshot_sha256,
            &mut wire,
        )
        .map_err(|_| Error)
    })
    .await
    .map_err(|_| Error)?
}

fn send_request(wire: &mut DeadlineStream, request: &impl serde::Serialize) -> Result<(), Error> {
    let mut bytes = serde_json::to_vec(request).map_err(|_| Error)?;
    if bytes.len() >= crate::MAX_PACKET_BYTES {
        return Err(Error);
    }
    bytes.push(b'\n');
    wire.write_all(&bytes).map_err(|_| Error)
}
fn digest_valid(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
