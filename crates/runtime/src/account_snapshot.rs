//! Exact baseline staging over an already authenticated account rendezvous.
use crate::{
    AccountChannelAddress, SnapshotManifest, connect_account_channel, export_snapshot,
    import_snapshot,
};
use std::{
    io::{self, Read, Write},
    net::Shutdown,
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(30);

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountSnapshotRequest {
    pub snapshot_sha256: String,
    pub destination: PathBuf,
}

#[derive(Debug, thiserror::Error)]
#[error("account snapshot transfer failed; inspect the unpublished destination before retry")]
pub struct AccountSnapshotError;

/// Stage selected exact bytes in a new receiving-account directory. No credential
/// transfer, worker execution or overwrite is implied. The caller selects both
/// the manifest and its digest independently of the receiving account.
/// # Errors
/// Refuses digest/peer mismatch, unsafe snapshots, existing targets or failed I/O.
pub async fn stage_account_snapshot(
    address: &AccountChannelAddress,
    source: &Path,
    manifest: &SnapshotManifest,
    destination: &Path,
) -> Result<(), AccountSnapshotError> {
    let digest = manifest.sha256().map_err(|_| AccountSnapshotError)?;
    if digest != address.packet_sha256 || !destination.is_absolute() {
        return Err(AccountSnapshotError);
    }
    let stream = connect_account_channel(address, WAIT)
        .await
        .map_err(|_| AccountSnapshotError)?;
    let request = AccountSnapshotRequest {
        snapshot_sha256: digest,
        destination: destination.into(),
    };
    let source = source.to_path_buf();
    let manifest = manifest.clone();
    tokio::task::spawn_blocking(move || {
        let mut wire = DeadlineStream::new(stream, Vec::new())?;
        let mut header = serde_json::to_vec(&request).map_err(|_| AccountSnapshotError)?;
        if header.len() > 16384 {
            return Err(AccountSnapshotError);
        }
        header.push(b'\n');
        wire.write_all(&header).map_err(|_| AccountSnapshotError)?;
        export_snapshot(&source, &manifest, &request.snapshot_sha256, &mut wire)
            .map_err(|_| AccountSnapshotError)?;
        wire.stream
            .shutdown(Shutdown::Write)
            .map_err(|_| AccountSnapshotError)?;
        let mut reply = Vec::new();
        (&mut wire)
            .take(66)
            .read_to_end(&mut reply)
            .map_err(|_| AccountSnapshotError)?;
        if reply != request.snapshot_sha256.as_bytes() {
            return Err(AccountSnapshotError);
        }
        Ok(())
    })
    .await
    .map_err(|_| AccountSnapshotError)?
}

/// Import a request from a channel whose kernel peer has already been admitted.
/// `admitted_digest` is the authenticated handshake selection, not the request's
/// own assertion. Buffered bytes after its JSON line must be preserved verbatim.
/// # Errors
/// Refuses digest mismatch and failed/bounded transfer. Partial directories remain
/// unpublished and cannot be reused as a successful staging result.
pub async fn receive_account_snapshot(
    stream: tokio::net::UnixStream,
    prefetched: Vec<u8>,
    request: AccountSnapshotRequest,
    admitted_digest: &str,
) -> Result<(), AccountSnapshotError> {
    if request.snapshot_sha256 != admitted_digest {
        return Err(AccountSnapshotError);
    }
    tokio::task::spawn_blocking(move || {
        let mut wire = DeadlineStream::new(stream, prefetched)?;
        import_snapshot(&mut wire, &request.destination, &request.snapshot_sha256)
            .map_err(|_| AccountSnapshotError)?;
        wire.write_all(request.snapshot_sha256.as_bytes())
            .map_err(|_| AccountSnapshotError)?;
        Ok(())
    })
    .await
    .map_err(|_| AccountSnapshotError)?
}

// A fresh timeout on every read would allow a slow peer to retain a worker
// indefinitely. All socket I/O shares one absolute transfer deadline.
pub(crate) struct DeadlineStream {
    stream: UnixStream,
    prefetched: io::Cursor<Vec<u8>>,
    deadline: Instant,
}
impl DeadlineStream {
    pub(crate) fn new(
        stream: tokio::net::UnixStream,
        prefetched: Vec<u8>,
    ) -> Result<Self, AccountSnapshotError> {
        let stream = stream.into_std().map_err(|_| AccountSnapshotError)?;
        Ok(Self {
            stream,
            prefetched: io::Cursor::new(prefetched),
            deadline: Instant::now() + WAIT,
        })
    }
    fn remaining(&self) -> io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "snapshot transfer deadline"))
    }
}
impl DeadlineStream {
    fn ready(&self, event: nix::poll::PollFlags) -> io::Result<()> {
        use std::os::fd::AsFd;
        loop {
            let timeout = nix::poll::PollTimeout::try_from(self.remaining()?)
                .map_err(|_| io::Error::other("invalid transfer deadline"))?;
            let mut descriptors = [nix::poll::PollFd::new(self.stream.as_fd(), event)];
            match nix::poll::poll(&mut descriptors, timeout) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "snapshot transfer deadline",
                    ));
                }
                Ok(_) => return Ok(()),
                Err(nix::errno::Errno::EINTR) => {}
                Err(error) => return Err(io::Error::from_raw_os_error(error as i32)),
            }
        }
    }
}
impl Read for DeadlineStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.remaining()?;
        let count = self.prefetched.read(bytes)?;
        if count != 0 || bytes.is_empty() {
            return Ok(count);
        }
        loop {
            self.ready(nix::poll::PollFlags::POLLIN)?;
            match self.stream.read(bytes) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) => {}
                result => return result,
            }
        }
    }
}
impl Write for DeadlineStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            self.ready(nix::poll::PollFlags::POLLOUT)?;
            match self.stream.write(bytes) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) => {}
                result => return result,
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn absolute_deadline_bounds_a_silent_peer() {
        let (stream, _peer) = tokio::net::UnixStream::pair().unwrap();
        tokio::task::spawn_blocking(move || {
            let mut wire = DeadlineStream::new(stream, Vec::new()).unwrap();
            wire.deadline = Instant::now() + Duration::from_millis(50);
            let error = wire.read(&mut [0]).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn buffered_payload_precedes_eof_without_changing_closed_socket_options() {
        let (stream, peer) = tokio::net::UnixStream::pair().unwrap();
        drop(peer);
        tokio::task::spawn_blocking(move || {
            let mut wire = DeadlineStream::new(stream, b"buffered".to_vec()).unwrap();
            let mut bytes = Vec::new();
            wire.read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes, b"buffered");
        })
        .await
        .unwrap();
    }
}
