//! Same-host account transport. Kernel UID and packet binding authenticate a
//! connection, not a worker PID, execution acknowledgement or write fence.
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountChannelAddress {
    pub path: PathBuf,
    pub worker_uid: u32,
    pub coordinator_uid: u32,
    pub packet_sha256: String,
}

#[derive(Debug, thiserror::Error)]
pub enum AccountChannelError {
    #[error("account channel configuration is invalid")]
    Invalid,
    #[error("account channel peer or packet binding was refused")]
    Refused,
    #[error("account channel transport failed or timed out")]
    Transport,
}

/// One connection only, including a refused or stalled first peer. The operator
/// supplies an existing traversable directory owned by the receiving account.
pub struct AccountChannelListener {
    listener: UnixListener,
    address: AccountChannelAddress,
    socket_identity: (u64, u64),
}

impl AccountChannelListener {
    /// Bind without replacing an existing entry or changing directory permissions.
    /// The socket permits cross-UID connection; kernel credentials gate admission.
    /// Other processes under either selected UID are in that account's trust domain.
    /// # Errors
    /// Refuses noncanonical/unowned/writable-by-others directories and invalid hashes.
    pub fn bind(
        path: &Path,
        coordinator_uid: u32,
        packet_sha256: String,
    ) -> Result<Self, AccountChannelError> {
        let parent = path.parent().ok_or(AccountChannelError::Invalid)?;
        let metadata = fs::metadata(parent).map_err(|_| AccountChannelError::Invalid)?;
        if !path.is_absolute()
            || path.as_os_str().len() > 100
            || path.file_name().is_none()
            || !metadata.is_dir()
            || fs::canonicalize(parent).ok().as_deref() != Some(parent)
            || metadata.uid() != nix::unistd::geteuid().as_raw()
            || metadata.mode() & 0o022 != 0
        {
            return Err(AccountChannelError::Invalid);
        }
        frame(&packet_sha256)?;
        let listener = UnixListener::bind(path).map_err(|_| AccountChannelError::Transport)?;
        let metadata = fs::symlink_metadata(path).map_err(|_| AccountChannelError::Transport)?;
        let endpoint = Self {
            listener,
            address: AccountChannelAddress {
                path: path.into(),
                worker_uid: nix::unistd::geteuid().as_raw(),
                coordinator_uid,
                packet_sha256,
            },
            socket_identity: (metadata.dev(), metadata.ino()),
        };
        fs::set_permissions(path, fs::Permissions::from_mode(0o666))
            .map_err(|_| AccountChannelError::Transport)?;
        Ok(endpoint)
    }

    #[must_use]
    pub fn address(&self) -> &AccountChannelAddress {
        &self.address
    }

    /// Authenticate the peer and exact packet before exposing the byte stream.
    /// No task grant, release or automatic reconnect is implied.
    /// # Errors
    /// Rejects unexpected UID, packet/version mismatch, timeout or disconnection.
    pub async fn accept(self, wait: Duration) -> Result<UnixStream, AccountChannelError> {
        valid_wait(wait)?;
        tokio::time::timeout(wait, async {
            let (mut stream, _) = self
                .listener
                .accept()
                .await
                .map_err(|_| AccountChannelError::Transport)?;
            require_uid(&stream, self.address.coordinator_uid)?;
            let mut incoming = [0; 69];
            stream
                .read_exact(&mut incoming)
                .await
                .map_err(|_| AccountChannelError::Transport)?;
            if incoming != frame(&self.address.packet_sha256)? {
                return Err(AccountChannelError::Refused);
            }
            stream
                .write_all(&[1])
                .await
                .map_err(|_| AccountChannelError::Transport)?;
            Ok(stream)
        })
        .await
        .map_err(|_| AccountChannelError::Transport)?
    }
}

/// Persistent rendezvous for tasks from one explicitly selected coordinator UID.
/// The coordinator selects each packet after durable preparation; unlike the
/// one-shot listener, no packet or receiver tag must be known at startup.
pub struct AccountWorkerListener(AccountChannelListener);

impl AccountWorkerListener {
    /// Bind a worker-owned endpoint with the same directory and peer requirements.
    /// # Errors
    /// Refuses an invalid path or an occupied socket; never changes its parent.
    pub fn bind(path: &Path, coordinator_uid: u32) -> Result<Self, AccountChannelError> {
        AccountChannelListener::bind(path, coordinator_uid, "0".repeat(64)).map(Self)
    }

    /// Admit one packet identity from the authenticated coordinator. The caller
    /// must bind the subsequent request to this digest and enforce ACK/Release.
    /// A failed attempt does not consume the endpoint or grant task authority.
    /// # Errors
    /// Refuses wrong UID, malformed/version-mismatched headers and stalled peers.
    pub async fn accept(
        &mut self,
        wait: Duration,
    ) -> Result<(UnixStream, String), AccountChannelError> {
        valid_wait(wait)?;
        tokio::time::timeout(wait, async {
            let (mut stream, _) = self
                .0
                .listener
                .accept()
                .await
                .map_err(|_| AccountChannelError::Transport)?;
            require_uid(&stream, self.0.address.coordinator_uid)?;
            let mut incoming = [0; 69];
            stream
                .read_exact(&mut incoming)
                .await
                .map_err(|_| AccountChannelError::Transport)?;
            let digest = std::str::from_utf8(&incoming[5..])
                .map_err(|_| AccountChannelError::Refused)?
                .to_owned();
            if frame(&digest).map_err(|_| AccountChannelError::Refused)? != incoming {
                return Err(AccountChannelError::Refused);
            }
            stream
                .write_all(&[1])
                .await
                .map_err(|_| AccountChannelError::Transport)?;
            Ok((stream, digest))
        })
        .await
        .map_err(|_| AccountChannelError::Transport)?
    }
}

impl Drop for AccountChannelListener {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.address.path)
            .is_ok_and(|m| (m.dev(), m.ino()) == self.socket_identity)
        {
            let _ = fs::remove_file(&self.address.path);
        }
    }
}

/// Connect only as the selected coordinator and authenticate the kernel peer
/// before sending packet identity or any caller payload. No bearer secret needed.
/// # Errors
/// Rejects wrong account, malformed address/hash, peer mismatch or timeout.
pub async fn connect_account_channel(
    address: &AccountChannelAddress,
    wait: Duration,
) -> Result<UnixStream, AccountChannelError> {
    valid_wait(wait)?;
    let hello = frame(&address.packet_sha256)?;
    if !address.path.is_absolute() || address.path.as_os_str().len() > 100 {
        return Err(AccountChannelError::Invalid);
    }
    if nix::unistd::geteuid().as_raw() != address.coordinator_uid {
        return Err(AccountChannelError::Refused);
    }
    tokio::time::timeout(wait, async {
        let mut stream = UnixStream::connect(&address.path)
            .await
            .map_err(|_| AccountChannelError::Transport)?;
        require_uid(&stream, address.worker_uid)?;
        stream
            .write_all(&hello)
            .await
            .map_err(|_| AccountChannelError::Transport)?;
        if stream
            .read_u8()
            .await
            .map_err(|_| AccountChannelError::Transport)?
            != 1
        {
            return Err(AccountChannelError::Refused);
        }
        Ok(stream)
    })
    .await
    .map_err(|_| AccountChannelError::Transport)?
}

fn require_uid(stream: &UnixStream, uid: u32) -> Result<(), AccountChannelError> {
    if stream
        .peer_cred()
        .map_err(|_| AccountChannelError::Refused)?
        .uid()
        != uid
    {
        return Err(AccountChannelError::Refused);
    }
    Ok(())
}
fn frame(digest: &str) -> Result<[u8; 69], AccountChannelError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(AccountChannelError::Invalid);
    }
    let mut frame = [0; 69];
    frame[..5].copy_from_slice(b"ACCT1");
    frame[5..].copy_from_slice(digest.as_bytes());
    Ok(frame)
}
fn valid_wait(wait: Duration) -> Result<(), AccountChannelError> {
    if wait.is_zero() || wait > Duration::from_secs(30) {
        return Err(AccountChannelError::Invalid);
    }
    Ok(())
}
