//! Single-use local capability handoff. The host must exclude untrusted same-UID
//! processes from the private directory and environment with its OS policy.
use crate::BrokerScope;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, Metadata},
    io::Read,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};

pub const BROKER_HANDOFF_ENV: &str = "CONDUCTOR_BROKER_HANDOFF";
const LIMIT: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BrokerEndpointError {
    #[error("invalid private broker endpoint")]
    Invalid,
    #[error("broker endpoint authentication refused")]
    Refused,
    #[error("broker endpoint unavailable or timed out")]
    Transport,
}

/// Sensitive capability, intentionally not Debug. Never log or persist in audit.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Handoff {
    version: u16,
    path: PathBuf,
    parent_device: u64,
    parent_inode: u64,
    socket_device: u64,
    socket_inode: u64,
    token: String,
    scope: BrokerScope,
}
fn owner() -> u32 {
    nix::unistd::geteuid().as_raw()
}
fn private_directory(path: &Path) -> Result<Metadata, BrokerEndpointError> {
    let meta = fs::symlink_metadata(path).map_err(|_| BrokerEndpointError::Invalid)?;
    if !path.is_absolute()
        || !meta.is_dir()
        || meta.uid() != owner()
        || meta.mode() & 0o077 != 0
        || path
            .canonicalize()
            .map_err(|_| BrokerEndpointError::Invalid)?
            != path
    {
        return Err(BrokerEndpointError::Invalid);
    }
    Ok(meta)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}
fn token(text: &str) -> Result<[u8; 32], BrokerEndpointError> {
    if text.len() != 64
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(BrokerEndpointError::Invalid);
    }
    let mut bytes = [0; 32];
    for (index, value) in bytes.iter_mut().enumerate() {
        *value = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
            .map_err(|_| BrokerEndpointError::Invalid)?;
    }
    Ok(bytes)
}
fn scope_hash(scope: &BrokerScope) -> Result<[u8; 32], BrokerEndpointError> {
    if !scope.valid() {
        return Err(BrokerEndpointError::Invalid);
    }
    Ok(Sha256::digest(serde_json::to_vec(scope).map_err(|_| BrokerEndpointError::Invalid)?).into())
}
fn timeout_valid(wait: Duration) -> Result<(), BrokerEndpointError> {
    if wait.is_zero() || wait > Duration::from_secs(30) {
        Err(BrokerEndpointError::Invalid)
    } else {
        Ok(())
    }
}
impl Handoff {
    fn check_path(&self) -> Result<(), BrokerEndpointError> {
        let parent = self.path.parent().ok_or(BrokerEndpointError::Invalid)?;
        let directory = private_directory(parent)?;
        let socket = fs::symlink_metadata(&self.path).map_err(|_| BrokerEndpointError::Invalid)?;
        if self.version != 1
            || !self.scope.valid()
            || directory.dev() != self.parent_device
            || directory.ino() != self.parent_inode
            || !socket.file_type().is_socket()
            || socket.uid() != owner()
            || socket.mode() & 0o077 != 0
            || socket.dev() != self.socket_device
            || socket.ino() != self.socket_inode
        {
            return Err(BrokerEndpointError::Refused);
        }
        Ok(())
    }
    fn authentication(&self) -> Result<[u8; 65], BrokerEndpointError> {
        let mut frame = [0; 65];
        frame[0] = 1;
        frame[1..33].copy_from_slice(&token(&self.token)?);
        frame[33..].copy_from_slice(&scope_hash(&self.scope)?);
        Ok(frame)
    }
}

/// Owns a one-accept listener and its socket entry. This is a capability, not a
/// process identity or OS sandbox. A wrong/stalled first peer consumes it too.
pub struct BrokerEndpoint {
    listener: UnixListener,
    handoff: Handoff,
}
impl BrokerEndpoint {
    /// Bind in an existing canonical, owned mode0700 directory outside model access.
    /// Other same-UID processes must not be able to mutate its parents/entries or
    /// inspect the handoff environment. Permission bits alone do not enforce that.
    /// # Errors
    /// Invalid directory/scope, unavailable randomness, occupied path or bind failure.
    pub fn bind(directory: &Path, scope: BrokerScope) -> Result<Self, BrokerEndpointError> {
        scope_hash(&scope)?;
        let parent = private_directory(directory)?;
        let path = directory.join(format!("b-{}.sock", scope.invocation_id));
        if path.as_os_str().len() > 100 {
            return Err(BrokerEndpointError::Invalid);
        }
        let mut random = [0; 32];
        File::open("/dev/urandom")
            .and_then(|mut f| f.read_exact(&mut random))
            .map_err(|_| BrokerEndpointError::Transport)?;
        let listener = UnixListener::bind(&path).map_err(|_| BrokerEndpointError::Transport)?;
        // The parent is already private before bind; do not change process umask.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|_| BrokerEndpointError::Transport)?;
        let socket = fs::symlink_metadata(&path).map_err(|_| BrokerEndpointError::Transport)?;
        let endpoint = Self {
            listener,
            handoff: Handoff {
                version: 1,
                path,
                parent_device: parent.dev(),
                parent_inode: parent.ino(),
                socket_device: socket.dev(),
                socket_inode: socket.ino(),
                token: hex(&random),
                scope,
            },
        };
        endpoint.handoff.check_path()?;
        Ok(endpoint)
    }
    /// Export only into the explicitly selected proxy child's private environment.
    /// The returned string is a bearer capability: never put it in argv, logs,
    /// persisted launch/audit records, user-visible configuration or model context.
    /// # Errors
    /// The pinned endpoint changed or the bounded handoff cannot be encoded.
    pub fn environment_value(&self) -> Result<String, BrokerEndpointError> {
        self.handoff.check_path()?;
        let value =
            serde_json::to_string(&self.handoff).map_err(|_| BrokerEndpointError::Invalid)?;
        if value.len() > LIMIT {
            return Err(BrokerEndpointError::Invalid);
        }
        Ok(value)
    }
    /// Consume exactly one connection. Authentication uses a fixed-size frame,
    /// matching scope digest, random capability and kernel peer UID. It does not
    /// attest a peer PID or defend against a hostile same-UID directory owner.
    /// Dropping/aborting this future closes admission and removes only our entry.
    /// # Errors
    /// Wrong identity/token/scope, changed endpoint, timeout or transport failure.
    pub async fn accept(self, wait: Duration) -> Result<UnixStream, BrokerEndpointError> {
        timeout_valid(wait)?;
        self.handoff.check_path()?;
        tokio::time::timeout(wait, async {
            let (mut stream, _) = self
                .listener
                .accept()
                .await
                .map_err(|_| BrokerEndpointError::Transport)?;
            if stream
                .peer_cred()
                .map_err(|_| BrokerEndpointError::Refused)?
                .uid()
                != owner()
            {
                return Err(BrokerEndpointError::Refused);
            }
            let mut frame = [0; 65];
            stream
                .read_exact(&mut frame)
                .await
                .map_err(|_| BrokerEndpointError::Transport)?;
            let expected = self.handoff.authentication()?;
            if frame.iter().zip(expected).fold(0, |d, (a, b)| d | (a ^ b)) != 0 {
                return Err(BrokerEndpointError::Refused);
            }
            self.handoff.check_path()?;
            stream
                .write_all(&[1])
                .await
                .map_err(|_| BrokerEndpointError::Transport)?;
            Ok(stream)
        })
        .await
        .map_err(|_| BrokerEndpointError::Transport)?
    }
}
impl Drop for BrokerEndpoint {
    fn drop(&mut self) {
        // Never remove a replacement entry. The host's same-UID exclusion remains
        // necessary: metadata checks are not an atomic unlink against a path mover.
        if self.handoff.check_path().is_ok() {
            let _ = fs::remove_file(&self.handoff.path);
        }
    }
}

/// Connect once using a bounded host-supplied environment capability. No retries.
/// Validate socket/parent identity and peer UID before disclosing the token.
/// Host OS isolation must exclude same-UID path replacement and environment reads;
/// this helper does not establish those protections or a native writer fence.
/// # Errors
/// Invalid/changed handoff, wrong peer, timeout or failed acknowledgement.
pub async fn connect_broker_endpoint(
    value: &str,
    wait: Duration,
) -> Result<(UnixStream, BrokerScope), BrokerEndpointError> {
    timeout_valid(wait)?;
    if value.len() > LIMIT {
        return Err(BrokerEndpointError::Invalid);
    }
    let handoff: Handoff = serde_json::from_str(value).map_err(|_| BrokerEndpointError::Invalid)?;
    let authentication = handoff.authentication()?;
    handoff.check_path()?;
    tokio::time::timeout(wait, async {
        let mut stream = UnixStream::connect(&handoff.path)
            .await
            .map_err(|_| BrokerEndpointError::Transport)?;
        if stream
            .peer_cred()
            .map_err(|_| BrokerEndpointError::Refused)?
            .uid()
            != owner()
        {
            return Err(BrokerEndpointError::Refused);
        }
        handoff.check_path()?;
        stream
            .write_all(&authentication)
            .await
            .map_err(|_| BrokerEndpointError::Transport)?;
        let mut reply = [0];
        stream
            .read_exact(&mut reply)
            .await
            .map_err(|_| BrokerEndpointError::Transport)?;
        if reply != [1] {
            return Err(BrokerEndpointError::Refused);
        }
        Ok((stream, handoff.scope))
    })
    .await
    .map_err(|_| BrokerEndpointError::Transport)?
}
