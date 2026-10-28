use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use conductor_protocol::{ErrorBody, ErrorCode, RequestEnvelope, ResponseEnvelope};
use conductor_service::ControlService;
use fs2::FileExt;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::task::JoinSet;
use tracing::{debug, warn};

pub const MAX_FRAME_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("socket I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("another daemon owns the runtime lock: {0}")]
    RuntimeLocked(PathBuf),
    #[error("refusing to replace a non-socket runtime path: {0}")]
    UnsafeSocketPath(PathBuf),
}

/// Process-lifetime ownership for one local daemon socket.
///
/// The lock file remains on disk, but its kernel lock is released on process
/// exit. Holding this guard makes it safe to remove a socket inode left by a
/// crashed prior owner; regular files are never removed.
pub struct RuntimeLock {
    _file: File,
}

impl RuntimeLock {
    /// Acquires exclusive daemon ownership and removes only a stale Unix socket.
    ///
    /// # Errors
    ///
    /// Fails when another process owns the lock, the lock cannot be opened, or
    /// the requested socket path exists but is not a Unix socket.
    pub fn acquire(
        lock_path: impl AsRef<Path>,
        socket_path: impl AsRef<Path>,
    ) -> Result<Self, DaemonError> {
        use std::os::unix::fs::FileTypeExt;

        let lock_path = lock_path.as_ref().to_path_buf();
        let socket_path = socket_path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        file.try_lock_exclusive()
            .map_err(|_| DaemonError::RuntimeLocked(lock_path))?;

        match std::fs::symlink_metadata(&socket_path) {
            Ok(metadata) if metadata.file_type().is_socket() => {
                std::fs::remove_file(&socket_path)?;
            }
            Ok(_) => return Err(DaemonError::UnsafeSocketPath(socket_path)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(DaemonError::Io(error)),
        }
        Ok(Self { _file: file })
    }
}

#[derive(Debug)]
struct SocketGuard {
    path: PathBuf,
}

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != io::ErrorKind::NotFound
        {
            warn!(path = %self.path.display(), %error, "failed to remove owned socket");
        }
    }
}

pub struct LocalServer<S> {
    listener: UnixListener,
    service: Arc<S>,
    guard: SocketGuard,
}

impl<S> LocalServer<S>
where
    S: ControlService,
{
    /// Binds a local Unix socket without deleting an existing path.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the parent is absent, the socket is already in
    /// use, permissions cannot be restricted, or binding otherwise fails.
    pub fn bind(path: impl AsRef<Path>, service: S) -> Result<Self, DaemonError> {
        use std::os::unix::fs::PermissionsExt;

        let path = path.as_ref().to_path_buf();
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;

        Ok(Self {
            listener,
            service: Arc::new(service),
            guard: SocketGuard { path },
        })
    }

    /// Serves requests until shutdown, then cancels accepted read-only requests.
    ///
    /// # Errors
    ///
    /// Returns when accepting a connection fails.
    pub async fn run_until<F>(self, shutdown: F) -> Result<(), DaemonError>
    where
        F: Future<Output = ()>,
    {
        let Self {
            listener,
            service,
            guard: _guard,
        } = self;
        let mut tasks = JoinSet::new();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => break,
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    let service = Arc::clone(&service);
                    tasks.spawn(async move {
                        if let Err(error) = serve_connection(stream, service.as_ref()).await {
                            debug!(%error, "local client connection failed");
                        }
                    });
                }
            }
        }

        drop(listener);
        tasks.shutdown().await;
        Ok(())
    }
}

async fn serve_connection<S>(mut stream: UnixStream, service: &S) -> Result<(), io::Error>
where
    S: ControlService,
{
    let mut input = Vec::new();
    (&mut stream)
        .take(MAX_FRAME_BYTES + 1)
        .read_to_end(&mut input)
        .await?;

    let response = if input.len() as u64 > MAX_FRAME_BYTES {
        ResponseEnvelope::failure(
            None,
            ErrorBody::new(ErrorCode::Validation, "request exceeds frame limit"),
        )
    } else {
        match serde_json::from_slice::<RequestEnvelope>(&input) {
            Ok(request) => match request.validate() {
                Ok(()) => match service.handle(request.payload.clone()).await {
                    Ok(payload) => ResponseEnvelope::success(&request, payload),
                    Err(error) => ResponseEnvelope::failure(Some(&request), error),
                },
                Err(error) => ResponseEnvelope::failure(Some(&request), error.into()),
            },
            Err(error) => ResponseEnvelope::failure(
                None,
                ErrorBody::new(ErrorCode::Validation, format!("invalid request: {error}")),
            ),
        }
    };

    let mut output = serde_json::to_vec(&response).map_err(io::Error::other)?;
    output.push(b'\n');
    stream.write_all(&output).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agent_conductor_store::Store;
    use conductor_protocol::{HealthStatus, Request, Response, ResponseResult};
    use conductor_service::{InMemoryControlService, PersistentControlService};
    use tokio::sync::oneshot;
    use uuid::Uuid;

    use super::*;

    fn socket_path() -> PathBuf {
        let directory = PathBuf::from("state");
        std::fs::create_dir_all(&directory).expect("workspace state directory exists");
        let id = Uuid::now_v7().simple().to_string();
        directory.join(format!("acd-{}.sock", &id[id.len() - 8..]))
    }

    async fn exchange(path: &Path, bytes: &[u8]) -> ResponseEnvelope {
        let mut stream = UnixStream::connect(path)
            .await
            .expect("daemon accepts client");
        stream.write_all(bytes).await.expect("request writes");
        stream.shutdown().await.expect("request half-closes");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("response reads");
        serde_json::from_slice(&response).expect("response is structured JSON")
    }

    #[tokio::test]
    async fn serves_health_and_removes_socket_after_shutdown() {
        let path = socket_path();
        let server =
            LocalServer::bind(&path, InMemoryControlService::new(3)).expect("socket binds");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.run_until(async move {
            let _ = shutdown_rx.await;
        }));

        let request = RequestEnvelope::new(Request::Health);
        let response = exchange(
            &path,
            &serde_json::to_vec(&request).expect("request serializes"),
        )
        .await;

        let ResponseResult::Ok(Response::Health(health)) = response.result else {
            panic!("unexpected response");
        };
        assert_eq!(health.status, HealthStatus::Ready);
        assert_eq!(health.boot_generation, 3);

        shutdown_tx.send(()).expect("shutdown sends");
        task.await
            .expect("daemon task joins")
            .expect("daemon exits cleanly");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn malformed_input_returns_a_structured_error() {
        let path = socket_path();
        let server =
            LocalServer::bind(&path, InMemoryControlService::new(1)).expect("socket binds");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.run_until(async move {
            let _ = shutdown_rx.await;
        }));

        let response = exchange(&path, b"not-json").await;
        let ResponseResult::Error(error) = response.result else {
            panic!("malformed request unexpectedly succeeded");
        };
        assert_eq!(error.code, ErrorCode::Validation);
        assert!(response.request_message_id.is_none());

        shutdown_tx.send(()).expect("shutdown sends");
        task.await
            .expect("daemon task joins")
            .expect("daemon exits cleanly");
    }

    #[tokio::test]
    async fn existing_socket_path_is_never_deleted() {
        let path = socket_path();
        let first =
            LocalServer::bind(&path, InMemoryControlService::new(1)).expect("first socket binds");

        let second = LocalServer::bind(&path, InMemoryControlService::new(2));
        assert!(second.is_err());
        assert!(path.exists());

        drop(first);
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn stalled_client_does_not_block_shutdown() {
        let path = socket_path();
        let server =
            LocalServer::bind(&path, InMemoryControlService::new(1)).expect("socket binds");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.run_until(async move {
            let _ = shutdown_rx.await;
        }));
        let _stalled_client = UnixStream::connect(&path).await.expect("client connects");

        shutdown_tx.send(()).expect("shutdown sends");
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("shutdown is bounded")
            .expect("daemon task joins")
            .expect("daemon exits cleanly");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn persistent_health_advances_across_daemon_restarts() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = directory.path().join("conductor.sqlite3");
        let path = directory.path().join("conductor.sock");

        for expected_generation in 1_u64..=2 {
            let store = Store::open(&database).await.expect("store opens");
            let started_at_ms = i64::try_from(expected_generation).expect("small generation") * 100;
            let service = PersistentControlService::start(store, started_at_ms)
                .await
                .expect("runtime starts");
            let server = LocalServer::bind(&path, service).expect("socket binds");
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let task = tokio::spawn(server.run_until(async move {
                let _ = shutdown_rx.await;
            }));

            let request = RequestEnvelope::new(Request::Health);
            let response = exchange(
                &path,
                &serde_json::to_vec(&request).expect("request serializes"),
            )
            .await;
            let ResponseResult::Ok(Response::Health(health)) = response.result else {
                panic!("unexpected response");
            };
            assert_eq!(health.status, HealthStatus::Ready);
            assert_eq!(health.boot_generation, expected_generation);

            shutdown_tx.send(()).expect("shutdown sends");
            task.await
                .expect("daemon task joins")
                .expect("daemon exits cleanly");
            assert!(!path.exists());
        }
    }

    #[test]
    fn runtime_lock_fences_second_owner_and_recovers_a_stale_socket() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let lock_path = directory.path().join("conductor.lock");
        let socket_path = directory.path().join("conductor.sock");
        let stale = std::os::unix::net::UnixListener::bind(&socket_path)
            .expect("stale socket inode creates");
        drop(stale);
        assert!(socket_path.exists());

        let first = RuntimeLock::acquire(&lock_path, &socket_path).expect("first owner locks");
        assert!(!socket_path.exists());
        let second = RuntimeLock::acquire(&lock_path, &socket_path);
        assert!(matches!(second, Err(DaemonError::RuntimeLocked(_))));
        drop(first);
        RuntimeLock::acquire(&lock_path, &socket_path).expect("lock releases on drop");
    }

    #[test]
    fn runtime_lock_never_removes_a_non_socket_path() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let lock_path = directory.path().join("conductor.lock");
        let socket_path = directory.path().join("conductor.sock");
        std::fs::write(&socket_path, b"operator data").expect("regular file creates");

        let result = RuntimeLock::acquire(&lock_path, &socket_path);
        assert!(matches!(result, Err(DaemonError::UnsafeSocketPath(_))));
        assert_eq!(
            std::fs::read(&socket_path).expect("regular file remains"),
            b"operator data"
        );
    }
}
