use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use conductor_protocol::{ErrorBody, ErrorCode, RequestEnvelope, ResponseEnvelope};
use conductor_service::ControlService;
use fs2::FileExt;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, warn};

pub const MAX_FRAME_BYTES: u64 = 1024 * 1024;
const MAX_CONCURRENT_CONNECTIONS: usize = 64;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

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
    permits: Arc<Semaphore>,
    read_timeout: Duration,
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
        Self::bind_with_config(
            path,
            service,
            MAX_CONCURRENT_CONNECTIONS,
            REQUEST_READ_TIMEOUT,
        )
    }

    fn bind_with_config(
        path: impl AsRef<Path>,
        service: S,
        max_connections: usize,
        read_timeout: Duration,
    ) -> Result<Self, DaemonError> {
        use std::os::unix::fs::PermissionsExt;

        let path = path.as_ref().to_path_buf();
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;

        Ok(Self {
            listener,
            service: Arc::new(service),
            guard: SocketGuard { path },
            permits: Arc::new(Semaphore::new(max_connections)),
            read_timeout,
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
            permits,
            read_timeout,
        } = self;
        let mut tasks = JoinSet::new();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => break,
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    match Arc::clone(&permits).try_acquire_owned() {
                        Ok(permit) => {
                            let service = Arc::clone(&service);
                            tasks.spawn(async move {
                                let _permit = permit;
                                if let Err(error) = serve_connection(stream, service.as_ref(), read_timeout).await {
                                    debug!(%error, "local client connection failed");
                                }
                            });
                        }
                        Err(_) => {
                            reject_overloaded(&stream);
                        }
                    }
                }
            }
        }

        drop(listener);
        tasks.shutdown().await;
        Ok(())
    }
}

async fn serve_connection<S>(
    mut stream: UnixStream,
    service: &S,
    read_timeout: Duration,
) -> Result<(), io::Error>
where
    S: ControlService,
{
    let mut input = Vec::new();
    match tokio::time::timeout(
        read_timeout,
        (&mut stream)
            .take(MAX_FRAME_BYTES + 1)
            .read_to_end(&mut input),
    )
    .await
    {
        Ok(result) => {
            result?;
        }
        Err(_) => {
            return write_response(
                &mut stream,
                &ResponseEnvelope::failure(
                    None,
                    ErrorBody::new(ErrorCode::Unavailable, "request read timed out"),
                ),
            )
            .await;
        }
    }

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

    write_response(&mut stream, &response).await
}

async fn write_response(
    stream: &mut UnixStream,
    response: &ResponseEnvelope,
) -> Result<(), io::Error> {
    let mut output = serde_json::to_vec(response).map_err(io::Error::other)?;
    output.push(b'\n');
    stream.write_all(&output).await?;
    stream.shutdown().await
}

fn reject_overloaded(stream: &UnixStream) {
    let response = ResponseEnvelope::failure(
        None,
        ErrorBody::new(ErrorCode::Unavailable, "connection limit reached"),
    );
    if let Ok(mut output) = serde_json::to_vec(&response) {
        output.push(b'\n');
        let _ = stream.try_write(&output);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use agent_conductor_domain::{
        Actor, ActorId, ActorRole, AttemptId, Command, CommandKind, CorrelationId, IdempotencyKey,
        LeaseStatus,
    };
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
    async fn stalled_and_excess_clients_are_bounded_and_recovery_accepts_health() {
        let path = socket_path();
        let server = LocalServer::bind_with_config(
            &path,
            InMemoryControlService::new(1),
            2,
            Duration::from_millis(50),
        )
        .expect("socket binds");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.run_until(async move {
            let _ = shutdown_rx.await;
        }));
        let mut first = UnixStream::connect(&path)
            .await
            .expect("first stalled client");
        let mut second = UnixStream::connect(&path)
            .await
            .expect("second stalled client");

        let request = RequestEnvelope::new(Request::Health);
        let body = serde_json::to_vec(&request).expect("request serializes");
        let mut excess = UnixStream::connect(&path)
            .await
            .expect("excess client connects");
        excess.write_all(&body).await.expect("request writes");
        excess.shutdown().await.expect("request half-closes");
        let mut excess_response = Vec::new();
        tokio::time::timeout(
            Duration::from_secs(1),
            excess.read_to_end(&mut excess_response),
        )
        .await
        .expect("overload response is bounded")
        .expect("overload response reads");
        let overload: ResponseEnvelope =
            serde_json::from_slice(&excess_response).expect("structured overload response");
        let ResponseResult::Error(error) = overload.result else {
            panic!("overloaded request unexpectedly succeeded")
        };
        assert_eq!(error.code, ErrorCode::Unavailable);

        for stalled in [&mut first, &mut second] {
            let mut response = Vec::new();
            tokio::time::timeout(Duration::from_secs(1), stalled.read_to_end(&mut response))
                .await
                .expect("timeout response is bounded")
                .expect("timeout response reads");
            let response: ResponseEnvelope =
                serde_json::from_slice(&response).expect("structured timeout response");
            let ResponseResult::Error(error) = response.result else {
                panic!("stalled request unexpectedly succeeded")
            };
            assert_eq!(error.code, ErrorCode::Unavailable);
        }

        let healthy = RequestEnvelope::new(Request::Health);
        let response = exchange(
            &path,
            &serde_json::to_vec(&healthy).expect("request serializes"),
        )
        .await;
        assert!(matches!(
            response.result,
            ResponseResult::Ok(Response::Health(_))
        ));

        shutdown_tx.send(()).expect("shutdown sends");
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("shutdown remains bounded")
            .expect("daemon joins")
            .expect("daemon exits cleanly");
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

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn persistent_mutations_replay_and_survive_daemon_restart() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = directory.path().join("conductor.sqlite3");
        let path = directory.path().join("conductor.sock");
        let project = Request::RegisterProject {
            actor_id: "operator".into(),
            idempotency_key: "project-1".into(),
            id: "project-1".into(),
            canonical_path: "/workspace/project".into(),
            filesystem_identity: "dev:1:ino:2".into(),
            git_common_dir_identity: None,
        };
        let agent = Request::RegisterAgent {
            actor_id: "operator".into(),
            idempotency_key: "agent-1".into(),
            id: "worker-1".into(),
            adapter_kind: "codex_cli".into(),
            protocol_fingerprint: "codex-v1".into(),
            capability_hash: "sha256:abc".into(),
            access_mode: "read_write".into(),
        };
        let create = Request::CreateTask {
            actor_id: "operator".into(),
            idempotency_key: "task-1".into(),
            id: "task-1".into(),
            project_id: "project-1".into(),
            objective: "persist across restart".into(),
            priority: 1,
            authority: serde_json::json!({}),
            required_checks: vec![],
        };
        let create_two = Request::CreateTask {
            actor_id: "operator".into(),
            idempotency_key: "task-2".into(),
            id: "task-2".into(),
            project_id: "project-1".into(),
            objective: "second task".into(),
            priority: 0,
            authority: serde_json::json!({}),
            required_checks: vec![],
        };

        let store = Store::open(&database).await.expect("store opens");
        let service = PersistentControlService::start(store, 100)
            .await
            .expect("runtime starts");
        let server = LocalServer::bind(&path, service).expect("socket binds");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.run_until(async move {
            let _ = shutdown_rx.await;
        }));
        for request in [project, agent.clone(), agent, create, create_two] {
            let envelope = RequestEnvelope::new(request);
            let response =
                exchange(&path, &serde_json::to_vec(&envelope).expect("serialize")).await;
            assert!(matches!(response.result, ResponseResult::Ok(_)));
        }
        let ready = Command {
            actor: Actor {
                id: ActorId("operator".into()),
                role: ActorRole::Operator,
            },
            expected_version: 0,
            correlation_id: CorrelationId("ready-correlation".into()),
            idempotency_key: IdempotencyKey("ready-1".into()),
            reason: "ready for worker".into(),
            kind: CommandKind::MarkReady,
        };
        let envelope = RequestEnvelope::new(Request::ApplyCommand {
            task_id: "task-1".into(),
            command: ready.clone(),
        });
        assert!(matches!(
            exchange(&path, &serde_json::to_vec(&envelope).expect("serialize"))
                .await
                .result,
            ResponseResult::Ok(_)
        ));
        let envelope = RequestEnvelope::new(Request::ApplyCommand {
            task_id: "task-1".into(),
            command: ready.clone(),
        });
        let replay = exchange(&path, &serde_json::to_vec(&envelope).expect("serialize")).await;
        let ResponseResult::Ok(Response::CommandApplied(replay)) = replay.result else {
            panic!("lifecycle command did not replay")
        };
        assert!(replay.replayed);
        let envelope = RequestEnvelope::new(Request::ApplyCommand {
            task_id: "task-2".into(),
            command: ready,
        });
        let cross_task = exchange(&path, &serde_json::to_vec(&envelope).expect("serialize")).await;
        let ResponseResult::Error(error) = cross_task.result else {
            panic!("cross-task idempotency replay succeeded")
        };
        assert_eq!(error.code, ErrorCode::Conflict);
        let ready_two = Command {
            actor: Actor {
                id: ActorId("operator".into()),
                role: ActorRole::Operator,
            },
            expected_version: 0,
            correlation_id: CorrelationId("ready-two".into()),
            idempotency_key: IdempotencyKey("ready-2".into()),
            reason: "ready second task".into(),
            kind: CommandKind::MarkReady,
        };
        let envelope = RequestEnvelope::new(Request::ApplyCommand {
            task_id: "task-2".into(),
            command: ready_two,
        });
        assert!(matches!(
            exchange(&path, &serde_json::to_vec(&envelope).expect("serialize"))
                .await
                .result,
            ResponseResult::Ok(_)
        ));
        let unknown = Command {
            actor: Actor {
                id: ActorId("operator".into()),
                role: ActorRole::Operator,
            },
            expected_version: 1,
            correlation_id: CorrelationId("unknown-worker".into()),
            idempotency_key: IdempotencyKey("unknown-worker".into()),
            reason: "unknown assignment".into(),
            kind: CommandKind::Claim {
                attempt_id: AttemptId("attempt-unknown".into()),
                worker: ActorId("missing-worker".into()),
                lease_duration_ms: 10_000,
                worker_margin_ms: 1_000,
                recovery_grace_ms: 2_000,
            },
        };
        let envelope = RequestEnvelope::new(Request::ApplyCommand {
            task_id: "task-2".into(),
            command: unknown,
        });
        let response = exchange(&path, &serde_json::to_vec(&envelope).expect("serialize")).await;
        let ResponseResult::Error(error) = response.result else {
            panic!("unknown worker was accepted")
        };
        assert_eq!(error.code, ErrorCode::Validation);
        let claim = Command {
            actor: Actor {
                id: ActorId("operator".into()),
                role: ActorRole::Operator,
            },
            expected_version: 1,
            correlation_id: CorrelationId("claim-correlation".into()),
            idempotency_key: IdempotencyKey("claim-1".into()),
            reason: "assign worker".into(),
            kind: CommandKind::Claim {
                attempt_id: AttemptId("attempt-1".into()),
                worker: ActorId("worker-1".into()),
                lease_duration_ms: 10_000,
                worker_margin_ms: 1_000,
                recovery_grace_ms: 2_000,
            },
        };
        let envelope = RequestEnvelope::new(Request::ApplyCommand {
            task_id: "task-1".into(),
            command: claim,
        });
        let mut disconnected = UnixStream::connect(&path).await.expect("connect");
        disconnected
            .write_all(&serde_json::to_vec(&envelope).expect("serialize"))
            .await
            .expect("write");
        disconnected.shutdown().await.expect("half close");
        drop(disconnected);
        let observer = Store::open(&database).await.expect("observer opens");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effect_intents WHERE effect_kind = 'worker.spawn' AND status = 'pending'")
                    .fetch_one(observer.readers()).await.expect("effect count");
                if count == 1 { break; }
                tokio::task::yield_now().await;
            }
        }).await.expect("committed effect becomes visible");
        shutdown_tx.send(()).expect("shutdown");
        task.await.expect("join").expect("clean exit");

        let store = Store::open(&database).await.expect("store reopens");
        let effects: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effect_intents WHERE effect_kind = 'worker.spawn' AND status = 'pending'").fetch_one(store.readers()).await.expect("effect count");
        assert_eq!(effects, 1, "committed effect survives disconnect");
        let service = PersistentControlService::start(store, 200)
            .await
            .expect("runtime restarts");
        let server = LocalServer::bind(&path, service).expect("socket binds");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.run_until(async move {
            let _ = shutdown_rx.await;
        }));
        let envelope = RequestEnvelope::new(Request::GetTask {
            task_id: "task-1".into(),
        });
        let response = exchange(&path, &serde_json::to_vec(&envelope).expect("serialize")).await;
        let ResponseResult::Ok(Response::Task(task_response)) = response.result else {
            panic!("task did not survive restart")
        };
        assert_eq!(task_response.aggregate.id.0, "task-1");
        assert_eq!(
            task_response
                .aggregate
                .attempt
                .as_ref()
                .expect("attempt")
                .lease
                .status,
            LeaseStatus::RecoveryPending
        );
        let stale = Command {
            actor: Actor {
                id: ActorId("worker-1".into()),
                role: ActorRole::Worker,
            },
            expected_version: task_response.aggregate.version,
            correlation_id: CorrelationId("stale-correlation".into()),
            idempotency_key: IdempotencyKey("stale-start".into()),
            reason: "stale worker start".into(),
            kind: CommandKind::Start {
                attempt_id: AttemptId("attempt-1".into()),
                lease_epoch: 0,
            },
        };
        let envelope = RequestEnvelope::new(Request::ApplyCommand {
            task_id: "task-1".into(),
            command: stale,
        });
        let response = exchange(&path, &serde_json::to_vec(&envelope).expect("serialize")).await;
        let ResponseResult::Error(error) = response.result else {
            panic!("stale epoch was accepted")
        };
        assert_eq!(error.code, ErrorCode::Conflict);
        shutdown_tx.send(()).expect("shutdown");
        task.await.expect("join").expect("clean exit");
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
