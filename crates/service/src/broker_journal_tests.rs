use super::*;
use agent_conductor_store::{FileBrokerBinding, FileBrokerStatus};
use conductor_runtime::{
    BrokerFilesystem, BrokerOperation, BrokerRequest, BrokerScope, continuous_millis,
};
use conductor_runtime::{BrokerJournal, RemoteBrokerJournal};
use sqlx::Connection;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

async fn fixture() -> (
    tempfile::TempDir,
    Store,
    FileBrokerBinding,
    sqlx::SqliteConnection,
) {
    let (root, store) = lifecycle_store().await;
    let service =
        PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
            .await
            .unwrap();
    let (_, token) = claim_task(&service).await;
    let binding = FileBrokerBinding {
        authority_id: "a".repeat(64),
        boot_generation: i64::try_from(service.boot_generation).unwrap(),
        invocation_id: Uuid::now_v7().to_string(),
        execution_token: token.clone(),
        task_id: "task".into(),
        attempt_id: "attempt".into(),
        lease_epoch: 1,
        root_identity_sha256: "b".repeat(64),
    };
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        root.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    sqlx::query("UPDATE recovery_authority SET authority_id=?")
        .bind(&binding.authority_id)
        .execute(&mut db)
        .await
        .unwrap();
    // Synthetic acknowledged execution; this tests store routing, not native launch.
    sqlx::query("INSERT INTO executions(effect_token,task_id,attempt_id,lease_epoch,runner_id,packet_json,packet_sha256,state,wrapper_identity,expected_wrapper_identity,created_at_ms,updated_at_ms) VALUES(?,'task','attempt',1,'runner','{}',?,'started',?,?,100,100)")
        .bind(token).bind("c".repeat(64)).bind(&binding.invocation_id).bind(&binding.invocation_id).execute(&mut db).await.unwrap();
    (root, store, binding, db)
}
fn request(binding: &FileBrokerBinding) -> BrokerRequest {
    BrokerRequest {
        version: 1,
        sequence: 1,
        scope: BrokerScope {
            authority_id: binding.authority_id.clone(),
            boot_generation: binding.boot_generation.try_into().unwrap(),
            invocation_id: binding.invocation_id.clone(),
            task_id: binding.task_id.clone(),
            attempt_id: binding.attempt_id.clone(),
            lease_epoch: 1,
        },
        operation: BrokerOperation::WriteText {
            path: "answer".into(),
            contents: "durable answer\n".into(),
            expected_sha256: None,
        },
    }
}
async fn assert_progress(
    store: &Store,
    binding: &FileBrokerBinding,
    admitted: i64,
    completed: i64,
) {
    assert_eq!(
        store.file_broker_status(binding).await.unwrap(),
        FileBrokerStatus {
            closed: false,
            admitted,
            completed
        }
    );
}
async fn round_trip(failure: Option<&str>, repeat_request: bool) {
    let (root, store, binding, mut db) = fixture().await;
    let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
        .await
        .unwrap();
    assert!(
        RegisteredBrokerJournal::register(store.clone(), binding.clone())
            .await
            .is_err()
    );
    if let Some(kind) = failure {
        sqlx::query(&format!("CREATE TRIGGER fail_journal BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.{kind}' BEGIN SELECT RAISE(FAIL,'fixture'); END")).execute(&mut db).await.unwrap();
    }
    let files = root.path().join("files");
    fs::create_dir(&files).unwrap();
    let req = request(&binding);
    let broker = BrokerFilesystem::new(
        &files.canonicalize().unwrap(),
        req.scope.clone(),
        continuous_millis().unwrap() + 60000,
    )
    .unwrap();
    let (host, peer) = tokio::net::UnixStream::pair().unwrap();
    let (_cancel, rx) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(journal.serve(
        host,
        broker,
        rx,
        Duration::from_secs(5),
        Duration::from_secs(5),
    ));
    let mut peer = BufReader::new(peer);
    let mut bytes = serde_json::to_vec(&req).unwrap();
    bytes.push(b'\n');
    peer.write_all(&bytes).await.unwrap();
    let mut reply = String::new();
    tokio::time::timeout(Duration::from_secs(10), peer.read_line(&mut reply))
        .await
        .unwrap()
        .unwrap();
    let admitted = i64::from(failure != Some("admitted"));
    let completed = i64::from(failure.is_none());
    let reopened = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    assert_progress(&reopened, &binding, admitted, completed).await;
    assert_eq!(files.join("answer").exists(), admitted == 1);
    if admitted == 1 {
        assert_eq!(
            fs::read_to_string(files.join("answer")).unwrap(),
            "durable answer\n"
        );
    }
    if failure.is_none() {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&reply).unwrap()["sequence"],
            1
        );
        if repeat_request {
            // Exact replay is refused with no second admission or reply.
            peer.write_all(&bytes).await.unwrap();
            reply.clear();
            peer.read_line(&mut reply).await.unwrap();
            assert!(reply.is_empty());
        }
    } else {
        assert!(reply.is_empty());
    }
    drop(peer);
    let end = worker.await.unwrap().unwrap();
    assert_eq!(end.completed_sequence, failure.is_none().then_some(1));
    assert_progress(&reopened, &binding, admitted, completed).await;
    let results: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT request_sha256,result_sha256 FROM file_operations")
            .fetch_all(&mut db)
            .await
            .unwrap();
    if admitted == 1 {
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, hash(&serde_json::to_string(&req).unwrap()));
        assert_eq!(
            results[0].1,
            (completed == 1).then(|| hash("durable answer\n"))
        );
    } else {
        assert!(results.is_empty());
    }
}
#[tokio::test]
async fn journal_success_is_durable_before_reply_and_replay_is_refused() {
    round_trip(None, true).await;
}
#[tokio::test]
async fn journal_admission_failure_prevents_file_effect() {
    round_trip(Some("admitted"), false).await;
}
#[tokio::test]
async fn journal_completion_failure_retains_file_and_pending_intent() {
    round_trip(Some("completed"), false).await;
}

#[tokio::test]
async fn journal_drop_retains_registration_and_refuses_reconstruction() {
    let (root, store, binding, _db) = fixture().await;
    let journal = RegisteredBrokerJournal::register(store, binding.clone())
        .await
        .unwrap();
    drop(journal);
    let reopened = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    assert_eq!(
        reopened.file_broker_status(&binding).await.unwrap(),
        FileBrokerStatus {
            closed: false,
            admitted: 0,
            completed: 0
        }
    );
    assert!(
        RegisteredBrokerJournal::register(reopened, binding)
            .await
            .is_err()
    );
}

async fn refused_binding(stale: bool) {
    let (root, store, binding, mut db) = fixture().await;
    let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
        .await
        .unwrap();
    let mut req = request(&binding);
    if stale {
        sqlx::query("UPDATE runtime_metadata SET boot_generation=boot_generation+1")
            .execute(&mut db)
            .await
            .unwrap();
    } else {
        req.scope.lease_epoch += 1;
    }
    let files = root.path().join("files");
    fs::create_dir(&files).unwrap();
    // A matching in-memory gate cannot bypass the journal's registered scope.
    let broker = BrokerFilesystem::new(
        &files.canonicalize().unwrap(),
        req.scope.clone(),
        continuous_millis().unwrap() + 60000,
    )
    .unwrap();
    let (host, mut peer) = tokio::net::UnixStream::pair().unwrap();
    let (_cancel, rx) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(journal.serve(
        host,
        broker,
        rx,
        Duration::from_secs(5),
        Duration::from_secs(5),
    ));
    let mut bytes = serde_json::to_vec(&req).unwrap();
    bytes.push(b'\n');
    peer.write_all(&bytes).await.unwrap();
    assert!(worker.await.unwrap().unwrap().completed_sequence.is_none());
    assert!(!files.join("answer").exists());
    assert_eq!(
        store.file_broker_status(&binding).await.unwrap(),
        FileBrokerStatus {
            closed: false,
            admitted: 0,
            completed: 0
        }
    );
}
#[tokio::test]
async fn journal_scope_mismatch_refuses_matching_in_memory_gate() {
    refused_binding(false).await;
}
#[tokio::test]
async fn journal_old_generation_refuses_new_operation() {
    refused_binding(true).await;
}

#[tokio::test]
async fn journal_clean_eof_keeps_durable_ownership_open() {
    round_trip(None, false).await;
}

#[test]
fn journal_rpc_child_process() {
    let Some(config) = std::env::var_os("CONDUCTOR_JOURNAL_TEST_CHILD") else {
        return;
    };
    let (binding, files, socket, failure): (FileBrokerBinding, PathBuf, PathBuf, Option<String>) =
        serde_json::from_slice(&fs::read(config).unwrap()).unwrap();
    let req = request(&binding);
    let broker = BrokerFilesystem::new(
        &files,
        req.scope.clone(),
        continuous_millis().unwrap() + 60000,
    )
    .unwrap();
    let stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    let journal =
        RemoteBrokerJournal::new(stream, req.scope.clone(), Duration::from_secs(5)).unwrap();
    let outcome =
        broker.execute_recorded(&req, |i| journal.admit(i), |i, r| journal.complete(i, r));
    assert_eq!(outcome.is_ok(), failure.is_none());
    assert_eq!(broker.revoke().is_ok(), failure.is_none());
    assert!(
        broker
            .execute_recorded(&req, |i| journal.admit(i), |i, r| journal.complete(i, r))
            .is_err()
    );
}

async fn remote_child(failure: Option<&str>) {
    let (root, store, binding, mut db) = fixture().await;
    let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
        .await
        .unwrap();
    if let Some(kind) = failure {
        sqlx::query(&format!("CREATE TRIGGER refuse_rpc BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.{kind}' BEGIN SELECT RAISE(FAIL,'fixture'); END")).execute(&mut db).await.unwrap();
    }
    let files = root.path().join("files");
    fs::create_dir(&files).unwrap();
    let files = files.canonicalize().unwrap();
    let socket = root.path().join("journal.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let config = root.path().join("child.json");
    fs::write(
        &config,
        serde_json::to_vec(&(&binding, &files, &socket, failure)).unwrap(),
    )
    .unwrap();
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "effect_tests::broker_journal_tests::journal_rpc_child_process",
            "--nocapture",
        ])
        .env("CONDUCTOR_JOURNAL_TEST_CHILD", &config)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let (_cancel, rx) = tokio::sync::watch::channel(false);
    let result = journal
        .serve_remote(stream, rx, Duration::from_secs(5))
        .await;
    assert_eq!(result.is_ok(), failure.is_none());
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let reopened = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    let admitted = i64::from(failure != Some("admitted"));
    assert_progress(&reopened, &binding, admitted, i64::from(failure.is_none())).await;
    assert_eq!(files.join("answer").exists(), admitted == 1);
    if admitted == 1 {
        assert_eq!(
            fs::read_to_string(files.join("answer")).unwrap(),
            "durable answer\n"
        );
    }
    assert!(
        RegisteredBrokerJournal::register(reopened, binding)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn journal_rpc_child_writes_only_after_durable_parent_admission() {
    remote_child(None).await;
}
#[tokio::test]
async fn journal_rpc_child_admission_failure_prevents_file_write() {
    remote_child(Some("admitted")).await;
}
#[tokio::test]
async fn journal_rpc_child_completion_failure_retains_unknown_file_write() {
    remote_child(Some("completed")).await;
}

fn wire_request(binding: &FileBrokerBinding) -> conductor_runtime::BrokerJournalRequest {
    conductor_runtime::BrokerJournalRequest {
        version: 1,
        scope: request(binding).scope,
        sequence: 1,
        request_sha256: "d".repeat(64),
        result_sha256: None,
    }
}
#[tokio::test]
async fn journal_rpc_refuses_scope_phase_unknown_fields_and_oversize() {
    for case in ["scope", "phase", "unknown", "oversize"] {
        let (_root, store, binding, _db) = fixture().await;
        let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
            .await
            .unwrap();
        let mut request = serde_json::to_value(wire_request(&binding)).unwrap();
        match case {
            "scope" => request["scope"]["lease_epoch"] = serde_json::json!(2),
            "phase" => request["result_sha256"] = serde_json::json!("e".repeat(64)),
            "unknown" => request["extra"] = serde_json::json!(true),
            _ => request["request_sha256"] = serde_json::json!("x".repeat(4096)),
        }
        let (host, mut peer) = tokio::net::UnixStream::pair().unwrap();
        let (_cancel, rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(journal.serve_remote(host, rx, Duration::from_secs(2)));
        let mut bytes = serde_json::to_vec(&request).unwrap();
        bytes.push(b'\n');
        peer.write_all(&bytes).await.unwrap();
        assert!(server.await.unwrap().is_err(), "{case}");
        assert_progress(&store, &binding, 0, 0).await;
    }
}
#[tokio::test]
async fn journal_rpc_replay_and_cancel_after_admission_preserve_pending_intent() {
    for duplicate in [false, true] {
        let (root, store, binding, _db) = fixture().await;
        let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
            .await
            .unwrap();
        let request = wire_request(&binding);
        let (host, peer) = tokio::net::UnixStream::pair().unwrap();
        let (cancel, rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(journal.serve_remote(host, rx, Duration::from_secs(2)));
        let mut peer = BufReader::new(peer);
        let mut bytes = serde_json::to_vec(&request).unwrap();
        bytes.push(b'\n');
        peer.write_all(&bytes).await.unwrap();
        let mut ack = String::new();
        peer.read_line(&mut ack).await.unwrap();
        let ack: conductor_runtime::BrokerJournalAck = serde_json::from_str(&ack).unwrap();
        assert_eq!(ack.journal_sha256, request.sha256().unwrap());
        if duplicate {
            peer.write_all(&bytes).await.unwrap();
        } else {
            cancel.send(true).unwrap();
        }
        assert!(server.await.unwrap().is_err());
        let reopened = Store::open(root.path().join("state.sqlite3"))
            .await
            .unwrap();
        assert_progress(&reopened, &binding, 1, 0).await;
        assert!(
            RegisteredBrokerJournal::register(reopened, binding)
                .await
                .is_err()
        );
    }
}

async fn runner_frame(
    output: &mut BufReader<tokio::process::ChildStdout>,
) -> conductor_runtime::BrokerRunnerEvent {
    let mut bytes = String::new();
    tokio::time::timeout(Duration::from_secs(10), output.read_line(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    serde_json::from_str(&bytes).unwrap()
}
async fn runner_send(input: &mut tokio::process::ChildStdin, value: &impl serde::Serialize) {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    input.write_all(&bytes).await.unwrap();
}
async fn runner_setup(mismatch: bool, heartbeat: u64) -> BrokerSetup {
    use conductor_runtime::{BrokerEndpoint, BrokerRunnerLaunch, broker_root_identity};
    use std::os::unix::fs::PermissionsExt;
    let (root, store, mut binding, _db) = fixture().await;
    let base = root.path().canonicalize().unwrap();
    let files = base.join("files");
    fs::create_dir(&files).unwrap();
    let journal_dir = base.join("j");
    let proxy_dir = base.join("p");
    let receipt_root = base.join("r");
    for dir in [&journal_dir, &proxy_dir, &receipt_root] {
        fs::create_dir(dir).unwrap();
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).unwrap();
    }
    binding.root_identity_sha256 = broker_root_identity(&files).unwrap();
    let scope = request(&binding).scope;
    let endpoint = BrokerEndpoint::bind(&journal_dir, scope.clone()).unwrap();
    let launch = BrokerRunnerLaunch {
        scope: scope.clone(),
        root: files.clone(),
        root_identity_sha256: if mismatch {
            "0".repeat(64)
        } else {
            binding.root_identity_sha256.clone()
        },
        endpoint_directory: proxy_dir,
        receipt_root,
        journal_handoff: endpoint.environment_value().unwrap(),
        maximum_deadline_ms: continuous_millis().unwrap() + 60000,
        heartbeat_timeout_ms: heartbeat,
    };
    let journal = RegisteredBrokerJournal::register(store.clone(), binding.clone())
        .await
        .unwrap();
    let (hold, rx) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(async move {
        let stream = endpoint.accept(Duration::from_secs(5)).await.unwrap();
        journal
            .serve_remote(stream, rx, Duration::from_secs(5))
            .await
    });
    BrokerSetup {
        root,
        store,
        binding,
        files,
        launch,
        server,
        hold,
    }
}
struct BrokerSetup {
    root: tempfile::TempDir,
    store: Store,
    binding: FileBrokerBinding,
    files: PathBuf,
    launch: conductor_runtime::BrokerRunnerLaunch,
    server: tokio::task::JoinHandle<Result<(), conductor_runtime::BrokerError>>,
    hold: tokio::sync::watch::Sender<bool>,
}
async fn runner_fixture(mismatch: bool, heartbeat: u64) -> BrokerRunnerFixture {
    use std::process::Stdio;
    let BrokerSetup {
        root,
        store,
        binding,
        files,
        launch,
        server,
        hold,
    } = runner_setup(mismatch, heartbeat).await;
    let hash = launch.sha256().unwrap();
    let bin = PathBuf::from(
        std::env::var_os("CONDUCTOR_TEST_BIN_DIR")
            .expect("build workspace and set CONDUCTOR_TEST_BIN_DIR"),
    )
    .join("conductor-runner");
    let mut child = tokio::process::Command::new(bin)
        .arg("--file-broker")
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let output = BufReader::new(child.stdout.take().unwrap());
    runner_send(&mut input, &launch).await;
    BrokerRunnerFixture {
        root,
        store,
        binding,
        files,
        hash,
        child,
        input,
        output,
        server,
        hold,
    }
}
struct BrokerRunnerFixture {
    root: tempfile::TempDir,
    store: Store,
    binding: FileBrokerBinding,
    files: PathBuf,
    hash: String,
    child: tokio::process::Child,
    input: tokio::process::ChildStdin,
    output: BufReader<tokio::process::ChildStdout>,
    server: tokio::task::JoinHandle<Result<(), conductor_runtime::BrokerError>>,
    hold: tokio::sync::watch::Sender<bool>,
}
async fn real_broker_runner(renew: bool) {
    use conductor_runtime::{BrokerRunnerControl, BrokerRunnerEvent, connect_broker_endpoint};
    let BrokerRunnerFixture {
        root,
        store,
        binding,
        files,
        hash,
        mut child,
        mut input,
        mut output,
        server,
        hold: _hold,
    } = runner_fixture(false, 5000).await;
    let scope = request(&binding).scope;
    let BrokerRunnerEvent::Ready {
        launch_sha256,
        root_identity_sha256,
        proxy_handoff,
        process,
    } = runner_frame(&mut output).await
    else {
        panic!("expected ready");
    };
    assert_eq!(launch_sha256, hash);
    assert_eq!(root_identity_sha256, binding.root_identity_sha256);
    assert_eq!(
        process,
        conductor_runtime::ChildIdentity::observe(child.id().unwrap())
            .await
            .unwrap()
    );
    assert_progress(&store, &binding, 0, 0).await;
    let now = continuous_millis().unwrap();
    let first_deadline = now + if renew { 1000 } else { 4000 };
    runner_send(
        &mut input,
        &BrokerRunnerControl::Release {
            launch_sha256: hash.clone(),
            lease_deadline_ms: first_deadline,
        },
    )
    .await;
    if renew {
        runner_send(
            &mut input,
            &BrokerRunnerControl::Renew {
                launch_sha256: hash.clone(),
                lease_deadline_ms: now + 4000,
            },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(continuous_millis().unwrap() > first_deadline);
    }
    let (peer, actual_scope) = connect_broker_endpoint(&proxy_handoff, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(actual_scope == scope);
    let mut peer = BufReader::new(peer);
    let req = request(&binding);
    let mut bytes = serde_json::to_vec(&req).unwrap();
    bytes.push(b'\n');
    peer.write_all(&bytes).await.unwrap();
    let mut reply = String::new();
    tokio::time::timeout(Duration::from_secs(5), peer.read_line(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert!(!reply.is_empty());
    assert_progress(&store, &binding, 1, 1).await;
    runner_send(&mut input, &BrokerRunnerControl::Cancel).await;
    let BrokerRunnerEvent::Stopped {
        launch_sha256,
        completed_sequence,
    } = runner_frame(&mut output).await
    else {
        panic!("expected stop");
    };
    assert_eq!(launch_sha256, hash);
    assert_eq!(completed_sequence, Some(1));
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert_runner_receipt(&root, &binding, &hash, process, Some(1));
    assert!(server.await.unwrap().is_ok());
    assert_late_write_refused(peer, &binding, &files).await;
    assert_progress(&store, &binding, 1, 1).await;
}
async fn assert_late_write_refused(
    mut peer: BufReader<tokio::net::UnixStream>,
    binding: &FileBrokerBinding,
    files: &Path,
) {
    let mut late = request(binding);
    late.sequence = 2;
    late.operation = BrokerOperation::WriteText {
        path: "late".into(),
        expected_sha256: None,
        contents: "forbidden".into(),
    };
    let mut bytes = serde_json::to_vec(&late).unwrap();
    bytes.push(b'\n');
    let _ = peer.write_all(&bytes).await;
    let mut reply = String::new();
    let read = peer.read_line(&mut reply).await;
    assert!(matches!(read, Ok(0) | Err(_)));
    assert!(!files.join("late").exists());
    assert_eq!(
        fs::read_to_string(files.join("answer")).unwrap(),
        "durable answer\n"
    );
}
#[tokio::test]
async fn journal_actual_runner_pins_root_writes_and_revokes_before_exit() {
    real_broker_runner(false).await;
}
#[tokio::test]
async fn journal_actual_runner_accepts_timely_renewal_beyond_initial_deadline() {
    real_broker_runner(true).await;
}

#[tokio::test]
async fn journal_actual_runner_refuses_wrong_root_before_ready_or_journal_connection() {
    let BrokerRunnerFixture {
        root: _root,
        store,
        binding,
        files,
        mut child,
        input,
        mut output,
        server,
        hold: _hold,
        ..
    } = runner_fixture(true, 5000).await;
    drop(input);
    let mut bytes = String::new();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), output.read_line(&mut bytes))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(!child.wait().await.unwrap().success());
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    assert_progress(&store, &binding, 0, 0).await;
    assert!(!files.join("answer").exists());
}
#[tokio::test]
async fn journal_actual_runner_requires_release_and_stops_on_control_or_lease_loss() {
    use conductor_runtime::{BrokerRunnerControl, BrokerRunnerEvent, connect_broker_endpoint};
    for mode in [
        "wrong_release",
        "expired_release",
        "eof_before",
        "eof_after",
        "heartbeat",
        "lease",
        "wrong_renew",
    ] {
        let BrokerRunnerFixture {
            root,
            store,
            binding,
            files,
            hash,
            mut child,
            mut input,
            mut output,
            server,
            hold: _hold,
        } = runner_fixture(false, if mode == "heartbeat" { 100 } else { 5000 }).await;
        let BrokerRunnerEvent::Ready {
            proxy_handoff,
            process,
            ..
        } = runner_frame(&mut output).await
        else {
            panic!("expected ready");
        };
        assert_eq!(
            process,
            conductor_runtime::ChildIdentity::observe(child.id().unwrap())
                .await
                .unwrap()
        );
        let released = !matches!(mode, "wrong_release" | "expired_release" | "eof_before");
        if mode != "eof_before" {
            let now = continuous_millis().unwrap();
            runner_send(
                &mut input,
                &BrokerRunnerControl::Release {
                    launch_sha256: if mode == "wrong_release" {
                        "0".repeat(64)
                    } else {
                        hash.clone()
                    },
                    lease_deadline_ms: if mode == "expired_release" {
                        now - 1
                    } else {
                        now + if mode == "lease" { 100 } else { 4000 }
                    },
                },
            )
            .await;
        }
        if mode == "wrong_renew" {
            runner_send(
                &mut input,
                &BrokerRunnerControl::Renew {
                    launch_sha256: "0".repeat(64),
                    lease_deadline_ms: continuous_millis().unwrap() + 4000,
                },
            )
            .await;
        }
        let retained = if mode.starts_with("eof") {
            drop(input);
            None
        } else {
            Some(input)
        };
        if released {
            let BrokerRunnerEvent::Stopped {
                completed_sequence, ..
            } = runner_frame(&mut output).await
            else {
                panic!("expected stop");
            };
            assert_eq!(completed_sequence, Some(0), "{mode}");
        }
        let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.success(), released, "{mode}");
        assert_runner_receipt(&root, &binding, &hash, process, released.then_some(0));
        drop(retained);
        assert!(server.await.unwrap().is_ok());
        assert!(
            connect_broker_endpoint(&proxy_handoff, Duration::from_secs(1))
                .await
                .is_err()
        );
        assert_progress(&store, &binding, 0, 0).await;
        assert!(!files.join("answer").exists());
    }
}

fn assert_runner_receipt(
    root: &tempfile::TempDir,
    binding: &FileBrokerBinding,
    hash: &str,
    process: conductor_runtime::ChildIdentity,
    completed: Option<u64>,
) {
    use conductor_runtime::{
        BrokerReceiptDirectory, BrokerShutdownObservation, BrokerShutdownReceipt,
    };
    let expected = BrokerShutdownReceipt {
        version: 1,
        scope: request(binding).scope,
        launch_sha256: hash.into(),
        root_identity_sha256: binding.root_identity_sha256.clone(),
        process,
        observation: completed.map_or(
            BrokerShutdownObservation::NeverReleased,
            |completed_sequence| BrokerShutdownObservation::Revoked { completed_sequence },
        ),
    };
    let directory =
        BrokerReceiptDirectory::open(&root.path().canonicalize().unwrap().join("r")).unwrap();
    assert!(directory.read(&expected).unwrap() == expected);
}

#[tokio::test]
async fn journal_actual_runner_killed_before_release_leaves_no_shutdown_receipt() {
    use conductor_runtime::BrokerRunnerEvent;
    let BrokerRunnerFixture {
        root,
        store,
        binding,
        mut child,
        input,
        mut output,
        server,
        hold: _hold,
        ..
    } = runner_fixture(false, 5000).await;
    let BrokerRunnerEvent::Ready { process, .. } = runner_frame(&mut output).await else {
        panic!("expected ready");
    };
    assert_eq!(
        process,
        conductor_runtime::ChildIdentity::observe(child.id().unwrap())
            .await
            .unwrap()
    );
    child.start_kill().unwrap();
    assert!(
        !tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    drop(input);
    assert!(server.await.unwrap().is_ok());
    let directory = root.path().join("r");
    assert!(
        directory
            .join(format!("broker-{}.claimed", binding.invocation_id))
            .is_file()
    );
    assert!(
        !directory
            .join(format!("broker-{}.json", binding.invocation_id))
            .exists()
    );
    assert_progress(&store, &binding, 0, 0).await;
}

#[tokio::test]
async fn journal_parent_client_records_identity_before_release_and_verifies_receipt() {
    use conductor_runtime::{PreparedBrokerRunner, connect_broker_endpoint};
    let BrokerSetup {
        root,
        store,
        binding,
        files,
        launch,
        server,
        hold: _hold,
    } = runner_setup(false, 5000).await;
    let bin =
        PathBuf::from(std::env::var_os("CONDUCTOR_TEST_BIN_DIR").unwrap()).join("conductor-runner");
    let prepared = PreparedBrokerRunner::prepare(&bin, &launch).await.unwrap();
    let expected = prepared.identity().clone();
    let owner_path = root.path().join("fixture-owner.json");
    let owner_copy = owner_path.clone();
    let mut session = prepared
        .release(|identity| async move {
            // Test persistence only; production store ownership remains separate.
            let mut owner = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(owner_copy)
                .unwrap();
            std::io::Write::write_all(&mut owner, &serde_json::to_vec(&identity).unwrap()).unwrap();
            owner.sync_all().unwrap();
            Ok(continuous_millis().unwrap() + 4000)
        })
        .await
        .unwrap();
    let saved: conductor_runtime::BrokerShutdownReceipt =
        serde_json::from_slice(&fs::read(owner_path).unwrap()).unwrap();
    assert!(saved == expected);
    let (mut peer, _) = connect_broker_endpoint(session.proxy_handoff(), Duration::from_secs(2))
        .await
        .unwrap();
    let mut bytes = serde_json::to_vec(&request(&binding)).unwrap();
    bytes.push(b'\n');
    peer.write_all(&bytes).await.unwrap();
    let mut peer = BufReader::new(peer);
    let mut reply = String::new();
    tokio::time::timeout(Duration::from_secs(5), peer.read_line(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert!(!reply.is_empty());
    assert_progress(&store, &binding, 1, 1).await;
    session.cancel().await.unwrap();
    let receipt = session.finish().await.unwrap();
    assert_eq!(receipt.process, expected.process);
    assert_runner_receipt(
        &root,
        &binding,
        &launch.sha256().unwrap(),
        receipt.process,
        Some(1),
    );
    assert!(server.await.unwrap().is_ok());
    assert_late_write_refused(peer, &binding, &files).await;
}

#[tokio::test]
async fn journal_parent_client_failed_persistence_never_releases() {
    use conductor_runtime::{BrokerError, PreparedBrokerRunner};
    let BrokerSetup {
        root,
        store,
        binding,
        files,
        launch,
        server,
        hold: _hold,
    } = runner_setup(false, 5000).await;
    let bin =
        PathBuf::from(std::env::var_os("CONDUCTOR_TEST_BIN_DIR").unwrap()).join("conductor-runner");
    let prepared = PreparedBrokerRunner::prepare(&bin, &launch).await.unwrap();
    let expected = prepared.identity().clone();
    assert!(
        prepared
            .release(|_| async { Err(BrokerError::Refused) })
            .await
            .is_err()
    );
    assert!(server.await.unwrap().is_ok());
    let directory = conductor_runtime::BrokerReceiptDirectory::open(&launch.receipt_root).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if directory.read(&expected).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_runner_receipt(
        &root,
        &binding,
        &launch.sha256().unwrap(),
        expected.process,
        None,
    );
    assert_progress(&store, &binding, 0, 0).await;
    assert!(!files.join("answer").exists());
}

#[tokio::test]
async fn journal_parent_client_unknown_completion_never_becomes_shutdown_proof() {
    use conductor_runtime::{PreparedBrokerRunner, connect_broker_endpoint};
    let BrokerSetup {
        root,
        store,
        binding,
        files,
        launch,
        server,
        hold: _hold,
    } = runner_setup(false, 5000).await;
    let mut db = sqlx::SqliteConnection::connect(&format!(
        "sqlite:{}",
        root.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER refuse_completion BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.completed' BEGIN SELECT RAISE(FAIL,'fixture'); END").execute(&mut db).await.unwrap();
    let bin =
        PathBuf::from(std::env::var_os("CONDUCTOR_TEST_BIN_DIR").unwrap()).join("conductor-runner");
    let prepared = PreparedBrokerRunner::prepare(&bin, &launch).await.unwrap();
    let session = prepared
        .release(|_| async { Ok(continuous_millis().unwrap() + 4000) })
        .await
        .unwrap();
    let (mut peer, _) = connect_broker_endpoint(session.proxy_handoff(), Duration::from_secs(2))
        .await
        .unwrap();
    let mut bytes = serde_json::to_vec(&request(&binding)).unwrap();
    bytes.push(b'\n');
    peer.write_all(&bytes).await.unwrap();
    assert!(session.finish().await.is_err());
    assert!(server.await.unwrap().is_err());
    assert_progress(&store, &binding, 1, 0).await;
    assert_eq!(
        fs::read_to_string(files.join("answer")).unwrap(),
        "durable answer\n"
    );
    assert!(
        !launch
            .receipt_root
            .join(format!("broker-{}.json", binding.invocation_id))
            .exists()
    );
}
