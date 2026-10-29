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
