use super::transaction_cancellation::{auxiliary_origin_fixture, snapshot};
use super::*;
use sqlx::Executor;

pub(super) fn audit(id: &str) -> EffectAudit<'_> {
    EffectAudit {
        id,
        actor_id: "broker",
        correlation_id: "fixture",
        now_ms: 100,
    }
}
pub(super) async fn fixture() -> (tempfile::TempDir, Store, FileBrokerBinding) {
    let (root, store, aux, host) = auxiliary_origin_fixture("admit").await;
    let b = FileBrokerBinding {
        authority_id: host.authority_id,
        boot_generation: 2,
        invocation_id: aux.invocation_id,
        execution_token: "token".into(),
        task_id: "task".into(),
        attempt_id: "attempt".into(),
        lease_epoch: 7,
        root_identity_sha256: "b".repeat(64),
    };
    {
        let mut c = store.inner.writer.lock().await;
        sqlx::query(
            "UPDATE executions SET state='started',wrapper_identity=?,expected_wrapper_identity=?",
        )
        .bind(&b.invocation_id)
        .bind(&b.invocation_id)
        .execute(&mut *c)
        .await
        .unwrap();
        c.execute("UPDATE tasks SET state='running';UPDATE leases SET status='active',boot_generation=2,remaining_ms=60000").await.unwrap();
    }
    (root, store, b)
}
#[tokio::test]
async fn durable_sequence_replay_collision_and_close() {
    let (root, store, b) = fixture().await;
    assert!(
        store
            .register_file_broker(&b, audit("register"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .register_file_broker(&b, audit("replay"))
            .await
            .unwrap()
    );
    let hash = "c".repeat(64);
    let result = "d".repeat(64);
    assert!(
        store
            .admit_file_operation(&b, 2, &hash, audit("gap"))
            .await
            .is_err()
    );
    assert!(
        store
            .admit_file_operation(&b, 1, &hash, audit("admit"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .admit_file_operation(&b, 1, &hash, audit("duplicate"))
            .await
            .unwrap()
    );
    assert!(
        store
            .admit_file_operation(&b, 1, &result, audit("changed"))
            .await
            .is_err()
    );
    assert!(
        store
            .admit_file_operation(&b, 2, &hash, audit("pending"))
            .await
            .is_err()
    );
    assert!(
        store
            .close_file_broker(&b, 1, audit("premature"))
            .await
            .is_err()
    );
    assert!(
        store
            .finish_file_operation(&b, 1, &hash, &result, audit("finish"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .finish_file_operation(&b, 1, &hash, &result, audit("repeat-finish"))
            .await
            .unwrap()
    );
    assert!(
        store
            .finish_file_operation(&b, 1, &hash, &hash, audit("rebind"))
            .await
            .is_err()
    );
    assert!(
        store
            .close_file_broker(&b, 0, audit("wrong-watermark"))
            .await
            .is_err()
    );
    assert!(
        store
            .close_file_broker(&b, 1, audit("close"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .close_file_broker(&b, 1, audit("reclose"))
            .await
            .unwrap()
    );
    assert!(
        store
            .admit_file_operation(&b, 2, &hash, audit("closed"))
            .await
            .is_err()
    );
    let reopened = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    assert_eq!(
        reopened.file_broker_status(&b).await.unwrap(),
        FileBrokerStatus {
            closed: true,
            admitted: 1,
            completed: 1
        }
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE kind LIKE 'file_broker.%'")
            .fetch_one(reopened.readers())
            .await
            .unwrap();
    assert_eq!(count, 4);
}
#[tokio::test]
async fn restart_preserves_unknown_and_never_grants_replay() {
    let (root, store, b) = fixture().await;
    store
        .register_file_broker(&b, audit("register"))
        .await
        .unwrap();
    store
        .admit_file_operation(&b, 1, &"c".repeat(64), audit("admit"))
        .await
        .unwrap();
    let reopened = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    assert!(
        !reopened
            .register_file_broker(&b, audit("repeat"))
            .await
            .unwrap()
    );
    assert!(
        !reopened
            .admit_file_operation(&b, 1, &"c".repeat(64), audit("repeat-admit"))
            .await
            .unwrap()
    );
    reopened
        .start_reserved_runtime(101, &b.authority_id, 3)
        .await
        .unwrap();
    assert_eq!(
        reopened.file_broker_status(&b).await.unwrap(),
        FileBrokerStatus {
            closed: false,
            admitted: 1,
            completed: 0
        }
    );
    assert!(
        reopened
            .finish_file_operation(&b, 1, &"c".repeat(64), &"d".repeat(64), audit("old-finish"))
            .await
            .is_err()
    );
    assert!(
        reopened
            .close_file_broker(&b, 1, audit("old-close"))
            .await
            .is_err()
    );
    let mut rebound = b.clone();
    rebound.boot_generation = 3;
    assert!(reopened.file_broker_status(&rebound).await.is_err());
}
#[tokio::test]
async fn stale_scope_or_live_ownership_never_admits() {
    for change in [
        "authority",
        "generation",
        "root",
        "epoch",
        "wrapper",
        "lease",
        "task",
        "ended",
    ] {
        let (_root, store, mut b) = fixture().await;
        store
            .register_file_broker(&b, audit("register"))
            .await
            .unwrap();
        match change {
            "authority" => b.authority_id = "e".repeat(64),
            "generation" => b.boot_generation = 3,
            "root" => b.root_identity_sha256 = "e".repeat(64),
            "epoch" => b.lease_epoch = 8,
            "wrapper" => {
                store
                    .inner
                    .writer
                    .lock()
                    .await
                    .execute("UPDATE executions SET wrapper_identity='other'")
                    .await
                    .unwrap();
            }
            "lease" => {
                store
                    .inner
                    .writer
                    .lock()
                    .await
                    .execute("UPDATE leases SET status='expired'")
                    .await
                    .unwrap();
            }
            "task" => {
                store
                    .inner
                    .writer
                    .lock()
                    .await
                    .execute("UPDATE tasks SET state='paused'")
                    .await
                    .unwrap();
            }
            "ended" => {
                store
                    .inner
                    .writer
                    .lock()
                    .await
                    .execute("UPDATE attempts SET ended_at_ms=101")
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        assert!(
            store
                .admit_file_operation(&b, 1, &"c".repeat(64), audit("refused"))
                .await
                .is_err(),
            "{change}"
        );
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
    }
}
async fn prepare_step(store: &Store, b: &FileBrokerBinding, step: &str) {
    if step != "register" {
        store
            .register_file_broker(b, audit("register"))
            .await
            .unwrap();
    }
    if matches!(step, "finish" | "close") {
        store
            .admit_file_operation(b, 1, &"c".repeat(64), audit("admit"))
            .await
            .unwrap();
    }
    if step == "close" {
        store
            .finish_file_operation(b, 1, &"c".repeat(64), &"d".repeat(64), audit("finish"))
            .await
            .unwrap();
    }
}
async fn run_step(store: &Store, b: &FileBrokerBinding, step: &str) -> Result<bool, StoreError> {
    match step {
        "register" => store.register_file_broker(b, audit("step")).await,
        "admit" => {
            store
                .admit_file_operation(b, 1, &"c".repeat(64), audit("step"))
                .await
        }
        "finish" => {
            store
                .finish_file_operation(b, 1, &"c".repeat(64), &"d".repeat(64), audit("step"))
                .await
        }
        "close" => store.close_file_broker(b, 1, audit("step")).await,
        _ => panic!("unknown step"),
    }
}
#[tokio::test]
async fn audit_failure_rolls_back_every_broker_transition() {
    for step in ["register", "admit", "finish", "close"] {
        let (_root, store, b) = fixture().await;
        prepare_step(&store, &b, step).await;
        store.inner.writer.lock().await.execute("CREATE TRIGGER refuse_broker_audit BEFORE INSERT ON audit_events WHEN NEW.kind LIKE 'file_broker.%' BEGIN SELECT RAISE(FAIL,'fixture refusal'); END").await.unwrap();
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        assert!(run_step(&store, &b, step).await.is_err());
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before,
            "{step}"
        );
        store
            .inner
            .writer
            .lock()
            .await
            .execute("DROP TRIGGER refuse_broker_audit")
            .await
            .unwrap();
        assert!(run_step(&store, &b, step).await.unwrap());
    }
}
#[tokio::test]
async fn cancellation_rolls_back_every_broker_transition() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::time::Duration;
    for step in ["register", "admit", "finish", "close"] {
        let (_root, store, b) = fixture().await;
        prepare_step(&store, &b, step).await;
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        let written = Arc::new(AtomicBool::new(false));
        let observed = written.clone();
        let (release, wait) = mpsc::channel();
        let table = if matches!(step, "register" | "close") {
            "file_brokers"
        } else {
            "file_operations"
        };
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |update| {
                if update.table == table {
                    observed.store(true, Ordering::SeqCst);
                    wait.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            });
        let owned = store.clone();
        let binding = b.clone();
        let task = tokio::spawn(async move { run_step(&owned, &binding, step).await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !written.load(Ordering::SeqCst) {
                assert!(!task.is_finished());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let mut writer = store.inner.writer.lock().await;
        writer.lock_handle().await.unwrap().remove_update_hook();
        assert_eq!(snapshot(&mut writer).await, before, "{step}");
        drop(writer);
        assert!(run_step(&store, &b, step).await.unwrap());
    }
}

async fn actual_write(fail_completion: bool) {
    use conductor_runtime::{
        BrokerError, BrokerFileResult, BrokerFilesystem, BrokerOperation, BrokerRequest,
        BrokerScope, continuous_millis,
    };
    let (root, store, b) = fixture().await;
    let files = root.path().join("files");
    std::fs::create_dir(&files).unwrap();
    store
        .register_file_broker(&b, audit("register"))
        .await
        .unwrap();
    if fail_completion {
        store.inner.writer.lock().await.execute("CREATE TRIGGER refuse_completion BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.completed' BEGIN SELECT RAISE(FAIL,'lost completion'); END").await.unwrap();
    }
    let handle = tokio::runtime::Handle::current();
    let owned = store.clone();
    let binding = b.clone();
    let path = files.canonicalize().unwrap();
    tokio::task::spawn_blocking(move || {
        let request = BrokerRequest {
            version: 1,
            sequence: 1,
            scope: BrokerScope {
                authority_id: binding.authority_id.clone(),
                boot_generation: 2,
                invocation_id: binding.invocation_id.clone(),
                task_id: binding.task_id.clone(),
                attempt_id: binding.attempt_id.clone(),
                lease_epoch: 7,
            },
            operation: BrokerOperation::WriteText {
                path: "answer".into(),
                contents: "retained actual bytes".into(),
                expected_sha256: None,
            },
        };
        let broker = BrokerFilesystem::new(
            &path,
            request.scope.clone(),
            continuous_millis().unwrap() + 60000,
        )
        .unwrap();
        let result = broker.execute_recorded(
            &request,
            |i| {
                assert!(!path.join("answer").exists());
                match handle.block_on(owned.admit_file_operation(
                    &binding,
                    1,
                    &i.request_sha256,
                    audit("admit"),
                )) {
                    Ok(true) => Ok(()),
                    _ => Err(BrokerError::Admission),
                }
            },
            |i, result| {
                assert_eq!(
                    std::fs::read_to_string(path.join("answer")).unwrap(),
                    "retained actual bytes"
                );
                let hash = match result {
                    BrokerFileResult::Written { sha256 }
                    | BrokerFileResult::Text { sha256, .. } => sha256,
                };
                handle
                    .block_on(owned.finish_file_operation(
                        &binding,
                        1,
                        &i.request_sha256,
                        hash,
                        audit("finish"),
                    ))
                    .map(|_| ())
                    .map_err(|_| BrokerError::Uncertain)
            },
        );
        if fail_completion {
            assert!(matches!(result, Err(BrokerError::Uncertain)));
            assert_eq!(broker.revoke(), Err(BrokerError::Uncertain));
        } else {
            assert!(result.is_ok());
            assert_eq!(broker.revoke(), Ok(1));
        }
    })
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(files.join("answer")).unwrap(),
        "retained actual bytes"
    );
    let reopened = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    assert_eq!(
        reopened.file_broker_status(&b).await.unwrap(),
        FileBrokerStatus {
            closed: false,
            admitted: 1,
            completed: i64::from(!fail_completion)
        }
    );
    let hash: String =
        sqlx::query_scalar("SELECT request_sha256 FROM file_operations WHERE invocation_id=?")
            .bind(&b.invocation_id)
            .fetch_one(reopened.readers())
            .await
            .unwrap();
    assert!(
        !reopened
            .admit_file_operation(&b, 1, &hash, audit("replay"))
            .await
            .unwrap()
    );
    assert_eq!(
        reopened
            .close_file_broker(&b, 1, audit("close"))
            .await
            .is_err(),
        fail_completion
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_file_write_with_failed_completion_remains_unknown_and_unreplayable() {
    actual_write(true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_file_write_records_success_before_clean_closure() {
    actual_write(false).await;
}
