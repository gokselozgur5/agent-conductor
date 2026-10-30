//! Deterministic cancellation at real SQLite writes, not timing-only aborts.
use super::*;
use sha2::{Digest, Sha256};
use sqlx::{Executor, Row};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc,
};

#[path = "transaction_crash.rs"]
mod abrupt_process;

#[derive(Clone, Copy, Debug)]
enum Owner {
    Project,
    Task,
    Persist,
    Sequence,
    Startup,
    Effect,
    Publish,
    Observation,
    Delivery,
    Ack,
    Enqueue,
    Dispatch,
    Prepare,
    Update,
}
impl Owner {
    fn barrier(self) -> (&'static str, usize) {
        match self {
            Self::Project | Self::Persist => ("projects", 1),
            Self::Sequence => ("projects", 2),
            Self::Task => ("tasks", 1),
            Self::Startup => ("runtime_metadata", 1),
            Self::Effect => ("effect_intents", 1),
            Self::Publish | Self::Delivery => ("event_deliveries", 1),
            Self::Observation => ("repository_observations", 1),
            Self::Ack => ("event_deliveries", 1),
            Self::Enqueue => ("workflow_wakes", 1),
            Self::Dispatch => ("workflow_dispatch", 1),
            Self::Prepare | Self::Update => ("executions", 1),
        }
    }
}
fn project(id: &str) -> NewProject<'_> {
    NewProject {
        id,
        canonical_path: id,
        filesystem_identity: id,
        git_common_dir_identity: None,
        created_at_ms: 100,
    }
}
fn batch(id: &str) -> CommitBatch {
    CommitBatch {
        actor_id: "operator".into(),
        idempotency_key: id.into(),
        request_hash: id.into(),
        task: None,
        project: Some(ProjectRegistration {
            id: id.into(),
            canonical_path: id.into(),
            filesystem_identity: id.into(),
            git_common_dir_identity: None,
            created_at_ms: 100,
        }),
        agent: None,
        new_task: None,
        attempt: None,
        transitions: vec![],
        audit: vec![AuditAppend {
            id: id.into(),
            task_id: None,
            actor_id: "operator".into(),
            kind: "fixture".into(),
            correlation_id: id.into(),
            payload_json: "{}".into(),
            occurred_at_ms: 100,
        }],
        effects: vec![],
        outcome_json: "{}".into(),
        committed_at_ms: 100,
        expires_at_ms: 1000,
    }
}
fn event() -> SemanticEvent {
    SemanticEvent {
        schema_version: 1,
        id: "event".into(),
        idempotency_key: "event".into(),
        source_agent_id: "source".into(),
        source_session_id: "session".into(),
        correlation_id: "event".into(),
        causation_id: None,
        generation: 0,
        topic: "fixture.changed".into(),
        project_id: None,
        task_id: None,
        attempt_id: None,
        lease_epoch: None,
        path_scope: None,
        payload_json: "{}".into(),
        created_at_ms: 100,
    }
}
fn observation(version: Option<i64>) -> RepositoryObservation<'static> {
    RepositoryObservation {
        id: "watch",
        config_json: "{}",
        expected_version: version,
        snapshot_json: "{}",
        now_ms: 100,
    }
}
async fn fixture(owner: Owner) -> (tempfile::TempDir, Store, Option<EventDelivery>) {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("state.sqlite3"))
        .await
        .unwrap();
    store.start_runtime(0).await.unwrap();
    store.create_project(project("seed")).await.unwrap();
    for id in ["source", "job:worker"] {
        store
            .create_agent(NewAgent {
                id,
                adapter_kind: "fake",
                protocol_fingerprint: "fixture",
                capability_hash: "fixture",
                access_mode: "read_write",
                created_at_ms: 0,
            })
            .await
            .unwrap();
    }
    store
        .upsert_event_subscription(&EventSubscription {
            schema_version: 1,
            id: "sub".into(),
            agent_id: "job:worker".into(),
            topic_filter: "fixture.*".into(),
            project_id: None,
            path_prefix: None,
            echo_source: false,
            created_at_ms: 0,
        })
        .await
        .unwrap();
    let mut delivery = None;
    match owner {
        Owner::Effect => {
            let mut seed = batch("effect-seed");
            seed.effects.push(EffectIntentAppend {
                token: "effect".into(),
                task_id: None,
                attempt_id: None,
                lease_epoch: None,
                correlation_id: "effect".into(),
                effect_kind: "fixture".into(),
                request_json: "{}".into(),
                created_at_ms: 100,
            });
            store.persist(&seed).await.unwrap();
        }
        Owner::Observation => store
            .publish_repository_observation(&observation(None), &[], &EventLimits::default())
            .await
            .unwrap(),
        Owner::Delivery | Owner::Ack => {
            store
                .publish_event(&event(), &EventLimits::default())
                .await
                .unwrap();
            if matches!(owner, Owner::Ack) {
                delivery = store
                    .claim_event_delivery("job:worker", 100, &EventLimits::default())
                    .await
                    .unwrap();
            }
        }
        Owner::Dispatch => store.enqueue_workflow("job", "{}", 100).await.unwrap(),
        Owner::Prepare | Owner::Update => seed_execution(&store, owner).await,
        _ => {}
    }
    (root, store, delivery)
}
async fn run(store: &Store, owner: Owner, delivery: Option<&EventDelivery>) -> Result<(), String> {
    macro_rules! done {
        ($e:expr) => {
            $e.map(|_| ()).map_err(|e| e.to_string())
        };
    }
    match owner {
        Owner::Project => done!(store.create_project(project("new")).await),
        Owner::Task => done!(
            store
                .create_task(NewTask {
                    id: "task",
                    project_id: "seed",
                    objective: "fixture",
                    state: "backlog",
                    priority: 0,
                    authority_json: "{}",
                    aggregate_json: "{}",
                    created_at_ms: 100
                })
                .await
        ),
        Owner::Persist => done!(store.persist(&batch("new")).await),
        Owner::Sequence => done!(
            store
                .persist_sequence(&[batch("first"), batch("second")])
                .await
        ),
        Owner::Startup => done!(store.start_runtime(100).await),
        Owner::Effect => done!(
            store
                .claim_effect(
                    "effect",
                    "runner",
                    1000,
                    EffectAudit {
                        id: "claim",
                        actor_id: "operator",
                        correlation_id: "effect",
                        now_ms: 100
                    }
                )
                .await
        ),
        Owner::Publish => done!(store.publish_event(&event(), &EventLimits::default()).await),
        Owner::Observation => done!(
            store
                .publish_repository_observation(
                    &observation(Some(0)),
                    &[event()],
                    &EventLimits::default()
                )
                .await
        ),
        Owner::Delivery => done!(
            store
                .claim_event_delivery("job:worker", 100, &EventLimits::default())
                .await
        ),
        Owner::Ack => {
            let delivery = delivery.unwrap();
            done!(
                store
                    .acknowledge_event_with_state(
                        &delivery.delivery_id,
                        "job:worker",
                        &delivery.claim_token,
                        &ConsumerStateEffect {
                            namespace: "fixture".into(),
                            key: "state".into(),
                            expected_version: None,
                            value_json: "{}".into()
                        },
                        100
                    )
                    .await
            )
        }
        Owner::Enqueue => done!(
            store
                .enqueue_workflow("job", r#"{"wake_subscription":"sub"}"#, 100)
                .await
        ),
        Owner::Dispatch => done!(store.claim_workflow(1, 100).await),
        Owner::Prepare => done!(
            store
                .prepare_execution_once(&preparation(), execution_audit("prepare"))
                .await
        ),
        Owner::Update => done!(
            store
                .update_execution(
                    "token",
                    0,
                    "ack",
                    &ExecutionUpdate::Acknowledge {
                        process: None,
                        wrapper_identity: "wrapper".into(),
                        packet_sha256: preparation().packet_sha256,
                    },
                    execution_audit("ack")
                )
                .await
        ),
    }
}
pub(super) async fn snapshot(connection: &mut SqliteConnection) -> Vec<(String, String)> {
    let tables:Vec<String>=sqlx::query_scalar("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetch_all(&mut *connection).await.unwrap();
    let mut result = vec![];
    for table in tables {
        let columns = sqlx::query(&format!("PRAGMA table_info(\"{table}\")"))
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        let quoted = columns
            .iter()
            .map(|row| format!("quote(\"{}\")", row.get::<String, _>("name")))
            .collect::<Vec<_>>()
            .join(",");
        let value:String=sqlx::query_scalar(&format!("SELECT json_group_array(value) FROM (SELECT json_array({quoted}) AS value FROM \"{table}\" ORDER BY value)")).fetch_one(&mut *connection).await.unwrap();
        result.push((table, value));
    }
    result
}
async fn check(owner: Owner) {
    let (_root, store, delivery) = fixture(owner).await;
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    let written = Arc::new(AtomicBool::new(false));
    let (release, wait) = mpsc::channel();
    {
        let mut connection = store.inner.writer.lock().await;
        let observed = written.clone();
        let (table, limit) = owner.barrier();
        let mut count = 0;
        connection
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |update| {
                if update.table == table {
                    count += 1;
                    if count == limit {
                        observed.store(true, Ordering::SeqCst);
                        wait.recv_timeout(Duration::from_secs(10)).unwrap();
                    }
                }
            });
    }
    let owned = store.clone();
    let receipt = delivery.clone();
    let request = tokio::spawn(async move { run(&owned, owner, receipt.as_ref()).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !written.load(Ordering::SeqCst) {
            assert!(
                !request.is_finished(),
                "{owner:?} exited before selected write"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    let mut connection = store.inner.writer.lock().await;
    connection.lock_handle().await.unwrap().remove_update_hook();
    let after = snapshot(&mut connection).await;
    let changed = before
        .iter()
        .zip(&after)
        .filter(|(a, b)| a != b)
        .map(|(a, _)| a.0.clone())
        .collect::<Vec<_>>();
    assert!(
        changed.is_empty(),
        "{owner:?} left partial state in tables {changed:?}"
    );
    drop(connection);
    let mut reader = store.readers().acquire().await.unwrap();
    assert_eq!(snapshot(&mut reader).await, before);
    drop(reader);
    run(&store, owner, delivery.as_ref()).await.unwrap();
    assert_ne!(
        snapshot(&mut *store.inner.writer.lock().await).await,
        before,
        "control did not mutate state"
    );
}
macro_rules! owner_test {
    ($name:ident,$owner:ident) => {
        #[tokio::test]
        async fn $name() {
            check(Owner::$owner).await;
        }
    };
}
owner_test!(cancel_create_project, Project);
owner_test!(cancel_create_task, Task);
owner_test!(cancel_persist, Persist);
owner_test!(cancel_persist_sequence, Sequence);
owner_test!(cancel_start_runtime, Startup);
owner_test!(cancel_mutate_effect, Effect);
owner_test!(cancel_publish_event, Publish);
owner_test!(cancel_repository_observation, Observation);
owner_test!(cancel_claim_delivery, Delivery);
owner_test!(cancel_acknowledge_delivery, Ack);
owner_test!(cancel_enqueue_workflow, Enqueue);
owner_test!(cancel_claim_workflow, Dispatch);

fn preparation() -> ExecutionPreparation {
    let packet_json =
        serde_json::json!({"contract":{"task_id":"task","attempt_id":"attempt","lease_epoch":7}})
            .to_string();
    ExecutionPreparation {
        expected_wrapper_identity: None,
        effect_token: "token".into(),
        task_id: "task".into(),
        attempt_id: "attempt".into(),
        lease_epoch: 7,
        runner_id: "runner".into(),
        packet_sha256: format!("{:x}", Sha256::digest(packet_json.as_bytes())),
        packet_json,
    }
}
fn execution_audit(id: &str) -> EffectAudit<'_> {
    EffectAudit {
        id,
        actor_id: "runtime",
        correlation_id: "correlation",
        now_ms: 100,
    }
}
async fn seed_execution(store: &Store, owner: Owner) {
    {
        let mut connection = store.inner.writer.lock().await;
        connection.execute("INSERT INTO tasks (id,project_id,objective,state,priority,version,authority_json,created_at_ms,updated_at_ms,aggregate_json) VALUES ('task','seed','fixture','running',0,2,'{}',0,0,'{}');
        INSERT INTO attempts (id,task_id,worker_id,lease_epoch,instruction_hashes_json,required_checks_json,capability_hash,started_at_ms) VALUES ('attempt','task','job:worker',7,'[]','[]','fixture',0);
        INSERT INTO leases VALUES ('attempt','task',7,'active',1,10000,1000,10000,0);
        INSERT INTO effect_intents (token,task_id,attempt_id,lease_epoch,correlation_id,effect_kind,request_json,status,runner_id,claim_deadline_ms,created_at_ms,updated_at_ms) VALUES ('token','task','attempt',7,'correlation','worker.spawn','{}','claimed','runner',10000,0,0);").await.unwrap();
    }
    if matches!(owner, Owner::Update) {
        store
            .prepare_execution_once(&preparation(), execution_audit("prepare"))
            .await
            .unwrap();
    }
}
owner_test!(cancel_prepare_execution, Prepare);
owner_test!(cancel_update_execution, Update);

// A canceled caller cannot undo an already issued successful COMMIT.
// Lost responses must be reconciled through the existing idempotency record.
async fn check_lost_commit_response(owner: Owner) {
    let (_root, store, _) = fixture(owner).await;
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    let committing = Arc::new(AtomicBool::new(false));
    let observed = committing.clone();
    let (release, wait) = mpsc::channel();
    store
        .inner
        .writer
        .lock()
        .await
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            observed.store(true, Ordering::SeqCst);
            wait.recv_timeout(Duration::from_secs(10)).unwrap();
            true
        });
    let owned = store.clone();
    let request = tokio::spawn(async move { run(&owned, owner, None).await });
    tokio::time::timeout(Duration::from_secs(10), async {
        while !committing.load(Ordering::SeqCst) {
            assert!(!request.is_finished(), "request exited before COMMIT");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    let mut connection = store.inner.writer.lock().await;
    connection.lock_handle().await.unwrap().remove_commit_hook();
    let committed = snapshot(&mut connection).await;
    assert_ne!(committed, before);
    drop(connection);
    let mut reader = store.readers().acquire().await.unwrap();
    assert_eq!(snapshot(&mut reader).await, committed);
    drop(reader);
    match owner {
        Owner::Persist => assert!(matches!(
            store.persist(&batch("new")).await.unwrap(),
            CommitResult::Replayed { .. }
        )),
        Owner::Publish => assert!(matches!(
            store
                .publish_event(&event(), &EventLimits::default())
                .await
                .unwrap(),
            PublishResult::Duplicate { .. }
        )),
        _ => unreachable!(),
    }
    assert_eq!(
        snapshot(&mut *store.inner.writer.lock().await).await,
        committed
    );
}
#[tokio::test]
async fn persist_lost_commit_response_replays_once() {
    check_lost_commit_response(Owner::Persist).await;
}
#[tokio::test]
async fn publish_lost_commit_response_replays_once() {
    check_lost_commit_response(Owner::Publish).await;
}

#[tokio::test]
async fn cancel_legacy_host_witness_and_settlement_rolls_back_all_tables() {
    for settling in [false, true] {
        let (_root, store, _) = fixture(Owner::Project).await;
        {
            let mut c = store.inner.writer.lock().await;
            c.execute("INSERT INTO tasks(id,project_id,objective,state,priority,version,authority_json,created_at_ms,updated_at_ms,aggregate_json) VALUES('legacy','seed','fixture','paused',0,1,'{}',0,0,'{}'); INSERT INTO legacy_auxiliary_ownership VALUES('legacy','verification_without_invocation',1)").await.unwrap();
        }
        store
            .start_reserved_runtime(0, &"a".repeat(64), 2)
            .await
            .unwrap();
        let mut host = AuxiliaryHostObservation {
            authority_id: "a".repeat(64),
            generation: 2,
            platform: "macos".into(),
            host_id: "b".repeat(64),
            boot_id: "c".repeat(64),
        };
        if settling {
            store
                .reconcile_legacy_auxiliary_host(&host, 0)
                .await
                .unwrap();
            store
                .start_reserved_runtime(0, &host.authority_id, 3)
                .await
                .unwrap();
            host.generation = 3;
            host.boot_id = "d".repeat(64);
        }
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        let written = Arc::new(AtomicBool::new(false));
        let observed = written.clone();
        let (release, wait) = mpsc::channel();
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |update| {
                if update.table == "audit_events" {
                    observed.store(true, Ordering::SeqCst);
                    wait.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            });
        let owned = store.clone();
        let request =
            tokio::spawn(async move { owned.reconcile_legacy_auxiliary_host(&host, 0).await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !written.load(Ordering::SeqCst) {
                assert!(!request.is_finished());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let mut c = store.inner.writer.lock().await;
        c.lock_handle().await.unwrap().remove_update_hook();
        assert_eq!(snapshot(&mut c).await, before);
        drop(c);
        assert_eq!(
            snapshot(&mut store.readers().acquire().await.unwrap()).await,
            before
        );
    }
}

pub(super) async fn auxiliary_origin_fixture(
    mode: &str,
) -> (
    tempfile::TempDir,
    Store,
    AuxiliaryPreparation,
    AuxiliaryHostObservation,
) {
    let (root, store, _) = fixture(Owner::Update).await;
    store
        .start_reserved_runtime(0, &"a".repeat(64), 2)
        .await
        .unwrap();
    let source = format!("snapshot:{}", "b".repeat(64));
    {
        let mut c = store.inner.writer.lock().await;
        c.execute("UPDATE executions SET state='fenced';UPDATE tasks SET state='verifying'")
            .await
            .unwrap();
        sqlx::query("UPDATE tasks SET aggregate_json=?")
            .bind(
                serde_json::json!({"attempt":{"id":"attempt"},"verification":{"source":source}})
                    .to_string(),
            )
            .execute(&mut *c)
            .await
            .unwrap();
    }
    let input = AuxiliaryPreparation {
        invocation_id: uuid::Uuid::now_v7().to_string(),
        task_id: "task".into(),
        execution_token: "token".into(),
        attempt_id: "attempt".into(),
        task_version: 2,
        boot_generation: 2,
        kind: AuxiliaryKind::Check,
        source,
        definition_sha256: "c".repeat(64),
        workspace: "/workspace".into(),
    };
    let mut host = AuxiliaryHostObservation {
        authority_id: "a".repeat(64),
        generation: 2,
        platform: "macos".into(),
        host_id: "d".repeat(64),
        boot_id: "e".repeat(64),
    };
    if mode != "admit" {
        if mode == "settle" {
            store
                .prepare_auxiliary_with_host(&input, &host, execution_audit("origin-prepare"))
                .await
                .unwrap();
        } else {
            store
                .prepare_auxiliary_once(&input, execution_audit("origin-prepare"))
                .await
                .unwrap();
        }
        store
            .start_reserved_runtime(0, &host.authority_id, 3)
            .await
            .unwrap();
        host.generation = 3;
        host.boot_id = "f".repeat(64);
    }
    (root, store, input, host)
}

#[tokio::test]
async fn cancel_auxiliary_origin_admission_observation_and_settlement() {
    for mode in ["admit", "observe", "settle"] {
        let (_root, store, input, host) = auxiliary_origin_fixture(mode).await;
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        let written = Arc::new(AtomicBool::new(false));
        let observed = written.clone();
        let (release, wait) = mpsc::channel();
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |update| {
                if update.table == "audit_events" {
                    observed.store(true, Ordering::SeqCst);
                    wait.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            });
        let owned = store.clone();
        let request = tokio::spawn(async move {
            if mode == "admit" {
                owned
                    .prepare_auxiliary_with_host(&input, &host, execution_audit("cancelled"))
                    .await
            } else {
                owned
                    .reconcile_auxiliary_host(&input, &host, execution_audit("cancelled"))
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !written.load(Ordering::SeqCst) {
                assert!(!request.is_finished());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let mut c = store.inner.writer.lock().await;
        c.lock_handle().await.unwrap().remove_update_hook();
        assert_eq!(snapshot(&mut c).await, before);
        drop(c);
        assert_eq!(
            snapshot(&mut store.readers().acquire().await.unwrap()).await,
            before
        );
    }
}

#[tokio::test]
async fn auxiliary_admission_survives_process_death_on_both_sides() {
    const MODE: &str = "CONDUCTOR_AUXILIARY_CRASH_MODE";
    const CONTROL: &str = "CONDUCTOR_AUXILIARY_CRASH_CONTROL";
    if let Ok(mode) = std::env::var(MODE) {
        let control = std::path::PathBuf::from(std::env::var(CONTROL).unwrap());
        crash_auxiliary_admission(&mode, &control).await;
    }
    for mode in [
        "check_before_commit",
        "verifier_before_commit",
        "check_after_commit",
        "verifier_after_commit",
    ] {
        let control = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "transaction_cancellation::auxiliary_admission_survives_process_death_on_both_sides", "--nocapture"])
            .env(MODE, mode).env(CONTROL, control.path()).env("TMPDIR", control.path())
            .spawn().unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if tokio::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("crash fixture timed out: {mode}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(status.code(), Some(77), "{mode}");
        let path = std::fs::read_to_string(control.path().join("database")).unwrap();
        let store = Store::open(path).await.unwrap();
        let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
            .fetch_one(store.readers())
            .await
            .unwrap();
        assert_eq!(integrity, "ok");
        if mode.ends_with("before_commit") {
            let before: Vec<(String, String)> =
                serde_json::from_slice(&std::fs::read(control.path().join("before.json")).unwrap())
                    .unwrap();
            assert_eq!(
                snapshot(&mut *store.inner.writer.lock().await).await,
                before
            );
        } else {
            let input: AuxiliaryPreparation =
                serde_json::from_slice(&std::fs::read(control.path().join("input.json")).unwrap())
                    .unwrap();
            let row: (String, String, i64, Option<String>, Option<String>) = sqlx::query_as("SELECT preparation_json,state,release_recorded,process_json,launch_binding_json FROM auxiliary_invocations WHERE invocation_id=?")
                .bind(&input.invocation_id).fetch_one(store.readers()).await.unwrap();
            assert_eq!(
                serde_json::from_str::<AuxiliaryPreparation>(&row.0).unwrap(),
                input
            );
            assert_eq!(
                (row.1, row.2, row.3, row.4),
                ("pending".into(), 0, None, None)
            );
            for table in ["auxiliary_host_evidence", "auxiliary_invocations"] {
                let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                    .fetch_one(store.readers())
                    .await
                    .unwrap();
                assert_eq!(count, 1);
            }
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE id='crash-admission'")
                    .fetch_one(store.readers())
                    .await
                    .unwrap();
            assert_eq!(count, 1);
            let host: AuxiliaryHostObservation =
                serde_json::from_slice(&std::fs::read(control.path().join("host.json")).unwrap())
                    .unwrap();
            let stored: (String, String, Option<String>) = sqlx::query_as(
                "SELECT kind,host_json,ownership_json FROM auxiliary_host_evidence WHERE invocation_id=?",
            ).bind(&input.invocation_id).fetch_one(store.readers()).await.unwrap();
            assert_eq!(stored.0, "admission");
            assert_eq!(
                serde_json::from_str::<AuxiliaryHostObservation>(&stored.1).unwrap(),
                host
            );
            assert!(stored.2.is_none());
            assert!(store.has_pending_auxiliary("task").await.unwrap());
        }
    }
}

async fn crash_auxiliary_admission(mode: &str, control: &std::path::Path) -> ! {
    let (root, store, mut input, host) = auxiliary_origin_fixture("admit").await;
    if mode.starts_with("verifier") {
        input.kind = AuxiliaryKind::Verifier;
    }
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    std::fs::write(
        control.join("before.json"),
        serde_json::to_vec(&before).unwrap(),
    )
    .unwrap();
    std::fs::write(
        control.join("database"),
        root.path().join("state.sqlite3").to_str().unwrap(),
    )
    .unwrap();
    std::fs::write(
        control.join("input.json"),
        serde_json::to_vec(&input).unwrap(),
    )
    .unwrap();
    std::fs::write(
        control.join("host.json"),
        serde_json::to_vec(&host).unwrap(),
    )
    .unwrap();
    if mode.ends_with("before_commit") {
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(|update| {
                if update.table == "audit_events" {
                    // Exit in the SQLite worker without unwinding or closing its transaction.
                    std::process::exit(77);
                }
            });
    }
    assert!(
        store
            .prepare_auxiliary_with_host(&input, &host, execution_audit("crash-admission"))
            .await
            .unwrap()
    );
    assert!(mode.ends_with("after_commit"));
    std::process::exit(77);
}
