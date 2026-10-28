//! Deterministic cancellation at real SQLite writes, not timing-only aborts.
use super::*;
use sha2::{Digest, Sha256};
use sqlx::{Executor, Row};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc,
};

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
                        wrapper_identity: "wrapper".into(),
                        packet_sha256: preparation().packet_sha256,
                    },
                    execution_audit("ack")
                )
                .await
        ),
    }
}
async fn snapshot(connection: &mut SqliteConnection) -> Vec<(String, String)> {
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
