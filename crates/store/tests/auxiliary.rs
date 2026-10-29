use agent_conductor_store::{
    AuxiliaryKind, AuxiliaryPreparation, EffectAudit, ExecutionPreparation, Store, StoreError,
};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Executor, SqliteConnection};

async fn seed() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().expect("directory");
    let path = directory.path().join("control.sqlite3");
    let store = Store::open(&path).await.expect("store");
    let mut connection = SqliteConnection::connect(&format!("sqlite://{}", path.display()))
        .await
        .expect("seed connection");
    connection.execute("INSERT INTO projects VALUES ('p','/project','device:inode',NULL,0);
        INSERT INTO agents VALUES ('worker','fake','v1','hash','read_write',0);
        INSERT INTO tasks (id,project_id,objective,state,priority,version,authority_json,created_at_ms,updated_at_ms,aggregate_json) VALUES ('task','p','fixture','running',0,2,'{}',0,0,'{}');
        INSERT INTO attempts (id,task_id,worker_id,lease_epoch,instruction_hashes_json,required_checks_json,capability_hash,started_at_ms) VALUES ('attempt','task','worker',7,'[]','[]','hash',0);
        INSERT INTO leases VALUES ('attempt','task',7,'active',1,10000,1000,10000,0);
        INSERT INTO effect_intents (token,task_id,attempt_id,lease_epoch,correlation_id,effect_kind,request_json,status,runner_id,claim_deadline_ms,created_at_ms,updated_at_ms) VALUES ('token','task','attempt',7,'correlation','worker.spawn','{}','claimed','runner',10000,0,0);")
        .await.expect("seed identities");
    (directory, store)
}
fn input() -> ExecutionPreparation {
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
fn audit(id: &str) -> EffectAudit<'_> {
    EffectAudit {
        id,
        actor_id: "runtime",
        correlation_id: "correlation",
        now_ms: 100,
    }
}

async fn fixture() -> (
    tempfile::TempDir,
    Store,
    AuxiliaryPreparation,
    SqliteConnection,
) {
    let (dir, store) = seed().await;
    store
        .prepare_execution(&input(), audit("writer"))
        .await
        .unwrap();
    let mut connection = SqliteConnection::connect(&format!(
        "sqlite://{}",
        dir.path().join("control.sqlite3").display()
    ))
    .await
    .unwrap();
    let source = format!("snapshot:{}", "a".repeat(64));
    let aggregate =
        serde_json::json!({"attempt":{"id":"attempt"},"verification":{"source":source}})
            .to_string();
    sqlx::query("UPDATE tasks SET state='verifying',aggregate_json=?")
        .bind(aggregate)
        .execute(&mut connection)
        .await
        .unwrap();
    connection
        .execute(
            "UPDATE executions SET state='fenced'; UPDATE runtime_metadata SET boot_generation=1;",
        )
        .await
        .unwrap();
    let preparation = AuxiliaryPreparation {
        invocation_id: uuid::Uuid::now_v7().to_string(),
        task_id: "task".into(),
        execution_token: "token".into(),
        attempt_id: "attempt".into(),
        task_version: 2,
        boot_generation: 1,
        kind: AuxiliaryKind::Check,
        source,
        definition_sha256: "b".repeat(64),
        workspace: "/isolated/check".into(),
    };
    (dir, store, preparation, connection)
}

#[tokio::test]
async fn replay_never_grants_another_spawn_and_pending_survives_reopen() {
    let (dir, store, preparation, _connection) = fixture().await;
    assert!(
        store
            .prepare_auxiliary_once(&preparation, audit("prepare"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .prepare_auxiliary_once(&preparation, audit("replay"))
            .await
            .unwrap()
    );
    let mut changed = preparation.clone();
    changed.kind = AuxiliaryKind::Verifier;
    assert!(matches!(
        store
            .prepare_auxiliary_once(&changed, audit("collision"))
            .await,
        Err(StoreError::IdempotencyCollision)
    ));
    drop(store);
    let store = Store::open(dir.path().join("control.sqlite3"))
        .await
        .unwrap();
    assert!(store.has_pending_auxiliary("task").await.unwrap());
    assert!(
        !store
            .prepare_auxiliary_once(&preparation, audit("reopen"))
            .await
            .unwrap()
    );
    store
        .settle_auxiliary(&preparation, audit("settle"))
        .await
        .unwrap();
    store
        .settle_auxiliary(&preparation, audit("settle-replay"))
        .await
        .unwrap();
    assert!(!store.has_pending_auxiliary("task").await.unwrap());
    assert!(
        !store
            .prepare_auxiliary_once(&preparation, audit("terminal-replay"))
            .await
            .unwrap()
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE kind LIKE 'auxiliary.%'")
            .fetch_one(store.readers())
            .await
            .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn stale_source_version_boot_and_unfenced_writer_refuse_admission() {
    let (_dir, store, preparation, mut connection) = fixture().await;
    for field in 0..5 {
        let mut input = preparation.clone();
        match field {
            0 => input.source = format!("snapshot:{}", "c".repeat(64)),
            1 => input.task_version += 1,
            2 => input.boot_generation += 1,
            3 => input.attempt_id = "other".into(),
            _ => input.workspace = "/root/../escape".into(),
        }
        assert!(
            store
                .prepare_auxiliary_once(&input, audit("invalid"))
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE executions SET state='started'")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(
        store
            .prepare_auxiliary_once(&preparation, audit("unfenced"))
            .await
            .is_err()
    );
    assert!(!store.has_pending_auxiliary("task").await.unwrap());
}

#[tokio::test]
async fn pending_owner_excludes_another_invocation_and_new_boot_cannot_settle_it() {
    let (_dir, store, preparation, mut connection) = fixture().await;
    store
        .prepare_auxiliary_once(&preparation, audit("prepare"))
        .await
        .unwrap();
    let mut other = preparation.clone();
    other.invocation_id = uuid::Uuid::now_v7().to_string();
    assert!(
        store
            .prepare_auxiliary_once(&other, audit("other"))
            .await
            .is_err()
    );
    sqlx::query("UPDATE runtime_metadata SET boot_generation=2")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(
        store
            .settle_auxiliary(&preparation, audit("stale-settle"))
            .await
            .is_err()
    );
    assert!(store.has_pending_auxiliary("task").await.unwrap());
}

#[tokio::test]
async fn audit_failure_rolls_back_admission_and_settlement() {
    let (_dir, store, preparation, mut connection) = fixture().await;
    sqlx::query("CREATE TRIGGER reject_aux BEFORE INSERT ON audit_events WHEN NEW.kind LIKE 'auxiliary.%' BEGIN SELECT RAISE(ABORT,'injected'); END;").execute(&mut connection).await.unwrap();
    assert!(
        store
            .prepare_auxiliary_once(&preparation, audit("prepare-fail"))
            .await
            .is_err()
    );
    assert!(!store.has_pending_auxiliary("task").await.unwrap());
    sqlx::query("DROP TRIGGER reject_aux")
        .execute(&mut connection)
        .await
        .unwrap();
    store
        .prepare_auxiliary_once(&preparation, audit("prepare"))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_aux BEFORE INSERT ON audit_events WHEN NEW.kind='auxiliary.settled' BEGIN SELECT RAISE(ABORT,'injected'); END;").execute(&mut connection).await.unwrap();
    assert!(
        store
            .settle_auxiliary(&preparation, audit("settle-fail"))
            .await
            .is_err()
    );
    assert!(store.has_pending_auxiliary("task").await.unwrap());
}

fn binding() -> agent_conductor_store::AuxiliaryLaunchBinding {
    agent_conductor_store::AuxiliaryLaunchBinding {
        launch_sha256: "c".repeat(64),
        authority_id: "d".repeat(64),
        receipt_root: "/private/receipts".into(),
        wrapper_path: "/private/wrapper".into(),
        wrapper_sha256: "e".repeat(64),
    }
}
fn process() -> agent_conductor_store::ExecutionProcess {
    agent_conductor_store::ExecutionProcess {
        platform: "linux".into(),
        host_id: "a".repeat(64),
        boot_id: "b".repeat(64),
        pid: 123,
        start_marker: "linux-start-ticks:12".into(),
    }
}
fn proof(input: &AuxiliaryPreparation) -> String {
    let binding = binding();
    let process = process();
    serde_json::json!({"version":1,"invocation_id":input.invocation_id,"authority_id":binding.authority_id,"boot_generation":input.boot_generation,"launch_sha256":binding.launch_sha256,"process":{"host":{"platform":process.platform,"host_id":process.host_id,"boot_id":process.boot_id},"pid":process.pid,"start_marker":process.start_marker},"observation":"owned_group_settled"}).to_string()
}

#[tokio::test]
async fn launch_and_release_replay_never_grant_another_external_action() {
    let (_dir, store, input, mut connection) = fixture().await;
    sqlx::query("UPDATE recovery_authority SET authority_id=?")
        .bind(binding().authority_id)
        .execute(&mut connection)
        .await
        .unwrap();
    store
        .prepare_auxiliary_once(&input, audit("prepare"))
        .await
        .unwrap();
    assert!(
        store
            .authorize_auxiliary_release(&input, &binding(), &process(), audit("no-launch"))
            .await
            .is_err()
    );
    assert!(
        store
            .bind_auxiliary_launch(&input, &binding(), audit("bind"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .bind_auxiliary_launch(&input, &binding(), audit("bind-replay"))
            .await
            .unwrap()
    );
    let mut changed = binding();
    changed.launch_sha256 = "f".repeat(64);
    assert!(
        store
            .bind_auxiliary_launch(&input, &changed, audit("changed"))
            .await
            .is_err()
    );
    sqlx::query("UPDATE tasks SET version=3")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(
        store
            .authorize_auxiliary_release(&input, &binding(), &process(), audit("stale-task"))
            .await
            .is_err()
    );
    sqlx::query("UPDATE tasks SET version=2")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(
        store
            .authorize_auxiliary_release(&input, &binding(), &process(), audit("release"))
            .await
            .unwrap()
    );
    assert!(
        !store
            .authorize_auxiliary_release(&input, &binding(), &process(), audit("release-replay"))
            .await
            .unwrap()
    );
    let mut changed = process();
    changed.pid += 1;
    assert!(
        store
            .authorize_auxiliary_release(&input, &binding(), &changed, audit("pid-changed"))
            .await
            .is_err()
    );
    assert!(
        store
            .settle_auxiliary(&input, audit("unproven-settlement"))
            .await
            .is_err()
    );
    let pending = store.pending_auxiliary_invocations().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert!(pending[0].release_recorded);
    assert_eq!(pending[0].process, Some(process()));
}

#[tokio::test]
async fn verified_old_boot_settlement_requires_current_authority_and_atomic_audit() {
    let (_dir, store, input, mut connection) = fixture().await;
    sqlx::query("UPDATE recovery_authority SET authority_id=?")
        .bind(binding().authority_id)
        .execute(&mut connection)
        .await
        .unwrap();
    store
        .prepare_auxiliary_once(&input, audit("prepare"))
        .await
        .unwrap();
    store
        .bind_auxiliary_launch(&input, &binding(), audit("bind"))
        .await
        .unwrap();
    store
        .authorize_auxiliary_release(&input, &binding(), &process(), audit("release"))
        .await
        .unwrap();
    sqlx::query("UPDATE runtime_metadata SET boot_generation=2")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(
        store
            .settle_auxiliary_proof(
                &input,
                &binding(),
                &process(),
                1,
                &proof(&input),
                audit("stale-runtime")
            )
            .await
            .is_err()
    );
    assert!(
        store
            .settle_auxiliary_proof(
                &input,
                &binding(),
                &process(),
                2,
                "{}",
                audit("unbound-proof")
            )
            .await
            .is_err()
    );
    sqlx::query("CREATE TRIGGER reject_aux_proof BEFORE INSERT ON audit_events WHEN NEW.kind='auxiliary.proof_settled' BEGIN SELECT RAISE(ABORT,'injected'); END;").execute(&mut connection).await.unwrap();
    assert!(
        store
            .settle_auxiliary_proof(
                &input,
                &binding(),
                &process(),
                2,
                &proof(&input),
                audit("audit-failure")
            )
            .await
            .is_err()
    );
    assert!(store.has_pending_auxiliary("task").await.unwrap());
    sqlx::query("DROP TRIGGER reject_aux_proof")
        .execute(&mut connection)
        .await
        .unwrap();
    store
        .settle_auxiliary_proof(
            &input,
            &binding(),
            &process(),
            2,
            &proof(&input),
            audit("settle"),
        )
        .await
        .unwrap();
    store
        .settle_auxiliary_proof(
            &input,
            &binding(),
            &process(),
            2,
            &proof(&input),
            audit("settle-replay"),
        )
        .await
        .unwrap();
    assert!(!store.has_pending_auxiliary("task").await.unwrap());
}

#[tokio::test]
async fn no_launch_abandonment_requires_current_unbound_owner_and_atomic_audit() {
    let (_dir, store, preparation, mut connection) = fixture().await;
    store
        .prepare_auxiliary_once(&preparation, audit("prepare"))
        .await
        .unwrap();
    connection.execute("CREATE TRIGGER reject_no_launch BEFORE INSERT ON audit_events WHEN NEW.kind='auxiliary.not_launched' BEGIN SELECT RAISE(ABORT,'injected'); END;").await.unwrap();
    assert!(
        store
            .abandon_auxiliary_before_launch(&preparation, audit("failed"))
            .await
            .is_err()
    );
    assert!(store.has_pending_auxiliary("task").await.unwrap());
    connection
        .execute("DROP TRIGGER reject_no_launch; UPDATE runtime_metadata SET boot_generation=2;")
        .await
        .unwrap();
    assert!(
        store
            .abandon_auxiliary_before_launch(&preparation, audit("stale"))
            .await
            .is_err()
    );
    connection
        .execute("UPDATE runtime_metadata SET boot_generation=1;")
        .await
        .unwrap();
    let mut changed = preparation.clone();
    changed.definition_sha256 = "c".repeat(64);
    assert!(
        store
            .abandon_auxiliary_before_launch(&changed, audit("changed"))
            .await
            .is_err()
    );
    store
        .abandon_auxiliary_before_launch(&preparation, audit("abandon"))
        .await
        .unwrap();
    store
        .abandon_auxiliary_before_launch(&preparation, audit("replay"))
        .await
        .unwrap();
    assert!(!store.has_pending_auxiliary("task").await.unwrap());
    assert!(
        !store
            .prepare_auxiliary_once(&preparation, audit("no-respawn"))
            .await
            .unwrap()
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE kind='auxiliary.not_launched'")
            .fetch_one(store.readers())
            .await
            .unwrap();
    assert_eq!(count, 1);

    for column in ["launch_binding_json", "process_json", "release_recorded"] {
        let mut next = preparation.clone();
        next.invocation_id = uuid::Uuid::now_v7().to_string();
        store
            .prepare_auxiliary_once(&next, audit(column))
            .await
            .unwrap();
        let value = if column == "release_recorded" {
            "1"
        } else {
            "'{}'"
        };
        sqlx::query(&format!(
            "UPDATE auxiliary_invocations SET {column}={value} WHERE invocation_id=?"
        ))
        .bind(&next.invocation_id)
        .execute(&mut connection)
        .await
        .unwrap();
        assert!(
            store
                .abandon_auxiliary_before_launch(&next, audit("refuse-bound"))
                .await
                .is_err()
        );
        assert!(store.has_pending_auxiliary("task").await.unwrap());
        // Restore this synthetic corruption before testing the next independent gate.
        sqlx::query("DELETE FROM auxiliary_invocations WHERE invocation_id=?")
            .bind(&next.invocation_id)
            .execute(&mut connection)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn unreleased_recovery_preserves_absent_ack_and_requires_current_authority() {
    let (_dir, store, input, mut connection) = fixture().await;
    sqlx::query("UPDATE recovery_authority SET authority_id=?")
        .bind(binding().authority_id)
        .execute(&mut connection)
        .await
        .unwrap();
    store
        .prepare_auxiliary_once(&input, audit("prepare"))
        .await
        .unwrap();
    store
        .bind_auxiliary_launch(&input, &binding(), audit("bind"))
        .await
        .unwrap();
    let proof = proof(&input).replace("owned_group_settled", "never_released");
    connection
        .execute("UPDATE runtime_metadata SET boot_generation=2")
        .await
        .unwrap();
    assert!(
        store
            .settle_auxiliary_unreleased_proof(&input, &binding(), 1, &proof, audit("stale"))
            .await
            .is_err()
    );
    for invalid in [
        proof.replace("never_released", "owned_group_settled"),
        proof.replace(&input.invocation_id, &uuid::Uuid::now_v7().to_string()),
        "{}".into(),
    ] {
        assert!(
            store
                .settle_auxiliary_unreleased_proof(
                    &input,
                    &binding(),
                    2,
                    &invalid,
                    audit("wrong-proof")
                )
                .await
                .is_err()
        );
    }
    sqlx::query("UPDATE recovery_authority SET authority_id=?")
        .bind("f".repeat(64))
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(
        store
            .settle_auxiliary_unreleased_proof(
                &input,
                &binding(),
                2,
                &proof,
                audit("wrong-authority")
            )
            .await
            .is_err()
    );
    sqlx::query("UPDATE recovery_authority SET authority_id=?")
        .bind(binding().authority_id)
        .execute(&mut connection)
        .await
        .unwrap();
    connection.execute("CREATE TRIGGER reject_unreleased BEFORE INSERT ON audit_events WHEN NEW.kind='auxiliary.proof_settled' BEGIN SELECT RAISE(ABORT,'injected'); END;").await.unwrap();
    assert!(
        store
            .settle_auxiliary_unreleased_proof(&input, &binding(), 2, &proof, audit("rollback"))
            .await
            .is_err()
    );
    assert!(store.has_pending_auxiliary("task").await.unwrap());
    connection
        .execute("DROP TRIGGER reject_unreleased")
        .await
        .unwrap();
    store
        .settle_auxiliary_unreleased_proof(&input, &binding(), 2, &proof, audit("settle"))
        .await
        .unwrap();
    store
        .settle_auxiliary_unreleased_proof(&input, &binding(), 2, &proof, audit("replay"))
        .await
        .unwrap();
    assert!(!store.has_pending_auxiliary("task").await.unwrap());
    let observed: (Option<String>, i64) =
        sqlx::query_as("SELECT process_json,release_recorded FROM auxiliary_invocations")
            .fetch_one(store.readers())
            .await
            .unwrap();
    assert_eq!(observed, (None, 0));
    let changed = proof.replace("linux-start-ticks:12", "linux-start-ticks:13");
    assert!(
        store
            .settle_auxiliary_unreleased_proof(&input, &binding(), 2, &changed, audit("changed"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn unreleased_shortcut_refuses_an_acknowledged_invocation() {
    let (_dir, store, input, mut connection) = fixture().await;
    sqlx::query("UPDATE recovery_authority SET authority_id=?")
        .bind(binding().authority_id)
        .execute(&mut connection)
        .await
        .unwrap();
    store
        .prepare_auxiliary_once(&input, audit("prepare"))
        .await
        .unwrap();
    store
        .bind_auxiliary_launch(&input, &binding(), audit("bind"))
        .await
        .unwrap();
    store
        .authorize_auxiliary_release(&input, &binding(), &process(), audit("release"))
        .await
        .unwrap();
    let proof = proof(&input).replace("owned_group_settled", "never_released");
    assert!(
        store
            .settle_auxiliary_unreleased_proof(&input, &binding(), 1, &proof, audit("refuse"))
            .await
            .is_err()
    );
    assert!(store.has_pending_auxiliary("task").await.unwrap());
    // An acknowledged pre-release failure still uses its independently bound identity.
    store
        .settle_auxiliary_proof(
            &input,
            &binding(),
            &process(),
            1,
            &proof,
            audit("observed-settle"),
        )
        .await
        .unwrap();
}

fn publication(input: &AuxiliaryPreparation, key: &str) -> agent_conductor_store::CommitBatch {
    use agent_conductor_store::{AuditAppend, CommitBatch, TaskMutation, TaskTransitionAppend};
    let kind = if input.kind == AuxiliaryKind::Check {
        "record_check"
    } else {
        "record_verdict"
    };
    let payload=serde_json::json!([input.task_id,{"actor":{"id":"publisher","role":"system"},"expected_version":input.task_version,"correlation_id":input.invocation_id,"idempotency_key":key,"reason":"bound result","kind":{"type":kind,"source":input.source,"check_id":"test","passed":true,"accepted":true}}]).to_string();
    CommitBatch {
        actor_id:"publisher".into(),idempotency_key:key.into(),request_hash:format!("{:x}",Sha256::digest(payload.as_bytes())),
        task:Some(TaskMutation{task_id:input.task_id.clone(),expected_version:input.task_version,new_version:input.task_version+1,state:"verifying".into(),aggregate_json:serde_json::json!({"id":input.task_id,"version":input.task_version+1,"state":"verifying","attempt":{"id":input.attempt_id},"verification":{"source":input.source}}).to_string(),updated_at_ms:100}),
        project:None,agent:None,new_task:None,attempt:None,
        transitions:vec![TaskTransitionAppend{id:format!("transition:{key}"),task_id:input.task_id.clone(),actor_id:"publisher".into(),prior_state:"verifying".into(),new_state:"verifying".into(),reason:"bound result".into(),correlation_id:input.invocation_id.clone(),occurred_at_ms:100}],
        audit:vec![AuditAppend{id:format!("audit:{key}"),task_id:Some(input.task_id.clone()),actor_id:"publisher".into(),kind:kind.into(),correlation_id:input.invocation_id.clone(),payload_json:payload,occurred_at_ms:100}],
        effects:vec![],outcome_json:"{}".into(),committed_at_ms:100,expires_at_ms:10000,
    }
}
async fn settled_publication_fixture(
    kind: AuxiliaryKind,
    observation: &str,
) -> (
    tempfile::TempDir,
    Store,
    AuxiliaryPreparation,
    SqliteConnection,
) {
    let (dir, store, mut input, mut connection) = fixture().await;
    input.kind = kind;
    sqlx::query("UPDATE recovery_authority SET authority_id=?")
        .bind(binding().authority_id)
        .execute(&mut connection)
        .await
        .unwrap();
    store
        .prepare_auxiliary_once(&input, audit("prepare"))
        .await
        .unwrap();
    store
        .bind_auxiliary_launch(&input, &binding(), audit("bind"))
        .await
        .unwrap();
    store
        .authorize_auxiliary_release(&input, &binding(), &process(), audit("release"))
        .await
        .unwrap();
    let proof = proof(&input).replace("owned_group_settled", observation);
    store
        .settle_auxiliary_proof(&input, &binding(), &process(), 1, &proof, audit("settle"))
        .await
        .unwrap();
    (dir, store, input, connection)
}
#[tokio::test]
async fn publication_links_one_result_atomically_and_replays_without_duplicate_audit() {
    use agent_conductor_store::CommitResult;
    for kind in [AuxiliaryKind::Check, AuxiliaryKind::Verifier] {
        let (_dir, store, input, mut connection) =
            settled_publication_fixture(kind, "owned_group_settled").await;
        let batch = publication(&input, "result");
        connection.execute("CREATE TRIGGER fail_publication BEFORE INSERT ON auxiliary_publications BEGIN SELECT RAISE(ABORT,'injected'); END;").await.unwrap();
        assert!(
            store
                .persist_auxiliary_publication(&input, &batch)
                .await
                .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT version FROM tasks WHERE id='task'")
                .fetch_one(store.readers())
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM audit_events WHERE id='audit:result'"
            )
            .fetch_one(store.readers())
            .await
            .unwrap(),
            0
        );
        assert!(
            store
                .command_outcome("publisher", "result", &batch.request_hash)
                .await
                .unwrap()
                .is_none()
        );
        connection
            .execute("DROP TRIGGER fail_publication")
            .await
            .unwrap();
        assert!(matches!(
            store
                .persist_auxiliary_publication(&input, &batch)
                .await
                .unwrap(),
            CommitResult::Applied { .. }
        ));
        assert!(matches!(
            store
                .persist_auxiliary_publication(&input, &batch)
                .await
                .unwrap(),
            CommitResult::Replayed { .. }
        ));
        assert!(matches!(
            store
                .persist_auxiliary_publication(&input, &publication(&input, "another-result"))
                .await,
            Err(StoreError::IdempotencyCollision)
        ));
        let row:(String,String)=sqlx::query_as("SELECT p.invocation_id,a.correlation_id FROM auxiliary_publications p JOIN audit_events a ON a.id=p.audit_id").fetch_one(store.readers()).await.unwrap();
        assert_eq!(row, (input.invocation_id.clone(), input.invocation_id));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT version FROM tasks WHERE id='task'")
                .fetch_one(store.readers())
                .await
                .unwrap(),
            3
        );
    }
}
#[tokio::test]
async fn publication_refuses_unsettled_unreleased_stale_and_changed_scope() {
    let (_dir, store, input, _connection) = fixture().await;
    store
        .prepare_auxiliary_once(&input, audit("prepare"))
        .await
        .unwrap();
    assert!(
        store
            .persist_auxiliary_publication(&input, &publication(&input, "pending"))
            .await
            .is_err()
    );
    let (_dir, store, input, _connection) =
        settled_publication_fixture(AuxiliaryKind::Check, "never_released").await;
    assert!(
        store
            .persist_auxiliary_publication(&input, &publication(&input, "unreleased"))
            .await
            .is_err()
    );
    let (_dir, store, input, mut c) =
        settled_publication_fixture(AuxiliaryKind::Check, "owned_group_settled").await;
    let batch = publication(&input, "result");
    for sql in [
        "UPDATE runtime_metadata SET boot_generation=2",
        "UPDATE tasks SET version=3",
        "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.verification.source','changed')",
        "UPDATE auxiliary_invocations SET process_json=json_set(process_json,'$.pid',124)",
    ] {
        c.execute("SAVEPOINT changed_scope").await.unwrap();
        c.execute(sql).await.unwrap();
        // The store uses another connection: publish the altered fixture state.
        c.execute("RELEASE changed_scope").await.unwrap();
        assert!(
            store
                .persist_auxiliary_publication(&input, &batch)
                .await
                .is_err()
        );
        c.execute("UPDATE runtime_metadata SET boot_generation=1; UPDATE tasks SET version=2;")
            .await
            .unwrap();
        sqlx::query(
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.verification.source',?)",
        )
        .bind(&input.source)
        .execute(&mut c)
        .await
        .unwrap();
        sqlx::query("UPDATE auxiliary_invocations SET process_json=?")
            .bind(serde_json::to_string(&process()).unwrap())
            .execute(&mut c)
            .await
            .unwrap();
    }
    for change in 0..3 {
        let mut bad = batch.clone();
        match change {
            0 => bad.audit[0].correlation_id = "another-invocation".into(),
            1 => bad.audit[0].kind = "record_verdict".into(),
            _ => bad.request_hash = "0".repeat(64),
        }
        assert!(
            store
                .persist_auxiliary_publication(&input, &bad)
                .await
                .is_err()
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT version FROM tasks WHERE id='task'")
            .fetch_one(store.readers())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM auxiliary_publications")
            .fetch_one(store.readers())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn pending_recovery_remains_bounded_without_refusing_large_inventory() {
    let (_dir, store, template, mut c) = fixture().await;
    for i in 0..130 {
        let id = format!("job-{i:03}");
        sqlx::query("INSERT INTO tasks(id,project_id,objective,state,priority,version,authority_json,created_at_ms,updated_at_ms,aggregate_json) VALUES(?,'p','fixture','verifying',0,2,'{}',0,0,'{}')").bind(&id).execute(&mut c).await.unwrap();
        sqlx::query("INSERT INTO attempts(id,task_id,worker_id,lease_epoch,instruction_hashes_json,required_checks_json,capability_hash,started_at_ms) VALUES(?,?,'worker',7,'[]','[]','hash',0)").bind(&id).bind(&id).execute(&mut c).await.unwrap();
        sqlx::query("INSERT INTO effect_intents(token,task_id,attempt_id,lease_epoch,correlation_id,effect_kind,request_json,status,created_at_ms,updated_at_ms) VALUES(?,?,?,7,'fixture','worker.spawn','{}','done',0,0)").bind(&id).bind(&id).bind(&id).execute(&mut c).await.unwrap();
        sqlx::query("INSERT INTO executions(effect_token,task_id,attempt_id,lease_epoch,runner_id,packet_json,packet_sha256,state,created_at_ms,updated_at_ms) VALUES(?,?,?,7,'runner','{}','hash','fenced',0,0)").bind(&id).bind(&id).bind(&id).execute(&mut c).await.unwrap();
        let mut input = template.clone();
        input.task_id = id.clone();
        input.attempt_id = id.clone();
        input.execution_token = id.clone();
        input.invocation_id = uuid::Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO auxiliary_invocations(invocation_id,task_id,execution_token,preparation_json,boot_generation,state,created_at_ms) VALUES(?,?,?,?,1,'pending',0)").bind(&input.invocation_id).bind(&id).bind(&id).bind(serde_json::to_string(&input).unwrap()).execute(&mut c).await.unwrap();
    }
    let first = store
        .pending_auxiliary_invocations()
        .await
        .expect("a large inventory must permit bounded recovery");
    assert_eq!(first.len(), 128);
    let cursor = first.last().unwrap().preparation.invocation_id.clone();
    let second = store.pending_auxiliary_page(Some(&cursor)).await.unwrap();
    assert_eq!(second.len(), 2);
    assert!(
        second
            .iter()
            .all(|row| row.invocation.is_ok() && row.invocation_id > cursor)
    );
    assert!(
        store
            .pending_auxiliary_page(Some(&second.last().unwrap().invocation_id))
            .await
            .unwrap()
            .is_empty()
    );
    let first_id = &first[0].preparation.invocation_id;
    for sql in [
        "UPDATE auxiliary_invocations SET preparation_json='{}' WHERE invocation_id=?",
        "UPDATE auxiliary_invocations SET preparation_json=json_set(preparation_json,'$.task_id','foreign') WHERE invocation_id=?",
        "UPDATE auxiliary_invocations SET launch_binding_json=json_quote(printf('%20000s','x')) WHERE invocation_id=?",
    ] {
        sqlx::query("UPDATE auxiliary_invocations SET preparation_json=?,launch_binding_json=NULL WHERE invocation_id=?").bind(serde_json::to_string(&first[0].preparation).unwrap()).bind(first_id).execute(&mut c).await.unwrap();
        sqlx::query(sql)
            .bind(first_id)
            .execute(&mut c)
            .await
            .unwrap();
        let page = store.pending_auxiliary_page(None).await.unwrap();
        assert_eq!(page.len(), 128);
        assert!(page[0].invocation.is_err());
        assert_eq!(
            page.iter().filter(|row| row.invocation.is_ok()).count(),
            127
        );
        assert!(store.pending_auxiliary_invocations().await.is_err());
        assert!(
            store
                .has_pending_auxiliary(&first[0].preparation.task_id)
                .await
                .unwrap()
        );
    }
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM auxiliary_invocations WHERE state='pending'")
            .fetch_one(&mut c)
            .await
            .unwrap();
    assert_eq!(count, 130);
}
