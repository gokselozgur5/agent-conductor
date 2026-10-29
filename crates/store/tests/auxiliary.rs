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
