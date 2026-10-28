use agent_conductor_store::{
    EffectAudit, ExecutionPreparation, ExecutionState, ExecutionUpdate, Store, StoreError,
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
fn acknowledgement() -> ExecutionUpdate {
    ExecutionUpdate::Acknowledge {
        wrapper_identity: "wrapper-nonce".into(),
        packet_sha256: input().packet_sha256,
    }
}
async fn audit_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM audit_events")
        .fetch_one(store.readers())
        .await
        .expect("audit count")
}

#[tokio::test]
async fn prepared_identity_replays_and_survives_database_reopen() {
    let (directory, store) = seed().await;
    let original = store
        .prepare_execution(&input(), audit("prepare"))
        .await
        .expect("prepare");
    let replay = store
        .prepare_execution(&input(), audit("ignored-retry"))
        .await
        .expect("retry");
    assert_eq!(original, replay);
    assert_eq!(audit_count(&store).await, 1);
    let mut changed = input();
    changed.runner_id = "other".into();
    assert!(matches!(
        store.prepare_execution(&changed, audit("collision")).await,
        Err(StoreError::IdempotencyCollision)
    ));
    drop(store);
    let store = Store::open(directory.path().join("control.sqlite3"))
        .await
        .expect("reopen");
    assert_eq!(
        store.execution("token").await.expect("read"),
        Some(original)
    );
}

#[tokio::test]
async fn wrapper_hash_revision_and_receipt_are_fenced_atomically() {
    let (_directory, store) = seed().await;
    store
        .prepare_execution(&input(), audit("prepare"))
        .await
        .expect("prepare");
    let mut wrong = acknowledgement();
    if let ExecutionUpdate::Acknowledge { packet_sha256, .. } = &mut wrong {
        *packet_sha256 = "wrong".into();
    }
    assert!(
        store
            .update_execution("token", 0, "wrong", &wrong, audit("wrong"))
            .await
            .is_err()
    );
    assert_eq!(audit_count(&store).await, 1);
    let started = store
        .update_execution("token", 0, "ack", &acknowledgement(), audit("ack"))
        .await
        .expect("ack");
    assert_eq!(started.state, ExecutionState::Started);
    assert_eq!(
        store
            .update_execution("token", 0, "ack", &acknowledgement(), audit("retry"))
            .await
            .expect("replay"),
        started
    );
    let checkpoint = ExecutionUpdate::Checkpoint {
        wrapper_identity: "imposter".into(),
        payload_json: "{}".into(),
    };
    assert!(
        store
            .update_execution("token", 1, "checkpoint", &checkpoint, audit("checkpoint"))
            .await
            .is_err()
    );
    assert!(
        store
            .update_execution("token", 0, "stale", &acknowledgement(), audit("stale"))
            .await
            .is_err()
    );
    assert_eq!(audit_count(&store).await, 2);
    assert_eq!(
        store
            .execution("token")
            .await
            .expect("read")
            .expect("record"),
        started
    );
}

#[tokio::test]
async fn cancelled_or_stale_attempt_cannot_prepare_or_acknowledge() {
    for prepare_first in [false, true] {
        let (directory, store) = seed().await;
        if prepare_first {
            store
                .prepare_execution(&input(), audit("prepare"))
                .await
                .expect("prepare");
        }
        let mut connection = SqliteConnection::connect(&format!(
            "sqlite://{}",
            directory.path().join("control.sqlite3").display()
        ))
        .await
        .expect("connect");
        connection.execute("UPDATE tasks SET state='cancelled'; UPDATE leases SET status='released'; UPDATE attempts SET ended_at_ms=50;").await.expect("cancel fixture");
        if prepare_first {
            assert!(
                store
                    .update_execution("token", 0, "ack", &acknowledgement(), audit("ack"))
                    .await
                    .is_err()
            );
            assert_eq!(
                store
                    .execution("token")
                    .await
                    .expect("read")
                    .expect("record")
                    .state,
                ExecutionState::Prepared
            );
        } else {
            assert!(
                store
                    .prepare_execution(&input(), audit("prepare"))
                    .await
                    .is_err()
            );
        }
    }
}

#[tokio::test]
async fn audit_failure_rolls_back_record_and_allows_next_transaction() {
    let (_directory, store) = seed().await;
    store
        .prepare_execution(&input(), audit("same-audit"))
        .await
        .expect("prepare");
    assert!(
        store
            .update_execution("token", 0, "ack", &acknowledgement(), audit("same-audit"))
            .await
            .is_err()
    );
    let row = store
        .execution("token")
        .await
        .expect("read")
        .expect("record");
    assert_eq!(row.revision, 0);
    assert_eq!(row.state, ExecutionState::Prepared);
    store
        .update_execution("token", 0, "ack", &acknowledgement(), audit("new-audit"))
        .await
        .expect("rollback releases writer");
    assert_eq!(audit_count(&store).await, 2);
}

#[tokio::test]
async fn checkpoint_survives_fencing_and_terminal_attempt_cannot_restart() {
    let (_directory, store) = seed().await;
    store
        .prepare_execution(&input(), audit("prepare"))
        .await
        .expect("prepare");
    store
        .update_execution("token", 0, "ack", &acknowledgement(), audit("ack"))
        .await
        .expect("ack");
    let checkpoint = ExecutionUpdate::Checkpoint {
        wrapper_identity: "wrapper-nonce".into(),
        payload_json: r#"{"next":"test dirty work","snapshot":"hash"}"#.into(),
    };
    let saved = store
        .update_execution("token", 1, "checkpoint", &checkpoint, audit("checkpoint"))
        .await
        .expect("checkpoint");
    let fence = ExecutionUpdate::Fenced {
        reason: "supervisor lost lease".into(),
    };
    let fenced = store
        .update_execution("token", 2, "fence", &fence, audit("fence"))
        .await
        .expect("fence");
    assert_eq!(fenced.checkpoint_json, saved.checkpoint_json);
    assert_eq!(fenced.state, ExecutionState::Fenced);
    assert!(
        store
            .update_execution("token", 3, "restart", &acknowledgement(), audit("restart"))
            .await
            .is_err()
    );
    assert!(
        store
            .update_execution(
                "token",
                3,
                "late-checkpoint",
                &checkpoint,
                audit("late-checkpoint")
            )
            .await
            .is_err()
    );
    assert_eq!(audit_count(&store).await, 4);
}
