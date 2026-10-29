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
fn acknowledgement() -> ExecutionUpdate {
    ExecutionUpdate::Acknowledge {
        process: None,
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

#[tokio::test]
async fn concurrent_preparation_grants_exactly_one_spawn_and_reopen_grants_none() {
    let (directory, store) = seed().await;
    let preparation = input();
    let (first, second) = tokio::join!(
        store.prepare_execution_once(&preparation, audit("first")),
        store.prepare_execution_once(&preparation, audit("second")),
    );
    let (first_record, first_created) = first.expect("first prepare");
    let (second_record, second_created) = second.expect("second prepare");
    assert_eq!(first_record, second_record);
    assert_ne!(first_created, second_created);
    assert_eq!(audit_count(&store).await, 1);
    drop(store);
    let store = Store::open(directory.path().join("control.sqlite3"))
        .await
        .expect("reopen");
    let (recovered, created) = store
        .prepare_execution_once(&preparation, audit("restart"))
        .await
        .expect("reconcile intent");
    assert!(
        !created,
        "unobserved launch intent cannot authorize another spawn"
    );
    assert_eq!(recovered, first_record);
}

#[tokio::test]
async fn exit_and_elapsed_time_are_not_reassignment_fences() {
    let (_directory, store) = seed().await;
    assert!(!store.has_unfenced_execution("task").await.expect("empty"));
    store
        .prepare_execution(&input(), audit("prepare"))
        .await
        .expect("prepare");
    assert!(
        store
            .has_unfenced_execution("task")
            .await
            .expect("prepared")
    );
    store
        .update_execution("token", 0, "ack", &acknowledgement(), audit("ack"))
        .await
        .expect("ack");
    store
        .update_execution(
            "token",
            1,
            "exit",
            &ExecutionUpdate::Exited {
                wrapper_identity: "wrapper-nonce".into(),
                exit_code: Some(0),
            },
            audit("exit"),
        )
        .await
        .expect("exit");
    assert!(store.has_unfenced_execution("task").await.expect("exited"));
    store
        .update_execution(
            "token",
            2,
            "fence",
            &ExecutionUpdate::Fenced {
                reason: "controlled fixture fence confirmed".into(),
            },
            audit("fence"),
        )
        .await
        .expect("fence");
    assert!(!store.has_unfenced_execution("task").await.expect("fenced"));
}

#[tokio::test]
async fn blocked_execution_can_record_later_fence_without_restarting() {
    let (_directory, store) = seed().await;
    store
        .prepare_execution(&input(), audit("prepare"))
        .await
        .expect("prepare");
    store
        .update_execution(
            "token",
            0,
            "blocked",
            &ExecutionUpdate::Blocked {
                reason: "wrapper lost".into(),
            },
            audit("blocked"),
        )
        .await
        .expect("blocked");
    assert!(
        store
            .update_execution("token", 1, "ack", &acknowledgement(), audit("ack"))
            .await
            .is_err()
    );
    let fenced = store
        .update_execution(
            "token",
            1,
            "fence",
            &ExecutionUpdate::Fenced {
                reason: "independently confirmed fixture fence".into(),
            },
            audit("fence"),
        )
        .await
        .expect("reconciled fence");
    assert_eq!(fenced.state, ExecutionState::Fenced);
    assert!(!store.has_unfenced_execution("task").await.expect("clear"));
}

#[tokio::test]
async fn original_workspace_lookup_is_task_scoped_and_rejects_ambiguity() {
    let (directory, store) = seed().await;
    let mut original = input();
    let mut packet: serde_json::Value = serde_json::from_str(&original.packet_json).unwrap();
    packet["contract"]["workspace"] = "/artifacts/work/tree".into();
    original.packet_json = packet.to_string();
    original.packet_sha256 = format!("{:x}", Sha256::digest(original.packet_json.as_bytes()));
    store
        .prepare_execution(&original, audit("initial"))
        .await
        .unwrap();
    assert_eq!(
        store
            .execution_for_workspace("task", "/artifacts/work/tree")
            .await
            .unwrap()
            .unwrap()
            .preparation,
        original
    );
    assert!(
        store
            .execution_for_workspace("other", "/artifacts/work/tree")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .execution_for_workspace("task", "/artifacts/continuation/work/tree")
            .await
            .unwrap()
            .is_none()
    );
    let mut db = SqliteConnection::connect(&format!(
        "sqlite://{}",
        directory.path().join("control.sqlite3").display()
    ))
    .await
    .unwrap();
    db.execute("UPDATE attempts SET ended_at_ms=1 WHERE id='attempt';
        INSERT INTO attempts (id,task_id,worker_id,lease_epoch,instruction_hashes_json,required_checks_json,capability_hash,started_at_ms) VALUES ('attempt2','task','worker',8,'[]','[]','hash',1);
        INSERT INTO effect_intents (token,task_id,attempt_id,lease_epoch,correlation_id,effect_kind,request_json,status,runner_id,claim_deadline_ms,created_at_ms,updated_at_ms) VALUES ('token2','task','attempt2',8,'correlation','worker.spawn','{}','claimed','runner',10000,1,1);
        INSERT INTO executions (effect_token,task_id,attempt_id,lease_epoch,runner_id,packet_json,packet_sha256,state,created_at_ms,updated_at_ms) SELECT 'token2',task_id,'attempt2',8,runner_id,packet_json,packet_sha256,state,1,1 FROM executions WHERE effect_token='token';").await.unwrap();
    assert!(
        store
            .execution_for_workspace("task", "/artifacts/work/tree")
            .await
            .unwrap_err()
            .to_string()
            .contains("ambiguous workspace execution")
    );
}

#[tokio::test]
async fn evidence_links_execution_identity_without_packet_checkpoint_or_effect_token() {
    let (_directory, store) = seed().await;
    let mut preparation = input();
    let mut packet: serde_json::Value = serde_json::from_str(&preparation.packet_json).unwrap();
    packet["private"] = serde_json::json!("PRIVATE_PACKET_CONTENT");
    preparation.packet_json = packet.to_string();
    preparation.packet_sha256 = format!("{:x}", Sha256::digest(preparation.packet_json.as_bytes()));
    let bound_audit = |id| EffectAudit {
        id,
        actor_id: "runtime",
        correlation_id: "token",
        now_ms: 100,
    };
    store
        .prepare_execution(&preparation, bound_audit("prepare-evidence"))
        .await
        .unwrap();
    store
        .update_execution(
            "token",
            0,
            "start-evidence",
            &ExecutionUpdate::Acknowledge {
                process: None,
                wrapper_identity: "PRIVATE_WRAPPER".into(),
                packet_sha256: preparation.packet_sha256.clone(),
            },
            bound_audit("start-evidence"),
        )
        .await
        .unwrap();
    store
        .update_execution(
            "token",
            1,
            "checkpoint-evidence",
            &ExecutionUpdate::Checkpoint {
                wrapper_identity: "PRIVATE_WRAPPER".into(),
                payload_json: "{\"private\":\"PRIVATE_CHECKPOINT\"}".into(),
            },
            bound_audit("checkpoint-evidence"),
        )
        .await
        .unwrap();
    store
        .update_execution(
            "token",
            2,
            "exit-evidence",
            &ExecutionUpdate::Exited {
                wrapper_identity: "PRIVATE_WRAPPER".into(),
                exit_code: Some(7),
            },
            bound_audit("exit-evidence"),
        )
        .await
        .unwrap();
    assert!(store.has_unfenced_execution("task").await.unwrap());
    let rows = store.task_evidence("task", None).await.unwrap();
    assert_eq!(rows.len(), 3, "checkpoints are a separate view");
    for row in &rows {
        let raw = row.execution.as_deref().unwrap();
        let value: serde_json::Value = serde_json::from_str(raw).unwrap();
        assert_eq!(value["attempt_id"], "attempt");
        assert_eq!(value["lease_epoch"], "7");
        assert_eq!(value["current_recorded_state"], "exited");
        assert_eq!(value["recorded_exit_code"], 7);
        assert_eq!(value["packet_sha256"], preparation.packet_sha256);
        for secret in [
            "PRIVATE_PACKET_CONTENT",
            "PRIVATE_WRAPPER",
            "PRIVATE_CHECKPOINT",
            "effect_token",
        ] {
            assert!(!raw.contains(secret));
        }
    }
    assert_eq!(rows.last().unwrap().kind, "execution.prepared");
    assert!(store.task_evidence("other", None).await.unwrap().is_empty());
}

fn observed_process() -> agent_conductor_store::ExecutionProcess {
    agent_conductor_store::ExecutionProcess {
        platform: "linux".into(),
        host_id: "a".repeat(64),
        boot_id: "b".repeat(64),
        pid: 42,
        start_marker: "linux-start-ticks:12345".into(),
    }
}

#[tokio::test]
async fn native_birth_binding_rolls_back_with_ack_and_survives_reopen() {
    let (directory, store) = seed().await;
    store
        .prepare_execution(&input(), audit("prepare"))
        .await
        .unwrap();
    let path = directory.path().join("control.sqlite3");
    let mut db = SqliteConnection::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    db.execute("UPDATE recovery_authority SET authority_id='bound'; CREATE TRIGGER reject_ack BEFORE INSERT ON audit_events WHEN NEW.kind='execution.started' BEGIN SELECT RAISE(ABORT,'fixture'); END;").await.unwrap();
    let update = ExecutionUpdate::Acknowledge {
        process: Some(observed_process()),
        wrapper_identity: "wrapper".into(),
        packet_sha256: input().packet_sha256,
    };
    assert!(
        store
            .update_execution("token", 0, "ack", &update, audit("ack"))
            .await
            .is_err()
    );
    let unchanged = store.execution("token").await.unwrap().unwrap();
    assert_eq!(unchanged.state, ExecutionState::Prepared);
    assert!(unchanged.process.is_none());
    assert_eq!(unchanged.revision, 0);
    db.execute("DROP TRIGGER reject_ack").await.unwrap();
    let accepted = store
        .update_execution("token", 0, "ack", &update, audit("ack"))
        .await
        .unwrap();
    assert_eq!(accepted.process, Some(observed_process()));
    assert_eq!(
        store
            .update_execution("token", 0, "ack", &update, audit("retry"))
            .await
            .unwrap(),
        accepted
    );
    let mut changed = update.clone();
    if let ExecutionUpdate::Acknowledge {
        process: Some(process),
        ..
    } = &mut changed
    {
        process.pid += 1;
    }
    assert!(matches!(
        store
            .update_execution("token", 0, "ack", &changed, audit("collision"))
            .await,
        Err(StoreError::IdempotencyCollision)
    ));
    drop(db);
    drop(store);
    let reopened = Store::open(&path).await.unwrap();
    assert_eq!(
        reopened.execution("token").await.unwrap().unwrap().process,
        Some(observed_process())
    );
}

#[tokio::test]
async fn bound_execution_refuses_missing_or_malformed_native_birth() {
    let (directory, store) = seed().await;
    store
        .prepare_execution(&input(), audit("prepare"))
        .await
        .unwrap();
    let path = directory.path().join("control.sqlite3");
    let mut db = SqliteConnection::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    db.execute("UPDATE recovery_authority SET authority_id='bound'")
        .await
        .unwrap();
    assert!(
        store
            .update_execution("token", 0, "ack", &acknowledgement(), audit("ack"))
            .await
            .is_err()
    );
    for (pid, platform, marker) in [
        (0, "linux", "linux-start-ticks:1"),
        (42, "linux", "macos-start-us:1:2"),
        (42, "macos", "macos-start-us:1:1000000"),
        (42, "linux", "linux-start-ticks:-1"),
    ] {
        let mut process = observed_process();
        process.pid = pid;
        process.platform = platform.into();
        process.start_marker = marker.into();
        let update = ExecutionUpdate::Acknowledge {
            process: Some(process),
            wrapper_identity: "wrapper".into(),
            packet_sha256: input().packet_sha256,
        };
        assert!(
            store
                .update_execution("token", 0, "bad", &update, audit("bad"))
                .await
                .is_err()
        );
        assert_eq!(
            store.execution("token").await.unwrap().unwrap().state,
            ExecutionState::Prepared
        );
    }
    assert_eq!(audit_count(&store).await, 1);
    let legacy = serde_json::to_string(&acknowledgement()).unwrap();
    assert!(!legacy.contains("process"));
}

#[tokio::test]
async fn expected_wrapper_is_durable_intent_and_rejects_a_different_ack() {
    let (directory, store) = seed().await;
    let mut preparation = input();
    let identity = uuid::Uuid::now_v7().to_string();
    preparation.expected_wrapper_identity = Some(identity.clone());
    let (record, fresh) = store
        .prepare_execution_once(&preparation, audit("prepare"))
        .await
        .unwrap();
    assert!(fresh);
    assert_eq!(record.state, ExecutionState::Prepared);
    assert!(record.wrapper_identity.is_none());
    assert!(record.process.is_none());
    assert!(store.has_unfenced_execution("task").await.unwrap());
    let (_, fresh) = store
        .prepare_execution_once(&preparation, audit("retry"))
        .await
        .unwrap();
    assert!(
        !fresh,
        "durable intent replay must not authorize another spawn"
    );
    let mut changed = preparation.clone();
    changed.expected_wrapper_identity = Some(uuid::Uuid::now_v7().to_string());
    assert!(matches!(
        store.prepare_execution(&changed, audit("collision")).await,
        Err(StoreError::IdempotencyCollision)
    ));
    assert!(
        store
            .update_execution("token", 0, "wrong", &acknowledgement(), audit("wrong"))
            .await
            .is_err()
    );
    assert_eq!(
        store.execution("token").await.unwrap(),
        Some(record.clone())
    );
    assert_eq!(audit_count(&store).await, 1);
    drop(store);
    let store = Store::open(directory.path().join("control.sqlite3"))
        .await
        .unwrap();
    assert_eq!(store.execution("token").await.unwrap(), Some(record));
    let update = ExecutionUpdate::Acknowledge {
        wrapper_identity: identity.clone(),
        packet_sha256: preparation.packet_sha256,
        process: None,
    };
    let acknowledged = store
        .update_execution("token", 0, "ack", &update, audit("ack"))
        .await
        .unwrap();
    assert_eq!(acknowledged.wrapper_identity, Some(identity));
    assert_eq!(
        store
            .update_execution("token", 0, "ack", &update, audit("replay"))
            .await
            .unwrap(),
        acknowledged
    );
    assert_eq!(audit_count(&store).await, 2);
}

#[tokio::test]
async fn invalid_expected_identity_and_failed_audit_never_commit_intent() {
    let (directory, store) = seed().await;
    for identity in [
        "",
        "wrapper",
        "00000000-0000-0000-0000-000000000000",
        "019ABC00-0000-7000-8000-000000000001",
    ] {
        let mut preparation = input();
        preparation.expected_wrapper_identity = Some(identity.into());
        assert!(
            store
                .prepare_execution(&preparation, audit("invalid"))
                .await
                .is_err()
        );
    }
    assert!(store.execution("token").await.unwrap().is_none());
    assert_eq!(audit_count(&store).await, 0);
    let mut db = SqliteConnection::connect(&format!(
        "sqlite://{}",
        directory.path().join("control.sqlite3").display()
    ))
    .await
    .unwrap();
    db.execute("CREATE TRIGGER reject_preparation_audit BEFORE INSERT ON audit_events BEGIN SELECT RAISE(ABORT, 'injected audit refusal'); END;").await.unwrap();
    let mut preparation = input();
    preparation.expected_wrapper_identity = Some(uuid::Uuid::now_v7().to_string());
    assert!(
        store
            .prepare_execution(&preparation, audit("failed"))
            .await
            .is_err()
    );
    assert!(store.execution("token").await.unwrap().is_none());
    db.execute("DROP TRIGGER reject_preparation_audit")
        .await
        .unwrap();
    let (_, fresh) = store
        .prepare_execution_once(&preparation, audit("accepted"))
        .await
        .unwrap();
    assert!(fresh);
    db.execute("UPDATE executions SET expected_wrapper_identity='malformed'")
        .await
        .unwrap();
    assert!(
        store.execution("token").await.is_err(),
        "corrupt stored binding must fail closed"
    );
}

#[test]
fn legacy_preparation_serialization_does_not_invent_an_expected_wrapper() {
    let original = input();
    let json = serde_json::to_value(&original).unwrap();
    assert!(json.get("expected_wrapper_identity").is_none());
    let decoded: ExecutionPreparation = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, original);
}
