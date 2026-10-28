// Included in effect_tests to reuse its real lifecycle fixtures and controlled clock.
async fn checkpoint_seed_execution(
    store: &Store,
    token: &str,
    packet: &conductor_runtime::ResumePacket,
) {
    use agent_conductor_store::{EffectAudit, ExecutionPreparation, ExecutionUpdate};
    let claim_id = format!("checkpoint-claim:{token}");
    let prepare_id = format!("checkpoint-prepare:{token}");
    let ack_id = format!("checkpoint-ack:{token}");
    let audit = |id| EffectAudit {
        id,
        actor_id: "runtime",
        correlation_id: token,
        now_ms: 100,
    };
    store
        .claim_effect(token, "runner", 10000, audit(&claim_id))
        .await
        .unwrap();
    let prepared = store
        .prepare_execution(
            &ExecutionPreparation {
                effect_token: token.into(),
                task_id: packet.contract.task_id.clone(),
                attempt_id: packet.contract.attempt_id.clone(),
                lease_epoch: i64::try_from(packet.contract.lease_epoch).unwrap(),
                runner_id: "runner".into(),
                packet_json: serde_json::to_string(packet).unwrap(),
                packet_sha256: packet.sha256().unwrap(),
            },
            audit(&prepare_id),
        )
        .await
        .unwrap();
    store
        .update_execution(
            token,
            prepared.revision,
            "checkpoint-ack",
            &ExecutionUpdate::Acknowledge {
                wrapper_identity: "wrapper".into(),
                packet_sha256: packet.sha256().unwrap(),
            },
            audit(&ack_id),
        )
        .await
        .unwrap();
}
fn checkpoint_claim() -> conductor_runtime::WorkerCheckpoint {
    serde_json::from_value(serde_json::json!({"version":1,"task_id":"task","attempt_id":"attempt","lease_epoch":1,"sequence":1,"completed_work":"Read input","current_state":"Partial work preserved","next_action":"Finish required checks","open_decisions":["format"],"running_commands":[],"changed_artifacts":["answer.txt"]})).unwrap()
}

#[tokio::test]
async fn checkpoint_admission_binds_current_epoch_sequence_and_monotonic_owner() {
    let (directory, store) = lifecycle_store().await;
    let clock = FixedClock::new(0);
    let service = PersistentControlService::start_with_clock(store.clone(), 100, clock.clone())
        .await
        .unwrap();
    let (_, token) = claim_task(&service).await;
    let packet = runtime_launch(directory.path()).packet;
    checkpoint_seed_execution(&store, &token, &packet).await;
    let mut claim = checkpoint_claim();
    assert!(
        service
            .capture_worker_checkpoint(&token, "wrong", &claim)
            .await
            .is_err()
    );
    claim.lease_epoch = 2;
    assert!(
        service
            .capture_worker_checkpoint(&token, "wrapper", &claim)
            .await
            .is_err()
    );
    claim.lease_epoch = 1;
    assert!(
        service
            .capture_worker_checkpoint(&token, "wrapper", &claim)
            .await
            .unwrap()
    );
    let first = store.execution(&token).await.unwrap().unwrap();
    assert!(
        !service
            .capture_worker_checkpoint(&token, "wrapper", &claim)
            .await
            .unwrap()
    );
    claim.next_action = "Conflicting same sequence".into();
    assert!(
        !service
            .capture_worker_checkpoint(&token, "wrapper", &claim)
            .await
            .unwrap()
    );
    assert_eq!(store.execution(&token).await.unwrap().unwrap(), first);
    claim.sequence = 2;
    assert!(
        service
            .capture_worker_checkpoint(&token, "wrapper", &claim)
            .await
            .unwrap()
    );
    let view = service.checkpoint_view("task").await.unwrap();
    assert_eq!(view["checkpoint"]["claims"]["sequence"], 2);
    assert_eq!(view["inherited"], false);
    let prior = store.execution(&token).await.unwrap().unwrap();
    clock.set(10);
    claim.sequence = 3;
    assert!(
        service
            .capture_worker_checkpoint(&token, "wrapper", &claim)
            .await
            .is_err()
    );
    assert_eq!(store.execution(&token).await.unwrap().unwrap(), prior);
}

#[tokio::test]
async fn checkpoint_audit_failure_rolls_back_and_fenced_restart_preserves_claims() {
    use agent_conductor_store::{EffectAudit, ExecutionUpdate};
    let (directory, store) = lifecycle_store().await;
    let service =
        PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
            .await
            .unwrap();
    let (_, token) = claim_task(&service).await;
    let packet = runtime_launch(directory.path()).packet;
    checkpoint_seed_execution(&store, &token, &packet).await;
    let before = store.execution(&token).await.unwrap().unwrap();
    let mut db = SqliteConnection::connect(&format!(
        "sqlite:{}",
        directory.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    db.execute("CREATE TRIGGER reject_checkpoint BEFORE INSERT ON audit_events WHEN NEW.kind='execution.checkpoint' BEGIN SELECT RAISE(ABORT, 'injected'); END").await.unwrap();
    assert!(
        service
            .capture_worker_checkpoint(&token, "wrapper", &checkpoint_claim())
            .await
            .is_err()
    );
    assert_eq!(store.execution(&token).await.unwrap().unwrap(), before);
    db.execute("DROP TRIGGER reject_checkpoint").await.unwrap();
    assert!(
        service
            .capture_worker_checkpoint(&token, "wrapper", &checkpoint_claim())
            .await
            .unwrap()
    );
    let accepted = store.execution(&token).await.unwrap().unwrap();
    store
        .update_execution(
            &token,
            accepted.revision,
            "fixture-fence",
            &ExecutionUpdate::Fenced {
                reason: "controlled fixture fence".into(),
            },
            EffectAudit {
                id: "fixture-fence",
                actor_id: "runtime",
                correlation_id: &token,
                now_ms: 100,
            },
        )
        .await
        .unwrap();
    assert!(
        service
            .capture_worker_checkpoint(&token, "wrapper", &checkpoint_claim())
            .await
            .is_err()
    );
    drop(service);
    db.close().await.unwrap();
    let restarted = PersistentControlService::start_with_clock(store, 200, FixedClock::new(0))
        .await
        .unwrap();
    assert_eq!(
        restarted.retained_checkpoint("task").await.unwrap(),
        accepted.checkpoint_json
    );
    let task = restarted.workflow_task("task").await.unwrap();
    assert!(
        restarted
            .continuation_packet("task", task.version)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn inherited_checkpoint_remains_bound_to_fenced_origin_when_new_attempt_has_no_claim() {
    let (directory, store) = lifecycle_store().await;
    let service =
        PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
            .await
            .unwrap();
    let (_, token) = claim_task(&service).await;
    let mut packet = runtime_launch(directory.path()).packet;
    checkpoint_seed_execution(&store, &token, &packet).await;
    service
        .capture_worker_checkpoint(&token, "wrapper", &checkpoint_claim())
        .await
        .unwrap();
    let accepted = store.execution(&token).await.unwrap().unwrap();
    checkpoint_fence(&store, &token).await;
    for (key, kind) in [
        ("pause", CommandKind::Pause),
        ("resume", CommandKind::Resume),
    ] {
        let task = service.workflow_task("task").await.unwrap();
        service
            .handle(Request::ApplyCommand {
                task_id: "task".into(),
                command: lifecycle_command(
                    &task,
                    Actor {
                        id: "operator".into(),
                        role: ActorRole::Operator,
                    },
                    kind,
                    key,
                ),
            })
            .await
            .unwrap();
    }
    let task = service.workflow_task("task").await.unwrap();
    let Response::CommandApplied(response) = service
        .handle(Request::ApplyCommand {
            task_id: "task".into(),
            command: lifecycle_command(
                &task,
                Actor {
                    id: "worker".into(),
                    role: ActorRole::Worker,
                },
                CommandKind::Claim {
                    attempt_id: "attempt-2".into(),
                    worker: "worker".into(),
                    lease_duration_ms: 10,
                    worker_margin_ms: 1,
                    recovery_grace_ms: 5,
                },
                "claim-2",
            ),
        })
        .await
        .unwrap()
    else {
        panic!("claim")
    };
    packet.contract.attempt_id = "attempt-2".into();
    packet.contract.lease_epoch = 2;
    packet.checkpoint = accepted.checkpoint_json.clone();
    checkpoint_seed_execution(&store, &response.effect_tokens[0], &packet).await;
    assert_eq!(
        service.retained_checkpoint("task").await.unwrap(),
        accepted.checkpoint_json
    );
    let view = service.checkpoint_view("task").await.unwrap();
    assert_eq!(view["inherited"], true);
    assert_eq!(view["checkpoint"]["claims"]["attempt_id"], "attempt");
    assert_eq!(view["current_attempt_id"], "attempt-2");
    let mut db = SqliteConnection::connect(&format!(
        "sqlite:{}",
        directory.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    sqlx::query("UPDATE executions SET checkpoint_json='{}' WHERE effect_token=?")
        .bind(&token)
        .execute(&mut db)
        .await
        .unwrap();
    assert!(service.retained_checkpoint("task").await.is_err());
}

async fn checkpoint_fence(store: &Store, token: &str) {
    use agent_conductor_store::{EffectAudit, ExecutionUpdate};
    let record = store.execution(token).await.unwrap().unwrap();
    store
        .update_execution(
            token,
            record.revision,
            "fixture-fence",
            &ExecutionUpdate::Fenced {
                reason: "controlled fixture fence".into(),
            },
            EffectAudit {
                id: "fixture-fence",
                actor_id: "runtime",
                correlation_id: token,
                now_ms: 100,
            },
        )
        .await
        .unwrap();
}
