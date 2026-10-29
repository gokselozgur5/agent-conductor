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
                expected_wrapper_identity: None,
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
            process: None,
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
    inherited_checkpoint_fixture(false).await;
}

#[tokio::test]
async fn legacy_secret_checkpoint_is_redacted_without_losing_inherited_correspondence() {
    inherited_checkpoint_fixture(true).await;
}

async fn inherited_checkpoint_fixture(legacy: bool) {
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
    let original = if legacy {
        inject_legacy_checkpoint(directory.path(), &token, accepted.checkpoint_json.as_deref().unwrap()).await
    } else { accepted.checkpoint_json.clone().unwrap() };
    let normalized = service.retained_checkpoint("task").await.unwrap().unwrap();
    assert!(!normalized.contains(&checkpoint_canary()));
    assert_eq!(store.execution(&token).await.unwrap().unwrap().checkpoint_json.as_deref(), Some(original.as_str()));
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
    packet.checkpoint = Some(normalized.clone());
    checkpoint_seed_execution(&store, &response.effect_tokens[0], &packet).await;
    assert_eq!(
        service.retained_checkpoint("task").await.unwrap(),
        Some(normalized)
    );
    let view = service.checkpoint_view("task").await.unwrap();
    assert_eq!(view["inherited"], true);
    assert_eq!(view["checkpoint"]["claims"]["attempt_id"], "attempt");
    assert_eq!(view["current_attempt_id"], "attempt-2");
    assert_inherited_tamper_rejected(&service, directory.path(), &token, &original).await;
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

fn checkpoint_canary() -> String {
    ["sk", "-ant-", "syntheticCheckpointCanary12345"].concat()
}
async fn inject_legacy_checkpoint(root: &std::path::Path, token: &str, original: &str) -> String {
    let mut value: serde_json::Value = serde_json::from_str(original).unwrap();
    value["claims"]["current_state"] = serde_json::json!(format!("Legacy {} preserved context", checkpoint_canary()));
    let bytes = value.to_string();
    let mut db = SqliteConnection::connect(&format!("sqlite:{}", root.join("state.sqlite3").display())).await.unwrap();
    sqlx::query("UPDATE executions SET checkpoint_json=? WHERE effect_token=?").bind(&bytes).bind(token).execute(&mut db).await.unwrap();
    bytes
}

#[tokio::test]
async fn direct_checkpoint_admission_redacts_every_free_text_field_before_persistence() {
    let (directory, store) = lifecycle_store().await;
    let service = PersistentControlService::start_with_clock(store.clone(),100,FixedClock::new(0)).await.unwrap();
    let (_, token) = claim_task(&service).await;
    let packet = runtime_launch(directory.path()).packet;
    checkpoint_seed_execution(&store,&token,&packet).await;
    let mut claim = checkpoint_claim();
    let secret = checkpoint_canary();
    let text = format!("Retain context {secret}");
    claim.completed_work=text.clone();claim.current_state=text.clone();claim.next_action=text.clone();
    claim.open_decisions=vec![text.clone()];claim.running_commands=vec![text.clone()];claim.changed_artifacts=vec![text];
    assert!(service.capture_worker_checkpoint(&token,"wrapper",&claim).await.unwrap());
    let record=store.execution(&token).await.unwrap().unwrap();
    let encoded=record.checkpoint_json.as_deref().unwrap();
    assert!(!encoded.contains(&secret));assert_eq!(encoded.matches("[redacted]").count(),6);
    assert_eq!(record.preparation.packet_sha256,packet.sha256().unwrap());
    let rows:Vec<String>=sqlx::query_scalar("SELECT outcome_json FROM execution_receipts WHERE effect_token=?").bind(&token).fetch_all(store.readers()).await.unwrap();
    assert!(rows.iter().all(|row|!row.contains(&secret)));
    let view=service.handle(Request::TaskCheckpoint{task_id:"task".into()}).await.unwrap();
    assert!(!serde_json::to_string(&view).unwrap().contains(&secret));
    assert!(!service.retained_checkpoint("task").await.unwrap().unwrap().contains(&secret));
    assert!(!service.capture_worker_checkpoint(&token,"wrapper",&claim).await.unwrap());
    claim.lease_epoch+=1;
    assert!(service.capture_worker_checkpoint(&token,"wrapper",&claim).await.is_err());
    assert_eq!(store.execution(&token).await.unwrap().unwrap(),record);
}

async fn assert_inherited_tamper_rejected(service: &PersistentControlService<FixedClock>, root: &std::path::Path, token: &str, original: &str) {
    let mut db = SqliteConnection::connect(&format!("sqlite:{}",root.join("state.sqlite3").display())).await.unwrap();
    let mut altered:serde_json::Value=serde_json::from_str(original).unwrap();
    altered["claims"]["next_action"]=serde_json::json!("Forged ordinary next action");
    sqlx::query("UPDATE executions SET checkpoint_json=? WHERE effect_token=?").bind(altered.to_string()).bind(token).execute(&mut db).await.unwrap();
    assert!(service.retained_checkpoint("task").await.is_err());
    sqlx::query("UPDATE executions SET checkpoint_json=? WHERE effect_token=?").bind(original).bind(token).execute(&mut db).await.unwrap();
    assert!(service.retained_checkpoint("task").await.is_ok());
    sqlx::query("UPDATE executions SET checkpoint_json='{}' WHERE effect_token=?").bind(token).execute(&mut db).await.unwrap();
    assert!(service.retained_checkpoint("task").await.is_err());
}
