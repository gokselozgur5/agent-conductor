// Real SQLite transactions with lifecycle fixtures; no native process is launched.
#[tokio::test]
async fn successor_admission_is_atomic_replay_safe_and_preserves_original_plan() {
    let (directory, store) = lifecycle_store().await;
    let service =
        PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
            .await
            .unwrap();
    claim_task(&service).await;
    let task = service.workflow_task("task").await.unwrap();
    store.enqueue_workflow("task", "{}", 100).await.unwrap();
    store
        .claim_workflow(service.boot_generation, 100)
        .await
        .unwrap()
        .unwrap();
    store
        .finish_workflow("task", service.boot_generation, "{}", 100)
        .await
        .unwrap();
    let mut db = SqliteConnection::connect(&format!(
        "sqlite:{}",
        directory.path().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    db.execute("CREATE TRIGGER reject_successor BEFORE INSERT ON audit_events WHEN NEW.kind='workflow.reassigned' BEGIN SELECT RAISE(ABORT, 'injected'); END").await.unwrap();
    assert!(
        store
            .reassign_workflow("task", task.version, "worker", "{}", "attempt", 100)
            .await
            .is_err()
    );
    assert!(store.workflow_assignment("task").await.unwrap().is_none());
    assert_eq!(store.workflows().await.unwrap()[0].status, "finished");
    db.execute("DROP TRIGGER reject_successor").await.unwrap();
    store
        .reassign_workflow("task", task.version, "worker", "{}", "attempt", 100)
        .await
        .unwrap();
    store
        .reassign_workflow("task", task.version, "worker", "{}", "attempt", 101)
        .await
        .unwrap();
    assert!(
        store
            .reassign_workflow("task", task.version + 1, "worker", "{}", "attempt", 102)
            .await
            .is_err()
    );
    assert!(
        store
            .reassign_workflow(
                "task",
                task.version,
                "worker",
                "{\"changed\":true}",
                "attempt",
                102
            )
            .await
            .is_err()
    );
    assert_eq!(
        store.workflow_plan("task").await.unwrap().as_deref(),
        Some("{}")
    );
    assert_eq!(
        store.workflows().await.unwrap()[0].resume_version,
        Some(i64::try_from(task.version).unwrap())
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE kind='workflow.reassigned'")
            .fetch_one(&mut db)
            .await
            .unwrap();
    assert_eq!(count, 1);
    drop(service);
    let restarted =
        PersistentControlService::start_with_clock(store.clone(), 200, FixedClock::new(0))
            .await
            .unwrap();
    assert_eq!(
        store.workflow_assignment("task").await.unwrap(),
        Some(("worker".into(), "{}".into()))
    );
    assert_eq!(
        restarted
            .workflow_task("task")
            .await
            .unwrap()
            .attempt
            .unwrap()
            .worker
            .0,
        "worker"
    );
}

#[tokio::test]
async fn successor_service_refuses_unfenced_same_worker_and_unregistered_profiles() {
    let (directory, store) = lifecycle_store().await;
    let service =
        PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
            .await
            .unwrap();
    let (_, token) = claim_task(&service).await;
    let packet = runtime_launch(directory.path()).packet;
    checkpoint_seed_execution(&store, &token, &packet).await;
    let task = service.workflow_task("task").await.unwrap();
    let pin = conductor_runtime::VendorPin {
        kind: conductor_runtime::AdapterKind::Claude,
        version: "fixture".into(),
        executable: "/bin/true".into(),
        executable_sha256: "a".repeat(64),
        conformance_sha256: "b".repeat(64),
    };
    let pin_json = serde_json::to_string(&pin).unwrap();
    assert!(
        service
            .queue_reassignment("task", task.version, "successor", &pin_json)
            .await
            .unwrap_err()
            .to_string()
            .contains("verified prior write fences")
    );
    checkpoint_fence(&store, &token).await;
    assert!(
        service
            .queue_reassignment("task", task.version, "worker", &pin_json)
            .await
            .unwrap_err()
            .to_string()
            .contains("must differ")
    );
    let plan:NativeWorkflowPlan=serde_json::from_value(serde_json::json!({"task_id":"task","project":directory.path(),"objective":"fixture","artifacts":directory.path().join("artifacts"),"wrapper":"/bin/true","worker":pin,"verifier":pin,"checks":[{"id":"test","argv":["/bin/true"],"timeout_ms":1000}],"model_timeout_ms":1000})).unwrap();
    store
        .enqueue_workflow("task", &serde_json::to_string(&plan).unwrap(), 100)
        .await
        .unwrap();
    assert!(
        service
            .queue_reassignment("task", task.version, "successor", &pin_json)
            .await
            .unwrap_err()
            .to_string()
            .contains("already be registered")
    );
    assert!(
        service
            .queue_reassignment("task", task.version, "task:verifier", &pin_json)
            .await
            .unwrap_err()
            .to_string()
            .contains("unsupported successor")
    );
    service
        .handle(Request::RegisterAgent {
            actor_id: "operator".into(),
            idempotency_key: "successor-register".into(),
            id: "successor".into(),
            adapter_kind: "claude".into(),
            protocol_fingerprint: "c".repeat(64),
            capability_hash: "a".repeat(64),
            access_mode: "read_write".into(),
        })
        .await
        .unwrap();
    assert!(
        service
            .queue_reassignment("task", task.version, "successor", &pin_json)
            .await
            .unwrap_err()
            .to_string()
            .contains("registered capabilities")
    );
    assert!(store.workflow_assignment("task").await.unwrap().is_none());
    assert_eq!(
        service.workflow_task("task").await.unwrap().version,
        task.version
    );
}
