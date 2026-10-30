fn decision_canary() -> String {
    ["sk", "-ant-", "syntheticDecisionCanary12345"].concat()
}

fn decision_request(task: &Task, kind: CommandKind, key: &str) -> Request {
    Request::ApplyCommand {
        task_id: "task".into(),
        command: lifecycle_command(task, Actor {
            id: ActorId("operator".into()), role: ActorRole::Operator,
        }, kind, key),
    }
}

fn decision_task(response: Response) -> CommandAppliedResponse {
    let Response::CommandApplied(value) = response else {
        panic!("expected applied command");
    };
    value
}

#[tokio::test]
async fn decision_privacy_preserves_original_request_identity_and_redacts_publication() {
    let (_directory, store) = lifecycle_store().await;
    let service = PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
        .await.unwrap();
    let secret = decision_canary();
    let original = decision_request(&Task::new(TaskId("task".into()), vec![]),
        CommandKind::OpenDecision { id: "format".into(), question: format!("Choose {secret}") }, "open");
    let opened = decision_task(service.handle(original.clone()).await.unwrap());
    assert_eq!(opened.task.aggregate.decisions[0].question, "Choose [redacted]");
    let replay = decision_task(service.handle(original.clone()).await.unwrap());
    assert!(replay.replayed);
    assert_eq!(replay.task, opened.task);
    let mut different = original;
    let Request::ApplyCommand { command, .. } = &mut different else { unreachable!() };
    let CommandKind::OpenDecision { question, .. } = &mut command.kind else { unreachable!() };
    question.push_str("DifferentSecret");
    assert!(service.handle(different).await.is_err());
    let resolved = decision_task(service.handle(decision_request(&opened.task.aggregate,
        CommandKind::ResolveDecision { id: "format".into(), rationale: format!("Use text {secret}") },
        "resolve")).await.unwrap());
    assert_eq!(resolved.task.aggregate.decisions[0].resolution.as_ref().unwrap().rationale,
        "Use text [redacted]");
    for sql in ["SELECT aggregate_json FROM tasks", "SELECT payload_json FROM audit_events",
                "SELECT outcome_json FROM command_idempotency"] {
        let rows: Vec<String> = sqlx::query_scalar(sql).fetch_all(store.readers()).await.unwrap();
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| !row.contains(&secret)));
    }
}

#[tokio::test]
async fn decision_privacy_does_not_shrink_invalid_input_into_an_accepted_command() {
    let (_directory, store) = lifecycle_store().await;
    let service = PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
        .await.unwrap();
    let original = store.task("task").await.unwrap().unwrap();
    let request = decision_request(&Task::new(TaskId("task".into()), vec![]),
        CommandKind::OpenDecision { id: "format".into(),
            question: format!("{}{}", decision_canary(), "x".repeat(3000)) }, "oversized");
    assert!(service.handle(request).await.is_err());
    assert_eq!(store.task("task").await.unwrap().unwrap(), original);
}

#[tokio::test]
async fn decision_privacy_redacts_legacy_views_and_replays_without_rewriting_records() {
    let (directory, store) = lifecycle_store().await;
    let service = PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
        .await.unwrap();
    let request = decision_request(&Task::new(TaskId("task".into()), vec![]),
        CommandKind::OpenDecision { id: "format".into(), question: "Ordinary question".into() }, "legacy");
    let mut outcome = decision_task(service.handle(request.clone()).await.unwrap());
    let secret = decision_canary();
    outcome.task.objective = format!("Legacy objective {secret}");
    outcome.task.aggregate.decisions[0].question = format!("Legacy {secret}");
    outcome.task.aggregate.decisions[0].resolution = Some(agent_conductor_domain::DecisionResolution {
        rationale: format!("Legacy rationale {secret}"),
        resolved_by: ActorId("operator".into()),
        resolved_at_ms: 100,
    });
    let raw = serde_json::to_string(&outcome.task.aggregate).unwrap();
    let mut db = SqliteConnection::connect(&format!("sqlite:{}", directory.path().join("state.sqlite3").display()))
        .await.unwrap();
    sqlx::query("UPDATE tasks SET objective=? WHERE id='task'")
        .bind(&outcome.task.objective).execute(&mut db).await.unwrap();
    sqlx::query("UPDATE tasks SET aggregate_json=? WHERE id='task'")
        .bind(&raw).execute(&mut db).await.unwrap();
    sqlx::query("UPDATE command_idempotency SET outcome_json=? WHERE idempotency_key='legacy'")
        .bind(serde_json::to_string(&outcome).unwrap()).execute(&mut db).await.unwrap();
    for query in [Request::GetTask { task_id: "task".into() },
        Request::ListTasks { project_id: None },
        Request::ListTaskPage { project_id: None, after: None }, request] {
        let json = serde_json::to_string(&service.handle(query).await.unwrap()).unwrap();
        assert!(!json.contains(&secret));
        assert!(json.contains("Legacy objective [redacted]"));
        assert!(json.contains("Legacy [redacted]"));
        assert!(json.contains("Legacy rationale [redacted]"));
        assert!(json.contains("format"));
    }
    assert_eq!(store.task("task").await.unwrap().unwrap().aggregate_json, raw);
}

#[tokio::test]
async fn objective_projection_and_legacy_creation_replay_preserve_original_identity() {
    let (directory, store) = lifecycle_store().await;
    let service = PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
        .await.unwrap();
    let secret = decision_canary();
    let objective = format!("Perform task {secret}");
    let request = Request::CreateTask {
        actor_id: "operator".into(), idempotency_key: "objective-create".into(),
        id: "objective-task".into(), project_id: "project".into(), objective: objective.clone(),
        priority: 0, authority: serde_json::json!({}), required_checks: vec![],
    };
    let mut response = service.handle(request.clone()).await.unwrap();
    assert!(!serde_json::to_string(&response).unwrap().contains(&secret));
    assert_eq!(store.task("objective-task").await.unwrap().unwrap().objective, objective);
    let Response::Task(task) = &mut response else { panic!("task expected") };
    task.objective.clone_from(&objective);
    let legacy = serde_json::to_string(&response).unwrap();
    let mut db = SqliteConnection::connect(&format!("sqlite:{}", directory.path().join("state.sqlite3").display()))
        .await.unwrap();
    sqlx::query("UPDATE command_idempotency SET outcome_json=? WHERE idempotency_key='objective-create'")
        .bind(&legacy).execute(&mut db).await.unwrap();
    for query in [Request::GetTask { task_id: "objective-task".into() },
        Request::ListTasks { project_id: None },
        Request::ListTaskPage { project_id: None, after: None }, request.clone()] {
        let json = serde_json::to_string(&service.handle(query).await.unwrap()).unwrap();
        assert!(!json.contains(&secret));
        assert!(json.contains("Perform task [redacted]"));
    }
    let mut different = request;
    let Request::CreateTask { objective, .. } = &mut different else { unreachable!() };
    objective.push_str("DifferentSecret");
    assert!(service.handle(different).await.is_err());
    let saved: String = sqlx::query_scalar("SELECT outcome_json FROM command_idempotency WHERE idempotency_key='objective-create'")
        .fetch_one(store.readers()).await.unwrap();
    assert_eq!(saved, legacy);
}

#[tokio::test]
async fn blocker_free_text_is_redacted_in_task_response() {
    let (directory, store) = lifecycle_store().await;
    let service = PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0)).await.unwrap();
    let ready = decision_task(service.handle(decision_request(
        &Task::new(TaskId("task".into()), vec![]), CommandKind::MarkReady, "ready"
    )).await.unwrap());
    let secret = decision_canary();
    let request = decision_request(&ready.task.aggregate,
        CommandKind::Block { blocker: agent_conductor_domain::Blocker {
            kind: "operator_input".into(), detail: format!("Failure {secret}"),
            resume_when: format!("Replace {secret}"),
        } }, "block");
    let mut invalid = request.clone();
    let Request::ApplyCommand { command, .. } = &mut invalid else { unreachable!() };
    let CommandKind::Block { blocker } = &mut command.kind else { unreachable!() };
    blocker.kind = "   ".into();
    assert!(service.handle(invalid).await.is_err());
    assert_eq!(serde_json::from_str::<Task>(&store.task("task").await.unwrap().unwrap().aggregate_json).unwrap().state, agent_conductor_domain::TaskState::Ready);
    let mut outcome = decision_task(service.handle(request.clone()).await.unwrap());
    let blocker = outcome.task.aggregate.blocker.as_ref().unwrap();
    assert_eq!(blocker.kind, "operator_input");
    assert_eq!(blocker.detail, "Failure [redacted]");
    assert_eq!(blocker.resume_when, "Replace [redacted]");
    assert!(decision_task(service.handle(request.clone()).await.unwrap()).replayed);
    let mut changed = request.clone();
    let Request::ApplyCommand { command, .. } = &mut changed else { unreachable!() };
    let CommandKind::Block { blocker } = &mut command.kind else { unreachable!() };
    blocker.detail.push_str("DifferentSecret");
    assert!(service.handle(changed).await.is_err());
    for sql in ["SELECT aggregate_json FROM tasks", "SELECT payload_json FROM audit_events", "SELECT outcome_json FROM command_idempotency"] {
        let rows: Vec<String> = sqlx::query_scalar(sql).fetch_all(store.readers()).await.unwrap();
        assert!(!rows.is_empty());
        assert!(rows.iter().all(|row| !row.contains(&secret)));
    }
    // Legacy reads sanitize presentation without rewriting historical evidence.
    let blocker = outcome.task.aggregate.blocker.as_mut().unwrap();
    blocker.detail = format!("Legacy detail {secret}");
    blocker.resume_when = format!("Legacy resume {secret}");
    let raw = serde_json::to_string(&outcome.task.aggregate).unwrap();
    let legacy = serde_json::to_string(&outcome).unwrap();
    let mut db = SqliteConnection::connect(&format!("sqlite:{}", directory.path().join("state.sqlite3").display())).await.unwrap();
    sqlx::query("UPDATE tasks SET aggregate_json=? WHERE id='task'").bind(&raw).execute(&mut db).await.unwrap();
    sqlx::query("UPDATE command_idempotency SET outcome_json=? WHERE idempotency_key='block'").bind(&legacy).execute(&mut db).await.unwrap();
    for query in [Request::GetTask { task_id: "task".into() }, Request::ListTasks { project_id: None }, Request::ListTaskPage { project_id: None, after: None }, request] {
        let value = serde_json::to_string(&service.handle(query).await.unwrap()).unwrap();
        assert!(!value.contains(&secret));
        assert!(value.contains("Legacy detail [redacted]"));
        assert!(value.contains("Legacy resume [redacted]"));
        assert!(value.contains("operator_input"));
    }
    assert_eq!(store.task("task").await.unwrap().unwrap().aggregate_json, raw);
    let saved: String = sqlx::query_scalar("SELECT outcome_json FROM command_idempotency WHERE idempotency_key='block'").fetch_one(store.readers()).await.unwrap();
    assert_eq!(saved, legacy);
}

#[tokio::test]
async fn command_reason_privacy_preserves_validation_and_original_identity() {
    let (_directory, store) = lifecycle_store().await;
    let service = PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0)).await.unwrap();
    let mut request = decision_request(&Task::new(TaskId("task".into()), vec![]), CommandKind::MarkReady, "reason");
    let secret = decision_canary();
    let Request::ApplyCommand { command, .. } = &mut request else { unreachable!() };
    command.reason = format!("Operator reason {secret}");
    let mut invalid = request.clone();
    let Request::ApplyCommand { command, .. } = &mut invalid else { unreachable!() };
    command.reason = "   ".into();
    assert!(service.handle(invalid).await.is_err());
    let applied = decision_task(service.handle(request.clone()).await.unwrap());
    assert_eq!(applied.task.aggregate.version, 1);
    assert!(decision_task(service.handle(request.clone()).await.unwrap()).replayed);
    let mut changed = request;
    let Request::ApplyCommand { command, .. } = &mut changed else { unreachable!() };
    command.reason.push_str("DifferentSecret");
    assert!(service.handle(changed).await.is_err());
    let rows: Vec<String> = sqlx::query_scalar("SELECT payload_json FROM audit_events WHERE task_id='task'").fetch_all(store.readers()).await.unwrap();
    assert!(rows.iter().any(|row| row.contains("Operator reason [redacted]")));
    assert!(rows.iter().all(|row| !row.contains(&secret)));
    let task = store.task("task").await.unwrap().unwrap();
    assert!(!task.aggregate_json.contains(&secret));
    let outcomes: Vec<String> = sqlx::query_scalar("SELECT outcome_json FROM command_idempotency").fetch_all(store.readers()).await.unwrap();
    assert!(outcomes.iter().all(|row| !row.contains(&secret)));
}
