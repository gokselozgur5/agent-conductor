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
    outcome.task.aggregate.decisions[0].question = format!("Legacy {secret}");
    outcome.task.aggregate.decisions[0].resolution = Some(agent_conductor_domain::DecisionResolution {
        rationale: format!("Legacy rationale {secret}"),
        resolved_by: ActorId("operator".into()),
        resolved_at_ms: 100,
    });
    let raw = serde_json::to_string(&outcome.task.aggregate).unwrap();
    let mut db = SqliteConnection::connect(&format!("sqlite:{}", directory.path().join("state.sqlite3").display()))
        .await.unwrap();
    sqlx::query("UPDATE tasks SET aggregate_json=? WHERE id='task'")
        .bind(&raw).execute(&mut db).await.unwrap();
    sqlx::query("UPDATE command_idempotency SET outcome_json=? WHERE idempotency_key='legacy'")
        .bind(serde_json::to_string(&outcome).unwrap()).execute(&mut db).await.unwrap();
    for query in [Request::GetTask { task_id: "task".into() },
        Request::ListTasks { project_id: None },
        Request::ListTaskPage { project_id: None, after: None }, request] {
        let json = serde_json::to_string(&service.handle(query).await.unwrap()).unwrap();
        assert!(!json.contains(&secret));
        assert!(json.contains("Legacy [redacted]"));
        assert!(json.contains("Legacy rationale [redacted]"));
        assert!(json.contains("format"));
    }
    assert_eq!(store.task("task").await.unwrap().unwrap().aggregate_json, raw);
}
