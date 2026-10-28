// Shared lifecycle fixture: no native vendor conformance or OS containment claim.
async fn exercise_retained_observation(
    service: &PersistentControlService<FixedClock>,
    store: &Store,
    workspace: &std::path::Path,
    project: &str,
    token: &str,
) {
    use std::time::Duration;
    let row = store.task("task").await.unwrap().unwrap();
    let task: Task = serde_json::from_str(&row.aggregate_json).unwrap();
    if task.state == TaskState::Running {
        service
            .apply_row(
                row,
                lifecycle_command(
                    &task,
                    Actor {
                        id: ActorId("operator".into()),
                        role: ActorRole::Operator,
                    },
                    CommandKind::Pause,
                    "pause-observation-fixture",
                ),
            )
            .await
            .unwrap();
    }
    store
        .create_agent(NewAgent {
            id: "retained-observer",
            adapter_kind: "observer",
            protocol_fingerprint: "fixture",
            capability_hash: "fixture",
            access_mode: "read_only",
            created_at_ms: 100,
        })
        .await
        .unwrap();
    let config = RepositoryWatch {
        id: "retained-scope".into(),
        project_id: project.into(),
        source_agent_id: "retained-observer".into(),
        root: workspace.into(),
    };
    let mut state = OwnedWatchState::default();
    fs::write(workspace.join("after-stop.txt"), "external bytes").unwrap();
    assert_eq!(
        service
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(20))
            .await
            .unwrap(),
        0,
        "disabled scope must not observe retained unowned work"
    );
    service.configure_repository_observation(std::slice::from_ref(&config));
    assert_eq!(
        service
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(21))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        service
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(22))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        service
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(23))
            .await
            .unwrap(),
        0
    );
    let events:Vec<(String,String,Option<i64>,Option<String>)>=sqlx::query_as("SELECT source_agent_id,topic,lease_epoch,causation_id FROM semantic_events WHERE topic LIKE 'workspace.external.%'").fetch_all(store.readers()).await.unwrap();
    assert_eq!(
        events,
        vec![(
            "retained-observer".into(),
            "workspace.external.created".into(),
            None,
            None
        )]
    );
    exercise_restarted_observation(store, workspace, project, token, config).await;
}

async fn exercise_restarted_observation(
    store: &Store,
    workspace: &std::path::Path,
    project: &str,
    token: &str,
    config: RepositoryWatch,
) {
    use std::time::Duration;
    // A real service restart must keep the publication baseline and observer scope explicit.
    let restarted =
        PersistentControlService::start_with_clock(store.clone(), 200, FixedClock::new(0))
            .await
            .unwrap();
    restarted.configure_repository_observation(&[config]);
    let mut state = OwnedWatchState::default();
    assert_eq!(
        restarted
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(1))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        restarted
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(2))
            .await
            .unwrap(),
        0
    );
    fs::write(workspace.join("after-stop.txt"), "changed after restart").unwrap();
    assert_eq!(
        restarted
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(3))
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        restarted
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(4))
            .await
            .unwrap(),
        1
    );
    let mut connection = SqliteConnection::connect(&format!(
        "sqlite://{}",
        workspace.parent().unwrap().join("state.sqlite3").display()
    ))
    .await
    .unwrap();
    let record = store.execution(token).await.unwrap().unwrap();
    let mut packet: serde_json::Value =
        serde_json::from_str(&record.preparation.packet_json).unwrap();
    packet["objective"] = "forged".into();
    sqlx::query("UPDATE executions SET packet_json=? WHERE effect_token=?")
        .bind(packet.to_string())
        .bind(token)
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(
        restarted
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(5))
            .await
            .is_err()
    );
    sqlx::query("UPDATE executions SET packet_json=? WHERE effect_token=?")
        .bind(&record.preparation.packet_json)
        .bind(token)
        .execute(&mut connection)
        .await
        .unwrap();
    let row = store.task("task").await.unwrap().unwrap();
    let task: Task = serde_json::from_str(&row.aggregate_json).unwrap();
    restarted
        .apply_row(
            row,
            lifecycle_command(
                &task,
                Actor {
                    id: ActorId("operator".into()),
                    role: ActorRole::Operator,
                },
                CommandKind::Cancel,
                "cancel-observation-fixture",
            ),
        )
        .await
        .unwrap();
    fs::write(workspace.join("terminal.txt"), "unobserved terminal tree").unwrap();
    assert_eq!(
        restarted
            .poll_owned_workspaces(project, &mut state, Duration::from_secs(6))
            .await
            .unwrap(),
        0
    );
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM semantic_events WHERE topic LIKE 'workspace.external.%'",
    )
    .fetch_one(store.readers())
    .await
    .unwrap();
    assert_eq!(count, 2);
}
