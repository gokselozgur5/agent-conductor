#[tokio::test]
async fn unrestricted_capture_preserves_unfenced_state_and_refuses_continuation() {
    use agent_conductor_store::{EffectAudit, ExecutionState};
    let (directory, store) = lifecycle_store().await;
    let service =
        PersistentControlService::start_with_clock(store.clone(), 100, FixedClock::new(0))
            .await
            .unwrap();
    let (task, token) = claim_task(&service).await;
    let row = store.task("task").await.unwrap().unwrap();
    service
        .apply_row(
            row,
            lifecycle_command(
                &task,
                Actor {
                    id: ActorId("worker".into()),
                    role: ActorRole::Worker,
                },
                CommandKind::Start {
                    attempt_id: AttemptId("attempt".into()),
                    lease_epoch: 1,
                },
                "full-start",
            ),
        )
        .await
        .unwrap();
    store
        .claim_effect(
            &token,
            "runner",
            10000,
            EffectAudit {
                id: "full-claim",
                actor_id: "runtime",
                correlation_id: &token,
                now_ms: 100,
            },
        )
        .await
        .unwrap();
    let workspace = directory.path().join("work");
    fs::create_dir(&workspace).unwrap();
    fs::write(workspace.join("result.txt"), "candidate source").unwrap();
    let launch = unrestricted_capture_launch(&workspace);
    let wrapper =
        PathBuf::from(std::env::var("CONDUCTOR_TEST_BIN_DIR").unwrap()).join("conductor-runner");
    let (record, connection) = service
        .launch_execution(&wrapper, &launch, &token, "runner")
        .await
        .unwrap();
    let (_, candidate) = service
        .await_native_execution(
            &token,
            record.wrapper_identity.as_deref().unwrap(),
            &launch.packet,
            connection,
        )
        .await
        .unwrap();
    assert!(!candidate.file_only_tools);
    let captured = service
        .finalize_native_source(&token, &candidate, &directory.path().join("source"), &[])
        .await
        .unwrap();
    let record = store.execution(&token).await.unwrap().unwrap();
    assert_eq!(record.state, ExecutionState::Exited);
    assert!(store.has_unfenced_execution("task").await.unwrap());
    service
        .validate_capture_eligibility(&record, &launch.packet)
        .await
        .unwrap();
    let task = service.workflow_task("task").await.unwrap();
    assert_eq!(task.state, TaskState::Verifying);
    assert_eq!(
        task.verification
            .as_ref()
            .unwrap()
            .source
            .as_ref()
            .unwrap()
            .0,
        format!("snapshot:{}", captured.sha256().unwrap())
    );
    assert!(
        service
            .continuation_packet("task", task.version)
            .await
            .is_err()
    );
    assert_failed_capture_ineligible(&service, record, &launch.packet).await;
    assert_capture_projection_revokes_blocked(&store, &token).await;
}

fn unrestricted_capture_launch(workspace: &Path) -> conductor_runtime::RunnerLaunch {
    use conductor_runtime::{AdapterKind, ExecutionAccount, FullCapabilityProfile, NetworkPolicy};
    let mut launch = runtime_launch(&workspace.canonicalize().unwrap());
    let user = nix::unistd::User::from_uid(nix::unistd::getuid())
        .unwrap()
        .unwrap();
    launch.packet.contract.full_capability = Some(FullCapabilityProfile {
        account: ExecutionAccount::resolve(&user.name).unwrap(),
        path: "/usr/bin:/bin".into(),
    });
    launch.packet.contract.authority.read_roots = vec!["/".into()];
    launch.packet.contract.authority.write_roots = vec!["/".into()];
    launch.packet.contract.authority.network = NetworkPolicy::Unrestricted;
    launch.packet.contract.adapter.kind = AdapterKind::Claude;
    launch.executable = "/bin/sh".into();
    launch.arguments = vec!["-c".into(), "printf '%s\\n' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"candidate\"}'".into()];
    launch
}

async fn assert_failed_capture_ineligible<C: Clock>(
    service: &PersistentControlService<C>,
    record: agent_conductor_store::ExecutionRecord,
    packet: &conductor_runtime::ResumePacket,
) {
    use agent_conductor_store::ExecutionState;
    let mut invalid = record.clone();
    invalid.exit_code = Some(1);
    assert!(
        service
            .validate_capture_eligibility(&invalid, packet)
            .await
            .is_err()
    );
    invalid = record;
    invalid.state = ExecutionState::Blocked;
    assert!(
        service
            .validate_capture_eligibility(&invalid, packet)
            .await
            .is_err()
    );
}

async fn assert_capture_projection_revokes_blocked(store: &Store, token: &str) {
    use agent_conductor_store::{EffectAudit, ExecutionUpdate};
    let eligible: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM capture_eligible_executions WHERE effect_token=?")
            .bind(token)
            .fetch_one(store.readers())
            .await
            .unwrap();
    assert_eq!(eligible, 1);
    let record = store.execution(token).await.unwrap().unwrap();
    store
        .update_execution(
            token,
            record.revision,
            "block-capture",
            &ExecutionUpdate::Blocked {
                reason: "test ambiguous execution".into(),
            },
            EffectAudit {
                id: "block-capture",
                actor_id: "runtime",
                correlation_id: token,
                now_ms: 100,
            },
        )
        .await
        .unwrap();
    let eligible: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM capture_eligible_executions WHERE effect_token=?")
            .bind(token)
            .fetch_one(store.readers())
            .await
            .unwrap();
    assert_eq!(eligible, 0);
    assert!(store.has_unfenced_execution("task").await.unwrap());
}
