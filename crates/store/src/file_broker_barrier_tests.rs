use super::file_broker_tests::{audit, fixture};
use super::transaction_cancellation::snapshot;
use super::*;
use sqlx::Executor;

async fn dispatch(store: &Store, status: &str) {
    sqlx::query("INSERT INTO workflow_dispatch(task_id,plan_json,status,boot_generation,created_at_ms,updated_at_ms) VALUES('task','{}',?,2,0,0)").bind(status).execute(&mut *store.inner.writer.lock().await).await.unwrap();
}

#[tokio::test]
async fn broker_sql_barriers_preserve_all_tables_and_closed_controls_succeed() {
    for (statement, positive) in [
        (
            "UPDATE executions SET state='fenced' WHERE effect_token='token'",
            true,
        ),
        ("UPDATE tasks SET state='verifying' WHERE id='task'", true),
        ("UPDATE tasks SET state='completed' WHERE id='task'", true),
        ("UPDATE tasks SET state='claimed' WHERE id='task'", true),
        (
            "UPDATE workflow_dispatch SET status='queued' WHERE task_id='task'",
            true,
        ),
        (
            "UPDATE workflow_dispatch SET status='running' WHERE task_id='task'",
            true,
        ),
        (
            "UPDATE workflow_dispatch SET status='finished' WHERE task_id='task'",
            true,
        ),
        (
            "INSERT INTO workflow_dispatch(task_id,plan_json,status,created_at_ms,updated_at_ms) VALUES('task','{}','queued',0,0)",
            false,
        ),
        (
            "INSERT INTO auxiliary_invocations(invocation_id,task_id,execution_token,preparation_json,boot_generation,state,created_at_ms) VALUES('aux','task','token','{}',2,'pending',0)",
            true,
        ),
        (
            "INSERT INTO attempts(id,task_id,worker_id,lease_epoch,instruction_hashes_json,required_checks_json,capability_hash,started_at_ms) SELECT 'second',task_id,worker_id,8,'[]','[]','fixture',0 FROM attempts WHERE id='attempt'",
            false,
        ),
        (
            "INSERT INTO executions(effect_token,task_id,attempt_id,lease_epoch,runner_id,packet_json,packet_sha256,state,created_at_ms,updated_at_ms) SELECT effect_token,task_id,attempt_id,lease_epoch,runner_id,packet_json,packet_sha256,'prepared',0,0 FROM executions WHERE effect_token='token'",
            false,
        ),
    ] {
        let (_root, store, b) = fixture().await;
        dispatch(&store, "needs_recovery").await;
        store
            .register_file_broker(&b, audit("register"))
            .await
            .unwrap();
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        let error = store
            .inner
            .writer
            .lock()
            .await
            .execute(statement)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("file broker ownership unresolved"),
            "{statement}: {error}"
        );
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
        store
            .close_file_broker(&b, 0, audit("close"))
            .await
            .unwrap();
        if positive {
            store
                .inner
                .writer
                .lock()
                .await
                .execute(statement)
                .await
                .unwrap();
        } else {
            let error = store
                .inner
                .writer
                .lock()
                .await
                .execute(statement)
                .await
                .unwrap_err();
            assert!(
                !error
                    .to_string()
                    .contains("file broker ownership unresolved")
            );
        }
    }
}

#[tokio::test]
async fn open_broker_prevents_execution_fence_and_receipt_publication() {
    let (_root, store, b) = fixture().await;
    store
        .register_file_broker(&b, audit("register"))
        .await
        .unwrap();
    let revision: i64 =
        sqlx::query_scalar("SELECT revision FROM executions WHERE effect_token='token'")
            .fetch_one(store.readers())
            .await
            .unwrap();
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    let update = ExecutionUpdate::Fenced {
        reason: "trusted fixture stop".into(),
    };
    assert!(
        store
            .update_execution("token", revision, "fence", &update, audit("fence"))
            .await
            .is_err()
    );
    assert_eq!(
        snapshot(&mut *store.inner.writer.lock().await).await,
        before
    );
    store
        .close_file_broker(&b, 0, audit("close"))
        .await
        .unwrap();
    assert_eq!(
        store
            .update_execution("token", revision, "fence", &update, audit("fence"))
            .await
            .unwrap()
            .state,
        ExecutionState::Fenced
    );
}

#[tokio::test]
async fn broker_blocks_manual_automatic_and_successor_continuations_atomically() {
    for mode in ["manual", "auto", "successor"] {
        let (_root, store, b) = fixture().await;
        dispatch(&store, "needs_recovery").await;
        store
            .register_file_broker(&b, audit("register"))
            .await
            .unwrap();
        let version: i64 = sqlx::query_scalar("SELECT version FROM tasks WHERE id='task'")
            .fetch_one(store.readers())
            .await
            .unwrap();
        let version = u64::try_from(version).unwrap();
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        assert!(store.interrupted_workflows().await.unwrap().is_empty());
        match mode {
            "manual" => assert!(store.requeue_workflow("task", version, 101).await.is_err()),
            "auto" => assert!(
                !store
                    .auto_requeue_workflow("task", version, 2, 101)
                    .await
                    .unwrap()
            ),
            "successor" => assert!(
                store
                    .reassign_workflow("task", version, "source", "{}", "attempt", 101)
                    .await
                    .is_err()
            ),
            _ => unreachable!(),
        }
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
        store
            .close_file_broker(&b, 0, audit("close"))
            .await
            .unwrap();
        assert_eq!(store.interrupted_workflows().await.unwrap().len(), 1);
        match mode {
            "manual" => store.requeue_workflow("task", version, 102).await.unwrap(),
            "auto" => assert!(
                store
                    .auto_requeue_workflow("task", version, 2, 102)
                    .await
                    .unwrap()
            ),
            "successor" => store
                .reassign_workflow("task", version, "source", "{}", "attempt", 102)
                .await
                .unwrap(),
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn blocked_queue_candidate_does_not_starve_others_but_running_work_still_counts() {
    for status in ["queued", "running"] {
        let (_root, store, b) = fixture().await;
        dispatch(&store, status).await;
        store
            .register_file_broker(&b, audit("register"))
            .await
            .unwrap();
        store.inner.writer.lock().await.execute("INSERT INTO workflow_dispatch(task_id,plan_json,status,created_at_ms,updated_at_ms) VALUES('other','{}','queued',1,1)").await.unwrap();
        let selected = store.claim_workflow(2, 100).await.unwrap();
        if status == "running" {
            assert!(selected.is_none());
        } else {
            assert_eq!(selected.unwrap().task_id, "other");
        }
    }
}

#[tokio::test]
async fn runtime_reports_old_or_contradictory_brokers_without_exposing_binding() {
    for state in ["old", "exited", "paused", "expired", "closed-pending"] {
        let (_root, store, b) = fixture().await;
        store
            .register_file_broker(&b, audit("register"))
            .await
            .unwrap();
        assert!(
            store
                .file_broker_recovery_diagnostic()
                .await
                .unwrap()
                .is_none()
        );
        match state {
            "old" => {
                store
                    .start_reserved_runtime(101, &b.authority_id, 3)
                    .await
                    .unwrap();
            }
            "exited" => {
                store
                    .inner
                    .writer
                    .lock()
                    .await
                    .execute("UPDATE executions SET state='exited'")
                    .await
                    .unwrap();
            }
            "paused" => {
                store
                    .inner
                    .writer
                    .lock()
                    .await
                    .execute("UPDATE tasks SET state='paused'")
                    .await
                    .unwrap();
            }
            "expired" => {
                store
                    .inner
                    .writer
                    .lock()
                    .await
                    .execute("UPDATE leases SET status='expired'")
                    .await
                    .unwrap();
            }
            "closed-pending" => {
                store
                    .admit_file_operation(&b, 1, &"c".repeat(64), audit("admit"))
                    .await
                    .unwrap();
                // Corrupt contradictory state is a diagnostic control, not an API.
                store
                    .inner
                    .writer
                    .lock()
                    .await
                    .execute("UPDATE file_brokers SET state='closed'")
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        if state == "closed-pending" {
            let error = store
                .inner
                .writer
                .lock()
                .await
                .execute("UPDATE executions SET state='fenced'")
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("file broker ownership unresolved")
            );
        }
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        let diagnostic = store
            .file_broker_recovery_diagnostic()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(diagnostic.code, "file_broker_recovery_pending");
        let context: serde_json::Value = serde_json::from_str(&diagnostic.context_json).unwrap();
        assert_eq!(context["count"], 1);
        assert_eq!(context["brokers"][0]["invocation_id"], b.invocation_id);
        assert!(!diagnostic.context_json.contains(&b.root_identity_sha256));
        assert!(
            store
                .check_runtime_invariants()
                .await
                .unwrap()
                .iter()
                .any(|v| v.code == "file_broker_recovery_pending")
        );
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
    }
}

#[tokio::test]
async fn broker_allows_existing_attempt_renewal_and_release_but_not_new_identity() {
    use sqlx::Row;
    let (_root, store, b) = fixture().await;
    store
        .register_file_broker(&b, audit("register"))
        .await
        .unwrap();
    let row = sqlx::query("SELECT * FROM attempts WHERE id='attempt'")
        .fetch_one(store.readers())
        .await
        .unwrap();
    let mut batch = CommitBatch {
        actor_id: "runtime".into(),
        idempotency_key: "renew-broker".into(),
        request_hash: "renew".into(),
        task: None,
        project: None,
        agent: None,
        new_task: None,
        attempt: Some(AttemptLeaseMutation {
            attempt_id: "attempt".into(),
            task_id: "task".into(),
            worker_id: row.get("worker_id"),
            lease_epoch: 7,
            lease_status: "active".into(),
            boot_generation: 2,
            remaining_ms: 50000,
            grace_ms: 1000,
            display_expires_at_ms: 50100,
            last_heartbeat_at_ms: 100,
            instruction_hashes_json: row.get("instruction_hashes_json"),
            required_checks_json: row.get("required_checks_json"),
            capability_hash: row.get("capability_hash"),
            ended_at_ms: None,
        }),
        transitions: vec![],
        audit: vec![],
        effects: vec![],
        outcome_json: "{}".into(),
        committed_at_ms: 100,
        expires_at_ms: 1000,
    };
    store.persist(&batch).await.unwrap();
    let remaining: i64 =
        sqlx::query_scalar("SELECT remaining_ms FROM leases WHERE attempt_id='attempt'")
            .fetch_one(store.readers())
            .await
            .unwrap();
    assert_eq!(remaining, 50000);
    batch.idempotency_key = "release-broker".into();
    batch.request_hash = "release".into();
    let attempt = batch.attempt.as_mut().unwrap();
    attempt.lease_status = "released".into();
    attempt.ended_at_ms = Some(101);
    store.persist(&batch).await.unwrap();
    assert!(
        store
            .file_broker_recovery_diagnostic()
            .await
            .unwrap()
            .is_some()
    );
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    batch.idempotency_key = "new-broker-attempt".into();
    batch.request_hash = "new".into();
    let attempt = batch.attempt.as_mut().unwrap();
    attempt.attempt_id = "next".into();
    attempt.lease_epoch = 8;
    attempt.lease_status = "active".into();
    attempt.ended_at_ms = None;
    assert!(store.persist(&batch).await.is_err());
    assert_eq!(
        snapshot(&mut *store.inner.writer.lock().await).await,
        before
    );
    store
        .close_file_broker(&b, 0, audit("close"))
        .await
        .unwrap();
    store.persist(&batch).await.unwrap();
}
