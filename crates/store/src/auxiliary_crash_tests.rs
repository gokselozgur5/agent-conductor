//! Abrupt SQLite publication exits, distinct from native worker recovery.
use super::transaction_cancellation::{auxiliary_origin_fixture, snapshot};
use super::*;
use sha2::{Digest, Sha256};
use std::path::Path;

fn publication(input: &AuxiliaryPreparation, key: &str) -> crate::CommitBatch {
    use crate::{AuditAppend, CommitBatch, TaskMutation, TaskTransitionAppend};
    let kind = if input.kind == AuxiliaryKind::Check {
        "record_check"
    } else {
        "record_verdict"
    };
    let payload=serde_json::json!([input.task_id,{"actor":{"id":"publisher","role":"system"},"expected_version":input.task_version,"correlation_id":input.invocation_id,"idempotency_key":key,"reason":"bound result","kind":{"type":kind,"source":input.source,"check_id":"test","passed":true,"accepted":true}}]).to_string();
    CommitBatch {
        actor_id:"publisher".into(),idempotency_key:key.into(),request_hash:format!("{:x}",Sha256::digest(payload.as_bytes())),
        task:Some(TaskMutation{task_id:input.task_id.clone(),expected_version:input.task_version,new_version:input.task_version+1,state:"verifying".into(),aggregate_json:serde_json::json!({"id":input.task_id,"version":input.task_version+1,"state":"verifying","attempt":{"id":input.attempt_id},"verification":{"source":input.source}}).to_string(),updated_at_ms:100}),
        project:None,agent:None,new_task:None,attempt:None,
        transitions:vec![TaskTransitionAppend{id:format!("transition:{key}"),task_id:input.task_id.clone(),actor_id:"publisher".into(),prior_state:"verifying".into(),new_state:"verifying".into(),reason:"bound result".into(),correlation_id:input.invocation_id.clone(),occurred_at_ms:100}],
        audit:vec![AuditAppend{id:format!("audit:{key}"),task_id:Some(input.task_id.clone()),actor_id:"publisher".into(),kind:kind.into(),correlation_id:input.invocation_id.clone(),payload_json:payload,occurred_at_ms:100}],
        effects:vec![],outcome_json:"{}".into(),committed_at_ms:100,expires_at_ms:10000,
    }
}
fn audit(id: &str) -> EffectAudit<'_> {
    EffectAudit {
        id,
        actor_id: "runtime",
        correlation_id: "fixture",
        now_ms: 100,
    }
}
async fn publication_fixture(role: &str) -> (tempfile::TempDir, Store, AuxiliaryPreparation) {
    let (root, store, mut input, host) = auxiliary_origin_fixture("admit").await;
    if role == "verifier" {
        input.kind = AuxiliaryKind::Verifier;
    }
    let binding = AuxiliaryLaunchBinding {
        launch_sha256: "c".repeat(64),
        authority_id: host.authority_id.clone(),
        receipt_root: "/private/receipts".into(),
        wrapper_path: "/private/wrapper".into(),
        wrapper_sha256: "e".repeat(64),
    };
    let process = ExecutionProcess {
        platform: host.platform.clone(),
        host_id: host.host_id.clone(),
        boot_id: host.boot_id.clone(),
        pid: 123,
        start_marker: "macos-start-us:1:2".into(),
    };
    store
        .prepare_auxiliary_with_host(&input, &host, audit("publication-prepare"))
        .await
        .unwrap();
    store
        .bind_auxiliary_launch(&input, &binding, audit("publication-bind"))
        .await
        .unwrap();
    store
        .authorize_auxiliary_release(&input, &binding, &process, audit("publication-release"))
        .await
        .unwrap();
    let proof=serde_json::json!({"version":1,"invocation_id":input.invocation_id,"authority_id":binding.authority_id,"boot_generation":input.boot_generation,"launch_sha256":binding.launch_sha256,"process":{"host":{"platform":process.platform,"host_id":process.host_id,"boot_id":process.boot_id},"pid":process.pid,"start_marker":process.start_marker},"observation":"owned_group_settled"}).to_string();
    store
        .settle_auxiliary_proof(
            &input,
            &binding,
            &process,
            2,
            &proof,
            audit("publication-settle"),
        )
        .await
        .unwrap();
    (root, store, input)
}
async fn crash_publication(mode: &str, control: &Path) -> ! {
    let (role, stage) = mode.split_once(':').unwrap();
    let (root, store, input) = publication_fixture(role).await;
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    std::fs::write(
        control.join("before.json"),
        serde_json::to_vec(&before).unwrap(),
    )
    .unwrap();
    std::fs::write(
        control.join("database"),
        root.path().join("state.sqlite3").to_str().unwrap(),
    )
    .unwrap();
    std::fs::write(
        control.join("input.json"),
        serde_json::to_vec(&input).unwrap(),
    )
    .unwrap();
    if stage != "after_commit" {
        let table = match stage {
            "task_write" => "tasks",
            "link_write" => "auxiliary_publications",
            _ => panic!("unknown crash stage"),
        };
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |update| {
                if update.table == table {
                    std::process::exit(78);
                }
            });
    }
    assert!(matches!(
        store
            .persist_auxiliary_publication(&input, &publication(&input, "crash-result"))
            .await
            .unwrap(),
        CommitResult::Applied { .. }
    ));
    assert_eq!(stage, "after_commit");
    std::process::exit(78);
}
async fn verify_publication(mode: &str, control: &Path) {
    let path = std::fs::read_to_string(control.join("database")).unwrap();
    let store = Store::open(path).await.unwrap();
    let input: AuxiliaryPreparation =
        serde_json::from_slice(&std::fs::read(control.join("input.json")).unwrap()).unwrap();
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(store.readers())
        .await
        .unwrap();
    assert_eq!(integrity, "ok");
    let batch = publication(&input, "crash-result");
    let before: Vec<(String, String)> =
        serde_json::from_slice(&std::fs::read(control.join("before.json")).unwrap()).unwrap();
    if !mode.ends_with("after_commit") {
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
        assert!(matches!(
            store
                .persist_auxiliary_publication(&input, &batch)
                .await
                .unwrap(),
            CommitResult::Applied { .. }
        ));
    }
    let before_replay = snapshot(&mut *store.inner.writer.lock().await).await;
    for table in [
        "auxiliary_invocations",
        "auxiliary_host_evidence",
        "executions",
    ] {
        assert_eq!(
            before.iter().find(|row| row.0 == table),
            before_replay.iter().find(|row| row.0 == table)
        );
    }
    let audit: (String, String, String) =
        sqlx::query_as("SELECT kind,correlation_id,payload_json FROM audit_events WHERE id=?")
            .bind(&batch.audit[0].id)
            .fetch_one(store.readers())
            .await
            .unwrap();
    assert_eq!(
        audit,
        (
            batch.audit[0].kind.clone(),
            input.invocation_id.clone(),
            batch.audit[0].payload_json.clone()
        )
    );

    assert!(matches!(
        store
            .persist_auxiliary_publication(&input, &batch)
            .await
            .unwrap(),
        CommitResult::Replayed { .. }
    ));
    assert_eq!(
        snapshot(&mut *store.inner.writer.lock().await).await,
        before_replay
    );
    let task: (i64, String, String) =
        sqlx::query_as("SELECT version,state,aggregate_json FROM tasks WHERE id='task'")
            .fetch_one(store.readers())
            .await
            .unwrap();
    let expected = batch.task.as_ref().unwrap();
    assert_eq!(
        task,
        (
            expected.new_version,
            expected.state.clone(),
            expected.aggregate_json.clone()
        )
    );
    let link: (String,String,String,String,String) = sqlx::query_as("SELECT invocation_id,audit_id,actor_id,idempotency_key,request_hash FROM auxiliary_publications").fetch_one(store.readers()).await.unwrap();
    assert_eq!(
        link,
        (
            input.invocation_id.clone(),
            batch.audit[0].id.clone(),
            batch.actor_id.clone(),
            batch.idempotency_key.clone(),
            batch.request_hash.clone()
        )
    );
    assert_eq!(
        store
            .command_outcome(&batch.actor_id, &batch.idempotency_key, &batch.request_hash)
            .await
            .unwrap(),
        Some(batch.outcome_json.clone())
    );
    for table in ["auxiliary_publications", "task_transitions"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(store.readers())
            .await
            .unwrap();
        assert_eq!(count, 1);
    }
    assert!(matches!(
        store
            .persist_auxiliary_publication(&input, &publication(&input, "different-result"))
            .await,
        Err(StoreError::IdempotencyCollision)
    ));
}
#[tokio::test]
async fn publication_survives_abrupt_exit_and_replay() {
    const MODE: &str = "CONDUCTOR_PUBLICATION_CRASH_MODE";
    const CONTROL: &str = "CONDUCTOR_PUBLICATION_CRASH_CONTROL";
    if let Ok(mode) = std::env::var(MODE) {
        crash_publication(&mode, Path::new(&std::env::var(CONTROL).unwrap())).await;
    }
    for role in ["check", "verifier"] {
        for stage in ["task_write", "link_write", "after_commit"] {
            let mode = format!("{role}:{stage}");
            let control = tempfile::tempdir().unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "auxiliary_crash_tests::publication_survives_abrupt_exit_and_replay",
                    "--nocapture",
                ])
                .env(MODE, &mode)
                .env(CONTROL, control.path())
                .env("TMPDIR", control.path())
                .spawn()
                .unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            let status = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                if tokio::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("publication child timed out: {mode}");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            assert_eq!(status.code(), Some(78), "{mode}");
            verify_publication(&mode, control.path()).await;
        }
    }
}

async fn crash_host_reconciliation(mode: &str, control: &Path) -> ! {
    let (operation, boundary) = mode.split_once(':').unwrap();
    let (root, store, input, host) = auxiliary_origin_fixture(operation).await;
    let before = snapshot(&mut *store.inner.writer.lock().await).await;
    std::fs::write(
        control.join("database"),
        root.path().join("state.sqlite3").to_str().unwrap(),
    )
    .unwrap();
    std::fs::write(
        control.join("before.json"),
        serde_json::to_vec(&before).unwrap(),
    )
    .unwrap();
    if boundary == "before" {
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(|update| {
                if update.table == "audit_events" {
                    std::process::exit(81);
                }
            });
    }
    store
        .reconcile_auxiliary_host(&input, &host, audit("crash-host-reconciliation"))
        .await
        .unwrap();
    assert_eq!(
        boundary, "after",
        "selected pre-commit write was not reached"
    );
    let after = snapshot(&mut *store.inner.writer.lock().await).await;
    assert_ne!(before, after, "positive control did not change state");
    std::fs::write(
        control.join("after.json"),
        serde_json::to_vec(&after).unwrap(),
    )
    .unwrap();
    std::process::exit(82);
}

#[tokio::test]
async fn host_reconciliation_survives_abrupt_exit_on_both_sides() {
    const MODE: &str = "CONDUCTOR_HOST_RECONCILIATION_CRASH_MODE";
    const CONTROL: &str = "CONDUCTOR_HOST_RECONCILIATION_CRASH_CONTROL";
    if let Ok(mode) = std::env::var(MODE) {
        crash_host_reconciliation(
            &mode,
            &std::path::PathBuf::from(std::env::var(CONTROL).unwrap()),
        )
        .await;
    }
    for operation in ["observe", "settle"] {
        for boundary in ["before", "after"] {
            let mode = format!("{operation}:{boundary}");
            let control = tempfile::tempdir().unwrap();
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "auxiliary_crash_tests::host_reconciliation_survives_abrupt_exit_on_both_sides",
                    "--nocapture",
                ])
                .env(MODE, &mode)
                .env(CONTROL, control.path())
                .env("TMPDIR", control.path())
                .spawn()
                .unwrap();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
            let status = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                if tokio::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("host reconciliation crash deadline: {mode}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            };
            assert_eq!(
                status.code(),
                Some(if boundary == "before" { 81 } else { 82 }),
                "{mode}"
            );
            let store =
                Store::open(std::fs::read_to_string(control.path().join("database")).unwrap())
                    .await
                    .unwrap();
            let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
                .fetch_one(store.readers())
                .await
                .unwrap();
            assert_eq!(integrity, "ok");
            let expected: Vec<(String, String)> = serde_json::from_slice(
                &std::fs::read(control.path().join(if boundary == "before" {
                    "before.json"
                } else {
                    "after.json"
                }))
                .unwrap(),
            )
            .unwrap();
            assert_eq!(
                snapshot(&mut *store.inner.writer.lock().await).await,
                expected,
                "{mode}"
            );
            assert_eq!(
                snapshot(&mut store.readers().acquire().await.unwrap()).await,
                expected,
                "{mode}"
            );
        }
    }
}
