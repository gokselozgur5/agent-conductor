//! Reconcile dispatch bookkeeping from historical acceptance, never run work.
use crate::{Store, StoreError};
use serde_json::{Value, json};
use sqlx::{Connection, Row, SqliteConnection};

fn accepted_source(id: &str, version: i64, aggregate: &str) -> Option<(String, String, String)> {
    let value: Value = serde_json::from_str(aggregate).ok()?;
    if version < 1
        || value["id"] != id
        || value["version"].as_i64() != Some(version)
        || value["state"] != "completed"
        || !crate::completion_projection_is_valid(&value)
        || !value["decisions"].is_array()
    {
        return None;
    }
    let text = |pointer| {
        value
            .pointer(pointer)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 256)
            .map(str::to_owned)
    };
    let worker = text("/attempt/worker")?;
    let attempt = text("/attempt/id")?;
    let verifier = text("/verification/verdict/verifier")?;
    if verifier == worker {
        return None;
    }
    let source = text("/verification/source")?;
    let hash = source.strip_prefix("snapshot:")?;
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    let required = value["required_checks"].as_array()?;
    let checks = value["verification"]["checks"].as_array()?;
    let mut ids = std::collections::BTreeSet::new();
    for check in required {
        let id = check["id"]
            .as_str()
            .filter(|id| !id.is_empty() && id.len() <= 256)?;
        if !ids.insert(id)
            || !checks.iter().any(|c| {
                c["check_id"] == id
                    && c["passed"] == true
                    && c["source"] == source
                    && c["recorded_by"]
                        .as_str()
                        .is_some_and(|v| !v.is_empty() && v != worker)
            })
        {
            return None;
        }
    }
    Some((source, attempt, worker))
}
async fn ownership_clear(
    db: &mut SqliteConnection,
    id: &str,
    version: i64,
    source: &str,
    attempt: &str,
    worker: &str,
) -> Result<bool, StoreError> {
    let valid:i64=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM attempts WHERE id=?2 AND task_id=?1 AND worker_id=?3 AND ended_at_ms IS NOT NULL) AND NOT EXISTS(SELECT 1 FROM leases WHERE task_id=?1 AND status IN ('active','recovery_pending')) AND NOT EXISTS(SELECT 1 FROM executions WHERE task_id=?1 AND state!='fenced') AND NOT EXISTS(SELECT 1 FROM auxiliary_invocations WHERE task_id=?1 AND state='pending') AND NOT EXISTS(SELECT 1 FROM legacy_auxiliary_ownership WHERE task_id=?1) AND NOT EXISTS(SELECT 1 FROM unresolved_file_broker_tasks WHERE task_id=?1) AND EXISTS(SELECT 1 FROM audit_events WHERE task_id=?1 AND kind='complete' AND CASE WHEN length(payload_json)<=1048576 AND json_valid(payload_json) THEN json_extract(payload_json,'$[0]')=?1 AND json_extract(payload_json,'$[1].kind.type')='complete' AND json_extract(payload_json,'$[1].kind.observed_source')=?4 AND json_extract(payload_json,'$[1].expected_version')=?5 ELSE 0 END)")
        .bind(id).bind(attempt).bind(worker).bind(source).bind(version-1).fetch_one(db).await?;
    Ok(valid == 1)
}
impl Store {
    /// Reconciles one bounded page of prior-generation completed dispatches.
    /// Historical acceptance supplies the outcome; no filesystem recheck or work
    /// launch occurs. Invalid rows remain unchanged while the shared cursor advances.
    /// # Errors
    /// Refuses a stale/future generation; database/audit failure rolls back the page.
    pub async fn reconcile_completed_dispatch(
        &self,
        generation: u64,
        now: i64,
    ) -> Result<usize, StoreError> {
        let generation =
            i64::try_from(generation).map_err(|_| StoreError::BootGenerationOverflow)?;
        let mut cursor = self.inner.completed_dispatch_cursor.lock().await;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        let current: i64 =
            sqlx::query_scalar("SELECT boot_generation FROM runtime_metadata WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await?;
        if generation <= 0 || generation != current {
            return Err(StoreError::Integrity(
                "completed dispatch reconciliation generation refused".into(),
            ));
        }
        let rows=sqlx::query("SELECT d.task_id,d.boot_generation,t.version,CASE WHEN length(t.aggregate_json)<=1048576 THEN t.aggregate_json ELSE NULL END AS aggregate_json FROM workflow_dispatch d JOIN tasks t ON t.id=d.task_id WHERE d.status='needs_recovery' AND d.boot_generation>0 AND d.boot_generation<?1 AND t.state='completed' AND (?2 IS NULL OR d.task_id>?2) ORDER BY d.task_id LIMIT 128")
            .bind(generation).bind(cursor.as_deref()).fetch_all(&mut *tx).await?;
        let next = if rows.len() == 128 {
            rows.last().map(|r| r.get::<String, _>("task_id"))
        } else {
            None
        };
        let mut count = 0;
        for row in rows {
            let id: String = row.try_get("task_id")?;
            let version: i64 = row.try_get("version")?;
            let Some(aggregate) = row.try_get::<Option<String>, _>("aggregate_json")? else {
                continue;
            };
            let Some((source, attempt, worker)) = accepted_source(&id, version, &aggregate) else {
                continue;
            };
            if !ownership_clear(&mut tx, &id, version, &source, &attempt, &worker).await? {
                continue;
            }
            let previous: i64 = row.try_get("boot_generation")?;
            let outcome =
                json!({"completed":true,"source":source,"reconciled_from_task":true}).to_string();
            sqlx::query("UPDATE workflow_dispatch SET status='finished',outcome_json=?,updated_at_ms=? WHERE task_id=? AND status='needs_recovery'")
                .bind(outcome).bind(now).bind(&id).execute(&mut *tx).await?;
            let payload=json!({"task_version":version,"source":source,"previous_generation":previous,"generation":generation}).to_string();
            sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,'conductor-runtime','workflow.completed_reconciled',?,?,?)")
                .bind(uuid::Uuid::now_v7().to_string()).bind(&id).bind(&id).bind(payload).bind(now).execute(&mut *tx).await?;
            count += 1;
        }
        tx.commit().await?;
        *cursor = next;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Executor;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::time::Duration;

    async fn fixture() -> (tempfile::TempDir, Store) {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("state.sqlite3"))
            .await
            .unwrap();
        store.start_runtime(0).await.unwrap();
        store.start_runtime(1).await.unwrap();
        store.inner.writer.lock().await.execute("INSERT INTO projects VALUES('p','/project','fixture',NULL,0); INSERT INTO agents VALUES('worker','fake','v1','hash','read_write',0);").await.unwrap();
        (root, store)
    }
    async fn seed(store: &Store, id: &str) -> String {
        let source = format!("snapshot:{}", "b".repeat(64));
        let attempt = format!("attempt:{id}");
        let aggregate=json!({"id":id,"version":7,"state":"completed","attempt":{"id":attempt,"worker":"worker","lease":{"status":"ended"}},"decisions":[],"required_checks":[{"id":"test"}],"verification":{"source":source,"checks":[{"check_id":"test","passed":true,"source":source,"recorded_by":"checker"}],"verdict":{"accepted":true,"source":source,"verifier":"reviewer"}}}).to_string();
        let mut db = store.inner.writer.lock().await;
        sqlx::query("INSERT INTO tasks(id,project_id,objective,state,priority,version,authority_json,aggregate_json,created_at_ms,updated_at_ms) VALUES(?,'p','fixture','completed',0,7,'{}',?,0,0)").bind(id).bind(&aggregate).execute(&mut *db).await.unwrap();
        sqlx::query("INSERT INTO attempts(id,task_id,worker_id,lease_epoch,instruction_hashes_json,required_checks_json,capability_hash,started_at_ms,ended_at_ms) VALUES(?,?,'worker',1,'[]','[]','hash',0,1)").bind(attempt).bind(id).execute(&mut *db).await.unwrap();
        sqlx::query("INSERT INTO workflow_dispatch(task_id,plan_json,status,boot_generation,created_at_ms,updated_at_ms,auto_recovery_count) VALUES(?,'{}','needs_recovery',1,0,0,1)").bind(id).execute(&mut *db).await.unwrap();
        let payload =
            json!([id,{"expected_version":6,"kind":{"type":"complete","observed_source":source}}])
                .to_string();
        sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,'system','complete',?,?,0)").bind(format!("complete:{id}")).bind(id).bind(id).bind(payload).execute(&mut *db).await.unwrap();
        aggregate
    }
    #[tokio::test]
    async fn completed_dispatch_reconciles_once_without_task_or_counter_mutation() {
        let (_root, store) = fixture().await;
        let original = seed(&store, "task").await;
        assert!(store.reconcile_completed_dispatch(1, 2).await.is_err());
        assert!(store.reconcile_completed_dispatch(3, 2).await.is_err());
        assert_eq!(store.reconcile_completed_dispatch(2, 2).await.unwrap(), 1);
        assert_eq!(store.reconcile_completed_dispatch(2, 3).await.unwrap(), 0);
        let row: (String, String, i64, i64) = sqlx::query_as(
            "SELECT status,outcome_json,boot_generation,auto_recovery_count FROM workflow_dispatch",
        )
        .fetch_one(store.readers())
        .await
        .unwrap();
        assert_eq!((&row.0, row.2, row.3), (&"finished".to_owned(), 1, 1));
        let outcome: Value = serde_json::from_str(&row.1).unwrap();
        assert_eq!(outcome["completed"], true);
        assert_eq!(outcome["reconciled_from_task"], true);
        assert_eq!(
            store.task("task").await.unwrap().unwrap().aggregate_json,
            original
        );
        let audits: Vec<(String, i64)> =
            sqlx::query_as("SELECT kind,COUNT(*) FROM audit_events GROUP BY kind ORDER BY kind")
                .fetch_all(store.readers())
                .await
                .unwrap();
        assert_eq!(
            audits,
            vec![
                ("complete".into(), 1),
                ("workflow.completed_reconciled".into(), 1)
            ]
        );
    }
    #[tokio::test]
    async fn invalid_completion_projection_and_ownership_remain_unreconciled() {
        for mutation in [
            "UPDATE tasks SET aggregate_json='{}'",
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.padding',printf('%.*c',1048576,'x'))",
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.verification.verdict.verifier','')",
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.id','other')",
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.version',6)",
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.verification.verdict.verifier','worker')",
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.verification.checks[0].passed',json('false'))",
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.required_checks[0].id',json('null'),'$.verification.checks[0].check_id',json('null'))",
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.decisions',json('[{}]'))",
            "UPDATE tasks SET aggregate_json=json_set(aggregate_json,'$.attempt.lease.status','active')",
            "UPDATE tasks SET state='verifying'",
            "UPDATE workflow_dispatch SET boot_generation=2",
            "UPDATE workflow_dispatch SET boot_generation=3",
            "UPDATE workflow_dispatch SET status='queued'",
            "UPDATE attempts SET ended_at_ms=NULL",
            "DELETE FROM audit_events",
            "UPDATE audit_events SET payload_json=json_set(payload_json,'$[1].expected_version',5)",
            "INSERT INTO legacy_auxiliary_ownership VALUES('task','verification_without_invocation',0)",
            "INSERT INTO leases VALUES('attempt:task','task',1,'active',1,100,10,100,0)",
        ] {
            let (_root, store) = fixture().await;
            seed(&store, "task").await;
            store
                .inner
                .writer
                .lock()
                .await
                .execute(mutation)
                .await
                .unwrap();
            let before =
                crate::transaction_cancellation::snapshot(&mut *store.inner.writer.lock().await)
                    .await;
            assert_eq!(
                store.reconcile_completed_dispatch(2, 2).await.unwrap(),
                0,
                "{mutation}"
            );
            assert_eq!(
                crate::transaction_cancellation::snapshot(&mut *store.inner.writer.lock().await)
                    .await,
                before
            );
        }
    }
    #[tokio::test]
    async fn unfinished_process_ownership_blocks_completed_dispatch_reconciliation() {
        for ownership in ["execution", "auxiliary", "broker"] {
            let (_root, store) = fixture().await;
            seed(&store, "task").await;
            {
                let mut db = store.inner.writer.lock().await;
                db.execute("INSERT INTO effect_intents(token,task_id,attempt_id,lease_epoch,correlation_id,effect_kind,request_json,status,created_at_ms,updated_at_ms) VALUES('effect','task','attempt:task',1,'fixture','worker.spawn','{}','claimed',0,0); INSERT INTO executions(effect_token,task_id,attempt_id,lease_epoch,runner_id,packet_json,packet_sha256,state,created_at_ms,updated_at_ms) VALUES('effect','task','attempt:task',1,'runner','{}','hash','started',0,0);").await.unwrap();
                if ownership == "auxiliary" {
                    db.execute("UPDATE executions SET state='fenced'; INSERT INTO auxiliary_invocations(invocation_id,task_id,execution_token,preparation_json,boot_generation,state,created_at_ms) VALUES('aux','task','effect','{}',1,'pending',0);").await.unwrap();
                }
            }
            if ownership == "broker" {
                // Historical contradictory fixture: broker still open despite an old fence.
                store.inner.writer.lock().await.execute("UPDATE executions SET state='fenced'; INSERT INTO file_brokers(invocation_id,execution_token,binding_json,state,created_at_ms) VALUES('broker','effect','{}','open',0)").await.unwrap();
            }
            let before =
                crate::transaction_cancellation::snapshot(&mut *store.inner.writer.lock().await)
                    .await;
            assert_eq!(store.reconcile_completed_dispatch(2, 2).await.unwrap(), 0);
            assert_eq!(
                crate::transaction_cancellation::snapshot(&mut *store.inner.writer.lock().await)
                    .await,
                before
            );
        }
    }
    #[tokio::test]
    async fn invalid_page_does_not_starve_valid_completion_and_clones_share_cursor() {
        let (_root, store) = fixture().await;
        for n in 0..128 {
            seed(&store, &format!("a{n:03}")).await;
        }
        store
            .inner
            .writer
            .lock()
            .await
            .execute("UPDATE tasks SET aggregate_json='{}'")
            .await
            .unwrap();
        seed(&store, "z-valid").await;
        assert_eq!(store.reconcile_completed_dispatch(2, 2).await.unwrap(), 0);
        assert_eq!(
            store
                .clone()
                .reconcile_completed_dispatch(2, 3)
                .await
                .unwrap(),
            1
        );
        assert_eq!(store.reconcile_completed_dispatch(2, 4).await.unwrap(), 0);
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_dispatch WHERE status='needs_recovery'",
        )
        .fetch_one(store.readers())
        .await
        .unwrap();
        assert_eq!(count, 128);
    }
    #[tokio::test]
    async fn reconciliation_audit_failure_and_cancellation_roll_back_dispatch() {
        let (_root, store) = fixture().await;
        seed(&store, "task").await;
        let before =
            crate::transaction_cancellation::snapshot(&mut *store.inner.writer.lock().await).await;
        store.inner.writer.lock().await.execute("CREATE TRIGGER reject_reconciliation BEFORE INSERT ON audit_events WHEN NEW.kind='workflow.completed_reconciled' BEGIN SELECT RAISE(ABORT,'fixture'); END;").await.unwrap();
        assert!(store.reconcile_completed_dispatch(2, 2).await.is_err());
        assert_eq!(
            crate::transaction_cancellation::snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
        store
            .inner
            .writer
            .lock()
            .await
            .execute("DROP TRIGGER reject_reconciliation")
            .await
            .unwrap();
        let written = Arc::new(AtomicBool::new(false));
        let observed = written.clone();
        let (release, wait) = mpsc::channel();
        store
            .inner
            .writer
            .lock()
            .await
            .lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |update| {
                if update.table == "audit_events" {
                    observed.store(true, Ordering::SeqCst);
                    wait.recv_timeout(Duration::from_secs(10)).unwrap();
                }
            });
        let owned = store.clone();
        let request = tokio::spawn(async move { owned.reconcile_completed_dispatch(2, 2).await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !written.load(Ordering::SeqCst) {
                assert!(!request.is_finished());
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let mut db = store.inner.writer.lock().await;
        db.lock_handle().await.unwrap().remove_update_hook();
        assert_eq!(
            crate::transaction_cancellation::snapshot(&mut db).await,
            before
        );
        drop(db);
        assert_eq!(store.reconcile_completed_dispatch(2, 3).await.unwrap(), 1);
    }
}
