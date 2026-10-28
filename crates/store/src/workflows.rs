//! Durable dispatch admission and ownership, separate from task acceptance.
use crate::{Store, StoreError, finish_transaction};
use serde::Serialize;
use sqlx::{Executor, FromRow};

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct WorkflowDispatch {
    pub task_id: String,
    #[serde(skip_serializing)]
    pub plan_json: String,
    pub status: String,
    pub boot_generation: Option<i64>,
    pub outcome_json: Option<String>,
    pub resume_version: Option<i64>,
    pub auto_recovery_count: i64,
    pub waiting_for_event: bool,
}
const SELECT: &str = "SELECT task_id, plan_json, status, boot_generation, outcome_json, resume_version, auto_recovery_count, EXISTS(SELECT 1 FROM workflow_wakes w WHERE w.task_id=workflow_dispatch.task_id AND w.delivery_id IS NULL) AS waiting_for_event FROM workflow_dispatch";
impl Store {
    /// Persists one immutable operator-approved plan, replaying identical input.
    /// # Errors
    /// Rejects collisions, invalid bounds and queue capacity exhaustion.
    pub async fn enqueue_workflow(
        &self,
        task_id: &str,
        plan: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        if task_id.is_empty() || task_id.len() > 128 || plan.len() > 131_072 {
            return Err(StoreError::Integrity(
                "invalid workflow admission bounds".into(),
            ));
        }
        let mut writer = self.inner.writer.lock().await;
        writer.execute("BEGIN IMMEDIATE").await?;
        let result = async {
            let existing: Option<String> = sqlx::query_scalar("SELECT plan_json FROM workflow_dispatch WHERE task_id = ?").bind(task_id).fetch_optional(&mut *writer).await?;
            if let Some(existing) = existing {
                return if existing == plan { Ok(()) } else { Err(StoreError::IdempotencyCollision) };
            }
        let input: serde_json::Value = serde_json::from_str(plan)
            .map_err(|_| StoreError::Integrity("invalid workflow JSON".into()))?;
        let wake = input
            .get("wake_subscription")
            .filter(|v| !v.is_null())
            .map(|v| {
                v.as_str()
                    .filter(|s| !s.is_empty() && s.len() <= 128)
                    .ok_or_else(|| StoreError::Integrity("invalid wake subscription".into()))
            })
            .transpose()?;
            let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_dispatch WHERE status IN ('queued','running')").fetch_one(&mut *writer).await?;
            if pending >= 256 { return Err(StoreError::Integrity("workflow queue capacity reached".into())); }
            sqlx::query("INSERT INTO workflow_dispatch(task_id,plan_json,status,created_at_ms,updated_at_ms) VALUES (?,?,'queued',?,?)")
                .bind(task_id).bind(plan).bind(now).bind(now).execute(&mut *writer).await?;
            if let Some(subscription) = wake {
                let eligible: Option<i64> = sqlx::query_scalar("SELECT 1 FROM event_subscriptions WHERE id=? AND agent_id=? AND active=1 AND echo_source=0")
                    .bind(subscription).bind(format!("{task_id}:worker")).fetch_optional(&mut *writer).await?;
                if eligible.is_none() { return Err(StoreError::Integrity("wake requires an active non-echo subscription for the planned worker".into())); }
                sqlx::query("INSERT INTO workflow_wakes(task_id,subscription_id) VALUES (?,?)").bind(task_id).bind(subscription).execute(&mut *writer).await?;
            }
            Ok(())
        }.await;
        finish_transaction(&mut writer, result).await
    }
    /// Reads the persisted trigger as bounded untrusted task context.
    /// # Errors
    /// Returns database errors.
    pub async fn workflow_trigger(&self, task_id: &str) -> Result<Option<String>, StoreError> {
        Ok(sqlx::query_scalar::<_, Option<String>>(
            "SELECT trigger_json FROM workflow_wakes WHERE task_id=?",
        )
        .bind(task_id)
        .fetch_optional(&self.inner.readers)
        .await?
        .flatten())
    }
    /// Claims at most one plan; no second running dispatch is admitted.
    /// # Errors
    /// Fails on database errors or stale boot ownership.
    pub async fn claim_workflow(
        &self,
        generation: u64,
        now: i64,
    ) -> Result<Option<WorkflowDispatch>, StoreError> {
        let generation =
            i64::try_from(generation).map_err(|_| StoreError::BootGenerationOverflow)?;
        let mut writer = self.inner.writer.lock().await;
        writer.execute("BEGIN IMMEDIATE").await?;
        let result = async {
            let current: i64 = sqlx::query_scalar("SELECT boot_generation FROM runtime_metadata WHERE singleton = 1").fetch_one(&mut *writer).await?;
            if current != generation { return Err(StoreError::Integrity("stale dispatcher boot".into())); }
            let running: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_dispatch WHERE status = 'running'").fetch_one(&mut *writer).await?;
            if running != 0 { return Ok(None); }
            let rows = sqlx::query_as::<_, WorkflowDispatch>(&format!("{SELECT} WHERE status = 'queued' ORDER BY created_at_ms,task_id LIMIT 256")).fetch_all(&mut *writer).await?;
            let mut selected = None;
            for row in rows {
                let binding: Option<(String, Option<String>)> = sqlx::query_as("SELECT subscription_id,delivery_id FROM workflow_wakes WHERE task_id=?").bind(&row.task_id).fetch_optional(&mut *writer).await?;
                if let Some((subscription, None)) = binding {
                        let delivery: Option<(String,String)> = sqlx::query_as("SELECT d.id,json_object('id',e.id,'correlation_id',e.correlation_id,'generation',e.generation,'topic',e.topic,'project_id',e.project_id,'task_id',e.task_id,'path_scope',e.path_scope,'payload',json(e.payload_json)) FROM event_deliveries d JOIN semantic_events e ON e.id=d.event_id JOIN event_subscriptions s ON s.id=d.subscription_id WHERE d.subscription_id=? AND s.active=1 AND s.echo_source=0 AND s.agent_id=d.target_agent_id AND d.target_agent_id=? AND e.source_agent_id<>d.target_agent_id AND d.status='pending' AND d.available_at_ms<=? AND e.expires_at_ms>? AND e.created_at_ms<=? AND d.attempts<e.max_retries AND e.generation<8 ORDER BY e.created_at_ms,d.id LIMIT 1")
                            .bind(&subscription).bind(format!("{}:worker",row.task_id)).bind(now).bind(now).bind(now).fetch_optional(&mut *writer).await?;
                        let Some((delivery, trigger)) = delivery else { continue; };
                        if trigger.len() > 65_536 { continue; }
                        sqlx::query("UPDATE event_deliveries SET status='acked',acked_at_ms=? WHERE id=?").bind(now).bind(&delivery).execute(&mut *writer).await?;
                        sqlx::query("UPDATE workflow_wakes SET delivery_id=?,trigger_json=? WHERE task_id=?").bind(delivery).bind(trigger).bind(&row.task_id).execute(&mut *writer).await?;
                }
                selected = Some(row); break;
            }
            let row = selected;
            if let Some(mut row) = row {
                sqlx::query("UPDATE workflow_dispatch SET status='running',boot_generation=?,updated_at_ms=? WHERE task_id=? AND status='queued'").bind(generation).bind(now).bind(&row.task_id).execute(&mut *writer).await?;
                row.status = "running".into(); row.boot_generation = Some(generation); row.waiting_for_event = false;
                Ok(Some(row))
            } else { Ok(None) }
        }.await;
        finish_transaction(&mut writer, result).await
    }
    /// Marks old claims for explicit reconciliation, never automatic relaunch.
    /// # Errors
    /// Returns database or generation conversion errors.
    pub async fn recover_workflow_dispatch(
        &self,
        generation: u64,
        now: i64,
    ) -> Result<(), StoreError> {
        let generation =
            i64::try_from(generation).map_err(|_| StoreError::BootGenerationOverflow)?;
        let mut writer = self.inner.writer.lock().await;
        sqlx::query("UPDATE workflow_dispatch SET status='needs_recovery', updated_at_ms=? WHERE status='running' AND boot_generation < ?")
            .bind(now).bind(generation).execute(&mut *writer).await?;
        Ok(())
    }
    /// Records dispatch outcome only for its owning boot.
    /// # Errors
    /// Rejects stale ownership and oversized outcomes.
    pub async fn finish_workflow(
        &self,
        task_id: &str,
        generation: u64,
        outcome: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        if outcome.len() > 131_072 {
            return Err(StoreError::Integrity("workflow outcome too large".into()));
        }
        let generation =
            i64::try_from(generation).map_err(|_| StoreError::BootGenerationOverflow)?;
        let mut writer = self.inner.writer.lock().await;
        let changed = sqlx::query("UPDATE workflow_dispatch SET status='finished',outcome_json=?,updated_at_ms=? WHERE task_id=? AND status='running' AND boot_generation=? AND boot_generation=(SELECT boot_generation FROM runtime_metadata WHERE singleton=1)")
            .bind(outcome).bind(now).bind(task_id).bind(generation).execute(&mut *writer).await?.rows_affected();
        if changed != 1 {
            return Err(StoreError::Integrity("dispatch ownership changed".into()));
        }
        Ok(())
    }
    /// Queues an explicitly approved continuation against a specific task version.
    /// # Errors
    /// Refuses running/queued/missing dispatches or an invalid version.
    pub async fn requeue_workflow(
        &self,
        task_id: &str,
        version: u64,
        now: i64,
    ) -> Result<(), StoreError> {
        let version =
            i64::try_from(version).map_err(|_| StoreError::Integrity("version overflow".into()))?;
        let mut writer = self.inner.writer.lock().await;
        let changed = sqlx::query("UPDATE workflow_dispatch SET status='queued',resume_version=?,outcome_json=NULL,updated_at_ms=? WHERE task_id=? AND status IN ('finished','needs_recovery') AND (SELECT COUNT(*) FROM workflow_dispatch WHERE status IN ('queued','running')) < 256")
            .bind(version).bind(now).bind(task_id).execute(&mut *writer).await?.rows_affected();
        if changed != 1 {
            return Err(StoreError::Integrity(
                "dispatch is not available for continuation".into(),
            ));
        }
        Ok(())
    }

    /// Bounded unfinished claims eligible for one opt-in automatic continuation.
    /// # Errors
    /// Returns database errors.
    pub async fn interrupted_workflows(&self) -> Result<Vec<WorkflowDispatch>, StoreError> {
        Ok(sqlx::query_as::<_,WorkflowDispatch>(&format!("{SELECT} WHERE status='needs_recovery' AND auto_recovery_count=0 ORDER BY created_at_ms,task_id LIMIT 256")).fetch_all(&self.inner.readers).await?)
    }
    /// Requeues at most once automatically, bound to the active daemon boot.
    /// # Errors
    /// Returns database or integer conversion errors; false means no admission.
    pub async fn auto_requeue_workflow(
        &self,
        task_id: &str,
        version: u64,
        generation: u64,
        now: i64,
    ) -> Result<bool, StoreError> {
        let version =
            i64::try_from(version).map_err(|_| StoreError::Integrity("version overflow".into()))?;
        let generation =
            i64::try_from(generation).map_err(|_| StoreError::BootGenerationOverflow)?;
        let mut writer = self.inner.writer.lock().await;
        let changed=sqlx::query("UPDATE workflow_dispatch SET status='queued',resume_version=?,auto_recovery_count=1,outcome_json=NULL,updated_at_ms=? WHERE task_id=? AND status='needs_recovery' AND auto_recovery_count=0 AND (SELECT boot_generation FROM runtime_metadata WHERE singleton=1)=? AND (SELECT COUNT(*) FROM workflow_dispatch WHERE status IN ('queued','running'))<256")
            .bind(version).bind(now).bind(task_id).bind(generation).execute(&mut *writer).await?.rows_affected();
        Ok(changed == 1)
    }

    /// Returns the latest bounded dispatch receipts, not stored plan contents.
    /// # Errors
    /// Returns database errors.
    pub async fn workflows(&self) -> Result<Vec<WorkflowDispatch>, StoreError> {
        Ok(sqlx::query_as::<_, WorkflowDispatch>(&format!(
            "{SELECT} ORDER BY created_at_ms DESC,task_id LIMIT 256"
        ))
        .fetch_all(&self.inner.readers)
        .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn automatic_recovery_is_once_only_and_boot_bound() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("dispatch.sqlite3"))
            .await
            .unwrap();
        let first = store.start_runtime(1).await.unwrap().boot_generation;
        store.enqueue_workflow("a", "{}", 2).await.unwrap();
        store.claim_workflow(first, 3).await.unwrap().unwrap();
        let second = store.start_runtime(4).await.unwrap().boot_generation;
        store.recover_workflow_dispatch(second, 5).await.unwrap();
        assert_eq!(store.interrupted_workflows().await.unwrap().len(), 1);
        assert!(!store.auto_requeue_workflow("a", 7, first, 6).await.unwrap());
        assert!(
            store
                .auto_requeue_workflow("a", 7, second, 6)
                .await
                .unwrap()
        );
        let row = store.claim_workflow(second, 7).await.unwrap().unwrap();
        assert_eq!(row.resume_version, Some(7));
        assert_eq!(row.auto_recovery_count, 1);
        let third = store.start_runtime(8).await.unwrap().boot_generation;
        store.recover_workflow_dispatch(third, 9).await.unwrap();
        assert!(store.interrupted_workflows().await.unwrap().is_empty());
        assert!(
            !store
                .auto_requeue_workflow("a", 9, third, 10)
                .await
                .unwrap()
        );
        store.requeue_workflow("a", 9, 10).await.unwrap();
        let row = store.claim_workflow(third, 11).await.unwrap().unwrap();
        assert_eq!(row.auto_recovery_count, 1);
        assert_eq!(row.resume_version, Some(9));
    }
    #[tokio::test]
    async fn dispatch_is_single_claim_idempotent_and_restart_fenced() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("dispatch.sqlite3"))
            .await
            .unwrap();
        let first = store.start_runtime(1).await.unwrap().boot_generation;
        store.enqueue_workflow("a", "{}", 2).await.unwrap();
        store.enqueue_workflow("a", "{}", 3).await.unwrap();
        assert!(matches!(
            store.enqueue_workflow("a", "changed", 3).await,
            Err(StoreError::IdempotencyCollision)
        ));
        store.enqueue_workflow("b", "{}", 4).await.unwrap();
        let (left, right) = tokio::join!(
            store.claim_workflow(first, 5),
            store.claim_workflow(first, 5)
        );
        assert_eq!(
            usize::from(left.unwrap().is_some()) + usize::from(right.unwrap().is_some()),
            1
        );
        let second = store.start_runtime(6).await.unwrap().boot_generation;
        assert!(store.claim_workflow(first, 7).await.is_err());
        assert!(store.finish_workflow("a", first, "{}", 7).await.is_err());
        store.recover_workflow_dispatch(second, 8).await.unwrap();
        let rows = store.workflows().await.unwrap();
        assert_eq!(
            rows.iter().find(|r| r.task_id == "a").unwrap().status,
            "needs_recovery"
        );
        assert_eq!(
            rows.iter().find(|r| r.task_id == "b").unwrap().status,
            "queued"
        );
        let next = store.claim_workflow(second, 9).await.unwrap().unwrap();
        assert_eq!(next.task_id, "b");
        store
            .finish_workflow("b", second, "{\"completed\":false}", 10)
            .await
            .unwrap();
        assert!(store.claim_workflow(second, 11).await.unwrap().is_none());
        store.requeue_workflow("a", 17, 11).await.unwrap();
        assert!(store.requeue_workflow("a", 17, 11).await.is_err());
        let continued = store.claim_workflow(second, 11).await.unwrap().unwrap();
        assert_eq!(continued.task_id, "a");
        assert_eq!(continued.resume_version, Some(17));
        assert!(store.requeue_workflow("a", 17, 11).await.is_err());

        assert!(store.finish_workflow("b", second, "{}", 12).await.is_err());
        assert!(
            !serde_json::to_string(&store.workflows().await.unwrap())
                .unwrap()
                .contains("plan_json")
        );
    }
}
