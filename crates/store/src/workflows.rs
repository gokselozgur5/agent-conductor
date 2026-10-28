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
}
const SELECT: &str =
    "SELECT task_id, plan_json, status, boot_generation, outcome_json FROM workflow_dispatch";
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
            let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_dispatch WHERE status IN ('queued','running')").fetch_one(&mut *writer).await?;
            if pending >= 256 { return Err(StoreError::Integrity("workflow queue capacity reached".into())); }
            sqlx::query("INSERT INTO workflow_dispatch(task_id,plan_json,status,created_at_ms,updated_at_ms) VALUES (?,?,'queued',?,?)")
                .bind(task_id).bind(plan).bind(now).bind(now).execute(&mut *writer).await?;
            Ok(())
        }.await;
        finish_transaction(&mut writer, result).await
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
            let row = sqlx::query_as::<_, WorkflowDispatch>(&format!("{SELECT} WHERE status = 'queued' ORDER BY created_at_ms,task_id LIMIT 1")).fetch_optional(&mut *writer).await?;
            if let Some(mut row) = row {
                sqlx::query("UPDATE workflow_dispatch SET status='running',boot_generation=?,updated_at_ms=? WHERE task_id=? AND status='queued'").bind(generation).bind(now).bind(&row.task_id).execute(&mut *writer).await?;
                row.status = "running".into(); row.boot_generation = Some(generation);
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
        assert!(store.finish_workflow("b", second, "{}", 12).await.is_err());
        assert!(
            !serde_json::to_string(&store.workflows().await.unwrap())
                .unwrap()
                .contains("plan_json")
        );
    }
}
