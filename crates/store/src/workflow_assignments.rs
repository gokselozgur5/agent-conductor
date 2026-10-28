//! Atomic successor selection and continuation admission; original plans stay immutable.
use crate::{Store, StoreError, finish_transaction};
use sqlx::Executor;

impl Store {
    /// # Errors
    /// Returns persistence errors.
    pub async fn workflow_assignment(
        &self,
        task_id: &str,
    ) -> Result<Option<(String, String)>, StoreError> {
        Ok(
            sqlx::query_as("SELECT worker_id,pin_json FROM workflow_assignments WHERE task_id=?")
                .bind(task_id)
                .fetch_optional(&self.inner.readers)
                .await?,
        )
    }

    /// The service establishes fencing and registered capability compatibility.
    /// Queue admission and its successor/audit record commit together.
    /// # Errors
    /// Refuses stale task state, unavailable dispatch, bounds or SQL failures.
    pub async fn reassign_workflow(
        &self,
        task_id: &str,
        version: u64,
        worker_id: &str,
        pin_json: &str,
        source_attempt: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        let version = i64::try_from(version).map_err(|_| invalid("version overflow"))?;
        if worker_id.is_empty()
            || worker_id.len() > 128
            || pin_json.len() > 4096
            || source_attempt.is_empty()
        {
            return Err(invalid("invalid successor bounds"));
        }
        let mut writer = self.inner.writer.lock().await;
        writer.execute("BEGIN IMMEDIATE").await?;
        let result = async {
            let current: Option<i64> = sqlx::query_scalar("SELECT version FROM tasks WHERE id=?").bind(task_id).fetch_optional(&mut *writer).await?;
            if current != Some(version) { return Err(invalid("successor task version changed")); }
            let prior: Option<(String,String,i64)> = sqlx::query_as("SELECT worker_id,pin_json,expected_version FROM workflow_assignments WHERE task_id=?").bind(task_id).fetch_optional(&mut *writer).await?;
            let status: Option<String> = sqlx::query_scalar("SELECT status FROM workflow_dispatch WHERE task_id=?").bind(task_id).fetch_optional(&mut *writer).await?;
            if status.as_deref() == Some("queued") && prior == Some((worker_id.into(),pin_json.into(),version)) { return Ok(()); }
            let changed = sqlx::query("UPDATE workflow_dispatch SET status='queued',resume_version=?,outcome_json=NULL,updated_at_ms=? WHERE task_id=? AND status IN ('finished','needs_recovery') AND (SELECT COUNT(*) FROM workflow_dispatch WHERE status IN ('queued','running')) < 256")
                .bind(version).bind(now).bind(task_id).execute(&mut *writer).await?.rows_affected();
            if changed != 1 { return Err(invalid("dispatch is not available for reassignment")); }
            sqlx::query("INSERT INTO workflow_assignments(task_id,worker_id,pin_json,source_attempt_id,expected_version,updated_at_ms) VALUES(?,?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET worker_id=excluded.worker_id,pin_json=excluded.pin_json,source_attempt_id=excluded.source_attempt_id,expected_version=excluded.expected_version,updated_at_ms=excluded.updated_at_ms")
                .bind(task_id).bind(worker_id).bind(pin_json).bind(source_attempt).bind(version).bind(now).execute(&mut *writer).await?;
            let payload = serde_json::json!({"worker_id":worker_id,"source_attempt_id":source_attempt,"expected_version":version,"pin":serde_json::from_str::<serde_json::Value>(pin_json).map_err(|_|invalid("invalid successor pin"))?}).to_string();
            sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,'operator','workflow.reassigned',?,?,?)")
                .bind(uuid::Uuid::now_v7().to_string()).bind(task_id).bind(task_id).bind(payload).bind(now).execute(&mut *writer).await?;
            Ok(())
        }.await;
        finish_transaction(&mut writer, result).await
    }
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
