//! Atomic successor selection and continuation admission; original plans stay immutable.
use crate::{Store, StoreError};
use sqlx::Connection;

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
        let mut connection = self.inner.writer.lock().await;
        let mut writer = connection.begin_with("BEGIN IMMEDIATE").await?;
        let result = async {
            let current: Option<i64> = sqlx::query_scalar("SELECT version FROM tasks WHERE id=?").bind(task_id).fetch_optional(&mut *writer).await?;
            if current != Some(version) { return Err(invalid("successor task version changed")); }
            let prior: Option<(String,String,i64)> = sqlx::query_as("SELECT worker_id,pin_json,expected_version FROM workflow_assignments WHERE task_id=?").bind(task_id).fetch_optional(&mut *writer).await?;
            let status: Option<String> = sqlx::query_scalar("SELECT status FROM workflow_dispatch WHERE task_id=?").bind(task_id).fetch_optional(&mut *writer).await?;
            if status.as_deref() == Some("queued") && prior == Some((worker_id.into(),pin_json.into(),version)) { return Ok(()); }
            let changed = sqlx::query("UPDATE workflow_dispatch SET status='queued',resume_version=?,outcome_json=NULL,updated_at_ms=? WHERE task_id=? AND status IN ('finished','needs_recovery') AND (SELECT COUNT(*) FROM workflow_dispatch WHERE status IN ('queued','running') AND NOT EXISTS (SELECT 1 FROM legacy_auxiliary_ownership l WHERE l.task_id=workflow_dispatch.task_id)) < 256")
                .bind(version).bind(now).bind(task_id).execute(&mut *writer).await?.rows_affected();
            if changed != 1 { return Err(invalid("dispatch is not available for reassignment")); }
            sqlx::query("INSERT INTO workflow_assignments(task_id,worker_id,pin_json,source_attempt_id,expected_version,updated_at_ms) VALUES(?,?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET worker_id=excluded.worker_id,pin_json=excluded.pin_json,source_attempt_id=excluded.source_attempt_id,expected_version=excluded.expected_version,updated_at_ms=excluded.updated_at_ms")
                .bind(task_id).bind(worker_id).bind(pin_json).bind(source_attempt).bind(version).bind(now).execute(&mut *writer).await?;
            let payload = serde_json::json!({"worker_id":worker_id,"source_attempt_id":source_attempt,"expected_version":version,"pin":serde_json::from_str::<serde_json::Value>(pin_json).map_err(|_|invalid("invalid successor pin"))?}).to_string();
            sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,'operator','workflow.reassigned',?,?,?)")
                .bind(uuid::Uuid::now_v7().to_string()).bind(task_id).bind(task_id).bind(payload).bind(now).execute(&mut *writer).await?;
            Ok(())
        }.await;
        result?;
        writer.commit().await?;
        Ok(())
    }
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{Connection, Executor};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };

    #[tokio::test]
    async fn cancelling_after_assignment_write_rolls_back_and_releases_writer() {
        let root = tempfile::tempdir().unwrap();
        let store = Store::open(root.path().join("state.sqlite3"))
            .await
            .unwrap();
        let written = Arc::new(AtomicBool::new(false));
        let (release, wait) = mpsc::channel();
        {
            let mut connection = store.inner.writer.lock().await;
            connection.execute("INSERT INTO projects(id,canonical_path,filesystem_identity,created_at_ms) VALUES('p','/fixture','fixture',0)").await.unwrap();
            connection.execute("INSERT INTO agents VALUES('successor','claude','fixture','fixture','read_write',0)").await.unwrap();
            connection.execute("INSERT INTO tasks(id,project_id,objective,state,authority_json,created_at_ms,updated_at_ms,aggregate_json) VALUES('task','p','fixture','paused','{}',0,0,'{}')").await.unwrap();
            connection.execute("INSERT INTO workflow_dispatch(task_id,plan_json,status,created_at_ms,updated_at_ms) VALUES('task','{}','finished',0,0)").await.unwrap();
            let observed = written.clone();
            connection
                .lock_handle()
                .await
                .unwrap()
                .set_update_hook(move |update| {
                    if update.table == "workflow_assignments" {
                        observed.store(true, Ordering::SeqCst);
                        wait.recv_timeout(Duration::from_secs(5)).unwrap();
                    }
                });
        }
        let owned = store.clone();
        let request = tokio::spawn(async move {
            owned
                .reassign_workflow("task", 0, "successor", "{}", "old", 1)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !written.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let mut connection = store.inner.writer.lock().await;
        connection.lock_handle().await.unwrap().remove_update_hook();
        // A queued rollback must run before the next statement on this connection.
        connection.execute("SELECT 1").await.unwrap();
        assert!(
            !connection.is_in_transaction(),
            "cancelled reassignment left a transaction open"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_assignments")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let status: String =
            sqlx::query_scalar("SELECT status FROM workflow_dispatch WHERE task_id='task'")
                .fetch_one(&mut *connection)
                .await
                .unwrap();
        assert_eq!(status, "finished");
        drop(connection);
        store
            .reassign_workflow("task", 0, "successor", "{}", "old", 2)
            .await
            .unwrap();
    }
}
