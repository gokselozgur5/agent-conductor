use super::{FileBrokerBinding, FileBrokerLaunchBinding, audit, invalid, owner};
use crate::{EffectAudit, Store, StoreError};
use sqlx::Connection;

impl Store {
    /// Bind the approved launch before spawn. Only a fresh commit permits spawn;
    /// exact replay does not authorize another process. No bearer is persisted.
    /// # Errors
    /// Refuses stale leases, acknowledged executions, rebound launch or audit failure.
    pub async fn prepare_file_broker_launch(
        &self,
        binding: &FileBrokerBinding,
        launch: &FileBrokerLaunchBinding,
        event: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        let encoded = binding.encoded()?;
        launch.validate()?;
        let launch_json = serde_json::to_string(launch).map_err(|_| invalid())?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        owner(&mut tx, binding).await?;
        let valid: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executions e JOIN attempts a ON a.id=e.attempt_id AND a.task_id=e.task_id AND a.lease_epoch=e.lease_epoch JOIN leases l ON l.attempt_id=a.id AND l.task_id=a.task_id AND l.epoch=a.lease_epoch JOIN tasks t ON t.id=a.task_id WHERE e.effect_token=? AND e.task_id=? AND e.attempt_id=? AND e.lease_epoch=? AND e.expected_wrapper_identity=? AND e.wrapper_identity IS NULL AND e.process_json IS NULL AND e.state='prepared' AND a.ended_at_ms IS NULL AND l.status='active' AND l.boot_generation=? AND l.remaining_ms>0 AND t.state IN ('claimed','running') AND json_extract(t.aggregate_json,'$.attempt.id')=a.id")
            .bind(&binding.execution_token).bind(&binding.task_id).bind(&binding.attempt_id).bind(binding.lease_epoch).bind(&binding.invocation_id).bind(binding.boot_generation).fetch_one(&mut *tx).await?;
        if valid != 1 {
            return Err(invalid());
        }
        if let Some((saved_binding, saved_launch, state)) = sqlx::query_as::<
            _,
            (String, String, String),
        >(
            "SELECT binding_json,launch_json,state FROM file_broker_launches WHERE invocation_id=?",
        )
        .bind(&binding.invocation_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            if saved_binding != encoded || saved_launch != launch_json || state != "open" {
                return Err(StoreError::IdempotencyCollision);
            }
            tx.commit().await?;
            return Ok(false);
        }
        let existing: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM file_brokers WHERE execution_token=?")
                .bind(&binding.execution_token)
                .fetch_one(&mut *tx)
                .await?;
        if existing != 0 {
            return Err(invalid());
        }
        sqlx::query("INSERT INTO file_broker_launches(invocation_id,execution_token,binding_json,launch_json,state,created_at_ms) VALUES(?,?,?,?,'open',?)")
            .bind(&binding.invocation_id).bind(&binding.execution_token).bind(encoded).bind(launch_json).bind(event.now_ms).execute(&mut *tx).await?;
        audit(&mut tx, binding, "file_broker.launch_prepared", 0, event).await?;
        tx.commit().await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_broker_tests::{audit, fixture};
    use crate::transaction_cancellation::snapshot;
    use sqlx::Executor;
    #[tokio::test]
    async fn launch_intent_is_fresh_once_and_audit_failure_rolls_back() {
        let (_root, store, binding) = fixture().await;
        let launch = FileBrokerLaunchBinding {
            launch_sha256: "a".repeat(64),
            wrapper_path: "/approved/runner".into(),
            wrapper_sha256: "b".repeat(64),
            receipt_root: "/private/receipts".into(),
            receipt_root_identity_sha256: "c".repeat(64),
            platform: "linux".into(),
            host_id: "d".repeat(64),
            boot_id: "e".repeat(64),
        };
        assert!(
            store
                .prepare_file_broker_launch(&binding, &launch, audit("launch-started"))
                .await
                .is_err()
        );
        store.inner.writer.lock().await.execute("UPDATE executions SET state='prepared',wrapper_identity=NULL,process_json=NULL; CREATE TRIGGER reject_launch BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.launch_prepared' BEGIN SELECT RAISE(FAIL,'fixture'); END").await.unwrap();
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        assert!(
            store
                .prepare_file_broker_launch(&binding, &launch, audit("launch-failed"))
                .await
                .is_err()
        );
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
        store
            .inner
            .writer
            .lock()
            .await
            .execute("DROP TRIGGER reject_launch")
            .await
            .unwrap();
        assert!(
            store
                .prepare_file_broker_launch(&binding, &launch, audit("launch-fresh"))
                .await
                .unwrap()
        );
        assert!(
            !store
                .prepare_file_broker_launch(&binding, &launch, audit("launch-replay"))
                .await
                .unwrap()
        );
        assert_eq!(store.file_broker_launch(&binding).await.unwrap(), launch);
        let mut rebound = binding.clone();
        rebound.root_identity_sha256 = "f".repeat(64);
        assert!(store.file_broker_launch(&rebound).await.is_err());
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        let mut changed = launch;
        changed.wrapper_sha256 = "f".repeat(64);
        assert!(
            store
                .prepare_file_broker_launch(&binding, &changed, audit("launch-changed"))
                .await
                .is_err()
        );
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
    }
}
