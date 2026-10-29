use super::{FileBrokerBinding, FileBrokerLaunchBinding, digest, invalid};
use crate::{EffectAudit, ExecutionProcess, Store, StoreError};
use sqlx::Connection;

/// Trusted caller's independently verified NeverReleased receipt binding.
/// Receipt-provided process identity must not be used to construct this proof.
pub struct UnreleasedBrokerLaunchProof {
    pub launch: FileBrokerLaunchBinding,
    pub process: ExecutionProcess,
    pub shutdown_sha256: String,
}
impl Store {
    /// Close a never-registered preparation using an ACK-bound shutdown proof.
    /// Never grants spawn, release, execution fencing or task acceptance.
    /// # Errors
    /// Rejects absent ACK, changed bindings, stale authority/generation, registered
    /// brokers, conflicting replay and failed audit persistence.
    pub async fn recover_unregistered_broker_launch(
        &self,
        binding: &FileBrokerBinding,
        proof: &UnreleasedBrokerLaunchProof,
        generation: i64,
        event: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        proof.launch.validate()?;
        proof.process.validate()?;
        if !digest(&proof.shutdown_sha256)
            || generation < binding.boot_generation
            || proof.process.platform != proof.launch.platform
            || proof.process.host_id != proof.launch.host_id
            || proof.process.boot_id != proof.launch.boot_id
        {
            return Err(invalid());
        }
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        let current: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM recovery_authority a JOIN runtime_metadata r USING(singleton) WHERE a.singleton=1 AND a.authority_id=? AND r.boot_generation=?")
            .bind(&binding.authority_id).bind(generation).fetch_one(&mut *tx).await?;
        let registered: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM file_brokers WHERE execution_token=? OR invocation_id=?",
        )
        .bind(&binding.execution_token)
        .bind(&binding.invocation_id)
        .fetch_one(&mut *tx)
        .await?;
        let acknowledged: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executions WHERE effect_token=? AND task_id=? AND attempt_id=? AND lease_epoch=? AND expected_wrapper_identity=? AND wrapper_identity=? AND process_json=?")
            .bind(&binding.execution_token).bind(&binding.task_id).bind(&binding.attempt_id).bind(binding.lease_epoch)
            .bind(&binding.invocation_id).bind(&binding.invocation_id)
            .bind(serde_json::to_string(&proof.process).map_err(|_| invalid())?).fetch_one(&mut *tx).await?;
        if current != 1 || registered != 0 || acknowledged != 1 {
            return Err(invalid());
        }
        let saved: Option<(String, String, String, Option<String>)> = sqlx::query_as("SELECT binding_json,launch_json,state,shutdown_sha256 FROM file_broker_launches WHERE invocation_id=? AND execution_token=?")
            .bind(&binding.invocation_id).bind(&binding.execution_token).fetch_optional(&mut *tx).await?;
        let (encoded, launch, state, prior) = saved.ok_or_else(invalid)?;
        if encoded != binding.encoded()?
            || launch != serde_json::to_string(&proof.launch).map_err(|_| invalid())?
        {
            return Err(invalid());
        }
        if state == "closed" {
            if prior.as_deref() != Some(proof.shutdown_sha256.as_str()) {
                return Err(StoreError::IdempotencyCollision);
            }
            tx.commit().await?;
            return Ok(false);
        }
        sqlx::query("UPDATE file_broker_launches SET state='closed',shutdown_sha256=? WHERE invocation_id=?")
            .bind(&proof.shutdown_sha256).bind(&binding.invocation_id).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,?,'file_broker.launch_recovered',?,?,?)")
            .bind(event.id).bind(&binding.task_id).bind(event.actor_id).bind(event.correlation_id)
            .bind(serde_json::json!({"invocation_id":binding.invocation_id,"origin_generation":binding.boot_generation,"recovery_generation":generation}).to_string())
            .bind(event.now_ms).execute(&mut *tx).await?;
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
    async fn unregistered_recovery_requires_ack_and_atomic_matching_proof() {
        let (_root, store, binding) = fixture().await;
        let mut proof = UnreleasedBrokerLaunchProof {
            launch: FileBrokerLaunchBinding {
                launch_sha256: "a".repeat(64),
                wrapper_path: "/approved/runner".into(),
                wrapper_sha256: "b".repeat(64),
                receipt_root: "/private/receipts".into(),
                receipt_root_identity_sha256: "c".repeat(64),
                platform: "linux".into(),
                host_id: "d".repeat(64),
                boot_id: "e".repeat(64),
            },
            process: ExecutionProcess {
                platform: "linux".into(),
                host_id: "d".repeat(64),
                boot_id: "e".repeat(64),
                pid: 42,
                start_marker: "linux-start-ticks:123".into(),
            },
            shutdown_sha256: "f".repeat(64),
        };
        sqlx::query("INSERT INTO file_broker_launches VALUES(?,?,?,?,'open',100,NULL)")
            .bind(&binding.invocation_id)
            .bind(&binding.execution_token)
            .bind(binding.encoded().unwrap())
            .bind(serde_json::to_string(&proof.launch).unwrap())
            .execute(&mut *store.inner.writer.lock().await)
            .await
            .unwrap();
        assert!(
            store
                .recover_unregistered_broker_launch(&binding, &proof, 2, audit("recover-no-ack"))
                .await
                .is_err()
        );
        sqlx::query("UPDATE executions SET process_json=?")
            .bind(serde_json::to_string(&proof.process).unwrap())
            .execute(&mut *store.inner.writer.lock().await)
            .await
            .unwrap();
        assert!(
            store
                .recover_unregistered_broker_launch(&binding, &proof, 3, audit("recover-future"))
                .await
                .is_err()
        );
        proof.process.pid += 1;
        assert!(
            store
                .recover_unregistered_broker_launch(
                    &binding,
                    &proof,
                    2,
                    audit("recover-changed-process")
                )
                .await
                .is_err()
        );
        proof.process.pid -= 1;
        assert_registered_refused(&store, &binding, &proof).await;
        store.inner.writer.lock().await.execute("CREATE TRIGGER reject_launch_recovery BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.launch_recovered' BEGIN SELECT RAISE(FAIL,'fixture'); END").await.unwrap();
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        assert!(
            store
                .recover_unregistered_broker_launch(
                    &binding,
                    &proof,
                    2,
                    audit("recover-audit-fail")
                )
                .await
                .is_err()
        );
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            before
        );
        store.inner.writer.lock().await.execute("DROP TRIGGER reject_launch_recovery; UPDATE runtime_metadata SET boot_generation=3").await.unwrap();
        assert!(
            store
                .recover_unregistered_broker_launch(&binding, &proof, 2, audit("recover-stale"))
                .await
                .is_err()
        );
        assert!(
            store
                .recover_unregistered_broker_launch(&binding, &proof, 3, audit("recover-launch"))
                .await
                .unwrap()
        );
        assert!(
            !store
                .recover_unregistered_broker_launch(&binding, &proof, 3, audit("recover-repeat"))
                .await
                .unwrap()
        );
        proof.shutdown_sha256 = "a".repeat(64);
        assert!(
            store
                .recover_unregistered_broker_launch(
                    &binding,
                    &proof,
                    3,
                    audit("recover-changed-receipt")
                )
                .await
                .is_err()
        );
        assert!(
            !store
                .execution_has_unresolved_file_broker(&binding.execution_token)
                .await
                .unwrap()
        );
    }
    async fn assert_registered_refused(
        store: &Store,
        binding: &FileBrokerBinding,
        proof: &UnreleasedBrokerLaunchProof,
    ) {
        sqlx::query("INSERT INTO file_brokers(invocation_id,execution_token,binding_json,state,created_at_ms) VALUES(?,?,?,'open',100)")
            .bind(&binding.invocation_id).bind(&binding.execution_token).bind(binding.encoded().unwrap())
            .execute(&mut *store.inner.writer.lock().await).await.unwrap();
        let registered = snapshot(&mut *store.inner.writer.lock().await).await;
        assert!(
            store
                .recover_unregistered_broker_launch(binding, proof, 2, audit("recover-registered"))
                .await
                .is_err()
        );
        assert_eq!(
            snapshot(&mut *store.inner.writer.lock().await).await,
            registered
        );
        sqlx::query("DELETE FROM file_brokers WHERE invocation_id=?")
            .bind(&binding.invocation_id)
            .execute(&mut *store.inner.writer.lock().await)
            .await
            .unwrap();
    }
}
