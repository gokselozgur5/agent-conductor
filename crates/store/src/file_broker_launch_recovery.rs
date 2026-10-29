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
        self.recover_preowner_launch(binding, proof, generation, event, false)
            .await
    }

    /// Recover a never-released preparation, including an unowned registration.
    /// The trusted caller must independently verify the ACK-bound receipt.
    /// # Errors
    /// Refuses any persisted release owner, operation, changed binding or proof.
    pub async fn recover_unowned_broker_launch(
        &self,
        binding: &FileBrokerBinding,
        proof: &UnreleasedBrokerLaunchProof,
        generation: i64,
        event: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        self.recover_preowner_launch(binding, proof, generation, event, true)
            .await
    }

    async fn recover_preowner_launch(
        &self,
        binding: &FileBrokerBinding,
        proof: &UnreleasedBrokerLaunchProof,
        generation: i64,
        event: EffectAudit<'_>,
        allow_registration: bool,
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
        let registered = unowned_registration(&mut tx, binding, allow_registration).await?;
        let acknowledged: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executions WHERE effect_token=? AND task_id=? AND attempt_id=? AND lease_epoch=? AND expected_wrapper_identity=? AND wrapper_identity=? AND process_json=?")
            .bind(&binding.execution_token).bind(&binding.task_id).bind(&binding.attempt_id).bind(binding.lease_epoch)
            .bind(&binding.invocation_id).bind(&binding.invocation_id)
            .bind(serde_json::to_string(&proof.process).map_err(|_| invalid())?).fetch_one(&mut *tx).await?;
        if current != 1 || acknowledged != 1 {
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
        if registered
            .as_deref()
            .is_some_and(|registered_state| registered_state != state)
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
        if registered.is_some() {
            sqlx::query(
                "UPDATE file_brokers SET state='closed',closed_at_ms=? WHERE invocation_id=?",
            )
            .bind(event.now_ms)
            .bind(&binding.invocation_id)
            .execute(&mut *tx)
            .await?;
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

async fn unowned_registration(
    db: &mut sqlx::SqliteConnection,
    binding: &FileBrokerBinding,
    allow: bool,
) -> Result<Option<String>, StoreError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT binding_json,state FROM file_brokers WHERE execution_token=? OR invocation_id=?",
    )
    .bind(&binding.execution_token)
    .bind(&binding.invocation_id)
    .fetch_all(&mut *db)
    .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    if !allow || rows.len() != 1 || rows[0].0 != binding.encoded()? {
        return Err(invalid());
    }
    let activity: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM file_broker_owners WHERE invocation_id=?) OR EXISTS(SELECT 1 FROM file_operations WHERE invocation_id=?)")
        .bind(&binding.invocation_id).bind(&binding.invocation_id).fetch_one(db).await?;
    if activity != 0 {
        return Err(invalid());
    }
    Ok(Some(rows[0].1.clone()))
}

impl Store {
    /// Discover at most 128 ACK-bearing, never-registered open preparations.
    /// Decode each binding independently so malformed rows cannot hide later work.
    /// # Errors
    /// Returns database errors; each malformed candidate retains its own error.
    pub async fn pending_unregistered_broker_launch_page(
        &self,
        after: Option<&str>,
    ) -> Result<Vec<crate::FileBrokerRecoveryCandidate>, StoreError> {
        let rows: Vec<(String, String, String)> = sqlx::query_as("SELECT p.invocation_id,p.execution_token,CASE WHEN length(p.binding_json)<=4096 THEN p.binding_json ELSE '' END FROM file_broker_launches p JOIN executions e ON e.effect_token=p.execution_token WHERE p.state='open' AND e.process_json IS NOT NULL AND NOT EXISTS(SELECT 1 FROM file_brokers b WHERE b.execution_token=p.execution_token OR b.invocation_id=p.invocation_id) AND (? IS NULL OR p.invocation_id>?) ORDER BY p.invocation_id LIMIT 128")
            .bind(after).bind(after).fetch_all(&self.inner.readers).await?;
        Ok(rows
            .into_iter()
            .map(|(invocation_id, execution_token, encoded)| {
                let binding = (|| {
                    let binding: FileBrokerBinding =
                        serde_json::from_str(&encoded).map_err(|_| invalid())?;
                    if binding.invocation_id != invocation_id
                        || binding.execution_token != execution_token
                        || binding.encoded()? != encoded
                    {
                        return Err(invalid());
                    }
                    Ok(binding)
                })();
                crate::FileBrokerRecoveryCandidate {
                    invocation_id,
                    binding,
                }
            })
            .collect())
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
        recovery_control(false).await;
    }
    #[tokio::test]
    async fn registered_unowned_recovery_closes_both_records_atomically() {
        recovery_control(true).await;
    }
    async fn recovery_control(registered: bool) {
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
                .recover_unowned_broker_launch(&binding, &proof, 2, audit("recover-no-ack"))
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
                .recover_unowned_broker_launch(&binding, &proof, 3, audit("recover-future"))
                .await
                .is_err()
        );
        proof.process.pid += 1;
        assert!(
            store
                .recover_unowned_broker_launch(
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
        if registered {
            sqlx::query("INSERT INTO file_brokers(invocation_id,execution_token,binding_json,state,created_at_ms) VALUES(?,?,?,'open',100)")
                .bind(&binding.invocation_id).bind(&binding.execution_token).bind(binding.encoded().unwrap())
                .execute(&mut *store.inner.writer.lock().await).await.unwrap();
            assert_unowned_conflicts_refused(&store, &binding, &proof).await;
        }

        store.inner.writer.lock().await.execute("CREATE TRIGGER reject_launch_recovery BEFORE INSERT ON audit_events WHEN NEW.kind='file_broker.launch_recovered' BEGIN SELECT RAISE(FAIL,'fixture'); END").await.unwrap();
        let before = snapshot(&mut *store.inner.writer.lock().await).await;
        assert!(
            store
                .recover_unowned_broker_launch(&binding, &proof, 2, audit("recover-audit-fail"))
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
                .recover_unowned_broker_launch(&binding, &proof, 2, audit("recover-stale"))
                .await
                .is_err()
        );
        assert!(
            store
                .recover_unowned_broker_launch(&binding, &proof, 3, audit("recover-launch"))
                .await
                .unwrap()
        );
        assert!(
            !store
                .recover_unowned_broker_launch(&binding, &proof, 3, audit("recover-repeat"))
                .await
                .unwrap()
        );
        proof.shutdown_sha256 = "a".repeat(64);
        assert!(
            store
                .recover_unowned_broker_launch(
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
        if registered {
            assert!(store.file_broker_status(&binding).await.unwrap().closed);
        }
    }
    async fn assert_unowned_conflicts_refused(
        store: &Store,
        binding: &FileBrokerBinding,
        proof: &UnreleasedBrokerLaunchProof,
    ) {
        // Deliberately contradictory persisted evidence must never authorize closure.
        let cases = [
            (
                "INSERT INTO file_broker_owners SELECT invocation_id,'{}',100,NULL FROM file_brokers",
                "DELETE FROM file_broker_owners",
            ),
            (
                "INSERT INTO file_operations SELECT invocation_id,1,printf('%064d',0),NULL,100,NULL FROM file_brokers",
                "DELETE FROM file_operations",
            ),
            (
                "INSERT INTO file_operations SELECT invocation_id,1,printf('%064d',0),printf('%064d',1),100,101 FROM file_brokers",
                "DELETE FROM file_operations",
            ),
            (
                "UPDATE file_brokers SET state='closed',closed_at_ms=101",
                "UPDATE file_brokers SET state='open',closed_at_ms=NULL",
            ),
            (
                "UPDATE file_brokers SET binding_json=json_set(binding_json,'$.lease_epoch',999)",
                "UPDATE file_brokers SET binding_json=(SELECT binding_json FROM file_broker_launches)",
            ),
        ];
        let original = snapshot(&mut *store.inner.writer.lock().await).await;
        for (introduce, restore) in cases {
            store
                .inner
                .writer
                .lock()
                .await
                .execute(introduce)
                .await
                .unwrap();
            let before = snapshot(&mut *store.inner.writer.lock().await).await;
            assert!(
                store
                    .recover_unowned_broker_launch(binding, proof, 2, audit("recover-conflict"))
                    .await
                    .is_err(),
                "accepted contradictory evidence: {introduce}"
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
                .execute(restore)
                .await
                .unwrap();
            assert_eq!(
                snapshot(&mut *store.inner.writer.lock().await).await,
                original
            );
        }
    }
    #[tokio::test]
    async fn discovery_requires_ack_and_preserves_malformed_candidates() {
        let (_root, store, binding) = fixture().await;
        sqlx::query("INSERT INTO file_broker_launches VALUES(?,?,?,'{}','open',100,NULL)")
            .bind(&binding.invocation_id)
            .bind(&binding.execution_token)
            .bind(binding.encoded().unwrap())
            .execute(&mut *store.inner.writer.lock().await)
            .await
            .unwrap();
        assert!(
            store
                .pending_unregistered_broker_launch_page(None)
                .await
                .unwrap()
                .is_empty()
        );
        store
            .inner
            .writer
            .lock()
            .await
            .execute("UPDATE executions SET process_json='{}'")
            .await
            .unwrap();
        let page = store
            .pending_unregistered_broker_launch_page(None)
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(
            page[0].binding.as_ref().unwrap().encoded().unwrap(),
            binding.encoded().unwrap()
        );
        assert!(
            store
                .pending_unregistered_broker_launch_page(Some(&binding.invocation_id))
                .await
                .unwrap()
                .is_empty()
        );
        store
            .inner
            .writer
            .lock()
            .await
            .execute("UPDATE file_broker_launches SET binding_json='{}'")
            .await
            .unwrap();
        let page = store
            .pending_unregistered_broker_launch_page(None)
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert!(page[0].binding.is_err());
        sqlx::query("INSERT INTO file_brokers(invocation_id,execution_token,binding_json,state,created_at_ms) VALUES(?,?,'{}','open',100)")
            .bind(&binding.invocation_id).bind(&binding.execution_token)
            .execute(&mut *store.inner.writer.lock().await).await.unwrap();
        assert!(
            store
                .pending_unregistered_broker_launch_page(None)
                .await
                .unwrap()
                .is_empty()
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
