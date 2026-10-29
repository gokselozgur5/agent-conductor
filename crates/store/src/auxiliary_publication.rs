//! Atomically publish one native result with its settled process provenance.
use crate::{AuxiliaryKind, AuxiliaryPreparation, CommitBatch, CommitResult, Store, StoreError};
use sha2::{Digest, Sha256};
use sqlx::{Connection, SqliteConnection};

impl Store {
    /// Commits a domain-validated result and its unique invocation/audit link.
    /// This is not a domain transition evaluator; the service supplies the batch.
    /// # Errors
    /// Refuses unproven settlement, changed source/attempt/boot, mismatched result
    /// scope, duplicate publication or any transactional persistence failure.
    pub async fn persist_auxiliary_publication(
        &self,
        input: &AuxiliaryPreparation,
        batch: &CommitBatch,
    ) -> Result<CommitResult, StoreError> {
        input.validate()?;
        validate_batch(input, batch)?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        let preparation: Option<String> = sqlx::query_scalar(
            "SELECT preparation_json FROM auxiliary_invocations WHERE invocation_id=?",
        )
        .bind(&input.invocation_id)
        .fetch_optional(&mut *tx)
        .await?;
        if preparation != Some(serde_json::to_string(input).map_err(|_| invalid())?) {
            return Err(invalid());
        }
        let existing: Option<(String,String,String)> = sqlx::query_as("SELECT actor_id,idempotency_key,request_hash FROM auxiliary_publications WHERE invocation_id=?")
            .bind(&input.invocation_id).fetch_optional(&mut *tx).await?;
        if let Some(existing) = existing {
            if existing
                != (
                    batch.actor_id.clone(),
                    batch.idempotency_key.clone(),
                    batch.request_hash.clone(),
                )
            {
                return Err(StoreError::IdempotencyCollision);
            }
            let result = crate::persist_in_transaction(&mut tx, batch).await?;
            if !matches!(result, CommitResult::Replayed { .. }) {
                return Err(invalid());
            }
            tx.commit().await?;
            return Ok(result);
        }
        require_settled_owner(&mut tx, input).await?;
        let result = crate::persist_in_transaction(&mut tx, batch).await?;
        if !matches!(result, CommitResult::Applied { .. }) {
            return Err(invalid());
        }
        sqlx::query("INSERT INTO auxiliary_publications(invocation_id,audit_id,actor_id,idempotency_key,request_hash) VALUES(?,?,?,?,?)")
            .bind(&input.invocation_id).bind(&batch.audit[0].id).bind(&batch.actor_id).bind(&batch.idempotency_key).bind(&batch.request_hash).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(result)
    }
}
fn validate_batch(input: &AuxiliaryPreparation, batch: &CommitBatch) -> Result<(), StoreError> {
    let task = batch.task.as_ref().ok_or_else(invalid)?;
    let kind = match input.kind {
        AuxiliaryKind::Check => "record_check",
        AuxiliaryKind::Verifier => "record_verdict",
    };
    if batch.audit.len() != 1
        || batch.transitions.len() != 1
        || !batch.effects.is_empty()
        || batch.project.is_some()
        || batch.agent.is_some()
        || batch.new_task.is_some()
        || batch.attempt.is_some()
        || task.task_id != input.task_id
        || task.expected_version != input.task_version
        || task.state != "verifying"
    {
        return Err(invalid());
    }
    let audit = &batch.audit[0];
    let transition = &batch.transitions[0];
    if audit.kind != kind
        || audit.task_id.as_deref() != Some(&input.task_id)
        || audit.correlation_id != input.invocation_id
        || audit.actor_id != batch.actor_id
        || transition.task_id != input.task_id
        || transition.correlation_id != input.invocation_id
        || transition.actor_id != batch.actor_id
        || transition.prior_state != "verifying"
        || transition.new_state != "verifying"
    {
        return Err(invalid());
    }
    let payload: serde_json::Value =
        serde_json::from_str(&audit.payload_json).map_err(|_| invalid())?;
    let aggregate: serde_json::Value =
        serde_json::from_str(&task.aggregate_json).map_err(|_| invalid())?;
    if payload[0] != input.task_id
        || payload[1]["kind"]["type"] != kind
        || payload[1]["kind"]["source"] != input.source
        || payload[1]["correlation_id"] != input.invocation_id
        || payload[1]["expected_version"] != input.task_version
        || payload[1]["actor"]["id"] != batch.actor_id
        || payload[1]["idempotency_key"] != batch.idempotency_key
        || payload[1]["reason"] != transition.reason
        || format!("{:x}", Sha256::digest(audit.payload_json.as_bytes())) != batch.request_hash
        || aggregate["verification"]["source"] != input.source
        || aggregate["attempt"]["id"] != input.attempt_id
    {
        return Err(invalid());
    }
    Ok(())
}
async fn require_settled_owner(
    c: &mut SqliteConnection,
    input: &AuxiliaryPreparation,
) -> Result<(), StoreError> {
    let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM auxiliary_invocations a JOIN tasks t ON t.id=a.task_id JOIN executions e ON e.effect_token=a.execution_token JOIN runtime_metadata r ON r.singleton=1 JOIN recovery_authority h ON h.singleton=1 WHERE a.invocation_id=? AND a.state='settled' AND a.release_recorded=1 AND a.process_json IS NOT NULL AND json_extract(a.settlement_json,'$.observation')='owned_group_settled' AND json_extract(a.settlement_json,'$.invocation_id')=a.invocation_id AND json_extract(a.settlement_json,'$.launch_sha256')=json_extract(a.launch_binding_json,'$.launch_sha256') AND json_extract(a.settlement_json,'$.authority_id')=h.authority_id AND json_extract(a.launch_binding_json,'$.authority_id')=h.authority_id AND json_extract(a.settlement_json,'$.boot_generation')=a.boot_generation AND json_extract(a.settlement_json,'$.process.pid')=json_extract(a.process_json,'$.pid') AND json_extract(a.settlement_json,'$.process.start_marker')=json_extract(a.process_json,'$.start_marker') AND json_extract(a.settlement_json,'$.process.host.platform')=json_extract(a.process_json,'$.platform') AND json_extract(a.settlement_json,'$.process.host.host_id')=json_extract(a.process_json,'$.host_id') AND json_extract(a.settlement_json,'$.process.host.boot_id')=json_extract(a.process_json,'$.boot_id') AND a.boot_generation=r.boot_generation AND t.version=? AND t.state='verifying' AND json_extract(t.aggregate_json,'$.verification.source')=? AND json_extract(t.aggregate_json,'$.attempt.id')=? AND e.attempt_id=? AND e.task_id=t.id AND e.state='fenced' AND NOT EXISTS(SELECT 1 FROM auxiliary_invocations x WHERE x.task_id=t.id AND x.state='pending') AND NOT EXISTS(SELECT 1 FROM executions x WHERE x.task_id=t.id AND x.state!='fenced') AND NOT EXISTS(SELECT 1 FROM legacy_auxiliary_ownership l WHERE l.task_id=t.id)")
        .bind(&input.invocation_id).bind(input.task_version).bind(&input.source).bind(&input.attempt_id).bind(&input.attempt_id).fetch_one(c).await?;
    if count != 1 {
        return Err(invalid());
    }
    Ok(())
}
fn invalid() -> StoreError {
    StoreError::Integrity("auxiliary result provenance refused".into())
}
