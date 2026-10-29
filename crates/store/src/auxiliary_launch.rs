//! Persisted supervisor admission. Secret launch bytes never enter this module.
use crate::{AuxiliaryPreparation, EffectAudit, ExecutionProcess, Store, StoreError};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Row, SqliteConnection};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliaryLaunchBinding {
    pub launch_sha256: String,
    pub authority_id: String,
    pub receipt_root: String,
    pub wrapper_path: String,
    pub wrapper_sha256: String,
}
impl AuxiliaryLaunchBinding {
    fn validate(&self) -> Result<(), StoreError> {
        let hash = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if [
            &self.launch_sha256,
            &self.authority_id,
            &self.wrapper_sha256,
        ]
        .iter()
        .any(|s| !hash(s))
            || [&self.receipt_root, &self.wrapper_path].iter().any(|s| {
                s.len() > 4096
                    || s.contains('\0')
                    || !std::path::Path::new(s).is_absolute()
                    || std::path::Path::new(s)
                        .components()
                        .any(|c| matches!(c, std::path::Component::ParentDir))
            })
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct AuxiliaryInvocation {
    pub preparation: AuxiliaryPreparation,
    pub binding: Option<AuxiliaryLaunchBinding>,
    pub process: Option<ExecutionProcess>,
    pub release_recorded: bool,
}
impl Store {
    /// Binds a launch digest and wrapper pin before creating any process.
    /// Only a fresh `true` permits spawning; replay is reconciliation work.
    /// # Errors
    /// Refuses stale ownership, changed binding, unknown invocation or authority.
    pub async fn bind_auxiliary_launch(
        &self,
        input: &AuxiliaryPreparation,
        binding: &AuxiliaryLaunchBinding,
        audit: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        input.validate()?;
        binding.validate()?;
        let encoded = serde_json::to_string(binding).map_err(|_| invalid())?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin().await?;
        let row = invocation(&mut tx, input).await?;
        if let Some(existing) = row.try_get::<Option<String>, _>("launch_binding_json")? {
            if existing != encoded {
                return Err(StoreError::IdempotencyCollision);
            }
            tx.commit().await?;
            return Ok(false);
        }
        owner(&mut tx, input, &binding.authority_id).await?;
        sqlx::query("UPDATE auxiliary_invocations SET launch_binding_json=? WHERE invocation_id=?")
            .bind(encoded)
            .bind(&input.invocation_id)
            .execute(&mut *tx)
            .await?;
        audit_event(&mut tx, input, "auxiliary.launch_bound", &audit).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Persists independently observed supervisor identity before Release.
    /// Only the first successful authorization returns true.
    /// # Errors
    /// Refuses stale task/source/boot, substituted process or missing launch pin.
    pub async fn authorize_auxiliary_release(
        &self,
        input: &AuxiliaryPreparation,
        binding: &AuxiliaryLaunchBinding,
        process: &ExecutionProcess,
        audit: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        input.validate()?;
        binding.validate()?;
        process.validate()?;
        let encoded = serde_json::to_string(process).map_err(|_| invalid())?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin().await?;
        let row = invocation(&mut tx, input).await?;
        if row.try_get::<Option<String>, _>("launch_binding_json")?
            != Some(serde_json::to_string(binding).map_err(|_| invalid())?)
        {
            return Err(invalid());
        }
        if row.try_get::<i64, _>("release_recorded")? == 1 {
            if row.try_get::<Option<String>, _>("process_json")? != Some(encoded) {
                return Err(StoreError::IdempotencyCollision);
            }
            tx.commit().await?;
            return Ok(false);
        }
        owner(&mut tx, input, &binding.authority_id).await?;
        sqlx::query("UPDATE auxiliary_invocations SET process_json=?,release_recorded=1 WHERE invocation_id=?")
            .bind(encoded).bind(&input.invocation_id).execute(&mut *tx).await?;
        audit_event(&mut tx, input, "auxiliary.release_authorized", &audit).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Reads bounded outstanding invocations without granting launch authority.
    /// # Errors
    /// Refuses malformed or excessive persisted input.
    pub async fn pending_auxiliary_invocations(
        &self,
    ) -> Result<Vec<AuxiliaryInvocation>, StoreError> {
        let rows=sqlx::query("SELECT CASE WHEN length(preparation_json)<=16384 THEN preparation_json ELSE 'oversized' END AS preparation_json,CASE WHEN launch_binding_json IS NULL OR length(launch_binding_json)<=16384 THEN launch_binding_json ELSE 'oversized' END AS launch_binding_json,CASE WHEN process_json IS NULL OR length(process_json)<=4096 THEN process_json ELSE 'oversized' END AS process_json,release_recorded FROM auxiliary_invocations WHERE state='pending' LIMIT 129").fetch_all(&self.inner.readers).await?;
        if rows.len() > 128 {
            return Err(invalid());
        }
        rows.into_iter()
            .map(|r| {
                let preparation: AuxiliaryPreparation =
                    serde_json::from_str(&r.try_get::<String, _>("preparation_json")?)
                        .map_err(|_| invalid())?;
                preparation.validate()?;
                let binding: Option<AuxiliaryLaunchBinding> = r
                    .try_get::<Option<String>, _>("launch_binding_json")?
                    .map(|s| serde_json::from_str(&s).map_err(|_| invalid()))
                    .transpose()?;
                if let Some(v) = &binding {
                    v.validate()?;
                }
                let process: Option<ExecutionProcess> = r
                    .try_get::<Option<String>, _>("process_json")?
                    .map(|s| serde_json::from_str(&s).map_err(|_| invalid()))
                    .transpose()?;
                if let Some(v) = &process {
                    v.validate()?;
                }
                Ok(AuxiliaryInvocation {
                    preparation,
                    binding,
                    process,
                    release_recorded: r.try_get::<i64, _>("release_recorded")? == 1,
                })
            })
            .collect()
    }

    /// Settles exact, externally verified terminal proof in the current runtime.
    /// The service must read and validate private evidence before calling this.
    /// # Errors
    /// Refuses changed invocation/launch/process, stale service generation or
    /// authority, and audit failures. Replaying the identical proof is harmless.
    #[allow(clippy::too_many_arguments)]
    pub async fn settle_auxiliary_proof(
        &self,
        input: &AuxiliaryPreparation,
        binding: &AuxiliaryLaunchBinding,
        process: &ExecutionProcess,
        current_generation: u64,
        proof: &str,
        audit: EffectAudit<'_>,
    ) -> Result<(), StoreError> {
        self.settle_auxiliary_receipt(
            input,
            binding,
            process,
            current_generation,
            proof,
            true,
            audit,
        )
        .await
    }

    /// Settles a trusted never-released receipt without inventing an ACK.
    /// The service must validate the private receipt and retained wrapper pin.
    /// Receipt process data remains evidence only, not observed process state.
    /// # Errors
    /// Refuses any saved process/release, non-prelaunch outcome or changed scope.
    pub async fn settle_auxiliary_unreleased_proof(
        &self,
        input: &AuxiliaryPreparation,
        binding: &AuxiliaryLaunchBinding,
        current_generation: u64,
        proof: &str,
        audit: EffectAudit<'_>,
    ) -> Result<(), StoreError> {
        if proof.len() > 4096 {
            return Err(invalid());
        }
        let observed: serde_json::Value = serde_json::from_str(proof).map_err(|_| invalid())?;
        if observed["observation"] != "never_released" {
            return Err(invalid());
        }
        let p = &observed["process"];
        let process: ExecutionProcess = serde_json::from_value(serde_json::json!({
            "platform":p["host"]["platform"],"host_id":p["host"]["host_id"],"boot_id":p["host"]["boot_id"],
            "pid":p["pid"],"start_marker":p["start_marker"]
        })).map_err(|_| invalid())?;
        self.settle_auxiliary_receipt(
            input,
            binding,
            &process,
            current_generation,
            proof,
            false,
            audit,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn settle_auxiliary_receipt(
        &self,
        input: &AuxiliaryPreparation,
        binding: &AuxiliaryLaunchBinding,
        process: &ExecutionProcess,
        current_generation: u64,
        proof: &str,
        acknowledged: bool,
        audit: EffectAudit<'_>,
    ) -> Result<(), StoreError> {
        input.validate()?;
        binding.validate()?;
        process.validate()?;
        if proof.len() > 4096 {
            return Err(invalid());
        }
        let observed: serde_json::Value = serde_json::from_str(proof).map_err(|_| invalid())?;
        if !matches!(
            observed["observation"].as_str(),
            Some("never_released" | "owned_group_settled")
        ) || observed
            != serde_json::json!({"version":1,"invocation_id":input.invocation_id,"authority_id":binding.authority_id,"boot_generation":input.boot_generation,"launch_sha256":binding.launch_sha256,"process":{"host":{"platform":process.platform,"host_id":process.host_id,"boot_id":process.boot_id},"pid":process.pid,"start_marker":process.start_marker},"observation":observed["observation"]})
        {
            return Err(invalid());
        }
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin().await?;
        let row = sqlx::query("SELECT * FROM auxiliary_invocations WHERE invocation_id=?")
            .bind(&input.invocation_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(invalid)?;
        if row.try_get::<String, _>("preparation_json")?
            != serde_json::to_string(input).map_err(|_| invalid())?
            || row.try_get::<Option<String>, _>("launch_binding_json")?
                != Some(serde_json::to_string(binding).map_err(|_| invalid())?)
            || row.try_get::<Option<String>, _>("process_json")?
                != if acknowledged {
                    Some(serde_json::to_string(process).map_err(|_| invalid())?)
                } else {
                    None
                }
            || row.try_get::<i64, _>("release_recorded")? != i64::from(acknowledged)
        {
            return Err(invalid());
        }
        let current:(Option<String>,i64)=sqlx::query_as("SELECT a.authority_id,r.boot_generation FROM recovery_authority a JOIN runtime_metadata r ON a.singleton=r.singleton WHERE a.singleton=1").fetch_one(&mut *tx).await?;
        if current.0.as_deref() != Some(&binding.authority_id)
            || u64::try_from(current.1).ok() != Some(current_generation)
        {
            return Err(invalid());
        }
        if row.try_get::<String, _>("state")? == "settled" {
            if row
                .try_get::<Option<String>, _>("settlement_json")?
                .as_deref()
                != Some(proof)
            {
                return Err(StoreError::IdempotencyCollision);
            }
            tx.commit().await?;
            return Ok(());
        }
        sqlx::query("UPDATE auxiliary_invocations SET state='settled',settlement_json=?,settled_at_ms=? WHERE invocation_id=?")
            .bind(proof).bind(audit.now_ms).bind(&input.invocation_id).execute(&mut *tx).await?;
        audit_event(&mut tx, input, "auxiliary.proof_settled", &audit).await?;
        tx.commit().await?;
        Ok(())
    }
}
async fn invocation(
    connection: &mut SqliteConnection,
    input: &AuxiliaryPreparation,
) -> Result<sqlx::sqlite::SqliteRow, StoreError> {
    let row=sqlx::query("SELECT preparation_json,state,launch_binding_json,process_json,release_recorded FROM auxiliary_invocations WHERE invocation_id=?").bind(&input.invocation_id).fetch_optional(connection).await?.ok_or_else(invalid)?;
    if row.try_get::<String, _>("preparation_json")?
        != serde_json::to_string(input).map_err(|_| invalid())?
        || row.try_get::<String, _>("state")? != "pending"
    {
        return Err(invalid());
    }
    Ok(row)
}
async fn owner(
    connection: &mut SqliteConnection,
    input: &AuxiliaryPreparation,
    authority: &str,
) -> Result<(), StoreError> {
    let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM tasks t JOIN executions e ON e.effect_token=? JOIN runtime_metadata r ON r.singleton=1 JOIN recovery_authority a ON a.singleton=1 WHERE t.id=? AND t.version=? AND t.state='verifying' AND e.state='fenced' AND e.task_id=t.id AND e.attempt_id=? AND json_extract(t.aggregate_json,'$.attempt.id')=e.attempt_id AND json_extract(t.aggregate_json,'$.verification.source')=? AND r.boot_generation=? AND a.authority_id=? AND NOT EXISTS(SELECT 1 FROM executions x WHERE x.task_id=t.id AND x.state!='fenced')")
        .bind(&input.execution_token).bind(&input.task_id).bind(input.task_version).bind(&input.attempt_id).bind(&input.source).bind(input.boot_generation).bind(authority).fetch_one(connection).await?;
    if count != 1 {
        return Err(invalid());
    }
    Ok(())
}
async fn audit_event(
    connection: &mut SqliteConnection,
    input: &AuxiliaryPreparation,
    kind: &str,
    audit: &EffectAudit<'_>,
) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO audit_events (id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES (?,?,?,?,?,?,?)")
        .bind(audit.id).bind(&input.task_id).bind(audit.actor_id).bind(kind).bind(audit.correlation_id).bind(serde_json::json!({"invocation_id":input.invocation_id}).to_string()).bind(audit.now_ms).execute(connection).await?;
    Ok(())
}
fn invalid() -> StoreError {
    StoreError::Integrity("auxiliary supervisor binding refused".into())
}
