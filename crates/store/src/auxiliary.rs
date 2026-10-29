//! Durable auxiliary invocation ownership. Pending means unknown until settled.
use crate::{EffectAudit, Store, StoreError};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, Row};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuxiliaryKind {
    Check,
    Verifier,
}

/// Immutable trusted launch binding, without credentials or raw process output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliaryPreparation {
    pub invocation_id: String,
    pub task_id: String,
    pub execution_token: String,
    pub attempt_id: String,
    pub task_version: i64,
    pub boot_generation: i64,
    pub kind: AuxiliaryKind,
    pub source: String,
    pub definition_sha256: String,
    pub workspace: String,
}

impl AuxiliaryPreparation {
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        let uuid = uuid::Uuid::parse_str(&self.invocation_id).map_err(|_| invalid())?;
        let hash = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        if uuid.is_nil()
            || uuid.to_string() != self.invocation_id
            || self.task_version < 0
            || self.boot_generation <= 0
            || [&self.task_id, &self.execution_token, &self.attempt_id]
                .iter()
                .any(|s| s.is_empty() || s.len() > 256)
            || !self.source.strip_prefix("snapshot:").is_some_and(hash)
            || !hash(&self.definition_sha256)
            || self.workspace.len() > 4096
            || self.workspace.contains('\0')
            || !std::path::Path::new(&self.workspace).is_absolute()
            || std::path::Path::new(&self.workspace)
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(invalid());
        }
        Ok(())
    }
}

impl Store {
    /// Commits an auxiliary invocation before capture/spawn. Only `true` grants
    /// a fresh launch; an identical replay never authorizes another process.
    /// # Errors
    /// Rejects stale task/source/boot, unfenced writers, concurrent auxiliary
    /// ownership, malformed bindings and changed reuse of an invocation ID.
    pub async fn prepare_auxiliary_once(
        &self,
        input: &AuxiliaryPreparation,
        audit: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        self.prepare_auxiliary_internal(input, None, audit).await
    }

    /// Commits native host evidence together with the pre-effect invocation.
    /// The trusted caller obtains native identity; replay never backfills evidence.
    /// # Errors
    /// Refuses missing/rebound authority, generation mismatch and changed replay.
    pub async fn prepare_auxiliary_with_host(
        &self,
        input: &AuxiliaryPreparation,
        host: &crate::AuxiliaryHostObservation,
        audit: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        self.prepare_auxiliary_internal(input, Some(host), audit)
            .await
    }

    async fn prepare_auxiliary_internal(
        &self,
        input: &AuxiliaryPreparation,
        host: Option<&crate::AuxiliaryHostObservation>,
        audit: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        input.validate()?;
        let encoded = serde_json::to_string(input).map_err(|_| invalid())?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin().await?;
        let host_json = if let Some(host) = host {
            crate::auxiliary_host::require_owner(&mut tx, host).await?;
            if host.generation != input.boot_generation {
                return Err(invalid());
            }
            Some(serde_json::to_string(host).map_err(|_| invalid())?)
        } else {
            None
        };
        if let Some(existing) = sqlx::query_scalar::<_, String>(
            "SELECT preparation_json FROM auxiliary_invocations WHERE invocation_id=?",
        )
        .bind(&input.invocation_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            if existing != encoded {
                return Err(StoreError::IdempotencyCollision);
            }
            if let Some(expected) = &host_json {
                let existing:Option<String>=sqlx::query_scalar("SELECT host_json FROM auxiliary_host_evidence WHERE invocation_id=? AND kind='admission'").bind(&input.invocation_id).fetch_optional(&mut *tx).await?;
                if existing.as_ref() != Some(expected) {
                    return Err(StoreError::IdempotencyCollision);
                }
            }
            tx.commit().await?;
            return Ok(false);
        }
        let owned: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capture_eligible_executions e JOIN tasks t ON t.id=e.task_id JOIN runtime_metadata r ON r.singleton=1 WHERE e.effect_token=? AND e.task_id=? AND e.attempt_id=? AND t.version=? AND t.state='verifying' AND json_extract(t.aggregate_json,'$.attempt.id')=e.attempt_id AND json_extract(t.aggregate_json,'$.verification.source')=? AND r.boot_generation=?")
            .bind(&input.execution_token).bind(&input.task_id).bind(&input.attempt_id).bind(input.task_version).bind(&input.source).bind(input.boot_generation).fetch_one(&mut *tx).await?;
        if owned != 1 {
            return Err(invalid());
        }
        sqlx::query("INSERT INTO auxiliary_invocations (invocation_id,task_id,execution_token,preparation_json,boot_generation,state,created_at_ms,settled_at_ms) VALUES (?,?,?,?,?,'pending',?,NULL)")
            .bind(&input.invocation_id)
            .bind(&input.task_id)
            .bind(&input.execution_token)
            .bind(&encoded)
            .bind(input.boot_generation)
            .bind(audit.now_ms)
            .execute(&mut *tx)
            .await?;
        if let Some(host_json) = host_json {
            sqlx::query("INSERT INTO auxiliary_host_evidence(invocation_id,kind,host_json) VALUES(?,'admission',?)")
                .bind(&input.invocation_id).bind(host_json).execute(&mut *tx).await?;
        }
        append_audit(&mut tx, input, "auxiliary.prepared", &audit).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Records settlement observed by the same owning runtime. The caller must
    /// first prove process-group cleanup; errors/PID absence are not that proof.
    /// # Errors
    /// Refuses unknown/rebound invocations and stale runtime generations.
    pub async fn settle_auxiliary(
        &self,
        input: &AuxiliaryPreparation,
        audit: EffectAudit<'_>,
    ) -> Result<(), StoreError> {
        self.settle_unbound_auxiliary(input, audit, "auxiliary.settled")
            .await
    }

    /// Abandons an invocation before supervisor launch was bound.
    /// The owning caller must know it has not attempted spawn; a missing PID or
    /// a restart is not that evidence. This transaction excludes launch binding.
    /// # Errors
    /// Refuses stale generations, changed preparation or any launch/process state.
    pub async fn abandon_auxiliary_before_launch(
        &self,
        input: &AuxiliaryPreparation,
        audit: EffectAudit<'_>,
    ) -> Result<(), StoreError> {
        self.settle_unbound_auxiliary(input, audit, "auxiliary.not_launched")
            .await
    }

    async fn settle_unbound_auxiliary(
        &self,
        input: &AuxiliaryPreparation,
        audit: EffectAudit<'_>,
        kind: &str,
    ) -> Result<(), StoreError> {
        input.validate()?;
        let encoded = serde_json::to_string(input).map_err(|_| invalid())?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin().await?;
        let row = sqlx::query("SELECT a.preparation_json,a.state,a.launch_binding_json,a.process_json,a.release_recorded,r.boot_generation FROM auxiliary_invocations a JOIN runtime_metadata r ON r.singleton=1 WHERE a.invocation_id=?")
            .bind(&input.invocation_id).fetch_optional(&mut *tx).await?.ok_or_else(invalid)?;
        if row
            .try_get::<Option<String>, _>("launch_binding_json")?
            .is_some()
            || row.try_get::<Option<String>, _>("process_json")?.is_some()
            || row.try_get::<i64, _>("release_recorded")? != 0
            || row.try_get::<String, _>("preparation_json")? != encoded
            || row.try_get::<i64, _>("boot_generation")? != input.boot_generation
        {
            return Err(invalid());
        }
        if row.try_get::<String, _>("state")? == "settled" {
            tx.commit().await?;
            return Ok(());
        }
        sqlx::query("UPDATE auxiliary_invocations SET state='settled',settled_at_ms=? WHERE invocation_id=?")
            .bind(audit.now_ms).bind(&input.invocation_id).execute(&mut *tx).await?;
        append_audit(&mut tx, input, kind, &audit).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Unknown auxiliary execution and legacy ownership block recovery independently of writer fences.
    /// # Errors
    /// Returns database errors; never interprets missing process observations.
    pub async fn has_pending_auxiliary(&self, task_id: &str) -> Result<bool, StoreError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM auxiliary_invocations WHERE task_id=?1 AND state='pending') + (SELECT COUNT(*) FROM legacy_auxiliary_ownership WHERE task_id=?1)",
        )
        .bind(task_id)
        .fetch_one(&self.inner.readers)
        .await?;
        Ok(count != 0)
    }
}

async fn append_audit(
    connection: &mut sqlx::SqliteConnection,
    input: &AuxiliaryPreparation,
    kind: &str,
    audit: &EffectAudit<'_>,
) -> Result<(), StoreError> {
    let payload =
        serde_json::json!({"invocation_id":input.invocation_id,"kind":input.kind}).to_string();
    sqlx::query("INSERT INTO audit_events (id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES (?,?,?,?,?,?,?)")
        .bind(audit.id).bind(&input.task_id).bind(audit.actor_id).bind(kind).bind(audit.correlation_id).bind(payload).bind(audit.now_ms).execute(connection).await?;
    Ok(())
}
fn invalid() -> StoreError {
    StoreError::Integrity("invalid auxiliary invocation ownership".into())
}

impl Store {
    pub(crate) async fn auxiliary_recovery_diagnostic(
        &self,
    ) -> Result<Option<crate::RuntimeInvariantViolation>, StoreError> {
        // One read snapshot supplies both the total and bounded details. Do not
        // decode untrusted launch/preparation data or infer native settlement.
        let rows = sqlx::query("SELECT substr(a.invocation_id,1,256) AS invocation_id,substr(a.task_id,1,256) AS task_id,a.boot_generation,r.boot_generation AS current_generation,COUNT(*) OVER() AS total FROM auxiliary_invocations a JOIN runtime_metadata r ON r.singleton=1 WHERE a.state='pending' AND r.boot_generation>0 AND a.boot_generation!=r.boot_generation AND NOT EXISTS(SELECT 1 FROM legacy_auxiliary_ownership l WHERE l.task_id=a.task_id) ORDER BY a.invocation_id LIMIT 128")
            .fetch_all(&self.inner.readers).await?;
        let Some(first) = rows.first() else {
            return Ok(None);
        };
        let total: i64 = first.try_get("total")?;
        let generation: i64 = first.try_get("current_generation")?;
        let mut invocations = Vec::with_capacity(rows.len());
        for row in rows {
            invocations.push(serde_json::json!({
                "invocation_id": row.try_get::<String, _>("invocation_id")?,
                "task_id": row.try_get::<String, _>("task_id")?,
                "generation": row.try_get::<i64, _>("boot_generation")?,
            }));
        }
        Ok(Some(crate::RuntimeInvariantViolation {
            code: "auxiliary_recovery_pending".into(),
            summary: "auxiliary ownership from another daemon generation remains unresolved; preserve state and artifacts until verified settlement; continuation and backup remain refused".into(),
            context_json: serde_json::json!({"count":total,"current_generation":generation,"invocations":invocations,"limit":128}).to_string(),
        }))
    }
}
