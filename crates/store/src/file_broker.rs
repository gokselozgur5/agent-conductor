//! Durable broker intents. Replay never grants a second filesystem effect.
use crate::{EffectAudit, Store, StoreError};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, SqliteConnection};
#[path = "file_broker_launch.rs"]
mod launch;
#[path = "file_broker_launch_binding.rs"]
mod launch_binding;
#[path = "file_broker_launch_prepare.rs"]
mod launch_prepare;
pub use launch::FileBrokerLaunchBinding;
#[path = "file_broker_owner.rs"]
mod ownership;
pub use ownership::{FileBrokerOwner, FileBrokerRecoveryCandidate};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileBrokerBinding {
    pub authority_id: String,
    pub boot_generation: i64,
    pub invocation_id: String,
    pub execution_token: String,
    pub task_id: String,
    pub attempt_id: String,
    pub lease_epoch: i64,
    /// Trusted launcher's pinned root identity; no file content or path is stored.
    pub root_identity_sha256: String,
}
#[derive(Debug, PartialEq, Eq)]
pub struct FileBrokerStatus {
    pub closed: bool,
    pub admitted: i64,
    pub completed: i64,
}
fn invalid() -> StoreError {
    StoreError::Integrity("file broker ownership or operation refused".into())
}
fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl FileBrokerBinding {
    fn encoded(&self) -> Result<String, StoreError> {
        if !digest(&self.authority_id)
            || !digest(&self.root_identity_sha256)
            || self.boot_generation <= 0
            || self.lease_epoch <= 0
            || [&self.execution_token, &self.task_id, &self.attempt_id]
                .iter()
                .any(|s| s.trim().is_empty() || s.len() > 256 || s.chars().any(char::is_control))
            || !uuid::Uuid::parse_str(&self.invocation_id)
                .is_ok_and(|u| !u.is_nil() && u.to_string() == self.invocation_id)
        {
            return Err(invalid());
        }
        serde_json::to_string(self).map_err(|_| invalid())
    }
}
async fn owner(db: &mut SqliteConnection, b: &FileBrokerBinding) -> Result<(), StoreError> {
    let n:i64=sqlx::query_scalar("SELECT COUNT(*) FROM recovery_authority a JOIN runtime_metadata r ON r.singleton=a.singleton WHERE a.singleton=1 AND a.authority_id=? AND r.boot_generation=?")
        .bind(&b.authority_id).bind(b.boot_generation).fetch_one(db).await?;
    if n != 1 {
        return Err(invalid());
    }
    Ok(())
}
async fn live(db: &mut SqliteConnection, b: &FileBrokerBinding) -> Result<(), StoreError> {
    owner(db, b).await?;
    let n:i64=sqlx::query_scalar("SELECT COUNT(*) FROM executions e JOIN attempts a ON a.id=e.attempt_id AND a.task_id=e.task_id AND a.lease_epoch=e.lease_epoch JOIN leases l ON l.attempt_id=a.id AND l.task_id=a.task_id AND l.epoch=a.lease_epoch JOIN tasks t ON t.id=a.task_id WHERE e.effect_token=? AND e.task_id=? AND e.attempt_id=? AND e.lease_epoch=? AND e.state='started' AND e.wrapper_identity=? AND e.expected_wrapper_identity=? AND a.ended_at_ms IS NULL AND l.status='active' AND l.boot_generation=? AND l.remaining_ms>0 AND t.state IN ('claimed','running') AND json_extract(t.aggregate_json,'$.attempt.id')=a.id")
        .bind(&b.execution_token).bind(&b.task_id).bind(&b.attempt_id).bind(b.lease_epoch).bind(&b.invocation_id).bind(&b.invocation_id).bind(b.boot_generation).fetch_one(db).await?;
    if n != 1 {
        return Err(invalid());
    }
    Ok(())
}
async fn bound(
    db: &mut SqliteConnection,
    b: &FileBrokerBinding,
    encoded: &str,
) -> Result<String, StoreError> {
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT binding_json,state FROM file_brokers WHERE invocation_id=?")
            .bind(&b.invocation_id)
            .fetch_optional(db)
            .await?;
    let (actual, state) = row.ok_or_else(invalid)?;
    if actual != encoded {
        return Err(StoreError::IdempotencyCollision);
    }
    Ok(state)
}
async fn audit(
    db: &mut SqliteConnection,
    b: &FileBrokerBinding,
    kind: &str,
    sequence: i64,
    a: EffectAudit<'_>,
) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO audit_events(id,task_id,actor_id,kind,correlation_id,payload_json,occurred_at_ms) VALUES(?,?,?,?,?,?,?)")
        .bind(a.id).bind(&b.task_id).bind(a.actor_id).bind(kind).bind(a.correlation_id)
        .bind(serde_json::json!({"invocation_id":b.invocation_id,"sequence":sequence}).to_string()).bind(a.now_ms).execute(db).await?;
    Ok(())
}
impl Store {
    /// Bind one broker to an acknowledged current execution. Only true creates
    /// a fresh capability; replay never authorizes constructing another broker.
    /// # Errors
    /// Rejects stale ownership, changed identities, duplicate execution binding.
    pub async fn register_file_broker(
        &self,
        b: &FileBrokerBinding,
        a: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        let encoded = b.encoded()?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        live(&mut tx, b).await?;
        launch_binding::registered_binding(&mut tx, b).await?;
        if sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM file_brokers WHERE invocation_id=?")
            .bind(&b.invocation_id)
            .fetch_one(&mut *tx)
            .await?
            != 0
        {
            bound(&mut tx, b, &encoded).await?;
            tx.commit().await?;
            return Ok(false);
        }
        sqlx::query("INSERT INTO file_brokers(invocation_id,execution_token,binding_json,state,created_at_ms) VALUES(?,?,?,'open',?)")
            .bind(&b.invocation_id).bind(&b.execution_token).bind(encoded).bind(a.now_ms).execute(&mut *tx).await?;
        audit(&mut tx, b, "file_broker.registered", 0, a).await?;
        tx.commit().await?;
        Ok(true)
    }
    /// Commit intent before a file effect. Only true grants one execution;
    /// identical replay returns false, including after successful completion.
    /// # Errors
    /// Rejects sequence gaps, pending operations, changed replay and stale owners.
    pub async fn admit_file_operation(
        &self,
        b: &FileBrokerBinding,
        sequence: i64,
        request_sha256: &str,
        a: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        let encoded = b.encoded()?;
        if !(1..=100_000).contains(&sequence) || !digest(request_sha256) {
            return Err(invalid());
        }
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        live(&mut tx, b).await?;
        if bound(&mut tx, b, &encoded).await? != "open" {
            return Err(invalid());
        }
        if let Some(existing) = sqlx::query_scalar::<_, String>(
            "SELECT request_sha256 FROM file_operations WHERE invocation_id=? AND sequence=?",
        )
        .bind(&b.invocation_id)
        .bind(sequence)
        .fetch_optional(&mut *tx)
        .await?
        {
            if existing != request_sha256 {
                return Err(StoreError::IdempotencyCollision);
            }
            tx.commit().await?;
            return Ok(false);
        }
        let (last,pending):(i64,i64)=sqlx::query_as("SELECT COALESCE(MAX(sequence),0),COUNT(*) FILTER(WHERE completed_at_ms IS NULL) FROM file_operations WHERE invocation_id=?").bind(&b.invocation_id).fetch_one(&mut *tx).await?;
        if sequence != last + 1 || pending != 0 {
            return Err(invalid());
        }
        sqlx::query("INSERT INTO file_operations(invocation_id,sequence,request_sha256,admitted_at_ms) VALUES(?,?,?,?)").bind(&b.invocation_id).bind(sequence).bind(request_sha256).bind(a.now_ms).execute(&mut *tx).await?;
        audit(&mut tx, b, "file_broker.admitted", sequence, a).await?;
        tx.commit().await?;
        Ok(true)
    }
    /// Record a trusted synchronous operation's successful result before reply.
    /// This is not a write fence, task acceptance or permission to replay files.
    /// # Errors
    /// Rejects missing/changed intents, result rebinding and closed/stale owners.
    pub async fn finish_file_operation(
        &self,
        b: &FileBrokerBinding,
        sequence: i64,
        request_sha256: &str,
        result_sha256: &str,
        a: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        let encoded = b.encoded()?;
        if !(1..=100_000).contains(&sequence) || !digest(request_sha256) || !digest(result_sha256) {
            return Err(invalid());
        }
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        owner(&mut tx, b).await?;
        if bound(&mut tx, b, &encoded).await? != "open" {
            return Err(invalid());
        }
        let row:Option<(String,Option<String>)>=sqlx::query_as("SELECT request_sha256,result_sha256 FROM file_operations WHERE invocation_id=? AND sequence=?").bind(&b.invocation_id).bind(sequence).fetch_optional(&mut *tx).await?;
        let (request, result) = row.ok_or_else(invalid)?;
        if request != request_sha256 {
            return Err(StoreError::IdempotencyCollision);
        }
        if let Some(result) = result {
            if result != result_sha256 {
                return Err(StoreError::IdempotencyCollision);
            }
            tx.commit().await?;
            return Ok(false);
        }
        sqlx::query("UPDATE file_operations SET result_sha256=?,completed_at_ms=? WHERE invocation_id=? AND sequence=?").bind(result_sha256).bind(a.now_ms).bind(&b.invocation_id).bind(sequence).execute(&mut *tx).await?;
        audit(&mut tx, b, "file_broker.completed", sequence, a).await?;
        tx.commit().await?;
        Ok(true)
    }
    /// Observe exact persisted progress without granting any effect or clearing
    /// unknown operations. Safe for recovery inspection across generations.
    /// # Errors
    /// Rejects unknown or rebound broker identities and database failures.
    pub async fn file_broker_status(
        &self,
        b: &FileBrokerBinding,
    ) -> Result<FileBrokerStatus, StoreError> {
        let encoded = b.encoded()?;
        let row:Option<(String,i64,i64)>=sqlx::query_as("SELECT b.state,COUNT(o.sequence),COUNT(o.completed_at_ms) FROM file_brokers b LEFT JOIN file_operations o ON o.invocation_id=b.invocation_id WHERE b.invocation_id=? AND b.binding_json=? GROUP BY b.invocation_id")
            .bind(&b.invocation_id).bind(encoded).fetch_optional(&self.inner.readers).await?;
        let (state, admitted, completed) = row.ok_or_else(invalid)?;
        Ok(FileBrokerStatus {
            closed: state == "closed",
            admitted,
            completed,
        })
    }
    /// Record closure only after the trusted wrapper has revoked its file gate.
    /// This record alone never fences an execution or completes a task.
    /// # Errors
    /// Refuses pending operations, a mismatched completion watermark or old owner.
    pub async fn close_file_broker(
        &self,
        b: &FileBrokerBinding,
        completed: i64,
        a: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        self.close_file_broker_inner(b, completed, None, None, a)
            .await
    }
    async fn close_file_broker_inner(
        &self,
        b: &FileBrokerBinding,
        completed: i64,
        evidence: Option<(&FileBrokerOwner, &str)>,
        recovery_generation: Option<i64>,
        a: EffectAudit<'_>,
    ) -> Result<bool, StoreError> {
        let encoded = b.encoded()?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(generation) = recovery_generation {
            if generation <= b.boot_generation || evidence.is_none() {
                return Err(invalid());
            }
            let matches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM recovery_authority a JOIN runtime_metadata r ON r.singleton=a.singleton WHERE a.singleton=1 AND a.authority_id=? AND r.boot_generation=?")
                .bind(&b.authority_id).bind(generation).fetch_one(&mut *tx).await?;
            if matches != 1 {
                return Err(invalid());
            }
        } else {
            owner(&mut tx, b).await?;
        }

        let state = bound(&mut tx, b, &encoded).await?;
        let saved: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT owner_json,shutdown_sha256 FROM file_broker_owners WHERE invocation_id=?",
        )
        .bind(&b.invocation_id)
        .fetch_optional(&mut *tx)
        .await?;
        match (saved, evidence) {
            (None, None) => {}
            (Some((saved, prior)), Some((expected, receipt))) if saved == expected.encoded()? => {
                if prior.as_deref().is_some_and(|old| old != receipt) {
                    return Err(StoreError::IdempotencyCollision);
                }
                if state == "closed" && prior.is_none() {
                    return Err(invalid());
                }
                sqlx::query(
                    "UPDATE file_broker_owners SET shutdown_sha256=? WHERE invocation_id=?",
                )
                .bind(receipt)
                .bind(&b.invocation_id)
                .execute(&mut *tx)
                .await?;
            }
            _ => return Err(invalid()),
        }

        let (last,pending):(i64,i64)=sqlx::query_as("SELECT COALESCE(MAX(sequence),0),COUNT(*) FILTER(WHERE completed_at_ms IS NULL) FROM file_operations WHERE invocation_id=?").bind(&b.invocation_id).fetch_one(&mut *tx).await?;
        if completed != last || pending != 0 {
            return Err(invalid());
        }
        if state == "closed" {
            tx.commit().await?;
            return Ok(false);
        }
        sqlx::query("UPDATE file_brokers SET state='closed',closed_at_ms=? WHERE invocation_id=?")
            .bind(a.now_ms)
            .bind(&b.invocation_id)
            .execute(&mut *tx)
            .await?;
        if let Some(generation) = recovery_generation {
            ownership::recovery_audit(&mut tx, b, completed, generation, a).await?;
        } else {
            audit(&mut tx, b, "file_broker.closed", completed, a).await?;
        }
        tx.commit().await?;
        Ok(true)
    }
}

impl Store {
    pub(crate) async fn file_broker_recovery_diagnostic(
        &self,
    ) -> Result<Option<crate::RuntimeInvariantViolation>, StoreError> {
        use sqlx::Row;
        let rows=sqlx::query("SELECT substr(b.invocation_id,1,256) AS invocation_id,substr(e.task_id,1,256) AS task_id,b.state,COUNT(*) OVER() AS total FROM file_brokers b JOIN executions e ON e.effect_token=b.execution_token LEFT JOIN tasks t ON t.id=e.task_id LEFT JOIN leases l ON l.attempt_id=e.attempt_id JOIN runtime_metadata r ON r.singleton=1 JOIN recovery_authority a ON a.singleton=1 WHERE (b.state='open' AND (json_extract(b.binding_json,'$.boot_generation') IS NOT r.boot_generation OR json_extract(b.binding_json,'$.authority_id') IS NOT a.authority_id OR e.state!='started' OR t.state NOT IN ('claimed','running') OR l.status IS NOT 'active')) OR (b.state='closed' AND EXISTS(SELECT 1 FROM file_operations o WHERE o.invocation_id=b.invocation_id AND o.completed_at_ms IS NULL)) ORDER BY b.invocation_id LIMIT 128")
            .fetch_all(&self.inner.readers).await?;
        let Some(first) = rows.first() else {
            return Ok(None);
        };
        let total: i64 = first.try_get("total")?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            records.push(serde_json::json!({
                "invocation_id":row.try_get::<String,_>("invocation_id")?,
                "task_id":row.try_get::<String,_>("task_id")?,
                "state":row.try_get::<String,_>("state")?,
            }));
        }
        Ok(Some(crate::RuntimeInvariantViolation{
            code:"file_broker_recovery_pending".into(),
            summary:"file broker ownership remains unresolved; preserve files and operation evidence before continuation or backup".into(),
            context_json:serde_json::json!({"count":total,"brokers":records,"limit":128}).to_string(),
        }))
    }
}

impl Store {
    /// Whether this execution still owns unresolved broker effects/capabilities.
    /// # Errors
    /// Returns database read errors; false alone is never execution fencing.
    pub async fn execution_has_unresolved_file_broker(
        &self,
        token: &str,
    ) -> Result<bool, StoreError> {
        let pending: i64 = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM file_brokers b WHERE b.execution_token=? AND (b.state='open' OR EXISTS(SELECT 1 FROM file_operations o WHERE o.invocation_id=b.invocation_id AND o.completed_at_ms IS NULL)))")
            .bind(token).fetch_one(&self.inner.readers).await?;
        Ok(pending != 0)
    }
}
