//! Durable wrapper intent and observations. These records do not complete tasks.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection};

use super::{EffectAudit, Store, StoreError};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPreparation {
    pub effect_token: String,
    pub task_id: String,
    pub attempt_id: String,
    pub lease_epoch: i64,
    pub runner_id: String,
    pub packet_json: String,
    pub packet_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    Prepared,
    Started,
    Exited,
    Fenced,
    Blocked,
}
impl ExecutionState {
    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "started" => Ok(Self::Started),
            "exited" => Ok(Self::Exited),
            "fenced" => Ok(Self::Fenced),
            "blocked" => Ok(Self::Blocked),
            _ => Err(StoreError::Integrity("unknown execution state".into())),
        }
    }
    const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Started => "started",
            Self::Exited => "exited",
            Self::Fenced => "fenced",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub preparation: ExecutionPreparation,
    pub revision: i64,
    pub state: ExecutionState,
    pub wrapper_identity: Option<String>,
    pub checkpoint_json: Option<String>,
    pub exit_code: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionUpdate {
    Acknowledge {
        wrapper_identity: String,
        packet_sha256: String,
    },
    Checkpoint {
        wrapper_identity: String,
        payload_json: String,
    },
    Exited {
        wrapper_identity: String,
        exit_code: Option<i32>,
    },
    Fenced {
        reason: String,
    },
    Blocked {
        reason: String,
    },
}

impl Store {
    /// Lists observations requiring reconciliation after controller restart.
    /// # Errors
    /// Returns database or record decoding errors.
    pub async fn unresolved_executions(&self) -> Result<Vec<ExecutionRecord>, StoreError> {
        sqlx::query("SELECT * FROM executions WHERE state IN ('prepared', 'started', 'exited') ORDER BY effect_token")
            .fetch_all(&self.inner.readers).await?.into_iter().map(decode).collect()
    }

    /// Whether any recorded execution lacks explicit fencing evidence.
    /// Exit status, blocked status and elapsed leases are not fencing evidence.
    /// # Errors
    /// Returns database errors; callers must refuse reassignment on failure.
    pub async fn has_unfenced_execution(&self, task_id: &str) -> Result<bool, StoreError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM executions WHERE task_id = ? AND state != 'fenced'",
        )
        .bind(task_id)
        .fetch_one(&self.inner.readers)
        .await?;
        Ok(count != 0)
    }

    /// Reads a recorded wrapper intent/observation without consulting live processes.
    /// # Errors
    /// Returns a database or invalid-record error.
    pub async fn execution(&self, token: &str) -> Result<Option<ExecutionRecord>, StoreError> {
        let row = sqlx::query("SELECT * FROM executions WHERE effect_token = ?")
            .bind(token)
            .fetch_optional(&self.inner.readers)
            .await?;
        row.map(decode).transpose()
    }

    /// Persists an immutable packet before any wrapper is launched. Duplicate
    /// preparation returns the current record, never a new permission to spawn.
    /// # Errors
    /// Rejects changed identities, stale claims, invalid packet hash or missing ownership.
    pub async fn prepare_execution(
        &self,
        input: &ExecutionPreparation,
        audit: EffectAudit<'_>,
    ) -> Result<ExecutionRecord, StoreError> {
        self.prepare_execution_once(input, audit)
            .await
            .map(|(record, _)| record)
    }

    /// Records a launch intent and distinguishes a fresh insertion from replay.
    /// Only the caller receiving `true` may create the wrapper. A replay after a
    /// crash is recovery work, never permission to spawn a second process.
    /// # Errors
    /// Rejects invalid input, stale ownership and changed idempotency input.
    pub async fn prepare_execution_once(
        &self,
        input: &ExecutionPreparation,
        audit: EffectAudit<'_>,
    ) -> Result<(ExecutionRecord, bool), StoreError> {
        validate_preparation(input)?;
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(row) = sqlx::query("SELECT * FROM executions WHERE effect_token = ?")
            .bind(&input.effect_token)
            .fetch_optional(&mut *tx)
            .await?
        {
            let record = decode(row)?;
            if record.preparation != *input {
                return Err(StoreError::IdempotencyCollision);
            }
            tx.commit().await?;
            return Ok((record, false));
        }
        require_live(&mut tx, input, audit.now_ms).await?;
        sqlx::query("INSERT INTO executions (effect_token, task_id, attempt_id, lease_epoch, runner_id, packet_json, packet_sha256, state, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?)")
            .bind(&input.effect_token).bind(&input.task_id).bind(&input.attempt_id).bind(input.lease_epoch)
            .bind(&input.runner_id).bind(&input.packet_json).bind(&input.packet_sha256)
            .bind(audit.now_ms).bind(audit.now_ms).execute(&mut *tx).await?;
        append_audit(&mut tx, input, "execution.prepared", &audit, "{}").await?;
        tx.commit().await?;
        Ok((
            ExecutionRecord {
                preparation: input.clone(),
                revision: 0,
                state: ExecutionState::Prepared,
                wrapper_identity: None,
                checkpoint_json: None,
                exit_code: None,
            },
            true,
        ))
    }

    /// Atomically applies an expected-revision observation, audit and replay receipt.
    /// Fenced/blocked records cannot be restarted under the same attempt identity.
    /// # Errors
    /// Rejects stale revisions, wrong wrapper/hash, changed replay input and illegal state edges.
    pub async fn update_execution(
        &self,
        token: &str,
        expected_revision: i64,
        command_id: &str,
        update: &ExecutionUpdate,
        audit: EffectAudit<'_>,
    ) -> Result<ExecutionRecord, StoreError> {
        if command_id.is_empty() || command_id.len() > 256 {
            return Err(invalid());
        }
        let request = serde_json::to_string(&(expected_revision, update)).map_err(|_| invalid())?;
        if request.len() > 16 * 1024 {
            return Err(invalid());
        }
        let request_hash = format!("{:x}", Sha256::digest(request.as_bytes()));
        let mut writer = self.inner.writer.lock().await;
        let mut tx = writer.begin_with("BEGIN IMMEDIATE").await?;
        if let Some((old_hash, outcome)) = sqlx::query_as::<_, (String, String)>(
            "SELECT request_hash, outcome_json FROM execution_receipts WHERE effect_token = ? AND command_id = ?")
            .bind(token).bind(command_id).fetch_optional(&mut *tx).await?
        {
            if old_hash != request_hash { return Err(StoreError::IdempotencyCollision); }
            let record = serde_json::from_str(&outcome).map_err(|_| invalid())?;
            tx.commit().await?;
            return Ok(record);
        }
        let mut record = decode(
            sqlx::query("SELECT * FROM executions WHERE effect_token = ?")
                .bind(token)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(invalid)?,
        )?;
        if record.revision != expected_revision {
            return Err(invalid());
        }
        if matches!(
            update,
            ExecutionUpdate::Acknowledge { .. } | ExecutionUpdate::Checkpoint { .. }
        ) {
            require_live(&mut tx, &record.preparation, audit.now_ms).await?;
        }
        let kind = apply_update(&mut record, update)?;
        record.revision = record.revision.checked_add(1).ok_or_else(invalid)?;
        sqlx::query("UPDATE executions SET revision = ?, state = ?, wrapper_identity = ?, checkpoint_json = ?, exit_code = ?, updated_at_ms = ? WHERE effect_token = ?")
            .bind(record.revision).bind(record.state.as_str()).bind(&record.wrapper_identity)
            .bind(&record.checkpoint_json).bind(record.exit_code).bind(audit.now_ms).bind(token)
            .execute(&mut *tx).await?;
        append_audit(
            &mut tx,
            &record.preparation,
            kind,
            &audit,
            &serde_json::json!({"revision": record.revision}).to_string(),
        )
        .await?;
        sqlx::query("INSERT INTO execution_receipts (effect_token, command_id, request_hash, outcome_json) VALUES (?, ?, ?, ?)")
            .bind(token).bind(command_id).bind(request_hash).bind(serde_json::to_string(&record).map_err(|_| invalid())?)
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(record)
    }
}

fn apply_update(
    record: &mut ExecutionRecord,
    update: &ExecutionUpdate,
) -> Result<&'static str, StoreError> {
    match update {
        ExecutionUpdate::Acknowledge {
            wrapper_identity,
            packet_sha256,
        } => {
            if record.state != ExecutionState::Prepared
                || wrapper_identity.trim().is_empty()
                || wrapper_identity.len() > 256
                || *packet_sha256 != record.preparation.packet_sha256
            {
                return Err(invalid());
            }
            record.wrapper_identity = Some(wrapper_identity.clone());
            record.state = ExecutionState::Started;
            Ok("execution.started")
        }
        ExecutionUpdate::Checkpoint {
            wrapper_identity,
            payload_json,
        } => {
            require_wrapper(record, wrapper_identity)?;
            serde_json::from_str::<serde_json::Value>(payload_json).map_err(|_| invalid())?;
            record.checkpoint_json = Some(payload_json.clone());
            Ok("execution.checkpoint")
        }
        ExecutionUpdate::Exited {
            wrapper_identity,
            exit_code,
        } => {
            require_wrapper(record, wrapper_identity)?;
            record.state = ExecutionState::Exited;
            record.exit_code = *exit_code;
            Ok("execution.exited")
        }
        ExecutionUpdate::Fenced { reason } | ExecutionUpdate::Blocked { reason } => {
            if record.state == ExecutionState::Fenced
                || (record.state == ExecutionState::Blocked
                    && matches!(update, ExecutionUpdate::Blocked { .. }))
                || reason.trim().is_empty()
            {
                return Err(invalid());
            }
            record.state = if matches!(update, ExecutionUpdate::Fenced { .. }) {
                ExecutionState::Fenced
            } else {
                ExecutionState::Blocked
            };
            Ok(if record.state == ExecutionState::Fenced {
                "execution.fenced"
            } else {
                "execution.blocked"
            })
        }
    }
}

fn require_wrapper(record: &ExecutionRecord, identity: &str) -> Result<(), StoreError> {
    if record.state != ExecutionState::Started
        || record.wrapper_identity.as_deref() != Some(identity)
    {
        return Err(invalid());
    }
    Ok(())
}

async fn require_live(
    connection: &mut SqliteConnection,
    input: &ExecutionPreparation,
    now: i64,
) -> Result<(), StoreError> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effect_intents e JOIN attempts a ON a.id = e.attempt_id AND a.task_id = e.task_id AND a.lease_epoch = e.lease_epoch JOIN leases l ON l.attempt_id = a.id AND l.epoch = a.lease_epoch JOIN tasks t ON t.id = a.task_id WHERE e.token = ? AND e.task_id = ? AND e.attempt_id = ? AND e.lease_epoch = ? AND e.runner_id = ? AND e.status IN ('claimed', 'observed', 'done') AND e.claim_deadline_ms > ? AND a.ended_at_ms IS NULL AND l.status = 'active' AND t.state IN ('claimed', 'running')")
        .bind(&input.effect_token).bind(&input.task_id).bind(&input.attempt_id).bind(input.lease_epoch)
        .bind(&input.runner_id).bind(now).fetch_one(connection).await?;
    if count != 1 {
        return Err(invalid());
    }
    Ok(())
}

fn validate_preparation(input: &ExecutionPreparation) -> Result<(), StoreError> {
    if input.packet_json.len() > 256 * 1024
        || input.lease_epoch <= 0
        || [
            &input.effect_token,
            &input.task_id,
            &input.attempt_id,
            &input.runner_id,
        ]
        .iter()
        .any(|s| s.trim().is_empty() || s.len() > 256)
        || format!("{:x}", Sha256::digest(input.packet_json.as_bytes())) != input.packet_sha256
    {
        return Err(invalid());
    }
    let packet: serde_json::Value =
        serde_json::from_str(&input.packet_json).map_err(|_| invalid())?;
    if packet
        .pointer("/contract/task_id")
        .and_then(serde_json::Value::as_str)
        != Some(input.task_id.as_str())
        || packet
            .pointer("/contract/attempt_id")
            .and_then(serde_json::Value::as_str)
            != Some(input.attempt_id.as_str())
        || packet
            .pointer("/contract/lease_epoch")
            .and_then(serde_json::Value::as_i64)
            != Some(input.lease_epoch)
    {
        return Err(invalid());
    }
    Ok(())
}

async fn append_audit(
    connection: &mut SqliteConnection,
    input: &ExecutionPreparation,
    kind: &str,
    audit: &EffectAudit<'_>,
    payload: &str,
) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO audit_events (id, task_id, actor_id, kind, correlation_id, payload_json, occurred_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(audit.id).bind(&input.task_id).bind(audit.actor_id).bind(kind).bind(audit.correlation_id).bind(payload).bind(audit.now_ms)
        .execute(connection).await?;
    Ok(())
}
fn decode(row: sqlx::sqlite::SqliteRow) -> Result<ExecutionRecord, StoreError> {
    Ok(ExecutionRecord {
        preparation: ExecutionPreparation {
            effect_token: row.try_get("effect_token")?,
            task_id: row.try_get("task_id")?,
            attempt_id: row.try_get("attempt_id")?,
            lease_epoch: row.try_get("lease_epoch")?,
            runner_id: row.try_get("runner_id")?,
            packet_json: row.try_get("packet_json")?,
            packet_sha256: row.try_get("packet_sha256")?,
        },
        revision: row.try_get("revision")?,
        state: ExecutionState::parse(row.try_get("state")?)?,
        wrapper_identity: row.try_get("wrapper_identity")?,
        checkpoint_json: row.try_get("checkpoint_json")?,
        exit_code: row.try_get("exit_code")?,
    })
}
fn invalid() -> StoreError {
    StoreError::Integrity("execution identity, state or ownership mismatch".into())
}
