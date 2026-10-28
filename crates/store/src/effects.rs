use serde::{Deserialize, Serialize};
use sqlx::{Executor, Row};

use super::{Store, StoreError, finish_transaction};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectStatus {
    Pending,
    Claimed,
    Observed,
    Done,
    Abandoned,
}

impl EffectStatus {
    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "claimed" => Ok(Self::Claimed),
            "observed" => Ok(Self::Observed),
            "done" => Ok(Self::Done),
            "abandoned" => Ok(Self::Abandoned),
            _ => Err(StoreError::Integrity(format!(
                "unknown effect status {value}"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Observed => "observed",
            Self::Done => "done",
            Self::Abandoned => "abandoned",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectRecord {
    pub token: String,
    pub task_id: Option<String>,
    pub attempt_id: Option<String>,
    pub lease_epoch: Option<i64>,
    pub correlation_id: String,
    pub effect_kind: String,
    pub request_json: String,
    pub status: EffectStatus,
    pub runner_id: Option<String>,
    pub claim_deadline_ms: Option<i64>,
    pub observation_json: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectAudit<'a> {
    pub id: &'a str,
    pub actor_id: &'a str,
    pub correlation_id: &'a str,
    pub now_ms: i64,
}

impl Store {
    /// Loads one durable effect intent.
    ///
    /// # Errors
    /// Returns a database or integrity error when the row cannot be decoded.
    pub async fn effect(&self, token: &str) -> Result<Option<EffectRecord>, StoreError> {
        fetch_effect(&self.inner.readers, token).await
    }

    /// Confirms that a claimed worker-spawn intent still belongs to the live
    /// task attempt immediately before the external action is attempted.
    ///
    /// # Errors
    /// Returns a database error when the ownership projection cannot be read.
    pub async fn effect_is_executable(
        &self,
        token: &str,
        runner_id: &str,
    ) -> Result<bool, StoreError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM effect_intents e LEFT JOIN attempts a ON a.id = e.attempt_id AND a.task_id = e.task_id AND a.lease_epoch = e.lease_epoch LEFT JOIN leases l ON l.attempt_id = a.id AND l.task_id = a.task_id AND l.epoch = a.lease_epoch LEFT JOIN tasks t ON t.id = a.task_id WHERE e.token = ? AND e.status = 'claimed' AND e.runner_id = ? AND (e.task_id IS NULL OR (a.ended_at_ms IS NULL AND l.status = 'active' AND t.state IN ('claimed', 'running', 'verifying'))) ",
        )
        .bind(token)
        .bind(runner_id)
        .fetch_one(&self.inner.readers)
        .await?;
        Ok(count == 1)
    }

    /// Claims a pending effect, or takes over an expired claim, using the same token.
    ///
    /// # Errors
    /// Rejects missing, terminal, observed, or unexpired claims.
    pub async fn claim_effect(
        &self,
        token: &str,
        runner_id: &str,
        claim_deadline_ms: i64,
        audit: EffectAudit<'_>,
    ) -> Result<EffectRecord, StoreError> {
        if runner_id.trim().is_empty() || claim_deadline_ms <= audit.now_ms {
            return Err(transition_error(
                token,
                "runner and future deadline are required",
            ));
        }
        let now_ms = audit.now_ms;
        self.mutate_effect(token, audit, |record| match record.status {
            EffectStatus::Pending => {
                record.status = EffectStatus::Claimed;
                record.runner_id = Some(runner_id.to_owned());
                record.claim_deadline_ms = Some(claim_deadline_ms);
                Ok("effect.claimed")
            }
            EffectStatus::Claimed
                if record
                    .claim_deadline_ms
                    .is_some_and(|deadline| now_ms >= deadline) =>
            {
                record.runner_id = Some(runner_id.to_owned());
                record.claim_deadline_ms = Some(claim_deadline_ms);
                Ok("effect.reclaimed")
            }
            EffectStatus::Claimed => Err(transition_error(token, "claim has not expired")),
            _ => Err(transition_error(token, "effect is not claimable")),
        })
        .await
    }

    /// Records the tagged external identity observed for a claimed effect.
    ///
    /// # Errors
    /// Rejects the wrong runner, invalid JSON, or a non-claimed effect.
    pub async fn observe_effect(
        &self,
        token: &str,
        runner_id: &str,
        observation_json: &str,
        audit: EffectAudit<'_>,
    ) -> Result<EffectRecord, StoreError> {
        serde_json::from_str::<serde_json::Value>(observation_json)
            .map_err(|_| transition_error(token, "observation must be valid JSON"))?;
        self.mutate_effect(token, audit, |record| {
            require_runner(record, token, runner_id, EffectStatus::Claimed)?;
            record.status = EffectStatus::Observed;
            record.observation_json = Some(observation_json.to_owned());
            Ok("effect.observed")
        })
        .await
    }

    /// Marks an observed effect complete.
    ///
    /// # Errors
    /// Rejects the wrong runner or an effect without a recorded observation.
    pub async fn complete_effect(
        &self,
        token: &str,
        runner_id: &str,
        audit: EffectAudit<'_>,
    ) -> Result<EffectRecord, StoreError> {
        let now_ms = audit.now_ms;
        self.mutate_effect(token, audit, |record| {
            if record.status != EffectStatus::Observed {
                return Err(transition_error(token, "effect is not observed"));
            }
            let same_runner = record.runner_id.as_deref() == Some(runner_id);
            let expired_owner = record
                .claim_deadline_ms
                .is_some_and(|deadline| now_ms >= deadline);
            if !same_runner && !expired_owner {
                return Err(transition_error(
                    token,
                    "observing runner still owns the effect",
                ));
            }
            record.runner_id = Some(runner_id.to_owned());
            record.status = EffectStatus::Done;
            Ok(if same_runner {
                "effect.done"
            } else {
                "effect.adopted_done"
            })
        })
        .await
    }

    /// Abandons any non-terminal effect after reconciliation proves it unsafe.
    ///
    /// # Errors
    /// Rejects an already terminal effect.
    pub async fn abandon_effect(
        &self,
        token: &str,
        reason_json: &str,
        audit: EffectAudit<'_>,
    ) -> Result<EffectRecord, StoreError> {
        serde_json::from_str::<serde_json::Value>(reason_json)
            .map_err(|_| transition_error(token, "abandon reason must be valid JSON"))?;
        self.mutate_effect(token, audit, |record| {
            if matches!(record.status, EffectStatus::Done | EffectStatus::Abandoned) {
                return Err(transition_error(token, "effect is already terminal"));
            }
            record.status = EffectStatus::Abandoned;
            record.observation_json = Some(reason_json.to_owned());
            Ok("effect.abandoned")
        })
        .await
    }

    async fn mutate_effect<F>(
        &self,
        token: &str,
        audit: EffectAudit<'_>,
        mutate: F,
    ) -> Result<EffectRecord, StoreError>
    where
        F: FnOnce(&mut EffectRecord) -> Result<&'static str, StoreError>,
    {
        let mut writer = self.inner.writer.lock().await;
        writer.execute("BEGIN IMMEDIATE").await?;
        let result = async {
            let mut record = fetch_effect(&mut *writer, token)
                .await?
                .ok_or_else(|| StoreError::EffectNotFound(token.to_owned()))?;
            let audit_kind = mutate(&mut record)?;
            record.updated_at_ms = audit.now_ms;
            sqlx::query(
                "UPDATE effect_intents SET status = ?, runner_id = ?, claim_deadline_ms = ?, observation_json = ?, updated_at_ms = ? WHERE token = ?",
            )
            .bind(record.status.as_str())
            .bind(&record.runner_id)
            .bind(record.claim_deadline_ms)
            .bind(&record.observation_json)
            .bind(record.updated_at_ms)
            .bind(token)
            .execute(&mut *writer)
            .await?;
            sqlx::query(
                "INSERT INTO audit_events (id, task_id, actor_id, kind, correlation_id, payload_json, occurred_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(audit.id)
            .bind(&record.task_id)
            .bind(audit.actor_id)
            .bind(audit_kind)
            .bind(audit.correlation_id)
            .bind(serde_json::json!({"token": token, "status": record.status.as_str()}).to_string())
            .bind(audit.now_ms)
            .execute(&mut *writer)
            .await?;
            Ok(record)
        }
        .await;
        finish_transaction(&mut writer, result).await
    }
}

async fn fetch_effect<'e, E>(executor: E, token: &str) -> Result<Option<EffectRecord>, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let row = sqlx::query(
        "SELECT token, task_id, attempt_id, lease_epoch, correlation_id, effect_kind, request_json, status, runner_id, claim_deadline_ms, observation_json, created_at_ms, updated_at_ms FROM effect_intents WHERE token = ?",
    )
    .bind(token)
    .fetch_optional(executor)
    .await?;
    row.map(effect_from_row).transpose()
}

fn effect_from_row(row: sqlx::sqlite::SqliteRow) -> Result<EffectRecord, StoreError> {
    Ok(EffectRecord {
        token: row.try_get("token")?,
        task_id: row.try_get("task_id")?,
        attempt_id: row.try_get("attempt_id")?,
        lease_epoch: row.try_get("lease_epoch")?,
        correlation_id: row.try_get("correlation_id")?,
        effect_kind: row.try_get("effect_kind")?,
        request_json: row.try_get("request_json")?,
        status: EffectStatus::parse(row.try_get("status")?)?,
        runner_id: row.try_get("runner_id")?,
        claim_deadline_ms: row.try_get("claim_deadline_ms")?,
        observation_json: row.try_get("observation_json")?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

fn require_runner(
    record: &EffectRecord,
    token: &str,
    runner_id: &str,
    status: EffectStatus,
) -> Result<(), StoreError> {
    if record.status != status || record.runner_id.as_deref() != Some(runner_id) {
        return Err(transition_error(
            token,
            "runner or effect status does not match",
        ));
    }
    Ok(())
}

fn transition_error(token: &str, detail: &str) -> StoreError {
    StoreError::EffectTransition {
        token: token.to_owned(),
        detail: detail.to_owned(),
    }
}
