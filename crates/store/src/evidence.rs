use crate::{Store, StoreError};

/// Bounded audit input for the service's allowlisted evidence projection.
#[derive(Debug, sqlx::FromRow)]
pub struct EvidenceRow {
    pub sequence: i64,
    pub kind: String,
    pub actor_id: String,
    pub occurred_at_ms: i64,
    pub payload: Option<String>,
    pub execution: Option<String>,
}

impl Store {
    /// Reads one task's evidence journal without loading packets or raw output.
    /// Execution metadata is its current recorded snapshot, not historical liveness.
    /// # Errors
    /// Returns database errors.
    pub async fn task_evidence(
        &self,
        task_id: &str,
        before: Option<i64>,
    ) -> Result<Vec<EvidenceRow>, StoreError> {
        Ok(sqlx::query_as(
            "SELECT a.sequence,a.kind,a.actor_id,a.occurred_at_ms,
             CASE WHEN length(CAST(a.payload_json AS BLOB))<=65536 THEN a.payload_json END AS payload,
             CASE WHEN e.effect_token IS NOT NULL THEN json_object(
               'attempt_id',e.attempt_id,'lease_epoch',CAST(e.lease_epoch AS TEXT),
               'packet_sha256',e.packet_sha256,'runner_id',e.runner_id,
               'current_recorded_state',e.state,'current_revision',CAST(e.revision AS TEXT),
               'recorded_exit_code',e.exit_code,'updated_at_ms',e.updated_at_ms) END AS execution
             FROM audit_events a LEFT JOIN executions e
               ON e.effect_token=a.correlation_id AND e.task_id=a.task_id
             WHERE a.task_id=? AND a.sequence<? AND a.kind IN
               ('execution.prepared','execution.started','execution.exited','execution.fenced','execution.blocked',
                'begin_verification','bind_verification_source','record_check','record_verdict','complete')
             ORDER BY a.sequence DESC LIMIT 101",
        )
        .bind(task_id)
        .bind(before.unwrap_or(i64::MAX))
        .fetch_all(&self.inner.readers)
        .await?)
    }
}
