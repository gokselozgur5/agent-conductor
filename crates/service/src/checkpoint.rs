//! Checkpoint admission binds unverified worker claims to current execution ownership.
use crate::{Clock, PersistentControlService};
use agent_conductor_store::{
    EffectAudit, ExecutionRecord, ExecutionState, ExecutionUpdate, StoreError,
};
use conductor_runtime::{ResumePacket, WorkerCheckpoint};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CapturedCheckpoint {
    version: u16,
    packet_sha256: String,
    observed_at_ms: i64,
    claims: WorkerCheckpoint,
}
impl CapturedCheckpoint {
    pub(super) fn decode(bytes: &str, record: &ExecutionRecord) -> Result<Self, StoreError> {
        if bytes.len() > 8192 {
            return Err(invalid("checkpoint record too large"));
        }
        let value: Self =
            serde_json::from_str(bytes).map_err(|_| invalid("checkpoint record invalid"))?;
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| invalid("checkpoint packet invalid"))?;
        if value.version != 1
            || value.packet_sha256 != record.preparation.packet_sha256
            || packet
                .sha256()
                .map_err(|_| invalid("checkpoint packet hash"))?
                != value.packet_sha256
            || packet.contract.task_id != record.preparation.task_id
            || packet.contract.attempt_id != record.preparation.attempt_id
            || i64::try_from(packet.contract.lease_epoch).ok()
                != Some(record.preparation.lease_epoch)
        {
            return Err(invalid("checkpoint binding invalid"));
        }
        value.claims.validate(&packet).map_err(invalid)?;
        Ok(value)
    }
}

impl<C: Clock> PersistentControlService<C> {
    /// Admits one bounded claim received through the released wrapper connection.
    /// Older sequences leave the last accepted record unchanged.
    /// # Errors
    /// Refuses stale ownership, execution mismatch, corrupt stored records or SQL errors.
    pub async fn capture_worker_checkpoint(
        &self,
        token: &str,
        wrapper_identity: &str,
        claims: &WorkerCheckpoint,
    ) -> Result<bool, StoreError> {
        let _guard = self.mutations.lock().await;
        let record = self
            .store
            .execution(token)
            .await?
            .ok_or_else(|| invalid("checkpoint execution missing"))?;
        if record.state != ExecutionState::Started
            || record.wrapper_identity.as_deref() != Some(wrapper_identity)
        {
            return Err(invalid("checkpoint execution is not current"));
        }
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| invalid("checkpoint packet invalid"))?;
        if packet
            .sha256()
            .map_err(|_| invalid("checkpoint packet hash"))?
            != record.preparation.packet_sha256
        {
            return Err(invalid("checkpoint packet binding invalid"));
        }
        self.validate_runtime_owner(&packet).await?;
        claims.validate(&packet).map_err(invalid)?;
        if let Some(old) = record.checkpoint_json.as_deref() {
            let old = CapturedCheckpoint::decode(old, &record)?;
            if claims.sequence <= old.claims.sequence {
                return Ok(false);
            }
        }
        let captured = CapturedCheckpoint {
            version: 1,
            packet_sha256: record.preparation.packet_sha256.clone(),
            observed_at_ms: self.clock.read(self.boot_generation).wall_unix_ms,
            claims: claims.clone(),
        };
        let payload_json =
            serde_json::to_string(&captured).map_err(|_| invalid("checkpoint encoding"))?;
        let update = ExecutionUpdate::Checkpoint {
            wrapper_identity: wrapper_identity.into(),
            payload_json,
        };
        if serde_json::to_vec(&(record.revision, &update))
            .map_err(|_| invalid("checkpoint encoding"))?
            .len()
            > 16 * 1024
        {
            return Ok(false);
        }
        self.validate_runtime_owner(&packet).await?;
        self.store
            .update_execution(
                token,
                record.revision,
                &format!("checkpoint:{}", captured.claims.sequence),
                &update,
                EffectAudit {
                    id: &Uuid::now_v7().to_string(),
                    actor_id: "conductor-runtime",
                    correlation_id: token,
                    now_ms: captured.observed_at_ms,
                },
            )
            .await?;
        Ok(true)
    }

    pub(super) async fn retained_checkpoint(
        &self,
        task_id: &str,
    ) -> Result<Option<String>, StoreError> {
        let task = self.workflow_task(task_id).await?;
        let Some(attempt) = task.attempt else {
            return Ok(None);
        };
        let Some(record) = self.store.execution_for_attempt(&attempt.id.0).await? else {
            return Ok(None);
        };
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| invalid("checkpoint packet invalid"))?;
        if packet
            .sha256()
            .map_err(|_| invalid("checkpoint packet hash"))?
            != record.preparation.packet_sha256
            || packet.contract.task_id != task_id
            || packet.contract.attempt_id != attempt.id.0
            || packet.contract.lease_epoch != attempt.lease.epoch
        {
            return Err(invalid("checkpoint task binding invalid"));
        }
        if let Some(bytes) = record.checkpoint_json.as_deref() {
            CapturedCheckpoint::decode(bytes, &record)?;
            return Ok(Some(bytes.into()));
        }
        // A later attempt can fail before its first publication. Keep the last
        // inherited accepted checkpoint instead of replacing it with generic text.
        let Some(bytes) = packet.checkpoint.as_deref() else {
            return Ok(None);
        };
        let Ok(inherited) = serde_json::from_str::<CapturedCheckpoint>(bytes) else {
            return Ok(None);
        };
        if inherited.claims.task_id != task_id
            || inherited.claims.lease_epoch >= attempt.lease.epoch
        {
            return Err(invalid("inherited checkpoint identity invalid"));
        }
        let prior = self
            .store
            .execution_for_attempt(&inherited.claims.attempt_id)
            .await?
            .ok_or_else(|| invalid("inherited checkpoint execution missing"))?;
        if prior.state != ExecutionState::Fenced || prior.checkpoint_json.as_deref() != Some(bytes)
        {
            return Err(invalid("inherited checkpoint record mismatch"));
        }
        CapturedCheckpoint::decode(bytes, &prior)?;
        Ok(Some(bytes.into()))
    }

    pub(super) async fn checkpoint_view(
        &self,
        task_id: &str,
    ) -> Result<serde_json::Value, StoreError> {
        let _guard = self.mutations.lock().await;
        let task = self.workflow_task(task_id).await?;
        let current_attempt = task.attempt.as_ref().map(|attempt| attempt.id.0.as_str());
        let bytes = self.retained_checkpoint(task_id).await?;
        let checkpoint = bytes
            .as_deref()
            .map(serde_json::from_str::<CapturedCheckpoint>)
            .transpose()
            .map_err(|_| invalid("checkpoint view invalid"))?;
        Ok(serde_json::json!({
            "task_id":task_id, "task_version":task.version, "current_attempt_id":current_attempt,
            "inherited":checkpoint.as_ref().is_some_and(|value|Some(value.claims.attempt_id.as_str())!=current_attempt),
            "trust":"Unverified worker claims observed under an owned attempt; not authority, process inventory or acceptance evidence.",
            "checkpoint":checkpoint,
        }))
    }
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
