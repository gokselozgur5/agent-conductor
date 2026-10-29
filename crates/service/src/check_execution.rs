//! Trusted runtime check port. Commands come from the persisted frozen packet.
use crate::{Clock, PersistentControlService};
use agent_conductor_domain::{
    Actor, ActorId, ActorRole, CheckId, Command, CommandKind, CorrelationId, IdempotencyKey,
    SourceIdentity, Task, TaskState,
};
use agent_conductor_store::{ExecutionState, PersistedTask, StoreError};
use conductor_runtime::{CheckDefinition, CheckOutcome, CheckReceipt, ResumePacket, prepare_check};
use std::path::PathBuf;
use tokio::sync::watch;
use uuid::Uuid;

/// Trusted runtime-selected paths, never interpreted from vendor output.
pub struct CheckWorkspace {
    pub wrapper: PathBuf,
    pub source: PathBuf,
    pub inventory: Vec<String>,
    pub destination: PathBuf,
    pub toolchain_roots: Vec<PathBuf>,
}
struct CheckContext {
    row: PersistedTask,
    task: Task,
    definition: CheckDefinition,
    source: SourceIdentity,
    digest: String,
    packet_digest: String,
}
impl<C: Clock> PersistentControlService<C> {
    /// Runs a frozen required check and records its receipt in the transactional
    /// lifecycle audit. Revalidates source, attempt, fence and task version after
    /// the process exits. It does not record a verifier verdict or complete work.
    /// # Errors
    /// Refuses unfenced execution, missing/source-mismatched contracts, invalid
    /// isolation or any lifecycle mutation during the check. No passing record is
    /// published on capture failure or a concurrent cancellation/source change.
    pub async fn execute_required_check(
        &self,
        execution_token: &str,
        check_id: &str,
        workspace: &CheckWorkspace,
        cancellation: watch::Receiver<bool>,
    ) -> Result<CheckReceipt, StoreError> {
        let before = {
            let _guard = self.mutations.lock().await;
            self.check_context(execution_token, check_id).await?
        };
        let definition = serde_json::to_vec(&before.definition)
            .map_err(|_| invalid("check definition encoding failed"))?;
        let invocation = self
            .begin_auxiliary(
                &before.task,
                agent_conductor_store::AuxiliaryKind::Check,
                &before.source.0,
                &workspace.destination,
                &definition,
            )
            .await?;
        let prepared = prepare_check(
            &workspace.source,
            &workspace.inventory,
            &before.digest,
            &workspace.destination,
            &before.definition,
            &workspace.toolchain_roots,
        )
        .map_err(|_| invalid("required check capture or isolation failed"))?;
        let result = self
            .supervise_auxiliary(
                &invocation,
                &prepared.spec,
                &workspace.wrapper,
                cancellation,
            )
            .await?;
        let receipt = prepared.finish(result);
        if receipt.outcome == CheckOutcome::InfrastructureError {
            return Err(invalid("check process settlement is uncertain"));
        }
        let _guard = self.mutations.lock().await;
        let after = self.check_context(execution_token, check_id).await?;
        if before.task.version != after.task.version
            || before.packet_digest != after.packet_digest
            || before.source != after.source
        {
            return Err(invalid("task changed while required check was running"));
        }
        let receipt_json = serde_json::to_string(&receipt)
            .map_err(|_| invalid("check receipt encoding failed"))?;
        let id = Uuid::now_v7().to_string();
        let command = Command {
            actor: Actor {
                id: ActorId("conductor-check-executor".into()),
                role: ActorRole::System,
            },
            expected_version: after.task.version,
            correlation_id: CorrelationId(execution_token.into()),
            idempotency_key: IdempotencyKey(format!("runtime-check:{id}")),
            reason: receipt_json,
            kind: CommandKind::RecordCheck {
                check_id: CheckId(check_id.into()),
                passed: receipt.outcome == CheckOutcome::Passed,
                source: after.source,
            },
        };
        self.apply_row(after.row, command)
            .await
            .map_err(|_| invalid("required check publication refused"))?;
        Ok(receipt)
    }

    async fn check_context(&self, token: &str, check_id: &str) -> Result<CheckContext, StoreError> {
        let record = self
            .store
            .execution(token)
            .await?
            .ok_or_else(|| invalid("execution missing"))?;
        if record.state != ExecutionState::Fenced
            || self
                .store
                .has_unfenced_execution(&record.preparation.task_id)
                .await?
        {
            return Err(invalid("check requires confirmed writer fencing"));
        }
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| invalid("invalid persisted packet"))?;
        let packet_digest = packet
            .sha256()
            .map_err(|_| invalid("invalid packet digest"))?;
        if packet_digest != record.preparation.packet_sha256 {
            return Err(invalid("persisted packet identity mismatch"));
        }
        let row = self
            .store
            .task(&record.preparation.task_id)
            .await?
            .ok_or_else(|| invalid("task missing"))?;
        let task: Task =
            serde_json::from_str(&row.aggregate_json).map_err(|_| invalid("invalid task"))?;
        let (definition, source, digest) = bound_check(&task, &packet, check_id)?;
        Ok(CheckContext {
            row,
            task,
            definition,
            source,
            digest,
            packet_digest,
        })
    }
}
fn bound_check(
    task: &Task,
    packet: &ResumePacket,
    check_id: &str,
) -> Result<(CheckDefinition, SourceIdentity, String), StoreError> {
    let attempt = task
        .attempt
        .as_ref()
        .ok_or_else(|| invalid("task has no attempt"))?;
    if task.state != TaskState::Verifying
        || task.id.0 != packet.contract.task_id
        || attempt.id.0 != packet.contract.attempt_id
        || attempt.lease.epoch != packet.contract.lease_epoch
        || attempt.worker.0 != packet.contract.worker_id
        || !task
            .required_checks
            .iter()
            .any(|check| check.id.0 == check_id)
    {
        return Err(invalid("required check attempt is not current"));
    }
    let definition = packet
        .contract
        .checks
        .iter()
        .find(|check| check.id == check_id)
        .ok_or_else(|| invalid("check absent from frozen contract"))?
        .clone();
    let source = task
        .verification
        .as_ref()
        .and_then(|v| v.source.clone())
        .ok_or_else(|| invalid("verification source is not bound"))?;
    let digest = source
        .0
        .strip_prefix("snapshot:")
        .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .ok_or_else(|| invalid("verification source is not a snapshot identity"))?
        .to_owned();
    Ok((definition, source, digest))
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
