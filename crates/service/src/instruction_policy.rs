//! Instruction invalidation uses the authoritative task lifecycle and frozen packet.
use crate::{Clock, PersistentControlService};
use agent_conductor_domain::{ActorRole, Blocker, CommandKind, Task, TaskState};
use agent_conductor_store::StoreError;
use conductor_runtime::{ResumePacket, read_pinned_instructions};

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn invalidate_instruction_tasks(&self) -> Result<(), StoreError> {
        let _guard = self.mutations.lock().await;
        for row in self.store.tasks(None).await? {
            let task: Task = serde_json::from_str(&row.aggregate_json).map_err(|_| invalid())?;
            if !matches!(
                task.state,
                TaskState::Claimed | TaskState::Running | TaskState::Verifying
            ) {
                continue;
            }
            let Some(attempt) = &task.attempt else {
                continue;
            };
            let Some(record) = self.store.execution_for_attempt(&attempt.id.0).await? else {
                continue;
            };
            let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
                .map_err(|_| invalid())?;
            if packet.contract.attempt_id != attempt.id.0
                || packet.contract.task_id != task.id.0
                || packet.sha256().map_err(|_| invalid())? != record.preparation.packet_sha256
            {
                return Err(invalid());
            }
            self.ensure_instruction_current_locked(&packet).await?;
        }
        Ok(())
    }

    pub(super) async fn ensure_instruction_current_locked(
        &self,
        packet: &ResumePacket,
    ) -> Result<bool, StoreError> {
        if read_pinned_instructions(packet).is_ok() {
            return Ok(true);
        }
        let task = self.workflow_task(&packet.contract.task_id).await?;
        if task.attempt.as_ref().is_some_and(|a| {
            a.id.0 == packet.contract.attempt_id && a.lease.epoch == packet.contract.lease_epoch
        }) && matches!(
            task.state,
            TaskState::Claimed | TaskState::Running | TaskState::Verifying
        ) {
            self.workflow_command_locked(&task.id.0,"conductor-instructions",ActorRole::System,
                CommandKind::Block {blocker:Blocker {kind:"instruction_changed".into(),detail:"approved instruction source changed or became unavailable".into(),resume_when:"operator restores approved instruction identity or authorizes a new plan".into()}},
                "frozen instruction identity no longer valid").await?;
        }
        Ok(false)
    }
}
fn invalid() -> StoreError {
    StoreError::Integrity("invalid instruction execution binding".into())
}
