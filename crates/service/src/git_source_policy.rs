//! Frozen source Git identity invalidates only its current task attempt.
use crate::{Clock, NativeWorkflowPlan, PersistentControlService};
use agent_conductor_domain::{ActorRole, Blocker, CommandKind, TaskState};
use agent_conductor_store::StoreError;
use conductor_runtime::{GitSnapshot, GitSourceIdentity, ResumePacket, observe_git_source};

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn workflow_git_source(
        &self,
        plan: &NativeWorkflowPlan,
        baseline: &GitSnapshot,
        continuing: bool,
    ) -> Result<Option<Box<GitSourceIdentity>>, StoreError> {
        if continuing {
            let task = self.workflow_task(&plan.task_id).await?;
            let attempt = task.attempt.as_ref().ok_or_else(invalid)?;
            let record = self
                .store
                .execution_for_attempt(&attempt.id.0)
                .await?
                .ok_or_else(invalid)?;
            let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
                .map_err(|_| invalid())?;
            if packet.contract.task_id != plan.task_id
                || packet.contract.attempt_id != attempt.id.0
                || packet.contract.lease_epoch != attempt.lease.epoch
                || packet.sha256().map_err(|_| invalid())? != record.preparation.packet_sha256
            {
                return Err(invalid());
            }
            if let Some(source) = &packet.contract.git_source
                && observe_git_source(&source.root).await.as_ref().ok() != Some(source.as_ref())
            {
                return Err(invalid());
            }
            return Ok(packet.contract.git_source);
        }
        // Accepted dependency snapshots carry no live Git ownership.
        if plan.input_task.is_some() {
            return Ok(None);
        }
        let root = plan.project.canonicalize().map_err(|_| invalid())?;
        let source = observe_git_source(&root).await.map_err(|_| invalid())?;
        if source.head != baseline.starting_head
            || Some(source.reference.as_str()) != baseline.starting_ref.as_deref()
        {
            return Err(invalid());
        }
        Ok(Some(Box::new(source)))
    }

    pub(super) async fn ensure_git_source_current_locked(
        &self,
        packet: &ResumePacket,
    ) -> Result<bool, StoreError> {
        let Some(source) = &packet.contract.git_source else {
            // Legacy and immutable dependency inputs have no inferred live binding.
            return Ok(true);
        };
        let task = self.workflow_task(&packet.contract.task_id).await?;
        if !task.attempt.as_ref().is_some_and(|attempt| {
            attempt.id.0 == packet.contract.attempt_id
                && attempt.lease.epoch == packet.contract.lease_epoch
        }) || !matches!(
            task.state,
            TaskState::Claimed | TaskState::Running | TaskState::Verifying
        ) {
            return Ok(false);
        }
        let record = self
            .store
            .execution_for_attempt(&packet.contract.attempt_id)
            .await?
            .ok_or_else(invalid)?;
        if packet.sha256().map_err(|_| invalid())? != record.preparation.packet_sha256 {
            return Err(invalid());
        }
        if observe_git_source(&source.root).await.as_ref().ok() == Some(source.as_ref()) {
            return Ok(true);
        }
        self.workflow_command_locked(&task.id.0, "conductor-git-source", ActorRole::System,
            CommandKind::Block { blocker: Blocker {
                kind: "git_source_changed".into(),
                detail: "bound source repository identity changed or became unavailable".into(),
                resume_when: "operator restores the bound identity or authorizes a new plan; unresolved workers still require reconciliation".into(),
            }}, "frozen Git source identity no longer valid").await?;
        Ok(false)
    }
}
fn invalid() -> StoreError {
    StoreError::Integrity("invalid or changed task Git source binding".into())
}
