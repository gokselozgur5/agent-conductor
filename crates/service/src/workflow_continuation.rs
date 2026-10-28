//! Explicit continuation from a prior, demonstrably fenced attempt workspace.
use crate::{Clock, NativeWorkflowPlan, PersistentControlService, WorkflowOutcome};
use agent_conductor_domain::{ActorRole, CommandKind, Task, TaskState};
use agent_conductor_store::{ExecutionState, StoreError};
use conductor_runtime::{GitSnapshot, ResumePacket, snapshot_attempt_tree};
use std::{collections::BTreeMap, os::unix::fs::DirBuilderExt};
use uuid::Uuid;

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn queue_continuation(
        &self,
        task_id: &str,
        expected_version: u64,
    ) -> Result<(), StoreError> {
        let _guard = self.mutations.lock().await;
        self.continuation_packet(task_id, expected_version).await?;
        self.store
            .requeue_workflow(
                task_id,
                expected_version,
                self.clock.read(self.boot_generation).wall_unix_ms,
            )
            .await
    }
    pub(super) async fn continuation_packet(
        &self,
        task_id: &str,
        version: u64,
    ) -> Result<(Task, ResumePacket), StoreError> {
        let task = self.workflow_task(task_id).await?;
        if task.version != version
            || task.state.is_terminal()
            || self.store.has_unfenced_execution(task_id).await?
        {
            return Err(invalid(
                "continuation requires current nonterminal task and verified prior write fences",
            ));
        }
        let attempt = task
            .attempt
            .as_ref()
            .ok_or_else(|| invalid("retained attempt missing"))?;
        let record = self
            .store
            .execution_for_attempt(&attempt.id.0)
            .await?
            .ok_or_else(|| invalid("retained execution missing"))?;
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| invalid("retained packet invalid"))?;
        if record.state != ExecutionState::Fenced
            || packet.contract.task_id != task_id
            || packet.contract.attempt_id != attempt.id.0
            || packet.contract.lease_epoch != attempt.lease.epoch
            || packet.contract.worker_id != attempt.worker.0
            || packet.sha256().map_err(|_| invalid("packet hash failed"))?
                != record.preparation.packet_sha256
        {
            return Err(invalid("retained execution identity mismatch"));
        }
        Ok((task, packet))
    }
    pub(super) async fn continue_native_workflow(
        &self,
        plan: &NativeWorkflowPlan,
        expected_version: u64,
        worker: BTreeMap<String, String>,
        verifier: BTreeMap<String, String>,
    ) -> Result<WorkflowOutcome, StoreError> {
        let packet = {
            let _guard = self.mutations.lock().await;
            let (task, packet) = self
                .continuation_packet(&plan.task_id, expected_version)
                .await?;
            if matches!(
                task.state,
                TaskState::Running | TaskState::Verifying | TaskState::Claimed
            ) {
                self.workflow_command_locked(
                    &plan.task_id,
                    "operator",
                    ActorRole::Operator,
                    CommandKind::Pause,
                    "fenced continuation ends previous attempt",
                )
                .await?;
            }
            if task.state != TaskState::Ready {
                self.workflow_command_locked(
                    &plan.task_id,
                    "operator",
                    ActorRole::Operator,
                    CommandKind::Resume,
                    "explicit retained-work continuation",
                )
                .await?;
            }
            packet
        };
        let root = plan
            .artifacts
            .canonicalize()
            .map_err(|_| invalid("retained artifact root missing"))?;
        let source = packet
            .contract
            .workspace
            .canonicalize()
            .map_err(|_| invalid("retained workspace missing"))?;
        if !source.starts_with(&root)
            || packet.objective != plan.objective
            || packet.contract.checks != plan.checks
        {
            return Err(invalid("retained workspace or plan binding mismatch"));
        }
        let artifacts = root.join(format!("continuation-{}", Uuid::now_v7()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&artifacts)
            .map_err(|_| invalid("new continuation directory failed"))?;
        let manifest = snapshot_attempt_tree(&source, &artifacts.join("work"), &[])
            .map_err(|_| invalid("retained workspace snapshot failed"))?;
        let baseline = GitSnapshot {
            starting_head: packet.contract.starting_head,
            status_sha256: manifest
                .sha256()
                .map_err(|_| invalid("snapshot hash failed"))?,
            manifest,
        };
        self.run_prepared_workflow(plan, artifacts, baseline, true, worker, verifier)
            .await
    }
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
