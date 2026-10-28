//! Binds a normally exited, capability-limited writer's captured tree to review.
use crate::{Clock, PersistentControlService};
use agent_conductor_domain::{
    Actor, ActorId, ActorRole, Command, CommandKind, CorrelationId, IdempotencyKey, SourceIdentity,
    Task,
};
use agent_conductor_store::{EffectAudit, ExecutionState, ExecutionUpdate, StoreError};
use conductor_runtime::{
    ResumePacket, SnapshotManifest, VendorCandidate, accepts_file_writer_exit,
    snapshot_attempt_tree,
};
use std::path::Path;
use uuid::Uuid;

impl<C: Clock> PersistentControlService<C> {
    /// Finalizes a restricted writer after accepted normal wrapper exit. Captures
    /// new/deleted files outside its writable root and binds that copy for checks.
    /// Does not accept command-capable writers, interrupted runs or model prose
    /// as fence evidence. The trusted pinned CLI enforces file-tool confinement.
    /// # Errors
    /// Refuses absent/failed exit, mismatched evidence, expired ownership, capture
    /// failure or lifecycle conflicts. Partial artifacts remain for recovery.
    pub async fn finalize_native_source(
        &self,
        token: &str,
        candidate: &VendorCandidate,
        destination: &Path,
        baseline_paths: &[String],
    ) -> Result<SnapshotManifest, StoreError> {
        let _guard = self.mutations.lock().await;
        let record = self
            .store
            .execution(token)
            .await?
            .ok_or_else(|| invalid("execution missing"))?;
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| invalid("invalid stored packet"))?;
        if record.state != ExecutionState::Exited
            || record.exit_code != Some(0)
            || record.preparation.packet_sha256 != candidate.packet_sha256
            || !accepts_file_writer_exit(&packet, candidate)
        {
            return Err(invalid(
                "adapter does not establish a restricted writer exit",
            ));
        }
        self.validate_runtime_owner(&packet).await?;
        let id = Uuid::now_v7().to_string();
        self.store.update_execution(token, record.revision, &format!("file-writer-exit:{id}"),
            &ExecutionUpdate::Fenced { reason: serde_json::json!({
                "basis": "accepted pinned file-only CLI exited normally; attempt-scoped write tools only",
                "packet_sha256": candidate.packet_sha256,
                "output_sha256": candidate.output_sha256,
                "adapter_sha256": packet.contract.adapter.executable_sha256,
                "conformance_sha256": packet.contract.adapter.conformance_sha256,
            }).to_string() },
            EffectAudit { id: &id, actor_id: "conductor-runtime", correlation_id: token, now_ms: self.clock.read(self.boot_generation).wall_unix_ms },
        ).await?;
        let source = packet.contract.workspace.clone();
        let target = destination.to_path_buf();
        let paths = baseline_paths.to_vec();
        let manifest =
            tokio::task::spawn_blocking(move || snapshot_attempt_tree(&source, &target, &paths))
                .await
                .map_err(|_| invalid("source capture task failed"))?
                .map_err(|_| invalid("source capture failed"))?;
        self.validate_runtime_owner(&packet).await?;
        let source = SourceIdentity(format!(
            "snapshot:{}",
            manifest
                .sha256()
                .map_err(|_| invalid("source hash failed"))?
        ));
        for (actor, kind, label) in [
            (
                Actor {
                    id: ActorId(packet.contract.worker_id.clone()),
                    role: ActorRole::Worker,
                },
                CommandKind::BeginVerification {
                    attempt_id: packet.contract.attempt_id.as_str().into(),
                    lease_epoch: packet.contract.lease_epoch,
                },
                "begin",
            ),
            (
                Actor {
                    id: ActorId("conductor-runtime".into()),
                    role: ActorRole::System,
                },
                CommandKind::BindVerificationSource { source },
                "bind",
            ),
        ] {
            let row = self
                .store
                .task(&packet.contract.task_id)
                .await?
                .ok_or_else(|| invalid("task missing"))?;
            let task: Task =
                serde_json::from_str(&row.aggregate_json).map_err(|_| invalid("invalid task"))?;
            self.apply_row(
                row,
                Command {
                    actor,
                    expected_version: task.version,
                    correlation_id: CorrelationId(token.into()),
                    idempotency_key: IdempotencyKey(format!("finalize:{id}:{label}")),
                    reason: "bind restricted writer captured source for independent verification"
                        .into(),
                    kind,
                },
            )
            .await
            .map_err(|_| invalid("source lifecycle publication failed"))?;
        }
        Ok(manifest)
    }
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
