//! Binds a normally exited writer's captured tree to independent review.
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
    /// Captures a normally exited writer's candidate for independent checks.
    /// Restricted writers retain their existing file-tool fence evidence.
    /// Unrestricted writers remain Exited: inspecting their copied output does
    /// not establish descendant fencing or authorize workspace reassignment.
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
            || candidate.attempt_id != packet.contract.attempt_id
            || (packet.contract.full_capability.is_none()
                && !accepts_file_writer_exit(&packet, candidate))
        {
            return Err(invalid(
                "adapter does not establish an eligible writer exit",
            ));
        }
        self.validate_runtime_owner(&packet).await?;
        let id = Uuid::now_v7().to_string();
        if packet.contract.full_capability.is_some() {
            self.validate_capture_eligibility(&record, &packet).await?;
        } else {
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
        }
        let source = packet.contract.workspace.clone();
        let target = destination.to_path_buf();
        let paths = baseline_paths.to_vec();
        let manifest =
            tokio::task::spawn_blocking(move || snapshot_attempt_tree(&source, &target, &paths))
                .await
                .map_err(|_| invalid("source capture task failed"))?
                .map_err(|_| invalid("source capture failed"))?;
        self.validate_runtime_owner(&packet).await?;
        self.publish_final_owned_capture(token, &packet, &manifest)
            .await?;
        let source = SourceIdentity(format!(
            "snapshot:{}",
            manifest
                .sha256()
                .map_err(|_| invalid("source hash failed"))?
        ));
        self.publish_captured_source(&packet, token, &id, source)
            .await?;
        Ok(manifest)
    }
    pub(super) async fn publish_captured_source(
        &self,
        packet: &ResumePacket,
        token: &str,
        id: &str,
        source: SourceIdentity,
    ) -> Result<(), StoreError> {
        let mut row = self
            .store
            .task(&packet.contract.task_id)
            .await?
            .ok_or_else(|| invalid("task missing"))?;
        let mut batches = Vec::new();
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
            let task: Task =
                serde_json::from_str(&row.aggregate_json).map_err(|_| invalid("invalid task"))?;
            let command = Command {
                actor,
                expected_version: task.version,
                correlation_id: CorrelationId(token.into()),
                idempotency_key: IdempotencyKey(format!("finalize:{id}:{label}")),
                reason: "bind captured source for independent verification".into(),
                kind,
            };
            let request_json = serde_json::to_string(&(row.id.as_str(), &command))
                .map_err(|_| invalid("source command serialization failed"))?;
            let (batch, _) = self
                .prepare_command_batch(
                    row.clone(),
                    command,
                    crate::hash(&request_json),
                    request_json,
                )
                .await
                .map_err(|_| invalid("source lifecycle preparation failed"))?;
            // Build the next transaction from authoritative state, never the
            // display response (which may redact legacy decision text).
            row.aggregate_json.clone_from(
                &batch
                    .task
                    .as_ref()
                    .ok_or_else(|| invalid("source lifecycle task mutation missing"))?
                    .aggregate_json,
            );
            batches.push(batch);
        }
        self.store.persist_sequence(&batches).await?;
        Ok(())
    }
}
fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
