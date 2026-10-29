//! Trusted auxiliary ownership shared by checks and independent review.
use crate::{Clock, PersistentControlService};
use agent_conductor_domain::Task;
use agent_conductor_store::{AuxiliaryKind, AuxiliaryPreparation, EffectAudit, StoreError};
use sha2::{Digest, Sha256};
use std::path::Path;
use uuid::Uuid;

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn publish_auxiliary_result(
        &self,
        input: &AuxiliaryPreparation,
        actor: agent_conductor_domain::Actor,
        kind: agent_conductor_domain::CommandKind,
        reason: String,
    ) -> Result<(), StoreError> {
        use agent_conductor_domain::{Command, CorrelationId, IdempotencyKey};
        let row = self.store.task(&input.task_id).await?.ok_or_else(invalid)?;
        let command = Command {
            actor,
            expected_version: u64::try_from(input.task_version).map_err(|_| invalid())?,
            correlation_id: CorrelationId(input.invocation_id.clone()),
            idempotency_key: IdempotencyKey(format!("auxiliary-result:{}", input.invocation_id)),
            reason,
            kind,
        };
        let request = serde_json::to_string(&(row.id.as_str(), &command)).map_err(|_| invalid())?;
        let (batch, _) = self
            .prepare_command_batch(row, command, crate::hash(&request), request)
            .await
            .map_err(|_| invalid())?;
        self.store
            .persist_auxiliary_publication(input, &batch)
            .await?;
        Ok(())
    }

    /// Only for synchronous preparation that cannot spawn a supervisor/payload.
    /// Never wrap the launch/session call or infer this result during recovery.
    pub(super) async fn auxiliary_prelaunch_result<T>(
        &self,
        input: &AuxiliaryPreparation,
        result: Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                let id = Uuid::now_v7().to_string();
                self.store
                    .abandon_auxiliary_before_launch(
                        input,
                        EffectAudit {
                            id: &id,
                            actor_id: "conductor-runtime",
                            correlation_id: &input.invocation_id,
                            now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
                        },
                    )
                    .await?;
                Err(error)
            }
        }
    }

    pub(super) async fn begin_review_auxiliary(
        &self,
        task: &Task,
        packet: &conductor_runtime::ResumePacket,
        source: &str,
        artifacts: &Path,
    ) -> Result<AuxiliaryPreparation, StoreError> {
        let definition = serde_json::to_vec(packet).map_err(|_| invalid())?;
        self.begin_auxiliary(
            task,
            AuxiliaryKind::Verifier,
            source,
            &artifacts.join("review-input"),
            &definition,
        )
        .await
    }

    pub(super) async fn current_auxiliary_host(
        &self,
    ) -> Result<agent_conductor_store::AuxiliaryHostObservation, StoreError> {
        let (authority, generation) = self.store.runtime_authority().await?;
        if generation != self.boot_generation {
            return Err(invalid());
        }
        let authority_id = authority.ok_or_else(invalid)?;
        let native = conductor_runtime::HostEpoch::current()
            .await
            .map_err(|_| invalid())?;
        Ok(agent_conductor_store::AuxiliaryHostObservation {
            authority_id,
            generation: i64::try_from(generation).map_err(|_| invalid())?,
            platform: native.platform,
            host_id: native.host_id,
            boot_id: native.boot_id,
        })
    }

    pub(super) async fn begin_auxiliary(
        &self,
        task: &Task,
        kind: AuxiliaryKind,
        source: &str,
        workspace: &Path,
        definition: &[u8],
    ) -> Result<AuxiliaryPreparation, StoreError> {
        let attempt = task.attempt.as_ref().ok_or_else(invalid)?;
        let execution = self
            .store
            .execution_for_attempt(&attempt.id.0)
            .await?
            .ok_or_else(invalid)?;
        let input = AuxiliaryPreparation {
            invocation_id: Uuid::now_v7().to_string(),
            task_id: task.id.0.clone(),
            execution_token: execution.preparation.effect_token,
            attempt_id: attempt.id.0.clone(),
            task_version: i64::try_from(task.version).map_err(|_| invalid())?,
            boot_generation: i64::try_from(self.boot_generation).map_err(|_| invalid())?,
            kind,
            source: source.into(),
            definition_sha256: format!("{:x}", Sha256::digest(definition)),
            workspace: workspace.to_str().ok_or_else(invalid)?.into(),
        };
        let host = self.current_auxiliary_host().await?;
        let id = Uuid::now_v7().to_string();
        if !self
            .store
            .prepare_auxiliary_with_host(
                &input,
                &host,
                EffectAudit {
                    id: &id,
                    actor_id: "conductor-runtime",
                    correlation_id: &input.invocation_id,
                    now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
                },
            )
            .await?
        {
            return Err(invalid());
        }
        Ok(input)
    }
}
fn invalid() -> StoreError {
    StoreError::Integrity("auxiliary invocation admission refused".into())
}
