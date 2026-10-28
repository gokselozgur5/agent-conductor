//! Internal controlled-runtime port. Not an operator command or vendor adapter.
use super::{Clock, PersistentControlService};
use agent_conductor_domain::{Task, TaskState};
use agent_conductor_store::{
    EffectAudit, ExecutionPreparation, ExecutionRecord, ExecutionState, ExecutionUpdate, StoreError,
};
use conductor_runtime::{
    AdapterKind, ResumePacket, RunnerConnection, RunnerConnectionError, RunnerLaunch,
};
use std::path::Path;
use uuid::Uuid;

impl<C: Clock> PersistentControlService<C> {
    /// Starts one controlled fake worker from an already claimed spawn effect.
    /// Preparation, current ownership checks and recorded acknowledgement precede
    /// release. The mutation lock orders release against task cancellation.
    /// Real vendor adapters are refused until their isolation/conformance gate.
    /// # Errors
    /// Refuses stale ownership, replay, unsupported adapters and protocol failures.
    /// Failed launch leaves durable intent for reconciliation, never deletes it.
    pub async fn launch_controlled_execution(
        &self,
        wrapper: &Path,
        launch: &RunnerLaunch,
        effect_token: &str,
        runner_id: &str,
    ) -> Result<(ExecutionRecord, RunnerConnection), StoreError> {
        let _guard = self.mutations.lock().await;
        if launch.packet.contract.adapter.kind != AdapterKind::Fake {
            return Err(runtime_error("vendor isolation is not accepted"));
        }
        self.validate_runtime_owner(&launch.packet).await?;
        let packet_json =
            serde_json::to_string(&launch.packet).map_err(|_| runtime_error("packet encoding"))?;
        let input = ExecutionPreparation {
            effect_token: effect_token.into(),
            task_id: launch.packet.contract.task_id.clone(),
            attempt_id: launch.packet.contract.attempt_id.clone(),
            lease_epoch: i64::try_from(launch.packet.contract.lease_epoch)
                .map_err(|_| runtime_error("epoch overflow"))?,
            runner_id: runner_id.into(),
            packet_json,
            packet_sha256: launch
                .packet
                .sha256()
                .map_err(|_| runtime_error("packet hash"))?,
        };
        let audit_id = Uuid::now_v7().to_string();
        let now = self.clock.read(self.boot_generation);
        let (record, fresh) = self
            .store
            .prepare_execution_once(
                &input,
                EffectAudit {
                    id: &audit_id,
                    actor_id: "conductor-runtime",
                    correlation_id: effect_token,
                    now_ms: now.wall_unix_ms,
                },
            )
            .await?;
        if !fresh {
            return Err(runtime_error(
                "execution already exists; reconcile before retry",
            ));
        }
        let wrapper_identity = Uuid::now_v7().to_string();
        let mut acknowledged = None;
        let connection = RunnerConnection::launch(wrapper, launch, |ack| async {
            // The wrapper may take time to start. Recheck the monotonic lease,
            // not just the SQL active flag, immediately before recording its ACK.
            self.validate_runtime_owner(&launch.packet)
                .await
                .map_err(|_| RunnerConnectionError::Persistence)?;
            let audit_id = Uuid::now_v7().to_string();
            let now = self.clock.read(self.boot_generation);
            let result = self
                .store
                .update_execution(
                    effect_token,
                    record.revision,
                    &wrapper_identity,
                    &ExecutionUpdate::Acknowledge {
                        wrapper_identity: wrapper_identity.clone(),
                        packet_sha256: ack.packet_sha256,
                    },
                    EffectAudit {
                        id: &audit_id,
                        actor_id: "conductor-runtime",
                        correlation_id: effect_token,
                        now_ms: now.wall_unix_ms,
                    },
                )
                .await
                .map_err(|_| RunnerConnectionError::Persistence)?;
            // A slow database acknowledgement must not revive an expired lease.
            self.validate_runtime_owner(&launch.packet)
                .await
                .map_err(|_| RunnerConnectionError::Persistence)?;
            acknowledged = Some(result);
            Ok(())
        })
        .await
        .map_err(|_| runtime_error("wrapper release failed; reconcile durable execution"))?;
        Ok((
            acknowledged.ok_or_else(|| runtime_error("acknowledgement missing"))?,
            connection,
        ))
    }

    /// Renews only the current live execution under lifecycle serialization.
    /// # Errors
    /// Refuses ended/replaced/expired attempts or an unacknowledged execution.
    /// On refusal, the caller must drop/cancel the connection and reconcile.
    pub async fn renew_controlled_execution(
        &self,
        token: &str,
        wrapper_identity: &str,
        connection: &mut RunnerConnection,
    ) -> Result<(), StoreError> {
        let _guard = self.mutations.lock().await;
        let record = self
            .store
            .execution(token)
            .await?
            .ok_or_else(|| runtime_error("missing execution"))?;
        if record.preparation.packet_sha256 != connection.packet_sha256()
            || record.state != ExecutionState::Started
            || record.wrapper_identity.as_deref() != Some(wrapper_identity)
        {
            return Err(runtime_error("execution identity mismatch"));
        }
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| runtime_error("invalid stored packet"))?;
        self.validate_runtime_owner(&packet).await?;
        connection
            .renew()
            .await
            .map_err(|_| runtime_error("wrapper renewal failed"))
    }

    pub(super) async fn validate_runtime_owner(
        &self,
        packet: &ResumePacket,
    ) -> Result<(), StoreError> {
        packet
            .validate()
            .map_err(|_| runtime_error("invalid runtime contract"))?;
        let row = self
            .store
            .task(&packet.contract.task_id)
            .await?
            .ok_or_else(|| runtime_error("missing task"))?;
        let task: Task = serde_json::from_str(&row.aggregate_json)
            .map_err(|_| runtime_error("invalid task aggregate"))?;
        let attempt = task
            .attempt
            .as_ref()
            .ok_or_else(|| runtime_error("missing attempt"))?;
        let now = self.clock.read(self.boot_generation);
        if !matches!(task.state, TaskState::Claimed | TaskState::Running)
            || attempt.id.0 != packet.contract.attempt_id
            || attempt.worker.0 != packet.contract.worker_id
            || attempt.lease.epoch != packet.contract.lease_epoch
            || !attempt.lease.accepts_worker_mutation(now)
            || task.required_checks.iter().any(|required| {
                !packet
                    .contract
                    .checks
                    .iter()
                    .any(|check| check.id == required.id.0)
            })
        {
            return Err(runtime_error("runtime attempt or lease is not current"));
        }
        Ok(())
    }
}
fn runtime_error(reason: &str) -> StoreError {
    StoreError::Integrity(reason.into())
}
