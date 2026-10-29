//! Persistent account reports and imported source binding, without local PID claims.
use crate::{Clock, PersistentControlService};
use agent_conductor_store::{
    EffectAudit, ExecutionRecord, ExecutionState, ExecutionUpdate, StoreError,
};
use conductor_runtime::{AccountRunnerConnection, ResumePacket, RunnerEvent, VendorCandidate};
use std::time::Duration;
use uuid::Uuid;

/// One successful parsed peer report, consumed when its candidate is imported.
pub struct AccountExecutionOutcome {
    pub(super) record: ExecutionRecord,
    pub(super) packet: ResumePacket,
    pub(super) candidate: VendorCandidate,
}

impl<C: Clock> PersistentControlService<C> {
    /// Drive the account channel with current-owner renewals and persist a parsed
    /// terminal report. Transport loss and stop messages are never successful exits.
    /// # Errors
    /// Refuses stale ownership, mismatched receiver, malformed output or uncertainty.
    pub async fn await_account_execution(
        &self,
        token: &str,
        mut connection: AccountRunnerConnection,
    ) -> Result<AccountExecutionOutcome, StoreError> {
        let wrapper = connection.receiver_identity().wrapper_identity.clone();
        let mut timer = tokio::time::interval(Duration::from_millis(200));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let event = loop {
            tokio::select! {
                event = connection.event() => match event {
                    Ok(RunnerEvent::Checkpoint { claims }) => {
                        if self.capture_worker_checkpoint(token, &wrapper, &claims).await.is_err() {
                            let _ = connection.cancel().await;
                            return Err(invalid("account checkpoint refused"));
                        }
                    },
                    result => break result.ok(),
                },
                _ = timer.tick() => {
                    if self.renew_account_execution(token, &wrapper, &mut connection).await.is_err() {
                        // A peer can close its input before its terminal frame is
                        // consumed. Final admission below rechecks the durable owner.
                        break tokio::time::timeout(Duration::from_secs(1), self.drain_account_terminal(token, &wrapper, &mut connection)).await.ok().flatten();
                    }
                }
            }
        };
        let _guard = self.mutations.lock().await;
        let record = self.account_result_context(token, &connection).await?;
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| invalid("account packet invalid"))?;
        if let Err(error) = self.validate_runtime_owner(&packet).await {
            self.account_update(
                &record,
                ExecutionUpdate::Blocked {
                    reason: "account report lost current ownership".into(),
                },
            )
            .await?;
            return Err(error);
        }
        let Some(RunnerEvent::Finished {
            exit_code,
            stdout,
            stderr,
        }) = event
        else {
            self.account_update(
                &record,
                ExecutionUpdate::Blocked {
                    reason: "account terminal outcome unknown".into(),
                },
            )
            .await?;
            return Err(invalid("account terminal outcome unknown"));
        };
        let output = conductor_runtime::ProcessOutput {
            exit_code,
            stdout,
            stderr,
        };
        let Ok(candidate) = conductor_runtime::interpret_vendor_output(&packet, &output) else {
            self.account_update(
                &record,
                ExecutionUpdate::Blocked {
                    reason: "account candidate refused".into(),
                },
            )
            .await?;
            return Err(invalid("account candidate refused"));
        };
        let record = self
            .account_update(
                &record,
                ExecutionUpdate::ReportAccount {
                    wrapper_identity: wrapper,
                    output_sha256: candidate.output_sha256.clone(),
                },
            )
            .await?;
        self.record_vendor_usage(&packet, "worker", token, &output.stdout)
            .await?;
        Ok(AccountExecutionOutcome {
            record,
            packet,
            candidate,
        })
    }

    async fn drain_account_terminal(
        &self,
        token: &str,
        wrapper: &str,
        connection: &mut AccountRunnerConnection,
    ) -> Option<RunnerEvent> {
        loop {
            match connection.event().await.ok()? {
                RunnerEvent::Checkpoint { claims } => {
                    self.capture_worker_checkpoint(token, wrapper, &claims)
                        .await
                        .ok()?;
                }
                terminal => return Some(terminal),
            }
        }
    }

    async fn account_result_context(
        &self,
        token: &str,
        connection: &AccountRunnerConnection,
    ) -> Result<ExecutionRecord, StoreError> {
        let record = self
            .store
            .execution(token)
            .await?
            .ok_or_else(|| invalid("account execution missing"))?;
        let identity = connection.receiver_identity();
        let peer = record
            .account_peer
            .as_ref()
            .ok_or_else(|| invalid("account peer missing"))?;
        let (authority, generation) = self.store.runtime_authority().await?;
        if !record.account_channel
            || record.state != ExecutionState::Started
            || record.process.is_some()
            || record.account_result.is_some()
            || record.wrapper_identity.as_deref() != Some(&identity.wrapper_identity)
            || record.preparation.packet_sha256 != connection.packet_sha256()
            || identity.authority_id != peer.authority_id
            || authority.as_deref() != Some(&peer.authority_id)
            || generation != self.boot_generation
            || peer.boot_generation != self.boot_generation
        {
            return Err(invalid("account result connection mismatch"));
        }
        Ok(record)
    }

    pub(super) async fn account_update(
        &self,
        record: &ExecutionRecord,
        update: ExecutionUpdate,
    ) -> Result<ExecutionRecord, StoreError> {
        let id = Uuid::now_v7().to_string();
        self.store
            .update_execution(
                &record.preparation.effect_token,
                record.revision,
                &id,
                &update,
                EffectAudit {
                    id: &id,
                    actor_id: "conductor-runtime",
                    correlation_id: &record.preparation.effect_token,
                    now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
                },
            )
            .await
    }
}
pub(super) fn invalid(message: &str) -> StoreError {
    StoreError::Integrity(message.into())
}
