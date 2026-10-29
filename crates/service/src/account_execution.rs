//! Account-channel preparation and release. No local child identity is invented;
//! terminal/capture integration must establish its own explicit observation basis.
use crate::{Clock, PersistentControlService};
use agent_conductor_store::{
    EffectAudit, ExecutionPreparation, ExecutionRecord, ExecutionUpdate, StoreError,
};
use conductor_runtime::{
    AccountChannelAddress, AccountNativeLaunch, AccountRunnerConnection, AdapterKind, ResumePacket,
    RunnerConnectionError, RunnerProcessIdentity, VendorPin,
};
use uuid::Uuid;

/// Consumed by one connection attempt. Only fresh durable preparation creates it;
/// it cannot be deserialized or reconstructed to replay an uncertain release.
pub struct AccountExecutionIntent {
    record: ExecutionRecord,
    packet: ResumePacket,
    identity: RunnerProcessIdentity,
}

impl AccountExecutionIntent {
    /// Non-secret receiver tag, persisted before the operator starts its listener.
    #[must_use]
    pub fn identity(&self) -> &RunnerProcessIdentity {
        &self.identity
    }
}

impl<C: Clock> PersistentControlService<C> {
    /// Renew only the recorded account execution with current task ownership.
    /// # Errors
    /// Rejects mismatched bindings, terminal state, expiry or failed transport.
    pub async fn renew_account_execution(
        &self,
        token: &str,
        wrapper_identity: &str,
        connection: &mut AccountRunnerConnection,
    ) -> Result<(), StoreError> {
        let _guard = self.mutations.lock().await;
        let record = self
            .store
            .execution(token)
            .await?
            .ok_or_else(|| invalid("account execution missing"))?;
        let packet = ResumePacket::decode(record.preparation.packet_json.as_bytes())
            .map_err(|_| invalid("stored account packet invalid"))?;
        let peer = record
            .account_peer
            .as_ref()
            .ok_or_else(|| invalid("account peer binding missing"))?;
        let identity = connection.receiver_identity();
        let (authority, generation) = self.store.runtime_authority().await?;
        if record.state != agent_conductor_store::ExecutionState::Started
            || record.wrapper_identity.as_deref() != Some(wrapper_identity)
            || record.preparation.packet_sha256 != connection.packet_sha256()
            || record.account_result.is_some()
            || record.process.is_some()
            || packet.contract.full_capability.is_none()
            || identity.wrapper_identity != wrapper_identity
            || peer.authority_id != identity.authority_id
            || authority.as_deref() != Some(&peer.authority_id)
            || generation != self.boot_generation
            || peer.boot_generation != self.boot_generation
        {
            return Err(invalid("account execution binding mismatch"));
        }
        let deadline = self.validate_runtime_owner(&packet).await?;
        connection
            .renew(deadline)
            .await
            .map_err(|_| invalid("account renewal transport failed"))
    }

    /// Persist current full-profile intent before any account listener connection.
    /// This does not inspect a foreign private workspace or publish owned scans.
    /// # Errors
    /// Refuses missing authority, stale ownership, non-full profiles and replay.
    pub async fn prepare_account_execution(
        &self,
        packet: &ResumePacket,
        effect_token: &str,
        runner_id: &str,
    ) -> Result<AccountExecutionIntent, StoreError> {
        let _guard = self.mutations.lock().await;
        self.validate_runtime_owner(packet).await?;
        if packet.contract.full_capability.is_none()
            || packet.contract.adapter.kind == AdapterKind::Fake
        {
            return Err(invalid("account execution requires a full native profile"));
        }
        let (authority, generation) = self.store.runtime_authority().await?;
        let authority_id =
            authority.ok_or_else(|| invalid("account execution requires runtime authority"))?;
        if generation != self.boot_generation {
            return Err(invalid("account execution belongs to a stale boot"));
        }
        let identity = RunnerProcessIdentity {
            authority_id,
            packet_sha256: packet.sha256().map_err(|_| invalid("packet hash failed"))?,
            wrapper_identity: Uuid::now_v7().to_string(),
        };
        identity
            .argument()
            .map_err(|_| invalid("account identity invalid"))?;
        let input = ExecutionPreparation {
            expected_wrapper_identity: Some(identity.wrapper_identity.clone()),
            effect_token: effect_token.into(),
            task_id: packet.contract.task_id.clone(),
            attempt_id: packet.contract.attempt_id.clone(),
            lease_epoch: i64::try_from(packet.contract.lease_epoch)
                .map_err(|_| invalid("epoch overflow"))?,
            runner_id: runner_id.into(),
            packet_json: serde_json::to_string(packet)
                .map_err(|_| invalid("packet encoding failed"))?,
            packet_sha256: identity.packet_sha256.clone(),
        };
        let id = Uuid::now_v7().to_string();
        let (record, fresh) = self
            .store
            .prepare_account_execution_once(
                &input,
                EffectAudit {
                    id: &id,
                    actor_id: "conductor-runtime",
                    correlation_id: effect_token,
                    now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
                },
            )
            .await?;
        if !fresh {
            return Err(invalid(
                "account execution already exists; reconcile before retry",
            ));
        }
        Ok(AccountExecutionIntent {
            record,
            packet: packet.clone(),
            identity,
        })
    }

    /// Consume fresh intent and persist its ACK before channel Release. Account
    /// credentials stay at the receiver; process metadata deliberately stays None.
    /// No retry or source-capture eligibility is granted by this operation.
    /// # Errors
    /// Refuses changed intent, stale ownership, wrong peer/packet, or failed ACK.
    pub async fn release_account_execution(
        &self,
        intent: AccountExecutionIntent,
        address: &AccountChannelAddress,
        pin: &VendorPin,
    ) -> Result<(ExecutionRecord, AccountRunnerConnection), StoreError> {
        let _guard = self.mutations.lock().await;
        let token = &intent.record.preparation.effect_token;
        self.validate_runtime_owner(&intent.packet).await?;
        if self.store.execution(token).await?.as_ref() != Some(&intent.record) {
            return Err(invalid("account execution preparation changed"));
        }
        let mut acknowledged = None;
        let slot = &mut acknowledged;
        let launch = AccountNativeLaunch {
            packet: intent.packet.clone(),
            pin: pin.clone(),
        };
        let expected = intent.identity.clone();
        let connection =
            AccountRunnerConnection::connect(address, &launch, &expected, |ack| async move {
                self.validate_runtime_owner(&intent.packet)
                    .await
                    .map_err(|_| RunnerConnectionError::Persistence)?;
                let id = Uuid::now_v7().to_string();
                let record = self
                    .store
                    .update_execution(
                        token,
                        intent.record.revision,
                        &intent.identity.wrapper_identity,
                        &ExecutionUpdate::AcknowledgeAccount {
                            wrapper_identity: intent.identity.wrapper_identity.clone(),
                            packet_sha256: ack.packet_sha256,
                            peer: agent_conductor_store::AccountExecutionPeer {
                                worker_uid: address.worker_uid,
                                coordinator_uid: address.coordinator_uid,
                                authority_id: intent.identity.authority_id.clone(),
                                boot_generation: self.boot_generation,
                            },
                        },
                        EffectAudit {
                            id: &id,
                            actor_id: "conductor-runtime",
                            correlation_id: token,
                            now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
                        },
                    )
                    .await
                    .map_err(|_| RunnerConnectionError::Persistence)?;
                let deadline = self
                    .validate_runtime_owner(&intent.packet)
                    .await
                    .map_err(|_| RunnerConnectionError::Persistence)?;
                *slot = Some(record);
                Ok(deadline)
            })
            .await
            .map_err(|_| invalid("account release failed; reconcile durable execution"))?;
        Ok((
            acknowledged.ok_or_else(|| invalid("account acknowledgement missing"))?,
            connection,
        ))
    }
}

fn invalid(reason: &str) -> StoreError {
    StoreError::Integrity(reason.into())
}
