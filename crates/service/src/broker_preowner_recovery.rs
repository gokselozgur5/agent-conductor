//! Recover only independently acknowledged, never-registered broker preparations.
use agent_conductor_store::{EffectAudit, FileBrokerBinding, Store, UnreleasedBrokerLaunchProof};
use conductor_runtime::{
    BrokerError, BrokerReceiptDirectory, BrokerScope, BrokerShutdownObservation,
    BrokerShutdownReceipt, ChildIdentity, HostEpoch, broker_root_identity,
};
use sha2::{Digest, Sha256};
use std::path::Path;

/// Validate a `NeverReleased` receipt against persisted ACK and launch metadata.
/// A missing ACK stays unresolved; identity from the receipt is never authority.
/// Host policy must protect the private receipt directory and its ancestors.
/// # Errors
/// Refuses missing/substituted proof, foreign host, moved roots or store conflicts.
pub async fn recover_unregistered_broker_from_receipt(
    store: &Store,
    binding: &FileBrokerBinding,
    generation: i64,
    event: EffectAudit<'_>,
) -> Result<bool, BrokerError> {
    let host = HostEpoch::current()
        .await
        .map_err(|_| BrokerError::Refused)?;
    recover_on_host(store, binding, generation, event, &host).await
}

pub(super) async fn recover_on_host(
    store: &Store,
    binding: &FileBrokerBinding,
    generation: i64,
    event: EffectAudit<'_>,
    host: &HostEpoch,
) -> Result<bool, BrokerError> {
    let launch = store
        .file_broker_launch(binding)
        .await
        .map_err(|_| BrokerError::Refused)?;
    let execution = store
        .execution(&binding.execution_token)
        .await
        .map_err(|_| BrokerError::Refused)?
        .ok_or(BrokerError::Refused)?;
    let process = execution.process.ok_or(BrokerError::Refused)?;
    let root = Path::new(&launch.receipt_root);
    if host.platform != launch.platform
        || host.host_id != launch.host_id
        || process.platform != launch.platform
        || process.host_id != launch.host_id
        || process.boot_id != launch.boot_id
        || broker_root_identity(root)? != launch.receipt_root_identity_sha256
    {
        return Err(BrokerError::Refused);
    }
    let expected = BrokerShutdownReceipt {
        version: 1,
        scope: BrokerScope {
            authority_id: binding.authority_id.clone(),
            boot_generation: binding
                .boot_generation
                .try_into()
                .map_err(|_| BrokerError::Invalid)?,
            invocation_id: binding.invocation_id.clone(),
            task_id: binding.task_id.clone(),
            attempt_id: binding.attempt_id.clone(),
            lease_epoch: binding
                .lease_epoch
                .try_into()
                .map_err(|_| BrokerError::Invalid)?,
        },
        launch_sha256: launch.launch_sha256.clone(),
        root_identity_sha256: binding.root_identity_sha256.clone(),
        process: ChildIdentity {
            host: HostEpoch {
                platform: process.platform.clone(),
                host_id: process.host_id.clone(),
                boot_id: process.boot_id.clone(),
            },
            pid: process.pid,
            start_marker: process.start_marker.clone(),
        },
        observation: BrokerShutdownObservation::NeverReleased,
    };
    let receipt = BrokerReceiptDirectory::open(root)?.read(&expected)?;
    if broker_root_identity(root)? != launch.receipt_root_identity_sha256 {
        return Err(BrokerError::Refused);
    }
    let shutdown_sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&receipt).map_err(|_| BrokerError::Invalid)?)
    );
    store
        .recover_unregistered_broker_launch(
            binding,
            &UnreleasedBrokerLaunchProof {
                launch,
                process,
                shutdown_sha256,
            },
            generation,
            event,
        )
        .await
        .map_err(|_| BrokerError::Admission)
}

impl<C: crate::Clock> crate::PersistentControlService<C> {
    pub(super) async fn recover_preowner_broker_receipts(
        &self,
    ) -> Result<usize, agent_conductor_store::StoreError> {
        let mut cursor = self.broker_preowner_cursor.lock().await;
        let page = self
            .store
            .pending_unregistered_broker_launch_page(cursor.as_deref())
            .await?;
        *cursor = if page.len() == 128 {
            page.last().map(|row| row.invocation_id.clone())
        } else {
            None
        };
        if page.is_empty() {
            return Ok(0);
        }
        let Ok(host) = HostEpoch::current().await else {
            return Ok(0);
        };
        let generation = i64::try_from(self.boot_generation)
            .map_err(|_| agent_conductor_store::StoreError::BootGenerationOverflow)?;
        let mut recovered = 0;
        for candidate in page {
            let Ok(binding) = candidate.binding else {
                continue;
            };
            let id = uuid::Uuid::now_v7().to_string();
            let event = EffectAudit {
                id: &id,
                actor_id: "conductor-preowner-recovery",
                correlation_id: &binding.invocation_id,
                now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
            };
            if matches!(
                recover_on_host(&self.store, &binding, generation, event, &host).await,
                Ok(true)
            ) {
                recovered += 1;
            }
        }
        Ok(recovered)
    }
}
