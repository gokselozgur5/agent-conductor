//! Trusted bridge from parent-observed native identity to durable broker ownership.
use agent_conductor_store::{
    EffectAudit, ExecutionProcess, FileBrokerBinding, FileBrokerOwner, Store,
};
use conductor_runtime::{
    BrokerError, BrokerReceiptDirectory, BrokerShutdownObservation, BrokerShutdownReceipt,
    broker_root_identity,
};
use sha2::{Digest, Sha256};
use std::path::Path;

fn bound_owner(
    binding: &FileBrokerBinding,
    receipt: &BrokerShutdownReceipt,
    root: &Path,
) -> Result<FileBrokerOwner, BrokerError> {
    if receipt.version != 1
        || receipt.scope.authority_id != binding.authority_id
        || i64::try_from(receipt.scope.boot_generation).ok() != Some(binding.boot_generation)
        || receipt.scope.invocation_id != binding.invocation_id
        || receipt.scope.task_id != binding.task_id
        || receipt.scope.attempt_id != binding.attempt_id
        || i64::try_from(receipt.scope.lease_epoch).ok() != Some(binding.lease_epoch)
        || receipt.root_identity_sha256 != binding.root_identity_sha256
    {
        return Err(BrokerError::Refused);
    }
    BrokerReceiptDirectory::open(root)?;
    Ok(FileBrokerOwner {
        receipt_root: Some(root.to_str().ok_or(BrokerError::Invalid)?.into()),
        launch_sha256: receipt.launch_sha256.clone(),
        receipt_root_identity_sha256: broker_root_identity(root)?,
        process: ExecutionProcess {
            platform: receipt.process.host.platform.clone(),
            host_id: receipt.process.host.host_id.clone(),
            boot_id: receipt.process.host.boot_id.clone(),
            pid: receipt.process.pid,
            start_marker: receipt.process.start_marker.clone(),
        },
    })
}
/// Record the independently observed prepared identity before parent Release.
/// The caller must own the unreaped approved wrapper; serialized identity alone
/// is not admission. Exact replay refuses a second release.
/// # Errors
/// Refuses mismatched binding, stale lease, replay or failed durable persistence.
pub async fn record_broker_owner(
    store: &Store,
    binding: &FileBrokerBinding,
    prepared: &BrokerShutdownReceipt,
    root: &Path,
    event: EffectAudit<'_>,
) -> Result<(), BrokerError> {
    if prepared.observation != BrokerShutdownObservation::NeverReleased {
        return Err(BrokerError::Refused);
    }
    prepared
        .process
        .revalidate()
        .await
        .map_err(|_| BrokerError::Refused)?;
    let owner = bound_owner(binding, prepared, root)?;
    if !store
        .authorize_file_broker_release(binding, &owner, event)
        .await
        .map_err(|_| BrokerError::Admission)?
    {
        return Err(BrokerError::Refused);
    }
    Ok(())
}
/// Verify the independent receipt against persisted parent ownership before
/// closing the broker. Never fences the execution or accepts the task.
/// Current-generation closure only; use recovery for an older generation.
/// # Errors
/// Refuses missing/substituted receipts, moved roots, pending operations or stale ownership.
pub async fn close_broker_from_receipt(
    store: &Store,
    binding: &FileBrokerBinding,
    receipt: &BrokerShutdownReceipt,
    root: &Path,
    event: EffectAudit<'_>,
) -> Result<bool, BrokerError> {
    let expected = store
        .file_broker_owner(binding)
        .await
        .map_err(|_| BrokerError::Refused)?;
    let mut actual = bound_owner(binding, receipt, root)?;
    if expected.receipt_root.is_none() {
        actual.receipt_root = None;
    }
    if expected != actual {
        return Err(BrokerError::Refused);
    }
    let observed = BrokerReceiptDirectory::open(root)?.read(receipt)?;
    if broker_root_identity(root)? != expected.receipt_root_identity_sha256 {
        return Err(BrokerError::Refused);
    }
    let completed = match observed.observation {
        BrokerShutdownObservation::NeverReleased => 0,
        BrokerShutdownObservation::Revoked { completed_sequence } => {
            i64::try_from(completed_sequence).map_err(|_| BrokerError::Invalid)?
        }
    };
    let bytes = serde_json::to_vec(&observed).map_err(|_| BrokerError::Invalid)?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    store
        .close_owned_file_broker(binding, &expected, completed, &digest, event)
        .await
        .map_err(|_| BrokerError::Refused)
}

/// Recover solely from persisted owner/operation state and a private receipt.
/// No terminal message, PID absence, signaling or receipt-provided identity is
/// used to establish ownership. Missing evidence remains unresolved.
/// # Errors
/// Refuses foreign hosts, stale generations, changed roots and incomplete effects.
pub async fn recover_broker_from_receipt(
    store: &Store,
    binding: &FileBrokerBinding,
    root: &Path,
    generation: i64,
    event: EffectAudit<'_>,
) -> Result<bool, BrokerError> {
    let host = conductor_runtime::HostEpoch::current()
        .await
        .map_err(|_| BrokerError::Refused)?;
    recover_on_host(store, binding, root, generation, event, &host).await
}
async fn recover_on_host(
    store: &Store,
    binding: &FileBrokerBinding,
    root: &Path,
    generation: i64,
    event: EffectAudit<'_>,
    host: &conductor_runtime::HostEpoch,
) -> Result<bool, BrokerError> {
    use conductor_runtime::{BrokerScope, ChildIdentity, HostEpoch};
    let owner = store
        .file_broker_owner(binding)
        .await
        .map_err(|_| BrokerError::Refused)?;
    if owner.process.platform != host.platform
        || owner.process.host_id != host.host_id
        || broker_root_identity(root)? != owner.receipt_root_identity_sha256
        || generation < binding.boot_generation
    {
        return Err(BrokerError::Refused);
    }
    let status = store
        .file_broker_status(binding)
        .await
        .map_err(|_| BrokerError::Refused)?;
    if status.admitted != status.completed {
        return Err(BrokerError::Uncertain);
    }
    let completed = u64::try_from(status.completed).map_err(|_| BrokerError::Invalid)?;
    let mut expected = BrokerShutdownReceipt {
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
        launch_sha256: owner.launch_sha256.clone(),
        root_identity_sha256: binding.root_identity_sha256.clone(),
        process: ChildIdentity {
            host: HostEpoch {
                platform: owner.process.platform.clone(),
                host_id: owner.process.host_id.clone(),
                boot_id: owner.process.boot_id.clone(),
            },
            pid: owner.process.pid,
            start_marker: owner.process.start_marker.clone(),
        },
        observation: BrokerShutdownObservation::Revoked {
            completed_sequence: completed,
        },
    };
    let directory = BrokerReceiptDirectory::open(root)?;
    let receipt = match directory.read(&expected) {
        Ok(receipt) => receipt,
        Err(_) if completed == 0 => {
            expected.observation = BrokerShutdownObservation::NeverReleased;
            directory.read(&expected)?
        }
        Err(error) => return Err(error),
    };
    if broker_root_identity(root)? != owner.receipt_root_identity_sha256 {
        return Err(BrokerError::Refused);
    }
    let digest = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&receipt).map_err(|_| BrokerError::Invalid)?)
    );
    if generation == binding.boot_generation {
        store
            .close_owned_file_broker(binding, &owner, status.completed, &digest, event)
            .await
    } else {
        store
            .recover_owned_file_broker(
                binding,
                &owner,
                status.completed,
                &digest,
                generation,
                event,
            )
            .await
    }
    .map_err(|_| BrokerError::Refused)
}

impl<C: crate::Clock> crate::PersistentControlService<C> {
    pub(super) async fn recover_file_broker_receipts(
        &self,
    ) -> Result<usize, agent_conductor_store::StoreError> {
        let mut cursor = self.broker_recovery_cursor.lock().await;
        let page = self
            .store
            .pending_file_broker_page(cursor.as_deref())
            .await?;
        *cursor = if page.len() == 128 {
            page.last().map(|row| row.invocation_id.clone())
        } else {
            None
        };
        if page.is_empty() {
            return Ok(0);
        }
        let Ok(host) = conductor_runtime::HostEpoch::current().await else {
            return Ok(0);
        };
        let generation = i64::try_from(self.boot_generation)
            .map_err(|_| agent_conductor_store::StoreError::BootGenerationOverflow)?;
        let mut recovered = 0;
        for candidate in page {
            let Ok(binding) = candidate.binding else {
                continue;
            };
            // Re-read exact ownership inside recovery before accepting evidence.
            let owner = match self.store.file_broker_owner(&binding).await {
                Ok(owner) => owner,
                Err(agent_conductor_store::StoreError::Integrity(_)) => continue,
                Err(error) => return Err(error),
            };
            let Some(root) = owner.receipt_root else {
                continue;
            };
            let id = uuid::Uuid::now_v7().to_string();
            let event = EffectAudit {
                id: &id,
                actor_id: "conductor-broker-recovery",
                correlation_id: &binding.invocation_id,
                now_ms: self.clock.read(self.boot_generation).wall_unix_ms,
            };
            if matches!(
                recover_on_host(
                    &self.store,
                    &binding,
                    Path::new(&root),
                    generation,
                    event,
                    &host
                )
                .await,
                Ok(true)
            ) {
                recovered += 1;
            }
        }
        Ok(recovered)
    }
}
