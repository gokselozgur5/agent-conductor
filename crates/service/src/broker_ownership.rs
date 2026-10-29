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
/// Current-authority/current-generation closure only; older recovery is refused.
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
    let actual = bound_owner(binding, receipt, root)?;
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
