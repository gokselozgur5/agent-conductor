//! Trusted launch preparation; executable approval remains the caller's policy.
use agent_conductor_store::{EffectAudit, FileBrokerBinding, FileBrokerLaunchBinding, Store};
use conductor_runtime::{
    BrokerError, BrokerReceiptDirectory, BrokerRunnerLaunch, HostEpoch, broker_root_identity,
};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

fn approved_hash(wrapper: &Path, expected: &str) -> Result<(), BrokerError> {
    if !wrapper.is_absolute()
        || wrapper.canonicalize().map_err(|_| BrokerError::Refused)? != wrapper
    {
        return Err(BrokerError::Refused);
    }
    let file = File::open(wrapper).map_err(|_| BrokerError::Refused)?;
    if !file.metadata().map_err(|_| BrokerError::Refused)?.is_file() {
        return Err(BrokerError::Refused);
    }
    let mut hash = Sha256::new();
    let size = io::copy(&mut file.take(512 * 1024 * 1024 + 1), &mut hash)
        .map_err(|_| BrokerError::Refused)?;
    if size > 512 * 1024 * 1024 || format!("{:x}", hash.finalize()) != expected {
        return Err(BrokerError::Refused);
    }
    Ok(())
}

/// Persist this exact private launch before creating its approved wrapper.
/// The supplied digest must come from independent executable approval. Caller
/// must prevent replacement through spawn; hashing is not executable admission.
/// Replay refuses creation. Cancellation after commit leaves ownership unresolved.
/// # Errors
/// Refuses mismatched scope, roots, executable hash, host identity or persistence.
pub async fn record_broker_launch(
    store: &Store,
    binding: &FileBrokerBinding,
    launch: &BrokerRunnerLaunch,
    wrapper: &Path,
    approved_sha256: &str,
    event: EffectAudit<'_>,
) -> Result<(), BrokerError> {
    if launch.scope.authority_id != binding.authority_id
        || i64::try_from(launch.scope.boot_generation).ok() != Some(binding.boot_generation)
        || launch.scope.invocation_id != binding.invocation_id
        || launch.scope.task_id != binding.task_id
        || launch.scope.attempt_id != binding.attempt_id
        || i64::try_from(launch.scope.lease_epoch).ok() != Some(binding.lease_epoch)
        || launch.root_identity_sha256 != binding.root_identity_sha256
        || broker_root_identity(&launch.root)? != binding.root_identity_sha256
    {
        return Err(BrokerError::Refused);
    }
    approved_hash(wrapper, approved_sha256)?;
    BrokerReceiptDirectory::open(&launch.receipt_root)?;
    let host = HostEpoch::current()
        .await
        .map_err(|_| BrokerError::Refused)?;
    let metadata = FileBrokerLaunchBinding {
        launch_sha256: launch.sha256()?,
        wrapper_path: wrapper.to_str().ok_or(BrokerError::Invalid)?.into(),
        wrapper_sha256: approved_sha256.into(),
        receipt_root: launch
            .receipt_root
            .to_str()
            .ok_or(BrokerError::Invalid)?
            .into(),
        receipt_root_identity_sha256: broker_root_identity(&launch.receipt_root)?,
        platform: host.platform,
        host_id: host.host_id,
        boot_id: host.boot_id,
    };
    if !store
        .prepare_file_broker_launch(binding, &metadata, event)
        .await
        .map_err(|_| BrokerError::Admission)?
    {
        return Err(BrokerError::Refused);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn approved_wrapper_hash_rejects_changed_bytes_and_aliases() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().canonicalize().unwrap().join("runner");
        std::fs::write(&file, b"approved fixture").unwrap();
        let expected = format!("{:x}", Sha256::digest(b"approved fixture"));
        approved_hash(&file, &expected).unwrap();
        let alias = file.with_file_name("alias");
        std::os::unix::fs::symlink(&file, &alias).unwrap();
        assert!(approved_hash(&alias, &expected).is_err());
        std::fs::write(&file, b"changed fixture").unwrap();
        assert!(approved_hash(&file, &expected).is_err());
        assert!(approved_hash(file.parent().unwrap(), &expected).is_err());
    }
}
