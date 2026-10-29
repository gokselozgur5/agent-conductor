//! Explicit unrestricted harness launch under an already selected OS account.
//! Successful exit is candidate output, never acceptance or descendant fencing.
use crate::{AdapterKind, ExecutionAccount, ProcessSpec, VendorPin, binary_sha256};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

#[derive(Debug, thiserror::Error)]
pub enum FullCapabilityError {
    #[error(transparent)]
    Account(#[from] crate::ExecutionAccountError),
    #[error("full-capability executable does not match the configured pin")]
    Identity,
    #[error("full-capability launch requires an absolute working directory and PATH")]
    Environment,
}

/// Build a command with the account's normal configuration and unrestricted tools.
/// `path` is the operator's explicit per-account tool search path. Authentication
/// comes from that account's home, not the coordinator's environment or secrets.
/// # Errors
/// Rejects mismatched accounts/binaries and invalid working directories or PATH.
pub fn full_capability_command(
    account: &ExecutionAccount,
    pin: &VendorPin,
    directory: PathBuf,
    path: &str,
    prompt: String,
    timeout: Duration,
) -> Result<ProcessSpec, FullCapabilityError> {
    account.validate_worker()?;
    if pin.kind == AdapterKind::Fake
        || !pin.executable.is_absolute()
        || binary_sha256(&pin.executable).ok().as_ref() != Some(&pin.executable_sha256)
    {
        return Err(FullCapabilityError::Identity);
    }
    if !directory.is_absolute()
        || !directory.is_dir()
        || path.is_empty()
        || std::env::split_paths(path).any(|entry| !entry.is_absolute())
    {
        return Err(FullCapabilityError::Environment);
    }
    let arguments = match pin.kind {
        AdapterKind::Codex => vec![
            "--yolo".into(),
            "exec".into(),
            "--json".into(),
            "--skip-git-repo-check".into(),
            "--".into(),
            prompt,
        ],
        AdapterKind::Claude => vec![
            "--dangerously-skip-permissions".into(),
            "--output-format".into(),
            "stream-json".into(),
            "--verbose".into(),
            "--print".into(),
            "--".into(),
            prompt,
        ],
        AdapterKind::Fake => return Err(FullCapabilityError::Identity),
    };
    let environment = BTreeMap::from([
        ("HOME".into(), account.home.to_string_lossy().into_owned()),
        ("USER".into(), account.name.clone()),
        ("LOGNAME".into(), account.name.clone()),
        ("PATH".into(), path.into()),
    ]);
    Ok(ProcessSpec {
        executable: pin.executable.clone(),
        arguments,
        directory,
        environment,
        timeout,
        output_limit: 16 * 1024 * 1024,
    })
}
