//! Account identity for a worker started by the operator under its own OS user.
//! Account selection is not privilege escalation or a filesystem sandbox.
use nix::unistd::{User, getegid, geteuid, getgid, getuid};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAccount {
    pub name: String,
    pub uid: u32,
    pub gid: u32,
    pub home: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionAccountError {
    #[error("selected OS account is unavailable")]
    Unavailable,
    #[error("worker must run as the selected OS account with its current home and group")]
    Mismatch,
}

impl ExecutionAccount {
    /// Resolve account metadata without reading credentials or starting a process.
    /// # Errors
    /// Returns an error if the account cannot be resolved or has no absolute home.
    pub fn resolve(name: &str) -> Result<Self, ExecutionAccountError> {
        let user = User::from_name(name)
            .map_err(|_| ExecutionAccountError::Unavailable)?
            .ok_or(ExecutionAccountError::Unavailable)?;
        if !user.dir.is_absolute() {
            return Err(ExecutionAccountError::Unavailable);
        }
        Ok(Self {
            name: user.name,
            uid: user.uid.as_raw(),
            gid: user.gid.as_raw(),
            home: user.dir,
        })
    }

    /// Verify a frozen selection against both the account database and worker.
    /// The operator starts a worker in each account; this never invokes sudo or
    /// changes another account's identity, supplementary groups or credentials.
    /// # Errors
    /// Rejects stale account metadata and workers running under another identity.
    pub fn validate_worker(&self) -> Result<(), ExecutionAccountError> {
        if Self::resolve(&self.name)? != *self
            || getuid().as_raw() != self.uid
            || geteuid().as_raw() != self.uid
            || getgid().as_raw() != self.gid
            || getegid().as_raw() != self.gid
        {
            return Err(ExecutionAccountError::Mismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_account_roundtrips_and_stale_identity_is_rejected() {
        let user = User::from_uid(getuid()).unwrap().unwrap();
        let account = ExecutionAccount::resolve(&user.name).unwrap();
        account.validate_worker().unwrap();
        let encoded = serde_json::to_vec(&account).unwrap();
        let restored: ExecutionAccount = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(restored, account);
        let mut stale = account;
        stale.home.push("obsolete-home");
        assert!(matches!(
            stale.validate_worker(),
            Err(ExecutionAccountError::Mismatch)
        ));
    }
}
