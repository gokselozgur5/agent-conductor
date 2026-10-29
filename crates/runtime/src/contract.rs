use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const RUNTIME_PROTOCOL_VERSION: u16 = 1;
pub const MAX_PACKET_BYTES: usize = 256 * 1024;
const MAX_ITEMS: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterKind {
    Fake,
    Codex,
    Claude,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterIdentity {
    pub kind: AdapterKind,
    pub version: String,
    pub executable_sha256: String,
    pub conformance_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicy {
    Denied,
    InferenceOnly,
    Unrestricted,
}

/// Requested authority, not proof that a particular adapter enforces it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityEnvelope {
    pub read_roots: Vec<PathBuf>,
    pub write_roots: Vec<PathBuf>,
    pub network: NetworkPolicy,
    pub allow_workspace_commands: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionPin {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckDefinition {
    pub id: String,
    /// Exact operator-approved argv, never parsed through an implicit shell.
    pub argv: Vec<String>,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptContract {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_capability: Option<crate::FullCapabilityProfile>,
    pub version: u16,
    pub task_id: String,
    pub attempt_id: String,
    pub lease_epoch: u64,
    pub worker_id: String,
    pub adapter: AdapterIdentity,
    pub workspace: PathBuf,
    pub starting_head: String,
    pub authority: CapabilityEnvelope,
    pub instructions: Vec<InstructionPin>,
    pub checks: Vec<CheckDefinition>,
    pub max_runtime_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumePacket {
    pub contract: AttemptContract,
    pub objective: String,
    pub checkpoint: Option<String>,
    /// Data only: no authority is parsed from repository or worker prose.
    pub untrusted_context: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketAcknowledgement {
    pub version: u16,
    pub attempt_id: String,
    pub lease_epoch: u64,
    pub packet_sha256: String,
    pub instruction_sha256: Vec<String>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ContractError {
    #[error("unsupported runtime protocol version")]
    Version,
    #[error("missing or invalid contract identity")]
    Identity,
    #[error("invalid content digest")]
    Digest,
    #[error("invalid or escaping workspace authority")]
    Authority,
    #[error("invalid required check")]
    Check,
    #[error("runtime packet exceeds configured bounds")]
    Bounds,
    #[error("resume acknowledgement does not match the frozen packet")]
    Acknowledgement,
    #[error("invalid runtime packet encoding")]
    Encoding,
}

impl AttemptContract {
    /// Validates the typed request. Physical path resolution and negative
    /// capability probes are additionally required before actual execution.
    /// # Errors
    /// Rejects missing identities, unbounded input and escaping write authority.
    pub fn validate(&self) -> Result<(), ContractError> {
        self.validate_execution_profile()?;
        if self.version != RUNTIME_PROTOCOL_VERSION {
            return Err(ContractError::Version);
        }
        if [
            &self.task_id,
            &self.attempt_id,
            &self.worker_id,
            &self.adapter.version,
        ]
        .iter()
        .any(|value| !identity(value))
            || self.lease_epoch == 0
        {
            return Err(ContractError::Identity);
        }
        if !digest(&self.adapter.executable_sha256, &[64])
            || !digest(&self.adapter.conformance_sha256, &[64])
            || !digest(&self.starting_head, &[40, 64])
        {
            return Err(ContractError::Digest);
        }
        if self.max_runtime_ms == 0
            || self.max_runtime_ms > 86_400_000
            || self.instructions.len() > MAX_ITEMS
            || self.checks.len() > MAX_ITEMS
            || self.authority.read_roots.len() > MAX_ITEMS
            || self.authority.write_roots.len() > MAX_ITEMS
        {
            return Err(ContractError::Bounds);
        }
        if !absolute_clean(&self.workspace)
            || self.authority.read_roots.is_empty()
            || self
                .authority
                .read_roots
                .iter()
                .any(|path| !absolute_clean(path))
            || !self
                .authority
                .read_roots
                .iter()
                .any(|root| self.workspace.starts_with(root))
            || self.authority.write_roots.iter().any(|path| {
                !absolute_clean(path)
                    || (self.full_capability.is_none() && !path.starts_with(&self.workspace))
            })
        {
            return Err(ContractError::Authority);
        }
        let mut pinned = std::collections::BTreeSet::new();
        for instruction in &self.instructions {
            if !absolute_clean(&instruction.path)
                || !self
                    .authority
                    .read_roots
                    .iter()
                    .any(|root| instruction.path.starts_with(root))
                || !pinned.insert(&instruction.path)
            {
                return Err(ContractError::Authority);
            }
            if !digest(&instruction.sha256, &[64]) {
                return Err(ContractError::Digest);
            }
        }
        let mut checks = std::collections::BTreeSet::new();
        for check in &self.checks {
            if !identity(&check.id)
                || !checks.insert(&check.id)
                || check.argv.is_empty()
                || check.argv.len() > MAX_ITEMS
                || check.argv[0].is_empty()
                || check
                    .argv
                    .iter()
                    .any(|arg| arg.len() > 8192 || arg.contains('\0'))
                || check.timeout_ms == 0
                || check.timeout_ms > self.max_runtime_ms
            {
                return Err(ContractError::Check);
            }
        }
        Ok(())
    }

    fn validate_execution_profile(&self) -> Result<(), ContractError> {
        if let Some(profile) = &self.full_capability {
            if !identity(&profile.account.name)
                || !absolute_clean(&profile.account.home)
                || profile.path.is_empty()
                || profile.path.len() > 8192
                || profile.path.contains('\0')
                || std::env::split_paths(&profile.path).any(|path| !absolute_clean(&path))
                || self.authority.network != NetworkPolicy::Unrestricted
                || !self.authority.allow_workspace_commands
                || self.authority.read_roots != [PathBuf::from("/")]
                || self.authority.write_roots != [PathBuf::from("/")]
            {
                return Err(ContractError::Authority);
            }
        } else if self.authority.network == NetworkPolicy::Unrestricted {
            return Err(ContractError::Authority);
        }
        Ok(())
    }
}

impl ResumePacket {
    /// Parse and validate bounded, typed context without promoting prose to authority.
    /// # Errors
    /// Rejects oversized, unknown-field, malformed or invalid contracts.
    pub fn decode(bytes: &[u8]) -> Result<Self, ContractError> {
        if bytes.len() > MAX_PACKET_BYTES {
            return Err(ContractError::Bounds);
        }
        let packet: Self = serde_json::from_slice(bytes).map_err(|_| ContractError::Encoding)?;
        packet.validate()?;
        Ok(packet)
    }

    /// # Errors
    /// Returns the underlying contract or packet bound violation.
    pub fn validate(&self) -> Result<(), ContractError> {
        self.contract.validate()?;
        if self.objective.trim().is_empty() || self.untrusted_context.len() > MAX_ITEMS {
            return Err(ContractError::Bounds);
        }
        if self.encoded()?.len() > MAX_PACKET_BYTES {
            return Err(ContractError::Bounds);
        }
        Ok(())
    }

    /// Hashes the complete packet, including checkpoint and labelled context.
    /// # Errors
    /// Rejects invalid packets before deriving their identity.
    pub fn sha256(&self) -> Result<String, ContractError> {
        self.validate()?;
        Ok(format!("{:x}", Sha256::digest(self.encoded()?)))
    }

    /// # Errors
    /// Refuses stale epoch, different packet, missing pins or altered instruction order.
    pub fn verify_ack(&self, ack: &PacketAcknowledgement) -> Result<(), ContractError> {
        if ack.version != RUNTIME_PROTOCOL_VERSION
            || ack.attempt_id != self.contract.attempt_id
            || ack.lease_epoch != self.contract.lease_epoch
            || ack.packet_sha256 != self.sha256()?
            || ack.instruction_sha256
                != self
                    .contract
                    .instructions
                    .iter()
                    .map(|pin| pin.sha256.clone())
                    .collect::<Vec<_>>()
        {
            return Err(ContractError::Acknowledgement);
        }
        Ok(())
    }

    fn encoded(&self) -> Result<Vec<u8>, ContractError> {
        serde_json::to_vec(self).map_err(|_| ContractError::Encoding)
    }
}

fn identity(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}
fn digest(value: &str, lengths: &[usize]) -> bool {
    lengths.contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn absolute_clean(path: &Path) -> bool {
    path.is_absolute()
        && !path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
}
