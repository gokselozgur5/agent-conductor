use crate::{ExecutionRecord, StoreError};
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

/// Peer credentials observed by the trusted account-channel adapter. This is
/// neither a child PID nor evidence of process exit, capture eligibility or fencing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountExecutionPeer {
    pub worker_uid: u32,
    pub coordinator_uid: u32,
    pub authority_id: String,
    pub boot_generation: u64,
}

impl AccountExecutionPeer {
    pub(crate) fn validate(&self) -> Result<(), StoreError> {
        if self.boot_generation == 0
            || self.authority_id.len() != 64
            || !self
                .authority_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid());
        }
        Ok(())
    }
}

pub(crate) async fn validate_ack(
    connection: &mut SqliteConnection,
    record: &ExecutionRecord,
    peer: &AccountExecutionPeer,
) -> Result<(), StoreError> {
    peer.validate()?;
    let (authority, generation): (Option<String>, i64) = sqlx::query_as("SELECT a.authority_id,r.boot_generation FROM recovery_authority a JOIN runtime_metadata r ON r.singleton=a.singleton WHERE a.singleton=1").fetch_one(connection).await?;
    let packet: serde_json::Value =
        serde_json::from_str(&record.preparation.packet_json).map_err(|_| invalid())?;
    if !record.account_channel
        || authority.as_deref() != Some(&peer.authority_id)
        || u64::try_from(generation).ok() != Some(peer.boot_generation)
        || record.preparation.expected_wrapper_identity.is_none()
        || packet
            .pointer("/contract/full_capability/account/uid")
            .and_then(serde_json::Value::as_u64)
            != Some(u64::from(peer.worker_uid))
        || packet
            .pointer("/contract/authority/network")
            .and_then(serde_json::Value::as_str)
            != Some("unrestricted")
        || packet
            .pointer("/contract/authority/allow_workspace_commands")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn validate_preparation(input: &crate::ExecutionPreparation) -> Result<(), StoreError> {
    let packet: serde_json::Value =
        serde_json::from_str(&input.packet_json).map_err(|_| invalid())?;
    if input.expected_wrapper_identity.is_none()
        || packet
            .pointer("/contract/full_capability/account/uid")
            .and_then(serde_json::Value::as_u64)
            .is_none_or(|uid| u32::try_from(uid).is_err())
        || packet
            .pointer("/contract/authority/network")
            .and_then(serde_json::Value::as_str)
            != Some("unrestricted")
        || packet
            .pointer("/contract/authority/allow_workspace_commands")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn decode(json: Option<String>) -> Result<Option<AccountExecutionPeer>, StoreError> {
    json.map(|text| {
        if text.len() > 1024 {
            return Err(invalid());
        }
        let peer: AccountExecutionPeer = serde_json::from_str(&text).map_err(|_| invalid())?;
        peer.validate()?;
        Ok(peer)
    })
    .transpose()
}

fn invalid() -> StoreError {
    StoreError::Integrity("account peer binding invalid".into())
}
