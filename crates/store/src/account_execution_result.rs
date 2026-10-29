use crate::{ExecutionRecord, ExecutionState, StoreError};
use serde::{Deserialize, Serialize};

/// A parsed successful peer report and, optionally, its imported source digest.
/// The execution remains Started/unfenced; neither field asserts local exit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountExecutionResult {
    pub output_sha256: String,
    pub snapshot_sha256: Option<String>,
}

pub(crate) fn report(record: &mut ExecutionRecord, output: &str) -> Result<(), StoreError> {
    eligible(record)?;
    if record.account_result.is_some() || !digest(output) {
        return Err(invalid());
    }
    record.account_result = Some(AccountExecutionResult {
        output_sha256: output.into(),
        snapshot_sha256: None,
    });
    Ok(())
}

pub(crate) fn capture(
    record: &mut ExecutionRecord,
    output: &str,
    snapshot: &str,
) -> Result<(), StoreError> {
    eligible(record)?;
    let result = record.account_result.as_mut().ok_or_else(invalid)?;
    if result.output_sha256 != output || result.snapshot_sha256.is_some() || !digest(snapshot) {
        return Err(invalid());
    }
    result.snapshot_sha256 = Some(snapshot.into());
    Ok(())
}

pub(crate) fn decode(json: Option<String>) -> Result<Option<AccountExecutionResult>, StoreError> {
    json.map(|text| {
        if text.len() > 1024 {
            return Err(invalid());
        }
        let result: AccountExecutionResult = serde_json::from_str(&text).map_err(|_| invalid())?;
        if !digest(&result.output_sha256)
            || result
                .snapshot_sha256
                .as_deref()
                .is_some_and(|s| !digest(s))
        {
            return Err(invalid());
        }
        Ok(result)
    })
    .transpose()
}
fn eligible(record: &ExecutionRecord) -> Result<(), StoreError> {
    if !record.account_channel
        || record.state != ExecutionState::Started
        || record.account_peer.is_none()
        || record.process.is_some()
    {
        return Err(invalid());
    }
    Ok(())
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn invalid() -> StoreError {
    StoreError::Integrity("account result binding invalid".into())
}
