//! Trusted check execution against disposable copies of one identified source.
//! A successful command is check evidence, never a verifier verdict.
use crate::{
    CheckDefinition, NetworkPolicy, ProcessError, SnapshotError, run_process, sandboxed_command,
    snapshot_paths,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::watch;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckOutcome {
    Passed,
    Failed,
    TimedOut,
    Cancelled,
    OutputLimit,
    InfrastructureError,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckReceipt {
    pub check_id: String,
    pub source_sha256: String,
    pub definition_sha256: String,
    pub outcome: CheckOutcome,
    pub exit_code: Option<i32>,
    pub stdout_sha256: Option<String>,
    pub stderr_sha256: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    #[error("invalid approved check definition or source identity")]
    Invalid,
    #[error("check source capture failed: {0}")]
    Snapshot(#[from] SnapshotError),
    #[error("captured source differs from the verification source")]
    SourceChanged,
    #[error("check isolation could not be established")]
    Isolation,
}

/// Executes exact operator-approved argv in a fresh private copy. The original
/// source is never writable by the command. Each check must use a distinct new
/// destination so generated files cannot leak into another check. Read roots are
/// trusted toolchain directories selected by the operator, not model output.
/// Output contents are not retained in receipts; only bounded stream hashes are.
/// # Errors
/// Refuses malformed definitions, source mismatch, existing destinations and
/// unsupported isolation. Runtime failures produce explicit nonpassing receipts.
#[allow(clippy::too_many_arguments)]
pub async fn execute_check(
    source: &Path,
    inventory: &[String],
    expected_source_sha256: &str,
    destination: &Path,
    check: &CheckDefinition,
    toolchain_roots: &[PathBuf],
    cancellation: watch::Receiver<bool>,
) -> Result<CheckReceipt, CheckError> {
    let prepared = prepare_check(
        source,
        inventory,
        expected_source_sha256,
        destination,
        check,
        toolchain_roots,
    )?;
    let result = run_process(&prepared.spec, cancellation).await;
    Ok(prepared.finish(result))
}

/// Captured check input and approved isolated process specification.
pub struct PreparedCheck {
    pub spec: crate::ProcessSpec,
    receipt: CheckReceipt,
}

/// Captures and validates a disposable check before a separately supervised launch.
/// # Errors
/// Same capture, definition and isolation refusals as `execute_check`.
pub fn prepare_check(
    source: &Path,
    inventory: &[String],
    expected_source_sha256: &str,
    destination: &Path,
    check: &CheckDefinition,
    toolchain_roots: &[PathBuf],
) -> Result<PreparedCheck, CheckError> {
    if check.id.trim().is_empty()
        || check.id.len() > 256
        || check.argv.is_empty()
        || check.argv.len() > 128
        || check
            .argv
            .iter()
            .any(|a| a.len() > 16 * 1024 || a.contains('\0'))
        || !Path::new(&check.argv[0]).is_absolute()
        || check.timeout_ms == 0
        || check.timeout_ms > 86_400_000
        || expected_source_sha256.len() != 64
        || !expected_source_sha256
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
    {
        return Err(CheckError::Invalid);
    }
    let manifest = snapshot_paths(source, destination, inventory)?;
    let source_sha256 = manifest.sha256()?;
    if source_sha256 != expected_source_sha256 {
        return Err(CheckError::SourceChanged);
    }
    let definition_sha256 = digest(&serde_json::to_vec(check).map_err(|_| CheckError::Invalid)?);
    let spec = sandboxed_command(
        &destination
            .join("tree")
            .canonicalize()
            .map_err(|_| CheckError::Isolation)?,
        Path::new(&check.argv[0]),
        &check.argv[1..],
        toolchain_roots,
        NetworkPolicy::Denied,
        Duration::from_millis(check.timeout_ms),
    )
    .map_err(|_| CheckError::Isolation)?;
    let receipt = CheckReceipt {
        check_id: check.id.clone(),
        source_sha256,
        definition_sha256,
        outcome: CheckOutcome::InfrastructureError,
        exit_code: None,
        stdout_sha256: None,
        stderr_sha256: None,
    };
    Ok(PreparedCheck { spec, receipt })
}
impl PreparedCheck {
    /// Converts an observed process result into source-bound check evidence.
    #[must_use]
    pub fn finish(self, result: Result<crate::ProcessOutput, ProcessError>) -> CheckReceipt {
        let mut receipt = self.receipt;
        match result {
            Ok(output) => {
                receipt.exit_code = output.exit_code;
                receipt.stdout_sha256 = Some(digest(&output.stdout));
                receipt.stderr_sha256 = Some(digest(&output.stderr));
                receipt.outcome = if output.exit_code == Some(0) {
                    CheckOutcome::Passed
                } else {
                    CheckOutcome::Failed
                };
            }
            Err(ProcessError::Timeout) => receipt.outcome = CheckOutcome::TimedOut,
            Err(ProcessError::Cancelled) => receipt.outcome = CheckOutcome::Cancelled,
            Err(ProcessError::OutputLimit) => receipt.outcome = CheckOutcome::OutputLimit,
            Err(_) => {}
        }
        receipt
    }
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
