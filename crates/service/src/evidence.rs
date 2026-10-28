//! Allowlisted historical evidence. Reading it never revalidates source or fences.
use crate::{Clock, PersistentControlService, not_found, store_error};
use agent_conductor_domain::{ActorRole, Command, CommandKind};
use agent_conductor_store::EvidenceRow;
use conductor_protocol::{ErrorBody, ErrorCode, Response, TaskEvidenceEntry};
use conductor_runtime::{CheckOutcome, CheckReceipt};
use serde_json::{Value, json};

impl<C: Clock> PersistentControlService<C> {
    pub(super) async fn evidence(
        &self,
        task_id: &str,
        before: Option<String>,
    ) -> Result<Response, ErrorBody> {
        let before =
            before
                .map(|value| {
                    value.parse::<i64>().ok().filter(|v| *v > 0).ok_or_else(|| {
                        ErrorBody::new(ErrorCode::Validation, "invalid evidence cursor")
                    })
                })
                .transpose()?;
        self.store
            .task(task_id)
            .await
            .map_err(store_error)?
            .ok_or_else(not_found)?;
        let mut rows = self
            .store
            .task_evidence(task_id, before)
            .await
            .map_err(store_error)?;
        let has_more = rows.len() > 100;
        rows.truncate(100);
        let next_before = has_more.then(|| rows.last().expect("full page").sequence.to_string());
        let entries = rows
            .into_iter()
            .map(|row| {
                let details =
                    project(task_id, &row).unwrap_or_else(|| json!({"availability":"unavailable"}));
                TaskEvidenceEntry {
                    sequence: row.sequence.to_string(),
                    kind: row.kind,
                    actor_id: row.actor_id,
                    occurred_at_ms: row.occurred_at_ms,
                    details,
                }
            })
            .collect();
        Ok(Response::TaskEvidence {
            entries,
            next_before,
        })
    }
}

fn project(task_id: &str, row: &EvidenceRow) -> Option<Value> {
    if row.kind.starts_with("execution.") {
        let execution: Value = serde_json::from_str(row.execution.as_deref()?).ok()?;
        let payload: Value = serde_json::from_str(row.payload.as_deref()?).ok()?;
        return Some(
            json!({"availability":"recorded", "current_execution_snapshot":execution,
            "event_revision":payload.get("revision").and_then(Value::as_i64).map(|v| v.to_string())}),
        );
    }
    let (bound_task, command): (String, Command) =
        serde_json::from_str(row.payload.as_deref()?).ok()?;
    if bound_task != task_id || command.actor.id.0 != row.actor_id {
        return None;
    }
    let mut details = command_details(row, &command)?;
    details["availability"] = json!("recorded");
    details["task_version_before"] = json!(command.expected_version.to_string());
    details["actor_role"] = serde_json::to_value(command.actor.role).ok()?;
    Some(details)
}

fn command_details(row: &EvidenceRow, command: &Command) -> Option<Value> {
    match (&*row.kind, &command.kind) {
        (
            "record_check",
            CommandKind::RecordCheck {
                check_id,
                passed,
                source,
            },
        ) => {
            let mut result = json!({"check_id":check_id,"passed":passed,"source":source,
                "receipt_status":"unavailable"});
            if command.actor.role == ActorRole::System
                && command.actor.id.0 == "conductor-check-executor"
                && let Some(receipt) =
                    check_receipt(&command.reason, &check_id.0, *passed, &source.0)
            {
                result["runtime_receipt"] = receipt;
                result["receipt_status"] = json!("recorded_runtime_receipt");
            }
            Some(result)
        }
        ("record_verdict", CommandKind::RecordVerdict { accepted, source }) => {
            let reason: Value = serde_json::from_str(&command.reason).unwrap_or(Value::Null);
            let digest = reason
                .get("output_sha256")
                .and_then(Value::as_str)
                .filter(|s| digest(s));
            Some(
                json!({"accepted":accepted,"source":source,"review_output_sha256":digest,
                "output_digest_status":if digest.is_some() {"recorded"} else {"unavailable"}}),
            )
        }
        ("bind_verification_source", CommandKind::BindVerificationSource { source }) => {
            Some(json!({"source":source}))
        }
        (
            "begin_verification",
            CommandKind::BeginVerification {
                attempt_id,
                lease_epoch,
            },
        ) => Some(json!({"attempt_id":attempt_id,"lease_epoch":lease_epoch.to_string()})),
        ("complete", CommandKind::Complete { observed_source }) => {
            Some(json!({"recorded_completion":true,"source":observed_source}))
        }
        _ => None,
    }
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn check_receipt(reason: &str, check_id: &str, passed: bool, source: &str) -> Option<Value> {
    let receipt: CheckReceipt = serde_json::from_str(reason).ok()?;
    if receipt.check_id != check_id
        || source != format!("snapshot:{}", receipt.source_sha256)
        || passed != (receipt.outcome == CheckOutcome::Passed)
        || (passed && receipt.exit_code != Some(0))
        || !digest(&receipt.source_sha256)
        || !digest(&receipt.definition_sha256)
        || [&receipt.stdout_sha256, &receipt.stderr_sha256]
            .iter()
            .any(|v| v.as_ref().is_some_and(|s| !digest(s)))
    {
        return None;
    }
    // Re-serialization strips unknown fields, including raw output or explanations.
    serde_json::to_value(receipt).ok()
}

#[cfg(test)]
mod tests;
