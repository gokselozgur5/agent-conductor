use agent_conductor_domain::{CommandKind, Task};
use conductor_runtime::redact_credentials;

/// Redacts only free decision text, retaining identities and authority fields.
pub(crate) fn redact_task(task: &mut Task) {
    for decision in &mut task.decisions {
        decision.question = redact_credentials(&decision.question);
        if let Some(resolution) = &mut decision.resolution {
            resolution.rationale = redact_credentials(&resolution.rationale);
        }
    }
}

/// Call only after validating the original command and hashing its full input.
pub(crate) fn redact_command(kind: &mut CommandKind) -> bool {
    let text = match kind {
        CommandKind::OpenDecision { question, .. } => question,
        CommandKind::ResolveDecision { rationale, .. } => rationale,
        _ => return false,
    };
    let redacted = redact_credentials(text);
    if *text == redacted {
        return false;
    }
    *text = redacted;
    true
}

/// Sanitizes presentation fields without rewriting the authoritative task input.
pub(crate) fn redact_task_response(response: &mut conductor_protocol::TaskResponse) {
    response.objective = redact_credentials(&response.objective);
    redact_task(&mut response.aggregate);
}

pub(crate) fn redact_creation_response(
    mut response: conductor_protocol::Response,
) -> conductor_protocol::Response {
    if let conductor_protocol::Response::Task(task) = &mut response {
        redact_task_response(task);
    }
    response
}
