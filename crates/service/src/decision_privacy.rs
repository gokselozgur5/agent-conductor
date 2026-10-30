use agent_conductor_domain::{Command, CommandKind, Task};
use conductor_runtime::redact_credentials;

/// Redacts free decision/blocker text, retaining identities and authority fields.
pub(crate) fn redact_task(task: &mut Task) {
    if let Some(blocker) = &mut task.blocker {
        blocker.detail = redact_credentials(&blocker.detail);
        blocker.resume_when = redact_credentials(&blocker.resume_when);
    }
    for decision in &mut task.decisions {
        decision.question = redact_credentials(&decision.question);
        if let Some(resolution) = &mut decision.resolution {
            resolution.rationale = redact_credentials(&resolution.rationale);
        }
    }
}

/// Call only after validating the original command and hashing its full input.
pub(crate) fn redact_command(command: &mut Command) -> bool {
    let reason = redact_credentials(&command.reason);
    let reason_changed = reason != command.reason;
    command.reason = reason;
    let kind_changed = redact_kind(&mut command.kind);
    reason_changed || kind_changed
}

fn redact_kind(kind: &mut CommandKind) -> bool {
    if let CommandKind::Block { blocker } = kind {
        let detail = redact_credentials(&blocker.detail);
        let resume_when = redact_credentials(&blocker.resume_when);
        let changed = detail != blocker.detail || resume_when != blocker.resume_when;
        blocker.detail = detail;
        blocker.resume_when = resume_when;
        return changed;
    }
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
