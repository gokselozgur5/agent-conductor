use crate::{
    ActorRole, Attempt, CheckResult, ClockReading, Command, CommandKind, Lease, LeaseStatus,
    Rejection, Task, TaskState, TransitionRecord, Verdict, Verification,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Outcome {
    pub task: Task,
    pub transition: TransitionRecord,
}

/// Applies one command without reading a clock or persistence layer implicitly.
///
/// # Errors
///
/// Returns a typed [`Rejection`] when the optimistic version, actor authority,
/// state transition, lease fence, or verification invariant is invalid.
#[allow(clippy::too_many_lines)] // Keeping the transition table contiguous makes omissions auditable.
pub fn apply(task: &Task, command: &Command, clock: ClockReading) -> Result<Outcome, Rejection> {
    if command.expected_version != task.version {
        return Err(Rejection::VersionConflict {
            expected: command.expected_version,
            actual: task.version,
        });
    }
    if task.state.is_terminal() {
        return Err(illegal(task, command));
    }
    if command.reason.trim().is_empty() {
        return Err(Rejection::InvalidCommand("reason must not be empty"));
    }
    if command.idempotency_key.0.trim().is_empty() {
        return Err(Rejection::InvalidCommand(
            "idempotency key must not be empty",
        ));
    }

    let mut next = task.clone();
    let from = next.state;

    match &command.kind {
        CommandKind::MarkReady => {
            require_role(command, &[ActorRole::Operator, ActorRole::System])?;
            require_state(task, command, &[TaskState::Backlog])?;
            next.state = TaskState::Ready;
        }
        CommandKind::Claim {
            attempt_id,
            worker,
            lease_duration_ms,
            worker_margin_ms,
            recovery_grace_ms,
        } => {
            require_state(task, command, &[TaskState::Ready])?;
            require_claim_authority(command, worker)?;
            validate_lease_duration(*lease_duration_ms, *worker_margin_ms)?;
            let epoch = task
                .last_lease_epoch
                .checked_add(1)
                .ok_or(Rejection::InvalidLease("lease epoch overflow"))?;
            let deadline = clock
                .monotonic_ms
                .checked_add(*lease_duration_ms)
                .ok_or(Rejection::InvalidLease("lease deadline overflow"))?;
            let worker_deadline = deadline
                .checked_sub(*worker_margin_ms)
                .ok_or(Rejection::InvalidLease("invalid worker margin"))?;
            next.state = TaskState::Claimed;
            next.last_lease_epoch = epoch;
            next.blocker = None;
            next.verification = None;
            next.attempt = Some(Attempt {
                id: attempt_id.clone(),
                worker: worker.clone(),
                lease: Lease {
                    epoch,
                    boot_generation: clock.boot_generation,
                    deadline_monotonic_ms: deadline,
                    worker_deadline_monotonic_ms: worker_deadline,
                    remaining_at_last_heartbeat_ms: *lease_duration_ms,
                    recovery_grace_ms: *recovery_grace_ms,
                    status: LeaseStatus::Active,
                },
            });
        }
        CommandKind::Start {
            attempt_id,
            lease_epoch,
        } => {
            require_state(task, command, &[TaskState::Claimed])?;
            require_role(command, &[ActorRole::Worker])?;
            validate_owned_worker(task, command, attempt_id, *lease_epoch, clock, true)?;
            next.state = TaskState::Running;
        }
        CommandKind::Heartbeat {
            attempt_id,
            lease_epoch,
            lease_duration_ms,
            worker_margin_ms,
        } => {
            require_state(
                task,
                command,
                &[TaskState::Claimed, TaskState::Running, TaskState::Verifying],
            )?;
            require_role(command, &[ActorRole::Worker])?;
            validate_owned_worker(task, command, attempt_id, *lease_epoch, clock, false)?;
            validate_lease_duration(*lease_duration_ms, *worker_margin_ms)?;
            let deadline = clock
                .monotonic_ms
                .checked_add(*lease_duration_ms)
                .ok_or(Rejection::InvalidLease("lease deadline overflow"))?;
            let worker_deadline = deadline
                .checked_sub(*worker_margin_ms)
                .ok_or(Rejection::InvalidLease("invalid worker margin"))?;
            let attempt = next
                .attempt
                .as_mut()
                .ok_or(Rejection::InvalidLease("task has no active attempt"))?;
            attempt.lease.deadline_monotonic_ms = deadline;
            attempt.lease.worker_deadline_monotonic_ms = worker_deadline;
            attempt.lease.remaining_at_last_heartbeat_ms = *lease_duration_ms;
        }
        CommandKind::EnterRecovery => {
            require_role(command, &[ActorRole::System])?;
            require_state(
                task,
                command,
                &[TaskState::Claimed, TaskState::Running, TaskState::Verifying],
            )?;
            let attempt = next
                .attempt
                .as_mut()
                .ok_or(Rejection::InvalidLease("task has no active attempt"))?;
            if attempt.lease.status != LeaseStatus::Active {
                return Err(Rejection::InvalidLease(
                    "only an active lease can enter recovery",
                ));
            }
            if clock.boot_generation <= attempt.lease.boot_generation {
                return Err(Rejection::InvalidLease(
                    "recovery requires a newer boot generation",
                ));
            }
            let recovery_window = attempt
                .lease
                .remaining_at_last_heartbeat_ms
                .min(attempt.lease.recovery_grace_ms);
            if recovery_window == 0 {
                return Err(Rejection::InvalidLease("recovery window is empty"));
            }
            let deadline = clock
                .monotonic_ms
                .checked_add(recovery_window)
                .ok_or(Rejection::InvalidLease("recovery deadline overflow"))?;
            attempt.lease.boot_generation = clock.boot_generation;
            attempt.lease.deadline_monotonic_ms = deadline;
            attempt.lease.worker_deadline_monotonic_ms = clock.monotonic_ms;
            attempt.lease.status = LeaseStatus::RecoveryPending;
        }
        CommandKind::RecoverHeartbeat {
            attempt_id,
            lease_epoch,
            lease_duration_ms,
            worker_margin_ms,
        } => {
            require_role(command, &[ActorRole::Worker])?;
            require_state(
                task,
                command,
                &[TaskState::Claimed, TaskState::Running, TaskState::Verifying],
            )?;
            validate_lease_duration(*lease_duration_ms, *worker_margin_ms)?;
            let attempt = task
                .attempt
                .as_ref()
                .ok_or(Rejection::InvalidLease("task has no active attempt"))?;
            if attempt.id != *attempt_id
                || attempt.worker != command.actor.id
                || attempt.lease.epoch != *lease_epoch
                || attempt.lease.status != LeaseStatus::RecoveryPending
                || attempt.lease.boot_generation != clock.boot_generation
                || clock.monotonic_ms >= attempt.lease.deadline_monotonic_ms
            {
                return Err(Rejection::InvalidLease(
                    "recovery heartbeat does not match the pending lease",
                ));
            }
            let deadline = clock
                .monotonic_ms
                .checked_add(*lease_duration_ms)
                .ok_or(Rejection::InvalidLease("lease deadline overflow"))?;
            let recovered = next
                .attempt
                .as_mut()
                .ok_or(Rejection::InvalidLease("task has no active attempt"))?;
            recovered.lease.deadline_monotonic_ms = deadline;
            recovered.lease.worker_deadline_monotonic_ms = deadline - *worker_margin_ms;
            recovered.lease.remaining_at_last_heartbeat_ms = *lease_duration_ms;
            recovered.lease.status = LeaseStatus::Active;
        }
        CommandKind::RequeueExpired => {
            require_role(command, &[ActorRole::System])?;
            require_state(
                task,
                command,
                &[TaskState::Claimed, TaskState::Running, TaskState::Verifying],
            )?;
            let attempt = task
                .attempt
                .as_ref()
                .ok_or(Rejection::InvalidLease("task has no active attempt"))?;
            if attempt.lease.status == LeaseStatus::Ended {
                return Err(Rejection::InvalidLease("lease has already ended"));
            }
            if attempt.lease.boot_generation != clock.boot_generation {
                return Err(Rejection::InvalidLease(
                    "lease deadline belongs to another boot generation",
                ));
            }
            let requeue_at = attempt
                .lease
                .deadline_monotonic_ms
                .checked_add(attempt.lease.recovery_grace_ms)
                .ok_or(Rejection::InvalidLease("lease grace deadline overflow"))?;
            if clock.monotonic_ms < requeue_at {
                return Err(Rejection::InvalidLease(
                    "lease deadline and recovery grace have not elapsed",
                ));
            }
            end_lease(&mut next);
            next.state = TaskState::Ready;
            next.verification = None;
        }
        CommandKind::Pause => {
            require_role(command, &[ActorRole::Operator, ActorRole::System])?;
            require_state(
                task,
                command,
                &[
                    TaskState::Ready,
                    TaskState::Claimed,
                    TaskState::Running,
                    TaskState::Verifying,
                ],
            )?;
            end_lease(&mut next);
            next.state = TaskState::Paused;
        }
        CommandKind::Block { blocker } => {
            require_role(command, &[ActorRole::Operator, ActorRole::System])?;
            require_state(
                task,
                command,
                &[
                    TaskState::Ready,
                    TaskState::Claimed,
                    TaskState::Running,
                    TaskState::Verifying,
                ],
            )?;
            if blocker.kind.trim().is_empty()
                || blocker.detail.trim().is_empty()
                || blocker.resume_when.trim().is_empty()
            {
                return Err(Rejection::InvalidCommand(
                    "blocker kind, detail, and resume condition are required",
                ));
            }
            end_lease(&mut next);
            next.state = TaskState::Blocked;
            next.blocker = Some(blocker.clone());
        }
        CommandKind::Resume => {
            require_role(command, &[ActorRole::Operator, ActorRole::System])?;
            require_state(task, command, &[TaskState::Paused, TaskState::Blocked])?;
            next.state = TaskState::Ready;
            next.blocker = None;
            next.verification = None;
        }
        CommandKind::BeginVerification {
            attempt_id,
            lease_epoch,
        } => {
            require_state(task, command, &[TaskState::Running])?;
            require_role(command, &[ActorRole::Worker])?;
            validate_owned_worker(task, command, attempt_id, *lease_epoch, clock, true)?;
            next.state = TaskState::Verifying;
            next.verification = Some(Verification {
                source: None,
                checks: Vec::new(),
                verdict: None,
            });
        }
        CommandKind::BindVerificationSource { source } => {
            require_role(command, &[ActorRole::System])?;
            require_state(task, command, &[TaskState::Verifying])?;
            if source.0.trim().is_empty() {
                return Err(Rejection::InvalidCommand(
                    "source identity must not be empty",
                ));
            }
            let verification = verification_mut(&mut next)?;
            if verification.source.is_some() {
                return Err(Rejection::InvalidCommand(
                    "verification source is already bound",
                ));
            }
            verification.source = Some(source.clone());
        }
        CommandKind::RecordCheck {
            check_id,
            passed,
            source,
        } => {
            require_role(command, &[ActorRole::System, ActorRole::Verifier])?;
            require_state(task, command, &[TaskState::Verifying])?;
            reject_current_worker(task, command)?;
            require_bound_source(task, source)?;
            if !task
                .required_checks
                .iter()
                .any(|check| check.id == *check_id)
            {
                return Err(Rejection::InvalidCommand("check is not required"));
            }
            let actor_id = command.actor.id.clone();
            let verification = verification_mut(&mut next)?;
            verification
                .checks
                .retain(|result| result.check_id != *check_id);
            verification.checks.push(CheckResult {
                check_id: check_id.clone(),
                passed: *passed,
                source: source.clone(),
                recorded_by: actor_id,
            });
        }
        CommandKind::RecordVerdict { accepted, source } => {
            require_role(command, &[ActorRole::Verifier])?;
            require_state(task, command, &[TaskState::Verifying])?;
            require_bound_source(task, source)?;
            if task
                .attempt
                .as_ref()
                .is_some_and(|attempt| attempt.worker == command.actor.id)
            {
                return Err(Rejection::Unauthorized {
                    role: command.actor.role,
                    command: command_name(&command.kind),
                });
            }
            verification_mut(&mut next)?.verdict = Some(Verdict {
                accepted: *accepted,
                source: source.clone(),
                verifier: command.actor.id.clone(),
            });
        }
        CommandKind::RejectVerification => {
            require_role(command, &[ActorRole::Verifier, ActorRole::System])?;
            require_state(task, command, &[TaskState::Verifying])?;
            reject_current_worker(task, command)?;
            let attempt = task
                .attempt
                .as_ref()
                .ok_or(Rejection::InvalidLease("task has no active attempt"))?;
            if !lease_accepts_heartbeat(&attempt.lease, clock) {
                return Err(Rejection::InvalidLease(
                    "cannot resume an expired or recovery-pending attempt",
                ));
            }
            next.state = TaskState::Running;
            next.verification = None;
        }
        CommandKind::Complete { observed_source } => {
            require_role(command, &[ActorRole::Operator, ActorRole::System])?;
            require_state(task, command, &[TaskState::Verifying])?;
            validate_completion(task, observed_source)?;
            end_lease(&mut next);
            next.state = TaskState::Completed;
        }
        CommandKind::AddRequiredCheck { check } => {
            require_role(command, &[ActorRole::Operator, ActorRole::Verifier])?;
            require_state(task, command, &[TaskState::Backlog, TaskState::Ready])?;
            if task
                .attempt
                .as_ref()
                .is_some_and(|attempt| attempt.worker == command.actor.id)
            {
                return Err(Rejection::Unauthorized {
                    role: command.actor.role,
                    command: command_name(&command.kind),
                });
            }
            if check.id.0.trim().is_empty() || check.description.trim().is_empty() {
                return Err(Rejection::InvalidCommand(
                    "check id and description are required",
                ));
            }
            if next
                .required_checks
                .iter()
                .any(|existing| existing.id == check.id)
            {
                return Err(Rejection::InvalidCommand("check already exists"));
            }
            next.required_checks.push(check.clone());
        }
        CommandKind::Cancel => {
            require_role(command, &[ActorRole::Operator, ActorRole::System])?;
            end_lease(&mut next);
            next.state = TaskState::Cancelled;
        }
    }

    next.version = next
        .version
        .checked_add(1)
        .ok_or(Rejection::InvalidCommand("task version overflow"))?;
    Ok(Outcome {
        transition: TransitionRecord {
            actor: command.actor.clone(),
            reason: command.reason.clone(),
            correlation_id: command.correlation_id.clone(),
            idempotency_key: command.idempotency_key.clone(),
            occurred_at_unix_ms: clock.wall_unix_ms,
            from,
            to: next.state,
            resulting_version: next.version,
        },
        task: next,
    })
}

fn validate_completion(
    task: &Task,
    observed_source: &crate::SourceIdentity,
) -> Result<(), Rejection> {
    let verification = task
        .verification
        .as_ref()
        .ok_or(Rejection::VerificationIncomplete("verification is missing"))?;
    let source = verification
        .source
        .as_ref()
        .ok_or(Rejection::VerificationIncomplete(
            "source identity is not bound",
        ))?;
    if source != observed_source {
        return Err(Rejection::VerificationIncomplete("source identity changed"));
    }
    for required in &task.required_checks {
        if !verification.checks.iter().any(|result| {
            result.check_id == required.id && result.passed && result.source == *source
        }) {
            return Err(Rejection::VerificationIncomplete(
                "a required check is missing or failed",
            ));
        }
    }
    let verdict = verification
        .verdict
        .as_ref()
        .ok_or(Rejection::VerificationIncomplete("verdict is missing"))?;
    if !verdict.accepted || verdict.source != *source {
        return Err(Rejection::VerificationIncomplete(
            "verdict is rejected or bound to another source",
        ));
    }
    if task
        .attempt
        .as_ref()
        .is_some_and(|attempt| attempt.worker == verdict.verifier)
    {
        return Err(Rejection::VerificationIncomplete(
            "worker cannot verify its own task",
        ));
    }
    Ok(())
}

fn validate_lease_duration(duration: u64, margin: u64) -> Result<(), Rejection> {
    if duration == 0 || margin >= duration {
        return Err(Rejection::InvalidLease(
            "lease duration must exceed the worker margin",
        ));
    }
    Ok(())
}

fn validate_owned_worker(
    task: &Task,
    command: &Command,
    attempt_id: &crate::AttemptId,
    lease_epoch: u64,
    clock: ClockReading,
    mutation: bool,
) -> Result<(), Rejection> {
    let attempt = task
        .attempt
        .as_ref()
        .ok_or(Rejection::InvalidLease("task has no active attempt"))?;
    if attempt.id != *attempt_id
        || attempt.worker != command.actor.id
        || attempt.lease.epoch != lease_epoch
    {
        return Err(Rejection::InvalidLease(
            "attempt, worker, or epoch is stale",
        ));
    }
    let accepted = if mutation {
        attempt.lease.accepts_worker_mutation(clock)
    } else {
        lease_accepts_heartbeat(&attempt.lease, clock)
    };
    if !accepted {
        return Err(Rejection::InvalidLease(
            "lease is not active for this command",
        ));
    }
    Ok(())
}

fn lease_accepts_heartbeat(lease: &Lease, clock: ClockReading) -> bool {
    lease.status == LeaseStatus::Active
        && lease.boot_generation == clock.boot_generation
        && clock.monotonic_ms < lease.deadline_monotonic_ms
}

fn require_bound_source(task: &Task, source: &crate::SourceIdentity) -> Result<(), Rejection> {
    let bound = task
        .verification
        .as_ref()
        .and_then(|verification| verification.source.as_ref())
        .ok_or(Rejection::VerificationIncomplete(
            "source identity is not bound",
        ))?;
    if bound != source {
        return Err(Rejection::VerificationIncomplete(
            "evidence is bound to another source",
        ));
    }
    Ok(())
}

fn verification_mut(task: &mut Task) -> Result<&mut Verification, Rejection> {
    task.verification
        .as_mut()
        .ok_or(Rejection::VerificationIncomplete("verification is missing"))
}

fn end_lease(task: &mut Task) {
    if let Some(attempt) = &mut task.attempt {
        attempt.lease.status = LeaseStatus::Ended;
    }
}

fn require_claim_authority(command: &Command, worker: &crate::ActorId) -> Result<(), Rejection> {
    match command.actor.role {
        ActorRole::Operator | ActorRole::System => Ok(()),
        ActorRole::Worker if command.actor.id == *worker => Ok(()),
        role => Err(Rejection::Unauthorized {
            role,
            command: command_name(&command.kind),
        }),
    }
}

fn reject_current_worker(task: &Task, command: &Command) -> Result<(), Rejection> {
    if task
        .attempt
        .as_ref()
        .is_some_and(|attempt| attempt.worker == command.actor.id)
    {
        Err(Rejection::Unauthorized {
            role: command.actor.role,
            command: command_name(&command.kind),
        })
    } else {
        Ok(())
    }
}

fn require_role(command: &Command, allowed: &[ActorRole]) -> Result<(), Rejection> {
    if allowed.contains(&command.actor.role) {
        Ok(())
    } else {
        Err(Rejection::Unauthorized {
            role: command.actor.role,
            command: command_name(&command.kind),
        })
    }
}

fn require_state(task: &Task, command: &Command, allowed: &[TaskState]) -> Result<(), Rejection> {
    if allowed.contains(&task.state) {
        Ok(())
    } else {
        Err(illegal(task, command))
    }
}

fn illegal(task: &Task, command: &Command) -> Rejection {
    Rejection::IllegalTransition {
        from: task.state,
        command: command_name(&command.kind),
    }
}

const fn command_name(kind: &CommandKind) -> &'static str {
    match kind {
        CommandKind::MarkReady => "mark_ready",
        CommandKind::Claim { .. } => "claim",
        CommandKind::Start { .. } => "start",
        CommandKind::Heartbeat { .. } => "heartbeat",
        CommandKind::EnterRecovery => "enter_recovery",
        CommandKind::RecoverHeartbeat { .. } => "recover_heartbeat",
        CommandKind::RequeueExpired => "requeue_expired",
        CommandKind::Pause => "pause",
        CommandKind::Block { .. } => "block",
        CommandKind::Resume => "resume",
        CommandKind::BeginVerification { .. } => "begin_verification",
        CommandKind::BindVerificationSource { .. } => "bind_verification_source",
        CommandKind::RecordCheck { .. } => "record_check",
        CommandKind::RecordVerdict { .. } => "record_verdict",
        CommandKind::RejectVerification => "reject_verification",
        CommandKind::Complete { .. } => "complete",
        CommandKind::AddRequiredCheck { .. } => "add_required_check",
        CommandKind::Cancel => "cancel",
    }
}
