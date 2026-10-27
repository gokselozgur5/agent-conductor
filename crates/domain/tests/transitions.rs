use agent_conductor_domain::{
    Actor, ActorId, ActorRole, AttemptId, Blocker, CheckId, ClockReading, Command, CommandKind,
    CorrelationId, IdempotencyKey, LeaseStatus, Rejection, RequiredCheck, SourceIdentity, Task,
    TaskId, TaskState, apply,
};

const LEASE_MS: u64 = 1_000;
const MARGIN_MS: u64 = 100;

fn clock(monotonic_ms: u64) -> ClockReading {
    ClockReading {
        boot_generation: 7,
        monotonic_ms,
        wall_unix_ms: 1_800_000_000_000 + i64::try_from(monotonic_ms).unwrap(),
    }
}

fn clock_on_boot(boot_generation: u64, monotonic_ms: u64) -> ClockReading {
    ClockReading {
        boot_generation,
        monotonic_ms,
        wall_unix_ms: 1_800_000_000_000 + i64::try_from(monotonic_ms).unwrap(),
    }
}

fn actor(id: &str, role: ActorRole) -> Actor {
    Actor {
        id: ActorId::from(id),
        role,
    }
}

fn command(task: &Task, actor: Actor, kind: CommandKind) -> Command {
    Command {
        actor,
        expected_version: task.version,
        correlation_id: CorrelationId::from("corr-1"),
        idempotency_key: IdempotencyKey(format!("key-{}", task.version)),
        reason: "test transition".to_owned(),
        kind,
    }
}

fn required_check() -> RequiredCheck {
    RequiredCheck {
        id: CheckId::from("tests"),
        description: "all tests pass".to_owned(),
    }
}

fn ready_task() -> Task {
    let task = Task::new(TaskId::from("task-1"), vec![required_check()]);
    apply(
        &task,
        &command(
            &task,
            actor("operator", ActorRole::Operator),
            CommandKind::MarkReady,
        ),
        clock(0),
    )
    .unwrap()
    .task
}

fn running_task() -> Task {
    let task = ready_task();
    let task = apply(
        &task,
        &command(
            &task,
            actor("worker", ActorRole::Worker),
            CommandKind::Claim {
                attempt_id: AttemptId::from("attempt-1"),
                worker: ActorId::from("worker"),
                lease_duration_ms: LEASE_MS,
                worker_margin_ms: MARGIN_MS,
                recovery_grace_ms: 250,
            },
        ),
        clock(10),
    )
    .unwrap()
    .task;
    let epoch = task.last_lease_epoch;
    apply(
        &task,
        &command(
            &task,
            actor("worker", ActorRole::Worker),
            CommandKind::Start {
                attempt_id: AttemptId::from("attempt-1"),
                lease_epoch: epoch,
            },
        ),
        clock(20),
    )
    .unwrap()
    .task
}

fn verifying_task() -> Task {
    let task = running_task();
    let epoch = task.last_lease_epoch;
    apply(
        &task,
        &command(
            &task,
            actor("worker", ActorRole::Worker),
            CommandKind::BeginVerification {
                attempt_id: AttemptId::from("attempt-1"),
                lease_epoch: epoch,
            },
        ),
        clock(30),
    )
    .unwrap()
    .task
}

#[test]
fn happy_path_requires_fenced_source_checks_and_independent_verdict() {
    let task = verifying_task();
    assert_eq!(task.state, TaskState::Verifying);
    assert_eq!(task.verification.as_ref().unwrap().source, None);

    let source = SourceIdentity::from("git-tree:abc123");
    let task = apply(
        &task,
        &command(
            &task,
            actor("system", ActorRole::System),
            CommandKind::BindVerificationSource {
                source: source.clone(),
            },
        ),
        clock(40),
    )
    .unwrap()
    .task;
    let task = apply(
        &task,
        &command(
            &task,
            actor("system", ActorRole::System),
            CommandKind::RecordCheck {
                check_id: CheckId::from("tests"),
                passed: true,
                source: source.clone(),
            },
        ),
        clock(50),
    )
    .unwrap()
    .task;
    let task = apply(
        &task,
        &command(
            &task,
            actor("verifier", ActorRole::Verifier),
            CommandKind::RecordVerdict {
                accepted: true,
                source: source.clone(),
            },
        ),
        clock(60),
    )
    .unwrap()
    .task;
    let outcome = apply(
        &task,
        &command(
            &task,
            actor("system", ActorRole::System),
            CommandKind::Complete {
                observed_source: source,
            },
        ),
        clock(70),
    )
    .unwrap();

    assert_eq!(outcome.task.state, TaskState::Completed);
    assert_eq!(
        outcome.task.attempt.unwrap().lease.status,
        LeaseStatus::Ended
    );
    assert_eq!(outcome.transition.from, TaskState::Verifying);
    assert_eq!(outcome.transition.to, TaskState::Completed);
}

#[test]
fn stale_epoch_and_worker_deadline_are_rejected() {
    let task = running_task();
    let stale = command(
        &task,
        actor("worker", ActorRole::Worker),
        CommandKind::BeginVerification {
            attempt_id: AttemptId::from("attempt-1"),
            lease_epoch: task.last_lease_epoch + 1,
        },
    );
    assert!(matches!(
        apply(&task, &stale, clock(30)),
        Err(Rejection::InvalidLease(_))
    ));

    let at_worker_deadline = command(
        &task,
        actor("worker", ActorRole::Worker),
        CommandKind::BeginVerification {
            attempt_id: AttemptId::from("attempt-1"),
            lease_epoch: task.last_lease_epoch,
        },
    );
    assert!(matches!(
        apply(&task, &at_worker_deadline, clock(910)),
        Err(Rejection::InvalidLease(_))
    ));
}

#[test]
fn heartbeat_is_allowed_after_mutation_cutoff_but_before_lease_expiry() {
    let task = running_task();
    let heartbeat = command(
        &task,
        actor("worker", ActorRole::Worker),
        CommandKind::Heartbeat {
            attempt_id: AttemptId::from("attempt-1"),
            lease_epoch: task.last_lease_epoch,
            lease_duration_ms: LEASE_MS,
            worker_margin_ms: MARGIN_MS,
        },
    );
    let task = apply(&task, &heartbeat, clock(950)).unwrap().task;
    let lease = &task.attempt.unwrap().lease;
    assert_eq!(lease.deadline_monotonic_ms, 1_950);
    assert_eq!(lease.worker_deadline_monotonic_ms, 1_850);
}

#[test]
fn restart_requires_recovery_heartbeat_before_mutation() {
    let task = running_task();
    let epoch = task.last_lease_epoch;
    let task = apply(
        &task,
        &command(
            &task,
            actor("system", ActorRole::System),
            CommandKind::EnterRecovery,
        ),
        clock_on_boot(8, 5),
    )
    .unwrap()
    .task;
    let lease = &task.attempt.as_ref().unwrap().lease;
    assert_eq!(lease.status, LeaseStatus::RecoveryPending);
    assert_eq!(lease.boot_generation, 8);
    assert_eq!(lease.deadline_monotonic_ms, 255);

    let mutation = command(
        &task,
        actor("worker", ActorRole::Worker),
        CommandKind::BeginVerification {
            attempt_id: AttemptId::from("attempt-1"),
            lease_epoch: epoch,
        },
    );
    assert!(matches!(
        apply(&task, &mutation, clock_on_boot(8, 10)),
        Err(Rejection::InvalidLease(_))
    ));

    let task = apply(
        &task,
        &command(
            &task,
            actor("worker", ActorRole::Worker),
            CommandKind::RecoverHeartbeat {
                attempt_id: AttemptId::from("attempt-1"),
                lease_epoch: epoch,
                lease_duration_ms: LEASE_MS,
                worker_margin_ms: MARGIN_MS,
            },
        ),
        clock_on_boot(8, 20),
    )
    .unwrap()
    .task;
    assert_eq!(
        task.attempt.as_ref().unwrap().lease.status,
        LeaseStatus::Active
    );

    let task = apply(
        &task,
        &command(
            &task,
            actor("worker", ActorRole::Worker),
            CommandKind::BeginVerification {
                attempt_id: AttemptId::from("attempt-1"),
                lease_epoch: epoch,
            },
        ),
        clock_on_boot(8, 30),
    )
    .unwrap()
    .task;
    assert_eq!(task.state, TaskState::Verifying);
}

#[test]
fn pause_and_block_end_the_lease_and_resume_requires_a_new_epoch() {
    for kind in [
        CommandKind::Pause,
        CommandKind::Block {
            blocker: Blocker {
                kind: "external_change".to_owned(),
                detail: "HEAD changed".to_owned(),
                resume_when: "operator reviews recovery snapshot".to_owned(),
            },
        },
    ] {
        let task = running_task();
        let old_epoch = task.last_lease_epoch;
        let task = apply(
            &task,
            &command(&task, actor("operator", ActorRole::Operator), kind),
            clock(40),
        )
        .unwrap()
        .task;
        assert_eq!(
            task.attempt.as_ref().unwrap().lease.status,
            LeaseStatus::Ended
        );

        let task = apply(
            &task,
            &command(
                &task,
                actor("operator", ActorRole::Operator),
                CommandKind::Resume,
            ),
            clock(50),
        )
        .unwrap()
        .task;
        let task = apply(
            &task,
            &command(
                &task,
                actor("worker-2", ActorRole::Worker),
                CommandKind::Claim {
                    attempt_id: AttemptId::from("attempt-2"),
                    worker: ActorId::from("worker-2"),
                    lease_duration_ms: LEASE_MS,
                    worker_margin_ms: MARGIN_MS,
                    recovery_grace_ms: 250,
                },
            ),
            clock(60),
        )
        .unwrap()
        .task;
        assert_eq!(task.last_lease_epoch, old_epoch + 1);
    }
}

#[test]
fn worker_cannot_verify_itself_even_if_presented_as_a_verifier() {
    let task = verifying_task();
    let source = SourceIdentity::from("git-tree:abc123");
    let task = apply(
        &task,
        &command(
            &task,
            actor("system", ActorRole::System),
            CommandKind::BindVerificationSource {
                source: source.clone(),
            },
        ),
        clock(40),
    )
    .unwrap()
    .task;
    let verdict = command(
        &task,
        actor("worker", ActorRole::Verifier),
        CommandKind::RecordVerdict {
            accepted: true,
            source,
        },
    );
    assert!(matches!(
        apply(&task, &verdict, clock(50)),
        Err(Rejection::Unauthorized { .. })
    ));
}

#[test]
fn completion_rejects_missing_or_stale_evidence() {
    let task = verifying_task();
    let source = SourceIdentity::from("git-tree:abc123");
    let task = apply(
        &task,
        &command(
            &task,
            actor("system", ActorRole::System),
            CommandKind::BindVerificationSource {
                source: source.clone(),
            },
        ),
        clock(40),
    )
    .unwrap()
    .task;

    let incomplete = command(
        &task,
        actor("system", ActorRole::System),
        CommandKind::Complete {
            observed_source: source.clone(),
        },
    );
    assert!(matches!(
        apply(&task, &incomplete, clock(50)),
        Err(Rejection::VerificationIncomplete(_))
    ));

    let stale_check = command(
        &task,
        actor("system", ActorRole::System),
        CommandKind::RecordCheck {
            check_id: CheckId::from("tests"),
            passed: true,
            source: SourceIdentity::from("git-tree:different"),
        },
    );
    assert!(matches!(
        apply(&task, &stale_check, clock(60)),
        Err(Rejection::VerificationIncomplete(_))
    ));
}

#[test]
fn worker_cannot_change_acceptance_checks() {
    let task = running_task();
    let add_check = command(
        &task,
        actor("worker", ActorRole::Worker),
        CommandKind::AddRequiredCheck {
            check: RequiredCheck {
                id: CheckId::from("lint"),
                description: "clippy passes".to_owned(),
            },
        },
    );
    assert!(matches!(
        apply(&task, &add_check, clock(30)),
        Err(Rejection::Unauthorized { .. })
    ));
}

#[test]
fn optimistic_version_and_terminal_states_fail_closed() {
    let task = ready_task();
    let mut stale = command(
        &task,
        actor("worker", ActorRole::Worker),
        CommandKind::Claim {
            attempt_id: AttemptId::from("attempt-1"),
            worker: ActorId::from("worker"),
            lease_duration_ms: LEASE_MS,
            worker_margin_ms: MARGIN_MS,
            recovery_grace_ms: 250,
        },
    );
    stale.expected_version -= 1;
    assert_eq!(
        apply(&task, &stale, clock(10)).unwrap_err(),
        Rejection::VersionConflict {
            expected: 0,
            actual: 1
        }
    );

    let cancelled = apply(
        &task,
        &command(
            &task,
            actor("operator", ActorRole::Operator),
            CommandKind::Cancel,
        ),
        clock(20),
    )
    .unwrap()
    .task;
    assert!(matches!(
        apply(
            &cancelled,
            &command(
                &cancelled,
                actor("operator", ActorRole::Operator),
                CommandKind::Resume,
            ),
            clock(30),
        ),
        Err(Rejection::IllegalTransition { .. })
    ));
}
