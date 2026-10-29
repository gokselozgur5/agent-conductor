use agent_conductor_domain::{
    Actor, ActorId, ActorRole, AttemptId, Blocker, CheckId, ClockReading, Command, CommandKind,
    CorrelationId, EffectIntentSpec, EffectKind, IdempotencyKey, LeaseStatus, Rejection,
    RequiredCheck, SourceIdentity, Task, TaskId, TaskState, apply,
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

#[test]
fn claim_declares_the_worker_spawn_effect_in_the_domain_outcome() {
    let task = ready_task();
    let outcome = apply(
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
    .expect("claim");

    assert_eq!(
        outcome.effects,
        vec![EffectIntentSpec {
            kind: EffectKind::WorkerSpawn,
            attempt_id: AttemptId::from("attempt-1"),
            lease_epoch: 1,
            worker: ActorId::from("worker"),
        }]
    );
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
fn heartbeat_accepts_only_the_instant_before_expiry_even_if_wall_time_moves_back() {
    for task in [running_task(), verifying_task()] {
        let deadline = task.attempt.as_ref().unwrap().lease.deadline_monotonic_ms;
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
        for at in [deadline - 1, deadline, deadline + 1] {
            let mut reading = clock(at);
            // A wall-clock correction cannot revive an expired monotonic lease.
            reading.wall_unix_ms = 0;
            let result = apply(&task, &heartbeat, reading);
            if at < deadline {
                let renewed = result.unwrap().task.attempt.unwrap().lease;
                assert_eq!(renewed.deadline_monotonic_ms, at + LEASE_MS);
                assert_eq!(
                    renewed.worker_deadline_monotonic_ms,
                    at + LEASE_MS - MARGIN_MS
                );
            } else {
                assert!(matches!(result, Err(Rejection::InvalidLease(_))));
            }
        }
    }
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
fn recovery_requires_a_new_generation_and_refences_each_restart() {
    let task = running_task();
    let enter_recovery = |task: &Task| {
        command(
            task,
            actor("system", ActorRole::System),
            CommandKind::EnterRecovery,
        )
    };

    assert!(matches!(
        apply(&task, &enter_recovery(&task), clock_on_boot(7, 30)),
        Err(Rejection::InvalidLease(_))
    ));
    assert!(matches!(
        apply(&task, &enter_recovery(&task), clock_on_boot(6, 30)),
        Err(Rejection::InvalidLease(_))
    ));

    let recovering = apply(&task, &enter_recovery(&task), clock_on_boot(8, 30))
        .unwrap()
        .task;
    let original_deadline = recovering
        .attempt
        .as_ref()
        .unwrap()
        .lease
        .deadline_monotonic_ms;
    let recovered_again = apply(
        &recovering,
        &enter_recovery(&recovering),
        clock_on_boot(9, original_deadline - 1),
    )
    .expect("a later boot generation must fence a pending recovery again")
    .task;
    assert_eq!(
        recovered_again
            .attempt
            .as_ref()
            .unwrap()
            .lease
            .boot_generation,
        9
    );
    assert_eq!(
        recovering
            .attempt
            .as_ref()
            .unwrap()
            .lease
            .deadline_monotonic_ms,
        original_deadline
    );
}

#[test]
fn active_lease_requeues_only_after_deadline_and_grace() {
    let task = verifying_task();
    let requeue = command(
        &task,
        actor("system", ActorRole::System),
        CommandKind::RequeueExpired,
    );

    assert!(matches!(
        apply(&task, &requeue, clock(1_259)),
        Err(Rejection::InvalidLease(_))
    ));
    let outcome = apply(&task, &requeue, clock(1_260)).unwrap();
    assert_eq!(outcome.task.state, TaskState::Ready);
    assert_eq!(outcome.task.version, task.version + 1);
    assert_eq!(outcome.transition.from, TaskState::Verifying);
    assert_eq!(outcome.transition.to, TaskState::Ready);
    assert_eq!(outcome.task.verification, None);
    assert_eq!(
        outcome.task.attempt.as_ref().unwrap().lease.status,
        LeaseStatus::Ended
    );
    assert!(matches!(
        apply(&outcome.task, &requeue, clock(1_260)),
        Err(Rejection::VersionConflict { .. })
    ));
}

#[test]
fn recovery_pending_lease_uses_its_new_deadline_and_grace() {
    let task = running_task();
    let recovering = apply(
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
    let requeue = command(
        &recovering,
        actor("system", ActorRole::System),
        CommandKind::RequeueExpired,
    );

    assert!(matches!(
        apply(&recovering, &requeue, clock_on_boot(8, 504)),
        Err(Rejection::InvalidLease(_))
    ));
    let requeued = apply(&recovering, &requeue, clock_on_boot(8, 505))
        .unwrap()
        .task;
    assert_eq!(requeued.state, TaskState::Ready);
    assert_eq!(
        requeued.attempt.as_ref().unwrap().lease.status,
        LeaseStatus::Ended
    );
}

#[test]
fn requeue_preserves_attempt_but_new_claim_fences_the_old_worker() {
    let task = running_task();
    let old_attempt = task.attempt.as_ref().unwrap().id.clone();
    let old_epoch = task.last_lease_epoch;
    let requeued = apply(
        &task,
        &command(
            &task,
            actor("system", ActorRole::System),
            CommandKind::RequeueExpired,
        ),
        clock(1_260),
    )
    .unwrap()
    .task;
    assert_eq!(requeued.attempt.as_ref().unwrap().id, old_attempt);

    let claimed = apply(
        &requeued,
        &command(
            &requeued,
            actor("worker-2", ActorRole::Worker),
            CommandKind::Claim {
                attempt_id: AttemptId::from("attempt-2"),
                worker: ActorId::from("worker-2"),
                lease_duration_ms: LEASE_MS,
                worker_margin_ms: MARGIN_MS,
                recovery_grace_ms: 250,
            },
        ),
        clock(1_270),
    )
    .unwrap()
    .task;
    assert_eq!(claimed.last_lease_epoch, old_epoch + 1);

    let stale_start = command(
        &claimed,
        actor("worker", ActorRole::Worker),
        CommandKind::Start {
            attempt_id: old_attempt,
            lease_epoch: old_epoch,
        },
    );
    assert!(matches!(
        apply(&claimed, &stale_start, clock(1_280)),
        Err(Rejection::InvalidLease(_))
    ));
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
fn worker_cannot_record_checks_or_reject_verification_as_a_verifier() {
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

    for kind in [
        CommandKind::RecordCheck {
            check_id: CheckId::from("tests"),
            passed: true,
            source,
        },
        CommandKind::RejectVerification,
    ] {
        let disguised_worker = command(&task, actor("worker", ActorRole::Verifier), kind);
        assert!(matches!(
            apply(&task, &disguised_worker, clock(50)),
            Err(Rejection::Unauthorized { .. })
        ));
    }
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
fn acceptance_checks_are_frozen_after_claim() {
    for task in [
        {
            let ready = ready_task();
            apply(
                &ready,
                &command(
                    &ready,
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
            .task
        },
        running_task(),
    ] {
        let add_check = command(
            &task,
            actor("verifier", ActorRole::Verifier),
            CommandKind::AddRequiredCheck {
                check: RequiredCheck {
                    id: CheckId::from("lint"),
                    description: "clippy passes".to_owned(),
                },
            },
        );
        assert!(matches!(
            apply(&task, &add_check, clock(30)),
            Err(Rejection::IllegalTransition { .. })
        ));
    }
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

#[test]
fn decisions_are_bounded_independent_and_editable_only_before_work_or_after_pause() {
    let task = ready_task();
    let open = CommandKind::OpenDecision {
        id: "storage".into(),
        question: "Which retention policy?".into(),
    };
    for role in [ActorRole::Worker, ActorRole::System] {
        assert!(
            apply(
                &task,
                &command(&task, actor("worker", role), open.clone()),
                clock(1)
            )
            .is_err()
        );
    }
    let task = apply(
        &task,
        &command(&task, actor("operator", ActorRole::Operator), open.clone()),
        clock(1),
    )
    .unwrap()
    .task;
    assert_eq!(task.decisions[0].opened_by.0, "operator");
    assert!(
        apply(
            &task,
            &command(&task, actor("operator", ActorRole::Operator), open.clone()),
            clock(2)
        )
        .is_err()
    );
    for candidate in [running_task(), verifying_task()] {
        assert!(
            apply(
                &candidate,
                &command(
                    &candidate,
                    actor("operator", ActorRole::Operator),
                    open.clone()
                ),
                clock(40)
            )
            .is_err()
        );
    }
    let running = running_task();
    let mut paused = apply(
        &running,
        &command(
            &running,
            actor("operator", ActorRole::Operator),
            CommandKind::Pause,
        ),
        clock(40),
    )
    .unwrap()
    .task;
    paused.decisions.clone_from(&task.decisions);
    let resolve = CommandKind::ResolveDecision {
        id: "storage".into(),
        rationale: "Retain artifacts until explicit operator cleanup.".into(),
    };
    for role in [ActorRole::Worker, ActorRole::Operator, ActorRole::Verifier] {
        assert!(
            apply(
                &paused,
                &command(&paused, actor("worker", role), resolve.clone()),
                clock(41)
            )
            .is_err()
        );
    }
    let resolved = apply(
        &paused,
        &command(
            &paused,
            actor("independent", ActorRole::Verifier),
            resolve.clone(),
        ),
        clock(42),
    )
    .unwrap()
    .task;
    assert_eq!(
        resolved.decisions[0]
            .resolution
            .as_ref()
            .unwrap()
            .resolved_by
            .0,
        "independent"
    );
    assert!(
        apply(
            &resolved,
            &command(&resolved, actor("operator", ActorRole::Operator), resolve),
            clock(43)
        )
        .is_err()
    );
}

#[test]
fn decision_text_capacity_and_duplicate_identity_are_bounded() {
    let task = ready_task();
    let task = apply(
        &task,
        &command(
            &task,
            actor("operator", ActorRole::Operator),
            CommandKind::OpenDecision {
                id: "storage".into(),
                question: "Retention?".into(),
            },
        ),
        clock(1),
    )
    .unwrap()
    .task;
    for (id, question) in [
        (String::new(), "question".into()),
        ("x".repeat(129), "question".into()),
        ("new".into(), " ".into()),
        ("new".into(), "q".repeat(1025)),
    ] {
        assert!(
            apply(
                &task,
                &command(
                    &task,
                    actor("operator", ActorRole::Operator),
                    CommandKind::OpenDecision { id, question }
                ),
                clock(2)
            )
            .is_err()
        );
    }
    let mut full = task.clone();
    full.decisions = vec![task.decisions[0].clone(); 32];
    assert!(
        apply(
            &full,
            &command(
                &full,
                actor("operator", ActorRole::Operator),
                CommandKind::OpenDecision {
                    id: "new".into(),
                    question: "question".into()
                }
            ),
            clock(2)
        )
        .is_err()
    );
    for rationale in [" ".into(), "x".repeat(2049)] {
        assert!(
            apply(
                &task,
                &command(
                    &task,
                    actor("operator", ActorRole::Operator),
                    CommandKind::ResolveDecision {
                        id: "storage".into(),
                        rationale
                    }
                ),
                clock(2)
            )
            .is_err()
        );
    }
}

fn enact(task: &Task, id: &str, role: ActorRole, kind: CommandKind) -> Task {
    apply(task, &command(task, actor(id, role), kind), clock(50))
        .unwrap()
        .task
}

fn review_decision_attempt(task: &Task) -> Task {
    let attempt = AttemptId(format!("decision-attempt-{}", task.last_lease_epoch + 1));
    let task = enact(
        task,
        "worker",
        ActorRole::Worker,
        CommandKind::Claim {
            attempt_id: attempt.clone(),
            worker: ActorId::from("worker"),
            lease_duration_ms: LEASE_MS,
            worker_margin_ms: MARGIN_MS,
            recovery_grace_ms: 10,
        },
    );
    let epoch = task.last_lease_epoch;
    let task = enact(
        &task,
        "worker",
        ActorRole::Worker,
        CommandKind::Start {
            attempt_id: attempt.clone(),
            lease_epoch: epoch,
        },
    );
    let mut task = enact(
        &task,
        "worker",
        ActorRole::Worker,
        CommandKind::BeginVerification {
            attempt_id: attempt,
            lease_epoch: epoch,
        },
    );
    let source = SourceIdentity::from("immutable:source");
    for (role, kind) in [
        (
            ActorRole::System,
            CommandKind::BindVerificationSource {
                source: source.clone(),
            },
        ),
        (
            ActorRole::System,
            CommandKind::RecordCheck {
                check_id: CheckId::from("tests"),
                passed: true,
                source: source.clone(),
            },
        ),
        (
            ActorRole::Verifier,
            CommandKind::RecordVerdict {
                accepted: true,
                source,
            },
        ),
    ] {
        task = enact(&task, "independent", role, kind);
    }
    task
}

#[test]
fn unresolved_decision_blocks_valid_completion_until_resolved_and_fresh_attempt_reviewed() {
    let ready = ready_task();
    let decided = enact(
        &ready,
        "operator",
        ActorRole::Operator,
        CommandKind::OpenDecision {
            id: "acceptance".into(),
            question: "Confirm deliverable format".into(),
        },
    );
    let task = review_decision_attempt(&decided);
    assert_eq!(task.decisions, decided.decisions);
    let complete = CommandKind::Complete {
        observed_source: SourceIdentity::from("immutable:source"),
    };
    assert_eq!(
        apply(
            &task,
            &command(&task, actor("system", ActorRole::System), complete.clone()),
            clock(50)
        ),
        Err(Rejection::VerificationIncomplete(
            "task has unresolved decisions"
        ))
    );
    let paused = enact(&task, "operator", ActorRole::Operator, CommandKind::Pause);
    let resolved = enact(
        &paused,
        "operator",
        ActorRole::Operator,
        CommandKind::ResolveDecision {
            id: "acceptance".into(),
            rationale: "Text artifact is required.".into(),
        },
    );
    let resumed = enact(
        &resolved,
        "operator",
        ActorRole::Operator,
        CommandKind::Resume,
    );
    let reviewed = review_decision_attempt(&resumed);
    assert_eq!(reviewed.last_lease_epoch, task.last_lease_epoch + 1);
    assert_eq!(reviewed.decisions, resolved.decisions);
    let completed = enact(&reviewed, "system", ActorRole::System, complete);
    assert_eq!(completed.state, TaskState::Completed);
    assert!(
        apply(
            &completed,
            &command(
                &completed,
                actor("operator", ActorRole::Operator),
                CommandKind::OpenDecision {
                    id: "later".into(),
                    question: "late".into()
                }
            ),
            clock(51)
        )
        .is_err()
    );
}

#[test]
fn legacy_task_without_decisions_decodes_as_empty() {
    let mut old = serde_json::to_value(ready_task()).unwrap();
    old.as_object_mut().unwrap().remove("decisions");
    assert!(
        serde_json::from_value::<Task>(old)
            .unwrap()
            .decisions
            .is_empty()
    );
}
